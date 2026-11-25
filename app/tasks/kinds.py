"""What the four kinds of task do. Each handler takes a :class:`Task` and
returns what the task came to, as plain data; an exception is a task that
failed, with the exception's words kept as its reason.

Nothing here is done in a new way: each handler is the service function the
commands of the earlier stages use — the scan, the check of the pool, the
verifier and the backfill, the reconciliation — with two things a task needs
on top: a budget on what it may ask of Horizon, counted before a request is
sent, and a way to be stopped between two requests.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from sqlalchemy.orm import Session

from app.config import Settings
from app.discovery import scan
from app.pool import check as checking
from app.sources.horizon import Scope
from app.storage import pool_state
from app.storage.models import ServiceJob
from app.subscriptions import reconcile
from app.tasks.context import Budget, Task, TaskRefused, TaskStopped

logger = logging.getLogger(__name__)

# What a task may ask of Horizon when nobody said: a discovery pass is a
# thousand requests (as the command's default is), a check of the pool is
# three of each kind per server, a verification of a window is small or large
# with the window.
DISCOVERY_BUDGET = scan.DEFAULT_MAX_REQUESTS
VERIFY_BUDGET = 2000
PAGE = 200


# -- discovery ---------------------------------------------------------------------


def discovery(task: Task) -> dict[str, Any]:
    """One pass over the holders of an asset: the scan of stage 05, as it is.
    A dry run writes nothing, anywhere. ``resume`` goes on from where the last
    pass that did not finish stopped. The pass is stopped between two
    requests when the task is cancelled: a page it took by then is kept."""
    params = task.params
    asset_id = str(params["asset"])
    dry_run, resume = bool(params.get("dry_run")), bool(params.get("resume"))
    budget = int(params.get("max_requests") or DISCOVERY_BUDGET)
    after = scan.stopped_at(task.engine, asset_id) if resume else None

    def heard(report: scan.ScanReport) -> None:
        task.progress(
            run_id=report.run_id,
            pages=report.pages,
            found=report.found,
            added=len(report.added),
            already=len(report.already),
            disabled=len(report.disabled),
            without_line=len(report.without_line),
            last_address=report.last_address,
        )

    report = scan.scan(
        task.client,
        task.engine,
        asset_id,
        dry_run=dry_run,
        after_address=after,
        max_requests=budget,
        clock=task.clock,
        should_stop=task.stopped.is_set,
        on_page=heard,
    )
    return {**report.as_dict(), "budget": budget, "lines": report.lines()}


# -- the pool ----------------------------------------------------------------------


def check_pool(task: Task) -> dict[str, Any]:
    """Every server of the pool on its own — the check of stage 04, as it is:
    its root and one request for history, up to three times each. The result
    is kept where the status page reads it, as the command keeps it. A server
    that is not available is said so in the result; the task itself did not
    fail on that."""
    repeats = int(task.params.get("repeats") or checking.MAX_REPEATS)
    checked = checking.check_pool(task.client, repeats=repeats)
    with Session(task.engine) as session, session.begin():
        pool_state.save(session, checked.as_dict())
    unavailable = [server.url for server in checked.servers if not server.available]
    return {**checked.as_dict(), "http": task.client.stats.as_dict(), "unavailable": unavailable}


# -- the history of one subject over a window ---------------------------------------


def verify(task: Task) -> dict[str, Any]:
    """One subject and one window: compared with what the pool has
    (``mode = "verify"``), or read again (``mode = "recover"``).

    The comparison is of the records of the subject's streams themselves —
    their ids, read from the pool and stored nowhere — against what is
    stored. The context of a transaction (what was fetched besides its
    streams) is not compared: the verifier of stage 03 does not, and nothing
    more is claimed here.

    A new reading turns the coverage of the window back into pending and
    reads it with the backfill, as ``reimport`` does: a record is stored once
    whatever is read twice, and nothing outside the window is touched.

    Both stop at ``max_requests``, counted before a request is sent. What is
    not finished says so, and is never shown as a match.
    """
    from app.backfill import verify as verifying
    from app.backfill.cli import forget
    from app.backfill.jobs import stream_jobs
    from app.backfill.loader import Backfill, Limits, RunStopped, Window

    params = task.params
    kind, subject = str(params["kind"]), str(params["subject"])
    mode = str(params.get("mode") or "verify")
    window = Window(int(params["from_ledger"]), int(params["to_ledger"]))
    budget = int(params.get("max_requests") or VERIFY_BUDGET)
    if mode not in ("verify", "recover"):
        raise TaskRefused(f"mode is verify or recover, not {mode!r}")
    with Session(task.engine) as session:
        jobs = [job for job in stream_jobs(session)[0] if job.key.subject_type == kind and job.key.subject_key == subject]
    if not jobs:
        raise TaskRefused(f"{kind} {subject[:12]}… is not watched here, or is switched off")
    task.client.scope = Scope(window.from_ledger, window.to_ledger)
    result: dict[str, Any] = {
        "mode": mode,
        "kind": kind,
        "subject": subject,
        "window": [window.from_ledger, window.to_ledger],
        "budget": budget,
        "finished": True,
    }
    first = task.client.stats.requests
    try:
        if mode == "recover":
            forget(task.engine, jobs, window, task.source)
            limits = Limits(
                requests=lambda: task.client.stats.requests - first,
                max_requests=budget,
                stop=task.stopped,
            )
            with Backfill(task.client, task.engine, source=task.source, limits=limits, context=task.settings.history_context) as backfill:
                reports = backfill.run(window, jobs, concurrency=1)
            result["streams"] = {
                report.name: {"status": report.status, "pages": report.pages, "records": report.records, "error": report.error}
                for report in reports
            }
            result["read_again"] = all(report.status == "verified" for report in reports)
        else:
            with Budget(task.client, budget, stop=task.stopped) as spent:
                estimates = verifying.estimate(task.client, jobs, window, PAGE)
                comparisons = verifying.verify(task.client, task.engine, jobs, window, PAGE)
            result["estimate"] = [estimate.as_dict() for estimate in estimates]
            result["streams"] = [comparison.as_dict() for comparison in comparisons]
            result["matches"] = all(comparison.matches for comparison in comparisons)
    except RunStopped as stop:
        if task.stopped.is_set():
            raise TaskStopped("stopped on request") from stop
        result["finished"], result["stopped"] = False, str(stop)
    except Budget.Used as stop:
        if task.stopped.is_set():
            raise TaskStopped("stopped on request") from stop
        result["finished"], result["stopped"] = False, str(stop)
        result["matches"] = False
    result["requests"] = task.client.stats.requests - first
    return result


# -- the Notifier --------------------------------------------------------------------


def reconcile_with_notifier(task: Task) -> dict[str, Any]:
    """One pass with the Notifier — or, with ``dry_run``, only what a pass
    would do, read there and written nowhere. The same pass the worker runs,
    by hand; and the question "what is really there?" for a button."""
    if task.api is None:
        raise TaskRefused(
            "no Notifier API is configured (MTL_HORIZON_NOTIFIER_API_URL): subscriptions are made by hand and adopted"
        )
    callback = task.settings.notifier_callback_url
    if task.params.get("dry_run"):
        planned = reconcile.plan(task.engine, task.api, callback, keys=task.settings.notifier_signer_keys, now=task.clock())
        return {**planned, "dry_run": True}
    report = reconcile.run(task.engine, task.api, callback, now=task.clock())
    return {
        "dry_run": False,
        "wanted": report.wanted,
        "created": report.created,
        "taken": report.taken,
        "confirmed": report.confirmed,
        "failed": report.failed,
        "ambiguous": report.ambiguous,
        "retired": report.retired,
        "problems": report.problems,
        "unreachable": report.unreachable,
        "lines": report.lines(),
    }


HANDLERS: dict[str, Callable[[Task], dict[str, Any]]] = {
    "discovery": discovery,
    "pool_check": check_pool,
    "verify": verify,
    "reconcile": reconcile_with_notifier,
}


def describe(job: ServiceJob) -> str:
    """A task in a few words, for a line of a log or of a table."""
    params = job.params or {}
    if job.kind == "discovery":
        return f"discovery of {params.get('asset', '?')}" + (" — dry run" if params.get("dry_run") else "")
    if job.kind == "pool_check":
        return "a check of the pool"
    if job.kind == "verify":
        return (
            f"a {'new reading' if params.get('mode') == 'recover' else 'comparison'} of "
            f"{params.get('kind', '?')} {str(params.get('subject', '?'))[:8]}… over {params.get('from_ledger')}..{params.get('to_ledger')}"
        )
    return "a dry run with the Notifier" if params.get("dry_run") else "a pass with the Notifier"


def settings_of(settings: Settings) -> dict[str, Any]:  # pragma: no cover - for the tasks page
    """What the tasks are allowed by the settings: their default budgets."""
    return {"discovery_budget": DISCOVERY_BUDGET, "verify_budget": VERIFY_BUDGET}
