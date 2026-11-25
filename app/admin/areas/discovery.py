"""Discovery: the assets whose holders are looked for, how their last passes
went and where the ones that did not finish stopped; and the passes put in
the queue from here — the same task the asset's page puts.

A pass is never run by a request: the request puts a task in the queue
(:mod:`app.storage.service_jobs`) and the worker's background thread takes
it. The page reads the passes and the tasks from the database. The schedule
is an environment setting: the page says what it is and changes nothing.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request
from sqlalchemy.engine import Engine

from app.admin import lists, support
from app.admin.door import Door
from app.discovery import scan
from app.storage import service_jobs
from app.storage.models import DiscoveryRun, ServiceJob
from app.tasks.kinds import DISCOVERY_BUDGET

# What one form may ask of a pass: from one request to the most a pass is given.
MOST_REQUESTS = DISCOVERY_BUDGET
# How many ended tasks the page lists.
ENDED_SHOWN = 30


def budget_of(given: str | None) -> int:
    """What a form asks a pass may cost: a whole number of requests, from one
    to the most a pass is given. A ValueError, in words for the owner."""
    try:
        budget = int(str(given or "").strip())
    except ValueError:
        raise ValueError("бюджет запросов — целое число, например 20") from None
    if not 1 <= budget <= MOST_REQUESTS:
        raise ValueError(f"бюджет запросов — от 1 до {MOST_REQUESTS}")
    return budget


def figures(job: ServiceJob) -> dict[str, Any]:
    """What a task of discovery says of itself, in a few numbers: from its
    result when it has one, else from what it tells while it runs."""
    said = dict(job.result or job.progress or {})
    kept = {
        "страниц": said.get("pages"),
        "найдено": said.get("found"),
        "новых": said.get("added"),
        "уже включены": said.get("already_enabled", said.get("already")),
        "отключены": said.get("disabled"),
        "без линии": said.get("without_line"),
        "эмитент": said.get("issuers_skipped"),
        "запросов": said.get("requests"),
    }
    return {name: value for name, value in kept.items() if value not in (None, "")}


def how_far(job: ServiceJob) -> str | None:
    """The address a task's pass got to; where it began, when it got nowhere."""
    said = dict(job.result or job.progress or {})
    return said.get("last_address") or said.get("after_address")


def stopped_because(job: ServiceJob) -> str | None:
    return (job.result or {}).get("stopped_because")


def where_stopped(engine: Engine, asset_id: str, newest: DiscoveryRun | None) -> str | None:
    """Where a pass is to be continued from: where the last pass over the
    asset stopped, when that one did not finish. One that was interrupted and
    then followed by a whole pass leaves nothing to continue — though the
    service still remembers where it stopped (``scan.stopped_at``)."""
    return scan.stopped_at(engine, asset_id) if newest is not None and newest.state == scan.INCOMPLETE else None


def schedule_in_words(seconds: float) -> str:
    """The schedule of discovery as the environment sets it: off, or how often."""
    if not seconds:
        return "выключено — проходы запускаются только кнопками, по одному активу"
    if seconds >= 3600 and seconds % 3600 == 0:
        return f"раз в {int(seconds // 3600)} ч"
    if seconds >= 60 and seconds % 60 == 0:
        return f"раз в {int(seconds // 60)} мин"
    return f"раз в {seconds:g} с"


def build(door: Door) -> APIRouter:
    router = APIRouter(prefix="/discovery")

    @router.get("")
    def discovery_page(request: Request, login: Any = Depends(door.open)) -> Any:
        engine: Engine = door.engine
        marked, total = lists.assets(engine, discover=True, limit=support.MOST_PER_PAGE, offset=0)
        runs = {row.asset_id: lists.runs(engine, row.asset_id, limit=5) for row in marked}
        stopped = {row.asset_id: where_stopped(engine, row.asset_id, runs[row.asset_id][:1][0] if runs[row.asset_id] else None) for row in marked}
        active = lists.active_tasks(engine)
        tasks = [*active, *service_jobs.recent(engine, states=("done", "failed", "cancelled"), kind="discovery", limit=ENDED_SHOWN)]
        return support.page(
            request, "discovery.html", login, "discovery",
            rows=marked, total=total, runs=runs, stopped=stopped, tasks=tasks, going=len(active),
            found=lists.account_counts(engine).found, schedule=schedule_in_words(door.settings.discovery_interval_seconds),
            figures=figures, how_far=how_far, stopped_because=stopped_because, most=MOST_REQUESTS,
        )

    return router
