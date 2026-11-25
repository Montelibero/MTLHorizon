"""Command line of the worker: ``python -m app.worker <command>``.

Commands: ``run`` (take jobs until stopped — the service), ``once`` (one
job), ``status`` (the queue, the journal and the last ledger seen, asking
nothing of any server), ``retry`` (take jobs that gave up back into the
queue), ``events`` (the last deliveries kept, with what was made of them).

The worker asks Horizon for a transaction when word of it comes, and for
nothing else: no account is polled. ``status`` says so too — the highest
ledger seen in a notification is just that, not a frontier.
"""

from __future__ import annotations

import argparse
import faulthandler
import logging
import signal
import sys
import threading
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app import __version__
from app.cli_common import configure_logging, engine_for, make_client, print_table
from app.config import ConfigError, Settings
from app.storage import events as journal
from app.storage import jobs
from app.storage.models import NotifierEvent, ProcessingJob
from app.storage.pace import EVENTS
from app.worker.loader import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_POLL,
    DEFAULT_RETRY_BASE,
    DEFAULT_RETRY_CAP,
    DEFAULT_LEASE,
    Worker,
)

logger = logging.getLogger("app.worker")

OK, FAILED, STOPPED = 0, 1, 2


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    stream = configure_logging(args.log_format, args.log_level, args.log_file)
    # `kill -USR1 <pid>` writes the stack of every thread to the log.
    try:
        faulthandler.register(signal.SIGUSR1, file=stream, all_threads=True)
    except (OSError, ValueError):  # no real file behind the stream (tests)
        pass
    settings = Settings.from_env()
    try:
        return args.handler(args, settings)
    except ConfigError as error:
        print(f"error: {error}", file=sys.stderr)
        return FAILED
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return STOPPED


# -- commands ------------------------------------------------------------------


def _worker(settings: Settings, engine: Any, args: argparse.Namespace) -> Worker:
    # The worker's requests have the right of way: a backfill or a check
    # waits for them, and is not starved while they flow.
    return Worker(
        make_client(settings, engine, share=EVENTS),
        engine,
        max_attempts=args.max_attempts,
        retry_base=args.retry_base,
        retry_cap=args.retry_cap,
        lease_seconds=args.lease_seconds,
    )


def command_run(args: argparse.Namespace, settings: Settings) -> int:
    """The service: take jobs until SIGTERM or SIGINT."""
    engine = engine_for(settings)
    stop = threading.Event()
    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, lambda signum, frame: stop.set())
    with _worker(settings, engine, args) as worker:
        logger.info("worker %s started: %d job(s) waiting", worker.name, jobs.counts(engine)["pending"])
        try:
            worker.run(stop, poll_seconds=args.poll_seconds)
        except Exception:  # noqa: BLE001 - the queue or the database is unreachable
            logger.exception("the worker could not go on")
            return FAILED
    engine.dispose()
    logger.info("stopped on request: %d done, %d failed", worker.done, worker.failed)
    return OK


def command_once(args: argparse.Namespace, settings: Settings) -> int:
    """One job, the next whose time has come; then exit."""
    engine = engine_for(settings)
    with _worker(settings, engine, args) as worker:
        taken = worker.once()
    engine.dispose()
    if not taken:
        print("nothing to do: no job is waiting")
        return OK
    print(f"one job: {worker.done} done, {worker.failed} not done")
    return OK if worker.done else FAILED


def command_status(args: argparse.Namespace, settings: Settings) -> int:
    """The queue and the journal. Nothing is asked of any server."""
    engine = engine_for(settings)
    with Session(engine) as session:
        counts = dict.fromkeys(jobs.STATES, 0)
        for state, number in session.execute(select(ProcessingJob.state, func.count()).group_by(ProcessingJob.state)):
            counts[state] = number
        next_try = session.scalar(
            select(func.min(ProcessingJob.next_attempt_at)).where(ProcessingJob.state == jobs.PENDING)
        )
        running = list(
            session.scalars(
                select(ProcessingJob).where(ProcessingJob.state == jobs.RUNNING).order_by(ProcessingJob.lease_expires_at)
            )
        )
        gave_up = list(
            session.scalars(
                select(ProcessingJob)
                .where(ProcessingJob.state.in_(jobs.GAVE_UP))
                .order_by(ProcessingJob.finished_at.desc().nulls_last())
                .limit(args.examples)
            )
        )
        kept = dict.fromkeys(journal.STATES, 0)
        for state, number in session.execute(select(NotifierEvent.state, func.count()).group_by(NotifierEvent.state)):
            kept[state] = number
        last_received = session.scalar(select(func.max(NotifierEvent.received_at)))
        highest = session.scalar(select(func.max(NotifierEvent.ledger_sequence)))
        size = session.execute(
            text("SELECT pg_size_pretty(pg_total_relation_size('notifier_event')), pg_total_relation_size('notifier_event')")
        ).one()
    engine.dispose()

    print(
        f"queue: {counts['pending']} pending, {counts['running']} running, {counts['done']} done, "
        f"{counts['partial']} partial, {counts['dead']} dead"
    )
    if next_try is not None:
        print(f"next try at {_when(next_try)}")
    rows = [
        {
            "job": job.id,
            "transaction": job.transaction_hash[:8],
            "ledger": job.ledger_sequence or "-",
            "worker": job.leased_by or "-",
            "lease until": _when(job.lease_expires_at),
            "try": job.attempts,
        }
        for job in running
    ]
    if rows:
        print_table("running", rows, ("job", "transaction", "ledger", "worker", "lease until", "try"))
    if gave_up:
        print_table(
            "jobs that gave up (`python -m app.worker retry` takes them back)",
            [
                {
                    "job": job.id,
                    "transaction": job.transaction_hash[:8],
                    "state": job.state,
                    "tries": job.attempts,
                    "stored": ",".join(job.result.get("stored", [])) if job.result else "-",
                    "why": job.last_error or "-",
                }
                for job in gave_up
            ],
            ("job", "transaction", "state", "tries", "stored", "why"),
        )
    print(
        f"notifications: {sum(kept.values())} kept — {kept['queued']} queued, {kept['unmatched']} unmatched, "
        f"{kept['unsupported']} unsupported; last received {_when(last_received)}"
    )
    if highest is not None:
        print(
            f"highest ledger among notifications: {highest} — that is the highest seen, not a frontier: "
            f"nothing says every ledger below it, or every transaction in it, was seen"
        )
    print(f"journal size: {size[0]}")
    return OK


def command_retry(args: argparse.Namespace, settings: Settings) -> int:
    """Take jobs that gave up back into the queue."""
    engine = engine_for(settings)
    taken = jobs.retry(engine, ids=args.id or None)
    engine.dispose()
    print(f"{taken} job(s) back in the queue")
    return OK


def command_events(args: argparse.Namespace, settings: Settings) -> int:
    """The last deliveries kept, newest first."""
    engine = engine_for(settings)
    with Session(engine) as session:
        chosen = select(NotifierEvent).order_by(NotifierEvent.received_at.desc(), NotifierEvent.id).limit(args.limit)
        if args.state:
            chosen = chosen.where(NotifierEvent.state == args.state)
        kept = list(session.scalars(chosen))
    engine.dispose()
    print_table(
        f"last {len(kept)} notification(s)",
        [
            {
                "received": _when(event.received_at),
                "id": event.id[:12],
                "subscription": event.subscription[:12],
                "operation": event.operation_type or "-",
                "ledger": event.ledger_sequence or "-",
                "state": event.state,
                "job": event.job_id or "-",
                "matched": ", ".join(f"{rule['rule']} {rule['key'][:14]}" for rule in event.matched) or "-",
                "diagnosis": (event.diagnosis or "")[:70],
            }
            for event in kept
        ],
        ("received", "id", "subscription", "operation", "ledger", "state", "job", "matched", "diagnosis"),
    )
    return OK


def _when(moment: datetime | None) -> str:
    return "-" if moment is None else moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# -- the parser ----------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.worker",
        description="Bring in the transactions Operations Notifier tells of, whole; ask for nothing else.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--log-format", choices=("text", "json"), default="text", help="log lines on stderr (default: text)")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-file", help="also write the log to this file, line by line as it happens")
    commands = parser.add_subparsers(dest="command", required=True)

    def working(sub: argparse.ArgumentParser) -> None:
        sub.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS, help="tries before a job gives up (default: 8)")
        sub.add_argument(
            "--retry-base", type=float, default=DEFAULT_RETRY_BASE, help="seconds before the second try; doubles every try (default: 5)"
        )
        sub.add_argument("--retry-cap", type=float, default=DEFAULT_RETRY_CAP, help="longest wait between tries in seconds (default: 600)")
        sub.add_argument(
            "--lease-seconds",
            type=float,
            default=DEFAULT_LEASE,
            help="how long a job is left to the worker that took it without a sign of life (default: 120)",
        )

    sub = commands.add_parser("run", help="take jobs until stopped (SIGTERM, SIGINT): the service")
    sub.set_defaults(handler=command_run)
    working(sub)
    sub.add_argument("--poll-seconds", type=float, default=DEFAULT_POLL, help="how long to wait when no job is waiting (default: 2)")

    sub = commands.add_parser("once", help="take the next job whose time has come, do it, exit")
    sub.set_defaults(handler=command_once)
    working(sub)

    sub = commands.add_parser("status", help="the queue, the journal and the highest ledger seen; asks nothing of any server")
    sub.set_defaults(handler=command_status)
    sub.add_argument("--examples", type=int, default=5, help="how many jobs that gave up to list (default: 5)")
    sub.add_argument("--output", help="also write the status to this file as JSON")

    sub = commands.add_parser("retry", help="take jobs that gave up back into the queue")
    sub.set_defaults(handler=command_retry)
    sub.add_argument("--id", type=int, action="append", help="only these job(s); repeatable (default: every job that gave up)")

    sub = commands.add_parser("events", help="the last notifications kept, newest first")
    sub.set_defaults(handler=command_events)
    sub.add_argument("--state", choices=journal.STATES, help="only notifications in this state")
    sub.add_argument("--limit", type=int, default=20, help="how many to list (default: 20)")
    return parser


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
