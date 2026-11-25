"""Command line of the history imports: ``python -m app.history <command>``.

Commands: ``run`` (give the queue's jobs slices until stopped — the service),
``once`` (one slice), ``status`` (the queue, asking nothing of any server),
``pause`` and ``resume`` (by hand), ``retry`` (jobs that gave up back into
the queue), ``enqueue`` (plan the history of accounts, or of markets).

A history import is planned once and then works its way through the pool's
history slice by slice; nobody has to watch it. What it has not verified is
said here and not papered over: a stream that keeps failing is named, and so
is the range above which events are not guaranteed yet.
"""

from __future__ import annotations

import argparse
import faulthandler
import logging
import signal
import sys
import threading
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from stellar_sdk import StrKey

from app import __version__
from app.backfill import config as watch_lists
from app.backfill.jobs import market_key
from app.cli_common import configure_logging, engine_for, make_client, print_table, require_schema, short_name
from app.config import ConfigError, Settings
from app.history import runner as reading
from app.history.runner import (
    DEFAULT_LEASE,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_POLL,
    DEFAULT_RETRY_BASE,
    DEFAULT_RETRY_CAP,
    HistoryWorker,
    Slice,
)
from app.sources.horizon import RateLimited, Unavailable
from app.storage import history_jobs
from app.storage.models import HistoryJob, TrackedAccount, TrackedMarket

logger = logging.getLogger("app.history")

OK, FAILED, STOPPED = 0, 1, 2

TAKEN_BACK = "`python -m app.history %s` takes them back"
# The streams of an account, in the order a person reads them. What a job
# kept comes back in the order JSON keeps keys, which is none.
STREAMS = ("transactions", "operations", "effects", "trades")


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


def _worker(settings: Settings, engine: Engine, args: argparse.Namespace) -> HistoryWorker:
    # History is read in the background: the worker of events has the right
    # of way, and is not starved while history flows.
    return HistoryWorker(
        make_client(settings, engine),
        engine,
        slice_requests=args.slice_requests or settings.history_slice_requests,
        slice_seconds=args.slice_seconds or settings.history_slice_seconds,
        context=settings.history_context,
        max_attempts=args.max_attempts,
        retry_base=args.retry_base,
        retry_cap=args.retry_cap,
        lease_seconds=args.lease_seconds,
    )


def command_run(args: argparse.Namespace, settings: Settings) -> int:
    """The service: give slices until SIGTERM or SIGINT."""
    engine = engine_for(settings)
    if not require_schema(engine, settings, logger):
        engine.dispose()
        return FAILED
    stop = threading.Event()
    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, lambda signum, frame: stop.set())
    with _worker(settings, engine, args) as worker:
        waiting = history_jobs.counts(engine)[history_jobs.WAITING]
        logger.info("history worker %s started: %d job(s) waiting", worker.name, waiting)
        try:
            worker.run(stop, poll_seconds=args.poll_seconds)
        except Exception:  # noqa: BLE001 - the queue or the database is unreachable
            logger.exception("the history worker could not go on")
            return FAILED
    engine.dispose()
    logger.info("stopped on request: %d job(s) done, %d slice(s) failed", worker.done, worker.failed)
    return OK


def command_once(args: argparse.Namespace, settings: Settings) -> int:
    """One slice, of the job that waited longest; then exit."""
    engine = engine_for(settings)
    with _worker(settings, engine, args) as worker:
        taken = worker.once()
        last = worker.last
    engine.dispose()
    if not taken or last is None:
        print("nothing to do: no job is waiting")
        return OK
    print(_saying(last))
    return FAILED if worker.failed else OK


def command_status(args: argparse.Namespace, settings: Settings) -> int:
    """The queue. Nothing is asked of any server."""
    engine = engine_for(settings)
    counts = history_jobs.counts(engine)
    with Session(engine) as session:
        next_try = session.scalar(
            select(func.min(HistoryJob.next_attempt_at)).where(HistoryJob.state == history_jobs.WAITING)
        )
        running = _jobs(
            session,
            select(HistoryJob)
            .where(HistoryJob.state == history_jobs.RUNNING)
            .order_by(HistoryJob.lease_expires_at, HistoryJob.id),
        )
        waiting = _jobs(
            session,
            select(HistoryJob)
            .where(HistoryJob.state == history_jobs.WAITING, HistoryJob.last_error.is_not(None))
            .order_by(HistoryJob.next_attempt_at, HistoryJob.id)
            .limit(args.examples),
        )
        gave_up = _jobs(
            session,
            select(HistoryJob)
            .where(HistoryJob.state.in_(history_jobs.GAVE_UP))
            .order_by(HistoryJob.finished_at.desc().nulls_last(), HistoryJob.id)
            .limit(args.examples),
        )
        paused = _jobs(
            session,
            select(HistoryJob)
            .where(HistoryJob.state == history_jobs.PAUSED)
            .order_by(HistoryJob.id)
            .limit(args.examples),
        )
        # Not final: the boundary may still be raised, and until it is,
        # nothing says every event above it will be heard of.
        not_final = select(HistoryJob).where(HistoryJob.boundary_final.is_(False))
        unfinal = session.scalar(select(func.count()).select_from(not_final.subquery()))
        examples = _jobs(session, not_final.order_by(HistoryJob.id).limit(args.examples))
    engine.dispose()

    print(
        f"queue: {counts[history_jobs.WAITING]} waiting, {counts[history_jobs.RUNNING]} running, "
        f"{counts[history_jobs.DONE]} done, {counts[history_jobs.PARTIAL]} partial, "
        f"{counts[history_jobs.ERROR]} error, {counts[history_jobs.PAUSED]} paused"
    )
    if next_try is not None:
        print(f"next try at {_when(next_try)}")
    if running:
        print_table(
            "running",
            [
                {
                    "job": job.id,
                    "subject": _subject(job),
                    "worker": job.leased_by or "-",
                    "lease until": _when(job.lease_expires_at),
                    "slice": job.slices,
                }
                for job in running
            ],
            ("job", "subject", "worker", "lease until", "slice"),
        )
    if waiting:
        print_table(
            "waiting for another try",
            [
                {
                    "job": job.id,
                    "subject": _subject(job),
                    "try": job.attempts,
                    "next try": _when(job.next_attempt_at),
                    "why": job.last_error or "-",
                }
                for job in waiting
            ],
            ("job", "subject", "try", "next try", "why"),
        )
    if gave_up:
        print_table(
            f"jobs that gave up ({TAKEN_BACK % 'retry'})",
            [
                {
                    "job": job.id,
                    "subject": _subject(job),
                    "state": job.state,
                    "tries": job.attempts,
                    "streams": _streams(job.result),
                    "unavailable": _unavailable(job.result),
                    "why": job.last_error or "-",
                }
                for job in gave_up
            ],
            ("job", "subject", "state", "tries", "streams", "unavailable", "why"),
        )
    if paused:
        print_table(
            f"paused ({TAKEN_BACK % 'resume'})",
            [
                {
                    "job": job.id,
                    "subject": _subject(job),
                    "because": job.paused_because or "-",
                    "streams": _streams(job.result),
                    "unavailable": _unavailable(job.result),
                }
                for job in paused
            ],
            ("job", "subject", "because", "streams", "unavailable"),
        )
    for job in examples:
        print(
            f"unverified range: job {job.id} ({_subject(job)}): "
            f"events are not guaranteed above ledger {job.to_ledger}"
        )
    if unfinal > len(examples):
        print(f"unverified range: and {unfinal - len(examples)} more job(s) whose boundary is not final")
    return OK


def command_pause(args: argparse.Namespace, settings: Settings) -> int:
    """Stop working on jobs until they are resumed."""
    engine = engine_for(settings)
    try:
        paused = _selected(engine, args, lambda **which: history_jobs.pause(engine, because=args.because, **which))
    finally:
        engine.dispose()
    print(f"{paused} job(s) paused: {args.because}")
    return OK


def command_resume(args: argparse.Namespace, settings: Settings) -> int:
    """Paused jobs back into the queue."""
    engine = engine_for(settings)
    try:
        resumed = _selected(engine, args, lambda **which: history_jobs.resume(engine, **which))
    finally:
        engine.dispose()
    print(f"{resumed} job(s) back in the queue")
    return OK


def command_retry(args: argparse.Namespace, settings: Settings) -> int:
    """Take jobs that gave up back into the queue."""
    engine = engine_for(settings)
    try:
        taken = history_jobs.retry(engine, ids=args.id or None, now=datetime.now(UTC))
    finally:
        engine.dispose()
    print(f"{taken} job(s) back in the queue")
    return OK


def command_enqueue(args: argparse.Namespace, settings: Settings) -> int:
    """Plan the history of accounts or markets: up to a ledger given, or up
    to the latest the pool has now."""
    engine = engine_for(settings)
    try:
        with Session(engine) as session:
            wanted = _wanted(session, args)
        if args.to_ledger is None:
            with make_client(settings, engine) as client:
                to_ledger = client.history().latest
        else:
            to_ledger = args.to_ledger
        for kind, subject in wanted:
            job, created = history_jobs.plan(
                engine,
                kind=kind,
                subject=subject,
                to_ledger=to_ledger,
                from_ledger=args.from_ledger,
                context=args.context or settings.history_context,
                # The subject is switched on (said above), so a pause saying
                # it was switched off is out of date.
                resume=True,
                now=datetime.now(UTC),
            )
            print(_planned(job, created))
    except ValueError as error:  # a ConfigError among them
        raise ConfigError(str(error)) from None
    except (Unavailable, RateLimited) as error:
        raise ConfigError(f"the pool cannot be asked for its latest ledger now: {error}") from None
    finally:
        engine.dispose()
    return OK


# -- pieces the commands share -------------------------------------------------


def _jobs(session: Session, chosen: Any) -> list[HistoryJob]:
    return [*session.scalars(chosen)]


def _saying(last: Slice) -> str:
    """One slice in a line: the job, what became of the slice, and why, when
    it did not go as wanted."""
    line = (
        f"job {last.job_id} ({last.kind} {short_name(last.subject)}): {last.outcome}, "
        f"the job is {last.state}, {last.requests} request(s)"
    )
    return line + (f" — {last.error}" if last.error else "")


def _planned(job: HistoryJob, created: bool) -> str:
    bottom = "as far back as the pool keeps" if job.from_ledger is None else f"from ledger {job.from_ledger}"
    line = (
        f"{'planned' if created else 'already planned'} job {job.id}: {_subject(job)}, {bottom} up to ledger "
        f"{job.to_ledger}, {job.state}, context {job.context}"
    )
    if not job.boundary_final:
        line += f"; events are not guaranteed above ledger {job.to_ledger} until the boundary is final"
    return line


def _selected(engine: Engine, args: argparse.Namespace, act: Callable[..., int]) -> int:
    """``act`` over the jobs the command line names — by id, by account, by
    market, or all of them. How many it took."""
    if args.all:
        return act(all=True)
    count = sum(act(ids=[job_id]) for job_id in args.id or ())
    for kind, subject in _named(args):
        count += act(kind=kind, subject=subject)
    return count


def _wanted(session: Session, args: argparse.Namespace) -> list[tuple[str, str]]:
    """The subjects ``enqueue`` is asked to plan, each checked: one that is
    watched and switched on, for a job of it to have streams to read."""
    wanted = _named(args)
    for kind, subject in wanted:
        if kind == "account":
            tracked = session.get(TrackedAccount, subject)
        else:
            tracked = session.get(TrackedMarket, tuple(subject.split(">")))
        if tracked is None or not tracked.enabled:
            raise ConfigError(
                f"{kind} {short_name(subject)} is not watched, or is switched off: its history is not read. "
                "Add it to the watch list, or switch it on, first."
            )
    return wanted


def _named(args: argparse.Namespace) -> list[tuple[str, str]]:
    """(kind, subject) for every account and market the command line names;
    ConfigError when one of them is not what it says it is."""
    named = [("account", _account(given)) for given in args.account or ()]
    named += [("market", _market(base, counter)) for base, counter in args.market or ()]
    return named


def _account(given: str) -> str:
    if not StrKey.is_valid_ed25519_public_key(given):
        raise ConfigError(f"{given!r} is not an account address")
    return given


def _market(base: str, counter: str) -> str:
    """The key of a market given as two assets (``native`` or ``CODE:ISSUER``)."""
    errors: list[str] = []
    pair = [
        watch_lists.parse_asset_id(base, "--market", errors),
        watch_lists.parse_asset_id(counter, "--market", errors),
    ]
    if errors:
        raise ConfigError("; ".join(errors))
    if pair[0] == pair[1]:
        raise ConfigError("a market needs two different assets")
    return market_key(*pair)  # type: ignore[arg-type]


def _subject(job: HistoryJob) -> str:
    return f"{job.kind} {short_name(job.subject)}"


def _streams(result: dict[str, Any] | None) -> str:
    """What became of every stream of a job, in few words: which is not
    covered and why, which is not finished, which is verified."""
    streams = (result or {}).get("streams") or {}
    if not streams:
        return "-"
    def place(name: str) -> tuple[int, str]:
        stream = name.rpartition(" / ")[2]
        return (STREAMS.index(stream) if stream in STREAMS else len(STREAMS)), name

    fates: dict[str, list[str]] = {}
    for name in sorted(streams, key=place):
        fates.setdefault(streams[name]["status"], []).append(name.rpartition(" / ")[2])
    said = []
    if reading.FAILED in fates:
        said.append(f"{', '.join(fates[reading.FAILED])} not covered ({_cause(streams)})")
    if reading.STOPPED in fates:
        said.append(f"{', '.join(fates[reading.STOPPED])} not finished")
    if reading.VERIFIED in fates:
        said.append(f"{', '.join(fates[reading.VERIFIED])} verified")
    return "; ".join(said)


def _cause(streams: dict[str, Any]) -> str:
    """Why the streams that failed did, without the request in it: the
    request is in the job's ``last_error``."""
    causes = [
        stream["error"].split(" to GET ")[0]
        for stream in streams.values()
        if stream["status"] == reading.FAILED and stream["error"]
    ]
    return ", ".join(dict.fromkeys(causes)) or "no reason given"


def _unavailable(result: dict[str, Any] | None) -> str:
    """How many ledgers of the window lie out of the pool's reach."""
    if not result or not result["unavailable_ledgers"]:
        return "-"
    return f"{result['unavailable_ledgers']} ledger(s)"


def _when(moment: datetime | None) -> str:
    return "-" if moment is None else moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# -- the parser ----------------------------------------------------------------


def _naming(sub: argparse.ArgumentParser, *, planned: bool) -> None:
    """The ways a command names what it is about: accounts and markets — for
    ``enqueue``, which plans them (``planned``), or for a command that works
    on jobs that exist, which also names single jobs by their id, or all."""
    what = "the history" if planned else "the job"
    group = sub.add_mutually_exclusive_group(required=True)
    if not planned:
        group.add_argument("--id", type=int, action="append", help="this job; repeatable")
    group.add_argument("--account", nargs="+", metavar="G", help=f"{what} of each of these accounts")
    group.add_argument(
        "--market",
        nargs=2,
        action="append",
        metavar=("BASE", "COUNTER"),
        help=f"{what} of each of these markets (native or CODE:ISSUER); repeatable",
    )
    if not planned:
        group.add_argument("--all", action="store_true", help="every job")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.history",
        description=(
            "Import the history of watched accounts and markets slice by slice, as a queue that survives restarts."
        ),
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument(
        "--log-format", choices=("text", "json"), default="text", help="log lines on stderr (default: text)"
    )
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-file", help="also write the log to this file, line by line as it happens")
    commands = parser.add_subparsers(dest="command", required=True)

    def working(sub: argparse.ArgumentParser) -> None:
        # The budget of a slice is the setting's unless said. What is fetched
        # for a record is not a worker's to change: every job says what it
        # was planned with (`enqueue --context`, or the setting).
        sub.add_argument(
            "--slice-requests",
            type=int,
            help="requests one slice may make before the next subject gets its turn (default: the setting, 50)",
        )
        sub.add_argument("--slice-seconds", type=float, help="how long one slice may take (default: the setting, 60)")
        sub.add_argument(
            "--max-attempts",
            type=int,
            default=DEFAULT_MAX_ATTEMPTS,
            help="slices that fail in a row before a job gives up (default: 10)",
        )
        sub.add_argument(
            "--retry-base",
            type=float,
            default=DEFAULT_RETRY_BASE,
            help="seconds before the second try of a failed slice; doubles every try (default: 30)",
        )
        sub.add_argument(
            "--retry-cap",
            type=float,
            default=DEFAULT_RETRY_CAP,
            help="longest wait between tries in seconds (default: 3600)",
        )
        sub.add_argument(
            "--lease-seconds",
            type=float,
            default=DEFAULT_LEASE,
            help="how long a job is left to the worker that took it without a sign of life (default: 300)",
        )

    sub = commands.add_parser(
        "run",
        help="give the queue's jobs slices until stopped (SIGTERM, SIGINT): the service",
        description="The service: give the jobs of the queue slices until SIGTERM or SIGINT, "
        "the subject that waited longest first.",
    )
    sub.set_defaults(handler=command_run)
    working(sub)
    sub.add_argument(
        "--poll-seconds", type=float, default=DEFAULT_POLL, help="how long to wait when no job is due (default: 5)"
    )

    sub = commands.add_parser(
        "once",
        help="give the job that waited longest one slice, then exit",
        description="One slice, of the job that waited longest; then exit. Exits 1 when the slice failed.",
    )
    sub.set_defaults(handler=command_once)
    working(sub)

    sub = commands.add_parser(
        "status",
        help="the queue: what waits, what is not covered, what is not guaranteed; asks nothing of any server",
        description="The queue. Nothing is asked of any server.",
    )
    sub.set_defaults(handler=command_status)
    sub.add_argument("--examples", type=int, default=5, help="how many jobs to list in each table (default: 5)")

    for name, handler, help_ in (
        ("pause", command_pause, "stop working on jobs until they are resumed"),
        ("resume", command_resume, "paused jobs back into the queue"),
    ):
        sub = commands.add_parser(name, help=help_)
        sub.set_defaults(handler=handler)
        _naming(sub, planned=False)
    commands.choices["pause"].add_argument("--because", required=True, help="why, in a few words: kept with the job")

    sub = commands.add_parser("retry", help="take jobs that gave up back into the queue")
    sub.set_defaults(handler=command_retry)
    sub.add_argument(
        "--id", type=int, action="append", help="only these job(s); repeatable (default: every job that gave up)"
    )

    sub = commands.add_parser("enqueue", help="plan the history of accounts, or of markets")
    sub.set_defaults(handler=command_enqueue)
    _naming(sub, planned=True)
    sub.add_argument("--to-ledger", type=int, help="the upper boundary (default: the latest ledger the pool has now)")
    sub.add_argument("--from-ledger", type=int, help="the lower boundary (default: as far back as the pool keeps)")
    sub.add_argument(
        "--context",
        choices=history_jobs.CONTEXTS,
        help="what is fetched for a record before it is stored (default: the setting, parents)",
    )
    return parser


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
