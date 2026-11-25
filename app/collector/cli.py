"""Command line of the live collector: ``python -m app.collector <command>``.

Commands: ``once`` (one cycle), ``run`` (one cycle every interval until
stopped), ``status`` (frontiers and lag, asking nothing of the source),
``check`` (what the source does when read forwards, on a small window).
Exit codes of a cycle: 0 every stream is read to the source's latest ledger,
1 a stream failed, 2 stopped by a limit or a signal.

One collector serves a database. While a cycle reads, it holds the source;
a backfill that is reading lets go of it.
"""

from __future__ import annotations

import argparse
import faulthandler
import logging
import resource
import signal
import sys
import threading
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from app import __version__
from app.backfill.jobs import StreamJob, stream_jobs
from app.backfill.loader import CONTEXT_MODES, FULL, Limits
from app.cli_common import (
    configure_logging,
    engine_for,
    make_client,
    print_table,
    short_name,
    source_name,
    source_name_for,
    write_report,
)
from app.collector import check
from app.collector.loader import Collector, CycleReport, WrongNetwork
from app.config import ConfigError, Settings
from app.sources.horizon import HorizonClient
from app.storage import collector_state, locks
from app.storage.models import TrackedAccount, TrackedMarket
from app.storage.locks import AdvisoryLock
from app.storage.writer import HistoryWriter

logger = logging.getLogger("app.collector")

OK, FAILED, STOPPED = 0, 1, 2
# A cycle that could not run at all (the source or the database is down) is
# tried again after a pause that doubles each time, up to this.
MAX_CYCLE_PAUSE = 6 * 3600


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


def command_once(args: argparse.Namespace, settings: Settings) -> int:
    engine = engine_for(settings)
    jobs, left_out = pick_jobs(engine, args)
    stop = threading.Event()
    _stop_on_signal(stop)
    with make_client(args, settings) as client, AdvisoryLock(engine, locks.INSTANCE) as instance:
        if not instance.acquire():
            raise ConfigError("another collector already serves this database: `run` is going, or a `once` is")
        report = _cycle(engine, client, source_name(client), settings, args, jobs, stop)
    engine.dispose()
    _print(report, left_out)
    if args.report:
        write_report(args.report, _report(report, client, left_out))
    return _exit_code(report)


def command_run(args: argparse.Namespace, settings: Settings) -> int:
    engine = engine_for(settings)
    stop = threading.Event()
    _stop_on_signal(stop)
    interval = args.interval if args.interval is not None else settings.collector_interval_seconds
    with make_client(args, settings) as client, AdvisoryLock(engine, locks.INSTANCE) as instance:
        if not instance.acquire():
            raise ConfigError("another collector already serves this database: one is enough")
        source = source_name(client)
        with _collector(client, engine, source, settings, args) as collector:
            due, failures = time.monotonic(), 0
            while not stop.is_set():
                if time.monotonic() < due:
                    stop.wait(due - time.monotonic())
                    continue
                if not instance.confirm():
                    raise ConfigError("this collector lost its lock: its database session is gone")
                jobs, left_out = pick_jobs(engine, args)  # the watch list may have changed
                try:
                    report = _cycle(engine, client, source, settings, args, jobs, stop, collector=collector)
                except WrongNetwork as error:
                    logger.error("%s", error)
                    return FAILED
                except ConfigError as error:
                    # A backfill would not let go in time: asked again next cycle.
                    logger.warning("%s", error)
                    due = time.monotonic() + interval
                    continue
                except Exception as failure:  # noqa: BLE001 - the cycle could not run: the source or the database is down
                    failures += 1
                    logger.exception("cycle failed")
                    _note(engine, source, f"cycle failed: {type(failure).__name__}: {failure}")
                    pause = min(interval * 2**failures, MAX_CYCLE_PAUSE)
                    logger.warning("next cycle in %.0fs", pause)
                    due = time.monotonic() + pause
                    continue
                if report is None:  # stopped while a backfill still held the source
                    break
                failures = 0
                _print(report, left_out)
                due = time.monotonic() + interval
    engine.dispose()
    logger.info("stopped on request")
    return OK


def command_status(args: argparse.Namespace, settings: Settings) -> int:
    """Where every stream has got to, and how far behind it is."""
    engine = engine_for(settings)
    source = source_name_for(args, settings)
    jobs, left_out = pick_jobs(engine, args)
    with Session(engine) as session:
        states = {state.key: state for state in collector_state.streams(session, source)}
        seen = collector_state.source_state(session, source)
    engine.dispose()
    running = locks.is_held(engine, locks.INSTANCE)
    latest = seen.latest_ledger if seen is not None else None
    rows = []
    for job in jobs:
        state = states.get((job.resource_type, job.stream_key))
        if state is None:
            rows.append(
                {
                    "stream": short_name(job.name),
                    "start": "-",
                    "frontier": "not started",
                    "lag": "-",
                    "last success": "-",
                    "failures": "-",
                    "error": "the collector has not run since it was tracked",
                }
            )
            continue
        rows.append(
            {
                "stream": short_name(job.name),
                "start": state.start_ledger,
                "frontier": state.frontier_ledger,
                "lag": latest - state.frontier_ledger if latest is not None else "-",
                "last success": _when(state.last_success_at),
                "failures": state.failures or 0,
                "error": state.last_error or "",
            }
        )
    print_table(
        f"live frontiers, source {source}" + (" (a collector is running)" if running else " (no collector is running)"),
        rows,
        ("stream", "start", "frontier", "lag", "last success", "failures", "error"),
    )
    keys = {(job.resource_type, job.stream_key) for job in jobs}
    frontiers = [states[key].frontier_ledger for key in keys if key in states]
    if frontiers:
        behind = f", {latest - min(frontiers)} ledgers behind the latest ledger seen" if latest is not None else ""
        print(f"global frontier {min(frontiers)}{behind}")
    if seen is not None:
        line = (
            f"source: latest ledger {seen.latest_ledger} ({_when(seen.latest_closed_at)}), "
            f"history kept from {seen.elder_ledger}; cycles {seen.cycles}, last started {_when(seen.last_cycle_at)}"
        )
        if seen.last_error:
            line += f"; ended early: {seen.last_error}"
        print(line)
    gone = len(set(states) - keys)
    if gone:
        print(f"{gone} stream(s) not in the watch list any more are left as they are")
    for line in left_out:
        print(line)
    return OK


def command_check(args: argparse.Namespace, settings: Settings) -> int:
    """Read forwards from one account and one market on a small window, and
    say whether the source behaves as the collector needs."""
    engine = engine_for(settings)
    with Session(engine) as session:
        account = args.account or session.scalars(
            select(TrackedAccount.account_id).where(TrackedAccount.mode == "FULL").order_by(TrackedAccount.account_id).limit(1)
        ).first()
        market = session.scalars(select(TrackedMarket).where(TrackedMarket.enabled.is_(True)).limit(1)).first()
    engine.dispose()
    if account is None or market is None:
        raise ConfigError("check needs a FULL account and an enabled market in the watch list")
    with locks.SourceUse(engine, source_name_for(args, settings)) as use, make_client(args, settings) as client:
        if not use.acquire(wait=args.wait_lock):
            raise ConfigError(f"another reader has held the source for {args.wait_lock}s")
        limits = Limits(requests=lambda: client.stats.requests, max_requests=args.max_requests)
        client.before_request = limits.check
        findings = check.ascending(
            client,
            account=account,
            base_asset_id=market.base_asset_id,
            counter_asset_id=market.counter_asset_id,
            window=args.window,
        )
        ok = check.print_findings(findings)
        print(f"{client.stats.requests} request(s)")
        if args.output:
            write_report(
                args.output, {"ok": ok, "requests": client.stats.as_dict(), "findings": [finding.as_dict() for finding in findings]}
            )
    return OK if ok else FAILED


# -- shared pieces -------------------------------------------------------------


def _collector(client: HorizonClient, engine: Engine, source: str, settings: Settings, args: argparse.Namespace) -> Collector:
    return Collector(
        client,
        engine,
        source=source,
        network_passphrase=settings.network_passphrase,
        page_size=args.page_size,
        context=args.context,
        concurrency=settings.horizon_concurrency,
    )


def _cycle(
    engine: Engine,
    client: HorizonClient,
    source: str,
    settings: Settings,
    args: argparse.Namespace,
    jobs: Sequence[StreamJob],
    stop: threading.Event,
    *,
    collector: Collector | None = None,
) -> CycleReport | None:
    """One cycle, with the collector's right of way: the demand lock is held
    from now until the cycle is over, the source lock while it reads.

    None when stopped while a backfill still held the source.
    """
    with AdvisoryLock(engine, locks.DEMAND + source) as demand, AdvisoryLock(engine, locks.SOURCE + source) as held:
        if not demand.acquire():
            raise ConfigError("another collector is waiting for this source: one collector serves a database")
        if not held.acquire(wait=args.wait_lock, stop=stop):
            if stop.is_set():
                return None
            raise ConfigError(
                f"a backfill has held the source for {args.wait_lock}s. It should let a collector in; "
                f"stop it, or give --wait-lock more seconds."
            )
        limits = _limits(client, args, settings, stop)
        if collector is None:
            with _collector(client, engine, source, settings, args) as once:
                return once.cycle(jobs, start_from=args.start_ledger, limits=limits)
        return collector.cycle(jobs, start_from=args.start_ledger, limits=limits)


def _limits(client: HorizonClient, args: argparse.Namespace, settings: Settings, stop: threading.Event) -> Limits:
    """What this cycle may cost; requests are counted from now."""
    first = client.stats.requests
    max_requests = args.max_requests if args.max_requests is not None else settings.collector_max_requests
    max_seconds = args.max_seconds
    if max_seconds is None:
        max_seconds = settings.collector_max_seconds
    if max_seconds is None:
        interval = args.interval if getattr(args, "interval", None) is not None else settings.collector_interval_seconds
        max_seconds = 0.9 * interval
    return Limits(
        requests=lambda: client.stats.requests - first, max_requests=max_requests, max_seconds=max_seconds, stop=stop
    )


def pick_jobs(engine: Engine, args: argparse.Namespace) -> tuple[list[StreamJob], list[str]]:
    with Session(engine) as session:
        jobs, left_out = stream_jobs(session)
    chosen = [job for job in jobs if not args.subject or any(part in job.name for part in args.subject)]
    return chosen, left_out


def _stop_on_signal(stop: threading.Event) -> None:
    def stop_now(signum: int, frame: Any) -> None:
        stop.set()

    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, stop_now)


def _note(engine: Engine, source: str, error: str) -> None:
    try:
        with HistoryWriter(engine).transaction() as session:
            collector_state.cycle_ended(session, source, error=error)
    except Exception:  # noqa: BLE001 - noting the error must not hide it
        logger.exception("could not note the failed cycle")


def _when(moment: datetime | None) -> str:
    return "-" if moment is None else moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _exit_code(report: CycleReport | None) -> int:
    if report is None:
        return STOPPED
    if report.failed:
        return FAILED
    return STOPPED if report.stopped else OK


def _report(report: CycleReport, client: HorizonClient, left_out: Sequence[str]) -> dict[str, Any]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "version": __version__,
        **report.as_dict(),
        "http": client.stats.as_dict(),
        "process": {
            "max_rss_mb": round(usage.ru_maxrss / 1024, 1),
            "cpu_seconds": round(usage.ru_utime + usage.ru_stime, 1),
        },
        "left_out": list(left_out),
    }


def _print(report: CycleReport | None, left_out: Sequence[str]) -> None:
    if report is None:
        return
    if report.latest_ledger is not None:
        started = f"; {len(report.started)} stream(s) started at {report.start_ledger}" if report.started else ""
        print(
            f"source's latest ledger {report.latest_ledger} ({_when(report.latest_closed_at)}), "
            f"history kept from {report.elder_ledger}; cycle {report.seconds:.1f}s, "
            f"{report.requests} request(s) in all, {report.pages} page(s){started}"
        )
    if report.global_frontier is not None:
        print(f"global frontier {report.global_frontier}, {report.lag} ledger(s) behind")
    rows = [
        {
            "stream": short_name(result.name),
            "status": result.status,
            "start": result.start_ledger,
            "frontier": result.frontier_ledger,
            "lag": result.lag,
            "pages": result.pages,
            "records": result.records,
            "error": result.error or "",
        }
        for result in report.streams
    ]
    print_table("streams", rows, ("stream", "status", "start", "frontier", "lag", "pages", "records", "error"))
    if report.error:
        print(f"cycle ended early: {report.error}")
    for line in left_out:
        print(line)


# -- the parser ----------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.collector", description="Collect the new history of the watch lists from Horizon, cycle by cycle."
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--log-format", choices=("text", "json"), default="text", help="log lines on stderr (default: text)")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-file", help="also write the log to this file, line by line as it happens")
    commands = parser.add_subparsers(dest="command", required=True)

    def source_options(sub: argparse.ArgumentParser) -> None:
        sub.add_argument("--horizon-url", help="Horizon to read (default: MTL_HORIZON_URL or the public server)")
        sub.add_argument("--rate", type=float, help="requests per second (default: MTL_HORIZON_REQUESTS_PER_SECOND or 1)")
        sub.add_argument("--page-size", type=int, default=200, help="records per page, at most 200 (default: 200)")

    def cycle_options(sub: argparse.ArgumentParser) -> None:
        sub.add_argument("--subject", action="append", help="only streams whose name contains this; repeatable")
        sub.add_argument(
            "--start-ledger",
            type=int,
            help="where streams seen for the first time start: live history begins with the next ledger "
            "(default: the source's latest ledger at that moment). Give the ledger a backfill ended at.",
        )
        sub.add_argument(
            "--max-requests", type=int, help="stop the cycle at this many requests (default: MTL_HORIZON_COLLECTOR_MAX_REQUESTS)"
        )
        sub.add_argument("--max-seconds", type=float, help="stop the cycle after this long (default: nine tenths of the interval)")
        sub.add_argument(
            "--context",
            choices=CONTEXT_MODES,
            default=FULL,
            help="what to fetch for an account record's transaction: full (default) or parents. "
            "A market's trades are always read with their parents only.",
        )
        sub.add_argument(
            "--wait-lock", type=int, default=600, help="seconds to wait while a backfill holds the source (default: 600)"
        )

    sub = commands.add_parser("once", help="read every stream to the source's latest ledger, once")
    sub.set_defaults(handler=command_once)
    source_options(sub)
    cycle_options(sub)
    sub.add_argument("--report", help="write a JSON report of the cycle to this file")

    sub = commands.add_parser("run", help="read again every --interval seconds until stopped (SIGTERM, SIGINT)")
    sub.set_defaults(handler=command_run)
    source_options(sub)
    cycle_options(sub)
    sub.add_argument("--interval", type=float, help="seconds between cycles (default: MTL_HORIZON_COLLECTOR_INTERVAL or 900)")

    sub = commands.add_parser("check", help="read forwards from one account and one market on a small window; stores nothing")
    sub.set_defaults(handler=command_check)
    source_options(sub)
    sub.add_argument("--account", help="whose streams to read (default: the first FULL account)")
    sub.add_argument("--window", type=int, default=86400, help="ledgers to read from, below the latest (default: 86400, a day)")
    sub.add_argument("--max-requests", type=int, default=20, help="stop at this many requests (default: 20)")
    sub.add_argument("--wait-lock", type=int, default=60, help="seconds to wait while another reader holds the source (default: 60)")
    sub.add_argument("--output", help="write the findings to this file as JSON")

    sub = commands.add_parser("status", help="frontiers and lag of every stream; asks nothing of the source")
    sub.set_defaults(handler=command_status)
    sub.add_argument("--subject", action="append", help="only streams whose name contains this; repeatable")
    sub.add_argument("--horizon-url", help="the source whose frontiers to show (default: MTL_HORIZON_URL)")
    return parser


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
