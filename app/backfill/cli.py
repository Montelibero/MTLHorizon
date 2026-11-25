"""Command line of the backfill: ``python -m app.backfill <command>``.

Commands: ``import-config``, ``probe``, ``estimate``, ``run``, ``status``,
``reimport``, ``verify``, ``fingerprint``. Exit codes of a run: 0 verified,
1 a stream failed, 2 stopped by a limit or a signal, 3 finished with ranges
the source cannot verify.
"""

from __future__ import annotations

import argparse
import faulthandler
import json
import logging
import re
import resource
import signal
import sys
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

from app import __version__
from app.backfill import config as watch_lists
from app.backfill import verify as checking
from app.backfill.jobs import StreamJob, stream_jobs
from app.backfill.loader import Backfill, Limits, Window
from app.backfill.probe import Market, probe
from app.backfill.verify import print_table
from app.config import ConfigError, Settings
from app.storage import coverage
from app.storage.db import create_db_engine
from app.storage.models import Base
from app.storage.network import assert_network
from app.storage.upsert import checkpoint
from app.storage.writer import HistoryWriter
from app.sources.horizon import HorizonClient, HorizonSettings

logger = logging.getLogger("app.backfill")

OK, FAILED, STOPPED, UNVERIFIED = 0, 1, 2, 3

Command = Callable[[argparse.Namespace, Settings], int]


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    stream = configure_logging(args.log_format, args.log_level, args.log_file)
    # `kill -USR1 <pid>` writes the stack of every thread to the log.
    try:
        faulthandler.register(signal.SIGUSR1, file=stream, all_threads=True)
    except (OSError, ValueError):  # no real file behind the stream (tests)
        pass
    settings = Settings.from_env()
    started = time.monotonic()
    try:
        return args.handler(args, settings)
    except (ConfigError, watch_lists.ConfigError) as error:
        print(f"error: {error}", file=sys.stderr)
        return FAILED
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return STOPPED
    finally:
        logger.debug("command finished in %.1fs", time.monotonic() - started)


# -- commands ------------------------------------------------------------------


def command_import_config(args: argparse.Namespace, settings: Settings) -> int:
    config = watch_lists.load(Path(args.file))
    if args.dry_run:
        print("dry run: nothing is written")
    engine = engine_for(settings)
    with Session(engine) as session:
        session.begin()
        result = watch_lists.apply(session, config, change_modes=args.apply_changes)
        if args.dry_run:
            session.rollback()
        else:
            session.commit()
    for line in result.lines():
        print(line)
    engine.dispose()
    return OK


def command_probe(args: argparse.Namespace, settings: Settings) -> int:
    engine = engine_for(settings)
    with Session(engine) as session:
        market, account = _probe_subjects(session, args)
    engine.dispose()
    with make_client(args, settings) as client:
        result = probe(client, network_passphrase=settings.network_passphrase, market=market, account=account)
    result["checked_at"] = datetime.now(UTC).isoformat(timespec="seconds")
    result["requests"] = client.stats.as_dict()
    text_ = json.dumps(result, indent=1, ensure_ascii=False, default=str)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(text_ + "\n", encoding="utf-8")
        print(f"written to {args.output}")
    else:
        print(text_)
    return OK


def command_estimate(args: argparse.Namespace, settings: Settings) -> int:
    engine = engine_for(settings)
    with make_client(args, settings) as client:
        window = resolve_window(client, args, settings)
        jobs = pick_jobs(engine, args)
        estimates = checking.estimate(client, jobs, window, args.page_size)
    rows = [estimate.as_dict() for estimate in estimates]
    print_table(f"window [{window.from_ledger}, {window.to_ledger}]", rows, ("stream", "pages", "records", "transactions", "context_requests_upper_bound"))
    pages = sum(row["pages"] for row in rows)
    contexts = sum(row["context_requests_upper_bound"] for row in rows)
    rate = client.settings.requests_per_second
    print(f"\npages to read: {pages}; context requests at most {contexts}; "
          f"at {rate:g} requests/s that is up to {(pages + contexts) / rate / 60:.0f} minutes")
    engine.dispose()
    return OK


def command_run(args: argparse.Namespace, settings: Settings) -> int:
    engine = engine_for(settings)
    with make_client(args, settings) as client:
        window = resolve_window(client, args, settings)
        jobs, left_out = pick_jobs(engine, args, with_left_out=True)
        limits = Limits(
            requests=lambda: client.stats.requests,
            max_pages=args.max_pages,
            max_requests=args.max_requests,
            max_seconds=args.max_seconds,
        )
        _stop_on_signal(limits)
        started = time.monotonic()
        with Backfill(client, engine, source=source_name(client), page_size=args.page_size, limits=limits) as backfill:
            if args.command == "reimport":
                forget(engine, jobs, window, source_name(client))
            reports = backfill.run(window, jobs, concurrency=settings.horizon_concurrency)
            summary = _summary(window, client, backfill, reports, started, left_out)
    engine.dispose()
    _print_summary(summary)
    if args.report:
        _write_report(args.report, summary)
    return _exit_code(reports)


def command_status(args: argparse.Namespace, settings: Settings) -> int:
    engine = engine_for(settings)
    with Session(engine) as session:
        jobs, left_out = stream_jobs(session)
        jobs = pick_jobs(engine, args)
        rows = []
        for job in jobs:
            state = checkpoint(
                session, source=source_name_for(args, settings), resource_type=job.resource_type,
                stream_key=job.stream_key, direction="desc",
            )
            ranges = coverage.ranges(session, job.key)
            if args.from_ledger and args.to_ledger:
                ranges = [r for r in ranges if r.to_ledger >= args.from_ledger and r.from_ledger <= args.to_ledger]
            rows.append({"stream": job.name, "ranges": " ".join(f"{r.state}:{r.from_ledger}..{r.to_ledger}" for r in ranges) or "-", "checkpoint": (state[0] or "-") if state else "-"})
        for name, table in Base.metadata.tables.items():
            if name == "alembic_version":
                continue
            rows.append({"stream": f"rows {name}", "ranges": str(session.scalar(select(func.count()).select_from(table))), "checkpoint": ""})
    print_table("coverage (state:from..to)", rows, ("stream", "ranges", "checkpoint"))
    for line in left_out:
        print(line)
    engine.dispose()
    return OK


def command_verify(args: argparse.Namespace, settings: Settings) -> int:
    engine = engine_for(settings)
    with make_client(args, settings) as client:
        window = resolve_window(client, args, settings)
        jobs = pick_jobs(engine, args)
        comparisons = checking.verify(client, engine, jobs, window, args.page_size)
        matches = all(comparison.matches for comparison in comparisons)
        if args.aggregations:
            matches = _verify_aggregations(client, engine, window) and matches
    rows = [comparison.as_dict() for comparison in comparisons]
    print_table(f"stored against the source, window [{window.from_ledger}, {window.to_ledger}]", rows, ("stream", "source", "stored", "matches", "missing", "extra"))
    for row in rows:
        if row["missing_examples"]:
            print(f"  missing from storage, e.g. {row['stream']}: {row['missing_examples']}")
        if row["extra_examples"]:
            print(f"  in storage but not in the source stream, e.g. {row['stream']}: {row['extra_examples']}")
    engine.dispose()
    return OK if matches else FAILED


def _verify_aggregations(client: HorizonClient, engine: Engine, window: Window) -> bool:
    """Every tracked market, whole UTC days of the window: stored sums against
    what the source aggregates."""
    from app.backfill.verify import whole_days
    from app.sources.horizon_format import stroops_to_amount
    from app.storage.models import TrackedMarket

    first_day, days = whole_days(client, window)
    if days == 0:
        print("aggregations: the window holds no whole UTC day")
        return True
    matches = True
    with Session(engine) as session:
        markets = list(session.scalars(select(TrackedMarket).where(TrackedMarket.enabled.is_(True))))
    for market in markets:
        comparisons = checking.compare_aggregations(
            client, engine, market.base_asset_id, market.counter_asset_id, first_day, days
        )
        print(
            f"\n{market.base_asset_id.split(':')[0]} / {market.counter_asset_id.split(':')[0]} "
            f"against /trade_aggregations, {days} whole UTC days"
        )
        print_table(
            "per UTC day, source against stored",
            [
                {
                    "day": comparison.day,
                    "trades source/stored": f"{comparison.source['trades']}/{comparison.stored['trades']}",
                    "base volume source/stored": f"{stroops_to_amount(comparison.source['base_volume'])}/{stroops_to_amount(comparison.stored['base_volume'])}",
                    "counter volume source/stored": f"{stroops_to_amount(comparison.source['counter_volume'])}/{stroops_to_amount(comparison.stored['counter_volume'])}",
                }
                for comparison in comparisons
            ],
            ("day", "trades source/stored", "base volume source/stored", "counter volume source/stored"),
        )
        matches = all(comparison.matches for comparison in comparisons) and matches
    return matches


def command_fingerprint(args: argparse.Namespace, settings: Settings) -> int:
    engine = engine_for(settings)
    window = Window(args.from_ledger, args.to_ledger)
    print(json.dumps({"window": [window.from_ledger, window.to_ledger], "tables": checking.fingerprint(engine, window)}, indent=1))
    engine.dispose()
    return OK


# -- shared pieces -------------------------------------------------------------


def _probe_subjects(session: Session, args: argparse.Namespace) -> tuple[Market | None, str | None]:
    from app.storage.models import TrackedAccount, TrackedMarket

    market = None
    if args.base and args.counter:
        market = (args.base, args.counter)
    else:
        tracked = session.scalars(select(TrackedMarket).where(TrackedMarket.enabled.is_(True)).limit(1)).first()
        market = (tracked.base_asset_id, tracked.counter_asset_id) if tracked else None
    account = args.account or session.scalars(
        select(TrackedAccount.account_id).where(TrackedAccount.mode == "FULL").order_by(TrackedAccount.account_id).limit(1)
    ).first()
    return market, account


def pick_jobs(engine: Engine, args: argparse.Namespace, with_left_out: bool = False):
    with Session(engine) as session:
        jobs, left_out = stream_jobs(session)
    chosen = [job for job in jobs if not args.subject or any(part in job.name for part in args.subject)]
    return (chosen, left_out) if with_left_out else chosen


def source_name(client: HorizonClient) -> str:
    return f"horizon:{urlsplit(client.settings.base_url).netloc}"


def source_name_for(args: argparse.Namespace, settings: Settings) -> str:
    return f"horizon:{urlsplit(args.horizon_url or settings.horizon_url).netloc}"


def make_client(args: argparse.Namespace, settings: Settings) -> HorizonClient:
    return HorizonClient(
        HorizonSettings(
            base_url=args.horizon_url or settings.horizon_url,
            requests_per_second=args.rate or settings.horizon_requests_per_second,
        )
    )


def engine_for(settings: Settings) -> Engine:
    if not settings.database_url:
        raise ConfigError("MTL_HORIZON_DATABASE_URL is not set")
    engine = create_db_engine(settings.database_url)
    with engine.connect() as connection, connection.begin():
        assert_network(connection, settings.network_passphrase)
    return engine


def resolve_window(client: HorizonClient, args: argparse.Namespace, settings: Settings) -> Window:
    if args.command not in ("run", "reimport"):
        return Window(args.from_ledger, args.to_ledger)
    root = client.root()
    if root.get("network_passphrase") != settings.network_passphrase:
        raise ConfigError(
            f"{client.settings.base_url} serves the network {root.get('network_passphrase')!r}, "
            f"this installation is configured for {settings.network_passphrase!r}"
        )
    to_ledger = args.to_ledger or int(root["history_latest_ledger"])
    from_ledger = args.from_ledger or int(root["history_elder_ledger"])
    return Window(from_ledger, to_ledger)


def forget(engine: Engine, jobs: Sequence[StreamJob], window: Window, source: str) -> None:
    """Turn the coverage of the window back into pending, for a reimport."""
    with HistoryWriter(engine).transaction() as session:
        for job in jobs:
            coverage.invalidate(session, job.key, source, window.from_ledger, window.to_ledger)
    logger.info("coverage of [%d, %d] reset for %d streams", window.from_ledger, window.to_ledger, len(jobs))


def _stop_on_signal(limits: Limits) -> None:
    def stop(signum: int, frame: Any) -> None:
        limits.stop.set()

    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, stop)


def _summary(window: Window, client: HorizonClient, backfill: Backfill, reports: Sequence[Any], started: float, left_out: Sequence[str]) -> dict[str, Any]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "version": __version__,
        "source": source_name(client),
        "window": [window.from_ledger, window.to_ledger],
        "page_size": backfill.page_size,
        "seconds": round(time.monotonic() - started, 1),
        "pages": backfill.limits.pages,
        "http": client.stats.as_dict(),
        "database": backfill.writer.as_dict(),
        "context": {"fetched": backfill.contexts.fetched, "reused": backfill.contexts.reused},
        "source_history_elder_ledger": backfill.elder,
        "process": {"max_rss_mb": round(usage.ru_maxrss / 1024, 1), "cpu_seconds": round(usage.ru_utime + usage.ru_stime, 1)},
        "streams": [report.as_dict() for report in reports],
        "left_out": list(left_out),
    }


def _print_summary(summary: dict[str, Any]) -> None:
    http, database = summary["http"], summary["database"]
    print(
        f"source {summary['source']}, window {summary['window']}, page size {summary['page_size']}\n"
        f"{http['requests']} requests ({http['retries']} retries, {http['waited_seconds']}s waited), "
        f"{summary['pages']} pages, {summary['seconds']}s; "
        f"{database['transactions']} database transactions in {database['seconds_in_transaction']}s; "
        f"contexts fetched {summary['context']['fetched']}, reused {summary['context']['reused']}"
    )
    rows = [
        {
            "stream": _short(report["stream"]),
            "status": report["status"],
            "pages": report["pages"],
            "records": report["records"],
            **{state: report["coverage_ledgers"].get(state, 0) for state in coverage.STATES},
            "error": report["error"] or "",
        }
        for report in summary["streams"]
    ]
    print_table(
        "coverage in ledgers", rows, ("stream", "status", "pages", "records", "complete", "empty", "unverifiable", "pending", "error")
    )
    for line in summary["left_out"]:
        print(line)


_ADDRESS = re.compile(r"\bG[A-Z0-9]{55}\b")


def _short(name: str) -> str:
    """A stream name with accounts cut to what tells them apart in a table."""
    return _ADDRESS.sub(lambda match: match.group(0)[:6] + "…", name)


def _write_report(path: str, summary: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(summary, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"report written to {target}")


def _exit_code(reports: Sequence[Any]) -> int:
    statuses = {report.status for report in reports}
    if "failed" in statuses:
        return FAILED
    if "stopped" in statuses:
        return STOPPED
    if any(report.coverage.get(coverage.UNVERIFIABLE) for report in reports):
        return UNVERIFIED
    return OK


class JsonLines(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "time": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            },
            ensure_ascii=False,
        )


def configure_logging(log_format: str, level: str, log_file: str | None = None):
    """Log to stderr and, when asked, to a file as well. A container's output
    is not always at hand while the process runs; the file is. Returns the
    stream the logs go to."""
    formatter = JsonLines() if log_format == "json" else logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s", "%H:%M:%S")
    stream: Any = sys.stderr
    handlers: list[logging.Handler] = [logging.StreamHandler(stream)]
    if log_file:
        stream = open(log_file, "a", encoding="utf-8")  # noqa: SIM115 - lives as long as the process
        handlers.append(logging.StreamHandler(stream))
    for handler in handlers:
        handler.setFormatter(formatter)
    logging.basicConfig(level=getattr(logging, level.upper()), handlers=handlers, force=True)
    return stream
    # One line per request is noise next to the run's own events.
    logging.getLogger("httpx").setLevel(logging.WARNING)


# -- the parser ----------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m app.backfill", description="Backfill a ledger window of the watch lists from Horizon.")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--log-format", choices=("text", "json"), default="text", help="log lines on stderr (default: text)")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-file", help="also write the log to this file, line by line as it happens")
    commands = parser.add_subparsers(dest="command", required=True)

    def command(name: str, handler: Command, help_: str, *, source: bool = True, window: str | None = None, subjects: bool = False) -> argparse.ArgumentParser:
        """A subcommand. ``window``: None, "optional" or "required"."""
        sub = commands.add_parser(name, help=help_)
        sub.set_defaults(handler=handler)
        if source:
            sub.add_argument("--horizon-url", help="Horizon to read (default: MTL_HORIZON_URL or the public server)")
            sub.add_argument("--rate", type=float, help="requests per second (default: MTL_HORIZON_REQUESTS_PER_SECOND or 1)")
            sub.add_argument("--page-size", type=int, default=200, help="records per page, at most 200 (default: 200)")
        if window:
            sub.add_argument("--from-ledger", type=int, required=window == "required", help="lowest ledger of the window (default: the source's oldest)")
            sub.add_argument("--to-ledger", type=int, required=window == "required", help="highest ledger of the window (default: the source's latest at start)")
        if subjects:
            sub.add_argument("--subject", action="append", help="only streams whose name contains this; repeatable")
        return sub

    sub = command("import-config", command_import_config, "load a YAML watch list", source=False)
    sub.add_argument("--file", required=True, help="watch list in YAML")
    sub.add_argument("--dry-run", action="store_true", help="validate and report, write nothing")
    sub.add_argument("--apply-changes", action="store_true", help="change the mode of accounts whose stored mode differs")

    sub = command("probe", command_probe, "check the live Horizon once and report")
    sub.add_argument("--base", help="asset id of the market to look at (default: the first tracked market)")
    sub.add_argument("--counter", help="asset id of the market's other side")
    sub.add_argument("--account", help="account whose streams to look at (default: the first FULL account)")
    sub.add_argument("--output", help="write the report to this file instead of stdout")

    command("estimate", command_estimate, "count the pages of the window without storing anything", window="required", subjects=True)
    sub = command("verify", command_verify, "compare the stored records with the source's pages", window="required", subjects=True)
    sub.add_argument("--aggregations", action="store_true", help="also sum every tracked market per UTC day against /trade_aggregations")
    command("fingerprint", command_fingerprint, "row counts and digests of the stored window", window="required")

    for name in ("run", "reimport"):
        sub = command(
            name,
            command_run,
            "read the window backwards and store it; resumes where it stopped" if name == "run" else "forget the coverage of the window, then run",
            window="optional",
            subjects=True,
        )
        for flag in ("max-pages", "max-requests", "max-seconds"):
            sub.add_argument(f"--{flag}", type=int, help=f"stop when reached; progress is kept ({flag.replace('-', ' ')})")
        sub.add_argument("--report", help="write a JSON report of the run to this file")

    sub = command("status", command_status, "show coverage, checkpoints and row counts", source=False, subjects=True)
    sub.add_argument("--from-ledger", type=int, help="show only ranges touching this ledger or above")
    sub.add_argument("--to-ledger", type=int, help="show only ranges touching this ledger or below")
    return parser


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
