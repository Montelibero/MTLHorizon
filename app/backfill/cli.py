"""Command line of the backfill: ``python -m app.backfill <command>``.

Commands: ``import-config``, ``probe``, ``estimate``, ``run``, ``status``,
``reimport``, ``verify``, ``fingerprint``. Exit codes of a run: 0 verified,
1 a stream failed, 2 stopped by a limit or a signal, 3 finished with ranges
the source cannot verify.

A historical run (``run``, ``reimport``) never chooses its window on its
own: the ledgers are given, or all the history the source keeps is asked for
with ``--full-retention`` — and either way the run has a budget. It reads
through the pool of Horizon servers like everything else, at the pace every
process shares; a run may go on while events are handled, and it never
starves: while events flow it gets one request in ten, and all of them when
they do not.
"""

from __future__ import annotations

import argparse
import faulthandler
import json
import logging
import resource
import signal
import sys
import threading
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app import __version__
from app.backfill import config as watch_lists
from app.backfill import verify as checking
from app.backfill.jobs import StreamJob, stream_jobs
from app.backfill.loader import CONTEXT_MODES, FULL, Backfill, Limits, Window
from app.backfill.probe import Market, probe
from app.cli_common import (
    Command,
    configure_logging,
    engine_for,
    make_client,
    print_table,
    short_name,
    source_name,
    source_name_for,
    write_report,
)
from app.config import ConfigError, Settings
from app.sources.horizon import HorizonClient, Scope
from app.storage import coverage
from app.storage.models import Base
from app.storage.upsert import checkpoint
from app.storage.writer import HistoryWriter

logger = logging.getLogger("app.backfill")

OK, FAILED, STOPPED, UNVERIFIED = 0, 1, 2, 3

ROOT_FIELDS = (
    "horizon_version",
    "history_elder_ledger",
    "history_latest_ledger",
    "history_latest_ledger_closed_at",
)


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
        result = watch_lists.apply(session, config, apply_changes=args.apply_changes)
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
    with make_client(settings, engine) as client:
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
    with make_client(settings, engine) as client:
        window = Window(args.from_ledger, args.to_ledger)
        client.scope = Scope(window.from_ledger, window.to_ledger)
        jobs = pick_jobs(engine, args)
        estimates = checking.estimate(client, jobs, window, args.page_size)
    rows = [estimate.as_dict() for estimate in estimates]
    print_table(
        f"window [{window.from_ledger}, {window.to_ledger}]",
        rows,
        ("stream", "pages", "records", "transactions", "context_requests_upper_bound"),
    )
    pages = sum(row["pages"] for row in rows)
    contexts = sum(row["context_requests_upper_bound"] for row in rows)
    rate = client.settings.requests_per_second
    print(
        f"\npages to read: {pages}; context requests at most {contexts}; "
        f"at {rate:g} requests/s that is up to {(pages + contexts) / rate / 60:.0f} minutes"
    )
    engine.dispose()
    return OK


def command_run(args: argparse.Namespace, settings: Settings) -> int:
    window = historical_window(args)  # None when --full-retention was asked for
    engine = engine_for(settings)
    stop = threading.Event()
    _stop_on_signal(stop)
    engine = engine_for(settings)
    with make_client(settings, engine) as client:
        started = time.monotonic()
        all_there_is = window is None
        window, source_at_start = resolve_window(client, args, settings, window)
        # What this run is about: a server that keeps none of the window, or
        # has not got as far as its top, is not asked for it. Of all the
        # history there is, the bottom is wherever the pool's history ends.
        client.scope = Scope(None if all_there_is else window.from_ledger, window.to_ledger)
        jobs, left_out = pick_jobs(engine, args, with_left_out=True)
        limits = Limits(
            requests=lambda: client.stats.requests,
            max_pages=args.max_pages,
            max_requests=args.max_requests,
            max_seconds=args.max_seconds,
            stop=stop,
        )
        with Backfill(
            client, engine, source=source_name(client), page_size=args.page_size, limits=limits, context=args.context
        ) as backfill:
            if args.command == "reimport":
                forget(engine, jobs, window, source_name(client))
            reports = backfill.run(window, jobs, concurrency=settings.horizon_concurrency)
            summary = _summary(window, client, backfill, reports, started, left_out)
    engine.dispose()
    summary["source_at_start"] = source_at_start
    _print_summary(summary)
    if args.report:
        write_report(args.report, summary)
    return _exit_code(reports)


def command_status(args: argparse.Namespace, settings: Settings) -> int:
    engine = engine_for(settings)
    with Session(engine) as session:
        jobs = pick_jobs(engine, args)
        rows = []
        for job in jobs:
            state = checkpoint(
                session,
                source=source_name_for(settings),
                resource_type=job.resource_type,
                stream_key=job.stream_key,
                direction="desc",
            )
            ranges = coverage.ranges(session, job.key)
            if args.from_ledger and args.to_ledger:
                ranges = [r for r in ranges if r.to_ledger >= args.from_ledger and r.from_ledger <= args.to_ledger]
            rows.append(
                {
                    "stream": job.name,
                    "ranges": " ".join(f"{r.state}:{r.from_ledger}..{r.to_ledger}" for r in ranges) or "-",
                    "checkpoint": (state[0] or "-") if state else "-",
                }
            )
        left_out = stream_jobs(session)[1]
        for name, table in Base.metadata.tables.items():
            if name == "alembic_version":
                continue
            rows.append(
                {"stream": f"rows {name}", "ranges": str(session.scalar(select(func.count()).select_from(table))), "checkpoint": ""}
            )
    print_table("coverage (state:from..to)", rows, ("stream", "ranges", "checkpoint"))
    for line in left_out:
        print(line)
    engine.dispose()
    return OK


def command_verify(args: argparse.Namespace, settings: Settings) -> int:
    engine = engine_for(settings)
    with make_client(settings, engine) as client:
        window = Window(args.from_ledger, args.to_ledger)
        client.scope = Scope(window.from_ledger, window.to_ledger)
        jobs = pick_jobs(engine, args)
        comparisons = checking.verify(client, engine, jobs, window, args.page_size)
        matches = all(comparison.matches for comparison in comparisons)
        if args.aggregations:
            matches = _verify_aggregations(client, engine, window, args.bucket_hours) and matches
    rows = [comparison.as_dict() for comparison in comparisons]
    print_table(
        f"stored against the source, window [{window.from_ledger}, {window.to_ledger}]",
        rows,
        ("stream", "source", "stored", "matches", "missing", "extra"),
    )
    for row in rows:
        if row["missing_examples"]:
            print(f"  missing from storage, e.g. {row['stream']}: {row['missing_examples']}")
        if row["extra_examples"]:
            print(f"  in storage but not in the source stream, e.g. {row['stream']}: {row['extra_examples']}")
    engine.dispose()
    return OK if matches else FAILED


def _verify_aggregations(client: HorizonClient, engine, window: Window, bucket_hours: int = 24) -> bool:
    """Every tracked market, whole buckets of the window: the stored trades
    against what the source aggregates — count, both volumes, and the
    average price with the extremes."""
    from app.backfill.verify import PRICE_FIELDS, whole_buckets
    from app.storage.models import TrackedMarket

    resolution = bucket_hours * 3_600_000
    first, buckets = whole_buckets(client, window, resolution)
    if buckets == 0:
        print("aggregations: the window holds no whole bucket")
        return True
    matches = True
    with Session(engine) as session:
        markets = list(session.scalars(select(TrackedMarket).where(TrackedMarket.enabled.is_(True))))
    for market in markets:
        comparisons = checking.compare_aggregations(
            client, engine, market.base_asset_id, market.counter_asset_id, first, buckets, resolution
        )
        equal_prices = [c for c in comparisons if all(name in c.source for name in PRICE_FIELDS)]
        print(
            f"\n{market.base_asset_id.split(':')[0]} / {market.counter_asset_id.split(':')[0]} "
            f"against /trade_aggregations, {buckets} whole buckets of {bucket_hours} h"
            + (f"; price fields served for {len(equal_prices)} of them" if equal_prices else "; no price fields served")
        )
        print_table(
            "per bucket, source against stored",
            [
                {
                    "bucket": comparison.start,
                    "trades source/stored": f"{comparison.source.get('trades', '-')}/{comparison.stored['trades']}",
                    "base volume source/stored": _amounts(comparison, "base_volume"),
                    "counter volume source/stored": _amounts(comparison, "counter_volume"),
                    "prices": _prices(comparison),
                }
                for comparison in comparisons
            ],
            ("bucket", "trades source/stored", "base volume source/stored", "counter volume source/stored", "prices"),
        )
        for comparison in comparisons:
            if not comparison.matches:
                print(f"  differs in {comparison.start}: {', '.join(comparison.differs_in)}")
        matches = all(comparison.matches for comparison in comparisons) and matches
    return matches


def _amounts(comparison, name: str) -> str:
    from app.sources.horizon_format import stroops_to_amount

    source = comparison.source.get(name)
    return f"{'-' if source is None else stroops_to_amount(source)}/{stroops_to_amount(comparison.stored[name])}"


def _prices(comparison) -> str:
    """The average and the four extreme prices: equal, or which of them differ."""
    from app.backfill.verify import AVG, PRICE_FIELDS

    served = [name for name in (AVG, *PRICE_FIELDS) if name in comparison.source]
    if not served:
        return "-"
    if all(comparison.source[name] == comparison.stored.get(name) for name in served):
        return f"{len(served)}/{len(served)} equal"
    return "DIFFER: " + ",".join(name for name in served if comparison.source[name] != comparison.stored.get(name))


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
        select(TrackedAccount.account_id).where(TrackedAccount.enabled.is_(True)).order_by(TrackedAccount.account_id).limit(1)
    ).first()
    return market, account


def pick_jobs(engine, args: argparse.Namespace, with_left_out: bool = False):
    with Session(engine) as session:
        jobs, left_out = stream_jobs(session)
    chosen = [job for job in jobs if not args.subject or any(part in job.name for part in args.subject)]
    return (chosen, left_out) if with_left_out else chosen


def historical_window(args: argparse.Namespace) -> Window | None:
    """The window of a historical run, and whether it has a budget.

    The ledgers are given explicitly, or all the history the source keeps is
    asked for with ``--full-retention`` (then None is returned: the source
    names the ledgers). A run without either used to take the source's whole
    history by default, and one did — for ten minutes before anyone looked
    (`docs/stage-02-report.md`). Either way the run has a budget.
    """
    given = args.from_ledger is not None or args.to_ledger is not None
    if args.full_retention and given:
        raise ConfigError("--full-retention and --from-ledger/--to-ledger are alternatives: give one of them")
    if not args.full_retention and (args.from_ledger is None or args.to_ledger is None):
        raise ConfigError(
            "say which ledgers to read: --from-ledger and --to-ledger, or --full-retention "
            "for all the history the source keeps"
        )
    if args.max_pages is None and args.max_requests is None and args.max_seconds is None:
        raise ConfigError(
            "give the run a budget: --max-requests, --max-pages or --max-seconds; where it stops, the next run goes on"
        )
    if args.full_retention:
        return None
    try:
        return Window(args.from_ledger, args.to_ledger)
    except ValueError as error:
        raise ConfigError(str(error)) from None


def resolve_window(
    client, args: argparse.Namespace, settings: Settings, window: Window | None
) -> tuple[Window, dict[str, Any]]:
    """The window of the run, and what the source says of itself at its start.

    A run never writes into a database of another network, so the source is
    asked first. With ``--full-retention`` the window is everything the pool
    keeps: its oldest ledger on whichever server keeps the most, and the
    latest ledger of the server that has got furthest, as they are right now.
    """
    root = client.root()
    if root.get("network_passphrase") != settings.network_passphrase:
        raise ConfigError(
            f"{client.name} serves the network {root.get('network_passphrase')!r}, "
            f"this installation is configured for {settings.network_passphrase!r}"
        )
    if window is None:
        history = client.history()  # of a pool: asked of every server that could answer
        window = Window(history.elder, history.latest)
        logger.info("full retention: the pool keeps ledgers %d..%d", window.from_ledger, window.to_ledger)
    return window, {name: root.get(name) for name in ROOT_FIELDS}


def forget(engine, jobs: Sequence[StreamJob], window: Window, source: str) -> None:
    """Turn the coverage of the window back into pending, for a reimport."""
    with HistoryWriter(engine).transaction() as session:
        for job in jobs:
            coverage.invalidate(session, job.key, source, window.from_ledger, window.to_ledger)
    logger.info("coverage of [%d, %d] reset for %d streams", window.from_ledger, window.to_ledger, len(jobs))


def _stop_on_signal(stop: threading.Event) -> None:
    def stop_now(signum: int, frame: Any) -> None:
        stop.set()

    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, stop_now)


def _summary(
    window: Window, client, backfill: Backfill, reports: Sequence[Any], started: float, left_out: Sequence[str]
) -> dict[str, Any]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "version": __version__,
        "source": source_name(client),
        "window": [window.from_ledger, window.to_ledger],
        "context_mode": backfill.context,
        "page_size": backfill.page_size,
        "seconds": round(time.monotonic() - started, 1),
        "pages": backfill.limits.pages,
        "http": client.stats.as_dict(),
        "database": backfill.writer.as_dict(),
        "context": {"fetched": backfill.contexts.fetched, "reused": backfill.contexts.reused},
        "source_history_elder_ledger": backfill.elder,
        "process": {
            "max_rss_mb": round(usage.ru_maxrss / 1024, 1),
            "cpu_seconds": round(usage.ru_utime + usage.ru_stime, 1),
        },
        "streams": [report.as_dict() for report in reports],
        "left_out": list(left_out),
    }


def _print_summary(summary: dict[str, Any]) -> None:
    http, database = summary["http"], summary["database"]
    print(
        f"source {summary['source']}, window {summary['window']}, page size {summary['page_size']}, "
        f"context {summary['context_mode']}\n"
        f"{http['requests']} requests ({http['retries']} retries, {http['waited_seconds']}s waited), "
        f"{summary['pages']} pages, {summary['seconds']}s; "
        f"{database['transactions']} database transactions in {database['seconds_in_transaction']}s; "
        f"contexts fetched {summary['context']['fetched']}, reused {summary['context']['reused']}"
    )
    rows = [
        {
            "stream": short_name(report["stream"]),
            "status": report["status"],
            "pages": report["pages"],
            "records": report["records"],
            **{state: report["coverage_ledgers"].get(state, 0) for state in coverage.STATES},
            "error": report["error"] or "",
        }
        for report in summary["streams"]
    ]
    print_table(
        "coverage in ledgers",
        rows,
        ("stream", "status", "pages", "records", "complete", "empty", "unverifiable", "pending", "error"),
    )
    for line in summary["left_out"]:
        print(line)


def _exit_code(reports: Sequence[Any]) -> int:
    statuses = {report.status for report in reports}
    if "failed" in statuses:
        return FAILED
    if "stopped" in statuses:
        return STOPPED
    if any(report.coverage.get(coverage.UNVERIFIABLE) for report in reports):
        return UNVERIFIED
    return OK


# -- the parser ----------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.backfill", description="Backfill a ledger window of the watch lists from Horizon."
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--log-format", choices=("text", "json"), default="text", help="log lines on stderr (default: text)")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-file", help="also write the log to this file, line by line as it happens")
    commands = parser.add_subparsers(dest="command", required=True)

    def command(
        name: str,
        handler: Command,
        help_: str,
        *,
        source: bool = True,
        window: str | None = None,
        subjects: bool = False,
    ) -> argparse.ArgumentParser:
        """A subcommand. ``window``: None, "required" (the ledgers are given),
        or "historical" (the ledgers are given, or --full-retention)."""
        sub = commands.add_parser(name, help=help_)
        sub.set_defaults(handler=handler)
        if source:
            # Which Horizon servers are read, and how fast, comes from the
            # environment alone: MTL_HORIZON_URLS and MTL_HORIZON_REQUESTS_PER_SECOND.
            sub.add_argument("--page-size", type=int, default=200, help="records per page, at most 200 (default: 200)")
        if window:
            how = "" if window == "required" else " (or --full-retention)"
            sub.add_argument("--from-ledger", type=int, required=window == "required", help="lowest ledger of the window" + how)
            sub.add_argument("--to-ledger", type=int, required=window == "required", help="highest ledger of the window" + how)
        if subjects:
            sub.add_argument("--subject", action="append", help="only streams whose name contains this; repeatable")
        return sub

    sub = command("import-config", command_import_config, "load a YAML watch list", source=False)
    sub.add_argument("--file", required=True, help="watch list in YAML")
    sub.add_argument("--dry-run", action="store_true", help="validate and report, write nothing")
    sub.add_argument("--apply-changes", action="store_true", help="change the flag of entries whose stored flag differs from the file")

    sub = command("probe", command_probe, "check the live Horizon once and report")
    sub.add_argument("--base", help="asset id of the market to look at (default: the first tracked market)")
    sub.add_argument("--counter", help="asset id of the market's other side")
    sub.add_argument("--account", help="account whose streams to look at (default: the first enabled account)")
    sub.add_argument("--output", help="write the report to this file instead of stdout")

    command("estimate", command_estimate, "count the pages of the window without storing anything", window="required", subjects=True)
    sub = command("verify", command_verify, "compare the stored records with the source's pages", window="required", subjects=True)
    sub.add_argument("--aggregations", action="store_true", help="also sum every tracked market, bucket by bucket, against /trade_aggregations")
    sub.add_argument("--bucket-hours", type=int, default=24, help="bucket size of the aggregation check in hours (default: 24)")
    command("fingerprint", command_fingerprint, "row counts and digests of the stored window", window="required")

    for name in ("run", "reimport"):
        sub = command(
            name,
            command_run,
            "read the window backwards and store it; resumes where it stopped" if name == "run" else "forget the coverage of the window, then run",
            window="historical",
            subjects=True,
        )
        sub.add_argument(
            "--full-retention",
            action="store_true",
            help="read all the history the source keeps: from its oldest to its latest ledger right now",
        )
        for flag in ("max-pages", "max-requests", "max-seconds"):
            sub.add_argument(f"--{flag}", type=int, help=f"stop when reached; progress is kept ({flag.replace('-', ' ')})")
        sub.add_argument(
            "--context",
            choices=CONTEXT_MODES,
            default=FULL,
            help="what to fetch for a record's transaction: full (default: all its operations, effects and trades) "
            "or parents (only what the record cannot be stored without)",
        )
        sub.add_argument("--report", help="write a JSON report of the run to this file")

    sub = command("status", command_status, "show coverage, checkpoints and row counts", source=False, subjects=True)
    sub.add_argument("--from-ledger", type=int, help="show only ranges touching this ledger or above")
    sub.add_argument("--to-ledger", type=int, help="show only ranges touching this ledger or below")
    return parser


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
