"""Command line of the Horizon pool: ``python -m app.pool <command>``.

Commands: ``check`` (ask every server what it is and whether it answers, each
on its own; keeps what it found) and ``status`` (what was kept: the last
check, and what the running processes last saw of each server; asks nothing
of any server).

The servers come from ``MTL_HORIZON_URLS``; there is no address to give
here. A check changes no history and no cursor, and prefers no server: what
it finds is shown and kept, and the pool decides anew with every request.
"""

from __future__ import annotations

import argparse
import faulthandler
import logging
import signal
import sys
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from app import __version__
from app.cli_common import configure_logging, engine_for, make_client, print_table, write_report
from app.config import ConfigError, Settings, shown_url
from app.pool import check
from app.storage import pool_state

logger = logging.getLogger("app.pool")

OK, FAILED = 0, 1


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
        return FAILED


# -- commands ------------------------------------------------------------------


def command_check(args: argparse.Namespace, settings: Settings) -> int:
    """Every server on its own; nothing is asked of another in its place."""
    engine = engine_for(settings)
    with make_client(settings, engine) as pool:
        logger.info("checking %d server(s), at most %d request(s) each", len(pool.servers), 2 * args.repeats)
        checked = check.check_pool(pool, repeats=args.repeats)
        with Session(engine) as session, session.begin():
            pool_state.save(session, checked.as_dict())
        http = pool.stats.as_dict()
    engine.dispose()
    check.print_check(checked)
    if args.output:
        write_report(args.output, {**checked.as_dict(), "http": http})
    return OK if all(server.available for server in checked.servers) else FAILED


def command_status(args: argparse.Namespace, settings: Settings) -> int:
    """What is kept of the servers: the last check, and what the running
    processes last saw of them. Nothing is asked of any server."""
    engine = engine_for(settings)
    with Session(engine) as session:
        kept = {row.url: row for row in pool_state.rows(session)}
    engine.dispose()
    rows = []
    for url in settings.horizon_urls:
        row = kept.get(shown_url(url))
        rows.append(
            {
                "server": shown_url(url),
                "check": "-" if row is None or row.checked_at is None else ("available" if row.available else "unavailable"),
                "latest": "-" if row is None or row.latest_ledger is None else row.latest_ledger,
                "elder": "-" if row is None or row.elder_ledger is None else row.elder_ledger,
                "lag": "-" if row is None or row.lag_ledgers is None else row.lag_ledgers,
                "ms": "-" if row is None or row.root_ms is None else f"{row.root_ms}/{'' if row.history_ms is None else row.history_ms}",
                "checked": "never" if row is None else _when(row.checked_at),
                "last error": (row.last_error or "-") if row is not None else "-",
                "cooling until": _when(row.cooling_until) if row is not None and row.cooling_until is not None else "-",
            }
        )
    print_table(
        f"pool: {len(rows)} server(s) from MTL_HORIZON_URLS, the first being the primary",
        rows,
        ("server", "check", "latest", "elder", "lag", "ms", "checked", "last error", "cooling until"),
    )
    print("ms: median of the root answers / of the history answers; lag: behind the freshest server of the same check")
    configured = {entry["server"] for entry in rows}
    for url in kept:
        if url not in configured:
            print(f"{url} is not in MTL_HORIZON_URLS any more; what was kept of it stays until a check runs")
    return OK


def _when(moment: datetime | None) -> str:
    return "-" if moment is None else moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# -- the parser ----------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.pool", description="Check the servers of the Horizon pool, each on its own."
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--log-format", choices=("text", "json"), default="text", help="log lines on stderr (default: text)")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-file", help="also write the log to this file, line by line as it happens")
    commands = parser.add_subparsers(dest="command", required=True)

    sub = commands.add_parser("check", help="ask every server what it is and whether it answers for history; keeps what it found")
    sub.set_defaults(handler=command_check)
    sub.add_argument("--repeats", type=int, default=check.MAX_REPEATS, help="requests of each kind, at most three (default: 3)")
    sub.add_argument("--output", help="also write the result to this file as JSON")

    sub = commands.add_parser("status", help="what is kept of the servers; asks nothing of them")
    sub.set_defaults(handler=command_status)
    return parser


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
