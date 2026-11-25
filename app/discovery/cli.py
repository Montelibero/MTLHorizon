"""Command line of discovery: ``python -m app.discovery <command>``.

Commands: ``run`` (find the holders of the discovery assets and add the new
ones; ``--dry-run`` writes nothing anywhere), ``status`` (the assets, the
last passes, where the accounts came from; asks nothing of any server),
``on`` and ``off`` (mark an asset for discovery, or stop marking it — the
flag can also be given in the watch list file).
"""

from __future__ import annotations

import argparse
import faulthandler
import logging
import signal
import sys
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app import __version__
from app.backfill.config import parse_asset_id
from app.backfill.store import asset_parts
from app.cli_common import configure_logging, engine_for, make_client, print_table, write_report
from app.config import ConfigError, Settings
from app.discovery import scan
from app.storage.models import DiscoveryRun, HistoryAsset, TrackedAccount, TrackedAsset
from app.storage.upsert import upsert_row

logger = logging.getLogger("app.discovery")

OK, FAILED = 0, 1


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    stream = configure_logging(args.log_format, args.log_level, args.log_file)
    try:
        faulthandler.register(signal.SIGUSR1, file=stream, all_threads=True)
    except (OSError, ValueError):  # no real file behind the stream (tests)
        pass
    settings = Settings.from_env()
    try:
        return args.handler(args, settings)
    except (ConfigError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return FAILED
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return FAILED


# -- commands ------------------------------------------------------------------


def command_run(args: argparse.Namespace, settings: Settings) -> int:
    """Find the holders, add the new ones. One pass per asset."""
    errors: list[str] = []
    chosen = [asset for asset in (parse_asset_id(given, "--asset", errors) for given in args.asset or []) if asset]
    if errors:
        raise ConfigError("; ".join(errors))
    if "native" in chosen:
        raise ConfigError("native cannot be discovered: every account of the network holds it")
    engine = engine_for(settings)
    with make_client(settings, engine) as client:
        reports = scan.run_all(
            client, engine, chosen or None, dry_run=args.dry_run, resume=args.resume, max_requests=args.max_requests
        )
    if args.output:
        write_report(args.output, {"runs": [report.as_dict() for report in reports]})
    engine.dispose()
    if not reports:
        print("no asset is marked for discovery: `python -m app.discovery on CODE:ISSUER`, or `discover: true` in the watch list")
    for report in reports:
        for line in report.lines():
            print(line)
    return OK if all(report.state in (scan.COMPLETE, scan.DRY_RUN) for report in reports) else FAILED


def command_status(args: argparse.Namespace, settings: Settings) -> int:
    """The discovery assets and the last passes. Asks nothing of any server."""
    engine = engine_for(settings)
    with Session(engine) as session:
        assets = list(
            session.scalars(select(TrackedAsset).where(TrackedAsset.discover.is_(True)).order_by(TrackedAsset.asset_id))
        )
        last: dict[str, DiscoveryRun] = {}
        for run in session.scalars(
            select(DiscoveryRun)
            .where(DiscoveryRun.dry_run.is_(False))
            .order_by(DiscoveryRun.started_at.desc())
        ):
            last.setdefault(run.asset_id, run)
        origins = dict(session.execute(select(TrackedAccount.origin, func.count()).group_by(TrackedAccount.origin)).all())
        total = session.scalar(select(func.count()).select_from(TrackedAccount)) or 0
        found_by = session.scalar(select(func.count()).select_from(TrackedAccount).where(TrackedAccount.discovered_by.is_not(None))) or 0
    engine.dispose()
    print(f"discovery: {len(assets)} asset(s) marked, {origins.get('discovery', 0)} account(s) found by it, {found_by} carrying a discovery note; {total} watched in all")
    print_table(
        "origins of the watched accounts",
        [{"origin": origin, "accounts": number} for origin, number in sorted(origins.items())],
        ("origin", "accounts"),
    )
    print_table(
        "assets marked for discovery",
        [
            {
                "asset": asset.asset_id,
                "history too": "yes" if asset.enabled else "no",
                "last pass": _state(last[asset.asset_id]) if asset.asset_id in last else "never run",
                "when": _when(last[asset.asset_id].started_at) if asset.asset_id in last else "-",
                "read": last[asset.asset_id].found if asset.asset_id in last else "-",
                "added": last[asset.asset_id].added if asset.asset_id in last else "-",
                "stopped": (last[asset.asset_id].last_error or "-") if asset.asset_id in last else "-",
            }
            for asset in assets
        ],
        ("asset", "history too", "last pass", "when", "read", "added", "stopped"),
    )
    if not assets:
        print("nothing is marked: `python -m app.discovery on CODE:ISSUER`, or `discover: true` in the watch list")
    return OK


def command_on(args: argparse.Namespace, settings: Settings) -> int:
    return _mark(args, settings, True)


def command_off(args: argparse.Namespace, settings: Settings) -> int:
    return _mark(args, settings, False)


def _mark(args: argparse.Namespace, settings: Settings, discover: bool) -> int:
    """Mark an asset for discovery, or unmark it. An asset that is not
    watched yet is noted, with its history switched off: the two rules are
    apart."""
    errors: list[str] = []
    asset = parse_asset_id(args.asset, "asset", errors)
    if asset is None:
        raise ConfigError("; ".join(errors))
    if asset == "native":
        raise ConfigError("native cannot be discovered: every account of the network holds it")
    engine = engine_for(settings)
    with Session(engine) as session, session.begin():
        tracked = session.get(TrackedAsset, asset)
        if tracked is None:
            upsert_row(session, HistoryAsset.__table__, {"asset_id": asset}, asset_parts(asset))
            upsert_row(
                session, TrackedAsset.__table__, {"asset_id": asset},
                {"reason": args.reason or "discovery", "enabled": False, "discover": discover},
            )
            outcome = "noted"
        else:
            tracked.discover = discover
            outcome = "marked" if discover else "unmarked"
    engine.dispose()
    what = "marked for discovery" if discover else "no longer marked for discovery"
    print(f"{asset}: {outcome} and {what}")
    if discover:
        print("run `python -m app.discovery run --dry-run` to see what it finds; nothing is found until a pass runs"
              + (" or the schedule is on" if settings.discovery_interval_seconds else ""))
    return OK


def _state(run: DiscoveryRun) -> str:
    return run.state if run.state != scan.COMPLETE else "complete"


def _when(moment: datetime | None) -> str:
    return "-" if moment is None else moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# -- the parser ----------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.discovery",
        description="Find the accounts that hold an asset and add them to the watch list.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--log-format", choices=("text", "json"), default="text", help="log lines on stderr (default: text)")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-file", help="also write the log to this file, line by line as it happens")
    commands = parser.add_subparsers(dest="command", required=True)

    sub = commands.add_parser("run", help="find the holders of the discovery assets and add the new ones")
    sub.set_defaults(handler=command_run)
    sub.add_argument("--asset", action="append", help="only this asset, CODE:ISSUER; repeatable (default: every asset marked for discovery)")
    sub.add_argument("--dry-run", action="store_true", help="say what would be added; write nothing, not even a record of the pass")
    sub.add_argument("--resume", action="store_true", help="go on from where the last pass that did not finish stopped")
    sub.add_argument(
        "--max-requests",
        type=int,
        default=scan.DEFAULT_MAX_REQUESTS,
        help="stop the pass here; one page of 200 accounts is one request (default: 1000)",
    )
    sub.add_argument("--output", help="also write what the passes found to this file as JSON")

    sub = commands.add_parser("status", help="the discovery assets and the last passes; asks nothing of any server")
    sub.set_defaults(handler=command_status)

    for name, handler, what, why in (
        ("on", command_on, "mark an asset for discovery", "why it is marked (an asset not watched yet is noted with it)"),
        ("off", command_off, "stop marking an asset for discovery", "why it is unmarked"),
    ):
        sub = commands.add_parser(name, help=what)
        sub.set_defaults(handler=handler)
        sub.add_argument("asset", help="the asset as CODE:ISSUER")
        sub.add_argument("--reason", help=why)
    return parser


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
