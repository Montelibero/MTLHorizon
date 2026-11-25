"""Command line of the watch: ``python -m app.watch <command>``.

Commands: ``run`` (one pass of keeping accounts, their history imports and
their subscriptions in step — the worker does this by itself while no event
is waiting), ``status`` (where every account is in its life; asks nothing
of any server), and ``add-account``, ``disable-account``, ``enable-account``
— every way an account comes or goes is the same life: a start boundary, a
history import, a subscription.
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
from app.cli_common import configure_logging, engine_for, make_client
from app.config import ConfigError, Settings, shown_url
from app.storage import history_jobs
from app.storage.models import HistoryJob, TrackedAccount, WatchSubscription
from app.watch import accounts as watched
from app.watch import provision

logger = logging.getLogger("app.watch")

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
    """One pass, now."""
    engine = engine_for(settings)
    with make_client(settings, engine) as client:
        api = provision.make_api(settings)
        try:
            report = provision.run(engine, settings, client=client, api=api)
        finally:
            if api is not None:
                api.close()
    engine.dispose()
    for line in report.lines():
        print(line)
    return OK


def command_status(args: argparse.Namespace, settings: Settings) -> int:
    """Where every account is in its life. Asks nothing of any server."""
    engine = engine_for(settings)
    with Session(engine) as session:
        origins = dict(session.execute(select(TrackedAccount.origin, func.count()).group_by(TrackedAccount.origin)).all())
        enabled = session.scalar(select(func.count()).select_from(TrackedAccount).where(TrackedAccount.enabled.is_(True))) or 0
        total = session.scalar(select(func.count()).select_from(TrackedAccount)) or 0
        due = session.scalars(
            select(TrackedAccount.account_id)
            .where(TrackedAccount.enabled.is_(True), TrackedAccount.boundary_due.is_(True))
            .order_by(TrackedAccount.account_id)
        ).all()
        jobs = list(session.scalars(select(HistoryJob).order_by(HistoryJob.state, HistoryJob.id)))
        subscriptions = dict(
            session.execute(select(WatchSubscription.state, func.count()).group_by(WatchSubscription.state)).all()
        )
        problems = list(
            session.scalars(
                select(WatchSubscription)
                .where(WatchSubscription.state.in_(("error", "ambiguous", "retiring")), WatchSubscription.last_error.is_not(None))
                .order_by(WatchSubscription.next_attempt_at)
                .limit(args.examples)
            )
        )
    engine.dispose()
    counts = history_jobs.counts(engine) if jobs else {state: 0 for state in ("waiting", "running", "done", "partial", "error", "paused")}
    final = sum(1 for job in jobs if job.boundary_final)
    print(f"accounts: {total} watched, {enabled} enabled; by origin: " + ", ".join(f"{name} {number}" for name, number in sorted(origins.items())))
    if due:
        print(f"  {len(due)} wait for a start boundary (the worker writes it while no event is waiting): {', '.join(address[:6] + '…' for address in due[:args.examples])}")
    print(
        f"history imports: {counts.get('waiting', 0)} waiting, {counts.get('running', 0)} running, {counts.get('done', 0)} done, "
        f"{counts.get('partial', 0)} partial, {counts.get('error', 0)} error, {counts.get('paused', 0)} paused"
    )
    if jobs and final < len(jobs):
        print(
            f"  {len(jobs) - final} of them have an upper boundary taken before their account was told of: "
            f"events are not guaranteed above that ledger (`python -m app.history status` says which)"
        )
    states = ("wanted", "active", "error", "ambiguous", "retiring", "manual", "retired")
    print("subscriptions: " + ", ".join(f"{state} {subscriptions.get(state, 0)}" for state in states))
    if settings.notifier_api_url:
        print(f"  at the Notifier {shown_url(settings.notifier_api_url)}; webhooks are sent to {settings.notifier_callback_url or '(not set: nothing is registered)'}")
    else:
        print("  no Notifier API is configured: subscriptions are made by hand and taken in with `python -m app.subscriptions adopt`")
    for row in problems:
        print(f"  {row.kind} {row.key[:12]}… [{row.state}]: {row.last_error}")
    return OK


def command_add(args: argparse.Namespace, settings: Settings) -> int:
    return _change(args, settings, "added")


def command_disable(args: argparse.Namespace, settings: Settings) -> int:
    return _change(args, settings, "disabled")


def command_enable(args: argparse.Namespace, settings: Settings) -> int:
    return _change(args, settings, "enabled")


def _change(args: argparse.Namespace, settings: Settings, what: str) -> int:
    """Add an account, or switch one off or on, then run the pass that does
    what follows from it — unless told not to (the worker runs it too)."""
    engine = engine_for(settings)
    with Session(engine) as session, session.begin():
        for address in args.address:
            if what == "added":
                change = watched.add_account(session, address, reason=args.reason or "added by hand", origin=watched.MANUAL)
            else:
                change = watched.set_enabled(session, address, enabled=what == "enabled", reason=args.reason)
            print(f"{address}: {change.outcome}")
    if args.no_pass:
        engine.dispose()
        print("the worker does what follows while no event is waiting, or `python -m app.watch run` now")
        return OK
    with make_client(settings, engine) as client:
        api = provision.make_api(settings)
        try:
            report = provision.run(engine, settings, client=client, api=api)
        finally:
            if api is not None:
                api.close()
    engine.dispose()
    for line in report.lines():
        print(line)
    return OK


def _when(moment: datetime | None) -> str:
    return "-" if moment is None else moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# -- the parser ----------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.watch", description="Accounts, their history imports and their subscriptions, kept in step."
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--log-format", choices=("text", "json"), default="text", help="log lines on stderr (default: text)")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-file", help="also write the log to this file, line by line as it happens")
    commands = parser.add_subparsers(dest="command", required=True)

    sub = commands.add_parser("run", help="one pass: boundaries, history imports, subscriptions")
    sub.set_defaults(handler=command_run)

    sub = commands.add_parser("status", help="where every account is in its life; asks nothing of any server")
    sub.set_defaults(handler=command_status)
    sub.add_argument("--examples", type=int, default=5, help="how many accounts and problems to list (default: 5)")

    sub = commands.add_parser("add-account", help="add an account by hand: it gets a boundary, a history import and a subscription")
    sub.set_defaults(handler=command_add)
    sub.add_argument("address", nargs="+", help="the account's G… address; repeatable")
    sub.add_argument("--reason", help="why it is watched")
    sub.add_argument("--no-pass", action="store_true", help="change the watch list only; the worker does the rest")

    for name, handler, what in (
        ("disable-account", command_disable, "switch an account off: its import pauses and its subscription retires; the history stays"),
        ("enable-account", command_enable, "switch an account on again: what was missed is read, from the ledger that is the latest"),
    ):
        sub = commands.add_parser(name, help=what)
        sub.set_defaults(handler=handler)
        sub.add_argument("address", nargs="+", help="the account's G… address; repeatable")
        sub.add_argument("--reason", help="a new reason, if there is one")
        sub.add_argument("--no-pass", action="store_true", help="change the watch list only; the worker does the rest")
    return parser


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
