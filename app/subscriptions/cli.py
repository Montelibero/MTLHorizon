"""Command line of the subscriptions: ``python -m app.subscriptions <command>``.

Commands: ``status`` (what is wanted, what is registered, what went wrong;
asks nothing of the Notifier), ``reconcile`` (one pass with the Notifier:
register, confirm, retire — ``--dry-run`` says what it would do and does
nothing), ``adopt`` (take a subscription made by hand at the Notifier as
this installation's, by its id there) and ``release`` (forget one that was
adopted: it stays at the Notifier).
"""

from __future__ import annotations

import argparse
import faulthandler
import logging
import signal
import sys
from collections.abc import Sequence
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app import __version__
from app.cli_common import configure_logging, engine_for, print_table
from app.config import ConfigError, Settings, shown_url
from app.notifier.client import NotifierError, remote_id, same_subscription, subscription_params
from app.storage.models import WatchSubscription
from app.subscriptions import desired, reconcile
from app.subscriptions.desired import ACCOUNT, ASSET
from app.watch import provision

logger = logging.getLogger("app.subscriptions")

OK, FAILED = 0, 1
STATES = ("wanted", "active", "error", "ambiguous", "retiring", "manual", "retired")


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


def command_status(args: argparse.Namespace, settings: Settings) -> int:
    """What is wanted, what is registered and what went wrong. Asks nothing
    of the Notifier."""
    engine = engine_for(settings)
    with Session(engine) as session:
        wanted = desired.wanted(session)
        rows = {(row.kind, row.key): row for row in session.scalars(select(WatchSubscription).where(WatchSubscription.state != "retired"))}
        counts = dict(session.execute(select(WatchSubscription.state, func.count()).group_by(WatchSubscription.state)).all())
        troubles = list(
            session.scalars(
                select(WatchSubscription)
                .where(WatchSubscription.state.in_(("error", "ambiguous", "retiring")))
                .order_by(WatchSubscription.next_attempt_at)
                .limit(args.examples)
            )
        )
    engine.dispose()
    print(f"wanted: {len(wanted)} subscription(s) — " + ", ".join(
        f"{kind} {sum(1 for want in wanted.values() if want.kind == kind)}" for kind in (ACCOUNT, ASSET)))
    print("kept: " + ", ".join(f"{state} {counts.get(state, 0)}" for state in STATES))
    unregistered = [want for key, want in wanted.items() if key not in rows or rows[key].state not in reconcile.CONFIRMED]
    if unregistered:
        print(f"  {len(unregistered)} wanted and not confirmed yet: {', '.join(f'{want.kind} {want.key[:8]}…' for want in unregistered[:args.examples])}")
    if not settings.notifier_api_url:
        print("  no Notifier API is configured (MTL_HORIZON_NOTIFIER_API_URL): nothing is registered automatically; make the subscriptions by hand and `adopt` them here")
    elif not settings.notifier_callback_url:
        print("  MTL_HORIZON_NOTIFIER_CALLBACK_URL is not set: nothing is registered until it says where webhooks go")
    if troubles:
        print_table(
            "not in step",
            [
                {"row": row.id, "kind": row.kind, "key": row.key[:16], "state": row.state, "at the Notifier": row.remote_id or "-",
                 "try": row.attempts, "next try": _when(row.next_attempt_at), "why": (row.last_error or "")[:90]}
                for row in troubles
            ],
            ("row", "kind", "key", "state", "at the Notifier", "try", "next try", "why"),
        )
    return OK


def command_reconcile(args: argparse.Namespace, settings: Settings) -> int:
    """One pass with the Notifier — or, with ``--dry-run``, what the pass
    would do."""
    if not settings.notifier_api_url:
        raise ConfigError("MTL_HORIZON_NOTIFIER_API_URL is not set: subscriptions are made by hand and adopted here")
    if not settings.notifier_callback_url:
        raise ConfigError("MTL_HORIZON_NOTIFIER_CALLBACK_URL is not set: it says where the Notifier sends its webhooks")
    engine = engine_for(settings)
    api = provision.make_api(settings)
    assert api is not None
    try:
        if args.dry_run:
            return _dry_run(engine, api, settings)
        report = reconcile.run(engine, api, settings.notifier_callback_url)
    finally:
        api.close()
    engine.dispose()
    for line in report.lines():
        print(line)
    return OK if not report.problems and report.unreachable is None else FAILED


def _dry_run(engine: Any, api: Any, settings: Settings) -> int:
    """Say what a pass would do — create, take, retire — and leave
    everything, here and there, as it is."""
    with Session(engine) as session:
        wanted = desired.wanted(session)
        rows = {
            (row.kind, row.key): row
            for row in session.scalars(select(WatchSubscription).where(WatchSubscription.state != "retired"))
        }
    engine.dispose()
    print(f"the Notifier at {shown_url(settings.notifier_api_url)}; webhooks to {settings.notifier_callback_url}")
    try:
        status = api.status()
        listing = api.list_subscriptions()
    except NotifierError as error:
        print(f"the Notifier could not be read: {error}")
        return FAILED
    if status.get("publicKey") not in settings.notifier_signer_keys:
        print(
            f"the Notifier signs with {status.get('publicKey')}, which is not among MTL_HORIZON_NOTIFIER_KEYS: "
            "its webhooks would be refused here, and a pass would register nothing"
        )
    would: list[dict[str, str]] = []
    for key, want in sorted(wanted.items()):
        row = rows.get(key)
        if row is not None and row.state in reconcile.CONFIRMED:
            continue
        params = subscription_params(want.kind, want.key, settings.notifier_callback_url)
        fits = [sub for sub in listing if same_subscription(sub, params)]
        if not fits:
            what = "create"
        elif len(fits) == 1:
            what = f"take {remote_id(fits[0])}"
        else:
            what = f"AMBIGUOUS, {len(fits)} fit: " + ", ".join(str(remote_id(sub)) for sub in fits)
        would.append({"what": what, "kind": want.kind, "key": want.key[:16], "why": ", ".join(want.rules)})
    for key, row in sorted(rows.items()):
        if key not in wanted and row.remote_id:
            would.append(
                {
                    "what": f"retire {row.remote_id}",
                    "kind": row.kind,
                    "key": row.key[:16],
                    "why": "this installation made it" if row.managed else "made by hand: forgotten here, left there",
                }
            )
    if would:
        print_table("a pass would", would, ("what", "kind", "key", "why"))
    else:
        print("a pass would do nothing: everything wanted is registered and confirmed")
    foreign = [sub for sub in listing if sub.get("reaction_url") != settings.notifier_callback_url]
    print(f"{len(listing)} subscription(s) at the Notifier; {len(foreign)} with another address are left as they are")
    print("dry run: nothing was registered, retired or written")
    return OK


def command_adopt(args: argparse.Namespace, settings: Settings) -> int:
    """Take a subscription made by hand as this installation's."""
    engine = engine_for(settings)
    api = provision.make_api(settings) if not args.no_verify else None
    try:
        row_id = reconcile.adopt(
            engine, args.kind, args.key, args.id, callback_url=settings.notifier_callback_url, api=api
        )
    finally:
        if api is not None:
            api.close()
    engine.dispose()
    print(
        f"taken as row {row_id}: {args.kind} {args.key} is subscription {args.id} at the Notifier; "
        "it is never deleted by this installation"
    )
    return OK


def command_release(args: argparse.Namespace, settings: Settings) -> int:
    engine = engine_for(settings)
    said = reconcile.release(engine, args.id)
    engine.dispose()
    print(said)
    return OK


def _when(moment: Any) -> str:
    from datetime import UTC

    return "-" if moment is None else moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# -- the parser ----------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m app.subscriptions", description="The subscriptions this installation keeps at Operations Notifier.")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--log-format", choices=("text", "json"), default="text", help="log lines on stderr (default: text)")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-file", help="also write the log to this file, line by line as it happens")
    commands = parser.add_subparsers(dest="command", required=True)

    sub = commands.add_parser("status", help="what is wanted, what is registered, what went wrong; asks nothing of the Notifier")
    sub.set_defaults(handler=command_status)
    sub.add_argument("--examples", type=int, default=8, help="how many rows to list (default: 8)")

    sub = commands.add_parser("reconcile", help="one pass with the Notifier: register, confirm, retire")
    sub.set_defaults(handler=command_reconcile)
    sub.add_argument("--dry-run", action="store_true", help="say what the pass would do; register and retire nothing, write nothing")

    sub = commands.add_parser("adopt", help="take a subscription made by hand at the Notifier as this installation's")
    sub.set_defaults(handler=command_adopt)
    sub.add_argument("--kind", choices=(ACCOUNT, ASSET), required=True)
    sub.add_argument("--key", required=True, help="the account's address, or the asset as CODE:ISSUER (native for XLM)")
    sub.add_argument("--id", required=True, help="the subscription's id at the Notifier")
    sub.add_argument("--no-verify", action="store_true", help="do not read it back from the Notifier first")

    sub = commands.add_parser("release", help="forget a subscription adopted by hand; it stays at the Notifier")
    sub.set_defaults(handler=command_release)
    sub.add_argument("--id", type=int, required=True, help="the row's id here, as `status` lists it")
    return parser


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
