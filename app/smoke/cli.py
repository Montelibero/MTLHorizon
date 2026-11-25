"""``python -m app.smoke <status | subscribe | watch | unsubscribe>``: the
smoke test of the integration with a real Operations Notifier, for its owner
to run when they allow it. It touches nothing but the one subscription it
makes for the test.

    python -m app.smoke status                      # what the Notifier says of itself; what is there
    python -m app.smoke subscribe --account G…      # the one test subscription, on that account
    python -m app.smoke watch [--timeout 900]       # a delivery of it arrives and its transaction is brought in
    python -m app.smoke unsubscribe                  # the test subscription is deleted, and seen to be gone

Run it with the cap on (``MTL_HORIZON_LIVE_BUDGET``, 20 at most for a whole
run) while the API and the worker of this installation are running: the
delivery comes to the API's webhook, and the worker brings the transaction
in — the test only watches that happen.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from typing import Any

from app import __version__
from app.cli_common import configure_logging, engine_for
from app.config import ConfigError, Settings
from app.notifier.client import NotifierError
from app.smoke import notifier as smoke
from app.sources import live
from app.watch import provision

logger = logging.getLogger("app.smoke")

OK, FAILED = 0, 1


def command_status(settings: Settings) -> int:
    engine = engine_for(settings)
    api = _api(settings, engine)
    try:
        found = smoke.status(engine, settings, api)
    finally:
        engine.dispose()
    print(f"the Notifier signs with {found['signer']}")
    print("  the key is among MTL_HORIZON_NOTIFIER_KEYS: its deliveries are taken here" if found["signer_known"] else "  the key is NOT among MTL_HORIZON_NOTIFIER_KEYS: its deliveries would be refused here")
    if found["version"] is not None:
        print(f"  version: {found['version']}")
    print(f"{found['at_notifier']} subscription(s) at the Notifier; {len(found['with_our_address'])} with this installation's callback address")
    kept = found["test"]
    if kept:
        print(f"the test subscription {kept['id']} on {kept['account'][:8]}… (made {kept.get('at')}): " + ("there" if found["test_there"] else "not there any more"))
    else:
        print("no test subscription was made from here")
    return OK if found["signer_known"] else FAILED


def command_subscribe(settings: Settings, account: str) -> int:
    engine = engine_for(settings)
    api = _api(settings, engine)
    try:
        made = smoke.subscribe(engine, settings, api, account)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return FAILED
    finally:
        engine.dispose()
    print(("the test subscription is made: " if made["created"] else "the test subscription is there already: ") + f"{made['id']} on {made['account']}")
    print("make a transaction of that account (or wait for one), then: python -m app.smoke watch")
    return OK


def command_watch(settings: Settings, timeout: float) -> int:
    engine = engine_for(settings)
    try:
        seen = smoke.watch(engine, timeout=timeout)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return FAILED
    finally:
        engine.dispose()
    print(f"{seen['arrived']} deliverie(s) of the test subscription arrived; ledgers {seen['ledgers'] or '-'}; the jobs: {seen['jobs'] or '-'}")
    for diagnosis in seen["diagnoses"]:
        print(f"  diagnosis: {diagnosis}")
    if seen["finished"] and seen["good"]:
        print("the transaction came through the webhook, was checked by its signature, and was brought in: the integration works for this one delivery")
        print("this proves one delivery, not that every delivery comes: python -m app.smoke unsubscribe, when you are done")
        return OK
    if seen["timed_out"]:
        print("nothing arrived in time. Is the API of this installation reachable from the Notifier at the callback address? Is the worker running?")
    else:
        print("the transaction could not be brought in whole: look at `python -m app.worker events` and `python -m app.worker status`")
    return FAILED


def command_unsubscribe(settings: Settings) -> int:
    engine = engine_for(settings)
    api = _api(settings, engine)
    try:
        gone = smoke.unsubscribe(engine, api)
    finally:
        engine.dispose()
    if gone["id"] is None:
        print(gone["because"])
    else:
        print(f"the test subscription {gone['id']} is " + ("deleted, and seen to be gone" if gone["gone"] and gone["deleted"] else ("gone" if gone["gone"] else "STILL THERE: delete it by hand")))
    return OK if gone["gone"] else FAILED


def _api(settings: Settings, engine: Any) -> Any:
    if not settings.notifier_api_url:
        raise ConfigError("MTL_HORIZON_NOTIFIER_API_URL is not set: the smoke test talks to the Notifier's API")
    live.configure(settings.live_budget, engine)
    api = provision.make_api(settings, engine=engine)
    assert api is not None
    return api


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m app.smoke", description="The smoke test of the integration with a real Operations Notifier: one subscription, made and deleted by the test.")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--log-format", choices=("text", "json"), default="text", help="log lines on stderr (default: text)")
    parser.add_argument("--log-level", default="INFO")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="what the Notifier says of itself and what is there (two requests)").set_defaults(handler=command_status)
    subscribe = commands.add_parser("subscribe", help="make the one test subscription (one or two requests)")
    subscribe.add_argument("--account", required=True, help="the G… address the test subscription is on")
    subscribe.set_defaults(handler=command_subscribe)
    watch = commands.add_parser("watch", help="wait for a delivery of the test subscription and for its job (no request)")
    watch.add_argument("--timeout", type=float, default=900.0, help="how long to wait, in seconds (default: 900)")
    watch.set_defaults(handler=command_watch)
    commands.add_parser("unsubscribe", help="delete the test subscription and see it gone (two requests)").set_defaults(handler=command_unsubscribe)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    configure_logging(args.log_format, args.log_level)
    try:
        settings = Settings.from_env()
        if args.command == "subscribe":
            return args.handler(settings, args.account)
        if args.command == "watch":
            return args.handler(settings, args.timeout)
        return args.handler(settings)
    except (ConfigError, NotifierError) as error:
        print(str(error), file=sys.stderr)
        return FAILED

