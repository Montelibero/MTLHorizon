"""What the smoke test of the Notifier does, step by step, without a command
line around it. Each step says what it found as plain data.

The steps ask little of the Notifier: ``status`` two requests, ``subscribe``
one or two, ``unsubscribe`` two, ``watch`` none — it reads this installation's
own journal. With the cap of ``app.sources.live`` on (``MTL_HORIZON_LIVE_BUDGET``)
a run cannot go beyond what its owner allowed, whatever happens.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.config import Settings
from app.notifier.client import NotifierClient, NotifierRejected, remote_id, subscription_params
from app.notifier.format import G_ADDRESS
from app.storage import service_state
from app.storage.models import NotifierEvent, ProcessingJob

logger = logging.getLogger(__name__)

# Where the test subscription is remembered, so that only ever that one is deleted.
REMEMBERED = "smoke_subscription"
# What ends the watching: a delivery came, and its job is out of the queue's way.
FINISHED = ("done", "partial", "dead")


def remembered(engine: Engine) -> dict[str, Any] | None:
    with Session(engine) as session:
        kept = service_state.get(session, REMEMBERED)
    return kept if kept.get("id") else None


def _ids(listing: list[dict[str, Any]]) -> set[str]:
    return {str(remote_id(sub)) for sub in listing if remote_id(sub) is not None}


def status(engine: Engine, settings: Settings, api: NotifierClient) -> dict[str, Any]:
    """What the Notifier says of itself and what is there: two requests."""
    said = api.status()
    listing = api.list_subscriptions()
    signer = said.get("publicKey")
    ours = settings.notifier_callback_url
    kept = remembered(engine)
    return {
        "signer": signer,
        "signer_known": bool(settings.notifier_signer_keys) and signer in settings.notifier_signer_keys,
        "version": said.get("version"),
        "active_there": said.get("activeSubscriptions"),
        "at_notifier": len(listing),
        "with_our_address": sorted(str(remote_id(sub)) for sub in listing if sub.get("reaction_url") == ours),
        "callback": ours,
        "test": kept,
        "test_there": bool(kept) and kept["id"] in _ids(listing),
    }


def subscribe(engine: Engine, settings: Settings, api: NotifierClient, account: str, *, now: datetime | None = None) -> dict[str, Any]:
    """Make the one test subscription — unless it is there already."""
    now = now or datetime.now(UTC)
    if not settings.notifier_callback_url:
        raise ValueError("MTL_HORIZON_NOTIFIER_CALLBACK_URL is not set: the Notifier would not know where to send")
    if not G_ADDRESS.match(account or ""):
        raise ValueError("the account is to be a G… address")
    signer = api.status().get("publicKey")
    if not settings.notifier_signer_keys or signer not in settings.notifier_signer_keys:
        raise ValueError(
            f"the Notifier signs with {signer}, which is not among MTL_HORIZON_NOTIFIER_KEYS: its deliveries would be refused here"
        )
    listing = api.list_subscriptions()
    kept = remembered(engine)
    if kept and kept["id"] in _ids(listing):
        return {"created": False, "id": kept["id"], "account": kept["account"]}
    made = api.create(subscription_params("account", account, settings.notifier_callback_url))
    made_id = remote_id(made)
    if made_id is None:
        raise ValueError("the Notifier answered without the subscription's id")
    with Session(engine) as session, session.begin():
        service_state.put(session, REMEMBERED, {"id": str(made_id), "account": account, "at": now.isoformat(timespec="seconds")})
    return {"created": True, "id": str(made_id), "account": account}


def watch(
    engine: Engine, *, timeout: float = 600.0, sleep: Callable[[float], None] = time.sleep, every: float = 2.0
) -> dict[str, Any]:
    """Wait — asking no server — for a delivery of the test subscription and
    for the end of the job it makes. Its owner makes a transaction of the
    test account meanwhile, or waits for one."""
    kept = remembered(engine)
    if not kept:
        raise ValueError("no test subscription is remembered: subscribe first")
    deadline = time.monotonic() + timeout
    while True:
        with Session(engine) as session:
            events = session.execute(
                select(NotifierEvent.received_at, NotifierEvent.state, NotifierEvent.job_id, NotifierEvent.diagnosis, NotifierEvent.ledger_sequence)
                .where(NotifierEvent.subscription == kept["id"])
                .order_by(NotifierEvent.received_at)
            ).all()
            jobs = dict(
                session.execute(
                    select(ProcessingJob.id, ProcessingJob.state).where(ProcessingJob.id.in_([event[2] for event in events if event[2] is not None]))
                ).all()
            )
        arrived = len(events)
        states = sorted({jobs[event[2]] for event in events if event[2] in jobs})
        if arrived and states and all(state in FINISHED for state in states):
            good = any(state == "done" for state in states)
            return {
                "arrived": arrived,
                "finished": True,
                "timed_out": False,
                "good": good,
                "deliveries": [state for _, state, _, _, _ in events],
                "jobs": states,
                "ledgers": sorted({event[4] for event in events if event[4] is not None}),
                "diagnoses": sorted({event[3] for event in events if event[3]}),
            }
        if time.monotonic() >= deadline:
            return {
                "arrived": arrived, "finished": False, "timed_out": True, "good": False,
                "deliveries": [state for _, state, _, _, _ in events], "jobs": states,
                "ledgers": sorted({event[4] for event in events if event[4] is not None}),
                "diagnoses": sorted({event[3] for event in events if event[3]}),
            }
        sleep(every)


def unsubscribe(engine: Engine, api: NotifierClient) -> dict[str, Any]:
    """Delete the test subscription — the one remembered, and nothing else —
    and read the list to say it is gone."""
    kept = remembered(engine)
    if not kept:
        return {"deleted": False, "id": None, "gone": True, "because": "no test subscription is remembered"}
    already = False
    try:
        api.delete(kept["id"])
    except NotifierRejected as error:
        if error.status != 404:
            raise
        already = True
    gone = kept["id"] not in _ids(api.list_subscriptions())
    if gone:
        with Session(engine) as session, session.begin():
            session.execute(delete(service_state.ServiceState).where(service_state.ServiceState.name == REMEMBERED))
    return {"deleted": not already, "id": kept["id"], "gone": gone, "because": "it was gone already" if already else None}
