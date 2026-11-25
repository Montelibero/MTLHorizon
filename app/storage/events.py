"""Keeping what Operations Notifier sent, and the work it makes.

A delivery is kept as it came and, when a watch rule wants its transaction,
that transaction is queued — both in one transaction of the database, which
is committed before anything is answered to the Notifier. A delivery that
was kept already changes nothing: the Notifier sends again when it got no
answer, and a repeated delivery is recognised by the notification's id and
the subscription it was sent for.

Whether a delivery makes a job is decided by the watch lists, which are
three and independent, joined with OR: an enabled account, an enabled asset,
an enabled market the operation names — and, since stage 05, by discovery:
an asset marked for discovery is a rule too. A transaction is brought in
whole whoever named it: one operation of an account that is switched off
here may be the other side of a trade in a market that is switched on.

A delivery that came for a subscription this installation manages
(``watch_subscription``) is always brought in: the subscription is the
owner's decision, made once, and it would take a very complete payload to
second-guess it. What the local rules would have said is kept with the
delivery all the same.

What cannot be told is not dropped. When the operation is of a type this
reader does not know, or names something it cannot read, whether a rule
matches cannot be said for sure — and the Notifier matched it, or it would
not have come. The transaction is queued all the same, and why it could not
be told is kept with the delivery. When no rule matches and that is certain,
the delivery is kept and no job is made: the watch lists here do not want it,
whatever subscription of the Notifier did.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.notifier.format import OPERATION, Notice
from app.storage.jobs import DEAD, PARTIAL, PENDING
from app.storage.models import ProcessingJob, TrackedAccount, TrackedAsset, TrackedMarket, WatchSubscription
from app.storage.models import NotifierEvent as Event

logger = logging.getLogger(__name__)

QUEUED, UNMATCHED, UNSUPPORTED = "queued", "unmatched", "unsupported"
STATES = (QUEUED, UNMATCHED, UNSUPPORTED)
# The headers of a delivery that are kept with it. None can hold a secret;
# the signature is not among them: it is checked, and then it has done its work.
KEPT_HEADERS = ("X-Subscription", "User-Agent", "X-Requested-With", "Content-Type")


@dataclass(frozen=True)
class Received:
    """What became of a delivery.

    ``created`` False: it was kept already — the answer to the first one was
    lost on the way, or it was sent again for another reason — and nothing
    changed.
    """

    event: Event
    created: bool


def receive(
    engine: Engine,
    notice: Notice,
    *,
    received_at: datetime,
    headers: Mapping[str, str],
    signed_by: str | None,
) -> Received:
    """Keep a delivery and queue its transaction if a watch rule wants it.
    One transaction of the database: the two are kept together or not at all."""
    try:
        with Session(engine, expire_on_commit=False) as session, session.begin():
            kept = session.get(Event, (notice.id, notice.subscription))
            if kept is not None:
                return Received(kept, False)
            return Received(_keep(session, notice, received_at, headers, signed_by), True)
    except IntegrityError:
        # The same delivery was kept a moment ago, by a request that ran
        # beside this one. Nothing of this one is kept: not its job either,
        # for that one made it.
        with Session(engine) as session:
            kept = session.get(Event, (notice.id, notice.subscription))
        if kept is None:
            raise
        return Received(kept, False)


def _keep(
    session: Session, notice: Notice, received_at: datetime, headers: Mapping[str, str], signed_by: str | None
) -> Event:
    """Keep a delivery that was not kept, and queue its transaction if a
    watch rule wants it. Runs inside the caller's transaction."""
    matched = _matched_rules(session, notice)
    ours = _managed(session, notice.subscription)
    job = None
    if notice.type != OPERATION or notice.transaction_hash is None:
        state, diagnosis = UNSUPPORTED, _why_unsupported(notice)
    elif ours is not None:
        state, diagnosis = QUEUED, None
        if not matched:
            diagnosis = (
                f"queued for this installation's subscription ({ours.kind} {ours.key}) although no watch rule "
                + ("could be told to match: " + "; ".join(notice.problems) if not notice.told else "matches it")
            )
        job = _job(session, notice, received_at)
    elif not notice.told:
        state, diagnosis = QUEUED, (
            "queued although whether a watch rule matches could not be told: " + "; ".join(notice.problems)
        )
        job = _job(session, notice, received_at)
    elif matched:
        state, diagnosis = QUEUED, None
        job = _job(session, notice, received_at)
    else:
        state, diagnosis = UNMATCHED, (
            "no enabled account, asset or market names this operation, nor an asset marked for discovery, "
            "and the subscription it came for is not one this installation manages: "
            + ", ".join(_short(notice.names()))
        )
    event = Event(
        id=notice.id,
        subscription=notice.subscription,
        type=notice.type,
        received_at=received_at,
        notified_at=notice.created,
        sent_at=notice.sent,
        operation_id=notice.operation_id,
        operation_type=notice.operation_type,
        account=next(iter(sorted(notice.accounts)), None),
        transaction_hash=notice.transaction_hash,
        transaction_toid=notice.transaction_toid,
        ledger_sequence=notice.ledger,
        accounts=sorted(notice.accounts),
        assets=sorted(notice.assets),
        markets=[sorted(pair) for pair in notice.markets],
        matched=matched,
        diagnosis=diagnosis,
        state=state,
        job_id=None if job is None else job.id,
        payload=dict(notice.raw),  # as it came
        headers=_kept_headers(headers),
        signed_by=signed_by,
    )
    session.add(event)
    session.flush()
    logger.info(
        "notification %s for subscription %s: %s%s",
        notice.id,
        notice.subscription,
        state,
        "" if job is None else f" (job {job.id} for transaction {notice.transaction_hash[:8]})",
    )
    return event


def _managed(session: Session, subscription: str) -> WatchSubscription | None:
    """The subscription of this installation a delivery came for: the one
    whose id at the Notifier is the delivery's subscription, confirmed or
    adopted. A subscription on its way out is not this installation's any more."""
    if not subscription:
        return None
    return session.scalar(
        select(WatchSubscription).where(
            WatchSubscription.remote_id == subscription,
            WatchSubscription.state.in_(("active", "manual")),
        )
    )


def _kept_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """The few headers kept with a delivery, by their usual names, whatever
    case they came in."""
    lower = {name.lower(): value for name, value in headers.items()}
    return {name: lower[name.lower()] for name in KEPT_HEADERS if name.lower() in lower}


def _matched_rules(session: Session, notice: Notice) -> list[dict[str, str]]:
    """The enabled watch rules that name what the operation names."""
    matched: list[dict[str, str]] = []
    accounts = sorted(notice.accounts)
    assets = sorted(notice.assets)
    if accounts:
        for account in session.scalars(
            select(TrackedAccount.account_id).where(
                TrackedAccount.enabled.is_(True), TrackedAccount.account_id.in_(accounts)
            )
        ):
            matched.append({"rule": "account", "key": account})
    if assets:
        for asset in session.scalars(
            select(TrackedAsset.asset_id).where(TrackedAsset.enabled.is_(True), TrackedAsset.asset_id.in_(assets))
        ):
            matched.append({"rule": "asset", "key": asset})
        # A rule of its own: an asset marked for discovery is matched whether
        # or not its history is read.
        for asset in session.scalars(
            select(TrackedAsset.asset_id).where(TrackedAsset.discover.is_(True), TrackedAsset.asset_id.in_(assets))
        ):
            matched.append({"rule": "discovery", "key": asset})
    if notice.markets:
        # A market is an unordered pair; the trade names its two assets.
        pairs = [{sold, bought} for sold, bought in notice.markets]
        for market in session.scalars(
            select(TrackedMarket).where(
                TrackedMarket.enabled.is_(True),
                TrackedMarket.base_asset_id.in_(assets),
                TrackedMarket.counter_asset_id.in_(assets),
            )
        ):
            if {market.base_asset_id, market.counter_asset_id} in pairs:
                matched.append({"rule": "market", "key": f"{market.base_asset_id} / {market.counter_asset_id}"})
    return matched


def _job(session: Session, notice: Notice, now: datetime) -> ProcessingJob:
    """The job of the transaction, made if there is none yet.

    One job for one transaction, whoever brings word of it. Word that comes
    again for a job that had given up takes it back into the queue, a
    limited number of times: after that only a person does."""
    session.execute(
        insert(ProcessingJob)
        .values(
            transaction_hash=notice.transaction_hash,
            transaction_toid=notice.transaction_toid,
            ledger_sequence=notice.ledger,
            state=PENDING,
            attempts=0,
            next_attempt_at=now,
            revivals=0,
        )
        .on_conflict_do_nothing(index_elements=["transaction_hash"])
    )
    job = session.scalar(
        select(ProcessingJob).where(ProcessingJob.transaction_hash == notice.transaction_hash).with_for_update()
    )
    assert job is not None
    if job.state in (PARTIAL, DEAD) and job.revivals < MAX_REVIVALS:
        job.state, job.attempts, job.next_attempt_at = PENDING, 0, now
        job.revivals, job.leased_at, job.lease_expires_at, job.leased_by = job.revivals + 1, None, None, None
        logger.info("word of transaction %s came again: job %s goes back into the queue", notice.transaction_hash[:8], job.id)
    if job.ledger_sequence is None and notice.ledger is not None:
        job.ledger_sequence = notice.ledger
    return job


# How often word of a transaction that had given up takes its job back.
MAX_REVIVALS = 3


def _why_unsupported(notice: Notice) -> str:
    if notice.type != OPERATION:
        return f"a notification of type {notice.type!r}, which is not one this installation reads"
    return "there is no transaction to bring in: " + "; ".join(notice.problems)


def _short(named: Mapping[str, Any]) -> list[str]:
    accounts, assets, markets = named["accounts"], named["assets"], named["markets"]
    said = []
    if accounts:
        said.append("accounts " + ", ".join(account[:6] + "…" for account in accounts))
    if assets:
        said.append("assets " + ", ".join(assets))
    if markets:
        said.append("markets " + ", ".join(markets))
    return said or ["nothing this reader could name"]

