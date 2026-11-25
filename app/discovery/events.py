"""Trustlines opened on a discovery asset, seen in the history the worker
stores.

A pass over the network finds who holds an asset now. Between passes, the
next best thing is the moment a trustline is opened: a ``change_trust`` of
a successful transaction that created the line, as Horizon states it in a
``trustline_created`` effect. The worker brings the transaction in whole,
operations and effects among them, so the evidence is in the database — and
it is read from there, not from what a notification said it meant.

What is not an opening: a failed transaction (it changed nothing), a line
removed, and a limit changed on a line that exists — none of them is a
reason to add an account. Where it cannot be told — the effects of the
transaction are not stored yet — nothing is decided: the transaction is
looked at again when its job is done.

The scan keeps where it got to (when it last started), and each pass reads
what was stored since then, with a margin: reading again adds nothing, as
an account is added once. A crash between storing a transaction and the
next pass loses nothing, because the next pass starts from the same point.
"""

from __future__ import annotations

import logging
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.storage import service_state
from app.storage.models import HistoryEffect, HistoryOperation, HistoryTransaction, ProcessingJob
from app.watch import accounts as watched

logger = logging.getLogger(__name__)

STATE = "discovery_events"
CREATED = "trustline_created"
# A pass reads what was stored since it last started, this much earlier.
OVERLAP = timedelta(minutes=5)
# With nothing kept yet, the first pass reads the last day.
FIRST_LOOK_BACK = timedelta(days=1)


@dataclass(frozen=True)
class Opening:
    """A trustline created on an asset, in a stored transaction."""

    account_id: str
    asset_id: str
    transaction_hash: str
    ledger_sequence: int | None


def asset_of(raw: Mapping[str, Any] | None) -> str | None:
    """The asset a stored effect's record names, as an id; None if it names
    none or there is no record."""
    if not isinstance(raw, Mapping):
        return None
    if raw.get("asset_type") == "native":
        return "native"
    code, issuer = raw.get("asset_code"), raw.get("asset_issuer")
    if isinstance(code, str) and isinstance(issuer, str) and code and issuer:
        return f"{code}:{issuer}"
    return None


def openings(engine: Engine, assets: Collection[str], *, since: datetime) -> list[Opening]:
    """The trustlines created on one of ``assets`` in the transactions the
    worker brought in whole and finished on or after ``since``."""
    if not assets:
        return []
    found: list[Opening] = []
    with Session(engine) as session:
        rows = session.execute(
            select(HistoryEffect.raw, HistoryTransaction.transaction_hash, HistoryTransaction.ledger_sequence)
            .join(HistoryOperation, HistoryOperation.id == HistoryEffect.operation_id)
            .join(HistoryTransaction, HistoryTransaction.id == HistoryOperation.transaction_id)
            .join(ProcessingJob, ProcessingJob.transaction_hash == HistoryTransaction.transaction_hash)
            .where(
                ProcessingJob.state == "done",
                ProcessingJob.finished_at.is_not(None),
                ProcessingJob.finished_at >= since,
                HistoryTransaction.successful.is_(True),
                HistoryEffect.type == CREATED,
            )
            .order_by(HistoryTransaction.ledger_sequence, HistoryEffect.id)
        )
        for raw, transaction_hash, ledger in rows:
            asset_id = asset_of(raw)
            if asset_id in assets:
                account = raw.get("account")
                if isinstance(account, str) and account:
                    found.append(Opening(account, asset_id, transaction_hash, ledger))
    return found


def find_and_add(
    engine: Engine, assets: Collection[str], *, now: datetime | None = None
) -> list[watched.AccountChange]:
    """Look for trustlines opened on the assets since the last look, and add
    the accounts to the watch list. Returns what was added or noted."""
    now = now or datetime.now(UTC)
    with Session(engine) as session:
        kept = service_state.get(session, STATE)
    try:
        last = datetime.fromisoformat(kept["since"]) if kept.get("since") else None
    except ValueError:
        last = None
    since = (last - OVERLAP) if last is not None else (now - FIRST_LOOK_BACK)
    changes: list[watched.AccountChange] = []
    seen: set[str] = set()
    with Session(engine) as session, session.begin():
        for opening in openings(engine, assets, since=since):
            if opening.account_id in seen:
                continue  # several lines of one asset in one transaction: one account
            seen.add(opening.account_id)
            changes.append(
                watched.add_account(
                    session,
                    opening.account_id,
                    reason=(
                        f"discovered: opened a trustline to {opening.asset_id} "
                        f"in transaction {opening.transaction_hash[:8]} (ledger {opening.ledger_sequence})"
                    ),
                    origin=watched.DISCOVERY,
                    discovered_by=[opening.asset_id],
                )
            )
        service_state.put(session, STATE, {"since": now.isoformat(timespec="seconds"), "openings": len(changes)})
    added = [change for change in changes if change.outcome == watched.ADDED]
    if added:
        logger.info("discovery: %d account(s) added on a trustline opened on %s", len(added), ", ".join(sorted(assets)))
    return changes
