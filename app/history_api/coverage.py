"""/v1/coverage: what the archive has of a subject, and where it falls short.

A stream of a subject is *verified* over the ledger ranges that were read to
the end from the source (``sync_coverage``); each range says in what state it
is and why it is in it. Outside those ranges there may be records — brought
by another stream, or by a live event — and there may be nothing; the archive
does not know which, and this endpoint does not pretend to. What it says, per
stream: the ranges and their states, where the stored records reach, and — in
``incomplete`` — every way the stream falls short of the subject's whole
history, in words.

Which server of the pool read a range (``served_by``) is left out: this API
names the logical source, not the servers behind it.

``complete`` is never true. An archive of selected accounts and markets
cannot know that it holds everything that ever happened to one of them: the
source keeps a moving window, deliveries are not guaranteed, and the subject
may be older than both. The endpoint exists to say so with specifics.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import asc, desc, func, select
from sqlalchemy.engine import Connection

from app.backfill.jobs import market_key
from app.history_api import streams
from app.storage.models import (
    HistoryEffect,
    HistoryJob,
    HistoryLedger,
    HistoryOperation,
    HistoryOperationParticipant,
    HistoryTrade,
    HistoryTransactionParticipant,
    SyncCoverage,
    TrackedAccount,
    TrackedMarket,
)

STREAMS = ("transactions", "operations", "effects", "trades")
STATES = ("complete", "empty", "pending", "unverifiable")
PARTS = ("operations", "effects", "trades")

_STATEMENT = (
    "This is what the archive collected for {subject}, not its history on the network. Records exist only where a "
    "watched account or market brought them, and only the verified ranges were read to the end. An empty page of "
    "the history API means nothing more is in this archive, not that nothing happened."
)


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def _spans(ledgers: Iterable[tuple[int, int]]) -> str:
    """Ledger ranges for people: ``100–104, 110, 118–120``."""
    return ", ".join(str(low) if low == high else f"{low}\u2013{high}" for low, high in sorted(set(ledgers)))


# -- the coverage rows ---------------------------------------------------------


def _policies(connection: Connection, subject_type: str, subject_key: str) -> dict[str, list[dict[str, Any]]]:
    """Per stream, the ranges stored for the subject, grouped by the policy
    they were collected under, lowest ledger first inside each."""
    rows = connection.execute(
        select(SyncCoverage.__table__)
        .where(SyncCoverage.subject_type == subject_type, SyncCoverage.subject_key == subject_key)
        .order_by(
            SyncCoverage.data_type,
            SyncCoverage.selection_policy,
            SyncCoverage.policy_version,
            SyncCoverage.from_ledger,
        )
    ).all()
    grouped: dict[tuple[str, str, int], list[Any]] = defaultdict(list)
    for row in rows:
        grouped[(row.data_type, row.selection_policy, row.policy_version)].append(row)
    by_stream: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for (data_type, policy, version), ranges in grouped.items():
        ledgers = dict.fromkeys(STATES, 0)
        for row in ranges:
            ledgers[row.state] += row.to_ledger - row.from_ledger + 1
        by_stream[data_type].append(
            {
                "policy": policy,
                "policy_version": version,
                # What was planned, at its widest: nothing is claimed outside.
                "window": {
                    "from_ledger": min(row.from_ledger for row in ranges),
                    "to_ledger": max(row.to_ledger for row in ranges),
                },
                "ledgers": ledgers,
                "ranges": [
                    {
                        "from_ledger": row.from_ledger,
                        "to_ledger": row.to_ledger,
                        "state": row.state,
                        "verified_at": _iso(row.verified_at),
                        "detail": row.detail,
                        "source": row.source,
                    }
                    for row in ranges
                ],
            }
        )
    return dict(by_stream)


def _policy_statements(stream: str, policy: dict[str, Any]) -> list[str]:
    """How the verified ranges of one policy fall short, in words."""
    said: list[str] = []
    unread = [(row["from_ledger"], row["to_ledger"]) for row in policy["ranges"] if row["state"] == "pending"]
    if unread:
        said.append(f"{stream}: pending, not read yet \u2014 ledgers {_spans(unread)}")
    lost: dict[str | None, list[tuple[int, int]]] = defaultdict(list)
    for row in policy["ranges"]:
        if row["state"] == "unverifiable":
            lost[row["detail"]].append((row["from_ledger"], row["to_ledger"]))
    for detail, ledgers in lost.items():
        why = f" ({detail})" if detail else ""
        said.append(f"{stream}: unverifiable \u2014 ledgers {_spans(ledgers)}{why}")
    window = policy["window"]
    said.append(
        f"{stream}: nothing is claimed below ledger {window['from_ledger']} "
        f"or above ledger {window['to_ledger']}"
    )
    return said


# -- where the stored records reach --------------------------------------------


def _end(aggregate: Any, table: Any, column: str, where: Any) -> Any:
    return select(aggregate(table.c[column])).where(where).scalar_subquery()


def _ends(connection: Connection, lowest: Any, highest: Any) -> tuple[int, int] | None:
    """Both ends of what two expressions name; None when there is nothing."""
    low, high = connection.execute(select(lowest, highest)).one()
    return None if low is None else (low, high)


def _account_extent(connection: Connection, stream: str, account: str) -> tuple[int, int] | None:
    """First and last stored record of one stream of an account, as operation
    ids; None when there are none. Each end is one index lookup."""
    keyed = {
        "transactions": (HistoryTransactionParticipant.__table__, "transaction_id"),
        "operations": (HistoryOperationParticipant.__table__, "operation_id"),
        "effects": (HistoryEffect.__table__, "operation_id"),
    }
    if stream in keyed:
        table, column = keyed[stream]
        return _ends(
            connection,
            _end(func.min, table, column, table.c.account_id == account),
            _end(func.max, table, column, table.c.account_id == account),
        )
    # A trade of the account with itself is on both sides: the ends over both.
    trades = HistoryTrade.__table__
    return _ends(
        connection,
        func.least(
            _end(func.min, trades, "operation_id", trades.c.base_account_id == account),
            _end(func.min, trades, "operation_id", trades.c.counter_account_id == account),
        ),
        func.greatest(
            _end(func.max, trades, "operation_id", trades.c.base_account_id == account),
            _end(func.max, trades, "operation_id", trades.c.counter_account_id == account),
        ),
    )


def _market_extent(connection: Connection, base: str, counter: str) -> tuple[int, int] | None:
    """First and last stored trade of a market, in either orientation; None
    when there are none. The market is indexed by time, which grows with the
    ledger: the ends are the first and the last row of each orientation."""
    trades = HistoryTrade.__table__

    def edge(stored_base: str, stored_counter: str, direction: Any) -> Any:
        return (
            select(trades.c.operation_id)
            .where(trades.c.base_asset_id == stored_base, trades.c.counter_asset_id == stored_counter)
            .order_by(direction(trades.c.ledger_close_time))
            .limit(1)
            .scalar_subquery()
        )

    return _ends(
        connection,
        func.least(edge(base, counter, asc), edge(counter, base, asc)),
        func.greatest(edge(base, counter, desc), edge(counter, base, desc)),
    )


def _stored(extent: tuple[int, int] | None) -> dict[str, Any] | None:
    """An extent for the answer, in ledgers: the ledger is the top of an id."""
    if extent is None:
        return None
    return {"first_ledger": extent[0] >> 32, "last_ledger": extent[1] >> 32}


def _extent_statements(stream: str, stored: tuple[int, int] | None, policies: list[dict[str, Any]]) -> list[str]:
    if stored is None or not policies:
        return []
    below = stored[0] >> 32 < min(policy["window"]["from_ledger"] for policy in policies)
    above = stored[1] >> 32 > max(policy["window"]["to_ledger"] for policy in policies)
    said: list[str] = []
    if below:
        said.append(
            f"{stream}: stored records reach down to ledger {stored[0] >> 32}, below the verified window; "
            "they are not verified"
        )
    if above:
        said.append(
            f"{stream}: stored records reach up to ledger {stored[1] >> 32}, above the verified window; "
            "they are not verified"
        )
    return said


# -- subjects ------------------------------------------------------------------


def account(connection: Connection, account_id: str) -> dict[str, Any]:
    policies = _policies(connection, "account", account_id)
    found_streams: dict[str, Any] = {}
    incomplete = []
    for stream in STREAMS:
        found = policies.get(stream, [])
        stored = _account_extent(connection, stream, account_id)
        found_streams[stream] = {"stored": _stored(stored), "policies": found}
        if not found:
            incomplete.append(
                f"{stream}: nothing is verified; no range of this stream was planned or read for this account"
            )
        for policy in found:
            incomplete.extend(_policy_statements(stream, policy))
        incomplete.extend(_extent_statements(stream, stored, found))
    tracked = connection.execute(
        select(TrackedAccount.__table__).where(TrackedAccount.account_id == account_id)
    ).first()
    return {
        "subject": {"type": "account", "id": account_id},
        "tracked": None
        if tracked is None
        else {
            "enabled": bool(tracked.enabled),
            "origin": tracked.origin,
            "discovered_by": tracked.discovered_by,
            "watch_since_ledger": tracked.watch_since_ledger,
            "boundary_due": bool(tracked.boundary_due),
        },
        "import": _import(connection, "account", account_id),
        "archive": _archive(connection),
        "streams": found_streams,
        "complete": False,
        "incomplete": incomplete,
        "statement": _STATEMENT.format(subject="this account"),
    }


def market(connection: Connection, base: str, counter: str) -> dict[str, Any]:
    key = market_key(base, counter)
    low, _, high = key.partition(">")
    found = _policies(connection, "market", key).get("trades", [])
    stored = _market_extent(connection, low, high)
    incomplete = (
        [] if found else ["trades: nothing is verified; no range of this stream was planned or read for this market"]
    )
    for policy in found:
        incomplete.extend(_policy_statements("trades", policy))
    incomplete.extend(_extent_statements("trades", stored, found))
    tracked = connection.execute(
        select(TrackedMarket.__table__).where(
            TrackedMarket.base_asset_id == low, TrackedMarket.counter_asset_id == high
        )
    ).first()
    return {
        "subject": {"type": "market", "id": key, "assets": [low, high]},
        "tracked": None if tracked is None else {"enabled": bool(tracked.enabled)},
        "import": _import(connection, "market", key),
        "archive": _archive(connection),
        "streams": {"trades": {"stored": _stored(stored), "policies": found}},
        "complete": False,
        "incomplete": incomplete,
        "statement": _STATEMENT.format(subject="this market"),
    }


def transaction(connection: Connection, transaction_hash: str) -> dict[str, Any] | None:
    """The context of one transaction: which of its parts were loaded whole,
    how many records of each are stored, and what that leaves open. None when
    the archive does not have the transaction."""
    row = streams.transaction_by_hash(connection, transaction_hash)
    if row is None:
        return None
    loaded = list(row.context_loaded or [])
    operations = connection.execute(
        select(func.count()).select_from(HistoryOperation.__table__).where(HistoryOperation.transaction_id == row.id)
    ).scalar_one()
    low, high = streams.effects_span(None, row)
    effects = connection.execute(
        select(func.count())
        .select_from(HistoryEffect.__table__)
        .where(HistoryEffect.id >= low, HistoryEffect.id <= high)
    ).scalar_one()
    trades = connection.execute(
        select(func.count())
        .select_from(HistoryTrade.__table__)
        .where(HistoryTrade.operation_id >= row.id + 1, HistoryTrade.operation_id <= row.id + row.operation_count)
    ).scalar_one()
    stored = {"operations": operations, "effects": effects, "trades": trades}
    incomplete = []
    for part in PARTS:
        # A failed transaction changed nothing: it has no effects and no trades.
        if part in loaded or (part != "operations" and not row.successful):
            continue
        how_many = (
            f"{stored[part]} are stored, and there may be more"
            if stored[part]
            else "none are stored, and there may be some"
        )
        incomplete.append(f"{part}: not loaded as a whole; {how_many}")
    if not row.successful:
        incomplete.append("the transaction failed: it has no effects and no trades on the network")
    return {
        "subject": {
            "type": "transaction",
            "hash": transaction_hash,
            "transaction_hash": row.transaction_hash,
            "inner_transaction_hash": row.inner_transaction_hash,
            "id": str(row.id),
            "ledger": row.ledger_sequence,
            "successful": bool(row.successful),
        },
        "context_loaded": loaded,
        "context": {part: {"complete": part in loaded, "stored": stored[part]} for part in PARTS},
        "incomplete": incomplete,
        "statement": (
            "The parts in context_loaded were fetched for this transaction as the source serves them; the others "
            "hold only what other streams happened to bring. What the source itself does not serve is not here either."
        ),
    }


def _import(connection: Connection, kind: str, subject: str) -> dict[str, Any] | None:
    """The history import of the subject as the queue keeps it; None when none was ever planned."""
    job = connection.execute(
        select(HistoryJob.__table__).where(HistoryJob.kind == kind, HistoryJob.subject == subject)
    ).first()
    if job is None:
        return None
    return {
        "state": job.state,
        "from_ledger": job.from_ledger,
        "to_ledger": job.to_ledger,
        "boundary_final": bool(job.boundary_final),
        "context": job.context,
        "attempts": job.attempts,
        "slices": job.slices,
        "records": job.records,
        "started_at": _iso(job.started_at),
        "finished_at": _iso(job.finished_at),
        "last_error": job.last_error,
        "paused_because": job.paused_because,
        "result": job.result,
    }


def _archive(connection: Connection) -> dict[str, Any]:
    """The ledgers the whole archive reaches over."""
    low, high = connection.execute(select(func.min(HistoryLedger.sequence), func.max(HistoryLedger.sequence))).one()
    return {"oldest_ledger": low, "newest_ledger": high}
