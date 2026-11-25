"""Write rules for the history tables: insert, or merge into the existing row.

A record can be met more than once: the same transaction is found through
several tracked accounts, and a later fetch may know more about a record than
the first one did. Writing it again must therefore never duplicate a row,
never replace a known value with an unknown one, and never silently replace
one known value with a different one. The last case is reported as
:class:`ContentConflict`: either two sources disagree or a parser is wrong,
and both need a human.

Two writers adding the same new record at the same time are not reconciled
here: the later one gets a unique violation and has to retry its batch.

A writer that wants to know what storing cost — the import of a subject's
history, which says how much it added and how much was there already — has
its session count it (``count_stored``): every record of the four history
data types is told to the count as inserted or as met stored. The rules are
the same whether anybody counts or not.
"""

from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence
from enum import Enum
from typing import Any

from sqlalchemy import func, select, tuple_, update
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import TableClause

from app.storage.models import HistoryEffect, HistoryOperation, HistoryTrade, HistoryTransaction, SyncState

# The collections that make up the context of a transaction.
CONTEXT_STREAMS = ("operations", "effects", "trades")

# The data types a history import counts what it stored by; a stream's records are of one of them.
DATA_TYPES = ("transactions", "operations", "effects", "trades")

Row = dict[str, Any]


class UpsertOutcome(str, Enum):
    INSERTED = "inserted"
    UNCHANGED = "unchanged"
    ENRICHED = "enriched"


# The data types a history import counts what it stored by, and the tables
# their rows are kept in. Ledgers, accounts, assets and participation are
# bookkeeping around a record, and are not counted.
COUNTED_TYPES = {
    HistoryTransaction.__table__.name: "transactions",
    HistoryOperation.__table__.name: "operations",
    HistoryEffect.__table__.name: "effects",
    HistoryTrade.__table__.name: "trades",
}
# Where a session that counts keeps its count (``count_stored``).
COUNTING = "mtl-horizon:stored"


class StoredCounts:
    """What storing pages cost: the pages, the records of the streams read
    in them, and — by data type — the rows inserted and the rows that were
    stored already when met.

    A row is counted as often as it is met: the operations of a transaction
    that was brought in with its whole context, and one of which is then
    the record of the stream, are inserted once and met once more. So a
    count tells what storing the pages did, not how many rows the subject
    has: a record is met through several accounts.

    The count of one transaction is made by ``count_stored`` and read once
    that transaction is committed — what is rolled back is counted nowhere.
    A run adds the counts of its committed pages to one of its own, which
    may be done from several threads at once.
    """

    def __init__(self, *, pages: int = 0, records: int = 0) -> None:
        self.pages = pages
        self.records = records
        # Only the data types that were met are keys here.
        self.inserted: dict[str, int] = {}
        self.repeated: dict[str, int] = {}
        self._lock = threading.Lock()

    def met(self, data_type: str, outcome: UpsertOutcome) -> None:
        """A record of ``data_type`` was stored now: it is inserted, or it
        was there already — unchanged, or it was there and is completed."""
        counts = self.inserted if outcome == UpsertOutcome.INSERTED else self.repeated
        counts[data_type] = counts.get(data_type, 0) + 1

    def add(self, other: "StoredCounts") -> None:
        """Add what ``other`` counted: a page's to a run's."""
        with self._lock:
            self.pages += other.pages
            self.records += other.records
            for name in ("inserted", "repeated"):
                mine, theirs = getattr(self, name), getattr(other, name)
                for data_type, number in theirs.items():
                    mine[data_type] = mine.get(data_type, 0) + number

    def as_dict(self) -> Row:
        """The counts by data type, every data type named, those not met as zero."""
        return {
            "pages": self.pages,
            "records": self.records,
            "inserted": {data_type: self.inserted.get(data_type, 0) for data_type in DATA_TYPES},
            "repeated": {data_type: self.repeated.get(data_type, 0) for data_type in DATA_TYPES},
        }


def count_stored(session: Session, counts: StoredCounts | None = None) -> StoredCounts:
    """Count what is stored through ``session`` from now on, and return the
    count — to be read once the transaction is committed.

    A writer says so that it wants to know what storing cost, and every
    record of a counted data type (``COUNTED_TYPES``) stored through the
    session is added to the count as it is stored. ``counts`` is the count
    to add to, for a writer that keeps it after the session is gone; one is
    made when none is given. Nothing else changes for the writer, and
    nothing happens at all in a session that did not ask.
    """
    counts = counts if counts is not None else StoredCounts()
    session.info[COUNTING] = counts
    return counts


def _counted(session: Session, table: TableClause, outcome: UpsertOutcome) -> UpsertOutcome:
    """Tell the count of the session, when it keeps one, what became of a
    record of ``table``; the outcome, to be returned."""
    counting = session.info.get(COUNTING)
    if counting is not None:
        data_type = COUNTED_TYPES.get(table.name)
        if data_type is not None:
            counting.met(data_type, outcome)
    return outcome


class ContentConflict(ValueError):
    """The same record arrived twice with different values for a column."""

    def __init__(
        self, table: str, key: Mapping[str, Any], column: str, existing: object, incoming: object
    ) -> None:
        self.table = table
        self.key = dict(key)
        self.column = column
        self.existing = existing
        self.incoming = incoming
        super().__init__(
            f"{table}: record {self.key!r} is stored with {column}={existing!r}, "
            f"incoming value is {incoming!r}"
        )


def _merge_json(
    table: str,
    key: Mapping[str, Any],
    existing: Mapping[str, Any] | None,
    incoming: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Key-wise union of two JSON objects.

    Keys only one side has are kept from that side, so a fuller record
    completes a sparser one in either direction. A key present on both sides
    must carry the same value; values are compared as JSON (object key order
    does not matter, array order does).
    """
    merged = dict(existing) if existing else {}
    for name, value in (incoming or {}).items():
        if name in merged and merged[name] != value:
            raise ContentConflict(table, key, f"raw.{name}", merged[name], value)
        merged[name] = value
    return merged


def _lock_existing(session: Session, table: TableClause, key: Row) -> Row | None:
    """Stored values of the record keyed by ``key``, or None if there is none.

    The row stays locked until commit, so another writer of the same record
    waits here and then sees the merged result.
    """
    condition = [table.c[name] == value for name, value in key.items()]
    row = session.execute(select(table).where(*condition).with_for_update()).first()
    if row is None:
        return None
    return {column: getattr(row, column) for column in table.c.keys()}


def _merge_into_existing(
    session: Session, table: TableClause, key: Row, values: Row, stored: Row
) -> UpsertOutcome:
    condition = [table.c[name] == value for name, value in key.items()]
    patch: Row = {}
    for column, incoming in values.items():
        existing = stored[column]
        if isinstance(table.c[column].type, sqltypes.JSON):
            merged = _merge_json(table.name, key, existing, incoming)
            if merged != existing:
                patch[column] = merged
        elif existing is None and incoming is not None:
            patch[column] = incoming
        elif existing is not None and incoming is not None and existing != incoming:
            raise ContentConflict(table.name, key, column, existing, incoming)
    if not patch:
        return UpsertOutcome.UNCHANGED
    session.execute(update(table).where(*condition).values(**patch))
    return UpsertOutcome.ENRICHED


def upsert_row(session: Session, table: TableClause, key: Row, values: Row) -> UpsertOutcome:
    """Write one record keyed by the columns in ``key``.

    ``key`` must name the table's primary key (or a unique key). A record not
    stored yet is inserted, and ``values`` then has to be a complete row. A
    stored record only receives the values it is missing, so ``values`` may
    be a partial record that completes it.

    The session is told the outcome when it counts (``count_stored``).
    """
    stored = _lock_existing(session, table, key)
    if stored is None:
        session.execute(insert(table).values(**{**values, **key}))
        return _counted(session, table, UpsertOutcome.INSERTED)
    return _counted(session, table, _merge_into_existing(session, table, key, values, stored))


# Columns that exchange meaning when a trade is presented with the pair reversed.
_SWAPPED = {
    "base_asset_id": "counter_asset_id",
    "base_amount": "counter_amount",
    "base_account_id": "counter_account_id",
    "base_liquidity_pool_id": "counter_liquidity_pool_id",
    "base_offer_id": "counter_offer_id",
}
_FLIPPED = frozenset({"base_is_seller", "price_n", "price_d"})
# Columns that must be equal for the reversed presentation to be the same trade.
_INVARIANT = tuple(
    column
    for column in HistoryTrade.__table__.c.keys()
    if column not in _SWAPPED
    and column not in _SWAPPED.values()
    and column not in _FLIPPED
    and column not in ("raw", "created_at", "updated_at")
)


def _is_pair_reversed(stored: Mapping[str, Any], incoming: Mapping[str, Any]) -> bool:
    """True when ``incoming`` is ``stored`` with base and counter exchanged,
    which is how Horizon answers a market query for the pair in the other
    order. Columns ``incoming`` does not carry are not compared."""
    if incoming["base_is_seller"] != (not stored["base_is_seller"]):
        return False
    if (incoming.get("price_n"), incoming.get("price_d")) != (stored["price_d"], stored["price_n"]):
        return False
    for base, counter in _SWAPPED.items():
        if base in incoming and incoming[base] != stored[counter]:
            return False
    return all(incoming[column] == stored[column] for column in _INVARIANT if column in incoming)


def upsert_trade(session: Session, values: Row) -> UpsertOutcome:
    """Write a trade.

    The trade id does not depend on which side is base, so the same trade seen
    with the pair reversed is recognised and not stored twice; the orientation
    of the first record is kept.
    """
    table = HistoryTrade.__table__
    key = {"id": values["id"]}
    stored = _lock_existing(session, table, key)
    if stored is None:
        session.execute(insert(table).values(**values))
        return _counted(session, table, UpsertOutcome.INSERTED)
    if _is_pair_reversed(stored, values):
        if stored["raw"] is None and values.get("raw") is not None:
            session.execute(update(table).where(table.c.id == values["id"]).values(raw=values["raw"]))
            return _counted(session, table, UpsertOutcome.ENRICHED)
        return _counted(session, table, UpsertOutcome.UNCHANGED)
    return _counted(session, table, _merge_into_existing(session, table, key, values, stored))


def set_checkpoint(
    session: Session,
    *,
    source: str,
    resource_type: str,
    stream_key: str,
    direction: str,
    cursor: str | None,
    cursor_ledger: int | None = None,
) -> None:
    """Save the checkpoint of one frontier of a stream.

    Call it in the same transaction as the data collected up to the cursor;
    the caller's commit or rollback then applies to both together. A
    checkpoint always replaces the previous one of its frontier.
    """
    key = {
        "source": source,
        "resource_type": resource_type,
        "direction": direction,
        "stream_key": stream_key,
    }
    # updated_at is set here: the column's own onupdate only fires for rows
    # changed through the ORM, not through this statement.
    values = {"cursor": cursor, "cursor_ledger": cursor_ledger, "updated_at": func.now()}
    session.execute(
        insert(SyncState.__table__)
        .values(**values, **key)
        .on_conflict_do_update(index_elements=list(key), set_=values)
    )


def checkpoint(
    session: Session, *, source: str, resource_type: str, stream_key: str, direction: str
) -> tuple[str | None, int | None] | None:
    """The stored cursor and its ledger of one frontier, or None if it has none."""
    row = session.get(
        SyncState, (source, resource_type, direction, stream_key)
    )
    return None if row is None else (row.cursor, row.cursor_ledger)


def checkpoints(
    session: Session, source: str, keys: Sequence[tuple[str, str, str]]
) -> dict[tuple[str, str, str], tuple[str | None, int | None]]:
    """The stored cursor and its ledger of several frontiers at once:
    ``(resource_type, direction, stream_key)`` to ``(cursor, cursor_ledger)``
    for ``source``. A frontier nothing is stored for is left out. For a page
    that shows where many streams got to, in one request."""
    found: dict[tuple[str, str, str], tuple[str | None, int | None]] = {}
    if not keys:
        return found
    frontier = tuple_(SyncState.resource_type, SyncState.direction, SyncState.stream_key)
    for row in session.scalars(select(SyncState).where(SyncState.source == source, frontier.in_(tuple(keys)))):
        found[(row.resource_type, row.direction, row.stream_key)] = (row.cursor, row.cursor_ledger)
    return found


def context_loaded(session: Session, transaction_id: int) -> frozenset[str]:
    """The parts of a transaction's context that are stored; empty for a
    transaction that is not stored at all."""
    row = session.execute(
        select(HistoryTransaction.__table__.c.context_loaded).where(
            HistoryTransaction.__table__.c.id == transaction_id
        )
    ).first()
    return frozenset(row[0] if row is not None else [])


def mark_context(session: Session, transaction_id: int, *streams: str) -> None:
    """Record that ``streams`` of the transaction's context are stored."""
    loaded = context_loaded(session, transaction_id)
    merged = [stream for stream in CONTEXT_STREAMS if stream in loaded or stream in streams]
    unknown = set(streams) - set(CONTEXT_STREAMS)
    if unknown:
        raise ValueError(f"not a context stream: {sorted(unknown)}")
    session.execute(
        update(HistoryTransaction.__table__)
        .where(HistoryTransaction.__table__.c.id == transaction_id)
        .values(context_loaded=merged)
    )
