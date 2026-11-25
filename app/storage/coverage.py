"""Coverage bookkeeping: which ledger ranges of a stream are verified.

Coverage is kept per (subject, data type, policy, policy version) as rows
covering inclusive ledger ranges that never overlap. A range is one of:

* ``pending``: not verified yet;
* ``complete``: verified, records were found;
* ``empty``: verified, no records;
* ``unverifiable``: the source cannot answer for it any more (it now lies
  below the history the source keeps), or a part of it was lost that way.

Each range also says which server of the pool read it (``served_by``): a
pool of several servers hands a window over from one to the next where the
history of the first ends, and a range is then read from more than one.
Nothing is read twice: the ranges of one stream never overlap, whoever read
them.

Everything here runs inside the caller's transaction.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.storage.models import SyncCoverage

PENDING = "pending"
COMPLETE = "complete"
EMPTY = "empty"
UNVERIFIABLE = "unverifiable"
STATES = (PENDING, COMPLETE, EMPTY, UNVERIFIABLE)


@dataclass(frozen=True)
class CoverageKey:
    subject_type: str
    subject_key: str
    data_type: str
    selection_policy: str
    policy_version: int


@dataclass(frozen=True)
class Piece:
    """A range to store. ``verified_at`` None on a verified state means now.
    ``served_by`` is the server that read it, as it is shown to people."""

    from_ledger: int
    to_ledger: int
    state: str
    detail: str | None = None
    verified_at: datetime | None = None
    served_by: str | None = None


@dataclass(frozen=True)
class LedgerRange:
    from_ledger: int
    to_ledger: int
    state: str
    detail: str | None
    verified_at: datetime | None
    served_by: str | None = None

    @property
    def ledgers(self) -> int:
        return self.to_ledger - self.from_ledger + 1


def pending(from_ledger: int, to_ledger: int) -> Piece:
    return Piece(from_ledger, to_ledger, PENDING)


def complete(from_ledger: int, to_ledger: int, served_by: str | None = None) -> Piece:
    return Piece(from_ledger, to_ledger, COMPLETE, served_by=served_by)


def empty(from_ledger: int, to_ledger: int, served_by: str | None = None) -> Piece:
    return Piece(from_ledger, to_ledger, EMPTY, served_by=served_by)


def unverifiable(from_ledger: int, to_ledger: int, detail: str) -> Piece:
    return Piece(from_ledger, to_ledger, UNVERIFIABLE, detail)


def _conditions(key: CoverageKey) -> list:
    return [
        SyncCoverage.subject_type == key.subject_type,
        SyncCoverage.subject_key == key.subject_key,
        SyncCoverage.data_type == key.data_type,
        SyncCoverage.selection_policy == key.selection_policy,
        SyncCoverage.policy_version == key.policy_version,
    ]


def _as_ranges(rows: Iterable[SyncCoverage]) -> list[LedgerRange]:
    return sorted(
        (
            LedgerRange(row.from_ledger, row.to_ledger, row.state, row.detail, row.verified_at, row.served_by)
            for row in rows
        ),
        key=lambda row: row.from_ledger,
    )


def ranges(session: Session, key: CoverageKey) -> list[LedgerRange]:
    """Every range stored for ``key``, lowest ledger first."""
    return _as_ranges(session.scalars(select(SyncCoverage).where(*_conditions(key))))


def within(session: Session, key: CoverageKey, from_ledger: int, to_ledger: int) -> list[LedgerRange]:
    """The ranges of ``key`` that intersect [from_ledger, to_ledger], unclipped."""
    condition = list(_conditions(key))
    condition += [SyncCoverage.to_ledger >= from_ledger, SyncCoverage.from_ledger <= to_ledger]
    return _as_ranges(session.scalars(select(SyncCoverage).where(*condition)))


def replace(
    session: Session,
    key: CoverageKey,
    source: str,
    from_ledger: int,
    to_ledger: int,
    pieces: Sequence[Piece],
) -> list[LedgerRange]:
    """Re-tile [from_ledger, to_ledger] of ``key`` with ``pieces``.

    Stored rows keep everything outside the interval. Neighbouring pieces of
    one state and one detail become one row, verified as of the later of the
    two times, read by every server that read either; so pages stored one by
    one do not grow the table, and putting a split row back together gives
    the row there was.
    """
    if from_ledger > to_ledger:
        raise ValueError(f"empty interval [{from_ledger}, {to_ledger}]")
    for piece in pieces:
        if piece.state not in STATES:
            raise ValueError(f"unknown state {piece.state!r}")
        if not from_ledger <= piece.from_ledger <= piece.to_ledger <= to_ledger:
            raise ValueError(f"piece [{piece.from_ledger}, {piece.to_ledger}] is not inside [{from_ledger}, {to_ledger}]")

    condition = list(_conditions(key))
    # Rows that overlap the interval, and rows that merely touch it: those
    # are taken in when the piece next to them has their state, so that a
    # range verified in several steps ends up as one row.
    condition += [SyncCoverage.to_ledger >= from_ledger - 1, SyncCoverage.from_ledger <= to_ledger + 1]
    kept: list[Piece] = []
    pieces = list(pieces)
    for row in sorted(session.scalars(select(SyncCoverage).where(*condition).with_for_update()), key=lambda r: r.from_ledger):
        if row.to_ledger < from_ledger:  # touches on the left
            if pieces and (row.state, row.detail) == (pieces[0].state, pieces[0].detail):
                kept.append(_piece_of(row))
                session.delete(row)
            continue
        if row.from_ledger > to_ledger:  # touches on the right
            if pieces and (row.state, row.detail) == (pieces[-1].state, pieces[-1].detail):
                kept.append(_piece_of(row))
                session.delete(row)
            continue
        if row.from_ledger < from_ledger:
            kept.append(_piece_of(row, to_ledger=from_ledger - 1))
        if row.to_ledger > to_ledger:
            kept.append(_piece_of(row, from_ledger=to_ledger + 1))
        session.delete(row)
    session.flush()

    now = datetime.now(UTC)
    tiled: list[Piece] = []
    for piece in sorted(kept + pieces, key=lambda piece: piece.from_ledger):
        verified_at = None if piece.state == PENDING else (piece.verified_at or now)
        piece = Piece(piece.from_ledger, piece.to_ledger, piece.state, piece.detail, verified_at, piece.served_by)
        previous = tiled[-1] if tiled else None
        if (
            previous is not None
            and previous.to_ledger + 1 == piece.from_ledger
            and (previous.state, previous.detail) == (piece.state, piece.detail)
        ):
            moments = [moment for moment in (previous.verified_at, piece.verified_at) if moment is not None]
            tiled[-1] = Piece(
                previous.from_ledger,
                piece.to_ledger,
                piece.state,
                piece.detail,
                max(moments, default=None),
                _both(previous.served_by, piece.served_by),
            )
        else:
            tiled.append(piece)

    for before, after in zip(tiled, tiled[1:]):
        if after.from_ledger <= before.to_ledger:
            raise ValueError(f"pieces overlap at ledger {after.from_ledger}")
    session.add_all(
        SyncCoverage(
            subject_type=key.subject_type,
            subject_key=key.subject_key,
            data_type=key.data_type,
            selection_policy=key.selection_policy,
            policy_version=key.policy_version,
            from_ledger=piece.from_ledger,
            to_ledger=piece.to_ledger,
            source=source,
            state=piece.state,
            verified_at=piece.verified_at,
            detail=piece.detail,
            served_by=piece.served_by,
        )
        for piece in tiled
    )
    session.flush()
    return ranges(session, key)


def _piece_of(row: SyncCoverage, *, from_ledger: int | None = None, to_ledger: int | None = None) -> Piece:
    """A stored row as a piece, cut to [``from_ledger``, ``to_ledger``] where
    those are given."""
    return Piece(
        row.from_ledger if from_ledger is None else from_ledger,
        row.to_ledger if to_ledger is None else to_ledger,
        row.state,
        row.detail,
        row.verified_at,
        row.served_by,
    )


def _both(first: str | None, second: str | None) -> str | None:
    """The servers of two neighbouring ranges that become one: each once, in
    the order they read."""
    names = list(dict.fromkeys(name for served in (first, second) if served for name in served.split(",") if name))
    return ",".join(names) if names else None


def plan(
    session: Session, key: CoverageKey, source: str, from_ledger: int, to_ledger: int
) -> list[LedgerRange]:
    """Make sure [from_ledger, to_ledger] is tiled by ranges of ``key``.

    Verified ranges stay as they are, wherever they were verified; ledgers no
    range speaks for become ``pending``. Ranges of ``key`` that stick out of
    the interval are cut at its edge, so a later run can verify the rest.
    """
    pieces: list[Piece] = []
    cursor = from_ledger
    for row in within(session, key, from_ledger, to_ledger):
        start, end = max(row.from_ledger, from_ledger), min(row.to_ledger, to_ledger)
        if start > cursor:
            pieces.append(pending(cursor, start - 1))
        pieces.append(_piece_of(row, from_ledger=start, to_ledger=end))
        cursor = end + 1
    if cursor <= to_ledger:
        pieces.append(pending(cursor, to_ledger))
    return replace(session, key, source, from_ledger, to_ledger, pieces)


def invalidate(
    session: Session, key: CoverageKey, source: str, from_ledger: int, to_ledger: int
) -> list[LedgerRange]:
    """Forget every claim of ``key`` inside [from_ledger, to_ledger]: the range
    is to be read again from the source."""
    return replace(session, key, source, from_ledger, to_ledger, [pending(from_ledger, to_ledger)])


def tally(rows: Iterable[LedgerRange], from_ledger: int, to_ledger: int) -> dict[str, int]:
    """Ledgers per state inside [from_ledger, to_ledger], clipped to it."""
    counts = dict.fromkeys(STATES, 0)
    for row in rows:
        start, end = max(row.from_ledger, from_ledger), min(row.to_ledger, to_ledger)
        if start <= end:
            counts[row.state] += end - start + 1
    return counts


def clear(session: Session, key: CoverageKey) -> None:
    session.execute(delete(SyncCoverage).where(*_conditions(key)))
