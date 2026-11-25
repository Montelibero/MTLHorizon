"""The live frontiers of the watch list's streams: ``collector_source`` and
``collector_stream``.

Everything here runs inside the caller's transaction. The frontier of a
stream is written in the same transaction as the records, the coverage and
the checkpoint it follows, so it never claims what is not stored.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.storage.models import CollectorSource, CollectorStream

StreamKey = tuple[str, str]


@dataclass(frozen=True)
class StreamState:
    """What is stored about the live frontier of one stream."""

    source: str
    resource_type: str
    stream_key: str
    start_ledger: int
    frontier_ledger: int
    latest_ledger: int | None
    last_attempt_at: datetime | None
    last_success_at: datetime | None
    failures: int
    last_error: str | None

    @property
    def key(self) -> StreamKey:
        return (self.resource_type, self.stream_key)


@dataclass(frozen=True)
class SourceState:
    """What the collector last saw of its source."""

    latest_ledger: int | None
    elder_ledger: int | None
    latest_closed_at: datetime | None
    cycles: int
    last_cycle_at: datetime | None
    last_error: str | None


def _state(row: CollectorStream) -> StreamState:
    return StreamState(
        row.source,
        row.resource_type,
        row.stream_key,
        row.start_ledger,
        row.frontier_ledger,
        row.latest_ledger,
        row.last_attempt_at,
        row.last_success_at,
        row.failures,
        row.last_error,
    )


def source_state(session: Session, source: str) -> SourceState | None:
    row = session.get(CollectorSource, source)
    if row is None:
        return None
    return SourceState(
        row.latest_ledger, row.elder_ledger, row.latest_closed_at, row.cycles, row.last_cycle_at, row.last_error
    )


def streams(session: Session, source: str) -> list[StreamState]:
    """Every stream the collector has a frontier for."""
    rows = session.scalars(
        select(CollectorStream).where(CollectorStream.source == source).order_by(CollectorStream.stream_key)
    )
    return [_state(row) for row in rows]


def attach(
    session: Session,
    source: str,
    keys: Sequence[StreamKey],
    latest_ledger: int,
    *,
    start_from: int | None = None,
) -> tuple[dict[StreamKey, StreamState], list[StreamKey]]:
    """The state of every stream of ``keys``, creating the rows of the streams
    seen for the first time.

    A stream seen for the first time starts at the source's latest ledger:
    live history begins with the ledger after it, and what lies before is
    for a backfill to plan. ``start_from`` names that ledger instead (to
    continue after a backfill); it may not lie ahead of the source. Returns
    the states by key and the keys that were created.
    """
    if start_from is not None and start_from > latest_ledger:
        raise ValueError(f"cannot start at ledger {start_from}: the source has reached {latest_ledger}")
    known = {state.key: state for state in streams(session, source)}
    created: list[StreamKey] = []
    added: list[CollectorStream] = []
    for key in keys:
        if key in known:
            continue
        start = latest_ledger if start_from is None else start_from
        row = CollectorStream(
            source=source,
            resource_type=key[0],
            stream_key=key[1],
            start_ledger=start,
            frontier_ledger=start,
            latest_ledger=latest_ledger,
        )
        session.add(row)
        added.append(row)
        created.append(key)
    session.flush()
    for row in added:
        session.refresh(row)  # the defaults are the database's, not the object's
    states = {state.key: state for state in streams(session, source)}
    return {key: states[key] for key in keys}, created


def cycle_started(
    session: Session,
    source: str,
    *,
    latest_ledger: int,
    elder_ledger: int,
    latest_closed_at: datetime | None,
    at: datetime,
) -> None:
    """A cycle begins: what the source looks like right now is kept, and
    when the cycle began."""
    row = session.get(CollectorSource, source)
    if row is None:
        row = CollectorSource(source=source)
        session.add(row)
    row.latest_ledger = latest_ledger
    row.elder_ledger = elder_ledger
    row.latest_closed_at = latest_closed_at
    row.cycles = (row.cycles or 0) + 1
    row.last_cycle_at = at
    row.last_error = None


def cycle_ended(session: Session, source: str, *, error: str | None) -> None:
    """A cycle ended; ``error`` says why it ended early, if it did."""
    row = session.get(CollectorSource, source)
    if row is not None:
        row.last_error = error


def _row(session: Session, key: StreamKey, source: str) -> CollectorStream:
    row = session.get(CollectorStream, (source, key[0], key[1]))
    assert row is not None, f"stream {key} was not attached"
    return row


def set_frontier(session: Session, source: str, key: StreamKey, frontier_ledger: int) -> None:
    """The stream is read to its end through ``frontier_ledger``."""
    row = _row(session, key, source)
    if frontier_ledger > row.frontier_ledger:
        row.frontier_ledger = frontier_ledger


def note_attempt(session: Session, source: str, key: StreamKey, *, latest_ledger: int, at: datetime) -> None:
    """The stream was read in a cycle that fixed ``latest_ledger``."""
    row = _row(session, key, source)
    row.latest_ledger = latest_ledger
    row.last_attempt_at = at


def note_success(session: Session, source: str, key: StreamKey, *, at: datetime) -> None:
    """The stream was read to the latest ledger of its cycle."""
    row = _row(session, key, source)
    row.last_success_at = at
    row.failures = 0
    row.last_error = None


def note_failure(session: Session, source: str, key: StreamKey, *, error: str, at: datetime) -> None:
    """The stream could not be read; its frontier stays where it was."""
    row = _row(session, key, source)
    row.failures = (row.failures or 0) + 1
    row.last_error = error
