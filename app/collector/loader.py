"""Live collection: read each stream forward to the ledger the source has reached.

A cycle asks the source for its latest ledger H, then reads every stream
from its frontier to H with ``order=asc``. A page is stored in one
transaction together with the coverage it proves, the checkpoint and the
stream's new frontier, so the frontier never claims what is not stored.

What the source no longer keeps — the collector was away longer than the
source holds history — is marked ``unverifiable``, never ``complete``, and
reading goes on from the oldest ledger the source keeps. A stream the source
cannot serve is asked again after a wait that doubles with each failure.

Markets are read before accounts, and with only the parents a trade needs: a
chart must not wait for the history of the accounts.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.backfill import store
from app.backfill.jobs import StreamJob
from app.backfill.loader import (
    CONTEXT_MODES,
    FULL,
    PARENTS,
    ContextFetcher,
    ContextIndex,
    Limits,
    RunStopped,
    StreamFailed,
    client_stats,
    prepare,
)
from app.config import ConfigError
from app.sources import horizon_records as records
from app.sources.horizon import BeforeHistory, HorizonClient, RateLimited
from app.sources.horizon_cursors import InvalidCursor, after_ledger, parse_cursor
from app.storage import collector_state, coverage
from app.storage.collector_state import StreamKey, StreamState
from app.storage.upsert import checkpoint, set_checkpoint
from app.storage.writer import HistoryWriter

logger = logging.getLogger(__name__)

VERIFIED, FAILED, STOPPED, WAITING = "verified", "failed", "stopped", "waiting"
# However often a stream failed, it is asked again after at most a day.
MAX_RETRY_WAIT = 24 * 3600


class WrongNetwork(Exception):
    """The source serves another network than this database belongs to."""


@dataclass
class StreamResult:
    """How one stream fared in a cycle."""

    name: str
    status: str = VERIFIED
    start_ledger: int | None = None
    frontier_ledger: int | None = None
    # Ledgers between the frontier and the latest ledger the cycle fixed.
    lag: int | None = None
    pages: int = 0
    records: int = 0
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "stream": self.name,
            "status": self.status,
            "start_ledger": self.start_ledger,
            "frontier_ledger": self.frontier_ledger,
            "lag": self.lag,
            "pages": self.pages,
            "records": self.records,
            "error": self.error,
        }


@dataclass
class CycleReport:
    latest_ledger: int | None = None
    elder_ledger: int | None = None
    latest_closed_at: datetime | None = None
    # Streams seen for the first time in this cycle, and the ledger they start at.
    started: list[str] = field(default_factory=list)
    start_ledger: int | None = None
    seconds: float = 0.0
    pages: int = 0
    requests: int = 0
    streams: list[StreamResult] = field(default_factory=list)
    # Why the cycle ended early, when it did.
    error: str | None = None

    @property
    def failed(self) -> bool:
        return any(result.status == FAILED for result in self.streams)

    @property
    def stopped(self) -> bool:
        return self.error is not None or any(result.status == STOPPED for result in self.streams)

    @property
    def global_frontier(self) -> int | None:
        """The lowest frontier of the streams: what is collected without a gap."""
        frontiers = [result.frontier_ledger for result in self.streams if result.frontier_ledger is not None]
        return min(frontiers) if frontiers else None

    @property
    def lag(self) -> int | None:
        """How many ledgers behind the source the lowest frontier is."""
        if self.global_frontier is None or self.latest_ledger is None:
            return None
        return self.latest_ledger - self.global_frontier

    def as_dict(self) -> dict[str, Any]:
        return {
            "latest_ledger": self.latest_ledger,
            "elder_ledger": self.elder_ledger,
            "latest_closed_at": self.latest_closed_at,
            "started": self.started,
            "start_ledger": self.start_ledger,
            "seconds": round(self.seconds, 1),
            "pages": self.pages,
            "requests": self.requests,
            "global_frontier": self.global_frontier,
            "lag": self.lag,
            "error": self.error,
            "streams": [result.as_dict() for result in self.streams],
        }


class Collector:
    """Reads the streams of the watch list forward, cycle by cycle.

    ``context`` is what is fetched for the records of an account's streams
    before they are stored; ``market_context`` the same for a market's
    trades (``full`` or ``parents``, see :mod:`app.backfill.loader`).
    ``retry_base`` is how long a failed stream waits before it is asked
    again; the wait doubles with every further failure.
    """

    def __init__(
        self,
        client: HorizonClient,
        engine: Engine,
        *,
        source: str,
        network_passphrase: str,
        page_size: int = 200,
        limits: Limits | None = None,
        context: str = FULL,
        market_context: str = PARENTS,
        concurrency: int = 1,
        retry_base: float = 900.0,
    ) -> None:
        if context not in CONTEXT_MODES or market_context not in CONTEXT_MODES:
            raise ValueError(f"context modes are {CONTEXT_MODES}")
        self.client = client
        self.engine = engine
        self.source = source
        self.network_passphrase = network_passphrase
        self.page_size = page_size
        self.context = context
        self.market_context = market_context
        self.concurrency = max(1, concurrency)
        self.retry_base = retry_base
        self.limits = limits if limits is not None else Limits(requests=client_stats(client))
        client.before_request = self.limits.check
        self.writer = HistoryWriter(engine)
        self.index = ContextIndex(engine)
        self.contexts = ContextFetcher(client, self.index.transaction, self.index.operation_parent)
        self.pages = 0
        self._cycle_error: str | None = None

    def close(self) -> None:
        self.index.close()

    def __enter__(self) -> "Collector":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- a cycle -------------------------------------------------------------

    def cycle(
        self, jobs: Sequence[StreamJob], *, start_from: int | None = None, limits: Limits | None = None
    ) -> CycleReport:
        """Read every stream from its frontier to the source's latest ledger.

        Streams seen for the first time start at that ledger, or at
        ``start_from``. ``limits`` bound this cycle: what a stream does not
        reach is read in a later one.
        """
        started = time.monotonic()
        if limits is not None:
            self.limits = limits
            self.client.before_request = limits.check
        self.pages = 0
        report = CycleReport()
        self.client.forget_answered()
        root = self.client.root()
        if root.get("network_passphrase") != self.network_passphrase:
            raise WrongNetwork(
                f"{self.client.settings.base_url} serves the network {root.get('network_passphrase')!r}, "
                f"this database belongs to {self.network_passphrase!r}"
            )
        latest, elder = int(root["history_latest_ledger"]), int(root["history_elder_ledger"])
        if start_from is not None and start_from > latest:
            raise ConfigError(f"--start-ledger {start_from} lies ahead of the source: its latest ledger is {latest}")
        report.latest_ledger, report.elder_ledger = latest, elder
        report.latest_closed_at = _moment(root.get("history_latest_ledger_closed_at"))

        # One transaction: every new stream gets its start ledger, and what
        # the source looks like in this cycle is kept.
        now = datetime.now(UTC)
        keys = [(job.resource_type, job.stream_key) for job in jobs]
        with self.writer.transaction() as session:
            states, created = collector_state.attach(session, self.source, keys, latest, start_from=start_from)
            collector_state.cycle_started(
                session,
                self.source,
                latest_ledger=latest,
                elder_ledger=elder,
                latest_closed_at=report.latest_closed_at,
                at=now,
            )
        report.started = [job.name for job in jobs if (job.resource_type, job.stream_key) in created]
        report.start_ledger = (latest if start_from is None else start_from) if created else None

        # Markets first. A stream that fails does not stop the others.
        ordered = sorted(jobs, key=lambda job: job.key.subject_type != "market")
        results = [StreamResult(job.name) for job in ordered]
        self._cycle_error = None
        submitted: list[tuple[Future[None], StreamResult]] = []
        workers = max(1, min(self.concurrency, len(jobs)))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="stream") as pool:
            for job, result in zip(ordered, results):
                state = states[(job.resource_type, job.stream_key)]
                submitted.append((pool.submit(self._run_stream, job, state, latest, elder, result), result))
            # Waiting in submission order is fine: the pool runs them together.
            for future, result in submitted:
                try:
                    future.result()
                except Exception as failure:  # noqa: BLE001 - one stream must not sink the others
                    logger.exception("%s failed", result.name)
                    result.status, result.error = FAILED, f"{type(failure).__name__}: {failure}"
        report.streams = results
        report.pages = self.pages
        report.requests = self.client.stats.requests
        report.seconds = time.monotonic() - started
        report.error = self._cycle_error
        if report.error is not None:
            with self.writer.transaction() as session:
                collector_state.cycle_ended(session, self.source, error=report.error)
        return report

    def _run_stream(
        self, job: StreamJob, state: StreamState, latest: int, elder: int, result: StreamResult
    ) -> None:
        key = state.key
        result.start_ledger, result.frontier_ledger = state.start_ledger, state.frontier_ledger
        result.lag = latest - state.frontier_ledger
        try:
            self.limits.check()
            if not self._due(state):
                result.status = WAITING
                result.error = f"failed {state.failures} time(s) in a row; asked again later"
                return
            with self.writer.transaction() as session:
                collector_state.note_attempt(session, self.source, key, latest_ledger=latest, at=datetime.now(UTC))
            frontier = self._skip_verified(job, state.frontier_ledger, latest)
            for attempt in (1, 2):
                try:
                    if frontier + 1 < elder:
                        frontier = self._out_of_reach(job, frontier + 1, min(elder - 1, latest))
                    if frontier < latest:
                        frontier = self._read(job, frontier + 1, latest, result)
                    break
                except BeforeHistory:
                    # The history moved up between the root and the page.
                    elder = self._source_elder()
                    if attempt == 2:
                        raise
        except RateLimited:
            # The source asks for a longer wait than the client allows: no
            # stream asks for anything more in this cycle.
            self.limits.stop.set()
            self._cycle_error = "rate limited: the source asks for a longer wait than the client allows"
            result.status, result.error = STOPPED, "rate limited by the source"
            self._note_failure(key, "rate limited by the source")
        except RunStopped as stop:
            result.status, result.error = STOPPED, str(stop)
        except Exception as failure:  # noqa: BLE001 - the stream failed, the others go on
            logger.exception("%s failed", job.name)
            result.status, result.error = FAILED, f"{type(failure).__name__}: {failure}"
            self._note_failure(key, result.error)
        else:
            with self.writer.transaction() as session:
                collector_state.note_success(session, self.source, key, at=datetime.now(UTC))
        self._settle(key, latest, result)

    # -- reading one stream --------------------------------------------------

    def _read(self, job: StreamJob, lower: int, upper: int, result: StreamResult) -> int:
        """Read the stream from ``lower`` up to ``upper``, ledger by ledger
        in the source's order; return the frontier reached (``upper``)."""
        context = self.market_context if job.key.subject_type == "market" else self.context
        frontier = lower - 1
        cursor = self._resume(job, frontier)
        pages = self.client.iter_pages(
            job.path,
            resource_type=job.resource_type,
            order="asc",
            cursor=cursor or str(after_ledger(frontier)),
            limit=self.page_size,
            **job.filters,
        )
        # The ledger of the last record read: where the range still has records.
        # A cycle that stopped inside a range resumes right after its record.
        open_ledger: int | None = frontier + 1 if cursor else None
        for page in pages:
            self.pages += 1
            result.pages += 1
            ledgers = [records.ledger_of(record, job.resource_type) for record in page.records]
            if ledgers and ledgers[0] < lower:
                raise StreamFailed(
                    f"{job.name}: a record lies in ledger {ledgers[0]}, below ledger {lower} where reading starts"
                )
            # The first record above ``upper`` ends the range: everything up
            # to ``upper`` has been answered by then.
            cut = next((position for position, ledger in enumerate(ledgers) if ledger > upper), len(page.records))
            inside, inside_ledgers = page.records[:cut], ledgers[:cut]
            finished = cut < len(page.records) or page.exhausted
            top = upper if finished else inside_ledgers[-1] - 1
            # The ledgers below the first record's hold nothing; from there on
            # the range has records — unless this page holds none and the
            # range still starts at the ledger of the last record read.
            first = inside_ledgers[0] if inside else lower
            has_records = bool(inside) or first == open_ledger

            # First what the page needs from the source, then one transaction.
            prepare(self.contexts, inside, job, context)
            try:
                with self.writer.transaction() as session:
                    store.store_prepared(session, self.contexts)
                    for record in inside:
                        store.store_stream_record(session, job, record, parents_only=context == PARENTS)
                    pieces = []
                    if first > lower:
                        pieces.append(coverage.empty(lower, first - 1))
                    if top >= first:
                        pieces.append(coverage.complete(first, top) if has_records else coverage.empty(first, top))
                    if pieces:
                        end = max(top, first - 1)
                        coverage.replace(session, job.key, self.source, lower, end, pieces)
                        collector_state.set_frontier(session, self.source, (job.resource_type, job.stream_key), end)
                    if inside and not finished:
                        set_checkpoint(
                            session,
                            source=self.source,
                            resource_type=job.resource_type,
                            stream_key=job.stream_key,
                            direction="asc",
                            cursor=records.paging_token(inside[-1]),
                            cursor_ledger=inside_ledgers[-1],
                        )
            except Exception:
                self.contexts.discarded()
                raise
            self.contexts.committed()
            result.records += len(inside)
            if finished:
                return upper
            # The last record's ledger may hold more records, on the next page.
            assert inside
            frontier, lower, open_ledger = top, inside_ledgers[-1], inside_ledgers[-1]

        # The page after a full one came back empty: the stream has nothing
        # more, and is read to ``upper``.
        with self.writer.transaction() as session:
            piece = coverage.complete(lower, upper) if lower == open_ledger else coverage.empty(lower, upper)
            coverage.replace(session, job.key, self.source, lower, upper, [piece])
            collector_state.set_frontier(session, self.source, (job.resource_type, job.stream_key), upper)
        return upper

    def _resume(self, job: StreamJob, frontier: int) -> str | None:
        """The cursor a cycle stopped at, when it was written together with
        this frontier: the checkpoint lies in the first ledger not read to
        its end."""
        with Session(self.engine) as session:
            stored = checkpoint(
                session,
                source=self.source,
                resource_type=job.resource_type,
                stream_key=job.stream_key,
                direction="asc",
            )
        if not stored or not stored[0] or stored[1] != frontier + 1:
            return None
        try:
            parse_cursor(job.resource_type, stored[0])
        except InvalidCursor:
            return None
        return stored[0]

    def _skip_verified(self, job: StreamJob, frontier: int, latest: int) -> int:
        """Ledgers above the frontier that a backfill verified for this
        stream are not read again: the frontier moves to their end."""
        with Session(self.engine) as session:
            rows = coverage.within(session, job.key, frontier + 1, latest)
        reached = frontier
        for row in rows:
            if row.state in (coverage.COMPLETE, coverage.EMPTY) and row.from_ledger <= reached + 1 <= row.to_ledger:
                reached = row.to_ledger
        if reached > frontier:
            with self.writer.transaction() as session:
                collector_state.set_frontier(
                    session, self.source, (job.resource_type, job.stream_key), reached
                )
        return reached

    def _out_of_reach(self, job: StreamJob, lower: int, top: int) -> int:
        """[lower, top] fell out of the source's history before it was read."""
        note = f"the source keeps history from ledger {top + 1} on; the collector was further behind than that"
        with self.writer.transaction() as session:
            coverage.replace(session, job.key, self.source, lower, top, [coverage.unverifiable(lower, top, note)])
            collector_state.set_frontier(session, self.source, (job.resource_type, job.stream_key), top)
        logger.warning(
            "%s: ledgers %d..%d are below the history the source keeps; marked unverifiable", job.name, lower, top
        )
        return top

    def _source_elder(self) -> int:
        return int(self.client.root()["history_elder_ledger"])

    def _due(self, state: StreamState) -> bool:
        """Whether a stream that failed is to be asked again now."""
        if not state.failures or state.last_attempt_at is None:
            return True
        wait = min(self.retry_base * 2 ** (state.failures - 1), MAX_RETRY_WAIT)
        return datetime.now(UTC) >= state.last_attempt_at + timedelta(seconds=wait)

    def _note_failure(self, key: StreamKey, error: str) -> None:
        with self.writer.transaction() as session:
            collector_state.note_failure(session, self.source, key, error=error, at=datetime.now(UTC))

    def _settle(self, key: StreamKey, latest: int, result: StreamResult) -> None:
        """The frontier as it is stored now, whatever happened above."""
        with Session(self.engine) as session:
            stored = {(row.resource_type, row.stream_key): row for row in collector_state.streams(session, self.source)}
        row = stored.get(key)
        if row is not None:
            result.frontier_ledger = row.frontier_ledger
            result.lag = latest - row.frontier_ledger


def _moment(value: object) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None
