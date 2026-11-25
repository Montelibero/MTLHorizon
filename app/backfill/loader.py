"""Backfill: read a ledger window of the watch lists from Horizon, newest ledger first.

Per stream the window is read page by page with ``order=desc``. The network
is used only between transactions; one short transaction then stores the page,
the coverage it proves and the checkpoint to continue from. A stop at any
moment leaves a state the next run picks up: what was committed is verified,
the rest is still pending.

Coverage is only claimed for what was read. What the source no longer keeps
— it answered 410, or its history moved up through ledgers not read yet —
becomes ``unverifiable``, never ``complete``.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import Connection, Engine, text
from sqlalchemy.orm import Session

from app.backfill import store
from app.backfill.jobs import TRADE_MAKING_OPERATIONS, StreamJob
from app.sources import horizon_records as records
from app.sources.horizon import BeforeHistory, HorizonClient, OrderKey, Page
from app.sources.horizon_cursors import InvalidCursor, after_ledger, order_key, parse_cursor
from app.storage import coverage
from app.storage.upsert import checkpoint, set_checkpoint
from app.storage.writer import HistoryWriter

logger = logging.getLogger(__name__)

CONTEXT_STREAMS = frozenset(records.CONTEXT_STREAMS)

_CONTEXT_STATE = text(
    "SELECT transaction_hash, context_loaded FROM history_transactions "
    "WHERE transaction_hash = :hash OR inner_transaction_hash = :hash"
)
_OPERATION_PARENT = text(
    "SELECT t.transaction_hash FROM history_operations o "
    "JOIN history_transactions t ON t.id = o.transaction_id WHERE o.id = :id"
)


class RunStopped(Exception):
    """The run ended before everything was verified: a limit or a signal."""


class StreamFailed(Exception):
    """A stream could not be read or stored; its unfinished page was not committed."""


@dataclass(frozen=True)
class Window:
    """The ledgers to verify, both ends included."""

    from_ledger: int
    to_ledger: int

    def __post_init__(self) -> None:
        if not 0 < self.from_ledger <= self.to_ledger:
            raise ValueError(f"empty ledger window [{self.from_ledger}, {self.to_ledger}]")


@dataclass
class Limits:
    """What a run may do before it stops where it stands.

    The request limit, the time limit and the stop event apply to every
    request; the page limit to stream pages, so that a page taken is also a
    page stored.
    """

    requests: Callable[[], int]
    max_pages: int | None = None
    max_requests: int | None = None
    max_seconds: float | None = None
    stop: threading.Event = field(default_factory=threading.Event)
    started: float = field(default_factory=time.monotonic)
    pages: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def check(self) -> None:
        if self.stop.is_set():
            raise RunStopped("stopped on request")
        if self.max_requests is not None and self.requests() >= self.max_requests:
            raise RunStopped(f"request limit reached ({self.max_requests})")
        if self.max_seconds is not None and time.monotonic() - self.started >= self.max_seconds:
            raise RunStopped(f"time limit reached ({self.max_seconds:.0f}s)")

    def before_page(self) -> None:
        self.check()
        if self.max_pages is not None and self.pages >= self.max_pages:
            raise RunStopped(f"page limit reached ({self.max_pages})")

    def page_taken(self) -> None:
        with self._lock:
            self.pages += 1


@dataclass
class StreamReport:
    name: str
    coverage: dict[str, int] = field(default_factory=dict)
    pages: int = 0
    records: int = 0
    status: str = "stopped"
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "stream": self.name,
            "status": self.status,
            "pages": self.pages,
            "records": self.records,
            "coverage_ledgers": self.coverage,
            "error": self.error,
        }


@dataclass
class ContextBundle:
    """A transaction and the parts of its context fetched for it.

    A part that is None was not fetched because it is stored already.
    """

    transaction: Mapping[str, Any]
    transaction_id: int
    operations: list[Mapping[str, Any]] | None
    effects: list[Mapping[str, Any]] | None
    trades: list[Mapping[str, Any]] | None
    # What storing this bundle completes, a subset of the context streams.
    completes: frozenset[str]


# transaction hash -> the context streams stored for it, or None
ContextLookup = Callable[[str], "tuple[str, frozenset[str]] | None"]
# operation id -> the hash of its transaction, or None when not stored
OperationLookup = Callable[[int], "str | None"]


class ContextFetcher:
    """Fetches what is missing of a transaction's context, once.

    A transaction whose context is stored is recognised by the database
    (``history_transactions.context_loaded``), so meeting it again, here or
    in a later run, costs nothing. A bundle fetched for a page that is then
    not committed stays here and is stored by whichever page needs it next.
    """

    def __init__(self, client: HorizonClient, lookup: ContextLookup, lookup_operation: OperationLookup) -> None:
        self._client = client
        self._lookup = lookup
        self._lookup_operation = lookup_operation
        self._bundles: dict[str, ContextBundle] = {}
        self._aliases: dict[str, str] = {}
        self._stored: set[str] = set()
        self._parents: dict[int, str] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()
        self._local = threading.local()
        self.fetched = 0
        self.reused = 0

    def prepare(self, transaction_hash: str, transaction: Mapping[str, Any] | None = None) -> None:
        """Make sure the transaction and its context are stored with the page
        this thread is preparing."""
        with self._lock:
            lock = self._locks.setdefault(transaction_hash, threading.Lock())
        with lock:
            if self._is_stored(transaction_hash):
                self.reused += 1
            else:
                found = self._lookup(transaction_hash)
                if found is not None and CONTEXT_STREAMS <= found[1]:
                    self._stored.add(found[0])
                    self.reused += 1
                else:
                    self._fetch(transaction_hash, transaction, found[1] if found else frozenset())
        self._prepared().append(self._aliases.get(transaction_hash, transaction_hash))

    def transaction_hash_of(self, operation_id: int) -> str:
        """The transaction an operation belongs to: from memory, from the
        database, and only then from the source."""
        known = self._parents.get(operation_id)
        if known is None:
            known = self._lookup_operation(operation_id)
        if known is None:
            operation = self._client.get(f"/operations/{operation_id}")
            known = records.transaction_hash(operation)
        self._parents[operation_id] = known
        return known

    def bundles(self) -> list[ContextBundle]:
        """The bundles this thread's page has to store."""
        bundles = []
        for transaction_hash in dict.fromkeys(self._prepared()):
            if transaction_hash in self._stored:
                continue
            bundle = self._bundles.get(transaction_hash)
            if bundle is None:
                raise StreamFailed(f"the context of transaction {transaction_hash[:8]} was fetched but is gone")
            bundles.append(bundle)
        return bundles

    def committed(self) -> None:
        """This thread's page is stored, so its bundles are in the database."""
        self._stored.update(self._prepared())
        for transaction_hash in self._prepared():
            self._bundles.pop(transaction_hash, None)
        self._prepared().clear()

    def discarded(self) -> None:
        self._prepared().clear()

    def _prepared(self) -> list[str]:
        prepared = getattr(self._local, "prepared", None)
        if prepared is None:
            prepared = self._local.prepared = []
        return prepared

    def _is_stored(self, transaction_hash: str) -> bool:
        transaction_hash = self._aliases.get(transaction_hash, transaction_hash)
        return transaction_hash in self._stored or transaction_hash in self._bundles

    def _fetch(
        self,
        transaction_hash: str,
        transaction: Mapping[str, Any] | None,
        loaded: frozenset[str],
    ) -> None:
        if transaction is None:
            transaction = self._client.get(f"/transactions/{transaction_hash}")
        key, values = records.transaction_values(transaction)
        outer = values["transaction_hash"]
        if values["inner_transaction_hash"]:
            self._aliases[values["inner_transaction_hash"]] = outer
        if transaction_hash not in (outer, values["inner_transaction_hash"]):
            raise records.RecordError(f"/transactions/{transaction_hash[:8]} answered with another transaction")

        # Operations are read again only to tell whether the transaction can
        # have trades; they are not stored again.
        operations = None
        if "operations" not in loaded or "trades" not in loaded:
            operations = self._collection(f"/transactions/{outer}/operations", "operation", key["id"])
        effects = None if "effects" in loaded else self._collection(f"/transactions/{outer}/effects", "effect", key["id"])
        trades = None
        if "trades" not in loaded and any(operation.get("type") in TRADE_MAKING_OPERATIONS for operation in operations or []):
            trades = self._trades_of(key["id"])
        self._bundles[outer] = ContextBundle(
            transaction,
            key["id"],
            operations if "operations" not in loaded else None,
            effects,
            trades,
            CONTEXT_STREAMS - loaded,
        )
        for operation in operations or []:
            self._parents[int(operation["id"])] = outer
        self.fetched += 1

    def _trades_of(self, transaction_id: int) -> list[Mapping[str, Any]]:
        """The trades of a transaction: the global stream read from the
        transaction's position, cut where the next transaction begins."""
        trades: list[Mapping[str, Any]] = []
        pages = self._client.iter_pages(
            "/trades", resource_type="trade", order="asc", cursor=str(transaction_id), limit=200
        )
        for page in pages:
            for trade in page.records:
                operation_id = int(str(trade["paging_token"]).split("-", 1)[0])
                if operation_id >= transaction_id + 4096:
                    return trades
                trades.append(trade)
        return trades

    def _collection(self, path: str, resource_type: str, transaction_id: int) -> list[Mapping[str, Any]]:
        found: list[Mapping[str, Any]] = []
        for page in self._client.iter_pages(path, resource_type=resource_type, order="asc", limit=200):
            for record in page.records:
                if records.transaction_of(record) != transaction_id:
                    raise StreamFailed(f"{path}: a record belongs to another transaction")
                found.append(record)
        return found


class Backfill:
    """Reads a ledger window for the given streams and stores it."""

    def __init__(
        self,
        client: HorizonClient,
        engine: Engine,
        *,
        source: str,
        page_size: int = 200,
        limits: Limits | None = None,
    ) -> None:
        self.client = client
        self.source = source
        self.page_size = page_size
        self.limits = limits if limits is not None else Limits(requests=client_stats(client))
        client.before_request = self.limits.check
        self.writer = HistoryWriter(engine)
        self._read_lock = threading.Lock()
        self._reader: Connection = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        self.contexts = ContextFetcher(client, self._context_state, self._operation_parent)
        # The oldest ledger the source still answers for, as last seen.
        self.elder: int | None = None
        self._latest: int | None = None

    def close(self) -> None:
        self._reader.close()

    def __enter__(self) -> "Backfill":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- running -------------------------------------------------------------

    def plan(self, window: Window, jobs: Sequence[StreamJob]) -> None:
        """Tile the window of every stream with coverage ranges."""
        with self.writer.transaction() as session:
            for job in jobs:
                coverage.plan(session, job.key, self.source, window.from_ledger, window.to_ledger)

    def run(self, window: Window, jobs: Sequence[StreamJob], concurrency: int = 1) -> list[StreamReport]:
        self.plan(window, jobs)
        reports = [StreamReport(job.name) for job in jobs]
        workers = max(1, min(concurrency, len(jobs)))
        submitted: list[tuple[Future[None], StreamReport]] = []
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="stream") as pool:
            for job, report in zip(jobs, reports):
                submitted.append((pool.submit(self._run_job, job, window, report), report))
            # Waiting in submission order is fine: the pool runs them together.
            for future, report in submitted:
                try:
                    future.result()
                except RunStopped as stop:
                    report.status, report.error = "stopped", str(stop)
                    self.limits.stop.set()
                except Exception as failure:  # noqa: BLE001 - one stream must not sink the others
                    logger.exception("%s failed", report.name)
                    report.status, report.error = "failed", f"{type(failure).__name__}: {failure}"
        for job, report in zip(jobs, reports):
            report.coverage = coverage.tally(self._ranges(job, window), window.from_ledger, window.to_ledger)
        return reports

    def _run_job(self, job: StreamJob, window: Window, report: StreamReport) -> None:
        todo = [row for row in self._ranges(job, window) if row.state == coverage.PENDING]
        for row in sorted(todo, key=lambda item: item.from_ledger, reverse=True):
            if self.elder is not None and row.to_ledger < self.elder:
                self._out_of_reach(job, row, window)
                continue
            self._read_range(job, row, window, report)
        report.status = "verified"

    def _ranges(self, job: StreamJob, window: Window) -> list[coverage.LedgerRange]:
        with self._read_lock, Session(self._reader) as session:
            return coverage.within(session, job.key, window.from_ledger, window.to_ledger)

    def _read_range(
        self, job: StreamJob, row: coverage.LedgerRange, window: Window, report: StreamReport
    ) -> None:
        # A pending range may be wider than the window (one planned for a
        # wider window, or ranges merged by a reimport): only the window is read.
        lower = max(row.from_ledger, window.from_ledger)
        upper = min(row.to_ledger, window.to_ledger)
        cursor, segment_has_records = self._resume(job, lower, upper)
        # The position every record must be strictly below: the checkpoint, then
        # the last record of the page before.
        boundary = None if cursor is None else order_key(job.resource_type, cursor)
        page = None
        last_ledger: int | None = None
        while True:
            try:
                page = self._next_page(job, page, cursor, upper)
            except BeforeHistory:
                self._end_of_history(job, lower, upper, last_ledger)
                return
            self.limits.page_taken()
            report.pages += 1

            # Only a page read in order proves anything about the ledgers it
            # spans; one that is not is a broken answer and is not stored.
            keys = self._descending(job, page)
            if keys and boundary is not None and keys[0] >= boundary:
                raise StreamFailed(f"{job.name}: a record lies at or above the cursor {cursor!r}, which was read already")
            if keys:
                boundary = keys[-1]
            ledgers = [records.ledger_of(record, job.resource_type) for record in page.records]
            if ledgers and ledgers[0] > upper:
                raise StreamFailed(f"{job.name}: a record lies above ledger {upper}, the upper bound")
            inside = [record for record, ledger in zip(page.records, ledgers) if ledger >= lower]
            inside_ledgers = [ledger for ledger in ledgers if ledger >= lower]
            finished = page.exhausted or (bool(ledgers) and ledgers[-1] < lower)

            # First everything the page needs from the network, then one transaction.
            self._prepare(inside, job)
            last_ledger = inside_ledgers[-1] if inside_ledgers else None
            # The source's history may have moved up past ledgers of this range
            # that were never read: an empty page does not tell.
            elder = self._source_elder() if finished else None
            try:
                with self.writer.transaction() as session:
                    self._store_page(
                        session, job, inside, inside_ledgers, lower, upper, last_ledger, elder, segment_has_records
                    )
            except Exception:
                self.contexts.discarded()
                raise
            self.contexts.committed()
            report.records += len(inside)

            if finished:
                logger.info("%s: verified ledgers %d..%d", job.name, lower, upper)
                return
            assert last_ledger is not None and inside
            # The next segment starts inside the last ledger, which has records.
            segment_has_records = True
            upper = last_ledger
            cursor = records.paging_token(inside[-1])

    def _descending(self, job: StreamJob, page: Page) -> list[OrderKey]:
        """Sort keys of the page's records, which must strictly descend."""
        try:
            keys = [order_key(job.resource_type, records.paging_token(record)) for record in page.records]
        except InvalidCursor as error:
            raise StreamFailed(f"{job.name}: a record has no usable paging_token ({error})") from error
        for earlier, later in zip(keys, keys[1:]):
            if later >= earlier:
                raise StreamFailed(f"{job.name}: records are not strictly descending, {later} follows {earlier}")
        return keys

    def _next_page(self, job: StreamJob, page: Page | None, cursor: str | None, upper: int) -> Page:
        self.limits.before_page()
        if page is None:
            return self.client.page(
                job.path,
                resource_type=job.resource_type,
                order="desc",
                cursor=cursor or str(after_ledger(upper)),
                limit=self.page_size,
                **job.filters,
            )
        following = self.client.next_page(page)
        assert following is not None, "an exhausted page is a finished range"
        return following

    def _prepare(self, inside: Sequence[Mapping[str, Any]], job: StreamJob) -> None:
        for record in inside:
            if job.stream == "transactions":
                _, values = records.transaction_values(record)
                self.contexts.prepare(values["transaction_hash"], record)
            elif job.stream == "operations":
                self.contexts.prepare(records.transaction_hash(record))
            else:
                operation_id = int(records.paging_token(record).split("-", 1)[0])
                self.contexts.prepare(self.contexts.transaction_hash_of(operation_id))

    def _store_page(
        self,
        session: Session,
        job: StreamJob,
        inside: Sequence[Mapping[str, Any]],
        inside_ledgers: Sequence[int],
        lower: int,
        upper: int,
        last_ledger: int | None,
        elder: int | None,
        segment_has_records: bool,
    ) -> None:
        """Store the page; ``elder`` is set when the range is read to its end."""
        for bundle in self.contexts.bundles():
            store.store_context(session, bundle, bundle.completes)
        for record in inside:
            self._store_record(session, job, record)

        if elder is not None:
            has_records = segment_has_records or bool(inside)
            kept_from = max(lower, elder)  # the elder ledger is not the start of the window
            verified = coverage.complete(kept_from, upper) if has_records else coverage.empty(kept_from, upper)
            pieces = [verified]
            if lower < elder:
                pieces.insert(0, coverage.unverifiable(lower, elder - 1, self._history_note()))
            coverage.replace(session, job.key, self.source, lower, upper, pieces)
            return
        assert last_ledger is not None and inside
        if last_ledger < upper:
            # Everything above the last record's ledger has now been read.
            has_records = segment_has_records or any(ledger > last_ledger for ledger in inside_ledgers)
            piece = coverage.complete(last_ledger + 1, upper) if has_records else coverage.empty(last_ledger + 1, upper)
            coverage.replace(session, job.key, self.source, last_ledger + 1, upper, [piece])
        set_checkpoint(
            session,
            source=self.source,
            resource_type=job.resource_type,
            stream_key=job.stream_key,
            direction="desc",
            cursor=records.paging_token(inside[-1]),
            cursor_ledger=last_ledger,
        )

    def _store_record(self, session: Session, job: StreamJob, record: Mapping[str, Any]) -> None:
        """Store a record of the stream and the participation Horizon stated
        by returning it in this account's stream."""
        if job.stream == "transactions":
            transaction_id = store.store_transaction(session, record)
            if job.participant:
                store.link_transaction(session, transaction_id, [job.participant])
        elif job.stream == "operations":
            operation_id = store.store_operation(session, record)
            if job.participant:
                store.link_operation(session, operation_id, [job.participant])
                store.link_transaction(session, records.transaction_of(record), [job.participant])
        elif job.stream == "effects":
            store.store_effect(session, record)
        else:
            store.store_trade(session, record)

    # -- resuming, the end of history and reads -------------------------------

    def _resume(self, job: StreamJob, lower: int, upper: int) -> tuple[str | None, bool]:
        """The cursor a stopped run left at the top of this range, if any.

        The checkpoint is only trusted when its ledger is the top of the range
        still pending: it was written in the transaction that verified
        everything above it.
        """
        with self._read_lock, Session(self._reader) as session:
            stored = checkpoint(
                session,
                source=self.source,
                resource_type=job.resource_type,
                stream_key=job.stream_key,
                direction="desc",
            )
        if not stored or not stored[0] or stored[1] != upper:
            return None, False
        try:
            parse_cursor(job.resource_type, stored[0])
        except InvalidCursor:
            return None, False
        return stored[0], True

    def _end_of_history(self, job: StreamJob, lower: int, upper: int, last_ledger: int | None) -> None:
        """Horizon answered 410: close the range as far as it was really read.

        410 comes when the cursor lies at or below ``toid(elder, 0, 0)``, the
        first position of the oldest ledger the source keeps. What was read
        above the last record stays verified; what was not read can no longer
        be, and is marked ``unverifiable`` — with one exception. A transaction
        whose cursor gets 410 in the elder ledger is the first one of that
        ledger: nothing precedes it there, so that ledger is read to its end.
        """
        elder = self._source_elder()
        note = self._history_note()
        if last_ledger is None:
            # Nothing of the range could be read.
            top, pieces = upper, [coverage.unverifiable(lower, upper, note)]
        elif last_ledger >= elder:
            # Read down to the first ledger the source still has.
            top = last_ledger
            pieces = [coverage.complete(elder, last_ledger)]
            if lower < elder:
                pieces.insert(0, coverage.unverifiable(lower, elder - 1, note))
        else:
            # The source dropped ledgers that had not been read yet.
            top = last_ledger
            pieces = [
                coverage.unverifiable(
                    lower, last_ledger, note + f"; ledger {last_ledger} was read only down to its top"
                )
            ]
        with self.writer.transaction() as session:
            coverage.replace(session, job.key, self.source, lower, top, pieces)
        logger.info("%s: history ends in [%d, %d]", job.name, lower, upper)

    def _source_elder(self) -> int:
        """The oldest ledger the source still answers for, as of now. A request."""
        root = self.client.root()
        elder = int(root["history_elder_ledger"])
        self.elder = elder if self.elder is None else max(self.elder, elder)
        self._latest = root.get("history_latest_ledger")
        return elder

    def _history_note(self) -> str:
        return f"source keeps history from ledger {self.elder} (history_latest_ledger {self._latest})"

    def _out_of_reach(self, job: StreamJob, row: coverage.LedgerRange, window: Window) -> None:
        note = f"below the history the source keeps (from ledger {self.elder})"
        lower, upper = max(row.from_ledger, window.from_ledger), min(row.to_ledger, window.to_ledger)
        with self.writer.transaction() as session:
            coverage.replace(
                session, job.key, self.source, lower, upper, [coverage.unverifiable(lower, upper, note)]
            )

    def _operation_parent(self, operation_id: int) -> str | None:
        """The transaction of an operation, when the operation is stored."""
        with self._read_lock, Session(self._reader) as session:
            return session.execute(_OPERATION_PARENT, {"id": operation_id}).scalar()

    def _context_state(self, transaction_hash: str) -> tuple[str, frozenset[str]] | None:
        """What the database has of a transaction's context."""
        with self._read_lock, Session(self._reader) as session:
            row = session.execute(_CONTEXT_STATE, {"hash": transaction_hash}).first()
        return None if row is None else (row[0], frozenset(row[1]))


def client_stats(client: HorizonClient) -> Callable[[], int]:
    return lambda: client.stats.requests

