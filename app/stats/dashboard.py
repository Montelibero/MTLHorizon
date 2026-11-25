"""What the admin dashboard shows, read from PostgreSQL and from nothing else.

A browser refreshes the dashboard every half a minute while the API, the
worker and the history service write to the same database, and the worker
must never wait behind a page. So a snapshot is taken in one read-only
transaction with a statement timeout that lasts exactly as long as that
transaction, in bounded queries: the queues are counted through their index
by state, the newest rows are reached through their indexes, and the one
expensive thing — counting a large table — is done exactly only while the
table is small, otherwise estimated, and either way kept in a cache for five
minutes.

Nothing here asks Horizon or the Notifier for anything, opens no log file and
writes nothing. Every figure says when it was measured and whether it was
counted or estimated; a page that shows the one without the other says more
than it knows.
"""

from __future__ import annotations

import dataclasses
import re
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql.elements import TextClause

WORKER_HEARTBEAT = "heartbeat.worker"
HISTORY_HEARTBEAT = "heartbeat.history"
HISTORY_PAUSE = "history_paused"

# A service says something at least every 15 seconds. Nothing for longer than
# a minute, and it is not running, as far as anyone can tell.
HEARTBEAT_INTERVAL_SECONDS = 15.0
STALE_AFTER_SECONDS = 60.0

# What a statement of a snapshot may take. A page that is slow fails and says
# so; it does not hold the database.
STATEMENT_TIMEOUT_SECONDS = 5.0
COUNT_CACHE_SECONDS = 300.0
# Tables with fewer rows than this, as the planner's ``pg_class.reltuples``
# says, are counted exactly; larger ones are shown with the planner's own
# number and said to be an estimate.
EXACT_COUNT_LIMIT = 1_000_000
# A table that was never analyzed has no ``reltuples`` (-1). Its size in pages
# is known all the same: up to this many it is counted whatever the rows are.
NEVER_ANALYZED_PAGES_LIMIT = 1_280  # 10 MB of the main fork
# The age of the oldest pending job is read from its row while pending jobs
# are few; beyond that only what the queue index keeps: a lower bound.
OLDEST_AGE_EXACT_LIMIT = 10_000
ERRORS_SHOWN = 10
PROBLEMS_SHOWN = 100
# How many of the newest finished jobs say what the last ledger was.
FINISHED_SAMPLE = 20

QUEUE_STATES = ("pending", "running", "done", "partial", "dead")
HISTORY_STATES = ("waiting", "running", "done", "partial", "error", "paused")
SUBSCRIPTION_STATES = ("wanted", "active", "error", "ambiguous", "retiring", "manual", "retired")

# How the tables of unique records are called in the figures. Each record is
# stored once under its natural key, so the rows are the distinct records.
UNIQUE_TABLES = {
    "transactions": "history_transactions",
    "operations": "history_operations",
    "effects": "history_effects",
    "trades": "history_trades",
}
COUNTED_TABLES = (*UNIQUE_TABLES.values(), "processing_job")
QUEUE_TABLES = ("processing_job", "history_job")

COUNTED = "counted"
RELTUPLES = "pg_class.reltuples"
NEVER_ANALYZED = "not known: the table was never analyzed (ANALYZE gives the planner its number) and is too large to count for a page"

# What the queue of events is as a whole. The dashboard has to tell an empty
# queue with a live worker from a worker that is not there, and work that is
# being taken from work that waits for nobody.
QUEUE_EMPTY = "empty"
QUEUE_WORKING = "working"
QUEUE_NOT_TAKEN = "not_taken"
QUEUE_IDLE_WITH_DUE_WORK = "idle_with_due_work"

QUEUE_NOTE = (
    "pending: to be done when next_attempt_at has come. running: taken by a worker under a lease. "
    "partial and dead: gave up after its tries; they wait for a person (retry) or for word of the "
    "transaction to come again."
)
FINISHED_LEDGER_NOTE = (
    "The greatest ledger among the newest finished jobs of the live queue: the last ledger a "
    "finished job brought in. It is not a frontier. Nothing follows from it about the ledgers "
    "below it, about transactions of which no notification came, or about the latest ledger of "
    "the network."
)

_ADDRESS = re.compile(r"G[A-Z0-9]{55}")

_SERVICE_STATES = text("SELECT name, value FROM service_state")

_QUEUE_COUNTS = text(
    "SELECT state, count(*) FROM processing_job "
    "WHERE state IN ('pending', 'running', 'partial', 'dead') GROUP BY state"
)
_QUEUE_DUE = text("SELECT count(*) FROM processing_job WHERE state = 'pending' AND next_attempt_at <= :now")
_PAST_LEASE = text("SELECT count(*) FROM processing_job WHERE state = 'running' AND lease_expires_at < :now")
_GAVE_UP = text(
    "SELECT id AS job_id, state, transaction_hash, ledger_sequence, attempts, last_error, finished_at "
    "FROM processing_job WHERE state IN ('partial', 'dead') "
    "ORDER BY finished_at DESC NULLS LAST, id DESC LIMIT :limit"
)

_LAST_NOTIFICATION = text(
    "SELECT received_at, state, ledger_sequence, operation_type FROM notifier_event "
    "ORDER BY received_at DESC LIMIT 1"
)
# The newest done jobs are taken backwards through the queue index
# (state, next_attempt_at): a done job's next_attempt_at is when its last
# attempt became due, which is the queue's own record of how recent it is.
# There is no index on finished_at; sorting every job ever done by it would
# grow with the queue's whole history.
_FINISHED_SAMPLE = text(
    "SELECT max(ledger_sequence), max(finished_at), count(*) FROM ("
    "SELECT ledger_sequence, finished_at FROM processing_job WHERE state = 'done' "
    "ORDER BY next_attempt_at DESC LIMIT :limit) recent"
)

_HISTORY_COUNTS = text("SELECT state, count(*) FROM history_job GROUP BY state")

_SUBSCRIPTION_COUNTS = text("SELECT state, count(*) FROM watch_subscription GROUP BY state")
_SUBSCRIPTION_PROBLEMS = text(
    "SELECT id, state, kind, key, remote_id, attempts, last_error, next_attempt_at FROM watch_subscription "
    "WHERE state IN ('error', 'ambiguous') ORDER BY next_attempt_at, id LIMIT :limit"
)

_DISCOVERY_ASSETS = text("SELECT asset_id FROM tracked_assets WHERE discover ORDER BY asset_id")
_ACCOUNT_ORIGINS = text(
    "SELECT origin, enabled, count(*) FROM tracked_accounts GROUP BY origin, enabled ORDER BY origin, enabled"
)
_LAST_PASSES = text(
    "SELECT DISTINCT ON (asset_id) asset_id, id, state, dry_run, started_at, finished_at, pages, found, "
    "with_line, added, already, disabled, issuer_skipped, last_error "
    "FROM discovery_run WHERE NOT dry_run ORDER BY asset_id, started_at DESC, id DESC"
)

_DATABASE_SIZE = text("SELECT pg_database_size(current_database())")
_TABLE_SIZES = text(
    "SELECT c.relname, pg_total_relation_size(c.oid), pg_relation_size(c.oid), "
    "CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_relation_size(c.reltoastrelid) END, "
    "pg_indexes_size(c.oid), c.reltuples "
    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = current_schema() AND c.relkind = 'r' ORDER BY c.relname"
)
_TABLE_STAT = text(
    "SELECT c.reltuples, pg_relation_size(c.oid) / 8192 FROM pg_class c WHERE c.oid = to_regclass(:table)"
)

# Queues: the job that has waited longest, from its row while the queue is
# small, and from the index alone in a large one.
_OLDEST_EXACT = "SELECT id, created_at, next_attempt_at FROM {table} WHERE state = :state ORDER BY created_at, id LIMIT 1"
_OLDEST_BOUND = "SELECT min(next_attempt_at) FROM {table} WHERE state = :state"
_COUNT_ROWS = "SELECT count(*) FROM {table}"


def _named(table: str, template: str, allowed: tuple[str, ...]) -> TextClause:
    """``template`` with its table filled in. Only tables this module knows
    are accepted: a table name cannot be a parameter of a statement."""
    if table not in allowed:
        raise ValueError(f"not a table this module reads: {table!r}")
    return text(template.format(table=table))


# -- reading -------------------------------------------------------------------


@contextmanager
def reading(
    engine: Engine, *, timeout_seconds: float, work_mem: str | None = None
) -> Iterator[Connection]:
    """One read-only transaction in which no statement may take longer than
    ``timeout_seconds``.

    The settings are the transaction's own: they end with it, and a connection
    that goes back to the pool carries none of them. PostgreSQL refuses any
    write in such a transaction, so a reader cannot change anything by
    accident. JIT is switched off: the row comparisons the activity table
    looks through are estimated at many times the rows there are, and the
    planner compiles a query that takes microseconds for half as long again.
    """
    settings = "set_config('statement_timeout', :timeout, true), set_config('jit', 'off', true)"
    if work_mem is not None:
        settings += ", set_config('work_mem', :work_mem, true)"
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(text("SET TRANSACTION READ ONLY"))
            connection.execute(
                text(f"SELECT {settings}"), {"timeout": f"{max(timeout_seconds, 0.001):g}s", "work_mem": work_mem}
            )
            yield connection
        finally:
            transaction.rollback()  # read-only: there is nothing to keep


def timed_out(error: OperationalError) -> bool:
    """Whether a statement failed because its timeout came."""
    return "statement timeout" in str(error.orig)


def moment(value: datetime | None = None) -> datetime:
    """``value`` in UTC; now when None. A value without a zone is taken as UTC."""
    found = value or datetime.now(UTC)
    if found.tzinfo is None:
        return found.replace(tzinfo=UTC)
    return found.astimezone(UTC)


def read_moment(value: object) -> datetime | None:
    """A datetime from what a service stored (ISO 8601); None when it cannot be read."""
    if not isinstance(value, str):
        return None
    try:
        return moment(datetime.fromisoformat(value))
    except ValueError:
        return None


def short(key: str) -> str:
    """A subscription key with each account address cut to what tells it
    apart in a table: its first six characters."""
    return _ADDRESS.sub(lambda match: match.group(0)[:6] + "…", key)


def plain(value: Any) -> Any:
    """``value`` for a template or for JSON: datetimes as ISO 8601 with their
    zone, dataclasses as mappings, nothing else changed."""
    if isinstance(value, datetime):
        return value.isoformat()
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {item.name: plain(getattr(value, item.name)) for item in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value


# -- the cache -----------------------------------------------------------------


class TtlCache:
    """What was expensive to read, kept in the process for ``ttl_seconds``.

    Several browsers refreshing one dashboard share it. It is one process's:
    another process keeps its own, and nothing here depends on two of them
    agreeing. The age is kept from the moment of putting; a page shows the
    ``measured_at`` of what it shows, whether it comes from here or was read
    just now.
    """

    def __init__(self, ttl_seconds: float = COUNT_CACHE_SECONDS, max_entries: int = 256) -> None:
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self._kept: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Any | None:
        with self._lock:
            found = self._kept.get(key)
            if found is None:
                return None
            expires, value = found
            if time.monotonic() >= expires:
                del self._kept[key]
                return None
            return value

    def put(self, key: str, value: Any, measured_at: datetime) -> None:
        with self._lock:
            now = time.monotonic()
            for name in [name for name, (expires, _) in self._kept.items() if expires <= now]:
                del self._kept[name]
            while len(self._kept) >= self.max_entries:
                del self._kept[min(self._kept, key=lambda name: self._kept[name][0])]
            self._kept[key] = (now + self.ttl_seconds, value)


# -- figures -------------------------------------------------------------------


@dataclass(frozen=True)
class Figure:
    """One number: what it is, whether it was counted or estimated, when it
    was measured and on what (``basis``). ``value`` None means: not known."""

    value: int | None
    exact: bool
    measured_at: datetime
    basis: str = COUNTED


@dataclass(frozen=True)
class Heartbeat:
    """What a service last said of itself in ``service_state``, and whether
    that is recent enough to be counted on. A heartbeat is no proof that the
    work is right: only that somebody is there. ``other`` is whatever else the
    record says (jobs done, slices finished, ...), as the service keeps it."""

    record: str
    heard: bool
    service: str | None
    state: str | None
    at: datetime | None
    age_seconds: float | None
    stale: bool
    job_id: int | None
    other: dict[str, Any]


@dataclass(frozen=True)
class Worker:
    heartbeat: Heartbeat
    # idle | working | stale | unknown
    state: str
    alive: bool

    @property
    def done(self) -> int | None:
        """Jobs the worker says it finished since it started."""
        return _whole(self.heartbeat.other.get("done"))

    @property
    def failed(self) -> int | None:
        """Jobs the worker says it could not finish since it started."""
        return _whole(self.heartbeat.other.get("failed"))


@dataclass(frozen=True)
class Oldest:
    """The job that has waited longest in a queue.

    ``since`` and ``age_seconds`` are exact while the queue is small. In a
    large one only the queue index is read, and the age is a lower bound: how
    long the earliest attempt has been due — less than the age of a job that
    was tried before. Nothing is due: ``age_seconds`` is None and
    ``next_attempt_at`` says when something will be.
    """

    job_id: int | None
    since: datetime | None
    age_seconds: float | None
    exact: bool
    next_attempt_at: datetime | None


@dataclass(frozen=True)
class Queue:
    pending: Figure
    running: Figure
    done: Figure
    partial: Figure
    dead: Figure
    # Pending jobs whose next_attempt_at has come: what a live worker takes now.
    pending_due: Figure
    # Running jobs whose lease ran out: left by a worker that is not coming back.
    running_past_lease: Figure
    waiting_for_a_person: Figure
    oldest_pending: Oldest | None
    # The newest jobs that gave up, with the error of their last try.
    errors: tuple[dict[str, Any], ...]
    # empty | working | not_taken | idle_with_due_work
    status: str
    note: str


@dataclass(frozen=True)
class History:
    heartbeat: Heartbeat
    # Whether the whole queue of history imports is paused by hand.
    queue_paused: bool
    # What is kept of the pause and of its lifting: because, by, at, resumed_by, resumed_at.
    pause: dict[str, Any]
    jobs: dict[str, Figure]
    oldest_waiting: Oldest | None
    # idle | working | paused | not_running | unknown
    status: str


@dataclass(frozen=True)
class Notification:
    """The last delivery Operations Notifier made, as the journal keeps it."""

    received_at: datetime
    age_seconds: float
    ledger_sequence: int | None
    state: str
    operation_type: str | None


@dataclass(frozen=True)
class FinishedLedger:
    """The greatest ledger among the newest finished jobs — ``note`` says what
    that is and is not. ``finished_at`` is when the newest of them finished."""

    ledger_sequence: int | None
    finished_at: datetime | None
    sampled_jobs: int
    note: str = FINISHED_LEDGER_NOTE


@dataclass(frozen=True)
class Freshness:
    last_notification: Notification | None
    last_ledger_among_finished_jobs: FinishedLedger


@dataclass(frozen=True)
class Subscriptions:
    counts: dict[str, Figure]
    # The subscriptions in error or ambiguous: what is wrong, and when it is tried again.
    problems: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class Discovery:
    marked: Figure
    assets: tuple[str, ...]
    # For each asset marked for discovery, its last pass that was not a dry run.
    last_passes: tuple[dict[str, Any], ...]
    # origin -> {"total": Figure, "enabled": Figure}
    accounts_by_origin: dict[str, dict[str, Figure]]
    accounts: Figure
    accounts_enabled: Figure


@dataclass(frozen=True)
class TableSize:
    """A table on disk: the whole of it with its indexes and with what is
    stored outside the main fork, and the parts of that. ``planner_rows`` is
    what the planner believes the table has (-1: never analyzed)."""

    name: str
    total_bytes: int
    heap_bytes: int
    toast_bytes: int
    index_bytes: int
    planner_rows: float


@dataclass(frozen=True)
class Database:
    size_bytes: Figure
    tables: tuple[TableSize, ...]
    # Rows of the tables that are counted or estimated: the four of unique records, and processing_job.
    rows: dict[str, Figure]

    @property
    def unique(self) -> dict[str, Figure]:
        """Distinct transactions, operations, effects and trades."""
        return {name: self.rows[table] for name, table in UNIQUE_TABLES.items() if table in self.rows}


@dataclass(frozen=True)
class Dashboard:
    """One snapshot. ``as_dict`` is what a page and a JSON endpoint show."""

    measured_at: datetime
    worker: Worker
    queue: Queue
    history: History
    freshness: Freshness
    subscriptions: Subscriptions
    discovery: Discovery
    database: Database

    def as_dict(self) -> dict[str, Any]:
        return plain(self)


def _whole(value: object) -> int | None:
    return None if isinstance(value, bool) or not isinstance(value, int) else value


def _age(now: datetime, at: datetime) -> float:
    return round((now - at).total_seconds(), 3)


# -- the snapshot --------------------------------------------------------------


def snapshot(engine: Engine, *, now: datetime | None = None, cache: Any | None = None) -> Dashboard:
    """Everything the dashboard shows, read at ``now`` (UTC; a value without a
    zone is taken as UTC).

    ``cache`` is anything with ``get(key)`` and ``put(key, value, measured_at)``
    — :class:`TtlCache`, for one; None reads everything anew. Only what is
    expensive is kept in it: the row counts of the large tables.
    """
    now = moment(now)
    with reading(engine, timeout_seconds=STATEMENT_TIMEOUT_SECONDS) as connection:
        states = _states(connection)
        database, fresh = _database(connection, now, cache)
        worker = _worker(states.get(WORKER_HEARTBEAT), now)
        return Dashboard(
            measured_at=now,
            worker=worker,
            queue=_queue(connection, database.rows.get("processing_job"), fresh.get("processing_job", False), worker, now),
            history=_history(states, connection, now),
            freshness=_freshness(connection, now),
            subscriptions=_subscriptions(connection, now),
            discovery=_discovery(connection, now),
            database=database,
        )


def _states(connection: Connection) -> dict[str, Mapping[str, Any]]:
    return {name: value for name, value in connection.execute(_SERVICE_STATES)}


def _heartbeat(record: str, value: Mapping[str, Any] | None, now: datetime) -> Heartbeat:
    kept: Mapping[str, Any] = value or {}
    at = read_moment(kept.get("at"))
    age = None if at is None else max(0.0, (now - at).total_seconds())
    named = {"at", "name", "state", "job_id"}
    return Heartbeat(
        record=record,
        heard=value is not None,
        service=kept.get("name") if isinstance(kept.get("name"), str) else None,
        state=kept.get("state") if isinstance(kept.get("state"), str) else None,
        at=at,
        age_seconds=None if age is None else round(age, 3),
        stale=age is None or age > STALE_AFTER_SECONDS,
        job_id=_whole(kept.get("job_id")),
        other={key: item for key, item in kept.items() if key not in named and item is not None},
    )


def _worker(value: Mapping[str, Any] | None, now: datetime) -> Worker:
    heartbeat = _heartbeat(WORKER_HEARTBEAT, value, now)
    if heartbeat.stale:
        state = "stale"  # never heard of, or silent for longer than a service that runs would be
    elif heartbeat.state in ("idle", "working"):
        state = heartbeat.state
    else:
        state = "unknown"
    return Worker(heartbeat=heartbeat, state=state, alive=not heartbeat.stale)


def _oldest(connection: Connection, table: str, state: str, count: int, now: datetime) -> Oldest | None:
    """The job in ``state`` that has waited longest, as far as is cheap to know."""
    if count == 0:
        return None
    if count <= OLDEST_AGE_EXACT_LIMIT:
        job_id, created, next_attempt = connection.execute(
            _named(table, _OLDEST_EXACT, QUEUE_TABLES), {"state": state}
        ).one()
        return Oldest(job_id, created, max(0.0, _age(now, created)), True, next_attempt)
    # A large queue: the index says when the earliest attempt is due, and no more.
    due = connection.execute(_named(table, _OLDEST_BOUND, QUEUE_TABLES), {"state": state}).scalar()
    if due is None or due > now:
        return Oldest(None, None, None, False, due)
    return Oldest(None, None, _age(now, due), False, due)


def _queue(connection: Connection, rows: Figure | None, rows_fresh: bool, worker: Worker, now: datetime) -> Queue:
    """The live queue.

    ``rows`` is how many jobs the table has in all (:func:`_database` counts
    it once in a while). ``done`` is that less the other states: the done jobs
    are what nothing ever removes, and they are never counted one by one.
    """
    found = dict.fromkeys(QUEUE_STATES, 0)
    for state, number in connection.execute(_QUEUE_COUNTS):
        found[state] = number
    rest = sum(found[state] for state in QUEUE_STATES if state != "done")
    done = Figure(None, False, now, "not known: the table's rows are not counted")
    if rows is not None and rows.value is not None:
        if not rows.exact:
            done = Figure(max(0, rows.value - rest), False, rows.measured_at, f"{rows.basis}, less the other states counted now")
        elif rows_fresh:
            done = Figure(max(0, rows.value - rest), True, now, "counted: the table's rows less the other states")
        else:
            done = Figure(
                max(0, rows.value - rest), False, rows.measured_at, "the table's rows, counted earlier, less the other states counted now"
            )
    due = connection.execute(_QUEUE_DUE, {"now": now}).scalar_one()
    past_lease = connection.execute(_PAST_LEASE, {"now": now}).scalar_one()
    errors = tuple(dict(row) for row in connection.execute(_GAVE_UP, {"limit": ERRORS_SHOWN}).mappings())
    if found["pending"] + found["running"] == 0:
        status = QUEUE_EMPTY
    elif not worker.alive:
        status = QUEUE_NOT_TAKEN
    elif worker.state == "idle" and due > 0 and (worker.heartbeat.age_seconds or 0.0) > HEARTBEAT_INTERVAL_SECONDS:
        status = QUEUE_IDLE_WITH_DUE_WORK
    else:
        status = QUEUE_WORKING
    return Queue(
        pending=Figure(found["pending"], True, now),
        running=Figure(found["running"], True, now),
        done=done,
        partial=Figure(found["partial"], True, now),
        dead=Figure(found["dead"], True, now),
        pending_due=Figure(due, True, now),
        running_past_lease=Figure(past_lease, True, now),
        waiting_for_a_person=Figure(found["partial"] + found["dead"], True, now, "partial and dead"),
        oldest_pending=_oldest(connection, "processing_job", "pending", found["pending"], now),
        errors=errors,
        status=status,
        note=QUEUE_NOTE,
    )


def _history(states: Mapping[str, Mapping[str, Any]], connection: Connection, now: datetime) -> History:
    heartbeat = _heartbeat(HISTORY_HEARTBEAT, states.get(HISTORY_HEARTBEAT), now)
    pause = dict(states.get(HISTORY_PAUSE) or {})
    paused = pause.get("paused") is True
    found = dict.fromkeys(HISTORY_STATES, 0)
    for state, number in connection.execute(_HISTORY_COUNTS):
        found[state] = number
    if heartbeat.stale:
        status = "not_running"
    elif paused or heartbeat.state == "paused":
        status = "paused"
    elif heartbeat.state in ("idle", "working"):
        status = heartbeat.state
    else:
        status = "unknown"
    return History(
        heartbeat=heartbeat,
        queue_paused=paused,
        pause=pause,
        jobs={state: Figure(found[state], True, now) for state in HISTORY_STATES},
        oldest_waiting=_oldest(connection, "history_job", "waiting", found["waiting"], now),
        status=status,
    )


def _freshness(connection: Connection, now: datetime) -> Freshness:
    row = connection.execute(_LAST_NOTIFICATION).first()
    notification = (
        None
        if row is None
        else Notification(
            received_at=row.received_at,
            age_seconds=_age(now, row.received_at),
            ledger_sequence=row.ledger_sequence,
            state=row.state,
            operation_type=row.operation_type,
        )
    )
    ledger, finished, sampled = connection.execute(_FINISHED_SAMPLE, {"limit": FINISHED_SAMPLE}).one()
    return Freshness(notification, FinishedLedger(ledger, finished, sampled))


def _subscriptions(connection: Connection, now: datetime) -> Subscriptions:
    found = dict.fromkeys(SUBSCRIPTION_STATES, 0)
    for state, number in connection.execute(_SUBSCRIPTION_COUNTS):
        found[state] = number
    problems = tuple(
        {
            "id": row["id"],
            "state": row["state"],
            "kind": row["kind"],
            "key": short(row["key"]),
            "remote_id": row["remote_id"],
            "attempts": row["attempts"],
            "last_error": row["last_error"],
            "next_attempt_at": row["next_attempt_at"],
        }
        for row in connection.execute(_SUBSCRIPTION_PROBLEMS, {"limit": PROBLEMS_SHOWN}).mappings()
    )
    return Subscriptions({state: Figure(found[state], True, now) for state in SUBSCRIPTION_STATES}, problems)


def _discovery(connection: Connection, now: datetime) -> Discovery:
    assets = tuple(row[0] for row in connection.execute(_DISCOVERY_ASSETS))
    passes = tuple(
        {
            "asset_id": row["asset_id"],
            "run_id": row["id"],
            "state": row["state"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "pages": row["pages"],
            "found": row["found"],
            "with_line": row["with_line"],
            "added": row["added"],
            "already": row["already"],
            "disabled": row["disabled"],
            "issuer_skipped": row["issuer_skipped"],
            "last_error": row["last_error"],
        }
        for row in connection.execute(_LAST_PASSES).mappings()
    )
    counted: dict[str, dict[str, int]] = {}
    total = enabled = 0
    for origin, is_enabled, number in connection.execute(_ACCOUNT_ORIGINS):
        entry = counted.setdefault(origin, {"total": 0, "enabled": 0})
        entry["total"] += number
        total += number
        if is_enabled:
            entry["enabled"] += number
            enabled += number
    return Discovery(
        marked=Figure(len(assets), True, now),
        assets=assets,
        last_passes=passes,
        accounts_by_origin={
            origin: {"total": Figure(entry["total"], True, now), "enabled": Figure(entry["enabled"], True, now)}
            for origin, entry in sorted(counted.items())
        },
        accounts=Figure(total, True, now),
        accounts_enabled=Figure(enabled, True, now),
    )


def _database(connection: Connection, now: datetime, cache: Any | None) -> tuple[Database, dict[str, bool]]:
    """The database on disk, and the rows of its large tables. With the
    second: for each of those tables, whether its figure was measured in this
    very snapshot or comes from the cache."""
    size = connection.execute(_DATABASE_SIZE).scalar_one()
    tables = tuple(
        TableSize(
            name=row[0], total_bytes=row[1], heap_bytes=row[2], toast_bytes=row[3], index_bytes=row[4], planner_rows=row[5]
        )
        for row in connection.execute(_TABLE_SIZES)
    )
    rows: dict[str, Figure] = {}
    fresh: dict[str, bool] = {}
    for table in COUNTED_TABLES:
        rows[table], fresh[table] = _rows(connection, table, now, cache)
    return Database(Figure(size, True, now, "pg_database_size"), tables, rows), fresh


def _rows(connection: Connection, table: str, now: datetime, cache: Any | None) -> tuple[Figure, bool]:
    """How many rows ``table`` has: counted exactly while the table is small,
    otherwise the planner's estimate; either way kept in the cache. With it:
    whether it was measured now."""
    key = f"rows:{table}"
    kept = None if cache is None else cache.get(key)
    if isinstance(kept, Figure):
        return kept, False
    found = connection.execute(_TABLE_STAT, {"table": table}).first()
    if found is None:
        return Figure(None, False, now, f"not known: there is no table {table}"), True
    reltuples, pages = found
    if reltuples >= 0:
        figure = _estimate(reltuples, now)
        if reltuples < EXACT_COUNT_LIMIT:
            figure = _counted(connection, table, now, figure)
    elif pages <= NEVER_ANALYZED_PAGES_LIMIT:
        figure = _counted(connection, table, now, Figure(None, False, now, f"{NEVER_ANALYZED} in time"))
    else:
        figure = Figure(None, False, now, NEVER_ANALYZED)
    if cache is not None:
        cache.put(key, figure, figure.measured_at)
    return figure, True


def _estimate(reltuples: float, now: datetime) -> Figure:
    return Figure(int(round(reltuples)), False, now, RELTUPLES)


def _counted(connection: Connection, table: str, now: datetime, fallback: Figure) -> Figure:
    """Count the table exactly. A count that runs out of time gives
    ``fallback`` instead of failing the page, and says so."""
    try:
        with connection.begin_nested():
            value = connection.execute(_named(table, _COUNT_ROWS, COUNTED_TABLES)).scalar_one()
    except OperationalError as error:
        if not timed_out(error):
            raise
        return dataclasses.replace(fallback, basis=f"{fallback.basis}; the exact count ran out of time")
    return Figure(value, True, now)
