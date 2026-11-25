"""What the history imports came to, read from the database alone: for the
pages that show the whole of it, the table of jobs, and one job.

Every function takes the engine and the moment the page is shown (``now``),
and returns plain data — a dataclass with ``as_dict()`` holding lists and
dicts of numbers, strings and None. No HTML is made here, no server is
asked — not Horizon, not the Notifier — and no log is parsed. What is said
of the pool is what its last check said, and says so.

Accounts and markets are counted apart, never as one percentage. A job's
window is its own: a stream is measured against the ledgers of its window
and nothing else; pending and unverifiable ledgers are not verified; and
where a number is a share it says what was divided by what. What cannot be
known is None, not a guess — where the pool's history starts, for a job that
has not read yet. There is no ETA here, no count of records not yet read,
no date worked out from an average ledger.

Each function reads in one read-only transaction in which no statement may
run for long: a page that is slow fails as a page, and does not stand in the
way of the worker.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import case, func, or_, select, text, true
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from app.backfill.jobs import POLICY_VERSIONS
from app.storage import coverage, heartbeats, history_jobs
from app.storage.models import (
    HistoryJob,
    HistoryLedger,
    HorizonNodeState,
    ServiceState,
    SyncCoverage,
    TrackedAccount,
    TrackedMarket,
)
from app.storage.upsert import DATA_TYPES, checkpoints

# The name everything read from the pool is kept under — the logical source
# of ``app.cli_common``, said here again so that reading a page imports
# nothing that could ask a server.
SOURCE = "horizon:pool"
# The streams of a job, by its kind, and the policy its coverage is kept under.
STREAMS = {"account": ("transactions", "operations", "effects", "trades"), "market": ("trades",)}
POLICY = {"account": "FULL", "market": "MARKET"}
VERIFIED = (coverage.COMPLETE, coverage.EMPTY)

# No statement of a page may run longer than this: the database is the
# worker's before it is the page's.
STATEMENT_TIMEOUT = 5.0
# The most a page of the table has, however much is asked for.
MAX_PER_PAGE = 200

# The states of the table of jobs, what is being worked on first.
ACTIVE_FIRST = {
    history_jobs.RUNNING: 1,
    history_jobs.WAITING: 2,
    history_jobs.PAUSED: 3,
    history_jobs.PARTIAL: 4,
    history_jobs.ERROR: 5,
    history_jobs.DONE: 6,
}
ORDER = (
    "what is active first: running, then waiting, then paused, partial, error, done; "
    "waiting by the time of the next try, then the job least recently served"
)

# Why a waiting job waits, one reason each, and what is said of it.
GLOBAL_PAUSE = "global_pause"
SERVERS = "servers_cannot_answer"
RATE_LIMIT = "rate_limited"
POOL_SILENT = "pool_silent"
POOL_BEHIND = "pool_behind"
BACKOFF = "backoff_after_failure"
ITS_TURN = "its_turn"
WAITING_REASONS = {
    GLOBAL_PAUSE: "the whole queue is paused",
    SERVERS: "no server of the pool could answer",
    RATE_LIMIT: "a server asks to wait (429)",
    POOL_SILENT: "a server that may keep older history has not said of itself",
    POOL_BEHIND: "the pool has not got as far as the job's boundary",
    BACKOFF: "a slice failed, and the job waits before trying again",
    ITS_TURN: "waiting for its slice",
}
WORKING, PAUSED, GAVE_UP, FINISHED, NOT_FINAL = (
    "working",
    "paused",
    "gave_up",
    "finished",
    "finished_boundary_not_final",
)

UNKNOWN_WINDOW = (
    "the window is not defined yet: the job is asked as far back as the pool keeps, and no slice of it has "
    "found where the pool's history begins"
)
SHARE_METHOD = (
    "ledgers verified (complete or empty) inside the window, over the ledgers of that window; pending and "
    "unverifiable ledgers are not verified"
)

METHODS = {
    "measured_at": "when this was read from the database: the clock of whoever shows the page",
    "subjects": (
        "every switched-on account, and every switched-on market, of the watch lists — with a job or without; "
        "the jobs of subjects that are switched off or gone are not among them, and are counted in not_counted"
    ),
    "states": "the state of the subject's job; a subject without one is in no_job",
    "waiting_reasons": (
        "why each waiting job waits, one reason each: the pause of the whole queue first, then what its last "
        "slice said"
    ),
    "boundary_not_final": (
        "jobs whose upper boundary is not vouched for yet: events are not guaranteed above it, and the job is "
        "extended, and has more to read, when they are"
    ),
    "completion": "jobs that are done over every subject that needs history: a share of subjects, never of records",
    "finished": (
        "a job is done when every stream of its subject is verified up to its boundary — the window that was "
        "available to it; ledgers below what the pool keeps cannot be verified, and are said apart, never "
        "counted as read"
    ),
    "unavailable_ledgers": (
        "summed over the jobs from what the last reading of each found unverifiable: a ledger missing to two "
        "subjects is counted twice"
    ),
    "window": (
        "the lowest and the highest ledger the jobs of a kind are asked for, and where their verified coverage "
        "begins; a job asked as far back as the pool keeps has no bottom until its first slice found it"
    ),
    "pool": "what the last check of each server found, and when: Horizon is not asked for this",
    "share": SHARE_METHOD,
    "speed": (
        "records, and pages, per minute of the time the job's slices took from beginning to end — the waits "
        "between them not included"
    ),
    "counters": (
        "what the job's slices read, and what storing it added — apart — counted as each page was stored; the "
        "worker of events never counts here"
    ),
}


class TooSlow(RuntimeError):
    """A statement of the page ran longer than this module allows. The page
    cannot be built now; the database was let go, and is the worker's."""


@contextmanager
def _reading(engine: Engine) -> Iterator[Session]:
    """A session in one transaction that may only read, in which no statement
    may take longer than ``STATEMENT_TIMEOUT``. What takes longer is let go
    of and said as ``TooSlow``."""
    with engine.connect() as connection, connection.begin():
        connection.execute(text("SET TRANSACTION READ ONLY"))
        # For this transaction only: ``SET`` takes no parameter, ``set_config`` does.
        connection.execute(
            select(func.set_config("statement_timeout", str(int(STATEMENT_TIMEOUT * 1000)), true()))
        )
        with Session(bind=connection, join_transaction_mode="create_savepoint") as session:
            try:
                yield session
            except DBAPIError as error:
                if getattr(error.orig, "sqlstate", None) == "57014":  # query_canceled
                    raise TooSlow(
                        f"reading the imports took longer than {STATEMENT_TIMEOUT:.0f}s: the page cannot be built now"
                    ) from error
                raise


# -- the results ----------------------------------------------------------------


@dataclass(frozen=True)
class Overview:
    """Every subject that needs history, by what its import came to: accounts
    and markets apart."""

    measured_at: datetime
    accounts: dict[str, Any]
    markets: dict[str, Any]
    # The pause of the whole queue, and the worker's last word of itself.
    queue: dict[str, Any]
    pool: dict[str, Any]
    windows: dict[str, dict[str, Any]]
    method: dict[str, str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "measured_at": _iso(self.measured_at),
            "accounts": self.accounts,
            "markets": self.markets,
            "queue": self.queue,
            "pool": self.pool,
            "windows": self.windows,
            "method": self.method,
        }


@dataclass(frozen=True)
class Jobs:
    """One page of the table of jobs: what is being worked on first."""

    measured_at: datetime
    items: list[dict[str, Any]]
    total: int
    page: int
    per_page: int
    pages: int
    has_more: bool
    filters: dict[str, Any]
    order: str
    # The pause of the whole queue: a reason of the rows depends on it.
    queue: dict[str, Any]
    method: dict[str, str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "measured_at": _iso(self.measured_at),
            "items": self.items,
            "total": self.total,
            "page": self.page,
            "per_page": self.per_page,
            "pages": self.pages,
            "has_more": self.has_more,
            "filters": self.filters,
            "order": self.order,
            "queue": self.queue,
            "method": self.method,
        }


@dataclass(frozen=True)
class JobDetail:
    """One job: its header, each of its streams with its coverage and its
    checkpoint, how much of the window is verified, and its counters."""

    measured_at: datetime
    job: dict[str, Any]
    streams: dict[str, Any]
    verified: dict[str, Any]
    counters: dict[str, Any]
    method: dict[str, str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "measured_at": _iso(self.measured_at),
            "job": self.job,
            "streams": self.streams,
            "verified": self.verified,
            "counters": self.counters,
            "method": self.method,
        }


# -- the whole of it ---------------------------------------------------------------


def overview(engine: Engine, *, now: datetime) -> Overview:
    """Every account, and every market, that needs history, by the state of
    its import — a subject without a job among them — with why each waiting
    job waits, how many really finished the window that was available to
    them and what was not available; the bottom of the pool as its last
    check saw it, the windows the jobs are asked for, and the worker's last
    word of itself. Nothing is asked of any server."""
    pause = _pause(engine, now)
    with _reading(engine) as session:
        kinds = {kind: _subjects(session, kind, pause["paused"]) for kind in STREAMS}
        pool = _pool(session)
        windows = {kind: _window(session, kind, pool["elder_ledger"]) for kind in STREAMS}
        worker = _worker(session, now)
    return Overview(
        measured_at=now,
        accounts=kinds["account"],
        markets=kinds["market"],
        queue={**pause, "worker": worker},
        pool=pool,
        windows=windows,
        method=METHODS,
    )


def _pause(engine: Engine, now: datetime) -> dict[str, Any]:
    """The pause of the whole queue as the pages say it."""
    kept = history_jobs.pause_state(engine)
    at = _read(kept["at"])
    return {
        **kept,
        # For how long, by the clock of the page: ahead of it is nothing.
        "paused_for_seconds": max(0.0, -_seconds(at, now)) if kept["paused"] and at is not None else None,
    }


def _subjects(session: Session, kind: str, queue_paused: bool) -> dict[str, Any]:
    """Every subject of the kind that needs history, by what its import came
    to. One row per subject: its job's, or none."""
    facts = (
        HistoryJob.id,
        HistoryJob.state,
        HistoryJob.boundary_final,
        HistoryJob.last_error,
        HistoryJob.last_slice_outcome,
        HistoryJob.paused_because,
        HistoryJob.result["unavailable_ledgers"].as_integer(),
    )
    if kind == "account":
        # The subject is the row; its job, if any, hangs on it.
        found = (
            select(*facts)
            .select_from(TrackedAccount)
            .outerjoin(HistoryJob, (HistoryJob.kind == "account") & (HistoryJob.subject == TrackedAccount.account_id))
            .where(TrackedAccount.enabled.is_(True))
        )
    else:
        key = _market_key()
        found = (
            select(*facts)
            .select_from(TrackedMarket)
            .outerjoin(HistoryJob, (HistoryJob.kind == "market") & (HistoryJob.subject == key))
            .where(TrackedMarket.enabled.is_(True))
        )
    said: dict[str, Any] = {
        "subjects": 0,
        "with_job": 0,
        "no_job": 0,
        "states": dict.fromkeys(history_jobs.STATES, 0),
        "waiting_reasons": dict.fromkeys(WAITING_REASONS, 0),
        "paused_reasons": {},
        "boundary_not_final": {"total": 0, "by_state": {}},
        "unavailable": {"subjects": 0, "ledgers": 0},
    }
    done = with_unavailable = 0
    for job_id, state, final, error, outcome, because, unavailable in session.execute(found):
        said["subjects"] += 1
        if job_id is None:
            said["no_job"] += 1
            continue
        said["with_job"] += 1
        said["states"][state] += 1
        if not final:
            not_final = said["boundary_not_final"]
            not_final["total"] += 1
            not_final["by_state"][state] = not_final["by_state"].get(state, 0) + 1
        if unavailable:
            said["unavailable"]["subjects"] += 1
            said["unavailable"]["ledgers"] += unavailable
        if state == history_jobs.WAITING:
            said["waiting_reasons"][GLOBAL_PAUSE if queue_paused else _why_waiting(error, outcome)] += 1
        elif state == history_jobs.PAUSED:
            reason = because or "no reason kept"
            said["paused_reasons"][reason] = said["paused_reasons"].get(reason, 0) + 1
        elif state == history_jobs.DONE:
            done += 1
            with_unavailable += bool(unavailable)
    said["finished"] = {
        "window_available": done,
        "nothing_unavailable": done - with_unavailable,
        "with_unavailable": with_unavailable,
    }
    said["completion"] = _share(done, said["subjects"])
    said["completion_of_jobs"] = _share(done, said["with_job"])
    said["not_counted"] = _not_counted(session, kind)
    return said


def _market_key() -> Any:
    """The key of a market row, as the jobs name it: the pair, lower asset first."""
    return TrackedMarket.base_asset_id.concat(">").concat(TrackedMarket.counter_asset_id)


def _not_counted(session: Session, kind: str) -> dict[str, int]:
    """The jobs of the kind whose subject is switched off or gone from the
    watch list, by state: not subjects that need history, and not counted."""
    if kind == "account":
        watched = select(TrackedAccount.account_id).where(TrackedAccount.enabled.is_(True))
    else:
        watched = select(_market_key()).where(TrackedMarket.enabled.is_(True))
    found = dict.fromkeys(history_jobs.STATES, 0)
    for state, number in session.execute(
        select(HistoryJob.state, func.count())
        .where(HistoryJob.kind == kind, HistoryJob.subject.not_in(watched))
        .group_by(HistoryJob.state)
    ):
        found[state] = number
    return found


def _share(part: int, whole: int) -> dict[str, Any]:
    """A share that says what was divided by what. None when there is nothing
    to divide by: no subject, no window."""
    return {"completed": part, "planned": whole, "share": part / whole if whole else None}


def _pool(session: Session) -> dict[str, Any]:
    """What the last check found of each server of the pool. Nothing is asked
    of a server: this is as of that check, and says when it was."""
    servers = [
        {
            "url": row.url,
            "position": row.position,
            "available": row.available,
            "elder_ledger": row.elder_ledger,
            "latest_ledger": row.latest_ledger,
            "lag_ledgers": row.lag_ledgers,
            "checked_at": _iso(row.checked_at),
            "last_error": row.last_error,
            "cooling_until": _iso(row.cooling_until),
        }
        for row in session.scalars(select(HorizonNodeState).order_by(HorizonNodeState.position, HorizonNodeState.url))
    ]
    elders = [server["elder_ledger"] for server in servers if server["elder_ledger"] is not None]
    latest = [server["latest_ledger"] for server in servers if server["latest_ledger"] is not None]
    checked = [moment for moment in (_read(server["checked_at"]) for server in servers) if moment is not None]
    return {
        "servers": servers,
        "elder_ledger": min(elders) if elders else None,
        "latest_ledger": max(latest) if latest else None,
        "checked_at": _iso(max(checked)) if checked else None,
        "unavailable": [server["url"] for server in servers if not server["available"]],
        # Servers that never said what they keep: they may keep older history
        # than anybody has heard of, and hold jobs that wait for them.
        "silent": [server["url"] for server in servers if server["elder_ledger"] is None],
        "note": (
            "as of the last check of each server: Horizon was not asked for this, and what a server keeps may "
            "have moved up since"
        ),
    }


def _window(session: Session, kind: str, elder: int | None) -> dict[str, Any]:
    """The window the jobs of a kind are asked for — at its widest — and
    where the history they verified begins."""
    asked = select(
        func.count(),
        func.min(HistoryJob.from_ledger),
        func.max(HistoryJob.to_ledger),
        func.count().filter(HistoryJob.from_ledger.is_(None)),
    ).where(HistoryJob.kind == kind)
    number, lowest, highest, as_far_back = session.execute(asked).one()
    verified_from = session.scalar(
        select(func.min(SyncCoverage.from_ledger)).where(
            SyncCoverage.subject_type == kind, SyncCoverage.state.in_(VERIFIED)
        )
    )
    return {
        "jobs": number,
        "lowest_from": lowest,
        "as_far_back_as_the_pool_keeps": as_far_back,
        "highest_to": highest,
        "verified_starts_at": verified_from,
        # How far the pool's bottom, as its last check saw it, has moved up
        # since the oldest ledger this installation verified. What is known
        # of the movement of retention, and no more than that.
        "elder_above_verified_start": None if elder is None or verified_from is None else elder - verified_from,
    }


def _worker(session: Session, now: datetime) -> dict[str, Any] | None:
    """The worker's last word of itself (``heartbeat.history``), and how old
    it is: an empty queue and a stopped service look the same in the jobs."""
    row = session.get(ServiceState, heartbeats.HISTORY)
    if row is None:
        return None
    age = heartbeats.age(dict(row.value), now)
    return {**row.value, "age_seconds": None if age is None else max(0.0, age.total_seconds())}


# -- the table of jobs ----------------------------------------------------------------


def jobs(
    engine: Engine,
    *,
    kind: str | None = None,
    state: str | None = None,
    q: str | None = None,
    page: int = 1,
    per_page: int = 50,
    now: datetime,
) -> Jobs:
    """One page of the table of jobs — at most ``per_page`` rows, never
    more — each with its window, what is left of each of its streams, where
    each got to and when that ledger closed, when it started and last moved,
    what it tries next and why it waits or failed, and what it read and
    added. ``kind`` and ``state`` filter; ``q`` finds a subject by part of
    it."""
    if kind is not None and kind not in STREAMS:
        raise ValueError(f"kind is one of {tuple(STREAMS)}, not {kind!r}")
    if state is not None and state not in history_jobs.STATES:
        raise ValueError(f"state is one of {history_jobs.STATES}, not {state!r}")
    per_page = max(1, min(MAX_PER_PAGE, per_page))
    page = max(1, page)
    chosen = select(HistoryJob).where(*_chosen(kind, state, q))
    pause = _pause(engine, now)
    with _reading(engine) as session:
        total = session.scalar(select(func.count()).select_from(chosen.subquery())) or 0
        pages = max(1, -(-total // per_page))
        found = session.scalars(
            chosen.order_by(
                case(ACTIVE_FIRST, value=HistoryJob.state, else_=len(ACTIVE_FIRST) + 1),
                HistoryJob.next_attempt_at.asc().nulls_last(),
                HistoryJob.last_slice_at.asc().nulls_first(),
                HistoryJob.id,
            )
            .limit(per_page)
            .offset((page - 1) * per_page)
        )
        items = _said(session, list(found), pause["paused"], now, whole=False)
    return Jobs(
        measured_at=now,
        items=items,
        total=total,
        page=page,
        per_page=per_page,
        pages=pages,
        has_more=page < pages,
        filters={"kind": kind, "state": state, "q": q},
        order=ORDER,
        queue=pause,
        method={"measured_at": METHODS["measured_at"], "share": SHARE_METHOD, "speed": METHODS["speed"]},
    )


def _chosen(kind: str | None, state: str | None, q: str | None) -> tuple[Any, ...]:
    """The conditions of the filters asked for."""
    conditions: tuple[Any, ...] = ()
    if kind is not None:
        conditions += (HistoryJob.kind == kind,)
    if state is not None:
        conditions += (HistoryJob.state == state,)
    if q:
        conditions += (HistoryJob.subject.icontains(q, autoescape=True),)
    return conditions


def job_detail(engine: Engine, job_id: int, *, now: datetime) -> JobDetail | None:
    """One job: each of its streams with every piece of its coverage and its
    gaps, its checkpoint and how much of the window is verified; what it
    read, what it added, how long it took and how fast. None when there is
    no such job.

    The share of a stream is unknown — None — while the window is not
    defined: a job asked as far back as the pool keeps does not know where
    the pool's history starts until a slice of it found it. Nothing here
    says when a job will be done, or how much is left to read in records.
    """
    pause = _pause(engine, now)
    with _reading(engine) as session:
        job = session.get(HistoryJob, job_id)
        if job is None:
            return None
        item = _said(session, [job], pause["paused"], now, whole=True)[0]
    return JobDetail(
        measured_at=now,
        job={key: value for key, value in item.items() if key not in ("streams", "verified", "counters")},
        streams=item["streams"],
        verified=item["verified"],
        counters=item["counters"],
        method={
            "measured_at": METHODS["measured_at"],
            "share": SHARE_METHOD,
            "verified": "the same, over the ledgers verified in every stream of the job",
            "window": (
                "from the ledger the job was asked for, or — as far back as the pool keeps — from where its "
                "first slice found the pool's history to begin, up to the job's boundary as it is now"
            ),
            "speed": METHODS["speed"],
            "counters": METHODS["counters"],
        },
    )


# -- what is said of each job --------------------------------------------------------


def _said(
    session: Session, jobs: Sequence[HistoryJob], queue_paused: bool, now: datetime, *, whole: bool
) -> list[dict[str, Any]]:
    """The jobs as the table shows them — with every piece of coverage when
    ``whole`` is asked, as the page of one job does. Coverage and
    checkpoints are read for all of them at once."""
    pieces = _pieces(session, jobs)
    cursors = checkpoints(
        session,
        SOURCE,
        [(stream.removesuffix("s"), "desc", job.subject) for job in jobs for stream in STREAMS[job.kind]],
    )
    return [_one(session, job, pieces, cursors, queue_paused, now, whole=whole) for job in jobs]


def _closed(session: Session, ledgers: Sequence[int]) -> dict[int, datetime]:
    """When each of these ledgers closed, for those a transaction of which
    is stored: a ledger nothing of which was stored has no row."""
    if not ledgers:
        return {}
    found = session.execute(
        select(HistoryLedger.sequence, HistoryLedger.close_time).where(HistoryLedger.sequence.in_(ledgers))
    )
    return dict(found.all())


def _pieces(session: Session, jobs: Sequence[HistoryJob]) -> dict[tuple[str, str, str], list[coverage.LedgerRange]]:
    """The coverage of the streams of these jobs, by (kind, subject, stream),
    lowest ledger first. One request, whichever kinds the jobs are of."""
    where: list[Any] = []
    for kind, names in STREAMS.items():
        subjects = [job.subject for job in jobs if job.kind == kind]
        if subjects:
            where.append(
                (SyncCoverage.subject_type == kind)
                & SyncCoverage.subject_key.in_(subjects)
                & SyncCoverage.data_type.in_(names)
                & (SyncCoverage.selection_policy == POLICY[kind])
                & (SyncCoverage.policy_version == POLICY_VERSIONS[POLICY[kind]])
            )
    found: dict[tuple[str, str, str], list[coverage.LedgerRange]] = {}
    if not where:
        return found
    chosen = select(
        SyncCoverage.subject_type,
        SyncCoverage.subject_key,
        SyncCoverage.data_type,
        SyncCoverage.from_ledger,
        SyncCoverage.to_ledger,
        SyncCoverage.state,
        SyncCoverage.detail,
        SyncCoverage.verified_at,
        SyncCoverage.served_by,
    ).where(or_(*where))  # what is of an account, or of a market
    for kind, subject, stream, low, high, state, detail, verified_at, served_by in session.execute(chosen):
        found.setdefault((kind, subject, stream), []).append(
            coverage.LedgerRange(low, high, state, detail, verified_at, served_by)
        )
    for ranges in found.values():
        ranges.sort(key=lambda row: row.from_ledger)
    return found


def _one(
    session: Session,
    job: HistoryJob,
    pieces: Mapping[tuple[str, str, str], list[coverage.LedgerRange]],
    cursors: Mapping[tuple[str, str, str], tuple[str | None, int | None]],
    queue_paused: bool,
    now: datetime,
    *,
    whole: bool,
) -> dict[str, Any]:
    """One job as the table shows it."""
    bottom, how = _bottom(job, pieces)
    streams: dict[str, Any] = {}
    for stream in STREAMS[job.kind]:
        said = _stream(
            job, stream, pieces.get((job.kind, job.subject, stream), ()), bottom, whole=whole
        )
        cursor, ledger = cursors.get((stream.removesuffix("s"), "desc", job.subject), (None, None))
        # Where the reading of the stream stands: at its checkpoint while it
        # has one; a finished stream has none, and stands where its verified
        # coverage ends, at the bottom.
        stood = ledger if ledger is not None else said["verified"]["from"]
        said["cursor"] = {
            "token": cursor,
            "at_ledger": stood,
            "ledger_time": None,
            "how": "its checkpoint" if ledger is not None else ("verified down to here" if stood is not None else None),
        }
        streams[stream] = said
    # The time of the ledger a stream stands at, when a transaction of it is
    # stored: one request for all of them.
    stood_at = [said["cursor"]["at_ledger"] for said in streams.values()]
    closed = _closed(session, [ledger for ledger in stood_at if ledger is not None])
    for said in streams.values():
        ledger = said["cursor"]["at_ledger"]
        said["cursor"]["ledger_time"] = _iso(closed[ledger]) if ledger in closed else None
    reached = [ledger for ledger in stood_at if ledger is not None]
    deepest = min(reached) if reached else None
    window = job.to_ledger - bottom + 1 if bottom is not None else None
    verified = _in_every([said["verified"]["spans"] for said in streams.values()]) if bottom is not None else None
    return {
        "id": job.id,
        "kind": job.kind,
        "subject": job.subject,
        "state": job.state,
        "priority": job.priority,
        "reason": _reason(job, queue_paused),
        "context": job.context,
        "boundary_final": job.boundary_final,
        "window": {"bottom": bottom, "top": job.to_ledger, "bottom_how": how, "read": (job.result or {}).get("window")},
        "streams": streams,
        "streams_left": [name for name, said in streams.items() if said["left"]] if bottom is not None else None,
        # The deepest ledger any stream got to, and when it closed.
        "reached_ledger": deepest,
        "reached_ledger_time": next(
            (said["cursor"]["ledger_time"] for said in streams.values() if said["cursor"]["at_ledger"] == deepest), None
        )
        if deepest is not None
        else None,
        "started_at": _iso(job.started_at),
        "last_progress_at": _iso(job.last_progress_at),
        "last_slice_at": _iso(job.last_slice_at),
        "finished_at": _iso(job.finished_at),
        # The next try is of a job that waits: nothing is next for the others.
        "next_attempt_at": _iso(job.next_attempt_at) if job.state == history_jobs.WAITING else None,
        "next_attempt_in_seconds": _seconds(job.next_attempt_at, now) if job.state == history_jobs.WAITING else None,
        "last_slice_outcome": job.last_slice_outcome,
        "last_error": job.last_error,
        "paused_because": job.paused_because,
        "leased_by": job.leased_by,
        "lease_expires_at": _iso(job.lease_expires_at),
        "attempts": job.attempts,
        "servers": (job.result or {}).get("servers") or {},
        "verified": {
            "verified": verified,
            "of": window,
            "share": verified / window if window else None,
            "unknown_because": None if bottom is not None else UNKNOWN_WINDOW,
            "method": "ledgers verified in every stream, over the ledgers of the job's window",
        },
        "counters": _counters(job),
    }


def _bottom(job: HistoryJob, pieces: Mapping[tuple[str, str, str], list[coverage.LedgerRange]]) -> tuple[int | None, str]:
    """The bottom of a job's window, and how it is known: the ledger asked
    for; or, as far back as the pool keeps, where its first slice found the
    pool's history to begin. None while no slice of it found that yet."""
    if job.from_ledger is not None:
        return job.from_ledger, "the ledger the job was asked for"
    lowest = [rows[0].from_ledger for stream in STREAMS[job.kind] if (rows := pieces.get((job.kind, job.subject, stream)))]
    if lowest:
        return min(lowest), "where the pool's history began when the job first read"
    return None, UNKNOWN_WINDOW


def _stream(
    job: HistoryJob, stream: str, rows: Sequence[coverage.LedgerRange], bottom: int | None, *, whole: bool
) -> dict[str, Any]:
    """One stream of a job over its window: what is verified, what is left,
    what cannot be verified — and, when ``whole``, every piece of its
    coverage and its gaps."""
    ledgers: dict[str, int] = dict.fromkeys(coverage.STATES, 0)
    pending: list[tuple[int, int]] = []
    unverifiable: list[tuple[int, int]] = []
    verified: list[tuple[int, int]] = []
    pieces = []
    planned_to: int | None = None
    for row in rows:
        if bottom is None:
            low, high = row.from_ledger, row.to_ledger
        else:
            low, high = max(row.from_ledger, bottom), min(row.to_ledger, job.to_ledger)
        pieces.append(
            {
                "from": row.from_ledger,
                "to": row.to_ledger,
                "state": row.state,
                "served_by": row.served_by,
                "verified_at": _iso(row.verified_at),
                "detail": row.detail,
                "in_window": [low, high] if bottom is not None and low <= high else None,
            }
        )
        planned_to = row.to_ledger if planned_to is None else max(planned_to, row.to_ledger)
        if bottom is None or low > high:
            continue
        ledgers[row.state] += high - low + 1
        if row.state == coverage.PENDING:
            pending.append((low, high))
        elif row.state == coverage.UNVERIFIABLE:
            unverifiable.append((low, high))
        elif row.state in VERIFIED:
            verified.append((low, high))
    if bottom is None:
        said: dict[str, Any] = {"left": None, "unverifiable_ledgers": None}
    else:
        # What no range speaks for yet — above what was planned, up to a
        # boundary raised since — is left to read as well.
        unplanned = max(0, job.to_ledger - planned_to) if planned_to is not None else job.to_ledger - bottom + 1
        said = {
            "left": ledgers[coverage.PENDING] + unplanned,
            "pending_ledgers": ledgers[coverage.PENDING],
            "unplanned_ledgers": unplanned,
            "unverifiable_ledgers": ledgers[coverage.UNVERIFIABLE],
        }
    said.update(
        {
            # The last confirmed range: where the verified coverage of the
            # stream in the window begins and ends; and its pieces, merged.
            "verified": {
                "from": verified[0][0] if verified else None,
                "to": verified[-1][1] if verified else None,
                "ledgers": ledgers[coverage.COMPLETE] + ledgers[coverage.EMPTY],
                "spans": _merged(verified),
            },
            "share": _stream_share(verified, bottom, job.to_ledger),
            "last_slice": ((job.result or {}).get("streams") or {}).get(f"{job.kind} {job.subject} / {stream}"),
        }
    )
    if whole:
        said["pieces"] = pieces
        said["ledgers"] = ledgers
        said["gaps"] = {"pending": _merged(pending), "unverifiable": _merged(unverifiable)}
    return said


def _stream_share(verified: Sequence[tuple[int, int]], bottom: int | None, top: int) -> dict[str, Any]:
    """The verified ledgers of a stream against the ledgers of its window."""
    if bottom is None:
        return {"verified": None, "of": None, "share": None, "unknown_because": UNKNOWN_WINDOW}
    whole = top - bottom + 1
    part = sum(high - low + 1 for low, high in verified)
    return {"verified": part, "of": whole, "share": part / whole if whole else None, "unknown_because": None}


def _in_every(groups: Sequence[Sequence[Sequence[int]]]) -> int:
    """How many ledgers lie in a span of every group: the ledgers all the
    streams of a job verified. Each group's spans do not overlap."""
    moments: list[tuple[int, int]] = []
    for group in groups:
        moments += [(low, 1) for low, high in group] + [(high + 1, -1) for low, high in group]
    moments.sort()
    depth = counted = started = 0
    for ledger, change in moments:
        if depth == len(groups):
            counted += ledger - started
        depth += change
        started = ledger
    return counted


def _merged(spans: Iterable[tuple[int, int]]) -> list[list[int]]:
    """Spans that touch or overlap made one, lowest first."""
    merged: list[list[int]] = []
    for low, high in sorted(spans):
        if merged and low <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], high)
        else:
            merged.append([low, high])
    return merged


def _counters(job: HistoryJob) -> dict[str, Any]:
    """What the job read and what it added — apart — how long it took, and
    how fast that was. No ETA: nothing here says what is left, in records."""
    inserted = _by_type(job.inserted)
    repeated = _by_type(job.repeated)
    minutes = job.time_spent_ms / 60_000
    return {
        "requests": job.requests,
        "pages": job.pages,
        "records": job.records,
        "inserted": inserted,
        "repeated": repeated,
        "slices": job.slices,
        "failed_slices": job.failed_slices,
        "time_spent_seconds": job.time_spent_ms / 1000,
        "records_per_minute": round(job.records / minutes, 1) if minutes else None,
        "pages_per_minute": round(job.pages / minutes, 1) if minutes else None,
        # Every record read is met once as stored, so the counts by type come
        # to at least the records — unless some were read before anything
        # counted what storing it did (stage 05).
        "counts_what_was_read": sum(inserted.values()) + sum(repeated.values()) >= job.records,
    }


def _by_type(counts: Mapping[str, Any] | None) -> dict[str, int]:
    return {data_type: int((counts or {}).get(data_type, 0) or 0) for data_type in DATA_TYPES}


def _reason(job: HistoryJob, queue_paused: bool) -> dict[str, str]:
    """Why a job is in its state: a word for a filter, and a sentence."""
    if job.state == history_jobs.RUNNING:
        return {"code": WORKING, "text": f"being read by {job.leased_by}" if job.leased_by else "being read"}
    if job.state == history_jobs.PAUSED:
        return {"code": PAUSED, "text": job.paused_because or "paused, no reason kept"}
    if job.state in history_jobs.GAVE_UP:
        return {"code": GAVE_UP, "text": job.last_error or "gave up, no reason kept"}
    if job.state == history_jobs.DONE:
        if not job.boundary_final:
            return {
                "code": NOT_FINAL,
                "text": (
                    f"every stream verified up to ledger {job.to_ledger}, a boundary that is not final: events "
                    "above it are not guaranteed, and the job has more to read when they are"
                ),
            }
        return {"code": FINISHED, "text": f"every stream verified up to ledger {job.to_ledger}"}
    why = _why_waiting(job.last_error, job.last_slice_outcome)
    if queue_paused:
        text = WAITING_REASONS[GLOBAL_PAUSE]
        if why == ITS_TURN:
            return {"code": GLOBAL_PAUSE, "text": text}
        return {"code": GLOBAL_PAUSE, "text": f"{text}; and before that: {WAITING_REASONS[why]}"}
    return {"code": why, "text": WAITING_REASONS[why]}


def _why_waiting(error: str | None, outcome: str | None) -> str:
    """Why a waiting job waits, from what its last slice said. One reason: the
    servers' state is told apart from a failure of the job."""
    error = error or ""
    if "did not say of itself just now" in error:
        return POOL_SILENT
    if "the pool has got only to ledger" in error:
        return POOL_BEHIND
    if "RateLimited:" in error:
        return RATE_LIMIT
    if "Unavailable:" in error:
        return SERVERS
    if error or outcome in (history_jobs.FAILED, history_jobs.ERROR):
        return BACKOFF
    return ITS_TURN


# -- small things ------------------------------------------------------------------


def _iso(moment: datetime | None) -> str | None:
    """A moment in UTC, as text; None as None."""
    if moment is None:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).isoformat(timespec="seconds")


def _read(moment: str | None) -> datetime | None:
    """A moment as ``_iso`` wrote it; None when it is nothing readable."""
    if not isinstance(moment, str):
        return None
    try:
        found = datetime.fromisoformat(moment)
    except ValueError:
        return None
    return found if found.tzinfo is not None else found.replace(tzinfo=UTC)


def _seconds(moment: datetime | None, now: datetime) -> float | None:
    """How far a moment is from ``now``, in seconds: ahead is positive."""
    if moment is None:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    return round((moment - now).total_seconds(), 1)
