"""The queue of history imports, kept in PostgreSQL.

One job is the whole history there is of one subject — an account's four
streams, or a market's trades — and it is not done in one go but in slices.
Every slice is one bounded run of the backfill over the same coverage and
checkpoints, so a job keeps no progress of its own: what a slice stored stays
stored whatever happens to the job afterwards, and the next slice goes on
from the coverage. A job has its boundaries, its lease and its counters.

A worker takes a job under a lease: the job is ``running`` and nobody else
takes it until ``lease_expires_at``. A worker that dies in a slice takes
nothing along — whoever meets the job after its lease ran out puts it back —
but it is counted as a failure all the same: a subject that keeps losing its
worker is to be looked at, not retried forever.

Failures are counted per slice, not per request, and only when a server
answered: a slice that could read nothing because no server could answer is
the servers' doing and is not counted. A slice that made progress starts the
count over — what is to be stopped is a range that never moves, not a
subject that moves slowly.

Nothing is deleted here. A job that gave up waits for a person
(``python -m app.history retry``); a job whose subject was switched off is
paused, and the history it imported stays.

What a job stored is not what it read. The rows a slice stored — inserted,
or found stored already — are counted by the transaction that stored the
page (``add_stored_in``), whoever holds the job by then; what was asked of
the servers is counted when the slice ends. A crash between the two cannot
make a job claim what is not stored. Only slices of a job count so: the
worker of events stores through the same rules and is told nothing.

Two pauses are told apart. ``pause`` stops jobs, each on its own, with the
reason kept on the job. ``pause_all`` stops the queue as a whole — one
record in ``service_state``, no job touched — and ``take`` gives nothing
until ``resume_all``. It says nothing of the worker of events, which takes
nothing from this queue, and it outlives every process.

Priority (``set_priority``) changes who goes next, never who goes at all.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import BigInteger, cast, func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.storage import service_state
from app.storage.locks import _key  # noqa: no private marker - the number of a name, shared
from app.storage.models import HistoryJob, ServiceState
from app.storage.upsert import DATA_TYPES, StoredCounts

WAITING, RUNNING, DONE, PARTIAL, ERROR, PAUSED = "waiting", "running", "done", "partial", "error", "paused"
STATES = (WAITING, RUNNING, DONE, PARTIAL, ERROR, PAUSED)
# A job in one of these gave up on its own, and waits to be retried.
GAVE_UP = (PARTIAL, ERROR)
# What is fetched for a record before it is stored. The database knows the same two.
PARENTS, FULL = "parents", "full"
CONTEXTS = (PARENTS, FULL)
KINDS = ("account", "market")
# How a slice ended (``slice_done``).
PROGRESS, FINISHED, FAILED, DEFERRED = "progress", "finished", "failed", "deferred"
OUTCOMES = (PROGRESS, FINISHED, FAILED, ERROR, DEFERRED, PAUSED)
# ...and how it is said to have ended when its worker never came back.
LOST = "lost"
# Slices that failed in a row before a subject is left alone, when none is given.
DEFAULT_MAX_ATTEMPTS = 10
# What one point of priority is worth in the queue (``take``).
PRIORITY_STEP = timedelta(minutes=10)
# When a job that was never served counts as served: before every job that
# was, whatever its priority. Tie-break is the id, as it always was.
NEVER_SERVED = datetime(2000, 1, 1, tzinfo=UTC)
# The pause of the whole queue, by name in ``service_state``.
QUEUE_PAUSED = "history_paused"

_LOST_WORKER = "its worker did not finish the slice before the lease ran out"
# Who may change the pause of the whole queue: one at a time, for one transaction.
_PAUSE_LOCK = text("SELECT pg_advisory_xact_lock(:key)")


# -- planning ------------------------------------------------------------------


def plan(
    engine: Engine,
    *,
    kind: str,
    subject: str,
    to_ledger: int,
    from_ledger: int | None = None,
    context: str = PARENTS,
    boundary_final: bool = False,
    resume: bool = False,
    now: datetime | None = None,
) -> tuple[HistoryJob, bool]:
    """Make sure the history of a subject is planned up to ``to_ledger``, and
    return its job and whether it was made now.

    A job that exists only has its boundaries widened, never narrowed: the
    upper one to the highest ledger asked for, the lower one to the lowest
    given (None — as far back as the pool keeps — is lower than any ledger).
    Its progress is not touched, since none is kept here. Only a job that had
    nothing left to do goes back to ``waiting``: one that was done, or had
    given up, whose range grew or whose boundary became final. Its tries
    start over then.

    A paused job stays paused, with its boundary widened all the same: a
    pause is a person's decision, or says the subject is switched off. Only
    the second is lifted here, with ``resume``, by whoever switched the
    subject on again.

    ``context`` is what a job made now is read with. One that exists keeps
    the context it was planned with: what is read for a subject is not to
    change under way, least of all because a later call said nothing.
    """
    if kind not in KINDS:
        raise ValueError(f"kind is one of {KINDS}, not {kind!r}")
    if context not in CONTEXTS:
        raise ValueError(f"context is one of {CONTEXTS}, not {context!r}")
    _check_range(to_ledger, from_ledger)
    now = now or datetime.now(UTC)
    with Session(engine, expire_on_commit=False) as session, session.begin():
        found = select(HistoryJob).where(HistoryJob.kind == kind, HistoryJob.subject == subject).with_for_update()
        # Asking first spares the id of a subject that is only planned again.
        made = session.scalar(found) is None and _insert(
            session, kind, subject, to_ledger, from_ledger, boundary_final, context, now
        )
        job = session.scalar(found)
        assert job is not None
        if made:
            return job, True
        if _widened(job, to_ledger, from_ledger, boundary_final) and job.state in (*GAVE_UP, DONE):
            _wake(job, now)
        elif job.state == PAUSED and resume and job.paused_because == _switched_off(kind):
            _wake(job, now)
        return job, False


def _insert(
    session: Session,
    kind: str,
    subject: str,
    to_ledger: int,
    from_ledger: int | None,
    boundary_final: bool,
    context: str,
    now: datetime,
) -> bool:
    """The row of a job that was not there. False when another call made it
    first, in the moment between asking and inserting: that one is waited
    for, and is seen by the asking that follows."""
    made = session.scalar(
        insert(HistoryJob.__table__)
        .values(
            kind=kind,
            subject=subject,
            state=WAITING,
            from_ledger=from_ledger,
            to_ledger=to_ledger,
            boundary_final=boundary_final,
            context=context,
            next_attempt_at=now,
        )
        .on_conflict_do_nothing(index_elements=["kind", "subject"])
        .returning(HistoryJob.id)
    )
    return made is not None


def extend_boundary(
    engine: Engine,
    job_id: int,
    *,
    to_ledger: int,
    boundary_final: bool = True,
    now: datetime | None = None,
) -> HistoryJob | None:
    """Raise the upper boundary of a job: events are guaranteed for its
    subject from ``to_ledger`` on, so the history below it is all that is
    left to read. A job that had nothing left to do goes back to
    ``waiting``, as it does when it is planned again. None when there is no
    such job."""
    _check_range(to_ledger, None)
    now = now or datetime.now(UTC)
    with Session(engine, expire_on_commit=False) as session, session.begin():
        job = session.get(HistoryJob, job_id, with_for_update=True)
        if job is None:
            return None
        if _widened(job, to_ledger, None, boundary_final) and job.state in (*GAVE_UP, DONE):
            _wake(job, now)
        return job


def _check_range(to_ledger: int, from_ledger: int | None) -> None:
    if to_ledger <= 0:
        raise ValueError("to_ledger is the upper boundary: a ledger above zero")
    if from_ledger is not None and from_ledger > to_ledger:
        raise ValueError(f"from_ledger {from_ledger} is above to_ledger {to_ledger}")


def _widened(job: HistoryJob, to_ledger: int, from_ledger: int | None, boundary_final: bool) -> bool:
    """Move the boundaries of ``job`` outwards where asked. Whether its range
    grew or its boundary became final.

    The boundary is final only as far as the caller vouches for: a job whose
    boundary was already higher than that keeps a range above it that nobody
    vouches for, and stays not final; and one that was final and is raised
    without being vouched for again is not final any more.
    """
    grew = False
    if to_ledger > job.to_ledger:
        job.to_ledger, job.boundary_final, grew = to_ledger, bool(boundary_final), True
    if job.from_ledger is not None and from_ledger is not None and from_ledger < job.from_ledger:
        job.from_ledger, grew = from_ledger, True
    if boundary_final and not job.boundary_final and job.to_ledger <= to_ledger:
        job.boundary_final, grew = True, True
    return grew


def _wake(job: HistoryJob, now: datetime) -> None:
    """A job back at the top of the queue, as if it had not started."""
    job.state, job.attempts, job.next_attempt_at = WAITING, 0, now
    job.finished_at, job.paused_because = None, None


def _switched_off(kind: str) -> str:
    """Why a job is paused when its subject was switched off: the account's,
    or the market's, doing — and the only pause that planning lifts."""
    return f"{kind} disabled"


def switched_off(kind: str) -> str:
    """The reason a job of a switched-off subject of ``kind`` is paused with —
    the one ``plan`` lifts with ``resume``."""
    return _switched_off(kind)


# -- the lease -----------------------------------------------------------------


def take(
    engine: Engine,
    worker: str,
    *,
    lease_seconds: float,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    now: datetime | None = None,
) -> HistoryJob | None:
    """Take, under a lease of ``lease_seconds``, the job that was served
    longest ago among those whose time has come. None when there is none —
    and while the whole queue is paused (``pause_all``), whoever asks.

    Leases that ran out are put back first. The job least recently served is
    first — one that was never served before all others — so that a subject
    with much history does not keep the others waiting: every subject gets a
    slice before any gets a second.

    Priority makes a job look served longer ago than it was: one of
    priority N as if N steps (``PRIORITY_STEP``) had passed since its last
    slice. So it goes first until the others have caught up with that lead
    — and each of them then gets a slice in turn, for whoever was really
    served longest ago goes next. No job is left out: one of priority N
    takes at most its lead from the others, and a job that waits longer
    than that goes before it. A job that was never served goes before every
    job that was, whatever anybody's priority.
    """
    now = now or datetime.now(UTC)
    with Session(engine, expire_on_commit=False) as session, session.begin():
        _release_expired(session, now, max_attempts)
        if paused_in(session):
            return None
        job = session.scalar(
            select(HistoryJob)
            .where(HistoryJob.state == WAITING, HistoryJob.next_attempt_at <= now)
            .order_by(_served_ago(), HistoryJob.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if job is None:
            return None
        job.state = RUNNING
        job.leased_at, job.lease_expires_at, job.leased_by = now, now + timedelta(seconds=lease_seconds), worker
        job.last_slice_at, job.slices = now, job.slices + 1
        job.started_at = job.started_at or now
        return job


def _served_ago() -> Any:
    """When a job was last served as the queue sees it, least recently served
    first. The priority is taken as a big number: a large one makes a job
    wait or go first for a long time, and is never an error."""
    lead = cast(HistoryJob.priority, BigInteger) * int(PRIORITY_STEP.total_seconds())
    return func.coalesce(HistoryJob.last_slice_at, NEVER_SERVED) - func.make_interval(0, 0, 0, 0, 0, 0, lead)


def _release_expired(session: Session, now: datetime, max_attempts: int) -> int:
    """Jobs whose worker did not come back before its lease ran out: back to
    the queue at once, the death counted as a failed slice — or, when the
    count is used up, ended as a job that keeps failing is. What the worker
    stored before it died is in the coverage, and was counted as it was
    stored; it needs nothing here."""
    released = 0
    for job in session.scalars(
        select(HistoryJob)
        .where(HistoryJob.state == RUNNING, HistoryJob.lease_expires_at < now)
        .with_for_update(skip_locked=True)
    ):
        job.leased_at, job.lease_expires_at, job.leased_by = None, None, None
        job.last_error, job.attempts, job.failed_slices, job.last_slice_outcome = (
            _LOST_WORKER,
            job.attempts + 1,
            job.failed_slices + 1,
            LOST,
        )
        if job.attempts >= max_attempts:
            job.state, job.finished_at = ERROR, now
        else:
            job.state, job.next_attempt_at = WAITING, now
        released += 1
    return released


def renew(engine: Engine, job_id: int, worker: str, *, lease_seconds: float, now: datetime | None = None) -> bool:
    """The worker is alive and still on the job. False when the job is no
    longer its: it was put back, paused or given away, and the work is to stop."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        return renew_in(session, job_id, worker, lease_seconds=lease_seconds, now=now)


def renew_in(session: Session, job_id: int, worker: str, *, lease_seconds: float, now: datetime) -> bool:
    """The same, inside the caller's transaction."""
    changed = session.execute(
        update(HistoryJob)
        .where(
            HistoryJob.id == job_id,
            HistoryJob.state == RUNNING,
            HistoryJob.leased_by == worker,
        )
        .values(lease_expires_at=now + timedelta(seconds=lease_seconds))
        .execution_options(synchronize_session=False)
    )
    return changed.rowcount == 1  # type: ignore[attr-defined]


# -- what a slice stored -------------------------------------------------------


def add_stored_in(session: Session, job_id: int, stored: StoredCounts, *, at: datetime) -> None:
    """Add what a stored page stored to the counters of its job: the page,
    the records in it, the rows inserted and the rows found stored already,
    and when it last moved forward. Runs inside the transaction that stored
    the page, and nowhere else: committed with the page, rolled back with
    it, so the counters cannot say what is not stored — not after a crash,
    not after a retry of the same page.

    Counted for the job whoever holds it by then. The page was read for its
    subject, and may land after the job was paused or taken away: a pause
    loses nothing, and neither does the count. The job's row is locked here
    for the rest of the transaction, after the writer's lock the page
    already holds; nothing takes the two the other way round.
    """
    job = session.get(HistoryJob, job_id, with_for_update=True)
    if job is None:
        return
    job.pages += stored.pages
    job.records += stored.records
    job.inserted = _plus(job.inserted, stored.inserted)
    job.repeated = _plus(job.repeated, stored.repeated)
    job.last_progress_at = at


def _plus(kept: Mapping[str, Any] | None, added: Mapping[str, int]) -> dict[str, int]:
    """A job's count by data type with a page's added: every data type named."""
    kept = kept or {}
    return {data_type: int(kept.get(data_type, 0)) + added.get(data_type, 0) for data_type in DATA_TYPES}


# -- the end of a slice --------------------------------------------------------


def slice_done(
    engine: Engine,
    job: HistoryJob,
    *,
    outcome: str,
    error: str | None = None,
    result: dict[str, Any] | None = None,
    requests: int = 0,
    seconds: float = 0.0,
    delay: float = 0.0,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    now: datetime | None = None,
) -> str:
    """End the slice that was made of ``job`` (as ``take`` gave it) and
    return the state the job is in. ``requests`` and ``seconds`` — the time
    the slice took — are added to the job's. What the slice stored was
    counted as it stored it (``add_stored_in``), and is not said here.

    * ``progress`` — the slice stopped at its limit and work remains: back
      to the queue at once, the failures forgotten.
    * ``finished`` — every stream is verified up to the boundary. Only up to
      the boundary as it is now: one that was raised while the slice ran
      leaves work, and the job goes back to the queue.
    * ``failed`` — some streams failed and others were read: counted, and
      tried again after ``delay``. After ``max_attempts`` such slices in a
      row the job is ``partial``.
    * ``error`` — nothing of the subject could be read: counted the same
      way; the job ends as ``error``.
    * ``deferred`` — no server could answer: the servers' state. Not
      counted; asked again after ``delay``.
    * ``paused`` — the subject was switched off meanwhile.

    A failed slice is counted as such for good (``failed_slices``), however
    the job's tries stand.

    The state is written only for a job that is still this slice's. One
    whose lease ran out and that was put back, or taken by another worker,
    is theirs to write now; the counters are the truth either way.
    """
    if outcome not in OUTCOMES:
        raise ValueError(f"outcome is one of {OUTCOMES}, not {outcome!r}")
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        current = session.scalar(select(HistoryJob).where(HistoryJob.id == job.id).with_for_update())
        assert current is not None, "a job is never deleted"
        current.requests += requests
        current.time_spent_ms += round(max(0.0, seconds) * 1000)
        if outcome in (FAILED, ERROR):
            current.failed_slices += 1
        if current.state != RUNNING or current.leased_by != job.leased_by or current.leased_at != job.leased_at:
            return current.state
        if outcome == FINISHED and result is not None and _outgrown(current, result["window"]):
            outcome = PROGRESS
        current.leased_at, current.lease_expires_at, current.leased_by = None, None, None
        current.last_slice_outcome = outcome
        if result is not None:
            current.result = result
        if outcome == PAUSED:
            current.state, current.paused_because = PAUSED, _switched_off(current.kind)
            current.last_error = error
        elif outcome == DEFERRED:
            current.state, current.next_attempt_at = WAITING, now + timedelta(seconds=delay)
            current.last_error = error
        elif outcome in (FAILED, ERROR):
            current.last_error, current.attempts = error, current.attempts + 1
            if current.attempts < max_attempts:
                current.state, current.next_attempt_at = WAITING, now + timedelta(seconds=delay)
            else:
                current.state, current.finished_at = (PARTIAL if outcome == FAILED else ERROR), now
        elif outcome == FINISHED:
            current.state, current.finished_at, current.last_error = DONE, now, None
        else:
            current.state, current.attempts, current.next_attempt_at, current.last_error = WAITING, 0, now, None
        return current.state


def _outgrown(job: HistoryJob, window: Any) -> bool:
    """Whether the boundaries of ``job`` moved beyond the window a slice
    read: what it verified is not all there is to read."""
    bottom, top = window
    return top < job.to_ledger or (job.from_ledger is not None and bottom > job.from_ledger)


# -- by hand -------------------------------------------------------------------


def pause(
    engine: Engine,
    *,
    ids: Iterable[int] | None = None,
    kind: str | None = None,
    subject: str | None = None,
    all: bool = False,
    because: str,
) -> int:
    """Stop working on jobs until they are resumed: all of them, or the given
    ones. A job being worked on stops at its next request — its worker finds
    the job gone from it and leaves the slice where it stands — and one that
    is paused already takes the reason given now. A job that is done has
    nothing to stop and is left alone. Returns how many jobs are paused."""
    with Session(engine) as session, session.begin():
        changed = session.execute(
            update(HistoryJob)
            .where(*_chosen(ids, kind, subject, all), HistoryJob.state != DONE)
            .values(
                state=PAUSED,
                paused_because=because,
                leased_at=None,
                lease_expires_at=None,
                leased_by=None,
            )
            .execution_options(synchronize_session=False)
        )
        return changed.rowcount  # type: ignore[attr-defined]


def resume(
    engine: Engine,
    *,
    ids: Iterable[int] | None = None,
    kind: str | None = None,
    subject: str | None = None,
    all: bool = False,
    now: datetime | None = None,
) -> int:
    """Paused jobs back into the queue, now, their tries starting over:
    all of them, or the given ones. Returns how many."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        changed = session.execute(
            update(HistoryJob)
            .where(*_chosen(ids, kind, subject, all), HistoryJob.state == PAUSED)
            .values(
                state=WAITING,
                attempts=0,
                next_attempt_at=now,
                finished_at=None,
                paused_because=None,
            )
            .execution_options(synchronize_session=False)
        )
        return changed.rowcount  # type: ignore[attr-defined]


def retry(engine: Engine, *, ids: Iterable[int] | None = None, now: datetime | None = None) -> int:
    """Take jobs that gave up back into the queue, now: the given ones, or
    all of them. Their tries start over; what they read is still read.
    Returns how many."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        changed = session.execute(
            update(HistoryJob)
            .where(*(HistoryJob.id.in_(tuple(ids)),) if ids is not None else (), HistoryJob.state.in_(GAVE_UP))
            .values(state=WAITING, attempts=0, next_attempt_at=now, finished_at=None)
            .execution_options(synchronize_session=False)
        )
        return changed.rowcount  # type: ignore[attr-defined]


def _chosen(
    ids: Iterable[int] | None, kind: str | None, subject: str | None, everything: bool
) -> tuple[Any, ...]:
    """The conditions naming the jobs asked for: none means every job."""
    if everything:
        return ()
    if ids is None and kind is None and subject is None:
        raise ValueError("say which jobs: ids, kind, subject or all")
    chosen: tuple[Any, ...] = ()
    if ids is not None:
        chosen += (HistoryJob.id.in_(tuple(ids)),)
    if kind is not None:
        chosen += (HistoryJob.kind == kind,)
    if subject is not None:
        chosen += (HistoryJob.subject == subject,)
    return chosen


def set_priority(
    engine: Engine,
    *,
    priority: int,
    ids: Iterable[int] | None = None,
    kind: str | None = None,
    subject: str | None = None,
) -> int:
    """Say of jobs how much sooner they are served than the others, in
    points (``take``: a point is ``PRIORITY_STEP`` of going first). The
    jobs named — by id, by account or by market — and 0 is the place they
    had. Nothing else of a job is touched: not its state, not its counters,
    not its place in line from its last slice. Returns how many jobs were
    changed."""
    with Session(engine) as session, session.begin():
        changed = session.execute(
            update(HistoryJob)
            .where(*_chosen(ids, kind, subject, False))
            .values(priority=priority)
            .execution_options(synchronize_session=False)
        )
        return changed.rowcount  # type: ignore[attr-defined]


# -- the pause of the whole queue ----------------------------------------------


def pause_all(engine: Engine, *, because: str, by: str, now: datetime | None = None) -> dict[str, Any]:
    """Stop the whole queue until ``resume_all``: from now on ``take`` gives
    nothing, whoever asks. Returns what is kept of it (``pause_state``).

    No job is touched. One that waited goes on waiting; one whose slice is
    running ends where it stands — its worker sees the pause before its next
    request — and what that slice stored is stored. The pause is one record
    in the database, so it outlives every process; and it is not ``pause``,
    which stops jobs one by one and says why on each. The worker of events
    takes nothing from this queue and is not stopped.
    """
    now = now or datetime.now(UTC)
    state = {"paused": True, "because": because, "by": by, "at": _moment(now)}
    with Session(engine) as session, session.begin():
        _one_pause_writer(session)
        state = _pause_record(state)
        _keep(session, state)
    return state


def resume_all(engine: Engine, *, by: str, now: datetime | None = None) -> dict[str, Any]:
    """The whole queue back to work: ``take`` gives jobs again. Returns what
    is kept of the pause afterwards (``pause_state``).

    No job is touched: what waited is taken in its turn, and a slice that
    was cut off is gone on with from where it stood. What the last pause
    said stays in the record — the pause is not history to be ashamed of —
    with ``paused`` false and who lifted it when.
    """
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        _one_pause_writer(session)
        state = {
            **_pause_record(service_state.get(session, QUEUE_PAUSED)),
            "paused": False,
            "resumed_by": by,
            "resumed_at": _moment(now),
        }
        _keep(session, state)
    return state


def pause_state(engine: Engine) -> dict[str, Any]:
    """Whether the whole queue is paused, and what is known of the last
    pause: ``paused``, always there; ``because``, ``by`` and ``at``, of the
    pause — None when there was none; ``resumed_by`` and ``resumed_at``,
    when it was lifted. Nothing is asked of any server."""
    with Session(engine) as session:
        return _pause_record(service_state.get(session, QUEUE_PAUSED))


def is_paused(engine: Engine) -> bool:
    """Whether the whole queue is paused."""
    with Session(engine) as session:
        return paused_in(session)


def paused_in(session: Session) -> bool:
    """The same, inside the caller's transaction."""
    return bool(service_state.get(session, QUEUE_PAUSED).get("paused"))


def _pause_record(kept: Mapping[str, Any]) -> dict[str, Any]:
    """The pause of the queue as it is said: every field there, what is not
    known as None. Nothing stored means: not paused, and nothing is known
    of a pause."""
    return {
        "paused": bool(kept.get("paused")),
        "because": kept.get("because"),
        "by": kept.get("by"),
        "at": kept.get("at"),
        "resumed_by": kept.get("resumed_by"),
        "resumed_at": kept.get("resumed_at"),
    }


def _moment(now: datetime) -> str:
    return now.astimezone(UTC).isoformat(timespec="milliseconds")


def _one_pause_writer(session: Session) -> None:
    """Who changes the pause reads the record and writes it: one at a time,
    or the reading of one could be written over the writing of another. The
    lock ends with the transaction."""
    session.execute(_PAUSE_LOCK, {"key": _key(QUEUE_PAUSED)})


def _keep(session: Session, state: Mapping[str, Any]) -> None:
    """The record of the pause, written over whatever is there — as
    ``service_state.write`` does, in the caller's transaction."""
    session.execute(
        insert(ServiceState)
        .values(name=QUEUE_PAUSED, value=dict(state))
        .on_conflict_do_update(index_elements=["name"], set_={"value": dict(state), "updated_at": func.now()})
        .execution_options(synchronize_session=False)
    )


# -- looking -------------------------------------------------------------------


# Named as the command line says it. Nothing in this module calls the builtin
# of the same name afterwards, which would be this function by then.
def list(engine: Engine, state: str | None = None) -> list[HistoryJob]:
    """Every job, oldest first; or those in one state."""
    chosen = select(HistoryJob)
    if state is not None:
        chosen = chosen.where(HistoryJob.state == state)
    with Session(engine) as session:
        return [*session.scalars(chosen.order_by(HistoryJob.id))]


def counts(engine: Engine) -> dict[str, int]:
    """How many jobs are in each state."""
    with Session(engine) as session:
        found = dict.fromkeys(STATES, 0)
        for state, number in session.execute(select(HistoryJob.state, func.count()).group_by(HistoryJob.state)):
            found[state] = number
    return found


def get(engine: Engine, job_id: int) -> HistoryJob | None:
    with Session(engine) as session:
        return session.get(HistoryJob, job_id)
