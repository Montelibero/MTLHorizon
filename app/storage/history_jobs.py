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
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.storage.models import HistoryJob

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
# Slices that failed in a row before a subject is left alone, when none is given.
DEFAULT_MAX_ATTEMPTS = 10

_LOST_WORKER = "its worker did not finish the slice before the lease ran out"


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
    longest ago among those whose time has come. None when there is none.

    Leases that ran out are put back first. The job least recently served is
    first — one that was never served before all others — so that a subject
    with much history does not keep the others waiting: every subject gets a
    slice before any gets a second.
    """
    now = now or datetime.now(UTC)
    with Session(engine, expire_on_commit=False) as session, session.begin():
        _release_expired(session, now, max_attempts)
        job = session.scalar(
            select(HistoryJob)
            .where(HistoryJob.state == WAITING, HistoryJob.next_attempt_at <= now)
            .order_by(HistoryJob.last_slice_at.asc().nulls_first(), HistoryJob.id)
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


def _release_expired(session: Session, now: datetime, max_attempts: int) -> int:
    """Jobs whose worker did not come back before its lease ran out: back to
    the queue at once, the death counted as a failed slice — or, when the
    count is used up, ended as a job that kept failing is. What the worker
    stored before it died is in the coverage and needs nothing here."""
    released = 0
    for job in session.scalars(
        select(HistoryJob)
        .where(HistoryJob.state == RUNNING, HistoryJob.lease_expires_at < now)
        .with_for_update(skip_locked=True)
    ):
        job.leased_at, job.lease_expires_at, job.leased_by = None, None, None
        job.last_error, job.attempts = _LOST_WORKER, job.attempts + 1
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


# -- the end of a slice --------------------------------------------------------


def slice_done(
    engine: Engine,
    job: HistoryJob,
    *,
    outcome: str,
    error: str | None = None,
    result: dict[str, Any] | None = None,
    requests: int = 0,
    pages: int = 0,
    records: int = 0,
    delay: float = 0.0,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    now: datetime | None = None,
) -> str:
    """End the slice that was made of ``job`` (as ``take`` gave it) and
    return the state the job is in. The counters are added to the job's.

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
        current.pages += pages
        current.records += records
        if current.state != RUNNING or current.leased_by != job.leased_by or current.leased_at != job.leased_at:
            return current.state
        if outcome == FINISHED and result is not None and _outgrown(current, result["window"]):
            outcome = PROGRESS
        current.leased_at, current.lease_expires_at, current.leased_by = None, None, None
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
