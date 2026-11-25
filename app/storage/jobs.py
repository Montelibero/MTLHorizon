"""The queue of transactions to bring in, kept in PostgreSQL.

A worker takes a job under a lease: the job is ``running`` and nobody else
takes it until ``lease_expires_at``. The worker makes the lease longer while
it works; one that dies makes nothing, and whoever meets the job after its
lease ran out puts it back — a job is never lost with its worker. Two
workers on one database share the queue without stepping on each other:
each takes what it can lock, and skips what another holds.

A try that fails is tried again after a pause that grows with each try, up
to ``max_attempts``. Then the job stops trying and says what it has: some of
the transaction stored — ``partial`` — or nothing — ``dead``. Both wait for a
person (``python -m app.worker retry``) or for word of the transaction to
come again. Nothing is deleted.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select, text, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.storage.models import ProcessingJob

PENDING, RUNNING, DONE, PARTIAL, DEAD = "pending", "running", "done", "partial", "dead"
# Tries before a job gives up, when none is given.
DEFAULT_MAX_ATTEMPTS = 8
STATES = (PENDING, RUNNING, DONE, PARTIAL, DEAD)
# A job in one of these has given up and waits.
GAVE_UP = (PARTIAL, DEAD)

_STORED = text(
    "SELECT context_loaded FROM history_transactions WHERE transaction_hash = :hash OR inner_transaction_hash = :hash"
)


def take(
    engine: Engine,
    worker: str,
    *,
    lease_seconds: float,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    now: datetime | None = None,
) -> ProcessingJob | None:
    """Take the next job whose time has come, under a lease of ``lease_seconds``.

    Jobs whose lease ran out are put back first — while they have tries
    left; one that has used them ends as any job that failed does. None
    when there is nothing to take.
    """
    now = now or datetime.now(UTC)
    with Session(engine, expire_on_commit=False) as session, session.begin():
        release_expired(session, now, max_attempts=max_attempts)
        job = session.scalar(
            select(ProcessingJob)
            .where(ProcessingJob.state == PENDING, ProcessingJob.next_attempt_at <= now)
            .order_by(ProcessingJob.next_attempt_at, ProcessingJob.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if job is None:
            return None
        job.state, job.attempts = RUNNING, job.attempts + 1
        job.leased_at, job.lease_expires_at, job.leased_by = now, now + timedelta(seconds=lease_seconds), worker
        job.started_at = job.started_at or now
        job.last_error = None
        return job


def release_expired(session: Session, now: datetime, *, max_attempts: int = DEFAULT_MAX_ATTEMPTS) -> int:
    """Jobs whose worker did not come back before its lease ran out: back to
    the queue at once while they have tries left, or to the state of a job
    that has used them. Runs inside the caller's transaction."""
    expired = session.scalars(
        select(ProcessingJob).where(ProcessingJob.state == RUNNING, ProcessingJob.lease_expires_at < now).with_for_update(skip_locked=True)
    )
    released = 0
    for job in expired:
        stored = _stored(session, job.transaction_hash)
        again = 0 if job.attempts < max_attempts else None
        _after_failure(session, job, now, "its worker did not finish before the lease ran out", again, stored)
        released += 1
    return released


def renew(engine: Engine, job_id: int, worker: str, *, lease_seconds: float, now: datetime | None = None) -> bool:
    """The worker is alive and still on the job. False when the job is no
    longer its: it was put back or taken away, and the work is to stop."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        return renew_in(session, job_id, worker, lease_seconds=lease_seconds, now=now)


def renew_in(session: Session, job_id: int, worker: str, *, lease_seconds: float, now: datetime) -> bool:
    """The same, inside the caller's transaction — the one that stores a part
    of the transaction, so that a part is never stored for a job whose lease
    just ran out and that another worker may be on."""
    changed = session.execute(
        update(ProcessingJob)
        .where(
            ProcessingJob.id == job_id,
            ProcessingJob.state == RUNNING,
            ProcessingJob.leased_by == worker,
        )
        .values(lease_expires_at=now + timedelta(seconds=lease_seconds))
        .execution_options(synchronize_session=False)
    )
    return changed.rowcount == 1  # type: ignore[attr-defined]


def finish(session: Session, job_id: int, *, state: str, result: dict[str, Any] | None, error: str | None, now: datetime) -> None:
    """End a job that will not be tried again on its own: ``done``, or one
    that gave up. Runs inside the caller's transaction — the one that stores
    the last of the transaction, so that the records and the mark that says
    they are all there are written together or not at all."""
    session.execute(
        update(ProcessingJob)
        .where(ProcessingJob.id == job_id)
        .values(
            state=state,
            result=result,
            last_error=error,
            finished_at=now,
            lease_expires_at=None,
            leased_at=None,
            leased_by=None,
        )
        .execution_options(synchronize_session=False)
    )


def put_back(engine: Engine, job: ProcessingJob, worker: str, *, why: str, now: datetime | None = None) -> bool:
    """Give a job back untouched — the worker was stopped before it could do
    anything — as if the try had not been: to be taken again at once, and the
    try not counted. False when the job is no longer that worker's."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        changed = session.execute(
            update(ProcessingJob)
            .where(
                ProcessingJob.id == job.id,
                ProcessingJob.state == RUNNING,
                ProcessingJob.leased_by == worker,
            )
            .values(
                state=PENDING,
                attempts=ProcessingJob.attempts - 1,
                next_attempt_at=now,
                last_error=why,
                leased_at=None,
                lease_expires_at=None,
                leased_by=None,
            )
            .execution_options(synchronize_session=False)
        )
        return changed.rowcount == 1  # type: ignore[attr-defined]


def defer(
    engine: Engine, job: ProcessingJob, *, error: str, delay: float, now: datetime | None = None
) -> None:
    """Put a job back because no server could answer: that is the servers'
    doing, not the job's, so the try is not counted. It is asked again in
    ``delay`` seconds."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        current = session.scalar(select(ProcessingJob).where(ProcessingJob.id == job.id).with_for_update())
        assert current is not None
        current.state, current.next_attempt_at = PENDING, now + timedelta(seconds=delay)
        current.attempts = max(0, current.attempts - 1)
        current.last_error = error
        current.leased_at, current.lease_expires_at, current.leased_by = None, None, None


def failed(
    engine: Engine, job: ProcessingJob, *, error: str, delay: float | None, max_attempts: int, now: datetime | None = None
) -> str:
    """A try failed. Back to the queue with the next try in ``delay``
    seconds, or — when the tries are used up, or ``delay`` is None — to
    ``partial`` when some of the transaction is stored and to ``dead`` when
    none is. Returns the state the job is in."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        current = session.scalar(select(ProcessingJob).where(ProcessingJob.id == job.id).with_for_update())
        assert current is not None
        stored = _stored(session, current.transaction_hash)
        if delay is None or current.attempts >= max_attempts:
            _after_failure(session, current, now, error, None, stored)
            return current.state
        _after_failure(session, current, now, error, delay, stored)
        return current.state


def _after_failure(
    session: Session, job: ProcessingJob, now: datetime, error: str, delay: float | None, stored: frozenset[str]
) -> None:
    """Put a job back for another try, or end its tries, after a failure."""
    job.last_error = error
    if delay is not None:
        job.state, job.next_attempt_at = PENDING, now + timedelta(seconds=delay)
    elif stored:
        job.state, job.finished_at = PARTIAL, now
        job.result = {**_result(job.result), "missing": sorted(stored ^ frozenset(_STREAMS))}
    else:
        job.state, job.finished_at = DEAD, now
    job.leased_at, job.lease_expires_at, job.leased_by = None, None, None


def _result(result: dict[str, Any] | None) -> dict[str, Any]:
    return dict(result or {})


_STREAMS = ("operations", "effects", "trades")


def _stored(session: Session, transaction_hash: str) -> frozenset[str]:
    """The parts of the transaction's context that are stored."""
    row = session.execute(_STORED, {"hash": transaction_hash}).first()
    return frozenset(row[0]) if row is not None else frozenset()


def retry(engine: Engine, *, ids: Iterable[int] | None = None, now: datetime | None = None) -> int:
    """Take jobs that gave up back into the queue, now: all of them, or the
    given ones. Their tries start over. Returns how many."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        chosen = select(ProcessingJob.id).where(ProcessingJob.state.in_(GAVE_UP))
        if ids is not None:
            chosen = chosen.where(ProcessingJob.id.in_(list(ids)))
        changed = session.execute(
            update(ProcessingJob)
            .where(ProcessingJob.id.in_(chosen), ProcessingJob.state.in_(GAVE_UP))
            .values(state=PENDING, attempts=0, next_attempt_at=now, finished_at=None)
            .execution_options(synchronize_session=False)
        )
        return changed.rowcount  # type: ignore[attr-defined]


def counts(engine: Engine) -> dict[str, int]:
    """How many jobs are in each state."""
    with Session(engine) as session:
        found = dict.fromkeys(STATES, 0)
        for state, number in session.execute(select(ProcessingJob.state, func.count()).group_by(ProcessingJob.state)):
            found[state] = number
    return found


def get(engine: Engine, job_id: int) -> ProcessingJob | None:
    with Session(engine) as session:
        return session.get(ProcessingJob, job_id)
