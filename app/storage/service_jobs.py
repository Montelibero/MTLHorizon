"""Tasks that take time, started from the admin UI (or a command) and run
by the worker's background thread: kept in PostgreSQL.

A task is put here by the request that asked for it — which is answered at
once — and taken from here by a runner under a lease. Its state is read from
here by whoever shows it. A runner that dies takes nothing along: whoever
meets the task after its lease ran out puts it back, until the tries are
used up.

Two tasks of one kind and one ``one_at_a_time`` key are never queued or
running at once (a partial unique index): ``enqueue`` refuses the second and
gives back the first.

``state`` is one of:

* ``queued`` — waiting to be taken;
* ``running`` — taken under a lease (``leased_by`` until ``lease_expires_at``);
* ``done`` — it ran to its end;
* ``failed`` — it could not be run, or its runner kept dying;
* ``cancelled`` — stopped before it ran, or while it ran (the runner is told
  so by :func:`cancel_requested` and ends where it stands).

Nothing is deleted here.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.storage.models import ServiceJob

QUEUED, RUNNING, DONE, FAILED, CANCELLED = "queued", "running", "done", "failed", "cancelled"
STATES = (QUEUED, RUNNING, DONE, FAILED, CANCELLED)
ACTIVE = (QUEUED, RUNNING)
# discovery | pool_check | verify | reconcile — as the table's constraint says.
KINDS = ("discovery", "pool_check", "verify", "reconcile")
# A runner that dies takes its task nowhere: whoever meets the task after
# its lease ran out puts it back, this many times at most.
DEFAULT_MAX_ATTEMPTS = 3
# Long enough for a task that makes no request for a while (a verify that
# compares for minutes); a long task has its lease renewed while it runs.
DEFAULT_LEASE = 900.0

_LOST_RUNNER = "its runner did not come back before the lease ran out"


# -- putting a task in the queue -----------------------------------------------


def enqueue(
    engine: Engine,
    kind: str,
    params: Mapping[str, Any],
    *,
    one_at_a_time: str | None = None,
    requested_by: str = "admin",
    now: datetime | None = None,
) -> tuple[ServiceJob, bool]:
    """Make sure a task of ``kind`` with ``params`` is queued, and return it
    with whether it was made now.

    With ``one_at_a_time`` there is at most one such task of this kind
    queued or running: a second request adds nothing and gives back the one
    that is there.
    """
    if kind not in KINDS:
        raise ValueError(f"kind is one of {KINDS}, not {kind!r}")
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        statement = (
            insert(ServiceJob)
            .values(kind=kind, params=dict(params), state=QUEUED, requested_by=requested_by, one_at_a_time=one_at_a_time, created_at=now)
            # The predicate in literals, as the index's is written: PostgreSQL
            # finds the index by matching its predicate to this one.
            .on_conflict_do_nothing(
                index_elements=["kind", "one_at_a_time"],
                index_where=text("state IN ('queued', 'running') AND one_at_a_time IS NOT NULL"),
            )
            .returning(ServiceJob.id)
        )
        made = session.execute(statement).scalar_one_or_none()
        if made is None:
            row = session.scalar(
                select(ServiceJob).where(
                    ServiceJob.kind == kind, ServiceJob.one_at_a_time == one_at_a_time, ServiceJob.state.in_(ACTIVE)
                )
            )
            if row is None:
                # It ended between the two statements: nothing is in the way now.
                return enqueue(engine, kind, params, one_at_a_time=one_at_a_time, requested_by=requested_by, now=now)
            return _copy(row), False
        return _copy(session.get(ServiceJob, made)), True


def _copy(row: ServiceJob) -> ServiceJob:
    """The row as plain data, alive after the session is gone."""
    return ServiceJob(**{column.name: getattr(row, column.name) for column in ServiceJob.__table__.columns})


# -- taking and holding a task -------------------------------------------------


def take(
    engine: Engine,
    runner: str,
    *,
    lease_seconds: float = DEFAULT_LEASE,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    now: datetime | None = None,
) -> ServiceJob | None:
    """Take, under a lease of ``lease_seconds``, the task that was queued
    first. None when there is none. Tasks whose lease ran out are put back
    first — or ended as failed when their tries are used up. ``attempts``
    of a task says how many times it was taken."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        _release_expired(session, now, max_attempts)
        job = session.scalar(
            select(ServiceJob)
            .where(ServiceJob.state == QUEUED)
            .order_by(ServiceJob.created_at, ServiceJob.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if job is None:
            return None
        job.state = RUNNING
        job.started_at, job.leased_by = job.started_at or now, runner
        job.lease_expires_at = now + timedelta(seconds=lease_seconds)
        job.attempts += 1  # what went wrong before is said until the task ends well
        return _copy(job)


def _release_expired(session: Session, now: datetime, max_attempts: int) -> int:
    """Tasks whose runner did not come back before the lease ran out: back to
    the queue, the death counted — or, when the count is used up, failed.
    What a task did while it ran stays done: a discovery's accounts are in
    the watch list, a page a pass read is kept."""
    released = 0
    for job in session.scalars(
        select(ServiceJob).where(ServiceJob.state == RUNNING, ServiceJob.lease_expires_at < now).with_for_update(skip_locked=True)
    ):
        job.leased_by, job.lease_expires_at = None, None
        job.last_error = _LOST_RUNNER
        if job.attempts >= max_attempts:  # attempts counts how many times it was taken
            job.state, job.finished_at = FAILED, now
        else:
            job.state = QUEUED
        released += 1
    return released


def renew(engine: Engine, job: ServiceJob, *, lease_seconds: float = DEFAULT_LEASE, now: datetime | None = None) -> bool:
    """The runner is alive and still on the task. False when the task is no
    longer its — its lease ran out and somebody put it back — and the work
    is to stop."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        changed = session.execute(
            update(ServiceJob)
            .where(
                ServiceJob.id == job.id,
                ServiceJob.state == RUNNING,
                ServiceJob.leased_by == job.leased_by,
                ServiceJob.started_at == job.started_at,
            )
            .values(lease_expires_at=now + timedelta(seconds=lease_seconds))
            .execution_options(synchronize_session=False)
        )
        return changed.rowcount == 1  # type: ignore[attr-defined]


def cancel_requested(engine: Engine, job_id: int) -> bool:
    """Whether somebody asked for the running task to stop."""
    with Session(engine) as session:
        wanted = session.scalar(select(ServiceJob.cancel_requested).where(ServiceJob.id == job_id))
    return bool(wanted)


def request_cancel(engine: Engine, job_id: int, *, now: datetime | None = None) -> str:
    """Stop the task: one that is queued is cancelled at once; one that is
    running is told to stop and ends where it stands. Returns the state the
    task is in now; a task that ended is left as it is."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        job = session.get(ServiceJob, job_id, with_for_update=True)
        if job is None:
            raise ValueError(f"no task {job_id}")
        if job.state == QUEUED:
            job.state, job.finished_at = CANCELLED, now
            job.last_error = "cancelled before it ran"
        elif job.state == RUNNING:
            job.cancel_requested = True
        return job.state


# -- what a runner says of its task -------------------------------------------


def progress(engine: Engine, job: ServiceJob, **fields: Any) -> bool:
    """Keep ``fields`` in the task's ``progress`` for whoever looks at it.
    False when the task is no longer this runner's: nothing is written."""
    with Session(engine) as session, session.begin():
        current = session.get(ServiceJob, job.id)
        if current is None or current.state != RUNNING or current.leased_by != job.leased_by or current.started_at != job.started_at:
            return False
        current.progress = {**(current.progress or {}), **fields}
        return True


def finish(
    engine: Engine,
    job: ServiceJob,
    *,
    result: Mapping[str, Any] | None = None,
    error: str | None = None,
    now: datetime | None = None,
) -> str:
    """End the task as this runner's: ``done`` with ``result``, or ``failed``
    with ``error``. A task that was cancelled while it ran ends cancelled
    whatever it came to. Returns the state the task is in — which is not
    this runner's to say any more when its lease ran out."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        current = session.get(ServiceJob, job.id)
        if current is None or current.state != RUNNING or current.leased_by != job.leased_by or current.started_at != job.started_at:
            return current.state if current is not None else FAILED
        current.finished_at, current.leased_by, current.lease_expires_at = now, None, None
        if result is not None:
            current.result = dict(result)
        current.last_error = error
        if current.cancel_requested:
            current.state = CANCELLED
            current.last_error = current.last_error or "stopped on request"
        else:
            current.state = FAILED if error else DONE
        return current.state


# -- reading -------------------------------------------------------------------


def get(engine: Engine, job_id: int) -> ServiceJob | None:
    """The task as plain data, or None."""
    with Session(engine) as session:
        row = session.get(ServiceJob, job_id)
        return None if row is None else _copy(row)


def recent(
    engine: Engine,
    *,
    states: tuple[str, ...] | None = None,
    kind: str | None = None,
    limit: int = 50,
) -> list[ServiceJob]:
    """The newest tasks first, at most ``limit`` of them, by state and kind
    when asked."""
    query = select(ServiceJob)
    if states:
        query = query.where(ServiceJob.state.in_(states))
    if kind is not None:
        query = query.where(ServiceJob.kind == kind)
    with Session(engine) as session:
        rows = session.scalars(query.order_by(ServiceJob.created_at.desc(), ServiceJob.id.desc()).limit(max(1, limit)))
        return [_copy(row) for row in rows]


def counts(engine: Engine) -> dict[str, int]:
    """How many tasks are in each state."""
    with Session(engine) as session:
        found = dict.fromkeys(STATES, 0)
        for state, number in session.execute(select(ServiceJob.state, func.count()).group_by(ServiceJob.state)):
            found[state] = number
        return found

