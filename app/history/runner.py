"""The worker of history imports: it takes the jobs of the queue and gives
each one slice after slice.

A slice is one bounded run of the backfill for one subject: a budget of
requests and seconds, kept before every page, so that a page once taken is
stored whole. What it stores is committed page by page with its coverage and
its checkpoint — and with the job's count of what the page stored, in the
same transaction — so a slice that is cut short, by its budget, by a signal,
by the pause of the whole queue, by the worker dying, loses nothing and
claims nothing, and the next slice goes on from there. The subject that
waited longest gets the next slice, a job of priority sooner: a week of
history on one account delays nobody.

The job's state is written in short transactions of their own, never in one
open while Horizon is asked. The lease is made longer before a request, from
the client's hook; a job that was paused, or taken away because its lease ran
out, is left where it stands at the next request — and so is a slice that is
on when the whole queue is paused.

What a slice could not read is told apart. A server that answered with an
error is the stream's failure and is counted; no server answering is the
pool's state, and is waited for. While no job is due nothing is asked of
anybody.

The worker says of itself, in ``service_state`` under ``heartbeat.history``,
that it is there and what it is doing: a dashboard tells a queue that is
empty from a service that is not running. It says so when it is news, and
at least every ``heartbeat_seconds`` while it runs; the time said is the
worker's own clock.

Every slice ends with one line in the log: the job, its subject, what became
of each stream, where each one got to, what it cost and how it ended. The
line is the same in the text and the JSON log; in JSON what a machine reads
is next to the message, under ``slice``. Nothing is logged per record.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.backfill.jobs import StreamJob, stream_jobs
from app.backfill.loader import CONTEXT_MODES, PARENTS, Backfill, Limits, RunStopped, StreamReport, Window
from app.cli_common import LOGICAL_SOURCE
from app.sources.horizon import HorizonClient, RateLimited, Scope, Unavailable
from app.storage import coverage, heartbeats, history_jobs, retry
from app.storage.models import HistoryJob, TrackedAccount
from app.storage.upsert import StoredCounts, checkpoints

logger = logging.getLogger(__name__)

# One slice, and how often to look whether one is due.
DEFAULT_SLICE_REQUESTS = 50
DEFAULT_SLICE_SECONDS = 60.0
DEFAULT_POLL = 5.0
# Slices that failed in a row before a job is left alone; how long the first
# wait after a failure is, and the longest.
DEFAULT_MAX_ATTEMPTS = 10
DEFAULT_RETRY_BASE = 30.0
DEFAULT_RETRY_CAP = 3600.0
# A slice is bounded well below this. The lease is made longer while the slice
# runs, and is longer than any one request can take, so that a worker that
# is alive keeps its job and one that died is noticed in time.
DEFAULT_LEASE = 300.0
# How long a slice that could not be answered by any server waits before it
# is tried again. A server that failed is left alone for a cooldown of a
# minute or so (and longer each time), so asking again two minutes later
# finds it back; an outage costs minutes, not the hour a failure may cost.
DEFAULT_DEFER = 120.0
# A slice the database aborted (a deadlock's victim: ``app.storage.retry``)
# is asked again this soon: the other transaction is gone by then.
ABORTED_RETRY = 2.0
# How old the worker's word of itself (the heartbeat) may grow before it is
# said again while the service runs.
DEFAULT_HEARTBEAT = 15.0

# What the reader says of a stream (``StreamReport.status``).
VERIFIED, FAILED, STOPPED = "verified", "failed", "stopped"
# ...and of a slice that was on when the whole queue was paused. The reader
# takes it as its own stop, and ends the stream where it stands.
LEFT_WHERE_IT_STANDS = "the history queue is paused: the slice is left where it stands"
# ...and of a stream it could not read because no server could answer it.
SERVERS_FAULT = ("Unavailable:", "RateLimited:")

# What the heartbeat says the worker is: on nothing, on a job, or kept from
# the queue by the pause of the whole queue.
IDLE, WORKING, HELD = "idle", "working", "paused"
# ...and what a slice is called whose job stopped being this worker's.
LOST = "lost"
LOST_TEXT = "the job is no longer this worker's: the slice is left where it stands"
# The streams of a job, by its kind.
STREAMS = {"account": ("transactions", "operations", "effects", "trades"), "market": ("trades",)}


class JobLost(RunStopped):
    """The job is no longer this worker's: it was paused, put back or taken
    away while the worker was on it. The slice ends where it stands; what it
    stored is kept, and the job's state is left to whoever holds the job.

    A stop for the reader (``RunStopped``): no stream of the subject is
    marked failed on its account, and nothing more is asked for it.
    """


class Stopped(RunStopped):
    """A signal asked the worker to stop. The slice ends where it stands, and
    is not a failure."""


class Paused(RunStopped):
    """The whole queue was paused (``history_jobs.pause_all``) while the slice
    was on. The slice ends where it stands and is not a failure; the job is
    not paused — the queue is — and goes back to waiting for the resume.

    A stop for the reader, like a signal: no stream of the subject is marked
    failed on its account, and nothing more is asked for it.
    """


def worker_name() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{os.urandom(4).hex()}"


@dataclass
class SliceLimits(Limits):
    """What one slice may do before it stops where it stands.

    The budget is kept where stopping keeps what was done: before a page.
    A page that is taken is read with its context and stored whole, whatever
    it costs on top of the budget. A page cut off in the middle of its
    context stores nothing, and the next slice would take the same page into
    the same budget: a page costlier than the budget would hold its subject
    still for good, every slice spending its requests to no end. So a slice
    costs at most its budget and the context of one page.

    A stop is obeyed before every request: the slice ends there, and the
    page it was on is taken again by the next slice.

    The budget counts what the reader asks for. What the pool says of itself
    to find the window is asked before the reader starts, and is not part
    of it: a pool of many servers would else spend a small budget before
    the first page is taken, slice after slice, and the subject would never
    move.
    """

    def check(self) -> None:
        """Before a request. Only a stop: the reader sets it when a stream
        was stopped, and then nothing more is asked for the slice."""
        if self.stop.is_set():
            raise RunStopped("stopped on request")

    def before_page(self) -> None:
        """Before a page: the whole budget."""
        Limits.check(self)
        if self.max_pages is not None and self.pages >= self.max_pages:
            raise RunStopped("page limit reached")


@dataclass(frozen=True)
class Slice:
    """How the last slice went: for the command line to say."""

    job_id: int
    kind: str
    subject: str
    # progress | finished | failed | error | deferred | paused — or ``LOST``:
    # the job stopped being this worker's in the middle of the slice.
    outcome: str
    # The state the job is in now.
    state: str
    requests: int
    error: str | None
    # What the slice's committed pages stored, by data type, and how long the
    # slice took from its beginning to its end.
    stored: StoredCounts = field(default_factory=StoredCounts)
    seconds: float = 0.0


class HistoryWorker:
    """Works on the queue of history imports, one slice at a time.

    ``client`` is the pool of Horizon servers, with the pace every process
    shares; a worker is expected to come with the ``background`` share: the
    worker of events has the right of way.

    A slice is read with the context its job was planned with. ``context`` is
    what a job is read with that says none.

    ``heartbeat_seconds`` is how old the worker's word of itself
    (``heartbeat.history``) may grow while it runs. ``clock`` is what time it
    is for the queue and for that word; ``now`` in tests may be moved by hand.
    """

    def __init__(
        self,
        client: HorizonClient,
        engine: Engine,
        *,
        name: str | None = None,
        slice_requests: int = DEFAULT_SLICE_REQUESTS,
        slice_seconds: float = DEFAULT_SLICE_SECONDS,
        context: str = PARENTS,
        page_size: int = 200,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        retry_base: float = DEFAULT_RETRY_BASE,
        retry_cap: float = DEFAULT_RETRY_CAP,
        lease_seconds: float = DEFAULT_LEASE,
        heartbeat_seconds: float = DEFAULT_HEARTBEAT,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if context not in CONTEXT_MODES:
            raise ValueError(f"unknown context mode {context!r}")
        self.client = client
        self.engine = engine
        self.name = name or worker_name()
        # What the time is, for the queue; the real one.
        self.clock = clock or (lambda: datetime.now(UTC))
        self.slice_requests = slice_requests
        self.slice_seconds = slice_seconds
        self.context = context
        self.page_size = page_size
        self.max_attempts = max_attempts
        self.retry_base = retry_base
        self.retry_cap = retry_cap
        self.lease_seconds = lease_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self._stop: threading.Event | None = None
        self._job: HistoryJob | None = None
        self._limits: Limits | None = None
        self._first = 0
        # Where the reading begins: what was asked before it found the window.
        self._asked = 0
        # What each server had answered when the slice began.
        self._served_by: dict[str, int] = {}
        self._started = 0.0
        self._last_renewal = 0.0
        self._lost = False
        # Whether the slice that is on was ended by the pause of the whole queue.
        self._paused = False
        # What the slice that is on — or the one that just ended — stored.
        self._stored = StoredCounts()
        # What was last said of the worker (the state and the job), and when.
        self._said: tuple[str, int | None] | None = None
        self._said_at: datetime | None = None
        # For the log: slices that finished a job, and slices that failed.
        self.done = 0
        self.failed = 0
        self.last: Slice | None = None

    def close(self) -> None:
        """Nothing is held between slices: the reader of a slice opens and
        closes its own connections."""

    def __enter__(self) -> "HistoryWorker":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- running -------------------------------------------------------------

    def run(self, stop: threading.Event, *, poll_seconds: float = DEFAULT_POLL) -> None:
        """Give slices to the jobs of the queue until ``stop`` is set; wait
        ``poll_seconds`` between two looks when nothing is due."""
        self._stop = stop
        while not stop.is_set():
            if self.once(stop=stop) is False:
                stop.wait(poll_seconds)

    def once(self, *, stop: threading.Event | None = None) -> bool:
        """Take the job that waited longest, if one is due, and give it a
        slice. False when there was none — the queue is empty, nothing is
        due, or the whole queue is paused; the queue is not waited on here."""
        self._stop, self.last = stop, None
        job = history_jobs.take(
            self.engine,
            self.name,
            lease_seconds=self.lease_seconds,
            max_attempts=self.max_attempts,
            now=self.clock(),
        )
        if job is None:
            # The queue may have been paused or gone again since the last look.
            self._say(self._resting(), None)
            return False
        self._say(WORKING, job.id)
        try:
            self.last = self._slice(job)
        except JobLost:
            # Nothing of the job is written: it is somebody else's now.
            self.failed += 1
            state = history_jobs.get(self.engine, job.id)
            said = state.state if state else "?"
            self.last = Slice(
                job.id, job.kind, job.subject, LOST, said, self._spent(), LOST_TEXT, self._stored, self._gone()
            )
            self._tell(job, self.last, None, None)
        self._say(self._resting(), None)
        return True

    def _slice(self, job: HistoryJob) -> Slice:
        """Give ``job`` one slice, and say how it went — whatever happened
        inside it."""
        self._job, self._started, self._lost = job, time.monotonic(), False
        self._paused = False
        self._stored = StoredCounts()
        self._first = self.client.stats.requests
        self._asked = self._first
        self._served_by = dict(self.client.stats.by_server)
        self._last_renewal = self._started
        # The limits get a stop of their own. The reader sets it when one is
        # reached, and that must end the slice, not the service: the event
        # that ends the service is looked at before every request instead.
        self._limits = SliceLimits(
            requests=lambda: self.client.stats.requests - self._asked,
            max_requests=self.slice_requests,
            max_seconds=self.slice_seconds,
            started=self._started,
        )
        given = self.client.before_request
        try:
            self.client.before_request = self._before_request
            return self._read(job)
        except JobLost:
            raise
        except Paused as pause:
            # The whole queue was paused while the slice was on: it ends
            # where it stands, as on a signal, and is gone on with.
            return self._end(job, history_jobs.PROGRESS, str(pause), None)
        except RunStopped:
            # A signal, or the slice's time limit before its first page: the
            # slice ends where it stands, and is gone on with.
            return self._end(job, history_jobs.PROGRESS, None, None)
        except (Unavailable, RateLimited) as failure:
            # No server could answer, or the server asks to wait longer than
            # a request waits: the servers' state, not the job's failure. The
            # slice is not counted, and the job waits for them to return.
            return self._end(job, history_jobs.DEFERRED, f"{type(failure).__name__}: {failure}", None)
        except Exception as failure:  # noqa: BLE001 - one slice that failed: the job says why and is tried again later
            code = retry.contention(failure)
            if code is not None and job.attempts < self.max_attempts:
                # The database chose this slice as the victim of a deadlock, or
                # could not serialize it — the writer of history is the one
                # aborted in a cycle, for it waits first. Not the subject's
                # failure: what the slice stored is stored, and it is asked
                # again soon. It counts as a try, so that it cannot go on.
                return self._end(
                    job,
                    history_jobs.ERROR,
                    f"the database aborted the slice ({retry.said(code)}): asked again in a moment",
                    None,
                    again_in=ABORTED_RETRY,
                )
            return self._end(job, history_jobs.ERROR, f"{type(failure).__name__}: {failure}", None)
        finally:
            self.client.before_request = given
            self._job, self._limits = None, None

    def _read(self, job: HistoryJob) -> Slice:
        """One slice of ``job``: its streams, its window, and what the
        backfill made of them."""
        with Session(self.engine) as session:
            streams = [stream for stream in stream_jobs(session)[0] if _belongs(stream, job)]
            switched_off = job.kind == "account" and not _enabled(session, job.subject)
        if switched_off or not streams:
            # The account, or the market, was switched off meanwhile: nothing
            # more is read, and nothing that was read is deleted.
            return self._end(job, history_jobs.PAUSED, f"{job.kind} disabled: its history is not read", None)

        # What this slice is about: a server that keeps none of it, or has
        # not got as far as its top, is not asked. Of all the history there
        # is, the bottom is wherever the pool's history ends.
        self.client.scope = Scope(job.from_ledger, job.to_ledger)
        if job.from_ledger is None:
            history = self.client.history()
            if job.to_ledger > history.latest:
                return self._end(
                    job,
                    history_jobs.DEFERRED,
                    f"the pool has got only to ledger {history.latest}, and the boundary is ledger {job.to_ledger}",
                    None,
                )
            window = Window(min(history.elder, job.to_ledger), job.to_ledger)
            # Where the window begins is where the servers that answered keep
            # history from. One that did not answer just now may keep more,
            # and the pool's history may not end there: the job is not
            # finished on this window's word while one is silent.
            silent = self.client.silent()
        else:
            window = Window(job.from_ledger, job.to_ledger)
            silent: list[str] = []
        # What the pool said of itself found the window; it is not the
        # reading. The budget is for the pages, and would else be spent by a
        # pool of many servers before the first page is taken.
        self._asked = self.client.stats.requests
        assert self._limits is not None
        reading: Backfill | None = None
        try:
            with Backfill(
                self.client,
                self.engine,
                source=LOGICAL_SOURCE,
                page_size=self.page_size,
                limits=self._limits,
                context=job.context or self.context,
                # What a page stored is the job's to count, and is counted in
                # the transaction that stored the page: the two cannot part.
                on_page_stored=lambda session, stored: history_jobs.add_stored_in(
                    session, job.id, stored, at=self.clock()
                ),
            ) as reading:
                # The reader puts the limits' check before every request; this
                # worker's goes first and ends with it.
                self.client.before_request = self._before_request
                reports = reading.run(window, streams, concurrency=1)
        finally:
            # What the slice committed is the slice's, however it ended.
            if reading is not None:
                self._stored.add(reading.stored)
        if self._lost:
            raise JobLost()
        outcome, error = _outcome(reports)
        if outcome == history_jobs.PROGRESS and self._paused:
            # The reader took the pause as its stop; the slice is still to be
            # said of as ended by the pause, not by its budget.
            error = LEFT_WHERE_IT_STANDS
        if outcome == history_jobs.FINISHED and silent:
            outcome = history_jobs.DEFERRED
            error = (
                f"ledgers below {window.from_ledger} are not read: {', '.join(silent)} did not say of itself just now "
                "and may keep them"
            )
        result = _result(
            self.client,
            self.engine,
            streams,
            window,
            reports,
            self._served_by,
            (job.result or {}).get("servers"),
        )
        return self._end(job, outcome, error, result)

    def _end(
        self,
        job: HistoryJob,
        outcome: str,
        error: str | None,
        result: dict[str, Any] | None,
        *,
        again_in: float | None = None,
    ) -> Slice:
        """Write how the slice ended and what it asked of the servers, count
        it, and say so. ``again_in``: when the next try is due, when the
        caller knows better than the failure's growing pause."""
        if again_in is not None:
            delay = again_in
        elif outcome == history_jobs.DEFERRED:
            delay = min(self.retry_cap, DEFAULT_DEFER)
        else:
            delay = min(self.retry_base * 2 ** min(job.attempts, 16), self.retry_cap)
        state = history_jobs.slice_done(
            self.engine,
            job,
            outcome=outcome,
            error=error,
            result=result,
            requests=self._spent(),
            seconds=self._gone(),
            delay=delay,
            max_attempts=self.max_attempts,
            now=self.clock(),
        )
        if state == history_jobs.DONE:
            self.done += 1
        elif outcome in (history_jobs.FAILED, history_jobs.ERROR):
            self.failed += 1
        waits = (history_jobs.FAILED, history_jobs.ERROR, history_jobs.DEFERRED)
        again = delay if outcome in waits and state == history_jobs.WAITING else None
        last = Slice(job.id, job.kind, job.subject, outcome, state, self._spent(), error, self._stored, self._gone())
        self._tell(job, last, again, result)
        return last

    def _tell(self, job: HistoryJob, last: Slice, again_in: float | None, result: dict[str, Any] | None) -> None:
        """One line for a slice that ended: the job, what became of each of
        its streams, where each got to, what the slice stored and asked, how
        long it took and how it ended. The same line in the text and the JSON
        log; in JSON, the same as fields under ``slice``.

        Nothing of a record is in it, and no secret: the subject is a public
        address or a market, a cursor is the source's own paging token.
        """
        said = dict.fromkeys(STREAMS[job.kind])  # stream -> what became of it in this slice
        for name, stream in ((result or {}).get("streams") or {}).items():
            said[name.rpartition(" / ")[2]] = stream["status"]
        # A frontier is named by the kind of its records — ``transaction``,
        # not ``transactions`` — and the stream it belongs to.
        names = STREAMS[job.kind]
        with Session(self.engine) as session:
            frontiers = checkpoints(
                session, LOGICAL_SOURCE, [(name.removesuffix("s"), "desc", job.subject) for name in names]
            )
        cursor: dict[str, Any] = dict.fromkeys(names)
        reached: dict[str, Any] = {}
        for name in names:
            found = frontiers.get((name.removesuffix("s"), "desc", job.subject))
            if found is not None:
                cursor[name], reached[name] = found
        stored = self._stored.as_dict()
        fields = {
            "job_id": job.id,
            "kind": job.kind,
            "subject": job.subject,
            "priority": job.priority,
            "outcome": last.outcome,
            "state": last.state,
            "error": last.error,
            "window": (result or {}).get("window"),
            "streams": said,
            "cursor": cursor,
            "reached": reached,
            "requests": last.requests,
            "duration_seconds": round(last.seconds, 3),
            "asked_again_in": None if again_in is None else round(again_in),
            **stored,
        }
        message = (
            f"job {job.id} ({job.kind} {job.subject}): {last.outcome}, the job is {last.state}: "
            f"{last.requests} request(s), {stored['pages']} page(s), {stored['records']} record(s), "
            f"{last.seconds:.1f}s; inserted {counted(stored['inserted'])}, repeated {counted(stored['repeated'])}; "
            f"streams {keyed(said)}; at ledger {keyed(reached)}"
        )
        message += f"; asked again in {again_in:.0f}s" if again_in is not None else ""
        message += f" — {last.error}" if last.error else ""
        if last.state in history_jobs.GAVE_UP:
            level = logging.ERROR
        elif last.error:
            level = logging.WARNING
        else:
            level = logging.INFO
        logger.log(level, message, extra={"slice": fields})

    def _spent(self) -> int:
        """The requests of the slice so far: the pool's own included."""
        return self.client.stats.requests - self._first

    def _gone(self) -> float:
        """How long the slice has been going, or went."""
        return time.monotonic() - self._started

    # -- the worker's word of itself -------------------------------------------

    def _say(self, state: str, job_id: int | None) -> None:
        """Say that the worker is there and what it is doing, with the time by
        the worker's clock — when it is news, or when the last word said is
        as old as the worker lets it grow. Nothing is written otherwise: a
        poll that changes nothing writes nothing."""
        if self._said == (state, job_id) and not self._word_is_old():
            return
        heartbeats.beat(
            self.engine,
            heartbeats.HISTORY,
            now=self.clock(),
            name=self.name,
            state=state,
            job_id=job_id,
            slices_done=self.done,
            slices_failed=self.failed,
        )
        self._said, self._said_at = (state, job_id), self.clock()

    def _word_is_old(self) -> bool:
        """Whether nothing was said yet, or the last word is as old as the
        worker lets it grow. A word is said again then, whatever it says;
        otherwise only when what the worker is doing changed."""
        return self._said_at is None or self.clock() - self._said_at >= timedelta(seconds=self.heartbeat_seconds)

    def _resting(self) -> str:
        """What the worker is when it is on no job: kept from the queue by the
        pause of the whole queue, or waiting for a job."""
        return HELD if history_jobs.is_paused(self.engine) else IDLE

    # -- before every request ----------------------------------------------------

    def _before_request(self) -> None:
        """Before every request of a slice: stop when asked; end the slice
        where it stands when the whole queue is paused; keep the lease while
        the job is this worker's; keep to the slice's limits."""
        if self._lost:
            raise JobLost()
        if self._stop is not None and self._stop.is_set():
            raise Stopped()
        if self._job is not None:
            if history_jobs.is_paused(self.engine):
                self._paused = True
                raise Paused(LEFT_WHERE_IT_STANDS)
            self._say(WORKING, self._job.id)
            if time.monotonic() - self._last_renewal >= self.lease_seconds / 3:
                kept = history_jobs.renew(
                    self.engine, self._job.id, self.name, lease_seconds=self.lease_seconds, now=self.clock()
                )
                if not kept:
                    self._lost = True
                    raise JobLost()
                self._last_renewal = time.monotonic()
        if self._limits is not None:
            self._limits.check()


# -- what the reports of a slice say ---------------------------------------------


def _belongs(stream: StreamJob, job: HistoryJob) -> bool:
    """Whether ``stream`` is one of the job's subject: an account's four, or
    a market's trades."""
    return stream.key.subject_type == job.kind and stream.key.subject_key == job.subject


def _enabled(session: Session, account: str) -> bool:
    """Whether the account is watched and switched on."""
    tracked = session.get(TrackedAccount, account)
    return tracked is not None and tracked.enabled


def _outcome(reports: Sequence[StreamReport]) -> tuple[str, str | None]:
    """What became of a slice, from what became of its streams: the outcome,
    and what is to be said of it.

    Every stream verified: the subject is finished. Some cut off and none
    failed: progress, to be gone on with. A failure that is the servers'
    state — none could answer — is not the job's and is not counted. A real
    failure is: the job of a subject of which something could be read ends as
    ``partial`` when it keeps failing, and that of a subject of which nothing
    could be read as ``error``.
    """
    failed = [report for report in reports if report.status == FAILED]
    if not failed:
        finished = all(report.status == VERIFIED for report in reports)
        return (history_jobs.FINISHED if finished else history_jobs.PROGRESS), None
    error = "; ".join(f"{_named(report)}: {report.error}" for report in failed)
    if any((report.error or "").startswith(SERVERS_FAULT) for report in failed):
        return history_jobs.DEFERRED, error
    if len(failed) < len(reports):
        return history_jobs.FAILED, error
    return history_jobs.ERROR, error


def _result(
    client: HorizonClient,
    engine: Engine,
    streams: Sequence[StreamJob],
    window: Window,
    reports: Sequence[StreamReport],
    served_before: dict[str, int],
    served_earlier: dict[str, int] | None,
) -> dict[str, Any]:
    """What became of every stream, the window it is said of, what lies out
    of the pool's reach — reported, never counted as read — and which
    servers of the pool answered this job.

    The client counts answers for its whole life, with every job it worked
    on: what this slice added to the count goes to this job, on top of what
    its earlier slices had.
    """
    served = dict(served_earlier or {})
    for name, count in client.stats.by_server.items():
        gained = count - served_before.get(name, 0)
        if gained:
            served[name] = served.get(name, 0) + gained
    with Session(engine) as session:
        spans = [
            (max(row.from_ledger, window.from_ledger), min(row.to_ledger, window.to_ledger))
            for stream in streams
            for row in coverage.within(session, stream.key, window.from_ledger, window.to_ledger)
            if row.state == coverage.UNVERIFIABLE
        ]
    # Ledgers, not ranges: streams that cannot verify the same ledgers have
    # one history missing between them, not one each.
    unavailable, reach = 0, window.from_ledger - 1
    for low, high in sorted(spans):
        unavailable += max(0, high - max(low, reach + 1) + 1)
        reach = max(reach, high)
    return {
        "window": [window.from_ledger, window.to_ledger],
        "streams": {
            report.name: {"status": report.status, "coverage": report.coverage, "error": report.error}
            for report in reports
        },
        "unavailable_ledgers": unavailable,
        "servers": served,
    }


def _named(report: StreamReport) -> str:
    """A stream by what tells it apart for its subject: transactions,
    operations, effects or trades."""
    return report.name.rpartition(" / ")[2]


def keyed(values: Mapping[str, Any]) -> str:
    """``name=value`` of what is known, in the order given; ``-`` when
    nothing is."""
    return " ".join(f"{name}={value}" for name, value in values.items() if value is not None) or "-"


def counted(counts: Mapping[str, Any]) -> str:
    """``name=value`` of a count by data type, the zeros left out; ``-`` when
    there is nothing to say."""
    return keyed({data_type: number for data_type, number in counts.items() if number}) or "-"
