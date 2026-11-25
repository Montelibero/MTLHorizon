"""The worker of history imports: it takes the jobs of the queue and gives
each one slice after slice.

A slice is one bounded run of the backfill for one subject: a budget of
requests and seconds, kept before every page, so that a page once taken is
stored whole. What it stores is committed page by page with its coverage and
its checkpoint, so a slice that is cut short — by its budget, by a signal,
by the worker dying — loses nothing, and the next slice goes on from there.
The subject that waited longest gets the next slice: a week of history on
one account delays nobody.

The job's state is written in short transactions of their own, never in one
open while Horizon is asked. The lease is made longer before a request, from
the client's hook; a job that was paused, or taken away because its lease ran
out, is left where it stands at the next request.

What a slice could not read is told apart. A server that answered with an
error is the stream's failure and is counted; no server answering is the
pool's state, and is waited for. While no job is due nothing is asked of
anybody.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.backfill.jobs import StreamJob, stream_jobs
from app.backfill.loader import CONTEXT_MODES, PARENTS, Backfill, Limits, RunStopped, StreamReport, Window
from app.cli_common import LOGICAL_SOURCE, short_name
from app.sources.horizon import HorizonClient, RateLimited, Scope, Unavailable
from app.storage import coverage, history_jobs
from app.storage.models import HistoryJob, TrackedAccount

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

# What the reader says of a stream (``StreamReport.status``).
VERIFIED, FAILED, STOPPED = "verified", "failed", "stopped"
# ...and of a stream it could not read because no server could answer it.
SERVERS_FAULT = ("Unavailable:", "RateLimited:")


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
    # progress | finished | failed | error | deferred | paused, or lost: the
    # job stopped being this worker's in the middle of the slice.
    outcome: str
    # The state the job is in now.
    state: str
    requests: int
    error: str | None


class HistoryWorker:
    """Works on the queue of history imports, one slice at a time.

    ``client`` is the pool of Horizon servers, with the pace every process
    shares; a worker is expected to come with the ``background`` share: the
    worker of events has the right of way.

    A slice is read with the context its job was planned with. ``context`` is
    what a job is read with that says none.
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
        slice. False when there was none; the queue is not waited on here."""
        self._stop, self.last = stop, None
        job = history_jobs.take(
            self.engine,
            self.name,
            lease_seconds=self.lease_seconds,
            max_attempts=self.max_attempts,
            now=self.clock(),
        )
        if job is None:
            return False
        try:
            self.last = self._slice(job)
        except JobLost:
            # Nothing of the job is written: it is somebody else's now.
            self.failed += 1
            state = history_jobs.get(self.engine, job.id)
            self.last = Slice(
                job.id, job.kind, job.subject, "lost", state.state if state else "?", self._spent(), None
            )
            logger.warning(
                "job %s (%s %s) is no longer this worker's: the slice is left where it stands",
                job.id,
                job.kind,
                short_name(job.subject),
            )
        return True

    def _slice(self, job: HistoryJob) -> Slice:
        """Give ``job`` one slice, and say how it went — whatever happened
        inside it."""
        self._job, self._started, self._lost = job, time.monotonic(), False
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
        with Backfill(
            self.client,
            self.engine,
            source=LOGICAL_SOURCE,
            page_size=self.page_size,
            limits=self._limits,
            context=job.context or self.context,
        ) as backfill:
            # The reader puts the limits' check before every request; this
            # worker's goes first and ends with it.
            self.client.before_request = self._before_request
            reports = backfill.run(window, streams, concurrency=1)
        if self._lost:
            raise JobLost()
        outcome, error = _outcome(reports)
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
        return self._end(
            job,
            outcome,
            error,
            result,
            pages=sum(report.pages for report in reports),
            records=sum(report.records for report in reports),
        )

    def _end(
        self,
        job: HistoryJob,
        outcome: str,
        error: str | None,
        result: dict[str, Any] | None,
        *,
        pages: int = 0,
        records: int = 0,
    ) -> Slice:
        """Write how the slice ended and what it cost, count it, and say so."""
        if outcome == history_jobs.DEFERRED:
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
            pages=pages,
            records=records,
            delay=delay,
            max_attempts=self.max_attempts,
            now=self.clock(),
        )
        if state == history_jobs.DONE:
            self.done += 1
        elif outcome in (history_jobs.FAILED, history_jobs.ERROR):
            self.failed += 1
        waits = (history_jobs.FAILED, history_jobs.ERROR, history_jobs.DEFERRED)
        again = outcome in waits and state == history_jobs.WAITING
        if state in history_jobs.GAVE_UP:
            level = logging.ERROR
        elif error:
            level = logging.WARNING
        else:
            level = logging.INFO
        logger.log(
            level,
            "job %s (%s %s): %s, the job is %s: %d request(s), %d record(s), %.1fs%s%s",
            job.id,
            job.kind,
            short_name(job.subject),
            outcome,
            state,
            self._spent(),
            records,
            time.monotonic() - self._started,
            f"; asked again in {delay:.0f}s" if again else "",
            f" — {error}" if error else "",
        )
        return Slice(job.id, job.kind, job.subject, outcome, state, self._spent(), error)

    def _spent(self) -> int:
        """The requests of the slice so far: the pool's own included."""
        return self.client.stats.requests - self._first

    def _before_request(self) -> None:
        """Before every request of a slice: stop when asked, keep the lease
        while the job is this worker's, and keep to the slice's limits."""
        if self._lost:
            raise JobLost()
        if self._stop is not None and self._stop.is_set():
            raise Stopped()
        if self._job is not None and time.monotonic() - self._last_renewal >= self.lease_seconds / 3:
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
