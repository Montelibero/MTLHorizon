"""The worker: takes the jobs notifications made and brings their
transactions in whole.

Day by day, history arrives here. Operations Notifier posts an operation;
the webhook keeps the delivery and queues the transaction; and this worker
takes the job, asks Horizon for the transaction, its operations, its effects
and — when it made any — its trades, and stores them. It asks for nothing
else: no account is polled, and with no notification nothing is asked of
anybody. The job is one transaction of the database per part stored, never
one open while Horizon is asked, and the last part is stored in the same
transaction that marks the job done.

What this does not promise: that every event of the network gets here. It
brings in what it is told of, whole. Whether anything was missed is for the
recovery tools to say (``docs/architecture.md``).
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.engine import Engine

from app.backfill import store
from app.backfill.loader import CONTEXT_STREAMS, ContextFetcher, ContextIndex
from app.sources.horizon import HorizonClient, NotFound, Scope
from app.storage import jobs
from app.storage.models import ProcessingJob
from app.storage.writer import HistoryWriter

logger = logging.getLogger(__name__)

DEFAULT_MAX_ATTEMPTS = 8
DEFAULT_RETRY_BASE = 5.0
DEFAULT_RETRY_CAP = 600.0
# A single request is given up on after a minute and asked again up to five
# times; the lease is made longer before every request, and is longer than
# any one request can take.
DEFAULT_LEASE = 120.0
DEFAULT_POLL = 2.0


class JobLost(Exception):
    """The job is no longer this worker's: it was put back or given away
    while the worker was on it. Everything of this try is left unstored."""


class Stopped(Exception):
    """A signal asked the worker to stop. The job goes back as it was."""


def worker_name() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{os.urandom(4).hex()}"


class Worker:
    """Takes jobs from the queue and brings their transactions in.

    ``client`` is the pool of Horizon servers, with the pace every process
    shares; a worker is expected to come with the right of way (``Pace`` of
    the ``events`` share).
    """

    def __init__(
        self,
        client: HorizonClient,
        engine: Engine,
        *,
        name: str | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        retry_base: float = DEFAULT_RETRY_BASE,
        retry_cap: float = DEFAULT_RETRY_CAP,
        lease_seconds: float = DEFAULT_LEASE,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.client = client
        self.engine = engine
        self.name = name or worker_name()
        # What the time is, for the queue; the real one.
        self.clock = clock or (lambda: datetime.now(UTC))
        self.max_attempts = max_attempts
        self.retry_base = retry_base
        self.retry_cap = retry_cap
        self.lease_seconds = lease_seconds
        self.writer = HistoryWriter(engine)
        self.index = ContextIndex(engine)
        self.contexts = ContextFetcher(client, self.index.transaction, self.index.operation_parent)
        self._stop: threading.Event | None = None
        self._job: ProcessingJob | None = None
        self._last_renewal = 0.0
        self._stored: set[str] = set()
        self._finished = False
        self.done = 0
        self.failed = 0

    def close(self) -> None:
        self.index.close()

    def __enter__(self) -> "Worker":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- running -------------------------------------------------------------

    def run(self, stop: threading.Event, *, poll_seconds: float = DEFAULT_POLL, max_jobs: int | None = None) -> None:
        """Take and do jobs until ``stop`` is set, or ``max_jobs`` are done."""
        self._stop = stop
        while not stop.is_set() and (max_jobs is None or self.done + self.failed < max_jobs):
            if self.once(stop=stop) is False and max_jobs is None:
                stop.wait(poll_seconds)

    def once(self, *, stop: threading.Event | None = None) -> bool:
        """Take the next job whose time has come and do it. False when there
        was none; the queue is not waited on here."""
        self._stop = stop
        job = jobs.take(self.engine, self.name, lease_seconds=self.lease_seconds, max_attempts=self.max_attempts, now=self.clock())
        if job is None:
            return False
        self._do(job)
        return True

    def _do(self, job: ProcessingJob) -> None:
        """Bring one transaction in, or say why not."""
        first = self.client.stats.requests
        self._job, self._last_renewal = job, time.monotonic()
        self._stored, self._finished = set(), False
        # The transaction's ledger: a server that keeps none of it, or has
        # not got as far as it, is not asked.
        self.client.scope = None if job.ledger_sequence is None else Scope(job.ledger_sequence, job.ledger_sequence)
        self.client.before_request = self._before_request
        started = time.monotonic()
        state = jobs.RUNNING
        try:
            self.contexts.fetch_in_steps(job.transaction_hash, self._keep)
            state = jobs.DONE
            if not self._finished:  # everything was stored already: there was nothing to hand over
                self._end(job, jobs.DONE, None)
        except NotFound as error:
            self.failed += 1
            state = self._end(job, jobs.DEAD, f"the transaction is on none of the servers: {error}")
        except JobLost:
            state, self.failed = jobs.PENDING, self.failed + 1
            logger.warning("job %s is no longer this worker's: left as it is", job.id)
        except Stopped:
            jobs.put_back(self.engine, job, self.name, why="stopped before it was done", now=self.clock())
            state, self.failed = jobs.PENDING, self.failed + 1
        except Exception as error:  # noqa: BLE001 - a try that failed: the job says why and is tried again later
            self.failed += 1
            state = jobs.failed(
                self.engine,
                job,
                error=f"{type(error).__name__}: {error}",
                delay=self._delay(job),
                max_attempts=self.max_attempts,
                now=self.clock(),
            )
            level = logging.WARNING if state == jobs.PENDING else logging.ERROR
            logger.log(level, "job %s (transaction %s): try %d failed, %s: %s", job.id, job.transaction_hash[:8], job.attempts, state, error)
        else:
            self.done += 1
        if state == jobs.DONE:
            logger.info(
                "job %s: transaction %s brought in, %d request(s), %.1fs",
                job.id,
                job.transaction_hash[:8],
                self.client.stats.requests - first,
                time.monotonic() - started,
            )

    def _delay(self, job: ProcessingJob) -> float | None:
        """How long to wait before the next try; None when the tries are used up."""
        if job.attempts >= self.max_attempts:
            return None
        return min(self.retry_base * 2 ** (job.attempts - 1), self.retry_cap)

    def _end(self, job: ProcessingJob, state: str, error: str | None) -> str:
        with self.writer.transaction() as session:
            jobs.finish(session, job.id, state=state, result=self._result(job) if state == jobs.DONE else None, error=error, now=self.clock())
        return state

    def _keep(self, bundle: Any, last: bool) -> None:
        """Store a part as soon as it is fetched, in one short transaction;
        the last part together with the mark that says the job is done."""
        assert self._job is not None
        with self.writer.transaction() as session:
            store.store_context(session, bundle, bundle.completes)
            # The lease is made longer with the store, and only of a job that
            # is still this worker's.
            if not jobs.renew_in(session, self._job.id, self.name, lease_seconds=self.lease_seconds, now=self.clock()):
                raise JobLost()
            self._stored.update(bundle.completes)
            if last:
                jobs.finish(session, self._job.id, state=jobs.DONE, result=self._result(self._job), error=None, now=self.clock())
                self._finished = True

    def _result(self, job: ProcessingJob) -> dict[str, Any]:
        return {
            "transaction": job.transaction_hash,
            "stored_now": sorted(self._stored),
            "context": sorted(CONTEXT_STREAMS),
            "worker": self.name,
        }

    def _before_request(self) -> None:
        """Before every request: stop when asked, and keep the lease."""
        if self._stop is not None and self._stop.is_set():
            raise Stopped()
        if self._job is None:
            return
        if time.monotonic() - self._last_renewal < self.lease_seconds / 3:
            return
        if not jobs.renew(self.engine, self._job.id, self.name, lease_seconds=self.lease_seconds):
            raise JobLost()
        self._last_renewal = time.monotonic()

