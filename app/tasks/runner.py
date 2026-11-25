"""The runner of tasks: takes them one at a time from the queue, keeps the
lease while one runs, writes down what it comes to.

A task runs in the worker's background thread and may take long — a
discovery pass over an asset with many holders is a quarter of an hour. So
the lease is made longer while the task runs, by a thread of its own, and
that thread also watches for two things that end the task where it stands:
a cancel asked for it, and a lease that was lost (it ran out, and somebody
put the task back). A task is told of either through ``Task.stopped``.
"""

from __future__ import annotations

import logging
import logging
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy.engine import Engine

from app.cli_common import LOGICAL_SOURCE
from app.config import Settings
from app.notifier.client import NotifierClient
from app.storage import heartbeats, service_jobs
from app.storage.heartbeats import BACKGROUND
from app.storage.models import ServiceJob
from app.tasks.context import Budget, Task, TaskRefused, TaskStopped
from app.tasks.kinds import HANDLERS, describe

logger = logging.getLogger(__name__)

# How often the thread of a task looks whether it is cancelled or has lost
# its lease, and renews the lease: a third of it, at least this often.
DEFAULT_POLL = 1.0
HEARTBEAT_EVERY = 10.0


class TaskRunner:
    """Takes tasks from ``service_job`` and runs them, one at a time."""

    def __init__(
        self,
        engine: Engine,
        settings: Settings,
        *,
        client: Any,
        api: NotifierClient | None = None,
        clock: Callable[[], datetime] | None = None,
        name: str | None = None,
        source: str = LOGICAL_SOURCE,
        lease_seconds: float = service_jobs.DEFAULT_LEASE,
    ) -> None:
        self.engine, self.settings = engine, settings
        self.client, self.api = client, api
        self.clock = clock or (lambda: datetime.now(UTC))
        self.name = name or f"tasks:{id(self):x}"
        self.source = source
        self.lease_seconds = lease_seconds
        self.done = 0
        self.failed = 0
        self._beat_state, self._beat_at = "", 0.0
        self._stop: threading.Event | None = None

    # -- running -------------------------------------------------------------

    def run(self, stop: threading.Event, *, poll_seconds: float = DEFAULT_POLL) -> None:
        """Take and run tasks until ``stop`` is set; wait ``poll_seconds``
        between two looks when there was none."""
        self._stop = stop
        while not stop.is_set():
            self._beat("idle")
            if self.once() is None:
                stop.wait(poll_seconds)
        self._beat("idle")

    def once(self) -> ServiceJob | None:
        """Take the next task and run it. None when there was none."""
        job = service_jobs.take(self.engine, self.name, lease_seconds=self.lease_seconds, now=self.clock())
        if job is None:
            return None
        self._beat("working", job)
        stopped = threading.Event()
        keeper = threading.Thread(target=self._keep, args=(job, stopped), name=f"keep:{job.id}", daemon=True)
        keeper.start()
        task = Task(
            self.engine, self.settings, job, self.client, self.api, stopped, self.clock, self.source
        )
        logger.info("task %s: %s", job.id, describe(job))
        try:
            result = HANDLERS[job.kind](task)
        except TaskRefused as error:
            self.failed += 1
            logger.warning("task %s could not be run: %s", job.id, error)
            state = service_jobs.finish(self.engine, job, error=str(error), now=self.clock())
        except TaskStopped as error:
            logger.info("task %s: %s", job.id, error)
            state = service_jobs.finish(self.engine, job, error=str(error), now=self.clock())
        except Exception as error:  # noqa: BLE001 - one task that failed: said, kept, and the next one is taken
            self.failed += 1
            logger.exception("task %s failed", job.id)
            state = service_jobs.finish(self.engine, job, error=f"{type(error).__name__}: {error}", now=self.clock())
        else:
            self.done += 1
            state = service_jobs.finish(self.engine, job, result=result, now=self.clock())
        finally:
            stopped.set()
            keeper.join(self.lease_seconds)
        self._beat("idle")
        logger.info("task %s: %s", job.id, state)
        return service_jobs.get(self.engine, job.id)

    def _keep(self, job: ServiceJob, stopped: threading.Event) -> None:
        """While the task runs: say that it is still this runner's, and end
        it when it is cancelled or no longer is."""
        every = max(self.lease_seconds / 3, DEFAULT_POLL)
        next_renewal = time.monotonic() + every
        while not stopped.wait(DEFAULT_POLL):
            now = time.monotonic()
            try:
                if now >= next_renewal:
                    next_renewal = now + every
                    if not service_jobs.renew(self.engine, job, lease_seconds=self.lease_seconds, now=self.clock()):
                        logger.warning("task %s is no longer this runner's: it stops where it stands", job.id)
                        stopped.set()
                        return
                if service_jobs.cancel_requested(self.engine, job.id):
                    logger.info("task %s was asked to stop: it ends before its next request", job.id)
                    stopped.set()
                    return
            except Exception:  # noqa: BLE001 - the database is gone: the task itself will find out
                logger.exception("the lease of task %s could not be kept", job.id)
                stopped.set()
                return

    def _beat(self, state: str, job: ServiceJob | None = None) -> None:
        """Say that the thread of tasks is alive, as the worker of events
        does; at most every few seconds unless something changed."""
        now = time.monotonic()
        if state == self._beat_state and now - self._beat_at < HEARTBEAT_EVERY:
            return
        self._beat_state, self._beat_at = state, now
        try:
            heartbeats.beat(
                self.engine,
                BACKGROUND,
                now=self.clock(),
                name=self.name,
                state=state,
                task=None if job is None else {"id": job.id, "kind": job.kind},
                done=self.done,
                failed=self.failed,
            )
        except Exception:  # noqa: BLE001 - said again soon; not worth stopping for
            logger.debug("the heartbeat could not be written", exc_info=True)
