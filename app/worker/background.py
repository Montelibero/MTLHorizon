"""The worker's other hand: what is not an event.

Two threads of the worker's, each with what it needs of its own — a Horizon
pool with the background share of the pace, for an event has the right of
way, and a client of the Notifier's API.

**Provisioning** is the pass that keeps accounts, their history imports and
their subscriptions in step (:mod:`app.watch.provision`). It runs on its
schedule, whether or not events are waiting. Until stage 06 it ran only
while the queue of events was empty: under a steady stream of events a new
account could wait for its boundary and its subscription for as long as the
stream lasted. The pass asks Horizon for nothing when nothing is due, and
one pass runs at a time per database.

**Tasks** are what the admin UI starts and what takes time
(:mod:`app.tasks`), one at a time; the schedule of discovery, when it is on,
puts its passes there too, so that a pass is a task with a state like any
other.

Both say every few seconds that they are alive: ``heartbeat.provision`` and
``heartbeat.background`` in ``service_state``.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.engine import Engine

from app.cli_common import make_client
from app.config import Settings
from app.discovery import scan
from app.storage import heartbeats, service_jobs
from app.storage.heartbeats import PROVISION
from app.storage.locks import AdvisoryLock
from app.storage.models import DiscoveryRun
from app.storage.pace import BACKGROUND as BACKGROUND_SHARE
from app.tasks.runner import TaskRunner
from app.watch import provision

logger = logging.getLogger(__name__)

# One pass of provisioning at a time per database: two workers that find the
# same account due would both ask the pool for its latest ledger.
PROVISION_LOCK = "mtl-horizon:provision"
HEARTBEAT_EVERY = 10.0
DEFAULT_POLL = 1.0
_NOT_GIVEN = object()


class Background:
    """Made with the worker's ``stop`` event and started with ``start``; its
    threads are daemons and end with that event.

    ``pass_`` may be given in place of the pass itself (tests), ``client``
    and ``api`` in place of the ones made here.
    """

    def __init__(
        self,
        engine: Engine,
        settings: Settings,
        *,
        stop: threading.Event,
        client: Any = None,
        api: Any = _NOT_GIVEN,
        clock: Callable[[], datetime] | None = None,
        pass_: Callable[[], Any] | None = None,
        provision_every: float | None = None,
        poll_seconds: float = DEFAULT_POLL,
        name: str | None = None,
    ) -> None:
        self.engine, self.settings, self.stop = engine, settings, stop
        self.clock = clock or (lambda: datetime.now(UTC))
        self.provision_every = settings.provision_interval_seconds if provision_every is None else provision_every
        self.poll_seconds = poll_seconds
        self.passes = 0
        # The pool and the Notifier's client are made here when not given,
        # and closed with the threads.
        self._own_client = client is None
        self.client = client if client is not None else make_client(settings, engine, share=BACKGROUND_SHARE)
        self.api = provision.make_api(settings, engine=engine) if api is _NOT_GIVEN else api
        self._own_api = api is _NOT_GIVEN and self.api is not None
        self.pass_ = pass_ or (lambda: provision.run(engine, settings, client=self.client, api=self.api, now=self.clock()))
        self.tasks = TaskRunner(engine, settings, client=self.client, api=self.api, clock=self.clock, name=name)
        self.name = name or self.tasks.name
        self.threads: list[threading.Thread] = []
        self._beat_state, self._beat_at = "", 0.0

    # -- the threads ----------------------------------------------------------

    def start(self) -> "Background":
        self.threads = [
            threading.Thread(target=self._provision_loop, name="provision", daemon=True),
            threading.Thread(
                target=self.tasks.run, args=(self.stop,), kwargs={"poll_seconds": self.poll_seconds}, name="tasks", daemon=True
            ),
        ]
        for thread in self.threads:
            thread.start()
        return self

    def close(self) -> None:
        if self._own_client:
            self.client.close()
        if self._own_api:
            self.api.close()

    def _provision_loop(self) -> None:
        # The first pass at once: a worker that starts finds what is due.
        while not self.stop.is_set():
            self.run_pass()
            if self.stop.wait(self.provision_every):
                return

    # -- one pass -------------------------------------------------------------

    def run_pass(self) -> Any:
        """One pass of provisioning — None when another worker is making one
        at this moment — and, when the schedule of discovery is on, its passes
        put in the queue of tasks."""
        report: Any = None
        with AdvisoryLock(self.engine, PROVISION_LOCK) as lock:
            if lock.acquire():
                try:
                    self._beat("working")
                    report = self.pass_()
                except Exception:  # noqa: BLE001 - the next pass tries again; the rows still say what is to be done
                    logger.exception("the pass that keeps accounts in step failed; it is run again")
                finally:
                    self._beat("idle")
                    lock.release()
        self._say(report)
        try:
            self._schedule_discovery()
        except Exception:  # noqa: BLE001 - tried again with the next pass
            logger.exception("the schedule of discovery could not be kept")
        self.passes += 1
        return report

    def _say(self, report: Any) -> None:
        """The pass in the log: a line or two when it did something, debug
        when it found everything in step."""
        if report is None or not hasattr(report, "lines"):
            return
        done = report.as_dict()
        quiet = not any(
            done.get(name) for name in ("accounts_added", "start_boundaries", "imports_planned", "upper_boundaries", "imports_paused")
        ) and not (done.get("subscriptions_wanted") and done.get("subscriptions_not_done"))
        for line in report.lines():
            logger.log(logging.DEBUG if quiet else logging.INFO, "provisioning: %s", line)

    def _schedule_discovery(self) -> int:
        """The schedule of discovery, when it is on: a pass is put in the
        queue for every asset whose last pass began longer ago than the
        interval. One that is queued or running already is not put twice.
        Returns how many were put."""
        interval = self.settings.discovery_interval_seconds
        if not interval or interval <= 0:
            return 0
        now = self.clock()
        with self.engine.connect() as connection:
            last_started = dict(
                connection.execute(select(DiscoveryRun.asset_id, func.max(DiscoveryRun.started_at)).group_by(DiscoveryRun.asset_id)).all()
            )
        put = 0
        for asset_id in scan.discovery_assets(self.engine):
            last = last_started.get(asset_id)
            if last is not None and (now - last).total_seconds() < interval:
                continue
            _, made = service_jobs.enqueue(
                self.engine,
                "discovery",
                {"asset": asset_id, "resume": True, "max_requests": scan.DEFAULT_MAX_REQUESTS},
                one_at_a_time=asset_id,
                requested_by="schedule",
                now=now,
            )
            put += made
        return put

    def _beat(self, state: str) -> None:
        """Say that the thread of provisioning is alive: when it changes, and
        at most every few seconds. Not worth a pass for, when it cannot."""
        now = time.monotonic()
        if state == self._beat_state and now - self._beat_at < HEARTBEAT_EVERY:
            return
        self._beat_state, self._beat_at = state, now
        try:
            heartbeats.beat(self.engine, PROVISION, now=self.clock(), name=self.name, state=state, passes=self.passes)
        except Exception:  # noqa: BLE001
            logger.debug("the heartbeat could not be written", exc_info=True)
