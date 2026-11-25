"""What a handler of a task is given, and the two ways a task ends before
its work is done."""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy.engine import Engine

from app.config import Settings
from app.notifier.client import NotifierClient
from app.storage import service_jobs
from app.storage.models import ServiceJob


class TaskRefused(Exception):
    """The task cannot be run at all — its parameters say nothing, or what
    it needs is not configured. Not a retry: the reason is kept."""


class TaskStopped(Exception):
    """The task was asked to stop, or lost its lease: it ends where it
    stands. What it did by then stays done."""


@dataclass
class Task:
    """What a handler is given: the task, what it may use, and how it is
    told to stop."""

    engine: Engine
    settings: Settings
    job: ServiceJob
    # The pool of Horizon servers, with the background share of the pace.
    client: Any
    # The Notifier's API, or None when none is configured.
    api: NotifierClient | None
    stopped: threading.Event
    clock: Callable[[], datetime]
    source: str

    @property
    def params(self) -> dict[str, Any]:
        return dict(self.job.params or {})

    def progress(self, **fields: Any) -> bool:
        """Say something of how far the task has got; False when the task is
        no longer this runner's."""
        return service_jobs.progress(self.engine, self.job, **fields)


class Budget:
    """Counts what a task asked of Horizon and stops it at its limit — before
    a request is sent, not after it. Used as a context manager over the
    client's ``before_request``; ``count()`` is what was asked by then."""

    class Used(Exception):
        """The budget is used up: the work stops where it stands."""

    def __init__(self, client: Any, limit: int, *, stop: threading.Event | None = None) -> None:
        self.client, self.limit, self.stop = client, limit, stop
        self._inner: Callable[[], None] | None = None
        self._first = 0

    def __enter__(self) -> "Budget":
        self._inner, self._first = self.client.before_request, self.client.stats.requests
        self.client.before_request = self._before
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.client.before_request = self._inner

    def count(self) -> int:
        return self.client.stats.requests - self._first

    def _before(self) -> None:
        if self._inner is not None:
            self._inner()
        if self.stop is not None and self.stop.is_set():
            raise TaskStopped("stopped on request")
        if self.count() >= self.limit:
            raise Budget.Used(f"the budget of {self.limit} requests is used")
