"""The cap on what this installation may ask of the outside world — for a
check that its owner allowed to touch the real servers.

With ``MTL_HORIZON_LIVE_BUDGET`` set to a number, no more than that many HTTP
attempts leave the installation for as long as the run lasts: to Horizon and
to the Notifier's API alike, the root probes, the retries, the failovers and
every diagnosis included. The count is taken **before** an attempt is made,
by the transport every request of every client goes through, and an attempt
over the budget is refused before a packet leaves — it looks to the caller
like a server that did not answer. Attempts are also kept a second apart.

The count is kept in the database (``service_state``, ``live_budget``): it
is one for every process of the installation and survives a restart, so a
run that was restarted does not get a second budget. A new run begins by
clearing that row (``DELETE FROM service_state WHERE name = 'live_budget'``).

Without the setting nothing is counted and nothing is refused: the
installation asks as it always did.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import httpx
from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine

NAME = "live_budget"
# However many processes ask, no two attempts leave within this time.
MIN_INTERVAL = 1.0


class Exhausted(Exception):
    """The budget is used up: the attempt is not made."""


class LiveBudget:
    """How many attempts were made against how many are allowed. With an
    engine the count is the installation's, in ``service_state``; without one
    it is this process's own."""

    def __init__(self, limit: int, engine: Engine | None = None) -> None:
        self.limit, self.engine = limit, engine
        self._used = 0
        self._last_at: float | None = None
        self._lock = threading.Lock()

    @property
    def used(self) -> int:
        if self.engine is None:
            return self._used
        with self.engine.connect() as connection:
            row = connection.execute(text("SELECT value->>'used' FROM service_state WHERE name = :name"), {"name": NAME}).scalar()
        return int(row or 0)

    def take(self) -> None:
        """Count one attempt about to be made; :class:`Exhausted` when there
        is none left. Waits, if need be, so that no two attempts leave within
        ``MIN_INTERVAL``."""
        with self._lock:
            previous = self._claim()
            if previous is not None:
                wait = MIN_INTERVAL - (time.time() - previous)
                if wait > 0:
                    time.sleep(wait)
            self._stamp(time.time())

    def _claim(self) -> float | None:
        """When the attempt before this one was made, or None; raises
        :class:`Exhausted` and counts nothing when the budget is used up."""
        if self.engine is None:
            if self._used >= self.limit:
                raise Exhausted(f"the budget of {self.limit} live request(s) is used")
            self._used += 1
            return self._last_at
        with self.engine.begin() as connection:
            row = connection.execute(
                text("SELECT value FROM service_state WHERE name = :name FOR UPDATE"), {"name": NAME}
            ).scalar()
            if row is None:
                connection.execute(
                    text("INSERT INTO service_state (name, value) VALUES (:name, :value)").bindparams(
                        bindparam("value", type_=JSONB())
                    ),
                    {"name": NAME, "value": {"limit": self.limit, "used": 0, "last_at": None}},
                )
                row = {"limit": self.limit, "used": 0, "last_at": None}
            used = int(row.get("used") or 0)
            if used >= int(row.get("limit") or self.limit):
                raise Exhausted(f"the budget of {row.get('limit')} live request(s) is used")
            connection.execute(
                text("UPDATE service_state SET value = jsonb_set(value, '{used}', to_jsonb((value->>'used')::int + 1)) WHERE name = :name"),
                {"name": NAME},
            )
            last = row.get("last_at")
            return float(last) if isinstance(last, (int, float)) else None

    def _stamp(self, at: float) -> None:
        if self.engine is None:
            self._last_at = at
            return
        with self.engine.begin() as connection:
            connection.execute(
                text("UPDATE service_state SET value = jsonb_set(value, '{last_at}', to_jsonb(:at)) WHERE name = :name"),
                {"name": NAME, "at": at},
            )


# The cap of this process; None when there is none. What really sends when
# the cap is on — httpx's own transport, unless a test puts a fake here.
BUDGET: LiveBudget | None = None
INNER: httpx.BaseTransport | None = None


def configure(limit: int, engine: Engine | None = None) -> None:
    """Put the cap on this process (``limit`` of zero or less: take it off).
    Said again with the same limit, it is the same cap: what was used stays
    used."""
    global BUDGET
    if limit <= 0:
        BUDGET = None
    elif BUDGET is None or BUDGET.limit != limit:
        BUDGET = LiveBudget(limit, engine)


def transport() -> httpx.BaseTransport | None:
    """The transport every real request is to go through: None when there is
    no cap (httpx makes its own), the capped one when there is."""
    if BUDGET is None:
        return None
    return Capped(INNER if INNER is not None else httpx.HTTPTransport(), BUDGET)


class Spent(httpx.ConnectError):
    """The attempt was not made: the budget is used up. A connection error to
    every client — nothing was answered — that the clients of this
    installation tell from a server that is down, to say what it is."""


class Capped(httpx.BaseTransport):
    """Another transport, asked only as far as the budget goes."""

    def __init__(self, inner: httpx.BaseTransport, budget: LiveBudget) -> None:
        self.inner, self.budget = inner, budget

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        try:
            self.budget.take()
        except Exhausted as error:
            raise Spent(str(error), request=request) from error
        return self.inner.handle_request(request)

    def close(self) -> None:
        self.inner.close()

    def __enter__(self) -> Any:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
