"""The pace of requests to Horizon, kept in PostgreSQL and shared by every
process of an installation: the worker that handles events, a backfill, a
verification, a diagnostic check.

The pace is one for all of them together and one for the whole pool of
servers: more processes or more servers do not make it faster, and the
provider's limit, which is per address, is never gone around by switching
between servers. One row holds it; every request takes a slot in that row
before it goes, in one statement, so that two processes never take one slot.

Events have the right of way, but a reader of history is never starved.
A background reader — a backfill, a verification, a check — takes one
request in ``spare`` while events are flowing, and every request while
none is: the worker is idle most of the time, and a backfill then runs at
the full pace.

A 429 pauses the row for every process for as long as the server asks: the
allowance it talks about is the address's, not one process's.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, ProgrammingError

from app.config import ConfigError

logger = logging.getLogger(__name__)

EVENTS = "events"
BACKGROUND = "background"
SHARES = (EVENTS, BACKGROUND)

# Events count as flowing until this long after the last one, when the pace
# is fast enough that three requests are sooner than that.
MIN_IDLE = 2.0


@dataclass(frozen=True)
class PaceSettings:
    # How many requests in a row events may take while a background reader
    # waits for one.
    spare: int = 10
    # Seconds after an event until events count as not flowing; None for
    # three intervals, and never less than MIN_IDLE.
    idle: float | None = None

    def idle_seconds(self, interval: float) -> float:
        return self.idle if self.idle is not None else max(MIN_IDLE, 3 * interval)


# ``next_at`` is when the request after this one may go: the start of this
# one's slot plus the interval, so that slots follow one another and never
# overlap. The wait returned is to the start of this one's slot.
#
# "A background reader asks while events are flowing and has not waited
# long enough": it takes no slot then and asks again. The same words decide
# the row and, in RETURNING, what happened.
_WAIT = """:share = 'background' AND pace.last_share = 'events'
       AND pace.last_at > (SELECT now FROM clock) - make_interval(secs => :idle)
       AND pace.since_background < :spare"""

_RESERVE = text(
    f"""
    WITH clock AS (SELECT clock_timestamp() AS now)
    INSERT INTO http_pace AS pace (name, next_at, last_at, last_share, since_background)
    SELECT :name, now + make_interval(secs => :interval), now, :share,
           CASE WHEN :share = 'events' THEN 1 ELSE 0 END
    FROM clock
    ON CONFLICT (name) DO UPDATE SET
        next_at = CASE WHEN {_WAIT} THEN pace.next_at
                       ELSE GREATEST(pace.next_at, (SELECT now FROM clock))
                            + make_interval(secs => :interval) END,
        last_at = CASE WHEN {_WAIT} THEN pace.last_at ELSE (SELECT now FROM clock) END,
        last_share = CASE WHEN {_WAIT} THEN pace.last_share ELSE :share END,
        since_background = CASE WHEN {_WAIT} THEN pace.since_background
                                WHEN :share = 'events' THEN pace.since_background + 1
                                ELSE 0 END
    RETURNING ({_WAIT}) AS waiting,
              GREATEST(0, EXTRACT(epoch FROM (next_at - make_interval(secs => :interval)
                                              - (SELECT now FROM clock)))) AS wait
    """
)

_PAUSE = text(
    """
    INSERT INTO http_pace AS pace (name, next_at, last_at, last_share)
    VALUES (:name, clock_timestamp() + make_interval(secs => :seconds), NULL, :share)
    ON CONFLICT (name) DO UPDATE SET next_at = GREATEST(pace.next_at, EXCLUDED.next_at)
    """
)


class Pace:
    """The shared pace, as one process takes part in it.

    ``share`` says what the process does: ``events`` or ``background``. The
    class keeps the slots and says how long to wait; the sleeping is the
    caller's.
    """

    def __init__(
        self,
        engine: Engine,
        *,
        share: str = BACKGROUND,
        name: str = "horizon",
        settings: PaceSettings | None = None,
    ) -> None:
        if share not in SHARES:
            raise ValueError(f"share is one of {SHARES}, not {share!r}")
        self._engine = engine
        self._share = share
        self._name = name
        self._settings = settings or PaceSettings()

    def reserve(self, interval: float) -> float | None:
        """Take the next slot for a request: how many seconds to wait before
        it may go. None when an event goes first: ask again in ``again()``
        seconds. Nothing is taken then."""
        idle = self._settings.idle_seconds(interval)
        try:
            with self._engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
                row = connection.execute(
                    _RESERVE,
                    {"name": self._name, "share": self._share, "interval": interval, "idle": idle, "spare": self._settings.spare},
                ).one()
        except ProgrammingError as error:
            raise ConfigError(f"cannot keep the pace in this database: {error.orig}") from error
        except OperationalError as error:
            raise ConfigError(f"cannot reach the database to keep the pace: {error.orig}") from error
        if row.waiting:
            return None
        return float(row.wait)

    def again(self, interval: float) -> float:
        """How long a reader that was told to wait asks again."""
        return max(0.05, min(self._settings.idle_seconds(interval), interval))

    def pause(self, seconds: float) -> None:
        """No request of any process goes for ``seconds``: a server said so."""
        with self._engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(_PAUSE, {"name": self._name, "share": self._share, "seconds": seconds})
