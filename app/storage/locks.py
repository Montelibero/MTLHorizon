"""Who may talk to the source: advisory locks held in PostgreSQL sessions.

The collector and a backfill never read the source at the same time: two
loaders would double the pace the source is asked to bear, and there is no
rate limiter shared between processes yet. So they exclude each other — and
the live collector has the right of way. A backfill that holds the source
lets go of it as soon as a collector asks, and takes it back when the
collector's cycle is over.

Three session-level advisory locks, each held by a database session of its
own for as long as it is held:

* ``instance`` — one collector per database;
* ``source`` — whoever reads the source now;
* ``demand`` — a collector holds it from the moment its cycle is due until
  the cycle is over. That is how a backfill learns to let go.

Advisory locks live in one database and die with their session: a process
that crashes takes none along.
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

INSTANCE = "mtl-horizon:collector-instance"
SOURCE = "mtl-horizon:source:"
DEMAND = "mtl-horizon:source-demand:"

_TRY = text("SELECT pg_try_advisory_lock(:key)")
_UNLOCK = text("SELECT pg_advisory_unlock(:key)")


def _key(name: str) -> int:
    """Advisory locks are keyed by numbers; this is the number of a name."""
    return int.from_bytes(hashlib.blake2b(name.encode(), digest_size=8).digest(), "big", signed=True)


class AdvisoryLock:
    """A session-level advisory lock held by one database session."""

    def __init__(self, engine: Engine, name: str) -> None:
        self._engine = engine
        self._key = _key(name)
        self._connection: Connection | None = None

    @property
    def held(self) -> bool:
        return self._connection is not None

    def acquire(self, *, wait: float = 0.0, stop: threading.Event | None = None, poll: float = 1.0) -> bool:
        """Take the lock, asking again for up to ``wait`` seconds.

        False when another session holds it for longer than that, or when
        ``stop`` is set while waiting.
        """
        if self.held:
            return True
        self._connection = self._engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        deadline = time.monotonic() + wait
        try:
            while True:
                if self._connection.execute(_TRY, {"key": self._key}).scalar():
                    return True
                if stop is not None and stop.is_set():
                    self.release()
                    return False
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.release()
                    return False
                time.sleep(min(poll, remaining))
        except BaseException:
            self._close()
            raise

    def confirm(self) -> bool:
        """Whether this session still holds the lock.

        A session that lost its connection lost the lock with it. This asks
        the database: the same session can take its own lock again, another
        session's cannot.
        """
        if not self.held:
            return False
        try:
            taken = self._connection.execute(_TRY, {"key": self._key}).scalar()  # type: ignore[union-attr]
            if taken:
                self._connection.execute(_UNLOCK, {"key": self._key})  # type: ignore[union-attr]
        except SQLAlchemyError:
            taken = False
        if not taken:
            self._close()
        return bool(taken)

    def release(self) -> None:
        """Let go of the lock and close its session."""
        if not self.held:
            return
        try:
            self._connection.execute(_UNLOCK, {"key": self._key})  # type: ignore[union-attr]
        except SQLAlchemyError:
            # The session is gone, and with it the lock.
            logger.debug("the session of advisory lock %#x was lost", self._key, exc_info=True)
        finally:
            self._close()

    def _close(self) -> None:
        connection, self._connection = self._connection, None
        if connection is not None:
            connection.close()

    def __enter__(self) -> "AdvisoryLock":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.release()


def is_held(engine: Engine, name: str) -> bool:
    """Whether some session holds the advisory lock ``name``.

    Asked by trying to take it for a moment, on a session of its own.
    """
    key = _key(name)
    with engine.connect() as connection:
        taken = connection.execute(_TRY, {"key": key}).scalar()
        if taken:
            connection.execute(_UNLOCK, {"key": key})
    return not taken


def demanded(engine: Engine, source: str) -> bool:
    """Whether a collector holds the demand lock of ``source``: it is
    waiting for the source, or reading it."""
    return is_held(engine, DEMAND + source)


class SourceUse:
    """The source lock for a long reader, a backfill.

    ``before_request`` goes in front of every request to the source. Every
    ``check_every`` seconds it looks whether a collector is waiting; then
    the source is let go, and taken back only after the collector's cycle.
    """

    def __init__(
        self,
        engine: Engine,
        source: str,
        *,
        stop: threading.Event | None = None,
        check_every: float = 5.0,
        poll: float = 1.0,
    ) -> None:
        self._engine = engine
        self._source = source
        self._stop = stop
        self._check_every = check_every
        self._poll = poll
        self._lock = AdvisoryLock(engine, SOURCE + source)
        self._checked = float("-inf")

    @property
    def held(self) -> bool:
        return self._lock.held

    def acquire(self, *, wait: float) -> bool:
        """Hold the source, waiting for a collector's cycle for up to ``wait``
        seconds. False when it did not end in time."""
        self._checked = time.monotonic()
        return self._lock.acquire(wait=wait, stop=self._stop)

    def release(self) -> None:
        self._lock.release()

    def before_request(self) -> None:
        """Called before every request to the source."""
        if not self._lock.held:
            return
        now = time.monotonic()
        if now - self._checked < self._check_every:
            return
        self._checked = now
        if not demanded(self._engine, self._source):
            return
        logger.info("a collector is waiting: the source is let go until its cycle is over")
        self._lock.release()
        while demanded(self._engine, self._source):
            if self._stop is not None and self._stop.is_set():
                return
            time.sleep(self._poll)
        self._lock.acquire(stop=self._stop)
        self._checked = time.monotonic()

    def __enter__(self) -> "SourceUse":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.release()
