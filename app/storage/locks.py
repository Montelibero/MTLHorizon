"""Who may work: advisory locks held in PostgreSQL sessions.

One collector serves a database: two would read the same streams twice. The
lock is session-level, held by a database session of its own for as long as
it is held, and it dies with its session: a process that crashes takes none
along.

Requests to the source are not divided by locks. Their pace is kept for
every process together in PostgreSQL (:mod:`app.storage.pace`), so the
worker that handles events, a backfill and a collector may read at the same
time without asking the source for more than it is allowed.
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

_TRY = text("SELECT pg_try_advisory_lock(:key)")
_UNLOCK = text("SELECT pg_advisory_unlock(:key)")


def _key(name: str) -> int:  # noqa: no private marker - the number of a name, shared
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
