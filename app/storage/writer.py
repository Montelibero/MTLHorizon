"""One writer at a time: short transactions that never wait on the network."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


class HistoryWriter:
    """Serializes writes to the history database.

    The insert-or-merge rules read a row and then write it. Two writers doing
    that for the same record at the same time could both decide to insert;
    one writer at a time makes that impossible. Readers are not held back.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._lock = threading.Lock()
        self.transactions = 0
        self.seconds_in_transaction = 0.0
        self.seconds_waiting = 0.0

    @contextmanager
    def transaction(self) -> Iterator[Session]:
        """A session in one transaction: committed on leaving the block,
        rolled back on an exception."""
        called = time.monotonic()
        with self._lock:
            started = time.monotonic()
            self.seconds_waiting += started - called
            try:
                with Session(self._engine, expire_on_commit=False) as session, session.begin():
                    yield session
            finally:
                self.transactions += 1
                self.seconds_in_transaction += time.monotonic() - started

    def as_dict(self) -> dict[str, float | int]:
        return {
            "transactions": self.transactions,
            "seconds_in_transaction": round(self.seconds_in_transaction, 3),
            "seconds_waiting": round(self.seconds_waiting, 3),
        }
