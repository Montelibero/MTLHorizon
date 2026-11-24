"""Engine and session factories for the history database."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


def create_db_engine(url: str) -> Engine:
    """Build an engine for ``url``.

    Creating the engine performs no I/O; the first connection is opened on
    first use. ``pool_pre_ping`` makes a connection that died while idle
    (database restart) be reopened transparently instead of failing a request.
    """
    return create_engine(url, pool_pre_ping=True)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)
