"""How a service says that it is alive.

A service that works a queue rewrites a small record in ``service_state``
every few seconds — what it is, when it last said anything, whether it is on
something. A dashboard reads these records to tell a queue that is empty
from a service that is not running, and work that is waiting from work that
is being taken. Nothing here is history, and a heartbeat is no proof that
the work is right: only that somebody is there.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.storage import service_state
from app.storage.models import ServiceState

WORKER = "heartbeat.worker"
HISTORY = "heartbeat.history"
BACKGROUND = "heartbeat.background"
PROVISION = "heartbeat.provision"
# Nothing heard for this long, and a service is looked for rather than
# counted on. A service says something at least every 15 seconds.
STALE_AFTER = timedelta(seconds=60)


def beat(engine: Engine, record: str, *, now: datetime | None = None, **state: Any) -> None:
    """Say that the service is alive and what it is doing: kept under
    ``record`` with the moment. Two processes may say it at once: the last
    word is kept, and neither fails."""
    now = now or datetime.now(UTC)
    service_state.write(engine, record, {"at": now.isoformat(timespec="milliseconds"), **state})


def read(engine: Engine, name: str) -> dict[str, Any] | None:
    """What the service last said; None when it never did."""
    with Session(engine) as session:
        row = session.get(ServiceState, name)
        if row is None:
            return None
        return dict(row.value)


def age(beat_: dict[str, Any] | None, now: datetime) -> timedelta | None:
    """How long ago the service said something; None when it never did, or
    said it in a form that cannot be read."""
    if not beat_:
        return None
    raw = beat_.get("at")
    if not isinstance(raw, str):
        return None
    try:
        at = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if at.tzinfo is None:
        at = at.replace(tzinfo=UTC)
    return now - at


def is_stale(beat_: dict[str, Any] | None, now: datetime) -> bool:
    """Whether the service has not been heard of for longer than a service
    that runs would be silent. A service that never said anything is stale
    too: it is not running, as far as anyone can tell."""
    how_long = age(beat_, now)
    return how_long is None or how_long > STALE_AFTER
