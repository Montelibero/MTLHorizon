"""What is known of the servers of the Horizon pool, kept in PostgreSQL for
a status page: what the last check of a server found, and what the running
processes last saw of it.

Nothing here holds a claim about history, and nothing here can stop a read:
a process that cannot note what it saw of a server goes on.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.storage.models import HorizonNodeState

logger = logging.getLogger(__name__)

# The columns a check writes; what the processes saw stay as they are.
_CHECKED = (
    "position",
    "available",
    "network_passphrase",
    "horizon_version",
    "latest_ledger",
    "elder_ledger",
    "latest_closed_at",
    "lag_ledgers",
    "root_ms",
    "history_ms",
    "root_status",
    "history_status",
    "errors",
    "timeouts",
    "rate_limit_headers",
    "report",
    "checked_at",
)


def note(engine: Engine, seen: Mapping[str, Any]) -> None:
    """A running process saw a server change state: answer again, fail, be
    left alone. ``seen`` is what a pool's server says of itself (``as_dict``).
    Best effort: what cannot be noted is logged and left."""
    try:
        with engine.begin() as connection:
            connection.execute(
                insert(HorizonNodeState)
                .values(
                    url=seen["url"],
                    position=seen["position"],
                    last_answer_at=seen.get("last_answer_at"),
                    last_error=seen.get("last_error"),
                    last_error_at=seen.get("last_error_at"),
                    cooling_until=seen.get("cooling_until"),
                )
                .on_conflict_do_update(
                    index_elements=["url"],
                    set_={
                        "position": seen["position"],
                        "last_answer_at": seen.get("last_answer_at"),
                        "last_error": seen.get("last_error"),
                        "last_error_at": seen.get("last_error_at"),
                        "cooling_until": seen.get("cooling_until"),
                    },
                )
            )
    except SQLAlchemyError:
        logger.debug("could not note what was seen of %s", seen.get("url"), exc_info=True)


def save(session: Session, checked: Mapping[str, Any], *, at: datetime | None = None) -> None:
    """Keep what a check found: one row per server it checked. Runs inside
    the caller's transaction. ``checked`` is a check's ``as_dict()``."""
    at = at or datetime.now(UTC)
    for server in checked["servers"]:
        values = {name: server.get(name) for name in _CHECKED}
        values["url"], values["checked_at"] = server["url"], at
        values["report"] = server
        session.execute(
            insert(HorizonNodeState)
            .values(**values)
            .on_conflict_do_update(index_elements=["url"], set_={**values, "updated_at": at})
        )


def rows(session: Session) -> list[HorizonNodeState]:
    """Every server anything was kept of, the primary first."""
    return list(session.scalars(select(HorizonNodeState).order_by(HorizonNodeState.position, HorizonNodeState.url)))
