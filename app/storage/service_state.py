"""What a service must remember between passes, by name: where a scan got
to, when something last ran. Small things, kept in PostgreSQL so that they
survive a restart and are the same for every worker of the installation.

Nothing here is history or a claim about it.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy.orm import Session

from app.storage.models import ServiceState


def get(session: Session, name: str, default: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """What is kept under ``name``; a copy of ``default`` when nothing is."""
    row = session.get(ServiceState, name)
    return dict(default or {}) if row is None else dict(row.value)


def put(session: Session, name: str, value: Mapping[str, Any]) -> None:
    """Keep ``value`` under ``name``. Runs inside the caller's transaction."""
    row = session.get(ServiceState, name)
    if row is None:
        session.add(ServiceState(name=name, value=dict(value)))
    else:
        row.value = dict(value)
    session.flush()
