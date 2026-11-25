"""Accounts coming into the watch list, and being switched off and on.

Every way in — the watch list file, a command, discovery — ends here, so
that an account is always added the same way: enabled, with a reason, with
where it came from, and with its start boundary still to be captured
(``boundary_due``), which is what makes the worker plan its history import.

Nothing here overrides a decision. An account that is already watched stays
as it is, switched on or off; discovery finding it again adds where it was
found to ``discovered_by`` and touches nothing else.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy.orm import Session

from app.storage.models import HistoryAccount, TrackedAccount
from app.storage.upsert import upsert_row

LEGACY, YAML, MANUAL, DISCOVERY = "legacy", "yaml", "manual", "discovery"
ORIGINS = (LEGACY, YAML, MANUAL, DISCOVERY)

ADDED, ENABLED, DISABLED = "added", "already enabled", "disabled; left as it is"


@dataclass(frozen=True)
class AccountChange:
    account_id: str
    outcome: str
    # Whether this call added where the account was found to ``discovered_by``.
    noted: bool = False


def add_account(
    session: Session, address: str, *, reason: str, origin: str, discovered_by: Sequence[str] = ()
) -> AccountChange:
    """Bring an account into the watch list, or note that it is there.

    Runs inside the caller's transaction. A new account is enabled and due
    for its start boundary. An account that is there is not switched on or
    off; only the assets whose discovery found it are added to what is kept
    of where it was found.
    """
    if origin not in ORIGINS or origin == LEGACY:
        raise ValueError(f"an account cannot be added with origin {origin!r}")
    tracked = session.get(TrackedAccount, address)
    if tracked is None:
        upsert_row(session, HistoryAccount.__table__, {"account_id": address}, {})
        upsert_row(
            session,
            TrackedAccount.__table__,
            {"account_id": address},
            {
                "reason": reason,
                "enabled": True,
                "origin": origin,
                "discovered_by": ",".join(dict.fromkeys(discovered_by)) or None,
                "boundary_due": True,
            },
        )
        return AccountChange(address, ADDED)
    noted = False
    found = [asset for asset in (tracked.discovered_by or "").split(",") if asset]
    for asset in discovered_by:
        if asset not in found:
            found.append(asset)
            noted = True
    if noted:
        tracked.discovered_by = ",".join(found)
        session.flush()
    return AccountChange(address, ENABLED if tracked.enabled else DISABLED, noted)


def set_enabled(session: Session, address: str, *, enabled: bool, reason: str | None = None) -> AccountChange:
    """Switch an account on or off, or add it by hand when it is not there.

    Switched on again, the account is due for a new start boundary: what was
    missed while it was off is read from there. Switched off, nothing is
    deleted: the history stays, and what follows — its import paused, its
    subscription retired — is done by the worker that keeps the watch lists
    and the Notifier in step.
    """
    tracked = session.get(TrackedAccount, address)
    if tracked is None:
        if not enabled:
            return AccountChange(address, "not watched")
        add_account(session, address, reason=reason or "added by hand", origin=MANUAL)
        return AccountChange(address, ADDED)
    if tracked.enabled == enabled:
        return AccountChange(address, "already " + ("enabled" if enabled else "disabled"))
    tracked.enabled = enabled
    if reason:
        tracked.reason = reason
    if enabled:
        tracked.boundary_due = True
    session.flush()
    return AccountChange(address, ENABLED if enabled else DISABLED)
