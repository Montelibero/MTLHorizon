"""The door of the admin UI.

One secret, from the environment (``MTL_HORIZON_ADMIN_SECRET``), opens it;
with no secret set it is closed to everyone, and says so — it does not open
for nobody in particular.

A login makes a session: a row in ``admin_session`` with the hash of a random
token and a secret of its own; the token goes into a cookie — ``HttpOnly``,
``SameSite=Strict``, ``Secure`` when the installation is reached over HTTPS,
and sent for ``/admin`` only. Nothing that could open the door again is kept
anywhere in plain form.

Every request that changes something carries, in its form, a token made
from the session's secret; a request without a right one is refused. That
is the CSRF defence, with the cookie's own ``SameSite`` and — when the
installation's public origin is set — a check that the request came from
there.

Logins that fail are counted, per address and overall, over a window; past
the allowance the address is blocked for a while that grows. The count is
kept in the database, so it is the same for every process.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.config import Settings
from app.storage.models import AdminLoginThrottle, AdminSession

COOKIE = "mtlh_admin"
# How long a session may be silent before it ends with its cookie.
MINUTE = timedelta(seconds=60)
# Logins that failed: this many in the window, from one address or overall,
# before it is blocked — for this long, growing with every failure after.
WINDOW = timedelta(minutes=15)
BLOCK = timedelta(minutes=15)
ALLOWED_PER_ADDRESS = 5
ALLOWED_OVERALL = 50
# A key blocked window after window is blocked longer each time — until it
# has been quiet, and unblocked, for this long.
QUIET = timedelta(hours=1)
LONGEST_BLOCK = timedelta(hours=8)
EVERYONE = "all"


# -- the secret and the session ------------------------------------------------


def secret_is_set(settings: Settings) -> bool:
    return bool(settings.admin_secret)


def secret_matches(settings: Settings, given: str) -> bool:
    """Whether ``given`` is the secret, told in a time that does not depend
    on where the two first differ."""
    if not settings.admin_secret or not given:
        return False
    return hmac.compare_digest(
        hashlib.sha256(given.encode()).digest(), hashlib.sha256(settings.admin_secret.encode()).digest()
    )


@dataclass(frozen=True)
class Login:
    """A session that is open, as far as the door is concerned."""

    id: int
    csrf: str
    expires_at: datetime
    address: str | None


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode()).digest().hex()


def open_session(engine: Engine, settings: Settings, *, address: str | None, now: datetime | None = None) -> tuple[Login, str]:
    """A new session for somebody who gave the secret: the session, and the
    token that stands for it in the cookie. Sessions long dead are dropped
    on the way."""
    now = now or datetime.now(UTC)
    token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    expires = now + timedelta(seconds=settings.admin_session_seconds)
    with Session(engine) as session, session.begin():
        session.execute(delete(AdminSession).where(AdminSession.expires_at < now - timedelta(days=1)))
        row = AdminSession(token_hash=_hash(token), csrf=csrf, expires_at=expires, last_seen_at=now, address=address)
        session.add(row)
        session.flush()
        return Login(int(row.id), csrf, expires, address), token


def close_session(engine: Engine, token: str | None) -> None:
    """End the session the token stands for, if there is one."""
    if not token:
        return
    with Session(engine) as session, session.begin():
        session.execute(delete(AdminSession).where(AdminSession.token_hash == _hash(token)))


def session_of(engine: Engine, token: str | None, *, now: datetime | None = None) -> Login | None:
    """The session the token stands for, while it has not expired; None
    otherwise. An expired one is dropped. A live one is said to have been
    seen — at most once a minute."""
    if not token:
        return None
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        row = session.scalar(select(AdminSession).where(AdminSession.token_hash == _hash(token)))
        if row is None:
            return None
        if row.expires_at <= now:
            session.delete(row)
            return None
        if now - row.last_seen_at >= MINUTE:
            row.last_seen_at = now
        return Login(int(row.id), row.csrf, row.expires_at, row.address)


def cookie(*, token: str, settings: Settings, drop: bool = False) -> dict[str, Any]:
    """The cookie of the session, as ``Response.set_cookie`` takes it — or
    the one that ends it (``drop``)."""
    return {
        "key": COOKIE,
        "value": "" if drop else token,
        "max_age": 0 if drop else int(settings.admin_session_seconds),
        "path": "/admin",
        "secure": settings.public_origin.startswith("https://"),
        "httponly": True,
        "samesite": "strict",
    }


# -- the token of every form that changes something ------------------------------


def csrf_token(csrf: str) -> str:
    """What goes into a form: made from the session's secret, and of no use
    without the cookie. The secret itself never leaves the server."""
    return hmac.new(csrf.encode(), b"csrf", hashlib.sha256).hexdigest()


def csrf_ok(csrf: str, given: str | None) -> bool:
    return bool(given) and hmac.compare_digest(csrf_token(csrf), given)


def origin_ok(settings: Settings, origin: str | None) -> bool:
    """Whether a request that changes something came from where this
    installation is: said by the ``Origin`` the browser sends. With no public
    origin configured there is nothing to compare with, and the form's token
    is the defence; a browser that says nothing is let through to it."""
    if not settings.public_origin or not origin:
        return True
    return origin.rstrip("/") == settings.public_origin.rstrip("/")


# -- logins that fail ------------------------------------------------------------


def _address_key(address: str | None) -> str:
    # The address is kept as a hash: the count is what matters, not who.
    return "address:" + hashlib.sha256((address or "?").encode()).digest().hex()


def blocked_until(engine: Engine, keys: list[str], *, now: datetime | None = None) -> datetime | None:
    now = now or datetime.now(UTC)
    with Session(engine) as session:
        until = session.scalar(
            select(func.max(AdminLoginThrottle.blocked_until)).where(
                AdminLoginThrottle.key.in_(keys), AdminLoginThrottle.blocked_until > now
            )
        )
    return until


def failed(engine: Engine, keys: list[str], *, now: datetime | None = None) -> datetime | None:
    """Count a login that failed for each of ``keys`` — a key that is
    blocked is not counted, it is refused before it gets here — and when one
    of them is blocked by this failure, until when.

    The failures of a key are counted over a window. The one that reaches the
    allowance blocks the key for a while, and a key blocked again in a later
    window is blocked longer each time; a key that has been quiet for an hour
    starts over. So a guesser gets five tries a window from one address and
    fifty overall, and less and less of them for as long as they go on.
    """
    now = now or datetime.now(UTC)
    blocked: datetime | None = None
    with Session(engine) as session, session.begin():
        for key in keys:
            row = session.get(AdminLoginThrottle, key, with_for_update=True)
            if row is None:
                row = AdminLoginThrottle(key=key, window_started_at=now, failures=0)
                session.add(row)
                session.flush()
            if now - row.window_started_at > WINDOW:
                row.window_started_at, row.failures = now, 0
            if row.blocks and (row.blocked_until is None or row.blocked_until <= now - QUIET):
                row.blocks = 0  # quiet for an hour since it was last blocked
            row.failures += 1
            allowed = ALLOWED_OVERALL if key == EVERYONE else ALLOWED_PER_ADDRESS
            if row.failures >= allowed and (row.blocked_until is None or row.blocked_until <= now):
                row.blocks += 1
                row.blocked_until = now + min(BLOCK * row.blocks, LONGEST_BLOCK)
            if row.blocked_until is not None and row.blocked_until > now:
                blocked = max(blocked or row.blocked_until, row.blocked_until)
        session.execute(delete(AdminLoginThrottle).where(AdminLoginThrottle.updated_at < now - timedelta(days=2)))
    return blocked


def keys_for(address: str | None) -> list[str]:
    """The keys a login from ``address`` is counted under."""
    return [_address_key(address), EVERYONE]


def succeeded(engine: Engine, address: str | None, *, now: datetime | None = None) -> None:
    """A login that worked: the address starts its count over. The overall
    count is kept: it is what slows down a guesser who changes addresses."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        session.execute(delete(AdminLoginThrottle).where(AdminLoginThrottle.key == _address_key(address)))
        session.execute(
            delete(AdminLoginThrottle).where(AdminLoginThrottle.key != EVERYONE, AdminLoginThrottle.updated_at < now - timedelta(days=1))
        )
