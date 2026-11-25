"""The door of the admin UI: one secret, sessions by a token of which only
the hash is kept, a token in every form, and a count of the logins that
failed."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.admin import auth
from app.config import Settings
from app.storage.models import AdminLoginThrottle, AdminSession


@pytest.fixture
def now() -> datetime:
    return datetime(2026, 11, 25, 12, 0, tzinfo=UTC)


def settings(**changes: object) -> Settings:
    return Settings(**{**Settings(database_url="postgresql://x").__dict__, **changes})  # type: ignore[arg-type]


# -- the secret ----------------------------------------------------------------------


def test_the_door_is_closed_without_a_secret_and_opens_only_for_the_secret() -> None:
    assert not auth.secret_is_set(settings())
    assert not auth.secret_matches(settings(), "") and not auth.secret_matches(settings(), "anything")
    assert not auth.secret_matches(settings(), "admin")  # a default there is none of
    kept = settings(admin_secret="s3cret")
    assert auth.secret_is_set(kept) and auth.secret_matches(kept, "s3cret")
    assert not auth.secret_matches(kept, "S3CRET") and not auth.secret_matches(kept, " s3cret ") and not auth.secret_matches(kept, "")


# -- sessions -------------------------------------------------------------------------


def test_a_session_is_kept_by_the_hash_of_its_token_and_ends_with_its_time(
    make_database: Callable[[], str], now: datetime
) -> None:
    engine = __import__("sqlalchemy").create_engine(make_database())
    try:
        current = settings(admin_secret="s3cret", admin_session_seconds=3600)
        login, token = auth.open_session(engine, current, address="127.0.0.1", now=now)
        assert token and len(token) >= 40 and login.csrf and login.csrf != token and login.expires_at == now + timedelta(hours=1)
        with Session(engine) as session:
            row = session.scalar(select(AdminSession))
            assert row is not None and row.token_hash != token and row.csrf == login.csrf
            assert row.token_hash == __import__("hashlib").sha256(token.encode()).hexdigest()
            assert token not in str(row.token_hash) and row.address == "127.0.0.1"
        found = auth.session_of(engine, token, now=now)
        assert found is not None and (found.id, found.csrf) == (login.id, login.csrf)
        assert auth.session_of(engine, token + "x", now=now) is None and auth.session_of(engine, None) is None
        assert auth.session_of(engine, token, now=now + timedelta(hours=1)) is None, "ended with its time"
        with Session(engine) as session:
            assert session.scalar(select(func.count()).select_from(AdminSession)) == 0, "and dropped"
        # Said to have been seen at most once a minute.
        login, token = auth.open_session(engine, current, address="10.0.0.1", now=now)
        for seconds in (10, 20, 61, 62):
            assert auth.session_of(engine, token, now=now + timedelta(seconds=seconds)) is not None
        with Session(engine) as session:
            row = session.scalar(select(AdminSession))
            assert row is not None and row.last_seen_at == now + timedelta(seconds=61)
        auth.close_session(engine, token)
        assert auth.session_of(engine, token, now=now) is None
        auth.close_session(engine, None)  # nothing to end: nothing happens
        auth.close_session(engine, "not-a-token")
    finally:
        engine.dispose()


def test_the_cookie_is_the_door_s_own_and_nothing_else_s() -> None:
    plain = settings(admin_secret="s3cret", admin_session_seconds=3600)
    cookie = auth.cookie(token="abc", settings=plain)
    assert cookie == {
        "key": auth.COOKIE,
        "value": "abc",
        "max_age": 3600,
        "path": "/admin",
        "secure": False,
        "httponly": True,
        "samesite": "strict",
    }
    behind = settings(admin_secret="s3cret", public_origin="https://horizon.example.org")
    assert auth.cookie(token="abc", settings=behind)["secure"] is True
    assert auth.cookie(token="abc", settings=settings(public_origin="http://localhost:8000"))["secure"] is False
    dropped = auth.cookie(token="abc", settings=behind, drop=True)
    assert dropped["value"] == "" and dropped["max_age"] == 0 and dropped["secure"] is True and dropped["path"] == "/admin"


# -- the token of every form ----------------------------------------------------------


def test_the_form_token_is_made_from_the_session_s_secret_and_of_no_use_without_it() -> None:
    token = auth.csrf_token("a-secret-of-the-session")
    assert len(token) == 64 and token != "a-secret-of-the-session" and "a-secret" not in token
    assert auth.csrf_ok("a-secret-of-the-session", token)
    assert not auth.csrf_ok("a-secret-of-the-session", token[:-1] + ("0" if token[-1] != "0" else "1"))
    assert not auth.csrf_ok("a-secret-of-the-session", None) and not auth.csrf_ok("a-secret-of-the-session", "")
    assert auth.csrf_token("another-session") != token


def test_with_a_public_origin_a_request_that_changes_something_has_to_come_from_there() -> None:
    assert auth.origin_ok(settings(), "https://anywhere.example.org") and auth.origin_ok(settings(), None)
    kept = settings(public_origin="https://horizon.example.org")
    assert auth.origin_ok(kept, "https://horizon.example.org") and auth.origin_ok(kept, "https://horizon.example.org/")
    assert not auth.origin_ok(kept, "https://evil.example.org") and not auth.origin_ok(kept, "null")
    assert auth.origin_ok(kept, None), "a browser that says nothing is let through to the form's token"


# -- logins that fail -----------------------------------------------------------------


def test_logins_that_fail_are_counted_and_block_their_address_for_a_while(
    make_database: Callable[[], str], now: datetime
) -> None:
    engine = __import__("sqlalchemy").create_engine(make_database())
    try:
        keys = auth.keys_for("203.0.113.7")
        assert auth.blocked_until(engine, keys, now=now) is None
        for number in range(4):
            assert auth.failed(engine, keys, now=now) is None
        assert auth.blocked_until(engine, keys, now=now) is None, "four failures: allowed still"
        assert auth.failed(engine, keys, now=now) == now + timedelta(minutes=15), "the fifth blocks"
        assert auth.blocked_until(engine, keys, now=now + timedelta(minutes=14)) == now + timedelta(minutes=15)
        assert auth.blocked_until(engine, keys, now=now + timedelta(minutes=15)) is None, "the block ended"
        # Blocked again in a later window: longer. (A try while blocked is refused before it is counted.)
        late = now + timedelta(minutes=16)
        for number in range(4):
            assert auth.failed(engine, keys, now=late) is None
        assert auth.failed(engine, keys, now=late) == late + timedelta(minutes=30)
        # Quiet, and unblocked, for an hour: the key starts over.
        much_later = late + timedelta(minutes=30) + timedelta(hours=1, minutes=1)
        for number in range(4):
            assert auth.failed(engine, keys, now=much_later) is None
        assert auth.failed(engine, keys, now=much_later) == much_later + timedelta(minutes=15)
        # Another address is not blocked by this one's failures; the address is kept as a hash.
        assert auth.blocked_until(engine, auth.keys_for("203.0.113.8"), now=now) is None
        with Session(engine) as session:
            assert "203.0.113.7" not in str([row.key for row in session.scalars(select(AdminLoginThrottle))])
        # A login that worked starts the address over — not the overall count.
        auth.succeeded(engine, "203.0.113.7", now=much_later)
        assert auth.blocked_until(engine, auth.keys_for("203.0.113.7"), now=much_later) is None
        with Session(engine) as session:
            everyone = session.get(AdminLoginThrottle, auth.EVERYONE)
            assert everyone is not None and everyone.failures == 5, "the overall count of the window is kept"
    finally:
        engine.dispose()


def test_failures_from_many_addresses_are_counted_too_and_cannot_block_for_good(
    make_database: Callable[[], str], now: datetime
) -> None:
    engine = __import__("sqlalchemy").create_engine(make_database())
    try:
        for number in range(49):
            assert auth.failed(engine, auth.keys_for(f"198.51.100.{number}"), now=now) is None
        until = auth.failed(engine, auth.keys_for("198.51.100.200"), now=now)
        assert until == now + timedelta(minutes=15), "the fiftieth failure in the window blocks everybody"
        assert auth.blocked_until(engine, auth.keys_for("192.0.2.1"), now=now) == until
        # The next window starts the count over: nothing is blocked for good.
        after = until + timedelta(seconds=1)
        assert auth.blocked_until(engine, auth.keys_for("192.0.2.1"), now=after) is None
        assert auth.failed(engine, auth.keys_for("192.0.2.1"), now=after) is None
    finally:
        engine.dispose()
