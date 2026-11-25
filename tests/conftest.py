"""Test environment: a real PostgreSQL database migrated to head.

``MTL_HORIZON_TEST_DATABASE_URL`` must point at a PostgreSQL server. Its
database name is ignored: a fresh database is created for the test session,
migrated to head and dropped afterwards, so the tests never touch a database
that holds anything else.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session

from app import cli_common
from app.config import Settings, shown_url

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def alembic_config(database_url: str) -> Config:
    config = Config(PROJECT_ROOT / "alembic.ini")
    config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    config.attributes["configure_logger"] = False
    return config


@pytest.fixture(autouse=True)
def no_real_servers(monkeypatch: pytest.MonkeyPatch) -> None:
    """No test reaches a real server.

    A pool a command builds reads its servers from the environment. A test
    that means a command to talk to a fake server provides it
    (``tests.fake_horizon.serve``); whatever is not provided refuses the
    connection, so a test that forgot to provide one fails here and does not
    go out into the world.
    """
    from app import cli_common

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"this test provided no fake server for {request.url.host}", request=request)

    monkeypatch.setattr(cli_common, "TRANSPORTS", {shown_url(url): httpx.MockTransport(refuse) for url in Settings.from_env().horizon_urls})
    monkeypatch.setattr(cli_common, "make_client", _guarded(cli_common.make_client))


def _guarded(make_client: Callable[..., Any]) -> Callable[..., Any]:
    """``make_client`` as it is, but never without a transport for a server:
    one that is not a fake in this test gets one that refuses."""

    def guarded(settings: Settings, engine: Engine | None = None, **options: Any) -> Any:
        transports = {**cli_common.TRANSPORTS}
        for url in settings.horizon_urls:
            transports.setdefault(shown_url(url), httpx.MockTransport(_refuse))
        saved = cli_common.TRANSPORTS
        cli_common.TRANSPORTS = transports
        try:
            return make_client(settings, engine, **options)
        finally:
            cli_common.TRANSPORTS = saved

    return guarded


def _refuse(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError(f"this test provided no fake server for {request.url.host}", request=request)


@pytest.fixture(scope="session")
def server_url() -> Iterator[str]:
    configured = os.environ.get("MTL_HORIZON_TEST_DATABASE_URL", "").strip()
    if not configured:
        pytest.skip("MTL_HORIZON_TEST_DATABASE_URL is not set")
    yield configured


def create_scratch_database(server_url: str, migrate: bool) -> Iterator[str]:
    """Yield the URL of a new database on the server, empty or at head."""
    admin = make_url(server_url).set(database="postgres")
    name = f"mtl_horizon_test_{uuid.uuid4().hex[:12]}"
    control = create_engine(admin, isolation_level="AUTOCOMMIT")
    with control.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    control.dispose()
    try:
        url = make_url(server_url).set(database=name).render_as_string(hide_password=False)
        if migrate:
            previous = os.environ.get("MTL_HORIZON_DATABASE_URL")
            os.environ["MTL_HORIZON_DATABASE_URL"] = url
            try:
                command.upgrade(alembic_config(url), "head")
            finally:
                if previous is None:
                    os.environ.pop("MTL_HORIZON_DATABASE_URL", None)
                else:
                    os.environ["MTL_HORIZON_DATABASE_URL"] = previous
        yield url
    finally:
        with control.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        control.dispose()


@pytest.fixture(scope="session")
def database_url(server_url: str) -> Iterator[str]:
    yield from create_scratch_database(server_url, migrate=True)


@pytest.fixture(scope="session")
def engine(database_url: str) -> Iterator[Engine]:
    engine = create_engine(database_url)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def template_url(server_url: str) -> Iterator[str]:
    """A migrated empty database that nothing connects to, so that it can be
    copied."""
    yield from create_scratch_database(server_url, migrate=True)


@pytest.fixture
def fresh_database_url(server_url: str, template_url: str) -> Iterator[str]:
    """A private database for one test, empty and at head: a copy of the
    template, made in milliseconds. For code that commits on its own, which
    an enclosing rollback could not undo."""
    template = make_url(template_url).set(database="postgres")
    name = f"mtl_horizon_test_{uuid.uuid4().hex[:12]}"
    control = create_engine(template, isolation_level="AUTOCOMMIT")
    with control.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}" TEMPLATE "{make_url(template_url).database}"'))
    control.dispose()
    try:
        yield make_url(template_url).set(database=name).render_as_string(hide_password=False)
    finally:
        with control.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        control.dispose()


@pytest.fixture
def make_database(server_url: str, template_url: str) -> Iterator[Callable[[], str]]:
    """A factory of private databases, each a copy of the template; all are
    dropped when the test ends. For tests that compare two databases."""
    control = create_engine(make_url(template_url).set(database="postgres"), isolation_level="AUTOCOMMIT")
    created: list[str] = []

    def make() -> str:
        name = f"mtl_horizon_test_{uuid.uuid4().hex[:12]}"
        with control.connect() as conn:
            conn.execute(text(f'CREATE DATABASE "{name}" TEMPLATE "{make_url(template_url).database}"'))
        created.append(name)
        return make_url(template_url).set(database=name).render_as_string(hide_password=False)

    try:
        yield make
    finally:
        with control.connect() as conn:
            for name in created:
                conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        control.dispose()


@pytest.fixture
def unmigrated_database_url(server_url: str) -> Iterator[str]:
    yield from create_scratch_database(server_url, migrate=False)


@pytest.fixture
def session(engine: Engine) -> Iterator[Session]:
    """A session whose writes vanish when the test ends.

    Everything a test writes runs inside one outer transaction that is rolled
    back afterwards, so tests are independent and the schema is exercised
    exactly as deployed. ``session.commit()`` is allowed inside a test: it
    only commits a savepoint.
    """
    connection = engine.connect()
    outer = connection.begin()
    try:
        yield Session(bind=connection, join_transaction_mode="create_savepoint")
    finally:
        outer.rollback()
        connection.close()
