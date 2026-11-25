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

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def alembic_config(database_url: str) -> Config:
    config = Config(PROJECT_ROOT / "alembic.ini")
    config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    config.attributes["configure_logger"] = False
    return config


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
