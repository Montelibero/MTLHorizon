"""Alembic environment.

The database URL comes from the environment variable
``MTL_HORIZON_DATABASE_URL``. It is deliberately not read from alembic.ini,
so that no credentials are stored in the repository.
"""

from __future__ import annotations

import os

from alembic import context
from sqlalchemy import create_engine, pool

from app.storage.models import Base

config = context.config
target_metadata = Base.metadata


def database_url() -> str:
    url = os.environ.get("MTL_HORIZON_DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("Set MTL_HORIZON_DATABASE_URL (see .env.example) to run migrations")
    return url


def run_migrations_offline() -> None:
    """Emit the migration SQL instead of running it."""
    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(database_url(), poolclass=pool.NullPool)
    try:
        with engine.connect() as connection:
            context.configure(connection=connection, target_metadata=target_metadata)
            with context.begin_transaction():
                context.run_migrations()
    finally:
        engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
