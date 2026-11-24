"""FastAPI application factory with the liveness and readiness probes.

``GET /health`` answers as long as the process can serve requests; it does
not touch the database. ``GET /ready`` answers 200 only when the process can
actually do its job: the database answers, its schema is at the head of the
migration chain, and it belongs to the configured Stellar network. Anything
else is a 503 naming the failing check.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app import __version__
from app.config import Settings
from app.storage.db import create_db_engine
from app.storage.network import NetworkMismatch, assert_network

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _alembic_config() -> Config:
    # Absolute paths: the probes must work from any working directory.
    config = Config(PROJECT_ROOT / "alembic.ini")
    config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    return config


def readiness_checks(engine: Engine | None, network_passphrase: str) -> dict[str, str]:
    """Map of check name to ``"ok"`` or a short reason. Never includes credentials."""
    if engine is None:
        return {"database": "not configured"}

    checks: dict[str, str] = {}
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            checks["database"] = "ok"

            # get_heads() returns revision ids; get_current_heads() the ones applied.
            expected = sorted(ScriptDirectory.from_config(_alembic_config()).get_heads())
            applied = sorted(MigrationContext.configure(conn).get_current_heads())
            if not applied:
                checks["migrations"] = "not applied"
            elif applied != expected:
                checks["migrations"] = "not at head"
            else:
                checks["migrations"] = "ok"

            if checks["migrations"] == "ok":
                # The network table only exists once the schema does.
                try:
                    assert_network(conn, network_passphrase)
                    checks["network"] = "ok"
                except NetworkMismatch:
                    checks["network"] = "database belongs to another network"
            else:
                checks["network"] = "not checked"
            conn.commit()
    except SQLAlchemyError:
        checks["database"] = "unavailable"
        checks.setdefault("migrations", "not checked")
        checks.setdefault("network", "not checked")
    return checks


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings if settings is not None else Settings.from_env()
    engine = create_db_engine(settings.database_url) if settings.database_url else None

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        if engine is not None:
            engine.dispose()

    app = FastAPI(title="MTL Horizon", version=__version__, lifespan=lifespan)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    def ready() -> JSONResponse:
        checks = readiness_checks(engine, settings.network_passphrase)
        healthy = all(value == "ok" for value in checks.values())
        body: dict[str, Any] = {"status": "ok" if healthy else "unavailable", "checks": checks}
        return JSONResponse(body, status_code=200 if healthy else 503)

    return app
