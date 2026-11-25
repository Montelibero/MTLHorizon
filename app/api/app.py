"""FastAPI application factory with the liveness and readiness probes, and
the endpoint Operations Notifier posts to.

``GET /health`` answers as long as the process can serve requests; it does
not touch the database. ``GET /ready`` answers 200 only when the process can
actually do its job: the database answers, its schema is at the head of the
migration chain, and it belongs to the configured Stellar network. Anything
else is a 503 naming the failing check.

``POST /webhooks/notifier`` takes what Operations Notifier posts
(:mod:`app.notifier.format`), and only that: every request is checked
against the keys the Notifier signs with before anything in it is read. A
delivery is kept — and its transaction queued — and only then is it answered
``2xx``, which is what tells the Notifier to stop sending it. Nothing is
done here that takes time: no Horizon is asked, no work is done; that is the
worker's (``python -m app.worker``).

    2xx  kept (now, or by an earlier delivery of the same notification);
    400  signed, and not a notification: nothing to keep it as;
    401  not signed by a key this installation knows: nothing is read;
    413  larger than this endpoint takes: nothing is read;
    503  no key is configured, or there is no database: nothing is kept,
         and the Notifier will send again.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from fastapi import FastAPI, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app import __version__
from app.config import Settings
from app.notifier import format as notifier
from app.notifier import signature
from app.storage import events
from app.storage.db import create_db_engine
from app.storage.network import NetworkMismatch, assert_network

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WEBHOOK_PATH = "/webhooks/notifier"
# A notification carries two XDR envelopes and whatever a contract call
# passed; a few kilobytes, at most some hundreds of them.
WEBHOOK_BODY_LIMIT = 2 * 1024 * 1024


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


class TooLarge(Exception):
    """The body is larger than the endpoint takes."""


async def _body(request: Request) -> bytes:
    """The body as it came, byte for byte: that is what was signed."""
    body = bytearray()
    chunks: AsyncIterator[bytes] = request.stream()
    async for chunk in chunks:
        body += chunk
        if len(body) > WEBHOOK_BODY_LIMIT:
            raise TooLarge
    return bytes(body)


def _answer(status: int, **body: Any) -> JSONResponse:
    return JSONResponse(body, status_code=status)


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

    @app.post(WEBHOOK_PATH)
    async def notifier_webhook(request: Request) -> Response:
        """What Operations Notifier posts. See the module: kept first, answered
        after; nothing read of an unsigned request."""
        if not settings.notifier_signer_keys or engine is None:
            detail = (
                "no Notifier signing key is configured (MTL_HORIZON_NOTIFIER_KEYS)"
                if not settings.notifier_signer_keys
                else "there is no database to keep notifications in"
            )
            return _answer(503, detail=detail)
        try:
            body = await _body(request)
        except TooLarge:
            return _answer(413, detail=f"the body is larger than {WEBHOOK_BODY_LIMIT} bytes")
        signed_by = signature.signed_by(settings.notifier_signer_keys, body, request.headers.get(signature.HEADER))
        if signed_by is None:
            logger.warning("a request to %s was not signed by a key this installation knows; nothing was read", WEBHOOK_PATH)
            return _answer(401, detail="not signed by a Notifier this installation knows")
        try:
            notice = notifier.read(json.loads(body))
        except ValueError:  # not JSON, or not a notification
            return _answer(400, detail="signed, and not a notification that can be kept")
        except notifier.Unreadable as error:
            return _answer(400, detail=f"signed, and not a notification that can be kept: {error}")
        try:
            kept = await run_in_threadpool(
                events.receive,
                engine,
                notice,
                received_at=datetime.now(UTC),
                headers=dict(request.headers),
                signed_by=signed_by,
            )
        except Exception:  # noqa: BLE001 - whatever went wrong, nothing was kept for sure
            # Not answered with a success: the Notifier sends again.
            logger.exception("notification %s could not be kept", notice.id)
            return _answer(500, detail="not kept; send it again")
        return _answer(200, ok=True, stored=kept.created, state=kept.event.state, job_id=kept.event.job_id)

    return app
