"""Pieces the command lines share: logging, the database, the client of
the Horizon pool, small reports. ``python -m app.backfill``,
``python -m app.collector``, ``python -m app.pool`` and ``python -m
app.worker`` are built from these.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.config import ConfigError, Settings
from app.storage import pace, pool_state
from app.storage.pace import Pace
from app.storage.db import create_db_engine
from app.storage.network import NetworkMismatch, assert_network
from app.sources.horizon import HorizonClient, HorizonSettings
from app.sources.pool import HorizonPool, PoolSettings

Command = Callable[[Any, Settings], int]

# Transports by the name a server is shown under. Tests put their fake
# servers here; nothing else does.
TRANSPORTS: dict[str, httpx.BaseTransport] = {}

_ADDRESS = re.compile(r"\bG[A-Z0-9]{55}\b")


def engine_for(settings: Settings) -> Engine:
    """The history database, after checking it belongs to the configured network."""
    if not settings.database_url:
        raise ConfigError("MTL_HORIZON_DATABASE_URL is not set")
    engine = create_db_engine(settings.database_url)
    try:
        with engine.connect() as connection, connection.begin():
            assert_network(connection, settings.network_passphrase)
    except (SQLAlchemyError, NetworkMismatch) as error:
        engine.dispose()
        raise ConfigError(f"cannot use the database: {error}") from error
    return engine


# How the database names the place history is read from. One name for the
# whole pool, whatever servers it is made of and whatever order they are
# listed in: a checkpoint is about a ledger, not about a server, and every
# server of the network answers for the same ledgers. Which server read
# what is kept apart, in ``sync_coverage.served_by``.
LOGICAL_SOURCE = "horizon:pool"


def schema_checks(engine: Engine, settings: Settings) -> dict[str, str]:
    """Whether the database is ready for a service to work on it: it
    answers, its schema is at the head of the migrations, it is of the
    configured network. A map of check name to ``"ok"`` or the reason."""
    from app.api.app import readiness_checks

    return readiness_checks(engine, settings.network_passphrase)


def require_schema(engine: Engine, settings: Settings, logger: logging.Logger) -> bool:
    """For a service about to start: say what is not ready, and False if
    anything is — the service is to exit and let the owner migrate first,
    not to fail query by query."""
    checks = _quietly(schema_checks, engine, settings)
    not_ok = {name: reason for name, reason in checks.items() if reason != "ok"}
    if not_ok:
        logger.error(
            "not starting: the database is not ready (%s); apply the migrations first: "
            "`docker compose run --rm api alembic upgrade head`",
            "; ".join(f"{name}: {reason}" for name, reason in not_ok.items()),
        )
        return False
    return True


def _quietly(check: Callable[..., dict[str, str]], engine: Engine, settings: Settings) -> dict[str, str]:
    """The check with what Alembic says of itself while it compares the
    schema left out of the log: a service that starts is not the place for
    ten lines about the plugins of a migration."""
    talkers = [logging.getLogger(name) for name in ("alembic.runtime.plugins", "alembic.runtime.migration")]
    levels = [talker.level for talker in talkers]
    for talker in talkers:
        talker.setLevel(logging.WARNING)
    try:
        return check(engine, settings)
    finally:
        for talker, level in zip(talkers, levels):
            talker.setLevel(level)


def source_name(client: HorizonClient) -> str:
    """The pool as its checkpoints and coverage name it."""
    return LOGICAL_SOURCE


def source_name_for(settings: Settings) -> str:
    return LOGICAL_SOURCE


def make_client(settings: Settings, engine: Engine | None = None, *, share: str = pace.BACKGROUND) -> HorizonPool:
    """The pool of Horizon servers, which every request to Horizon goes
    through — whoever asks and whichever command.

    With a database the pace is kept there for every process together, and
    what is seen of a server is kept for a status page. ``share`` says what
    the process does: the worker that handles events has the right of way,
    everything else reads in the background.
    """
    return HorizonPool(
        settings.horizon_urls,
        HorizonSettings(requests_per_second=settings.horizon_requests_per_second),
        network_passphrase=settings.network_passphrase,
        limiter=Pace(engine, share=share) if engine is not None else None,
        remember=None if engine is None else (lambda server: pool_state.note(engine, server.as_dict())),
        pool_settings=PoolSettings(
            health_check_interval=settings.horizon_health_check_interval,
            failover_cooldown=settings.horizon_failover_cooldown,
        ),
        transports=TRANSPORTS or None,
    )


def short_name(name: str) -> str:
    """A stream name with accounts cut to what tells them apart in a table."""
    return _ADDRESS.sub(lambda match: match.group(0)[:6] + "…", name)


def print_table(title: str, rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> None:
    widths = {column: max([len(column)] + [len(str(row.get(column, ""))) for row in rows]) for column in columns}
    print(title)
    print("  " + "  ".join(column.ljust(widths[column]) for column in columns))
    for row in rows:
        print("  " + "  ".join(str(row.get(column, "")).ljust(widths[column]) for column in columns))


def write_report(path: str, report: Mapping[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=1, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    print(f"report written to {target}")


class JsonLines(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "time": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            },
            ensure_ascii=False,
        )


def configure_logging(log_format: str, level: str, log_file: str | None = None):
    """Log to stderr and, when asked, to a file as well. A container's output
    is not always at hand while the process runs; the file is. Returns the
    stream the logs go to."""
    formatter = (
        JsonLines()
        if log_format == "json"
        else logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s", "%H:%M:%S")
    )
    stream: Any = sys.stderr
    handlers: list[logging.Handler] = [logging.StreamHandler(stream)]
    if log_file:
        stream = open(log_file, "a", encoding="utf-8")  # noqa: SIM115 - lives as long as the process
        handlers.append(logging.StreamHandler(stream))
    for handler in handlers:
        handler.setFormatter(formatter)
    logging.basicConfig(level=getattr(logging, level.upper()), handlers=handlers, force=True)
    # One line per request is noise next to the command's own events.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return stream
