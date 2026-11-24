"""Liveness and readiness probes."""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.api.app import create_app
from app.config import PUBLIC_NETWORK_PASSPHRASE, Settings

TESTNET = "Test SDF Network ; September 2015"
# Nothing listens on this port, so the database is unreachable.
UNREACHABLE = "postgresql+psycopg://user:not-a-password@127.0.0.1:1/mtl_horizon"


def test_health_needs_no_database() -> None:
    with TestClient(create_app(Settings(database_url=""))) as client:
        health = client.get("/health")
        ready = client.get("/ready")

    assert health.status_code == 200
    assert health.json() == {"status": "ok"}
    assert ready.status_code == 503
    assert ready.json()["checks"] == {"database": "not configured"}


def test_ready_on_a_migrated_database(database_url: str) -> None:
    settings = Settings(database_url=database_url, network_passphrase=PUBLIC_NETWORK_PASSPHRASE)
    with TestClient(create_app(settings)) as client:
        health = client.get("/health")
        ready = client.get("/ready")

    assert health.status_code == 200
    assert ready.status_code == 200
    assert ready.json() == {
        "status": "ok",
        "checks": {"database": "ok", "migrations": "ok", "network": "ok"},
    }


def test_not_ready_when_database_is_unreachable() -> None:
    with TestClient(create_app(Settings(database_url=UNREACHABLE))) as client:
        assert client.get("/health").status_code == 200
        ready = client.get("/ready")

    assert ready.status_code == 503
    assert ready.json()["checks"]["database"] == "unavailable"
    # The response never carries the credentials of the connection string.
    assert "not-a-password" not in ready.text


def test_not_ready_before_migrations_ran(unmigrated_database_url: str) -> None:
    settings = Settings(database_url=unmigrated_database_url)
    with TestClient(create_app(settings)) as client:
        ready = client.get("/ready")

    assert ready.status_code == 503
    # The database answers; the network is only checked once the schema exists.
    assert ready.json()["checks"] == {
        "database": "ok",
        "migrations": "not applied",
        "network": "not checked",
    }


def test_not_ready_for_another_network(database_url: str) -> None:
    settings = Settings(database_url=database_url, network_passphrase=TESTNET)
    with TestClient(create_app(settings)) as client:
        ready = client.get("/ready")

    assert ready.status_code == 503
    assert ready.json()["checks"] == {
        "database": "ok",
        "migrations": "ok",
        "network": "database belongs to another network",
    }
