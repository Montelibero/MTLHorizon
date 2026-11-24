"""The database is bound to one Stellar network."""

from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from app.config import PUBLIC_NETWORK_PASSPHRASE
from app.storage.network import NetworkMismatch, assert_network, database_network

TESTNET = "Test SDF Network ; September 2015"


def test_migrated_database_belongs_to_the_public_network(session: Session) -> None:
    assert database_network(session.connection()) == PUBLIC_NETWORK_PASSPHRASE
    assert_network(session.connection(), PUBLIC_NETWORK_PASSPHRASE)


def test_another_network_is_refused(session: Session) -> None:
    with pytest.raises(NetworkMismatch):
        assert_network(session.connection(), TESTNET)
