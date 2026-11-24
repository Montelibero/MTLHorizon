"""Network identity guard.

A history database belongs to exactly one Stellar network. The row in
``network_identity`` is written by the first migration (public network); this
module refuses to use a database that belongs to another network than the one
configured, so pubnet and testnet data can never end up in one database.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection

_SELECT = text("SELECT network_passphrase FROM network_identity WHERE network_id = 1")


class NetworkMismatch(RuntimeError):
    """The database and the process are configured for different networks."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def database_network(conn: Connection) -> str | None:
    """Return the passphrase of the network the database belongs to, if bound."""
    return conn.execute(_SELECT).scalar_one_or_none()


def assert_network(conn: Connection, network_passphrase: str) -> None:
    """Raise :class:`NetworkMismatch` unless ``conn`` is bound to the network."""
    bound = database_network(conn)
    if bound is None:
        raise NetworkMismatch("database is not bound to any Stellar network")
    if bound != network_passphrase:
        raise NetworkMismatch("database belongs to a different Stellar network")
