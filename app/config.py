"""Runtime settings.

Everything configurable comes from environment variables with the
``MTL_HORIZON_`` prefix. No secrets are stored in the repository.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

ENV_PREFIX = "MTL_HORIZON_"

# Stage 01 targets the public network only. The database records the network
# it belongs to (table ``network_identity``), so one database can never mix
# pubnet and testnet data.
PUBLIC_NETWORK_PASSPHRASE = "Public Global Stellar Network ; September 2015"


class ConfigError(ValueError):
    """Raised when an environment variable holds an unusable value."""


@dataclass(frozen=True)
class Settings:
    """Process-wide settings."""

    # Empty when the process has no database configured; the API then serves
    # only the liveness probe and reports itself as not ready.
    database_url: str
    network_passphrase: str = PUBLIC_NETWORK_PASSPHRASE
    # 127.0.0.1 for a bare `uvicorn` run; the Docker image overrides this
    # because a container port must be bound on all interfaces to be
    # publishable, and the host side of the mapping is what stays local.
    api_host: str = "127.0.0.1"
    api_port: int = 8000

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Settings":
        env = os.environ if env is None else env
        port = env.get(f"{ENV_PREFIX}API_PORT", "").strip() or "8000"
        if not port.isdigit() or not 0 < int(port) < 65536:
            raise ConfigError(f"{ENV_PREFIX}API_PORT must be a TCP port number, got {port!r}")
        return cls(
            database_url=env.get(f"{ENV_PREFIX}DATABASE_URL", "").strip(),
            network_passphrase=(
                env.get(f"{ENV_PREFIX}NETWORK_PASSPHRASE", "").strip() or PUBLIC_NETWORK_PASSPHRASE
            ),
            api_host=env.get(f"{ENV_PREFIX}API_HOST", "").strip() or "127.0.0.1",
            api_port=int(port),
        )
