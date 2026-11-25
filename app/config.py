"""Runtime settings.

Everything configurable comes from environment variables with the
``MTL_HORIZON_`` prefix. No secrets are stored in the repository.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit

ENV_PREFIX = "MTL_HORIZON_"

# Stage 01 targets the public network only. The database records the network
# it belongs to (table ``network_identity``), so one database can never mix
# pubnet and testnet data.
PUBLIC_NETWORK_PASSPHRASE = "Public Global Stellar Network ; September 2015"
DEFAULT_HORIZON_URL = "https://horizon.stellar.org"


def shown_url(url: str) -> str:
    """A server's address as it is shown and named: scheme, host and port.

    What may be secret in an address — a user name and a password before
    the host, a key in a path or a query — stays out of every name, table
    and line of a log. An address that cannot be read is not shown at all.
    """
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        return "<unreadable address>"
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return "<not an http address>"
    return f"{parts.scheme}://{parts.hostname}" + ("" if port is None else f":{port}")


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
    # Source of history: one Horizon or several, the first one being the
    # primary. Read-only; the public server unless overridden. The pace is
    # one for all of them together: more servers do not make it faster. The
    # public server publishes no limit, so the client keeps to one request
    # per second by default.
    horizon_urls: tuple[str, ...] = (DEFAULT_HORIZON_URL,)
    horizon_requests_per_second: float = 1.0
    # Requests in flight at once. Irrelevant below a few requests per second;
    # kept because latency, not rate, is the limit once the pace is raised.
    horizon_concurrency: int = 1
    # How long what a server said of itself (its network, its oldest and
    # newest ledger) is trusted before it is asked again, in seconds.
    horizon_health_check_interval: float = 300.0
    # How long a server that did not answer is left alone before it is asked
    # again, in seconds. It doubles each time it fails again.
    horizon_failover_cooldown: float = 60.0
    # Live collection: one cycle every so many seconds.
    collector_interval_seconds: float = 900.0
    # The keys Operations Notifier signs its webhooks with, as Stellar public
    # keys (what its own /api/status shows as publicKey). With none set, no
    # webhook is accepted: the endpoint is there, and answers that it is not
    # configured. Nothing is logged of a key's neighbours in the environment.
    notifier_signer_keys: tuple[str, ...] = ()
    # What one cycle may cost at most. What it does not reach is read in the
    # next cycle. The time budget is None for nine tenths of the interval.
    collector_max_requests: int = 2000
    collector_max_seconds: float | None = None

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Settings":
        env = os.environ if env is None else env
        name = f"{ENV_PREFIX}API_PORT"
        port = env.get(name, "").strip() or "8000"
        if not port.isdigit() or not 0 < int(port) < 65536:
            raise ConfigError(f"{name} must be a TCP port number, got {port!r}")
        return cls(
            database_url=env.get(f"{ENV_PREFIX}DATABASE_URL", "").strip(),
            network_passphrase=(
                env.get(f"{ENV_PREFIX}NETWORK_PASSPHRASE", "").strip() or PUBLIC_NETWORK_PASSPHRASE
            ),
            api_host=env.get(f"{ENV_PREFIX}API_HOST", "").strip() or "127.0.0.1",
            api_port=int(port),
            horizon_urls=_horizon_urls(env),
            horizon_requests_per_second=_positive_number(env, "REQUESTS_PER_SECOND", 1.0),
            horizon_concurrency=int(_positive_number(env, "CONCURRENCY", 1.0)),
            horizon_health_check_interval=_positive_number(env, "HEALTH_CHECK_INTERVAL", 300.0),
            horizon_failover_cooldown=_positive_number(env, "FAILOVER_COOLDOWN", 60.0),
            collector_interval_seconds=_positive_number(env, "COLLECTOR_INTERVAL", 900.0),
            collector_max_requests=int(_positive_number(env, "COLLECTOR_MAX_REQUESTS", 2000.0)),
            collector_max_seconds=_optional_positive_number(env, "COLLECTOR_MAX_SECONDS"),
            notifier_signer_keys=_signer_keys(env),
        )


def _horizon_urls(env: Mapping[str, str]) -> tuple[str, ...]:
    """The servers of the pool, the primary first.

    ``MTL_HORIZON_URLS`` is a comma-separated list. ``MTL_HORIZON_URL`` is
    the one server of the earlier stages; when both are set they have to
    agree on the primary, or it is not said which one that is. An address
    names a server and nothing more: no path, no query.
    """
    listed, single = env.get(f"{ENV_PREFIX}URLS", "").strip(), env.get(f"{ENV_PREFIX}URL", "").strip()
    if not listed:
        return (_server_address(single, f"{ENV_PREFIX}URL"),) if single else (DEFAULT_HORIZON_URL,)
    urls: list[str] = []
    seen: set[tuple[str, str, int]] = set()
    for item in listed.split(","):
        if not item.strip():
            raise ConfigError(f"{ENV_PREFIX}URLS has an empty address: two commas in a row, or a trailing one")
        url = _server_address(item.strip(), f"{ENV_PREFIX}URLS")
        if _identity(url) in seen:
            raise ConfigError(f"{ENV_PREFIX}URLS names {shown_url(url)} twice")
        seen.add(_identity(url))
        urls.append(url)
    if single and _identity(_server_address(single, f"{ENV_PREFIX}URL")) != _identity(urls[0]):
        raise ConfigError(
            f"{ENV_PREFIX}URL names {shown_url(single)}, but the first address of {ENV_PREFIX}URLS "
            f"is {shown_url(urls[0])}: keep one — the primary is the first of the list"
        )
    return tuple(urls)


def _server_address(value: str, name: str) -> str:
    """A server's address without its trailing slash; ConfigError when it
    cannot be one. What may be secret in it is kept out of the message."""
    shown = shown_url(value)
    try:
        parts = urlsplit(value)
        port = parts.port
    except ValueError:
        raise ConfigError(f"{name}: {shown} is not an address") from None
    if parts.scheme not in ("http", "https") or not parts.hostname or port == 0:
        raise ConfigError(f"{name}: {shown} is not an http or https address")
    if (parts.path and parts.path != "/") or parts.query or parts.fragment:
        raise ConfigError(f"{name}: {shown} must name a server and nothing more: no path, no query")
    return value.rstrip("/")


def _identity(url: str) -> tuple[str, str, int]:
    parts = urlsplit(url)
    return parts.scheme.lower(), (parts.hostname or "").lower(), parts.port or (443 if parts.scheme == "https" else 80)


def _signer_keys(env: Mapping[str, str]) -> tuple[str, ...]:
    """``MTL_HORIZON_NOTIFIER_KEYS``: Stellar public keys, comma-separated.
    An address that is not one is refused, and so is one named twice."""
    from app.notifier.signature import checked_key

    raw = env.get(f"{ENV_PREFIX}NOTIFIER_KEYS", "").strip()
    keys: list[str] = []
    for item in raw.split(",") if raw else ():
        if not item.strip():
            raise ConfigError(f"{ENV_PREFIX}NOTIFIER_KEYS has an empty key: two commas in a row, or a trailing one")
        try:
            key = checked_key(item)
        except ValueError:
            raise ConfigError(f"{ENV_PREFIX}NOTIFIER_KEYS holds what is not a Stellar public key (the {len(keys) + 1}. one)") from None
        if key in keys:
            raise ConfigError(f"{ENV_PREFIX}NOTIFIER_KEYS names a key twice (the {len(keys) + 1}. one)")
        keys.append(key)
    return tuple(keys)


def _positive_number(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(f"{ENV_PREFIX}{name}", "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ConfigError(f"{ENV_PREFIX}{name} must be a number, got {raw!r}") from None
    if value <= 0 or value != value:
        raise ConfigError(f"{ENV_PREFIX}{name} must be positive, got {raw!r}")
    return value


def _optional_positive_number(env: Mapping[str, str], name: str) -> float | None:
    """A positive number, or None when the variable is not set or empty."""
    if not env.get(f"{ENV_PREFIX}{name}", "").strip():
        return None
    return _positive_number(env, name, 0.0)
