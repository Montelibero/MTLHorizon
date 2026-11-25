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
    # Stage 05. The API of the Notifier, to register what this installation
    # wants to be told of; the token it is asked with (an admin token, or a
    # user token of its "token" mode); and the address its webhooks are
    # sent to. With no API address, nothing is registered automatically:
    # subscriptions are made by hand and adopted here by their ids.
    # The token is a secret: it never reaches a log, a table or a message.
    notifier_api_url: str = ""
    notifier_api_token: str = ""
    notifier_callback_url: str = ""
    # Discovery of the holders of an asset: how often to run it, in
    # seconds; 0, the default, is never — only by hand.
    discovery_interval_seconds: float = 0.0
    # How often the worker looks whether an account needs a subscription or
    # a history import, in seconds.
    provision_interval_seconds: float = 60.0
    # One slice of a history import: what it may cost before the next
    # subject gets its turn. And what is fetched for a record before it is
    # stored: parents (the streams, and only the parents a record cannot be
    # stored without) or full.
    history_slice_requests: int = 50
    history_slice_seconds: float = 60.0
    history_context: str = "parents"
    # What one cycle may cost at most. What it does not reach is read in the
    # next cycle. The time budget is None for nine tenths of the interval.
    collector_max_requests: int = 2000
    collector_max_seconds: float | None = None
    # Stage 06. The admin UI under /admin is opened by this secret. With no
    # secret set it is closed to everyone — and says so. It is a secret: it
    # never reaches a log, a page or a message.
    admin_secret: str = ""
    # How long a session of the admin UI lasts, in seconds.
    admin_session_seconds: float = 12 * 3600
    # Where this installation is reached from behind the owner's proxy:
    # scheme, host and port, and nothing more. Set — the cookies of the admin
    # UI are Secure, a request that changes something has to come from there,
    # and the history API writes its links with it.
    public_origin: str = ""
    # The history API: ``token`` — only with one of the tokens; ``open`` —
    # anyone who reaches the API reads it. The tokens are secrets.
    api_access: str = "token"
    api_tokens: tuple[str, ...] = ()
    # For a check of the real servers its owner allowed: the most HTTP
    # attempts this installation may make for as long as the run lasts —
    # every client, every retry, every probe. 0, the default, is no cap.
    live_budget: int = 0

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
            collector_max_requests=_positive_whole(env, "COLLECTOR_MAX_REQUESTS", 2000),
            collector_max_seconds=_optional_positive_number(env, "COLLECTOR_MAX_SECONDS"),
            notifier_signer_keys=_signer_keys(env),
            notifier_api_url=_address(env, "NOTIFIER_API_URL", path_allowed=True),
            notifier_api_token=env.get(f"{ENV_PREFIX}NOTIFIER_API_TOKEN", "").strip(),
            notifier_callback_url=_address(env, "NOTIFIER_CALLBACK_URL", path_allowed=True),
            discovery_interval_seconds=_non_negative_number(env, "DISCOVERY_INTERVAL", 0.0),
            provision_interval_seconds=_positive_number(env, "PROVISION_INTERVAL", 60.0),
            history_slice_requests=_positive_whole(env, "HISTORY_SLICE_REQUESTS", 50),
            history_slice_seconds=_positive_number(env, "HISTORY_SLICE_SECONDS", 60.0),
            history_context=_history_context(env),
            admin_secret=env.get(f"{ENV_PREFIX}ADMIN_SECRET", "").strip(),
            admin_session_seconds=_positive_number(env, "ADMIN_SESSION_SECONDS", 12 * 3600),
            public_origin=_address(env, "PUBLIC_ORIGIN", path_allowed=False),
            api_access=_api_access(env),
            api_tokens=_tokens(env, "API_TOKENS"),
            live_budget=_whole_or_zero(env, "LIVE_BUDGET"),
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


def _non_negative_number(env: Mapping[str, str], name: str, default: float) -> float:
    """Zero or a positive number; zero is "never" where it can be."""
    raw = env.get(f"{ENV_PREFIX}{name}", "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ConfigError(f"{ENV_PREFIX}{name} must be a number, got {raw!r}") from None
    if value < 0 or value != value:
        raise ConfigError(f"{ENV_PREFIX}{name} must be zero or positive, got {raw!r}")
    return value


def _history_context(env: Mapping[str, str]) -> str:
    raw = env.get(f"{ENV_PREFIX}HISTORY_CONTEXT", "").strip() or "parents"
    if raw not in ("parents", "full"):
        raise ConfigError(f"{ENV_PREFIX}HISTORY_CONTEXT must be parents or full, got {raw!r}")
    return raw


def _address(env: Mapping[str, str], name: str, *, path_allowed: bool) -> str:
    """An optional http or https address. A user name or a password in it
    is kept out of every message."""
    value = env.get(f"{ENV_PREFIX}{name}", "").strip()
    if not value:
        return ""
    try:
        parts = urlsplit(value)
        port = parts.port
    except ValueError:
        raise ConfigError(f"{ENV_PREFIX}{name} is not an address") from None
    if parts.scheme not in ("http", "https") or not parts.hostname or port == 0:
        raise ConfigError(f"{ENV_PREFIX}{name}: {shown_url(value)} is not an http or https address")
    if not path_allowed and (parts.path not in ("", "/") or parts.query or parts.fragment):
        raise ConfigError(f"{ENV_PREFIX}{name}: must name a server and nothing more")
    return value


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


def _api_access(env: Mapping[str, str]) -> str:
    """Who reads the history API: only the holder of a token, or anyone."""
    raw = env.get(f"{ENV_PREFIX}API_ACCESS", "").strip() or "token"
    if raw not in ("token", "open"):
        raise ConfigError(f"{ENV_PREFIX}API_ACCESS must be token or open, got {raw!r}")
    return raw


def _tokens(env: Mapping[str, str], name: str) -> tuple[str, ...]:
    """Secrets given as a comma-separated list. Nothing is said of them."""
    return tuple(token for token in (part.strip() for part in env.get(f"{ENV_PREFIX}{name}", "").split(",")) if token)


def _whole_or_zero(env: Mapping[str, str], name: str) -> int:
    """Zero or a positive whole number; zero is "none"."""
    raw = env.get(f"{ENV_PREFIX}{name}", "").strip()
    if not raw:
        return 0
    if not raw.isdigit():
        raise ConfigError(f"{ENV_PREFIX}{name} must be a whole number, got {raw!r}")
    return int(raw)


def _positive_whole(env: Mapping[str, str], name: str, default: int) -> int:
    """A number of things: one or more. Half a request a slice would be a
    slice that never reads, over and over."""
    value = _positive_number(env, name, float(default))
    if value < 1:
        raise ConfigError(f"{ENV_PREFIX}{name} must be a whole number of one or more, got {env.get(f'{ENV_PREFIX}{name}', '')!r}")
    return int(value)


def _optional_positive_number(env: Mapping[str, str], name: str) -> float | None:
    """A positive number, or None when the variable is not set or empty."""
    if not env.get(f"{ENV_PREFIX}{name}", "").strip():
        return None
    return _positive_number(env, name, 0.0)
