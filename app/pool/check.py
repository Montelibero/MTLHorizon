"""A check of the pool: every server on its own, by real requests to it.

This is what the ``pool check`` command shows and what a status page will
call. Nothing of it is failover: a request that a server does not answer is
not asked of another in its place — the point is to see what each server
does. The requests go through the pool's pace and count in its statistics.

What is measured is the time of a whole HTTP answer, not a ping: the root
of the server and one small request for history (a ledger just below its
latest), each up to ``repeats`` times — and not a third time after two in a
row without an answer. The time shown is the median of the answers that
came.
"""

from __future__ import annotations

import statistics
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from app.sources.pool import HorizonPool

MAX_REPEATS = 3
# How the rate limit headers are written by servers, by their lowercase names.
RATE_LIMIT_HEADERS = {
    "retry-after": "Retry-After",
    "x-ratelimit-limit": "X-RateLimit-Limit",
    "x-ratelimit-remaining": "X-RateLimit-Remaining",
    "x-ratelimit-reset": "X-RateLimit-Reset",
}


@dataclass(frozen=True)
class Sample:
    """One request, as it went."""

    seconds: float
    status: int | None  # None: no answer in time
    error: str | None
    headers: Mapping[str, str]
    body: dict[str, Any] | None

    @property
    def answered(self) -> bool:
        return self.status is not None

    @property
    def timeout(self) -> bool:
        return self.status is None

    @property
    def ok(self) -> bool:
        return self.error is None and self.status is not None and 200 <= self.status < 300 and self.body is not None

    def rate_limit(self) -> dict[str, str]:
        """The rate limit headers the server sent; empty when it sent none.
        Nothing is invented in their place."""
        found = {RATE_LIMIT_HEADERS[name.lower()]: value for name, value in self.headers.items() if name.lower() in RATE_LIMIT_HEADERS}
        for name, value in self.headers.items():
            if name.lower().startswith("x-ratelimit-") and name.lower() not in RATE_LIMIT_HEADERS:
                found["X-RateLimit-" + name.split("-", 2)[2].title()] = value  # a limiter's own
        return found

    def as_dict(self) -> dict[str, Any]:
        return {
            "seconds": round(self.seconds, 4),
            "status": self.status,
            "error": self.error,
            "rate_limit_headers": self.rate_limit(),
        }


@dataclass
class NodeCheck:
    """What one server did under the check."""

    url: str
    position: int
    root: list[Sample] = field(default_factory=list)
    history: list[Sample] = field(default_factory=list)
    # The ledger the request for history asked for.
    ledger: int | None = None
    network_passphrase: str | None = None
    horizon_version: str | None = None
    latest_ledger: int | None = None
    elder_ledger: int | None = None
    latest_closed_at: datetime | None = None
    network_ok: bool | None = None
    lag_ledgers: int | None = None
    available: bool = False
    reason: str = ""
    checked_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def take_root(self, root: Mapping[str, Any], expected_network: str | None) -> None:
        self.network_passphrase = root.get("network_passphrase")
        self.network_ok = None if expected_network is None else self.network_passphrase == expected_network
        self.horizon_version = root.get("horizon_version")
        self.latest_ledger = _whole(root.get("history_latest_ledger"))
        self.elder_ledger = _whole(root.get("history_elder_ledger"))
        self.latest_closed_at = _moment(root.get("history_latest_ledger_closed_at"))

    @property
    def root_ok(self) -> bool:
        return any(sample.ok for sample in self.root)

    @property
    def history_ok(self) -> bool:
        return any(sample.ok and self.ledger is not None and sample.body.get("sequence") == self.ledger for sample in self.history)

    @staticmethod
    def _median(samples: list[Sample]) -> int | None:
        seconds = [sample.seconds for sample in samples if sample.ok]
        return None if not seconds else round(statistics.median(seconds) * 1000)

    @property
    def root_ms(self) -> int | None:
        return self._median(self.root)

    @property
    def history_ms(self) -> int | None:
        return self._median(self.history)

    @staticmethod
    def _last_status(samples: list[Sample]) -> int | None:
        return next((sample.status for sample in reversed(samples) if sample.status is not None), None)

    @staticmethod
    def _timeouts(samples: list[Sample]) -> int:
        return sum(1 for sample in samples if sample.timeout)

    @staticmethod
    def _errors(samples: list[Sample]) -> int:
        return sum(1 for sample in samples if sample.answered and not sample.ok)

    @property
    def rate_limit_headers(self) -> dict[str, str]:
        """Those of the last answer that carried any."""
        for sample in reversed(self.root + self.history):
            found = sample.rate_limit()
            if found:
                return found
        return {}

    def as_dict(self) -> dict[str, Any]:
        """Plain data, for a table and for keeping: nothing in it is a datetime."""
        return {
            "url": self.url,
            "position": self.position,
            "available": self.available,
            "reason": self.reason,
            "network_passphrase": self.network_passphrase,
            "network_ok": self.network_ok,
            "horizon_version": self.horizon_version,
            "latest_ledger": self.latest_ledger,
            "elder_ledger": self.elder_ledger,
            "latest_closed_at": None if self.latest_closed_at is None else self.latest_closed_at.isoformat(timespec="seconds"),
            "lag_ledgers": self.lag_ledgers,
            "ledger": self.ledger,
            "root_ms": self.root_ms,
            "history_ms": self.history_ms,
            "root_status": self._last_status(self.root),
            "history_status": self._last_status(self.history),
            "errors": self._errors(self.root) + self._errors(self.history),
            "timeouts": self._timeouts(self.root) + self._timeouts(self.history),
            "rate_limit_headers": self.rate_limit_headers or None,
            "root": [sample.as_dict() for sample in self.root],
            "history": [sample.as_dict() for sample in self.history],
            "checked_at": self.checked_at.isoformat(timespec="seconds"),
        }


@dataclass
class PoolCheck:
    servers: list[NodeCheck]
    requests: int
    checked_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def active(self) -> str | None:
        """The server the pool would ask first: the first available one in
        the order of the list."""
        return next((server.url for server in self.servers if server.available), None)

    def as_dict(self) -> dict[str, Any]:
        return {
            "checked_at": self.checked_at.isoformat(timespec="seconds"),
            "requests": self.requests,
            "active": self.active,
            "servers": [server.as_dict() for server in self.servers],
        }


def check_pool(pool: HorizonPool, *, repeats: int = MAX_REPEATS) -> PoolCheck:
    """Ask every server of the pool, each on its own, what it is and whether
    it answers for history.

    Changes nothing: no server is preferred, left alone or forgotten for it,
    and no cursor moves.
    """
    repeats = max(1, min(repeats, MAX_REPEATS))
    first = pool.stats.requests
    checked = PoolCheck(servers=[], requests=0)
    newest: int | None = None
    for server in pool.servers:
        node = NodeCheck(url=server.name, position=server.position)
        checked.servers.append(node)
        misses = 0
        for _ in range(repeats):
            sample = _sample(pool, server, "/")
            node.root.append(sample)
            if sample.ok:
                node.take_root(sample.body or {}, pool.network_passphrase)
            misses = misses + 1 if sample.timeout else 0
            if misses >= 2:
                break  # two in a row without an answer: not a third time
        ledger = None if node.latest_ledger is None else max(node.elder_ledger or 1, node.latest_ledger - 1)
        if ledger is None and newest is not None:
            # Its root did not say; the ledger the freshest server has is asked for.
            ledger = newest - 1
        node.ledger = ledger
        misses = 0
        if ledger is not None:
            for _ in range(repeats):
                sample = _sample(pool, server, f"/ledgers/{ledger}")
                node.history.append(sample)
                misses = misses + 1 if sample.timeout else 0
                if misses >= 2:
                    break
        if node.root_ok and node.latest_ledger is not None:
            newest = node.latest_ledger if newest is None else max(newest, node.latest_ledger)
    for node in checked.servers:
        if not node.root_ok:
            node.available, node.reason = False, "its root does not answer"
        elif node.network_ok is False:
            node.available, node.reason = False, "it serves another network than this installation keeps"
        elif node.ledger is None:
            node.available, node.reason = False, "no ledger could be asked for"
        elif not node.history_ok:
            node.available, node.reason = False, f"it does not answer for ledger {node.ledger}"
        else:
            node.available, node.reason = True, ""
        if node.latest_ledger is not None and newest is not None:
            node.lag_ledgers = newest - node.latest_ledger
    checked.requests = pool.stats.requests - first
    return checked


def _sample(pool: HorizonPool, server: Any, path: str) -> Sample:
    outcome = pool.fetch(server, path)
    return Sample(outcome.seconds, outcome.status, outcome.error, outcome.headers, outcome.body)


def _moment(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _whole(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def print_check(checked: PoolCheck) -> None:
    """The check as a person reads it: one block per server."""
    moment = checked.checked_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"pool check at {moment}: {len(checked.servers)} server(s), {checked.requests} request(s)")
    for node in checked.servers:
        state = "available" if node.available else "unavailable"
        if node.url == checked.active:
            state += ", active"
        print(f"{node.position}. {node.url} — {state}" + (f": {node.reason}" if node.reason else ""))
        network = "-"
        if node.network_passphrase is not None:
            network = node.network_passphrase + (" (the configured one)" if node.network_ok else (" (ANOTHER NETWORK)" if node.network_ok is False else ""))
        print(f"   network: {network}")
        print(f"   version: {node.horizon_version or '-'}")
        print(
            f"   latest ledger: {node.latest_ledger or '-'} (closed {'-' if node.latest_closed_at is None else node.latest_closed_at.strftime('%Y-%m-%dT%H:%M:%SZ')}), "
            f"elder ledger: {node.elder_ledger or '-'}, lag: {'-' if node.lag_ledgers is None else node.lag_ledgers} ledger(s)"
        )
        print(f"   root: {_line(node.root, node.root_ms)}")
        print(f"   history GET {'/ledgers/' + str(node.ledger) if node.ledger is not None else '-'}: {_line(node.history, node.history_ms)}")
        limits = node.rate_limit_headers
        print("   rate limit: " + (" ".join(f"{name}={value}" for name, value in sorted(limits.items())) if limits else "the server sends no rate limit headers"))
    print(f"active source: {checked.active or 'none is available'}")


def _line(samples: list[Sample], median_ms: int | None) -> str:
    if not samples:
        return "not asked"
    answered = [sample for sample in samples if sample.answered]
    timeouts = sum(1 for sample in samples if sample.timeout)
    errors = sum(1 for sample in samples if sample.answered and not sample.ok)
    parts = [f"{len(samples) - errors - timeouts}/{len(samples)} answered"]
    if median_ms is not None:
        parts.append(f"median {median_ms} ms")
    statuses = sorted({sample.status for sample in answered if sample.status is not None})
    parts.append("status " + (", ".join(str(status) for status in statuses) if statuses else "none"))
    if errors:
        parts.append(f"{errors} error(s)")
    if timeouts:
        parts.append(f"{timeouts} timeout(s)")
    last = next((sample.error for sample in reversed(samples) if sample.error), None)
    return ", ".join(parts) + (f" — {last}" if last else "")
