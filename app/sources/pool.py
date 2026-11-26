"""Several Horizon servers behind one client: the pool.

The first server of the list that can answer is the active one, and it stays
so while it answers. When a server fails it is left alone for a cooldown
and the next one answers; the one after that when it fails too; and the
first again once its cooldown is over. Nothing jumps between servers without
a reason, and nothing is lost where a switch happens: a page is asked for by
its cursor, and checked for order whoever answers it.

A request that fails is not always a server that failed. An expensive
stream can be answered 503 by a server whose root answers at once; that
server stays, and the request is asked again — once on every other server
too, and then as before. A server whose root does not answer either is left
alone for the cooldown, which doubles each time it fails again in a row.

What makes a server fit to answer a request:

* it is not cooling down;
* it has said of itself — in its root — which network it serves and its
  oldest and newest ledger, and recently: a server is asked for its root
  before its first use and whenever that knowledge is stale
  (``health_check_interval``), never on a schedule of its own;
* it serves the network this installation keeps;
* it keeps the ledger the request is about and has got as far as the
  reading sequence goes (``client.scope``): a lagging server is not asked
  for what it has not got yet, and one with a short history is not asked
  for what lies below it — the server that has it is.

What is not done here: going around a provider's limit. The pace is one for
the whole pool and for every process (``limiter``, see
:mod:`app.storage.pace`); a 429 pauses all of them for as long as the
server asks, and no other server is asked instead.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import httpx

from app.config import server_name
from app.sources import live
from app.sources.horizon import (
    BeforeHistory,
    History,
    HorizonClient,
    HorizonSettings,
    MalformedResponse,
    NotFound,
    ResponseError,
    Unavailable,
    WrongNetwork,
    ledger_of,
    retry_after_in,
)

logger = logging.getLogger(__name__)

# Why a server is not to answer a request (``HorizonPool._unfit``).
STALE = "stale"  # ...until it has said of itself again
ANOTHER_NETWORK = "another network"
COOLING = "cooling"
BELOW_HISTORY = "below its history"  # the request is
BEHIND = "behind"  # it has not got as far as the reading sequence goes


@dataclass(frozen=True)
class PoolSettings:
    # How long what a server said of itself is trusted before it is asked again.
    health_check_interval: float = 300.0
    # How long a server that did not answer is left alone. It doubles each
    # time it fails again in a row.
    failover_cooldown: float = 60.0
    max_cooldown: float = 3600.0
    # A server's root is small. One that cannot answer in this many seconds
    # is not answering.
    probe_timeout: float = 10.0
    # What every process waits when a server refuses with a 429 and does not
    # say for how long.
    rate_limit_pause: float = 5.0


@dataclass(frozen=True)
class Outcome:
    """One request to one named server, as it went.

    ``status`` None: no answer in time, and ``error`` says why. ``body`` is
    the JSON object answered, when the answer was a 2xx and is one.
    """

    status: int | None
    seconds: float
    error: str | None
    headers: Mapping[str, str]
    body: dict[str, Any] | None

    @property
    def ok(self) -> bool:
        return self.error is None and self.status is not None and 200 <= self.status < 300 and self.body is not None


class Server:
    """One Horizon of the pool, and what is known of it."""

    def __init__(self, url: str, position: int, http: httpx.Client, transport: httpx.BaseTransport | None) -> None:
        self.url = url
        self.position = position
        self.http = http
        self.transport = transport
        self.name = server_name(url)
        self.origin = urlsplit(url)
        # Every request to this server goes under the path of its address,
        # which may hold a key: it is the path of its HTTP client's base.
        self.lock = threading.Lock()
        # Its last root, and when it was read (the client's clock).
        self.root: dict[str, Any] | None = None
        self.checked_at = float("-inf")
        self.wrong_network = False
        self.cooling_until: float | None = None
        # The same moment on the wall, for a status page.
        self.cooling_until_at: datetime | None = None
        # Cooldowns in a row; each is twice the one before.
        self.cooldowns = 0
        self.last_error: str | None = None
        self.last_error_at: datetime | None = None
        self.last_answer_at: datetime | None = None

    @property
    def elder(self) -> int | None:
        return None if self.root is None else self.root.get("history_elder_ledger")

    @property
    def latest(self) -> int | None:
        return None if self.root is None else self.root.get("history_latest_ledger")

    def as_dict(self) -> dict[str, Any]:
        """What the running processes know of it, for a status page."""
        return {
            "url": self.name,
            "position": self.position,
            "network_passphrase": None if self.root is None else self.root.get("network_passphrase"),
            "elder_ledger": self.elder,
            "latest_ledger": self.latest,
            "cooling_until": self.cooling_until_at,
            "last_error": self.last_error,
            "last_error_at": self.last_error_at,
            "last_answer_at": self.last_answer_at,
        }

    def __repr__(self) -> str:
        return f"Server({self.name})"


class HorizonPool(HorizonClient):
    """Client for GET requests against a pool of Horizon servers.

    ``limiter`` keeps the pace for every process (``reserve``, ``again`` and
    ``pause`` of :class:`app.storage.pace.Pace`); without one the pace is
    this process's own. ``remember`` is told when what is known of a server
    changes, for a status page. ``transports`` are for tests, by the name a
    server is shown under.
    """

    def __init__(
        self,
        urls: tuple[str, ...] | list[str],
        settings: HorizonSettings | None = None,
        *,
        network_passphrase: str | None = None,
        limiter: Any = None,
        remember: Callable[[Server], None] | None = None,
        pool_settings: PoolSettings | None = None,
        transports: Mapping[str, httpx.BaseTransport] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        rng: Any = None,
    ) -> None:
        urls = tuple(urls)
        if not urls:
            raise ValueError("a pool has at least one server")
        settings = replace(settings, base_url=urls[0]) if settings is not None else HorizonSettings(base_url=urls[0])
        super().__init__(settings, clock=clock, sleeper=sleeper, rng=rng)
        self._http.close()  # the pool has a client per server instead
        self.pool_settings = pool_settings or PoolSettings()
        self._network = network_passphrase
        self._limiter = limiter
        self._remember_hook = remember
        self._lock = threading.RLock()
        transports = dict(transports or {})
        self.nodes = tuple(
            Server(url, position, self._new_client(url, transports.get(server_name(url))), transports.get(server_name(url)))
            for position, url in enumerate(urls, start=1)
        )

    def __enter__(self) -> "HorizonPool":
        return self

    @property
    def servers(self) -> tuple[Server, ...]:
        return self.nodes

    @property
    def network_passphrase(self) -> str | None:
        """The network the installation keeps; a server of another one is
        never asked. None when it was not said, and then none is refused."""
        return self._network

    def close(self) -> None:
        for server in self.nodes:
            server.http.close()

    # -- one request to one named server -------------------------------------

    def fetch(
        self, server: Server, path: str, query: Mapping[str, Any] | None = None, *, timeout: float | None = None
    ) -> Outcome:
        """One request to one named server: no retry, no other server instead.

        Paced and counted like every request, so that it is part of the
        budget. A check of the pool uses this for every server on its own.
        """
        if self.before_request is not None:
            self.before_request()
        self._pace()
        self.stats.requests += 1
        params = dict(query or {})
        started = time.monotonic()
        try:
            response = self._under_deadline(
                f"{path} on {server.name}",
                lambda: server.http.get(path, params=params),
                lambda: self._stalled(server),
                timeout,
            )
        except live.Spent as error:
            # The cap on live requests: not this server's doing, and nothing
            # is left to ask of anyone.
            raise Unavailable(f"the live budget is used up, nothing more is asked: {error}") from error
        except httpx.TransportError as error:
            return Outcome(None, time.monotonic() - started, f"{type(error).__name__}: {error}", {}, None)
        self.stats.responses_by_status[response.status_code] += 1
        body = None
        if 200 <= response.status_code < 300:
            try:
                body = self._decode(response, path)
            except MalformedResponse as error:
                return Outcome(response.status_code, time.monotonic() - started, str(error), response.headers, None)
        return Outcome(response.status_code, time.monotonic() - started, None, response.headers, body)

    # -- hooks: choosing and keeping a server --------------------------------

    def _begin(self, path: str, query: Mapping[str, Any]) -> None:
        # Of the request the current thread is making: the server that failed
        # last, if one did. The next attempt starts after it in the list.
        self._local.failed: Server | None = None

    def _prepare(self, path: str, query: Mapping[str, Any]) -> None:
        server = self._pick(path, query)
        self._local.server = server
        self._local.served_by = server.name
        self._local.history_from = server.elder

    def _exchange(self, path: str, query: Mapping[str, Any], server: Server) -> httpx.Response:
        return server.http.get(path, params=dict(query))

    def _stalled(self, server: Server) -> None:
        """The attempt hit the deadline: drop this server's connection."""
        with server.lock:
            stuck, server.http = server.http, self._new_client(server.url, server.transport)
        stuck.close()

    def _links_origin(self):
        return self._local.server.origin

    def _note_transport_error(self, path: str, query: Mapping[str, Any], error: Exception) -> None:
        server = self._local.server
        self._local.failed = server
        self._suspect(path, server, f"no answer to GET {path} ({type(error).__name__}: {error})")

    def _note_status(self, path: str, status: int, response: httpx.Response) -> None:
        if status == 429:
            # The limit is the address's. Every process waits for as long as
            # the server asks, and no other server is asked instead.
            self._pause(response.headers)
            return
        self._local.failed = self._local.server
        self._suspect(path, self._local.server, f"answered {status} to GET {path}")

    def _take(self, path: str, body: Mapping[str, Any]) -> bool:
        if path != "/":
            return True
        if self._keep_root(self._local.server, body):
            return True
        self._local.failed = self._local.server
        return False

    def _note_answer(self, path: str, body: Mapping[str, Any]) -> None:
        super()._note_answer(path, body)
        self._thrived(self._local.server)

    def history(self) -> History:
        """What the pool keeps, as of now: the oldest ledger the pool keeps,
        and the newest ledger any of its servers has got to.

        A request to every server that is not left alone: a reading sequence
        starts from what the servers have got to right now, and what they
        said of themselves a while ago does not tell.

        The oldest ledger is said by every server that has said what it
        keeps, one that is silent at the moment included, by its last word:
        what a server keeps only moves up, so that word is never below the
        truth. To go by the servers that answer alone would give up on
        ledgers that are only waiting for one of them to come back — and
        call them out of reach for good.
        """
        now = self._clock()
        for server in self.nodes:
            with self._lock:
                available = not server.wrong_network and (server.cooling_until is None or now >= server.cooling_until)
            if available:
                self._check(server)
        now = self._clock()
        usable = [server for server in self.nodes if self._usable(server, now)]
        if not usable:
            strangers = [server.name for server in self.nodes if server.root is not None and server.wrong_network]
            if strangers:
                raise WrongNetwork(f"every server of the pool that answers serves another network: {', '.join(strangers)}")
            down = "; ".join(f"{server.name} {self._say(server, self._unfit(server, None), None)}" for server in self.nodes)
            raise Unavailable(f"no server of the pool answers: {down}")
        freshest = max(usable, key=lambda server: server.latest or 0)
        said = [server.elder for server in self.nodes if not server.wrong_network and server.elder is not None]
        return History(
            elder=min(said),
            latest=freshest.latest,
            name=freshest.name,
            network_passphrase=freshest.root.get("network_passphrase"),
            latest_closed_at=freshest.root.get("history_latest_ledger_closed_at"),
        )

    def lacks(self, ledger: int) -> bool:
        """Whether no server of the pool keeps ``ledger`` or anything below
        it: what lies there can no longer be read from any of them. A server
        that has not said what it keeps is given the benefit of the doubt."""
        return all(server.wrong_network or (server.elder is not None and server.elder > ledger) for server in self.nodes)

    def silent(self) -> list[str]:
        """The servers that have not said of themselves just now — down, or
        left alone after a failure — and so may keep history nobody has
        heard of. A server of another network is not one of the pool's."""
        now = self._clock()
        return [server.name for server in self.nodes if not server.wrong_network and not self._usable(server, now)]

    def _pace(self) -> None:
        if self._limiter is None:
            super()._pace()
            return
        while True:
            wait = self._limiter.reserve(self._interval)
            if wait is None:
                # An event goes first; ask again, having taken nothing.
                self._sleep(self._limiter.again(self._interval))
                continue
            if wait > 0:
                self._sleep(wait)
            return

    def _pause(self, headers: Mapping[str, str]) -> None:
        if self._limiter is not None:
            asked = retry_after_in(headers)
            self._limiter.pause(self.pool_settings.rate_limit_pause if asked is None else asked)

    # -- choosing ------------------------------------------------------------

    def _pick(self, path: str, query: Mapping[str, Any]) -> Server:
        """The first server fit to answer this request.

        In the order of the list — and, when one failed this request, round
        the circle from the one after it, so that the next attempt goes to
        the next server and not back to the one that just failed. A server
        whose knowledge is stale is asked for its root first; that is a
        request.
        """
        ledger = ledger_of(path, query)
        failed: Server | None = getattr(self._local, "failed", None)
        start = 0 if failed is None else (self.nodes.index(failed) + 1) % len(self.nodes)
        order = self.nodes[start:] + self.nodes[:start]
        # A request that names no ledger — a transaction by its hash — may be
        # about anything the sequence is about. A server that keeps none of
        # the sequence from where it starts is asked for it only when no
        # other fit server is left: it would answer 410 and cost a request.
        wanted = None if ledger is not None else (None if self.scope is None else self.scope.from_ledger)
        reasons: list[str] = []
        unfit: dict[Server, str] = {}
        kept_for_last: Server | None = None
        for round_ in (1, 2):
            reasons, unfit, kept_for_last = [], {}, None
            for server in order:
                why = self._unfit(server, ledger)
                if why == STALE:
                    if path == "/":
                        why = None  # this request is the asking
                    else:
                        said = self._check(server)
                        why = self._unfit(server, ledger)
                        if not said and why is None:
                            why = COOLING
                if why is not None:
                    unfit[server] = why
                    reasons.append(f"{server.name} {self._say(server, why, ledger)}")
                    continue
                if wanted is not None and server.elder is not None and server.elder > wanted:
                    kept_for_last = kept_for_last or server
                    continue
                return server
            if kept_for_last is not None:
                return kept_for_last
            if round_ == 1 and unfit and all(why == BEHIND for why in unfit.values()):
                # Every server says it has not got that far — but that was
                # a moment ago, and servers are getting further all the time.
                # They are asked again before this is held against them.
                for server in unfit:
                    self._forget(server)
                continue
            break
        raise Unavailable(f"no server of the pool can answer GET {path}: " + "; ".join(reasons))

    def _unfit(self, server: Server, ledger: int | None) -> str | None:
        """Why ``server`` is not to answer a request about ``ledger``; None
        when it is."""
        now = self._clock()
        with self._lock:
            if server.wrong_network:
                return ANOTHER_NETWORK
            if server.cooling_until is not None:
                if now < server.cooling_until:
                    return COOLING
                server.cooling_until = None  # its time is over: it may answer again
            if server.root is None or now - server.checked_at > self.pool_settings.health_check_interval:
                return STALE
            if ledger is not None and server.elder is not None and server.elder > ledger:
                return BELOW_HISTORY
            if ledger is not None and server.latest is not None and server.latest < ledger:
                return BEHIND  # it has not got that far: a 404 of its would say nothing
            through = None if self.scope is None else self.scope.through
            if through is not None and server.latest is not None and server.latest < through:
                return BEHIND
        return None

    @staticmethod
    def _say(server: Server, why: str, ledger: int | None) -> str:
        if why == STALE:
            return "has not said of itself recently"
        if why == ANOTHER_NETWORK:
            return "serves another network"
        if why == COOLING:
            return "is left alone for now"
        if why == BELOW_HISTORY:
            return f"keeps history only from ledger {server.elder}, not {ledger}"
        if why == BEHIND:
            return f"has got only to ledger {server.latest}"
        return why

    def _usable(self, server: Server, now: float) -> bool:
        return (
            server.root is not None
            and not server.wrong_network
            and server.elder is not None
            and server.latest is not None
            and (server.cooling_until is None or now >= server.cooling_until)
        )

    # -- what a server said of itself, and what it did -----------------------

    def _check(self, server: Server) -> bool:
        """Ask ``server`` for its root and keep what it says.

        False when it is not to be used: it did not answer, and is left
        alone for a while; or it serves another network, and is never asked.
        """
        outcome = self.fetch(server, "/", timeout=self.pool_settings.probe_timeout)
        if outcome.error is not None and outcome.status is None:
            self._leave(server, f"no answer to its root ({outcome.error})")
            return False
        if not outcome.ok:
            self._leave(server, f"its root answered {outcome.status}" + (f": {outcome.error}" if outcome.error else ""))
            return False
        return self._keep_root(server, outcome.body or {})

    @staticmethod
    def _forget(server: Server) -> None:
        """What the server said of itself is no longer true: it is to say so again."""
        server.checked_at = float("-inf")

    def _keep_root(self, server: Server, root: Mapping[str, Any]) -> bool:
        try:
            elder, latest = int(root["history_elder_ledger"]), int(root["history_latest_ledger"])
        except (KeyError, TypeError, ValueError):
            self._leave(server, "its root does not say which ledgers it keeps")
            return False
        with self._lock:
            server.root = {**root, "history_elder_ledger": elder, "history_latest_ledger": latest}
            server.checked_at = self._clock()
            server.wrong_network = self._network is not None and root.get("network_passphrase") != self._network
            stranger = server.wrong_network
        if stranger:
            logger.warning(
                "%s serves the network %r, not the one this installation keeps: it is not asked",
                server.name,
                root.get("network_passphrase"),
            )
            self._remember(server)
        return not stranger

    def _suspect(self, path: str, server: Server, what: str) -> None:
        """A request failed on ``server``. Its own root says whether the
        server did: one that answers at once stays in service, and the
        request was the problem."""
        if path == "/":
            self._leave(server, what)
            return
        if self._check(server):
            logger.info("%s %s; its root answers, so it stays in service", server.name, what)

    def _leave(self, server: Server, why: str) -> None:
        """The server is not to be asked for a while."""
        settings = self.pool_settings
        with self._lock:
            server.cooldowns += 1
            wait = min(settings.failover_cooldown * 2 ** (server.cooldowns - 1), settings.max_cooldown)
            server.cooling_until = self._clock() + wait
            server.cooling_until_at = datetime.now(UTC) + timedelta(seconds=wait)
            server.last_error, server.last_error_at = why, datetime.now(UTC)
            self.stats.failovers += 1
        logger.warning("%s is left alone for %.0fs: %s", server.name, wait, why)
        self._remember(server)

    def _thrived(self, server: Server) -> None:
        with self._lock:
            back = server.cooling_until is not None
            server.cooldowns, server.cooling_until, server.cooling_until_at = 0, None, None
            server.last_answer_at = datetime.now(UTC)
        if back:
            logger.info("%s answers again", server.name)
            self._remember(server)

    def _remember(self, server: Server) -> None:
        if self._remember_hook is not None:
            self._remember_hook(server)

    # -- 404 and 410: is there another server to ask? ------------------------

    def _resource_missing(
        self, path: str, query: Mapping[str, Any], error: ResponseError
    ) -> dict[str, Any] | None:
        """404 or 410 from the server that answered: ask the others that
        could say something about it, once each.

        The body of the one that has it. Or an error for what the answers
        add up to: a 410 from every server that keeps the ledger is the end
        of the pool's history; a 404 from a server that keeps the ledger is
        the resource's absence. And when a server that could have answered
        could not be asked, no answer is final: the request is to be asked
        again later. None when there was nobody else to ask: the error stands.
        """
        first = self._local.server
        ledger = ledger_of(path, query)
        silent: list[str] = []
        gone: list[tuple[Server, int]] = [(first, error.status)]
        for server in self.nodes:
            if server is first:
                continue
            why = self._unfit(server, ledger)
            if why == STALE:  # it has to say of itself before it can say anything about this
                said = self._check(server)
                why = self._unfit(server, ledger)
                if not said and why is None:
                    why = COOLING
            if why == COOLING:
                silent.append(server.name)  # it may have it, and cannot be asked now
                continue
            if why is not None:
                continue  # it cannot say anything about it: nothing is left to ask
            outcome = self.fetch(server, path, query)
            if outcome.status is None:
                # No answer. A root that answers says the request was the problem.
                if not self._check(server):
                    self._leave(server, f"no answer to GET {path} ({outcome.error})")
                silent.append(server.name)
                continue
            if outcome.ok:
                self._local.server, self._local.served_by = server, server.name
                self._local.history_from = server.elder
                self._thrived(server)
                return outcome.body
            status = outcome.status
            if status == 410:
                self._forget(server)  # it keeps less than it said of itself
                gone.append((server, status))
                continue
            if status == 404:
                gone.append((server, status))
                continue
            silent.append(server.name)
            if status == 429:
                self._pause(outcome.headers)
            elif status in (408, 425, 500, 502, 503, 504):
                self._suspect(path, server, f"answered {status} to GET {path}")
        if silent:
            raise Unavailable(
                f"GET {error.path}: {first.name} answered {error.status}, and {', '.join(silent)} could not be asked, "
                f"so that proves nothing yet"
            ) from error
        if 410 not in {status for _, status in gone}:
            return None  # 404 everywhere: the error stands
        # Retention moves. What the servers keep is asked again before
        # anything is said about where their history ends.
        self._forget(first)
        for server, status in gone:
            if status == 410 and not self._check(server):
                silent.append(f"{server.name} (its bounds could not be asked again)")
        if silent:
            raise Unavailable(f"GET {error.path}: where the history ends could not be established: {', '.join(silent)}") from error
        if any(status == 404 and _keeps(server, ledger) for server, status in gone):
            # A server that keeps the ledger has no such resource.
            who = next(server.name for server, status in gone if status == 404 and _keeps(server, ledger))
            raise NotFound(
                error.path, 404, {"title": "Resource Missing", "detail": f"not on {who} either, which keeps ledger {ledger}"}
            ) from error
        ended = [server for server, status in gone if status == 410 and server.elder is not None]
        if not ended:
            return None
        oldest = min(ended, key=lambda server: server.elder)
        latest = max(server.latest for server, _ in gone if server.latest is not None)
        finished = BeforeHistory(
            error.path,
            410,
            {"title": "Data Requested Is Before Recorded History", "detail": "below the history of every server of the pool"},
        )
        finished.history = History(
            min(server.elder for server in ended), latest, oldest.name, oldest.root.get("network_passphrase") if oldest.root else None
        )
        raise finished from error


def _keeps(server: Server, ledger: int | None) -> bool:
    """Whether a 404 of the server speaks for the ledger: it keeps the ledger
    and has got as far as it."""
    if ledger is None:
        return True
    if server.elder is None or server.latest is None:
        return False
    return server.elder <= ledger <= server.latest
