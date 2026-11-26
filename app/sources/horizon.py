"""HTTP client for one Horizon server.

Read-only, paced, retried when a retry can help, and suspicious of anything a
response says about where to go next.

The pace is kept by this client. The public server limits requests per
address and says so only once it refuses: a 429 carries ``X-RateLimit-*``
headers and no ``Retry-After``. So the pace is set below what the server
allows, and a 429 is waited out for as long as the server asks.

A pool of several servers is this client with a few hooks replaced. They
are marked ``hook`` below and do here what one server needs: nothing, or
what was always done (:mod:`app.sources.pool`).
"""

from __future__ import annotations

import email.utils
import logging
import random
import re
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import SplitResult, parse_qsl, urlsplit

import httpx

from app import __version__
from app.config import server_name, shown_url
from app.sources import live
from app.sources.horizon_cursors import InvalidCursor, order_key

# Worth asking again: the answer may differ next time. 404 and 410 are final.
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
# How often one and the same request may be answered before that is an error.
MAX_SAME_REQUEST = 3
PUBLIC_URL = "https://horizon.stellar.org"

logger = logging.getLogger(__name__)

OrderKey = tuple[int, ...]


class HorizonError(Exception):
    """No usable answer was obtained for a request."""


class ResponseError(HorizonError):
    """The server answered with a problem document."""

    def __init__(self, path: str, status: int, problem: Mapping[str, Any] | None) -> None:
        self.path = path
        self.status = status
        self.problem = dict(problem or {})
        title = self.problem.get("title") or httpx.codes.get_reason_phrase(status)
        detail = self.problem.get("detail")
        super().__init__(
            f"Horizon answered {status} {title} to GET {path}" + (f": {detail}" if detail else "")
        )


_ACCOUNT_STREAM = re.compile(r"^/accounts/[A-Z0-9]{56}/(operations|transactions|effects|payments|trades|offers)$")


class NotFound(ResponseError):
    """404: there is no such resource, or the server has no such route."""


class BeforeHistory(ResponseError):
    """410: the request reaches older than the history this server keeps.

    ``history`` is what the servers that so answered keep (a ``History``),
    when the client knows; a reader closes the range against it without
    asking again.
    """

    history: "History | None" = None


class RateLimited(HorizonError):
    """429 with a Retry-After longer than this run is allowed to wait."""


class RequestFailed(HorizonError):
    """A status that will not change on retry, or no answer after every attempt."""


class Unavailable(RequestFailed):
    """No server could answer this request just now: every one is out of
    service, or the ones that could have said something could not be asked.

    This is the state of the servers, not of the request: it says nothing
    about the resource, and a job that meets it has not failed."""


class WrongNetwork(HorizonError):
    """Every server that could answer belongs to another Stellar network."""


class MalformedResponse(HorizonError):
    """The answer does not look like what a Horizon response must look like."""


class UntrustedNextLink(HorizonError):
    """`_links.next` does not lead back to the same server and request."""


def _shown_href(href: object) -> str:
    """`_links.next` as it may be written in a message: the address without
    its path, which may hold the key of a gateway."""
    return shown_url(href) if isinstance(href, str) else repr(href)


@dataclass
class RequestStats:
    """Counters behind the run report."""

    requests: int = 0
    retries: int = 0
    responses_by_status: Counter[int] = field(default_factory=Counter)
    waited_seconds: float = 0.0
    # 429 answers met; each one is also a retry unless it ended the run.
    rate_limited: int = 0
    # Requests given up on at the deadline and repeated on a new connection.
    abandoned: int = 0
    # Answers (2xx) per server, by the name it is shown under.
    by_server: Counter[str] = field(default_factory=Counter)
    # How often a server had to be given up for another one.
    failovers: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "requests": self.requests,
            "retries": self.retries,
            "responses_by_status": dict(sorted(self.responses_by_status.items())),
            "waited_seconds": round(self.waited_seconds, 3),
            "rate_limited": self.rate_limited,
            "abandoned": self.abandoned,
            "by_server": dict(sorted(self.by_server.items())),
            "failovers": self.failovers,
        }


@dataclass(frozen=True)
class HorizonSettings:
    base_url: str = PUBLIC_URL
    requests_per_second: float = 1.0
    timeout_seconds: float = 20.0
    max_attempts: int = 5
    backoff_base_seconds: float = 1.0
    backoff_cap_seconds: float = 60.0
    # A wait the server asks for above this is not waited out. The run stops
    # instead and is resumed later: nothing is lost, the next page starts from
    # the checkpoint. The public server asks for 300 seconds when its bucket
    # for this address is empty.
    max_retry_after_seconds: float = 900.0
    # A request that has not answered after this long is given up on and
    # repeated on a new connection. The socket timeouts should catch every
    # dead connection; a public server behind a CDN can also drop one without
    # a word, leaving the read waiting for the TCP retransmissions to end.
    request_deadline_seconds: float | None = 60.0


@dataclass(frozen=True)
class Scope:
    """The ledgers a reading sequence is about: from ``from_ledger`` up to
    ``through``. A server that keeps none of it, or has not got as far as
    its top, is not asked. None is where the sequence does not care."""

    from_ledger: int | None = None
    through: int | None = None


@dataclass(frozen=True)
class History:
    """What a server, or a pool of them, keeps: ``elder`` the oldest ledger
    some available server still answers for, ``latest`` the newest ledger
    any of them has, and what the newest of them says of itself."""

    elder: int
    latest: int
    name: str = ""
    network_passphrase: str | None = None
    latest_closed_at: str | None = None


@dataclass(frozen=True)
class Page:
    """One page of a collection.

    ``query`` is what was asked for. ``next_query`` is the same request with
    the cursor of `_links.next`, after that link has been checked; it is None
    on the last page. ``served_by`` is the server that answered, as it is
    shown to people, and ``history_from`` the oldest ledger it keeps when it
    has said so: a page that ends above it says nothing about what lies below.
    """

    records: tuple[Mapping[str, Any], ...]
    path: str
    resource_type: str
    query: Mapping[str, Any]
    next_query: Mapping[str, Any] | None
    served_by: str | None = None
    history_from: int | None = None

    @property
    def next_cursor(self) -> str | None:
        return None if self.next_query is None else str(self.next_query["cursor"])

    @property
    def exhausted(self) -> bool:
        """No record follows this page: it is shorter than the limit asked
        for, so there was nothing more to return."""
        return self.next_query is None


class HorizonClient:
    """Client for GET requests against one Horizon server.

    ``clock``, ``sleeper`` and ``rng`` exist for tests; the defaults are the
    real ones.
    """

    def __init__(
        self,
        settings: HorizonSettings | None = None,
        *,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self.settings = settings or HorizonSettings()
        self.stats = RequestStats()
        self._origin = urlsplit(self.settings.base_url)
        # The path of the address, which may hold the key of a gateway, goes
        # with every request by itself: it is the path of the HTTP client's
        # base address. It is never shown.
        # A real client with a cap on (app.sources.live) sends through the
        # capped transport; a fake, or a process without a cap, as before.
        self._transport = transport if transport is not None else live.transport()
        self._http = self._new_client()
        self._interval = 1.0 / self.settings.requests_per_second
        self._pace_lock = threading.Lock()
        self._clock = clock
        self._next_slot = clock()
        # Of the request the current thread is making: which server is to
        # answer it (``server``) and which one did. The attempt itself runs in
        # a thread of its own, so the server is handed to it by name.
        self._local = threading.local()
        # Called before every attempt; a run uses it to stop between requests.
        self.before_request: Callable[[], None] | None = None
        # What a reading sequence is about, for whoever chooses the server.
        self.scope: Scope | None = None
        # The last request answered and how many times it was the same one
        # (see _repeated).
        self._answered: str | None = None
        self._times_answered = 0
        self._sleep = sleeper
        self._rng = rng or random.Random()

    @property
    def name(self) -> str:
        """The server as it is shown to people: no user name, no password."""
        return server_name(self.settings.base_url)

    def _new_client(self, base_url: str | None = None, transport: httpx.BaseTransport | None = None) -> httpx.Client:
        return httpx.Client(
            base_url=(base_url or self.settings.base_url).rstrip("/") + "/",
            timeout=httpx.Timeout(self.settings.timeout_seconds, connect=10.0),
            transport=transport if transport is not None else self._transport,
            follow_redirects=False,
            headers={
                "Accept": "application/hal+json, application/json",
                "User-Agent": f"mtl-horizon/{__version__} (read-only history indexer)",
            },
        )

    def __enter__(self) -> "HorizonClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    def _send(self, path: str, query: Mapping[str, Any]) -> httpx.Response:
        """GET on the shared client, under a deadline (see _under_deadline)."""
        server = self._local.server
        return self._under_deadline(
            path, lambda: self._exchange(path, query, server), lambda: self._stalled(server)
        )

    def _under_deadline(
        self,
        what: str,
        request: Callable[[], httpx.Response],
        stalled: Callable[[], None],
        deadline: float | None = None,
    ) -> httpx.Response:
        """One attempt of ``request`` in a thread of its own, under the deadline.

        A read that never returns cannot hold up its stream. At the deadline
        ``stalled`` drops the connection with everything on it, and the
        request counts as a transport failure, which is worth a retry.
        ``deadline`` None is the setting; a pool asks a server's root under
        a shorter one.
        """
        outcome: dict[str, Any] = {}

        def run() -> None:
            try:
                outcome["response"] = request()
            except BaseException as error:  # noqa: BLE001 - handed to the caller
                outcome["error"] = error

        limit = self.settings.request_deadline_seconds if deadline is None else deadline
        started = time.monotonic()
        if limit is None:
            run()
        else:
            worker = threading.Thread(target=run, name="horizon-request", daemon=True)
            worker.start()
            worker.join(limit)
            if worker.is_alive():
                stalled()
                self.stats.abandoned += 1
                logger.warning(
                    "no answer to GET %s within %.0fs: the connection is dropped and the request repeated",
                    what,
                    limit,
                )
                raise httpx.ReadTimeout(f"no answer within {limit:.0f}s")
        spent = time.monotonic() - started
        if spent > 10:
            logger.info("GET %s answered in %.1fs", what, spent)
        error, response = outcome.get("error"), outcome.get("response")
        if error is not None:
            raise error
        assert response is not None
        return response

    # -- requests -----------------------------------------------------------

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Fetch one resource (the root, a ledger, a transaction, an operation)."""
        return self._request(path, _clean(params))

    def root(self) -> dict[str, Any]:
        return self.get("/")

    def page(
        self,
        path: str,
        *,
        resource_type: str,
        order: str = "desc",
        cursor: str | None = None,
        limit: int | None = None,
        **filters: Any,
    ) -> Page:
        query = _clean({"order": order, "cursor": cursor, "limit": limit, **filters})
        return self._to_page(self._request(path, query), path, query, resource_type)

    def next_page(self, page: Page) -> Page | None:
        """Fetch the page after ``page``, or None when ``page`` is the last."""
        if page.exhausted:
            return None
        return self._to_page(
            self._request(page.path, page.next_query), page.path, page.next_query, page.resource_type
        )

    def iter_pages(
        self,
        path: str,
        *,
        resource_type: str,
        order: str = "desc",
        cursor: str | None = None,
        limit: int | None = None,
        **filters: Any,
    ) -> Iterator[Page]:
        """Yield the pages of a collection in order.

        Every record must sit strictly beyond the cursor and strictly beyond
        the previous record; a page that repeats or jumps back is a broken
        answer and stops the iteration.
        """
        page = self.page(
            path, resource_type=resource_type, order=order, cursor=cursor, limit=limit, **filters
        )
        boundary = None if not cursor else _boundary(resource_type, cursor)
        previous: OrderKey | None = None
        seen_cursors: set[str] = set()
        while True:
            for record in page.records:
                key = _record_key(record, resource_type)
                if previous is None:
                    if boundary is not None and not _after(key, boundary, order):
                        raise MalformedResponse(
                            f"{path}: first record is not beyond the cursor {cursor!r}"
                        )
                elif not _after(key, previous, order):
                    raise MalformedResponse(
                        f"{path}: records are not strictly {order} at {record.get('paging_token')!r}"
                    )
                previous = key
            yield page
            if page.exhausted:
                return
            next_cursor = page.next_cursor
            assert next_cursor is not None
            if next_cursor in seen_cursors or next_cursor == cursor:
                raise MalformedResponse(f"{path}: cursor {next_cursor!r} does not advance")
            seen_cursors.add(next_cursor)
            # A full page can still be the last one: the one after it is empty.
            following = self.next_page(page)
            assert following is not None
            if not following.records:
                return
            page = following

    def address_pages(
        self, path: str, *, cursor: str | None = None, limit: int = 200, **filters: Any
    ) -> Iterator[Page]:
        """The pages of a collection ordered by address — the accounts — in
        order.

        An address has no place in time, so there is no order key to check a
        record by: what is checked is that the addresses strictly ascend,
        that the first lies strictly beyond the cursor and that a page's
        link names the last address of the page as where to go on. A page
        that repeats, jumps back or skips is a broken answer and stops the
        reading. The filters asked for are kept on every page.
        """
        query = _clean({"order": "asc", "cursor": cursor, "limit": limit, **filters})
        page = self._to_page(self._request(path, query), path, query, "account")
        boundary = cursor
        seen: set[str] = set()
        while True:
            addresses = self._addresses(path, page)
            for address in addresses:
                if boundary is not None and address <= boundary:
                    raise MalformedResponse(f"{path}: account {address[:6]}… is not beyond the one before it")
                boundary = address
            yield page
            if page.exhausted:
                return
            next_cursor = page.next_cursor
            assert next_cursor is not None
            if next_cursor in seen or next_cursor == cursor or (addresses and next_cursor != addresses[-1]):
                raise MalformedResponse(f"{path}: cursor {next_cursor[:6]}… does not lead on from the page")
            seen.add(next_cursor)
            following = self.next_page(page)
            if following is None or not following.records:
                return
            cursor, page = next_cursor, following

    @staticmethod
    def _addresses(path: str, page: Page) -> list[str]:
        """The accounts of the page, which must strictly ascend."""
        addresses: list[str] = []
        for record in page.records:
            address = record.get("account_id")
            if not isinstance(address, str) or not address:
                raise MalformedResponse(f"{path}: a record names no account")
            addresses.append(address)
        return addresses

    def _request(self, path: str, query: Mapping[str, Any]) -> dict[str, Any]:
        asked = "&".join(f"{key}={value}" for key, value in sorted(query.items()) if value is not None)
        label = path + (f"?{asked}" if asked else "")
        attempts = self.settings.max_attempts
        self._begin(path, query)
        for attempt in range(1, attempts + 1):
            if self.before_request is not None:
                self.before_request()
            self._prepare(path, query)
            self._pace()
            self.stats.requests += 1
            started = time.monotonic()
            try:
                response = self._send(path, query)
            except live.Spent as error:
                # The cap on what may be asked of the real servers (a check
                # its owner allowed). Nothing more is asked: not of this
                # server again, not of another.
                raise Unavailable(f"the live budget is used up, nothing more is asked: {error}") from error
            except httpx.TransportError as error:
                # Timeouts and connection problems are included.
                self._note_transport_error(path, query, error)
                if attempt == attempts:
                    raise RequestFailed(
                        f"no answer from Horizon for GET {label} after {attempts} attempts: {error}"
                    ) from error
                self._backoff(attempt, None)
                continue

            status = response.status_code
            self.stats.responses_by_status[status] += 1
            logger.debug("GET %s -> %s in %.2fs", label, status, time.monotonic() - started)
            problem = _problem(response)
            missing: ResponseError | None = None
            if status == 410:
                missing = BeforeHistory(label, status, problem)
            elif status == 404:
                missing = NotFound(label, status, problem)
            if missing is not None:
                body = self._resource_missing(path, query, missing)
                if body is None and missing.status == 404 and _ACCOUNT_STREAM.fullmatch(path):
                    # Nobody else to ask, and no server knows any history of this
                    # account. Horizon answers 404 for an account it has nothing
                    # of, where an account with nothing after the cursor gets an
                    # empty page. Either way there is nothing: an empty page.
                    body = {"_embedded": {"records": []}}
                if body is None:
                    raise missing
                self._note_answered(label)
                self._note_answer(path, body)
                return body
            if status in RETRYABLE_STATUS:
                self._note_status(path, status, response)
                if attempt == attempts:
                    raise RequestFailed(
                        f"Horizon answered {status} to GET {label} on all {attempts} attempts"
                    )
                retry_after = None
                if status == 429:
                    self.stats.rate_limited += 1
                    logger.warning(
                        "rate limited (429) on GET %s: limit %s, remaining %s, reset in %s s",
                        path,
                        response.headers.get("X-RateLimit-Limit", "?"),
                        response.headers.get("X-RateLimit-Remaining", "?"),
                        response.headers.get("X-RateLimit-Reset", "?"),
                    )
                    asked = _retry_after(response)
                    if asked is not None and asked > self.settings.max_retry_after_seconds:
                        raise RateLimited(
                            f"Horizon asks to wait {asked:.0f}s for GET {label}, "
                            f"more than the {self.settings.max_retry_after_seconds:.0f}s this run allows"
                        )
                    retry_after = None if asked is None else asked + 1.0  # not back a second too early
                self._backoff(attempt, retry_after)
                continue
            if not 200 <= status < 300:
                title, detail = problem.get("title", ""), problem.get("detail", "")
                raise RequestFailed(f"Horizon answered {status} to GET {label}: {title} {detail}".strip())
            body = self._decode(response, label)
            if not self._take(path, body):
                # Answered, but by a server that is not to be used.
                continue
            self._note_answered(label)
            self._note_answer(path, body)
            return body
        raise AssertionError("unreachable")

    # -- hooks: what a pool of servers replaces ------------------------------
    # Here each is what one server needs: nothing, or what was always done.

    def _begin(self, path: str, query: Mapping[str, Any]) -> None:
        """A new request starts."""

    def _prepare(self, path: str, query: Mapping[str, Any]) -> None:
        """Before an attempt: name the server that is to answer it."""
        self._local.server = self._http
        self._local.served_by, self._local.history_from = self.name, None

    def _exchange(self, path: str, query: Mapping[str, Any], server: httpx.Client) -> httpx.Response:
        """One attempt of one request, on the server named by ``_prepare``."""
        return server.get(path, params=dict(query))

    def _stalled(self, server: httpx.Client) -> None:
        """The attempt hit the deadline: drop its connection."""
        stuck, self._http = self._http, self._new_client()
        stuck.close()

    def _note_transport_error(self, path: str, query: Mapping[str, Any], error: Exception) -> None:
        """No answer at all."""

    def _note_status(self, path: str, status: int, response: httpx.Response) -> None:
        """A status that is worth asking again."""

    def _resource_missing(
        self, path: str, query: Mapping[str, Any], error: ResponseError
    ) -> dict[str, Any] | None:
        """404 or 410: is there another server to ask? Its answer, or None."""
        return None

    def _take(self, path: str, body: Mapping[str, Any]) -> bool:
        """Whether an answer is from a server that is to be used."""
        return True

    def _note_answer(self, path: str, body: Mapping[str, Any]) -> None:
        """A usable answer."""
        served_by = getattr(self._local, "served_by", None)
        if served_by is not None:
            self.stats.by_server[served_by] += 1

    def _links_origin(self) -> SplitResult:
        """The server `_links.next` has to name: the one that answered."""
        return self._origin

    def history(self) -> History:
        """What the server keeps, as of now. A request."""
        root = self.root()
        return History(
            int(root["history_elder_ledger"]),
            int(root["history_latest_ledger"]),
            self.name,
            root.get("network_passphrase"),
            root.get("history_latest_ledger_closed_at"),
        )

    def lacks(self, ledger: int) -> bool:
        """Whether nothing this client can ask keeps ``ledger`` or anything
        below it: what lies there can no longer be read."""
        return True

    def forget_answered(self) -> None:
        """A new reading sequence begins: forget what was answered last.

        Refusing to ask for the same page again and again is about one
        sequence. A collector's cycle that finds nothing new asks for little
        but the source's root, cycle after cycle, and is not going round in
        circles.
        """
        with self._pace_lock:
            self._answered, self._times_answered = None, 0

    def _note_answered(self, label: str) -> None:
        """Refuse to go round in circles.

        Asking for the same page again and again is a bug in the caller, and
        on a public server it is also a load it should not see. Retries of a
        failed request do not count: only answers do.
        """
        with self._pace_lock:
            self._times_answered = self._times_answered + 1 if label == self._answered else 1
            self._answered = label
            repeated = self._times_answered
        if repeated > MAX_SAME_REQUEST:
            raise MalformedResponse(
                f"GET {label} was answered {repeated} times in a row: the same page is being asked for over and over"
            )

    def _pace(self) -> None:
        """Reserve the next slot for a request and wait for it."""
        with self._pace_lock:
            now = self._clock()
            start = max(now, self._next_slot)
            self._next_slot = start + self._interval
        if start > now:
            self._sleep(start - now)

    def _backoff(self, attempt: int, retry_after: float | None) -> None:
        cap = min(self.settings.backoff_cap_seconds, self.settings.backoff_base_seconds * 2 ** (attempt - 1))
        # Exponential in the attempt, random in its upper half: requests that
        # failed together do not retry together.
        delay = self._rng.uniform(cap / 2, cap)
        if retry_after is not None:
            delay = max(delay, retry_after)
        self.stats.retries += 1
        self.stats.waited_seconds += delay
        self._sleep(delay)

    def _decode(self, response: httpx.Response, label: str) -> dict[str, Any]:
        content_type = response.headers.get("content-type", "")
        if "json" not in content_type:
            raise MalformedResponse(f"{label}: content type is {content_type!r}, not JSON")
        try:
            body = response.json()
        except ValueError as error:
            raise MalformedResponse(
                f"{label}: body is not JSON ({error}): {response.text[:120]!r}"
            ) from error
        if not isinstance(body, dict):
            raise MalformedResponse(f"{label}: body is {type(body).__name__}, not an object")
        return body

    # -- pages and links -----------------------------------------------------

    def _to_page(
        self, body: Mapping[str, Any], path: str, query: Mapping[str, Any], resource_type: str
    ) -> Page:
        embedded = body.get("_embedded")
        records = embedded.get("records") if isinstance(embedded, dict) else None
        if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
            raise MalformedResponse(f"{path}: the page has no list of records in _embedded.records")
        # Horizon attaches `_links.next` to every page, the last one included,
        # and on an empty page it carries no cursor. A page shorter than the
        # limit is the last one whatever the link says, and the link is only
        # read when it is going to be followed.
        limit = query.get("limit")
        last = not records or (limit is not None and len(records) < int(limit))
        links = body.get("_links")
        href = None if last else (links.get("next", {}).get("href") if isinstance(links, dict) else None)
        next_query = self._next_query(href, path, query) if href else None
        return Page(
            tuple(records),
            path,
            resource_type,
            query,
            next_query,
            getattr(self._local, "served_by", None),
            getattr(self._local, "history_from", None),
        )

    def _next_query(self, href: object, path: str, query: Mapping[str, Any]) -> dict[str, Any]:
        """Turn `_links.next` into a request to this same server.

        Only the cursor is taken from the link. Everything else is what was
        asked before, so a link can neither drop a filter nor add a parameter;
        and the link must name this server and this collection, or it is
        refused.
        """
        target = urlsplit(href if isinstance(href, str) else "")
        origin = self._links_origin()
        if (
            (target.scheme, target.hostname, target.port) != (origin.scheme, origin.hostname, origin.port)
            or target.username
            or target.password
            or target.fragment
        ):
            raise UntrustedNextLink(
                f"{path}: _links.next does not stay on {origin.scheme}://{origin.netloc}: {_shown_href(href)}"
            )
        if (
            "\\" in target.path
            or ".." in target.path.split("/")
            or not target.path.rstrip("/").endswith(path.rstrip("/"))
        ):
            raise UntrustedNextLink(f"{path}: _links.next leaves the collection: {_shown_href(href)}")

        given = [(key, str(value)) for key, value in query.items() if value is not None and key != "cursor"]
        found = parse_qsl(target.query, keep_blank_values=True)
        names = [key for key, _ in found]
        if names.count("cursor") != 1 or not all(item in found for item in given):
            raise UntrustedNextLink(
                f"{path}: _links.next does not keep the request {dict(given)}: {_shown_href(href)}"
            )
        unknown = set(names) - {key for key, _ in given} - {"cursor"}
        if unknown:
            raise UntrustedNextLink(f"{path}: _links.next adds parameters {sorted(unknown)}")
        cursor = found[names.index("cursor")][1]
        if not cursor:
            raise MalformedResponse(f"{path}: _links.next has an empty cursor: {_shown_href(href)}")
        return {**query, "cursor": cursor}


def _clean(params: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in params.items() if value is not None}


def _problem(response: httpx.Response) -> dict[str, Any]:
    if "json" not in response.headers.get("content-type", ""):
        return {}
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _retry_after(response: httpx.Response) -> float | None:
    return retry_after_in(response.headers)


def retry_after_in(headers: Mapping[str, str]) -> float | None:
    """Seconds the server asks to wait before asking again.

    Retry-After first. Horizon sends none: it tells the wait in
    ``X-RateLimit-Reset`` (seconds until the allowance of this address is
    back).
    """
    value = ""
    for header in ("Retry-After", "X-RateLimit-Retry-After", "X-RateLimit-Reset"):
        value = headers.get(header, "").strip()
        if value:
            break
    if not value:
        return None
    if value.isdigit():
        return float(value)
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None or when.tzinfo is None:
        return None
    return max(0.0, when.timestamp() - time.time())


def ledger_of(path: str, query: Mapping[str, Any]) -> int | None:
    """The lowest ledger a request needs, when it says.

    A ledger's own path names it; a cursor or an operation id carries it; a
    start cursor that asks for everything after a ledger needs the one after
    it. A request that says none of that — a transaction by its hash — needs
    nothing in particular here: a server that has not got it is asked what
    it has, and the others are asked in its place.
    """
    parts = [part for part in path.split("/") if part]
    if len(parts) == 2 and parts[1].isdigit():
        if parts[0] == "ledgers":
            return int(parts[1])
        if parts[0] == "operations":
            return int(parts[1]) >> 32
    cursor = str(query.get("cursor") or "")
    first, separator, _ = cursor.partition("-")
    if first.isdigit() and len(first) <= 19:
        value = int(first)
        ledger = value >> 32
        if query.get("order") != "desc" and not separator and value & 0xFFFFFFFF == 0xFFFFFFFF:
            ledger += 1  # everything after that ledger
        return ledger
    return None


def _record_key(record: Mapping[str, Any], resource_type: str) -> OrderKey:
    token = record.get("paging_token")
    try:
        return order_key(resource_type, str(token))
    except (InvalidCursor, TypeError) as error:
        raise MalformedResponse(f"record has no usable paging_token: {token!r}") from error


def _boundary(resource_type: str, cursor: str) -> OrderKey:
    """Sort key of a start cursor.

    A start cursor names a position in the total order, not a record, so it
    does not have to obey the rules a record's paging token does. A pair
    cursor given as one number has no order component: it means "that
    position and everything after it".
    """
    if resource_type in ("effect", "trade"):
        first, separator, order = cursor.partition("-")
        if separator:
            return int(first), int(order)
        return _position(first), float("inf")
    if resource_type == "ledger":
        return (_position(cursor) >> 32,)
    return (_position(cursor),)


def _position(number: str) -> int:
    """A total order id given as a decimal string."""
    if not number.isdigit() or len(number) > 19 or int(number) > 2**63 - 1:
        raise MalformedResponse(f"cursor {number!r} is not a total order id")
    return int(number)


def _after(key: OrderKey, other: OrderKey, order: str) -> bool:
    if len(key) != len(other):
        raise MalformedResponse(f"records of different cursor shapes: {key} and {other}")
    if order == "desc":
        return key < other
    if order == "asc":
        return key > other
    raise ValueError(f"unknown order {order!r}")
