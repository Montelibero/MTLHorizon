"""The Horizon client against a mock transport: pace, retries, errors, links."""

from __future__ import annotations

import random
import time
from typing import Any

import httpx
import pytest

from app.sources.horizon import (
    BeforeHistory,
    HorizonClient,
    HorizonSettings,
    MalformedResponse,
    NotFound,
    RateLimited,
    RequestFailed,
    UntrustedNextLink,
)

ORIGIN = "https://horizon.test"
FILTER = {"base_asset_code": "EUR", "base_asset_type": "credit_alphanum4"}


def hal(body: dict[str, Any], status: int = 200) -> httpx.Response:
    kind = "application/problem+json" if status >= 400 else "application/hal+json"
    return httpx.Response(status, headers={"content-type": kind}, json=body)


def problem(status: int, retry_after: str | None = None, reset: str | None = None) -> httpx.Response:
    titles = {400: "Bad Request", 404: "Resource Missing", 410: "Data Requested Is Before Recorded History", 429: "Too Many Requests", 502: "Bad Gateway", 503: "Service Unavailable"}
    response = hal({"type": "https://stellar.org/horizon-errors/x", "title": titles[status], "status": status}, status)
    if retry_after is not None:
        response.headers["Retry-After"] = retry_after
    if reset is not None:
        response.headers.update({"X-RateLimit-Limit": "3000", "X-RateLimit-Remaining": "0", "X-RateLimit-Reset": reset})
    return response


def page(records: list[str], next_cursor: str | None, origin: str = ORIGIN, **overrides: str) -> httpx.Response:
    """A page of trades with the given paging tokens. ``next_cursor`` None
    leaves the link out, as no Horizon does but a test can."""
    query = {**FILTER, "limit": "200", "order": "desc", **overrides}
    links: dict[str, Any] = {"self": {"href": ORIGIN + "/trades"}}
    if next_cursor is not None:
        links["next"] = {"href": f"{origin}/trades?" + "&".join(f"{k}={v}" for k, v in sorted({**query, "cursor": next_cursor}.items()))}
    return hal({"_embedded": {"records": [{"id": token, "paging_token": token} for token in records]}, "_links": links})


def client(handler: Any, **settings: Any) -> tuple[HorizonClient, list[float]]:
    """A client whose clock only moves when it sleeps; the sleeps are kept."""
    sleeps: list[float] = []
    clock = [0.0]

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock[0] += seconds

    horizon = HorizonClient(
        HorizonSettings(ORIGIN, **settings),
        transport=httpx.MockTransport(handler),
        clock=lambda: clock[0],
        sleeper=sleep,
        rng=random.Random(7),
    )
    return horizon, sleeps


def test_requests_are_spaced_by_the_configured_rate() -> None:
    calls: list[str] = []
    horizon, sleeps = client(lambda request: (calls.append(request.url.path), hal({}))[1], requests_per_second=4.0)
    for path in ("/a", "/b", "/c"):
        horizon.get(path)
    assert [round(seconds, 3) for seconds in sleeps] == [0.25, 0.25]
    assert calls == ["/a", "/b", "/c"]


def test_429_is_retried_after_retry_after() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return problem(429, retry_after="3") if len(calls) == 1 else hal({})
    horizon, sleeps = client(handler, requests_per_second=1000)
    horizon.get("/trades")
    assert calls == ["/trades", "/trades"]
    assert sleeps[-1] >= 3.0  # the server's wish, not before
    assert horizon.stats.retries == 1


def test_the_wait_horizon_asks_for_is_in_x_ratelimit_reset() -> None:
    """What the public server answers when the allowance of an address is
    used up: no Retry-After, the wait in X-RateLimit-Reset."""
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return problem(429, reset="3") if len(calls) == 1 else hal({})

    horizon, sleeps = client(handler, requests_per_second=1000)
    horizon.get("/trades")
    assert len(calls) == 2 and sleeps[-1] >= 4.0  # the asked three seconds, and one more
    assert horizon.stats.rate_limited == 1


def test_a_wait_longer_than_the_run_allows_stops_it_cleanly() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return problem(429, reset="300")

    horizon, sleeps = client(handler, max_retry_after_seconds=120.0, requests_per_second=1000)
    with pytest.raises(RateLimited, match="300s"):
        horizon.get("/trades")
    assert len(calls) == 1 and not sleeps


def test_5xx_is_retried_with_growing_backoff() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return problem(503) if len(calls) <= 2 else hal({})
    horizon, sleeps = client(handler, requests_per_second=1000, backoff_base_seconds=1.0)
    horizon.get("/trades")
    assert calls == ["/trades"] * 3
    first, second = (s for s in sleeps if s > 0)
    assert 0.5 <= first <= 1.0 and 1.0 <= second <= 2.0  # exponential, jittered in the upper half


def test_a_request_that_never_answers_is_given_up_on_and_repeated() -> None:
    """A connection the server dropped without a word: the read does not
    return, and not even the socket timeout ends it."""
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if len(calls) == 1:
            time.sleep(2.0)
        return hal({})

    horizon, _ = client(handler, requests_per_second=1000, request_deadline_seconds=0.2, max_attempts=2)
    started = time.monotonic()
    assert horizon.get("/trades") == {}
    assert time.monotonic() - started < 1.5
    assert len(calls) == 2 and horizon.stats.abandoned == 1 and horizon.stats.retries == 1


def test_gives_up_after_max_attempts() -> None:
    horizon, _ = client(lambda request: problem(502), max_attempts=3, requests_per_second=1000)
    with pytest.raises(RequestFailed, match="all 3 attempts"):
        horizon.get("/trades")
    assert horizon.stats.requests == 3 and horizon.stats.retries == 2


def test_retry_after_beyond_what_a_run_may_wait_stops_the_run() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return problem(429, retry_after="3600") if len(calls) == 1 else hal({})
    horizon, sleeps = client(handler, max_retry_after_seconds=30.0, requests_per_second=1000)
    with pytest.raises(RateLimited):
        horizon.get("/trades")
    assert calls == ["/trades"] and not sleeps


def test_connection_errors_are_retried() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if len(calls) == 1:
            raise httpx.ConnectError("refused")
        return hal({})

    horizon, _ = client(handler, requests_per_second=1000)
    horizon.get("/trades")
    assert calls == ["/trades", "/trades"]


@pytest.mark.parametrize(("status", "error"), [(404, NotFound), (410, BeforeHistory)])
def test_404_and_410_are_final_and_distinct(status: int, error: type) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return problem(status)

    horizon, sleeps = client(handler, requests_per_second=1000)
    with pytest.raises(error):
        horizon.get("/trades")
    assert calls == ["/trades"] and not sleeps


def test_other_client_errors_are_not_retried() -> None:
    horizon, _ = client(lambda request: problem(400), requests_per_second=1000)
    with pytest.raises(RequestFailed, match="answered 400 to GET /operations/not-a-number: Bad Request"):
        horizon.get("/operations/not-a-number")
    assert horizon.stats.requests == 1


def test_redirects_are_not_followed() -> None:
    horizon, _ = client(lambda request: httpx.Response(302, headers={"location": "https://evil.test/x"}), requests_per_second=1000)
    with pytest.raises(RequestFailed):
        horizon.get("/trades")


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, text="<html>gateway</html>"),
        httpx.Response(200, headers={"content-type": "application/hal+json"}, text="{not json"),
        hal([1, 2, 3]),
        hal({"_embedded": {"records": {"id": "1"}}}),
        hal({"_links": {}}),
        hal({"_embedded": {"records": ["not a record"]}}),
    ],
)
def test_answers_that_are_not_pages_are_errors(response: httpx.Response) -> None:
    horizon, _ = client(lambda request: response, requests_per_second=1000)
    with pytest.raises(MalformedResponse):
        horizon.page("/trades", resource_type="trade", limit=200, order="desc", **FILTER)


def test_follows_the_checked_next_link_and_keeps_the_filters() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.query.decode())
        first = page(["9007199254740997-0", "9007199254740995-7"], "9007199254740995-7", limit="2")
        return first if len(seen) == 1 else page(["9007199254740993-1"], "9007199254740993-1", limit="2")

    horizon, _ = client(handler, requests_per_second=1000)
    pages = list(horizon.iter_pages("/trades", resource_type="trade", limit=2, order="desc", **FILTER))
    # The cursor is carried as it came, well beyond what a float could hold.
    assert [page_.records[0]["paging_token"] for page_ in pages] == ["9007199254740997-0", "9007199254740993-1"]
    assert [tuple(sorted(query.split("&"))) for query in seen] == [
        ("base_asset_code=EUR", "base_asset_type=credit_alphanum4", "limit=2", "order=desc"),
        ("base_asset_code=EUR", "base_asset_type=credit_alphanum4", "cursor=9007199254740995-7", "limit=2", "order=desc"),
    ]


def test_a_short_page_is_the_last_page_even_with_a_next_link() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return page(["5-0"], "5-0")

    horizon, _ = client(handler, requests_per_second=1000)
    pages = list(horizon.iter_pages("/trades", resource_type="trade", limit=200, order="desc", **FILTER))
    assert len(pages) == 1 and pages[0].exhausted and len(calls) == 1


def test_a_full_page_is_followed_by_an_empty_one() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.query.decode())
        if len(calls) == 1:
            return page(["5-1", "5-0"], "5-0", limit="2")
        return page([], "5-0", limit="2")

    horizon, _ = client(handler, requests_per_second=1000)
    pages = list(horizon.iter_pages("/trades", resource_type="trade", limit=2, order="desc", **FILTER))
    assert [len(page_.records) for page_ in pages] == [2] and len(calls) == 2


def test_a_cursor_that_comes_back_is_an_error() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.query.decode())
        return page(["9-0", "8-0"], "8-0", limit="2") if len(calls) == 1 else page(["9-0", "8-0"], "8-0", limit="2")

    horizon, _ = client(handler, requests_per_second=1000)
    with pytest.raises(MalformedResponse, match="not strictly desc|does not advance"):
        list(horizon.iter_pages("/trades", resource_type="trade", limit=2, order="desc", **FILTER))


@pytest.mark.parametrize(
    "tokens",
    [["10-1", "9-1", "9-1"], ["10-1", "11-1"]],  # a repeat, and a jump back towards newer
)
def test_records_out_of_order_are_an_error(tokens: list[str]) -> None:
    horizon, _ = client(lambda request: page(tokens, None), requests_per_second=1000)
    with pytest.raises(MalformedResponse, match="not strictly desc"):
        list(horizon.iter_pages("/trades", resource_type="trade", limit=200, order="desc", **FILTER))


def test_a_record_beyond_the_start_cursor_is_an_error() -> None:
    horizon, _ = client(lambda request: page(["900000000000-0"], None), requests_per_second=1000)
    with pytest.raises(MalformedResponse, match="not beyond the cursor"):
        list(horizon.iter_pages("/trades", resource_type="trade", limit=200, order="desc", cursor="500000000000-0", **FILTER))


def test_a_single_number_start_cursor_bounds_pair_records_by_position() -> None:
    """`cursor=<position>` excludes that position's own operation: the trades
    of a transaction are read from the transaction's own position."""
    horizon, _ = client(lambda request: page(["500000000001-0"], None), requests_per_second=1000)
    pages = list(horizon.iter_pages("/trades", resource_type="trade", limit=200, order="asc", cursor="500000000000", **FILTER))
    assert len(pages) == 1


@pytest.mark.parametrize(
    "href",
    [
        "https://evil.test/trades?base_asset_code=EUR&base_asset_type=credit_alphanum4&cursor=4-0&limit=200&order=desc",
        "https://horizon.test:8443/trades?base_asset_code=EUR&base_asset_type=credit_alphanum4&cursor=4-0&limit=200&order=desc",
        "https://user:pw@horizon.test/trades?base_asset_code=EUR&base_asset_type=credit_alphanum4&cursor=4-0&limit=200&order=desc",
        "https://horizon.test/operations?base_asset_code=EUR&base_asset_type=credit_alphanum4&cursor=4-0&limit=200&order=desc",
        "https://horizon.test/../trades?base_asset_code=EUR&base_asset_type=credit_alphanum4&cursor=4-0&limit=200&order=desc",
        "https://horizon.test/trades?cursor=4-0&limit=200&order=desc",  # the filter is gone
        "https://horizon.test/trades?base_asset_code=EUR&base_asset_type=credit_alphanum4&cursor=4-0&limit=200&order=asc",  # the order changed
        "https://horizon.test/trades?base_asset_code=EUR&base_asset_type=credit_alphanum4&cursor=4-0&limit=200&order=desc&trade_type=all",  # a parameter added
    ],
)
def test_next_links_that_do_not_lead_to_the_same_request_are_refused(href: str) -> None:
    # A full page: only there is the link followed, so only there is it read.
    records = [{"id": f"{1000 - number}-1", "paging_token": f"{1000 - number}-1"} for number in range(200)]
    body = {"_embedded": {"records": records}, "_links": {"next": {"href": href}}}
    horizon, _ = client(lambda request: hal(body), requests_per_second=1000)
    with pytest.raises(UntrustedNextLink):
        horizon.page("/trades", resource_type="trade", limit=200, order="desc", **FILTER)


def test_an_empty_page_with_a_next_link_is_the_last_page() -> None:
    """Horizon attaches `next` to an empty page too, with no cursor in it."""
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return page([], "")

    horizon, _ = client(handler, requests_per_second=1000)
    pages = list(horizon.iter_pages("/trades", resource_type="trade", limit=200, order="desc", **FILTER))
    assert [len(page_.records) for page_ in pages] == [0] and len(calls) == 1


def test_a_record_without_a_usable_cursor_is_an_error() -> None:
    body = {"_embedded": {"records": [{"id": "x", "paging_token": "0.5"}]}}
    horizon, _ = client(lambda request: hal(body), requests_per_second=1000)
    with pytest.raises(MalformedResponse, match="paging_token"):
        list(horizon.iter_pages("/trades", resource_type="trade", limit=200, order="desc"))


def test_asking_for_the_same_page_over_and_over_is_an_error() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return hal({})

    horizon, _ = client(handler, requests_per_second=1000)
    for _ in range(3):  # three times is allowed: a resumed stream may start over
        horizon.get("/trades", cursor="5-1")
    with pytest.raises(MalformedResponse, match="asked for over and over"):
        horizon.get("/trades", cursor="5-1")
    assert len(calls) == 4
    assert horizon.get("/trades", cursor="4-1") == {}  # a different request starts again


def test_the_run_can_stop_the_client_between_requests() -> None:
    class Stop(Exception):
        pass

    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return hal({})

    horizon, _ = client(handler, requests_per_second=1000)

    def before_request() -> None:
        if len(calls) >= 2:
            raise Stop

    horizon.before_request = before_request
    with pytest.raises(Stop):
        for _ in range(5):
            horizon.get("/trades")
    assert len(calls) == 2


def test_the_user_agent_names_the_project() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["user-agent"])
        return hal({})

    horizon, _ = client(handler, requests_per_second=1000)
    horizon.get("/")
    assert seen[0].startswith("mtl-horizon/") and "read-only" in seen[0]


def test_stats_are_reported_as_plain_data() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return problem(503) if len(calls) == 1 else hal({})

    horizon, _ = client(handler, requests_per_second=1000, backoff_base_seconds=0.0)
    horizon.get("/")
    assert horizon.stats.as_dict() == {
        "requests": 2, "retries": 1, "responses_by_status": {200: 1, 503: 1}, "waited_seconds": 0.0,
        "rate_limited": 0, "abandoned": 0,
    }
