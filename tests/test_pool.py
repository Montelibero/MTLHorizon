"""The pool of Horizon servers against fake servers: who answers, when the
next one is asked, and what is never done."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from time import monotonic, sleep
from typing import Any
from urllib.parse import urlsplit

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.backfill import config as watch_lists
from app.backfill import verify as checking
from app.backfill.jobs import stream_jobs
from app.backfill.loader import Backfill, Limits, Window
from app.config import PUBLIC_NETWORK_PASSPHRASE, server_name, shown_url
from app.pool import cli as pool_cli
from app.pool.check import NodeCheck, Sample, check_pool
from app.sources.horizon import (
    BeforeHistory,
    HorizonSettings,
    NotFound,
    RequestFailed,
    Scope,
    UntrustedNextLink,
    WrongNetwork,
)
from app.sources.horizon_cursors import after_ledger
from app.sources.pool import HorizonPool, PoolSettings
from app.storage import coverage, pool_state
from app.storage.pace import BACKGROUND, EVENTS, Pace, PaceSettings
from tests.fake_horizon import A, ELDER, LATEST, FakeHorizon, serve
from tests.test_backfill import WATCH_LIST, WINDOW

FIRST, SECOND = "https://a.horizon.test", "https://b.horizon.test"
TESTNET = "Test SDF Network ; September 2015"
LEDGER = "/ledgers/105"
SETTINGS = {"requests_per_second": 1000, "max_attempts": 4, "backoff_base_seconds": 0.0, "backoff_cap_seconds": 0.0}


class Clock:
    """A clock that only moves when something sleeps; the sleeps are kept."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def make(servers: dict[str, FakeHorizon], **options: Any) -> tuple[HorizonPool, Clock]:
    clock = Clock()
    horizon = HorizonPool(
        tuple(servers),
        network_passphrase=options.pop("network_passphrase", None),
        pool_settings=PoolSettings(health_check_interval=60, failover_cooldown=10, **options.pop("pool", {})),
        **{"settings": HorizonSettings(**{**SETTINGS, **options.pop("deadline", {})})},
        transports={shown_url(url): httpx.MockTransport(fake._handle) for url, fake in servers.items()},
        clock=clock,
        sleeper=clock.sleep,
        **options,
    )
    return horizon, clock


def refused() -> httpx.ConnectError:
    return httpx.ConnectError("connection refused")


def paths_of(fake: FakeHorizon) -> list[str]:
    return [path for path, _ in fake.requests]


def test_the_first_server_answers_and_the_other_is_never_asked() -> None:
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND)
    with make({FIRST: one, SECOND: two})[0] as pool:
        for ledger in (105, 106, 107):
            assert pool.get(f"/ledgers/{ledger}")["sequence"] == ledger
        assert pool.name == FIRST
        assert paths_of(two) == []  # not even for its root: a server is asked when it is needed
        assert pool.stats.by_server == {FIRST: 3}


def test_a_server_is_asked_for_its_root_before_its_first_use_and_again_when_that_is_stale() -> None:
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND)
    pool, clock = make({FIRST: one, SECOND: two})
    pool.get(LEDGER)
    pool.get("/ledgers/106")
    assert paths_of(one) == ["/", LEDGER, "/ledgers/106"]  # one root, then the requests
    clock.sleep(61)
    pool.get("/ledgers/107")
    assert paths_of(one)[-2:] == ["/", "/ledgers/107"]  # said of itself again after the interval
    assert pool.stats.requests == 5  # two roots included: they are part of the budget
    pool.close()


def test_a_server_that_stops_answering_is_left_alone_and_the_next_one_answers() -> None:
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND)
    pool, clock = make({FIRST: one, SECOND: two})
    assert pool.get(LEDGER)["sequence"] == 105
    one.down = refused()
    assert pool.get("/ledgers/106")["sequence"] == 106
    assert pool.stats.by_server == {FIRST: 1, SECOND: 1}
    first, second = pool.servers
    assert first.cooling_until is not None and "no answer to its root" in (first.last_error or "")
    assert pool.get("/ledgers/107")["sequence"] == 107
    assert paths_of(one)[-2:] == ["/ledgers/106", "/"]  # nothing more was asked of it
    assert pool.stats.by_server[SECOND] == 2 and pool.stats.failovers == 1
    # Its time over, it is asked again — its root first — and answers.
    clock.sleep(11)
    one.down = None
    assert pool.get("/ledgers/108")["sequence"] == 108
    assert paths_of(one)[-2:] == ["/", "/ledgers/108"]
    assert first.cooling_until is None and first.cooldowns == 0
    assert pool.stats.by_server == {FIRST: 2, SECOND: 2}
    pool.close()


def test_a_cooldown_doubles_while_the_server_keeps_failing() -> None:
    one, two = FakeHorizon(origin=FIRST, down=refused()), FakeHorizon(origin=SECOND)
    pool, clock = make({FIRST: one, SECOND: two})
    first = pool.servers[0]
    pool.get(LEDGER)
    assert first.cooldowns == 1 and first.cooling_until == 10
    clock.sleep(11)
    pool.get("/ledgers/106")
    assert first.cooldowns == 2 and first.cooling_until == pytest.approx(11 + 20, abs=0.1)
    clock.sleep(21)
    pool.get("/ledgers/107")
    assert first.cooldowns == 3 and first.cooling_until == pytest.approx(32 + 40, abs=0.1)
    assert pool.stats.by_server == {SECOND: 3}
    pool.close()


def test_a_503_for_one_request_is_not_a_server_that_failed() -> None:
    """An expensive stream gets a 503 from a server whose root answers at
    once: that server stays, and the request is asked of the next one."""
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND)
    one.fail("/ledgers", 503, times=3)
    pool, _ = make({FIRST: one, SECOND: two})
    assert pool.get(LEDGER)["sequence"] == 105
    assert pool.stats.by_server == {SECOND: 1}
    assert pool.servers[0].cooling_until is None  # its root answered: it stays
    assert paths_of(one)[-1] == "/"  # that is what was asked of it after the 503
    assert pool.stats.responses_by_status[503] == 1 and pool.stats.failovers == 0
    pool.close()


def test_a_503_that_does_not_stop_is_bounded() -> None:
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND)
    one.fail("/ledgers", 503, times=10)
    two.fail("/ledgers", 503, times=10)
    pool, _ = make({FIRST: one, SECOND: two})
    with pytest.raises(RequestFailed, match="503"):
        pool.get(LEDGER)
    # Every server once, then as before: the attempts there always were.
    assert [path for path, _ in one.requests if path != "/"] == [LEDGER] * 2
    assert [path for path, _ in two.requests if path != "/"] == [LEDGER] * 2
    assert pool.servers[0].cooling_until is None and pool.servers[1].cooling_until is None
    pool.close()


def test_a_server_whose_root_fails_too_is_left_alone() -> None:
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND)
    one.fail("", 503, times=10)  # everything, its root included
    pool, _ = make({FIRST: one, SECOND: two})
    assert pool.get(LEDGER)["sequence"] == 105
    assert pool.servers[0].cooling_until is not None
    assert pool.stats.by_server == {SECOND: 1}
    pool.close()


def test_a_429_is_waited_out_and_no_other_server_is_asked() -> None:
    """The limit is the address's: going to another name of the same provider
    would go around it."""

    class Limiter:
        def __init__(self) -> None:
            self.pauses: list[float] = []

        def reserve(self, interval: float) -> float:
            return 0.0

        def again(self, interval: float) -> float:
            return 0.05

        def pause(self, seconds: float) -> None:
            self.pauses.append(seconds)

    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND)
    one.fail("/ledgers", 429, times=1, retry_after="3")
    limiter = Limiter()
    pool, clock = make({FIRST: one, SECOND: two}, limiter=limiter)
    assert pool.get(LEDGER)["sequence"] == 105
    assert paths_of(two) == []
    assert limiter.pauses == [3]  # every process of the installation waits
    assert clock.sleeps[-1] >= 4  # the server's wish, and not a second too early
    assert pool.stats.rate_limited == 1 and pool.stats.by_server == {FIRST: 1}
    pool.close()


def test_a_410_is_answered_by_the_server_that_keeps_the_history() -> None:
    """The first server's history moved up after it said of itself; the
    request below it is asked of the next one, which keeps it."""
    one, two = FakeHorizon(origin=FIRST, elder=110), FakeHorizon(origin=SECOND, elder=ELDER)
    pool, _ = make({FIRST: one, SECOND: two})
    pool.get("/ledgers/120")  # the first server says of itself: history from 110
    one.elder = 114  # and then it reaps
    page = pool.page(f"/accounts/{A}/operations", resource_type="operation", order="desc", cursor=str(after_ledger(113)), limit=200)
    assert page.served_by == SECOND and page.history_from == ELDER
    assert [path for path, _ in one.requests][-1].startswith("/accounts/")
    assert pool.stats.responses_by_status[410] == 1
    pool.close()


def test_the_server_with_a_short_history_is_not_asked_for_what_lies_below_it() -> None:
    one, two = FakeHorizon(origin=FIRST, elder=114), FakeHorizon(origin=SECOND, elder=ELDER)
    pool, _ = make({FIRST: one, SECOND: two})
    pool.get("/ledgers/120")
    page = pool.page(f"/accounts/{A}/operations", resource_type="operation", order="desc", cursor=str(after_ledger(113)), limit=200)
    assert page.served_by == SECOND
    assert 410 not in pool.stats.responses_by_status  # never asked: no round trip wasted
    pool.close()


def test_reading_forwards_needs_the_ledger_after_the_start_cursor_not_the_one_before() -> None:
    one, two = FakeHorizon(origin=FIRST, elder=114), FakeHorizon(origin=SECOND, elder=ELDER)
    pool, _ = make({FIRST: one, SECOND: two})
    page = pool.page(f"/accounts/{A}/operations", resource_type="operation", order="asc", cursor=str(after_ledger(113)), limit=200)
    assert page.served_by == FIRST  # reading forwards from ledger 114 is within its history
    pool.close()


def test_a_lagging_server_is_not_asked_for_what_it_has_not_got() -> None:
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND, latest=122)
    pool, _ = make({SECOND: two, FIRST: one})  # the lagging one first in the list
    pool.scope = Scope(through=125)
    assert pool.get("/ledgers/125")["sequence"] == 125
    assert paths_of(two) == ["/"]  # said of itself, and was not asked again
    one.down = refused()
    with pytest.raises(RequestFailed, match=f"{SECOND} has got only to ledger 122"):
        pool.get("/ledgers/126")
    pool.close()


def test_the_scope_keeps_a_server_out_whose_history_starts_above_the_window() -> None:
    one, two = FakeHorizon(origin=FIRST, elder=110), FakeHorizon(origin=SECOND, elder=ELDER)
    pool, _ = make({FIRST: one, SECOND: two})
    pool.scope = Scope(from_ledger=105, through=125)
    assert pool.get("/ledgers/120")["sequence"] == 120  # the request is about 120, the window about 105
    assert paths_of(one) == ["/", "/ledgers/120"]
    pool.close()


def test_a_server_of_another_network_is_never_asked() -> None:
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND, network=TESTNET)
    pool, _ = make({FIRST: one, SECOND: two}, network_passphrase=FakeHorizon().network)
    assert pool.get(LEDGER)["sequence"] == 105
    assert paths_of(two) == []  # not asked what it is until it is needed
    one.down = refused()
    with pytest.raises(RequestFailed, match=SECOND + " serves another network"):
        pool.get("/ledgers/106")
    assert paths_of(two) == ["/"]  # asked once what it is, and never again
    assert pool.servers[1].wrong_network and pool.scope is None
    pool.close()


def test_a_pool_of_another_network_says_so_and_claims_nothing() -> None:
    one = FakeHorizon(origin=FIRST, network=TESTNET)
    pool, _ = make({FIRST: one}, network_passphrase=FakeHorizon().network)
    with pytest.raises(WrongNetwork, match=FIRST):
        pool.history()
    with pytest.raises(RequestFailed, match="serves another network"):
        pool.get(LEDGER)
    assert pool.stats.by_server == {}
    pool.close()


def test_history_tells_what_the_whole_pool_keeps() -> None:
    one, two = FakeHorizon(origin=FIRST, elder=110), FakeHorizon(origin=SECOND, latest=125)
    pool, _ = make({FIRST: one, SECOND: two})
    history = pool.history()
    assert (history.elder, history.latest, history.name) == (ELDER, LATEST, FIRST)
    assert not pool.lacks(105) and pool.lacks(ELDER - 1)  # the first keeps 105, nobody keeps 99
    pool.close()


def test_the_history_of_the_pool_does_not_shrink_while_a_server_is_silent() -> None:
    """The server that keeps the oldest history goes down. What it keeps is
    not given up on while it cannot be asked: the pool goes on saying it
    keeps from where that server said it did, and names it as silent — so
    that nothing below the others' history is called out of reach for good."""
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND, elder=110)
    pool, clock = make({FIRST: one, SECOND: two})
    try:
        assert pool.history().elder == ELDER and pool.silent() == []
        one.down = refused()
        assert pool.get("/ledgers/120")["sequence"] == 120  # the second answered in its place
        history = pool.history()
        assert (history.elder, history.name, history.latest) == (ELDER, SECOND, LATEST)
        assert pool.silent() == [FIRST] and not pool.lacks(ELDER)
        clock.sleep(11)  # its cooldown is over
        one.down = None
        assert pool.history().elder == ELDER and pool.silent() == []
    finally:
        pool.close()


def test_nothing_is_below_the_history_of_a_pool_while_a_server_has_not_said_what_it_keeps() -> None:
    one, two = FakeHorizon(origin=FIRST, elder=110), FakeHorizon(origin=SECOND)
    pool, _ = make({FIRST: one, SECOND: two})
    pool.get("/ledgers/120")  # only the first has said of itself
    assert not pool.lacks(105)  # the second may keep it
    assert pool.history().elder == ELDER
    assert pool.lacks(ELDER - 1)
    pool.close()


def test_when_no_server_answers_nothing_is_claimed() -> None:
    one, two = FakeHorizon(origin=FIRST, down=refused()), FakeHorizon(origin=SECOND, down=refused())
    pool, _ = make({FIRST: one, SECOND: two})
    with pytest.raises(RequestFailed, match="no server of the pool can answer"):
        pool.get(LEDGER)
    assert pool.stats.by_server == {} and pool.stats.failovers == 2
    pool.close()


def test_a_404_is_asked_of_the_other_servers() -> None:
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND)
    operation = next(iter(one.ledger.operations))
    pool, _ = make({FIRST: one, SECOND: two})
    one.hidden_operations.add(operation)
    assert pool.get(f"/operations/{operation}")["id"] == operation
    assert pool.stats.by_server == {SECOND: 1}
    pool.close()


def test_a_404_from_every_server_that_keeps_the_ledger_is_the_resources_absence() -> None:
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND)
    operation = next(iter(one.ledger.operations))
    pool, _ = make({FIRST: one, SECOND: two})
    one.hidden_operations.add(operation)
    two.hidden_operations.add(operation)
    with pytest.raises(NotFound, match=operation):
        pool.get(f"/operations/{operation}")
    pool.close()


def test_a_404_proves_nothing_while_a_server_that_might_have_it_cannot_be_asked() -> None:
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND, down=refused())
    operation = next(iter(one.ledger.operations))
    pool, _ = make({FIRST: one, SECOND: two})
    one.hidden_operations.add(operation)
    with pytest.raises(RequestFailed, match=f"{FIRST} answered 404, and {SECOND} could not be asked"):
        pool.get(f"/operations/{operation}")
    assert pool.stats.by_server == {}
    pool.close()


def test_a_410_below_every_servers_history_is_the_end_with_what_they_keep() -> None:
    one, two = FakeHorizon(origin=FIRST, elder=110), FakeHorizon(origin=SECOND, elder=108)
    pool, _ = make({FIRST: one, SECOND: two})
    pool.get("/ledgers/120")
    one.elder, two.elder = 114, 115  # both reap after the first said of itself
    with pytest.raises(BeforeHistory) as ended:
        pool.page(f"/accounts/{A}/operations", resource_type="operation", order="desc", cursor=str(after_ledger(113)), limit=200)
    assert ended.value.history is not None
    assert (ended.value.history.elder, ended.value.history.name) == (114, FIRST)
    assert [path for path, _ in two.requests] == ["/"]  # asked what it keeps, and no more: it keeps nothing there
    pool.close()


def test_a_410_of_the_first_and_a_404_of_the_one_that_keeps_the_ledger_is_not_found() -> None:
    """The first reaped after it said of itself; the second keeps the ledger
    and has no such transaction: that is its absence, not the end of the
    history."""
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND)
    pool, _ = make({FIRST: one, SECOND: two})
    transaction = next(record["hash"] for record in two.ledger.transactions.values() if record["ledger"] <= 110)
    assert pool.get("/ledgers/120")["sequence"] == 120
    one.elder = 114
    two.hidden_transactions.add(transaction)
    with pytest.raises(NotFound, match="which keeps ledger"):
        pool.get(f"/transactions/{transaction}", ledger_hint=110)
    assert pool.stats.responses_by_status[410] == 1
    pool.close()


def test_a_404_of_a_server_that_has_not_got_that_far_says_nothing() -> None:
    """A transaction of a ledger the second server has not ingested: its 404
    is not the transaction's absence."""
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND, latest=104)
    pool, _ = make({FIRST: one, SECOND: two})
    pool.scope = Scope(from_ledger=105, through=110)
    assert pool.get("/ledgers/120")["sequence"] == 120  # the first: elder 100, latest 130
    one.down = refused()
    with pytest.raises(RequestFailed, match="has got only to ledger 104"):
        pool.get(f"/transactions/{next(iter(one.ledger.transactions))}")
    assert paths_of(two) == ["/"]
    pool.close()


def test_the_reading_goes_on_by_cursor_when_the_server_changes_inside_it() -> None:
    """The first server dies after two pages; the rest comes from the next
    one, in order, and is what that one would have given from the start."""
    whole, one, two = (FakeHorizon(origin=url) for url in (FIRST, FIRST, SECOND))
    pool, _ = make({FIRST: one, SECOND: two})
    path = f"/accounts/{A}/operations"

    def read(client: Any) -> list[str]:
        pages = client.iter_pages(path, resource_type="operation", order="desc", cursor=str(after_ledger(LATEST)), limit=1)
        return [str(record["id"]) for page in pages for record in page.records]

    expected = read(whole.client)
    assert len(expected) >= 4  # several pages of one record

    def die(asked: str) -> None:
        if asked == path and sum(1 for p, _ in one.requests if p == path) == 2:
            one.down = refused()  # the request after this one finds it gone

    one.before_answer = die
    try:
        got, servers = [], []
        pages = pool.iter_pages(path, resource_type="operation", order="desc", cursor=str(after_ledger(LATEST)), limit=1)
        for page in pages:
            got += [str(record["id"]) for record in page.records]
            servers.append(page.served_by)
        assert got == expected
        assert servers[:2] == [FIRST, FIRST] and set(servers[2:]) == {SECOND}
        assert pool.stats.failovers == 1
    finally:
        pool.close()


def test_a_request_that_never_answers_is_given_up_on_and_the_server_with_it() -> None:
    """A connection the server dropped without a word: the read is given up
    on at the deadline, the server's root is asked under a shorter one, and
    when that does not answer either the server is left alone."""
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND)
    pool, _ = make({FIRST: one, SECOND: two}, pool={"probe_timeout": 0.1}, deadline={"request_deadline_seconds": 0.1})
    assert pool.get(LEDGER)["sequence"] == 105
    one.stall = 0.5
    started = monotonic()
    assert pool.get("/ledgers/106")["sequence"] == 106
    assert monotonic() - started < 0.9  # 0.1 s for the request, 0.1 s for its root, then the next server
    assert pool.stats.by_server == {FIRST: 1, SECOND: 1}
    assert pool.stats.abandoned == 2 and pool.stats.failovers == 1
    pool.close()


# -- through the loaders, into PostgreSQL --------------------------------------


class PoolPilot:
    """A private database with the watch list imported, read through a pool
    of fake servers."""

    def __init__(self, url: str, *fakes: FakeHorizon, limiter: bool = False) -> None:
        self.engine = create_engine(url)
        self.fakes = fakes
        self.clock = Clock()
        self.pool = HorizonPool(
            tuple(fake.origin for fake in fakes),
            HorizonSettings(**SETTINGS),
            network_passphrase=PUBLIC_NETWORK_PASSPHRASE,
            limiter=Pace(self.engine, settings=PaceSettings(idle=0.2)) if limiter else None,
            remember=lambda server: pool_state.note(self.engine, server.as_dict()),
            pool_settings=PoolSettings(health_check_interval=60, failover_cooldown=10),
            transports={fake.origin: httpx.MockTransport(fake._handle) for fake in fakes},
            clock=self.clock,
            sleeper=self.clock.sleep,
        )
        with Session(self.engine) as session, session.begin():
            watch_lists.apply(session, WATCH_LIST)

    def close(self) -> None:
        self.pool.close()
        self.engine.dispose()

    def run(self, window: Window = WINDOW, **limits: Any) -> list[Any]:
        self.pool.scope = Scope(window.from_ledger, window.to_ledger)  # what `backfill run` does
        limits_ = Limits(requests=lambda: self.pool.stats.requests, **limits)
        with Backfill(
            self.pool, self.engine, source=f"horizon:{urlsplit(self.fakes[0].origin).hostname}", page_size=200, limits=limits_
        ) as backfill:
            self.backfill = backfill
            return backfill.run(window, self.jobs())

    def jobs(self) -> list[Any]:
        with Session(self.engine) as session:
            return stream_jobs(session)[0]

    def coverage(self) -> list[tuple[str, int, int, str | None, str | None]]:
        """State, from, to, detail and server of every range of every stream."""
        with Session(self.engine) as session:
            rows = [row for job in self.jobs() for row in coverage.ranges(session, job.key)]
        return sorted((row.state, row.from_ledger, row.to_ledger, row.detail, row.served_by) for row in rows)


FULL, SHORT = "https://full.horizon.test", "https://short.horizon.test"


def pooled(make_database: Any, *fakes: FakeHorizon, limiter: bool = False) -> PoolPilot:
    return PoolPilot(make_database(), *fakes, limiter=limiter)


def test_a_window_is_read_whole_from_a_pool_whose_first_server_keeps_only_its_top(make_database: Callable[[], str]) -> None:
    """The first server reaped the bottom of the window; the second keeps
    it. The pool hands the rest over, and the result is what the second
    server alone would have stored — with no range given up as unverifiable."""
    short, full = FakeHorizon(origin=SHORT, elder=112), FakeHorizon(origin=FULL)
    pooled_pilot = pooled(make_database, short, full)
    whole = pooled(make_database, FakeHorizon(origin=FULL))
    try:
        pooled_pilot.run()
        whole.run()
        assert checking.fingerprint(pooled_pilot.engine, WINDOW) == checking.fingerprint(whole.engine, WINDOW)
        rows = pooled_pilot.coverage()
        assert {row[0] for row in rows} == {"complete", "empty"}  # nothing unverifiable, nothing pending
        read_by = {name for row in rows for name in (row[4] or "").split(",")}
        assert read_by == {SHORT, FULL}  # the top from the first, the bottom from the second
        assert all(row[4] == FULL for row in rows if row[2] < 112)  # below the first one's history: the second alone
        assert 410 not in pooled_pilot.pool.stats.responses_by_status  # never asked for what it had not got
        asked = [int(query["cursor"]) >> 32 for path, query in short.requests if "cursor" in query]
        assert min(asked) >= 112
    finally:
        pooled_pilot.close()
        whole.close()


def test_a_run_goes_on_from_the_next_server_when_the_first_dies_inside_it(make_database: Callable[[], str]) -> None:
    first, second = FakeHorizon(origin=FULL), FakeHorizon(origin=SHORT)
    pooled_pilot = pooled(make_database, first, second)
    whole = pooled(make_database, FakeHorizon(origin=FULL))
    asked = 0

    def die(path: str) -> None:
        nonlocal asked
        asked += 1
        if asked == 12:
            first.down = refused()

    first.before_answer = die
    try:
        pooled_pilot.run()
        whole.run()
        assert checking.fingerprint(pooled_pilot.engine, WINDOW) == checking.fingerprint(whole.engine, WINDOW)
        assert {row[0] for row in pooled_pilot.coverage()} == {"complete", "empty"}
        assert pooled_pilot.pool.stats.failovers >= 1 and pooled_pilot.pool.stats.by_server[SHORT] > 0
        with Session(pooled_pilot.engine) as session:
            kept = pool_state.rows(session)
        # What the running process saw of the first server is kept for a status page.
        state = next(row for row in kept if row.url == FULL)
        assert state.last_error is not None and state.cooling_until is not None
    finally:
        pooled_pilot.close()
        whole.close()


def test_when_every_server_is_down_the_window_stays_pending_and_is_read_when_one_is_back(
    make_database: Callable[[], str],
) -> None:
    first, second = FakeHorizon(origin=FULL, down=refused()), FakeHorizon(origin=SHORT, down=refused())
    pilot = pooled(make_database, first, second)
    try:
        reports = pilot.run()
        assert {report.status for report in reports} == {"failed"}
        assert all(row[0] == "pending" for row in pilot.coverage())  # nothing claimed
        assert pilot.pool.stats.by_server == {}
        second.down = None
        pilot.clock.sleep(11)  # the cooldown over
        assert {report.status for report in pilot.run()} == {"verified"}
        assert {row[0] for row in pilot.coverage()} == {"complete", "empty"}
        assert {row[4] for row in pilot.coverage()} == {SHORT}
    finally:
        pilot.close()


def test_a_server_of_another_network_is_never_read_even_when_it_is_the_only_one_answering(
    make_database: Callable[[], str],
) -> None:
    stranger, one = FakeHorizon(origin=FULL, network=TESTNET), FakeHorizon(origin=SHORT)
    pilot = pooled(make_database, stranger, one)
    try:
        assert {report.status for report in pilot.run()} == {"verified"}
        assert [path for path, _ in stranger.requests] == ["/"]  # asked once what it is
        assert {row[4] for row in pilot.coverage()} == {SHORT}
    finally:
        pilot.close()


# -- the pace every process shares ---------------------------------------------


def pacer(engine: Any, name: str, share: str = BACKGROUND, **settings: Any) -> Pace:
    """A process's part in a pace of its own, so that tests do not share one."""
    return Pace(engine, share=share, name=f"test-{name}", settings=PaceSettings(**settings))


def test_slots_are_given_out_once_over_every_process(engine: Any) -> None:
    pace = "slots"
    pacer(engine, pace).reserve(0.01)
    first = pacer(engine, pace).reserve(1.0)
    second = pacer(engine, pace).reserve(1.0)
    third = pacer(engine, pace).reserve(1.0)
    assert first == pytest.approx(0.0, abs=0.05)
    assert second == pytest.approx(1.0, abs=0.05) and third == pytest.approx(2.0, abs=0.05)


def test_events_go_first_and_a_background_reader_is_never_starved(engine: Any) -> None:
    pace = "events-first"
    events = pacer(engine, pace, EVENTS)
    back = pacer(engine, pace, BACKGROUND, spare=10, idle=0.2)
    assert back.reserve(1.0) == pytest.approx(0.0, abs=0.05)  # no event around: the reader has every request
    events.reserve(1.0)
    assert back.reserve(1.0) is None  # an event went just now: the reader waits
    waiting = 0
    for _ in range(12):  # events keep coming: one request in ten is the reader's
        events.reserve(1.0)
        if back.reserve(1.0) is not None:
            break
        waiting += 1
    assert waiting == 8  # the nine slots after the event that went before the loop are the events'
    events.reserve(1.0)
    assert back.reserve(1.0) is None
    sleep(0.25)  # events have stopped: nothing is kept waiting for them
    assert back.reserve(1.0) is not None


def test_a_429_pauses_every_process(engine: Any) -> None:
    pace = "pause"
    pacer(engine, pace, EVENTS).reserve(0.01)
    pacer(engine, pace, EVENTS).pause(3)
    assert pacer(engine, pace, EVENTS).reserve(1.0) == pytest.approx(3.0, abs=0.1)


def test_two_pools_do_not_ask_for_more_than_one_pace(make_database: Callable[[], str]) -> None:
    """A worker and a backfill at the same time, 100 requests a second each
    by their settings: together they still get 100 a second."""
    url = make_database()
    worker, back = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND)
    limiter_engine = create_engine(url)
    pools = [
        HorizonPool(
            (fake.origin,),
            HorizonSettings(**{**SETTINGS, "requests_per_second": 100}),
            limiter=Pace(limiter_engine, share=share, settings=PaceSettings(idle=0.05)),
            transports={fake.origin: httpx.MockTransport(fake._handle)},
        )
        for fake, share in ((worker, EVENTS), (back, BACKGROUND))
    ]
    done: list[int] = []

    def read(pool: HorizonPool) -> None:
        for ledger in range(100, 110):
            pool.get(f"/ledgers/{ledger}")
        done.append(1)

    started = monotonic()
    threads = [threading.Thread(target=read, args=(pool,)) for pool in pools]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
        spent = monotonic() - started
        assert done == [1, 1]
        # Twenty requests at a hundred a second take 0.19 s; each pool on its
        # own pace would have taken 0.10 s.
        assert spent >= 0.17
    finally:
        for pool in pools:
            pool.close()
        limiter_engine.dispose()


# -- the check -----------------------------------------------------------------


def test_a_check_asks_every_server_on_its_own_and_lets_no_other_answer_for_it() -> None:
    good, broken, stranger, behind = (
        FakeHorizon(origin=FIRST),
        FakeHorizon(origin=SECOND),
        FakeHorizon(origin="https://c.horizon.test", network=TESTNET),
        FakeHorizon(origin="https://d.horizon.test", latest=122),
    )
    broken.fail("", 503, times=20)  # everything it is asked, its root included
    good.headers = {"X-RateLimit-Limit": "3000", "X-RateLimit-Remaining": "2997"}
    pool, _ = make({fake.origin: fake for fake in (good, broken, stranger, behind)}, network_passphrase=FakeHorizon().network)
    try:
        checked = check_pool(pool)
    finally:
        pool.close()
    good_check, broken_check, stranger_check, behind_check = checked.servers
    assert checked.active == FIRST
    assert good_check.available and good_check.root_ms is not None and good_check.history_ms is not None
    assert good_check.latest_ledger == LATEST and good_check.elder_ledger == ELDER and good_check.lag_ledgers == 0
    assert good_check.rate_limit_headers == {"X-RateLimit-Limit": "3000", "X-RateLimit-Remaining": "2997"}
    assert good_check.ledger == LATEST - 1 and good_check.history_ok
    assert not broken_check.available and broken_check.reason == "its root does not answer"
    assert broken_check.root_ms is None and broken_check.history_ms is None
    kept = broken_check.as_dict()
    assert kept["root_status"] == 503 and kept["errors"] == 6 and kept["timeouts"] == 0
    assert not stranger_check.available and stranger_check.network_ok is False
    assert behind_check.available and behind_check.lag_ledgers == LATEST - 122
    assert broken_check.rate_limit_headers == {}  # nothing is invented where the server sent none
    # Every server got its own requests and not one more: six at most, two kinds of three.
    assert [len(fake.requests) for fake in (good, broken, stranger, behind)] == [6, 6, 6, 6]
    assert checked.requests == 24 == pool.stats.requests
    # A check leaves the pool as it was: no server preferred, none left alone, nothing remembered of it.
    assert all(server.root is None and server.cooling_until is None for server in pool.servers)
    json.dumps(checked.as_dict())  # plain data, as a page or a table wants it


def test_a_check_does_not_ask_a_third_time_after_two_timeouts_in_a_row() -> None:
    good, slow = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND, stall=0.5)
    pool, _ = make({FIRST: good, SECOND: slow}, deadline={"request_deadline_seconds": 0.1})
    try:
        checked = check_pool(pool)
    finally:
        pool.close()
    good_check, slow_check = checked.servers
    assert len(slow_check.root) == 2 and not slow_check.available
    assert slow_check.as_dict()["timeouts"] == 4
    assert good_check.available and len(good_check.root) == 3
    assert slow_check.ledger == LATEST - 1  # asked for the ledger the good server has, and timed out twice
    assert len(slow_check.history) == 2


def test_the_median_is_of_the_answers_that_came() -> None:
    samples = [
        Sample(0.010, 200, None, {}, {}),
        Sample(0.020, 200, None, {}, {}),
        Sample(1.5, 200, None, {}, {}),
        Sample(0.3, 503, None, {}, None),
        Sample(0.4, None, "ReadTimeout", {}, None),
    ]
    assert NodeCheck(FIRST, 1, root=samples).root_ms == 20


def test_pool_check_and_status_as_commands(
    make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    good, broken = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND)
    broken.fail("", 503, times=20)
    serve(monkeypatch, good, broken)
    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", make_database())

    assert pool_cli.main(["check", "--repeats", "1"]) == pool_cli.FAILED  # one of the two is not available

    out = capsys.readouterr().out
    assert f"1. {FIRST} — available, active" in out
    assert f"2. {SECOND} — unavailable: its root does not answer" in out
    assert "rate limit: the server sends no rate limit headers" in out
    assert f"active source: {FIRST}" in out
    assert pool_cli.main(["status"]) == pool_cli.OK
    out = capsys.readouterr().out
    assert FIRST in out and SECOND in out and "unavailable" in out and "available" in out
    assert "median of the root answers" in out
    assert pool_cli.main(["check", "--repeats", "9"]) == pool_cli.FAILED  # bounded: three at most
    assert "request(s)" in capsys.readouterr().out


def test_a_command_refuses_to_run_on_a_database_without_the_pace_table(
    unmigrated_database_url: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    serve(monkeypatch, FakeHorizon())
    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", unmigrated_database_url)
    assert pool_cli.main(["check"]) == pool_cli.FAILED
    assert "error:" in capsys.readouterr().err


GATEWAY_HOST = "https://mainnet.stellar.validationcloud.io"
GATEWAY = GATEWAY_HOST + "/v1/S3CRET-KEY"  # a gateway is named with its key in the path
GATEWAY_PREFIX = "/v1/S3CRET-KEY"


def gateway_pool(fake: FakeHorizon, seen: list[str]) -> tuple[HorizonPool, Clock]:
    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return fake._handle(request)

    clock = Clock()
    horizon = HorizonPool(
        (GATEWAY,),
        pool_settings=PoolSettings(health_check_interval=60, failover_cooldown=10),
        settings=HorizonSettings(**SETTINGS),
        transports={server_name(GATEWAY): httpx.MockTransport(handle)},
        clock=clock,
        sleeper=clock.sleep,
    )
    return horizon, clock


def test_a_gateway_is_asked_under_the_path_of_its_address_and_its_key_is_not_its_name() -> None:
    fake = FakeHorizon(origin=GATEWAY_HOST, prefix=GATEWAY_PREFIX)
    seen: list[str] = []
    pool, _ = gateway_pool(fake, seen)
    try:
        assert pool.get(LEDGER)["sequence"] == 105
        page = pool.page(f"/accounts/{A}/operations", resource_type="operation", order="desc", cursor=str(after_ledger(113)), limit=2)
        assert pool.next_page(page) is not None or page.exhausted
    finally:
        pool.close()
    # The root and every request carry the path; the links of the pages name it too.
    assert seen[:2] == [GATEWAY_PREFIX + "/", GATEWAY_PREFIX + LEDGER]
    assert seen[2:] and all(path.startswith(GATEWAY_PREFIX + "/accounts/") for path in seen[2:])
    assert paths_of(fake)[:2] == ["/", LEDGER]  # and the server sees the request it always did
    # Its name is the address without the path, with a mark that tells it from another key of the same host.
    assert pool.name == server_name(GATEWAY)
    assert pool.name.startswith(GATEWAY_HOST + "~")
    assert "S3CRET" not in pool.name + "".join(pool.stats.by_server)
    assert list(pool.stats.by_server) == [server_name(GATEWAY)]


def test_the_key_is_in_no_message_about_a_link() -> None:
    pool, _ = gateway_pool(FakeHorizon(origin=GATEWAY_HOST, prefix=GATEWAY_PREFIX), [])
    try:
        pool._local.server = pool.nodes[0]  # the server that answered, as after a request
        with pytest.raises(UntrustedNextLink) as leaves:
            pool._next_query(GATEWAY_HOST + GATEWAY_PREFIX + "/elsewhere?cursor=1", f"/accounts/{A}/operations", {"limit": "2"})
        with pytest.raises(UntrustedNextLink) as elsewhere:
            pool._next_query("https://other.example.org/x?cursor=1", f"/accounts/{A}/operations", {"limit": "2"})
    finally:
        pool.close()
    assert "S3CRET" not in str(leaves.value) + str(elsewhere.value)
