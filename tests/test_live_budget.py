"""The cap on what may be asked of the real servers, for a check its owner
allowed: counted before an attempt is made, by the transport itself; shared
by every process and kept across a restart; nothing goes around it."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx
import pytest
from sqlalchemy import create_engine, text

from app import cli_common
from app.config import ConfigError, PUBLIC_NETWORK_PASSPHRASE, Settings
from app.discovery import scan
from app.notifier.client import NotifierClient, NotifierUnavailable
from app.pool import check as checking
from app.sources import live
from app.sources.horizon import HorizonSettings, RequestFailed, Unavailable
from app.sources.pool import HorizonPool, PoolSettings
from tests.factories import address
from tests.test_backfill import WATCH_LIST
from tests.fake_horizon import EUR, FakeHorizon
from tests.fake_notifier import FakeNotifier
from tests.fake_notifier_api import FakeNotifierApi
from tests.test_pool import SETTINGS


@pytest.fixture(autouse=True)
def no_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(live, "BUDGET", None)
    monkeypatch.setattr(live, "INNER", None)
    monkeypatch.setattr(live, "MIN_INTERVAL", 0.0)  # the second between two attempts is its own test


def capped(fake: FakeHorizon, limit: int) -> tuple[HorizonPool, FakeHorizon]:
    """A pool with no transport of its own — as a process has none — behind
    the cap, with ``fake`` as what really answers."""
    live.BUDGET, live.INNER = live.LiveBudget(limit), httpx.MockTransport(fake._handle)
    pool = HorizonPool(
        (fake.origin,), HorizonSettings(**SETTINGS), network_passphrase=PUBLIC_NETWORK_PASSPHRASE, pool_settings=PoolSettings(health_check_interval=60, failover_cooldown=1)
    )
    return pool, fake


def test_exactly_as_many_attempts_reach_the_server_as_the_cap_allows_and_not_one_more(make_database: Callable[[], str]) -> None:
    fake = FakeHorizon()
    pool, _ = capped(fake, 3)
    try:
        assert pool.history().latest == fake.latest  # the root probe: the first attempt
        assert pool.get("/ledgers/105")["sequence"] == 105
        assert pool.get("/ledgers/106")["sequence"] == 106
        assert len(fake.requests) == 3 and live.BUDGET is not None and live.BUDGET.used == 3
        # The fourth is refused before a packet leaves; and what the pool does
        # then — ask for the root again, try the next server — is refused too.
        # It is said as what it is — the budget — and not as a server that is down.
        with pytest.raises(Unavailable, match="live budget is used up"):
            pool.get("/ledgers/107")
        assert len(fake.requests) == 3, "the refusal reached the server as nothing"
        with pytest.raises(Unavailable, match="live budget is used up"):
            pool.history()
        assert len(fake.requests) == 3
    finally:
        pool.close()


def test_the_diagnosis_of_the_pool_stops_at_the_cap() -> None:
    fake = FakeHorizon()
    pool, _ = capped(fake, 2)
    try:
        with pytest.raises(Unavailable, match="live budget is used up"):
            checking.check_pool(pool, repeats=3)
        assert len(fake.requests) == 2, "the root and one more: the rest of the diagnosis was refused before it left"
    finally:
        pool.close()


def test_a_discovery_pass_stops_at_the_cap_wherever_it_is(make_database: Callable[[], str]) -> None:
    """The pass's own budget counts pages at the client; the cap counts
    attempts at the transport, and it is the cap that holds when it is the
    smaller: the pass ends where it stands and says why."""
    from sqlalchemy.orm import Session

    from app.backfill import config as watch_lists
    from app.storage.models import TrackedAsset

    engine = create_engine(make_database())
    fake = FakeHorizon()
    fake.trustlines[EUR] = {address(500 + number): 10**7 for number in range(1000)}  # five pages
    with Session(engine) as session, session.begin():
        watch_lists.apply(session, WATCH_LIST)
        session.get(TrackedAsset, EUR).discover = True  # type: ignore[union-attr]
    pool, _ = capped(fake, 2)
    try:
        report = scan.scan(pool, engine, EUR, max_requests=1000)
        assert [path for path, _ in fake.requests] == ["/", "/accounts"], "the root and the first page"
        assert report.state == "incomplete" and "live budget is used up" in (report.stopped_because or ""), report.stopped_because
        assert report.pages == 1
    finally:
        pool.close()
        engine.dispose()


def test_the_count_is_the_installation_s_and_survives_a_restart(make_database: Callable[[], str]) -> None:
    engine = create_engine(make_database())
    try:
        first, second = live.LiveBudget(4, engine), live.LiveBudget(4, engine)  # two processes
        for number in range(3):
            first.take()
        second.take()
        for budget in (first, second, live.LiveBudget(4, engine)):  # the last: after a restart
            with pytest.raises(live.Exhausted):
                budget.take()
        with engine.connect() as connection:
            kept = connection.execute(text("SELECT value FROM service_state WHERE name = 'live_budget'")).scalar()
        assert kept["used"] == 4 and kept["limit"] == 4
        # A new run clears the row by hand: the cap is whole again.
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM service_state WHERE name = 'live_budget'"))
        live.LiveBudget(4, engine).take()
        assert live.LiveBudget(4, engine).used == 1
    finally:
        engine.dispose()


def test_no_two_attempts_leave_within_a_second(monkeypatch: pytest.MonkeyPatch) -> None:
    waits: list[float] = []
    monkeypatch.setattr(live, "MIN_INTERVAL", 1.0)
    monkeypatch.setattr(live.time, "sleep", lambda seconds: waits.append(seconds))
    budget = live.LiveBudget(5)
    budget.take()
    budget.take()
    assert waits and 0 < waits[0] <= 1.0


def test_the_notifier_s_api_is_capped_by_the_same_count() -> None:
    signer, fake = FakeNotifier(), None
    api_fake = FakeNotifierApi(signer.public_key)
    live.BUDGET, live.INNER = live.LiveBudget(1), api_fake.transport
    with NotifierClient("http://notifier.test") as api:
        assert api.status()["publicKey"] == signer.public_key
        with pytest.raises(NotifierUnavailable):
            api.list_subscriptions()
    assert len(api_fake.requests) == 1


def test_the_setting_puts_the_cap_on_and_nothing_else_does(monkeypatch: pytest.MonkeyPatch) -> None:
    base = {"MTL_HORIZON_DATABASE_URL": "postgresql://x"}
    assert Settings.from_env(base).live_budget == 0 and live.transport() is None
    assert Settings.from_env({**base, "MTL_HORIZON_LIVE_BUDGET": " 20 "}).live_budget == 20
    with pytest.raises(ConfigError, match="LIVE_BUDGET"):
        Settings.from_env({**base, "MTL_HORIZON_LIVE_BUDGET": "twenty"})
    # Every client a process makes puts the cap on; said again, it is the same cap.
    settings = Settings.from_env({**base, "MTL_HORIZON_LIVE_BUDGET": "20"})
    live.configure(settings.live_budget, None)
    budget = live.BUDGET
    assert budget is not None and budget.limit == 20
    budget.take()
    live.configure(settings.live_budget, None)
    assert live.BUDGET is budget and budget.used == 1
    assert isinstance(live.transport(), live.Capped)
    live.configure(0)
    assert live.BUDGET is None and live.transport() is None
