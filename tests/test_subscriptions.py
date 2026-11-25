"""The subscriptions this installation keeps at Operations Notifier: what is
wanted, how it is registered and confirmed, and what is never touched."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.config import PUBLIC_NETWORK_PASSPHRASE, Settings
from app.notifier.client import NotifierClient, NotifierRejected
from app.storage.models import TrackedAccount, TrackedAsset, TrackedMarket, WatchSubscription
from app.subscriptions import desired, reconcile
from app.subscriptions.cli import main as subscriptions_main
from app.subscriptions.desired import ACCOUNT, ASSET
from app.watch import accounts as watched
from tests.fake_horizon import A, B, C, EUR, ISSUER, RECT, FakeHorizon, serve
from tests.fake_notifier import FakeNotifier
from tests.fake_notifier_api import FakeNotifierApi
from tests.test_backfill import WATCH_LIST

CALLBACK = "https://mtl-horizon.test/webhooks/notifier"


class Site:
    """A private database with the watch list, and a fake Notifier."""

    def __init__(self, url: str, **api_options: Any) -> None:
        self.engine = create_engine(url)
        self.signer = FakeNotifier()
        self.api_fake = FakeNotifierApi(self.signer.public_key, **api_options)
        self.settings = Settings(
            database_url=url,
            network_passphrase=PUBLIC_NETWORK_PASSPHRASE,
            notifier_signer_keys=(self.signer.public_key,),
            notifier_api_url="http://notifier.test",
            notifier_api_token="",
            notifier_callback_url=CALLBACK,
        )
        self.now = datetime(2026, 11, 25, 12, 0, tzinfo=UTC)
        self.horizon = FakeHorizon()

    def api(self) -> NotifierClient:
        return NotifierClient(self.settings.notifier_api_url, token=self.settings.notifier_api_token, transport=self.api_fake.transport)

    def rows(self) -> dict[tuple[str, str], WatchSubscription]:
        with Session(self.engine) as session:
            return {
                (row.kind, row.key): row
                for row in session.scalars(select(WatchSubscription).where(WatchSubscription.state != "retired"))
            }

    def close(self) -> None:
        self.engine.dispose()


@pytest.fixture
def site(make_database: Callable[[], str]) -> Any:
    from app.backfill import config as watch_lists

    instance = Site(make_database())
    with Session(instance.engine) as session, session.begin():
        watch_lists.apply(session, WATCH_LIST)
    yield instance
    instance.close()


def settle(site: Site, api: NotifierClient, passes: int = 4) -> reconcile.Report:
    """Passes until everything that can be done is done; what they did, added up."""
    total = reconcile.Report()
    for number in range(passes):
        site.now += timedelta(minutes=2)
        report = reconcile.run(site.engine, api, CALLBACK, now=site.now)
        total.wanted = report.wanted
        total.created += report.created
        total.taken += report.taken
        total.confirmed += report.confirmed
        total.failed += report.failed
        total.retired += report.retired
        total.ambiguous += report.ambiguous
        total.problems += report.problems
        total.unreachable = report.unreachable or total.unreachable
    return total


# -- what is wanted --------------------------------------------------------------------


def test_wanted_is_read_from_the_watch_lists(site: Site) -> None:
    with Session(site.engine) as session:
        wants = desired.wanted(session)
    assert set(wants) == {(ACCOUNT, A), (ACCOUNT, B), (ASSET, EUR), (ASSET, RECT)}
    assert wants[(ACCOUNT, A)].rules == ("account enabled",)
    assert "history of the asset" in wants[(ASSET, EUR)].rules and any(rule.startswith("market") for rule in wants[(ASSET, EUR)].rules)
    assert wants[(ASSET, RECT)].rules == ("history of the asset",)  # one asset of the market is enough: EUR was chosen


def test_a_market_never_subscribes_native_and_only_one_of_its_assets(site: Site) -> None:
    with Session(site.engine) as session, session.begin():
        session.get(TrackedAsset, RECT).enabled = False  # type: ignore[union-attr]
        session.get(TrackedMarket, tuple(sorted((EUR, RECT)))).enabled = False  # type: ignore[call-overload]
        from app.backfill.config import parse

        watch_lists = __import__("app.backfill.config", fromlist=["apply"])
        watch_lists.apply(
            session,
            parse({"markets": [{"base": "native", "counter": EUR}, {"base": "native", "counter": RECT}]}, "test"),
        )
        wants = desired.wanted(session)
    assert (ASSET, "native") not in wants  # nearly every operation of the network deals in it
    assert (ASSET, RECT) in wants and (ASSET, EUR) in wants  # one asset for each market


def test_discovery_and_history_are_rules_apart(site: Site) -> None:
    with Session(site.engine) as session, session.begin():
        session.get(TrackedAsset, RECT).enabled, session.get(TrackedAsset, RECT).discover = False, True  # type: ignore[union-attr]
        wants = desired.wanted(session)
    assert wants[(ASSET, RECT)].rules == ("discovery",)


# -- registering -----------------------------------------------------------------------------


def test_everything_wanted_is_registered_once_and_confirmed_by_reading_it_back(site: Site) -> None:
    with site.api() as api:
        first = reconcile.run(site.engine, api, CALLBACK, now=site.now)
        # Created, and on the word of a create nothing is active yet.
        states = {row.state for row in site.rows().values()}
        assert first.created == 4 and states == {"error"}
        assert all("not confirmed yet" in (row.last_error or "") for row in site.rows().values())
        assert all(row.remote_id for row in site.rows().values())
        report = settle(site, api, passes=2)
    assert {row.state for row in site.rows().values()} == {"active"} and len(site.api_fake.subscriptions) == 4
    assert all(row.remote_id and row.confirmed_at for row in site.rows().values())
    assert report.created == 0 and report.confirmed == 4  # read back from the list, not made again
    # Again: nothing more is made, and nothing is asked of the Notifier.
    before = len(site.api_fake.requests)
    with site.api() as api:
        again = settle(site, api, passes=1)
    assert again.created == 0 and len(site.api_fake.subscriptions) == 4 and reconcile.due(site.engine, site.now) == 0
    assert len(site.api_fake.requests) == before


def test_a_create_whose_answer_was_lost_is_not_made_twice(site: Site) -> None:
    site.api_fake.create_but_hang(1)  # the first create is done there, but the answer never comes
    with site.api() as api:
        first = reconcile.run(site.engine, api, CALLBACK, now=site.now)
        assert first.created == 3 and first.failed == 1
        lost = next(row for row in site.rows().values() if row.remote_id is None)
        assert lost.state == "error" and "no answer" in (lost.last_error or "")
        report = settle(site, api)
    assert report.created == 0 and report.taken == 1 and report.confirmed == 3  # the fourth was found there, not made again
    assert len(site.api_fake.subscriptions) == 4 and {row.state for row in site.rows().values()} == {"active"}


def test_a_notifier_that_is_down_costs_time_and_nothing_else(site: Site) -> None:
    site.api_fake.hang(100)
    with site.api() as api:
        report = reconcile.run(site.engine, api, CALLBACK, now=site.now)
        assert report.unreachable and report.created == 0
        assert {row.state for row in site.rows().values()} == {"error"}
        assert all("could not be reached" in (row.last_error or "") for row in site.rows().values())
        # It is not asked again at once.
        assert reconcile.due(site.engine, site.now + timedelta(seconds=10)) == 0
        # Back.
        site.api_fake._hangs = 0
        report = settle(site, api, passes=6)
    assert {row.state for row in site.rows().values()} == {"active"} and len(site.api_fake.subscriptions) == 4


def test_what_is_refused_is_kept_and_said(site: Site) -> None:
    limited = Site(site.engine.url.render_as_string(hide_password=False), max_active=3)
    try:
        with limited.api() as api:
            report = reconcile.run(limited.engine, api, CALLBACK, now=limited.now)
            settle(limited, api)
        rows = limited.rows()
        assert len(limited.api_fake.subscriptions) == 3 and sum(1 for row in rows.values() if row.state == "active") == 3
        refused = next(row for row in rows.values() if row.state == "error")
        assert "refused, 400" in (refused.last_error or "") and "Max active subscriptions exceeded" in (refused.last_error or "")
        assert any("refused, 400" in problem for problem in report.problems)
    finally:
        limited.close()


def test_two_that_fit_are_a_matter_for_a_person(site: Site) -> None:
    """The Notifier does not merge two subscriptions made for one asset (it
    tells subscriptions apart by the request as it was written, and an
    asset's type is written by the service): so there can be two that fit.
    Which is this installation's is not for the reconciliation to guess."""
    with site.api() as api:
        api.create(reconcile.subscription_params(ASSET, RECT, CALLBACK))
        api.create(reconcile.subscription_params(ASSET, RECT, CALLBACK))
        assert len(site.api_fake.subscriptions) == 2
        report = settle(site, api)
    row = site.rows()[(ASSET, RECT)]
    assert row.state == "ambiguous" and "2 subscriptions fit" in (row.last_error or "")
    # Nothing made for RECT (two are there already, and it is not guessed which is meant); the other three as wanted.
    assert report.ambiguous >= 1 and report.created == 3 and len(site.api_fake.subscriptions) == 5
    assert reconcile.due(site.engine, site.now + timedelta(days=1)) == 0  # and it is not tried again on its own


def test_subscriptions_of_others_are_never_touched(site: Site) -> None:
    with site.api() as api:
        theirs = api.create({"reaction_url": "https://someone-else.test/hook", "account": A})
        manual = api.create(reconcile.subscription_params(ACCOUNT, B, CALLBACK))  # made by hand for this installation
    with site.api() as api:
        settle(site, api)
        assert any(sub["id"] == theirs["id"] for sub in api.list_subscriptions())  # left as it was
        assert site.rows()[(ACCOUNT, B)].remote_id == manual["id"]  # and the one made by hand for it was taken, not remade
        # The owner switches B off: its subscription goes — the other's stays.
        with Session(site.engine) as session, session.begin():
            watched.set_enabled(session, B, enabled=False)
        settle(site, api)
    ids = {sub["id"] for sub in site.api_fake.subscriptions}
    assert theirs["id"] in ids and manual["id"] not in ids
    assert site.rows().get((ACCOUNT, B)) is None


def test_an_asset_stays_while_any_rule_wants_it(site: Site) -> None:
    with site.api() as api:
        settle(site, api)
        before = {key: row.remote_id for key, row in site.rows().items()}
        with Session(site.engine) as session, session.begin():
            session.get(TrackedMarket, tuple(sorted((EUR, RECT)))).enabled = False  # type: ignore[call-overload]
            session.get(TrackedAccount, A).enabled = False  # type: ignore[union-attr]
            session.get(TrackedAsset, RECT).enabled = False  # type: ignore[union-attr]
        settle(site, api)
        # EUR: history still; RECT: the market is off and so is its history — it goes.
        assert (ASSET, EUR) in site.rows() and (ASSET, RECT) not in site.rows()
        assert site.rows()[(ASSET, EUR)].remote_id == before[(ASSET, EUR)]
        assert (ACCOUNT, A) not in site.rows() and (ACCOUNT, B) in site.rows()  # switching A off did not switch B off
    assert len(site.api_fake.subscriptions) == 2


def test_nothing_is_registered_with_a_notifier_whose_webhooks_would_be_refused(site: Site) -> None:
    site.settings = Settings(**{**site.settings.__dict__, "notifier_signer_keys": (FakeNotifier().public_key,)})
    from app.watch import provision

    with site.api() as api:
        report = provision.run(site.engine, site.settings, api=api, now=site.now)
    assert site.api_fake.subscriptions == [] and report.subscriptions is not None
    assert "not among MTL_HORIZON_NOTIFIER_KEYS" in (report.subscriptions.unreachable or "")
    assert all("not among MTL_HORIZON_NOTIFIER_KEYS" in (row.last_error or "") for row in site.rows().values())


# -- by hand ---------------------------------------------------------------------------------


def test_a_subscription_made_by_hand_is_adopted_by_its_id_and_never_deleted_here(site: Site) -> None:
    with site.api() as api:
        made = api.create(reconcile.subscription_params(ACCOUNT, A, CALLBACK))
        foreign = api.create({"reaction_url": "https://elsewhere.test/hook", "account": A})
        with pytest.raises(NotifierRejected):
            reconcile.adopt(site.engine, ACCOUNT, A, foreign["id"], callback_url=CALLBACK, api=api)  # not what this installation registers
        manual_id = reconcile.adopt(site.engine, ACCOUNT, A, made["id"], callback_url=CALLBACK, api=api)
        settle(site, api)
        # Forgotten by hand: it stays at the Notifier, and is no longer this installation's.
        assert "stays at the Notifier" in reconcile.release(site.engine, manual_id)
        assert site.rows().get((ACCOUNT, A)) is None and any(sub["id"] == made["id"] for sub in api.list_subscriptions())
        manual_id = reconcile.adopt(site.engine, ACCOUNT, A, made["id"], callback_url=CALLBACK, api=api)
    rows = site.rows()
    assert rows[(ACCOUNT, A)].state == "manual" and rows[(ACCOUNT, A)].managed is False
    assert len(site.api_fake.subscriptions) == 5  # made for it, the other's, and the three others as wanted
    with Session(site.engine) as session, session.begin():
        watched.set_enabled(session, A, enabled=False)
    with site.api() as api:
        reconcile.run(site.engine, api, CALLBACK, now=site.now)
    assert any(sub["id"] == made["id"] for sub in site.api_fake.subscriptions)  # made by hand: left there
    assert site.rows().get((ACCOUNT, A)) is None  # and forgotten here, on its own


# -- without the API ---------------------------------------------------------------------------


def test_without_an_api_address_everything_is_only_wished_for(site: Site) -> None:
    from app.watch import provision

    site.settings = Settings(**{**site.settings.__dict__, "notifier_api_url": ""})
    report = provision.run(site.engine, site.settings, now=site.now)
    assert report.subscriptions is None and {row.state for row in site.rows().values()} == {"wanted"}
    assert provision.make_api(site.settings) is None


# -- the commands ------------------------------------------------------------------------------


def test_commands(site: Site, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", site.engine.url.render_as_string(hide_password=False))
    monkeypatch.setenv("MTL_HORIZON_NOTIFIER_API_URL", "http://notifier.test")
    monkeypatch.setenv("MTL_HORIZON_NOTIFIER_CALLBACK_URL", CALLBACK)
    monkeypatch.setenv("MTL_HORIZON_NOTIFIER_KEYS", site.signer.public_key)
    monkeypatch.setattr("app.watch.provision.make_api", lambda settings, **options: site.api())
    assert subscriptions_main(["status"]) == 0
    out = capsys.readouterr().out
    assert "wanted: 4 subscription(s)" in out and "kept: wanted 0" in out  # wished for; no pass has run
    assert subscriptions_main(["reconcile", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "a pass would" in out and "create" in out and "dry run: nothing was registered" in out
    assert site.api_fake.subscriptions == []
    assert subscriptions_main(["reconcile"]) == 0
    assert subscriptions_main(["reconcile"]) == 0
    assert subscriptions_main(["status"]) == 0
    assert "active 4" in capsys.readouterr().out
    assert subscriptions_main(["adopt", "--kind", "asset", "--key", RECT, "--id", "no-such-id", "--no-verify"]) == 0
