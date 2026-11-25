"""Discovery: the holders of an asset read from Horizon and added to the
watch list, and trustlines opened on it seen in the stored history."""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import yaml
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import Session

from app.backfill import config as watch_lists
from app.discovery import cli as discovery_cli
from app.discovery import events, scan
from app.storage.models import DiscoveryRun, TrackedAccount, TrackedAsset
from app.watch import accounts as watched
from tests.factories import address
from tests.fake_horizon import A, D, EUR, ISSUER, RECT, FakeHorizon, Transaction, serve, trust
from tests.test_pool import refused
from tests.test_backfill import WATCH_LIST
from tests.test_worker import Shop

OTHER_ISSUER = address(77)
SAME_CODE_OTHER_ISSUER = f"EURMTL:{OTHER_ISSUER}"


@pytest.fixture
def shop(make_database: Callable[[], str]) -> Any:
    instance = Shop(make_database(), FakeHorizon())
    yield instance
    instance.close()


def mark(shop: Shop, asset: str, discover: bool = True) -> None:
    with Session(shop.engine) as session, session.begin():
        tracked = session.get(TrackedAsset, asset)
        if tracked is None:
            shop.asset(asset)
        session.get(TrackedAsset, asset).discover = discover  # type: ignore[union-attr]


def holders(count: int, *, start: int = 500) -> dict[str, int]:
    return {address(number): (number % 3) * 10**7 for number in range(start, start + count)}


def tracked(shop: Shop, account: str) -> TrackedAccount | None:
    with Session(shop.engine) as session:
        row = session.get(TrackedAccount, account)
        return None if row is None else TrackedAccount(**{c: getattr(row, c) for c in ("account_id", "enabled", "origin", "discovered_by", "boundary_due", "reason")})


# -- a pass over the network ------------------------------------------------------


@pytest.mark.parametrize("count", [0, 200, 201])
def test_the_holders_of_an_asset_are_read_and_added(shop: Shop, count: int) -> None:
    found = holders(count)
    shop.fake.trustlines[EUR] = found
    mark(shop, EUR)
    report = scan.scan(shop.pool, shop.engine, EUR)
    assert report.state == scan.COMPLETE and report.found == count and report.added == sorted(found)
    # A full page is followed by an empty one before the pass knows it is over.
    pages = 1 if count == 0 else (count + scan.PAGE - 1) // scan.PAGE
    assert report.pages == pages
    with Session(shop.engine) as session:
        added = {row.account_id: row for row in session.scalars(select(TrackedAccount).where(TrackedAccount.origin == "discovery"))}
        run = session.get(DiscoveryRun, report.run_id)
    assert sorted(added) == sorted(found)
    first = added[sorted(found)[0]] if found else None
    if first is not None:
        assert first.enabled and first.boundary_due and first.discovered_by == EUR
        assert first.reason.startswith("discovered: holds EURMTL")
    assert run is not None and run.state == scan.COMPLETE and run.servers[0]["server"] == shop.fake.origin
    assert report.requests == 1 + report.pages + (1 if count and count % scan.PAGE == 0 else 0)  # root, pages, the look after a full page


def test_a_thousand_holders(shop: Shop) -> None:
    found = holders(1000)
    shop.fake.trustlines[EUR] = found
    mark(shop, EUR)
    started = time.monotonic()
    report = scan.scan(shop.pool, shop.engine, EUR)
    print(f"\n1000 holders: {report.requests} requests, {report.pages} pages, {time.monotonic() - started:.2f}s")
    assert report.state == scan.COMPLETE and len(report.added) == 1000
    with Session(shop.engine) as session:
        assert session.scalar(select(func.count()).select_from(TrackedAccount)) == 1000 + len(WATCH_LIST.accounts)


def test_an_account_that_holds_nothing_still_counts(shop: Shop) -> None:
    shop.fake.trustlines[EUR] = {address(500): 0, address(501): 10**7}
    mark(shop, EUR)
    report = scan.scan(shop.pool, shop.engine, EUR)
    assert sorted(report.added) == [address(500), address(501)] and not report.without_line


def test_the_same_code_of_another_issuer_is_another_asset(shop: Shop) -> None:
    shop.fake.trustlines[SAME_CODE_OTHER_ISSUER] = {address(600): 10**7}
    shop.fake.trustlines[EUR] = {address(601): 10**7, address(602): 0}
    mark(shop, EUR)
    report = scan.scan(shop.pool, shop.engine, EUR)
    assert sorted(report.added) == [address(601), address(602)]
    assert address(600) not in report.added  # it holds the other EURMTL


def test_the_issuer_is_not_added(shop: Shop) -> None:
    shop.fake.trustlines[EUR] = {ISSUER: 0, A: 10**7}
    mark(shop, EUR)
    report = scan.scan(shop.pool, shop.engine, EUR)
    assert report.issuers == 1 and report.added == [] and report.already == [A]  # A is watched already


def test_a_second_pass_adds_nothing_and_changes_no_decision(shop: Shop) -> None:
    found = holders(5)
    shop.fake.trustlines[EUR] = found
    mark(shop, EUR)
    first = scan.scan(shop.pool, shop.engine, EUR)
    # The owner switches one of them off by hand.
    chosen = sorted(found)[1]
    with Session(shop.engine) as session, session.begin():
        watched.set_enabled(session, chosen, enabled=False, reason="switched off by hand")
    again = scan.scan(shop.pool, shop.engine, EUR)
    assert len(first.added) == 5 and again.added == [] and again.disabled == [chosen] and len(again.already) == 4
    row = tracked(shop, chosen)
    assert row is not None and not row.enabled and row.reason == "switched off by hand"


def test_an_account_of_two_discovery_assets_is_added_once(shop: Shop) -> None:
    shop.fake.trustlines[EUR] = {address(700): 10**7, address(701): 10**7}
    shop.fake.trustlines[RECT] = {address(700): 10**7, address(702): 10**7}
    mark(shop, EUR)
    mark(shop, RECT)
    reports = scan.run_all(shop.pool, shop.engine)
    assert len(reports) == 2
    row = tracked(shop, address(700))
    assert row is not None and row.discovered_by == f"{EUR},{RECT}"
    with Session(shop.engine) as session:
        assert session.scalar(select(func.count()).select_from(TrackedAccount)) == len(WATCH_LIST.accounts) + 3


def test_a_dry_run_writes_nothing(shop: Shop) -> None:
    shop.fake.trustlines[EUR] = holders(3)
    mark(shop, EUR)
    report = scan.scan(shop.pool, shop.engine, EUR, dry_run=True)
    assert report.state == scan.DRY_RUN and len(report.added) == 3 and report.run_id is None
    with Session(shop.engine) as session:
        assert session.scalar(select(func.count()).select_from(DiscoveryRun)) == 0
        assert session.scalar(select(func.count()).select_from(TrackedAccount)) == len(WATCH_LIST.accounts)


def test_a_pass_that_runs_out_of_requests_says_so_and_is_resumed(shop: Shop) -> None:
    shop.fake.trustlines[EUR] = holders(450)
    mark(shop, EUR)
    stopped = scan.scan(shop.pool, shop.engine, EUR, max_requests=2)  # the root and one page
    assert stopped.state == scan.INCOMPLETE and len(stopped.added) == 200 and "2 requests" in (stopped.stopped_because or "")
    with Session(shop.engine) as session:
        run = session.get(DiscoveryRun, stopped.run_id)
    assert run is not None and run.state == scan.INCOMPLETE and run.last_address == sorted(holders(450))[199]
    assert "did not reach the end" in stopped.lines()[-1]

    resumed = scan.scan(shop.pool, shop.engine, EUR, after_address=scan.stopped_at(shop.engine, EUR))
    assert resumed.state == scan.COMPLETE and len(resumed.added) == 250 and not resumed.already
    with Session(shop.engine) as session:
        assert session.scalar(select(func.count()).select_from(TrackedAccount)) == len(WATCH_LIST.accounts) + 450


def test_the_pass_is_honest_about_a_server_changed_on_the_way(make_database: Callable[[], str]) -> None:
    first, second = FakeHorizon(), FakeHorizon(origin="https://b.horizon.test")
    for fake in (first, second):
        fake.trustlines[EUR] = holders(450)
    shop = Shop(make_database(), first, second)
    try:
        mark(shop, EUR)
        asked = 0
        real = first.before_answer

        def die(path: str) -> None:
            nonlocal asked
            if path == "/accounts":
                asked += 1
                if asked == 1:
                    first.down = httpx.ConnectError("refused")

        first.before_answer = die
        report = scan.scan(shop.pool, shop.engine, EUR)
        assert report.state == scan.COMPLETE and len(report.added) == 450
        assert [entry["server"] for entry in report.servers] == [first.origin, second.origin, second.origin]
    finally:
        shop.close()


def test_native_cannot_be_discovered(shop: Shop) -> None:
    with pytest.raises(ValueError, match="every account"):
        scan.scan(shop.pool, shop.engine, "native")


# -- the flag in the watch list file, and the commands ------------------------------


def test_discover_is_a_flag_of_the_asset_of_its_own(shop: Shop) -> None:
    config = watch_lists.parse(
        yaml.safe_load(
            f"""
assets:
  - code: EURMTL
    issuer: {ISSUER}
    discover: true
  - code: RECTMTL
    issuer: {ISSUER}
    discover: false
"""
        ),
        "test",
    )
    with Session(shop.engine) as session, session.begin():
        watch_lists.apply(session, config, apply_changes=True)
        assets = {row.asset_id: (row.discover, row.enabled) for row in session.scalars(select(TrackedAsset))}
    assert assets[EUR] == (True, True) and assets[RECT] == (False, True)  # discovery does not touch the rule of history
    with pytest.raises(watch_lists.ConfigError, match="native cannot be discovered"):
        watch_lists.parse({"assets": [{"native": True, "discover": True}]}, "test")
    with pytest.raises(watch_lists.ConfigError, match="discover must be true or false"):
        watch_lists.parse({"assets": [{"code": "EURMTL", "issuer": ISSUER, "discover": "yes"}]}, "test")


def test_the_flag_changes_only_when_asked_for(shop: Shop) -> None:
    config = watch_lists.parse({"assets": [{"code": "EURMTL", "issuer": ISSUER, "discover": True}]}, "test")
    with Session(shop.engine) as session, session.begin():
        result = watch_lists.apply(session, config)
        left = session.get(TrackedAsset, EUR).discover
    assert left is False and any("discover=true" in line for line in result.skipped)
    with Session(shop.engine) as session, session.begin():
        watch_lists.apply(session, config, apply_changes=True)
        marked = session.get(TrackedAsset, EUR).discover
    assert marked is True


def test_discovery_as_commands(shop: Shop, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", shop.engine.url.render_as_string(hide_password=False))
    serve(monkeypatch, shop.fake)
    shop.fake.trustlines[EUR] = holders(3)
    assert discovery_cli.main(["on", "MTLRECT:" + ISSUER]) == discovery_cli.OK  # RECT is watched already
    assert discovery_cli.main(["on", "NEW:1:" + ISSUER]) == discovery_cli.FAILED
    assert discovery_cli.main(["on", "native"]) == discovery_cli.FAILED
    assert discovery_cli.main(["run", "--dry-run"]) == discovery_cli.OK
    out = capsys.readouterr().out
    assert "dry run over MTLRECT:" in out and "0 new" in out  # nothing holds RECT
    assert discovery_cli.main(["run"]) == discovery_cli.OK
    assert "complete" in capsys.readouterr().out
    assert discovery_cli.main(["status"]) == discovery_cli.OK
    out = capsys.readouterr().out
    assert "MTLRECT:" in out and "complete" in out and "discovery" in out
    assert discovery_cli.main(["off", "MTLRECT:" + ISSUER]) == discovery_cli.OK
    with Session(shop.engine) as session:
        assert session.get(TrackedAsset, RECT).discover is False


# -- trustlines opened on the asset, seen in the stored history -----------------------


def test_a_trustline_opened_on_the_asset_adds_the_account(shop: Shop) -> None:
    mark(shop, EUR)
    shop.fake.close(Transaction(121, 0, D, [trust(D, EUR)], {D}, {1: {D}}), latest=122)
    from tests.fake_notifier import notification

    from app.notifier import format as notifier_format
    from app.storage import events as journal

    operation = str((121 << 32) | 1)
    # The notification came for an asset subscription this installation manages.
    shop.receive(shop.notice(operation, subscription="asset-sub"))
    assert shop.worker.once() is True and shop.job().state == "done"
    changes = events.find_and_add(shop.engine, [EUR])
    assert [change.outcome for change in changes] == [watched.ADDED] and changes[0].account_id == D
    row = tracked(shop, D)
    assert row is not None and row.origin == "discovery" and row.discovered_by == EUR and row.boundary_due
    assert "opened a trustline" in row.reason
    # Looked at again: nothing happens twice.
    assert events.find_and_add(shop.engine, [EUR]) == []


def test_only_a_created_trustline_counts(shop: Shop) -> None:
    mark(shop, EUR)
    shop.fake.close(
        Transaction(121, 0, D, [trust(D, EUR, kind="removed")], {D}, {1: {D}}),  # a line closed
        Transaction(122, 0, D, [trust(D, EUR, kind="updated", limit=5000)], {D}, {1: {D}}),  # a limit changed
        Transaction(123, 0, D, [trust(D, EUR)], {D}, {1: {D}}, successful=False),  # failed: nothing changed
        Transaction(124, 0, D, [trust(D, RECT)], {D}, {1: {D}}),  # another asset, not marked
        latest=125,
    )
    for ledger in (121, 122, 123, 124):
        shop.receive(shop.notice(str((ledger << 32) | 1), subscription="asset-sub"))
        shop.worker.once()
    assert events.openings(shop.engine, [EUR], since=datetime(2020, 1, 1, tzinfo=UTC)) == []
    assert events.find_and_add(shop.engine, [EUR]) == []
    assert tracked(shop, D) is None


def test_nothing_is_decided_until_the_transaction_is_stored_whole(shop: Shop) -> None:
    from app.notifier import format as notifier_format  # noqa: F401

    mark(shop, EUR)
    digest = None
    shop.fake.close(Transaction(121, 0, D, [trust(D, EUR)], {D}, {1: {D}}), latest=122)
    for tx_hash, record in shop.fake.ledger.transactions.items():
        if record["ledger"] == 121:
            digest = tx_hash
    shop.fake.fail(f"{digest}/effects", 503, times=4)
    shop.receive(shop.notice(str((121 << 32) | 1), subscription="asset-sub"))
    shop.worker.once()
    assert shop.job().state == "pending"
    assert events.find_and_add(shop.engine, [EUR], now=shop.wall()) == []  # the effects are not there: undecided
    shop.fake.faults.clear()
    shop.wall.wait(6)
    shop.worker.once()
    assert shop.job().state == "done"
    # The look goes by the same clock as the jobs: the transaction finished after the last look began.
    assert [change.outcome for change in events.find_and_add(shop.engine, [EUR], now=shop.wall())] == [watched.ADDED]


def test_a_transaction_whose_job_finished_hours_late_is_found_by_the_next_look(shop: Shop) -> None:
    """The servers did not answer for two hours while the looks went on every
    minute, each moving its place forward. A look reads what finished since
    the look before it — by the time of the finishing, not by the ledger — so
    the transaction is found by the first look after its job is done, however
    late and whatever its ledger."""
    mark(shop, EUR)
    shop.fake.close(Transaction(121, 0, D, [trust(D, EUR)], {D}, {1: {D}}), latest=122)
    digest = next(hash_ for hash_, record in shop.fake.ledger.transactions.items() if record["ledger"] == 121)
    shop.fake.down = refused()
    shop.receive(shop.notice(str((121 << 32) | 1), subscription="asset-sub"))
    shop.worker.once()
    assert shop.job().state == "pending" and shop.job().attempts == 0  # the servers' state, not the job's failure
    for minute in range(120):
        shop.wall.wait(60)
        assert events.find_and_add(shop.engine, [EUR], now=shop.wall()) == []
    # Two hours after the first look, and long after its ledger: the servers are back.
    shop.fake.down = None
    shop.clock.sleep(4000)  # its cooldown doubled all the while it was down; long past it now
    shop.wall.wait(4000)
    shop.worker.once()
    assert shop.job().state == "done"
    found_now = events.find_and_add(shop.engine, [EUR], now=shop.wall())
    assert [change.outcome for change in found_now] == [watched.ADDED]
    found = tracked(shop, D)
    assert found is not None and found.origin == "discovery"
    again = events.find_and_add(shop.engine, [EUR], now=shop.wall())
    assert [change.outcome for change in again] == [watched.ENABLED], "read again within the margin, and nothing added twice"
    shop.wall.wait(360)
    events.find_and_add(shop.engine, [EUR], now=shop.wall())  # the place moves past the margin…
    shop.wall.wait(1)
    assert events.find_and_add(shop.engine, [EUR], now=shop.wall()) == []  # …and then it is not read any more


def test_a_manual_addition_takes_the_same_road(shop: Shop) -> None:
    with Session(shop.engine) as session, session.begin():
        first = watched.add_account(session, address(900), reason="added by hand", origin=watched.MANUAL)
        again = watched.add_account(session, address(900), reason="added by hand", origin=watched.MANUAL)
    assert first.outcome == watched.ADDED and again.outcome == watched.ENABLED
    row = tracked(shop, address(900))
    assert row is not None and row.origin == "manual" and row.boundary_due
    with pytest.raises(ValueError, match="legacy"):
        with Session(shop.engine) as session, session.begin():
            watched.add_account(session, address(901), reason="x", origin=watched.LEGACY)


def test_accounts_that_were_here_before_are_legacy_and_get_no_import_on_their_own(unmigrated_database_url: str) -> None:
    """A database as it was at the end of stage 04, with its watch list: after
    the migration every account it already had is legacy — not switched on
    to a history import or a subscription of its own."""
    import os

    from alembic import command

    from tests.conftest import alembic_config

    url = unmigrated_database_url
    os.environ["MTL_HORIZON_DATABASE_URL"] = url
    config = alembic_config(url)
    command.upgrade(config, "0007")
    engine = create_engine(url)
    try:
        # The watch list as stage 04 wrote it, with the columns stage 04 had.
        with engine.begin() as connection:
            for rule in WATCH_LIST.accounts:
                connection.execute(
                    text("INSERT INTO history_accounts (account_id) VALUES (:account) ON CONFLICT DO NOTHING"),
                    {"account": rule.address},
                )
                connection.execute(
                    text(
                        "INSERT INTO tracked_accounts (account_id, enabled, reason) VALUES (:account, :enabled, :reason)"
                    ),
                    {"account": rule.address, "enabled": rule.enabled is not False, "reason": rule.reason},
                )
        command.upgrade(config, "head")
        with Session(engine) as session:
            rows = {
                row.account_id: (row.origin, row.boundary_due, row.watch_since_ledger)
                for row in session.execute(
                    select(TrackedAccount.account_id, TrackedAccount.origin, TrackedAccount.boundary_due, TrackedAccount.watch_since_ledger)
                )
            }
        assert len(rows) == len(WATCH_LIST.accounts)
        assert {values[0] for values in rows.values()} == {"legacy"}
        assert not any(values[1] for values in rows.values()) and not any(values[2] for values in rows.values())
        # And what is added after is not legacy.
        with Session(engine) as session, session.begin():
            watched.add_account(session, address(900), reason="added after", origin=watched.MANUAL)
            assert session.get(TrackedAccount, address(900)).origin == "manual"  # type: ignore[union-attr]
    finally:
        engine.dispose()


def _all(shop: Shop) -> list[TrackedAccount]:
    with Session(shop.engine) as session:
        return list(session.scalars(select(TrackedAccount)))
