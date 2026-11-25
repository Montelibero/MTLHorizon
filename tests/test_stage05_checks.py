"""Four questions the stage 04 report left, asked of the code.

Each test first shows what the code does; where that was a defect, the fix
is in the code and the test keeps it from coming back.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx
import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from app.cli_common import source_name, source_name_for
from app.config import Settings
from app.sources.horizon import HorizonSettings
from app.sources.pool import HorizonPool
from app.storage import jobs
from app.storage.models import NotifierEvent

from tests.conftest import _is_inside
from tests.fake_horizon import A, B, EUR, RECT, FakeHorizon
from tests.test_pool import SETTINGS
from tests.test_worker import Shop, T1, T3, transaction_hash

shop = Shop


@pytest.fixture
def shop(make_database: Callable[[], str]) -> Any:
    instance = Shop(make_database(), FakeHorizon())
    yield instance
    instance.close()

FIRST, SECOND = "https://a.horizon.test", "https://b.horizon.test"


# -- (b) a temporary 404 of a fresh transaction -----------------------------------


def test_a_fresh_transaction_that_is_404_for_a_moment_is_not_declared_dead(shop: Shop) -> None:
    """The notification came seconds after the ledger closed; a server that
    has the ledger may still answer 404 for a moment. That is not the
    transaction's absence."""
    digest = transaction_hash(shop.fake, T1)
    shop.fake.hidden_transactions.add(digest)
    shop.deliver(T1)
    shop.worker.once()
    job = shop.job()
    assert job.state == jobs.PENDING, "a 404 on the first try must not end the job"
    assert "404" in (job.last_error or "")
    assert shop.job().attempts == 1

    shop.fake.hidden_transactions.clear()
    shop.wall.wait(6)
    assert shop.worker.once() is True
    assert shop.job().state == jobs.DONE and shop.count("history_transactions") == 1


def test_a_transaction_nobody_has_stops_being_asked_for_after_the_tries(shop: Shop) -> None:
    """Not on any server, every time: after the tries it is dead, with the
    reason, and nothing is stored."""
    shop.fake.hidden_transactions.add(transaction_hash(shop.fake, T1))
    shop.deliver(T1)
    for _ in range(shop.worker.max_attempts):
        shop.worker.once()
        shop.wall.wait(11)
    assert shop.job().state == jobs.DEAD
    assert "on none of the servers" in (shop.job().last_error or "")
    assert shop.count("history_transactions") == 0


# -- (d) a long unavailability of Horizon, and retention --------------------------


def test_while_no_server_answers_the_job_waits_and_is_not_spent(make_database: Callable[[], str]) -> None:
    """Horizon is down for hours. The job is not to use up its tries on that:
    it waits, and when a server is back it is done without anyone's help."""
    shop = Shop(make_database(), FakeHorizon(down=httpx.ConnectError("refused")))
    try:
        shop.deliver(T1)
        for _ in range(shop.worker.max_attempts + 2):
            shop.worker.once()
            shop.wall.wait(3600)
        job = shop.job()
        assert job.state == jobs.PENDING, "an outage of the servers is not a failure of the job"
        assert job.attempts == 0  # none of it was the job's doing
        shop.fake.down = None
        shop.wall.wait(3600)
        shop.clock.sleep(6)  # the server's cooldown is kept by the pool's own clock
        assert shop.worker.once() is True
        assert shop.job().state == jobs.DONE
    finally:
        shop.close()


def test_what_the_notifier_sent_stays_when_the_transaction_is_gone_from_horizon(shop: Shop) -> None:
    """Below retention, or never there: the history cannot be brought in,
    and what was sent is kept as it came, XDR included."""
    shop.deliver(T3)
    digest = transaction_hash(shop.fake, T3)
    shop.fake.hidden_transactions.add(digest)
    shop.fake.fail(f"{digest}", 410, times=20)  # for its operations and effects too
    for _ in range(shop.worker.max_attempts):
        shop.worker.once()
        shop.wall.wait(11)
    assert shop.job().state == jobs.DEAD
    with Session(shop.engine) as session:
        event = session.scalar(select(NotifierEvent))
        payload = event.payload  # type: ignore[union-attr]
    assert payload["transaction"]["hash"] == digest
    assert payload["transaction"]["envelope_xdr"] and payload["transaction"]["result_xdr"]
    assert payload["operation"]["type"] == "path_payment_strict_send"


# -- (c) the identity of the logical source ---------------------------------------


def pool_of(*fakes: FakeHorizon, **options: Any) -> HorizonPool:
    return HorizonPool(
        tuple(fake.origin for fake in fakes),
        HorizonSettings(**SETTINGS),
        transports={fake.origin: httpx.MockTransport(fake._handle) for fake in fakes},
        sleeper=lambda seconds: None,
        **options,
    )


def test_the_source_does_not_depend_on_which_server_comes_first_in_the_list() -> None:
    one = pool_of(FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND))
    other = pool_of(FakeHorizon(origin=SECOND), FakeHorizon(origin=FIRST))
    alone = pool_of(FakeHorizon(origin=FIRST))
    try:
        assert source_name(one) == source_name(other) == source_name(alone) == "horizon:pool"
    finally:
        one.close()
        other.close()
        alone.close()


def test_checkpoints_survive_a_changed_order_of_servers(make_database: Callable[[], str]) -> None:
    """A backfill stopped half-way, then the list of servers is reordered and
    one is added: the next run goes on from the checkpoint."""
    from app.backfill.jobs import stream_jobs
    from app.backfill.loader import Backfill, Limits, Window
    from app.storage.upsert import checkpoint

    url = make_database()
    engine = create_engine(url)
    with Session(engine) as session, session.begin():
        from tests.test_backfill import WATCH_LIST, watch_lists

        watch_lists.apply(session, WATCH_LIST)
        jobs_ = stream_jobs(session)[0]
    with pool_of(FakeHorizon(origin=FIRST)) as pool:
        requests = pool.stats.requests
        with Backfill(pool, engine, source=source_name(pool), page_size=2, limits=Limits(requests=lambda: requests, max_requests=12), context="parents") as backfill:
            backfill.run(Window(100, 130), jobs_)
    with Session(engine) as session:
        stored = checkpoint(session, source=source_name_for(Settings(database_url="")), resource_type="transaction", stream_key=A, direction="desc")
        moved = session.execute(text("SELECT count(*) FROM sync_state WHERE cursor IS NOT NULL")).scalar()
    assert moved > 0 and stored is not None and stored[0]

    with pool_of(FakeHorizon(origin=SECOND), FakeHorizon(origin=FIRST), FakeHorizon(origin="https://c.horizon.test")) as pool:
        with Backfill(pool, engine, source=source_name(pool), page_size=2, context="parents") as backfill:
            reports = backfill.run(Window(100, 130), jobs_)
    assert {report.status for report in reports} == {"verified"}
    with Session(engine) as session:
        rows = session.execute(text("SELECT count(*) FROM sync_state WHERE source <> 'horizon:pool'")).scalar()
        pending = session.execute(text("SELECT count(*) FROM sync_coverage WHERE state = 'pending'")).scalar()
    assert rows == 0  # nothing left under a name that depends on an address
    assert pending == 0  # and the run went on from the checkpoint to the end of the window
    engine.dispose()


# -- the isolation of the tests themselves ----------------------------------------


def test_a_test_cannot_open_a_connection_to_the_internet(server_url: str) -> None:
    """The guard of the suite: whatever client, a public address is refused
    before a packet leaves the process; the database is not."""
    import os
    import socket

    from sqlalchemy.engine import make_url

    with pytest.raises(ConnectionRefusedError, match="do not leave the machine"):
        socket.create_connection(("72.44.49.180", 443), timeout=3)  # where horizon.stellar.org is
    with pytest.raises(httpx.ConnectError, match="do not leave the machine"):
        httpx.get("https://horizon.stellar.org/", timeout=3)
    with pytest.raises(httpx.ConnectError, match="do not leave the machine"):
        httpx.Client(trust_env=False).get("https://horizon.stellar.org/", timeout=3)
    with pytest.raises(httpx.ConnectError, match="do not leave the machine"):
        httpx.Client(trust_env=False).post("https://72.44.49.180/api/subscription", json={}, timeout=3)

    address = make_url(os.environ["MTL_HORIZON_TEST_DATABASE_URL"])
    assert address.host is not None and address.port is not None
    with socket.create_connection((address.host, address.port), timeout=3) as inside:
        assert _is_inside(inside.getpeername()[0])  # the database: private, allowed


# -- the accounts of an asset, as the pool reads them -----------------------------


def test_accounts_are_read_in_address_order_with_their_filters_kept(shop: Shop) -> None:
    from tests.factories import address

    holders = {address(number): (number % 3) * 10**7 for number in range(500, 508)}  # some hold nothing
    shop.fake.trustlines[EUR] = holders
    shop.fake.trustlines[RECT] = {address(500): 10**7}
    shop.fake.trustlines["native"] = {address(900): 5}
    shop.fake.origin = shop.fake.origin
    with pool_of(shop.fake) as pool:
        pages = list(pool.address_pages("/accounts", asset=EUR, limit=3))
    addresses = [record["account_id"] for page in pages for record in page.records]
    assert addresses == sorted(holders)  # only the holders of EUR, zero balance or not
    assert [len(page.records) for page in pages] == [3, 3, 2]
    asked = [query for path, query in shop.fake.requests if path == "/accounts"]
    assert all(query["asset"] == EUR and query["limit"] == "3" and query["order"] == "asc" for query in asked)
    assert [query.get("cursor") for query in asked] == [None, address(502), address(505)]  # the link names the last address
    # An account that holds nothing of the asset still has its line, at zero.
    nothing = next(record for record in pages[0].records if record["account_id"] == address(501))  # 501 % 3 == 0: holds nothing
    line = next(balance for balance in nothing["balances"] if balance.get("asset_code") == "EURMTL")
    assert line["balance"] == "0.0000000" and line["limit"] == "922337203685.4775807"
    assert pages[0].served_by == shop.fake.origin


def test_an_account_page_out_of_order_is_refused(shop: Shop) -> None:
    shop.fake.trustlines[EUR] = {A: 1, B: 2}
    # A broken server: the records of a page come in the other order.
    shop.fake.tamper = lambda path, body: {**body, "_embedded": {"records": list(reversed(body["_embedded"]["records"]))}}
    with pool_of(shop.fake) as pool, pytest.raises(Exception, match="not beyond the one before it"):
        list(pool.address_pages("/accounts", asset=EUR, limit=10))


# -- the migration that gives the old names one name ---------------------------------


def test_the_migration_renames_the_old_source_names_and_merges_what_collides(unmigrated_database_url: str) -> None:
    """A database as stage 04 left it, with rows under two old names — the
    pool was re-listed around — some of which would be one row."""
    import os

    from alembic import command

    from tests.conftest import alembic_config

    url = unmigrated_database_url
    os.environ["MTL_HORIZON_DATABASE_URL"] = url
    config = alembic_config(url)
    command.upgrade(config, "0006")
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            for name, cursor, ledger in (("horizon:a.example.org", "500000000000", 95), ("horizon:b.example.org", "400000000000", 90)):
                connection.execute(
                    text(
                        "INSERT INTO sync_state (source, resource_type, direction, stream_key, cursor, cursor_ledger) "
                        "VALUES (:source, 'transaction', 'desc', 'G-account', :cursor, :ledger)"
                    ),
                    {"source": name, "cursor": cursor, "ledger": ledger},
                )
            connection.execute(
                text(
                    "INSERT INTO collector_stream (source, resource_type, stream_key, start_ledger, frontier_ledger) "
                    "VALUES ('horizon:a.example.org', 'transaction', 'G-account', 10, 80), ('horizon:b.example.org', 'transaction', 'G-account', 10, 95)"
                )
            )
            # The database allows no two ranges of one stream to overlap, whatever
            # the source: so two names can each have a part of the stream.
            connection.execute(text("INSERT INTO sync_coverage (subject_type, subject_key, data_type, selection_policy, policy_version, from_ledger, to_ledger, source, state, verified_at) "
                                    "VALUES ('account', 'G-account', 'transactions', 'FULL', 1, 10, 90, 'horizon:a.example.org', 'complete', now()), "
                                    "('account', 'G-account', 'transactions', 'FULL', 1, 91, 95, 'horizon:b.example.org', 'complete', now())"))
        command.upgrade(config, "head")
        with engine.connect() as connection:
            sources = connection.execute(text("SELECT DISTINCT source FROM sync_state UNION SELECT DISTINCT source FROM sync_coverage UNION SELECT DISTINCT source FROM collector_stream")).scalars().all()
            checkpoint = connection.execute(text("SELECT cursor FROM sync_state")).scalar()
            frontier = connection.execute(text("SELECT frontier_ledger FROM collector_stream")).scalar()
            identity = connection.execute(text("SELECT old_source FROM source_identity ORDER BY 1")).scalars().all()
        assert sources == ["horizon:pool"]
        assert checkpoint == "400000000000" and frontier == 95  # of two rows that would be one, the one further along stays: backwards, the lower ledger
        assert identity == ["horizon:a.example.org", "horizon:b.example.org"]
    finally:
        engine.dispose()


def test_of_two_checkpoints_that_would_be_one_the_one_further_along_stays(unmigrated_database_url: str) -> None:
    """Reading backwards the lower checkpoint is further; forwards, the
    higher. A row already under the pool's name wins a tie."""
    import os

    from alembic import command

    from tests.conftest import alembic_config

    url = unmigrated_database_url
    os.environ["MTL_HORIZON_DATABASE_URL"] = url
    config = alembic_config(url)
    command.upgrade(config, "0006")
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            for source, direction, stream, ledger in (
                ("horizon:a.example.org", "desc", "G-one", 80), ("horizon:b.example.org", "desc", "G-one", 90),
                ("horizon:a.example.org", "asc", "G-one", 100), ("horizon:b.example.org", "asc", "G-one", 95),
                ("horizon:a.example.org", "desc", "G-two", 50), ("horizon:pool", "desc", "G-two", 50),
            ):
                connection.execute(
                    text("INSERT INTO sync_state (source, resource_type, direction, stream_key, cursor, cursor_ledger) "
                         "VALUES (:source, 'transaction', :direction, :stream, '1', :ledger)"),
                    {"source": source, "direction": direction, "stream": stream, "ledger": ledger},
                )
        command.upgrade(config, "head")
        with engine.connect() as connection:
            rows = connection.execute(text("SELECT direction, stream_key, cursor_ledger FROM sync_state ORDER BY 1, 2")).all()
        assert rows == [("asc", "G-one", 100), ("desc", "G-one", 80), ("desc", "G-two", 50)]
    finally:
        engine.dispose()
