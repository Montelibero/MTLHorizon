"""The worker: bringing notified transactions in whole, once, and saying
what it could not."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from app.backfill import config as watch_lists
from app.backfill import verify as checking
from app.backfill.loader import Backfill
from app.config import PUBLIC_NETWORK_PASSPHRASE, Settings
from app.notifier import format as notifier_format
from app.sources.horizon import HorizonSettings
from app.sources.pool import HorizonPool, PoolSettings
from app.storage import events, jobs
from app.storage.models import HistoryTransaction, ProcessingJob, TrackedAccount
from app.worker import cli as worker_cli
from app.worker.loader import Worker
from tests.factories import address
from tests.fake_horizon import A, B, C, FakeHorizon, paid, serve
from tests.fake_notifier import notification
from tests.test_backfill import SOURCE, WATCH_LIST, WINDOW
from tests.test_pool import Clock, SETTINGS

T1 = str((100 << 32) | 1)  # A pays B
T3 = str((104 << 32) | (1 << 12) | 1)  # the fee-bump path payment with two trades
T4 = str((108 << 32) | 1)  # a data entry, in a transaction that failed
T5 = str((110 << 32) | (2 << 12) | 1)  # an operation type nobody knows
T6 = str((110 << 32) | (2 << 12) | 2)  # A pays C, in the same transaction
T7 = str((112 << 32) | 1)  # C sells RECT for EUR
T8 = str((118 << 32) | 1)  # C pays A

TRANSACTION = "/transactions/"


class Wall:
    """The time the queue goes by; only moves when a test moves it."""

    def __init__(self) -> None:
        self.now = datetime(2026, 11, 25, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def wait(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class Shop:
    """A private database with the watch list, fake servers, and a worker."""

    def __init__(self, url: str, *fakes: FakeHorizon, max_attempts: int = 3) -> None:
        self.engine = create_engine(url)
        self.fake = fakes[0]
        self.wall = Wall()
        self.clock = Clock()
        with Session(self.engine) as session, session.begin():
            watch_lists.apply(session, WATCH_LIST)
        self.pool = HorizonPool(
            tuple(fake.origin for fake in fakes),
            HorizonSettings(**SETTINGS),
            network_passphrase=PUBLIC_NETWORK_PASSPHRASE,
            pool_settings=PoolSettings(health_check_interval=60, failover_cooldown=5),
            transports={fake.origin: httpx.MockTransport(fake._handle) for fake in fakes},
            clock=self.clock,
            sleeper=self.clock.sleep,
        )
        self.worker = self.new_worker("worker-1", max_attempts=max_attempts)

    def new_worker(self, name: str, *, max_attempts: int = 3) -> Worker:
        return Worker(self.pool, self.engine, name=name, max_attempts=max_attempts, retry_base=5.0, lease_seconds=120, clock=self.wall)

    def close(self) -> None:
        self.pool.close()
        self.engine.dispose()

    def notice(self, operation: str, *, fake: FakeHorizon | None = None, subscription: str = "s1") -> Any:
        return notifier_format.read(notification(fake or self.fake, operation, subscription=subscription))

    def receive(self, notice: Any) -> Any:
        return events.receive(self.engine, notice, received_at=self.wall(), headers={}, signed_by="G-test")

    def deliver(self, *operations: str, fake: FakeHorizon | None = None, subscription: str = "s1") -> list[Any]:
        """Deliver a notification of each operation; each call makes new notifications."""
        return [self.receive(self.notice(operation, fake=fake, subscription=subscription)) for operation in operations]

    def job(self, job_id: int = 1) -> ProcessingJob:
        with Session(self.engine) as session:
            return session.get(ProcessingJob, job_id)  # type: ignore[return-value]

    def count(self, table: str) -> int:
        with self.engine.connect() as connection:
            return connection.execute(text(f"SELECT count(*) FROM {table}")).scalar()

    def asked(self, part: str) -> int:
        return sum(1 for path, _ in self.fake.requests if part in path)


@pytest.fixture
def shop(make_database: Callable[[], str]) -> Any:
    instance = Shop(make_database(), FakeHorizon())
    yield instance
    instance.close()


def transaction_hash(fake: FakeHorizon, operation: str) -> str:
    return fake.ledger.operations[operation]["transaction_hash"]


# -- one transaction, brought in whole -------------------------------------------


def test_a_notified_transaction_is_brought_in_whole(shop: Shop) -> None:
    shop.deliver(T1)
    assert shop.worker.once() is True
    job = shop.job()
    assert job.state == jobs.DONE and job.result is not None and job.result["context"] == ["effects", "operations", "trades"]
    assert shop.count("history_transactions") == 1 and shop.count("history_operations") == 1 and shop.count("history_effects") == 2
    with Session(shop.engine) as session:
        stored = session.scalar(select(HistoryTransaction).where(HistoryTransaction.transaction_hash == job.transaction_hash))
    assert stored is not None and set(stored.context_loaded) == {"operations", "effects", "trades"}
    # Its root once, then the transaction, its operations and its effects; no trades: it made none.
    assert [path for path, _ in shop.fake.requests] == [
        "/", f"{TRANSACTION}{job.transaction_hash}", f"{TRANSACTION}{job.transaction_hash}/operations", f"{TRANSACTION}{job.transaction_hash}/effects"
    ]
    assert shop.worker.once() is False  # nothing else to do; nothing is polled
    assert len(shop.fake.requests) == 4


def test_what_the_worker_stores_is_what_the_backfill_stores(make_database: Callable[[], str]) -> None:
    shop = Shop(make_database(), FakeHorizon())
    back = Shop(make_database(), FakeHorizon())
    try:
        for operation in (T1, T3, T4, T5, T7, T8):
            shop.deliver(operation)
        for _ in range(6):
            shop.worker.once()
        assert jobs.counts(shop.engine)["done"] == 6  # T5 and its transaction's other operation are one job
        with Backfill(back.pool, back.engine, source=SOURCE, page_size=200) as backfill:
            backfill.run(WINDOW, backfill_jobs(back.engine))
        assert checking.fingerprint(shop.engine, WINDOW) == checking.fingerprint(back.engine, WINDOW)
    finally:
        shop.close()
        back.close()


def backfill_jobs(engine: Any) -> list[Any]:
    from app.backfill.jobs import stream_jobs

    with Session(engine) as session:
        return stream_jobs(session)[0]


def test_one_transaction_in_several_notifications_is_fetched_once(shop: Shop) -> None:
    shop.deliver(T5, T6, subscription="s1")
    shop.deliver(T5, T6, subscription="s2")
    shop.deliver(T6, subscription="s3")
    assert shop.count("notifier_event") == 5 and shop.count("processing_job") == 1
    assert shop.worker.once() is True and shop.worker.once() is False
    assert shop.job().state == jobs.DONE
    assert shop.asked(transaction_hash(shop.fake, T5)) == 3  # once for the transaction, its operations and its effects
    assert shop.count("history_transactions") == 1 and shop.count("history_operations") == 2
    # The operation nobody knows is kept as it came; it is not left out.
    with shop.engine.connect() as connection:
        assert connection.execute(text("SELECT type FROM history_operations WHERE application_order = 1")).scalar() == "unknown"


def test_a_notification_that_comes_after_the_work_is_done_fetches_nothing(shop: Shop) -> None:
    shop.deliver(T1)
    shop.worker.once()
    asked = len(shop.fake.requests)
    shop.deliver(T1, subscription="later")
    assert shop.worker.once() is False and len(shop.fake.requests) == asked
    assert shop.count("notifier_event") == 2 and shop.count("processing_job") == 1


# -- workers, and what happens to them -------------------------------------------


def test_two_workers_never_take_one_job(shop: Shop) -> None:
    shop.deliver(T1, T7)
    one = jobs.take(shop.engine, "one", lease_seconds=60, now=shop.wall())
    two = jobs.take(shop.engine, "two", lease_seconds=60, now=shop.wall())
    assert one is not None and two is not None and one.id != two.id
    assert jobs.take(shop.engine, "three", lease_seconds=60, now=shop.wall()) is None
    assert [job.leased_by for job in (one, two)] == ["one", "two"]
    assert jobs.counts(shop.engine)["running"] == 2


def test_two_workers_working_at_the_same_time_bring_every_job_in_once(make_database: Callable[[], str]) -> None:
    shop = Shop(make_database(), FakeHorizon())
    try:
        for operation in (T1, T3, T5, T7):
            shop.deliver(operation)
        first, second = shop.worker, shop.new_worker("worker-2")
        done: list[int] = []

        def work(worker: Worker) -> None:
            for _ in range(4):
                if worker.once():
                    done.append(1)

        threads = [threading.Thread(target=work, args=(worker,)) for worker in (first, second)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)
        assert sum(done) == 4 and jobs.counts(shop.engine)["done"] == 4
        assert shop.count("history_transactions") == 4
    finally:
        shop.close()


def test_a_worker_that_dies_leaves_its_job_to_be_taken_again(shop: Shop) -> None:
    shop.deliver(T1)
    dead = jobs.take(shop.engine, "dead", lease_seconds=60, now=shop.wall())
    assert dead is not None and dead.attempts == 1
    assert jobs.take(shop.engine, "alive", lease_seconds=60, now=shop.wall()) is None  # the lease holds
    shop.wall.wait(61)
    alive = jobs.take(shop.engine, "alive", lease_seconds=60, max_attempts=3, now=shop.wall())
    assert alive is not None and alive.id == dead.id and alive.attempts == 2 and alive.leased_by == "alive"


def test_a_worker_that_dies_after_storing_a_part_is_resumed_from_there(shop: Shop) -> None:
    """It stored the transaction and its operations and was killed before the
    effects: nothing is lost, nothing is fetched twice, and while its lease
    holds nobody else touches the job."""
    shop.deliver(T1)
    digest = transaction_hash(shop.fake, T1)
    job = jobs.take(shop.engine, shop.worker.name, lease_seconds=120, max_attempts=3, now=shop.wall())
    assert job is not None

    def crash(bundle: Any, last: bool) -> None:
        shop.worker._keep(bundle, last)
        if "operations" in bundle.completes:
            raise RuntimeError("killed after storing the operations")

    shop.worker._job = job  # the worker had got this far
    with pytest.raises(RuntimeError, match="killed"):
        shop.worker.contexts.fetch_in_steps(digest, crash)
    assert (shop.count("history_transactions"), shop.count("history_operations"), shop.count("history_effects")) == (1, 1, 0)
    assert shop.job().state == jobs.RUNNING
    assert shop.worker.once() is False  # the lease holds: another worker leaves it alone
    shop.wall.wait(121)
    asked = shop.asked(f"{digest}/operations")

    assert shop.worker.once() is True
    assert (shop.job().state, shop.job().attempts) == (jobs.DONE, 2)
    assert shop.asked(f"{digest}/operations") == asked  # resumed from where it died
    assert shop.count("history_effects") == 2


def test_a_lease_that_ran_out_three_times_ends_in_partial_or_dead(shop: Shop) -> None:
    shop.deliver(T1)
    for _ in range(3):
        job = jobs.take(shop.engine, "gone", lease_seconds=60, max_attempts=3, now=shop.wall())
        assert job is not None
        shop.wall.wait(61)
    assert jobs.take(shop.engine, "next", lease_seconds=60, max_attempts=3, now=shop.wall()) is None
    assert shop.job().state == jobs.DEAD  # nothing was stored: there is nothing to keep trying for
    assert jobs.retry(shop.engine, now=shop.wall()) == 1 and shop.job().state == jobs.PENDING


def test_stopping_the_worker_puts_its_job_back_as_it_was(shop: Shop) -> None:
    shop.deliver(T1)
    stop = threading.Event()
    stop.set()
    assert shop.worker.once(stop=stop) is True
    job = shop.job()
    assert (job.state, job.attempts, job.leased_by) == (jobs.PENDING, 0, None)
    assert shop.fake.requests == []  # it stopped before asking anything
    assert shop.worker.once() is True and shop.job().state == jobs.DONE


# -- tries that fail -------------------------------------------------------------


def test_a_try_that_fails_halfway_keeps_what_it_fetched_and_fetches_only_the_rest(shop: Shop) -> None:
    digest = transaction_hash(shop.fake, T1)
    shop.fake.fail(f"{digest}/effects", 503, times=4)  # every attempt of the first try
    shop.deliver(T1)
    assert shop.worker.once() is True
    job = shop.job()
    assert (job.state, job.attempts) == (jobs.PENDING, 1)
    assert job.next_attempt_at == shop.wall() + timedelta(seconds=5) and "503" in (job.last_error or "")
    assert shop.count("history_transactions") == 1 and shop.count("history_operations") == 1  # what came before the failure stays
    asked_operations = shop.asked(f"{digest}/operations")

    shop.wall.wait(6)
    assert shop.worker.once() is True
    assert shop.job().state == jobs.DONE and shop.count("history_effects") == 2
    assert shop.asked(f"{digest}/operations") == asked_operations  # not asked for again
    # The first try: the transaction, its operations, the effects 4 times; the second: the transaction and its effects.
    assert shop.asked(digest) == 8


def test_a_job_that_keeps_failing_gives_up_as_partial_and_only_a_person_takes_it_back(shop: Shop) -> None:
    digest = transaction_hash(shop.fake, T1)
    shop.fake.fail(f"{digest}/effects", 503, times=100)
    shop.deliver(T1)
    for tries in (1, 2, 3):
        assert shop.worker.once() is True
        shop.wall.wait(11)
    assert shop.worker.once() is False
    job = shop.job()
    assert (job.state, job.attempts) == (jobs.PARTIAL, 3)
    assert job.result is not None and job.result["missing"] == ["effects"]
    assert job.finished_at is not None
    # Word of the transaction coming again does not fetch anything either: only a retry does.
    shop.fake.faults.clear()
    shop.deliver(T1, subscription="again")
    assert shop.job().state == jobs.PENDING  # it had given up, and word came again: back into the queue, once more
    assert shop.worker.once() is True and shop.job().state == jobs.DONE


def test_a_transaction_on_none_of_the_servers_is_dead_and_nothing_is_stored(shop: Shop) -> None:
    """Not there, time after time: asked for as many times as it has tries,
    then dead, with the reason."""
    digest = transaction_hash(shop.fake, T1)
    shop.fake.hidden_transactions.add(digest)
    shop.deliver(T1)
    for _ in range(shop.worker.max_attempts):
        shop.worker.once()
        shop.wall.wait(11)
    job = shop.job()
    assert (job.state, job.attempts) == (jobs.DEAD, shop.worker.max_attempts)
    assert "on none of the servers" in (job.last_error or "")
    assert shop.count("history_transactions") == 0


def test_a_transaction_ahead_of_every_server_is_waited_for_not_declared_missing(make_database: Callable[[], str]) -> None:
    source = FakeHorizon()
    source.close(paid(131, A, B), latest=132)
    ahead = Shop(make_database(), FakeHorizon(latest=130), FakeHorizon(origin="https://b.horizon.test", latest=130))
    try:
        operation = str((131 << 32) | 1)
        ahead.deliver(operation, fake=source)
        ahead.worker.once()
        job = ahead.job()
        assert (job.state, job.attempts) == (jobs.PENDING, 0)  # the servers' doing: the try is not counted
        assert "has got only to ledger 130" in (job.last_error or "")
        assert ahead.pool.stats.by_server == {}
        ahead.fake.close(paid(131, A, B), latest=132)  # the first server catches up
        ahead.wall.wait(601)  # a job that waits for the servers is asked again in ten minutes
        assert ahead.worker.once() is True and ahead.job().state == jobs.DONE
    finally:
        ahead.close()


def test_a_fee_bump_transaction_and_a_failed_one_are_brought_in_like_any_other(shop: Shop) -> None:
    shop.deliver(T3, T4)
    shop.worker.once()
    shop.worker.once()
    assert jobs.counts(shop.engine)["done"] == 2
    with Session(shop.engine) as session:
        bump = session.scalar(select(HistoryTransaction).where(HistoryTransaction.transaction_hash == transaction_hash(shop.fake, T3)))
        failed = session.scalar(select(HistoryTransaction).where(HistoryTransaction.transaction_hash == transaction_hash(shop.fake, T4)))
    assert bump is not None and failed is not None
    assert bump.inner_transaction_hash is not None  # the fee bump is known by both hashes
    assert shop.count("history_trades") == 2  # the pool trade and the market trade
    assert shop.asked("/trades?") + shop.asked("/trades") >= 1


def test_the_worker_and_a_backfill_at_the_same_time_store_no_duplicates(make_database: Callable[[], str]) -> None:
    alone = Shop(make_database(), FakeHorizon())
    both = Shop(make_database(), FakeHorizon())
    try:
        with Backfill(alone.pool, alone.engine, source=SOURCE, page_size=5) as backfill:
            backfill.run(WINDOW, backfill_jobs(alone.engine), concurrency=2)
        both.deliver(T1, T3, T7)
        results: list[Any] = []

        def read() -> None:
            with Backfill(both.pool, both.engine, source=SOURCE, page_size=5) as backfill:
                results.append(backfill.run(WINDOW, backfill_jobs(both.engine), concurrency=2))

        thread = threading.Thread(target=read)
        thread.start()
        try:
            for _ in range(3):
                both.worker.once()
        finally:
            thread.join(120)
        assert jobs.counts(both.engine)["done"] == 3
        assert checking.fingerprint(both.engine, WINDOW) == checking.fingerprint(alone.engine, WINDOW)
    finally:
        alone.close()
        both.close()


# -- the commands ------------------------------------------------------------------


def test_status_retry_and_events_as_commands(shop: Shop, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    shop.deliver(T1, T4, subscription="s1")
    shop.deliver(T1, subscription="s2")
    shop.fake.hidden_transactions.add(transaction_hash(shop.fake, T4))
    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", shop.engine.url.render_as_string(hide_password=False))
    serve(monkeypatch, shop.fake)  # the commands read the fake, as they would read real servers
    assert worker_cli.main(["status"]) == worker_cli.OK
    out = capsys.readouterr().out
    assert "queue: 2 pending" in out and "3 queued" in out and "highest ledger among notifications: 108" in out
    assert "not a frontier" in out
    assert worker_cli.main(["events", "--limit", "3"]) == worker_cli.OK
    assert "s2" in capsys.readouterr().out

    once = ["once", "--retry-base", "0", "--max-attempts", "3"]  # a repeated try is due at once; three tries
    assert worker_cli.main(once) == worker_cli.OK  # one of the two; which is not said
    assert worker_cli.main(once) == worker_cli.FAILED  # the other is on no server: not done yet
    assert worker_cli.main(["status"]) == worker_cli.OK
    out = capsys.readouterr().out
    assert "1 done" in out and "1 pending" in out and "not on any server yet" in out
    for _ in range(2):  # three tries in all, then it is dead
        assert worker_cli.main(once) == worker_cli.FAILED
    assert worker_cli.main(["status"]) == worker_cli.OK
    out = capsys.readouterr().out
    assert "1 done" in out and "1 dead" in out and "on none of the servers" in out
    assert worker_cli.main(["retry"]) == worker_cli.OK
    assert "1 job(s) back in the queue" in capsys.readouterr().out


def test_a_command_that_was_given_no_fake_server_never_leaves_the_test(monkeypatch: pytest.MonkeyPatch) -> None:
    """The guard of the suite itself: a pool built from the environment has
    no way out, whatever address the environment names."""
    from app import cli_common

    pool = cli_common.make_client(Settings(database_url="", horizon_urls=("https://horizon.stellar.org",)))
    try:
        with pytest.raises(Exception, match="no server of the pool can answer"):
            pool.get("/")
        assert "no fake server" in (pool.servers[0].last_error or "")
        assert pool.stats.responses_by_status == {}
    finally:
        pool.close()


# -- what it costs, and what it does not promise -----------------------------------


def _thousand_accounts(engine: Any) -> None:
    """A thousand more accounts switched on, as the watch list would bring them."""
    lists = watch_lists.parse(
        {"accounts": [{"address": address(number), "enabled": True, "reason": "a thousand, for the measure"} for number in range(100, 1100)]},
        "test",
    )
    with Session(engine) as session, session.begin():
        watch_lists.apply(session, lists)


def test_a_thousand_watched_accounts_cost_no_requests_while_nothing_happens(shop: Shop) -> None:
    _thousand_accounts(shop.engine)
    stop = threading.Event()
    thread = threading.Thread(target=shop.worker.run, args=(stop,), kwargs={"poll_seconds": 0.01})
    thread.start()
    time.sleep(0.4)  # what a worker does when the queue is empty
    stop.set()
    thread.join(30)
    assert not thread.is_alive()
    assert shop.fake.requests == []  # nobody is polled: a thousand accounts, not one request
    assert shop.pool.stats.requests == 0
    with Session(shop.engine) as session:
        assert len(list(session.scalars(select(TrackedAccount).where(TrackedAccount.enabled.is_(True))))) == 1002


def test_a_series_of_repeated_and_overlapping_notifications(shop: Shop) -> None:
    """121 transactions, each told of for two subscriptions, and one of the
    deliveries sent again. What it costs in requests, time and database, and
    that nothing is stored twice."""
    _thousand_accounts(shop.engine)
    source = shop.fake
    operations: list[str] = []
    for ledger in range(200, 320):
        source.close(paid(ledger, A, address(ledger)), latest=ledger)
        operations.append(str((ledger << 32) | 1))
    # Two operations of one transaction: the last one pays two accounts.
    source.close(paid(320, A, B, C), latest=320)
    last = [str((320 << 32) | 1), str((320 << 32) | 2)]

    started = time.monotonic()
    repeats = 0
    for operation in operations + last:
        first = shop.notice(operation, subscription="one")
        shop.receive(first)
        shop.receive(shop.notice(operation, subscription="two"))
        repeats += 0 if shop.receive(first).created else 1  # the same delivery, sent again
    delivering = time.monotonic() - started
    events_kept = shop.count("notifier_event")
    assert repeats == 122 and events_kept == 244

    started = time.monotonic()
    with shop.new_worker("measure", max_attempts=3) as worker:
        for _ in range(200):
            if not worker.once():
                break
    working = time.monotonic() - started

    assert jobs.counts(shop.engine) == {"pending": 0, "running": 0, "done": 121, "partial": 0, "dead": 0}
    assert shop.count("history_transactions") == 121 and shop.count("history_operations") == 122
    with shop.engine.connect() as connection:
        stored = connection.execute(text("SELECT count(*), count(DISTINCT id) FROM history_operations")).one()
        size = connection.execute(text("SELECT pg_total_relation_size('notifier_event'), pg_total_relation_size('processing_job')")).one()
    assert stored[0] == stored[1] == 122  # nothing twice
    requests = shop.pool.stats.requests
    # One root for the server, then the transaction, its operations and its effects: three a transaction.
    assert requests <= 3 * 121 + 1, requests
    print(
        f"\n121 transactions, {events_kept} deliveries kept and {repeats} repeats recognised: "
        f"{requests} requests, {delivering:.2f}s to keep the deliveries, {working:.2f}s to bring them in, "
        f"journal {size[0] // 1024} kB, queue {size[1] // 1024} kB"
    )


def test_what_nobody_told_of_is_not_there_and_the_status_says_so(
    shop: Shop, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The boundary of the guarantee, shown rather than papered over.

    The notifications told of ledgers up to 112. The transaction of ledger
    108 — it failed, and nobody is subscribed to its account — was not told
    of, and is not here. The status says what the highest ledger seen is,
    and what it is not.
    """
    shop.deliver(T1, T3, T7)
    for _ in range(3):
        shop.worker.once()
    assert jobs.counts(shop.engine)["done"] == 3
    assert shop.count("history_transactions") == 3  # not the failed transaction of ledger 108, nor the one of 110
    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", shop.engine.url.render_as_string(hide_password=False))
    serve(monkeypatch, shop.fake)
    assert worker_cli.main(["status"]) == worker_cli.OK
    out = capsys.readouterr().out
    assert "highest ledger among notifications: 112" in out
    assert "not a frontier" in out and "nothing says every ledger below it" in out
