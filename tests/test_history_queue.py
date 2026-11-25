"""The queue of history imports: planned once, read in slices, and still
there after whatever happens to it."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from datetime import timedelta
from typing import Any

import httpx
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.backfill import config as watch_lists
from app.backfill import verify as checking
from app.backfill.jobs import market_key, stream_jobs
from app.backfill.loader import Backfill, RunStopped
from app.cli_common import LOGICAL_SOURCE
from app.config import PUBLIC_NETWORK_PASSPHRASE, Settings
from app.history import cli as history_cli
from app.history.runner import DEFAULT_DEFER, HistoryWorker, SliceLimits
from app.sources.horizon import HorizonSettings
from app.sources.pool import HorizonPool, PoolSettings
from app.storage import coverage, history_jobs
from app.storage.models import HistoryJob
from tests.factories import address
from tests.fake_horizon import A, B, C, D, EUR, ISSUER, RECT, FakeHorizon, paid, sale, serve
from tests.test_backfill import WATCH_LIST, WINDOW
from tests.test_pool import SETTINGS, Clock
from tests.test_worker import Wall

MARKET = market_key(RECT, EUR)
STREAMS = ("transactions", "operations", "effects", "trades")
WAITING, RUNNING, DONE, PARTIAL, ERROR, PAUSED = history_jobs.STATES


class Yard:
    """A private database with the watch list, fake servers and a worker of
    history imports."""

    def __init__(self, url: str, *fakes: FakeHorizon, **options: Any) -> None:
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
        self.worker = self.new_worker("worker-1", **options)

    def new_worker(self, name: str, *, max_attempts: int = 3, **options: Any) -> HistoryWorker:
        return HistoryWorker(
            self.pool,
            self.engine,
            name=name,
            max_attempts=max_attempts,
            retry_base=30.0,
            clock=self.wall,
            **{"lease_seconds": 300.0, **options},
        )

    def plan(self, subject: str = A, **options: Any) -> tuple[HistoryJob, bool]:
        """Plan the history of an account up to the ledger the fake is at."""
        options.setdefault("to_ledger", self.fake.latest)
        return history_jobs.plan(self.engine, kind="account", subject=subject, now=self.wall(), **options)

    def watch(self, account: str) -> None:
        """One more account on the watch list, switched on."""
        lists = watch_lists.parse(
            {"accounts": [{"address": account, "enabled": True, "reason": "for the test"}]}, "test"
        )
        with Session(self.engine) as session, session.begin():
            watch_lists.apply(session, lists)

    def switch(self, account: str, enabled: bool) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                text("UPDATE tracked_accounts SET enabled = :enabled WHERE account_id = :account"),
                {"account": account, "enabled": enabled},
            )

    def job(self, job_id: int = 1) -> HistoryJob:
        job = history_jobs.get(self.engine, job_id)
        assert job is not None
        return job

    def rows(self, statement: str) -> list[Any]:
        with self.engine.connect() as connection:
            return connection.execute(text(statement)).all()

    def count(self, table: str) -> int:
        return self.rows(f"SELECT count(*) FROM {table}")[0][0]

    def close(self) -> None:
        self.pool.close()
        self.engine.dispose()


@pytest.fixture
def yard(make_database: Callable[[], str]) -> Iterator[Yard]:
    instance = Yard(make_database(), FakeHorizon())
    yield instance
    instance.close()


def finish(worker: HistoryWorker, limit: int = 200) -> int:
    """Give slices until nothing is due. How many were given."""
    given = 0
    while given < limit and worker.once():
        given += 1
    return given


def ranges_of(engine: Any, subject_type: str, key: str) -> dict[str, list[tuple[str, int, int]]]:
    """State and ledgers of every range stored for a subject, by stream."""
    with Session(engine) as session:
        return {
            stream.key.data_type: [
                (row.state, row.from_ledger, row.to_ledger) for row in coverage.ranges(session, stream.key)
            ]
            for stream in stream_jobs(session)[0]
            if (stream.key.subject_type, stream.key.subject_key) == (subject_type, key)
        }


def verified(ranges: dict[str, list[tuple[str, int, int]]], bottom: int, top: int) -> bool:
    """Whether every stream has [bottom, top] verified — read, or empty —
    over ranges that may come in several pieces."""
    def covered(spans: list[tuple[str, int, int]]) -> bool:
        reach = bottom - 1
        for state, low, high in sorted(spans, key=lambda span: span[1]):
            if state in ("complete", "empty") and low <= reach + 1:
                reach = max(reach, high)
        return reach >= top

    return bool(ranges) and all(covered(spans) for spans in ranges.values())


def fates(job: HistoryJob) -> dict[str, str]:
    """What became of every stream of a job, by stream: its status."""
    assert job.result is not None
    return {name.rpartition(" / ")[2]: stream["status"] for name, stream in job.result["streams"].items()}


def served(fake: FakeHorizon) -> list[str]:
    """The subjects asked for so far, in order: requests of one subject that
    follow each other count as one turn."""
    order: list[str] = []
    for path, _ in fake.requests:
        parts = path.split("/")
        subject = parts[2] if len(parts) == 4 and parts[1] == "accounts" else (MARKET if path == "/trades" else None)
        if subject is not None and (not order or order[-1] != subject):
            order.append(subject)
    return order


def in_turn(needs: dict[str, int]) -> list[str]:
    """The order a fair queue serves subjects in: the one that waited longest
    among those that still need slices; one never served before all others."""
    order: list[str] = []
    last: dict[str, int] = {}
    while any(needs.values()):
        due = [subject for subject, left in needs.items() if left > 0]
        subject = min(due, key=lambda who: (last.get(who, -1), list(needs).index(who)))
        last[subject] = len(order)
        needs[subject] -= 1
        order.append(subject)
    return order


def nothing_twice(engine: Any) -> None:
    """No row stored twice, in the tables where a record met again could
    leave a second row."""
    with engine.connect() as connection:
        for table, key in (
            ("history_transactions", "transaction_hash"),
            ("history_operations", "id"),
            ("history_effects", "id"),
            ("history_trades", "id"),
            ("history_transaction_participants", "transaction_id, account_id"),
            ("history_operation_participants", "operation_id, account_id"),
        ):
            stored, distinct = connection.execute(text(f"SELECT count(*), count(DISTINCT ({key})) FROM {table}")).one()
            assert stored == distinct, table


# -- planning ------------------------------------------------------------------


def test_planning_again_raises_the_boundary_and_never_starts_over(yard: Yard) -> None:
    job, created = yard.plan(A, to_ledger=120)
    assert created, "a subject not planned before is planned now"
    assert (job.state, job.context, job.boundary_final, job.from_ledger) == (WAITING, "parents", False, None)
    assert (job.next_attempt_at, job.to_ledger) == (yard.wall(), 120)

    again, created = yard.plan(A, to_ledger=110, from_ledger=105)
    assert not created, "a job that exists is not made again"
    assert (again.id, again.from_ledger, again.to_ledger) == (job.id, None, 120), "a boundary never moves inwards"

    yard.worker = yard.new_worker("small", slice_requests=1)
    assert yard.worker.once() is True and yard.worker.once() is True
    job = yard.job()
    assert (job.state, job.slices, job.attempts) == (WAITING, 2, 0)
    read = ranges_of(yard.engine, "account", A)
    assert read["transactions"] == read["operations"] == [("complete", 100, 120)]
    assert read["effects"] == read["trades"] == [("pending", 100, 120)]
    asked = len(yard.fake.requests)

    again, created = yard.plan(A, to_ledger=125)
    assert not created and (again.state, again.to_ledger, again.slices) == (WAITING, 125, 2)
    assert ranges_of(yard.engine, "account", A) == read, "what was verified stays verified"
    assert len(yard.fake.requests) == asked, "planning asks nothing"

    yard.worker = yard.new_worker("whole")  # a budget for all that is left
    assert finish(yard.worker) == 1 and yard.job().state == DONE
    assert verified(ranges_of(yard.engine, "account", A), 100, 125)
    slices, first_finished = yard.job().slices, yard.job().finished_at
    assert first_finished is not None

    # Done, and then there is more: back to the queue with its tries whole,
    # and what it read is not read again.
    job, created = yard.plan(A, to_ledger=130)
    assert not created and (job.state, job.attempts, job.finished_at, job.slices) == (WAITING, 0, None, slices)
    assert finish(yard.worker) == 1 and yard.job().state == DONE
    assert verified(ranges_of(yard.engine, "account", A), 100, 130)
    assert yard.job().slices == slices + 1 and yard.job().finished_at == first_finished, "the wall did not move"

    # The same boundary again: nothing is left to do, and nothing starts.
    job, created = yard.plan(A, to_ledger=130)
    assert not created and job.state == DONE and job.slices == slices + 1


def test_the_boundary_becomes_final_when_events_are_guaranteed_and_a_pause_stays(yard: Yard) -> None:
    job, _ = yard.plan(A, to_ledger=120)
    assert finish(yard.worker) == 1 and yard.job().state == DONE
    raised = history_jobs.extend_boundary(yard.engine, job.id, to_ledger=125, boundary_final=False, now=yard.wall())
    assert raised is not None and (raised.state, raised.to_ledger, raised.boundary_final) == (WAITING, 125, False)
    assert finish(yard.worker) == 1 and (yard.job().state, yard.job().boundary_final) == (DONE, False)

    # Paused by hand: the boundary moves all the same, and planning does not lift it.
    raised = history_jobs.extend_boundary(yard.engine, job.id, to_ledger=128, boundary_final=False, now=yard.wall())
    assert raised is not None and raised.state == WAITING
    assert history_jobs.pause(yard.engine, ids=[job.id], because="maintenance") == 1
    raised = history_jobs.extend_boundary(yard.engine, job.id, to_ledger=130, now=yard.wall())
    assert raised is not None and (raised.state, raised.to_ledger, raised.boundary_final) == (PAUSED, 130, True)
    yard.fake.close(latest=131)
    again, _ = yard.plan(A, to_ledger=131, resume=True)
    assert (again.state, again.to_ledger, again.boundary_final) == (PAUSED, 131, False), (
        "raised past what is vouched for: not final any more"
    )
    asked = len(yard.fake.requests)
    assert yard.worker.once() is False and len(yard.fake.requests) == asked, "a paused job is not worked on"

    # A boundary that was already above what is vouched for is not made final.
    other, _ = yard.plan(B, to_ledger=135)
    raised = history_jobs.extend_boundary(yard.engine, other.id, to_ledger=130, now=yard.wall())
    assert raised is not None and (raised.to_ledger, raised.boundary_final) == (135, False)
    raised = history_jobs.extend_boundary(yard.engine, other.id, to_ledger=135, now=yard.wall())
    assert raised is not None and raised.boundary_final is True
    assert history_jobs.extend_boundary(yard.engine, 999, to_ledger=140, now=yard.wall()) is None
    with pytest.raises(ValueError, match="above zero"):
        history_jobs.extend_boundary(yard.engine, other.id, to_ledger=0, now=yard.wall())
    assert history_jobs.pause(yard.engine, ids=[other.id], because="out of the way of this test") == 1

    assert history_jobs.resume(yard.engine, ids=[job.id], now=yard.wall()) == 1
    assert (yard.job().state, yard.job().attempts, yard.job().paused_because) == (WAITING, 0, None)
    assert finish(yard.worker) == 1 and history_jobs.counts(yard.engine)["done"] == 1
    assert (yard.job().state, yard.job().to_ledger) == (DONE, 131)


def test_two_workers_never_take_one_job(yard: Yard) -> None:
    yard.plan(A)
    yard.plan(B)
    one = history_jobs.take(yard.engine, "one", lease_seconds=60, now=yard.wall())
    two = history_jobs.take(yard.engine, "two", lease_seconds=60, now=yard.wall())
    assert one is not None and two is not None and one.id != two.id
    assert history_jobs.take(yard.engine, "three", lease_seconds=60, now=yard.wall()) is None
    assert ([job.leased_by for job in (one, two)], [job.lease_expires_at for job in (one, two)]) == (
        ["one", "two"],
        [yard.wall() + timedelta(seconds=60)] * 2,
    )
    assert history_jobs.counts(yard.engine) == {WAITING: 0, RUNNING: 2, DONE: 0, PARTIAL: 0, ERROR: 0, PAUSED: 0}
    assert [job.id for job in history_jobs.list(yard.engine, state=RUNNING)] == [one.id, two.id]
    assert [job.id for job in history_jobs.list(yard.engine)] == [one.id, two.id]
    assert history_jobs.get(yard.engine, 999) is None


def test_planning_that_says_nonsense_is_refused_and_stores_nothing(yard: Yard) -> None:
    for options, said in (
        ({"kind": "asset", "subject": A, "to_ledger": 130}, "kind"),
        ({"kind": "account", "subject": A, "to_ledger": 130, "context": "all"}, "context"),
        ({"kind": "account", "subject": A, "to_ledger": 120, "from_ledger": 125}, "above to_ledger"),
        ({"kind": "account", "subject": A, "to_ledger": 0}, "above zero"),
    ):
        with pytest.raises(ValueError, match=said):
            history_jobs.plan(yard.engine, now=yard.wall(), **options)
    with pytest.raises(ValueError, match="say which jobs"):
        history_jobs.pause(yard.engine, because="nothing named")
    assert yard.rows("SELECT count(*) FROM history_job") == [(0,)]

    # A lower bound given is honoured when it is the lower one; the upper
    # boundary and the progress are not touched by it.
    job, _ = yard.plan(B, to_ledger=130, from_ledger=110)
    assert (job.from_ledger, job.to_ledger) == (110, 130)
    again, created = yard.plan(B, from_ledger=105)
    assert not created and (again.from_ledger, again.to_ledger) == (105, 130)
    again, created = yard.plan(B, from_ledger=120)
    assert not created and again.from_ledger == 105, "a lower bound never moves up"
    assert history_jobs.extend_boundary(yard.engine, job.id, to_ledger=135, now=yard.wall()).from_ledger == 105


# -- slices --------------------------------------------------------------------


def test_slices_with_a_tiny_budget_bring_the_whole_history_and_count_what_it_cost(yard: Yard) -> None:
    yard.plan(B)
    yard.worker = yard.new_worker("small", slice_requests=2)
    states: list[tuple[str, str | None]] = []
    while yard.worker.once() and len(states) < 100:
        job = yard.job()
        states.append((job.state, job.leased_by))
    assert states[-1] == (DONE, None) and states[:-1] and set(states[:-1]) == {(WAITING, None)}, (
        "between two slices a job is never left running"
    )
    job = yard.job()
    assert job.requests == len(yard.fake.requests) == yard.pool.stats.requests, "every request the fake saw is counted"
    assert (job.slices, job.pages, job.records) == (len(states), 4, 11)
    assert (job.started_at, job.finished_at, job.last_error) == (yard.wall(), yard.wall(), None)
    assert job.result is not None and job.result["window"] == [100, 130] and job.result["unavailable_ledgers"] == 0
    assert list(job.result["servers"]) == [yard.fake.origin] and 0 < sum(job.result["servers"].values()) < job.requests
    assert fates(job) == dict.fromkeys(STREAMS, "verified")
    assert verified(ranges_of(yard.engine, "account", B), 100, 130)
    assert (yard.worker.done, yard.worker.failed) == (1, 0)


def test_what_the_queue_stores_is_what_the_backfill_stores(make_database: Callable[[], str]) -> None:
    queued, plain = Yard(make_database(), FakeHorizon()), Yard(make_database(), FakeHorizon())
    try:
        for subject in (A, B):
            queued.plan(subject)
        history_jobs.plan(queued.engine, kind="market", subject=MARKET, to_ledger=130, now=queued.wall())
        queued.worker = queued.new_worker("small", slice_requests=2)
        given = 0
        while given < 200 and queued.worker.once():
            given += 1
        assert history_jobs.counts(queued.engine)["done"] == 3 and given > 3
        assert queued.worker.failed == 0
        with Session(plain.engine) as session:
            jobs = [stream for stream in stream_jobs(session)[0] if stream.key.subject_key in (A, B, MARKET)]
        with Backfill(plain.pool, plain.engine, source=LOGICAL_SOURCE, page_size=3, context="parents") as backfill:
            reports = backfill.run(WINDOW, jobs, concurrency=2)
        assert {report.status for report in reports} == {"verified"}
        assert checking.fingerprint(queued.engine, WINDOW) == checking.fingerprint(plain.engine, WINDOW)
        nothing_twice(queued.engine)
    finally:
        queued.close()
        plain.close()


def test_the_subject_that_waited_longest_gets_the_next_slice(yard: Yard) -> None:
    yard.plan(A)
    yard.plan(B)
    history_jobs.plan(yard.engine, kind="market", subject=MARKET, to_ledger=130, now=yard.wall())
    yard.worker = yard.new_worker("fair", slice_requests=1)
    for slice_number in range(1, 10):
        assert yard.worker.once() is True
        assert len(served(yard.fake)) == slice_number, "one slice, one subject"
        yard.wall.wait(1)
    # An account needs four slices here (one by stream), the market one: a
    # subject is never served twice before every subject that still waits
    # had its turn.
    assert served(yard.fake) == in_turn({A: 4, B: 4, MARKET: 1})
    assert history_jobs.counts(yard.engine)["done"] == 3
    assert yard.worker.once() is False


def test_the_budget_of_a_slice_is_kept_before_pages_and_not_inside_one(yard: Yard) -> None:
    """A page is stored whole with its context, or not at all. So the budget
    is kept before a page: a page costlier than the budget would else be
    taken again by every slice, to the same end."""
    spent = [0]
    limits = SliceLimits(requests=lambda: spent[0], max_requests=5)
    spent[0] = 7
    limits.check()  # inside a page: the page goes on, and is stored
    with pytest.raises(RunStopped, match="request limit"):
        limits.before_page()
    spent[0] = 4
    limits.before_page()

    yard.watch(D)
    for ledger in (101, 102, 103, 105, 106, 107, 109, 111, 113, 114, 115, 116, 117, 119, 120, 121, 122, 123, 124, 125):
        yard.fake.close(sale(ledger, offer_owner=D), latest=130)
    # D is a side of twenty-one trades without being in their operations: the
    # page of its trades costs forty-two requests for the parents of its records.
    yard.plan(D)
    yard.worker = yard.new_worker("costly", slice_requests=2)
    assert finish(yard.worker) == 4 and yard.job().state == DONE, "a page a slice, and the costly one no less"
    job = yard.job()
    assert job.requests == len(yard.fake.requests) > 40 and (job.slices, job.pages, job.records) == (4, 4, 22)
    assert verified(ranges_of(yard.engine, "account", D), 100, 130)
    nothing_twice(yard.engine)


def test_a_job_whose_boundary_grew_while_it_was_read_is_not_declared_done(yard: Yard) -> None:
    job, _ = yard.plan(A, to_ledger=120)

    def raise_boundary(path: str) -> None:
        yard.fake.before_answer = None
        history_jobs.extend_boundary(yard.engine, job.id, to_ledger=125, now=yard.wall())

    yard.fake.before_answer = raise_boundary
    assert yard.worker.once() is True
    assert (yard.job().state, yard.job().to_ledger, yard.job().boundary_final) == (WAITING, 125, True)
    assert yard.job().result is not None and yard.job().result["window"] == [100, 120]
    assert finish(yard.worker) == 1 and yard.job().state == DONE
    assert verified(ranges_of(yard.engine, "account", A), 100, 125)


def test_a_job_ahead_of_the_pool_waits_for_it_and_is_not_spent(yard: Yard) -> None:
    ahead, _ = yard.plan(A, to_ledger=131)
    bounded, _ = yard.plan(B, to_ledger=131, from_ledger=100)
    for _ in range(yard.worker.max_attempts + 1):
        assert yard.worker.once() is True and yard.worker.once() is True
        for job in (ahead, bounded):
            now = yard.job(job.id)
            assert (now.state, now.attempts) == (WAITING, 0), "the pool's being behind is not the job's failure"
            assert "ledger 130" in (now.last_error or "")
        yard.wall.wait(yard.worker.retry_cap + 1)
    assert yard.worker.failed == 0 and yard.rows("SELECT count(*) FROM history_transactions") == [(0,)]

    yard.fake.close(paid(131, A, B), latest=131)  # the pool catches up
    assert finish(yard.worker) == 2
    assert (yard.job(ahead.id).state, yard.job(bounded.id).state) == (DONE, DONE)
    assert yard.job(ahead.id).result is not None and yard.job(ahead.id).result["window"] == [100, 131]
    assert yard.job(bounded.id).result is not None and yard.job(bounded.id).result["window"] == [100, 131]


# -- workers, and what happens to them ------------------------------------------


def test_a_worker_that_dies_leaves_its_job_to_be_taken_again_until_the_tries_are_used(yard: Yard) -> None:
    job, _ = yard.plan(B)

    def take(worker: str) -> HistoryJob | None:
        return history_jobs.take(yard.engine, worker, lease_seconds=60, max_attempts=3, now=yard.wall())

    dead = take("dead")
    assert dead is not None and (dead.state, dead.leased_by, dead.slices) == (RUNNING, "dead", 1)
    assert dead.started_at == yard.wall()
    assert take("soon") is None, "the lease holds"

    yard.wall.wait(30)
    assert history_jobs.renew(yard.engine, job.id, "dead", lease_seconds=60, now=yard.wall()) is True
    assert history_jobs.renew(yard.engine, job.id, "another", lease_seconds=60, now=yard.wall()) is False
    yard.wall.wait(59)
    assert take("soon") is None, "a renewed lease holds"
    yard.wall.wait(2)
    again = take("alive")
    assert again is not None and (again.id, again.state, again.attempts, again.leased_by) == (
        job.id, RUNNING, 1, "alive"
    )
    assert "lease ran out" in (again.last_error or "")

    yard.wall.wait(61)
    second = take("gone-again")
    assert second is not None and second.attempts == 2
    yard.wall.wait(61)
    assert take("next") is None
    job = yard.job()
    assert (job.state, job.attempts, job.finished_at, job.leased_by) == (ERROR, 3, yard.wall(), None)
    assert "lease ran out" in (job.last_error or "")
    assert yard.count("history_transactions") == 0, "nothing was ever read; nothing is kept trying for"
    assert yard.worker.once() is False, "a job that gave up is left alone until a person says otherwise"

    assert history_jobs.retry(yard.engine, now=yard.wall()) == 1
    job = yard.job()
    assert (job.state, job.attempts, job.finished_at, job.next_attempt_at) == (WAITING, 0, None, yard.wall())
    assert history_jobs.retry(yard.engine, now=yard.wall()) == 0
    assert finish(yard.worker) == 1 and yard.job().state == DONE


def test_the_end_of_a_slice_that_is_no_longer_its_workers_writes_nothing_of_the_job(yard: Yard) -> None:
    job, _ = yard.plan(B)
    taken = history_jobs.take(yard.engine, "one", lease_seconds=60, now=yard.wall())
    assert taken is not None
    yard.wall.wait(61)
    other = history_jobs.take(yard.engine, "two", lease_seconds=60, now=yard.wall())
    assert other is not None and (other.state, other.leased_by, other.attempts) == (RUNNING, "two", 1)
    state = history_jobs.slice_done(
        yard.engine, taken, outcome=history_jobs.FINISHED, result={"window": [100, 130]}, requests=3, now=yard.wall()
    )
    assert state == RUNNING, "the job is the other worker's now: its state is not written over"
    job = yard.job()
    assert (job.state, job.leased_by, job.finished_at, job.requests) == (RUNNING, "two", None, 3), (
        "but what was asked for is counted"
    )


def test_a_job_paused_or_taken_away_under_a_running_slice_is_left_at_its_next_request(yard: Yard) -> None:
    job, _ = yard.plan(B)
    yard.worker = yard.new_worker("held", lease_seconds=0)  # the lease is made longer before every request
    yard.fake.before_answer = lambda path: history_jobs.pause(yard.engine, ids=[job.id], because="by hand")
    assert yard.worker.once() is True
    assert len(yard.fake.requests) == 1, (
        "the first request was answered, the job was paused, and nothing more was asked"
    )
    assert yard.worker.last is not None and yard.worker.last.outcome == "lost"
    current = yard.job()
    assert (current.state, current.paused_because, current.leased_by, current.slices) == (PAUSED, "by hand", None, 1)
    assert (current.requests, current.result) == (0, None), "a slice whose job was taken away writes nothing of the job"
    assert yard.worker.once() is False and yard.worker.failed == 1

    assert history_jobs.resume(yard.engine, ids=[job.id], now=yard.wall()) == 1

    def expire(path: str) -> None:
        yard.wall.wait(1)  # the lease, of no length, ran out while the slice was on its first request
        history_jobs.take(yard.engine, "another", lease_seconds=60, now=yard.wall())

    yard.fake.before_answer = expire
    assert yard.worker.once() is True
    assert len(yard.fake.requests) == 2 and yard.worker.last is not None and yard.worker.last.outcome == "lost"
    current = yard.job()
    assert (current.state, current.leased_by, current.attempts, current.requests) == (RUNNING, "another", 1, 0)
    assert yard.worker.failed == 2

    # Whoever holds the job goes on from where the slice was left: nothing is lost.
    yard.fake.before_answer = None
    yard.wall.wait(61)
    assert finish(yard.worker) >= 1 and yard.job().state == DONE
    nothing_twice(yard.engine)


def test_a_signal_in_the_middle_of_a_slice_loses_nothing_and_the_next_slice_goes_on_from_there(
    make_database: Callable[[], str],
) -> None:
    queued, plain = Yard(make_database(), FakeHorizon()), Yard(make_database(), FakeHorizon())
    try:
        queued.plan(B)
        # Pages of one record: a page that does not end its range leaves a checkpoint.
        queued.worker = queued.new_worker("sliced", page_size=1)
        stop = threading.Event()

        def signal(path: str) -> None:
            if len(queued.fake.requests) >= 6:
                stop.set()

        queued.fake.before_answer = signal
        assert queued.worker.once(stop=stop) is True
        job = queued.job()
        assert (job.state, job.attempts, job.leased_by, job.last_error) == (WAITING, 0, None, None)
        assert len(queued.fake.requests) == 6, "it stopped at the first request after the signal"
        assert queued.worker.failed == 0 and queued.worker.last is not None and queued.worker.last.outcome == "progress"
        between = ranges_of(queued.engine, "account", B)
        assert between["transactions"] == [("complete", 100, 104), ("empty", 105, 130)], "what was read stays read"
        assert between["operations"] == [("pending", 100, 104), ("empty", 105, 130)], (
            "and the page it was on is still to do"
        )
        assert between["effects"] == between["trades"] == [("pending", 100, 130)]
        kept = sorted(row[0] for row in queued.rows("SELECT cursor_ledger FROM sync_state WHERE direction = 'desc'"))
        assert kept == [100, 104], "checkpoints to go on from"
        queued.fake.before_answer = None

        assert finish(queued.worker) >= 1 and queued.job().state == DONE
        with Session(plain.engine) as session:
            jobs = [stream for stream in stream_jobs(session)[0] if stream.key.subject_key == B]
        with Backfill(plain.pool, plain.engine, source=LOGICAL_SOURCE, page_size=200, context="parents") as backfill:
            backfill.run(WINDOW, jobs)
        assert checking.fingerprint(queued.engine, WINDOW) == checking.fingerprint(plain.engine, WINDOW)
        nothing_twice(queued.engine)
    finally:
        queued.close()
        plain.close()


def test_stopping_before_the_slice_begins_puts_the_job_back_untouched(yard: Yard) -> None:
    yard.plan(B)
    stop = threading.Event()
    stop.set()
    assert yard.worker.once(stop=stop) is True
    job = yard.job()
    assert (job.state, job.attempts, job.leased_by, job.requests) == (WAITING, 0, None, 0)
    assert yard.fake.requests == []
    assert yard.worker.once() is True and yard.job().state == DONE


# -- what a slice could not read ------------------------------------------------


def test_a_stream_that_keeps_failing_makes_the_job_partial_and_names_it(yard: Yard) -> None:
    yard.fake.fail("/trades", 503, times=1000)
    yard.plan(B)
    for tries, wait in ((1, 0), (2, 31), (3, 61)):
        yard.wall.wait(wait)
        assert yard.worker.once() is True
        job = yard.job()
        assert job.last_error is not None and job.last_error.startswith(
            "trades: RequestFailed: Horizon answered 503 to GET "
        )
        assert fates(job) == dict.fromkeys(STREAMS, "verified") | {"trades": "failed"}
        assert "RequestFailed: Horizon answered 503" in job.result["streams"][f"account {B} / trades"]["error"]
        if tries < 3:
            assert (job.state, job.attempts) == (WAITING, tries)
            assert job.next_attempt_at == yard.wall() + timedelta(seconds=30 * 2 ** (tries - 1)), (
                "the wait grows with every try"
            )
        else:
            assert (job.state, job.attempts, job.finished_at) == (PARTIAL, 3, yard.wall())
    assert yard.worker.once() is False, "it waits for a person now"
    assert (yard.worker.failed, yard.worker.done) == (3, 0)
    assert (yard.count("history_transactions"), yard.count("history_trades")) == (2, 0)

    yard.fake.faults.clear()
    assert history_jobs.retry(yard.engine, now=yard.wall()) == 1 and yard.job().state == WAITING
    assert yard.worker.once() is True and yard.job().state == DONE
    assert yard.count("history_trades") == 2 and yard.worker.failed == 3
    assert verified(ranges_of(yard.engine, "account", B), 100, 130)


def test_a_subject_of_which_nothing_can_be_read_ends_in_error(yard: Yard) -> None:
    yard.fake.fail(f"/accounts/{A}/", 503, times=1000)
    yard.plan(A)
    for tries, wait in ((1, 0), (2, 31), (3, 61)):
        yard.wall.wait(wait)
        assert yard.worker.once() is True
        job = yard.job()
        assert job.state == (WAITING if tries < 3 else ERROR) and job.attempts == tries
        assert fates(job) == dict.fromkeys(STREAMS, "failed")
    assert yard.fake.faults, "the fault is still there"
    assert (job.finished_at, job.state) == (yard.wall(), ERROR)
    assert yard.worker.once() is False and yard.worker.failed == 3
    assert yard.count("history_transactions") == 0
    assert history_jobs.retry(yard.engine, ids=[job.id], now=yard.wall()) == 1 and yard.job().state == WAITING


def test_while_no_server_answers_the_job_waits_and_is_not_spent(make_database: Callable[[], str]) -> None:
    """Horizon is down for hours. The job is not to use up its tries on that:
    it waits, and when a server is back it is done without anyone's help."""
    yard = Yard(make_database(), FakeHorizon(down=httpx.ConnectError("refused")))
    try:
        yard.plan(B)
        for _ in range(yard.worker.max_attempts + 2):
            assert yard.worker.once() is True
            job = yard.job()
            assert (job.state, job.attempts) == (WAITING, 0), "an outage of the servers is not a failure of the job"
            assert job.next_attempt_at == yard.wall() + timedelta(seconds=min(yard.worker.retry_cap, DEFAULT_DEFER)), (
                "minutes, not the hour a failure may cost: a server that was down for a minute is asked again soon"
            )
            assert job.last_error is not None and job.last_error.startswith(
                "Unavailable: no server of the pool answers"
            )
            yard.wall.wait(3600)
            yard.clock.sleep(6)  # the server's cooldown is kept by the pool's own clock
        assert yard.worker.failed == 0
        yard.fake.down = None
        yard.wall.wait(3600)
        yard.clock.sleep(1000)  # its cooldown doubled all the while it was down; long past it now
        assert yard.worker.once() is True and yard.job().state == DONE
    finally:
        yard.close()


def test_a_rate_limited_pool_is_waited_for_and_not_counted(yard: Yard) -> None:
    """A 429 whose Retry-After is longer than a request waits ends the
    request where it stands: the server asking for time, not the job failing."""
    yard.fake.fail(f"/accounts/{B}/effects", 429, times=1000, retry_after="100000")
    yard.plan(B)
    for _ in range(yard.worker.max_attempts + 1):
        assert yard.worker.once() is True
        job = yard.job()
        assert (job.state, job.attempts) == (WAITING, 0)
        assert job.last_error is not None and job.last_error.startswith("effects: RateLimited: Horizon asks to wait")
        yard.wall.wait(yard.worker.retry_cap + 1)
    yard.fake.faults.clear()
    assert finish(yard.worker) >= 1 and yard.job().state == DONE and yard.worker.failed == 0


def test_an_account_switched_off_pauses_its_job_and_nothing_is_deleted(yard: Yard) -> None:
    yard.fake.close(latest=133)
    yard.plan(B, to_ledger=131)
    yard.worker = yard.new_worker("small", slice_requests=1)
    assert yard.worker.once() is True
    kept, asked = yard.count("history_transactions"), len(yard.fake.requests)
    assert kept > 0

    yard.switch(B, False)
    assert yard.worker.once() is True
    job = yard.job()
    assert (job.state, job.paused_because, job.leased_by) == (PAUSED, "account disabled", None)
    assert job.last_error == "account disabled: its history is not read"
    assert len(yard.fake.requests) == asked, "a switched-off account is asked for nothing"
    assert yard.worker.once() is False
    assert (yard.count("history_transactions"), yard.count("sync_coverage")) == (kept, 4), "the history stays"

    again, created = yard.plan(B, to_ledger=132)
    assert not created and (again.state, again.to_ledger) == (PAUSED, 132), "the boundary moves; the pause stays"
    again, _ = yard.plan(B, resume=True)
    assert again.state == WAITING, "planning with resume lifts it"
    assert yard.worker.once() is True and len(yard.fake.requests) == asked, (
        "and the worker, finding the account off, pauses it again"
    )
    assert (yard.job().state, yard.job().paused_because) == (PAUSED, "account disabled")

    yard.switch(B, True)
    again, _ = yard.plan(B)
    assert again.state == PAUSED, "planning alone does not lift it"
    again, _ = yard.plan(B, resume=True)
    job = yard.job()
    assert (job.state, job.attempts, job.paused_because) == (WAITING, 0, None)
    assert finish(yard.worker) >= 1 and yard.job().state == DONE
    assert yard.count("history_transactions") >= kept

    # A pause by hand is a person's decision: switching the account off and on
    # again does not lift it. Only resume does.
    assert history_jobs.extend_boundary(yard.engine, job.id, to_ledger=134, now=yard.wall()).state == WAITING
    assert history_jobs.pause(yard.engine, ids=[job.id], because="not now") == 1
    yard.switch(B, False)
    yard.switch(B, True)
    assert yard.plan(B, resume=True)[0].state == PAUSED
    assert history_jobs.resume(yard.engine, ids=[job.id], now=yard.wall()) == 1 and yard.job().state == WAITING


def test_a_market_job_runs_the_market_backfill(yard: Yard) -> None:
    job, created = history_jobs.plan(yard.engine, kind="market", subject=MARKET, to_ledger=130, now=yard.wall())
    assert created and job.state == WAITING
    with yard.engine.begin() as connection:
        connection.execute(text("UPDATE tracked_markets SET enabled = false"))
    assert yard.worker.once() is True
    assert (yard.job().state, yard.job().paused_because, yard.fake.requests) == (PAUSED, "market disabled", [])
    with yard.engine.begin() as connection:
        connection.execute(text("UPDATE tracked_markets SET enabled = true"))
    assert history_jobs.resume(yard.engine, ids=[job.id], now=yard.wall()) == 1

    yard.worker = yard.new_worker("market", slice_requests=1)
    assert finish(yard.worker) == 1 and yard.job().state == DONE
    job = yard.job()
    assert job.result is not None and job.result["window"] == [100, 130]
    assert fates(job) == {"trades": "verified"}
    assert ranges_of(yard.engine, "market", MARKET) == {"trades": [("complete", 100, 130)]}
    assert yard.rows("SELECT count(*) FROM history_trades")[0][0] == 2
    assert yard.rows("SELECT ledger_sequence FROM history_transactions ORDER BY 1") == [(104,), (112,)]


def test_what_lies_below_the_history_the_pool_keeps_is_reported_and_never_counted_as_read(yard: Yard) -> None:
    job, _ = yard.plan(B, from_ledger=90)
    assert finish(yard.worker) >= 1 and yard.job().state == DONE
    job = yard.job()
    assert job.result is not None and job.result["window"] == [90, 130]
    assert job.result["unavailable_ledgers"] == 10, "ten ledgers, not forty: one history is missing, not one a stream"
    assert fates(job) == dict.fromkeys(STREAMS, "verified")
    assert all(stream["coverage"]["unverifiable"] == 10 for stream in job.result["streams"].values())
    assert verified(ranges_of(yard.engine, "account", B), 100, 130)
    assert all(spans[0] == ("unverifiable", 90, 99) for spans in ranges_of(yard.engine, "account", B).values())


def test_an_empty_queue_costs_no_requests_and_nobody_is_polled(yard: Yard) -> None:
    yard.plan(A)
    history_jobs.pause(yard.engine, all=True, because="nothing is due")
    stop = threading.Event()
    thread = threading.Thread(target=yard.worker.run, args=(stop,), kwargs={"poll_seconds": 0.01})
    thread.start()
    time.sleep(0.3)
    stop.set()
    thread.join(30)
    assert not thread.is_alive()
    assert yard.fake.requests == [] and yard.pool.stats.requests == 0
    assert yard.worker.last is None and yard.job().slices == 0


def test_a_thousand_subjects_are_planned_once_and_served_in_turn(make_database: Callable[[], str]) -> None:
    """What a discovery pass leaves behind: a thousand accounts to import.
    What it costs to plan them, that none is planned twice, and that a
    hundred slices serve a hundred subjects."""
    yard = Yard(make_database(), FakeHorizon())
    try:
        subjects = [address(number) for number in range(100, 1100)]
        lists = watch_lists.parse(
            {
                "accounts": [
                    {"address": who, "enabled": True, "reason": "a thousand, for the measure"} for who in subjects
                ]
            },
            "test",
        )
        with Session(yard.engine) as session, session.begin():
            watch_lists.apply(session, lists)
        yard.fake.close(paid(129, A, *subjects), latest=130)

        started = time.monotonic()
        created = [yard.plan(subject)[1] for subject in subjects]
        planning = time.monotonic() - started
        assert all(created)
        started = time.monotonic()
        assert not any(yard.plan(subject)[1] for subject in subjects), "planned again, nothing is made"
        replanning = time.monotonic() - started
        assert yard.rows("SELECT count(*), count(DISTINCT (kind, subject)) FROM history_job") == [(1000, 1000)]

        yard.worker = yard.new_worker("thousand", slice_requests=1)
        started = time.monotonic()
        for _ in range(100):
            assert yard.worker.once() is True
        working = time.monotonic() - started
        turns = served(yard.fake)
        assert len(turns) == 100 and len(set(turns)) == 100, "a hundred slices, a hundred subjects, each once"
        assert turns == sorted(turns, key=subjects.index)[:100], (
            "never served before served; in the order they were planned"
        )
        # Of what a slice costs, the part that grows with the watch list: the
        # reader names the streams of every tracked subject, each slice.
        started = time.monotonic()
        with Session(yard.engine) as session:
            stream_jobs(session)
        listing = time.monotonic() - started
        size = yard.rows("SELECT pg_total_relation_size('history_job')")[0][0]
        print(
            f"\n1000 subjects: planning {planning:.2f}s ({planning:.1f} ms a job), planning again {replanning:.2f}s; "
            f"100 slices (a root and a page each) in {working:.2f}s ({working * 10:.0f} ms a slice, of which listing "
            f"the streams of all 1000 takes {listing * 1000:.1f} ms), {yard.pool.stats.requests} requests; "
            f"queue table {size // 1024} kB"
        )
    finally:
        yard.close()


# -- the commands ------------------------------------------------------------------


def commands(monkeypatch: pytest.MonkeyPatch, yard: Yard) -> None:
    """The commands read the fake, as they would read real servers, and ask
    it at once: the fake's retries do not sleep for real."""
    from app import cli_common

    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", yard.engine.url.render_as_string(hide_password=False))
    serve(monkeypatch, yard.fake)
    monkeypatch.setattr(cli_common, "HorizonSettings", lambda **options: HorizonSettings(**{**SETTINGS, **options}))


def test_enqueue_pause_resume_retry_once_and_status_as_commands(
    make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    yard = Yard(make_database(), FakeHorizon())
    try:
        commands(monkeypatch, yard)
        ok, failed = history_cli.OK, history_cli.FAILED

        assert history_cli.main(["enqueue", "--account", B, "--to-ledger", "130"]) == ok
        out = capsys.readouterr().out
        assert "planned job 1: account" in out and "up to ledger 130, waiting, context parents" in out
        assert "events are not guaranteed above ledger 130" in out
        assert yard.fake.requests == [], "with --to-ledger no request is made"

        assert history_cli.main(["status"]) == ok
        out = capsys.readouterr().out
        assert "queue: 1 waiting, 0 running, 0 done, 0 partial, 0 error, 0 paused" in out
        assert "unverified range: job 1 (account" in out and "events are not guaranteed above ledger 130" in out

        assert history_cli.main(["pause", "--account", B, "--because", "not now"]) == ok
        assert "1 job(s) paused: not now" in capsys.readouterr().out
        assert (yard.job().state, yard.job().paused_because) == (PAUSED, "not now")
        assert history_cli.main(["once"]) == ok and "nothing to do" in capsys.readouterr().out
        assert history_cli.main(["status"]) == ok
        assert "not now" in capsys.readouterr().out
        assert history_cli.main(["resume", "--id", "1"]) == ok
        assert "1 job(s) back in the queue" in capsys.readouterr().out
        assert yard.job().state == WAITING

        yard.fake.fail("/trades", 503, times=1000)
        once = ["once", "--slice-requests", "200", "--max-attempts", "3", "--retry-base", "0"]
        assert history_cli.main(once) == failed
        out = capsys.readouterr().out
        assert "job 1 (account" in out and ": failed, the job is waiting" in out
        assert "trades: RequestFailed: Horizon answered 503" in out
        assert history_cli.main(["status"]) == ok
        out = capsys.readouterr().out
        assert "1 waiting" in out and "waiting for another try" in out
        assert "trades: RequestFailed: Horizon answered 503" in out
        for _ in range(2):  # three tries in all, then it gives up
            assert history_cli.main(once) == failed
        assert (yard.job().state, yard.job().attempts) == (PARTIAL, 3)
        assert history_cli.main(["status"]) == ok
        out = capsys.readouterr().out
        assert "queue: 0 waiting, 0 running, 0 done, 1 partial, 0 error, 0 paused" in out
        said = "trades not covered (RequestFailed: Horizon answered 503); transactions, operations, effects verified"
        assert said in out, out
        assert "0 ledger(s)" not in out, "nothing is said to be unavailable when nothing is"
        assert "unverified range: job 1 (account" in out, "and the range above the boundary is still not guaranteed"

        assert history_cli.main(["retry"]) == ok and "1 job(s) back in the queue" in capsys.readouterr().out
        yard.fake.faults.clear()
        assert history_cli.main(once) == ok and "finished, the job is done" in capsys.readouterr().out
        assert history_cli.main(["status"]) == ok
        out = capsys.readouterr().out
        assert "queue: 0 waiting, 0 running, 1 done, 0 partial, 0 error, 0 paused" in out
        assert "unverified range: job 1 (account" in out, "done up to a boundary that is not final: said so"
        assert history_cli.main(once) == ok and "nothing to do" in capsys.readouterr().out
    finally:
        yard.close()


def test_what_the_commands_are_told_to_plan_is_checked_and_said(
    make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    yard = Yard(make_database(), FakeHorizon())
    try:
        commands(monkeypatch, yard)
        ok, failed = history_cli.OK, history_cli.FAILED

        # Without --to-ledger the pool is asked, once, what its latest ledger is.
        assert history_cli.main(["enqueue", "--account", A]) == ok
        out = capsys.readouterr().out
        assert "planned job 1" in out and "up to ledger 130" in out
        assert [path for path, _ in yard.fake.requests] == ["/"]
        assert history_cli.main(["enqueue", "--market", RECT, EUR, "--to-ledger", "128", "--from-ledger", "105"]) == ok
        out = capsys.readouterr().out
        assert "planned job 2: market" in out and "from ledger 105 up to ledger 128" in out
        assert history_cli.main(["enqueue", "--account", A, "--to-ledger", "128", "--from-ledger", "105"]) == ok
        out = capsys.readouterr().out
        assert "already planned job 1: account" in out and "as far back as the pool keeps up to ledger 130" in out
        assert len(yard.fake.requests) == 1, "a ledger given: nothing more is asked"
        assert [job.subject for job in history_jobs.list(yard.engine)] == [A, MARKET]

        # What is not watched, or not what it says it is, is refused and planned as nothing.
        for command, said in (
            (["enqueue", "--account", C, "--to-ledger", "130"], "switched off"),
            (["enqueue", "--account", D, "--to-ledger", "130"], "not watched"),
            (["enqueue", "--account", "GNOTANADDRESS", "--to-ledger", "130"], "not an account address"),
            (
                ["enqueue", "--account", A, "--to-ledger", "120", "--from-ledger", "125"],
                "from_ledger 125 is above to_ledger 120",
            ),
            (["enqueue", "--market", "native", "native", "--to-ledger", "130"], "two different assets"),
            (["enqueue", "--market", "MTL", ISSUER, "--to-ledger", "130"], "is neither 'native' nor 'CODE:ISSUER'"),
        ):
            assert history_cli.main(command) == failed
            assert said in capsys.readouterr().err
        with pytest.raises(SystemExit) as stopped:  # said by the parser, not by a job
            history_cli.main(["enqueue", "--to-ledger", "130"])
        assert stopped.value.code == 2 and "--account" in capsys.readouterr().err
        assert len(history_jobs.list(yard.engine)) == 2

        assert history_cli.main(["pause", "--all", "--because", "maintenance"]) == ok
        assert "2 job(s) paused: maintenance" in capsys.readouterr().out
        assert history_jobs.counts(yard.engine)["paused"] == 2
        assert history_cli.main(["pause", "--market", EUR, RECT, "--because", "still not"]) == ok
        assert (yard.job(2).paused_because, yard.job(1).paused_because) == ("still not", "maintenance")
        assert history_cli.main(["resume", "--all"]) == ok and "2 job(s) back in the queue" in capsys.readouterr().out
        assert history_cli.main(["resume", "--all"]) == ok and "0 job(s) back in the queue" in capsys.readouterr().out
        assert history_jobs.counts(yard.engine)["waiting"] == 2

        # The context a job is planned with is the context it is read with:
        # full loads the whole context of every transaction met.
        assert history_cli.main(["enqueue", "--account", A, "--to-ledger", "130", "--context", "full"]) == ok
        assert history_cli.main(["enqueue", "--market", RECT, EUR, "--to-ledger", "130", "--context", "full"]) == ok
        assert "context parents" in capsys.readouterr().out, "a job keeps the context it was planned with"
        assert {job.subject: job.context for job in history_jobs.list(yard.engine)} == {A: "parents", MARKET: "parents"}
        assert history_cli.main(["enqueue", "--account", B, "--to-ledger", "130", "--context", "full"]) == ok
        planned = {job.subject: job.context for job in history_jobs.list(yard.engine)}
        assert planned[B] == "full" and Settings.from_env().history_context == "parents"
        for _ in range(3):
            assert history_cli.main(["once", "--slice-requests", "200"]) == ok
        assert history_jobs.counts(yard.engine)["done"] == 3
        loaded = yard.rows(
            "SELECT ledger_sequence FROM history_transactions WHERE cardinality(context_loaded) > 0 ORDER BY 1"
        )
        assert [row[0] for row in loaded] == [100, 104], "the transactions met by the account planned with full"
        others = "ledger_sequence IN (108, 110) AND cardinality(context_loaded) > 0"
        assert yard.rows(f"SELECT count(*) FROM history_transactions WHERE {others}") == [(0,)]
    finally:
        yard.close()
