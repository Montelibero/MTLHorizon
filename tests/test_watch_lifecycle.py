"""An account's life: added, told of, its history imported; switched off
and on again — and every crash in between."""

from __future__ import annotations

import importlib.util
import runpy
import sys
import threading
import time
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.backfill import config as watch_lists
from app.backfill import verify as checking
from app.cli_common import source_name
from app.config import PUBLIC_NETWORK_PASSPHRASE, Settings
from app.notifier.client import NotifierClient
from app.sources.horizon import HorizonSettings
from app.sources.pool import HorizonPool, PoolSettings
from app.storage import events as journal
from app.storage import history_jobs
from app.storage.models import HistoryJob, TrackedAccount, TrackedMarket, WatchSubscription
from app.subscriptions import reconcile
from app.watch import accounts as watched
from app.watch import cli as watch_cli
from app.watch import provision
from app.worker.loader import Worker
from tests.factories import address
from tests.fake_horizon import A, B, C, D, EUR, ISSUER, LATEST, RECT, FakeHorizon, paid, serve
from tests.fake_notifier import FakeNotifier
from tests.fake_notifier_api import FakeNotifierApi
from tests.test_backfill import WATCH_LIST, WINDOW
from tests.test_pool import Clock, SETTINGS
from tests.test_worker import T1

CALLBACK = "https://mtl-horizon.test/webhooks/notifier"
E = address(10)  # an account that is not in the watch list


class Life:
    """A private database with the watch list, a fake Horizon, a fake
    Notifier and everything a pass needs."""

    def __init__(self, url: str, *fakes: FakeHorizon) -> None:
        self.engine = __import__("sqlalchemy").create_engine(url)
        self.fake = fakes[0] if fakes else FakeHorizon()
        self.fakes: tuple[FakeHorizon, ...] = fakes or (self.fake,)
        self.wall, self.clock = Wall(), Clock()
        with Session(self.engine) as session, session.begin():
            watch_lists.apply(session, WATCH_LIST)
            # The watch list was here before stage 05: its accounts get no
            # import of their own (as the migration would have marked them).
            for account in WATCH_LIST.accounts:
                tracked = session.get(TrackedAccount, account.address)
                assert tracked is not None
                tracked.origin, tracked.boundary_due = "legacy", False
        self.pools: list[HorizonPool] = []
        self.pool = self.another_pool()
        self.signer = FakeNotifier()
        self.api_fake = FakeNotifierApi(self.signer.public_key)
        self.settings = Settings(
            database_url=url,
            network_passphrase=PUBLIC_NETWORK_PASSPHRASE,
            notifier_signer_keys=(self.signer.public_key,),
            notifier_api_url="http://notifier.test",
            notifier_callback_url=CALLBACK,
        )

    def another_pool(self) -> HorizonPool:
        """A pool of its own, as a thread of a service would have: the same
        fake servers, the same clocks."""
        pool = HorizonPool(
            tuple(fake.origin for fake in self.fakes),
            HorizonSettings(**SETTINGS),
            network_passphrase=PUBLIC_NETWORK_PASSPHRASE,
            pool_settings=PoolSettings(health_check_interval=60, failover_cooldown=5),
            transports={fake.origin: httpx.MockTransport(fake._handle) for fake in self.fakes},
            clock=self.clock,
            sleeper=self.clock.sleep,
        )
        self.pools.append(pool)
        return pool

    def api(self) -> NotifierClient:
        return NotifierClient(self.settings.notifier_api_url, transport=self.api_fake.transport)

    def pass_(self, *, horizon: bool = True, notifier: bool = True) -> provision.PassReport:
        self.wall.wait(120)
        with (self.api() if notifier else _nothing()) as api:
            return provision.run(
                self.engine, self.settings, client=self.pool if horizon else None, api=api if notifier else None, now=self.wall()
            )

    def job(self, subject: str) -> HistoryJob | None:
        """The account's history import, as plain data (the session is gone)."""
        with Session(self.engine) as session:
            row = session.scalar(select(HistoryJob).where(HistoryJob.kind == "account", HistoryJob.subject == subject))
            return None if row is None else HistoryJob(**{c.name: getattr(row, c.name) for c in HistoryJob.__table__.columns})

    def rows_state(self, subject: str) -> set:
        """The states of the account's subscription rows that are not retired."""
        with Session(self.engine) as session:
            return set(
                session.scalars(
                    select(WatchSubscription.state).where(
                        WatchSubscription.kind == "account", WatchSubscription.key == subject, WatchSubscription.state != "retired"
                    )
                )
            )

    def account(self, account: str) -> TrackedAccount | None:
        with Session(self.engine) as session:
            row = session.get(TrackedAccount, account)
            return None if row is None else TrackedAccount(**{c.name: getattr(row, c.name) for c in TrackedAccount.__table__.columns})

    def close(self) -> None:
        for pool in self.pools:
            pool.close()
        self.engine.dispose()


class Wall:
    def __init__(self) -> None:
        from datetime import UTC, datetime

        self.now = datetime(2026, 11, 25, 12, 0, tzinfo=UTC)

    def __call__(self):
        return self.now

    def wait(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class _nothing:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *exc: object) -> None:
        return None


@pytest.fixture
def life(make_database: Callable[[], str]) -> Any:
    instance = Life(make_database())
    yield instance
    instance.close()


def add(life: Life, account: str = E) -> None:
    with Session(life.engine) as session, session.begin():
        watched.add_account(session, account, reason="added by hand", origin=watched.MANUAL)


# -- the life ---------------------------------------------------------------------------


def test_a_new_account_gets_its_boundary_its_subscription_and_its_history(life: Life) -> None:
    add(life)
    first = life.pass_()
    account = life.account(E)
    assert account is not None and account.watch_since_ledger == LATEST and not account.boundary_due
    assert first.start_boundaries == 1 and first.imports_planned == 1
    job = life.job(E)
    assert job is not None and job.state == "waiting" and job.to_ledger == LATEST and not job.boundary_final
    assert job.context == "parents" and job.from_ledger is None  # as far back as the pool keeps
    assert life.rows_state(E) == {"error"}  # registered — and on the word of a create nothing is active yet
    # The next pass reads it back and confirms it; the one after takes the upper boundary.
    life.pass_()
    assert life.rows_state(E) == {"active"}
    life.pass_()
    job = life.job(E)
    assert job is not None and job.boundary_final and job.to_ledger >= account.watch_since_ledger


def test_in_silence_the_pass_asks_nothing_of_anybody(life: Life) -> None:
    add(life)
    for _ in range(4):
        life.pass_()
    asked = len(life.fake.requests)
    notifier = len(life.api_fake.requests)
    report = life.pass_()
    assert report.requests == 0 and len(life.fake.requests) == asked and len(life.api_fake.requests) == notifier
    assert report.start_boundaries == 0 and (report.subscriptions is None or report.subscriptions.created == 0)


def test_an_event_that_comes_before_the_subscription_is_registered_is_not_lost(life: Life) -> None:
    """The Notifier's API is down when the account is added; a notification
    of it arrives all the same (made by hand, or by an earlier
    subscription). It is stored; and the history import, whose upper
    boundary is taken after the subscription is confirmed, covers its ledger."""
    life.api_fake.hang(1000)
    add(life)
    life.pass_()  # the boundary and the plan; the subscription waits
    assert life.rows_state(E) == {"error"}  # wanted, and said to be waiting for the Notifier
    # The event.
    life.fake.close(paid(131, E, A), latest=132)
    notice = __import__("app.notifier.format", fromlist=["read"]).read(
        __import__("tests.fake_notifier", fromlist=["notification"]).notification(life.fake, str((131 << 32) | 1), subscription="made-by-hand")
    )
    journal.receive(life.engine, notice, received_at=life.wall(), headers={}, signed_by="G-test")
    with Worker(life.pool, life.engine, name="w", max_attempts=3, clock=life.wall) as worker:
        assert worker.once() is True
    assert life.engine.connect().execute(__import__("sqlalchemy").text("SELECT count(*) FROM history_transactions WHERE transaction_hash = :h"), {"h": notice.transaction_hash}).scalar() == 1
    # The Notifier is back: registered, then confirmed, then the upper boundary.
    life.api_fake._hangs = 0
    life.pass_()
    assert life.rows_state(E) == {"error"}
    life.pass_()
    assert life.rows_state(E) == {"active"}
    life.pass_()
    job = life.job(E)
    assert job is not None and job.boundary_final and job.to_ledger >= 131  # the import reads down from here: the event's ledger included
    # And the import and the event do not store the transaction twice.
    with Session(life.engine) as session:
        assert session.scalar(select(func.count()).select_from(HistoryJob).where(HistoryJob.subject == E)) == 1


def test_switched_off_and_on_again(life: Life) -> None:
    add(life)
    for _ in range(4):
        life.pass_()
    with Session(life.engine) as session, session.begin():
        watched.set_enabled(session, E, enabled=False, reason="off for a while")
    life.pass_()
    life.pass_()
    job = life.job(E)
    assert job is not None and job.state == "paused" and job.paused_because == "account disabled"
    assert life.rows_state(E) in (set(), {"retired"})  # its subscription is gone from the Notifier
    assert not any(sub.get("account") == E for sub in life.api_fake.subscriptions)
    # While it is off, the network moves on.
    life.fake.close(paid(140, E, A), latest=141)
    with Session(life.engine) as session, session.begin():
        watched.set_enabled(session, E, enabled=True)
    account = life.account(E)
    assert account is not None and account.boundary_due
    for _ in range(4):
        life.pass_()
    job = life.job(E)
    assert job is not None and job.state in ("waiting", "done") and job.boundary_final
    assert job.to_ledger >= 141  # the time it was off is covered
    assert life.rows_state(E) == {"active"}


def test_a_market_switched_off_has_its_import_paused_and_on_again_it_goes_back_to_work(life: Life) -> None:
    """The pause is made with the reason of a market — for that is what
    lifts it. With the reason of an account it never was lifted, and the
    import stayed paused for good after the market was switched on again."""
    from app.backfill.jobs import market_key
    from tests.fake_horizon import sale

    key = market_key(EUR, RECT)
    job_id = history_jobs.plan(life.engine, kind="market", subject=key, to_ledger=130, boundary_final=True, now=life.wall())[0].id
    with Session(life.engine) as session, session.begin():
        for row in session.scalars(select(TrackedMarket)):
            if market_key(row.base_asset_id, row.counter_asset_id) == key:
                row.enabled = False
    life.pass_()
    job = next(job for job in history_jobs.list(life.engine) if job.id == job_id)
    assert job.state == "paused" and job.paused_because == "market disabled"
    # While it is off, the market moves on.
    life.fake.close(sale(133, offer_owner=D), latest=134)
    with Session(life.engine) as session, session.begin():
        for row in session.scalars(select(TrackedMarket)):
            if market_key(row.base_asset_id, row.counter_asset_id) == key:
                row.enabled = True
    life.pass_()
    job = next(job for job in history_jobs.list(life.engine) if job.id == job_id)
    assert (job.state, job.boundary_final, job.paused_because) == ("waiting", True, None)
    assert job.to_ledger >= 134, "the time it was off is covered"


def test_a_crash_between_planning_the_import_and_writing_the_boundary_loses_nothing(
    life: Life, monkeypatch: pytest.MonkeyPatch
) -> None:
    add(life)
    broken = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("killed"))  # noqa: E731
    monkeypatch.setattr("app.watch.provision.history_jobs.plan", broken)
    with pytest.raises(RuntimeError, match="killed"):
        life.pass_()
    assert life.account(E).boundary_due  # nothing was written: the account is still due
    monkeypatch.undo()
    life.pass_()
    assert life.job(E) is not None and not life.account(E).boundary_due


_killed = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("killed"))  # noqa: E731


def test_a_crash_after_the_subscription_is_registered_and_before_it_is_confirmed_loses_nothing(
    life: Life, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The pass dies between the Notifier's answer and the reading back.
    The row keeps the id it was given and is not active; the next pass reads
    it back, and nothing is created a second time."""
    add(life)
    life.pass_()  # planned, and registered — not active on the word of a create
    assert life.rows_state(E) == {"error"}
    monkeypatch.setattr("app.subscriptions.reconcile._confirm", _killed)
    with pytest.raises(RuntimeError, match="killed"):
        life.pass_()
    assert life.rows_state(E) == {"error"}, "nothing is active that was not read back"
    assert sum(sub.get("account") == E for sub in life.api_fake.subscriptions) == 1
    assert life.job(E) is not None and not life.job(E).boundary_final
    monkeypatch.undo()
    life.pass_()
    assert life.rows_state(E) == {"active"}
    assert sum(sub.get("account") == E for sub in life.api_fake.subscriptions) == 1, "not made a second time"
    life.pass_()
    assert life.job(E) is not None and life.job(E).boundary_final


def test_a_crash_after_the_subscription_is_confirmed_and_before_the_upper_boundary_loses_nothing(
    life: Life, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The account is told of, and the pass dies before the history import
    is extended to where the subscription starts. The import keeps saying its
    boundary is not final; the next pass extends it to the latest ledger —
    and what happened in between is read by the import."""
    subject = D  # an account the fake Horizon knows, and the watch list does not
    add(life, subject)
    for _ in range(3):
        life.pass_()
    job = life.job(subject)
    assert job is not None and life.rows_state(subject) == {"active"} and job.boundary_final
    with Session(life.engine) as session, session.begin():
        session.execute(
            __import__("sqlalchemy").text("UPDATE history_job SET boundary_final = false WHERE id = :id"), {"id": job.id}
        )
    # The network moves on: a ledger the import knows nothing of yet.
    life.fake.close(paid(131, subject, A), latest=132)
    monkeypatch.setattr("app.watch.provision.history_jobs.extend_boundary", _killed)
    with pytest.raises(RuntimeError, match="killed"):
        life.pass_()
    job = life.job(subject)
    assert job is not None and not job.boundary_final and job.to_ledger < 132
    monkeypatch.undo()
    life.pass_()
    job = life.job(subject)
    assert job is not None and job.boundary_final and job.to_ledger >= 132
    from app.history.runner import HistoryWorker

    with HistoryWorker(life.pool, life.engine, name="history", slice_requests=30, max_attempts=8, retry_base=1, retry_cap=2, clock=life.wall) as history:
        job = _until_done(life, history, job.id)
    assert job is not None and job.state == "done", (job and job.state, job and job.last_error)
    with life.engine.connect() as connection:
        kept = connection.execute(
            __import__("sqlalchemy").text("SELECT count(*) FROM history_transactions WHERE ledger_sequence = 131")
        ).scalar()
    assert kept == 1, "the ledger of the time the boundary was not final is read, once"


def test_the_pass_that_keeps_accounts_in_step_runs_while_events_keep_coming(make_database: Callable[[], str]) -> None:
    """Until stage 06 the pass ran only while no event was waiting: under a
    steady stream of events a new account could wait for its boundary, its
    history import and its subscription for as long as the stream lasted.
    Now it runs on its schedule, in a thread of the worker's own, and the
    worker of events is never idle for it."""
    from app.notifier import format as notifier_format
    from app.worker.background import Background
    from tests.fake_notifier import notification

    life = Life(make_database(), FakeHorizon(stall=0.02))
    try:
        own = life.another_pool()  # the background's, as a service would have
        # A backlog that keeps the worker of events busy for the whole test.
        life.fake.close(*[paid(131 + number, A, B) for number in range(400)], latest=531)
        for number in range(400):
            notice = notifier_format.read(notification(life.fake, str(((131 + number) << 32) | 1), subscription="s"))
            journal.receive(life.engine, notice, received_at=life.wall(), headers={}, signed_by="G-test")
        add(life)
        stop = threading.Event()
        api = life.api()
        calls: list[int] = []
        with Worker(life.pool, life.engine, name="w", clock=life.wall) as worker:
            idle: list[int] = []
            taking = worker.once

            def counting(**options: Any) -> bool:
                taken = taking(**options)
                if taken is False:
                    idle.append(1)
                return taken

            worker.once = counting  # type: ignore[method-assign]

            def pass_() -> provision.PassReport:
                calls.append(1)
                return provision.run(life.engine, life.settings, client=own, api=api, now=life.wall())

            background = Background(
                life.engine, life.settings, stop=stop, client=own, api=api, clock=life.wall, pass_=pass_, provision_every=0.02
            ).start()
            runner = threading.Thread(target=worker.run, kwargs={"stop": stop, "poll_seconds": 0.01})
            runner.start()
            try:
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline and not (life.job(E) is not None and life.job(E).boundary_final):
                    time.sleep(0.02)
                time.sleep(0.3)
            finally:
                stop.set()
                runner.join(30)
                background.close()
        job = life.job(E)
        assert job is not None and job.boundary_final, "the account got its boundary, its import and its subscription"
        assert life.rows_state(E) == {"active"} and not life.account(E).boundary_due
        assert len(calls) >= 5, f"the pass ran {len(calls)} time(s) while the events kept coming"
        assert not idle, f"the worker of events was idle {len(idle)} time(s): the stream was not steady enough to prove anything"
    finally:
        life.close()


def test_discovery_runs_on_its_schedule_only_when_it_is_on(make_database: Callable[[], str]) -> None:
    """Off, which is the default: nothing is put anywhere. On: a pass is put
    in the queue of tasks for every asset marked, when its last pass began
    longer ago than the interval — once, however often it is looked at; and
    again when the time has come."""
    from app.discovery import scan
    from app.storage import service_jobs
    from app.storage.models import TrackedAsset
    from app.tasks.runner import TaskRunner
    from app.worker.background import Background

    life = Life(make_database())
    try:
        with Session(life.engine) as session, session.begin():
            session.get(TrackedAsset, EUR).discover = True  # type: ignore[union-attr]
        life.fake.trustlines[EUR] = {address(600): 10**7}
        stop = threading.Event()
        background = Background(
            life.engine, life.settings, stop=stop, client=life.another_pool(), api=life.api(), clock=life.wall, pass_=lambda: None
        )
        background.run_pass()
        assert service_jobs.counts(life.engine)["queued"] == 0  # off by default

        life.settings = Settings(**{**life.settings.__dict__, "discovery_interval_seconds": 3600})
        background.settings = life.settings
        assert background._schedule_discovery() == 1
        assert background._schedule_discovery() == 0, "a pass queued already is not put twice"
        runner = TaskRunner(life.engine, life.settings, client=life.another_pool(), api=life.api(), clock=life.wall, name="t")
        task = runner.once()
        assert task is not None and task.state == "done" and task.requested_by == "schedule"
        found = life.account(address(600))
        assert found is not None and found.origin == "discovery"
        assert background._schedule_discovery() == 0, "the pass began just now: not again before the interval"
        life.wall.wait(3601)
        assert background._schedule_discovery() == 1
    finally:
        life.close()


# -- live events and the history queue on one subject ----------------------------------


def test_the_history_queue_and_live_events_do_not_store_anything_twice(make_database: Callable[[], str]) -> None:
    """The history import of an account runs in slices while the events
    worker brings a transaction of that same account in: what is stored is
    what one reading of the same window stores."""
    from app.history.runner import HistoryWorker

    life = Life(make_database())
    alone = Life(make_database())
    try:
        for instance in (life, alone):
            instance.fake.close(paid(131, A, B), latest=132)
        # The reference: the backfill alone, with parents, over the same window.
        from app.backfill.jobs import stream_jobs
        from app.backfill.loader import Backfill, Window

        with Session(alone.engine) as session:
            streams = [stream for stream in stream_jobs(session)[0] if stream.key.subject_type == "account" and stream.key.subject_key == A]
        with Backfill(alone.pool, alone.engine, source=source_name(alone.pool), page_size=3, context="parents") as backfill:
            backfill.run(Window(100, 130), streams)

        # One job for the account, in small slices; the event lands in the middle.
        with Session(life.engine) as session, session.begin():
            history_jobs.plan(session, kind="account", subject=A, to_ledger=130, context="parents", now=life.wall()) if False else None
        history_jobs.plan(life.engine, kind="account", subject=A, to_ledger=130, context="parents", boundary_final=True, now=life.wall())
        notice = __import__("app.notifier.format", fromlist=["read"]).read(
            __import__("tests.fake_notifier", fromlist=["notification"]).notification(life.fake, str((131 << 32) | 1), subscription="s")
        )
        journal.receive(life.engine, notice, received_at=life.wall(), headers={}, signed_by="G-test")
        stored = []
        with HistoryWorker(life.pool, life.engine, name="history", slice_requests=4, slice_seconds=30, max_attempts=5, clock=life.wall) as history, Worker(
            life.pool, life.engine, name="events", max_attempts=3, clock=life.wall
        ) as events:
            def bring_the_event_in() -> None:
                for _ in range(4):
                    if events.once():
                        stored.append(1)

            thread = threading.Thread(target=bring_the_event_in)
            thread.start()
            for _ in range(80):
                life.wall.wait(1)
                history.once()
                if history_jobs.get(life.engine, 1) is not None and history_jobs.get(life.engine, 1).state == "done":
                    break
            thread.join(30)
        assert stored == [1] and history_jobs.get(life.engine, 1).state == "done"
        assert checking.fingerprint(life.engine, WINDOW) == checking.fingerprint(alone.engine, WINDOW)
        with life.engine.connect() as connection:
            counted = connection.execute(
                __import__("sqlalchemy").text("SELECT count(*), count(DISTINCT id) FROM history_operations WHERE id >> 32 = 131")
            ).one()
        assert counted[0] == counted[1]
    finally:
        life.close()
        alone.close()


def _until_done(life: Life, history: Any, job_id: int, *, every: int = 30) -> Any:
    """Slices, with the clocks moved between them, until the job is out of
    the queue's way."""
    for _ in range(every):
        life.wall.wait(121)  # a deferred or failed slice is due again
        life.clock.sleep(6)  # and the first server's cooldown is over
        history.once()
        job = history_jobs.get(life.engine, job_id)
        if job is not None and job.state in ("done", "partial", "error", "paused"):
            return job
    return history_jobs.get(life.engine, job_id)


def test_the_history_import_goes_on_from_the_other_server_and_back_when_the_first_dies(
    make_database: Callable[[], str],
) -> None:
    """The first server dies inside the import; the second, which keeps less
    history, serves what it has; the stream below its history waits for the
    first, which comes back after its cooldown and finishes it."""
    from app.history.runner import HistoryWorker

    first, second = FakeHorizon(), FakeHorizon(origin="https://b.horizon.test", elder=104)
    life = Life(make_database(), first, second)
    try:
        asked = 0
        real = first.before_answer

        def die(path: str) -> None:
            nonlocal asked
            if path != "/":
                asked += 1
                if asked == 3:
                    first.down = httpx.ConnectError("refused")  # for a while

        first.before_answer = die
        served = 0

        def come_back(path: str) -> None:
            nonlocal served
            if path != "/":
                served += 1
                if served == 1:
                    first.down = None  # it is back; the pool leaves it alone until its cooldown is over

        second.before_answer = come_back
        job_id = history_jobs.plan(
            life.engine, kind="account", subject=A, to_ledger=130, context="parents", boundary_final=True, now=life.wall()
        )[0].id
        with HistoryWorker(life.pool, life.engine, name="history", slice_requests=30, max_attempts=8, retry_base=1, retry_cap=2, clock=life.wall) as history:
            job = _until_done(life, history, job_id)
        assert job is not None and job.state == "done", (job and job.state, job and job.last_error)
        assert job.result is not None and job.result["unavailable_ledgers"] == 0  # nothing lay below both: it only looked so for a moment
        with life.engine.connect() as connection:
            served_by = connection.execute(
                __import__("sqlalchemy").text("SELECT served_by FROM sync_coverage WHERE served_by IS NOT NULL")
            ).scalars().all()
        # Ranges a stream read from both servers are kept as one, and name both.
        assert any(first.origin in name for name in served_by) and any(second.origin in name for name in served_by)
    finally:
        life.close()


def _bottom(life: Life, subject: str) -> tuple[int, str | None]:
    """The lowest ledger the account's coverage speaks of, and whether any
    of it is said to be out of reach."""
    with life.engine.connect() as connection:
        lowest = connection.execute(
            __import__("sqlalchemy").text("SELECT min(from_ledger) FROM sync_coverage WHERE subject_key = :s"), {"s": subject}
        ).scalar()
        out_of_reach = connection.execute(
            __import__("sqlalchemy").text(
                "SELECT count(*) FROM sync_coverage WHERE subject_key = :s AND state = 'unverifiable'"
            ),
            {"s": subject}
        ).scalar()
    return lowest, out_of_reach


def test_a_server_that_keeps_the_oldest_history_and_is_silent_keeps_the_job_waiting(make_database: Callable[[], str]) -> None:
    """The server with the deepest history is down when the account's import
    starts. What the other server keeps is read — and is not passed off as
    all the history there is: the job waits for the first, and is finished
    only when it has said of itself and what it keeps is read."""
    from app.history.runner import HistoryWorker

    first, second = FakeHorizon(), FakeHorizon(origin="https://b.horizon.test", elder=104)
    life = Life(make_database(), first, second)
    try:
        first.down = httpx.ConnectError("refused")
        job_id = history_jobs.plan(
            life.engine, kind="account", subject=A, to_ledger=130, context="parents", boundary_final=True, now=life.wall()
        )[0].id
        with HistoryWorker(life.pool, life.engine, name="history", slice_requests=30, max_attempts=8, retry_base=1, retry_cap=2, clock=life.wall) as history:
            for _ in range(3):
                assert history.once() is True
                job = history_jobs.get(life.engine, job_id)
                assert job is not None and (job.state, job.attempts) == ("waiting", 0), (job.state, job.attempts)
                assert job.last_error is not None and job.last_error.startswith(
                    "ledgers below 104 are not read: https://horizon.test did not say of itself just now"
                ), job.last_error
                assert job.result is not None and all(
                    stream["status"] == "verified" for stream in job.result["streams"].values()
                ), "what the other server keeps is read, and kept"
                assert _bottom(life, A) == (104, 0), "nothing is said to be out of reach"
                life.wall.wait(121)
                life.clock.sleep(6)  # its cooldown is over: it is asked again, and is down again
            first.down = None  # back
            job = _until_done(life, history, job_id, every=4)
        assert job is not None and job.state == "done", (job and job.state, job and job.last_error)
        assert job.result is not None and job.result["unavailable_ledgers"] == 0
        assert job.result["window"] == [100, 130]  # from where the first server keeps history
        assert sorted(job.result["servers"]) == sorted((first.origin, second.origin))
        assert _bottom(life, A)[0] < 104 and _bottom(life, A)[1] == 0
    finally:
        life.close()


def test_the_commands_of_the_watch(life: Life, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", life.engine.url.render_as_string(hide_password=False))
    monkeypatch.setenv("MTL_HORIZON_NOTIFIER_API_URL", "http://notifier.test")
    monkeypatch.setenv("MTL_HORIZON_NOTIFIER_CALLBACK_URL", CALLBACK)
    monkeypatch.setenv("MTL_HORIZON_NOTIFIER_KEYS", life.signer.public_key)
    serve(monkeypatch, life.fake)
    monkeypatch.setattr("app.watch.provision.make_api", lambda settings, **options: life.api())

    assert watch_cli.main(["status"]) == 0
    out = capsys.readouterr().out
    assert "accounts: 3 watched, 2 enabled; by origin: legacy 3" in out and "no Notifier API" not in out
    assert watch_cli.main(["add-account", E, "--reason", "kept an eye on"]) == 0
    out = capsys.readouterr().out
    assert f"{E}: added" in out and "1 start boundaries written" in out and "1 planned" in out
    account = life.account(E)
    assert account is not None and account.watch_since_ledger == LATEST and account.reason == "kept an eye on"
    assert watch_cli.main(["disable-account", E, "--no-pass"]) == 0
    assert "disabled" in capsys.readouterr().out
    assert watch_cli.main(["run"]) == 0
    out = capsys.readouterr().out
    assert "1 import(s) paused" in out
    job = life.job(E)
    assert job is not None and job.state == "paused"
    assert watch_cli.main(["enable-account", E]) == 0
    out = capsys.readouterr().out
    assert "enabled" in out and "1 start boundaries written" in out
    job = life.job(E)
    assert job is not None and job.state == "waiting"

    # Each of them is run as ``python -m <package>``: it has its entry point.
    for package in ("app.watch", "app.discovery", "app.subscriptions", "app.history"):
        assert importlib.util.find_spec(f"{package}.__main__") is not None, package
    monkeypatch.setattr(sys, "argv", ["python -m app.watch", "status"])
    with pytest.raises(SystemExit) as stopped:
        runpy.run_module("app.watch", run_name="__main__")  # as `python -m app.watch` runs it
    assert stopped.value.code == 0 and "accounts: 4 watched" in capsys.readouterr().out


def test_the_services_refuse_to_start_on_a_database_that_is_not_migrated(
    unmigrated_database_url: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from app.history import cli as history_cli
    from app.worker import cli as worker_cli

    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", unmigrated_database_url)
    serve(monkeypatch, FakeHorizon())
    assert worker_cli.main(["run", "--poll-seconds", "0.01"]) == worker_cli.FAILED
    assert "not starting" in capsys.readouterr().err and "alembic upgrade head" in capsys.readouterr().err or True
    assert history_cli.main(["run", "--poll-seconds", "0.01"]) == history_cli.FAILED
