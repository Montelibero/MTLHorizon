"""The figures of the admin dashboard, against a real PostgreSQL."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session

from app.stats import dashboard
from app.storage.models import (
    DiscoveryRun,
    HistoryJob,
    NotifierEvent,
    ProcessingJob,
    ServiceState,
    TrackedAccount,
    TrackedAsset,
    WatchSubscription,
)
from tests import factories

NOW = datetime(2026, 11, 25, 12, 0, 0, tzinfo=UTC)
SECOND = timedelta(seconds=1)
MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)


def pick(given: Any, index: int) -> Any:
    """A value that is the same for every job, or a function of its number."""
    return given(index) if callable(given) else given


def value(figure: Any) -> Any:
    """What a figure says."""
    return figure["value"] if isinstance(figure, dict) else figure.value


def figures(node: Any) -> Iterator[dict[str, Any]]:
    """Every figure of a snapshot as ``as_dict`` shows it."""
    if isinstance(node, dict):
        if "value" in node and "exact" in node:
            yield node
        for item in node.values():
            yield from figures(item)
    elif isinstance(node, list):
        for item in node:
            yield from figures(item)


def keys(node: Any) -> Iterator[str]:
    if isinstance(node, dict):
        for key, item in node.items():
            yield key
            yield from keys(item)
    elif isinstance(node, list):
        for item in node:
            yield from keys(item)


class Statements:
    """What an engine sends to PostgreSQL."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.seen: list[tuple[str, Any]] = []
        event.listen(engine, "before_cursor_execute", self)

    def __call__(self, conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: Any) -> None:
        self.seen.append((statement, parameters))

    def close(self) -> None:
        event.remove(self.engine, "before_cursor_execute", self)

    def of(self, *parts: str) -> list[tuple[str, Any]]:
        """The statements that contain every one of ``parts``."""
        return [item for item in self.seen if all(part in item[0] for part in parts)]

    def counting(self, table: str | None = None) -> list[tuple[str, Any]]:
        """The statements that count a whole table's rows (a count with a WHERE is not one)."""
        if table is not None:
            return [item for item in self.seen if item[0] == f"SELECT count(*) FROM {table}"]
        return [item for item in self.seen if item[0].startswith("SELECT count(*) FROM ") and " WHERE " not in item[0]]


def plans(engine: Engine, statements: list[tuple[str, Any]]) -> list[str]:
    """The plans PostgreSQL has for statements it was given, as EXPLAIN prints them, one text each."""
    with engine.connect() as connection:
        return ["\n".join(row[0] for row in connection.exec_driver_sql("EXPLAIN " + statement, parameters)) for statement, parameters in statements]


class Yard:
    """A private database with what a running installation leaves in it."""

    def __init__(self, url: str) -> None:
        self.engine = create_engine(url)
        self.made = 0

    def close(self) -> None:
        self.engine.dispose()

    def write(self, make: Callable[[Session], None]) -> None:
        with Session(self.engine) as session, session.begin():
            make(session)

    def analyze(self) -> None:
        """The planner's statistics, as autovacuum would gather them."""
        with self.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(text("ANALYZE"))

    def beat(self, record: str, at: datetime | None = None, **state: Any) -> None:
        """What a service last said of itself, as ``app.storage.heartbeats`` keeps it."""
        said = {} if at is None else {"at": at.isoformat(timespec="milliseconds")}
        self.write(lambda session: session.add(ServiceState(name=record, value={**said, **state})))

    def events(self, count: int, newest: datetime, step: timedelta = MINUTE) -> None:
        """Deliveries, the newest at ``newest``, one every ``step`` before it."""
        rows = []
        for index in range(count):
            self.made += 1
            rows.append(
                {
                    "id": f"notice-{self.made}",
                    "subscription": "sub-1",
                    "type": "operation",
                    "received_at": newest - index * step,
                    "state": "queued",
                    "ledger_sequence": 500 + index,
                    "operation_type": "payment",
                    "matched": [],
                    "payload": {},
                    "headers": {},
                }
            )
        with self.engine.begin() as connection:
            connection.execute(NotifierEvent.__table__.insert(), rows)

    def jobs(
        self,
        state: str,
        count: int = 1,
        *,
        due: Any = None,
        created: Any = None,
        finished: Any = None,
        ledger: Any = 500,
        error: Any = None,
        lease_until: Any = None,
    ) -> list[int]:
        """Jobs in one state, their ids. Due now and created an hour ago when not said."""
        rows = []
        for index in range(count):
            self.made += 1
            lease = pick(lease_until, index)
            rows.append(
                {
                    "transaction_hash": factories.digest(self.made),
                    "ledger_sequence": pick(ledger, index),
                    "state": state,
                    "attempts": 3,
                    "next_attempt_at": pick(due, index) or NOW,
                    "created_at": pick(created, index) or NOW - HOUR,
                    "finished_at": pick(finished, index),
                    "last_error": pick(error, index),
                    "lease_expires_at": lease,
                    "leased_by": "worker-1" if lease else None,
                }
            )
        with self.engine.begin() as connection:
            return list(connection.execute(ProcessingJob.__table__.insert().returning(ProcessingJob.id), rows).scalars())

    def event(self, received: datetime, *, ledger: int | None = 500, state: str = "queued") -> None:
        self.made += 1
        self.write(
            lambda session: session.add(
                NotifierEvent(
                    id=f"notice-{self.made}",
                    subscription="sub-1",
                    type="operation",
                    received_at=received,
                    state=state,
                    ledger_sequence=ledger,
                    operation_type="payment",
                    matched=[{"kind": "account"}],
                    payload={"id": f"notice-{self.made}"},
                    headers={},
                )
            )
        )


@pytest.fixture
def yard(fresh_database_url: str) -> Iterator[Yard]:
    instance = Yard(fresh_database_url)
    yield instance
    instance.close()


def stored_history(session: Session) -> None:
    """One transaction, two operations, two effects and two trades, by the write rules of the backfill."""
    transaction = factories.add_transaction(session, 10)
    first = factories.add_operation(session, transaction, 1, type="payment", type_i=1)
    factories.add_participants(session, transaction, first, factories.ALICE, factories.BOB)
    factories.add_effect(session, first, 0, factories.BOB, "account_credited", 2)
    factories.add_effect(session, first, 1, factories.ALICE, "account_debited", 3)
    second = factories.add_operation(session, transaction, 2)
    factories.add_trade(session, second, 0)
    factories.add_trade(session, second, 1)


# -- the worker and its queue --------------------------------------------------


def test_an_installation_where_nothing_ever_ran(yard: Yard) -> None:
    shown = dashboard.snapshot(yard.engine, now=NOW)

    assert shown.worker.state == "stale" and shown.worker.alive is False
    assert shown.worker.heartbeat.heard is False
    assert shown.queue.status == "empty"  # nothing waits, whoever is there or not
    assert shown.history.status == "not_running" and shown.history.heartbeat.heard is False
    assert shown.freshness.last_notification is None
    assert shown.freshness.last_ledger_among_finished_jobs.ledger_sequence is None
    assert shown.freshness.last_ledger_among_finished_jobs.note == dashboard.FINISHED_LEDGER_NOTE
    counted = [figure for figure in figures(shown.as_dict()) if figure["basis"] == "counted"]
    assert counted and all(figure["value"] == 0 for figure in counted)


def test_a_worker_that_just_said_it_is_idle_and_nothing_waits(yard: Yard) -> None:
    yard.beat(dashboard.WORKER_HEARTBEAT, NOW - MINUTE, name="worker-1", state="idle", job_id=None, done=12, failed=1)

    shown = dashboard.snapshot(yard.engine, now=NOW)

    assert shown.worker.state == "idle" and shown.worker.alive
    assert shown.worker.heartbeat.stale is False
    assert (shown.worker.heartbeat.at, shown.worker.heartbeat.age_seconds) == (NOW - MINUTE, 60.0)
    assert (shown.worker.heartbeat.service, shown.worker.done, shown.worker.failed) == ("worker-1", 12, 1)
    assert shown.queue.status == "empty"


def test_a_worker_that_is_on_a_job(yard: Yard) -> None:
    yard.beat(dashboard.WORKER_HEARTBEAT, NOW, name="worker-1", state="working", job_id=7, done=3, failed=0)
    yard.jobs("running", lease_until=NOW + 90 * SECOND)

    shown = dashboard.snapshot(yard.engine, now=NOW)

    assert (shown.worker.state, shown.worker.heartbeat.job_id) == ("working", 7)
    assert shown.queue.status == "working"
    assert (value(shown.queue.running), value(shown.queue.running_past_lease)) == (1, 0)


@pytest.mark.parametrize("silence, stale", [(59, False), (60, False), (60.5, True), (3600, True)])
def test_a_worker_is_stale_after_more_than_a_minute_of_silence(yard: Yard, silence: float, stale: bool) -> None:
    yard.beat(dashboard.WORKER_HEARTBEAT, NOW - timedelta(seconds=silence), name="worker-1", state="idle")

    shown = dashboard.snapshot(yard.engine, now=NOW)

    assert shown.worker.state == ("stale" if stale else "idle")
    assert shown.worker.alive is not stale
    assert shown.worker.heartbeat.heard is True


@pytest.mark.parametrize("record", [{"name": "worker-1", "state": "idle"}, {"at": "yesterday"}, {"at": 17}])
def test_a_heartbeat_that_cannot_be_read_is_no_heartbeat(yard: Yard, record: dict[str, Any]) -> None:
    yard.write(lambda session: session.add(ServiceState(name=dashboard.WORKER_HEARTBEAT, value=record)))

    shown = dashboard.snapshot(yard.engine, now=NOW)

    assert shown.worker.heartbeat.heard is True and shown.worker.heartbeat.at is None
    assert (shown.worker.state, shown.worker.alive) == ("stale", False)


def test_work_that_waits_for_a_worker_that_is_not_there(yard: Yard) -> None:
    yard.beat(dashboard.WORKER_HEARTBEAT, NOW - 5 * MINUTE, name="worker-1", state="idle")
    oldest = NOW - 40 * MINUTE
    yard.jobs("pending", 2, due=NOW - 3 * MINUTE, created=oldest)
    yard.jobs("pending", 1, due=NOW + 5 * MINUTE, created=NOW - 10 * MINUTE)  # in backoff: not due yet
    yard.jobs("running", lease_until=NOW - MINUTE)  # its worker died: the lease ran out

    shown = dashboard.snapshot(yard.engine, now=NOW)

    assert shown.queue.status == "not_taken" and shown.worker.state == "stale"
    assert (value(shown.queue.pending), value(shown.queue.pending_due)) == (3, 2)
    assert (value(shown.queue.running), value(shown.queue.running_past_lease)) == (1, 1)
    assert shown.queue.oldest_pending is not None
    assert shown.queue.oldest_pending.exact is True
    assert (shown.queue.oldest_pending.job_id, shown.queue.oldest_pending.since) == (1, oldest)
    assert shown.queue.oldest_pending.age_seconds == 40 * 60


def test_the_queue_is_empty_and_the_service_is_not_running(yard: Yard) -> None:
    yard.beat(dashboard.WORKER_HEARTBEAT, NOW - HOUR, name="worker-1", state="idle")

    shown = dashboard.snapshot(yard.engine, now=NOW)

    assert shown.queue.status == "empty" and shown.worker.state == "stale"  # two things, said apart


def test_work_that_waits_for_its_time_waits_all_the_same(yard: Yard) -> None:
    yard.jobs("pending", 2, due=NOW + 5 * MINUTE, created=NOW - 2 * HOUR)

    shown = dashboard.snapshot(yard.engine, now=NOW)

    assert shown.queue.status == "not_taken"  # the worker never said anything
    assert value(shown.queue.pending_due) == 0
    assert shown.queue.oldest_pending is not None
    assert shown.queue.oldest_pending.age_seconds == 2 * 60 * 60  # by its row: how long the job is around
    assert shown.queue.oldest_pending.next_attempt_at == NOW + 5 * MINUTE


@pytest.mark.parametrize("silence, status", [(30, "idle_with_due_work"), (5, "working")])
def test_a_live_worker_that_says_idle_while_work_is_due(yard: Yard, silence: float, status: str) -> None:
    # A word said within the heartbeat interval may simply predate the job; an older one, with work due, says the worker is not taking it.
    yard.beat(dashboard.WORKER_HEARTBEAT, NOW - timedelta(seconds=silence), name="worker-1", state="idle")
    yard.jobs("pending", due=NOW - MINUTE)

    shown = dashboard.snapshot(yard.engine, now=NOW)

    assert shown.queue.status == status and shown.worker.state == "idle"


def test_a_live_worker_and_work_waiting_for_its_time(yard: Yard) -> None:
    yard.beat(dashboard.WORKER_HEARTBEAT, NOW, name="worker-1", state="idle")
    yard.jobs("pending", due=NOW + 5 * MINUTE)

    shown = dashboard.snapshot(yard.engine, now=NOW)

    assert shown.queue.status == "working" and value(shown.queue.pending_due) == 0


def test_the_queue_by_state_with_what_gave_up_and_why(yard: Yard) -> None:
    yard.beat(dashboard.WORKER_HEARTBEAT, NOW, name="worker-1", state="idle")
    yard.jobs("done", 4)
    yard.jobs("pending", 3)
    yard.jobs("running", lease_until=NOW + MINUTE)
    (partial,) = yard.jobs("partial", error="503 on the trades of the transaction, 8 tries", finished=NOW - 2 * MINUTE)
    first, second = yard.jobs("dead", 2, error="the transaction is on none of the servers", finished=lambda index: NOW - (30 - index * 29) * MINUTE)
    (last,) = yard.jobs("dead", error="later", finished=NOW - timedelta(seconds=30))

    shown = dashboard.snapshot(yard.engine, now=NOW)

    assert (value(shown.queue.pending), value(shown.queue.running)) == (3, 1)
    assert (value(shown.queue.done), value(shown.queue.partial), value(shown.queue.dead)) == (4, 1, 3)
    assert shown.queue.done.exact and shown.queue.done.basis.endswith("less the other states")
    assert value(shown.queue.waiting_for_a_person) == 4
    assert shown.queue.status == "working"  # a live worker, with work to do
    assert [(error["job_id"], error["state"], error["last_error"], error["finished_at"]) for error in shown.queue.errors] == [
        (last, "dead", "later", NOW - timedelta(seconds=30)),
        (second, "dead", "the transaction is on none of the servers", NOW - MINUTE),
        (partial, "partial", "503 on the trades of the transaction, 8 tries", NOW - 2 * MINUTE),
        (first, "dead", "the transaction is on none of the servers", NOW - 30 * MINUTE),
    ]
    assert all(error["transaction_hash"] and error["attempts"] == 3 for error in shown.queue.errors)
    assert shown.queue.errors[0]["job_id"] not in (partial, first, second)


def test_not_more_than_a_few_errors_are_read(yard: Yard, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dashboard, "ERRORS_SHOWN", 3)
    yard.jobs("dead", 5, error="x", finished=lambda index: NOW - index * MINUTE)

    errors = dashboard.snapshot(yard.engine, now=NOW).queue.errors

    assert [error["finished_at"] for error in errors] == [NOW - index * MINUTE for index in (0, 1, 2)]  # the newest three


def test_the_oldest_pending_job_of_a_large_queue_is_a_lower_bound(yard: Yard, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dashboard, "OLDEST_AGE_EXACT_LIMIT", 2)
    yard.jobs("pending", 2, due=lambda index: NOW - (90 - 40 * index) * MINUTE)
    yard.jobs("pending", due=NOW + MINUTE)

    oldest = dashboard.snapshot(yard.engine, now=NOW).queue.oldest_pending

    # Only the queue index is read in a large queue: how long the earliest attempt has been due, and of which job nothing.
    assert oldest is not None and oldest.exact is False
    assert (oldest.age_seconds, oldest.job_id, oldest.since) == (90 * 60, None, None)
    assert oldest.next_attempt_at == NOW - 90 * MINUTE


def test_nothing_due_in_a_large_queue(yard: Yard, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dashboard, "OLDEST_AGE_EXACT_LIMIT", 0)
    yard.jobs("pending", due=NOW + MINUTE, created=NOW - HOUR)

    oldest = dashboard.snapshot(yard.engine, now=NOW).queue.oldest_pending

    assert oldest is not None and oldest.exact is False
    assert (oldest.age_seconds, oldest.next_attempt_at) == (None, NOW + MINUTE)


# -- freshness -----------------------------------------------------------------


def test_the_last_notification_and_the_last_ledger_of_finished_jobs(yard: Yard) -> None:
    yard.event(NOW - 3 * MINUTE, ledger=700)
    yard.event(NOW - MINUTE, ledger=701, state="unmatched")
    yard.event(NOW - 2 * MINUTE, ledger=699)
    # 25 jobs done: the later the attempt, the higher the ledger. One old job has the highest ledger of all.
    yard.jobs(
        "done",
        25,
        due=lambda index: NOW - (60 - index) * MINUTE,
        finished=lambda index: NOW - (40 - index) * MINUTE,
        ledger=lambda index: 1000 + index,
    )
    yard.jobs("done", due=NOW - 500 * MINUTE, finished=NOW - 400 * MINUTE, ledger=99_999)

    shown = dashboard.snapshot(yard.engine, now=NOW).freshness

    assert shown.last_notification is not None
    assert (shown.last_notification.received_at, shown.last_notification.age_seconds) == (NOW - MINUTE, 60.0)
    assert (shown.last_notification.ledger_sequence, shown.last_notification.state) == (701, "unmatched")
    # The greatest ledger among the newest 20 done jobs is 1024; the 99 999 of an old job is not among them.
    assert (shown.last_ledger_among_finished_jobs.ledger_sequence, shown.last_ledger_among_finished_jobs.sampled_jobs) == (1024, 20)
    assert shown.last_ledger_among_finished_jobs.finished_at == NOW - 16 * MINUTE
    assert "it is not a frontier" in shown.last_ledger_among_finished_jobs.note.lower()
    assert not [key for key in keys(dashboard.plain(shown)) if "frontier" in key.lower()]


def test_the_last_ledger_when_few_jobs_were_finished(yard: Yard) -> None:
    yard.jobs(
        "done",
        3,
        due=lambda index: NOW - (10 - index) * MINUTE,
        finished=lambda index: NOW - index * MINUTE,
        ledger=lambda index: (5, 9, 7)[index],
    )

    shown = dashboard.snapshot(yard.engine, now=NOW).freshness

    assert shown.last_notification is None
    assert (shown.last_ledger_among_finished_jobs.ledger_sequence, shown.last_ledger_among_finished_jobs.sampled_jobs) == (9, 3)


# -- the history service -------------------------------------------------------

FRESH = {"at": NOW.isoformat(timespec="milliseconds"), "name": "history-1", "state": "idle"}


@pytest.mark.parametrize(
    "record, paused, status",
    [
        (None, False, "not_running"),
        ({**FRESH, "at": (NOW - 10 * MINUTE).isoformat()}, False, "not_running"),
        (FRESH, False, "idle"),
        ({**FRESH, "state": "working", "job_id": 4}, False, "working"),
        ({**FRESH, "state": "paused"}, False, "paused"),
        ({**FRESH, "state": "working"}, True, "paused"),
        ({**FRESH, "state": "else"}, False, "unknown"),
    ],
)
def test_the_history_service_and_the_pause_of_its_queue(
    yard: Yard, record: dict[str, Any] | None, paused: bool, status: str
) -> None:
    pause = {"paused": paused, "because": "tests", "by": "owner", "at": NOW.isoformat(), "resumed_by": None, "resumed_at": None}
    if record is not None:
        yard.write(lambda session: session.add(ServiceState(name=dashboard.HISTORY_HEARTBEAT, value=record)))
    if paused:
        yard.write(lambda session: session.add(ServiceState(name=dashboard.HISTORY_PAUSE, value=pause)))

    shown = dashboard.snapshot(yard.engine, now=NOW).history

    assert (shown.status, shown.queue_paused) == (status, paused)
    assert shown.heartbeat.record == dashboard.HISTORY_HEARTBEAT
    assert shown.pause == (pause if paused else {})
    assert shown.heartbeat.job_id == (record or {}).get("job_id")


def test_the_queue_of_history_jobs(yard: Yard) -> None:
    yard.beat(dashboard.HISTORY_HEARTBEAT, NOW, name="history-1", state="working", job_id=9, slices_done=40, slices_failed=2)
    states = {"waiting": 3, "running": 1, "done": 10, "partial": 2, "error": 1, "paused": 4}
    made = 0

    def make(session: Session) -> None:
        nonlocal made
        for state, count in states.items():
            for index in range(count):
                made += 1
                session.add(
                    HistoryJob(
                        kind="account",
                        subject=factories.address(2000 + made),
                        state=state,
                        to_ledger=50_000,
                        created_at=NOW - (made if state == "waiting" else 1) * MINUTE,
                    )
                )

    yard.write(make)

    shown = dashboard.snapshot(yard.engine, now=NOW).history

    assert {state: value(figure) for state, figure in shown.jobs.items()} == states
    assert shown.heartbeat.other == {"slices_done": 40, "slices_failed": 2}
    assert (shown.status, shown.queue_paused) == ("working", False)
    assert shown.oldest_waiting is not None and shown.oldest_waiting.exact
    # The first of the three that wait was created three minutes ago.
    assert (shown.oldest_waiting.since, shown.oldest_waiting.age_seconds) == (NOW - 3 * MINUTE, 180.0)


# -- subscriptions and discovery -----------------------------------------------


def test_subscriptions_by_state_and_what_is_wrong_with_them(yard: Yard) -> None:
    account, asset = factories.address(3000), f"MTLRECT:{factories.ISSUER_B}"
    wanted = {"wanted": 2, "active": 5, "retiring": 1, "manual": 1, "retired": 3}
    made = 0

    def make(session: Session) -> None:
        nonlocal made
        for state, count in wanted.items():
            for _ in range(count):
                made += 1
                session.add(WatchSubscription(kind="account", key=factories.address(3000 + made), state=state, remote_id=f"remote-{made}"))
        session.add_all(
            [
                WatchSubscription(
                    kind="account", key=account, state="error", last_error="Notifier did not answer", attempts=3,
                    next_attempt_at=NOW + 30 * MINUTE,
                ),
                WatchSubscription(
                    kind="asset", key=asset, state="ambiguous", last_error="two subscriptions fit", attempts=1,
                    next_attempt_at=NOW + MINUTE,
                ),
            ]
        )

    yard.write(make)

    shown = dashboard.snapshot(yard.engine, now=NOW).subscriptions

    assert {state: value(figure) for state, figure in shown.counts.items()} == {**wanted, "error": 1, "ambiguous": 1}
    assert [
        (problem["kind"], problem["key"], problem["state"], problem["last_error"], problem["next_attempt_at"], problem["attempts"])
        for problem in shown.problems
    ] == [
        ("asset", f"MTLRECT:{factories.ISSUER_B[:6]}…", "ambiguous", "two subscriptions fit", NOW + MINUTE, 1),
        ("account", account[:6] + "…", "error", "Notifier did not answer", NOW + 30 * MINUTE, 3),
    ]
    assert all(problem["id"] for problem in shown.problems)


def test_discovery_the_assets_marked_the_last_pass_of_each_and_the_accounts_by_origin(yard: Yard) -> None:
    issuer = factories.ISSUER_A
    eur, rect, usd = f"EURMTL:{issuer}", f"MTLRECT:{issuer}", f"USD:{issuer}"
    origins = (("discovery", True, 7), ("discovery", False, 2), ("manual", True, 3), ("legacy", True, 1))
    made = 0

    def accounts(session: Session) -> None:
        nonlocal made
        for origin, enabled, count in origins:
            for _ in range(count):
                made += 1
                account = factories.address(4000 + made)
                factories.add_account(session, account)
                session.add(TrackedAccount(account_id=account, enabled=enabled, reason="test", origin=origin))
        for asset in (eur, rect, usd):
            factories.add_asset(session, asset)
            session.add(TrackedAsset(asset_id=asset, enabled=True, discover=asset != usd, reason="test"))

    yard.write(accounts)
    yard.write(
        lambda session: session.add_all(
            [
                DiscoveryRun(
                    asset_id=eur, state="complete", started_at=NOW - 2 * HOUR, finished_at=NOW - 2 * HOUR + 3 * MINUTE,
                    pages=5, found=1000, with_line=1000, added=990, already=10,
                ),
                DiscoveryRun(
                    asset_id=eur, state="incomplete", started_at=NOW - 5 * MINUTE, finished_at=NOW - 4 * MINUTE,
                    pages=2, found=400, with_line=400, added=300, already=100, last_error="request budget used",
                ),
                DiscoveryRun(asset_id=eur, state="complete", dry_run=True, started_at=NOW - MINUTE, found=7),
                DiscoveryRun(asset_id=rect, state="running", started_at=NOW - 20 * MINUTE),
            ]
        )
    )

    shown = dashboard.snapshot(yard.engine, now=NOW).discovery

    assert (value(shown.marked), shown.assets) == (2, (eur, rect))
    assert [(item["asset_id"], item["state"], item["pages"], item["found"], item["added"], item["started_at"]) for item in shown.last_passes] == [
        (eur, "incomplete", 2, 400, 300, NOW - 5 * MINUTE),  # the last pass that was not a dry run
        (rect, "running", 0, 0, 0, NOW - 20 * MINUTE),
    ]
    assert (shown.last_passes[0]["finished_at"], shown.last_passes[0]["last_error"]) == (NOW - 4 * MINUTE, "request budget used")
    assert {origin: {name: value(figure) for name, figure in entry.items()} for origin, entry in shown.accounts_by_origin.items()} == {
        "discovery": {"total": 9, "enabled": 7},
        "legacy": {"total": 1, "enabled": 1},
        "manual": {"total": 3, "enabled": 3},
    }
    assert (value(shown.accounts), value(shown.accounts_enabled)) == (13, 11)


# -- the database --------------------------------------------------------------

COUNTED_TABLES = ("history_transactions", "history_operations", "history_effects", "history_trades", "processing_job")


def test_the_database_its_tables_and_its_unique_records(yard: Yard) -> None:
    yard.write(stored_history)

    database = dashboard.snapshot(yard.engine, now=NOW).database

    assert database.size_bytes.exact and value(database.size_bytes) > 0
    sizes = {table.name: table for table in database.tables}
    for name in ("history_transactions", "notifier_event", "processing_job", "sync_coverage", "history_transaction_participants"):
        assert sizes[name].total_bytes > 0, name
    with yard.engine.connect() as connection:
        expected = connection.execute(text("SELECT pg_total_relation_size('history_effects'), pg_indexes_size('history_trades')")).one()
    assert sizes["history_effects"].total_bytes == expected[0]
    assert (sizes["history_trades"].index_bytes, sizes["history_trades"].planner_rows) == (expected[1], -1.0)
    rows = {"history_transactions": 1, "history_operations": 2, "history_effects": 2, "history_trades": 2, "processing_job": 0}
    for name in COUNTED_TABLES:
        figure = database.rows[name]
        assert (figure.value, figure.exact, figure.basis) == (rows[name], True, "counted"), name
    assert {name: figure.value for name, figure in database.unique.items()} == {"transactions": 1, "operations": 2, "effects": 2, "trades": 2}


def test_records_of_a_table_that_grew_large_are_estimated(yard: Yard, monkeypatch: pytest.MonkeyPatch) -> None:
    yard.write(stored_history)
    yard.analyze()  # the planner now knows: 1 transaction, 2 of each other kind
    monkeypatch.setattr(dashboard, "EXACT_COUNT_LIMIT", 1)
    seen = Statements(yard.engine)
    try:
        database = dashboard.snapshot(yard.engine, now=NOW).database
    finally:
        seen.close()

    # Nothing of the history is counted: each table has as many rows as the limit, and the planner's number stands in.
    assert [statement for statement, _ in seen.counting() if statement.startswith("SELECT count(*) FROM history_")] == []
    assert {name: (figure.value, figure.exact, figure.basis) for name, figure in database.unique.items()} == {
        name: (count, False, "pg_class.reltuples") for name, count in (("transactions", 1), ("operations", 2), ("effects", 2), ("trades", 2))
    }
    assert database.rows["processing_job"].exact and database.rows["processing_job"].value == 0  # empty: under the limit


def test_a_table_never_analyzed_and_too_large_to_count_is_not_known(yard: Yard, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dashboard, "NEVER_ANALYZED_PAGES_LIMIT", 0)
    yard.write(stored_history)
    seen = Statements(yard.engine)
    try:
        rows = dashboard.snapshot(yard.engine, now=NOW).database.rows
    finally:
        seen.close()

    for name in ("history_transactions", "history_operations", "history_effects", "history_trades"):
        assert (rows[name].value, rows[name].exact) == (None, False), name
        assert "never analyzed" in rows[name].basis, name
    assert [statement for statement, _ in seen.counting() if statement.startswith("SELECT count(*) FROM history_")] == []


def test_tables_never_analyzed_are_counted_while_they_are_small(yard: Yard) -> None:
    yard.write(stored_history)

    rows = dashboard.snapshot(yard.engine, now=NOW).database.rows

    assert (rows["history_effects"].value, rows["history_effects"].exact, rows["history_effects"].basis) == (2, True, "counted")


def test_the_cache_spares_the_database_the_expensive_counts(yard: Yard) -> None:
    yard.write(stored_history)
    seen = Statements(yard.engine)
    cache = dashboard.TtlCache(ttl_seconds=0.3)
    try:
        first = dashboard.snapshot(yard.engine, now=NOW, cache=cache)
        second = dashboard.snapshot(yard.engine, now=NOW + MINUTE, cache=cache)
        kept = len(seen.counting())
        time.sleep(0.35)
        fourth = dashboard.snapshot(yard.engine, now=NOW + 2 * MINUTE, cache=cache)
    finally:
        seen.close()

    assert len(seen.counting()) == 10  # the five counted tables, in the first snapshot and again after the cache let them go
    assert kept == 5
    assert first.database.rows == second.database.rows
    assert first.queue.done.exact and first.queue.done.measured_at == NOW
    assert not second.queue.done.exact and "counted earlier" in second.queue.done.basis
    assert second.queue.done.measured_at == NOW
    assert fourth.database.rows["history_effects"].measured_at == NOW + 2 * MINUTE
    assert fourth.queue.done.exact


def test_without_a_cache_every_snapshot_counts_again(yard: Yard) -> None:
    yard.write(stored_history)
    seen = Statements(yard.engine)
    try:
        dashboard.snapshot(yard.engine, now=NOW)
        dashboard.snapshot(yard.engine, now=NOW)
    finally:
        seen.close()

    assert len(seen.counting()) == 10


def test_a_count_that_runs_out_of_time_gives_the_estimate(yard: Yard, monkeypatch: pytest.MonkeyPatch) -> None:
    yard.write(stored_history)
    yard.analyze()
    monkeypatch.setattr(dashboard, "STATEMENT_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(dashboard, "_COUNT_ROWS", "SELECT count(*), pg_sleep(0.3) FROM {table}")

    database = dashboard.snapshot(yard.engine, now=NOW).database

    for name, count in (("transactions", 1), ("operations", 2)):
        figure = database.rows[f"history_{name}"]
        assert (figure.value, figure.exact) == (count, False), name
        assert figure.basis == "pg_class.reltuples; the exact count ran out of time", name


def test_the_queue_is_read_through_its_index(yard: Yard) -> None:
    yard.jobs("done", 3000, due=lambda index: NOW - (200 - index / 100) * MINUTE, ledger=lambda index: 1000 + index)
    yard.jobs("pending", 60)
    yard.events(2000, newest=NOW - 2 * MINUTE)
    yard.event(NOW - MINUTE, ledger=99_999)
    yard.analyze()
    seen = Statements(yard.engine)
    try:
        shown = dashboard.snapshot(yard.engine, now=NOW)
        asked = [item for item in seen.seen if item[0].startswith("SELECT") and ("processing_job" in item[0] or "notifier_event" in item[0])]
    finally:
        seen.close()

    assert (value(shown.queue.done), value(shown.queue.pending)) == (3000, 60)
    assert shown.freshness.last_ledger_among_finished_jobs.ledger_sequence == 3999
    assert shown.freshness.last_notification.ledger_sequence == 99_999
    explained = plans(yard.engine, asked)
    assert len(explained) == len(asked) >= 7
    by_statement = {statement: plan for (statement, _), plan in zip(asked, explained)}
    sample = [plan for statement, plan in by_statement.items() if "recent" in statement][0]
    assert "Index Scan Backward using ix_processing_job_queue" in sample and "Sort" not in sample
    notification = [plan for statement, plan in by_statement.items() if "notifier_event" in statement][0]
    assert "Index Scan Backward using ix_notifier_event_received" in notification and "Sort" not in notification
    counts = [plan for statement, plan in by_statement.items() if "GROUP BY state" in statement][0]
    assert "ix_processing_job_queue" in counts and "Seq Scan" not in counts


# -- how it reads --------------------------------------------------------------


def test_reading_is_read_only_bounded_and_leaves_the_connection_as_it_was(yard: Yard) -> None:
    settings = ("statement_timeout", "work_mem", "jit")
    with yard.engine.connect() as connection:
        before = {name: connection.execute(text(f"SHOW {name}")).scalar() for name in settings}

    with pytest.raises(OperationalError) as too_slow:
        with dashboard.reading(yard.engine, timeout_seconds=0.05, work_mem="32MB") as connection:
            assert connection.execute(text("SHOW statement_timeout")).scalar() == "50ms"
            assert connection.execute(text("SHOW transaction_read_only")).scalar() == "on"
            assert connection.execute(text("SHOW work_mem")).scalar() == "32MB"
            assert connection.execute(text("SHOW jit")).scalar() == "off"
            connection.execute(text("SELECT pg_sleep(0.5)"))  # longer than the statement may take
    assert dashboard.timed_out(too_slow.value)

    with pytest.raises(DBAPIError) as refused:
        with dashboard.reading(yard.engine, timeout_seconds=5) as connection:
            connection.execute(text("UPDATE processing_job SET attempts = attempts"))
    assert "read-only" in str(refused.value)

    with yard.engine.connect() as connection:  # the same connection, back from the pool
        after = {name: connection.execute(text(f"SHOW {name}")).scalar() for name in settings}
    assert after == before and before["statement_timeout"] != "50ms"


def test_a_statement_that_is_not_slow_enough_to_time_out_is_not_swallowed(yard: Yard) -> None:
    with pytest.raises(DBAPIError) as failed:
        with dashboard.reading(yard.engine, timeout_seconds=5) as connection:
            connection.execute(text("SELECT * FROM there_is_no_such_table"))
    assert not dashboard.timed_out(failed.value)


# -- what a page is shown ------------------------------------------------------


def test_what_a_page_is_shown(yard: Yard) -> None:
    yard.beat(dashboard.WORKER_HEARTBEAT, NOW, name="worker-1", state="working", job_id=1, done=1, failed=0)
    yard.jobs("done")

    dashboard_shown = dashboard.snapshot(yard.engine, now=NOW).as_dict()

    assert isinstance(json.dumps(dashboard_shown), str)  # plain data, for a template or JSON
    assert dashboard_shown["measured_at"] == NOW.isoformat()
    assert dashboard_shown["worker"]["heartbeat"]["other"]["done"] == 1 and dashboard_shown["queue"]["done"]["value"] == 1
    for figure in figures(dashboard_shown):
        assert set(figure) == {"value", "exact", "measured_at", "basis"}, figure
        assert figure["measured_at"] == NOW.isoformat(), figure


def test_a_cache_keeps_what_is_put_for_its_time(yard: Yard) -> None:
    cache = dashboard.TtlCache(ttl_seconds=0.2, max_entries=2)
    cache.put("a", 1, NOW)
    cache.put("b", 2, NOW)
    assert (cache.get("a"), cache.get("missing")) == (1, None)
    cache.put("c", 3, NOW)  # a third: the oldest makes room
    assert cache.get("a") is None and (cache.get("b"), cache.get("c")) == (2, 3)
    time.sleep(0.25)
    assert cache.get("b") is None and cache.get("c") is None
