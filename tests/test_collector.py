"""The live collector against the fake Horizon, stored in PostgreSQL."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.backfill import config as watch_lists
from app.backfill import verify as checking
from app.backfill.cli import build_parser as backfill_parser, historical_window
from app.backfill.jobs import StreamJob, stream_jobs
from app.backfill.loader import FULL, Limits, Window
from app.collector import check
from app.collector import cli as collector_cli
from app.config import ConfigError, PUBLIC_NETWORK_PASSPHRASE
from app.sources.horizon import RequestFailed
from app.collector.loader import Collector, CycleReport, StreamResult, WrongNetwork
from app.storage import collector_state, coverage, locks
from app.storage.collector_state import StreamState
from app.storage.locks import AdvisoryLock
from app.storage.writer import HistoryWriter
from tests.fake_horizon import EUR, ISSUER, RECT, A, B, C, D, S, FakeHorizon, Operation, Transaction, _payment, paid, sale, serve

SOURCE = "horizon:pool"  # the logical source, as the commands name it
START = 118  # the latest ledger when the collector is first started
WATCH_LIST = watch_lists.parse(
    yaml.safe_load(
        f"""
assets:
  - code: EURMTL
    issuer: {ISSUER}
  - code: MTLRECT
    issuer: {ISSUER}
markets:
  - base: MTLRECT:{ISSUER}
    counter: EURMTL:{ISSUER}
accounts:
  - address: {A}
    mode: FULL
    reason: test account
  - address: {B}
    mode: FULL
    reason: test account
  - address: {C}
    enabled: false
    reason: not loaded
"""
    ),
    "test",
)
MARKET = f"market {EUR}>{RECT} / trades"


class Live:
    """A private database with the watch list imported, and a fake source."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.engine = create_engine(url)
        self.fake = FakeHorizon(latest=START)
        with Session(self.engine) as session, session.begin():
            watch_lists.apply(session, WATCH_LIST)

    def close(self) -> None:
        self.engine.dispose()

    def jobs(self, *parts: str) -> list[StreamJob]:
        with Session(self.engine) as session:
            jobs, _ = stream_jobs(session)
        return [job for job in jobs if all(part in job.name for part in parts)]

    def cycle(
        self,
        *,
        subjects: tuple[str, ...] = (),
        page_size: int = 200,
        limits: Limits | None = None,
        start_from: int | None = None,
        retry_base: float = 900.0,
        context: str = FULL,
        concurrency: int = 1,
    ) -> CycleReport:
        with Collector(
            self.fake.client,
            self.engine,
            source=SOURCE,
            network_passphrase=PUBLIC_NETWORK_PASSPHRASE,
            page_size=page_size,
            retry_base=retry_base,
            context=context,
            concurrency=concurrency,
        ) as collector:
            return collector.cycle(self.jobs(*subjects), limits=limits, start_from=start_from)

    def limits(self, **limits: Any) -> Limits:
        first = self.fake.client.stats.requests
        return Limits(requests=lambda: self.fake.client.stats.requests - first, **limits)

    def states(self) -> dict[tuple[str, str], StreamState]:
        with Session(self.engine) as session:
            return {state.key: state for state in collector_state.streams(session, SOURCE)}

    def coverage_of(self, part: str) -> list[tuple[str, int, int, str | None]]:
        with Session(self.engine) as session:
            rows = [row for job in self.jobs(part) for row in coverage.ranges(session, job.key)]
        return sorted((row.state, row.from_ledger, row.to_ledger, row.detail) for row in rows)

    def rows(self, statement: str, **params: Any) -> list[Any]:
        with self.engine.connect() as connection:
            return connection.execute(text(statement), params).all()

    def count(self, table: str) -> int:
        return self.rows(f"SELECT count(*) FROM {table}")[0][0]

    def asked(self, path_part: str) -> list[dict[str, str]]:
        return self.fake.asked(path_part)


@pytest.fixture
def live(make_database: Callable[[], str]) -> Iterator[Live]:
    instance = Live(make_database())
    yield instance
    instance.close()


def _results(report: CycleReport) -> dict[str, StreamResult]:
    return {result.name: result for result in report.streams}


ALL_STREAMS = {f"account {account} / {stream}" for account in (A, B) for stream in ("transactions", "operations", "effects", "trades")} | {MARKET}


# -- starting and going on ------------------------------------------------------


def test_the_first_cycle_fixes_the_start_of_every_stream_at_once(live: Live) -> None:
    report = live.cycle()

    assert (report.latest_ledger, report.start_ledger) == (START, START)
    assert set(report.started) == ALL_STREAMS and len(report.streams) == 9
    assert {result.status for result in report.streams} == {"verified"}
    assert all(result.frontier_ledger == START and result.lag == 0 for result in report.streams)
    assert {(state.start_ledger, state.frontier_ledger) for state in live.states().values()} == {(START, START)}
    with Session(live.engine) as session:
        seen = collector_state.source_state(session, SOURCE)
    assert (seen.latest_ledger, seen.elder_ledger, seen.cycles) == (START, 100, 1)  # type: ignore[union-attr]
    # Nothing was asked but the root, and nothing was claimed.
    assert [path for path, _ in live.fake.requests] == ["/"]
    assert live.count("sync_coverage") == 0
    assert live.count("history_transactions") == 0


def test_a_cycle_reads_what_is_new_and_calls_the_rest_empty(live: Live) -> None:
    live.cycle()
    live.fake.close(paid(122, A, B), latest=125)

    report = live.cycle()

    assert {result.status for result in report.streams} == {"verified"}
    assert all(result.frontier_ledger == 125 for result in report.streams)
    assert (report.global_frontier, report.lag) == (125, 0)
    assert live.coverage_of(f"account {A} / transactions") == [
        ("complete", 122, 125, None),
        ("empty", 119, 121, None),
    ]
    assert live.coverage_of(f"account {B} / effects") == [("complete", 122, 125, None), ("empty", 119, 121, None)]
    assert live.coverage_of(f"account {B} / trades") == [("empty", 119, 125, None)]
    assert live.coverage_of(MARKET) == [("empty", 119, 125, None)]
    assert live.rows("SELECT ledger_sequence FROM history_transactions ORDER BY 1") == [(122,)]
    assert live.rows("SELECT context_loaded FROM history_transactions") == [(["operations", "effects", "trades"],)]
    # The start did not move, and every stream was read forwards.
    assert {state.start_ledger for state in live.states().values()} == {START}
    stream_queries = [query for path, query in live.fake.requests if path.startswith("/accounts/") or path == "/trades"]
    assert stream_queries and {query["order"] for query in stream_queries} == {"asc"}


def test_history_before_the_start_ledger_is_left_to_a_backfill(live: Live) -> None:
    live.cycle()  # A's last transaction lies in ledger 118, the start ledger
    live.fake.close(paid(119, A, B), latest=120)

    live.cycle()

    assert live.coverage_of(f"account {A} / transactions") == [("complete", 119, 120, None)]
    assert live.rows("SELECT ledger_sequence FROM history_transactions ORDER BY 1") == [(119,)]


def test_a_start_ledger_given_explicitly_continues_after_a_backfill(live: Live) -> None:
    live.fake.close(paid(112, A, B), latest=116)

    report = live.cycle(start_from=110)

    assert report.start_ledger == 110
    assert live.coverage_of(f"account {A} / transactions") == [
        ("complete", 112, 116, None),
        ("empty", 111, 111, None),
    ]
    assert {state.start_ledger for state in live.states().values()} == {110}


def test_a_start_ledger_ahead_of_the_source_is_refused(live: Live) -> None:
    with pytest.raises(ConfigError, match="lies ahead"):
        live.cycle(start_from=START + 1)
    assert live.states() == {}


def test_a_source_of_another_network_is_refused_before_anything_is_written(live: Live) -> None:
    live.fake.network = "Test SDF Network ; September 2015"

    with pytest.raises(WrongNetwork):
        live.cycle()

    assert live.states() == {} and live.count("collector_source") == 0


def test_cycles_step_by_step_store_what_one_read_of_everything_stores(make_database: Callable[[], str]) -> None:
    stepped, whole = Live(make_database()), Live(make_database())
    try:
        stepped.cycle()
        stepped.fake.close(paid(120, A, B), latest=121)
        stepped.cycle(page_size=2)
        payment = Transaction(
            123, 0, A, [_payment(B, 1_000_000), _payment(C, 2_000_000), _payment(B, 3_000_000)], {A, B, C}, {1: {A, B}, 2: {A, C}, 3: {A, B}}
        )
        stepped.fake.close(payment, sale(124), latest=126)
        short = stepped.cycle(page_size=2, limits=stepped.limits(max_requests=9))  # too small to finish
        assert short.stopped and not short.failed
        assert {result.status for result in stepped.cycle(page_size=2).streams} == {"verified"}
        assert all(state.frontier_ledger == 126 for state in stepped.states().values())

        whole.fake.close(paid(120, A, B), payment, sale(124), latest=126)
        assert {result.status for result in whole.cycle(start_from=START).streams} == {"verified"}

        window = Window(START + 1, 126)
        assert checking.fingerprint(stepped.engine, window) == checking.fingerprint(whole.engine, window)
    finally:
        stepped.close()
        whole.close()


def test_a_week_between_cycles_is_read_in_one_go(live: Live) -> None:
    """The collector was away for a week of ledgers — 720 an hour — and nothing
    it missed is lost or claimed that was not read."""
    live.cycle()
    hour = 720
    live.fake.close(
        paid(START + 14 * hour, A, B), paid(START + 90 * hour, B, C), sale(START + 160 * hour), latest=START + 168 * hour
    )

    report = live.cycle()

    assert {result.status for result in report.streams} == {"verified"}
    assert (report.global_frontier, report.lag) == (START + 168 * hour, 0)
    # Nothing up to the first record; from it on the range holds records.
    assert live.coverage_of(f"account {A} / transactions") == [
        ("complete", START + 14 * hour, START + 168 * hour, None),
        ("empty", START + 1, START + 14 * hour - 1, None),
    ]
    assert live.coverage_of(f"account {B} / effects") == [
        ("complete", START + 14 * hour, START + 168 * hour, None),
        ("empty", START + 1, START + 14 * hour - 1, None),
    ]
    assert live.rows("SELECT ledger_sequence FROM history_transactions ORDER BY 1") == [
        (START + 14 * hour,), (START + 90 * hour,), (START + 160 * hour,),
    ]


def test_a_page_ending_inside_a_ledger_proves_only_the_ledgers_below_it(live: Live) -> None:
    live.cycle()
    live.fake.close(
        Transaction(121, 0, A, [_payment(A, n) for n in range(1, 6)], {A}, {number: {A} for number in range(1, 6)}),
        latest=123,
    )  # ten effects of A in one ledger

    report = live.cycle(subjects=(f"account {A} / effects",), page_size=4)

    assert [result.status for result in report.streams] == ["verified"]
    assert live.coverage_of(f"account {A} / effects") == [("complete", 121, 123, None), ("empty", 119, 120, None)]
    assert live.rows("SELECT count(*) FROM history_effects WHERE account_id = :a", a=A) == [(10,)]
    # The second page was asked for from the fourth effect, not from the start
    # of the ledger (the third page is the empty one after a full page).
    cursors = [query["cursor"] for query in live.asked(f"{A}/effects")]
    assert cursors[1:] == [f"{(121 << 32) | 2}-2", f"{(121 << 32) | 4}-2"]


def test_what_a_backfill_verified_is_not_read_again(live: Live) -> None:
    live.cycle()
    job = live.jobs(f"account {A} / transactions")[0]
    with HistoryWriter(live.engine).transaction() as session:  # a backfill read up to 122 meanwhile
        coverage.plan(session, job.key, SOURCE, START + 1, 125)
        coverage.replace(session, job.key, SOURCE, START + 1, 122, [coverage.complete(START + 1, 122)])
    live.fake.close(paid(124, A, B), latest=125)

    live.cycle(subjects=(f"account {A} / transactions",))

    assert live.coverage_of(f"account {A} / transactions") == [
        ("complete", 119, 122, None),
        ("complete", 124, 125, None),
        ("empty", 123, 123, None),
    ]
    assert live.asked(f"{A}/transactions")[-1]["cursor"] == str((123 << 32) - 1)  # after everything in ledger 122


# -- what can go wrong ----------------------------------------------------------


def test_a_stream_the_source_cannot_serve_fails_alone_and_waits_before_it_is_asked_again(live: Live) -> None:
    live.cycle()
    live.fake.close(paid(120, A, B), latest=121)
    live.fake.fail(f"{A}/trades", 503, times=8)

    report = live.cycle()

    assert report.failed
    assert _results(report)[f"account {A} / trades"].status == "failed"
    assert {result.status for result in report.streams if result.name != f"account {A} / trades"} == {"verified"}
    states = live.states()
    assert (states[("trade", A)].failures, states[("trade", A)].frontier_ledger) == (1, START)
    assert "503" in states[("trade", A)].last_error
    assert states[("transaction", A)].frontier_ledger == 121
    assert live.coverage_of(f"account {A} / trades") == []  # nothing is claimed

    asked = len(live.asked(f"{A}/trades"))
    waiting = _results(live.cycle())[f"account {A} / trades"]
    assert waiting.status == "waiting" and waiting.lag == 3
    assert len(live.asked(f"{A}/trades")) == asked  # not asked: it waits

    live.fake.faults.clear()
    live.fake.close(paid(122, A, B), latest=123)
    report = live.cycle(retry_base=0.0)  # asked again at once
    assert _results(report)[f"account {A} / trades"].status == "verified"
    assert live.states()[("trade", A)].failures == 0
    assert live.coverage_of(f"account {A} / trades") == [("empty", 119, 123, None)]


def test_a_record_that_cannot_be_stored_fails_its_stream_and_claims_nothing(live: Live) -> None:
    live.cycle()
    live.fake.close(paid(120, A, B), latest=121)

    def without_source(path: str, body: dict[str, Any]) -> dict[str, Any]:
        if path == f"/accounts/{A}/transactions":
            for record in body["_embedded"]["records"]:
                record.pop("source_account")
        return body

    live.fake.tamper = without_source
    before = live.states()[("transaction", A)].last_success_at
    report = live.cycle(subjects=(f"account {A} / transactions",))

    (result,) = report.streams
    assert result.status == "failed" and "RecordError" in result.error
    assert live.count("history_transactions") == 0
    assert live.coverage_of(f"account {A} / transactions") == []
    state = live.states()[("transaction", A)]
    # The frontier and the time of the last success are those of the cycle before.
    assert (state.frontier_ledger, state.failures, state.last_success_at) == (START, 1, before)


def test_what_fell_out_of_the_history_before_it_was_read_is_unverifiable(live: Live) -> None:
    live.cycle()
    live.fake.elder = 125  # the collector was away for longer than the source keeps
    live.fake.close(paid(127, A, B), latest=129)

    report = live.cycle()

    assert {result.status for result in report.streams} == {"verified"}
    assert all(result.frontier_ledger == 129 for result in report.streams)
    lost = "the source keeps history from ledger 125 on; the collector was further behind than that"
    assert live.coverage_of(f"account {A} / transactions") == [
        ("complete", 127, 129, None),
        ("empty", 125, 126, None),
        ("unverifiable", 119, 124, lost),
    ]
    assert live.rows("SELECT ledger_sequence FROM history_transactions ORDER BY 1") == [(127,)]


def test_a_signal_stops_the_cycle_between_requests_and_the_next_one_finishes(live: Live) -> None:
    live.cycle()
    live.fake.close(paid(120, A, B), latest=121)
    stop = threading.Event()
    limits = live.limits(stop=stop)
    check = limits.check

    def stop_after_three_requests() -> None:  # what a signal handler does, after three requests
        if live.fake.client.stats.requests >= 3:
            stop.set()
        check()

    limits.check = stop_after_three_requests  # type: ignore[method-assign]
    report = live.cycle(page_size=2, limits=limits)

    assert report.stopped and not report.failed
    assert {result.status for result in report.streams} <= {"verified", "stopped"}
    # A stream is either read to the end or left where it stood: its coverage
    # claims nothing verified above its frontier.
    states = live.states()
    for job in live.jobs():
        state = states[(job.resource_type, job.stream_key)]
        with Session(live.engine) as session:
            rows = coverage.ranges(session, job.key)
        assert all(row.state == coverage.PENDING or row.to_ledger <= state.frontier_ledger for row in rows)
    finished = live.cycle(page_size=2)
    assert {result.status for result in finished.streams} == {"verified"}
    assert all(state.frontier_ledger == 121 for state in live.states().values())


# -- the watch list changes, and who may read the source ------------------------


def test_an_account_added_later_starts_at_its_own_ledger(live: Live) -> None:
    live.cycle()
    live.fake.close(paid(120, A, B), latest=121)
    added = watch_lists.parse(
        yaml.safe_load(
            f"""
assets:
  - code: EURMTL
    issuer: {ISSUER}
markets:
  - base: MTLRECT:{ISSUER}
    counter: EURMTL:{ISSUER}
accounts:
  - address: {A}
    mode: FULL
    reason: test account
  - address: {B}
    mode: FULL
    reason: test account
  - address: {D}
    mode: FULL
    reason: added later
"""
        ),
        "test",
    )
    with Session(live.engine) as session, session.begin():
        watch_lists.apply(session, added)
    live.fake.close(paid(124, D, B), latest=125)

    report = live.cycle()

    assert set(report.started) == {f"account {D} / {stream}" for stream in ("transactions", "operations", "effects", "trades")}
    states = live.states()
    assert {state.start_ledger for key, state in states.items() if key[1] == A} == {START}
    assert {state.start_ledger for key, state in states.items() if key[1] == D} == {125}
    assert live.coverage_of(f"account {D} / transactions") == []  # its ledger 124 is history, for a backfill
    # Read in one cycle: nothing of B in ledger 119, records in 120 and 124.
    assert live.coverage_of(f"account {B} / transactions") == [
        ("complete", 120, 125, None),
        ("empty", 119, 119, None),
    ]
    assert all(state.frontier_ledger == 125 for state in states.values())


def test_one_collector_serves_a_database(make_database: Callable[[], str]) -> None:
    url = make_database()
    first, second = create_engine(url), create_engine(url)
    one, other = AdvisoryLock(first, locks.INSTANCE), AdvisoryLock(second, locks.INSTANCE)
    try:
        assert one.acquire() and one.confirm()
        assert not other.acquire(wait=0.2)
        assert locks.is_held(second, locks.INSTANCE)
        one.release()
        assert not locks.is_held(second, locks.INSTANCE)
        assert other.acquire(wait=1.0)
    finally:
        one.release()
        other.release()
        first.dispose()
        second.dispose()


def test_the_market_is_read_before_the_accounts(live: Live) -> None:
    live.cycle()
    live.fake.close(sale(120), paid(121, A, B), latest=122)

    report = live.cycle(limits=live.limits(max_requests=4))  # the root, the market's page and its two parents

    results = _results(report)
    assert report.streams[0].name == MARKET
    assert (results[MARKET].status, results[MARKET].records) == ("verified", 1)
    assert results[f"account {A} / transactions"].status == "stopped"  # the budget was spent on the market
    assert live.coverage_of(MARKET) == [("complete", 120, 122, None), ("empty", 119, 119, None)]


def test_a_market_trade_is_stored_with_its_parents_and_no_claim_about_the_context(live: Live) -> None:
    live.cycle()
    live.fake.close(sale(120), latest=121)

    report = live.cycle(subjects=(MARKET,))

    assert [result.status for result in report.streams] == ["verified"]
    operation_id = (120 << 32) | 1
    # The market is asked for as EURMTL/MTLRECT; the fake's sale is the other way round.
    assert live.rows("SELECT base_account_id, counter_account_id FROM history_trades WHERE operation_id = :id", id=operation_id) == [(D, S)]
    assert live.rows("SELECT source_account FROM history_operations WHERE id = :id", id=operation_id) == [(S,)]
    assert live.rows("SELECT ledger_sequence, context_loaded FROM history_transactions") == [(120, [])]
    paths = [path for path, _ in live.fake.requests]
    assert f"/operations/{operation_id}" in paths
    assert not any("/transactions/" in path and path.endswith("/operations") for path in paths)  # no context was fetched


def test_a_passive_offer_of_a_tracked_account_brought_its_own_operation(live: Live) -> None:
    live.cycle()
    live.fake.close(sale(120, offer_owner=A), latest=121)  # A's offer is taken by S's operation

    report = live.cycle(subjects=(f"account {A} / trades",))

    assert [result.status for result in report.streams] == ["verified"]
    operation_id = (120 << 32) | 1
    assert live.rows("SELECT source_account FROM history_operations WHERE id = :id", id=operation_id) == [(S,)]
    assert live.rows("SELECT base_account_id FROM history_trades WHERE operation_id = :id", id=operation_id) == [(A,)]


# -- the command lines ---------------------------------------------------------


def _args(*argv: str) -> Any:
    return backfill_parser().parse_args(list(argv))


@pytest.mark.parametrize(
    "argv",
    [
        ["run"],
        ["run", "--from-ledger", "1"],
        ["run", "--to-ledger", "2"],
        ["run", "--max-requests", "5"],
        ["reimport", "--max-seconds", "5"],
    ],
)
def test_a_historical_run_needs_its_window(argv: list[str]) -> None:
    with pytest.raises(ConfigError, match="say which ledgers"):
        historical_window(_args(*argv))


@pytest.mark.parametrize(
    "argv",
    [["run", "--from-ledger", "1", "--to-ledger", "2"], ["run", "--full-retention"], ["reimport", "--full-retention"]],
)
def test_a_historical_run_needs_a_budget(argv: list[str]) -> None:
    with pytest.raises(ConfigError, match="give the run a budget"):
        historical_window(_args(*argv))


def test_full_retention_and_a_window_are_alternatives() -> None:
    with pytest.raises(ConfigError, match="alternatives"):
        historical_window(_args("run", "--full-retention", "--from-ledger", "1", "--to-ledger", "2", "--max-requests", "1"))


def test_a_window_with_a_budget_is_taken_as_given() -> None:
    assert historical_window(_args("run", "--from-ledger", "5", "--to-ledger", "9", "--max-pages", "1")) == Window(5, 9)
    assert historical_window(_args("run", "--full-retention", "--max-seconds", "60")) is None
    with pytest.raises(ConfigError, match="empty ledger window"):
        historical_window(_args("run", "--from-ledger", "9", "--to-ledger", "5", "--max-requests", "1"))


def test_status_says_where_every_stream_has_got_to(live: Live, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    live.cycle()
    live.fake.close(paid(120, A, B), latest=121)
    live.cycle()
    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", live.url)
    serve(monkeypatch, live.fake)

    assert collector_cli.main(["status"]) == 0

    out = capsys.readouterr().out
    assert "global frontier 121" in out
    assert "no collector is running" in out
    assert "latest ledger 121" in out
    assert out.count("verified") == 0  # status shows frontiers, not cycle results


def test_status_tells_a_stream_that_never_ran(live: Live, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", live.url)
    serve(monkeypatch, live.fake)

    assert collector_cli.main(["status", "--subject", MARKET]) == 0

    out = capsys.readouterr().out
    assert "not started" in out and "the collector has not run since it was tracked" in out


def test_the_last_success_and_the_lag_are_kept_per_stream(live: Live) -> None:
    before = datetime.now(UTC)
    live.cycle()
    live.fake.close(paid(120, A, B), latest=121)
    live.fake.fail(f"{A}/effects", 503, times=8)
    live.cycle()

    states = live.states()
    assert states[("effect", A)].last_success_at is not None and states[("effect", A)].last_success_at >= before.replace(tzinfo=UTC)
    assert (states[("effect", A)].latest_ledger, states[("effect", A)].frontier_ledger) == (121, START)
    assert states[("transaction", A)].last_error is None and states[("transaction", A)].failures == 0


# -- more that can happen to a cycle -------------------------------------------


def test_a_rate_limited_source_ends_the_cycle_and_the_next_one_goes_on(live: Live) -> None:
    live.cycle()
    live.fake.close(paid(120, A, B), latest=121)
    live.fake.fail(f"{A}/transactions", 429, times=1, retry_after="2000")  # more than the client may wait

    report = live.cycle()

    assert report.error is not None and "rate limited" in report.error
    results = _results(report)
    assert results[MARKET].status == "verified"  # the market was read first
    assert results[f"account {A} / transactions"].status == "stopped"
    assert results[f"account {A} / transactions"].error == "rate limited by the source"
    assert {result.status for result in report.streams[2:]} == {"stopped"}  # nobody asks for more
    assert live.coverage_of(f"account {A} / transactions") == []
    with Session(live.engine) as session:
        assert "rate limited" in collector_state.source_state(session, SOURCE).last_error  # type: ignore[union-attr]

    # The stream that was refused waits as long as any failed one before it is asked again.
    live.fake.faults.clear()
    assert _results(live.cycle())[f"account {A} / transactions"].status == "waiting"
    assert {result.status for result in live.cycle(retry_base=0.0).streams} == {"verified"}
    assert live.states()[("transaction", A)].frontier_ledger == 121


def test_a_source_out_of_service_changes_nothing(live: Live) -> None:
    live.cycle()
    live.fake.close(paid(120, A, B), latest=121)
    live.fake.root_down = True

    with pytest.raises(RequestFailed):
        live.cycle()

    assert {state.frontier_ledger for state in live.states().values()} == {START}
    live.fake.root_down = False
    assert {result.status for result in live.cycle().streams} == {"verified"}


def test_streams_read_in_parallel_store_what_one_after_the_other_stores(make_database: Callable[[], str]) -> None:
    serial, parallel = Live(make_database()), Live(make_database())
    payment = Transaction(
        123, 0, A, [_payment(B, 1_000_000), _payment(C, 2_000_000), _payment(B, 3_000_000)], {A, B, C}, {1: {A, B}, 2: {A, C}, 3: {A, B}}
    )
    try:
        for run in (serial, parallel):
            run.cycle()
            run.fake.close(paid(120, A, B), payment, sale(124), latest=126)
        assert {result.status for result in serial.cycle(page_size=2).streams} == {"verified"}
        assert {result.status for result in parallel.cycle(page_size=2, concurrency=3).streams} == {"verified"}

        window = Window(START + 1, 126)
        assert checking.fingerprint(parallel.engine, window) == checking.fingerprint(serial.engine, window)
    finally:
        serial.close()
        parallel.close()


def test_an_unknown_operation_is_stored_as_it_came(live: Live) -> None:
    live.cycle()
    live.fake.close(Transaction(120, 0, A, [Operation("unknown", 99)], {A}, {1: {A}}), latest=121)

    live.cycle()

    assert live.rows("SELECT type, type_i, raw->>'type' FROM history_operations WHERE id = :id", id=(120 << 32) | 1) == [
        ("unknown", 99, "unknown")
    ]


def _until(condition: Callable[[], bool], seconds: float = 20.0) -> None:
    deadline = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < deadline, "the collector did not get there in time"
        time.sleep(0.02)


def _track(engine, address: str, reason: str) -> None:
    with Session(engine) as session, session.begin():
        watch_lists.apply(
            session, watch_lists.parse(yaml.safe_load(f"accounts:\n  - address: {address}\n    mode: FULL\n    reason: {reason}\n"), "test")
        )


def test_run_collects_cycle_after_cycle_and_picks_up_the_watch_list(
    live: Live, make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", live.url)
    serve(monkeypatch, live.fake)
    stops: list[threading.Event] = []
    monkeypatch.setattr(collector_cli, "_stop_on_signal", lambda stop: stops.append(stop))
    outcome: list[int] = []
    argv = ["run", "--interval", "0.05", "--max-seconds", "30"]
    runner = threading.Thread(target=lambda: outcome.append(collector_cli.main(argv)))
    runner.start()
    try:
        _until(lambda: len(live.states()) == 9)  # the first cycle started every stream
        live.fake.close(paid(120, A, B), latest=121)
        _until(lambda: all(state.frontier_ledger == 121 for state in live.states().values()))
        _track(live.engine, D, "added while running")  # the watch list changes as the collector runs
        _until(lambda: len(live.states()) == 13)
        live.fake.close(paid(124, D, B), sale(125), latest=126)
        _until(lambda: all(state.frontier_ledger == 126 for state in live.states().values()))
    finally:
        if stops:
            stops[0].set()  # what SIGTERM does
        runner.join(30)
    assert not runner.is_alive() and outcome == [collector_cli.OK]

    states = live.states()
    assert {state.start_ledger for key, state in states.items() if key[1] != D} == {START}
    assert {state.start_ledger for key, state in states.items() if key[1] == D} == {121}
    # two payments, and the transaction of the market trade that came with it
    assert live.count("history_trades") == 1 and live.count("history_transactions") == 3

    # A restart goes on where it stood: the starts are not moved. (`run`
    # closed its pool, as a command does; the next one builds a new one.)
    assert collector_cli.main(["once"]) == collector_cli.OK
    assert {state.start_ledger for state in live.states().values()} == {START, 121}
    out = capsys.readouterr().out
    assert "global frontier 126" in out

    # Cycle after cycle stores what one read of the same watch list stores.
    # (The reference reads with D tracked from the start, so its participants
    # can be compared; the streams of D itself are compared by their records.)
    whole = Live(make_database())
    try:
        _track(whole.engine, D, "tracked from the start")
        whole.fake.close(paid(120, A, B), paid(124, D, B), sale(125), latest=126)
        assert {result.status for result in whole.cycle(start_from=START).streams} == {"verified"}
        window = Window(START + 1, 126)
        assert checking.fingerprint(live.engine, window) == checking.fingerprint(whole.engine, window)
    finally:
        whole.close()


def test_once_refuses_while_another_collector_serves(live: Live, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", live.url)
    serve(monkeypatch, live.fake)
    holder = AdvisoryLock(live.engine, locks.INSTANCE)
    assert holder.acquire()
    try:
        assert collector_cli.main(["once"]) == collector_cli.FAILED
        assert live.fake.requests == []  # nothing was asked of the source
        assert live.states() == {}
    finally:
        holder.release()


def test_a_cycle_that_finds_nothing_new_asks_for_the_root_alone_for_as_long_as_it_runs(live: Live) -> None:
    live.cycle()
    for _ in range(6):  # the client refuses a page asked for over and over; a cycle is one sequence
        assert {result.status for result in live.cycle().streams} == {"verified"}
    assert [path for path, _ in live.fake.requests] == ["/"] * 7
    with Session(live.engine) as session:
        assert collector_state.source_state(session, SOURCE).cycles == 7  # type: ignore[union-attr]



# -- the check before a source is trusted ---------------------------------------


def test_check_reads_forwards_and_finds_the_source_well_behaved(live: Live) -> None:
    live.fake.latest = 130

    findings = check.ascending(live.fake.client, account=A, base_asset_id=RECT, counter_asset_id=EUR, window=20)

    names = [finding.name for finding in findings]
    assert names == [
        "source",
        "transactions: forwards from ledger 111",
        "transactions: after its last record",
        "operations: forwards from ledger 111",
        "effects: forwards from ledger 111",
        "trades: forwards from ledger 111",
        "market trades: forwards from ledger 111",
        "market trades: after its last record",
        "a position below the history kept",
    ]
    assert all(finding.ok for finding in findings), [f for f in findings if not f.ok]
    assert live.fake.client.stats.requests == 9
    assert live.count("collector_stream") == 0  # nothing is stored


def test_check_reports_a_record_below_where_reading_starts(live: Live) -> None:
    live.fake.latest = 130
    stale = next(iter(live.fake.ledger.account_transactions[A]))  # a transaction of ledger 100

    def prepend(path: str, body: dict[str, Any]) -> dict[str, Any]:
        if path == f"/accounts/{A}/transactions":
            body["_embedded"]["records"].insert(0, stale)
        return body

    live.fake.tamper = prepend

    findings = check.ascending(live.fake.client, account=A, base_asset_id=RECT, counter_asset_id=EUR, window=20)

    broken = [finding for finding in findings if not finding.ok]
    assert [finding.name for finding in broken] == [
        "transactions: forwards from ledger 111",
        "transactions: after its last record",  # the page after the last record is not empty any more
    ]
    assert "below ledger 111" in broken[0].detail
