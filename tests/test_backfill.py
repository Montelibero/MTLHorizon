"""The backfill against the fake Horizon, stored in PostgreSQL."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pytest
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.backfill import config as watch_lists
from app.backfill import verify as checking
from app.backfill.cli import forget
from app.backfill.jobs import StreamJob, stream_jobs
from app.backfill.loader import Backfill, Limits, StreamReport, Window
from app.sources.horizon_cursors import after_ledger
from app.storage import coverage
from tests.fake_horizon import A, B, C, D, EUR, ISSUER, RECT, S, T0, FakeHorizon

SOURCE = "horizon:horizon.test"
WINDOW = Window(100, 130)
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
    mode: CANDIDATE
    reason: not loaded
"""
    ),
    "test",
)
TRANSACTION = re.compile(r"/transactions/[0-9a-f]{64}")
OPERATION = re.compile(r"/operations/[0-9]+")


def toid(ledger: int, order: int = 0, operation: int = 0) -> int:
    return (ledger << 32) | (order << 12) | operation


def _t7_hash(pilot: Pilot) -> str:
    """The transaction of the market trade that no tracked account takes part in."""
    return pilot.fake.ledger.operations[str(T7_FIRST_OPERATION)]["transaction_hash"]


T1, T3, T4, T5, T6 = toid(100), toid(104, 1), toid(108), toid(110, 2), toid(118)
T7_FIRST_OPERATION = toid(112, 0, 1)
HISTORY_KEPT = "source keeps history from ledger"


class Pilot:
    """A private database with the watch list imported, and a fake source."""

    def __init__(self, url: str) -> None:
        self.engine = create_engine(url)
        self.fake = FakeHorizon()
        self.contexts = (0, 0)
        with Session(self.engine) as session, session.begin():
            watch_lists.apply(session, WATCH_LIST)

    def close(self) -> None:
        self.engine.dispose()

    def jobs(self, *parts: str) -> list[StreamJob]:
        with Session(self.engine) as session:
            jobs, left_out = stream_jobs(session)
        self.left_out = left_out
        return [job for job in jobs if all(part in job.name for part in parts)]

    def run(
        self,
        window: Window = WINDOW,
        *,
        subjects: Sequence[str] = (),
        page_size: int = 200,
        concurrency: int = 1,
        stop_after: int | None = None,
        **limits: Any,
    ) -> list[StreamReport]:
        limits_ = Limits(requests=lambda: self.fake.client.stats.requests, **limits)
        with Backfill(self.fake.client, self.engine, source=SOURCE, page_size=page_size, limits=limits_) as backfill:
            if stop_after is not None:
                first_request = self.fake.client.stats.requests

                def before_request() -> None:  # what a signal handler does, after n requests
                    if self.fake.client.stats.requests - first_request >= stop_after:
                        limits_.stop.set()
                    limits_.check()

                backfill.client.before_request = before_request
            reports = backfill.run(window, self.jobs(*subjects), concurrency=concurrency)
            self.contexts = (backfill.contexts.fetched, backfill.contexts.reused)
        return reports

    def coverage_of(self, part: str) -> list[tuple[str, int, int, str | None]]:
        """State, from, to and detail of every range of the streams named."""
        with Session(self.engine) as session:
            rows = [row for job in self.jobs(part) for row in coverage.ranges(session, job.key)]
        return sorted((row.state, row.from_ledger, row.to_ledger, row.detail) for row in rows)

    def rows(self, statement: str, **params: Any) -> list[Any]:
        with self.engine.connect() as connection:
            return connection.execute(text(statement), params).all()

    def count(self, table: str) -> int:
        return self.rows(f"SELECT count(*) FROM {table}")[0][0]

    def asked_for_transactions(self, since: int = 0) -> list[str]:
        return [path for path, _ in self.fake.requests[since:] if TRANSACTION.fullmatch(path)]


@pytest.fixture
def pilot(make_database: Callable[[], str]) -> Iterator[Pilot]:
    instance = Pilot(make_database())
    yield instance
    instance.close()


# -- loading a window ----------------------------------------------------------


def test_the_window_is_loaded_and_matches_the_source(pilot: Pilot) -> None:
    reports = pilot.run()

    assert {report.status for report in reports} == {"verified"}, [report.error for report in reports]
    assert [pilot.count(table) for table in ("history_ledgers", "history_transactions", "history_operations", "history_effects", "history_trades")] == [6, 6, 7, 12, 3]
    comparisons = checking.verify(pilot.fake.client, pilot.engine, pilot.jobs(), WINDOW, 200)
    assert [comparison.as_dict() for comparison in comparisons if not comparison.matches] == []


def test_every_stream_is_covered_and_an_empty_one_says_so(pilot: Pilot) -> None:
    pilot.run()

    assert pilot.coverage_of(f"account {A} / transactions") == [("complete", 100, 130, None)]
    assert pilot.coverage_of(f"account {A} / trades") == [("empty", 100, 130, None)]
    assert pilot.coverage_of(f"account {B} / trades") == [("complete", 100, 130, None)]
    assert pilot.coverage_of("market") == [("complete", 100, 130, None)]


def test_a_window_without_records_is_verified_empty(pilot: Pilot) -> None:
    reports = pilot.run(Window(119, 130))

    assert {report.status for report in reports} == {"verified"}
    assert {row[0] for row in pilot.coverage_of("")} == {"empty"}
    assert pilot.count("history_transactions") == 0


def test_reading_starts_at_the_upper_bound_and_stays_below_it(pilot: Pilot) -> None:
    pilot.run(Window(100, 110))

    pilot.jobs()
    market = [query for query in pilot.fake.asked("/trades") if "base_asset_type" in query]
    starts = [pilot.fake.asked(f"{A}/transactions")[0], pilot.fake.asked(f"{B}/transactions")[0], market[0]]
    assert all(query["cursor"] == str(after_ledger(110)) for query in starts)
    assert pilot.rows("SELECT count(*) FROM history_transactions WHERE ledger_sequence > 110") == [(0,)]
    assert pilot.rows("SELECT count(*) FROM history_trades WHERE (operation_id >> 32) > 110") == [(0,)]
    assert all(row[2] <= 110 for row in pilot.coverage_of(""))


def test_a_second_run_of_a_verified_window_reads_nothing(pilot: Pilot) -> None:
    pilot.run()
    before = checking.fingerprint(pilot.engine, WINDOW)
    requests = len(pilot.fake.requests)

    reports = pilot.run()

    assert all(report.status == "verified" and report.pages == 0 for report in reports)
    assert checking.fingerprint(pilot.engine, WINDOW) == before
    assert pilot.fake.requests[requests:] == []  # not one request


# -- stopping, resuming, the source moving under the run -----------------------


def test_a_stopped_run_resumes_where_it_stood(pilot: Pilot) -> None:
    stopped = pilot.run(page_size=2, max_pages=1)
    assert stopped[0].status == "stopped" and stopped[0].pages == 1
    assert pilot.coverage_of(f"account {A} / transactions") == [("complete", 111, 130, None), ("pending", 100, 110, None)]
    assert pilot.rows("SELECT direction, resource_type, cursor, cursor_ledger FROM sync_state") == [("desc", "transaction", str(toid(110, 2)), 110)]

    reports = pilot.run(page_size=2)

    assert {report.status for report in reports} == {"verified"}
    assert pilot.coverage_of(f"account {A} / transactions") == [("complete", 100, 130, None)]
    assert pilot.count("history_transactions") == 6


def test_a_resumed_run_stores_what_an_uninterrupted_run_stores(make_database: Callable[[], str]) -> None:
    resumed, whole = Pilot(make_database()), Pilot(make_database())
    try:
        # Stopped after one page, then after two more; then continued with pages of five.
        assert resumed.run(page_size=2, max_pages=1)[0].status == "stopped"
        assert any(report.status == "stopped" for report in resumed.run(page_size=2, max_pages=2))
        assert {report.status for report in resumed.run(page_size=5)} == {"verified"}
        assert {report.status for report in whole.run(page_size=3)} == {"verified"}

        assert checking.fingerprint(resumed.engine, WINDOW) == checking.fingerprint(whole.engine, WINDOW)
    finally:
        resumed.close()
        whole.close()


def test_ledgers_the_source_dropped_before_they_were_read_are_not_complete(pilot: Pilot) -> None:
    pilot.run(page_size=2, max_pages=1)  # transactions of A are read down to ledger 110, the rest is pending
    pilot.fake.elder = 105  # the source reaps ledgers below 105 meanwhile; no 410 will say so

    reports = pilot.run(page_size=2)

    assert {report.status for report in reports} == {"verified"}
    assert pilot.coverage_of(f"account {A} / transactions") == [
        ("complete", 105, 130, None),
        ("unverifiable", 100, 104, HISTORY_KEPT + " 105 (history_latest_ledger 130)"),
    ]
    assert pilot.rows("SELECT count(*) FROM history_transactions WHERE ledger_sequence < 105") == [(0,)]


def test_a_window_below_the_history_of_the_source(pilot: Pilot) -> None:
    reports = pilot.run(Window(95, 130))

    assert {report.status for report in reports} == {"verified"}
    assert pilot.coverage_of(f"account {A} / effects") == [
        ("complete", 100, 130, None),
        ("unverifiable", 95, 99, HISTORY_KEPT + " 100 (history_latest_ledger 130)"),
    ]


def test_a_410_on_the_first_transaction_of_the_oldest_ledger_completes_it(pilot: Pilot) -> None:
    """Read with pages of two, stop, go on: the page after the last record of
    the oldest ledger kept gets 410, and the ledger counts as read to its end
    (the record is the first transaction of it)."""
    pilot.run(page_size=2, max_pages=3)

    reports = pilot.run(page_size=2)

    assert reports[0].status == "verified"
    assert pilot.coverage_of(f"account {A} / transactions") == [("complete", 100, 130, None)]


def test_a_window_entirely_below_the_history_of_the_source(pilot: Pilot) -> None:
    reports = pilot.run(Window(90, 99), subjects=[f"account {A} / transactions"])  # the first page gets 410

    assert [report.status for report in reports] == ["verified"]
    assert pilot.coverage_of(f"account {A} / transactions") == [
        ("unverifiable", 90, 99, HISTORY_KEPT + " 100 (history_latest_ledger 130)")
    ]
    assert pilot.count("history_transactions") == 0
    assert pilot.count("sync_state") == 0


def _reap_before_second_page(pilot: Pilot, elder: int) -> None:
    """Make the source drop its older history between the first page of A's
    transactions (ledgers 118 and 110) and the request for the second."""
    asked = 0

    def reap(path: str) -> None:
        nonlocal asked
        if path == f"/accounts/{A}/transactions":
            asked += 1
            if asked == 2:
                pilot.fake.elder = elder

    pilot.fake.before_answer = reap


def test_the_end_of_history_in_the_middle_of_a_range(pilot: Pilot) -> None:
    """The history moves up past the last record read: the next page gets 410.

    That answer does not say whether ledger 110 had more records below the
    one stored, so nothing from that ledger to the bottom of the range is
    claimed.
    """
    _reap_before_second_page(pilot, elder=111)

    reports = pilot.run(page_size=2, subjects=[f"account {A} / transactions"])

    assert [report.status for report in reports] == ["verified"]
    # Above the reaped ledger nothing changed: 111 up was verified by the first page.
    assert pilot.coverage_of(f"account {A} / transactions") == [
        ("complete", 111, 130, None),
        ("unverifiable", 100, 110, HISTORY_KEPT + " 111 (history_latest_ledger 130); ledger 110 was read only down to its top"),
    ]
    assert pilot.rows("SELECT ledger_sequence FROM history_transactions ORDER BY 1") == [(110,), (118,)]


def test_history_moving_up_to_the_last_record_read_is_not_a_410(pilot: Pilot) -> None:
    """The history now starts at the ledger of the last record read: the next
    page is simply empty. That proves that ledger is read to its end, and
    nothing about the ledgers below it."""
    _reap_before_second_page(pilot, elder=110)

    reports = pilot.run(page_size=2, subjects=[f"account {A} / transactions"])

    assert [report.status for report in reports] == ["verified"]
    assert pilot.coverage_of(f"account {A} / transactions") == [
        ("complete", 110, 130, None),
        ("unverifiable", 100, 109, HISTORY_KEPT + " 110 (history_latest_ledger 130)"),
    ]


def test_history_moving_between_a_page_and_its_context_fails_the_page(pilot: Pilot) -> None:
    """Rarer still: the ledger of a record just read is reaped before the
    record's transaction can be fetched whole. The page is not stored; the
    next run reads what is left and marks the rest."""

    def reap(path: str) -> None:
        if path.startswith("/transactions/"):  # the page is in, its context is being fetched
            pilot.fake.elder = 111

    pilot.fake.before_answer = reap
    reports = pilot.run(page_size=2, subjects=[f"account {A} / transactions"])

    assert [(report.status, report.error[:13]) for report in reports] == [("failed", "BeforeHistory")]
    assert pilot.coverage_of(f"account {A} / transactions") == [("pending", 100, 130, None)]
    assert pilot.count("history_transactions") == 0 and pilot.count("sync_state") == 0

    pilot.fake.before_answer = None
    reports = pilot.run(page_size=2, subjects=[f"account {A} / transactions"])

    assert [report.status for report in reports] == ["verified"]
    assert pilot.coverage_of(f"account {A} / transactions") == [
        ("complete", 111, 130, None),
        ("unverifiable", 100, 110, HISTORY_KEPT + " 111 (history_latest_ledger 130)"),
    ]
    assert pilot.rows("SELECT ledger_sequence FROM history_transactions ORDER BY 1") == [(118,)]


def test_a_pending_range_wider_than_the_window_is_read_only_inside_it(pilot: Pilot) -> None:
    pilot.run(Window(90, 130), subjects=[f"account {A} / transactions"], max_pages=0)  # planned, not read

    reports = pilot.run(Window(108, 130), page_size=2, subjects=[f"account {A} / transactions"])

    assert [report.status for report in reports] == ["verified"]
    assert pilot.coverage_of(f"account {A} / transactions") == [
        ("complete", 108, 130, None),
        ("pending", 90, 107, None),
    ]
    # The page reached below the window; its records from there were not stored.
    assert pilot.rows("SELECT ledger_sequence FROM history_transactions ORDER BY 1") == [(108,), (110,), (118,)]


def test_reimport_of_a_window_leaves_what_is_outside_it_alone(pilot: Pilot) -> None:
    subjects = [f"account {A} / transactions"]
    pilot.run(Window(90, 130), subjects=subjects, max_pages=0)
    pilot.run(Window(104, 125), page_size=2, subjects=subjects)
    assert pilot.coverage_of(subjects[0]) == [
        ("complete", 104, 125, None),
        ("pending", 90, 103, None),
        ("pending", 126, 130, None),
    ]

    # Forgetting the window merges all three into one pending range: the run
    # that follows must still read the window and nothing else.
    forget(pilot.engine, pilot.jobs(*subjects), Window(104, 125), SOURCE)
    assert pilot.coverage_of(subjects[0]) == [("pending", 90, 130, None)]
    reports = pilot.run(Window(104, 125), page_size=2, subjects=subjects)

    assert [report.status for report in reports] == ["verified"]
    assert pilot.coverage_of(subjects[0]) == [
        ("complete", 104, 125, None),
        ("pending", 90, 103, None),
        ("pending", 126, 130, None),
    ]
    assert pilot.rows("SELECT ledger_sequence FROM history_transactions ORDER BY 1") == [(108,), (110,), (118,)]


def test_a_missing_parent_fails_the_stream_and_leaves_it_pending(pilot: Pilot) -> None:
    pilot.fake.hidden_operations = {str(T7_FIRST_OPERATION)}  # a trade names an operation the source cannot find

    reports = pilot.run(subjects=["market"])

    assert [(report.status, report.error[:8]) for report in reports] == [("failed", "NotFound")]
    assert pilot.coverage_of("market") == [("pending", 100, 130, None)]
    assert pilot.count("history_trades") == 0  # the page was not stored
    assert pilot.count("sync_state") == 0  # and no checkpoint was written

    pilot.fake.hidden_operations.clear()
    assert {report.status for report in pilot.run(subjects=["market"])} == {"verified"}


def test_a_page_out_of_order_is_not_stored(pilot: Pilot) -> None:
    def reverse(path: str, body: dict[str, Any]) -> dict[str, Any]:
        if path == f"/accounts/{A}/transactions":
            body["_embedded"]["records"].reverse()
        return body

    pilot.fake.tamper = reverse
    reports = pilot.run(subjects=[f"account {A} / transactions"])

    (report,) = reports
    assert report.status == "failed" and "records are not strictly descending" in report.error
    assert pilot.coverage_of(f"account {A} / transactions") == [("pending", 100, 130, None)]
    assert pilot.count("history_transactions") == 0


def test_a_page_that_repeats_a_record_read_before_is_not_stored(pilot: Pilot) -> None:
    """The next page starts at the first, not the last, record of the page before."""

    def repeat_first(path: str, body: dict[str, Any]) -> dict[str, Any]:
        records = body["_embedded"]["records"]
        if path == f"/accounts/{A}/transactions" and len(records) == 2:
            link = urlsplit(body["_links"]["next"]["href"])
            query = [(key, records[0]["paging_token"] if key == "cursor" else value) for key, value in parse_qsl(link.query)]
            body["_links"]["next"]["href"] = urlunsplit(link._replace(query=urlencode(query)))
        return body

    pilot.fake.tamper = repeat_first
    reports = pilot.run(page_size=2, subjects=[f"account {A} / transactions"])

    (report,) = reports
    assert report.status == "failed" and "read already" in report.error
    # The first page is stored and verified; the second is not stored at all.
    assert pilot.coverage_of(f"account {A} / transactions") == [
        ("complete", 111, 130, None),
        ("pending", 100, 110, None),
    ]
    assert pilot.rows("SELECT count(*) FROM history_transactions") == [(2,)]


# -- what is stored -------------------------------------------------------------


def test_a_trade_met_in_the_other_orientation_is_stored_once(pilot: Pilot) -> None:
    pilot.run()

    stored = pilot.rows(
        "SELECT base_asset_id, base_account_id, base_is_seller, price_n, price_d FROM history_trades WHERE id = :id",
        id=f"{T7_FIRST_OPERATION}-0",
    )
    assert stored == [(RECT, C, True, 47, 10)]  # in the orientation it was first met in
    assert pilot.rows(
        "SELECT count(*) FROM history_trades WHERE (base_asset_id, counter_asset_id) IN ((:a, :b), (:b, :a))", a=EUR, b=RECT
    ) == [(2,)]


def test_the_context_of_a_transaction_is_fetched_once(pilot: Pilot) -> None:
    pilot.run()

    # Six transactions were met; their records came with the streams for all but one.
    fetched, reused = pilot.contexts
    assert fetched == 6 and reused > 0  # met again by other streams, read once
    assert pilot.asked_for_transactions() == ["/transactions/" + _t7_hash(pilot)]
    assert len([path for path, _ in pilot.fake.requests if OPERATION.fullmatch(path)]) == 1  # the market trade no account led to


def test_a_context_stored_by_an_earlier_run_is_not_fetched_again(pilot: Pilot) -> None:
    pilot.run()
    forget(pilot.engine, pilot.jobs("market"), WINDOW, SOURCE)
    requests = len(pilot.fake.requests)

    reports = pilot.run(subjects=["market"])

    assert {report.status for report in reports} == {"verified"}
    # Neither the transactions nor their operations were asked for again.
    assert pilot.asked_for_transactions(requests) == []
    assert not [path for path, _ in pilot.fake.requests[requests:] if OPERATION.fullmatch(path)]


def test_unknown_types_and_missing_meta_are_kept(pilot: Pilot) -> None:
    pilot.run()

    assert pilot.rows("SELECT type, type_i FROM history_operations WHERE id = :id", id=toid(110, 2, 1)) == [("unknown", 99)]
    assert pilot.rows("SELECT result_meta_xdr, context_loaded FROM history_transactions WHERE id = :id", id=T1) == [(None, ["operations", "effects", "trades"])]
    assert pilot.rows("SELECT result_meta_xdr FROM history_transactions WHERE id = :id", id=T5) == [("AAAAAA==",)]


def test_failed_transactions_and_their_operations_are_included(pilot: Pilot) -> None:
    pilot.run()

    assert pilot.rows("SELECT successful FROM history_transactions WHERE id = :id", id=T4) == [(False,)]
    assert pilot.rows("SELECT raw->>'transaction_successful' FROM history_operations WHERE transaction_id = :id", id=T4) == [("false",)]
    assert pilot.rows("SELECT count(*) FROM history_effects WHERE (operation_id >> 32) = 108") == [(0,)]


def test_a_fee_bump_transaction_keeps_both_hashes_and_both_fees(pilot: Pilot) -> None:
    pilot.run()

    row = pilot.rows(
        "SELECT transaction_hash, inner_transaction_hash, source_account, fee_source_account, max_fee, new_max_fee, signatures, inner_signatures FROM history_transactions WHERE id = :id",
        id=T3,
    )[0]
    assert row[0] != row[1]
    assert (row[2], row[3], row[4], row[5]) == (B, S, 10000, 20000)  # inner fee, fee of the fee bump
    assert row[6] == ["sig-of-" + B[:6]] and row[7] == ["inner"]


def test_participation_is_what_horizon_states_or_the_operation_bodies_say(pilot: Pilot) -> None:
    pilot.run()

    transactions_participants = set(pilot.rows("SELECT transaction_id, account_id FROM history_transaction_participants"))
    assert {(T1, A), (T1, B), (T3, S), (T6, A), (T6, C)} <= transactions_participants
    # D was only the other side of a trade: the REST records name it there, which does not make it a participant.
    assert not [row for row in transactions_participants if row[1] == D]
    assert (toid(118, 0, 1), A) in set(pilot.rows("SELECT operation_id, account_id FROM history_operation_participants"))
    assert pilot.rows("SELECT count(*) FROM history_accounts WHERE account_id = :d", d=D) == [(1,)]  # seen, so registered
    assert pilot.rows("SELECT count(*) FROM tracked_accounts WHERE account_id = :d", d=D) == [(0,)]  # but not watched


def test_an_account_that_is_not_full_is_not_read(pilot: Pilot) -> None:
    jobs = pilot.jobs()

    assert not any(C in job.name for job in jobs)
    assert pilot.left_out == [f"account {C}: mode CANDIDATE; history is loaded only for FULL"]
    pilot.run()
    assert pilot.rows("SELECT count(*) FROM history_transactions WHERE id = :id", id=toid(100, 3)) == [(0,)]


# -- reading again, in parallel, measuring ---------------------------------------


def test_reimport_reads_the_window_again_and_stores_the_same(pilot: Pilot) -> None:
    pilot.run()
    before = checking.fingerprint(pilot.engine, WINDOW)

    forget(pilot.engine, pilot.jobs(), WINDOW, SOURCE)
    reports = pilot.run()

    assert sum(report.pages for report in reports) == 9
    assert pilot.contexts[0] == 0 and pilot.contexts[1] > 0  # contexts already stored were not fetched again
    assert checking.fingerprint(pilot.engine, WINDOW) == before


def test_streams_read_in_parallel_store_the_same(pilot: Pilot) -> None:
    pilot.run()
    expected = checking.fingerprint(pilot.engine, WINDOW)

    forget(pilot.engine, pilot.jobs(), WINDOW, SOURCE)
    assert {report.status for report in pilot.run(concurrency=4, page_size=3)} == {"verified"}
    assert checking.fingerprint(pilot.engine, WINDOW) == expected


def test_reading_a_window_moves_on_to_the_next_page(pilot: Pilot) -> None:
    """The market has two pages of trades in the window when pages hold one
    record: the second page is asked for once, not the first one again."""
    market = pilot.jobs("market")[0]

    records = [record["id"] for record in checking.read_window(pilot.fake.client, market, WINDOW, 1)]

    assert len(records) == 2 and len(set(records)) == 2
    asked = [query.get("cursor") for query in pilot.fake.asked("/trades")]
    assert len(asked) == len(set(asked)), asked  # no page was asked for twice


def test_estimate_counts_pages_without_reading_contexts(pilot: Pilot) -> None:
    jobs = pilot.jobs(f"account {A} / transactions") + pilot.jobs("market")

    estimates = checking.estimate(pilot.fake.client, jobs, WINDOW, 200)

    assert [(e.pages, e.records, e.transactions, e.context_requests_upper_bound) for e in estimates] == [
        (1, 4, 4, 12),  # four transactions, three requests each
        (1, 2, 2, 10),  # two trades, each found through its operation: five requests at most
    ]
    assert not [path for path, _ in pilot.fake.requests if TRANSACTION.fullmatch(path) or OPERATION.fullmatch(path)]


def test_the_stored_market_sums_to_what_the_source_aggregates(pilot: Pilot) -> None:
    first_day = int(T0.timestamp() * 1000)

    assert checking.compare_aggregations(pilot.fake.client, pilot.engine, EUR, RECT, first_day, 1)[0].matches is False
    pilot.run()
    bucket = checking.compare_aggregations(pilot.fake.client, pilot.engine, EUR, RECT, first_day, 1)[0]

    assert bucket.matches
    assert bucket.source == {"trades": 2, "base_volume": 2 * 231829549, "counter_volume": 2 * 49325436}


# -- a page and its checkpoint stand or fall together ---------------------------


def test_a_page_that_cannot_be_stored_leaves_nothing_behind(pilot: Pilot) -> None:
    from app.backfill.store import store_transaction
    from tests.fake_horizon import history

    # The same transaction, stored already with another fee: the page will be
    # refused in the middle, after its first record was written.
    t5 = next(tx for tx in history() if tx.id == T5)
    with Session(pilot.engine) as session, session.begin():
        store_transaction(session, {**pilot.fake.ledger.transactions[t5.hash], "max_fee": "777"})

    reports = pilot.run(page_size=2, subjects=[f"account {A} / transactions"])

    assert [(report.status, report.error[:15]) for report in reports] == [("failed", "ContentConflict")]
    assert pilot.rows("SELECT count(*) FROM history_transactions WHERE id = :id", id=toid(118)) == [(0,)]  # the record before it
    assert pilot.rows("SELECT max_fee FROM history_transactions WHERE id = :id", id=T5) == [(777,)]  # not overwritten either
    assert pilot.coverage_of(f"account {A} / transactions") == [("pending", 100, 130, None)]
    assert pilot.count("sync_state") == 0


def test_a_signal_stops_the_run_between_requests(pilot: Pilot) -> None:
    """Pages of two: the first page needs five requests (the page, two
    transactions with operations and effects each); the signal comes after
    the fifth, so the page is stored and the next one is never asked for."""
    reports = pilot.run(page_size=2, stop_after=6)

    assert reports[0].status == "stopped" and "stopped on request" in reports[0].error
    assert {report.status for report in reports} == {"stopped"}
    assert pilot.coverage_of(f"account {A} / transactions") == [("complete", 111, 130, None), ("pending", 100, 110, None)]
    assert pilot.rows("SELECT cursor_ledger FROM sync_state") == [(110,)]  # the page and its checkpoint, together
