"""The activity of the watched accounts against a real PostgreSQL: what the
fake Horizon holds, loaded by the backfill, counted by ledger time."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.backfill import config as watch_lists
from app.backfill.jobs import stream_jobs
from app.backfill.loader import Backfill, StreamReport, Window
from app.sources.horizon_records import assets_in
from app.stats import activity
from app.stats.dashboard import TtlCache
from app.storage import coverage
from app.storage.coverage import CoverageKey
from tests import factories
from tests.fake_horizon import (
    CREDIT,
    DATA_CREATED,
    DEBIT,
    A,
    B,
    C,
    D,
    S,
    ISSUER,
    EUR,
    MTL,
    FakeHorizon,
    Ledger,
    Operation,
    Transaction,
    amount,
    asset,
    close_time,
    sale,
    trust,
)
from tests.test_dashboard_stats import Statements

KINDS = activity.KINDS
WATCHED = (A, B, C, D, S, ISSUER)
SOURCE = "horizon:pool"
MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)
ZERO = dict.fromkeys(KINDS, 0)

# The fake closes a ledger every five seconds, from its elder ledger on.
PER_DAY = 24 * 60 * 60 // 5
FIRST = 100
LATEST = FIRST + 47 * PER_DAY


def day(number: int, tick: int = 0) -> int:
    """The ledger that closes ``number`` whole days after the first one, plus ``tick`` slots of five seconds."""
    return FIRST + number * PER_DAY + tick


def closed(number: int, tick: int = 0) -> datetime:
    return close_time(day(number, tick))


NOW = closed(45)  # 2026-02-15: the week is 8–15 February, the month 15 January – 15 February


# -- a history to load ---------------------------------------------------------


def payment(to: str, asset_id: str = EUR, stroops: int = 1_000_000) -> Operation:
    return Operation(
        "payment",
        1,
        None,
        {"to": to, "amount": amount(stroops), **asset("", asset_id)},
        effects=[
            {"account": to, "type": "account_credited", "type_i": CREDIT, "amount": amount(stroops), **asset("", asset_id)},
            {"account": None, "type": "account_debited", "type_i": DEBIT, "amount": amount(stroops), **asset("", asset_id)},
        ],
    )


def pays(ledger: int, source: str, to: str, asset_id: str = EUR) -> Transaction:
    """A transaction of ``source`` paying ``to``."""
    return Transaction(ledger, 0, source, [payment(to, asset_id)], {source, to}, {1: {source, to}})


def creates(ledger: int, source: str, account: str) -> Transaction:
    """``source`` creates ``account``: an account_created effect is stored for it."""
    operation = Operation(
        "create_account",
        0,
        None,
        {"account": account, "destination": account, "starting_balance": amount(100_000_000)},
        effects=[
            {"account": account, "type": "account_created", "type_i": 0, "starting_balance": amount(100_000_000)},
            {"account": source, "type": "account_debited", "type_i": DEBIT, "amount": amount(100_000_000), "asset_type": "native"},
        ],
    )
    return Transaction(ledger, 0, source, [operation], {source, account}, {1: {source, account}})


def entry(account: str) -> Operation:
    """A data entry: an operation that names no asset."""
    return Operation(
        "manage_data",
        10,
        None,
        {"name": "config", "value": "aGVsbG8="},
        effects=[{"account": account, "type": "data_created", "type_i": DATA_CREATED, "name": "config", "value": "aGVsbG8="}],
    )


def history() -> list[Transaction]:
    """What the fake holds. D and the issuer are created in the stored history,
    so that what they had a trustline to can be proved; A, B, C and S are
    older than that, and for them nothing can."""
    return [
        pays(day(3), A, B),  # older than a month
        creates(day(15), S, D),
        creates(day(15, 1), S, ISSUER),
        Transaction(day(20), 0, D, [trust(D, EUR), trust(D, MTL)], {D}, {1: {D}, 2: {D}}),
        pays(day(21), C, A),  # in the month, in neither week
        # the week before
        pays(day(32), A, B),
        pays(day(33), A, B),
        pays(day(34), C, D),
        # the week
        pays(day(39), A, B),  # shared by two watched accounts
        pays(day(39, 1), ISSUER, A),  # an asset the payer issues: own
        pays(day(40), A, C),
        pays(day(40, 1), ISSUER, A, "native"),  # native is foreign to everybody
        pays(day(41), D, B, "native"),
        pays(day(41, 1), D, C),  # EUR, a line D opened at day 20: own
        Transaction(day(42), 0, B, [trust(B, EUR), entry(B)], {B}, {1: {B}, 2: {B}}),
        sale(day(43), offer_owner=D),  # D is a side of the trade and of nothing else in it
        pays(day(44, PER_DAY // 2), A, B),  # the last day
        pays(day(45, 1), A, B),  # after now: in no window, but the last event of A and B
    ]


def watch_list(accounts: tuple[str, ...]) -> Any:
    return watch_lists.parse({"accounts": [{"address": account, "enabled": True, "reason": "for the test"} for account in accounts]}, "test")


class Yard:
    """A private database with the watch list, a fake Horizon holding what it
    was given, and that loaded by the backfill."""

    def __init__(self, url: str, transactions: list[Transaction], accounts: tuple[str, ...] = WATCHED) -> None:
        self.engine = create_engine(url)
        self.fake = FakeHorizon(elder=FIRST, latest=LATEST)
        self.fake.history = transactions
        self.fake.ledger = Ledger(transactions)
        with Session(self.engine) as session, session.begin():
            watch_lists.apply(session, watch_list(accounts))
        with Session(self.engine) as session:
            self.jobs, _ = stream_jobs(session)
        self.reports: list[StreamReport] = self.load(Window(FIRST, LATEST))

    def load(self, window: Window) -> list[StreamReport]:
        with Backfill(self.fake.client, self.engine, source=SOURCE, page_size=200) as backfill:
            return backfill.run(window, self.jobs)

    def close(self) -> None:
        self.engine.dispose()

    def rows(self, statement: str, **parameters: Any) -> list[Any]:
        with self.engine.connect() as connection:
            return connection.execute(text(statement), parameters).all()

    def one(self, statement: str, **parameters: Any) -> Any:
        return self.rows(statement, **parameters)[0][0]

    def forget(self, account: str, from_ledger: int, to_ledger: int) -> None:
        """Make the effects of ``account`` in [from, to] not verified."""
        key = CoverageKey("account", account, "effects", "FULL", 1)
        with Session(self.engine) as session, session.begin():
            coverage.invalidate(session, key, SOURCE, from_ledger, to_ledger)


@pytest.fixture
def make_yard(make_database: Callable[[], str]) -> Iterator[Callable[..., Yard]]:
    made: list[Yard] = []

    def make(transactions: list[Transaction] | None = None, **options: Any) -> Yard:
        yard = Yard(make_database(), history() if transactions is None else transactions, **options)
        made.append(yard)
        return yard

    yield make
    for yard in made:
        yard.close()


@pytest.fixture
def yard(make_yard: Callable[..., Yard]) -> Yard:
    return make_yard()


# -- the other way round: by the ledger close time stored on the records -------


def reference(engine: Engine, start: datetime, end: datetime, accounts: list[str] | None = None) -> tuple[dict[str, tuple[int, ...]], dict[str, int]]:
    """What the table must say, read the other way round: each account's
    records by the ledger close time stored on them (not through a range of
    ledgers), and the distinct records — of the accounts given, or of all."""
    inside = "{alias}.ledger_close_time >= :start AND {alias}.ledger_close_time < :end"
    of = "" if accounts is None else "AND p.account_id = ANY(:accounts)"
    of_trade = "" if accounts is None else "AND a.account_id = ANY(:accounts)"
    sides = "CROSS JOIN LATERAL (VALUES (t.base_account_id), (t.counter_account_id)) AS a(account_id)"
    parameters = {"start": start, "end": end, "accounts": accounts}
    joined = {
        "transactions": "history_transactions r JOIN history_transaction_participants p ON p.transaction_id = r.id",
        "operations": "history_operations r JOIN history_operation_participants p ON p.operation_id = r.id",
    }
    statements = {
        "transactions": [
            f"SELECT p.account_id, count(*) FROM {joined['transactions']} WHERE {inside.format(alias='r')} {of} GROUP BY 1",
            f"SELECT count(DISTINCT r.id) FROM {joined['transactions']} WHERE {inside.format(alias='r')} {of}",
        ],
        "operations": [
            f"SELECT p.account_id, count(*) FROM {joined['operations']} WHERE {inside.format(alias='r')} {of} GROUP BY 1",
            f"SELECT count(DISTINCT r.id) FROM {joined['operations']} WHERE {inside.format(alias='r')} {of}",
        ],
        "effects": [
            f"SELECT p.account_id, count(*) FROM history_effects p WHERE {inside.format(alias='p')} {of} GROUP BY 1",
            f"SELECT count(*) FROM history_effects p WHERE {inside.format(alias='p')} {of}",
        ],
        "trades": [
            f"SELECT a.account_id, count(DISTINCT t.id) FROM history_trades t {sides} "
            f"WHERE {inside.format(alias='t')} AND a.account_id IS NOT NULL {of_trade} GROUP BY 1",
            f"SELECT count(DISTINCT t.id) FROM history_trades t {sides} WHERE {inside.format(alias='t')} AND a.account_id IS NOT NULL {of_trade}",
        ],
    }
    per_account: dict[str, dict[str, int]] = {}
    totals: dict[str, int] = {}
    with engine.connect() as connection:
        for kind, (each, all_of_them) in statements.items():
            for account, number in connection.execute(text(each), parameters):
                per_account.setdefault(account, {})[kind] = number
            totals[kind] = connection.execute(text(all_of_them), parameters).scalar_one()
    return {account: tuple(numbers.get(kind, 0) for kind in KINDS) for account, numbers in per_account.items()}, totals


def four(row: Any, window: str = "") -> tuple[int, ...]:
    """The four counts of a row: of the window, or of the window before it."""
    return tuple(getattr(row, kind) if not window else row.previous[kind] for kind in KINDS)


# -- the table -----------------------------------------------------------------


def test_the_history_is_loaded_and_verified_by_the_backfill(yard: Yard) -> None:
    assert {report.status for report in yard.reports} == {"verified"}, [report.error for report in yard.reports]
    tables = ("history_transactions", "history_operations", "history_effects", "history_trades")
    assert [yard.one(f"SELECT count(*) FROM {table}") for table in tables] == [18, 20, 35, 1]


def test_a_week_of_every_watched_account(yard: Yard) -> None:
    page = activity.table(yard.engine, now=NOW)

    assert (page.period, page.accounts, page.pages, page.bounds) == ("week", 6, 1, "[from, to)")
    assert (page.window.start, page.window.end) == (NOW - 7 * activity.DAY, NOW)
    assert (page.previous.start, page.previous.end) == (NOW - 14 * activity.DAY, NOW - 7 * activity.DAY)
    rows = {row.account_id: row for row in page.rows}
    assert set(rows) == set(WATCHED)
    assert all((row.enabled, row.origin) == (True, "yaml") for row in page.rows)

    alfa = rows[A]
    assert four(alfa) == (5, 5, 5, 0)  # three payments, two received from the issuer, one of the last day
    assert alfa.previous == {"transactions": 2, "operations": 2, "effects": 2, "trades": 0}
    assert (alfa.change["operations"].by, alfa.change["operations"].percent) == (3, 150.0)
    assert (alfa.change["transactions"].by, alfa.change["transactions"].percent) == (3, 150.0)

    bravo = rows[B]
    assert four(bravo) == (4, 5, 5, 0)  # paid twice, a trustline and a data entry of its own, paid once more
    assert (bravo.change["transactions"].by, bravo.change["transactions"].percent) == (2, 100.0)
    assert (bravo.change["effects"].by, bravo.change["effects"].percent) == (3, 150.0)

    delta = rows[D]
    assert four(delta) == (2, 2, 2, 1)  # two payments; and a trade it is one side of
    assert delta.previous == {"transactions": 1, "operations": 1, "effects": 1, "trades": 0}
    assert (delta.change["transactions"].by, delta.change["transactions"].percent) == (1, 100.0)
    assert (delta.change["trades"].by, delta.change["trades"].percent) == (1, None)  # the window before had none

    charlie, sam, issuer = rows[C], rows[S], rows[ISSUER]
    assert (four(charlie), charlie.previous["transactions"]) == ((2, 2, 2, 0), 1)
    assert four(sam) == (1, 1, 1, 1) and sam.previous == ZERO
    assert four(issuer) == (2, 2, 2, 0) and issuer.previous == ZERO
    assert all(sam.change[kind] == activity.Change(1, None) for kind in KINDS)
    assert all(issuer.change[kind] == activity.Change(2, None) for kind in ("transactions", "operations", "effects"))
    assert issuer.change["trades"] == activity.Change(0, None)  # none before, none now: a share of zero is no number


def test_the_last_event_of_an_account_is_of_all_its_time(yard: Yard) -> None:
    rows = {row.account_id: row for row in activity.table(yard.engine, now=NOW).rows}

    # A and B were last at day 45, after now: not in the window, but their newest record.
    assert (rows[A].last_event_at, rows[A].last_ledger) == (closed(45, 1), day(45, 1))
    assert rows[B].last_event_at == closed(45, 1)
    assert (rows[D].last_event_at, rows[D].last_ledger) == (closed(43), day(43))  # the trade, of which D is only a side
    assert (rows[S].last_event_at, rows[ISSUER].last_event_at, rows[C].last_event_at) == (closed(43), closed(40, 1), closed(41, 1))


def test_the_totals_are_distinct_records_and_never_a_sum_over_the_accounts(yard: Yard) -> None:
    page = activity.table(yard.engine, now=NOW)

    # 9 transactions: the one of day 39 is shared by A and B, and counts once.
    assert page.totals == {"transactions": 9, "operations": 10, "effects": 17, "trades": 1}
    assert sum(row.transactions for row in page.rows) == 16
    assert sum(row.operations for row in page.rows) == 17 and sum(row.effects for row in page.rows) == 17


@pytest.mark.parametrize("period", ["day", "week", "month"])
def test_every_period_equals_what_the_records_say_by_ledger_time(yard: Yard, period: str) -> None:
    page = activity.table(yard.engine, period=period, now=NOW)

    expected, totals = reference(yard.engine, page.window.start, page.window.end)
    before, _ = reference(yard.engine, page.previous.start, page.previous.end)
    assert page.totals == totals
    assert {row.account_id: four(row) for row in page.rows} == {account: expected.get(account, (0, 0, 0, 0)) for account in WATCHED}
    assert {row.account_id: four(row, "before") for row in page.rows} == {account: before.get(account, (0, 0, 0, 0)) for account in WATCHED}
    for row in page.rows:
        for kind in KINDS:
            now, then = getattr(row, kind), row.previous[kind]
            percent = None if then == 0 else round(100.0 * (now - then) / then, 2)
            assert row.change[kind] == activity.Change(now - then, percent), (row.account_id, kind)


def test_a_day_and_a_month(yard: Yard) -> None:
    last_day = activity.table(yard.engine, period="day", now=NOW)

    assert (last_day.window.start, last_day.window.end) == (NOW - activity.DAY, NOW)
    rows = {row.account_id: row for row in last_day.rows}
    assert (four(rows[A]), four(rows[B])) == ((1, 1, 1, 0), (1, 1, 1, 0))
    assert all(four(row) == (0, 0, 0, 0) for account, row in rows.items() if account not in (A, B))
    assert (rows[D].previous["trades"], rows[D].change["trades"]) == (1, activity.Change(-1, -100.0))  # the trade of the day before
    assert (rows[S].previous["transactions"], rows[S].change["transactions"]) == (1, activity.Change(-1, -100.0))
    assert last_day.totals == {"transactions": 1, "operations": 1, "effects": 2, "trades": 0}

    month = activity.table(yard.engine, period="month", now=NOW)

    assert (month.window.start, month.window.length) == (datetime(2026, 1, 15, tzinfo=UTC), 31 * activity.DAY)
    assert month.totals == {"transactions": 16, "operations": 18, "effects": 31, "trades": 1}
    assert {row.account_id: four(row) for row in month.rows}[D] == (5, 6, 6, 1)
    before = {row.account_id: row.previous for row in month.rows}
    paid_once = {"transactions": 1, "operations": 1, "effects": 1, "trades": 0}
    assert (before[A], before[B]) == (paid_once, paid_once)  # A paid B on January 4
    assert all(before[account] == ZERO for account in (C, D, S, ISSUER))


def test_an_explicit_range_and_the_window_before_it(yard: Yard) -> None:
    page = activity.table(yard.engine, period=(closed(20), closed(32)), now=NOW)

    assert (page.period, page.window.start, page.window.end) == ("range", closed(20), closed(32))
    assert (page.previous.start, page.previous.end) == (closed(8), closed(20))  # the same length, just before
    rows = {row.account_id: row for row in page.rows}
    assert four(rows[D]) == (1, 2, 2, 0)  # its two trustlines of day 20; day 32 is not in
    assert (four(rows[A]), rows[A].previous["transactions"]) == ((1, 1, 1, 0), 0)  # paid by C on day 21
    assert four(rows[D], "before") == (1, 1, 1, 0)  # created on day 15
    assert (rows[S].previous["operations"], rows[S].operations) == (2, 0)  # it created D and the issuer
    assert page.totals == {"transactions": 2, "operations": 3, "effects": 4, "trades": 0}
    expected, totals = reference(yard.engine, closed(20), closed(32))
    assert page.totals == totals
    assert {account: four(rows[account]) for account in rows} == {account: expected.get(account, (0, 0, 0, 0)) for account in WATCHED}


def test_a_range_without_a_time_zone_is_utc_and_a_bad_period_is_refused(yard: Yard) -> None:
    page = activity.table(yard.engine, period=(closed(20).replace(tzinfo=None), closed(32).replace(tzinfo=None)), now=NOW)

    assert page.totals["transactions"] == 2

    for bad in ("fortnight", (closed(32), closed(20)), (closed(20),), "2026-01-01", {"from": closed(20)}, (1, 2), (closed(20), "later")):
        with pytest.raises(ValueError):
            activity.table(yard.engine, period=bad, now=NOW)
    with pytest.raises(ValueError, match="sort"):
        activity.table(yard.engine, sort="volume", now=NOW)
    with pytest.raises(ValueError, match="page"):
        activity.table(yard.engine, page=0, now=NOW)
    with pytest.raises(ValueError, match="per_page"):
        activity.table(yard.engine, per_page=activity.MAX_PER_PAGE + 1, now=NOW)


def test_the_filter_of_enabled_accounts_and_the_totals_of_those_only(yard: Yard) -> None:
    with yard.engine.begin() as connection:
        connection.execute(text("UPDATE tracked_accounts SET enabled = false WHERE account_id = ANY(:off)"), {"off": [S, ISSUER]})

    every = activity.table(yard.engine, now=NOW)
    on = activity.table(yard.engine, now=NOW, enabled=True)
    off = activity.table(yard.engine, now=NOW, enabled=False)

    assert (every.accounts, on.accounts, off.accounts) == (6, 4, 2)
    assert {row.account_id for row in on.rows} == {A, B, C, D} and all(row.enabled for row in on.rows)
    assert {row.account_id for row in off.rows} == {S, ISSUER} and not any(row.enabled for row in off.rows)
    # What the enabled accounts take part in is still theirs: the payments of the issuer to A, and the trade D is a side of.
    assert on.totals == {"transactions": 8, "operations": 9, "effects": 14, "trades": 1}
    assert off.totals == {"transactions": 3, "operations": 3, "effects": 3, "trades": 1}  # of S and the issuer alone
    assert every.totals == {"transactions": 9, "operations": 10, "effects": 17, "trades": 1}
    _, expected = reference(yard.engine, every.window.start, every.window.end, [A, B, C, D])
    assert on.totals == expected
    assert all(row.operations == 2 for row in on.rows[2:])  # C and D: still sorted by operations


@pytest.mark.parametrize("kind", KINDS)
def test_sorting_by_any_count(yard: Yard, kind: str) -> None:
    page = activity.table(yard.engine, now=NOW, sort=kind)

    keys = [(getattr(row, kind), row.account_id) for row in page.rows]
    assert [count for count, _ in keys] == sorted((count for count, _ in keys), reverse=True)
    for first, second in zip(keys, keys[1:]):  # ties are told apart by the account, so a page is always the same
        if first[0] == second[0]:
            assert first[1] < second[1]

    ascending = activity.table(yard.engine, now=NOW, sort=kind, descending=False)
    assert [getattr(row, kind) for row in ascending.rows] == sorted(getattr(row, kind) for row in page.rows)


def test_sorting_by_account_and_the_pages(yard: Yard) -> None:
    first = activity.table(yard.engine, now=NOW, sort="account", descending=False, per_page=4)

    assert [row.account_id for row in first.rows] == sorted(WATCHED)[:4]
    assert (first.accounts, first.pages, first.page, first.per_page) == (6, 2, 1, 4)
    second = activity.table(yard.engine, now=NOW, sort="account", descending=False, per_page=4, page=2)
    assert [row.account_id for row in second.rows] == sorted(WATCHED)[4:]
    beyond = activity.table(yard.engine, now=NOW, sort="account", descending=False, per_page=4, page=3)
    assert beyond.rows == () and (beyond.accounts, beyond.pages) == (6, 2)
    last_first = activity.table(yard.engine, now=NOW, sort="account")
    assert [row.account_id for row in last_first.rows] == sorted(WATCHED, reverse=True)

    most_active = activity.table(yard.engine, now=NOW, per_page=2)
    assert [row.operations for row in most_active.rows] == [5, 5]
    next_page = activity.table(yard.engine, now=NOW, per_page=2, page=2)
    assert {row.account_id for row in most_active.rows}.isdisjoint(row.account_id for row in next_page.rows)
    assert [row.operations for row in next_page.rows] == [2, 2]


def test_the_figures_of_the_page_are_read_for_the_page_only(yard: Yard) -> None:
    seen = Statements(yard.engine)
    try:
        page = activity.table(yard.engine, now=NOW, per_page=2, page=2)
    finally:
        seen.close()

    assert len(seen.of("count(*) OVER ()")) == 1  # one count of every account under the filter: what is sorted by
    detailed = seen.of("last_event_at")
    assert len(detailed) == 1 and list(detailed[0][1]["accounts"]) == [row.account_id for row in page.rows]  # everything else: the two of the page
    assert len(seen.seen) <= 16  # bounded, whatever the table holds


def test_a_window_without_a_ledger(yard: Yard) -> None:
    empty = (NOW - activity.DAY, NOW - activity.DAY + timedelta(hours=1))
    page = activity.table(yard.engine, period=empty, now=NOW)

    assert page.totals == ZERO and len(page.rows) == 6
    assert all(four(row) == (0, 0, 0, 0) and row.previous == ZERO for row in page.rows)
    assert all(row.change[kind] == activity.Change(0, None) for row in page.rows for kind in KINDS)
    by_count = activity.table(yard.engine, period=empty, now=NOW, sort="trades")
    assert [row.account_id for row in by_count.rows] == sorted(WATCHED, reverse=True)  # nothing to sort by: by account


def test_nothing_watched(yard: Yard) -> None:
    with yard.engine.begin() as connection:
        connection.execute(text("DELETE FROM tracked_accounts"))

    page = activity.table(yard.engine, now=NOW)

    assert (page.accounts, page.pages, page.rows, page.totals) == (0, 1, (), ZERO)


# -- own and foreign assets ----------------------------------------------------


def test_the_assets_an_operation_names_are_what_the_record_reader_names(yard: Yard) -> None:
    with yard.engine.connect() as connection:
        in_sql = {
            operation_id: set(assets)
            for operation_id, assets in connection.execute(
                text(
                        f"SELECT o.id, array_agg(named.asset) FROM history_operations o, "
                        f"LATERAL ({activity._named_assets('o.raw')}) named GROUP BY o.id"
                    )
            )
        }
    stored = {row.id: row.raw for row in yard.rows("SELECT id, raw FROM history_operations")}
    assert len(stored) == 20 and len(in_sql) == 17  # three of them name no asset: two accounts created, one data entry
    for operation_id, raw in stored.items():
        assert in_sql.get(operation_id, set()) == assets_in(raw), operation_id

    # Forms this history does not hold: assets with a prefix, a path, the canonical string, nesting.
    bodies = [
        {"asset_type": "credit_alphanum12", "asset_code": "LONGCODE1234", "asset_issuer": factories.ISSUER_A, "to": factories.ALICE},
        {
            "source_asset_type": "native",
            "dest_asset_type": "credit_alphanum4",
            "dest_asset_code": "EUR",
            "dest_asset_issuer": factories.ISSUER_B,
            "path": [{"asset_type": "native"}, {"asset_type": "credit_alphanum4", "asset_code": "MTL", "asset_issuer": factories.ISSUER_A}],
            "claimants": [{"destination": factories.ALICE, "asset": f"USDMTL:{factories.ISSUER_A}"}, {"asset": "native"}],
        },
        {
            "buying_asset_type": "credit_alphanum4",
            "buying_asset_code": "EUR",
            "buying_asset_issuer": factories.ISSUER_A,
            "selling_asset_type": "native",
            "asset": "not a canonical string",
            "nested": {"deep": [{"asset_type": "native"}]},
        },
        {"type": "manage_data", "name": "x"},
    ]
    named = text(f"SELECT asset FROM ({activity._named_assets('CAST(:body AS jsonb)')}) named")
    with yard.engine.connect() as connection:
        for body in bodies:
            assert set(connection.execute(named, {"body": json.dumps(body)}).scalars()) == assets_in(body), body
        # An asset without its issuer is left out by the reader (nothing to track it by), and is NULL here.
        broken = {"asset_type": "credit_alphanum4", "asset_code": "EUR"}
        assert connection.execute(named, {"body": json.dumps(broken)}).scalars().all() == [None]
        assert assets_in(broken) == set()
        # A share of a liquidity pool is named by its pool: left out the same way.
        share = {"asset_type": "liquidity_pool_shares", "liquidity_pool_id": "a" * 64}
        assert connection.execute(named, {"body": json.dumps(share)}).scalars().all() == [None]
        assert assets_in(share) == set()


def test_own_foreign_and_no_asset_where_the_database_proves_it(yard: Yard) -> None:
    page = activity.table(yard.engine, now=NOW)
    rows = {row.account_id: row for row in page.rows}

    issuer = rows[ISSUER].assets
    assert (issuer.state, issuer.own, issuer.foreign, issuer.no_asset, issuer.unreadable, issuer.unclassified) == ("classified", 1, 1, 0, 0, 0)
    assert (issuer.own_share, issuer.reason) == (0.5, "")  # a payment in the asset it issues; one in native

    delta = rows[D].assets
    assert (delta.state, delta.own, delta.foreign, delta.no_asset, delta.own_share) == ("classified", 1, 1, 0, 0.5)  # EUR, a line D opened; native

    month = {row.account_id: row for row in activity.table(yard.engine, period="month", now=NOW).rows}
    delta = month[D].assets
    assert (delta.state, delta.own, delta.foreign, delta.no_asset, delta.unreadable) == ("classified", 4, 1, 1, 0)
    assert delta.own_share == 0.8  # two trustlines opened, two payments in EUR; one in native; its creation
    assert (month[ISSUER].assets.own, month[ISSUER].assets.foreign, month[ISSUER].assets.no_asset) == (1, 1, 1)
    assert page.assets_rule == activity.ASSETS_RULE and "Native (XLM)" in page.assets_rule


def test_unknown_where_nothing_proves_what_the_account_had_a_line_to(yard: Yard) -> None:
    rows = {row.account_id: row for row in activity.table(yard.engine, now=NOW).rows}

    for account in (A, B, C, S):
        assets = rows[account].assets
        assert (assets.state, assets.own, assets.foreign, assets.no_asset, assets.own_share) == ("unknown", None, None, None, None), account
        assert assets.unclassified == rows[account].operations and assets.reason == activity.NO_CREATION_STORED, account
    assert rows[B].assets.unclassified == 5  # it opened a line to EUR on day 42: that is known; that it had none before is not


def test_nothing_to_classify(yard: Yard) -> None:
    rows = {row.account_id: row for row in activity.table(yard.engine, period=(closed(44), closed(45)), now=NOW).rows}

    assert rows[S].operations == 0
    assert (rows[S].assets.state, rows[S].assets.own, rows[S].assets.unclassified, rows[S].assets.reason) == ("classified", 0, 0, "")


def test_classified_only_up_to_where_the_effects_are_verified(yard: Yard) -> None:
    yard.forget(D, day(21), day(21))  # one ledger of D's effects is no longer verified: a gap
    yard.forget(ISSUER, day(39, 1), LATEST)
    week = activity.table(yard.engine, now=NOW)
    month = activity.table(yard.engine, period="month", now=NOW)
    first = yard.one("SELECT min(sequence) FROM history_ledgers WHERE close_time >= :start", start=week.window.start)

    delta = {row.account_id: row for row in month.rows}[D].assets
    assert (delta.state, delta.own, delta.foreign, delta.no_asset, delta.unclassified) == ("partial", 2, 0, 1, 3)
    assert str(day(21) - 1) in delta.reason  # its creation and its two trustlines; the payments lie beyond the gap

    delta = {row.account_id: row for row in week.rows}[D].assets
    assert (delta.state, delta.unclassified, delta.reason) == ("unknown", 2, activity.NOT_VERIFIED_FROM_CREATION.format(ledger=first))

    issuer = {row.account_id: row for row in week.rows}[ISSUER].assets
    assert (issuer.state, issuer.unclassified) == ("unknown", 2)  # nothing of it is verified in the week
    assert str(day(39)) in issuer.reason


def test_a_range_that_was_not_read_is_not_verified(yard: Yard) -> None:
    with yard.engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE sync_coverage SET state = 'pending', verified_at = NULL "
                "WHERE subject_key = :account AND data_type = 'effects'"
            ),
            {"account": D},
        )
    first = yard.one("SELECT min(sequence) FROM history_ledgers WHERE close_time >= :start", start=NOW - 7 * activity.DAY)

    row = {row.account_id: row for row in activity.table(yard.engine, now=NOW).rows}[D]

    assert (row.assets.state, row.assets.unclassified) == ("unknown", 2)
    assert row.assets.reason == activity.NOT_VERIFIED_FROM_CREATION.format(ledger=first)


def test_an_account_created_more_than_once(make_yard: Callable[..., Yard]) -> None:
    twice = make_yard([*history(), creates(day(30), S, D)])

    row = {row.account_id: row for row in activity.table(twice.engine, now=NOW).rows}[D]

    assert (row.assets.state, row.assets.unclassified) == ("unknown", 2)
    assert row.assets.reason == activity.CREATED_MORE_THAN_ONCE


def test_an_operation_whose_body_is_not_stored(yard: Yard) -> None:
    paid = [row[0] for row in yard.rows(
        "SELECT id FROM history_operations WHERE source_account = :account AND raw->>'to' = :to", account=D, to=C
    )]
    (payment_to_c,) = paid
    with yard.engine.begin() as connection:
        connection.execute(text("UPDATE history_operations SET raw = NULL WHERE id = :operation"), {"operation": payment_to_c})

    row = {row.account_id: row for row in activity.table(yard.engine, now=NOW).rows}[D]

    assert (row.assets.state, row.assets.own, row.assets.foreign, row.assets.unreadable, row.assets.unclassified) == ("classified", 0, 1, 1, 0)


def test_a_trustline_effect_that_names_no_asset_that_can_be_read(make_yard: Callable[..., Yard]) -> None:
    nameless = Transaction(
        day(22),
        0,
        D,
        [Operation("change_trust", 6, None, {"limit": amount(1000 * 10**7)}, effects=[{"account": D, "type": "trustline_created", "type_i": 12}])],
        {D},
        {1: {D}},
    )
    yard = make_yard([*history(), nameless])

    row = {row.account_id: row for row in activity.table(yard.engine, now=NOW).rows}[D]

    assert (row.assets.state, row.assets.own, row.assets.unclassified) == ("unknown", None, 2)
    assert row.assets.reason == activity.UNREADABLE_LINE


def test_a_classification_that_runs_out_of_time_is_unknown_not_a_missing_page(yard: Yard, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(activity, "CLASSIFICATION_TIMEOUT_SECONDS", 0.001)

    page = activity.table(yard.engine, now=NOW)

    assert page.totals["transactions"] == 9 and len(page.rows) == 6  # the rest of the page is there
    rows = {row.account_id: row for row in page.rows}
    for account in (D, ISSUER):
        assert (rows[account].assets.state, rows[account].assets.reason) == ("unknown", activity.RAN_OUT_OF_TIME), account
    assert four(rows[D]) == (2, 2, 2, 1)


def test_what_the_page_is_shown(yard: Yard) -> None:
    shown = activity.table(yard.engine, now=NOW).as_dict()

    assert isinstance(json.dumps(shown), str)
    assert shown["measured_at"] == NOW.isoformat()
    assert shown["rows"][0]["change"]["operations"] == {"by": 3, "percent": 150.0}
    assert shown["window"] == {"start": (NOW - 7 * activity.DAY).isoformat(), "end": NOW.isoformat()}
    assert shown["volume_warning"] == activity.VOLUME_WARNING and shown["assets_rule"] == activity.ASSETS_RULE
    assert shown["rows"][0]["assets"]["state"] in ("unknown", "classified", "partial")


# -- the volume of data --------------------------------------------------------


def bytes_per_row(engine: Engine) -> dict[str, float]:
    """What the formula is made of, from the catalog: a table's bytes on disk over the rows the planner believes it has."""
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT c.relname, c.reltuples, pg_relation_size(c.oid), "
                "CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_relation_size(c.reltoastrelid) END, "
                "pg_indexes_size(c.oid) FROM pg_class c WHERE c.relname = ANY(:tables)"
            ),
            {"tables": list(activity.VOLUME_TABLE_NAMES)},
        ).all()
    return {name: (heap + toast + indexes) / tuples for name, tuples, heap, toast, indexes in rows}


def test_the_volume_of_data_by_the_formula(yard: Yard) -> None:
    before = activity.table(yard.engine, now=NOW)

    assert set(before.widths) == set(activity.VOLUME_TABLE_NAMES)
    assert all(table.bytes_per_row is None and "never analyzed" in table.reason for table in before.widths.values())
    assert all(row.volume_bytes is None and all(part is None for part in row.volume_parts.values()) for row in before.rows)

    with yard.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        connection.execute(text("ANALYZE"))

    page = activity.table(yard.engine, now=NOW)

    per_row = bytes_per_row(yard.engine)
    assert all(table.bytes_per_row == per_row[table.name] > 0 for table in page.widths.values())
    for row in page.rows:
        parts = {
            "transactions": row.transactions * (per_row["history_transaction_participants"] + per_row["history_transactions"]),
            "operations": row.operations * (per_row["history_operation_participants"] + per_row["history_operations"]),
            "effects": row.effects * per_row["history_effects"],
            "trades": row.trades * per_row["history_trades"],
        }
        assert row.volume_parts == {kind: round(number) for kind, number in parts.items()}
        assert row.volume_bytes == sum(row.volume_parts.values()) > 0
    assert page.volume_formula == activity.VOLUME_FORMULA
    assert page.as_dict()["widths"]["history_trades"]["bytes_per_row"] == per_row["history_trades"]


def test_the_widths_are_kept_in_the_cache_and_nothing_else_is(yard: Yard) -> None:
    cache = TtlCache()
    seen = Statements(yard.engine)
    try:
        activity.table(yard.engine, now=NOW, cache=cache)
        first = len(seen.seen)
        second = activity.table(yard.engine, now=NOW, cache=cache)
    finally:
        seen.close()

    assert len(seen.of("pg_indexes_size")) == 1  # the catalog was asked once
    assert len(seen.seen) - first == first - 1 and second.accounts == 6  # and the page was read again in full


# -- the speed of the import ---------------------------------------------------


def test_the_speed_of_the_import_is_by_the_time_a_row_was_stored(yard: Yard) -> None:
    loaded = yard.one("SELECT max(created_at) FROM history_effects")
    whole = loaded.replace(minute=0, second=0, microsecond=0)  # the hour the load lies in
    now = whole + HOUR  # a moment just after it
    effects = [row[0] for row in yard.rows("SELECT id FROM history_effects ORDER BY id")]
    total = len(effects)
    assert total == 35
    with yard.engine.begin() as connection:
        # Every row at one moment of that hour: the load itself may have straddled an hour of the clock.
        for table, column in activity.IMPORTED:
            connection.execute(text(f"UPDATE {table} SET {column} = :at"), {"at": whole + 30 * MINUTE})

        def stored(when: datetime, ids: list[Any]) -> None:
            connection.execute(text("UPDATE history_effects SET created_at = :at WHERE id = ANY(:ids)"), {"at": when, "ids": ids})

        stored(whole - 3 * HOUR + MINUTE, effects[:3])  # three hours before the load
        stored(whole - 26 * HOUR, effects[3:5])  # before the window
        stored(now + 10 * MINUTE, effects[5:6])  # not yet

    speed = activity.import_speed(yard.engine, now=now)

    assert (speed.hours, speed.since, speed.until, speed.measured_at) == (24, now - 23 * HOUR, now, now)
    assert speed.note == activity.IMPORT_NOTE
    table = speed.tables["history_effects"]
    assert (table.column, table.rows, table.rows_per_hour) == ("created_at", total - 3, round((total - 3) / 24, 2))
    assert (speed.tables["history_operations"].rows, speed.tables["history_trades"].rows) == (20, 1)
    assert (speed.tables["history_accounts"].column, speed.tables["history_accounts"].rows) == ("first_seen_at", 6)
    assert speed.measured_tables == len(activity.IMPORTED) and all(item.reason == "" for item in speed.tables.values())
    assert speed.rows == sum(item.rows for item in speed.tables.values()) and speed.rows_per_hour == round(speed.rows / 24, 2)
    assert [hour.hour for hour in speed.series] == [now - index * HOUR for index in range(23, -1, -1)]
    assert speed.series[19] == activity.HourLoad(whole - 3 * HOUR, 3, {"history_effects": 3})
    # The rest was stored in the hour of the load. The last hour of the window is the one now begins: nothing is in it yet.
    assert speed.series[-2].hour == whole and speed.series[-2].by_table["history_effects"] == total - 6
    assert (speed.series[-1].hour, speed.series[-1].rows) == (now, 0)
    assert sum(hour.rows for hour in speed.series) == speed.rows
    assert speed.as_dict()["series"][-2]["hour"] == whole.isoformat()
    # The same rows, by ledger time, are months old: no activity at all.
    assert activity.table(yard.engine, now=now).totals == ZERO


def test_the_import_speed_is_kept_in_the_cache(yard: Yard) -> None:
    cache = TtlCache()
    seen = Statements(yard.engine)
    try:
        first = activity.import_speed(yard.engine, cache=cache)
        statements = len(seen.seen)
        second = activity.import_speed(yard.engine, cache=cache)
    finally:
        seen.close()

    assert statements >= len(activity.IMPORTED) and len(seen.seen) == statements
    assert second is first


def test_tables_too_large_to_read_through_are_not_measured(yard: Yard, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(activity, "SCAN_LIMIT_BYTES", 1)

    speed = activity.import_speed(yard.engine)

    assert speed.rows is None and speed.rows_per_hour is None and speed.measured_tables == 0
    assert all(item.rows is None and "larger than" in item.reason for item in speed.tables.values())
    assert speed.as_dict()["tables"]["history_effects"]["rows"] is None
    assert [hour.rows for hour in speed.series] == [0] * 24


def test_a_table_that_runs_out_of_time_is_left_out(yard: Yard, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(activity, "IMPORT_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(
        activity,
        "_STORED",
        "SELECT date_trunc('hour', {column} AT TIME ZONE 'UTC') AS hour, count(*) FROM {table} "
        "WHERE {column} >= :since AND {column} < :until AND pg_sleep(0.3) IS NOT NULL GROUP BY 1",
    )

    speed = activity.import_speed(yard.engine)

    # Every table has rows stored in the window, so every one of them runs out of time: nothing is measured, and each says why.
    assert all(item.rows is None and "ran out of its time" in item.reason for item in speed.tables.values())
    assert (speed.measured_tables, speed.rows, speed.rows_per_hour) == (0, None, None)


def test_the_hours_of_the_import_speed(yard: Yard) -> None:
    now = datetime(2026, 2, 15, 10, 30, 45, tzinfo=UTC)

    speed = activity.import_speed(yard.engine, now=now, hours=6)

    whole = now.replace(minute=0, second=0)  # the window is cut into whole hours and ends at now
    assert (speed.since, speed.until, len(speed.series)) == (whole - 5 * HOUR, now, 6)
    assert [hour.hour for hour in speed.series] == [whole - index * HOUR for index in (5, 4, 3, 2, 1, 0)]
    for hours in (0, 24 * 32, 2.5, True):
        with pytest.raises(ValueError):
            activity.import_speed(yard.engine, now=now, hours=hours)


def test_nothing_stored_in_a_window(yard: Yard) -> None:
    speed = activity.import_speed(yard.engine, now=datetime(2026, 3, 15, tzinfo=UTC))

    assert (speed.rows, speed.rows_per_hour, speed.measured_tables) == (0, 0.0, len(activity.IMPORTED))
    assert all(item.rows == 0 for item in speed.tables.values())


def test_the_pages_ask_horizon_for_nothing(yard: Yard) -> None:
    asked = len(yard.fake.requests)

    activity.table(yard.engine, now=NOW)
    activity.table(yard.engine, period="month", now=NOW, per_page=1, page=3)
    activity.import_speed(yard.engine)

    assert len(yard.fake.requests) == asked


# -- a thousand accounts -------------------------------------------------------


def seed_scale(engine: Engine) -> datetime:
    """A thousand watched accounts with a few operations each, the first
    hundred of them created in the stored history with a trustline and their
    effects verified — by bulk inserts, for the shape of a loaded installation.
    Returns the moment just after the last ledger."""
    issuer = "'G' || lpad('1', 55, '0')"

    def acct(number: str) -> str:
        """Account ``number`` (of 1..1000) as its address."""
        return f"'G' || lpad(({number})::text, 55, '0')"

    def close(ledger: str) -> str:
        """When a ledger closed: five seconds per ledger from 2026-01-01."""
        return f"'2026-01-01+00'::timestamptz + make_interval(secs => 5 * {ledger})"

    def padded(id_column: str) -> str:
        """The left part of an effect id: the operation id in nineteen digits."""
        return f"lpad({id_column}::text, 19, '0')"

    native = "jsonb_build_object('asset_type', 'native')"
    euro = f"jsonb_build_object('asset_type', 'credit_alphanum4', 'asset_code', 'EUR', 'asset_issuer', {issuer})"
    columns = (
        "id, transaction_hash, ledger_sequence, application_order, source_account, account_sequence, max_fee, operation_count, "
        "successful, ledger_close_time, envelope_xdr, result_xdr, memo_type"
    )
    operation = (
        "INSERT INTO history_operations (id, transaction_id, application_order, source_account, type, type_i, "
        "ledger_close_time, raw) SELECT "
    )
    effect = (
        "INSERT INTO history_effects (id, paging_token, operation_id, effect_order, account_id, type, type_i, "
        "ledger_close_time{}) SELECT "
    )
    # k, the account an operation of ledgers 1..3 is about, is the application order of its transaction: the
    # operation id without its ledger and without the operation's own 1, shifted back.
    k = "((o.id - 1) - ((o.id >> 32) << 32)) >> 12"
    statements = [
        "INSERT INTO history_accounts (account_id) SELECT 'G' || lpad(g::text, 55, '0') FROM generate_series(1, 1000) g",
        "INSERT INTO history_assets (asset_id, asset_type, asset_code, asset_issuer) VALUES ('native', 'native', '', ''), "
        f"('EUR:' || {issuer}, 'credit_alphanum4', 'EUR', {issuer})",
        f"INSERT INTO history_ledgers (sequence, close_time) SELECT g, {close('g')} FROM generate_series(1, 3000) g",
    ]
    # 6000 transactions in ledgers 100..2999, of two operations each: a payment in native, and a path payment in EUR through native.
    statements += [
        f"INSERT INTO history_transactions ({columns}, raw) "
        "SELECT ((g % 2900 + 100)::bigint << 32) | ((g / 2900 + 1)::bigint << 12), md5(g::text) || md5((g + 1)::text), "
        f"g % 2900 + 100, g / 2900 + 1, {acct('(g * 7) % 1000 + 1')}, g, 100, 2, true, {close('g % 2900 + 100')}, "
        "'AAAA' || md5(g::text), 'AAAA', 'none', jsonb_build_object('asset_type', 'native', 'to', 'x', 'amount', '1.0000000') "
        "FROM generate_series(1, 6000) g",
        operation + f"t.id + k, t.id, k, {acct('((t.id >> 32) * 3 + k) % 1000 + 1')}, 'payment', 1, t.ledger_close_time, "
        "CASE k WHEN 1 THEN jsonb_build_object('asset_type', 'native', 'to', 'y', 'amount', '1.0000000') ELSE "
        f"{euro} || jsonb_build_object('to', 'z', 'amount', '2.0000000', 'path', jsonb_build_array({native}), "
        "'source_asset_type', 'native') END FROM history_transactions t CROSS JOIN generate_series(1, 2) k "
        "WHERE t.ledger_sequence >= 100",
        "INSERT INTO history_operation_participants (operation_id, account_id) SELECT id, source_account FROM history_operations",
        f"INSERT INTO history_operation_participants (operation_id, account_id) SELECT o.id, {acct('(o.id % 997) + 1')} "
        f"FROM history_operations o WHERE o.source_account <> {acct('(o.id % 997) + 1')}",
        "INSERT INTO history_transaction_participants (transaction_id, account_id) SELECT DISTINCT o.transaction_id, p.account_id "
        "FROM history_operation_participants p JOIN history_operations o ON o.id = p.operation_id",
        # An effect names its asset: a debit in native for the source, a credit in EUR for the other account.
        effect.format(", raw") + f"{padded('o.id')} || '-0000000001', o.id::text || '-1', o.id, 1, o.source_account, "
        f"'account_debited', 3, o.ledger_close_time, {native} FROM history_operations o",
        effect.format("") + f"{padded('p.operation_id')} || '-0000000002', p.operation_id::text || '-2', p.operation_id, 2, p.account_id, "
        "'account_credited', 2, o.ledger_close_time FROM history_operation_participants p JOIN history_operations o ON o.id = p.operation_id "
        "WHERE p.account_id <> o.source_account",
        "INSERT INTO history_trades (id, operation_id, trade_order, ledger_close_time, base_asset_id, counter_asset_id, base_amount, "
        f"counter_amount, base_account_id, counter_account_id, base_is_seller, trade_type) SELECT o.id::text || '-0', o.id, 0, "
        f"o.ledger_close_time, 'native', 'EUR:' || {issuer}, 1000000, 3000000, o.source_account, {acct('(o.id % 991) + 1')}, "
        "true, 'orderbook' FROM history_operations o WHERE o.id % 3 = 0",
    ]
    # The first hundred accounts: created at ledger 1, a trustline to EUR opened at ledger 2, a payment in EUR at ledger 3.
    for ledger, mark, source in ((1, "c", acct("1000")), (2, "t", acct("k")), (3, "p", acct("k"))):
        statements.append(
            f"INSERT INTO history_transactions ({columns}) SELECT ({ledger}::bigint << 32) | (k::bigint << 12), "
            f"md5('{mark}' || k::text) || md5(k::text), {ledger}, k, {source}, k, 100, 1, true, {close(str(ledger))}, "
            "'AAAA', 'AAAA', 'none' FROM generate_series(1, 100) k"
        )
    statements += [
        operation + "t.id + 1, t.id, 1, t.source_account, 'create_account', 0, t.ledger_close_time, "
        f"jsonb_build_object('type', 'create_account', 'account', {acct('t.application_order')}) "
        "FROM history_transactions t WHERE t.ledger_sequence = 1",
        operation + "t.id + 1, t.id, 1, t.source_account, 'change_trust', 6, t.ledger_close_time, "
        f"jsonb_build_object('type', 'change_trust') || {euro} FROM history_transactions t WHERE t.ledger_sequence = 2",
        operation + "t.id + 1, t.id, 1, t.source_account, 'payment', 1, t.ledger_close_time, "
        f"jsonb_build_object('type', 'payment', 'to', {acct('t.application_order % 99 + 1')}) || {euro} "
        "FROM history_transactions t WHERE t.ledger_sequence = 3",
        "INSERT INTO history_operation_participants (operation_id, account_id) SELECT o.id, o.source_account "
        "FROM history_operations o WHERE o.id >> 32 IN (1, 2, 3)",
        f"INSERT INTO history_operation_participants (operation_id, account_id) SELECT o.id, {acct(k)} "
        "FROM history_operations o WHERE o.id >> 32 = 1",
        f"INSERT INTO history_operation_participants (operation_id, account_id) SELECT o.id, {acct(f'({k} % 99) + 1')} "
        "FROM history_operations o WHERE o.id >> 32 = 3",
        "INSERT INTO history_transaction_participants (transaction_id, account_id) SELECT DISTINCT o.transaction_id, p.account_id "
        "FROM history_operation_participants p JOIN history_operations o ON o.id = p.operation_id WHERE o.id >> 32 IN (1, 2, 3)",
        effect.format("") + f"{padded('o.id')} || '-0000000001', o.id::text || '-1', o.id, 1, {acct(k)}, "
        "'account_created', 0, o.ledger_close_time FROM history_operations o WHERE o.id >> 32 = 1",
        effect.format(", raw") + f"{padded('o.id')} || '-0000000002', o.id::text || '-2', o.id, 2, o.source_account, "
        f"'account_debited', 3, o.ledger_close_time, {native} FROM history_operations o WHERE o.id >> 32 = 1",
        effect.format(", raw") + f"{padded('o.id')} || '-0000000001', o.id::text || '-1', o.id, 1, o.source_account, "
        f"'trustline_created', 20, o.ledger_close_time, {euro} FROM history_operations o WHERE o.id >> 32 = 2",
        effect.format(", raw") + f"{padded('o.id')} || '-0000000001', o.id::text || '-1', o.id, 1, {acct(f'({k} % 99) + 1')}, "
        f"'account_credited', 2, o.ledger_close_time, {euro} FROM history_operations o WHERE o.id >> 32 = 3",
        effect.format("") + f"{padded('o.id')} || '-0000000002', o.id::text || '-2', o.id, 2, o.source_account, "
        "'account_debited', 3, o.ledger_close_time FROM history_operations o WHERE o.id >> 32 = 3",
        "INSERT INTO tracked_accounts (account_id, enabled, reason, origin) SELECT 'G' || lpad(g::text, 55, '0'), g % 50 <> 0, "
        "'scale', (ARRAY['legacy','yaml','manual','discovery'])[1 + g % 4] FROM generate_series(1, 1000) g",
        "INSERT INTO sync_coverage (subject_type, subject_key, data_type, selection_policy, policy_version, from_ledger, "
        "to_ledger, source, state, verified_at) SELECT 'account', 'G' || lpad(k::text, 55, '0'), 'effects', 'FULL', 1, 1, 3000, "
        "'horizon:pool', 'complete', now() FROM generate_series(1, 100) k",
        "ANALYZE",
    ]
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        for statement in statements:
            connection.execute(text(statement))
    return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=5 * 3001)


PER_ACCOUNT_TABLES = ("history_transaction_participants", "history_operation_participants", "history_effects", "history_trades")


def plan_nodes(plan: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield plan
    for child in plan.get("Plans", []):
        yield from plan_nodes(child)


def explained(engine: Engine, statements: list[tuple[str, Any]]) -> list[list[dict[str, Any]]]:
    """The plan nodes PostgreSQL has for statements it was given."""
    with engine.connect() as connection:
        nodes = []
        for statement, parameters in statements:
            shown = connection.exec_driver_sql("EXPLAIN (FORMAT JSON) " + statement, parameters).one()[0]
            nodes.append(list(plan_nodes((shown if isinstance(shown, list) else json.loads(shown))[0]["Plan"])))
    return nodes


def test_a_thousand_watched_accounts_in_a_bounded_time_through_the_indexes(make_database: Callable[[], str]) -> None:
    engine = create_engine(make_database())
    try:
        now = seed_scale(engine)
        seen = Statements(engine)
        try:
            started = time.perf_counter()
            page = activity.table(engine, now=now, per_page=100)
            took = time.perf_counter() - started
            # Everything asked of the database for the page, in order: two ranges of ledgers, the accounts of the
            # page, the accounts created, their coverage, the operations judged, the totals, the width of the rows.
            statements = [
                (statement.strip(), parameters)
                for statement, parameters in seen.seen
                if statement.lstrip().upper().startswith(("SELECT", "WITH")) and "set_config" not in statement
            ]
        finally:
            seen.close()

        print(f"\nactivity of 1000 accounts: a page of 100, with the classification: {took * 1000:.0f} ms")
        assert took < 5.0, took  # bounded; what it took is in the report
        assert (page.accounts, page.pages, len(page.rows)) == (1000, 10, 100)
        assert [row.operations for row in page.rows] == sorted((row.operations for row in page.rows), reverse=True)
        assert all(row.volume_bytes is not None for row in page.rows)
        _, totals = reference(engine, page.window.start, page.window.end)
        assert page.totals == totals and totals["transactions"] == 6300
        proved = [row for row in page.rows if row.assets.state == activity.CLASSIFIED]
        assert len(proved) >= 20  # the accounts created in the stored history are among the most active
        for row in proved:
            assert row.assets.own >= 2 and row.assets.no_asset == 1  # the line it opened, and a payment or two in EUR; its creation
            assert (row.assets.unreadable, row.assets.unclassified) == (0, 0)
            assert row.assets.own + row.assets.foreign + row.assets.no_asset == row.operations
        assert all(
            row.assets.state == activity.UNKNOWN and row.assets.reason == activity.NO_CREATION_STORED
            for row in page.rows if row not in proved
        )

        nodes = explained(engine, statements)
        assert len(nodes) == len(statements) == 9, [statement[:60] for statement, _ in statements]
        scans = [
            (node["Node Type"], node.get("Index Name", ""), node.get("Relation Name"))
            for plan in nodes
            for node in plan
            if node.get("Relation Name") in PER_ACCOUNT_TABLES
        ]
        assert not [scan for scan in scans if scan[0] == "Seq Scan"], scans  # an account's rows come through the indexes that begin with the account
        assert {
            "ix_history_transaction_participants_account",
            "ix_history_operation_participants_account",
            "ix_history_effects_account",
            "ix_history_trades_base_account",
            "ix_history_trades_counter_account",
        } <= {scan[1] for scan in scans}, scans
        assert "ix_history_effects_type" in {node.get("Index Name") for plan in nodes for node in plan}  # the account_created effects

        second = activity.table(engine, now=now, per_page=100, page=2)
        assert {row.account_id for row in second.rows}.isdisjoint(row.account_id for row in page.rows)
        assert second.rows[0].operations <= page.rows[-1].operations
    finally:
        engine.dispose()
