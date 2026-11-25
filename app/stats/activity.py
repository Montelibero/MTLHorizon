"""The activity of the watched accounts: what each of them did in a period,
how much data that is, and how fast history is being stored.

Everything here is counted by ledger time — when the ledger closed, which
every record carries — and never by the time a record was imported. A history
import run today brings in ledgers of two years ago, and that is activity of
two years ago. The one figure about the time of storing is apart and says so
(:func:`import_speed`).

A window of time becomes a range of ledgers through ``history_ledgers``
(time to ledger is what its index on ``close_time`` is for), and an account's
rows of that range are reached through the indexes that begin with the
account: ``(account_id, transaction_id)`` and the like. The range is written
as a comparison of two columns with two values, because only such an index
can serve that. Written as two ordinary conditions, the same rows can also be
looked up through the primary key of the participants tables, which begins
with the id: the planner may choose it, and then walk the range of the whole
table once for every account. Where the account is a row of a table of its
own — the distinct totals, the classification — the subquery is kept a
subquery with OFFSET 0, read once per account: left to itself, the planner
may as well read the whole table and join. The comparison of two values is
estimated at many more rows than there are, which is why the transaction
switches JIT off.

Sorting has to know one count of every account under the filter; the other
figures are read only for the accounts of the page, once.

Nothing here asks Horizon or the Notifier for anything, opens no log file and
writes nothing: all of it runs in one read-only transaction with a statement
timeout.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql.elements import TextClause

from app.stats.dashboard import plain, reading, timed_out, moment

DAY = timedelta(days=1)
PERIODS = ("day", "week", "month")
DEFAULT_PERIOD = "week"
KINDS = ("transactions", "operations", "effects", "trades")
SORTS = (*KINDS, "account")
DEFAULT_SORT = "operations"
DEFAULT_PER_PAGE = 50
MAX_PER_PAGE = 500
# "[from, to)": a record of the moment ``to`` belongs to the next window, and
# two windows in a row share no record.
BOUNDS = "[from, to)"

STATEMENT_TIMEOUT_SECONDS = 10.0
# Counting distinct records sorts and hashes: each node may keep this much
# before it spills to disk. Set for the transaction only.
WORK_MEM = "64MB"
# Classifying operations by their assets is the best-effort part of a page:
# it reads the stored bodies of the operations, and an account with a long
# verified history may have more of them in a window than a page should read.
# It has a budget of its own, and what runs out of it is unknown.
CLASSIFICATION_TIMEOUT_SECONDS = 5.0
IMPORT_TIMEOUT_SECONDS = 15.0
# Nothing indexes the time a row was stored, so the speed of the import reads
# a table through. Beyond this size a table is left out and says so.
SCAN_LIMIT_BYTES = 256 * 1024 * 1024

# -- own and foreign assets ----------------------------------------------------

# Whether the operations of an account deal with the account's own assets or
# with others' is said only where the database can prove it. The rule, and
# the proof it rests on:
ASSETS_RULE = (
    "The assets an operation names are read from its stored body: every asset "
    "it carries, under a key ending in asset_type (asset, source_asset, "
    "buying_asset, the assets of a path, ...), or as the canonical string "
    "under 'asset'. An operation of an account is OWN when every asset it names is "
    "issued by that account, or is one the account is known to have had a "
    "trustline to at that time: a stored trustline_created effect of the "
    "account for that asset, in a ledger at or before the operation's. It is "
    "FOREIGN when it names an asset that is neither. Native (XLM) is issued by "
    "nobody and needs no trustline: it is foreign for every account. An "
    "operation that names no asset is counted apart (no_asset). "
    "An operation is judged only where the account's effects are verified "
    "without gaps (sync_coverage: complete or empty) from a stored "
    "account_created effect of the account — exactly one in the stored "
    "history — up to the operation's ledger. Only there does a missing "
    "trustline_created mean that there was no trustline. Operations outside "
    "that are not classified, and an account without that proof gets unknown."
)

CLASSIFIED = "classified"
PARTIAL = "partial"
UNKNOWN = "unknown"

NO_CREATION_STORED = (
    "no account_created effect of this account is stored: a trustline it opened before the stored "
    "history began cannot be told from one it never had, so foreign cannot be told from own"
)
CREATED_MORE_THAN_ONCE = (
    "the account was created more than once in the stored history (merged and created again): "
    "the rule is not applied to it"
)
NOT_VERIFIED_FROM_CREATION = (
    "the effects of the account are not verified without gaps from a stored account_created effect "
    "up to the ledger {ledger}: an effect that is not stored would make a trustline look absent"
)
VERIFIED_ONLY_UP_TO = (
    "classified only up to ledger {ledger}: to where the effects of the account are verified without "
    "gaps from its creation"
)
UNREADABLE_LINE = "a stored trustline_created effect of the account names no asset that can be read"
RAN_OUT_OF_TIME = "the query that classifies the operations ran out of its time budget"

# -- the volume of data --------------------------------------------------------

# For a kind of record: the tables in which one record of that kind, taking
# part in an account's activity, has a row.
VOLUME_TABLES = {
    "transactions": ("history_transaction_participants", "history_transactions"),
    "operations": ("history_operation_participants", "history_operations"),
    "effects": ("history_effects",),
    "trades": ("history_trades",),
}
VOLUME_TABLE_NAMES = tuple(sorted({table for tables in VOLUME_TABLES.values() for table in tables}))
VOLUME_FORMULA = (
    "volume = transactions x (bytes per row of history_transaction_participants + bytes per row of "
    "history_transactions) + operations x (bytes per row of history_operation_participants + bytes per "
    "row of history_operations) + effects x bytes per row of history_effects + trades x bytes per row "
    "of history_trades — for the rows of the window. Bytes per row of a table = (its heap + what is "
    "stored outside the main heap + its indexes) / the rows the planner believes it has "
    "(pg_class.reltuples). That number is an estimate, and so is everything made of it."
)
VOLUME_WARNING = (
    "The records of two watched accounts are shared and are counted for each of them: a transaction two "
    "watched accounts took part in is in the volume of both. The volumes of accounts must not be added up."
)
NO_ROW_COUNT = "the planner has no number of rows for this table: it was never analyzed (ANALYZE)"

IMPORT_NOTE = (
    "Rows stored, by the time each row was stored (created_at of the record tables, first_seen_at of the "
    "registries) — not by ledger time. The last hour is not over yet. The tables that link accounts to "
    "transactions and operations carry no time of their own and are not counted. A table too large to be "
    "read through for a page is left out and says so."
)

# The tables that keep the time a row was stored, and the column that keeps it.
IMPORTED = (
    ("history_ledgers", "created_at"),
    ("history_transactions", "created_at"),
    ("history_operations", "created_at"),
    ("history_effects", "created_at"),
    ("history_trades", "created_at"),
    ("history_accounts", "first_seen_at"),
    ("history_assets", "first_seen_at"),
)
IMPORTED_TABLES = tuple(table for table, _ in IMPORTED)


@dataclass(frozen=True)
class Span:
    """A window of time: from ``start`` up to, but not including, ``end``."""

    start: datetime
    end: datetime

    @property
    def length(self) -> timedelta:
        return self.end - self.start


@dataclass(frozen=True)
class Change:
    """How a count differs from the one of the window before. ``percent`` is
    None when that window had none: a share of zero is not a number."""

    by: int
    percent: float | None


@dataclass(frozen=True)
class Assets:
    """The operations of an account by their assets, under ``ASSETS_RULE``.

    ``state`` is ``classified`` when every operation of the window was judged,
    ``partial`` when only some were (``unclassified`` says how many were not),
    and ``unknown`` when none was — then ``own``, ``foreign`` and ``no_asset``
    are None and ``reason`` says why. ``unreadable`` counts operations whose
    stored body is missing or names an asset that cannot be read.
    ``own_share`` is own over own and foreign together, operations without an
    asset left out; None when there is nothing to share.
    """

    state: str
    own: int | None
    foreign: int | None
    no_asset: int | None
    unreadable: int | None
    unclassified: int
    own_share: float | None
    reason: str


@dataclass(frozen=True)
class AccountActivity:
    """One watched account in the window. The four counts are of the records
    the account took part in: transactions and operations through the
    participants tables, effects by their account, trades on either side.
    ``last_event_at`` is the ledger time of the account's newest stored
    record of any time, not only of the window."""

    account_id: str
    enabled: bool
    origin: str
    transactions: int
    operations: int
    effects: int
    trades: int
    previous: dict[str, int]
    change: dict[str, Change]
    last_ledger: int | None
    last_event_at: datetime | None
    assets: Assets
    volume_bytes: int | None
    volume_parts: dict[str, int | None]


@dataclass(frozen=True)
class TableWidth:
    """A table as the volume formula sees it. ``bytes_per_row`` is None when
    the planner has no number of rows for the table, and ``reason`` says so."""

    name: str
    planner_rows: float | None
    heap_bytes: int
    toast_bytes: int
    index_bytes: int
    bytes_per_row: float | None
    reason: str = ""


@dataclass(frozen=True)
class ActivityPage:
    """One page of the activity table. ``totals`` are the distinct records of
    all the accounts under the filter — each once, however many of them took
    part in it — and are never a sum over the accounts. ``as_dict`` is what a
    page and a JSON endpoint show."""

    period: str
    window: Span
    previous: Span
    bounds: str
    enabled: bool | None
    sort: str
    descending: bool
    page: int
    per_page: int
    accounts: int
    pages: int
    rows: tuple[AccountActivity, ...]
    totals: dict[str, int]
    assets_rule: str
    widths: dict[str, TableWidth]
    volume_formula: str
    volume_warning: str
    measured_at: datetime

    def as_dict(self) -> dict[str, Any]:
        return plain(self)


@dataclass(frozen=True)
class TableSpeed:
    """The rows of one table stored in the window, by ``column``, the table's
    own time of storing. ``rows`` and ``rows_per_hour`` are None, and
    ``reason`` says why, when the table was not measured."""

    column: str
    rows: int | None
    rows_per_hour: float | None
    reason: str = ""


@dataclass(frozen=True)
class HourLoad:
    """One hour of the window (UTC, from its start): rows stored, by table and
    together. The last hour of the window is not over yet."""

    hour: datetime
    rows: int
    by_table: dict[str, int]


@dataclass(frozen=True)
class ImportSpeed:
    """How fast history is being stored: rows per hour over the window, by the
    time each row was stored. This is the time of the import, which has
    nothing to do with the ledger time the activity is counted by."""

    hours: int
    since: datetime
    until: datetime
    measured_at: datetime
    tables: dict[str, TableSpeed]
    series: tuple[HourLoad, ...]
    # Of all the measured tables; None when none was measured.
    rows: int | None
    rows_per_hour: float | None
    measured_tables: int
    note: str

    def as_dict(self) -> dict[str, Any]:
        return plain(self)


@dataclass(frozen=True)
class _Stored:
    """What one table's reading through found: rows by the hour they were
    stored in, or why it was not read."""

    column: str
    hourly: dict[datetime, int] = field(default_factory=dict)
    reason: str = ""


# -- windows -------------------------------------------------------------------


def _month_before(now: datetime) -> datetime:
    """The same day a month earlier; the last day of that month when it has fewer days."""
    year, month = (now.year - 1, 12) if now.month == 1 else (now.year, now.month - 1)
    leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    days = (31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    return now.replace(year=year, month=month, day=min(now.day, days[month - 1]))


def windows(period: Any, now: datetime | None = None) -> tuple[Span, Span, str]:
    """The window of ``period`` and the one of the same length just before it.

    ``period`` is ``day``, ``week`` (the default) or ``month``, each ending at
    ``now`` — a month is the calendar month that ends there — or a pair of
    datetimes: an explicit window. Times without a zone are UTC.

    Returns the window, the window before it, and the period's name (``range``
    for an explicit one).
    """
    now = moment(now)
    if period is None:
        period = DEFAULT_PERIOD
    if isinstance(period, str):
        if period == "day":
            length = DAY
        elif period == "week":
            length = 7 * DAY
        elif period == "month":
            length = now - _month_before(now)
        else:
            raise ValueError(f"unknown period {period!r}: one of {PERIODS}, or a pair of datetimes")
        return Span(now - length, now), Span(now - 2 * length, now - length), period
    if isinstance(period, (str, bytes, Mapping)) or not isinstance(period, Sequence):
        raise ValueError("a period is 'day', 'week', 'month' or a pair of datetimes (from, to)")
    if len(period) != 2 or not all(isinstance(item, datetime) for item in period):
        raise ValueError("an explicit period is a pair of datetimes (from, to)")
    start, end = moment(period[0]), moment(period[1])
    if start >= end:
        raise ValueError("the period is empty: its end must be after its start")
    length = end - start
    return Span(start, end), Span(start - length, start), "range"


# -- pieces of the statements --------------------------------------------------


def _account_range(
    alias: str, column: str, id_column: str, low: str, high: str, account: str = "a.account_id"
) -> str:
    """The rows of ``alias`` that belong to ``account`` and whose id lies in
    ``[low, high)`` of total order ids.

    Written as a comparison of (account column, id column) with two values,
    which only an index on exactly these two columns in this order serves.
    The account is said once more on its own, for the planner to estimate the
    rows by it.
    """
    return (
        f"{alias}.{column} = {account} AND ({alias}.{column}, {alias}.{id_column}) >= ({account}, {low}) "
        f"AND ({alias}.{column}, {alias}.{id_column}) < ({account}, {high})"
    )


def _sides(low: str, high: str) -> str:
    """The trades of the account in the range, each once whichever side it is on."""
    base = _account_range("t", "base_account_id", "operation_id", low, high)
    counter = _account_range("t", "counter_account_id", "operation_id", low, high)
    return f"SELECT t.id FROM history_trades t WHERE {base} UNION SELECT t.id FROM history_trades t WHERE {counter}"


def _ids(kind: str, low: str, high: str) -> str:
    """A SELECT of the ids of the account's records of ``kind`` in the range."""
    if kind == "transactions":
        range_ = _account_range("p", "account_id", "transaction_id", low, high)
        return f"SELECT p.transaction_id AS id FROM history_transaction_participants p WHERE {range_}"
    if kind == "operations":
        range_ = _account_range("p", "account_id", "operation_id", low, high)
        return f"SELECT p.operation_id AS id FROM history_operation_participants p WHERE {range_}"
    if kind == "effects":
        range_ = _account_range("e", "account_id", "operation_id", low, high)
        return f"SELECT e.id FROM history_effects e WHERE {range_}"
    return _sides(low, high)


def _count(kind: str, low: str, high: str) -> str:
    """A SELECT of how many records of ``kind`` the account has in the range."""
    return f"SELECT count(*) AS n FROM ({_ids(kind, low, high)}) counted"


# The ledgers whose close time lies in [start, end): their lowest and highest
# sequence. Ledger close times grow with the sequence, so this is one range.
_LEDGER_RANGE = text(
    "SELECT min(sequence), max(sequence) FROM history_ledgers WHERE close_time >= :start AND close_time < :end"
)

# Of every record an account has, the newest: its ledger, and when that closed.
# OFFSET 0 keeps this a subquery, read once per account — left to itself, the
# planner folds it into the query and reads each index twice, once for the
# column and once for the join.
_LAST_EVENT = """
CROSS JOIN LATERAL (SELECT GREATEST(
    (SELECT max(p.transaction_id) FROM history_transaction_participants p WHERE p.account_id = a.account_id),
    (SELECT max(p.operation_id) FROM history_operation_participants p WHERE p.account_id = a.account_id),
    (SELECT max(e.operation_id) FROM history_effects e WHERE e.account_id = a.account_id),
    (SELECT max(t.operation_id) FROM history_trades t WHERE t.base_account_id = a.account_id),
    (SELECT max(t.operation_id) FROM history_trades t WHERE t.counter_account_id = a.account_id)) AS id OFFSET 0) newest
LEFT JOIN history_ledgers ledger ON ledger.sequence = (newest.id >> 32)
"""

_WIDTHS = text(
    "SELECT c.relname, c.reltuples, pg_relation_size(c.oid), "
    "CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_relation_size(c.reltoastrelid) END, pg_indexes_size(c.oid) "
    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = current_schema() AND c.relname = ANY(:tables) ORDER BY c.relname"
)
_SET_TIMEOUT = text("SELECT set_config('statement_timeout', :timeout, true)")

_TABLE_BYTES = "SELECT pg_relation_size(to_regclass(:table))"
_STORED = (
    "SELECT date_trunc('hour', {column} AT TIME ZONE 'UTC') AS hour, count(*) FROM {table} "
    "WHERE {column} >= :since AND {column} < :until GROUP BY 1"
)


# -- the classification --------------------------------------------------------


def _named_assets(body: str) -> str:
    """A SELECT of the distinct assets the JSON ``body`` names, at any depth,
    in either form Horizon uses: under any key ending in ``asset_type`` (an
    object, with the code and the issuer under the same prefix — ``asset``,
    ``source_asset``, ``buying_asset``, each asset of a path, ...), or as the
    canonical string under ``asset``. It is what
    :func:`app.sources.horizon_records.assets_in` reads of a record, in SQL.
    An asset without its code and issuer gives NULL."""
    return (
        "SELECT DISTINCT asset FROM ("
        "SELECT CASE WHEN e.k = 'asset' THEN e.v WHEN e.v = 'native' THEN 'native' "
        "ELSE (m->>(replace(e.k, 'asset_type', '') || 'asset_code')) || ':' "
        "|| (m->>(replace(e.k, 'asset_type', '') || 'asset_issuer')) END AS asset "
        f"FROM jsonb_path_query({body}, 'lax $.**') m "
        # Only an object has keys; an empty object stands in for whatever else lies there.
        "CROSS JOIN LATERAL jsonb_each_text(CASE WHEN jsonb_typeof(m) = 'object' THEN m ELSE '{}'::jsonb END) e(k, v) "
        "WHERE e.k LIKE '%asset_type' OR (e.k = 'asset' "
        "AND e.v ~ '^(native|[A-Za-z0-9]{1,12}[:]G[0-9A-Z]{55})$')"
        ") named"
    )


def _assets_of(body: str) -> str:
    """A lateral SELECT of the distinct assets ``body`` names, and whether one
    of them cannot be read."""
    return (
        "CROSS JOIN LATERAL (SELECT COALESCE(bool_or(asset IS NULL), false) AS broken, "
        "COALESCE(array_agg(DISTINCT asset) FILTER (WHERE asset IS NOT NULL), '{}'::text[]) AS assets "
        f"FROM ({_named_assets(body)}) named) named"
    )


# The operations of the proved accounts in the classified range, with the
# assets each names; the assets their trustline_created effects name, with the
# first ledger of each; and, per operation and asset, whether it is the
# account's own. Each account's rows are read in a subquery of its own
# (OFFSET 0 keeps it one): an account of the page is a row of ``page``, and
# left to itself the planner may as well read the whole table and join.
_CLASSIFY = text(
    f"""
WITH page AS (
    SELECT * FROM unnest(CAST(:accounts AS text[]), CAST(:created AS bigint[]), CAST(:reach AS bigint[]))
        AS page(account_id, created, reach)),
operations AS MATERIALIZED (
    SELECT page.account_id, o.id AS operation_id, named.broken, named.assets, o.raw IS NULL AS no_body
    FROM page
    CROSS JOIN LATERAL (
        SELECT o.id, o.raw FROM history_operation_participants p JOIN history_operations o ON o.id = p.operation_id
        WHERE {_account_range(
            'p', 'account_id', 'operation_id', '(GREATEST(page.created, :low_ledger)::bigint << 32)',
            '((page.reach + 1)::bigint << 32)', 'page.account_id')}
        OFFSET 0) o
    {_assets_of('o.raw')}
),
effects AS (
    SELECT page.account_id, (e.operation_id >> 32) AS ledger,
           COALESCE((SELECT array_agg(DISTINCT asset) FROM ({_named_assets('e.raw')}) named), '{{}}'::text[]) AS assets
    FROM page
    CROSS JOIN LATERAL (
        SELECT e.operation_id, e.raw FROM history_effects e
        WHERE {_account_range(
            'e', 'account_id', 'operation_id', '(page.created::bigint << 32)',
            '((page.reach + 1)::bigint << 32)', 'page.account_id')}
            AND e.type = 'trustline_created'
        OFFSET 0) e
),
-- One line per asset, from the first ledger it is seen open in. An effect
-- that names no asset, or none that can be read, leaves a NULL behind.
lines AS MATERIALIZED (
    SELECT account_id, a.asset AS asset, min(ledger) AS ledger
    FROM effects
    CROSS JOIN LATERAL unnest(assets || CASE WHEN cardinality(assets) = 0 OR array_position(assets, NULL) IS NOT NULL
                                               THEN ARRAY[NULL]::text[] ELSE '{{}}'::text[] END) AS a(asset)
    GROUP BY account_id, a.asset
),
judged AS MATERIALIZED (
    SELECT n.account_id, n.operation_id, (issued.asset_id IS NOT NULL OR line.asset IS NOT NULL) AS own
    FROM operations n CROSS JOIN LATERAL unnest(n.assets) AS a(asset)
    LEFT JOIN history_assets issued ON issued.asset_id = a.asset AND issued.asset_issuer = n.account_id
    LEFT JOIN lines line ON line.account_id = n.account_id AND line.asset = a.asset AND line.ledger <= (n.operation_id >> 32)
)
SELECT n.account_id,
       count(*) AS total,
       count(*) FILTER (WHERE n.no_body OR n.broken) AS unreadable,
       count(*) FILTER (WHERE NOT (n.no_body OR n.broken) AND n.assets = '{{}}') AS no_asset,
       count(*) FILTER (WHERE NOT (n.no_body OR n.broken) AND n.assets <> '{{}}' AND NOT EXISTS (
           SELECT 1 FROM judged j WHERE j.account_id = n.account_id AND j.operation_id = n.operation_id AND NOT j.own)) AS own,
       count(*) FILTER (WHERE EXISTS (
           SELECT 1 FROM judged j WHERE j.account_id = n.account_id AND j.operation_id = n.operation_id AND NOT j.own)) AS foreign,
       bool_or(EXISTS (SELECT 1 FROM lines l WHERE l.account_id = n.account_id AND l.asset IS NULL)) AS broken_lines
FROM operations n GROUP BY n.account_id
"""
)

# A stored account_created effect of each account, through the index on the
# effect type (account_created is type 0). Gathered as one set before the
# accounts are joined in, so that the planner does not read every effect of
# every account of the page instead.
_CREATED = text(
    "WITH created AS MATERIALIZED (SELECT account_id, operation_id >> 32 AS ledger FROM history_effects "
    "WHERE type_i = 0 AND type = 'account_created' AND operation_id < :top) "
    "SELECT account_id, ledger FROM created WHERE account_id = ANY(:accounts) ORDER BY account_id, ledger"
)
_COVERAGE = text(
    "SELECT subject_key, from_ledger, to_ledger, state FROM sync_coverage "
    "WHERE subject_type = 'account' AND data_type = 'effects' AND subject_key = ANY(:accounts) "
    "ORDER BY subject_key, from_ledger"
)


# -- the table -----------------------------------------------------------------


def table(
    engine: Engine,
    *,
    period: Any = DEFAULT_PERIOD,
    now: datetime | None = None,
    enabled: bool | None = None,
    sort: str = DEFAULT_SORT,
    descending: bool = True,
    page: int = 1,
    per_page: int = DEFAULT_PER_PAGE,
    cache: Any | None = None,
) -> ActivityPage:
    """One page of the activity of the watched accounts.

    ``period`` and ``now``: see :func:`windows`. ``enabled`` None: every
    watched account; True or False: only those that are, or are not, enabled.
    ``sort`` is one of the four counts, or ``account`` (by address); most
    first unless ``descending`` is False. The sorted count is read for every
    account under the filter — there is no other way to sort by it — and
    everything else only for the accounts of the page. ``cache``
    (:class:`app.stats.dashboard.TtlCache`) keeps what is the same for every
    page: how wide the rows of the tables are.
    """
    now = moment(now)
    if sort not in SORTS:
        raise ValueError(f"unknown sort {sort!r}: one of {SORTS}")
    if page < 1:
        raise ValueError("page starts at 1")
    if not 1 <= per_page <= MAX_PER_PAGE:
        raise ValueError(f"per_page is 1 to {MAX_PER_PAGE}")
    window, before, name = windows(period, now)
    with reading(engine, timeout_seconds=STATEMENT_TIMEOUT_SECONDS, work_mem=WORK_MEM) as connection:
        ledgers = {True: _ledgers(connection, window), False: _ledgers(connection, before)}
        ids = {current: _id_range(found) for current, found in ledgers.items()}
        statement, parameters = _rank(sort, ids[True], enabled, descending, per_page, (page - 1) * per_page)
        ranked = connection.execute(statement, parameters).all()
        counted = (
            {}
            if not ranked
            else {
                row["account_id"]: row
                for row in connection.execute(
                    _details(ids), {**_parameters(ids), "accounts": [row.account_id for row in ranked]}
                ).mappings()
            }
        )
        assets = _classify(
            connection,
            [row.account_id for row in ranked],
            ledgers[True],
            # 0 for every account when the window holds no ledger: there is no column for it then.
            {account: row["operations"] if "operations" in row else 0 for account, row in counted.items()},
        )
        totals = _totals(connection, ids[True], enabled)
        widths = _widths(connection, now, cache)
        # A page beyond the last has no row to say how many accounts there are.
        total = ranked[0].total if ranked else connection.execute(_accounts(enabled), {"enabled": enabled}).scalar_one()
    return ActivityPage(
        period=name,
        window=window,
        previous=before,
        bounds=BOUNDS,
        enabled=enabled,
        sort=sort,
        descending=descending,
        page=page,
        per_page=per_page,
        accounts=total,
        pages=max(1, -(-total // per_page)),
        rows=tuple(_activity(row, counted.get(row.account_id), assets.get(row.account_id), widths) for row in ranked),
        totals=totals,
        assets_rule=ASSETS_RULE,
        widths=widths,
        volume_formula=VOLUME_FORMULA,
        volume_warning=VOLUME_WARNING,
        measured_at=now,
    )


def _ledgers(connection: Connection, span: Span) -> tuple[int, int] | None:
    """The lowest and the highest sequence of the ledgers that closed in
    ``span``; None when none did, as far as the database knows."""
    low, high = connection.execute(_LEDGER_RANGE, {"start": span.start, "end": span.end}).one()
    return None if low is None else (low, high)


def _id_range(ledgers: tuple[int, int] | None) -> tuple[int, int] | None:
    """The range of total order ids of ``ledgers``: a total order id begins
    with the ledger, so every record of those ledgers lies in it."""
    return None if ledgers is None else (ledgers[0] << 32, (ledgers[1] + 1) << 32)


def _parameters(ids: Mapping[bool, tuple[int, int] | None]) -> dict[str, int]:
    current, before = ids[True] or (0, 0), ids[False] or (0, 0)
    return {"low": current[0], "high": current[1], "before_low": before[0], "before_high": before[1]}


def _accounts(enabled: bool | None) -> TextClause:
    """How many accounts are under the filter — for a page beyond the last,
    which has no row to say it."""
    where = "" if enabled is None else "WHERE enabled = :enabled"
    return text(f"SELECT count(*) FROM tracked_accounts {where}")


def _rank(
    sort: str, ids: tuple[int, int] | None, enabled: bool | None, descending: bool, limit: int, offset: int
) -> tuple[TextClause, dict[str, Any]]:
    """The accounts of the page: every account under the filter with the one
    count that is sorted by, in order, cut to the page — and, on each row,
    how many accounts there are under the filter, for the pages that follow."""
    if sort == "account" or ids is None:
        # Nothing is in the window: every count is 0, and the order is by account.
        lateral, order = "", f"a.account_id{' DESC' if descending else ''}"
    else:
        lateral, order = f"CROSS JOIN LATERAL ({_count(sort, ':low', ':high')}) k", f"k.n{' DESC' if descending else ''}, a.account_id"
    where = "" if enabled is None else "WHERE a.enabled = :enabled"
    statement = text(
        "SELECT a.account_id, a.enabled, a.origin, count(*) OVER () AS total "
        f"FROM tracked_accounts a {lateral} {where} ORDER BY {order} LIMIT :limit OFFSET :offset"
    )
    return statement, {"low": (ids or (0, 0))[0], "high": (ids or (0, 0))[1], "enabled": enabled, "limit": limit, "offset": offset}


def _details(ids: Mapping[bool, tuple[int, int] | None]) -> TextClause:
    """All the figures of an account, for the accounts of the page only."""
    laterals, columns = [], []
    for prefix, current in (("", True), ("before_", False)):
        if ids[current] is not None:
            for index, kind in enumerate(KINDS):
                alias = f"{prefix}c{index}"
                laterals.append(f"CROSS JOIN LATERAL ({_count(kind, f':{prefix}low', f':{prefix}high')}) {alias}")
                columns.append(f"{alias}.n AS {prefix}{kind}")
    shown = ["a.account_id", "a.enabled", "a.origin", *columns, "(newest.id >> 32) AS last_ledger", "ledger.close_time AS last_event_at"]
    return text(f"SELECT {', '.join(shown)} FROM tracked_accounts a {' '.join(laterals)} {_LAST_EVENT} WHERE a.account_id = ANY(:accounts)")


def _totals(connection: Connection, ids: tuple[int, int] | None, enabled: bool | None) -> dict[str, int]:
    """The distinct records of all the accounts under the filter in the
    window, each once: read on their own, never added up over the accounts.

    A transaction, an operation or a trade can be several accounts' — it is
    counted once. An effect is one account's, so its rows are its records.
    """
    if ids is None:
        return dict.fromkeys(KINDS, 0)
    where = "" if enabled is None else "WHERE a.enabled = :enabled"
    selects = ", ".join(
        f"(SELECT {'count(*)' if kind == 'effects' else 'count(DISTINCT x.id)'} FROM tracked_accounts a "
        f"CROSS JOIN LATERAL ({_ids(kind, ':low', ':high')} OFFSET 0) x {where}) AS {kind}"
        for kind in KINDS
    )
    row = connection.execute(text(f"SELECT {selects}"), {"low": ids[0], "high": ids[1], "enabled": enabled}).mappings().one()
    return {kind: row[kind] for kind in KINDS}


def _widths(connection: Connection, now: datetime, cache: Any | None) -> dict[str, TableWidth]:
    """How wide a row of each table of the volume formula is, as the catalog
    says. Kept in the cache: it is the same for every page."""
    key = "activity:widths"
    kept = None if cache is None else cache.get(key)
    if isinstance(kept, dict):
        return kept
    widths = {}
    for name, rows, heap, toast, indexes in connection.execute(_WIDTHS, {"tables": list(VOLUME_TABLE_NAMES)}):
        if rows is None or rows <= 0:
            widths[name] = TableWidth(name, None, heap, toast, indexes, None, NO_ROW_COUNT)
        else:
            widths[name] = TableWidth(name, rows, heap, toast, indexes, (heap + toast + indexes) / rows)
    if cache is not None:
        cache.put(key, widths, now)
    return widths


def _volume(counts: Mapping[str, int], widths: Mapping[str, TableWidth]) -> tuple[int | None, dict[str, int | None]]:
    """The volume by the formula, and its parts by kind of record."""
    parts = {}
    for kind, tables in VOLUME_TABLES.items():
        per_row = [widths[table].bytes_per_row if table in widths else None for table in tables]
        parts[kind] = None if any(item is None for item in per_row) else round(counts[kind] * sum(per_row))
    return (None if any(part is None for part in parts.values()) else sum(parts.values())), parts


def _change(current: int, before: int) -> Change:
    return Change(current - before, None if before == 0 else round(100.0 * (current - before) / before, 2))


def _activity(
    ranked: Any, counted: Mapping[str, Any] | None, assets: Assets | None, widths: Mapping[str, TableWidth]
) -> AccountActivity:
    """The row of one account of the page. ``counted`` is None when the window
    and the one before it both hold no ledger: every count is 0 then."""
    counted = counted or {}
    current = {kind: counted.get(kind, 0) or 0 for kind in KINDS}
    before = {kind: counted.get(f"before_{kind}", 0) or 0 for kind in KINDS}
    volume, parts = _volume(current, widths)
    return AccountActivity(
        account_id=ranked.account_id,
        enabled=ranked.enabled,
        origin=ranked.origin,
        transactions=current["transactions"],
        operations=current["operations"],
        effects=current["effects"],
        trades=current["trades"],
        previous=before,
        change={kind: _change(current[kind], before[kind]) for kind in KINDS},
        last_ledger=counted.get("last_ledger"),
        last_event_at=counted.get("last_event_at"),
        assets=assets if assets is not None else _unknown(current["operations"], NO_CREATION_STORED),
        volume_bytes=volume,
        volume_parts=parts,
    )


# -- own and foreign assets ----------------------------------------------------


def _verified_to(ranges: Sequence[tuple[int, int, str]], created: int, top: int) -> int:
    """The highest ledger up to which [``created``, ledger] is covered without
    gaps by verified ranges (complete or empty); ``created - 1`` when none is."""
    reach = created - 1
    for low, high, state in sorted(ranges):
        if reach >= top or low > reach + 1:
            break  # verified up to the top of the window; or a gap: beyond it nothing is proved
        if state in ("complete", "empty") and high > reach:
            reach = high
    return reach


def _unknown(unclassified: int, reason: str) -> Assets:
    """Nothing classified: unknown — unless there was nothing to classify."""
    if unclassified == 0:
        return Assets(CLASSIFIED, 0, 0, 0, 0, 0, None, "")
    return Assets(UNKNOWN, None, None, None, None, unclassified, None, reason)


def _assets(judged: Any, operations: int, reach: int) -> Assets:
    """The classification of one proved account from its counts. Operations
    of the window beyond where its effects are verified are not judged."""
    if judged is not None and judged.broken_lines:
        return _unknown(operations, UNREADABLE_LINE)
    own, foreign, no_asset, unreadable = (0, 0, 0, 0) if judged is None else (judged.own, judged.foreign, judged.no_asset, judged.unreadable)
    judged_count = own + foreign + no_asset + unreadable
    unclassified = max(0, operations - judged_count)
    if not judged_count:
        return _unknown(operations, VERIFIED_ONLY_UP_TO.format(ledger=reach))  # unknown unless there was nothing to judge
    share = None if own + foreign == 0 else round(own / (own + foreign), 4)
    if unclassified:
        return Assets(PARTIAL, own, foreign, no_asset, unreadable, unclassified, share, VERIFIED_ONLY_UP_TO.format(ledger=reach))
    return Assets(CLASSIFIED, own, foreign, no_asset, unreadable, 0, share, "")


def _classify(
    connection: Connection, accounts: Sequence[str], bounds: tuple[int, int] | None, operations: Mapping[str, int]
) -> dict[str, Assets]:
    """The operations of the accounts of the page by their assets, where the
    database proves it (``ASSETS_RULE``); unknown, with the reason, elsewhere.

    Best effort: judging the operations has a time budget of its own, and
    what runs out of it is unknown — not a missing page.
    """
    result = {account: _unknown(operations.get(account, 0), NO_CREATION_STORED) for account in accounts}
    if not accounts or bounds is None:
        return result
    low, high = bounds
    creations: dict[str, list[int]] = defaultdict(list)
    for account, ledger in connection.execute(_CREATED, {"top": (high + 1) << 32, "accounts": list(accounts)}):
        creations[account].append(ledger)
    created: dict[str, int] = {}
    for account, ledgers in creations.items():
        if len(ledgers) == 1:
            created[account] = ledgers[0]
        else:
            result[account] = _unknown(operations.get(account, 0), CREATED_MORE_THAN_ONCE)
    if not created:
        return result
    ranges: dict[str, list[tuple[int, int, str]]] = defaultdict(list)
    for account, from_ledger, to_ledger, state in connection.execute(_COVERAGE, {"accounts": list(created)}):
        ranges[account].append((from_ledger, to_ledger, state))
    proved: dict[str, tuple[int, int]] = {}
    for account, ledger in created.items():
        reach = _verified_to(ranges.get(account, []), ledger, high)
        if reach >= low:
            proved[account] = (ledger, reach)
        else:
            result[account] = _unknown(operations.get(account, 0), NOT_VERIFIED_FROM_CREATION.format(ledger=low))
    if not proved:
        return result
    try:
        try:
            with connection.begin_nested():
                connection.execute(_SET_TIMEOUT, {"timeout": f"{CLASSIFICATION_TIMEOUT_SECONDS:g}s"})
                judged = {
                    row.account_id: row
                    for row in connection.execute(
                        _CLASSIFY,
                        {
                            "accounts": list(proved),
                            "created": [ledger for ledger, _ in proved.values()],
                            "reach": [reach for _, reach in proved.values()],
                            "low_ledger": low,
                        },
                    ).all()
                }
        except OperationalError as error:
            if not timed_out(error):
                raise
            for account in proved:
                result[account] = _unknown(operations.get(account, 0), RAN_OUT_OF_TIME)
        else:
            for account, (_, reach) in proved.items():
                result[account] = _assets(judged.get(account), operations.get(account, 0), reach)
    finally:
        connection.execute(_SET_TIMEOUT, {"timeout": f"{STATEMENT_TIMEOUT_SECONDS:g}s"})
    return result


# -- the speed of the import ---------------------------------------------------


def import_speed(
    engine: Engine, *, now: datetime | None = None, hours: int = 24, cache: Any | None = None
) -> ImportSpeed:
    """Rows stored per hour over the last ``hours`` (24: the last day), by the
    time each row was stored. Apart from :func:`table` on purpose: activity
    is by ledger time, and this is by the time of the import.

    The window is cut into whole hours of UTC and ends at ``now``: the first
    hour is whole and the last is not over yet. Each table is read through —
    nothing indexes the time a row was stored — so a table larger than
    ``SCAN_LIMIT_BYTES`` is not measured, and says so. ``cache`` keeps the
    whole figure.
    """
    now = moment(now)
    if isinstance(hours, bool) or not isinstance(hours, int) or not 1 <= hours <= 24 * 31:
        raise ValueError("hours is a whole number of hours, from 1 to the hours of 31 days")
    key = f"activity:import_speed:{hours}"
    kept = None if cache is None else cache.get(key)
    if isinstance(kept, ImportSpeed):
        return kept
    since = (now - timedelta(hours=hours - 1)).replace(minute=0, second=0, microsecond=0)
    with reading(engine, timeout_seconds=IMPORT_TIMEOUT_SECONDS) as connection:
        stored = {table: _stored(connection, table, column, since, now) for table, column in IMPORTED}
    by_hour: dict[datetime, dict[str, int]] = defaultdict(dict)
    for table, found in stored.items():
        for hour, rows in found.hourly.items():
            by_hour[hour][table] = rows
    measured = [found for found in stored.values() if not found.reason]
    rows = sum(sum(found.hourly.values()) for found in measured)
    speed = ImportSpeed(
        hours=hours,
        since=since,
        until=now,
        measured_at=now,
        tables={
            table: TableSpeed(
                found.column,
                None if found.reason else sum(found.hourly.values()),
                None if found.reason else round(sum(found.hourly.values()) / hours, 2),
                found.reason,
            )
            for table, found in stored.items()
        },
        series=tuple(
            HourLoad(hour, sum(by_hour[hour].values()), dict(by_hour[hour]))
            for hour in (since + timedelta(hours=index) for index in range(hours))
        ),
        rows=rows if measured else None,
        rows_per_hour=round(rows / hours, 2) if measured else None,
        measured_tables=len(measured),
        note=IMPORT_NOTE,
    )
    if cache is not None:
        cache.put(key, speed, now)
    return speed


def _stored(connection: Connection, table: str, column: str, since: datetime, until: datetime) -> _Stored:
    """The rows of ``table`` stored in [since, until), by the hour they were
    stored in — or why they were not read. The statement has a savepoint of
    its own: a table that runs out of time is left out, the rest is measured."""
    if table not in IMPORTED_TABLES:
        raise ValueError(f"not a table this module reads: {table!r}")
    size = connection.execute(text(_TABLE_BYTES), {"table": table}).scalar()
    if size is None:
        return _Stored(column, reason="not measured: there is no such table")
    if size > SCAN_LIMIT_BYTES:
        return _Stored(
            column,
            reason=f"not measured: the table is larger than {SCAN_LIMIT_BYTES} bytes. Nothing indexes the time a row "
            "was stored, and a page does not read a table of that size through",
        )
    try:
        with connection.begin_nested():
            found = connection.execute(
                text(_STORED.format(table=table, column=column)), {"since": since, "until": until}
            ).all()
    except OperationalError as error:
        if not timed_out(error):
            raise
        return _Stored(column, reason="not measured: reading the table through ran out of its time")
    return _Stored(column, {hour.replace(tzinfo=UTC): count for hour, count in found})
