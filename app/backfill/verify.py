"""Measuring a loaded window: against the source, and against itself."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, text

from app.backfill.jobs import StreamJob, asset_params
from app.backfill.loader import Window
from app.sources import horizon_records as records
from app.sources.horizon import HorizonClient
from app.sources.horizon_cursors import after_ledger
from app.sources.horizon_format import amount_to_stroops

DAY_MS = 86_400_000

# Where the records of each account stream live once stored.
_ACCOUNT_STREAMS: dict[str, str] = {
    "transactions": (
        "SELECT t.id::text FROM history_transactions t "
        "JOIN history_transaction_participants p ON p.transaction_id = t.id "
        "WHERE p.account_id = :subject AND t.ledger_sequence BETWEEN :from AND :to"
    ),
    "operations": (
        "SELECT o.id::text FROM history_operations o "
        "JOIN history_operation_participants p ON p.operation_id = o.id "
        "WHERE p.account_id = :subject AND (o.id >> 32) BETWEEN :from AND :to"
    ),
    "effects": (
        "SELECT e.id FROM history_effects e "
        "WHERE e.account_id = :subject AND (e.operation_id >> 32) BETWEEN :from AND :to"
    ),
    "trades": (
        "SELECT t.id FROM history_trades t "
        "WHERE (t.base_account_id = :subject OR t.counter_account_id = :subject) "
        "AND (t.operation_id >> 32) BETWEEN :from AND :to"
    ),
}
_MARKET_TRADES = (
    "SELECT t.id FROM history_trades t "
    "WHERE ((t.base_asset_id = :a AND t.counter_asset_id = :b) "
    "OR (t.base_asset_id = :b AND t.counter_asset_id = :a)) "
    "AND (t.operation_id >> 32) BETWEEN :from AND :to"
)
_TABLES: dict[str, tuple[str, str, str]] = {
    "ledgers": ("history_ledgers", "sequence BETWEEN :from AND :to", "sequence"),
    "transactions": ("history_transactions", "ledger_sequence BETWEEN :from AND :to", "id"),
    "operations": ("history_operations", "(id >> 32) BETWEEN :from AND :to", "id"),
    "transaction_participants": (
        "history_transaction_participants",
        "(transaction_id >> 32) BETWEEN :from AND :to",
        "transaction_id, account_id",
    ),
    "operation_participants": (
        "history_operation_participants",
        "(operation_id >> 32) BETWEEN :from AND :to",
        "operation_id, account_id",
    ),
    "effects": ("history_effects", "(operation_id >> 32) BETWEEN :from AND :to", "id"),
    "trades": ("history_trades", "(operation_id >> 32) BETWEEN :from AND :to", "id"),
}


def read_window(
    client: HorizonClient, job: StreamJob, window, page_size: int, on_page: Callable[[], None] | None = None
) -> Iterator[Mapping[str, Any]]:
    """The records of one stream inside the window, newest first, read from
    the source and stored nowhere."""
    page = client.page(
        job.path,
        resource_type=job.resource_type,
        order="desc",
        cursor=str(after_ledger(window.to_ledger)),
        limit=page_size,
        **job.filters,
    )
    while True:
        if on_page is not None:
            on_page()
        for record in page.records:
            if records.ledger_of(record, job.resource_type) < window.from_ledger:
                return
            yield record
        if page.exhausted:
            return
        page = client.next_page(page)
        assert page is not None, "an exhausted page is the end of the stream"


def record_id(job: StreamJob, record: Mapping[str, Any]) -> str:
    return str(record["paging_token"]) if job.stream == "transactions" else str(record["id"])


# -- before a long run --------------------------------------------------------


@dataclass
class StreamEstimate:
    stream: str
    pages: int = 0
    records: int = 0
    # Distinct transactions for the transactions and operations streams; for
    # the others the distinct operations they point at, which is at least as
    # many as the transactions.
    transactions: int = 0
    # Whether `transactions` counts transactions exactly.
    exact: bool = False

    @property
    def context_requests_upper_bound(self) -> int:
        # A transaction found through its hash costs three requests (its
        # operations, effects, trades); one found through an operation two
        # more (the operation, then the transaction).
        return self.transactions * (3 if self.exact else 5)

    def as_dict(self) -> dict[str, Any]:
        return {
            "stream": self.stream,
            "pages": self.pages,
            "records": self.records,
            "transactions": self.transactions,
            "transactions_exact": self.exact,
            "context_requests_upper_bound": self.context_requests_upper_bound,
        }


def estimate(client: HorizonClient, jobs: Sequence[StreamJob], window, page_size: int) -> list[StreamEstimate]:
    """Count what each stream has in the window by reading its pages.

    Only the pages are read, not the contexts. The number of requests a run
    will need for contexts follows as an upper bound.
    """
    estimates: list[StreamEstimate] = []
    for job in jobs:
        found = StreamEstimate(job.name)
        found.exact = job.stream in ("transactions", "operations")
        seen: set[int] = set()

        def counted(found: StreamEstimate = found) -> None:
            found.pages += 1

        for record in read_window(client, job, window, page_size, counted):
            found.records += 1
            if job.stream == "transactions":
                seen.add(_transaction_id(record))
            elif job.stream == "operations":
                seen.add(records.transaction_of(record))
            else:
                seen.add(records.operation_of(record))
        found.transactions = len(seen)
        estimates.append(found)
    return estimates


def _transaction_id(transaction: Mapping[str, Any]) -> int:
    return int(str(transaction["paging_token"]))


# -- comparing with the source ------------------------------------------------


@dataclass
class StreamComparison:
    stream: str
    source: int
    stored: int
    missing: list[str] = field(default_factory=list)
    extra: list[str] = field(default_factory=list)

    @property
    def matches(self) -> bool:
        return not self.missing and not self.extra

    def as_dict(self) -> dict[str, Any]:
        shown = 10
        return {
            "stream": self.stream,
            "source": self.source,
            "stored": self.stored,
            "matches": self.matches,
            "missing": len(self.missing),
            "extra": len(self.extra),
            "missing_examples": self.missing[:shown],
            "extra_examples": self.extra[:shown],
        }


def verify(client: HorizonClient, engine: Engine, jobs: Sequence[StreamJob], window, page_size: int) -> list[StreamComparison]:
    """Read every stream of the window from the source and compare its record
    ids with what is stored for the same stream and ledger window."""
    comparisons: list[StreamComparison] = []
    for job in jobs:
        source: set[str] = set()
        for record in read_window(client, job, window, page_size):
            source.add(record_id(job, record))
        stored = set(_stored_ids(engine, job, window))
        comparisons.append(
            StreamComparison(
                job.name, len(source), len(stored), sorted(source - stored), sorted(stored - source)
            )
        )
    return comparisons


def _stored_ids(engine: Engine, job: StreamJob, window) -> list[str]:
    if job.key.subject_type == "market":
        low, high = job.key.subject_key.split(">", 1)
        statement, parameters = _MARKET_TRADES, {"a": low, "b": high}
    else:
        statement = _ACCOUNT_STREAMS[job.stream]
        parameters = {"subject": job.key.subject_key}
    with engine.connect() as connection:
        rows = connection.execute(
            text(statement), {**parameters, "from": window.from_ledger, "to": window.to_ledger}
        ).all()
    return [row[0] for row in rows]


# -- the market against /trade_aggregations -----------------------------------


@dataclass
class BucketComparison:
    day: str
    source: dict[str, int]
    stored: dict[str, int]

    @property
    def matches(self) -> bool:
        return self.source == self.stored

    def as_dict(self) -> dict[str, Any]:
        return {"day": self.day, "source": self.source, "stored": self.stored, "matches": self.matches}


def whole_days(client: HorizonClient, window: Window) -> tuple[int, int]:
    """The whole UTC days inside a ledger window: the first one and how many.

    A ledger window does not start at midnight. Days a ledger of the window
    closes in are not whole, and are left out.
    """
    first = _closed_ms(client, window.from_ledger)
    last = _closed_ms(client, window.to_ledger)
    first_day = -(-first // DAY_MS) * DAY_MS
    last_day = last // DAY_MS * DAY_MS
    return first_day, max(0, (last_day - first_day) // DAY_MS)


def _closed_ms(client: HorizonClient, sequence: int) -> int:
    closed = client.get(f"/ledgers/{sequence}")["closed_at"]
    seconds = datetime.fromisoformat(closed).timestamp()
    return int(seconds * 1000)


def compare_aggregations(
    client: HorizonClient, engine: Engine, base: str, counter: str, first_day_ms: int, days: int
) -> list[BucketComparison]:
    """Per UTC day: trade count and both volumes as the source aggregates
    them, against the same sums over the stored trades of the pair.

    The stored trades are added on the side they were stored, base or
    counter, which is the orientation of the source's answer and not of this
    request; the sums are turned around where needed.
    """
    buckets: dict[int, dict[str, int]] = {}
    for offset in range(0, days, 200):
        body = client.get(
            "/trade_aggregations",
            start_time=first_day_ms,
            end_time=first_day_ms + days * DAY_MS,
            resolution=DAY_MS,
            limit=min(200, days - offset),
            offset=offset,
            **asset_params("base_", base),
            **asset_params("counter_", counter),
        )
        for record in body.get("_embedded", {}).get("records", []):
            buckets[int(record["timestamp"])] = {
                "trades": int(record["trade_count"]),
                "base_volume": amount_to_stroops(record["base_volume"]),
                "counter_volume": amount_to_stroops(record["counter_volume"]),
            }
    zero = {"trades": 0, "base_volume": 0, "counter_volume": 0}
    comparisons = []
    with engine.connect() as connection:
        for day in range(first_day_ms, first_day_ms + days * DAY_MS, DAY_MS):
            trades, base_volume, counter_volume = connection.execute(
                text(_MARKET_DAY),
                {
                    "base": base,
                    "counter": counter,
                    "start": datetime.fromtimestamp(day / 1000, UTC),
                    "end": datetime.fromtimestamp((day + DAY_MS) / 1000, UTC),
                },
            ).one()
            comparisons.append(
                BucketComparison(
                    datetime.fromtimestamp(day / 1000, UTC).strftime("%Y-%m-%d"),
                    buckets.get(day, zero),
                    {"trades": trades, "base_volume": int(base_volume or 0), "counter_volume": int(counter_volume or 0)},
                )
            )
    return comparisons


_MARKET_DAY = (
    "SELECT count(*), "
    "COALESCE(sum(CASE WHEN base_asset_id = :base THEN base_amount ELSE counter_amount END), 0), "
    "COALESCE(sum(CASE WHEN base_asset_id = :counter THEN base_amount ELSE counter_amount END), 0) "
    "FROM history_trades WHERE (base_asset_id = :base AND counter_asset_id = :counter "
    "OR base_asset_id = :counter AND counter_asset_id = :base) "
    "AND ledger_close_time >= :start AND ledger_close_time < :end"
)


# -- a fingerprint of what is stored ------------------------------------------


def fingerprint(engine: Engine, window) -> dict[str, dict[str, Any]]:
    """Row counts and a digest of every stored row per table, for the window.

    Two databases holding the same rows have the same digest; row bookkeeping
    columns (``created_at``, ``updated_at``) and the raw JSON are left out.
    """
    result: dict[str, dict[str, Any]] = {}
    with engine.connect() as connection:
        for name, (table, condition, order) in _TABLES.items():
            rows, digest = connection.execute(
                text(
                    # Each row is hashed on its own first: the aggregate stays
                    # small whatever the size of the window.
                    f"SELECT count(*), md5(COALESCE(string_agg(md5((to_jsonb(t) - 'raw' - 'created_at' "
                    f"- 'updated_at')::text), '' ORDER BY {order}), '')) FROM {table} t WHERE {condition}"
                ),
                {"from": window.from_ledger, "to": window.to_ledger},
            ).one()
            result[name] = {"rows": rows, "digest": digest}
    return result


def print_table(title: str, rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> None:
    widths = {
        column: max([len(column)] + [len(str(row.get(column, ""))) for row in rows]) for column in columns
    }
    print(title)
    print("  " + "  ".join(column.ljust(widths[column]) for column in columns))
    for row in rows:
        print("  " + "  ".join(str(row.get(column, "")).ljust(widths[column]) for column in columns))
