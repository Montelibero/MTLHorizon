"""Measuring a loaded window: against the source, and against itself."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from fractions import Fraction
from typing import Any

from sqlalchemy import Engine, text

from app.backfill.jobs import StreamJob, asset_params
from app.backfill.loader import Window
from app.sources import horizon_records as records
from app.sources.horizon import HorizonClient
from app.sources.horizon_cursors import after_ledger
from app.sources.horizon_format import amount_to_stroops, stroops_to_amount

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
    """One bucket of the market: what the source aggregates, and the same
    over the stored trades of the pair.

    The source counts trades and sums both volumes, takes the average price
    as counter volume over base volume printed with seven decimals, and
    takes high, low, open and close from the prices the trades were made at
    — the offer prices, not the ratios of the amounts. Open and close are
    the prices of the first and the last trade of the bucket.

    Volumes are in stroops. Prices are exact fractions (counter per base)
    where the source serves them as ``*_r``, and seven-decimal strings where
    it does not.
    """

    start: str
    source: dict[str, Any]
    stored: dict[str, Any]

    @property
    def matches(self) -> bool:
        return self.source == self.stored

    @property
    def differs_in(self) -> list[str]:
        return sorted(name for name in set(self.source) | set(self.stored) if self.source.get(name) != self.stored.get(name))

    def as_dict(self) -> dict[str, Any]:
        return {
            "start": self.start,
            "source": _shown(self.source),
            "stored": _shown(self.stored),
            "matches": self.matches,
        }


PRICE_FIELDS = ("high", "low", "open", "close")
VOLUME_FIELDS = ("trades", "base_volume", "counter_volume")
AVG = "avg"


def whole_buckets(client: HorizonClient, window: Window, resolution_ms: int) -> tuple[int, int]:
    """The whole buckets of ``resolution_ms`` inside a ledger window: when
    the first begins (in ms since the epoch) and how many there are.

    A ledger window does not start on a bucket's edge. Buckets a ledger of
    the window closes in are not whole, and are left out.
    """
    first = _closed_ms(client, window.from_ledger)
    last = _closed_ms(client, window.to_ledger)
    first_bucket = -(-first // resolution_ms) * resolution_ms
    last_bucket = last // resolution_ms * resolution_ms
    return first_bucket, max(0, (last_bucket - first_bucket) // resolution_ms)


def whole_days(client: HorizonClient, window: Window) -> tuple[int, int]:
    """The whole UTC days inside a ledger window: the first one and how many."""
    return whole_buckets(client, window, DAY_MS)


def _closed_ms(client: HorizonClient, sequence: int) -> int:
    closed = client.get(f"/ledgers/{sequence}")["closed_at"]
    seconds = datetime.fromisoformat(closed).timestamp()
    return int(seconds * 1000)


def compare_aggregations(
    client: HorizonClient, engine: Engine, base: str, counter: str, first_ms: int, buckets: int, resolution_ms: int = DAY_MS
) -> list[BucketComparison]:
    """Per bucket of ``resolution_ms`` from ``first_ms`` on: the stored
    trades of the pair against what the source aggregates for them."""
    records: list[Mapping[str, Any]] = []
    for offset in range(0, buckets, 200):
        body = client.get(
            "/trade_aggregations",
            start_time=first_ms,
            end_time=first_ms + buckets * resolution_ms,
            resolution=resolution_ms,
            limit=min(200, buckets - offset),
            offset=offset,
            **asset_params("base_", base),
            **asset_params("counter_", counter),
        )
        records.extend(body.get("_embedded", {}).get("records", []))
    return compare_bucket_records(records, engine, base, counter, first_ms, buckets, resolution_ms)


# A stored trade of the pair in the orientation asked for:
# (base and counter amount in stroops, the price as numerator and
# denominator, the operation id and the order inside it).
Trade = tuple[int, int, int, int, int, int]


def compare_bucket_records(
    records: Sequence[Mapping[str, Any]],
    engine: Engine,
    base: str,
    counter: str,
    first_ms: int,
    buckets: int,
    resolution_ms: int,
) -> list[BucketComparison]:
    """The same comparison over buckets the source has already answered: its
    records as they came, one per bucket with trades."""
    served: dict[int, dict[str, Any]] = {}
    exact_prices: set[int] = set()  # buckets whose prices the source serves as fractions
    for record in records:
        start = int(record["timestamp"])
        served[start] = _bucket_values(record)
        if all(f"{name}_r" in record for name in PRICE_FIELDS):
            exact_prices.add(start)
    grouped: dict[int, list[Trade]] = defaultdict(list)
    with engine.connect() as connection:
        rows = connection.execute(
            text(_PAIR_TRADES),
            {
                "base": base,
                "counter": counter,
                "start": datetime.fromtimestamp(first_ms / 1000, UTC),
                "end": datetime.fromtimestamp((first_ms + buckets * resolution_ms) / 1000, UTC),
            },
        )
        for closed, stored_base, base_amount, counter_amount, price_n, price_d, operation_id, order in rows:
            if stored_base != base:  # stored the other way round
                base_amount, counter_amount = counter_amount, base_amount
                price_n, price_d = price_d, price_n
            moment = int(closed.timestamp() * 1000)
            grouped[moment - moment % resolution_ms].append(
                (base_amount, counter_amount, price_n, price_d, operation_id, order)
            )

    comparisons = []
    for index in range(buckets):
        start = first_ms + index * resolution_ms
        source = served.get(start)
        # Prices are compared exactly where the source gives them exactly.
        exact = start in exact_prices
        comparisons.append(
            BucketComparison(
                datetime.fromtimestamp(start / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                source if source is not None else _summed([], exact),
                _summed(grouped.get(start, []), exact),
            )
        )
    return comparisons


def _bucket_values(record: Mapping[str, Any]) -> dict[str, Any]:
    """One bucket of the source's answer; absent fields are left out."""
    values: dict[str, Any] = {}
    for name, served in (("trades", "trade_count"), ("base_volume", "base_volume"), ("counter_volume", "counter_volume")):
        if record.get(served) is not None:
            values[name] = int(record[served]) if name == "trades" else amount_to_stroops(record[served])
    if record.get(AVG) is not None:
        values[AVG] = _seven_decimals(Fraction(str(record[AVG])))
    for name in PRICE_FIELDS:
        exact = record.get(f"{name}_r")
        if isinstance(exact, Mapping) and exact.get("n") is not None:
            values[name] = Fraction(int(exact["n"]), int(exact["d"]))
        elif record.get(name) is not None:
            values[name] = _seven_decimals(Fraction(str(record[name])))
    return values


def _summed(trades: Sequence[Trade], exact: bool) -> dict[str, Any]:
    """The same bucket over stored trades of the pair, in the orientation
    asked for. ``exact`` keeps prices as fractions; otherwise they are
    printed with seven decimals, as the source prints them."""
    values: dict[str, Any] = {
        "trades": len(trades),
        "base_volume": sum(trade[0] for trade in trades),
        "counter_volume": sum(trade[1] for trade in trades),
    }
    if not trades:
        return values
    values[AVG] = _seven_decimals(Fraction(values["counter_volume"], values["base_volume"]))

    def price(trade: Trade) -> Fraction:
        return Fraction(trade[2], trade[3])

    priced = [trade for trade in trades if trade[3]]
    if priced:
        in_order = sorted(priced, key=lambda trade: (trade[4], trade[5]))
        chosen = {"high": max(in_order, key=price), "low": min(in_order, key=price), "open": in_order[0], "close": in_order[-1]}
        for name, trade in chosen.items():
            values[name] = price(trade) if exact else _seven_decimals(price(trade))
    return values


def _shown(values: Mapping[str, Any]) -> dict[str, str]:
    """Volumes as amounts, prices with seven decimals: how the source prints them."""
    shown = {}
    for name, value in values.items():
        if name == "trades":
            shown[name] = str(value)
        elif name in VOLUME_FIELDS:
            shown[name] = stroops_to_amount(value)
        else:
            shown[name] = _seven_decimals(Fraction(value)) if not isinstance(value, str) else value
    return shown


def _seven_decimals(value: Fraction) -> str:
    """A price with seven decimals, ties away from zero: as the source rounds."""
    scaled = value * 10**7
    whole, rest = divmod(scaled.numerator, scaled.denominator)
    if rest * 2 >= scaled.denominator:
        whole += 1
    return f"{whole // 10**7}.{whole % 10**7:07d}"


_PAIR_TRADES = (
    "SELECT ledger_close_time, base_asset_id, base_amount, counter_amount, price_n, price_d, operation_id, trade_order "
    "FROM history_trades WHERE (base_asset_id = :base AND counter_asset_id = :counter "
    "OR base_asset_id = :counter AND counter_asset_id = :base) "
    "AND ledger_close_time >= :start AND ledger_close_time < :end "
    "ORDER BY ledger_close_time, operation_id, trade_order"
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
