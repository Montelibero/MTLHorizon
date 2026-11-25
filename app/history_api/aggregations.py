"""Trade aggregations: the trades of a market counted and summed by buckets,
as upstream does it.

The formulas are the ones stage 03 verified against the live server and keeps
in :mod:`app.backfill.verify`; they are called from there and are not written a
second time here:

* ``trade_count``, ``base_volume``, ``counter_volume`` — the count and the
  sums of the two sides, in the orientation asked for;
* ``avg`` — the counter volume over the base volume, seven decimals;
* ``high``, ``low`` — the greatest and the least of the prices the trades were
  made at. Not the ratios of the sums: for a trade through a liquidity pool
  the two differ;
* ``open``, ``close`` — the prices of the first and the last trade of the
  bucket, by the order of the operations.

Buckets are cut on multiples of the resolution counted from the epoch, which
makes them UTC days, weeks and so on. Only buckets with trades are answered:
an interval without trades has none, as it has none upstream. The first and
the last bucket of a period may be cut by its edges — upstream does not mark
that, and neither does this.

Prices come in two forms, as upstream serves them: printed with seven
decimals, and as the exact fraction (``*_r``) of the trade chosen — the
fraction as it is stored, not reduced, as upstream prints it. There is no
exact ``avg``: upstream serves none.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from fractions import Fraction
from typing import Any

from sqlalchemy.engine import Connection

from app.backfill import verify
from app.history_api import streams
from app.sources.horizon_format import stroops_to_amount

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MILLISECOND = timedelta(milliseconds=1)

# What upstream takes: 1 minute, 5, 15, 1 hour, 1 day, 1 week.
RESOLUTIONS = (60_000, 300_000, 900_000, 3_600_000, 86_400_000, 604_800_000)

# A stored trade of the pair in the orientation asked for, as the formulas in
# verify.py read one: (base and counter amount in stroops, the price as
# numerator and denominator, the operation id and the order inside it).
Trade = tuple[int, int, int, int, int, int]


def buckets(
    connection: Connection, base: str, counter: str, start_ms: int, end_ms: int, resolution_ms: int
) -> list[dict[str, Any]]:
    """Every bucket with trades in [``start_ms``, ``end_ms``), oldest first."""
    rows = streams.rows(
        connection,
        streams.pair_trades_statement(
            base, counter, datetime.fromtimestamp(start_ms / 1000, UTC), datetime.fromtimestamp(end_ms / 1000, UTC)
        ),
    )
    grouped: dict[int, list[Trade]] = defaultdict(list)
    for closed, stored_base, base_amount, counter_amount, price_n, price_d, operation_id, order in rows:
        if stored_base != base:  # stored the other way round
            base_amount, counter_amount = counter_amount, base_amount
            price_n, price_d = price_d, price_n
        grouped[_bucket_ms(closed, resolution_ms)].append(
            (base_amount, counter_amount, price_n, price_d, operation_id, order)
        )
    return [_bucket(stamp, grouped[stamp]) for stamp in sorted(grouped)]


def _bucket_ms(closed: datetime, resolution_ms: int) -> int:
    # In whole milliseconds, without a float on the way: a bucket edge is exact.
    moment = (closed.astimezone(UTC) - EPOCH) // MILLISECOND
    return moment - moment % resolution_ms


def _bucket(stamp: int, trades: list[Trade]) -> dict[str, Any]:
    if not sum(trade[0] for trade in trades):
        # The average is the counter volume over the base volume: with nothing
        # on the base side there is nothing to divide by. The count and the
        # volumes are certain; the prices are not given, and not made up. (The
        # archive allows a trade with a zero amount; a bucket made of only
        # such is not something the source is known to answer for.)
        return {
            "timestamp": str(stamp),
            "trade_count": str(len(trades)),
            "base_volume": stroops_to_amount(0),
            "counter_volume": stroops_to_amount(sum(trade[1] for trade in trades)),
        }
    # The formulas of the verified comparison, exactly as they are; `exact`
    # keeps the prices as fractions so that both forms are printed from one.
    values = verify._summed(trades, True)
    shown = verify._shown(values)
    record: dict[str, Any] = {
        "timestamp": str(stamp),
        "trade_count": shown["trades"],
        "base_volume": shown["base_volume"],
        "counter_volume": shown["counter_volume"],
    }
    if verify.AVG in shown:
        record["avg"] = shown[verify.AVG]
    # The trade each price was taken from: the first, in the order the formula
    # sorts them, whose price is the one it chose. Its n and d are printed.
    # Trades without a stored price have none (very old ones, 0/0 upstream).
    in_order = sorted((trade for trade in trades if trade[3]), key=lambda trade: (trade[4], trade[5]))
    for name in verify.PRICE_FIELDS:
        if name not in values:
            continue
        price = values[name]
        chosen = next(trade for trade in in_order if Fraction(trade[2], trade[3]) == price)
        record[name] = shown[name]
        record[f"{name}_r"] = {"n": str(chosen[2]), "d": str(chosen[3])}
    return record
