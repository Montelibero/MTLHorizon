"""The streams of the archive, read from PostgreSQL.

Every read is one read-only transaction under a statement timeout
(:func:`reading`): nothing here can write, and a statement that runs too long
is cancelled by the server, so a heavy request costs the database a bounded
time however it was asked.

Every page is one walk of an index, as long as the page: ``limit + 1`` rows,
the last saying whether the page is full. A stream is never read whole to be
cut afterwards. The one place where the archive has no index in the order of a
stream is a market's trades (they are indexed by time); :func:`trades` reads
them from the cursor's ledger on, in time order, which on this network is the
order of the ids. The streams of an account are read as ordered walks whatever
the planner thinks of the rows (:func:`ordered`).

Statements are built apart from being run, so that a plan can be asked for
(``EXPLAIN``) of exactly what runs.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from sqlalchemy import Select, func, or_, select, text, union, union_all
from sqlalchemy.engine import Connection, Engine, Row

from app.history_api.paging import Paging, effect_key
from app.sources.horizon_cursors import LEDGER_SHIFT, MAX_ORDER, effect_id
from app.storage.models import (
    HistoryEffect,
    HistoryLedger,
    HistoryOperation,
    HistoryOperationParticipant,
    HistoryTransaction,
    HistoryTransactionParticipant,
    HistoryTrade,
)

# How long one statement may run, in milliseconds. A page takes a few; a
# chart over a long period takes more. Beyond it the server cancels the
# statement and the request is answered 503, to be asked more narrowly.
STATEMENT_TIMEOUT_MS = 10_000

TRANSACTIONS = HistoryTransaction.__table__
OPERATIONS = HistoryOperation.__table__
EFFECTS = HistoryEffect.__table__
TRADES = HistoryTrade.__table__
LEDGERS = HistoryLedger.__table__
TRANSACTION_PARTICIPANTS = HistoryTransactionParticipant.__table__
OPERATION_PARTICIPANTS = HistoryOperationParticipant.__table__

# The operation an effect or a trade belongs to, and its place in it: the
# order of both streams.
PAIR = (TRADES.c.operation_id, TRADES.c.trade_order)
EFFECT_PAIR = (EFFECTS.c.operation_id, EFFECTS.c.effect_order)

Filter = Any


@contextmanager
def reading(engine: Engine) -> Iterator[Connection]:
    """A connection in a read-only transaction, statements bounded in time.

    ``SET LOCAL`` lasts as long as the transaction, so the connection goes
    back to the pool as it was. The transaction is only read, so it ends with
    a rollback, and nothing holds a lock after it.
    """
    with engine.connect() as connection:
        connection.execute(text(f"SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}"))
        connection.execute(text("SET TRANSACTION READ ONLY"))
        try:
            yield connection
        finally:
            connection.rollback()


def timed_out(error: Exception) -> bool:
    """Whether a database error is the statement timeout (SQLSTATE 57014)."""
    return getattr(getattr(error, "orig", None), "sqlstate", None) == "57014"


# -- statements ----------------------------------------------------------------


def transactions_statement(paging: Paging, *, account: str | None, include_failed: bool) -> Select:
    """Transactions, of an account (every transaction it takes part in) or of
    the whole archive, by total order id."""
    stmt = select(TRANSACTIONS)
    conditions: list[Filter] = []
    if account is not None:
        stmt = stmt.join(
            TRANSACTION_PARTICIPANTS, TRANSACTION_PARTICIPANTS.c.transaction_id == TRANSACTIONS.c.id
        ).where(TRANSACTION_PARTICIPANTS.c.account_id == account)
        # The cursor is put to the participants' index as well. That index is
        # (account, transaction): with it the walk starts at the cursor, not
        # at the account's first row and through every one below the cursor.
        start = paging.condition(TRANSACTION_PARTICIPANTS.c.transaction_id)
        if start is not None:
            conditions.append(start)
    # Failed transactions are hidden unless asked for; a failed transaction has
    # no effects and no trades, so only these two streams need the flag.
    if not include_failed:
        conditions.append(TRANSACTIONS.c.successful)
    return _bounded(stmt, paging, (TRANSACTIONS.c.id,), conditions)


def operations_statement(
    paging: Paging,
    *,
    account: str | None,
    transaction_id: int | None,
    include_failed: bool,
    filters: Sequence[Filter] = (),
) -> Select:
    """Operations, by total order id: of an account, of one transaction, or of
    the whole archive.

    Each row carries what is the transaction's: whether it succeeded and its
    hash. The operations of a failed transaction are served when it is asked
    for itself (upstream does), and hidden in the streams unless asked.
    """
    stmt: Select = select(
        OPERATIONS, TRANSACTIONS.c.successful.label("transaction_successful"), TRANSACTIONS.c.transaction_hash
    ).join(TRANSACTIONS, TRANSACTIONS.c.id == OPERATIONS.c.transaction_id)
    conditions: list[Filter] = []
    if transaction_id is not None:
        conditions.append(OPERATIONS.c.transaction_id == transaction_id)
    else:
        if account is not None:
            stmt = stmt.join(
                OPERATION_PARTICIPANTS, OPERATION_PARTICIPANTS.c.operation_id == OPERATIONS.c.id
            ).where(OPERATION_PARTICIPANTS.c.account_id == account)
            # The cursor on the participants' index too, as for transactions.
            start = paging.condition(OPERATION_PARTICIPANTS.c.operation_id)
            if start is not None:
                conditions.append(start)
        if not include_failed:
            conditions.append(TRANSACTIONS.c.successful)
    conditions.extend(filters)
    return _bounded(stmt, paging, (OPERATIONS.c.id,), conditions)


def effects_statement(paging: Paging, *, account: str | None, span: tuple[str, str] | None) -> Select:
    """Effects by their place in the total order.

    An account's effects are indexed by (account, operation, order) and read
    as the pair they are. The whole archive, one transaction or one operation
    is read through the primary key on the effect id: zero-padded, its text
    order is the order of the pair, and the effects of an operation or of a
    transaction's operations are one stretch of it (``span``).
    """
    conditions: list[Filter] = []
    if account is not None:
        conditions.append(EFFECTS.c.account_id == account)
        return _bounded(select(EFFECTS), paging, EFFECT_PAIR, conditions)
    if span is not None:
        low, high = span
        conditions.extend([EFFECTS.c.id >= low, EFFECTS.c.id <= high])
    if paging.after is not None:
        conditions.append(paging.text_condition(EFFECTS.c.id, effect_key(paging.after)))
    return _bounded(select(EFFECTS), paging, (EFFECTS.c.id,), conditions, narrow=False)


def effects_span(operation_id: int | None, transaction: Row | None) -> tuple[str, str] | None:
    """The stretch of effect ids that holds the effects of ``operation_id``,
    or of every operation of ``transaction``; None for the whole archive."""
    if operation_id is not None:
        return effect_id(operation_id, 0), effect_id(operation_id, MAX_ORDER)
    if transaction is not None:
        # Operations are numbered from 1 and their ids are the transaction's plus the number.
        return effect_id(transaction.id + 1, 0), effect_id(transaction.id + transaction.operation_count, MAX_ORDER)
    return None


def trades_statement(
    paging: Paging,
    *,
    account: str | None,
    pair: tuple[str, str] | None,
    bound: datetime | None,
    trade_type: str | None,
) -> Select:
    """Trades by their place in the total order.

    Of an account: two walks, one per side (base and counter), each as long as
    a page, merged and sorted again — a trade of the account with itself is on
    both sides and comes once. Of the whole archive: the unique index on
    (operation, order). Of a market: see :func:`market_trades`.
    """
    kind = [] if trade_type is None else [TRADES.c.trade_type == trade_type]
    if pair is not None:
        return market_trades(paging, pair, bound, kind)
    if account is None:
        return _bounded(select(TRADES), paging, PAIR, kind)

    def side(account_column: Any) -> Select:
        # The indexes are (account, operation): the walk starts at the cursor's
        # operation, which the planner takes from the pair comparison itself.
        return _bounded(select(TRADES), paging, PAIR, [account_column == account, *kind])

    merged = union(side(TRADES.c.base_account_id), side(TRADES.c.counter_account_id)).subquery("account_trades")
    # Sorted again as a whole: the two walks each end where the page ends, and
    # the records of the page are the first of both together.
    key = (merged.c.operation_id, merged.c.trade_order)
    return select(merged).order_by(*paging.ordering(*key)).limit(paging.limit + 1)


def market_trades(paging: Paging, pair: tuple[str, str], bound: datetime | None, kind: list[Filter]) -> Select:
    """The trades of a market, in either orientation of the pair.

    A market is indexed by time: ``(base, counter, ledger_close_time)`` and
    the same the other way round. Ledger close time grows with the ledger
    sequence, and a trade's ledger is the top of its operation id; so a page
    is read from the close time of the cursor's ledger (``bound``), in time
    order, with the small sort inside one moment that puts it in the order of
    the ids — which is the order upstream pages in. Reading in id order would
    have to sort every trade of the market beyond the cursor first; bounded by
    the timeout, but the cost upstream's busiest accounts pay with a 503.
    """
    base, counter = pair
    condition = paging.condition(*PAIR)
    order = paging.ordering(TRADES.c.ledger_close_time, *PAIR)

    def side(stored_base: str, stored_counter: str) -> Select:
        conditions: list[Filter] = [
            TRADES.c.base_asset_id == stored_base, TRADES.c.counter_asset_id == stored_counter, *kind
        ]
        if bound is not None:
            conditions.append(
                TRADES.c.ledger_close_time >= bound if paging.ascending else TRADES.c.ledger_close_time <= bound
            )
        if condition is not None:
            conditions.append(condition)
        return select(TRADES).where(*conditions).order_by(*order).limit(paging.limit + 1)

    # The two orientations do not share a trade: nothing to deduplicate.
    merged = union_all(side(base, counter), side(counter, base)).subquery("market_trades")
    key = (merged.c.ledger_close_time, merged.c.operation_id, merged.c.trade_order)
    return select(merged).order_by(*paging.ordering(*key)).limit(paging.limit + 1)


def pair_trades_statement(base: str, counter: str, start: datetime, end: datetime) -> Select:
    """Every trade of a market closed in [``start``, ``end``): what the trade
    aggregations are made of."""
    columns = (
        "ledger_close_time",
        "base_asset_id",
        "base_amount",
        "counter_amount",
        "price_n",
        "price_d",
        "operation_id",
        "trade_order",
    )

    def side(stored_base: str, stored_counter: str) -> Select:
        return (
            select(*(TRADES.c[name] for name in columns))
            .where(
                TRADES.c.base_asset_id == stored_base,
                TRADES.c.counter_asset_id == stored_counter,
                TRADES.c.ledger_close_time >= start,
                TRADES.c.ledger_close_time < end,
            )
        )

    merged = union_all(side(base, counter), side(counter, base)).subquery("pair_trades")
    return select(merged).order_by(*(merged.c[name] for name in ("ledger_close_time", "operation_id", "trade_order")))


def _bounded(
    stmt: Select, paging: Paging, key: Sequence[Any], conditions: Sequence[Filter] = (), *, narrow: bool = True
) -> Select:
    """The page of ``stmt``: narrowed to what lies beyond the cursor (unless
    ``narrow`` says the conditions already do), ordered, cut one row past the
    limit."""
    if narrow:
        condition = paging.condition(*key)
        if condition is not None:
            conditions = [condition, *conditions]
    if conditions:
        stmt = stmt.where(*conditions)
    return stmt.order_by(*paging.ordering(*key)).limit(paging.limit + 1)


# -- running them --------------------------------------------------------------


def rows(connection: Connection, statement: Select) -> list[Row]:
    return connection.execute(statement).all()


@contextmanager
def ordered(connection: Connection) -> Iterator[None]:
    """Plans without bitmap scans, for one statement; then as they were.

    The streams of an account are to be a walk of the account's index that
    stops where the page ends. A bitmap scan cannot stop: it reads the
    account's whole range first. The planner picks one when it underestimates
    the rows beyond a cursor — on a tested archive it estimated 29 where
    there were 300, and read every trade of the account above the cursor to
    serve eleven. With bitmap scans out it takes the ordered walk, which is as
    long as the page whatever it thinks. Transaction-local, like the timeout;
    if the statement fails, the transaction ends and that restores the plans.
    """
    connection.execute(text("SET LOCAL enable_bitmapscan = off"))
    failed = False
    try:
        yield
    except BaseException:
        failed = True
        raise
    finally:
        if not failed:
            connection.execute(text("SET LOCAL enable_bitmapscan = on"))


def walk(connection: Connection, statement: Select) -> list[Row]:
    """The page of an account's stream: read as an ordered walk of its index."""
    with ordered(connection):
        return rows(connection, statement)


def transactions(
    connection: Connection, paging: Paging, *, account: str | None, include_failed: bool
) -> list[Row]:
    statement = transactions_statement(paging, account=account, include_failed=include_failed)
    return walk(connection, statement) if account is not None else rows(connection, statement)


def operations(
    connection: Connection,
    paging: Paging,
    *,
    account: str | None = None,
    transaction_id: int | None = None,
    include_failed: bool = True,
    filters: Sequence[Filter] = (),
) -> list[Row]:
    statement = operations_statement(
        paging, account=account, transaction_id=transaction_id, include_failed=include_failed, filters=filters
    )
    return walk(connection, statement) if account is not None else rows(connection, statement)


def effects(
    connection: Connection,
    paging: Paging,
    *,
    account: str | None = None,
    operation_id: int | None = None,
    transaction: Row | None = None,
) -> list[Row]:
    statement = effects_statement(paging, account=account, span=effects_span(operation_id, transaction))
    return walk(connection, statement) if account is not None else rows(connection, statement)


def trades(
    connection: Connection,
    paging: Paging,
    *,
    account: str | None = None,
    pair: tuple[str, str] | None = None,
    trade_type: str | None = None,
) -> list[Row]:
    bound = None
    if pair is not None and paging.after is not None:
        bound = close_time(connection, paging.after[0] >> LEDGER_SHIFT)
    statement = trades_statement(paging, account=account, pair=pair, bound=bound, trade_type=trade_type)
    return walk(connection, statement) if account is not None else rows(connection, statement)


# -- narrowing the operations of an account -------------------------------------


def operation_filters(
    type_filter: tuple[str, str | int] | None, data_name: str | None, data_value: str | None
) -> list[Filter]:
    """What narrows the operations of an account to a type, and to manage_data
    operations by name and by value as the stored answer shows them.

    Only for the stream of an account: the walk stays as long as the account's
    index, whatever these leave of it. The name and the value are read from
    the stored answer; a row stored without one matches nothing here, as it
    cannot be shown to match.
    """
    filters: list[Filter] = []
    if type_filter is not None:
        name, value = type_filter
        filters.append(OPERATIONS.c[name] == value)
    if data_name is not None:
        filters.append(OPERATIONS.c.raw["name"].astext == data_name)
    if data_value is not None:
        filters.append(OPERATIONS.c.raw["value"].astext == data_value)
    return filters


# -- single things -------------------------------------------------------------


def transaction_by_hash(connection: Connection, transaction_hash: str) -> Row | None:
    """The transaction whose hash — of the envelope, or of the inner
    transaction of a fee-bump — is ``transaction_hash``."""
    return connection.execute(
        select(TRANSACTIONS).where(
            or_(
                TRANSACTIONS.c.transaction_hash == transaction_hash,
                TRANSACTIONS.c.inner_transaction_hash == transaction_hash,
            )
        )
    ).first()


def operation(connection: Connection, operation_id: int) -> Row | None:
    return connection.execute(
        select(
            OPERATIONS, TRANSACTIONS.c.successful.label("transaction_successful"), TRANSACTIONS.c.transaction_hash
        )
            .join(TRANSACTIONS, TRANSACTIONS.c.id == OPERATIONS.c.transaction_id)
            .where(OPERATIONS.c.id == operation_id)
    ).first()


def operations_stored(connection: Connection, transaction_id: int) -> int:
    """How many operations of the transaction are stored."""
    return connection.execute(
        select(func.count()).select_from(OPERATIONS).where(OPERATIONS.c.transaction_id == transaction_id)
    ).scalar_one()


def context_loaded(connection: Connection, transaction_id: int) -> list[str]:
    """Which parts of the transaction were loaded from the source as a whole."""
    stored = connection.execute(
        select(TRANSACTIONS.c.context_loaded).where(TRANSACTIONS.c.id == transaction_id)
    ).scalar_one_or_none()
    return list(stored or [])


def latest_ledger(connection: Connection) -> int | None:
    """The newest ledger the archive has a row of; None when it has none."""
    return connection.execute(select(func.max(LEDGERS.c.sequence))).scalar()


def close_time(connection: Connection, ledger_sequence: int) -> datetime | None:
    """When the ledger closed; None when the archive has no row of it."""
    return connection.execute(select(LEDGERS.c.close_time).where(LEDGERS.c.sequence == ledger_sequence)).scalar()


__all__ = [
    "STATEMENT_TIMEOUT_MS",
    "close_time",
    "context_loaded",
    "effects",
    "effects_span",
    "effects_statement",
    "latest_ledger",
    "market_trades",
    "operation",
    "operations",
    "operations_statement",
    "operations_stored",
    "operation_filters",
    "pair_trades_statement",
    "ordered",
    "reading",
    "rows",
    "timed_out",
    "trades",
    "walk",
    "trades_statement",
    "transaction_by_hash",
    "transactions",
    "transactions_statement",
]
