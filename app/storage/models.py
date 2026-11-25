"""SQLAlchemy models of the history database.

The Alembic migrations are the source of truth for the database; these models
mirror them so that the application can type-check its queries. Constraint
and index names are written out in both places so that `alembic check` has
nothing to report.

Identifier rules that the database enforces itself:

* ``history_transactions.id`` and ``history_operations.id`` are total order
  ids (SEP-35): ``ledger << 32 | transaction << 12 | operation``.
* Effect and trade cursors are ``<operation id>-<order>``; the zero-padded
  effect ``id`` is the same pair formatted for lexicographic order.
* Ledger, transaction and operation records are required before anything
  that refers to them (foreign keys are enforced).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DDL,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    SmallInteger,
    Text,
    UniqueConstraint,
    event,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    # Without the referenced table: PostgreSQL truncates identifiers at 63
    # characters, and the long participant table names would run past it.
    "fk": "fk_%(table_name)s_%(column_0_name)s",
    "pk": "pk_%(table_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
}

# Ed25519 public key account address (G...). A checksum cannot be checked in
# SQL; the source adapters validate it before a row is written.
G_ADDRESS = "^G[0-9A-Z]{55}$"
SHA256_HEX = "^[0-9a-f]{64}$"

TOID_EXPRESSION = "(ledger_sequence::bigint << 32) | (application_order::bigint << 12)"


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


# The exclusion constraint on sync_coverage needs btree_gist for scalar
# equality operators. Only used by metadata.create_all; the migration issues
# the same statement.
event.listen(
    Base.metadata,
    "before_create",
    DDL("CREATE EXTENSION IF NOT EXISTS btree_gist"),
)


class Timestamped:
    """Row bookkeeping, unrelated to ledger time."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class NetworkIdentity(Base):
    """Which Stellar network this database belongs to. Exactly one row."""

    __tablename__ = "network_identity"

    network_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    network_name: Mapped[str] = mapped_column(Text, nullable=False)
    network_passphrase: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (CheckConstraint("network_id = 1", name="singleton"),)


class HistoryAccount(Base):
    """Registry of accounts seen in history. Presence here does not mean the
    account is tracked (see ``tracked_accounts``)."""

    __tablename__ = "history_accounts"

    account_id: Mapped[str] = mapped_column(Text, primary_key=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (CheckConstraint(f"account_id ~ '{G_ADDRESS}'", name="account_id_is_g_address"),)


class HistoryAsset(Base):
    """Registry of assets. Identity is (type, code, issuer), never code alone.

    ``asset_id`` is the canonical key: ``native`` for XLM, otherwise
    ``<code>:<issuer>``. Horizon keeps a per-instance surrogate id instead,
    which is meaningless outside that instance.
    """

    __tablename__ = "history_assets"

    asset_id: Mapped[str] = mapped_column(Text, primary_key=True)
    asset_type: Mapped[str] = mapped_column(Text, nullable=False)
    # Horizon stores XLM as (native, '', ''); its REST form omits both fields.
    asset_code: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    asset_issuer: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "(asset_type = 'native' AND asset_code = '' AND asset_issuer = '') OR "
            f"(asset_type <> 'native' AND asset_code <> '' AND asset_issuer ~ '{G_ADDRESS}')",
            name="asset_parts_consistent",
        ),
        CheckConstraint(
            "asset_id = CASE WHEN asset_type = 'native' "
            "THEN 'native' ELSE asset_code || ':' || asset_issuer END",
            name="asset_id_matches_parts",
        ),
        Index("ix_history_assets_asset_issuer", "asset_issuer"),
    )


class HistoryLedger(Base, Timestamped):
    """Ledger header facts. A row may start as a stub (sequence and close time
    learned from a transaction) and be completed later from the ledger
    resource; all other columns stay NULL until then."""

    __tablename__ = "history_ledgers"

    sequence: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    ledger_hash: Mapped[str | None] = mapped_column(Text)
    previous_ledger_hash: Mapped[str | None] = mapped_column(Text)
    close_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    protocol_version: Mapped[int | None] = mapped_column(Integer)
    transaction_count: Mapped[int | None] = mapped_column(Integer)
    successful_transaction_count: Mapped[int | None] = mapped_column(Integer)
    failed_transaction_count: Mapped[int | None] = mapped_column(Integer)
    operation_count: Mapped[int | None] = mapped_column(Integer)
    tx_set_operation_count: Mapped[int | None] = mapped_column(Integer)
    total_coins: Mapped[int | None] = mapped_column(BigInteger)
    fee_pool: Mapped[int | None] = mapped_column(BigInteger)
    base_fee: Mapped[int | None] = mapped_column(BigInteger)
    base_reserve: Mapped[int | None] = mapped_column(BigInteger)
    max_tx_set_size: Mapped[int | None] = mapped_column(Integer)
    header_xdr: Mapped[str | None] = mapped_column(Text)
    raw: Mapped[dict[str, object] | None] = mapped_column(JSONB)

    __table_args__ = (
        CheckConstraint("sequence > 0", name="sequence_positive"),
        CheckConstraint(f"ledger_hash IS NULL OR ledger_hash ~ '{SHA256_HEX}'", name="ledger_hash_is_sha256_hex"),
        CheckConstraint(
            f"previous_ledger_hash IS NULL OR previous_ledger_hash ~ '{SHA256_HEX}'",
            name="previous_ledger_hash_is_sha256_hex",
        ),
        Index("ix_history_ledgers_close_time", "close_time"),
    )


class HistoryTransaction(Base, Timestamped):
    """A transaction as Horizon stores it, including failed ones.

    ``id`` is the total order id of the transaction (operation part zero).
    For a fee-bump transaction the top-level fields describe the outer
    (fee-bump) envelope: ``transaction_hash`` is its hash, ``max_fee`` is its
    fee; the inner transaction is reachable through ``inner_transaction_hash``.
    """

    __tablename__ = "history_transactions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    transaction_hash: Mapped[str] = mapped_column(Text, nullable=False)
    inner_transaction_hash: Mapped[str | None] = mapped_column(Text)
    ledger_sequence: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("history_ledgers.sequence"), nullable=False
    )
    application_order: Mapped[int] = mapped_column(Integer, nullable=False)
    source_account: Mapped[str] = mapped_column(
        Text, ForeignKey("history_accounts.account_id"), nullable=False
    )
    source_account_muxed: Mapped[str | None] = mapped_column(Text)
    account_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_fee: Mapped[int] = mapped_column(BigInteger, nullable=False)
    fee_charged: Mapped[int | None] = mapped_column(BigInteger)
    operation_count: Mapped[int] = mapped_column(Integer, nullable=False)
    successful: Mapped[bool] = mapped_column(Boolean, nullable=False)
    # Ledger close time, denormalised from history_ledgers: it is immutable
    # per ledger and is what time-range queries and the REST `created_at` use.
    ledger_close_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    envelope_xdr: Mapped[str] = mapped_column(Text, nullable=False)
    result_xdr: Mapped[str] = mapped_column(Text, nullable=False)
    # NULL means "not obtained". Horizon omits result_meta_xdr in some
    # responses, and the envelope and result alone cannot replace it.
    result_meta_xdr: Mapped[str | None] = mapped_column(Text)
    fee_meta_xdr: Mapped[str | None] = mapped_column(Text)
    memo_type: Mapped[str] = mapped_column(Text, nullable=False)
    memo: Mapped[str | None] = mapped_column(Text)
    signatures: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default=text("'{}'")
    )
    # Fee-bump only; all NULL for a plain transaction.
    fee_source_account: Mapped[str | None] = mapped_column(
        Text, ForeignKey("history_accounts.account_id")
    )
    fee_source_account_muxed: Mapped[str | None] = mapped_column(Text)
    new_max_fee: Mapped[int | None] = mapped_column(BigInteger)
    inner_signatures: Mapped[list[str] | None] = mapped_column(ARRAY(Text))
    # Source record as JSON. Content only, never a byte-exact copy of the
    # HTTP response.
    raw: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    # Which parts of the transaction were loaded from the source as a whole:
    # any of "operations", "effects", "trades". The transaction row alone says
    # nothing about them: a row found in a stream may still be waiting for
    # its operations to be fetched.
    context_loaded: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default=text("'{}'")
    )

    __table_args__ = (
        UniqueConstraint("transaction_hash", name="uq_history_transactions_transaction_hash"),
        UniqueConstraint("inner_transaction_hash", name="uq_history_transactions_inner_transaction_hash"),
        CheckConstraint(f"transaction_hash ~ '{SHA256_HEX}'", name="transaction_hash_is_sha256_hex"),
        CheckConstraint(
            f"inner_transaction_hash IS NULL OR inner_transaction_hash ~ '{SHA256_HEX}'",
            name="inner_transaction_hash_is_sha256_hex",
        ),
        CheckConstraint(f"id = {TOID_EXPRESSION}", name="id_is_total_order_id"),
        CheckConstraint("application_order >= 0", name="application_order_nonnegative"),
        CheckConstraint("account_sequence >= 0", name="account_sequence_nonnegative"),
        CheckConstraint("memo_type IN ('none', 'text', 'id', 'hash', 'return')", name="memo_type_known"),
        Index("ix_history_transactions_ledger", "ledger_sequence", "application_order"),
    )


class HistoryOperation(Base, Timestamped):
    """One operation of a transaction.

    ``type`` and ``type_i`` are stored as they arrive: a type this code base
    does not know is kept (Horizon reports it as ``type: "unknown"`` plus its
    ``type_i``) instead of being rejected by an enum.
    """

    __tablename__ = "history_operations"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    transaction_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("history_transactions.id"), nullable=False
    )
    # 1-based index of the operation inside its transaction.
    application_order: Mapped[int] = mapped_column(Integer, nullable=False)
    # Source of the operation: its own source when set, else the source of
    # the transaction. Not necessarily the transaction's source.
    source_account: Mapped[str] = mapped_column(
        Text, ForeignKey("history_accounts.account_id"), nullable=False
    )
    source_account_muxed: Mapped[str | None] = mapped_column(Text)
    type: Mapped[str] = mapped_column(Text, nullable=False)
    type_i: Mapped[int] = mapped_column(Integer, nullable=False)
    ledger_close_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw: Mapped[dict[str, object] | None] = mapped_column(JSONB)

    __table_args__ = (
        CheckConstraint("application_order >= 1", name="application_order_positive"),
        CheckConstraint("id = transaction_id + application_order", name="id_is_total_order_id"),
        Index("ix_history_operations_transaction", "transaction_id", "application_order"),
        Index("ix_history_operations_type", "type_i", "id"),
        Index("ix_history_operations_source", "source_account", "type_i", "id"),
    )


class HistoryTransactionParticipant(Base):
    """Accounts whose transaction history includes the transaction.

    Mirrors Horizon's rule: the transaction source (and the fee source of a
    fee bump), every account whose ledger entry the transaction changed, and
    all operation participants. It is a curated set, not every address found
    anywhere in the raw record.
    """

    __tablename__ = "history_transaction_participants"

    transaction_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("history_transactions.id"), primary_key=True
    )
    account_id: Mapped[str] = mapped_column(
        Text, ForeignKey("history_accounts.account_id"), primary_key=True
    )

    __table_args__ = (Index("ix_history_transaction_participants_account", "account_id", "transaction_id"),)


class HistoryOperationParticipant(Base):
    """Accounts whose operation history includes the operation.

    Horizon's operation participants come from the operation body only: the
    operation source plus the accounts the operation names (destination,
    trustor, sponsor, ...). Account ledger-entry changes do not add them.
    """

    __tablename__ = "history_operation_participants"

    operation_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("history_operations.id"), primary_key=True
    )
    account_id: Mapped[str] = mapped_column(
        Text, ForeignKey("history_accounts.account_id"), primary_key=True
    )

    __table_args__ = (Index("ix_history_operation_participants_account", "account_id", "operation_id"),)


class HistoryEffect(Base, Timestamped):
    """An effect of a successful operation.

    ``id`` is Horizon's effect id (zero-padded, lexicographically ordered),
    ``paging_token`` its cursor; both encode the same (operation id, order)
    pair, which the database verifies.
    """

    __tablename__ = "history_effects"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    paging_token: Mapped[str] = mapped_column(Text, nullable=False)
    operation_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("history_operations.id"), nullable=False
    )
    effect_order: Mapped[int] = mapped_column(Integer, nullable=False)
    # The account the effect is about. This is the owner of the changed entry,
    # which need not be the source of the operation.
    account_id: Mapped[str] = mapped_column(
        Text, ForeignKey("history_accounts.account_id"), nullable=False
    )
    account_muxed: Mapped[str | None] = mapped_column(Text)
    type: Mapped[str] = mapped_column(Text, nullable=False)
    type_i: Mapped[int] = mapped_column(Integer, nullable=False)
    ledger_close_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw: Mapped[dict[str, object] | None] = mapped_column(JSONB)

    __table_args__ = (
        UniqueConstraint("paging_token", name="uq_history_effects_paging_token"),
        CheckConstraint("effect_order >= 0", name="effect_order_nonnegative"),
        CheckConstraint(
            "paging_token = operation_id::text || '-' || effect_order::text",
            name="paging_token_matches_parts",
        ),
        CheckConstraint(
            "id = lpad(operation_id::text, 19, '0') || '-' || lpad(effect_order::text, 10, '0')",
            name="id_matches_parts",
        ),
        Index("ix_history_effects_account", "account_id", "operation_id", "effect_order"),
        Index("ix_history_effects_type", "type_i", "operation_id", "effect_order"),
    )


class HistoryTrade(Base, Timestamped):
    """A trade, as Horizon reports it.

    ``id`` is Horizon's trade id, equal to its ``paging_token``:
    ``<operation id>-<order>``. It does not depend on which side is base, so
    the same trade requested with the pair reversed is still one row.

    ``base_*`` / ``counter_*`` keep the orientation of the source record
    (Horizon orders the pair by its internal asset ids, which are not visible
    through REST). Market indexes exist for both orientations.
    """

    __tablename__ = "history_trades"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    operation_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("history_operations.id"), nullable=False
    )
    trade_order: Mapped[int] = mapped_column(Integer, nullable=False)
    ledger_close_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    base_asset_id: Mapped[str] = mapped_column(
        Text, ForeignKey("history_assets.asset_id"), nullable=False
    )
    counter_asset_id: Mapped[str] = mapped_column(
        Text, ForeignKey("history_assets.asset_id"), nullable=False
    )
    # Integer stroops: exact, as on the network.
    base_amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    counter_amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # NULL on the side of a liquidity pool.
    base_account_id: Mapped[str | None] = mapped_column(Text, ForeignKey("history_accounts.account_id"))
    counter_account_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("history_accounts.account_id")
    )
    base_liquidity_pool_id: Mapped[str | None] = mapped_column(Text)
    counter_liquidity_pool_id: Mapped[str | None] = mapped_column(Text)
    # Horizon reports a synthetic id as counter_offer_id when the taker had
    # no offer of its own; these are not always real offer ids.
    base_offer_id: Mapped[int | None] = mapped_column(BigInteger)
    counter_offer_id: Mapped[int | None] = mapped_column(BigInteger)
    base_is_seller: Mapped[bool] = mapped_column(Boolean, nullable=False)
    # Exact price ratio counter/base as Horizon provides it.
    price_n: Mapped[int | None] = mapped_column(BigInteger)
    price_d: Mapped[int | None] = mapped_column(BigInteger)
    trade_type: Mapped[str] = mapped_column(Text, nullable=False)
    liquidity_pool_fee_bp: Mapped[int | None] = mapped_column(Integer)
    raw: Mapped[dict[str, object] | None] = mapped_column(JSONB)

    __table_args__ = (
        UniqueConstraint("operation_id", "trade_order", name="uq_history_trades_operation_order"),
        CheckConstraint("trade_order >= 0", name="trade_order_nonnegative"),
        CheckConstraint("id = operation_id::text || '-' || trade_order::text", name="id_matches_parts"),
        CheckConstraint("base_asset_id <> counter_asset_id", name="distinct_assets"),
        CheckConstraint("base_amount >= 0 AND counter_amount >= 0", name="amounts_nonnegative"),
        CheckConstraint("(price_n IS NULL) = (price_d IS NULL)", name="price_pair"),
        CheckConstraint("price_d IS NULL OR price_d > 0", name="price_denominator_positive"),
        Index("ix_history_trades_market_base", "base_asset_id", "counter_asset_id", "ledger_close_time"),
        Index("ix_history_trades_market_counter", "counter_asset_id", "base_asset_id", "ledger_close_time"),
        Index("ix_history_trades_base_account", "base_account_id", "operation_id"),
        Index("ix_history_trades_counter_account", "counter_account_id", "operation_id"),
    )


class TrackedAccount(Base, Timestamped):
    """The watch list of accounts. Managed by hand.

    ``enabled`` says whether the account's history is collected. It is not
    part of the identity, so an account can be switched off and on at any
    time. History already collected is not deleted, and switching an account
    off switches off neither the assets nor the markets: the three watch
    lists are independent.
    """

    __tablename__ = "tracked_accounts"

    account_id: Mapped[str] = mapped_column(
        Text, ForeignKey("history_accounts.account_id"), primary_key=True
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    reason: Mapped[str] = mapped_column(Text, nullable=False)


class TrackedAsset(Base, Timestamped):
    """The watch list of assets, independent of the account watch list."""

    __tablename__ = "tracked_assets"

    asset_id: Mapped[str] = mapped_column(Text, ForeignKey("history_assets.asset_id"), primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    reason: Mapped[str] = mapped_column(Text, nullable=False)


class TrackedMarket(Base, Timestamped):
    """The watch list of asset pairs, independent of both other watch lists.

    A market is an unordered pair: ``base_asset_id < counter_asset_id`` always
    holds, so the pair requested in either order is one row. Which side is
    displayed as base is a presentation choice made when querying.
    """

    __tablename__ = "tracked_markets"

    base_asset_id: Mapped[str] = mapped_column(
        Text, ForeignKey("history_assets.asset_id"), primary_key=True
    )
    counter_asset_id: Mapped[str] = mapped_column(
        Text, ForeignKey("history_assets.asset_id"), primary_key=True
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    reason: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (CheckConstraint("base_asset_id < counter_asset_id", name="pair_is_canonical"),)


class SyncCoverage(Base, Timestamped):
    """What has been verified, per subject, data type, selection policy and
    ledger range (inclusive on both ends).

    Coverage is a claim about a range, not a side effect of having some rows.
    ``empty`` means the range was checked and contained no records for this
    subject and data type; an absent or ``pending`` range has not been checked
    and must never be reported as complete.

    Ranges for one (subject, data type, policy, policy version) may not
    overlap, so the covered ledger set is the plain union of its ranges. A
    range is split by replacing its row with the parts in one transaction.
    """

    __tablename__ = "sync_coverage"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    # account | asset | market | global
    subject_type: Mapped[str] = mapped_column(Text, nullable=False)
    # Account address, asset id, "base>counter" market key; '' for global.
    subject_key: Mapped[str] = mapped_column(Text, nullable=False)
    # transactions | operations | effects | trades
    data_type: Mapped[str] = mapped_column(Text, nullable=False)
    selection_policy: Mapped[str] = mapped_column(Text, nullable=False)
    policy_version: Mapped[int] = mapped_column(Integer, nullable=False)
    from_ledger: Mapped[int] = mapped_column(BigInteger, nullable=False)
    to_ledger: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # Where the range was collected from, e.g. "horizon:horizon.stellar.org":
    # the pool, named by its first server.
    source: Mapped[str] = mapped_column(Text, nullable=False)
    # Which server of the pool read the range, as it is shown to people;
    # several, comma-separated, when more than one did. NULL for ranges read
    # before it was kept, and for pending ones.
    served_by: Mapped[str | None] = mapped_column(Text)
    # pending (not verified) | complete (verified, has records) | empty
    # (verified, no records) | unverifiable (the source can no longer answer
    # for this range: it lies below the history the source keeps)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Why the range is in its state, when that needs words.
    detail: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint("from_ledger > 0", name="from_ledger_positive"),
        CheckConstraint("to_ledger >= from_ledger", name="range_not_empty"),
        CheckConstraint(
            "state IN ('pending', 'complete', 'empty', 'unverifiable')", name="state_known"
        ),
        CheckConstraint(
            "(state = 'pending') = (verified_at IS NULL)", name="verified_at_matches_state"
        ),
        Index("ix_sync_coverage_subject", "subject_type", "subject_key", "data_type"),
    )


# The non-overlap rule from the SyncCoverage docstring, enforced by the
# database (used by metadata.create_all; the migration issues the same DDL).
event.listen(
    SyncCoverage.__table__,
    "after_create",
    DDL(
        "ALTER TABLE sync_coverage ADD CONSTRAINT ex_sync_coverage_ledger_range "
        "EXCLUDE USING gist ("
        "subject_type WITH =, subject_key WITH =, data_type WITH =, "
        "selection_policy WITH =, policy_version WITH =, "
        "int8range(from_ledger, to_ledger, '[]') WITH &&)"
    ),
)


class SyncState(Base, Timestamped):
    """Cursor checkpoint per source, stream and frontier.

    A checkpoint is only valid together with the data collected up to it, so
    it must be written in the same database transaction as that data; the
    database then keeps progress and data consistent on their own.

    ``cursor`` is the source's own paging token, kept verbatim: its format
    depends on ``resource_type`` (a total order id for transactions and
    operations, ``<id>-<order>`` for effects and trades, an address for
    accounts) and is not a number in general. ``cursor_ledger`` is the ledger
    sequence derived from it where the format allows, NULL otherwise.

    ``direction`` tells the two frontiers of one stream apart: ``asc`` moves
    after new records (live collection), ``desc`` moves back into history
    (backfill).
    """

    __tablename__ = "sync_state"

    source: Mapped[str] = mapped_column(Text, primary_key=True)
    # ledger | transaction | operation | effect | trade | account
    resource_type: Mapped[str] = mapped_column(Text, primary_key=True)
    direction: Mapped[str] = mapped_column(
        Text, primary_key=True, server_default=text("'asc'")
    )
    # Account address, market key, '' for an unfiltered stream.
    stream_key: Mapped[str] = mapped_column(Text, primary_key=True)
    cursor: Mapped[str | None] = mapped_column(Text)
    cursor_ledger: Mapped[int | None] = mapped_column(BigInteger)

    __table_args__ = (CheckConstraint("direction IN ('asc', 'desc')", name="direction_known"),)


class CollectorSource(Base, Timestamped):
    """What the collector last saw of its source: one row per source.

    Written at the start of every cycle, so a collector that stopped leaves
    the time of its last cycle here for ``status`` to show.
    """

    __tablename__ = "collector_source"

    # Where the history comes from, e.g. "horizon:horizon.stellar.org".
    source: Mapped[str] = mapped_column(Text, primary_key=True)
    # history_latest_ledger and history_elder_ledger of the last cycle.
    latest_ledger: Mapped[int | None] = mapped_column(BigInteger)
    elder_ledger: Mapped[int | None] = mapped_column(BigInteger)
    latest_closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cycles: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    last_cycle_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Why the last cycle ended early, when it did.
    last_error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (CheckConstraint("cycles >= 0", name="cycles_not_negative"),)


class CollectorStream(Base, Timestamped):
    """The live frontier of one stream of the watch list.

    ``start_ledger`` is fixed once, when the stream is first seen (L0): live
    history begins at ``start_ledger + 1``; what lies before it is for a
    backfill to plan. ``frontier_ledger`` is the last ledger read to its end.
    It only moves in the transaction that stores the records, the coverage
    and the checkpoint up to it, so it never claims what is not stored.
    """

    __tablename__ = "collector_stream"

    source: Mapped[str] = mapped_column(Text, primary_key=True)
    resource_type: Mapped[str] = mapped_column(Text, primary_key=True)
    stream_key: Mapped[str] = mapped_column(Text, primary_key=True)
    start_ledger: Mapped[int] = mapped_column(BigInteger, nullable=False)
    frontier_ledger: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # The upper ledger of the last cycle the stream was read in. The lag of
    # the stream is this minus the frontier.
    latest_ledger: Mapped[int | None] = mapped_column(BigInteger)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Cycles the stream failed in, one after the other. The next attempt
    # waits longer with each of them, so a stream the source cannot serve is
    # not asked for again and again.
    failures: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    last_error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint("frontier_ledger >= start_ledger", name="frontier_after_start"),
        CheckConstraint("failures >= 0", name="failures_not_negative"),
    )


class HttpPace(Base):
    """The pace of requests to Horizon, one row, shared by every process
    (see :mod:`app.storage.pace`). It is not history: it holds no claim."""

    __tablename__ = "http_pace"

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    # When the next request of any process may go.
    next_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    # When a request last took a slot, and of what kind.
    last_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_share: Mapped[str] = mapped_column(Text, nullable=False)
    # Event slots taken since a background reader last had one.
    since_background: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))

    __table_args__ = (
        CheckConstraint("last_share IN ('events', 'background')", name="share_known"),
        CheckConstraint("since_background >= 0", name="since_background_not_negative"),
    )


class HorizonNodeState(Base, Timestamped):
    """What is known of one server of the pool: its last check, and what the
    running processes last saw of it. Compact, for a status page; a check
    writes the first half, a pool the second. It holds no claim about
    history.

    ``url`` is the address as it is shown to people — scheme, host, port —
    so that a secret in the configured address never reaches the table.
    """

    __tablename__ = "horizon_node_state"

    url: Mapped[str] = mapped_column(Text, primary_key=True)
    # Its place in the list, the primary being 1.
    position: Mapped[int] = mapped_column(Integer, nullable=False)

    # -- the last check ------------------------------------------------------
    available: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    network_passphrase: Mapped[str | None] = mapped_column(Text)
    horizon_version: Mapped[str | None] = mapped_column(Text)
    latest_ledger: Mapped[int | None] = mapped_column(BigInteger)
    elder_ledger: Mapped[int | None] = mapped_column(BigInteger)
    latest_closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Behind the freshest server of the same check.
    lag_ledgers: Mapped[int | None] = mapped_column(BigInteger)
    # Median milliseconds of the successful answers, and the last status.
    root_ms: Mapped[int | None] = mapped_column(Integer)
    history_ms: Mapped[int | None] = mapped_column(Integer)
    root_status: Mapped[int | None] = mapped_column(Integer)
    history_status: Mapped[int | None] = mapped_column(Integer)
    errors: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    timeouts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    # The rate limit headers the server sent, when it sent any.
    rate_limit_headers: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    # The whole result of the check, for a page that wants more than the columns.
    report: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # -- what the running processes last saw of it ---------------------------
    last_answer_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    last_error_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cooling_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint("position > 0", name="position_positive"),
        CheckConstraint("errors >= 0", name="errors_not_negative"),
        CheckConstraint("timeouts >= 0", name="timeouts_not_negative"),
    )


class ProcessingJob(Base, Timestamped):
    """The work of bringing one transaction in whole: the transaction, its
    operations, its effects and, when it made any, its trades.

    One job for one transaction, whoever brought word of it: notifications of
    several operations of one transaction, deliveries repeated, deliveries
    for several subscriptions — they all lead here. What is stored already is
    not fetched again (``history_transactions.context_loaded``).

    ``state`` is one of:

    * ``pending`` — to be done, when ``next_attempt_at`` has come;
    * ``running`` — taken by a worker (the lease: ``leased_by`` until
      ``lease_expires_at``), and back to ``pending`` by whoever finds the
      lease expired: a worker that died takes no job along;
    * ``done`` — everything the transaction has is stored;
    * ``partial`` — some of it is stored; the rest was not had in
      ``attempts`` tries, and it waits for a retry;
    * ``dead`` — nothing of it could be stored, for as many tries.

    ``result`` says what was stored and what was not. ``last_error`` says
    why the last try went as it did.
    """

    __tablename__ = "processing_job"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    # The transaction as Horizon names it: of a fee-bump transaction, the hash of the envelope.
    transaction_hash: Mapped[str] = mapped_column(Text, nullable=False)
    transaction_toid: Mapped[int | None] = mapped_column(BigInteger)
    ledger_sequence: Mapped[int | None] = mapped_column(Integer)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    # The lease: who has the job, and until when.
    leased_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    leased_by: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # How many times word of the transaction came again after the job had
    # given up, and took it back into the queue.
    revivals: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    last_error: Mapped[str | None] = mapped_column(Text)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)

    __table_args__ = (
        UniqueConstraint("transaction_hash", name="transaction_hash"),
        CheckConstraint(
            "state IN ('pending', 'running', 'done', 'partial', 'dead')",
            name="state_known",
        ),
        CheckConstraint("attempts >= 0", name="attempts_not_negative"),
        CheckConstraint("revivals >= 0", name="revivals_not_negative"),
        Index("ix_processing_job_queue", "state", "next_attempt_at"),
    )


class NotifierEvent(Base, Timestamped):
    """One delivery of Operations Notifier, kept as it came, before anything
    is done about it: the journal of what was heard.

    A delivery is told from another by the notification's ``id`` and the
    ``subscription`` it was sent for — one notification is delivered once for
    every subscription that matched it. One that comes again is recognised
    and nothing changes.

    ``state`` is one of:

    * ``queued`` — a watch rule matched, or whether one does could not be
      told and the transaction is brought in all the same;
    * ``unmatched`` — no enabled watch rule names the operation's accounts,
      assets or markets, and that could be said for sure; kept, not worked;
    * ``unsupported`` — not a notification of an operation, or without a
      transaction to bring in; kept, not worked.

    ``matched`` names the rules that matched and ``diagnosis`` says why there
    is no job when there is none, or what could not be read. ``payload`` is
    the body as it came. Nothing here is ever deleted.
    """

    __tablename__ = "notifier_event"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    subscription: Mapped[str] = mapped_column(Text, primary_key=True)
    type: Mapped[str] = mapped_column(Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    # What the Notifier says of it: when it made the notification and when
    # it sent this delivery. ``created_at`` of the row is when it got here.
    notified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # What was read of it.
    operation_id: Mapped[int | None] = mapped_column(BigInteger)
    operation_type: Mapped[str | None] = mapped_column(Text)
    account: Mapped[str | None] = mapped_column(Text)
    transaction_hash: Mapped[str | None] = mapped_column(Text)
    transaction_toid: Mapped[int | None] = mapped_column(BigInteger)
    ledger_sequence: Mapped[int | None] = mapped_column(Integer)
    accounts: Mapped[list[str] | None] = mapped_column(JSONB)
    assets: Mapped[list[str] | None] = mapped_column(JSONB)
    markets: Mapped[list[list[str]] | None] = mapped_column(JSONB)
    # What was made of it.
    matched: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    diagnosis: Mapped[str | None] = mapped_column(Text)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    job_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("processing_job.id"))
    # The delivery itself, as it came.
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    # A few headers, none that could be a secret: the signature is not kept.
    headers: Mapped[dict[str, str]] = mapped_column(JSONB, nullable=False)
    # Which configured signing key it was signed with (a public key).
    signed_by: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint("state IN ('queued', 'unmatched', 'unsupported')", name="state_known"),
        Index("ix_notifier_event_received", "received_at"),
        Index("ix_notifier_event_transaction", "transaction_hash"),
        Index("ix_notifier_event_job", "job_id"),
    )
