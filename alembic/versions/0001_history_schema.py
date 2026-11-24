"""history schema: identity, history, watch lists, sync bookkeeping

Revision ID: 0001
Revises:
Create Date: 2026-11-24

Names match app/storage/models.py exactly. Check constraints are named
unqualified here (`id_is_total_order_id`, not `ck_..._id_is_total_order_id`):
Alembic prefixes them with the naming convention of the models, as the models
do themselves. Every other name is written out in full.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

G_ADDRESS = "^G[0-9A-Z]{55}$"
SHA256_HEX = "^[0-9a-f]{64}$"

# Order matters: a table is created before anything that references it.
TABLES = (
    "network_identity",
    "history_accounts",
    "history_assets",
    "history_ledgers",
    "history_transactions",
    "history_operations",
    "history_transaction_participants",
    "history_operation_participants",
    "history_effects",
    "history_trades",
    "tracked_accounts",
    "tracked_assets",
    "tracked_markets",
    "sync_coverage",
    "sync_state",
)


def _timestamps():
    return (
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )


def _now():
    return sa.DateTime(timezone=True)


def upgrade() -> None:
    # Equality on scalar columns inside the GiST exclusion constraint below.
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")

    op.create_table(
        "network_identity",
        sa.Column("network_id", sa.SmallInteger(), nullable=False),
        sa.Column("network_name", sa.Text(), nullable=False),
        sa.Column("network_passphrase", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("network_id", name="pk_network_identity"),
        sa.CheckConstraint("network_id = 1", name="singleton"),
    )
    # The database is created for the public network. Another network needs
    # another database; the application refuses to mix them.
    op.execute(
        "INSERT INTO network_identity (network_id, network_name, network_passphrase) "
        "VALUES (1, 'pubnet', 'Public Global Stellar Network ; September 2015')"
    )

    op.create_table(
        "history_accounts",
        sa.Column("account_id", sa.Text(), nullable=False),
        sa.Column(
            "first_seen_at", _now(), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("account_id", name="pk_history_accounts"),
        sa.CheckConstraint(
            f"account_id ~ '{G_ADDRESS}'", name="account_id_is_g_address"
        ),
    )

    op.create_table(
        "history_assets",
        sa.Column("asset_id", sa.Text(), nullable=False),
        sa.Column("asset_type", sa.Text(), nullable=False),
        sa.Column("asset_code", sa.Text(), server_default="", nullable=False),
        sa.Column("asset_issuer", sa.Text(), server_default="", nullable=False),
        sa.Column(
            "first_seen_at", _now(), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("asset_id", name="pk_history_assets"),
        sa.CheckConstraint(
            "(asset_type = 'native' AND asset_code = '' AND asset_issuer = '') OR "
            f"(asset_type <> 'native' AND asset_code <> '' AND asset_issuer ~ '{G_ADDRESS}')",
            name="asset_parts_consistent",
        ),
        sa.CheckConstraint(
            "asset_id = CASE WHEN asset_type = 'native' "
            "THEN 'native' ELSE asset_code || ':' || asset_issuer END",
            name="asset_id_matches_parts",
        ),
    )
    op.create_index("ix_history_assets_asset_issuer", "history_assets", ["asset_issuer"])

    op.create_table(
        "history_ledgers",
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("ledger_hash", sa.Text(), nullable=True),
        sa.Column("previous_ledger_hash", sa.Text(), nullable=True),
        sa.Column("close_time", _now(), nullable=False),
        sa.Column("protocol_version", sa.Integer(), nullable=True),
        sa.Column("transaction_count", sa.Integer(), nullable=True),
        sa.Column("successful_transaction_count", sa.Integer(), nullable=True),
        sa.Column("failed_transaction_count", sa.Integer(), nullable=True),
        sa.Column("operation_count", sa.Integer(), nullable=True),
        sa.Column("tx_set_operation_count", sa.Integer(), nullable=True),
        sa.Column("total_coins", sa.BigInteger(), nullable=True),
        sa.Column("fee_pool", sa.BigInteger(), nullable=True),
        sa.Column("base_fee", sa.BigInteger(), nullable=True),
        sa.Column("base_reserve", sa.BigInteger(), nullable=True),
        sa.Column("max_tx_set_size", sa.Integer(), nullable=True),
        sa.Column("header_xdr", sa.Text(), nullable=True),
        sa.Column("raw", postgresql.JSONB(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("sequence", name="pk_history_ledgers"),
        sa.CheckConstraint("sequence > 0", name="sequence_positive"),
        sa.CheckConstraint(
            f"ledger_hash IS NULL OR ledger_hash ~ '{SHA256_HEX}'",
            name="ledger_hash_is_sha256_hex",
        ),
        sa.CheckConstraint(
            f"previous_ledger_hash IS NULL OR previous_ledger_hash ~ '{SHA256_HEX}'",
            name="previous_ledger_hash_is_sha256_hex",
        ),
    )
    op.create_index("ix_history_ledgers_close_time", "history_ledgers", ["close_time"])

    op.create_table(
        "history_transactions",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("transaction_hash", sa.Text(), nullable=False),
        sa.Column("inner_transaction_hash", sa.Text(), nullable=True),
        sa.Column(
            "ledger_sequence",
            sa.BigInteger(),
            sa.ForeignKey(
                "history_ledgers.sequence",
                name="fk_history_transactions_ledger_sequence",
            ),
            nullable=False,
        ),
        sa.Column("application_order", sa.Integer(), nullable=False),
        sa.Column(
            "source_account",
            sa.Text(),
            sa.ForeignKey(
                "history_accounts.account_id",
                name="fk_history_transactions_source_account",
            ),
            nullable=False,
        ),
        sa.Column("source_account_muxed", sa.Text(), nullable=True),
        sa.Column("account_sequence", sa.BigInteger(), nullable=False),
        sa.Column("max_fee", sa.BigInteger(), nullable=False),
        sa.Column("fee_charged", sa.BigInteger(), nullable=True),
        sa.Column("operation_count", sa.Integer(), nullable=False),
        sa.Column("successful", sa.Boolean(), nullable=False),
        sa.Column("ledger_close_time", _now(), nullable=False),
        sa.Column("envelope_xdr", sa.Text(), nullable=False),
        sa.Column("result_xdr", sa.Text(), nullable=False),
        sa.Column("result_meta_xdr", sa.Text(), nullable=True),
        sa.Column("fee_meta_xdr", sa.Text(), nullable=True),
        sa.Column("memo_type", sa.Text(), nullable=False),
        sa.Column("memo", sa.Text(), nullable=True),
        sa.Column(
            "signatures",
            postgresql.ARRAY(sa.Text()),
            server_default=sa.text("'{}'"),
            nullable=False,
        ),
        sa.Column(
            "fee_source_account",
            sa.Text(),
            sa.ForeignKey(
                "history_accounts.account_id",
                name="fk_history_transactions_fee_source_account",
            ),
            nullable=True,
        ),
        sa.Column("fee_source_account_muxed", sa.Text(), nullable=True),
        sa.Column("new_max_fee", sa.BigInteger(), nullable=True),
        sa.Column("inner_signatures", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("raw", postgresql.JSONB(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("id", name="pk_history_transactions"),
        sa.UniqueConstraint("transaction_hash", name="uq_history_transactions_transaction_hash"),
        sa.UniqueConstraint(
            "inner_transaction_hash", name="uq_history_transactions_inner_transaction_hash"
        ),
        sa.CheckConstraint(
            f"transaction_hash ~ '{SHA256_HEX}'",
            name="transaction_hash_is_sha256_hex",
        ),
        sa.CheckConstraint(
            f"inner_transaction_hash IS NULL OR inner_transaction_hash ~ '{SHA256_HEX}'",
            name="inner_transaction_hash_is_sha256_hex",
        ),
        sa.CheckConstraint(
            "id = (ledger_sequence::bigint << 32) | (application_order::bigint << 12)",
            name="id_is_total_order_id",
        ),
        sa.CheckConstraint(
            "application_order >= 0", name="application_order_nonnegative"
        ),
        sa.CheckConstraint(
            "account_sequence >= 0", name="account_sequence_nonnegative"
        ),
        sa.CheckConstraint(
            "memo_type IN ('none', 'text', 'id', 'hash', 'return')",
            name="memo_type_known",
        ),
    )
    op.create_index(
        "ix_history_transactions_ledger", "history_transactions", ["ledger_sequence", "application_order"]
    )

    op.create_table(
        "history_operations",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column(
            "transaction_id",
            sa.BigInteger(),
            sa.ForeignKey(
                "history_transactions.id",
                name="fk_history_operations_transaction_id",
            ),
            nullable=False,
        ),
        sa.Column("application_order", sa.Integer(), nullable=False),
        sa.Column(
            "source_account",
            sa.Text(),
            sa.ForeignKey(
                "history_accounts.account_id",
                name="fk_history_operations_source_account",
            ),
            nullable=False,
        ),
        sa.Column("source_account_muxed", sa.Text(), nullable=True),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("type_i", sa.Integer(), nullable=False),
        sa.Column("ledger_close_time", _now(), nullable=False),
        sa.Column("raw", postgresql.JSONB(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("id", name="pk_history_operations"),
        sa.CheckConstraint(
            "application_order >= 1", name="application_order_positive"
        ),
        sa.CheckConstraint(
            "id = transaction_id + application_order", name="id_is_total_order_id"
        ),
    )
    op.create_index(
        "ix_history_operations_transaction", "history_operations", ["transaction_id", "application_order"]
    )
    op.create_index("ix_history_operations_type", "history_operations", ["type_i", "id"])
    op.create_index(
        "ix_history_operations_source", "history_operations", ["source_account", "type_i", "id"]
    )

    op.create_table(
        "history_transaction_participants",
        sa.Column(
            "transaction_id",
            sa.BigInteger(),
            sa.ForeignKey(
                "history_transactions.id",
                name="fk_history_transaction_participants_transaction_id",
            ),
            nullable=False,
        ),
        sa.Column(
            "account_id",
            sa.Text(),
            sa.ForeignKey(
                "history_accounts.account_id",
                name="fk_history_transaction_participants_account_id",
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(
            "transaction_id", "account_id", name="pk_history_transaction_participants"
        ),
    )
    op.create_index(
        "ix_history_transaction_participants_account",
        "history_transaction_participants",
        ["account_id", "transaction_id"],
    )

    op.create_table(
        "history_operation_participants",
        sa.Column(
            "operation_id",
            sa.BigInteger(),
            sa.ForeignKey(
                "history_operations.id",
                name="fk_history_operation_participants_operation_id",
            ),
            nullable=False,
        ),
        sa.Column(
            "account_id",
            sa.Text(),
            sa.ForeignKey(
                "history_accounts.account_id",
                name="fk_history_operation_participants_account_id",
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("operation_id", "account_id", name="pk_history_operation_participants"),
    )
    op.create_index(
        "ix_history_operation_participants_account",
        "history_operation_participants",
        ["account_id", "operation_id"],
    )

    op.create_table(
        "history_effects",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("paging_token", sa.Text(), nullable=False),
        sa.Column(
            "operation_id",
            sa.BigInteger(),
            sa.ForeignKey(
                "history_operations.id", name="fk_history_effects_operation_id"
            ),
            nullable=False,
        ),
        sa.Column("effect_order", sa.Integer(), nullable=False),
        sa.Column(
            "account_id",
            sa.Text(),
            sa.ForeignKey(
                "history_accounts.account_id", name="fk_history_effects_account_id"
            ),
            nullable=False,
        ),
        sa.Column("account_muxed", sa.Text(), nullable=True),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("type_i", sa.Integer(), nullable=False),
        sa.Column("ledger_close_time", _now(), nullable=False),
        sa.Column("raw", postgresql.JSONB(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("id", name="pk_history_effects"),
        sa.UniqueConstraint("paging_token", name="uq_history_effects_paging_token"),
        sa.CheckConstraint("effect_order >= 0", name="effect_order_nonnegative"),
        sa.CheckConstraint(
            "paging_token = operation_id::text || '-' || effect_order::text",
            name="paging_token_matches_parts",
        ),
        sa.CheckConstraint(
            "id = lpad(operation_id::text, 19, '0') || '-' || lpad(effect_order::text, 10, '0')",
            name="id_matches_parts",
        ),
    )
    op.create_index(
        "ix_history_effects_account", "history_effects", ["account_id", "operation_id", "effect_order"]
    )
    op.create_index(
        "ix_history_effects_type", "history_effects", ["type_i", "operation_id", "effect_order"]
    )

    op.create_table(
        "history_trades",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column(
            "operation_id",
            sa.BigInteger(),
            sa.ForeignKey(
                "history_operations.id", name="fk_history_trades_operation_id"
            ),
            nullable=False,
        ),
        sa.Column("trade_order", sa.Integer(), nullable=False),
        sa.Column("ledger_close_time", _now(), nullable=False),
        sa.Column(
            "base_asset_id",
            sa.Text(),
            sa.ForeignKey(
                "history_assets.asset_id", name="fk_history_trades_base_asset_id"
            ),
            nullable=False,
        ),
        sa.Column(
            "counter_asset_id",
            sa.Text(),
            sa.ForeignKey(
                "history_assets.asset_id", name="fk_history_trades_counter_asset_id"
            ),
            nullable=False,
        ),
        sa.Column("base_amount", sa.BigInteger(), nullable=False),
        sa.Column("counter_amount", sa.BigInteger(), nullable=False),
        sa.Column(
            "base_account_id",
            sa.Text(),
            sa.ForeignKey(
                "history_accounts.account_id", name="fk_history_trades_base_account_id"
            ),
            nullable=True,
        ),
        sa.Column(
            "counter_account_id",
            sa.Text(),
            sa.ForeignKey(
                "history_accounts.account_id",
                name="fk_history_trades_counter_account_id",
            ),
            nullable=True,
        ),
        sa.Column("base_liquidity_pool_id", sa.Text(), nullable=True),
        sa.Column("counter_liquidity_pool_id", sa.Text(), nullable=True),
        sa.Column("base_offer_id", sa.BigInteger(), nullable=True),
        sa.Column("counter_offer_id", sa.BigInteger(), nullable=True),
        sa.Column("base_is_seller", sa.Boolean(), nullable=False),
        sa.Column("price_n", sa.BigInteger(), nullable=True),
        sa.Column("price_d", sa.BigInteger(), nullable=True),
        sa.Column("trade_type", sa.Text(), nullable=False),
        sa.Column("liquidity_pool_fee_bp", sa.Integer(), nullable=True),
        sa.Column("raw", postgresql.JSONB(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("id", name="pk_history_trades"),
        sa.UniqueConstraint("operation_id", "trade_order", name="uq_history_trades_operation_order"),
        sa.CheckConstraint("trade_order >= 0", name="trade_order_nonnegative"),
        sa.CheckConstraint(
            "id = operation_id::text || '-' || trade_order::text",
            name="id_matches_parts",
        ),
        sa.CheckConstraint("base_asset_id <> counter_asset_id", name="distinct_assets"),
        sa.CheckConstraint(
            "base_amount >= 0 AND counter_amount >= 0", name="amounts_nonnegative"
        ),
        sa.CheckConstraint("(price_n IS NULL) = (price_d IS NULL)", name="price_pair"),
        sa.CheckConstraint(
            "price_d IS NULL OR price_d > 0", name="price_denominator_positive"
        ),
    )
    op.create_index(
        "ix_history_trades_market_base",
        "history_trades",
        ["base_asset_id", "counter_asset_id", "ledger_close_time"],
    )
    op.create_index(
        "ix_history_trades_market_counter",
        "history_trades",
        ["counter_asset_id", "base_asset_id", "ledger_close_time"],
    )
    op.create_index(
        "ix_history_trades_base_account", "history_trades", ["base_account_id", "operation_id"]
    )
    op.create_index(
        "ix_history_trades_counter_account", "history_trades", ["counter_account_id", "operation_id"]
    )

    op.create_table(
        "tracked_accounts",
        sa.Column(
            "account_id",
            sa.Text(),
            sa.ForeignKey(
                "history_accounts.account_id", name="fk_tracked_accounts_account_id"
            ),
            nullable=False,
        ),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        *_timestamps(),
        sa.PrimaryKeyConstraint("account_id", name="pk_tracked_accounts"),
        sa.CheckConstraint(
            "mode IN ('FULL', 'ASSETS_ONLY', 'CANDIDATE', 'DISABLED')",
            name="mode_known",
        ),
    )

    op.create_table(
        "tracked_assets",
        sa.Column(
            "asset_id",
            sa.Text(),
            sa.ForeignKey(
                "history_assets.asset_id", name="fk_tracked_assets_asset_id"
            ),
            nullable=False,
        ),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        *_timestamps(),
        sa.PrimaryKeyConstraint("asset_id", name="pk_tracked_assets"),
    )

    op.create_table(
        "tracked_markets",
        sa.Column(
            "base_asset_id",
            sa.Text(),
            sa.ForeignKey(
                "history_assets.asset_id", name="fk_tracked_markets_base_asset_id"
            ),
            nullable=False,
        ),
        sa.Column(
            "counter_asset_id",
            sa.Text(),
            sa.ForeignKey(
                "history_assets.asset_id", name="fk_tracked_markets_counter_asset_id"
            ),
            nullable=False,
        ),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        *_timestamps(),
        sa.PrimaryKeyConstraint("base_asset_id", "counter_asset_id", name="pk_tracked_markets"),
        sa.CheckConstraint("base_asset_id < counter_asset_id", name="pair_is_canonical"),
    )

    op.create_table(
        "sync_coverage",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("subject_type", sa.Text(), nullable=False),
        sa.Column("subject_key", sa.Text(), nullable=False),
        sa.Column("data_type", sa.Text(), nullable=False),
        sa.Column("selection_policy", sa.Text(), nullable=False),
        sa.Column("policy_version", sa.Integer(), nullable=False),
        sa.Column("from_ledger", sa.BigInteger(), nullable=False),
        sa.Column("to_ledger", sa.BigInteger(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("verified_at", _now(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("id", name="pk_sync_coverage"),
        sa.CheckConstraint("from_ledger > 0", name="from_ledger_positive"),
        sa.CheckConstraint("to_ledger >= from_ledger", name="range_not_empty"),
        sa.CheckConstraint(
            "state IN ('pending', 'complete', 'empty')", name="state_known"
        ),
        sa.CheckConstraint(
            "(state = 'pending') = (verified_at IS NULL)",
            name="verified_at_matches_state",
        ),
    )
    op.create_index(
        "ix_sync_coverage_subject",
        "sync_coverage",
        ["subject_type", "subject_key", "data_type"],
    )
    # One claim per ledger per (subject, data type, policy, policy version):
    # ranges for the same key never overlap, so coverage is a union of ranges.
    op.execute(
        "ALTER TABLE sync_coverage ADD CONSTRAINT ex_sync_coverage_ledger_range "
        "EXCLUDE USING gist ("
        "subject_type WITH =, subject_key WITH =, data_type WITH =, "
        "selection_policy WITH =, policy_version WITH =, "
        "int8range(from_ledger, to_ledger, '[]') WITH &&)"
    )

    op.create_table(
        "sync_state",
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("resource_type", sa.Text(), nullable=False),
        sa.Column("stream_key", sa.Text(), nullable=False),
        sa.Column("cursor", sa.Text(), nullable=True),
        sa.Column("cursor_ledger", sa.BigInteger(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("source", "resource_type", "stream_key", name="pk_sync_state"),
    )


def downgrade() -> None:
    for table in reversed(TABLES):
        op.drop_table(table)
