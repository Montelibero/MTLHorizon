"""pool: where coverage was read, the shared pace, the servers' state

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-25

Stage 04 reads Horizon through a pool of servers. Three additions:

* ``sync_coverage.served_by`` — which server of the pool read a range.
  Ranges read before stay as they are, with NULL: they were read from the
  one server there was;
* ``http_pace`` — one row that keeps the pace of requests for every
  process of the installation together;
* ``horizon_node_state`` — what the last check of a server found and what
  the running processes last saw of it. No claim about history is in it.

Neither table holds history. Constraint names are the ones the models use:
check constraints unqualified (Alembic completes them), primary key names
in full.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def _timestamps():
    return (
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )


def upgrade() -> None:
    op.add_column("sync_coverage", sa.Column("served_by", sa.Text(), nullable=True))
    op.create_table(
        "http_pace",
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("next_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_share", sa.Text(), nullable=False),
        sa.Column("since_background", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.PrimaryKeyConstraint("name", name="pk_http_pace"),
        sa.CheckConstraint("last_share IN ('events', 'background')", name="share_known"),
        sa.CheckConstraint("since_background >= 0", name="since_background_not_negative"),
    )
    op.create_table(
        "horizon_node_state",
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("available", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("network_passphrase", sa.Text(), nullable=True),
        sa.Column("horizon_version", sa.Text(), nullable=True),
        sa.Column("latest_ledger", sa.BigInteger(), nullable=True),
        sa.Column("elder_ledger", sa.BigInteger(), nullable=True),
        sa.Column("latest_closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lag_ledgers", sa.BigInteger(), nullable=True),
        sa.Column("root_ms", sa.Integer(), nullable=True),
        sa.Column("history_ms", sa.Integer(), nullable=True),
        sa.Column("root_status", sa.Integer(), nullable=True),
        sa.Column("history_status", sa.Integer(), nullable=True),
        sa.Column("errors", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("timeouts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("rate_limit_headers", postgresql.JSONB(), nullable=True),
        sa.Column("report", postgresql.JSONB(), nullable=True),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_answer_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_error_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cooling_until", sa.DateTime(timezone=True), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("url", name="pk_horizon_node_state"),
        sa.CheckConstraint("position > 0", name="position_positive"),
        sa.CheckConstraint("errors >= 0", name="errors_not_negative"),
        sa.CheckConstraint("timeouts >= 0", name="timeouts_not_negative"),
    )


def downgrade() -> None:
    # What was read from which server is lost; the ranges stay.
    op.drop_table("horizon_node_state")
    op.drop_table("http_pace")
    op.drop_column("sync_coverage", "served_by")
