"""collector state: where live collection starts and how far it has got

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-25

Stage 03 adds the live collector. It reads each stream forward from a ledger
fixed when the stream is first seen, and needs to keep that apart from the
coverage of a backfill:

* ``collector_source`` — what the collector last saw of its source (the
  latest and the oldest ledger it keeps) and when its last cycle was;
* ``collector_stream`` — per stream the ledger it starts after, the frontier
  it has read to, and how its last attempts went.

Neither table holds history itself. Constraint names are the ones the models
use: check constraints unqualified (Alembic completes them), primary key
names in full.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def _timestamps():
    return (
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )


def upgrade() -> None:
    op.create_table(
        "collector_source",
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("latest_ledger", sa.BigInteger(), nullable=True),
        sa.Column("elder_ledger", sa.BigInteger(), nullable=True),
        sa.Column("latest_closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cycles", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("last_cycle_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("source", name="pk_collector_source"),
        sa.CheckConstraint("cycles >= 0", name="cycles_not_negative"),
    )
    op.create_table(
        "collector_stream",
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("resource_type", sa.Text(), nullable=False),
        sa.Column("stream_key", sa.Text(), nullable=False),
        sa.Column("start_ledger", sa.BigInteger(), nullable=False),
        sa.Column("frontier_ledger", sa.BigInteger(), nullable=False),
        sa.Column("latest_ledger", sa.BigInteger(), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failures", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("source", "resource_type", "stream_key", name="pk_collector_stream"),
        sa.CheckConstraint("frontier_ledger >= start_ledger", name="frontier_after_start"),
        sa.CheckConstraint("failures >= 0", name="failures_not_negative"),
    )


def downgrade() -> None:
    # The frontiers of live collection are lost; the history and coverage stay.
    op.drop_table("collector_stream")
    op.drop_table("collector_source")
