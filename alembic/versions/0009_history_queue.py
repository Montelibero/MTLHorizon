"""the queue of history imports

Revision ID: 0009
Revises: 0008
Create Date: 2026-11-25

Stage 05 imports a subject's history in slices, slice by slice over the
backfill's own coverage and checkpoints. ``history_job`` holds one such
import: its state, its boundaries, its lease and its counters. It holds no
history. Constraint names are the ones the models use.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "history_job",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("subject", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("from_ledger", sa.BigInteger(), nullable=True),
        sa.Column("to_ledger", sa.BigInteger(), nullable=False),
        sa.Column("boundary_final", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("context", sa.Text(), nullable=False, server_default=sa.text("'parents'")),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("slices", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("requests", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("pages", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("records", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("last_slice_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("leased_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("leased_by", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("paused_because", sa.Text(), nullable=True),
        sa.Column("result", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_history_job"),
        sa.UniqueConstraint("kind", "subject", name="subject"),
        sa.CheckConstraint("kind IN ('account', 'market')", name="kind_known"),
        sa.CheckConstraint(
            "state IN ('waiting', 'running', 'done', 'partial', 'error', 'paused')",
            name="state_known",
        ),
        sa.CheckConstraint("context IN ('parents', 'full')", name="context_known"),
        sa.CheckConstraint("to_ledger > 0", name="to_ledger_positive"),
        sa.CheckConstraint("from_ledger IS NULL OR from_ledger <= to_ledger", name="from_ledger_below_to"),
        sa.CheckConstraint("attempts >= 0", name="attempts_not_negative"),
    )
    op.create_index("ix_history_job_queue", "history_job", ["state", "next_attempt_at"])


def downgrade() -> None:
    # The queue is lost; the history it imported, its coverage and its
    # checkpoints stay.
    op.drop_table("history_job")
