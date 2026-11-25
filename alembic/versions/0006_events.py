"""events: the journal of what the Notifier sent, and the jobs it makes

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-25

Stage 04 brings history in as events. Operations Notifier posts a
notification for every operation that matches a subscription; this
installation keeps every delivery, and queues the transaction it names:

* ``processing_job`` — one job for one transaction: bring the transaction in
  whole (the transaction, its operations, its effects, its trades), with a
  state, bounded retries and a lease that dies with the worker that took it;
* ``notifier_event`` — the deliveries as they came, which watch rule matched
  and which job, if any, they led to. Nothing in it is ever deleted.

Neither table changes the history tables. Constraint names are the ones the
models use: check constraints unqualified (Alembic completes them), unique
and primary key names in full.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def _timestamps():
    return (
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )


def upgrade() -> None:
    op.create_table(
        "processing_job",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("transaction_hash", sa.Text(), nullable=False),
        sa.Column("transaction_toid", sa.BigInteger(), nullable=True),
        sa.Column("ledger_sequence", sa.Integer(), nullable=True),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("leased_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("leased_by", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revivals", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("result", postgresql.JSONB(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("id", name="pk_processing_job"),
        sa.UniqueConstraint("transaction_hash", name="transaction_hash"),
        sa.CheckConstraint("state IN ('pending', 'running', 'done', 'partial', 'dead')", name="state_known"),
        sa.CheckConstraint("attempts >= 0", name="attempts_not_negative"),
        sa.CheckConstraint("revivals >= 0", name="revivals_not_negative"),
    )
    op.create_index("ix_processing_job_queue", "processing_job", ["state", "next_attempt_at"])
    op.create_table(
        "notifier_event",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("subscription", sa.Text(), nullable=False),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("notified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("operation_id", sa.BigInteger(), nullable=True),
        sa.Column("operation_type", sa.Text(), nullable=True),
        sa.Column("account", sa.Text(), nullable=True),
        sa.Column("transaction_hash", sa.Text(), nullable=True),
        sa.Column("transaction_toid", sa.BigInteger(), nullable=True),
        sa.Column("ledger_sequence", sa.Integer(), nullable=True),
        sa.Column("accounts", postgresql.JSONB(), nullable=True),
        sa.Column("assets", postgresql.JSONB(), nullable=True),
        sa.Column("markets", postgresql.JSONB(), nullable=True),
        sa.Column("matched", postgresql.JSONB(), nullable=False),
        sa.Column("diagnosis", sa.Text(), nullable=True),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("job_id", sa.BigInteger(), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("headers", postgresql.JSONB(), nullable=False),
        sa.Column("signed_by", sa.Text(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("id", "subscription", name="pk_notifier_event"),
        sa.ForeignKeyConstraint(["job_id"], ["processing_job.id"], name="fk_notifier_event_job_id"),
        sa.CheckConstraint("state IN ('queued', 'unmatched', 'unsupported')", name="state_known"),
    )
    op.create_index("ix_notifier_event_received", "notifier_event", ["received_at"])
    op.create_index("ix_notifier_event_transaction", "notifier_event", ["transaction_hash"])
    op.create_index("ix_notifier_event_job", "notifier_event", ["job_id"])


def downgrade() -> None:
    # The journal and the queue are lost; the history they brought stays.
    op.drop_table("notifier_event")
    op.drop_table("processing_job")
