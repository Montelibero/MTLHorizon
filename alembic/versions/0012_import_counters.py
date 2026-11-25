"""what a history import added and what was there, how long it worked, and its place in the queue

Revision ID: 0012
Revises: 0011
Create Date: 2026-11-25

Stage 06 lets the owner see what an import really brought in. A job counted
the pages and the records it read; read is not the same as added — the same
transaction is met through several accounts, and the worker of events may
have stored it first. So a job now also counts, by data type, the rows it
was the first to store (``inserted``) and the rows that were stored already
when it met them (``repeated``). Both are added in the transaction that
stores the page, so a crash between the two cannot make them disagree.

With them come the slices that failed, the time the job spent in its
slices, when it last stored a page and how its last slice ended — and its
``priority``: a job of priority N is served as if it had last been served
N ten-minute steps earlier than it was, which gives it more slices without
keeping any other subject waiting for good. Jobs that ran before are
counted from now on: their counters start empty. None of it holds history.
Constraint names are the ones the models use.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "history_job",
        sa.Column("inserted", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
    )
    op.add_column(
        "history_job",
        sa.Column("repeated", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
    )
    op.add_column("history_job", sa.Column("failed_slices", sa.Integer(), nullable=False, server_default=sa.text("0")))
    op.add_column("history_job", sa.Column("time_spent_ms", sa.BigInteger(), nullable=False, server_default=sa.text("0")))
    op.add_column("history_job", sa.Column("last_progress_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("history_job", sa.Column("last_slice_outcome", sa.Text(), nullable=True))
    op.add_column("history_job", sa.Column("priority", sa.Integer(), nullable=False, server_default=sa.text("0")))
    op.create_check_constraint("failed_slices_not_negative", "history_job", "failed_slices >= 0")
    op.create_check_constraint("time_spent_not_negative", "history_job", "time_spent_ms >= 0")


def downgrade() -> None:
    # What the imports brought in stays; only what was counted of it is lost.
    op.drop_constraint("time_spent_not_negative", "history_job", type_="check")
    op.drop_constraint("failed_slices_not_negative", "history_job", type_="check")
    op.drop_column("history_job", "priority")
    op.drop_column("history_job", "last_slice_outcome")
    op.drop_column("history_job", "last_progress_at")
    op.drop_column("history_job", "time_spent_ms")
    op.drop_column("history_job", "failed_slices")
    op.drop_column("history_job", "repeated")
    op.drop_column("history_job", "inserted")
