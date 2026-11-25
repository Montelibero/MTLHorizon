"""the subscriptions this installation manages at Operations Notifier

Revision ID: 0010
Revises: 0009
Create Date: 2026-11-25

Stage 05 registers, confirms and retires the subscriptions this installation
wants at Operations Notifier. ``watch_subscription`` keeps their ids at the
Notifier, their state and their errors. Only this installation's own
subscriptions are kept here. It holds no history. Constraint names are the
ones the models use.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "watch_subscription",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("remote_id", sa.Text(), nullable=True),
        sa.Column("state", sa.Text(), nullable=False, server_default=sa.text("'wanted'")),
        sa.Column("managed", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("filter_version", sa.Integer(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "next_attempt_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_watch_subscription"),
        sa.UniqueConstraint("kind", "key", name="subscription"),
        sa.CheckConstraint("kind IN ('account', 'asset')", name="kind_known"),
        sa.CheckConstraint(
            "state IN ('wanted', 'active', 'error', 'ambiguous', 'retiring', 'manual', 'retired')",
            name="state_known",
        ),
        sa.CheckConstraint("attempts >= 0", name="attempts_not_negative"),
    )
    op.create_index("ix_watch_subscription_queue", "watch_subscription", ["state", "next_attempt_at"])


def downgrade() -> None:
    # What was registered at the Notifier stays registered there; only the
    # memory of it is lost. Retire them by hand before downgrading.
    op.drop_table("watch_subscription")
