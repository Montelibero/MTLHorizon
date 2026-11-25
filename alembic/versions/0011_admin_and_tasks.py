"""durable tasks of the admin UI, and the sessions and the login throttle of its door

Revision ID: 0011
Revises: 0010
Create Date: 2026-11-25

Stage 06 lets the owner work from a browser. What takes time is not done in
the request that asked for it: it is a task in ``service_job``, taken by the
worker's background thread, with a state and a progress anyone can read.
``admin_session`` keeps the sessions of the admin UI — by the hash of their
token, never the token — and ``admin_login_throttle`` counts the failed
logins. None of it holds history. Constraint names are the ones the models
use.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "service_job",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("params", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("state", sa.Text(), nullable=False, server_default=sa.text("'queued'")),
        sa.Column("requested_by", sa.Text(), nullable=False, server_default=sa.text("'admin'")),
        # Two tasks of one kind and one key are never queued or running at
        # once: a discovery of one asset, a check of the pool.
        sa.Column("one_at_a_time", sa.Text(), nullable=True),
        sa.Column("cancel_requested", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        # What the task says of itself while it runs, and what it came to.
        sa.Column("progress", postgresql.JSONB(), nullable=True),
        sa.Column("result", postgresql.JSONB(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("leased_by", sa.Text(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_service_job"),
        sa.CheckConstraint(
            "kind IN ('discovery', 'pool_check', 'verify', 'reconcile')",
            name="kind_known",
        ),
        sa.CheckConstraint(
            "state IN ('queued', 'running', 'done', 'failed', 'cancelled')",
            name="state_known",
        ),
        sa.CheckConstraint("attempts >= 0", name="attempts_not_negative"),
    )
    op.create_index("ix_service_job_queue", "service_job", ["state", "created_at"])
    op.create_index(
        "uq_service_job_one_at_a_time",
        "service_job",
        ["kind", "one_at_a_time"],
        unique=True,
        postgresql_where=sa.text("state IN ('queued', 'running') AND one_at_a_time IS NOT NULL"),
    )

    op.create_table(
        "admin_session",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        # The hash of the token in the cookie. The token itself is kept nowhere.
        sa.Column("token_hash", sa.Text(), nullable=False),
        sa.Column("csrf", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        # Who logged in, as the process saw them: behind a proxy, the proxy.
        sa.Column("address", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_admin_session"),
        sa.UniqueConstraint("token_hash", name="uq_admin_session_token"),
    )
    op.create_index("ix_admin_session_expires", "admin_session", ["expires_at"])

    op.create_table(
        "admin_login_throttle",
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("failures", sa.Integer(), nullable=False, server_default=sa.text("0")),
        # Times in a row, window after window, that the key was blocked: each
        # makes the block longer, until the key has been quiet for an hour.
        sa.Column("blocks", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("blocked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("key", name="pk_admin_login_throttle"),
        sa.CheckConstraint("failures >= 0", name="failures_not_negative"),
        sa.CheckConstraint("blocks >= 0", name="blocks_not_negative"),
    )


def downgrade() -> None:
    # Everyone is logged out, and what was queued is forgotten; what a task
    # did while it ran stays done (a discovery's accounts, for one).
    op.drop_table("admin_login_throttle")
    op.drop_index("ix_admin_session_expires", table_name="admin_session")
    op.drop_table("admin_session")
    op.drop_index("uq_service_job_one_at_a_time", table_name="service_job")
    op.drop_index("ix_service_job_queue", table_name="service_job")
    op.drop_table("service_job")
