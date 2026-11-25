"""discovery: a rule of its own per asset, where accounts came from, the runs

Revision ID: 0008
Revises: 0007
Create Date: 2026-11-25

Stage 05 can find the accounts that hold an asset and add them to the watch
list. Everything here is additive and holds no history:

* ``tracked_assets.discover`` — find the holders of this asset. A rule of
  its own: neither depends on ``enabled``;
* ``tracked_accounts.origin``, ``discovered_by``, ``watch_since_ledger``
  and ``boundary_due`` — how an account got here (accounts that were
  already here are ``legacy``), which assets' discovery found it, the
  ledger that was the latest when it was added or switched on again (NULL
  when not captured), and whether that still has to be done;
* ``discovery_run`` — one pass of discovery: how far it got and which
  servers answered;
* ``service_state`` — what a service must remember between passes.

Constraint names are the ones the models use: check constraints
unqualified (Alembic completes them), unique and primary key names in full.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def _timestamps():
    return (
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )


def upgrade() -> None:
    op.add_column(
        "tracked_assets",
        sa.Column("discover", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.add_column(
        "tracked_accounts",
        sa.Column("origin", sa.Text(), nullable=False, server_default=sa.text("'legacy'")),
    )
    op.add_column("tracked_accounts", sa.Column("discovered_by", sa.Text(), nullable=True))
    op.add_column("tracked_accounts", sa.Column("watch_since_ledger", sa.BigInteger(), nullable=True))
    op.add_column(
        "tracked_accounts", sa.Column("boundary_due", sa.Boolean(), nullable=False, server_default=sa.text("false"))
    )
    # The accounts that were already here are legacy; those added from now
    # on say how they came.
    op.alter_column("tracked_accounts", "origin", server_default=sa.text("'yaml'"))
    op.create_table(
        "discovery_run",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("asset_id", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("dry_run", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("after_address", sa.Text(), nullable=True),
        sa.Column("last_address", sa.Text(), nullable=True),
        sa.Column("latest_ledger", sa.BigInteger(), nullable=True),
        sa.Column("servers", postgresql.JSONB(), nullable=True),
        sa.Column("pages", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("found", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("with_line", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("added", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("already", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("disabled", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("issuer_skipped", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.Text(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("id", name="pk_discovery_run"),
        sa.CheckConstraint(
            "state IN ('running', 'complete', 'incomplete', 'failed')", name="state_known"
        ),
    )
    op.create_index("ix_discovery_run_asset", "discovery_run", ["asset_id", "started_at"])
    op.create_table(
        "service_state",
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("value", postgresql.JSONB(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("name", name="pk_service_state"),
    )


def downgrade() -> None:
    # Where accounts came from and the record of the passes are lost; the
    # accounts, the history and the coverage stay.
    op.drop_table("service_state")
    op.drop_table("discovery_run")
    op.drop_column("tracked_accounts", "watch_since_ledger")
    op.drop_column("tracked_accounts", "boundary_due")
    op.drop_column("tracked_accounts", "discovered_by")
    op.drop_column("tracked_accounts", "origin")
    op.drop_column("tracked_assets", "discover")
