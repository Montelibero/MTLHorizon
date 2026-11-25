"""watch rules: an account is enabled or disabled

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-25

Stage 04 makes the three watch lists independent: an account, an asset and a
market are each switched on or off by themselves. Accounts had modes
instead; only FULL was ever collected, so FULL becomes enabled and every
other mode becomes disabled. Reason and dates stay, history already
collected stays, and so does the coverage collected under the account policy.

Constraint names are used as the models name them: for a check constraint
that is the unqualified name (Alembic completes it with the naming
convention of the models).
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "tracked_accounts",
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.execute("UPDATE tracked_accounts SET enabled = (mode = 'FULL')")
    op.drop_constraint("mode_known", "tracked_accounts", type_="check")
    op.drop_column("tracked_accounts", "mode")
    op.alter_column("tracked_accounts", "enabled", server_default=sa.text("true"))


def downgrade() -> None:
    # Which of the three inactive modes an account had is not kept.
    op.add_column("tracked_accounts", sa.Column("mode", sa.Text(), nullable=False, server_default="DISABLED"))
    op.execute("UPDATE tracked_accounts SET mode = CASE WHEN enabled THEN 'FULL' ELSE 'DISABLED' END")
    op.create_check_constraint(
        "mode_known", "tracked_accounts", "mode IN ('FULL', 'ASSETS_ONLY', 'CANDIDATE', 'DISABLED')"
    )
    op.drop_column("tracked_accounts", "enabled")
