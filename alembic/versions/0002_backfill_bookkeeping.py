"""backfill bookkeeping: context flag, checkpoint direction, unverifiable ranges

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-24

Stage 02 loads history from a Horizon server and needed three things the
first schema could not express:

* a transaction row can exist before its operations, effects and trades are
  fetched, so the row needs to say which of them are stored
  (``history_transactions.context_loaded``);
* one stream has two frontiers, one moving after new records and one moving
  back into history; both need their own checkpoint
  (``sync_state.direction`` joins the primary key);
* a range can be out of reach of the source, below the history it keeps,
  which is neither pending nor verified (``sync_coverage.state`` gains
  ``unverifiable``, and ``detail`` says why).

Constraint names are used as the models name them. For check constraints
that is the unqualified name (`state_known`): Alembic completes it with the
naming convention of the models when it creates or drops one, as the models
do themselves. Primary key names are written out in full.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "history_transactions",
        sa.Column(
            "context_loaded",
            postgresql.ARRAY(sa.Text()),
            server_default=sa.text("'{}'"),
            nullable=False,
        ),
    )

    op.add_column(
        "sync_state",
        sa.Column("direction", sa.Text(), server_default=sa.text("'asc'"), nullable=False),
    )
    op.drop_constraint("pk_sync_state", "sync_state", type_="primary")
    op.create_primary_key(
        "pk_sync_state", "sync_state", ["source", "resource_type", "direction", "stream_key"]
    )
    op.create_check_constraint("direction_known", "sync_state", "direction IN ('asc', 'desc')")

    op.drop_constraint("state_known", "sync_coverage", type_="check")
    op.create_check_constraint(
        "state_known",
        "sync_coverage",
        "state IN ('pending', 'complete', 'empty', 'unverifiable')",
    )
    op.add_column("sync_coverage", sa.Column("detail", sa.Text(), nullable=True))


def downgrade() -> None:
    # The first schema has no way to keep them apart: they become pending again.
    op.execute(
        "UPDATE sync_coverage SET state = 'pending', verified_at = NULL "
        "WHERE state = 'unverifiable'"
    )
    op.drop_column("sync_coverage", "detail")
    op.drop_constraint("state_known", "sync_coverage", type_="check")
    op.create_check_constraint(
        "state_known", "sync_coverage", "state IN ('pending', 'complete', 'empty')"
    )

    op.drop_constraint("direction_known", "sync_state", type_="check")
    op.drop_constraint("pk_sync_state", "sync_state", type_="primary")
    op.create_primary_key("pk_sync_state", "sync_state", ["source", "resource_type", "stream_key"])
    op.drop_column("sync_state", "direction")

    op.drop_column("history_transactions", "context_loaded")
