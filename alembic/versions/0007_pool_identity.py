"""the pool is one logical source, whatever servers it is made of

Revision ID: 0007
Revises: 0006
Create Date: 2026-11-25

Until now the database named the place history comes from by the first
address of the pool: ``horizon:horizon.stellar.org``. Reorder the
addresses, or add a backup in front, and every checkpoint and every
collector state was orphaned under a name nothing would look for again.
A checkpoint is about a ledger, not about a server: every server of the
network answers for the same ledgers, and which server read what is kept
apart in ``sync_coverage.served_by``.

So the source has one name from here on, ``horizon:pool``, and this
migration renames what the old names hold. A database that had more than
one old name (the pool was re-listed around, or two pools wrote to it)
keeps of each colliding checkpoint the one that is further along: the
other said less.

``source_identity`` keeps which names were renamed, so that a downgrade
can give them back; what was merged cannot be taken apart again.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None

POOL = "horizon:pool"


def upgrade() -> None:
    op.create_table(
        "source_identity",
        sa.Column("old_source", sa.Text(), nullable=False),
        sa.Column("renamed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("old_source", name="pk_source_identity"),
    )
    old = "source LIKE 'horizon:%' AND source <> :pool"
    for table in ("sync_coverage", "sync_state", "collector_source", "collector_stream"):
        op.execute(
            sa.text(f"INSERT INTO source_identity (old_source) SELECT DISTINCT source FROM {table} WHERE {old} "
                    "ON CONFLICT (old_source) DO NOTHING").bindparams(pool=POOL)
    )

    # No key on the source: renamed as they are.
    op.execute(sa.text(f"UPDATE sync_coverage SET source = :pool WHERE {old}").bindparams(pool=POOL))

    # Keyed by the source together with the stream. Rows that would become
    # one are reduced first to the one that is further along — reading
    # backwards, the lower checkpoint; forwards, the higher — and of two
    # that say the same, the one updated last, then the one made first. A
    # row already under the pool's name wins a tie: nothing moves.
    _keep_one(
        "sync_state",
        "resource_type, direction, stream_key",
        """
            CASE WHEN direction = 'desc' THEN -COALESCE(cursor_ledger, -1) ELSE COALESCE(cursor_ledger, -1) END DESC,
            (source = :pool) DESC,
            updated_at DESC,
            ctid
        """,
    )
    _keep_one("collector_stream", "resource_type, stream_key",
              "COALESCE(frontier_ledger, -1) DESC, (source = :pool) DESC, last_attempt_at DESC, ctid")
    _keep_one("collector_source", "",
              "COALESCE(latest_ledger, -1) DESC, (source = :pool) DESC, last_cycle_at DESC, ctid")
    for table in ("sync_state", "collector_source", "collector_stream"):
        op.execute(sa.text(f"UPDATE {table} SET source = :pool WHERE {old}").bindparams(pool=POOL))


def _keep_one(table: str, key: str, order: str) -> None:
    """Delete every row of ``table`` that would be one row with another once
    the sources are renamed, but the first of them by ``order``."""
    groups = f"PARTITION BY {key}" if key else ""
    op.execute(sa.text(f"""
        DELETE FROM {table}
        WHERE ctid IN (
            SELECT ctid FROM (
                SELECT ctid, row_number() OVER ({groups} ORDER BY {order}) AS place
                FROM {table}
                WHERE source LIKE 'horizon:%'
            ) ranked WHERE place > 1
        )
    """).bindparams(pool=POOL))


def downgrade() -> None:
    # Gives each old name its rows back where there was one; a name that
    # was merged with others is left under the pool's name.
    connection = op.get_bind()
    names = [row[0] for row in connection.execute(sa.text("SELECT old_source FROM source_identity ORDER BY old_source"))]
    if len(names) == 1:
        for table in ("sync_state", "collector_source", "collector_stream"):
            op.execute(sa.text(f"UPDATE {table} SET source = :old WHERE source = :pool").bindparams(old=names[0], pool=POOL))
        op.execute(
            sa.text("UPDATE sync_coverage SET source = :old WHERE source = :pool").bindparams(old=names[0], pool=POOL)
        )
    op.drop_table("source_identity")
