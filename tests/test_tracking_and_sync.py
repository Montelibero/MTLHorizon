"""Watch lists, coverage bookkeeping and checkpoints on a real PostgreSQL."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, insert, select, text, update
from sqlalchemy.orm import Session

from app.sources.horizon_cursors import to_total_order_id
from app.storage.models import (
    HistoryOperation,
    HistoryTrade,
    HistoryTransaction,
    SyncCoverage,
    SyncState,
    TrackedAccount,
    TrackedAsset,
    TrackedMarket,
)
from app.storage.upsert import set_checkpoint, upsert_row
from tests import factories as f

SOURCE = "horizon:horizon.stellar.org"
NOW = datetime(2026, 11, 24, 12, 0, 0, tzinfo=UTC)
STREAM = {"source": SOURCE, "resource_type": "transaction", "stream_key": f.ALICE, "direction": "desc"}
VERIFIED_RANGE = text(
    "SELECT subject_key, state, from_ledger, to_ledger FROM sync_coverage "
    "WHERE state IN ('complete', 'empty') ORDER BY from_ledger"
)


def coverage_row(**changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "subject_type": "account",
        "subject_key": f.ALICE,
        "data_type": "transactions",
        "selection_policy": "FULL",
        "policy_version": 1,
        "from_ledger": 10,
        "to_ledger": 20,
        "source": SOURCE,
        "state": "pending",
        "verified_at": None,
    }
    row.update(changes)
    return row


def add_coverage(session: Session, **changes: object) -> None:
    session.execute(insert(SyncCoverage).values(**coverage_row(**changes)))


def coverage_is_refused(session: Session, **changes: object) -> bool:
    return f.is_refused(session, lambda: add_coverage(session, **changes))


def test_unverified_range_is_not_coverage(session: Session) -> None:
    # Planned but not checked yet.
    add_coverage(session)
    assert session.execute(VERIFIED_RANGE).all() == []

    # Checked, and found to hold nothing: verified, yet no records exist.
    add_coverage(
        session, subject_key=f.BOB, from_ledger=1, to_ledger=9, state="empty", verified_at=NOW
    )
    assert session.execute(VERIFIED_RANGE).all() == [(f.BOB, "empty", 1, 9)]

    # The pending range stays distinguishable until it is checked.
    states = session.execute(
        text("SELECT subject_key, state FROM sync_coverage ORDER BY subject_key")
    ).all()
    assert states == [(f.ALICE, "pending"), (f.BOB, "empty")]

    session.execute(
        update(SyncCoverage)
        .where(SyncCoverage.subject_key == f.ALICE)
        .values(state="empty", verified_at=NOW)
    )
    pending = session.scalar(select(func.count()).where(SyncCoverage.state == "pending"))
    empty = session.scalar(select(func.count()).where(SyncCoverage.state == "empty"))
    assert (pending, empty) == (0, 2)


def test_verified_at_must_match_the_state(session: Session) -> None:
    # Verified, but without the moment of verification.
    assert coverage_is_refused(session, state="empty")
    assert coverage_is_refused(session, state="complete")
    # Pending, yet claiming to have been verified.
    assert coverage_is_refused(session, verified_at=NOW)
    assert coverage_is_refused(session, state="finished", verified_at=NOW)
    assert session.scalar(select(func.count()).select_from(SyncCoverage)) == 0


def test_overlapping_ranges_of_one_key_are_refused(session: Session) -> None:
    add_coverage(session)
    assert coverage_is_refused(session, from_ledger=15, to_ledger=25)
    assert coverage_is_refused(session, from_ledger=5, to_ledger=10)
    assert coverage_is_refused(session, from_ledger=12, to_ledger=18)

    # Touching ranges are fine; together they claim ledgers 10..30.
    add_coverage(session, from_ledger=21, to_ledger=30)
    covered = session.execute(text("SELECT sum(to_ledger - from_ledger + 1) FROM sync_coverage")).scalar()
    assert covered == 21


def test_ranges_are_per_data_type_and_policy(session: Session) -> None:
    add_coverage(session)
    # The same ledgers are a separate claim for another data type, another
    # policy and another version of the policy.
    add_coverage(session, data_type="effects")
    add_coverage(session, selection_policy="ASSETS_ONLY")
    add_coverage(session, policy_version=2)
    assert session.scalar(select(func.count()).select_from(SyncCoverage)) == 4


def test_switching_an_account_off_keeps_collected_history(session: Session) -> None:
    f.add_account(session, f.ALICE)
    upsert_row(session, TrackedAccount.__table__, {"account_id": f.ALICE}, {"enabled": True, "reason": "test"})
    transaction_id = f.add_transaction(session, 1, source_account=f.ALICE)
    f.add_operation(session, transaction_id)

    for enabled in (False, True):
        session.execute(update(TrackedAccount).where(TrackedAccount.account_id == f.ALICE).values(enabled=enabled))

    assert session.get(TrackedAccount, f.ALICE).enabled is True
    assert session.get(HistoryTransaction, transaction_id) is not None
    assert session.scalar(select(func.count()).select_from(HistoryOperation)) == 1


def test_the_watch_lists_are_independent(session: Session) -> None:
    """Switching an account off switches off neither its assets nor a market."""
    f.add_account(session, f.ALICE)
    upsert_row(session, TrackedAccount.__table__, {"account_id": f.ALICE}, {"enabled": True, "reason": "test"})
    f.add_asset(session, f.EUR)
    f.add_asset(session, f.USD)
    upsert_row(session, TrackedMarket.__table__, {"base_asset_id": f.EUR, "counter_asset_id": f.USD}, {"reason": "test"})
    upsert_row(session, TrackedAsset.__table__, {"asset_id": f.EUR}, {"reason": "test"})
    session.execute(update(TrackedAccount).where(TrackedAccount.account_id == f.ALICE).values(enabled=False))

    assert session.get(TrackedAccount, f.ALICE).enabled is False
    assert session.scalar(select(TrackedAsset.enabled)) is True
    assert session.scalar(select(TrackedMarket.enabled)) is True


def test_market_history_does_not_depend_on_the_account_watch_list(session: Session) -> None:
    # Bob and Carol trade; neither is tracked, the market is.
    transaction_id = f.add_transaction(session, 1, source_account=f.CAROL)
    operation_id = f.add_operation(session, transaction_id, source_account=f.CAROL)
    f.add_trade(session, operation_id, 0, base_account=f.BOB, counter_account=f.CAROL)
    upsert_row(
        session, TrackedMarket.__table__, {"base_asset_id": f.EUR, "counter_asset_id": f.USD}, {"reason": "test"}
    )

    assert session.scalar(select(func.count()).select_from(TrackedAccount)) == 0
    assert session.scalar(select(func.count()).select_from(TrackedMarket)) == 1
    assert session.scalar(select(func.count()).select_from(HistoryTransaction)) == 1
    assert session.scalar(select(func.count()).select_from(HistoryTrade)) == 1


def test_market_is_one_row_whichever_side_is_named_first(session: Session) -> None:
    f.add_asset(session, f.EUR)
    f.add_asset(session, f.USD)
    market = TrackedMarket.__table__
    upsert_row(session, market, {"base_asset_id": f.EUR, "counter_asset_id": f.USD}, {"reason": "a"})
    # The same pair the other way round is not a second market.
    assert f.is_refused(
        session,
        lambda: upsert_row(
            session, market, {"base_asset_id": f.USD, "counter_asset_id": f.EUR}, {"reason": "b"}
        ),
    )
    assert session.scalar(select(func.count()).select_from(TrackedMarket)) == 1


def test_checkpoint_travels_with_its_data(session: Session) -> None:
    def checkpoint() -> SyncState | None:
        return session.get(SyncState, (SOURCE, "transaction", "desc", f.ALICE))

    def transactions() -> int:
        return session.scalar(select(func.count()).select_from(HistoryTransaction)) or 0

    # A page whose transaction is rolled back takes its checkpoint with it.
    f.add_transaction(session, 1, source_account=f.ALICE)
    set_checkpoint(session, **STREAM, cursor=str(to_total_order_id(1, 1)), cursor_ledger=1)
    session.rollback()
    assert checkpoint() is None
    assert transactions() == 0

    # A committed page keeps both.
    f.add_transaction(session, 2, source_account=f.ALICE)
    set_checkpoint(session, **STREAM, cursor=str(to_total_order_id(2, 1)), cursor_ledger=2)
    session.commit()
    assert checkpoint().cursor == str(to_total_order_id(2, 1))  # type: ignore[union-attr]
    assert checkpoint().cursor_ledger == 2  # type: ignore[union-attr]
    assert transactions() == 1

    # The next page replaces the checkpoint; a stream has one. The cursor is
    # kept verbatim, whatever its format.
    set_checkpoint(session, **STREAM, cursor="opaque-token")
    assert checkpoint().cursor == "opaque-token"  # type: ignore[union-attr]
    assert checkpoint().cursor_ledger is None  # type: ignore[union-attr]
    assert session.scalar(select(func.count()).select_from(SyncState)) == 1

    # A replaced checkpoint is an updated row: its time of change moves too.
    session.execute(update(SyncState).values(updated_at=NOW))
    set_checkpoint(session, **STREAM, cursor=str(to_total_order_id(3, 1)), cursor_ledger=3)
    session.expire_all()
    assert checkpoint().updated_at > NOW  # type: ignore[union-attr]
