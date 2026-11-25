"""Coverage bookkeeping on PostgreSQL: tiling a window with ranges."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.storage import coverage
from app.storage.coverage import CoverageKey, complete, empty, pending, unverifiable
from app.storage.models import SyncCoverage
from tests import factories as f

SOURCE = "horizon:horizon.test"
KEY = CoverageKey("account", f.ALICE, "transactions", "FULL", 1)
OTHER_POLICY = CoverageKey("account", f.ALICE, "transactions", "ASSETS_ONLY", 1)
OTHER_VERSION = CoverageKey("account", f.ALICE, "transactions", "FULL", 2)
OTHER_STREAM = CoverageKey("account", f.ALICE, "effects", "FULL", 1)


def states(session: Session, key: CoverageKey = KEY) -> list[tuple[str, int, int]]:
    return [(row.state, row.from_ledger, row.to_ledger) for row in coverage.ranges(session, key)]


def test_planning_tiles_the_window_and_leaves_the_gaps_pending(session: Session) -> None:
    coverage.replace(session, KEY, SOURCE, 120, 150, [complete(120, 150)])

    coverage.plan(session, KEY, SOURCE, 100, 200)

    assert states(session) == [
        ("pending", 100, 119),
        ("complete", 120, 150),
        ("pending", 151, 200),
    ]


def test_a_verified_range_outside_the_window_is_cut_not_taken_along(session: Session) -> None:
    coverage.replace(session, KEY, SOURCE, 100, 200, [complete(100, 200)])

    coverage.plan(session, KEY, SOURCE, 150, 160)

    # A window that is verified already is left exactly as it was: one row.
    assert states(session) == [("complete", 100, 200)]


def test_storing_a_page_closes_its_part_of_the_pending_range(session: Session) -> None:
    coverage.plan(session, KEY, SOURCE, 100, 200)
    coverage.replace(session, KEY, SOURCE, 171, 200, [complete(171, 200)])

    assert states(session) == [("pending", 100, 170), ("complete", 171, 200)]
    coverage.replace(session, KEY, SOURCE, 100, 170, [empty(100, 170)])
    assert states(session) == [("empty", 100, 170), ("complete", 171, 200)]


def test_splitting_a_range_and_putting_it_back_does_not_grow_the_table(session: Session) -> None:
    coverage.replace(session, KEY, SOURCE, 100, 200, [complete(100, 200)])
    coverage.replace(session, KEY, SOURCE, 150, 160, [pending(150, 160)])
    assert states(session) == [("complete", 100, 149), ("pending", 150, 160), ("complete", 161, 200)]

    coverage.replace(session, KEY, SOURCE, 150, 160, [complete(150, 160)])
    assert states(session) == [("complete", 100, 200)]  # one row again


def test_ranges_verified_one_after_the_other_stay_one_row(session: Session) -> None:
    """What storing page after page does: the top of a pending range is cut
    off again and again."""
    coverage.plan(session, KEY, SOURCE, 100, 130)
    for top, bottom in ((130, 111), (110, 101), (100, 100)):
        coverage.replace(session, KEY, SOURCE, bottom, top, [complete(bottom, top)])

    rows = coverage.ranges(session, KEY)
    assert states(session) == [("complete", 100, 130)]


def test_ranges_of_different_states_stay_apart_however_close(session: Session) -> None:
    coverage.replace(session, KEY, SOURCE, 100, 130, [complete(105, 130), unverifiable(100, 104, "gone")])
    coverage.replace(session, KEY, SOURCE, 105, 110, [complete(105, 110)])

    assert states(session) == [("unverifiable", 100, 104), ("complete", 105, 130)]
    assert coverage.ranges(session, KEY)[0].detail == "gone"


def test_a_merged_range_is_verified_as_of_the_later_time(session: Session) -> None:
    coverage.replace(session, KEY, SOURCE, 100, 110, [coverage.Piece(100, 110, "complete", None, datetime(2026, 1, 1, tzinfo=UTC))])
    coverage.replace(session, KEY, SOURCE, 111, 120, [coverage.Piece(111, 120, "complete", None, datetime(2026, 2, 1, tzinfo=UTC))])

    (row,) = coverage.ranges(session, KEY)
    assert (row.from_ledger, row.to_ledger) == (100, 120) and row.verified_at == datetime(2026, 2, 1, tzinfo=UTC)


def test_unverifiable_is_its_own_state_and_says_why(session: Session) -> None:
    coverage.plan(session, KEY, SOURCE, 95, 200)
    coverage.replace(session, KEY, SOURCE, 95, 99, [unverifiable(95, 99, "below the history the source keeps")])

    rows = coverage.ranges(session, KEY)
    assert [(row.state, row.detail) for row in rows][0] == ("unverifiable", "below the history the source keeps")
    assert rows[0].verified_at is not None and rows[1].verified_at is None
    assert coverage.tally(rows, 95, 200) == {"pending": 101, "complete": 0, "empty": 0, "unverifiable": 5}


@pytest.mark.parametrize(
    ("state", "verified"),
    [("pending", True), ("complete", False), ("unverifiable", False), ("finished", True)],
)
def test_the_database_refuses_states_that_lie_about_verification(
    session: Session, state: str, verified: bool
) -> None:
    from datetime import UTC, datetime

    session.add(
        SyncCoverage(
            subject_type=KEY.subject_type, subject_key=KEY.subject_key, data_type=KEY.data_type,
            selection_policy=KEY.selection_policy, policy_version=KEY.policy_version,
            from_ledger=1, to_ledger=5, source=SOURCE, state=state,
            verified_at=datetime.now(UTC) if verified else None,
        )
    )
    with pytest.raises(IntegrityError):
        session.flush()
    session.rollback()


def test_ranges_of_one_key_never_overlap(session: Session) -> None:
    coverage.replace(session, KEY, SOURCE, 10, 20, [complete(10, 20)])
    with pytest.raises(ValueError, match="pieces overlap"):
        coverage.replace(session, KEY, SOURCE, 5, 25, [complete(5, 12), complete(12, 25)])


def test_policies_policy_versions_and_streams_are_tiled_separately(session: Session) -> None:
    for key in (KEY, OTHER_POLICY, OTHER_VERSION, OTHER_STREAM):
        coverage.plan(session, key, SOURCE, 100, 110)
    assert all(states(session, key) == [("pending", 100, 110)] for key in (KEY, OTHER_POLICY, OTHER_VERSION, OTHER_STREAM))


def test_forgetting_a_range_makes_it_pending_again(session: Session) -> None:
    coverage.replace(session, KEY, SOURCE, 100, 200, [complete(100, 200)])
    coverage.invalidate(session, KEY, SOURCE, 120, 150)
    assert states(session) == [("complete", 100, 119), ("pending", 120, 150), ("complete", 151, 200)]

    coverage.invalidate(session, KEY, SOURCE, 100, 200)
    assert states(session) == [("pending", 100, 200)]


def test_tally_clips_ranges_to_the_window(session: Session) -> None:
    rows = [
        coverage.LedgerRange(90, 119, "complete", None, None),
        coverage.LedgerRange(120, 150, "empty", None, None),
        coverage.LedgerRange(151, 300, "unverifiable", "why", None),
    ]
    assert coverage.tally(rows, 100, 200) == {"pending": 0, "complete": 20, "empty": 31, "unverifiable": 50}


def test_pieces_must_lie_inside_the_interval_they_replace(session: Session) -> None:
    with pytest.raises(ValueError, match="not inside"):
        coverage.replace(session, KEY, SOURCE, 10, 20, [complete(10, 21)])
    with pytest.raises(ValueError, match="empty interval"):
        coverage.replace(session, KEY, SOURCE, 20, 10, [])
