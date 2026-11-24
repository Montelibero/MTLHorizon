"""manage_data: participation versus source, and how a deleted entry shows up."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from tests import factories as f

OPERATIONS_FOR_PARTICIPANT = text(
    "SELECT o.id FROM history_operations o "
    "JOIN history_operation_participants p ON p.operation_id = o.id "
    "WHERE p.account_id = :account AND o.type_i = :type_i ORDER BY o.id DESC"
)
OPERATIONS_BY_SOURCE = text(
    "SELECT o.id FROM history_operations o "
    "WHERE o.source_account = :account AND o.type_i = :type_i ORDER BY o.id DESC"
)
DATA_EFFECTS_OF_ACCOUNT = text(
    "SELECT e.id FROM history_effects e WHERE e.account_id = :account AND e.type_i IN (40, 41, 42)"
)


def store_manage_data(
    session: Session, ledger: int, *, operation_source: str | None, value: str = "aGVsbG8="
) -> int:
    """A transaction sourced by Alice carrying one manage_data operation.

    ``operation_source`` is the operation's own source; None means it
    inherits the transaction's source, as on the network.
    """
    transaction_id = f.add_transaction(session, ledger, source_account=f.ALICE)
    operation_id = f.add_operation(
        session,
        transaction_id,
        source_account=operation_source,
        type="manage_data",
        type_i=f.MANAGE_DATA,
        raw={"type": "manage_data", "type_i": f.MANAGE_DATA, "name": "config", "value": value},
    )
    # Horizon: the only direct participant of manage_data is its source.
    f.add_participants(session, transaction_id, operation_id, operation_source or f.ALICE)
    return operation_id


def test_manage_data_is_found_by_participant_and_by_source(session: Session) -> None:
    bobs_operation = store_manage_data(session, ledger=10, operation_source=f.BOB)
    alices_operation = store_manage_data(session, ledger=11, operation_source=None)

    def for_participant(account: str) -> list[int]:
        return session.scalars(OPERATIONS_FOR_PARTICIPANT, {"account": account, "type_i": f.MANAGE_DATA}).all()

    def by_source(account: str) -> list[int]:
        return session.scalars(OPERATIONS_BY_SOURCE, {"account": account, "type_i": f.MANAGE_DATA}).all()

    # Bob is the source of his operation although Alice sources both transactions.
    assert for_participant(f.BOB) == by_source(f.BOB) == [bobs_operation]
    assert for_participant(f.ALICE) == by_source(f.ALICE) == [alices_operation]
    assert for_participant(f.CAROL) == by_source(f.CAROL) == []


def test_data_entry_owner_is_not_necessarily_the_operation_source(session: Session) -> None:
    # A contract invoked by Alice writes an entry owned by Carol (Soroban can
    # do that; the operation is invoke_host_function, not manage_data).
    transaction_id = f.add_transaction(session, 20, source_account=f.ALICE)
    operation_id = f.add_operation(
        session, transaction_id, source_account=f.ALICE, type="invoke_host_function", type_i=24
    )
    f.add_participants(session, transaction_id, operation_id, f.ALICE)
    effect = f.add_effect(
        session, operation_id, 0, f.CAROL, "data_created", f.DATA_CREATED, raw={"name": "config", "value": "aGVsbG8="}
    )

    data_effects = session.scalars(DATA_EFFECTS_OF_ACCOUNT, {"account": f.CAROL}).all()
    assert data_effects == [effect]
    assert session.scalars(DATA_EFFECTS_OF_ACCOUNT, {"account": f.ALICE}).all() == []

    # Carol's data changed, but she is not a participant of the operation: a
    # query by participants would miss it, a query by source would misattribute it.
    operations = session.scalars(OPERATIONS_FOR_PARTICIPANT, {"account": f.CAROL, "type_i": 24}).all()
    assert operations == []
    assert session.scalars(OPERATIONS_BY_SOURCE, {"account": f.CAROL, "type_i": 24}).all() == []


def test_deleting_an_entry_is_told_from_setting_an_empty_value(session: Session) -> None:
    # Horizon reports both as `value: ""` on the operation.
    deleted = store_manage_data(session, ledger=30, operation_source=None, value="")
    emptied = store_manage_data(session, ledger=31, operation_source=None, value="")
    f.add_effect(session, deleted, 0, f.ALICE, "data_removed", f.DATA_REMOVED, raw={"name": "config"})
    f.add_effect(session, emptied, 0, f.ALICE, "data_created", f.DATA_CREATED, raw={"name": "config", "value": ""})

    operations = session.execute(
        text("SELECT id, raw->>'value' AS value FROM history_operations ORDER BY id")
    ).all()
    assert operations == [(deleted, ""), (emptied, "")]

    # Only the effect type tells them apart.
    effects = session.execute(
        text(
            "SELECT e.operation_id, e.type_i FROM history_effects e "
            "WHERE e.type_i IN (40, 41, 42) ORDER BY e.operation_id"
        )
    ).all()
    assert effects == [(deleted, f.DATA_REMOVED), (emptied, f.DATA_CREATED)]
