"""Storing fetched records: the database half of loading, with no network.

Every function takes the session of an open transaction. Records are stored
through the insert-or-merge rules of :mod:`app.storage.upsert`: a record met
a second time fills in what was missing and never overwrites.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from app.sources import horizon_records as records
from app.storage.models import (
    HistoryAccount,
    HistoryAsset,
    HistoryEffect,
    HistoryLedger,
    HistoryOperation,
    HistoryOperationParticipant,
    HistoryTransaction,
    HistoryTransactionParticipant,
)
from app.storage.upsert import mark_context, upsert_row, upsert_trade

if TYPE_CHECKING:
    from app.backfill.jobs import StreamJob
    from app.backfill.loader import ContextBundle, ContextFetcher


def asset_parts(asset_id: str) -> dict[str, str]:
    """The columns of ``history_assets`` for a canonical asset id."""
    if asset_id == "native":
        return {"asset_type": "native", "asset_code": "", "asset_issuer": ""}
    code, issuer = asset_id.split(":", 1)
    return {
        "asset_type": "credit_alphanum4" if len(code) <= 4 else "credit_alphanum12",
        "asset_code": code,
        "asset_issuer": issuer,
    }


def store_ledger(session: Session, record: Mapping[str, Any]) -> None:
    key, values = records.ledger_values(record)
    upsert_row(session, HistoryLedger.__table__, key, values)


def store_transaction(session: Session, record: Mapping[str, Any]) -> int:
    """Store a transaction with its ledger stub and the accounts it names."""
    key, values = records.transaction_values(record)
    ensure_accounts(session, records.accounts_in(record))
    ledger_key, ledger_values = records.ledger_stub(record)
    upsert_row(session, HistoryLedger.__table__, ledger_key, ledger_values)
    upsert_row(session, HistoryTransaction.__table__, key, values)
    return key["id"]


def store_operation(session: Session, record: Mapping[str, Any]) -> int:
    key, values = records.operation_values(record)
    ensure_accounts(session, records.accounts_in(record))
    ensure_assets(session, records.assets_in(record))
    upsert_row(session, HistoryOperation.__table__, key, values)
    return key["id"]


def store_effect(session: Session, record: Mapping[str, Any]) -> str:
    key, values = records.effect_values(record)
    ensure_accounts(session, records.accounts_in(record))
    ensure_assets(session, records.assets_in(record))
    upsert_row(session, HistoryEffect.__table__, key, values)
    return key["id"]


def store_trade(session: Session, record: Mapping[str, Any]) -> str:
    key, values = records.trade_values(record)
    ensure_accounts(session, records.accounts_in(record))
    ensure_assets(session, records.assets_in(record))
    upsert_trade(session, {**values, **key})
    return key["id"]


def link_operation(session: Session, operation_id: int, accounts: Iterable[str]) -> None:
    for account in sorted(set(accounts)):
        ensure_accounts(session, [account])
        upsert_row(
            session,
            HistoryOperationParticipant.__table__,
            {"operation_id": operation_id, "account_id": account},
            {},
        )


def link_transaction(session: Session, transaction_id: int, accounts: Iterable[str]) -> None:
    for account in sorted(set(accounts)):
        ensure_accounts(session, [account])
        upsert_row(
            session,
            HistoryTransactionParticipant.__table__,
            {"transaction_id": transaction_id, "account_id": account},
            {},
        )


def store_context(session: Session, context: "ContextBundle", completes: Iterable[str]) -> None:
    """Store a transaction and the parts of its context that were fetched.

    With the operations come the participants they imply. Every list that is
    not None is complete, as far as the source serves it; ``completes`` names
    the streams that are stored for good by this.
    """
    store_transaction(session, context.transaction)
    transaction_id = context.transaction_id
    loaded = []
    if context.operations is not None:
        for record in context.operations:
            store_operation(session, record)
        participants = records.operation_participants(context.operations)
        for operation_id, accounts in participants.items():
            link_operation(session, operation_id, accounts)
        link_transaction(
            session, transaction_id, records.transaction_participants(context.transaction, participants)
        )
        loaded.append("operations")
    if context.effects is not None:
        for record in context.effects:
            store_effect(session, record)
        loaded.append("effects")
    if context.trades is not None:
        for record in context.trades:
            store_trade(session, record)
        loaded.append("trades")
    mark_context(session, transaction_id, *completes)


def store_stream_record(
    session: Session, job: "StreamJob", record: Mapping[str, Any], *, parents_only: bool
) -> None:
    """Store one record of a stream, and the participation Horizon states by
    returning the record in this account's stream.

    When whole contexts are fetched, the participants the operation bodies
    name are linked with the context. With ``parents_only`` there is no
    context, so they are linked here for the record itself.
    """
    if job.stream == "transactions":
        transaction_id = store_transaction(session, record)
        accounts = {job.participant} if job.participant else set()
        if parents_only:
            accounts |= records.transaction_participants(record, {})
        link_transaction(session, transaction_id, accounts)
    elif job.stream == "operations":
        operation_id = store_operation(session, record)
        stated = {job.participant} if job.participant else set()
        named = records.operation_participants([record]).get(operation_id, set()) if parents_only else set()
        link_operation(session, operation_id, stated | named)
        link_transaction(session, records.transaction_of(record), stated | named)
    elif job.stream == "effects":
        store_effect(session, record)
    else:
        store_trade(session, record)


def store_operation_alone(session: Session, record: Mapping[str, Any]) -> int:
    """Store one operation that was fetched on its own, for a record that
    needs it as a parent, with the participants its body names."""
    operation_id = store_operation(session, record)
    named = records.operation_participants([record]).get(operation_id, set())
    link_operation(session, operation_id, named)
    link_transaction(session, records.transaction_of(record), named)
    return operation_id


def store_prepared(session: Session, contexts: "ContextFetcher") -> None:
    """Store what was fetched for the records of a page: the transactions
    with the parts of their context that came with them, then the operations
    fetched one by one. The records of the page itself follow."""
    for bundle in contexts.bundles():
        store_context(session, bundle, bundle.completes)
    for record in contexts.pending_operations():
        store_operation_alone(session, record)


def ensure_accounts(session: Session, accounts: Iterable[str]) -> None:
    # Seen once in a transaction, an account or asset is not looked up again.
    for account in sorted(set(accounts) - session.info.setdefault("accounts", set())):
        upsert_row(session, HistoryAccount.__table__, {"account_id": account}, {})
        session.info["accounts"].add(account)


def ensure_assets(session: Session, asset_ids: Iterable[str]) -> None:
    for asset_id in sorted(set(asset_ids) - session.info.setdefault("assets", set())):
        upsert_row(session, HistoryAsset.__table__, {"asset_id": asset_id}, asset_parts(asset_id))
        session.info["assets"].add(asset_id)
