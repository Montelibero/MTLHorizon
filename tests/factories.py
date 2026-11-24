"""Synthetic records for storage tests.

Addresses are well-formed ed25519 account ids derived from small integers:
valid for the schema's format checks, not anyone's account.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from hashlib import sha256

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from stellar_sdk import StrKey

from app.sources.horizon_cursors import effect_id, pair_paging_token, to_total_order_id
from app.storage.models import (
    HistoryAccount,
    HistoryAsset,
    HistoryEffect,
    HistoryLedger,
    HistoryOperation,
    HistoryOperationParticipant,
    HistoryTransaction,
    HistoryTransactionParticipant,
    HistoryTrade,
)
from app.storage.upsert import upsert_row, upsert_trade

GENESIS_TIME = datetime(2024, 3, 1, 12, 0, 0, tzinfo=UTC)
MANAGE_DATA = 10  # Horizon's type_i for manage_data
DATA_CREATED, DATA_REMOVED, DATA_UPDATED = 40, 41, 42


def address(seed: int) -> str:
    return StrKey.encode_ed25519_public_key(seed.to_bytes(32, "big"))


def is_refused(session: Session, statement: Callable[[], object]) -> bool:
    """True when PostgreSQL rejects what ``statement`` writes.

    The attempt runs in a savepoint of its own, so the data written earlier
    in the test survives the rejection and the session stays usable.
    """
    try:
        with session.begin_nested():
            statement()
    except IntegrityError:
        return True
    return False


ISSUER_A = address(1)
ISSUER_B = address(2)
ALICE = address(3)
BOB = address(4)
CAROL = address(5)

NATIVE = "native"
ASSET_A = f"MTL:{ISSUER_A}"
ASSET_B = f"MTL:{ISSUER_B}"
EUR = f"EURMTL:{ISSUER_A}"
USD = f"USDMTL:{ISSUER_A}"


def digest(seed: int) -> str:
    return sha256(f"mtl-horizon-test-{seed}".encode()).hexdigest()


def close_time(ledger_sequence: int) -> datetime:
    return GENESIS_TIME + timedelta(seconds=5 * (ledger_sequence - 1))


def add_account(session: Session, account: str = ALICE) -> None:
    upsert_row(session, HistoryAccount.__table__, {"account_id": account}, {})


def add_asset(session: Session, asset_id: str = NATIVE) -> None:
    if asset_id == NATIVE:
        parts = {"asset_type": "native", "asset_code": "", "asset_issuer": ""}
    else:
        code, issuer = asset_id.split(":")
        parts = {
            "asset_type": "credit_alphanum4" if len(code) <= 4 else "credit_alphanum12",
            "asset_code": code,
            "asset_issuer": issuer,
        }
    upsert_row(session, HistoryAsset.__table__, {"asset_id": asset_id}, parts)


def add_ledger(session: Session, ledger_sequence: int, **extra: object) -> None:
    upsert_row(
        session,
        HistoryLedger.__table__,
        {"sequence": ledger_sequence},
        {"close_time": close_time(ledger_sequence), **extra},
    )


def transaction_values(
    ledger_sequence: int,
    application_order: int = 1,
    source_account: str = ALICE,
    transaction_hash: str | None = None,
    **extra: object,
) -> dict[str, object]:
    values: dict[str, object] = {
        "id": to_total_order_id(ledger_sequence, application_order),
        "transaction_hash": transaction_hash or digest(ledger_sequence * 100 + application_order),
        "ledger_sequence": ledger_sequence,
        "application_order": application_order,
        "source_account": source_account,
        "account_sequence": 1_000_000 + application_order,
        "max_fee": 100,
        "operation_count": 1,
        "successful": True,
        "ledger_close_time": close_time(ledger_sequence),
        "envelope_xdr": "envelope",
        "result_xdr": "result",
        "memo_type": "none",
    }
    values.update(extra)
    return values


def add_transaction(session: Session, ledger_sequence: int, **extra: object) -> int:
    """Store a transaction with its ledger and source account; returns its id."""
    values = transaction_values(ledger_sequence, **extra)
    add_ledger(session, ledger_sequence)
    add_account(session, values["source_account"])
    upsert_row(session, HistoryTransaction.__table__, {"id": values["id"]}, values)
    return values["id"]


def add_operation(
    session: Session,
    transaction_id: int,
    application_order: int = 1,
    source_account: str | None = None,
    type: str = "payment",
    type_i: int = 1,
    **extra: object,
) -> int:
    """Store an operation of a stored transaction; returns its id."""
    transaction = session.get(HistoryTransaction, transaction_id)
    operation_id = transaction_id + application_order
    source = source_account or transaction.source_account
    add_account(session, source)
    upsert_row(
        session,
        HistoryOperation.__table__,
        {"id": operation_id},
        {
            "transaction_id": transaction_id,
            "application_order": application_order,
            "source_account": source,
            "type": type,
            "type_i": type_i,
            "ledger_close_time": transaction.ledger_close_time,
            **extra,
        },
    )
    return operation_id


def add_participants(session: Session, transaction_id: int, operation_id: int, *accounts: str) -> None:
    """Link accounts to a transaction and to one of its operations."""
    for account in accounts:
        add_account(session, account)
        upsert_row(
            session,
            HistoryTransactionParticipant.__table__,
            {"transaction_id": transaction_id, "account_id": account},
            {},
        )
        upsert_row(
            session,
            HistoryOperationParticipant.__table__,
            {"operation_id": operation_id, "account_id": account},
            {},
        )


def add_effect(
    session: Session,
    operation_id: int,
    order: int,
    account: str,
    type: str,
    type_i: int,
    **extra: object,
) -> str:
    """Store an effect of a stored operation; returns its id."""
    effect = effect_id(operation_id, order)
    add_account(session, account)
    upsert_row(
        session,
        HistoryEffect.__table__,
        {"id": effect},
        {
            "paging_token": pair_paging_token(operation_id, order),
            "operation_id": operation_id,
            "effect_order": order,
            "account_id": account,
            "type": type,
            "type_i": type_i,
            "ledger_close_time": session.get(HistoryOperation, operation_id).ledger_close_time,
            **extra,
        },
    )
    return effect


def trade_values(
    session: Session,
    operation_id: int,
    order: int,
    *,
    base_asset: str = EUR,
    counter_asset: str = USD,
    base_amount: int = 10_000_000,
    counter_amount: int = 30_000_000,
    base_account: str | None = BOB,
    counter_account: str | None = ALICE,
    price: tuple[int, int] = (3, 1),
    **extra: object,
) -> dict[str, object]:
    values: dict[str, object] = {
        "id": pair_paging_token(operation_id, order),
        "operation_id": operation_id,
        "trade_order": order,
        "ledger_close_time": session.get(HistoryOperation, operation_id).ledger_close_time,
        "base_asset_id": base_asset,
        "counter_asset_id": counter_asset,
        "base_amount": base_amount,
        "counter_amount": counter_amount,
        "base_account_id": base_account,
        "counter_account_id": counter_account,
        "base_liquidity_pool_id": None,
        "counter_liquidity_pool_id": None,
        "base_offer_id": 100 if base_account else None,
        "counter_offer_id": None,
        "base_is_seller": True,
        "price_n": price[0],
        "price_d": price[1],
        "trade_type": "orderbook",
        "liquidity_pool_fee_bp": None,
    }
    values.update(extra)
    return values


def add_trade(session: Session, operation_id: int, order: int, **options: object) -> dict[str, object]:
    values = trade_values(session, operation_id, order, **options)  # type: ignore[arg-type]
    for asset in (values["base_asset_id"], values["counter_asset_id"]):
        add_asset(session, asset)
    for account in (values["base_account_id"], values["counter_account_id"]):
        if account:
            add_account(session, account)
    upsert_trade(session, values)
    return values
