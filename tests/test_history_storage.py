"""History tables against a real, migrated PostgreSQL."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.sources.horizon_cursors import effect_id, pair_paging_token, to_total_order_id
from app.sources.horizon_format import amount_to_stroops, price_to_ratio, stroops_to_amount
from app.storage.models import (
    HistoryAsset,
    HistoryEffect,
    HistoryLedger,
    HistoryOperation,
    HistoryTransaction,
    HistoryTrade,
)
from app.storage.upsert import ContentConflict, UpsertOutcome, upsert_row, upsert_trade
from tests import factories as f


def count(session: Session, model: type) -> int:
    return session.scalar(select(func.count()).select_from(model)) or 0


def test_minimal_linked_record(session: Session) -> None:
    ledger = 1_000
    transaction_id = f.add_transaction(session, ledger, source_account=f.ALICE)
    operation_id = f.add_operation(session, transaction_id)
    f.add_participants(session, transaction_id, operation_id, f.ALICE, f.BOB)
    effect = f.add_effect(session, operation_id, 0, f.ALICE, "account_debited", 3)
    trade = f.add_trade(session, operation_id, 0)

    row = session.execute(
        text(
            "SELECT t.transaction_hash, l.close_time, o.type AS operation_type, "
            "       e.id AS effect_id, tr.id AS trade_id, "
            "       ba.asset_code AS base_code, ca.asset_code AS counter_code "
            "FROM history_transactions t "
            "JOIN history_ledgers l ON l.sequence = t.ledger_sequence "
            "JOIN history_operations o ON o.transaction_id = t.id "
            "JOIN history_effects e ON e.operation_id = o.id "
            "JOIN history_trades tr ON tr.operation_id = o.id "
            "JOIN history_assets ba ON ba.asset_id = tr.base_asset_id "
            "JOIN history_assets ca ON ca.asset_id = tr.counter_asset_id"
        )
    ).one()

    assert row.transaction_hash == f.digest(ledger * 100 + 1)
    assert row.close_time == f.close_time(ledger)
    assert row.operation_type == "payment"
    assert row.effect_id == effect == effect_id(operation_id, 0)
    assert row.trade_id == trade["id"] == pair_paging_token(operation_id, 0)
    assert (row.base_code, row.counter_code) == ("EURMTL", "USDMTL")


def test_rewriting_a_record_neither_duplicates_nor_downgrades_it(session: Session) -> None:
    key = {"sequence": 42}
    values = {"close_time": f.close_time(42)}

    assert upsert_row(session, HistoryLedger.__table__, key, values) is UpsertOutcome.INSERTED
    # Same content again: nothing happens.
    assert upsert_row(session, HistoryLedger.__table__, key, values) is UpsertOutcome.UNCHANGED
    # A fuller record completes the stub.
    fuller = {**values, "ledger_hash": f.digest(1), "protocol_version": 22}
    assert upsert_row(session, HistoryLedger.__table__, key, fuller) is UpsertOutcome.ENRICHED
    # A sparser record must not blank what is known.
    assert upsert_row(session, HistoryLedger.__table__, key, values) is UpsertOutcome.UNCHANGED

    ledger = session.get(HistoryLedger, 42)
    assert count(session, HistoryLedger) == 1
    assert ledger.ledger_hash == f.digest(1)
    assert ledger.protocol_version == 22


def test_conflicting_content_is_reported_not_overwritten(session: Session) -> None:
    transaction_id = f.add_transaction(session, 10, max_fee=100)
    with pytest.raises(ContentConflict) as error:
        f.add_transaction(session, 10, max_fee=101)

    assert error.value.table == "history_transactions"
    assert error.value.column == "max_fee"
    assert (error.value.existing, error.value.incoming) == (100, 101)
    assert session.get(HistoryTransaction, transaction_id).max_fee == 100


def test_raw_json_is_merged_keywise(session: Session) -> None:
    key = {"sequence": 5}
    table = HistoryLedger.__table__
    close_time = {"close_time": f.close_time(5)}
    upsert_row(session, table, key, {**close_time, "raw": {"id": "a", "ledger": 5}})
    # A response that knows more completes the stored record.
    upsert_row(
        session, table, key, {**close_time, "raw": {"successful_transaction_count": 3}}
    )
    stored = session.get(HistoryLedger, 5)
    assert stored.raw == {"id": "a", "ledger": 5, "successful_transaction_count": 3}

    # A response that knows less leaves it alone; object key order is not content.
    upsert_row(session, table, key, {**close_time, "raw": {"ledger": 5, "id": "a"}})
    assert session.get(HistoryLedger, 5).raw == stored.raw

    with pytest.raises(ContentConflict) as error:
        upsert_row(session, table, key, {**close_time, "raw": {"ledger": 6}})
    assert error.value.column == "raw.ledger"


def test_transaction_found_through_two_participants_is_stored_once(session: Session) -> None:
    ledger = 20
    # The same transaction arrives once from Alice's stream and once from Bob's.
    for _ in range(2):
        transaction_id = f.add_transaction(session, ledger, source_account=f.ALICE)
        operation_id = f.add_operation(session, transaction_id, source_account=f.ALICE)
        f.add_participants(session, transaction_id, operation_id, f.ALICE, f.BOB)
        f.add_effect(session, operation_id, 0, f.BOB, "account_credited", 2)

    assert count(session, HistoryTransaction) == 1
    assert count(session, HistoryOperation) == 1
    assert count(session, HistoryEffect) == 1

    history = text(
        "SELECT t.id FROM history_transactions t "
        "JOIN history_transaction_participants p ON p.transaction_id = t.id "
        "WHERE p.account_id = :account"
    )
    for account in (f.ALICE, f.BOB):
        assert [row.id for row in session.execute(history, {"account": account})] == [transaction_id]


def test_amounts_prices_and_times_stay_exact_in_the_database(session: Session) -> None:
    big = "9007199254.7409930"  # more stroops than a double can hold
    transaction_id = f.add_transaction(session, 30)
    operation_id = f.add_operation(session, transaction_id)
    price = price_to_ratio("123456789", "1000000007")
    f.add_trade(
        session,
        operation_id,
        0,
        base_amount=amount_to_stroops(big),
        counter_amount=amount_to_stroops("0.0000001"),
        price=price,
    )

    stored = session.get(HistoryTrade, pair_paging_token(operation_id, 0))
    assert type(stored.base_amount) is int
    assert stored.base_amount == 90071992547409930
    assert stroops_to_amount(stored.base_amount) == big
    assert stored.counter_amount == 1
    assert (stored.price_n, stored.price_d) == price

    # Times keep microseconds.
    micro = datetime(2025, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
    upsert_row(session, HistoryLedger.__table__, {"sequence": 31}, {"close_time": micro})
    assert session.get(HistoryLedger, 31).close_time == micro
    assert session.get(HistoryTransaction, transaction_id).ledger_close_time == f.close_time(30)


def test_unknown_operation_type_and_missing_meta_are_kept(session: Session) -> None:
    transaction_id = f.add_transaction(session, 40, result_meta_xdr=None, raw={"hash": "h"})
    operation_id = f.add_operation(
        session, transaction_id, type="unknown", type_i=99, raw={"type": "unknown", "type_i": 99}
    )

    operation = session.get(HistoryOperation, operation_id)
    assert (operation.type, operation.type_i) == ("unknown", 99)
    transaction = session.get(HistoryTransaction, transaction_id)
    # Missing is NULL, not an empty string.
    assert transaction.result_meta_xdr is None

    # A later response that carries the meta completes the record.
    outcome = upsert_row(
        session,
        HistoryTransaction.__table__,
        {"id": transaction_id},
        {"result_meta_xdr": "meta", "raw": {"result_meta_xdr": "meta"}},
    )
    assert outcome is UpsertOutcome.ENRICHED
    session.refresh(transaction)
    assert transaction.result_meta_xdr == "meta"


def test_assets_with_one_code_and_different_issuers_are_distinct(session: Session) -> None:
    f.add_asset(session, f.ASSET_A)
    f.add_asset(session, f.ASSET_B)
    transaction_id = f.add_transaction(session, 50)
    operation_id = f.add_operation(session, transaction_id)
    f.add_trade(session, operation_id, 0, base_asset=f.ASSET_A, counter_asset=f.NATIVE)
    f.add_trade(session, operation_id, 1, base_asset=f.NATIVE, counter_asset=f.ASSET_B)

    assets = session.scalars(
        select(HistoryAsset.asset_id).where(HistoryAsset.asset_code == "MTL").order_by("asset_id")
    ).all()
    assert assets == sorted([f.ASSET_A, f.ASSET_B])
    issuers = session.execute(
        text(
            "SELECT tr.trade_order, a.asset_issuer FROM history_trades tr "
            "JOIN history_assets a ON a.asset_id IN (tr.base_asset_id, tr.counter_asset_id) "
            "WHERE a.asset_code = 'MTL' ORDER BY tr.trade_order"
        )
    ).all()
    assert issuers == [(0, f.ISSUER_A), (1, f.ISSUER_B)]


def test_native_asset_is_one_asset_without_code_and_issuer(session: Session) -> None:
    f.add_asset(session, f.NATIVE)
    native = session.get(HistoryAsset, "native")
    assert (native.asset_type, native.asset_code, native.asset_issuer) == ("native", "", "")


def test_xlm_cannot_be_stored_as_if_it_had_an_issuer(session: Session) -> None:
    assert f.is_refused(
        session,
        lambda: upsert_row(
            session,
            HistoryAsset.__table__,
            {"asset_id": "native"},
            {"asset_type": "native", "asset_code": "XLM", "asset_issuer": f.ISSUER_A},
        ),
    )


def test_effect_or_trade_without_its_operation_is_refused(session: Session) -> None:
    f.add_account(session, f.ALICE)
    f.add_account(session, f.BOB)
    f.add_asset(session, f.EUR)
    f.add_asset(session, f.USD)
    orphan = to_total_order_id(60, 1, 1)

    assert f.is_refused(
        session,
        lambda: upsert_row(
            session,
            HistoryEffect.__table__,
            {"id": effect_id(orphan, 0)},
            {
                "paging_token": pair_paging_token(orphan, 0),
                "operation_id": orphan,
                "effect_order": 0,
                "account_id": f.ALICE,
                "type": "account_created",
                "type_i": 0,
                "ledger_close_time": f.close_time(60),
            },
        ),
    )
    assert f.is_refused(
        session,
        lambda: upsert_trade(
            session,
            {
                "id": pair_paging_token(orphan, 0),
                "operation_id": orphan,
                "trade_order": 0,
                "ledger_close_time": f.close_time(60),
                "base_asset_id": f.EUR,
                "counter_asset_id": f.USD,
                "base_amount": 1,
                "counter_amount": 1,
                "base_account_id": f.BOB,
                "counter_account_id": f.ALICE,
                "base_is_seller": True,
                "trade_type": "orderbook",
            },
        ),
    )
    assert count(session, HistoryEffect) == 0
    assert count(session, HistoryTrade) == 0


def test_identifier_must_match_its_ledger_and_order(session: Session) -> None:
    f.add_ledger(session, 70)
    f.add_account(session, f.ALICE)
    wrong = {name: value for name, value in f.transaction_values(70, 2).items() if name != "id"}
    assert f.is_refused(
        session, lambda: upsert_row(session, HistoryTransaction.__table__, {"id": to_total_order_id(70, 1)}, wrong)
    )
    assert count(session, HistoryTransaction) == 0


def test_effect_cursor_must_encode_the_stored_operation_and_order(session: Session) -> None:
    transaction_id = f.add_transaction(session, 80)
    operation_id = f.add_operation(session, transaction_id)
    f.add_account(session, f.ALICE)
    mismatch = {
        "paging_token": pair_paging_token(operation_id, 1),
        "operation_id": operation_id,
        "effect_order": 1,
        "account_id": f.ALICE,
        "type": "account_created",
        "type_i": 0,
        "ledger_close_time": f.close_time(80),
    }
    # The id says order 0, the rest says order 1.
    assert f.is_refused(
        session, lambda: upsert_row(session, HistoryEffect.__table__, {"id": effect_id(operation_id, 0)}, mismatch)
    )


def test_trade_seen_with_the_pair_reversed_is_stored_once(session: Session) -> None:
    transaction_id = f.add_transaction(session, 90)
    operation_id = f.add_operation(session, transaction_id)

    first = f.add_trade(session, operation_id, 0, price=(3, 1))
    reversed_view = f.trade_values(
        session,
        operation_id,
        0,
        base_asset=f.USD,
        counter_asset=f.EUR,
        base_amount=first["counter_amount"],
        counter_amount=first["base_amount"],
        base_account=first["counter_account_id"],
        counter_account=first["base_account_id"],
        price=(1, 3),
    )
    reversed_view.update(base_is_seller=False, base_offer_id=None, counter_offer_id=100)
    assert upsert_trade(session, reversed_view) is UpsertOutcome.UNCHANGED

    assert count(session, HistoryTrade) == 1
    stored = session.get(HistoryTrade, first["id"])
    # The first orientation is kept and stays consistent with itself.
    assert (stored.base_asset_id, stored.counter_asset_id) == (f.EUR, f.USD)
    assert (stored.base_amount, stored.counter_amount, stored.base_is_seller) == (10_000_000, 30_000_000, True)
    assert (stored.price_n, stored.price_d) == (3, 1)


def test_a_different_trade_with_the_same_id_is_a_conflict(session: Session) -> None:
    transaction_id = f.add_transaction(session, 91)
    operation_id = f.add_operation(session, transaction_id)
    f.add_trade(session, operation_id, 0)
    with pytest.raises(ContentConflict):
        f.add_trade(session, operation_id, 0, base_amount=10_000_001)


def test_trades_of_one_operation_are_told_apart_by_order(session: Session) -> None:
    transaction_id = f.add_transaction(session, 92)
    operation_id = f.add_operation(session, transaction_id)
    f.add_trade(session, operation_id, 0)
    f.add_trade(session, operation_id, 1)
    assert count(session, HistoryTrade) == 2


def test_market_query_finds_the_pair_whichever_side_it_was_stored_on(session: Session) -> None:
    first_operation = f.add_operation(session, f.add_transaction(session, 100))
    second_operation = f.add_operation(session, f.add_transaction(session, 101))
    f.add_trade(session, first_operation, 0, base_asset=f.EUR, counter_asset=f.USD)
    f.add_trade(session, second_operation, 0, base_asset=f.USD, counter_asset=f.EUR)

    market = text(
        "SELECT tr.id FROM history_trades tr "
        "WHERE (tr.base_asset_id = :a AND tr.counter_asset_id = :b) "
        "   OR (tr.base_asset_id = :b AND tr.counter_asset_id = :a) "
        "ORDER BY tr.ledger_close_time, tr.operation_id, tr.trade_order"
    )
    expected = [pair_paging_token(first_operation, 0), pair_paging_token(second_operation, 0)]
    assert session.execute(market, {"a": f.EUR, "b": f.USD}).scalars().all() == expected
    # With the pair requested the other way round: the same trades, same order.
    assert session.execute(market, {"a": f.USD, "b": f.EUR}).scalars().all() == expected


def test_account_history_is_ordered_chronologically_by_total_order_id(session: Session) -> None:
    for ledger in (5, 40, 300):
        transaction_id = f.add_transaction(session, ledger, source_account=f.ALICE)
        operation_id = f.add_operation(session, transaction_id)
        f.add_participants(session, transaction_id, operation_id, f.ALICE)

    ids = session.scalars(
        text(
            "SELECT t.id FROM history_transactions t "
            "JOIN history_transaction_participants p ON p.transaction_id = t.id "
            "WHERE p.account_id = :account ORDER BY t.id DESC"
        ),
        {"account": f.ALICE},
    ).all()
    # 300, 40, 5: as numbers, not "5" < "40" < "300" as text would have it.
    assert ids == [to_total_order_id(ledger, 1) for ledger in (300, 40, 5)]
