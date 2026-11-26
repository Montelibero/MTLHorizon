"""Horizon records as stored values, on answers saved from the public server."""

from __future__ import annotations

import json
import copy
import pathlib
from datetime import UTC, datetime

import pytest

from app.sources import horizon_records as records
from tests.factories import ISSUER_A
from tests.fake_horizon import A, B, C, D, S, FakeHorizon

FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures" / "live"


def live(name: str) -> object:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def changed(record: dict, **changes: object) -> dict:
    record = copy.deepcopy(record)
    for key, value in changes.items():
        if value is ...:
            del record[key]
        else:
            record[key] = value
    return record


OUTER = "85a5c7be66fd02ca1702434287e94b8b8fed86432c1c7ba574b545d080645058"
INNER = "f1ab88fe2cbf02e93522d59feb32da4cb2a727448534c0d5831d08a695697e67"


def test_a_fee_bump_maps_the_same_whichever_hash_was_asked_for() -> None:
    by_outer = records.transaction_values(live("transaction_fee_bump_outer.json"))
    by_inner = records.transaction_values(live("transaction_fee_bump_by_inner_hash.json"))

    # Every column is the same; only the kept answer differs, in `id` and `hash`.
    assert {k: v for k, v in by_outer[1].items() if k != "raw"} == {
        k: v for k, v in by_inner[1].items() if k != "raw"
    }
    assert by_outer[1]["raw"]["hash"] == OUTER and by_inner[1]["raw"]["hash"] == INNER
    key, values = by_outer
    assert key == {"id": 281967128423751680}
    assert values["transaction_hash"] == OUTER  # the fee-bump's own hash
    assert values["inner_transaction_hash"] == INNER
    assert (values["ledger_sequence"], values["application_order"]) == (65650588, 142)
    assert values["source_account"] != values["fee_source_account"]  # someone else paid
    assert (values["max_fee"], values["new_max_fee"]) == (10000, 10_000_000_000)  # inner, fee-bump
    assert values["signatures"] == ["UGBROmn53/kdBXJ+++mtYY/RneclNY58pqHKM1wKRemv+8Mh2pzgJH96SyBlER0cm9sOBJvxi7HSk9R1Th4LCQ=="]
    answer = live("transaction_fee_bump_outer.json")
    assert values["inner_signatures"] == answer["inner_transaction"]["signatures"]


def test_a_plain_transaction_has_no_fee_bump_columns() -> None:
    _, values = records.transaction_values(_plain())
    assert values["transaction_hash"] == _plain()["hash"]
    assert values["inner_transaction_hash"] is None and values["fee_source_account"] is None
    assert values["inner_signatures"] is None
    # REST `max_fee` is whatever the transaction's own fee is once it is not a fee bump.
    assert values["max_fee"] == 10_000_000_000 and values["new_max_fee"] is None


def test_meta_xdr_absent_is_none_and_present_is_kept() -> None:
    _, absent = records.transaction_values(live("transaction_fee_bump_outer.json"))
    assert "result_meta_xdr" not in live("transaction_fee_bump_outer.json")
    assert absent["result_meta_xdr"] is None
    _, empty = records.transaction_values(changed(_plain(), result_meta_xdr=""))
    assert empty["result_meta_xdr"] is None
    _, present = records.transaction_values(changed(_plain(), result_meta_xdr="AAAAAA=="))
    assert present["result_meta_xdr"] == "AAAAAA=="


def test_ledger_sequence_must_agree_with_the_cursor() -> None:
    with pytest.raises(records.RecordError, match="paging_token names ledger"):
        records.transaction_values(changed(_plain(), ledger=65650589))


@pytest.mark.parametrize(
    "changes",
    [
        {"source_account_sequence": 149989736969146244.0},
        {"successful": "true"},
        {"successful": ...},
        {"envelope_xdr": ...},
        {"source_account": "MD6TM5MEXAAAAAAAAAAAJRIXN7LY3OHMTZCUOWI7BSGIS5TUCPQRWHGQ"},
        {"paging_token": "281967128423751685"},
        {"hash": "not-a-hash"},
        {"signatures": "sig"},
    ],
)
def test_records_that_cannot_be_stored_exactly_are_refused(changes: dict) -> None:
    with pytest.raises(records.RecordError):
        records.transaction_values(changed(_plain(), **changes))


def test_unknown_operation_type_is_kept_as_it_came() -> None:
    operation = live("operation_path_payment_in_fee_bump.json")
    key, values = records.operation_values(changed(operation, type="unknown", type_i=99))
    assert key == {"id": 281967128423751681}
    assert (values["transaction_id"], values["application_order"]) == (281967128423751680, 1)
    assert (values["type"], values["type_i"]) == ("unknown", 99)
    assert records.transaction_of(operation) == 281967128423751680
    assert records.operation_of(operation) == 281967128423751681


def test_effect_id_and_cursor_name_the_same_operation_and_order() -> None:
    effect = live("transaction_effects.json")["_embedded"]["records"][0]
    key, values = records.effect_values(effect)
    assert key == {"id": "0281967128423751681-0000000001"}
    assert (values["operation_id"], values["effect_order"]) == (281967128423751681, 1)
    with pytest.raises(records.RecordError):
        records.effect_values(changed(effect, paging_token="281967128423751680-1"))  # no operation index


def test_a_trade_in_both_orientations_is_one_trade_with_the_sides_exchanged() -> None:
    given = live("trades_of_transaction.json")[1]
    other = live("trades_of_operation_market_orientation.json")[0]
    (key, values), (other_key, other) = records.trade_values(given), records.trade_values(other)
    assert key == other_key == {"id": "281967128423751681-1"}
    for column in ("asset_id", "amount", "account_id", "offer_id", "liquidity_pool_id"):
        assert values[f"base_{column}"] == other[f"counter_{column}"]
        assert values[f"counter_{column}"] == other[f"base_{column}"]
    assert values["base_is_seller"] is not other["base_is_seller"]
    assert (values["price_n"], values["price_d"]) == (10, 47)
    assert (other["price_n"], other["price_d"]) == (47, 10)
    assert values["base_amount"] == 231829549 and values["counter_amount"] == 49325436


def test_a_pool_trade_has_a_pool_on_its_base_side() -> None:
    _, values = records.trade_values(live("trades_of_transaction.json")[0])
    assert values["base_account_id"] is None and values["base_offer_id"] is None
    assert values["base_liquidity_pool_id"] and values["liquidity_pool_fee_bp"] == 30
    assert values["trade_type"] == "liquidity_pool" and values["base_is_seller"] is True


@pytest.mark.parametrize(
    "changes",
    [
        {"id": "281967128423751681-9"},
        {"base_amount": 23.1829549},
        {"base_amount": "1.00000001"},
        {"base_asset_code": ...},
        {"base_is_seller": "false"},
        {"paging_token": "281967128423751680-1"},
        {"price": {"n": "1", "d": "0"}},
        {"counter_account": "CPOWDQQDVSAQGJXZW3EWPPJ5JCF4KTTHBYNB4U54AKQVDLZXLLYMXY7"},
    ],
)
def test_trades_that_cannot_be_stored_exactly_are_refused(changes: dict) -> None:
    with pytest.raises(records.RecordError):
        records.trade_values(changed(live("trades_of_transaction.json")[1], **changes))


def test_a_price_of_zero_over_zero_is_a_missing_price() -> None:
    _, values = records.trade_values(changed(live("trades_of_transaction.json")[1], price={"n": "0", "d": "0"}))
    assert (values["price_n"], values["price_d"]) == (None, None)


def test_ledger_close_time_comes_from_the_trade_itself() -> None:
    _, values = records.trade_values(live("trades_of_transaction.json")[1])
    assert values["ledger_close_time"] == datetime(2026, 11, 24, 16, 49, 53, tzinfo=UTC)
    assert records.ledger_of(live("trades_of_transaction.json")[1], "trade") == 65650588


def test_a_ledger_record_fills_every_column_the_resource_has() -> None:
    ledger = FakeHorizon().ledger.ledgers[104]
    key, values = records.ledger_values(ledger)
    assert key == {"sequence": 104}
    assert values["close_time"] == datetime(2026, 1, 1, 0, 0, 20, tzinfo=UTC)
    assert values["total_coins"] == 1_000_000_000_000_000_000 and values["base_fee"] == 100
    assert values["raw"] == ledger


def test_a_ledger_stub_knows_only_what_a_transaction_says() -> None:
    assert records.ledger_stub(_plain()) == (
        {"sequence": 65650588}, {"close_time": datetime(2026, 11, 24, 16, 49, 53, tzinfo=UTC)}
    )


def _plain() -> dict:
    return changed(live("transaction_fee_bump_outer.json"), fee_bump_transaction=..., inner_transaction=..., hash=OUTER)


# -- participants ---------------------------------------------------------------


def _operation(kind: str, number: int, **body: object) -> dict:
    return {
        "id": str((200 << 32) | (1 << 12) + number), "paging_token": str((200 << 32) | (1 << 12) + number),
        "source_account": A, "type": kind, "transaction_successful": True, **body,
    }


def test_operation_participants_follow_the_operation_bodies() -> None:
    operations = [
        _operation("payment", 1, to=B),
        _operation("create_account", 2, account=C),
        _operation("account_merge", 3, into=B),
        _operation("allow_trust", 4, trustor=C),
        _operation("clawback", 5, **{"from": D}),
        _operation("set_trust_line_flags", 6, trustor=B),
        _operation("create_claimable_balance", 7, claimants=[{"destination": B}, {"destination": S}]),
        _operation("begin_sponsoring_future_reserves", 8, sponsored_id=D, **{"source_account": S}),
        _operation("end_sponsoring_future_reserves", 9, **{"source_account": D}),
        _operation("revoke_sponsorship", 10, signer_account_id=B),
        _operation("manage_data", 11),
        _operation("invoke_host_function", 12),
        _operation("a_type_nobody_knows", 13),
    ]
    participants = records.operation_participants(operations)
    base = 200 << 32 | 1 << 12
    assert participants == {
        base + 1: {A, B}, base + 2: {A, C}, base + 3: {A, B}, base + 4: {A, C}, base + 5: {A, D},
        base + 6: {A, B}, base + 7: {A, B, S}, base + 8: {S, D},
        base + 9: {D, S},  # sponsored by the operation 8's source
        base + 10: {A, B},
        base + 11: {A}, base + 12: {A}, base + 13: {A},
    }


def test_end_sponsoring_in_a_failed_transaction_has_no_sponsor() -> None:
    operations = [
        _operation("begin_sponsoring_future_reserves", 1, sponsored_id=D, **{"source_account": S}),
        _operation("end_sponsoring_future_reserves", 2, **{"source_account": D}, transaction_successful=False),
    ]
    participants = records.operation_participants(operations)
    assert participants[(200 << 32 | 1 << 12) + 2] == {D}


def test_transaction_participants_are_sources_and_operation_participants() -> None:
    transaction = _plain()
    source = transaction["source_account"]
    assert records.transaction_participants(transaction, {1: {A, B}, 2: {C}}) == {source, A, B, C}

    fee_bump = live("transaction_fee_bump_outer.json")
    _, values = records.transaction_values(fee_bump)
    assert records.transaction_participants(fee_bump, {1: {B}}) == {
        values["source_account"], values["fee_source_account"], B
    }


def test_accounts_in_a_record_exclude_asset_issuers() -> None:
    operation = live("operation_path_payment_in_fee_bump.json")
    accounts = records.accounts_in(operation)
    assert accounts == {operation["source_account"], operation["to"]}
    assert records.accounts_in({"asset_issuer": ISSUER_A, "to": A}) == {A}


def test_assets_in_a_record_are_found_at_every_depth() -> None:
    operation = live("operation_path_payment_in_fee_bump.json")
    issuer = "GACKTN5DAZGWXRWB2WLM6OPBDHAMT6SJNGLJZPQMEZBUR4JUGBX2UK7V"
    assert records.assets_in(operation) == {f"EURMTL:{issuer}", f"MTLRECT:{issuer}", f"MTL:{issuer}"}
    nested = {"path": [{"asset_type": "native"}], "reserves": [{"asset": "USD:G" + "A" * 55}]}
    assert records.assets_in(nested) == {"native", "USD:G" + "A" * 55}


def test_an_account_in_a_record_must_be_a_real_address() -> None:
    with pytest.raises(records.RecordError, match="not an account address"):
        records.operation_values(changed(live("operation_path_payment_in_fee_bump.json"), source_account="G" + "A" * 55))


def test_a_pool_share_and_an_asset_without_its_parts_are_left_out__not_refused() -> None:
    """Operations and effects of accounts that work with liquidity pools carry
    pool shares, and sometimes an asset Horizon wrote without its issuer.
    The record is kept; it is only not tracked by that asset."""
    change_trust = {
        "id": "1",
        "type": "change_trust",
        "asset_type": "liquidity_pool_shares",
        "liquidity_pool_id": "a" * 64,
        "trustee": "G" * 55 + "A",
        "source_account": "G" * 55 + "B",
    }
    assert records.assets_in(change_trust) == set()
    partial = {"id": "2", "type": "account_credited", "asset_type": "credit_alphanum4", "asset_code": "MTL"}
    assert records.assets_in(partial) == set()
    whole = {"id": "3", "asset_type": "credit_alphanum12", "asset_code": "LONGCODE", "asset_issuer": "G" * 55 + "D"}
    assert records.assets_in(whole) == {"LONGCODE:" + "G" * 55 + "D"}
    mixed = {"id": "4", "type": "path_payment_strict_receive", "source_asset_type": "native", "asset_type": "liquidity_pool_shares", "liquidity_pool_id": "a" * 64}
    assert records.assets_in(mixed) == {"native"}
