"""Paging tokens by resource type, and their chronological order."""

from __future__ import annotations

import pytest

from app.sources.horizon_cursors import (
    InvalidCursor,
    after_ledger,
    effect_id,
    ledger_sequence_of,
    order_key,
    pair_paging_token,
    parse_cursor,
    to_total_order_id,
)
from tests.factories import ALICE, CAROL

# Ledger 500_000_000, third transaction, second operation.
OPERATION = to_total_order_id(500_000_000, 3, 2)
TRANSACTION = to_total_order_id(500_000_000, 3, 0)


def test_total_order_id_layout_matches_sep35() -> None:
    assert OPERATION == (500_000_000 << 32) | (3 << 12) | 2
    assert ledger_sequence_of(OPERATION) == 500_000_000
    assert parse_cursor("operation", str(OPERATION)).ledger_sequence == 500_000_000
    # A transaction shares its total order id with no operation: operations start at 1.
    assert TRANSACTION == to_total_order_id(500_000_000, 3)


def test_after_ledger_is_the_first_id_of_the_next_ledger() -> None:
    assert after_ledger(500_000_000) == to_total_order_id(500_000_000, (1 << 20) - 1, (1 << 12) - 1)
    assert after_ledger(500_000_000) < to_total_order_id(500_000_001)


@pytest.mark.parametrize(
    ("resource_type", "token", "ledger"),
    [
        ("ledger", str(7 << 32), 7),
        ("transaction", str(TRANSACTION), 500_000_000),
        ("operation", str(OPERATION), 500_000_000),
        ("effect", f"{OPERATION}-4", 500_000_000),
        ("trade", f"{OPERATION}-0", 500_000_000),
        # Horizon's effect `id` is the same pair zero-padded.
        ("effect", effect_id(OPERATION, 4), 500_000_000),
        ("account", ALICE, None),
    ],
)
def test_tokens_are_parsed_by_resource_type(resource_type: str, token: str, ledger: int | None) -> None:
    cursor = parse_cursor(resource_type, token)
    assert cursor.token == token
    assert cursor.ledger_sequence == ledger


@pytest.mark.parametrize(
    ("resource_type", "token"),
    [
        # A token of one type is not valid for another.
        ("transaction", str(OPERATION)),
        ("operation", str(TRANSACTION)),
        ("effect", str(OPERATION)),
        ("transaction", f"{OPERATION}-4"),
        ("ledger", str(OPERATION)),
        ("account", str(OPERATION)),
        ("effect", ALICE),
        # Not the documented shape.
        ("effect", f"{TRANSACTION}-4"),
        ("effect", f"{OPERATION}-99999999999"),
        ("effect", f"{OPERATION}-"),
        ("operation", "9223372036854775808"),
        ("operation", "-1"),
        ("operation", "+1"),
        ("operation", "1_0"),
        ("operation", " 1"),
        ("operation", "0x10"),
        ("operation", "1.0"),
        # A muxed (M...) address is not an account cursor either.
        ("account", "M" + ALICE[1:]),
    ],
)
def test_malformed_tokens_are_rejected(resource_type: str, token: str) -> None:
    with pytest.raises(InvalidCursor):
        parse_cursor(resource_type, token)


def test_unknown_resource_type_is_rejected() -> None:
    with pytest.raises(InvalidCursor):
        parse_cursor("offer", CAROL)


def test_pair_tokens_are_not_ordered_as_text() -> None:
    later_operation = OPERATION + 10
    tokens = [
        pair_paging_token(OPERATION, 2),
        pair_paging_token(later_operation, 0),
        pair_paging_token(OPERATION, 10),
        pair_paging_token(OPERATION, 1),
    ]
    by_number = sorted(tokens, key=lambda token: order_key("trade", token))
    assert by_number == [
        pair_paging_token(OPERATION, 1),
        pair_paging_token(OPERATION, 2),
        pair_paging_token(OPERATION, 10),
        pair_paging_token(later_operation, 0),
    ]
    # The text order disagrees both on the order component and on the
    # operation id, so sorting tokens as strings is not chronological order.
    assert sorted(tokens) != by_number


def test_effect_id_sorts_as_text_exactly_like_the_pair_as_numbers() -> None:
    operations = [OPERATION, OPERATION + 1, OPERATION + 1000, OPERATION - 5]
    ids = [effect_id(operation, order) for operation in operations for order in (0, 1, 10, 100)]
    as_text = sorted(ids)
    as_numbers = sorted(ids, key=lambda token: order_key("effect", token))
    assert as_text == as_numbers


def test_two_trades_of_one_operation_need_the_order_component() -> None:
    first, second = pair_paging_token(OPERATION, 0), pair_paging_token(OPERATION, 1)
    assert order_key("trade", first) < order_key("trade", second)
    assert order_key("effect", first) < order_key("effect", second)


def test_account_cursor_has_no_chronological_order() -> None:
    with pytest.raises(TypeError):
        order_key("account", ALICE)
