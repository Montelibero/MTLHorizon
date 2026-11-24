"""Exact conversions of Horizon amounts, prices, assets and timestamps."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.sources.horizon_format import (
    FormatError,
    amount_to_stroops,
    asset_key,
    asset_key_from_record,
    parse_timestamp,
    price_to_ratio,
    stroops_to_amount,
)

# Larger than 2**53 / 10**7: a double cannot hold this amount in stroops.
BIG_AMOUNT = "9007199254.7409930"
BIG_STROOPS = 90071992547409930


def test_amount_converts_exactly_beyond_float_precision() -> None:
    assert amount_to_stroops(BIG_AMOUNT) == BIG_STROOPS
    assert stroops_to_amount(BIG_STROOPS) == BIG_AMOUNT
    # What a float round trip would have made of it.
    assert int(float(BIG_AMOUNT) * 10**7) != BIG_STROOPS


@pytest.mark.parametrize(
    ("text", "stroops"),
    [("0", 0), ("1", 10_000_000), ("1.0000000", 10_000_000), ("0.0000001", 1), ("123.0000456", 1230000456)],
)
def test_amount_formats(text: str, stroops: int) -> None:
    assert amount_to_stroops(text) == stroops


@pytest.mark.parametrize(
    "text", ["1.00000001", "1.1234567890", "-1", "+1", "1e2", "NaN", "Infinity", "", "1 000", "1_000", "1,5"]
)
def test_amounts_that_do_not_convert_exactly_are_rejected(text: str) -> None:
    with pytest.raises(FormatError):
        amount_to_stroops(text)


def test_amount_must_fit_int64_stroops() -> None:
    # The largest seven-decimal amount that fits int64 stroops, and the next one.
    # int64 max is itself a whole number of stroops: 922337203685.4775807.
    assert amount_to_stroops("922337203685.4775807") == 2**63 - 1
    with pytest.raises(FormatError):
        amount_to_stroops("922337203685.4775808")


def test_price_is_an_exact_ratio() -> None:
    # 0.3333333 is what Horizon prints as the decimal price; the ratio has no loss.
    assert price_to_ratio("1", "3") == (1, 3)
    assert price_to_ratio(2, 1) == (2, 1)
    assert price_to_ratio("123456789012345678", "7") == (123456789012345678, 7)


@pytest.mark.parametrize(("numerator", "denominator"), [("1", "0"), ("-1", "3"), ("1.5", "3"), ("x", "3"), ("1", "y")])
def test_invalid_prices_are_rejected(numerator: str, denominator: str) -> None:
    with pytest.raises(FormatError):
        price_to_ratio(numerator, denominator)


def test_native_asset_has_one_identity_regardless_of_representation() -> None:
    assert asset_key("native") == "native"
    assert asset_key("native", "", "") == "native"
    assert asset_key_from_record({"asset_type": "native"}) == "native"


def test_asset_identity_includes_issuer_and_type() -> None:
    assert asset_key("credit_alphanum4", "MTL", "GAAA") == "MTL:GAAA"
    assert asset_key("credit_alphanum12", "MTL", "GBBB") == "MTL:GBBB"
    assert asset_key("credit_alphanum4", "MTL", "GAAA") != asset_key("credit_alphanum4", "MTL", "GBBB")
    assert asset_key("credit_alphanum4", "MTL", "GAAA") != asset_key("credit_alphanum12", "LONGCODE", "GAAA")


@pytest.mark.parametrize(
    "record", [{"asset_type": "credit_alphanum4"}, {"asset_type": "credit_alphanum4", "asset_code": "MTL"}]
)
def test_non_native_asset_needs_code_and_issuer(record: dict[str, str]) -> None:
    with pytest.raises(FormatError):
        asset_key_from_record(record)


def test_timestamp_keeps_microseconds_and_is_utc() -> None:
    parsed = parse_timestamp("2024-03-01T12:34:56.123456Z")
    assert parsed == datetime(2024, 3, 1, 12, 34, 56, 123456, tzinfo=UTC)
    assert parse_timestamp("2024-03-01T15:34:56.123456+03:00") == parsed


def test_naive_timestamp_is_rejected() -> None:
    with pytest.raises(FormatError):
        parse_timestamp("2024-03-01T12:34:56")
