"""Exact conversions between Horizon's REST representation and stored values.

Horizon sends amounts and prices as decimal strings. They are kept exact
here: integers (stroops) for amounts, a numerator/denominator pair for prices.
Nothing in this module produces a float.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

AMOUNT_DECIMALS = 7
STROOPS_PER_UNIT = 10**AMOUNT_DECIMALS
INT64_MAX = 2**63 - 1

# Horizon's own output format: whole part, dot, exactly seven fraction digits.
_AMOUNT = re.compile(r"[0-9]+(?:\.[0-9]{1,7})?")
_NONNEGATIVE_INT = re.compile(r"[0-9]{1,19}")


class FormatError(ValueError):
    """A value does not have the exact representation it must have."""


def _exact_int(value: str | int, what: str) -> int:
    text = value.strip() if isinstance(value, str) else str(value)
    if not _NONNEGATIVE_INT.fullmatch(text):
        raise FormatError(f"{what} {value!r} is not a non-negative integer")
    number = int(text)
    if number > INT64_MAX:
        raise FormatError(f"{what} {value!r} is outside int64")
    return number


def amount_to_stroops(value: str | int) -> int:
    """Convert an amount string such as ``"123.0000456"`` to integer stroops.

    ``"1"`` and ``"1.0000000"`` both give 10_000_000. More than seven decimal
    places, a sign, a blank or any other text is rejected: an amount that does
    not convert exactly must not be rounded silently.
    """
    text = value.strip() if isinstance(value, str) else str(value)
    if not _AMOUNT.fullmatch(text):
        raise FormatError(f"amount {value!r} is not an unsigned amount with at most 7 decimals")
    try:
        stroops = int(Decimal(text).scaleb(AMOUNT_DECIMALS))
    except InvalidOperation as error:
        raise FormatError(f"amount {value!r} is not a decimal number") from error
    if stroops > INT64_MAX:
        raise FormatError(f"amount {value!r} does not fit in int64 stroops")
    return stroops


def stroops_to_amount(stroops: int) -> str:
    """Inverse of :func:`amount_to_stroops`, in Horizon's format."""
    sign = "-" if stroops < 0 else ""
    stroops = abs(stroops)
    return f"{sign}{stroops // STROOPS_PER_UNIT}.{stroops % STROOPS_PER_UNIT:07d}"


def price_to_ratio(numerator: str | int, denominator: str | int) -> tuple[int, int]:
    """Convert Horizon's ``price`` object (``n`` and ``d``) to an exact ratio."""
    n = _exact_int(numerator, "price numerator")
    d = _exact_int(denominator, "price denominator")
    if d == 0:
        raise FormatError("price denominator is zero")
    return n, d


def asset_key(asset_type: str, asset_code: str = "", asset_issuer: str = "") -> str:
    """Canonical asset id: ``native`` for XLM, otherwise ``<code>:<issuer>``."""
    if asset_type == "native":
        return "native"
    if not asset_code or not asset_issuer:
        raise FormatError(f"asset of type {asset_type!r} needs both code and issuer")
    return f"{asset_code}:{asset_issuer}"


def asset_key_from_record(asset: Mapping[str, Any]) -> str:
    """Asset id from a Horizon asset object (``asset_type`` and optional
    ``asset_code`` / ``asset_issuer``)."""
    return asset_key(
        asset["asset_type"], asset.get("asset_code") or "", asset.get("asset_issuer") or ""
    )


def parse_timestamp(value: str) -> datetime:
    """RFC 3339 timestamp (Horizon's ``created_at`` and friends) to an aware
    datetime in UTC. Fractional seconds are kept, not rounded to milliseconds."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise FormatError(f"timestamp {value!r} has no time zone")
    return parsed.astimezone(timezone.utc)
