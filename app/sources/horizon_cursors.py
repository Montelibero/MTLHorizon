"""Horizon paging tokens and total order ids (SEP-35).

Verified against stellar-horizon @ 4e13b1bea4c87e770447814a242384e0eb00c015
and go-stellar-sdk @ 35ebe09ed33c24da6df989969f42cbd648f4ce34; file
references are in docs/horizon-schema.md.

==============  ==============================  =========================
resource       paging_token                   ledger sequence
==============  ==============================  =========================
ledger         ``sequence << 32``             ``value >> 32``
transaction    total order id, op part 0      ``value >> 32``
operation      total order id, op part >= 1   ``value >> 32``
effect         ``"<operation id>-<order>"``   first component ``>> 32``
trade          ``"<operation id>-<order>"``   first component ``>> 32``
account        the account address            not derivable
==============  ==============================  =========================

A paging token is not a number in general, and a composite one must not be
ordered as text: ``"10-1"`` sorts before ``"9-1"`` although it is later.
Always go through :func:`parse_cursor` and :func:`order_key`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

LEDGER_SHIFT = 32
TRANSACTION_SHIFT = 12
OPERATION_SHIFT = 0

LEDGER_MASK = (1 << 32) - 1
TRANSACTION_MASK = (1 << 20) - 1
OPERATION_MASK = (1 << 12) - 1

INT64_MAX = 2**63 - 1
# Horizon clamps the order component of an effect/trade cursor to this.
MAX_ORDER = 2**31 - 1

RESOURCE_TYPES = ("ledger", "transaction", "operation", "effect", "trade", "account")

# Decimal digits only: int() alone would also accept "+1", " 1" and "1_0".
_TOTAL_ORDER_ID = re.compile(r"[0-9]{1,19}")
_ORDER_PAIR = re.compile(r"([0-9]{1,19})-([0-9]{1,10})")
_G_ADDRESS = re.compile(r"G[0-9A-Z]{55}")


class InvalidCursor(ValueError):
    """The token is not a valid paging token of the given resource type."""


@dataclass(frozen=True)
class TotalOrderId:
    """Position of a ledger, transaction or operation in the network's total
    order: ``ledger << 32 | transaction << 12 | operation``."""

    ledger_sequence: int
    transaction_order: int
    operation_order: int

    @property
    def value(self) -> int:
        return to_total_order_id(self.ledger_sequence, self.transaction_order, self.operation_order)

    @property
    def transaction_id(self) -> "TotalOrderId":
        """The transaction this position belongs to (operation part zero)."""
        return TotalOrderId(self.ledger_sequence, self.transaction_order, 0)

    @classmethod
    def parse(cls, value: int) -> "TotalOrderId":
        if not 0 <= value <= INT64_MAX:
            raise InvalidCursor(f"total order id {value!r} is outside int64")
        return cls(
            ledger_sequence=(value >> LEDGER_SHIFT) & LEDGER_MASK,
            transaction_order=(value >> TRANSACTION_SHIFT) & TRANSACTION_MASK,
            operation_order=(value >> OPERATION_SHIFT) & OPERATION_MASK,
        )


def to_total_order_id(
    ledger_sequence: int, transaction_order: int = 0, operation_order: int = 0
) -> int:
    if not 0 <= ledger_sequence <= LEDGER_MASK:
        raise ValueError(f"ledger sequence {ledger_sequence!r} does not fit in 32 bits")
    if not 0 <= transaction_order <= TRANSACTION_MASK:
        raise ValueError(f"transaction order {transaction_order!r} does not fit in 20 bits")
    if not 0 <= operation_order <= OPERATION_MASK:
        raise ValueError(f"operation order {operation_order!r} does not fit in 12 bits")
    return (ledger_sequence << LEDGER_SHIFT) | (transaction_order << TRANSACTION_SHIFT) | operation_order


def ledger_sequence_of(total_order_id: int) -> int:
    """Ledger sequence in which a transaction or operation with this id happened."""
    return TotalOrderId.parse(total_order_id).ledger_sequence


def after_ledger(ledger_sequence: int) -> int:
    """First total order id after everything in ``ledger_sequence``."""
    return to_total_order_id(ledger_sequence, TRANSACTION_MASK, OPERATION_MASK)


def effect_id(operation_id: int, order: int) -> str:
    """Horizon effect ``id``: the cursor pair zero-padded so that text order
    equals chronological order."""
    return f"{operation_id:019d}-{order:010d}"


def pair_paging_token(operation_id: int, order: int) -> str:
    """Horizon ``paging_token`` of an effect or a trade (also the trade id)."""
    return f"{operation_id}-{order}"


@dataclass(frozen=True)
class Cursor:
    resource_type: str
    token: str
    # None when the format of the resource type carries no ledger sequence.
    ledger_sequence: int | None


def _read_int64(text: str) -> int:
    if not _TOTAL_ORDER_ID.fullmatch(text):
        raise InvalidCursor(f"{text!r} is not a decimal total order id")
    value = int(text)
    if value > INT64_MAX:
        raise InvalidCursor(f"{text!r} is outside int64")
    return value


def parse_cursor(resource_type: str, token: str) -> Cursor:
    """Validate a record's paging token for ``resource_type`` and derive the
    ledger sequence from it when the format allows."""
    if resource_type == "account":
        if not _G_ADDRESS.fullmatch(token):
            raise InvalidCursor(f"{token!r} is not an account address")
        return Cursor(resource_type, token, None)

    if resource_type == "ledger":
        value = _read_int64(token)
        if value & LEDGER_MASK:
            raise InvalidCursor(f"{token!r} is not a ledger paging token (low 32 bits set)")
        return Cursor(resource_type, token, value >> LEDGER_SHIFT)

    if resource_type in ("transaction", "operation"):
        order_id = TotalOrderId.parse(_read_int64(token))
        if resource_type == "transaction" and order_id.operation_order != 0:
            raise InvalidCursor(f"{token!r} has an operation index; not a transaction token")
        if resource_type == "operation" and order_id.operation_order == 0:
            raise InvalidCursor(f"{token!r} has no operation index; not an operation token")
        return Cursor(resource_type, token, order_id.ledger_sequence)

    if resource_type in ("effect", "trade"):
        match = _ORDER_PAIR.fullmatch(token)
        if match is None:
            raise InvalidCursor(f"{token!r} is not an '<operation id>-<order>' pair")
        operation_id, order = int(match[1]), int(match[2])
        if operation_id > INT64_MAX:
            raise InvalidCursor(f"{token!r} is outside int64")
        if TotalOrderId.parse(operation_id).operation_order == 0:
            raise InvalidCursor(f"{token!r} does not refer to an operation")
        if order > MAX_ORDER:
            raise InvalidCursor(f"{token!r} order is above {MAX_ORDER}")
        return Cursor(resource_type, token, TotalOrderId.parse(operation_id).ledger_sequence)

    raise InvalidCursor(f"unknown resource type {resource_type!r}")


def order_key(resource_type: str, token: str) -> tuple[int, ...]:
    """Sort key with the verified chronological meaning of a paging token.

    Effects and trades order by (operation id, order): numerically, never as
    text. The operation id alone is not enough, two trades of one operation
    share it.
    """
    parse_cursor(resource_type, token)
    if resource_type == "account":
        raise TypeError("account cursors have no chronological order")
    if resource_type in ("effect", "trade"):
        operation_id, _, order = token.partition("-")
        return int(operation_id), int(order)
    if resource_type == "ledger":
        return (int(token) >> LEDGER_SHIFT,)
    return (int(token),)
