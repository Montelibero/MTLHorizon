"""Horizon REST records as row values for the storage layer.

A record that cannot be converted exactly is refused with :class:`RecordError`:
it is better to stop a page than to store a number that was guessed. Nothing
here decodes XDR; the XDR strings are carried as they came.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from typing import Any

from stellar_sdk import StrKey

from app.sources.horizon_cursors import InvalidCursor, TotalOrderId, parse_cursor
from app.sources.horizon_format import (
    FormatError,
    amount_to_stroops,
    asset_key,
    parse_timestamp,
    price_to_ratio,
)

Record = Mapping[str, Any]
Row = dict[str, Any]

# The three collections that make up the context of a transaction.
CONTEXT_STREAMS = ("operations", "effects", "trades")

_G_ADDRESS = re.compile(r"G[0-9A-Z]{55}")
_SHA256_HEX = re.compile(r"[0-9a-f]{64}")
_INTEGER = re.compile(r"-?[0-9]{1,19}")
_CANONICAL_ASSET = re.compile(r"native|[A-Za-z0-9]{1,12}:G[0-9A-Z]{55}")


class RecordError(ValueError):
    """A record cannot be turned into stored values without guessing."""


# -- shared pieces -----------------------------------------------------------


def _int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not _INTEGER.fullmatch(str(value)):
        raise RecordError(f"{field} is not an integer: {value!r}")
    return int(value)


def _optional_int(record: Record, field: str) -> int | None:
    value = record.get(field)
    return None if value is None else _int(value, field)


def _text(record: Record, field: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value:
        raise RecordError(f"{field} is missing: {value!r}")
    return value


def _bool(record: Record, field: str) -> bool:
    value = record.get(field)
    if not isinstance(value, bool):
        raise RecordError(f"{field} is not a boolean: {value!r}")
    return value


def _hash(value: object, field: str) -> str:
    if not isinstance(value, str) or not _SHA256_HEX.fullmatch(value):
        raise RecordError(f"{field} is not a sha256 hex hash: {value!r}")
    return value


def _optional_hash(value: object, field: str) -> str | None:
    return None if not value else _hash(value, field)


def _strings(record: Record, field: str) -> list[str]:
    value = record.get(field)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise RecordError(f"{field} is not a list of strings: {value!r}")
    return list(value)


def _time(record: Record, field: str = "created_at") -> datetime:
    try:
        return parse_timestamp(_text(record, field))
    except (FormatError, ValueError) as error:
        raise RecordError(f"{field} is not an RFC 3339 timestamp: {record.get(field)!r}") from error


def _account(value: object, field: str) -> str:
    if isinstance(value, str) and _G_ADDRESS.fullmatch(value) and StrKey.is_valid_ed25519_public_key(value):
        return value
    raise RecordError(f"{field} is not an account address: {value!r}")


def _optional_account(record: Record, field: str) -> str | None:
    value = record.get(field)
    return None if value is None else _account(value, field)


def _asset(record: Mapping[str, Any], prefix: str) -> str:
    try:
        return asset_key(
            _text(record, f"{prefix}asset_type"),
            record.get(f"{prefix}asset_code") or "",
            record.get(f"{prefix}asset_issuer") or "",
        )
    except (FormatError, KeyError) as error:
        raise RecordError(f"{prefix}asset is not a complete asset: {error}") from error


def _optional_xdr(record: Record, field: str) -> str | None:
    # Absent or empty both mean the source did not give it.
    value = record.get(field)
    return value if isinstance(value, str) and value else None


def _total_order_id(record: Record, field: str) -> TotalOrderId:
    try:
        return TotalOrderId.parse(_int(record[field], field))
    except InvalidCursor as error:
        raise RecordError(f"{field} is not a total order id: {record.get(field)!r}") from error


# -- one record -> (key, values) ---------------------------------------------

def _without_links(record: Mapping[str, Any]) -> dict[str, Any]:
    """The record as Horizon sent it, without its ``_links``: they name the
    server that answered (for a gateway, with its key), and a record is the
    same record whoever sent it."""
    return {name: value for name, value in record.items() if name != "_links"}



def ledger_values(record: Record) -> tuple[Row, Row]:
    sequence = _int(record["sequence"], "sequence")
    close_time = _time(record, "closed_at")
    try:
        coins = _optional_amount(record, "total_coins")
        pool = _optional_amount(record, "fee_pool")
    except FormatError as error:
        raise RecordError(str(error)) from error
    return {"sequence": sequence}, {
        "close_time": close_time,
        "ledger_hash": _optional_hash(record.get("hash"), "hash"),
        "previous_ledger_hash": _optional_hash(record.get("prev_hash"), "prev_hash"),
        "protocol_version": _optional_int(record, "protocol_version"),
        "successful_transaction_count": _optional_int(record, "successful_transaction_count"),
        "failed_transaction_count": _optional_int(record, "failed_transaction_count"),
        "operation_count": _optional_int(record, "operation_count"),
        "tx_set_operation_count": _optional_int(record, "tx_set_operation_count"),
        "total_coins": coins,
        "fee_pool": pool,
        "base_fee": _optional_int(record, "base_fee_in_stroops"),
        "base_reserve": _optional_int(record, "base_reserve_in_stroops"),
        "max_tx_set_size": _optional_int(record, "max_tx_set_size"),
        "header_xdr": _optional_xdr(record, "header_xdr"),
        "raw": _without_links(record),
    }


def _optional_amount(record: Record, field: str) -> int | None:
    value = record.get(field)
    return None if value is None else amount_to_stroops(_text(record, field))


def ledger_stub(transaction: Record) -> tuple[Row, Row]:
    """The little a transaction knows about its ledger: sequence and close time."""
    return {"sequence": _int(transaction["ledger"], "ledger")}, {"close_time": _time(transaction)}


def transaction_values(record: Record) -> tuple[Row, Row]:
    order_id = _total_order_id(record, "paging_token")
    if order_id.operation_order != 0:
        raise RecordError(f"paging_token is not a transaction token: {record.get('paging_token')!r}")
    ledger_sequence = _int(record["ledger"], "ledger")
    if ledger_sequence != order_id.ledger_sequence:
        raise RecordError(f"paging_token names ledger {order_id.ledger_sequence}, ledger says {ledger_sequence}")
    values: Row = {
        "transaction_hash": None,
        "inner_transaction_hash": None,
        "ledger_sequence": ledger_sequence,
        "application_order": order_id.transaction_order,
        "source_account": _account(record.get("source_account"), "source_account"),
        "source_account_muxed": record.get("account_muxed"),
        "account_sequence": _int(record["source_account_sequence"], "source_account_sequence"),
        "max_fee": None,
        "fee_charged": _optional_int(record, "fee_charged"),
        "operation_count": _int(record["operation_count"], "operation_count"),
        "successful": _bool(record, "successful"),
        "ledger_close_time": _time(record),
        "envelope_xdr": _text(record, "envelope_xdr"),
        "result_xdr": _text(record, "result_xdr"),
        "result_meta_xdr": _optional_xdr(record, "result_meta_xdr"),
        "fee_meta_xdr": _optional_xdr(record, "fee_meta_xdr"),
        "memo_type": _text(record, "memo_type"),
        "memo": record.get("memo"),
        "signatures": [],
        "fee_source_account": None,
        "fee_source_account_muxed": None,
        "new_max_fee": None,
        "inner_signatures": None,
        "raw": _without_links(record),
    }
    if "fee_bump_transaction" in record:
        outer, inner = record["fee_bump_transaction"], record.get("inner_transaction")
        if not isinstance(outer, dict) or not isinstance(inner, dict):
            raise RecordError("fee-bump transaction without fee_bump_transaction and inner_transaction")
        # `hash`, `id` and `signatures` of the response depend on which hash
        # was asked for; these two do not.
        values.update(
            transaction_hash=_hash(outer.get("hash"), "fee_bump_transaction.hash"),
            inner_transaction_hash=_hash(inner.get("hash"), "inner_transaction.hash"),
            # REST `max_fee` is the fee of the fee-bump; the inner fee is its own.
            new_max_fee=_int(record["max_fee"], "max_fee"),
            max_fee=_int(inner["max_fee"], "inner_transaction.max_fee"),
            fee_source_account=_account(record.get("fee_account"), "fee_account"),
            fee_source_account_muxed=record.get("fee_account_muxed"),
            signatures=_strings(outer, "signatures"),
            inner_signatures=_strings(inner, "signatures"),
        )
    else:
        values.update(
            transaction_hash=_hash(record.get("hash"), "hash"),
            max_fee=_int(record["max_fee"], "max_fee"),
            signatures=_strings(record, "signatures"),
        )
    return {"id": order_id.value}, values


def operation_values(record: Record) -> tuple[Row, Row]:
    order_id = _total_order_id(record, "id")
    if order_id.operation_order == 0:
        raise RecordError(f"operation id has no operation index: {record.get('id')!r}")
    if str(record.get("paging_token")) != str(record["id"]):
        raise RecordError(f"id and paging_token differ: {record.get('id')!r}, {record.get('paging_token')!r}")
    operation_id = order_id.value
    return {"id": operation_id}, {
        "transaction_id": order_id.transaction_id.value,
        "application_order": order_id.operation_order,
        "source_account": _account(record.get("source_account"), "source_account"),
        "source_account_muxed": record.get("source_account_muxed"),
        # Kept as they came: a type this code does not know is not an error.
        "type": _text(record, "type"),
        "type_i": _int(record["type_i"], "type_i"),
        "ledger_close_time": _time(record),
        "raw": _without_links(record),
    }


def effect_values(record: Record) -> tuple[Row, Row]:
    token = _text(record, "paging_token")
    try:
        parse_cursor("effect", token)
    except InvalidCursor as error:
        raise RecordError(f"paging_token is not an effect cursor: {token!r}") from error
    operation_id, order = (int(part) for part in token.split("-"))
    return {"id": _text(record, "id")}, {
        "paging_token": token,
        "operation_id": operation_id,
        "effect_order": order,
        "account_id": _account(record.get("account"), "account"),
        "account_muxed": record.get("account_muxed"),
        "type": _text(record, "type"),
        "type_i": _int(record["type_i"], "type_i"),
        "ledger_close_time": _time(record),
        "raw": _without_links(record),
    }


def trade_values(record: Record) -> tuple[Row, Row]:
    token = _text(record, "paging_token")
    trade_id = _text(record, "id")
    if trade_id != token:
        raise RecordError(f"trade id and paging_token differ: {trade_id!r}, {token!r}")
    try:
        parse_cursor("trade", token)
    except InvalidCursor as error:
        raise RecordError(f"paging_token is not a trade cursor: {token!r}") from error
    operation_id, order = (int(part) for part in token.split("-"))

    price: tuple[int | None, int | None] = (None, None)
    given = record.get("price")
    if isinstance(given, dict):
        try:
            price = price_to_ratio(given["n"], given["d"])
        except FormatError:
            # 0/0 is how Horizon reports a trade ingested before prices were stored.
            if str(given.get("n")) != "0" or str(given.get("d")) != "0":
                raise RecordError(f"price is not a ratio: {given!r}") from None
            price = (None, None)
        except KeyError as error:
            raise RecordError(f"price has no {error.args[0]}") from None
    try:
        amounts = (amount_to_stroops(_text(record, "base_amount")), amount_to_stroops(_text(record, "counter_amount")))
    except FormatError as error:
        raise RecordError(str(error)) from error

    return {"id": trade_id}, {
        "operation_id": operation_id,
        "trade_order": order,
        "ledger_close_time": _time(record, "ledger_close_time"),
        "base_asset_id": _asset(record, "base_"),
        "counter_asset_id": _asset(record, "counter_"),
        "base_amount": amounts[0],
        "counter_amount": amounts[1],
        "base_account_id": _optional_account(record, "base_account"),
        "counter_account_id": _optional_account(record, "counter_account"),
        "base_liquidity_pool_id": record.get("base_liquidity_pool_id"),
        "counter_liquidity_pool_id": record.get("counter_liquidity_pool_id"),
        "base_offer_id": _optional_int(record, "base_offer_id"),
        "counter_offer_id": _optional_int(record, "counter_offer_id"),
        "base_is_seller": _bool(record, "base_is_seller"),
        "price_n": price[0],
        "price_d": price[1],
        "trade_type": _text(record, "trade_type"),
        "liquidity_pool_fee_bp": _optional_int(record, "liquidity_pool_fee_bp"),
        "raw": _without_links(record),
    }


# -- what a record points at -------------------------------------------------


def paging_token(record: Record) -> str:
    return _text(record, "paging_token")


def ledger_of(record: Record, resource_type: str) -> int:
    """Ledger sequence a record happened in, from its own cursor."""
    try:
        ledger = parse_cursor(resource_type, paging_token(record)).ledger_sequence
    except InvalidCursor as error:
        raise RecordError(f"paging_token does not name a ledger: {record.get('paging_token')!r}") from error
    assert ledger is not None
    return ledger


def operation_of(record: Record) -> int:
    """The operation an operation, effect or trade record belongs to, taken
    from its cursor: the total order id, or its first component."""
    return _operation_id(record)


def transaction_of(record: Record) -> int:
    """The transaction an operation, effect or trade record belongs to."""
    return TotalOrderId.parse(_operation_id(record)).transaction_id.value


def _operation_id(record: Record) -> int:
    first, _, _ = str(record.get("paging_token")).partition("-")
    order_id = _total_order_id({"paging_token": first}, "paging_token")
    if order_id.operation_order == 0:
        raise RecordError(f"paging_token does not name an operation: {record.get('paging_token')!r}")
    return order_id.value


def transaction_hash(record: Record) -> str:
    return _hash(record.get("transaction_hash"), "transaction_hash")


# -- what a record mentions --------------------------------------------------


def accounts_in(*records: Record) -> set[str]:
    """Every account address a record carries, at any depth.

    These are accounts seen in history, which is what ``history_accounts``
    records; it says nothing about them being tracked or being participants.
    """
    found: set[str] = set()
    for key, value in _strings_in(("", record) for record in records):
        # An issuer names who created an asset, not an account seen at work.
        if key.endswith("issuer"):
            continue
        if len(value) == 56 and value.startswith("G") and StrKey.is_valid_ed25519_public_key(value):
            found.add(value)
    return found


def assets_in(*records: Record) -> set[str]:
    """Every asset a record carries, at any depth, in either form it appears:
    as an object (``asset_type`` and optional code and issuer) or as the
    canonical string (``native`` or ``CODE:ISSUER``)."""
    found: set[str] = set()
    for mapping in _mappings_in(*records):
        for key, value in mapping.items():
            if key == "asset" and isinstance(value, str) and _CANONICAL_ASSET.fullmatch(value):
                found.add(value)
            elif key.endswith("asset_type"):
                prefix = key[: -len("asset_type")]
                found.add(_asset(mapping, prefix))
    return found


def _strings_in(items: Iterable[tuple[str, object]]) -> Iterable[tuple[str, str]]:
    """Every string in the records, with the key it sits under."""
    for key, value in items:
        if isinstance(value, str):
            yield key, value
        elif isinstance(value, Mapping):
            yield from _strings_in(value.items())
        elif isinstance(value, (list, tuple)):
            yield from _strings_in((key, item) for item in value)


def _mappings_in(*values: object) -> Iterable[Mapping[str, Any]]:
    for value in values:
        if isinstance(value, Mapping):
            yield value
            yield from _mappings_in(*value.values())
        elif isinstance(value, (list, tuple)):
            yield from _mappings_in(*value)


# -- participants ------------------------------------------------------------

# Operations that name one other account in their body (Horizon's
# Participants switch, internal/ingest/processors/operations_processor.go).
_NAMED_ACCOUNT = {
    "create_account": "account",
    "payment": "to",
    "path_payment_strict_receive": "to",
    "path_payment_strict_send": "to",
    "account_merge": "into",
    "allow_trust": "trustor",
    "begin_sponsoring_future_reserves": "sponsored_id",
    "clawback": "from",
    "set_trust_line_flags": "trustor",
}
_REVOKED_OWNERS = ("account_id", "data_account_id", "trustline_account_id", "signer_account_id")


def operation_participants(operations: Sequence[Record]) -> dict[int, set[str]]:
    """Accounts Horizon names as participants of each operation.

    Derived from the operation bodies alone, as Horizon does: the source,
    plus the accounts the body names. Not included, because REST does not
    carry them: the sponsor of a created ledger entry, and what an
    ``invoke_host_function`` operation changed.
    """
    result: dict[int, set[str]] = {}
    for index, operation in enumerate(operations):
        order_id = _total_order_id(operation, "id")
        source = _account(operation.get("source_account"), "source_account")
        accounts = {source}
        kind = operation.get("type")
        if kind in _NAMED_ACCOUNT:
            accounts.add(_account(operation.get(_NAMED_ACCOUNT[kind]), _NAMED_ACCOUNT[kind]))
        elif kind == "create_claimable_balance":
            claimants = operation.get("claimants")
            if not isinstance(claimants, list):
                raise RecordError("create_claimable_balance has no claimants")
            accounts.update(_account(claimant.get("destination"), "claimant.destination") for claimant in claimants)
        elif kind == "end_sponsoring_future_reserves" and operation.get("transaction_successful") is True:
            # The source of the begin_sponsoring operation that sponsors this
            # operation's source; Horizon looks no further than this transaction.
            accounts.update(_sponsoring_sources(operations[:index], source))
        elif kind == "revoke_sponsorship":
            accounts.update(
                _account(operation[field], field) for field in _REVOKED_OWNERS if operation.get(field)
            )
        result[order_id.value] = accounts
    return result


def _sponsoring_sources(earlier: Sequence[Record], sponsored: str) -> set[str]:
    for operation in reversed(earlier):
        if operation.get("type") == "begin_sponsoring_future_reserves" and operation.get("sponsored_id") == sponsored:
            return {_account(operation.get("source_account"), "source_account")}
    return set()


def transaction_participants(
    transaction: Record, operation_participants_: Mapping[int, set[str]]
) -> set[str]:
    """Accounts known to be participants of a transaction: its source, the
    fee source of a fee bump, and all participants of its operations.

    Horizon also adds every account whose ledger entry the transaction
    changed. That needs the transaction meta, which the public Horizon does
    not serve, so those participants are missing here for accounts that are
    not tracked themselves (their own streams name them).
    """
    accounts = {_account(transaction.get("source_account"), "source_account")}
    if "fee_bump_transaction" in transaction:
        accounts.add(_account(transaction.get("fee_account"), "fee_account"))
    for participants in operation_participants_.values():
        accounts.update(participants)
    return accounts
