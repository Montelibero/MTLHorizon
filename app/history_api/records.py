"""Stored rows as the records upstream Horizon answers with.

The same members, in the same types: amounts, prices and sequences as
strings, never a float. What the stored columns know comes from them (the
columns are checked by the database and do not depend on which way round the
source happened to be asked); everything else — the members that belong to the
type of the record, ``preconditions``, a memo — comes from the answer of the
source that was stored with the row. A row stored without one lacks them, and
they are absent rather than made up. A member that was never stored
(``result_meta_xdr``) is never present.

The links are built here, for this installation. The links stored with the
source's answer are not served: they name the source's server.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.engine import Row

from app.backfill.jobs import asset_params
from app.sources.horizon_format import stroops_to_amount

Link = dict[str, Any]


def moment(value: datetime) -> str:
    """A moment as Horizon prints one: UTC, to the second, RFC 3339."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _link(target: str, **members: Any) -> Link:
    return {"href": target, **members}


def _rest(record: dict[str, Any], raw: Mapping[str, Any] | None) -> dict[str, Any]:
    """What only the stored answer knows: its members that are not set from the columns."""
    if not raw:
        return {}
    return {name: value for name, value in raw.items() if name != "_links" and name not in record}


# -- transactions --------------------------------------------------------------


def transaction_links(base: str, shown_hash: str, row: Row) -> Link:
    root = f"{base}/transactions/{shown_hash}"
    token = str(row.id)
    return {
        "self": _link(root),
        "account": _link(f"{base}/accounts/{row.source_account}"),
        "ledger": _link(f"{base}/ledgers/{row.ledger_sequence}"),
        "operations": _link(f"{root}/operations{{?cursor,limit,order}}", templated=True),
        "effects": _link(f"{root}/effects{{?cursor,limit,order}}", templated=True),
        "precedes": _link(f"{base}/transactions?order=asc&cursor={token}"),
        "succeeds": _link(f"{base}/transactions?order=desc&cursor={token}"),
        "transaction": _link(root),
    }


def transaction_record(base: str, row: Row, asked_hash: str) -> dict[str, Any]:
    """A transaction as it is answered for ``asked_hash``.

    A fee-bump transaction is found by either of its hashes, and upstream
    answers differently for each: asked by the inner hash, ``id``, ``hash`` and
    ``signatures`` are the inner ones; the outer hash is always in
    ``fee_bump_transaction.hash``. Asked by the outer hash, or without one (in
    a stream), they are the outer ones.
    """
    raw = row.raw if isinstance(row.raw, dict) else None
    fee_bump = row.inner_transaction_hash is not None
    by_inner = fee_bump and asked_hash == row.inner_transaction_hash
    shown_hash = row.inner_transaction_hash if by_inner else row.transaction_hash
    record: dict[str, Any] = {
        "_links": transaction_links(base, shown_hash, row),
        "id": shown_hash,
        "paging_token": str(row.id),
        "successful": bool(row.successful),
        "hash": shown_hash,
        "ledger": row.ledger_sequence,
        "created_at": moment(row.ledger_close_time),
        "source_account": row.source_account,
        "source_account_sequence": str(row.account_sequence),
        "fee_account": row.fee_source_account or row.source_account,
    }
    if row.fee_charged is not None:
        record["fee_charged"] = str(row.fee_charged)
    # REST `max_fee` is the fee of the fee-bump; the inner transaction's fee is its own.
    record["max_fee"] = str(row.new_max_fee if fee_bump else row.max_fee)
    record["operation_count"] = row.operation_count
    record["envelope_xdr"] = row.envelope_xdr
    record["result_xdr"] = row.result_xdr
    if row.fee_meta_xdr is not None:
        record["fee_meta_xdr"] = row.fee_meta_xdr
    if row.result_meta_xdr is not None:
        record["result_meta_xdr"] = row.result_meta_xdr
    record["memo_type"] = row.memo_type
    if row.memo_type != "none" and row.memo is not None:
        record["memo"] = row.memo
    record["signatures"] = list(row.inner_signatures or []) if by_inner else list(row.signatures or [])
    if row.source_account_muxed:
        record["source_account_muxed"] = row.source_account_muxed
    if row.fee_source_account_muxed:
        record["fee_account_muxed"] = row.fee_source_account_muxed
    if fee_bump:
        record["fee_bump_transaction"] = {"hash": row.transaction_hash, "signatures": list(row.signatures or [])}
        record["inner_transaction"] = {
            "hash": row.inner_transaction_hash,
            "signatures": list(row.inner_signatures or []),
            "max_fee": str(row.max_fee),
        }
    record.update(_rest(record, raw))
    return record


# -- operations ----------------------------------------------------------------


def operation_links(base: str, row: Row) -> Link:
    root = f"{base}/operations/{row.id}"
    return {
        "self": _link(root),
        "transaction": _link(f"{base}/transactions/{row.transaction_hash}"),
        "effects": _link(f"{root}/effects"),
        "succeeds": _link(f"{base}/effects?order=desc&cursor={row.id}"),
        "precedes": _link(f"{base}/effects?order=asc&cursor={row.id}"),
    }


def operation_record(base: str, row: Row, data_change: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """An operation. ``transaction_successful`` and ``transaction_hash`` are
    the transaction's: whether it went through, and which one it is.

    A type this code base does not know stays as it was stored: upstream
    reports it as ``type: "unknown"`` with its ``type_i``, and so does this.
    """
    raw = row.raw if isinstance(row.raw, dict) else None
    record: dict[str, Any] = {
        "_links": operation_links(base, row),
        "id": str(row.id),
        "paging_token": str(row.id),
        "transaction_successful": bool(row.transaction_successful),
        "source_account": row.source_account,
        "type": row.type,
        "type_i": row.type_i,
        "created_at": moment(row.ledger_close_time),
        "transaction_hash": row.transaction_hash,
    }
    if row.source_account_muxed:
        record["source_account_muxed"] = row.source_account_muxed
    record.update(_rest(record, raw))
    if data_change is not None:
        record["data_change"] = dict(data_change)
    return record


# -- effects -------------------------------------------------------------------


def effect_links(base: str, row: Row) -> Link:
    return {
        "operation": _link(f"{base}/operations/{row.operation_id}"),
        "succeeds": _link(f"{base}/effects?order=desc&cursor={row.paging_token}"),
        "precedes": _link(f"{base}/effects?order=asc&cursor={row.paging_token}"),
    }


def effect_record(base: str, row: Row) -> dict[str, Any]:
    raw = row.raw if isinstance(row.raw, dict) else None
    record: dict[str, Any] = {
        "_links": effect_links(base, row),
        "id": row.id,
        "paging_token": row.paging_token,
        "account": row.account_id,
        "type": row.type,
        "type_i": row.type_i,
        "created_at": moment(row.ledger_close_time),
    }
    if row.account_muxed:
        record["account_muxed"] = row.account_muxed
    record.update(_rest(record, raw))
    return record


# -- trades --------------------------------------------------------------------


def _side_link(base: str, account: str | None, pool: str | None) -> str:
    if pool:
        return f"{base}/liquidity_pools/{pool}"
    return f"{base}/accounts/{account}"


def trade_record(base: str, row: Row, *, reversed_side: bool = False) -> dict[str, Any]:
    """A trade, in the orientation it was stored in, or with base and counter
    exchanged when it is asked as the other way round of its market. The id
    and the cursor do not depend on the orientation.

    Built from the columns alone: every member of a trade is one.

    Upstream leaves the ``self`` link of a trade empty — it has no route for
    one trade — and so does this; ``operation`` leads to what made it.
    """
    base_amount, counter_amount = row.base_amount, row.counter_amount
    price_n, price_d = row.price_n, row.price_d
    base_account, counter_account = row.base_account_id, row.counter_account_id
    base_pool, counter_pool = row.base_liquidity_pool_id, row.counter_liquidity_pool_id
    base_offer, counter_offer = row.base_offer_id, row.counter_offer_id
    base_asset, counter_asset = row.base_asset_id, row.counter_asset_id
    base_is_seller = row.base_is_seller
    if reversed_side:
        base_amount, counter_amount = counter_amount, base_amount
        price_n, price_d = price_d, price_n
        base_account, counter_account = counter_account, base_account
        base_pool, counter_pool = counter_pool, base_pool
        base_offer, counter_offer = counter_offer, base_offer
        base_asset, counter_asset = counter_asset, base_asset
        base_is_seller = not base_is_seller
    record: dict[str, Any] = {
        "_links": {
            "self": _link(""),
            "base": _link(_side_link(base, base_account, base_pool)),
            "counter": _link(_side_link(base, counter_account, counter_pool)),
            "operation": _link(f"{base}/operations/{row.operation_id}"),
        },
        "id": row.id,
        "paging_token": row.id,
        "ledger_close_time": moment(row.ledger_close_time),
        "trade_type": row.trade_type,
    }
    if row.liquidity_pool_fee_bp is not None:
        record["liquidity_pool_fee_bp"] = row.liquidity_pool_fee_bp
    if base_offer is not None:
        record["base_offer_id"] = str(base_offer)
    if base_account is not None:
        record["base_account"] = base_account
    if base_pool is not None:
        record["base_liquidity_pool_id"] = base_pool
    record["base_amount"] = stroops_to_amount(base_amount)
    record.update(asset_params("base_", base_asset))
    if counter_offer is not None:
        record["counter_offer_id"] = str(counter_offer)
    if counter_account is not None:
        record["counter_account"] = counter_account
    if counter_pool is not None:
        record["counter_liquidity_pool_id"] = counter_pool
    record["counter_amount"] = stroops_to_amount(counter_amount)
    record.update(asset_params("counter_", counter_asset))
    record["base_is_seller"] = base_is_seller
    # 0/0 is how upstream reports a trade that has no stored price.
    record["price"] = {"n": str(price_n or 0), "d": str(price_d or 0)}
    return record
