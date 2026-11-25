"""Looking at a live Horizon server once and writing down what it says.

Every check stands on its own: a failing one is recorded with the answer the
server gave and the others still run. The result is a plain dict, printed or
saved by the caller.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from app.backfill.jobs import asset_params
from app.sources import horizon_records as records
from app.sources.horizon import HorizonClient, HorizonError
from app.sources.horizon_cursors import after_ledger, to_total_order_id

# A market as (base asset id, counter asset id).
Market = tuple[str, str]
DAY_MS = 86_400_000


def probe(
    client: HorizonClient,
    *,
    network_passphrase: str,
    market: Market | None = None,
    account: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    root = _attempt(client.root)
    result["server"] = _server(root, network_passphrase)
    if not isinstance(root, dict):
        return result
    elder, latest = int(root["history_elder_ledger"]), int(root["history_latest_ledger"])

    result["history"] = _history(client, root, elder, latest)
    probe_ledger = max(elder + 1, latest - 1000)

    trades = _trades(client, market)
    result["trades"] = trades
    sample = trades.get("first")
    result["chain"] = _chain(client, sample) if sample else {"skipped": "no trade to follow"}
    result["cursors"] = _cursors(client, market, account, probe_ledger, elder)
    result["transactions_page"] = _transactions_page(client, account)
    if account:
        result["account_streams"] = _account_streams(client, account)
    if market:
        result["trade_aggregations"] = _attempt(
            _aggregations, client, market, latest
        )
    return result


# -- parts ----------------------------------------------------------------------


def _server(root: Mapping[str, Any] | dict[str, Any], network_passphrase: str) -> dict[str, Any]:
    if "error" in root:
        return dict(root)
    facts = {
        name: root.get(name)
        for name in (
            "horizon_version",
            "core_version",
            "network_passphrase",
            "current_protocol_version",
            "supported_protocol_version",
            "core_supported_protocol_version",
        )
    }
    facts["network_is_the_configured_one"] = root.get("network_passphrase") == network_passphrase
    return facts


def _history(client: HorizonClient, root: Mapping[str, Any], elder: int, latest: int) -> dict[str, Any]:
    def ledger(sequence: int) -> dict[str, Any]:
        answer = _attempt(client.get, f"/ledgers/{sequence}")
        return _ledger_facts(answer) if isinstance(answer, dict) else dict(answer)

    facts: dict[str, Any] = {
        "history_elder_ledger": elder,
        "history_latest_ledger": latest,
        "history_latest_ledger_closed_at": root.get("history_latest_ledger_closed_at"),
        "ingest_latest_ledger": root.get("ingest_latest_ledger"),
        "core_latest_ledger": root.get("core_latest_ledger"),
        "ledgers_kept": latest - elder + 1,
        "elder": ledger(elder),
        "elder_plus_one": ledger(elder + 1),
        "latest": ledger(latest),
        "below_elder": _answer(client.get, f"/ledgers/{elder - 1}"),
    }
    try:
        started = datetime.fromisoformat(facts["elder"]["closed_at"])
        ended = datetime.fromisoformat(facts["latest"]["closed_at"])
        facts["days_kept"] = round((ended - started).total_seconds() / 86400, 2)
    except (KeyError, TypeError, ValueError):
        pass
    return facts


def _ledger_facts(ledger: Mapping[str, Any]) -> dict[str, Any]:
    facts = {
        name: ledger.get(name)
        for name in (
            "sequence",
            "paging_token",
            "closed_at",
            "hash",
            "prev_hash",
            "protocol_version",
            "successful_transaction_count",
            "failed_transaction_count",
            "operation_count",
            "tx_set_operation_count",
        )
    }
    facts["header_xdr_length"] = len(ledger.get("header_xdr", ""))
    return facts


def _trades(client: HorizonClient, market: Market | None) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    if market:
        filters = {**asset_params("base_", market[0]), **asset_params("counter_", market[1])}
    page = _attempt(
        client.page, "/trades", resource_type="trade", order="desc", limit=200, **filters
    )
    if not hasattr(page, "records"):
        return {"as_given": dict(page)}
    given = list(page.records)
    facts: dict[str, Any] = {
        "as_given": {"records": len(given), "has_next": not page.exhausted},
        "first": given[0] if given else None,
    }
    types: dict[str, int] = {}
    for trade in given:
        types[str(trade.get("trade_type"))] = types.get(str(trade.get("trade_type")), 0) + 1
    facts["trade_types"] = types
    if market:
        reversed_filters = {
            **asset_params("base_", market[1]),
            **asset_params("counter_", market[0]),
        }
        other = _attempt(
            client.page, "/trades", resource_type="trade", order="desc", limit=200, **reversed_filters
        )
        if hasattr(other, "records"):
            mirrored = {str(trade["id"]): trade for trade in other.records}
            facts["other_orientation"] = {
                "records": len(other.records),
                "same_ids": set(mirrored) == {str(trade["id"]) for trade in given},
                "reversed_presentation": _reversed_presentation(given[0], mirrored.get(str(given[0]["id"]))) if given else None,
            }
    return facts


def _reversed_presentation(trade: Mapping[str, Any], other: Mapping[str, Any] | None) -> bool | None:
    """Whether ``other`` is ``trade`` with base and counter exchanged."""
    if other is None:
        return None

    def side(record: Mapping[str, Any], prefix: str) -> tuple[Any, ...]:
        return (
            record.get(f"{prefix}asset_code"),
            record.get(f"{prefix}asset_issuer"),
            record.get(f"{prefix}amount"),
            record.get(f"{prefix}account"),
            record.get(f"{prefix}liquidity_pool_id"),
            record.get(f"{prefix}offer_id"),
        )

    price, other_price = trade.get("price") or {}, other.get("price") or {}
    return (
        side(other, "base_") == side(trade, "counter_")
        and side(other, "counter_") == side(trade, "base_")
        and other.get("base_is_seller") is (not trade.get("base_is_seller"))
        and (other_price.get("n"), other_price.get("d")) == (price.get("d"), price.get("n"))
    )


def _chain(client: HorizonClient, trade: Mapping[str, Any]) -> dict[str, Any]:
    """trade -> operation -> transaction -> ledger, and what hangs off them."""
    operation_id = str(trade["paging_token"]).split("-", 1)[0]
    operation = _attempt(client.get, f"/operations/{operation_id}")
    facts: dict[str, Any] = {"trade_id": trade.get("id"), "operation": _operation_facts(operation)}
    if not isinstance(operation, dict):
        return facts
    ledger_of_operation = int(operation_id) >> 32
    transaction_hash = str(operation.get("transaction_hash"))
    transaction = _attempt(client.get, f"/transactions/{transaction_hash}")
    if isinstance(transaction, dict):
        facts["transaction"] = {
            "hash": transaction.get("hash"),
            "paging_token": transaction.get("paging_token"),
            "ledger": transaction.get("ledger"),
            "ledger_matches_operation": transaction.get("ledger") == ledger_of_operation,
            "is_fee_bump": "fee_bump_transaction" in transaction,
            "has_result_meta_xdr": bool(transaction.get("result_meta_xdr")),
            "has_fee_meta_xdr": bool(transaction.get("fee_meta_xdr")),
        }
    transaction_id = int(operation_id) & ~0xFFF
    operations = _attempt(
        client.page,
        f"/transactions/{transaction_hash}/operations",
        resource_type="operation",
        order="asc",
        limit=200,
    )
    effects = _attempt(
        client.page, f"/transactions/{transaction_hash}/effects", resource_type="effect", order="asc", limit=200
    )
    stream = _attempt(client.page, "/trades", resource_type="trade", order="asc", cursor=str(transaction_id), limit=200)
    facts["context"] = {
        "operations": _count(operations),
        "effects": _count(effects),
        "trades_of_transaction": sum(
            1 for record in (stream.records if hasattr(stream, "records") else [])
            if transaction_id < int(str(record["paging_token"]).split("-", 1)[0]) < transaction_id + 4096
        ),
        "trades_route_under_transaction": _answer(client.get, f"/transactions/{transaction_hash}/trades"),
        "trades_route_under_operation": _answer(client.get, f"/operations/{operation_id}/trades"),
        "effects_route_under_operation": _count(
            _attempt(client.page, f"/operations/{operation_id}/effects", resource_type="effect", order="asc", limit=200)
        ),
    }
    facts["unknown_transaction"] = _answer(client.get, "/transactions/" + "0" * 64)
    return facts


def _operation_facts(operation: Mapping[str, Any] | dict[str, Any]) -> dict[str, Any]:
    if "error" in operation:
        return dict(operation)
    return {name: operation.get(name) for name in ("id", "type", "type_i", "transaction_hash", "source_account", "transaction_successful")}


def _cursors(client: HorizonClient, market: Market | None, account: str | None, ledger: int, elder: int) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    if market:
        filters = {**asset_params("base_", market[0]), **asset_params("counter_", market[1])}
    checks: dict[str, Any] = {
        "probe_ledger": ledger,
        "trades_desc_from_after_ledger": _ledgers(client, "/trades", "trade", "desc", str(after_ledger(ledger)), filters),
        "trades_desc_from_ledger_toid": _ledgers(client, "/trades", "trade", "desc", str(to_total_order_id(ledger)), filters),
        "trades_asc_from_ledger_toid": _ledgers(client, "/trades", "trade", "asc", str(to_total_order_id(ledger)), filters),
    }
    # An account's effects when one is given (a short stream, cheap to read), else everyone's.
    path = f"/accounts/{account}/effects" if account else "/effects"
    checks["effects_stream_read"] = path
    checks["effects_desc_from_ledger_toid"] = _ledgers(client, path, "effect", "desc", str(to_total_order_id(ledger)), {})
    checks["effects_cursor_at_elder"] = _answer(client.get, path, cursor=str(to_total_order_id(elder)), limit=1, order="desc")
    checks["effects_cursor_at_elder_plus_one"] = _answer(client.get, path, cursor=str(to_total_order_id(elder + 1)), limit=1, order="desc")
    checks["transactions_cursor_below_elder"] = _answer(client.get, "/transactions", cursor=str(to_total_order_id(elder - 1)), limit=1, order="desc")
    return checks


def _ledgers(client: HorizonClient, path: str, resource_type: str, order: str, cursor: str, filters: Mapping[str, Any]) -> dict[str, Any]:
    page = _attempt(client.page, path, resource_type=resource_type, order=order, cursor=cursor, limit=200, **filters)
    if not hasattr(page, "records"):
        return dict(page)
    ledgers = [records.ledger_of(record, resource_type) for record in page.records]
    probe = int(cursor.split("-", 1)[0]) >> 32
    return {
        "records": len(ledgers),
        "highest": max(ledgers, default=None),
        "lowest": min(ledgers, default=None),
        "all_at_or_below_ledger": all(value <= probe for value in ledgers),
        "all_below_ledger": all(value < probe for value in ledgers),
        "all_at_or_above_ledger": all(value >= probe for value in ledgers),
    }


def _transactions_page(client: HorizonClient, account: str | None) -> dict[str, Any]:
    if account:
        page = _attempt(client.page, f"/accounts/{account}/transactions", resource_type="transaction", order="desc", limit=200, include_failed="true")
    else:
        page = _attempt(client.page, "/transactions", resource_type="transaction", order="desc", limit=200, include_failed="true")
    if not hasattr(page, "records"):
        return dict(page)
    page_records = list(page.records)
    return {
        "records": len(page_records),
        "with_result_meta_xdr": sum(1 for t in page_records if t.get("result_meta_xdr")),
        "with_fee_meta_xdr": sum(1 for t in page_records if t.get("fee_meta_xdr")),
        "failed": sum(1 for t in page_records if t.get("successful") is False),
        "fee_bump": sum(1 for t in page_records if "fee_bump_transaction" in t),
        "muxed": sum(1 for t in page_records if any(key.endswith("_muxed") for key in t)),
        "oldest": page_records[-1].get("created_at") if page_records else None,
        "newest": page_records[0].get("created_at") if page_records else None,
    }


def _account_streams(client: HorizonClient, account: str) -> dict[str, Any]:
    streams: dict[str, Any] = {}
    answers: dict[str, Any] = {}
    for stream, resource_type, filters in (
        ("transactions", "transaction", {"include_failed": "true"}),
        ("operations", "operation", {"include_failed": "true"}),
        ("effects", "effect", {}),
        ("trades", "trade", {}),
    ):
        page = _attempt(
            client.page, f"/accounts/{account}/{stream}", resource_type=resource_type, order="desc", limit=5, **filters
        )
        answers[stream] = {"records": len(page.records)} if hasattr(page, "records") else dict(page)
    return answers


def _aggregations(client: HorizonClient, market: Market, latest_ledger: int) -> dict[str, Any]:
    ledger = client.get(f"/ledgers/{latest_ledger}")
    # Whole UTC days ending with the day after the latest ledger closed.
    end = int(datetime.fromisoformat(ledger["closed_at"]).timestamp() * 1000)
    end = end - end % DAY_MS + DAY_MS
    body = client.get(
        "/trade_aggregations",
        start_time=end - 3 * DAY_MS,
        end_time=end,
        resolution=86_400_000,
        limit=10,
        **asset_params("base_", market[0]),
        **asset_params("counter_", market[1]),
    )
    buckets = body.get("_embedded", {}).get("records", [])
    return {
        "buckets": len(buckets),
        "limit_sent": 10,
        "days": [
            {"timestamp": b.get("timestamp"), "trade_count": b.get("trade_count"), "base_volume": b.get("base_volume"), "counter_volume": b.get("counter_volume")}
            for b in buckets
        ],
        "links": sorted(body.get("_links", {})),
    }


# -- plumbing -------------------------------------------------------------------


def _attempt(function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run a request; an error becomes {"error": ..., "status": ...}."""
    try:
        return function(*args, **kwargs)
    except HorizonError as error:
        return _failure(error)


def _answer(function: Callable[..., Any], *args: Any, **kwargs: Any) -> dict[str, Any]:
    """Status and, for a collection, how many records came; never the records."""
    answer = _attempt(function, *args, **kwargs)
    if not isinstance(answer, Mapping):
        return {"status": 200}
    if "error" in answer:
        return dict(answer)
    records = answer.get("_embedded", {}).get("records")
    return {"status": 200, "records": len(records) if records is not None else None}


def _failure(error: HorizonError) -> dict[str, Any]:
    return {
        "status": getattr(error, "status", None),
        "error": type(error).__name__,
        "detail": str(error)[:300],
    }


def _count(page: Any) -> int | dict[str, Any]:
    return len(page.records) if hasattr(page, "records") else dict(page)
