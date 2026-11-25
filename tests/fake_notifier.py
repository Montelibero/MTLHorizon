"""What Operations Notifier posts, made the way it makes it and signed the
way it signs it (its code at cea7e51, ``logic/stream-processor.js`` and
``logic/notifier.js``).

One notification is one operation of a transaction of the fake Horizon. The
body is JSON without spaces, as ``JSON.stringify`` writes it, and the
signature is ed25519 over exactly those bytes.
"""

from __future__ import annotations

import base64
import json
import uuid
from typing import Any

from stellar_sdk import Keypair

from tests.fake_horizon import FakeHorizon

SUBSCRIPTION = "6870f5c2a1b3c4d5e6f70819"
OTHER_SUBSCRIPTION = "9a8b7c6d5e4f3a2b1c0d"


class FakeNotifier:
    def __init__(self) -> None:
        self.keypair = Keypair.random()
        self.deliveries: list[dict[str, Any]] = []

    @property
    def public_key(self) -> str:
        """What the Notifier's own /api/status shows as publicKey."""
        return self.keypair.public_key

    def post(self, payload: dict[str, Any]) -> tuple[bytes, dict[str, str]]:
        """The body and the headers it posts ``payload`` with."""
        self.deliveries.append(payload)
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return body, {
            "Content-Type": "application/json",
            "User-Agent": "StellarNotifier/0.12.0",
            "X-Requested-With": "StellarNotifier/0.12.0 (+https://github.com/Montelibero/operations-notifier)",
            "X-Request-ED25519-Signature": base64.b64encode(self.keypair.sign(body)).decode("ascii"),
            "X-Subscription": str(payload["subscription"]),
        }


def notifier_asset(record: Any, prefix: str = "") -> dict[str, Any]:
    """A Horizon-style asset of a record, as the Notifier writes it."""
    kind = record.get(f"{prefix}asset_type")
    if kind is None:
        return {}
    if kind == "native":
        return {"asset_type": 0}
    code = record[f"{prefix}asset_code"]
    return {"asset_type": 1 if len(code) <= 4 else 2, "asset_code": code, "asset_issuer": record[f"{prefix}asset_issuer"]}


def notification(
    fake: FakeHorizon, operation_id: str, *, notification_id: str | None = None, subscription: str = SUBSCRIPTION
) -> dict[str, Any]:
    """The notification of one operation of the fake's history."""
    operation = fake.ledger.operations[operation_id]
    transaction = fake.ledger.transactions[operation["transaction_hash"]]
    application_order = int(operation_id) & 0xFFF
    body: dict[str, Any] = {"type_i": operation["type_i"], "type": operation["type"], "account": operation["source_account"]}
    if operation["type"] == "payment":
        body.update(destination=operation["to"], asset=notifier_asset(operation), amount=operation["amount"])
    elif operation["type"] in ("manage_sell_offer", "manage_buy_offer"):
        body.update(
            asset=notifier_asset(operation, "buying_"),
            amount=operation["amount"],
            source_asset=notifier_asset(operation, "selling_"),
            price=operation["price"],
            offerId="0",
        )
        trades = _trades(fake, operation_id)
        if trades:
            body["trades"] = trades
    elif operation["type"] == "path_payment_strict_send":
        body.update(
            destination=operation["to"],
            asset=notifier_asset(operation),
            amount=operation["amount"],
            source_asset=notifier_asset(operation, "source_"),
            dest_min="0.0000000",
            path=[notifier_asset({"asset_type": "credit_alphanum4", "asset_code": "EURMTL", "asset_issuer": operation["path"][0]["asset_issuer"]})],
            trades=[{"type": "liquidity_pool", "pool_id": fake.ledger.trades and "ab12" or "", "asset_sold": {"asset_type": 0}, "amount_sold": "1.0", "asset_bought": {"asset_type": 0}, "amount_bought": "1.0"}],
        )
    elif operation["type"] == "manage_data":
        body.update(name=operation["name"], value=operation["value"])
    return {
        "id": notification_id or uuid.uuid4().hex,
        "subscription": subscription,
        "type": "operation",
        "created": "2026-11-25T12:00:00.000Z",
        "sent": "2026-11-25T12:00:01.000Z",
        "operation": {**body, "id": str(int(transaction["paging_token"]) + application_order)},
        "transaction": {
            "hash": transaction["hash"],
            "fee": "100",
            "fee_charged": transaction["fee_charged"],
            "max_fee": transaction["max_fee"],
            "source": transaction["source_account"],
            "paging_token": transaction["paging_token"],
            "source_account_sequence": transaction["source_account_sequence"],
            "created_at": transaction["created_at"],
            "memo": None,
            "result_xdr": transaction["result_xdr"],
            "envelope_xdr": transaction["envelope_xdr"],
        },
    }


def _trades(fake: FakeHorizon, operation_id: str) -> list[dict[str, Any]]:
    """The order book fills of an offer operation, as the Notifier decodes
    them from the result."""
    found = []
    for trade in fake.ledger.trades.values():
        if trade["paging_token"] != f"{operation_id}-0":
            continue
        seller = trade["base_account"] if trade["base_is_seller"] else trade["counter_account"]
        sold = notifier_asset(trade, "base_") if trade["base_is_seller"] else notifier_asset(trade, "counter_")
        bought = notifier_asset(trade, "counter_") if trade["base_is_seller"] else notifier_asset(trade, "base_")
        found.append(
            {
                "type": "order_book",
                "seller_id": seller,
                "offer_id": trade["counter_offer_id"],
                "asset_sold": sold,
                "amount_sold": trade["base_amount"],
                "asset_bought": bought,
                "amount_bought": trade["counter_amount"],
            }
        )
    return found
