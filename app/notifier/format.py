"""The shape of what Operations Notifier posts to a reaction URL, as read
here. This is the only place that knows that shape; the rest of the
application works with :class:`Notice`.

Studied in the code of ``Montelibero/operations-notifier`` at revision
``cea7e51`` (v0.12.0), which was amended into ``e4039fb`` (v0.12.0) while
stage 05 ran, and at ``0fb6050`` (v0.13.0), which came next. The webhook and
its signature are the same there as in ``a474e9c`` (v0.11.0), which was
studied first; since then only fields that may be absent were added. An
example is no proof of what an installed version does, so nothing here needs
a field to be there, and whatever cannot be read is named in
``Notice.problems`` instead of being invented.

What is posted, as a JSON object::

    {"id": <notification id>, "subscription": <subscription id>, "type": "operation",
     "created": <when the Notifier made it>, "sent": <when it was sent>,
     "operation": {...}, "transaction": {...}}

* One notification is one operation. The same operation is one notification
  for every subscription that matched it, and each subscription gets a POST
  of its own: ``id`` and ``subscription`` together tell deliveries apart.
* ``operation.id`` is the operation's total order id as a decimal string:
  the transaction's ``paging_token`` plus the operation's one-based place.
  ``operation.account`` is the base ``G…`` address of the operation's source
  (of the transaction's source when the operation has none).
* Assets are written with a number for their type — 0 native, 1 alphanum4,
  2 alphanum12 — not with Horizon's words. 3 is a share of a liquidity pool
  (since v0.13.0; before, it was written as XLM): it has no name here, and
  that is said in ``problems``. What is read instead is the pair of the
  pool's own assets, ``assets``, on an operation made with the pool. A trade
  of a pool names the pool's assets itself, in ``asset_sold`` and
  ``asset_bought``.
* Next to the base ``G…`` address of an account the Notifier may write its
  muxed form, ``<field>_muxed`` and ``<field>_muxed_id`` (``account``,
  ``from``, ``trustor``, the transaction's ``source``). The base address is
  what is read.
* ``transaction`` carries ``hash``, ``paging_token``, ``source``,
  ``created_at``, ``result_xdr`` and ``envelope_xdr``. Of a fee-bump
  transaction ``hash`` is the envelope's hash, as in Horizon, and ``source``
  the inner transaction's source.
* A success is any answer 2xx. Anything else, and a timeout, is sent again
  after a pause that grows with the cube of the failures, until the
  notification is a week old or has failed a hundred times; then it is
  dropped, and after ten dropped in a row the subscription is disabled.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

G_ADDRESS = re.compile(r"^G[A-Z0-9]{55}$")
HASH = re.compile(r"^[0-9a-fA-F]{64}$")
OPERATION = "operation"

# Operation types by the number the Notifier gives them, which is Stellar's.
# For these this reader knows every field that names an account, an asset or
# a trade; the order is the numbering.
TYPES = (
    "create_account payment path_payment_strict_receive manage_sell_offer create_passive_sell_offer set_options "
    "change_trust allow_trust account_merge inflation manage_data bump_sequence manage_buy_offer "
    "path_payment_strict_send create_claimable_balance claim_claimable_balance begin_sponsoring_future_reserves "
    "end_sponsoring_future_reserves revoke_sponsorship clawback clawback_claimable_balance set_trustline_flags "
    "liquidity_pool_deposit liquidity_pool_withdraw invoke_host_function extend_footprint_ttl restore_footprint"
).split()
NUMBERED = dict(enumerate(TYPES))

# Fields that name an account other than by being the operation's source.
ACCOUNT_FIELDS = ("destination", "trustor", "from", "sponsoredId")
# The type number of a share of a liquidity pool.
POOL_SHARE = 3
ASSET_FIELDS = ("asset", "source_asset")


class Unreadable(ValueError):
    """What was posted cannot be a notification: there is no telling one
    delivery from another, so nothing can be stored of it."""


@dataclass(frozen=True)
class Notice:
    """One delivery, read.

    ``told`` says whether this reader could name every account, asset and
    trade of the operation: then it can say whether a watch rule matches.
    When it could not — a type it does not know, a field it cannot read —
    the notice is not dropped for that; it is kept, and why it could not be
    told is in ``problems``.
    """

    id: str
    subscription: str
    type: str
    created: datetime | None
    sent: datetime | None
    operation: Mapping[str, Any]
    transaction: Mapping[str, Any]
    # The body as it came, read as JSON and not touched.
    raw: Mapping[str, Any] = field(default_factory=dict)
    operation_id: int | None = None
    operation_type: str | None = None
    transaction_hash: str | None = None
    transaction_toid: int | None = None
    ledger: int | None = None
    accounts: frozenset[str] = frozenset()
    assets: frozenset[str] = frozenset()
    markets: frozenset[tuple[str, str]] = frozenset()
    problems: tuple[str, ...] = ()

    @property
    def told(self) -> bool:
        """Whether the operation is of a known type and everything in it was read."""
        return self.type == OPERATION and self.operation_type in TYPES and not self.problems

    def names(self) -> dict[str, Any]:
        """What the operation names, for a diagnosis."""
        return {
            "accounts": sorted(self.accounts),
            "assets": sorted(self.assets),
            "markets": sorted(" / ".join(pair) for pair in self.markets),
            "operation": {"id": self.operation_id, "type": self.operation_type},
        }


def read(payload: Any) -> Notice:
    """Read a posted body. ``Unreadable`` when it cannot be told what
    delivery it is; anything else is kept and named in ``problems``."""
    if not isinstance(payload, Mapping):
        raise Unreadable("the body is not a JSON object")
    notification_id, subscription = _key(payload, "id"), _key(payload, "subscription")
    operation, transaction = payload.get("operation"), payload.get("transaction")
    notice = Notice(
        id=notification_id,
        subscription=subscription,
        type=str(payload.get("type") or ""),
        created=_moment(payload.get("created")),
        sent=_moment(payload.get("sent")),
        operation=operation if isinstance(operation, Mapping) else {},
        transaction=transaction if isinstance(transaction, Mapping) else {},
    )
    if not isinstance(operation, Mapping):
        return _replace(notice, problems=("there is no operation in it",))
    problems: list[str] = []
    if notice.type != OPERATION:
        problems.append(f"notification type {notice.type!r} is not one this reader knows")

    operation_id, operation_type = _operation(notice.operation, problems)
    transaction_hash, transaction_toid = _transaction(notice.transaction, problems)
    ledger = None
    if transaction_toid is not None and operation_id is not None and transaction_toid >> 32 != operation_id >> 32:
        problems.append(f"the operation lies in ledger {operation_id >> 32}, the transaction in ledger {transaction_toid >> 32}")
    elif transaction_toid is not None:
        ledger = transaction_toid >> 32
    elif operation_id is not None:
        ledger = operation_id >> 32
    accounts, assets, markets = _names(notice.operation, problems)
    return _replace(
        notice,
        raw=dict(payload),
        operation_id=operation_id,
        operation_type=operation_type,
        transaction_hash=transaction_hash,
        transaction_toid=transaction_toid,
        ledger=ledger,
        accounts=accounts,
        assets=assets,
        markets=markets,
        problems=tuple(problems),
    )


def _replace(notice: Notice, **changes: Any) -> Notice:
    return Notice(**{**notice.__dict__, **changes})


def _key(payload: Mapping[str, Any], name: str) -> str:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, (str, int)) or not str(value).strip():
        raise Unreadable(f"there is no {name} in it")
    return str(value).strip()


def _moment(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:  # the Notifier writes UTC; a moment without one is read as UTC
        moment = moment.replace(tzinfo=UTC)
    return moment


def _operation(operation: Mapping[str, Any], problems: list[str]) -> tuple[int | None, str | None]:
    """The operation's total order id and its type; a problem for whatever of
    the two is not there or does not hold together."""
    raw_id = str(operation.get("id", ""))
    if raw_id.isdigit() and len(raw_id) <= 19:
        operation_id = int(raw_id)
        if operation_id >> 32 == 0 or operation_id & 0xFFF == 0:
            problems.append(f"operation id {raw_id} is not an operation's total order id")
            operation_id = None
    else:
        operation_id = None
        problems.append("the operation has no id that is a total order id")
    number, kind = operation.get("type_i"), operation.get("type")
    if not isinstance(kind, str) or not kind:
        return operation_id, None
    if isinstance(number, int) and not isinstance(number, bool) and 0 <= number < len(TYPES):
        if kind != NUMBERED[number]:
            problems.append(f"operation type {kind!r} does not go with its number {number}")
    elif kind not in TYPES:
        problems.append(f"operation type {kind!r} is not one this reader knows the fields of")
    return operation_id, kind


def _transaction(transaction: Mapping[str, Any], problems: list[str]) -> tuple[str | None, int | None]:
    raw_hash = transaction.get("hash")
    transaction_hash = None
    if isinstance(raw_hash, str) and HASH.match(raw_hash):
        transaction_hash = raw_hash.lower()
    else:
        problems.append("the transaction has no hash")
    token = str(transaction.get("paging_token", ""))
    transaction_toid = None
    if token.isdigit() and len(token) <= 19 and int(token) > 0:
        transaction_toid = int(token)
    else:
        problems.append("the transaction has no paging_token that is a total order id")
    return transaction_hash, transaction_toid


def _names(operation: Mapping[str, Any], problems: list[str]) -> tuple[frozenset[str], frozenset[str], frozenset[tuple[str, str]]]:
    """The accounts, the assets and the markets the operation names."""
    accounts: set[str] = set()
    for name in ("account", *ACCOUNT_FIELDS):
        value = operation.get(name)
        if value is None:
            continue
        if isinstance(value, str) and G_ADDRESS.match(value):
            accounts.add(value)
        else:
            problems.append(f"{name} is not an account address: {value!r}")
    if operation.get("type_i") == 10 and isinstance(operation.get("value"), str) and G_ADDRESS.match(operation["value"]):
        accounts.add(operation["value"])  # a data entry that names an account
    assets: set[str] = set()
    for name in ASSET_FIELDS:
        value = operation.get(name)
        if value is None:
            continue
        found = asset_id(value)
        if found is None:
            problems.append(_not_an_asset(name, value))
        else:
            assets.add(found)
    # Since v0.13.0: the pair of assets of the pool an operation is made with.
    for value in _list(operation.get("assets"), "assets", problems):
        found = asset_id(value)
        if found is None:
            problems.append(_not_an_asset("assets", value))
        else:
            assets.add(found)
    for value in _list(operation.get("path"), "path", problems):
        found = asset_id(value)
        if found is None:
            problems.append(f"path holds what is not an asset: {value!r}")
        else:
            assets.add(found)
    for claimant in _list(operation.get("claimants"), "claimants", problems):
        if isinstance(claimant, Mapping):
            destination = claimant.get("destination")
            if isinstance(destination, str) and G_ADDRESS.match(destination):
                accounts.add(destination)
            else:
                problems.append(f"a claimant names no account: {destination!r}")
            found = asset_id(claimant.get("asset"))
            if found is None:
                problems.append("a claimant names no asset that can be read")
            else:
                assets.add(found)
    markets: set[tuple[str, str]] = set()
    for trade in _list(operation.get("trades"), "trades", problems):
        if not isinstance(trade, Mapping):
            continue
        seller = trade.get("seller_id")
        if seller is not None:
            if isinstance(seller, str) and G_ADDRESS.match(seller):
                accounts.add(seller)  # an offer of this account was filled by the transaction
            else:
                problems.append(f"a trade names a seller that is not an account: {seller!r}")
        sold, bought = asset_id(trade.get("asset_sold")), asset_id(trade.get("asset_bought"))
        if sold is None or bought is None:
            problems.append("a trade names assets that cannot be read")
            continue
        assets.update((sold, bought))
        markets.add((sold, bought))
    return frozenset(accounts), frozenset(assets), frozenset(markets)


def _not_an_asset(name: str, value: Any) -> str:
    """What is wrong with ``value`` in the field ``name``: a pool share is an
    asset this reader knows and has no name for; anything else is not written
    as the Notifier writes assets."""
    if is_pool_share(value):
        return f"{name} is a pool share, which has no name here"
    return f"{name} is not an asset as the Notifier writes them: {value!r}"


def _list(value: Any, name: str, problems: list[str]) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        problems.append(f"{name} is not a list")
        return []
    return value


def is_pool_share(asset: Any) -> bool:
    """Whether ``asset`` is a share of a liquidity pool as the Notifier has
    written it since v0.13.0."""
    return isinstance(asset, Mapping) and asset.get("asset_type") == POOL_SHARE


def asset_id(asset: Any) -> str | None:
    """``native``, ``CODE:ISSUER``, or None when it is not an asset as the
    Notifier writes them."""
    if not isinstance(asset, Mapping):
        return None
    kind, code, issuer = asset.get("asset_type"), asset.get("asset_code"), asset.get("asset_issuer")
    if kind == 0 and not code and not issuer:
        return "native"
    if kind in (1, 2) and isinstance(code, str) and 1 <= len(code) <= 12 and code.isalnum() and isinstance(issuer, str):
        return f"{code}:{issuer}" if G_ADDRESS.match(issuer) else None
    return None
