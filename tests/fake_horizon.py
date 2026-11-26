"""A small Horizon inside the test process, answering through httpx.MockTransport.

It is built from a synthetic history of seven transactions and behaves like
the public server where the backfill depends on it:

* `_links.next` is attached to every page, the last one included, and an
  empty page repeats the cursor;
* a cursor at or below the oldest ledger kept gets 410 ``before_history``
  when reading backwards; reading forwards starts at that ledger instead;
* a market asked in the other orientation returns the same trades with base
  and counter exchanged;
* there is no ``/transactions/{hash}/trades`` and no ``/operations/{id}/trades``;
* transaction meta is served only where a transaction says so;
* ledgers keep closing: ``close`` adds transactions and moves the latest
  ledger, as the collector meets it.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from fractions import Fraction
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit

import httpx

from app.config import PUBLIC_NETWORK_PASSPHRASE
from app.sources.horizon import HorizonClient, HorizonSettings
from app.sources.horizon_format import amount_to_stroops, stroops_to_amount
from tests.factories import address

ORIGIN = "https://horizon.test"
ISSUER, A, B, C, D, S = (address(1), address(2), address(3), address(4), address(5), address(6))
EUR, RECT, MTL = f"EURMTL:{ISSUER}", f"MTLRECT:{ISSUER}", f"MTL:{ISSUER}"
POOL = "1b492b669959d3f082b5fb7dcc847d43371fd1f586209899603b93ac1a39b7f8"
ELDER, LATEST = 100, 130
T0 = datetime(2026, 1, 1, tzinfo=UTC)
INF = float("inf")

DEBIT, CREDIT, TRADE, POOL_TRADE, DATA_CREATED = 3, 2, 33, 92, 40
TRUSTLINE_CREATED, TRUSTLINE_UPDATED, TRUSTLINE_REMOVED = 12, 13, 14
MARKET = (EUR, RECT)

# Collection name -> the kind of record in it, which is also how its cursor is read.
KINDS = {"transactions": "transaction", "operations": "operation", "effects": "effect", "trades": "trade"}


def close_time(ledger: int) -> datetime:
    return T0 + timedelta(seconds=5 * (ledger - ELDER))


def iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def asset(prefix: str, asset_id: str) -> dict[str, str]:
    if asset_id == "native":
        return {f"{prefix}asset_type": "native"}
    code, issuer = asset_id.split(":", 1)
    kind = "credit_alphanum4" if len(code) <= 4 else "credit_alphanum12"
    return {f"{prefix}asset_type": kind, f"{prefix}asset_code": code, f"{prefix}asset_issuer": issuer}


def amount(stroops: int) -> str:
    return f"{stroops // 10**7}.{stroops % 10**7:07d}"


def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


@dataclass
class Operation:
    type: str
    type_i: int
    source: str | None = None  # None: the transaction's source
    body: dict[str, Any] = field(default_factory=dict)
    effects: list[dict[str, Any]] = field(default_factory=list)
    trades: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class Transaction:
    ledger: int
    order: int
    source: str
    operations: list[Operation]
    participants: set[str]  # as Horizon determines them
    operation_participants: dict[int, set[str]]  # operation number (from 1) -> accounts
    successful: bool = True
    fee_source: str | None = None  # set: the transaction is a fee bump
    with_meta: bool = False

    @property
    def id(self) -> int:
        return (self.ledger << 32) | (self.order << 12)

    @property
    def hash(self) -> str:
        return digest(f"outer {self.ledger}:{self.order}")

    @property
    def inner_hash(self) -> str:
        return digest(f"inner {self.ledger}:{self.order}")


def _payment(to: str, stroops: int, asset_id: str = EUR) -> Operation:
    return Operation(
        "payment",
        1,
        None,
        {"to": to, "amount": amount(stroops), **asset("", asset_id)},
        effects=[
            {"account": to, "type": "account_credited", "type_i": CREDIT, "amount": amount(stroops), **asset("", asset_id)},
            {"account": None, "type": "account_debited", "type_i": DEBIT, "amount": amount(stroops), **asset("", asset_id)},
        ],
    )


def history() -> list[Transaction]:
    """Seven transactions: a payment, a data entry, a fee-bump path payment
    with a pool trade and a market trade, a failed one, one with an operation
    type nobody knows, a payment to a tracked account, and a market trade
    between two accounts that are not tracked."""
    return [
        Transaction(100, 0, A, [_payment(B, 50_000_000)], {A, B}, {1: {A, B}}),
        Transaction(
            100, 3, C,
            [Operation("manage_data", 10, None, {"name": "config", "value": "aGVsbG8="},
                       effects=[{"account": C, "type": "data_created", "type_i": DATA_CREATED, "name": "config", "value": "aGVsbG8="}])],
            {C}, {1: {C}},
        ),
        Transaction(
            104, 1, B,
            [Operation(
                "path_payment_strict_send", 13, None,
                {"to": B, "amount": amount(9830482596), **asset("", RECT), "path": [asset("", EUR)], **asset("source_", MTL)},
                effects=[
                    {"account": B, "type": "account_credited", "type_i": CREDIT, "amount": amount(9830482596), **asset("", RECT)},
                    {"account": B, "type": "account_debited", "type_i": DEBIT, "amount": amount(7126809730), **asset("", MTL)},
                    {"account": B, "type": "liquidity_pool_trade", "type_i": POOL_TRADE, "sold": amount(7126809730), "bought": amount(49128652874)},
                    {"account": B, "type": "trade", "type_i": TRADE, "offer_id": "1867248625", "sold_amount": amount(231829549), "bought_amount": amount(49325436)},
                    {"account": C, "type": "trade", "type_i": TRADE, "offer_id": "1867248625", "sold_amount": amount(49325436), "bought_amount": amount(231829549)},
                ],
                trades=[
                    {"base": EUR, "counter": MTL, "base_amount": 49128652874, "counter_amount": 7126809730,
                     "base_pool": POOL, "counter_account": B, "seller": True, "price": (7126809730, 49128652874),
                     "kind": "liquidity_pool", "fee_bp": 30, "counter_offer_id": 4893653146851139585},
                    {"base": EUR, "counter": RECT, "base_amount": 231829549, "counter_amount": 49325436,
                     "base_account": B, "counter_account": C, "seller": False, "price": (10, 47),
                     "kind": "orderbook", "base_offer_id": 4893653146851139585, "counter_offer_id": 1867248625},
                ],
            )],
            {B, S, C}, {1: {B}}, fee_source=S,
        ),
        Transaction(108, 0, A, [Operation("manage_data", 10, None, {"name": "gone", "value": ""})], {A}, {1: {A}}, successful=False),
        Transaction(110, 2, A, [Operation("unknown", 99), _payment(C, 10_000_000)], {A, C}, {1: {A}, 2: {A, C}}, with_meta=True),
        Transaction(118, 0, C, [_payment(A, 20_000_000)], {C, A}, {1: {C, A}}),
        Transaction(
            112, 0, C,
            [Operation(
                "manage_sell_offer", 3, None,
                {"amount": amount(49325436), "price": "4.7000000", "price_r": {"n": 47, "d": 10}, **asset("buying_", EUR), **asset("selling_", RECT)},
                effects=[{"account": C, "type": "trade", "type_i": TRADE, "offer_id": "1804182570",
                          "sold_amount": amount(49325436), "bought_amount": amount(231829549)}],
                trades=[{"base": RECT, "counter": EUR, "base_amount": 49325436, "counter_amount": 231829549,
                         "base_account": C, "counter_account": D, "seller": True, "price": (47, 10),
                         "kind": "orderbook", "base_offer_id": 1804182570, "counter_offer_id": 112}],
            )],
            {C, D}, {1: {C}},
        ),
    ]


def trust(account: str, asset_id: str, *, limit: int = 1000, kind: str = "created") -> Operation:
    """``account`` opens a trustline to ``asset_id`` (or changes its limit, or
    closes it: ``kind`` "updated", "removed" with a zero limit)."""
    effect = {"created": "trustline_created", "updated": "trustline_updated", "removed": "trustline_removed"}[kind]
    stroops = 0 if kind == "removed" else limit * 10**7
    return Operation(
        "change_trust", 6, None,
        {"limit": amount(stroops), **asset("", asset_id)},
        effects=[{"account": account, "type": effect, "type_i": {"created": TRUSTLINE_CREATED, "updated": TRUSTLINE_UPDATED, "removed": TRUSTLINE_REMOVED}[kind],
                  "limit": amount(stroops), **asset("", asset_id)}],
    )


def paid(ledger: int, source: str, *to: str) -> Transaction:
    """A transaction of ``source`` paying each account of ``to``."""
    return Transaction(
        ledger,
        0,
        source,
        [_payment(account, 1_000_000) for account in to],
        {source, *to},
        {number: {source, account} for number, account in enumerate(to, start=1)},
    )


def sale(ledger: int, *, offer_owner: str | None = None) -> Transaction:
    """S sells RECT for EUR on the market, as the history does at ledger 112.

    With ``offer_owner`` the offer belongs to another account: that account
    is a side of the trade without being anywhere in the operation — a
    passive offer taken by somebody else's operation.
    """
    base_account = offer_owner or S
    operation = Operation(
        "manage_sell_offer",
        3,
        None,
        {
            "amount": amount(49325436),
            "price": "4.7000000",
            "price_r": {"n": 47, "d": 10},
            **asset("buying_", EUR),
            **asset("selling_", RECT),
        },
        effects=[
            {
                "account": S,
                "type": "trade",
                "type_i": TRADE,
                "offer_id": "1804182570",
                "sold_amount": amount(49325436),
                "bought_amount": amount(231829549),
            }
        ],
        trades=[
            {
                "base": RECT,
                "counter": EUR,
                "base_amount": 49325436,
                "counter_amount": 231829549,
                "base_account": base_account,
                "counter_account": S if offer_owner else D,
                "seller": True,
                "price": (47, 10),
                "kind": "orderbook",
                "base_offer_id": 1804182570,
                "counter_offer_id": ledger,
            }
        ],
    )
    participants = {S} if offer_owner else {S, D}
    return Transaction(ledger, 0, S, [operation], participants, {1: {S}})


def _transaction_record(tx: Transaction) -> dict[str, Any]:
    record: dict[str, Any] = {
        "id": tx.hash,
        "paging_token": str(tx.id),
        "successful": tx.successful,
        "hash": tx.hash,
        "ledger": tx.ledger,
        "created_at": iso(close_time(tx.ledger)),
        "source_account": tx.source,
        "source_account_sequence": str(1000 + tx.ledger),
        "fee_account": tx.fee_source or tx.source,
        "fee_charged": "200" if tx.fee_source else "100",
        "max_fee": "10000",
        "operation_count": len(tx.operations),
        "envelope_xdr": "AAAA" + tx.hash[:20],
        "result_xdr": "AAAAAAAAAGQ=",
        "fee_meta_xdr": "AAAAAg==",
        "memo_type": "none",
        "signatures": ["sig-of-" + tx.source[:6]],
    }
    if tx.with_meta:
        record["result_meta_xdr"] = "AAAAAA=="
    if tx.fee_source:
        record.update(
            max_fee="20000",
            fee_bump_transaction={"hash": tx.hash, "signatures": ["sig-of-" + tx.fee_source[:6]]},
            inner_transaction={"hash": tx.inner_hash, "signatures": ["inner"], "max_fee": "10000"},
        )
    return record


def _trade_record(ledger: int, operation_id: int, order: int, trade: dict[str, Any]) -> dict[str, Any]:
    record: dict[str, Any] = {
        "id": f"{operation_id}-{order}",
        "paging_token": f"{operation_id}-{order}",
        "ledger_close_time": iso(close_time(ledger)),
        "base_amount": amount(trade["base_amount"]),
        "counter_amount": amount(trade["counter_amount"]),
        "base_is_seller": trade["seller"],
        "price": {"n": str(trade["price"][0]), "d": str(trade["price"][1])},
        "trade_type": trade["kind"],
        **asset("base_", trade["base"]),
        **asset("counter_", trade["counter"]),
    }
    for side in ("base", "counter"):
        if f"{side}_account" in trade:
            record[f"{side}_account"] = trade[f"{side}_account"]
        elif f"{side}_pool" in trade:
            record[f"{side}_liquidity_pool_id"] = trade[f"{side}_pool"]
        if f"{side}_offer_id" in trade:
            record[f"{side}_offer_id"] = trade[f"{side}_offer_id"]
    if "fee_bp" in trade:
        record["liquidity_pool_fee_bp"] = trade["fee_bp"]
    return record


def reversed_trade(trade: Mapping[str, Any]) -> dict[str, Any]:
    """The same trade with base and counter exchanged: how Horizon presents
    a market asked in the other orientation."""
    other = dict(trade)
    for name in ("asset_type", "asset_code", "asset_issuer", "amount", "account", "liquidity_pool_id", "offer_id"):
        base, counter = f"base_{name}", f"counter_{name}"
        for target, source in ((base, counter), (counter, base)):
            other[target] = trade.get(source)
            if other[target] is None:
                del other[target]
    other["base_is_seller"] = not trade["base_is_seller"]
    other["price"] = {"n": trade["price"]["d"], "d": trade["price"]["n"]}
    return other


class Ledger:
    """The records of the history, by what they are asked by."""

    def __init__(self, transactions: list[Transaction]) -> None:
        self.ledgers: dict[int, dict[str, Any]] = {}
        self.transactions: dict[str, dict[str, Any]] = {}  # by hash
        self.by_inner_hash: dict[str, dict[str, Any]] = {}
        self.operations: dict[str, dict[str, Any]] = {}
        self.effects: dict[str, dict[str, Any]] = {}
        self.trades: dict[str, dict[str, Any]] = {}
        self.account_transactions: dict[str, list[dict[str, Any]]] = {}
        self.account_operations: dict[str, list[dict[str, Any]]] = {}
        self.account_effects: dict[str, list[dict[str, Any]]] = {}
        self.account_trades: dict[str, list[dict[str, Any]]] = {}
        self.of_transaction: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for tx in transactions:
            record = _transaction_record(tx)
            self.transactions[tx.hash] = record
            self.by_inner_hash[tx.inner_hash] = record
            self.ledgers[tx.ledger] = {
                "id": digest(f"ledger {tx.ledger}"), "paging_token": str(tx.ledger << 32), "sequence": tx.ledger,
                "closed_at": iso(close_time(tx.ledger)), "hash": digest(f"ledger {tx.ledger}"),
                "prev_hash": digest(f"ledger {tx.ledger - 1}"),
                "successful_transaction_count": 0, "failed_transaction_count": 0, "operation_count": 0,
                "protocol_version": 30, "base_fee_in_stroops": 100, "base_reserve_in_stroops": 5000000,
                "max_tx_set_size": 1000, "total_coins": "100000000000.0000000", "fee_pool": "0.0000000",
                "header_xdr": "AAAA",
            }
            ledger = self.ledgers[tx.ledger]
            ledger["successful_transaction_count" if tx.successful else "failed_transaction_count"] += 1
            ledger["operation_count"] += len(tx.operations)
            own = {"operations": [], "effects": [], "trades": []}
            self.of_transaction[tx.hash] = own
            for account in tx.participants:
                self.account_transactions.setdefault(account, []).append(record)
            for number, operation in enumerate(tx.operations, start=1):
                operation_id = tx.id + number
                operation_record = {
                    "id": str(operation_id), "paging_token": str(operation_id),
                    "transaction_successful": tx.successful,
                    "source_account": operation.source or tx.source,
                    "type": operation.type, "type_i": operation.type_i,
                    "created_at": iso(close_time(tx.ledger)), "transaction_hash": tx.hash, **operation.body,
                }
                self.operations[str(operation_id)] = operation_record
                own["operations"].append(operation_record)
                for account in tx.operation_participants[number]:
                    self.account_operations.setdefault(account, []).append(operation_record)
                if not tx.successful:
                    continue  # a failed transaction has neither effects nor trades
                for index, effect in enumerate(operation.effects, start=1):
                    token = f"{operation_id}-{index}"
                    effect_record = {
                        "id": f"{operation_id:019d}-{index:010d}", "paging_token": token,
                        "account": effect["account"] or tx.source,
                        "type": effect["type"], "type_i": effect["type_i"], "created_at": iso(close_time(tx.ledger)),
                        **{k: v for k, v in effect.items() if k not in ("account", "type", "type_i")},
                    }
                    self.effects[token] = effect_record
                    own["effects"].append(effect_record)
                    self.account_effects.setdefault(effect_record["account"], []).append(effect_record)
                for index, trade in enumerate(operation.trades):
                    trade_record = _trade_record(tx.ledger, operation_id, index, trade)
                    self.trades[trade_record["id"]] = trade_record
                    own["trades"].append(trade_record)
                    for side in ("base_account", "counter_account"):
                        if side in trade_record:
                            self.account_trades.setdefault(trade_record[side], []).append(trade_record)


class _Problem(Exception):
    def __init__(self, status: int) -> None:
        self.status = status


@dataclass
class Fault:
    path_part: str
    status: int
    times: int = 1
    retry_after: str | None = None


def serve(monkeypatch: Any, *fakes: "FakeHorizon", rate: float = 1000) -> None:
    """Make the command lines read these fake servers, as a process would read
    real ones: the addresses from the environment, the transports from here.
    A command then builds its pool over them."""
    from app import cli_common
    from app.config import shown_url

    monkeypatch.setenv("MTL_HORIZON_URLS", ",".join(fake.origin for fake in fakes))
    monkeypatch.delenv("MTL_HORIZON_URL", raising=False)  # the single address of the earlier stages, if the environment has it
    monkeypatch.setenv("MTL_HORIZON_REQUESTS_PER_SECOND", str(rate))
    monkeypatch.setattr(
        cli_common,
        "TRANSPORTS",
        {shown_url(fake.origin): httpx.MockTransport(fake._handle) for fake in fakes},
    )


def key_of(record: Mapping[str, Any], kind: str) -> tuple[float, ...]:
    """Sort key of a record: its position in the total order."""
    first, _, second = str(record["paging_token"]).partition("-")
    return (int(first), int(second)) if second else (int(first), 0.0)


def position_of(cursor: str, kind: str) -> tuple[float, ...]:
    """Sort key of a cursor, of the same shape as ``key_of`` returns for a
    record. A pair cursor given as one number has no order part."""
    first, _, second = cursor.partition("-")
    if kind in ("effect", "trade"):
        return (int(first), int(second) if second else INF)
    return (int(first), 0.0)


def token_of(record: Mapping[str, Any]) -> str:
    return str(record["paging_token"])


def _asset_place(record: Mapping[str, Any]) -> tuple[str, str]:
    """Where an asset stands in `/assets`: by its code, then its issuer."""
    return (record["asset_code"], record["asset_issuer"])


class FakeHorizon:
    def __init__(
        self,
        *,
        elder: int = ELDER,
        latest: int = LATEST,
        network: str = PUBLIC_NETWORK_PASSPHRASE,
        origin: str = ORIGIN,
        prefix: str = "",
        down: Exception | None = None,
        stall: float = 0.0,
    ) -> None:
        self.history: list[Transaction] = history()
        self.ledger = Ledger(self.history)
        self.elder = elder
        self.latest = latest
        self.network = network
        # Where this server lives: its pages' links name it, as a real one's do.
        self.origin = origin
        # The path its address carries (a gateway is named with its key in
        # the path): requests come in under it, and its links name it too.
        self.prefix = prefix.rstrip("/")
        # Every request fails with this exception: the server is unreachable.
        self.down = down
        # Every request takes this long to answer (seconds, really slept).
        self.stall = stall
        # Sent with every answer, as a server behind a limiter sends its own.
        self.headers: dict[str, str] = {}
        # The trustlines the network has now: asset id -> account -> balance
        # in stroops. A line with no balance is a line all the same.
        self.trustlines: dict[str, dict[str, int]] = {}
        # The assets the network has: issuer -> the codes it issued, as
        # `/assets?asset_issuer=` lists them.
        self.issued: dict[str, set[str]] = {}
        self.requests: list[tuple[str, dict[str, str]]] = []
        self.faults: list[Fault] = []
        self.hidden_operations: set[str] = set()  # /operations/{id} answers 404
        self.hidden_transactions: set[str] = set()  # /transactions/{hash} answers 404
        self.gone: set[str] = set()  # record ids left out of the answers
        self.tamper: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None
        self.before_answer: Callable[[str], None] | None = None  # runs before a request is answered
        self.root_down = False  # the root answers 503: the source is out of service
        self.client = self._new_client()

    # -- what tests arrange ---------------------------------------------------

    def reopen(self) -> HorizonClient:
        """A new client on this fake: a command closes the client it is given."""
        self.client = self._new_client()
        return self.client

    def _new_client(self) -> HorizonClient:
        return HorizonClient(
            HorizonSettings(self.origin, requests_per_second=1000, max_attempts=4, backoff_base_seconds=0.0, backoff_cap_seconds=0.0),
            transport=httpx.MockTransport(self._handle),
            sleeper=lambda seconds: None,
        )

    def fail(self, path_part: str, status: int, *, times: int = 1, retry_after: str | None = None) -> None:
        self.faults.append(Fault(path_part, status, times, retry_after))

    def asked(self, path_part: str) -> list[dict[str, str]]:
        """The queries of the requests whose path contains ``path_part``."""
        return [query for path, query in self.requests if path_part in path]

    def close(self, *transactions: Transaction, latest: int) -> None:
        """The network closes ledgers: ``transactions`` are in them, and the
        latest ledger is ``latest``."""
        self.history.extend(transactions)
        self.ledger = Ledger(self.history)
        self.latest = latest

    # -- the server -----------------------------------------------------------

    def _handle(self, request: httpx.Request) -> httpx.Response:
        url = urlsplit(str(request.url))
        query = dict(parse_qsl(url.query))
        path = url.path
        if self.prefix and (path == self.prefix or path.startswith(self.prefix + "/")):
            path = path[len(self.prefix):] or "/"
        self.requests.append((path, query))
        if self.stall:
            time.sleep(self.stall)
        if self.down is not None:
            raise self.down
        if self.before_answer is not None:
            self.before_answer(path)
        for fault in self.faults:
            if fault.times > 0 and fault.path_part in path:
                fault.times -= 1
                headers = {"Retry-After": fault.retry_after} if fault.retry_after else {}
                return self._problem(fault.status, headers)
        try:
            answer = self._answer(path, query)
        except _Problem as problem:
            return self._problem(problem.status)
        if isinstance(answer, dict):
            return self._json(answer)
        kind, records = answer
        if kind == "account":
            return self._page(path, query, kind, records, by_address=True)
        return self._page(path, query, kind, records)

    def _answer(self, path: str, query: dict[str, str]) -> Any:
        """An object (a dict) or a collection (kind, records)."""
        parts = [part for part in path.split("/") if part]
        if not parts:
            if self.root_down:
                raise _Problem(503)
            return self._root()
        if parts[0] == "ledgers" and len(parts) == 2:
            return self._ledger(int(parts[1]))
        if parts[0] == "accounts" and len(parts) == 1:
            return "account", self._accounts(query.get("asset"))
        if parts[0] == "assets" and len(parts) == 1:
            return "asset", self._assets(query.get("asset_issuer"))
        if parts[0] == "trade_aggregations":
            return self._aggregations(query)
        if parts[0] == "trades" and len(parts) == 1:
            return "trade", self._trades(query)
        if parts[0] == "accounts" and len(parts) == 3 and parts[2] in KINDS:
            return KINDS[parts[2]], self._account_stream(parts[1], parts[2], query)
        if parts[0] == "transactions" and len(parts) == 2:
            return self._transaction(parts[1])
        if parts[0] == "transactions" and len(parts) == 3:
            own = self.ledger.of_transaction[self._transaction(parts[1])['hash']]
            if parts[2] == "operations":
                return "operation", list(own["operations"])
            if parts[2] == "effects":
                return "effect", list(own["effects"])
        if parts[0] == "operations" and len(parts) == 2:
            return self._operation(parts[1])
        if parts[0] == "operations" and len(parts) == 3 and parts[2] == "effects":
            return "effect", [record for record in self.ledger.effects.values() if record["paging_token"].startswith(parts[1] + "-")]
        raise _Problem(404)

    def _accounts(self, asset_param: str | None) -> list[dict[str, Any]]:
        """The accounts that hold an asset — the line is what counts, not
        the balance — ordered by address, as `/accounts?asset=` answers."""
        if asset_param in (None, ""):
            holders: set[str] = set()
            for lines in self.trustlines.values():
                holders.update(lines)
        else:
            holders = set(self.trustlines.get("native" if asset_param == "native" else asset_param, {}))
        return [self._account_record(account) for account in sorted(holders)]

    def _assets(self, issuer: str | None) -> list[dict[str, Any]]:
        """The assets issued — by one issuer when asked — ordered by code and
        issuer, as `/assets` answers; an asset's paging token names both."""
        issued = sorted((code, owner) for owner, codes in self.issued.items() if issuer in (None, owner) for code in codes)
        return [
            {
                "asset_type": "credit_alphanum4" if len(code) <= 4 else "credit_alphanum12",
                "asset_code": code,
                "asset_issuer": owner,
                "paging_token": f"{code}_{owner}",
                "amount": "1000000.0000000",
                "num_accounts": len(self.trustlines.get(f"{code}:{owner}", {})),
                "flags": {"auth_required": False, "auth_revocable": False, "auth_clawback_enabled": False},
            }
            for code, owner in issued
        ]

    def _account_record(self, account: str) -> dict[str, Any]:
        balances: list[dict[str, Any]] = [{"balance": "10.0000000", "asset_type": "native", "min_balance": "0.0000000"}]
        for asset_id, lines in self.trustlines.items():
            if asset_id != "native" and account in lines:
                balances.append(
                    {
                        "balance": amount(lines[account]), "limit": "922337203685.4775807", **asset("", asset_id),
                        "is_authorized": True, "is_authorized_to_maintain_liabilities": True,
                        "last_modified_ledger": max(ELDER, self.latest - 1), "last_modified_time": iso(close_time(max(ELDER, self.latest - 1))),
                    }
                )
        return {
            "id": account, "paging_token": account, "account_id": account, "sequence": "1000",
            "subentry_count": len(balances) - 1, "last_modified_ledger": max(ELDER, self.latest - 1),
            "last_modified_time": iso(close_time(max(ELDER, self.latest - 1))),
            "thresholds": {"low_threshold": 0, "med_threshold": 0, "high_threshold": 0},
            "flags": {"auth_required": False, "auth_revocable": False, "auth_immutable": False, "auth_clawback_enabled": False},
            "balances": balances, "data": {}, "num_sponsoring": 0, "num_sponsored": 0,
            "signers": [{"public_key": account, "weight": 1, "type": "ed25519_public_key", "key": account}],
        }

    def _root(self) -> dict[str, Any]:
        return {
            "horizon_version": "30.0.1-fake", "core_version": "stellar-core 30.0.0 (fake)",
            "ingest_latest_ledger": self.latest, "history_latest_ledger": self.latest,
            "history_latest_ledger_closed_at": iso(close_time(self.latest)),
            "history_elder_ledger": self.elder, "core_latest_ledger": self.latest,
            "network_passphrase": self.network, "current_protocol_version": 30, "supported_protocol_version": 30,
        }

    def _kept(self, record: Mapping[str, Any]) -> bool:
        """History below the elder ledger is gone, as on a reaping server; and
        a server that has not ingested a ledger yet has nothing of it."""
        ledger = (int(str(record["paging_token"]).partition("-")[0]) >> 32) if "ledger" not in record else record["ledger"]
        return self.elder <= ledger <= self.latest

    def _trades(self, query: dict[str, str]) -> list[dict[str, Any]]:
        base, counter = self._asset(query, "base_"), self._asset(query, "counter_")
        found = []
        for record in self.ledger.trades.values():
            if not self._kept(record):
                continue
            pair = {self._asset(record, "base_"), self._asset(record, "counter_")}
            if base is not None and pair != {base, counter}:
                continue
            if base is not None and self._asset(record, "base_") != base:
                record = reversed_trade(record)
            found.append(record)
        return found

    @staticmethod
    def _asset(record: Mapping[str, Any], prefix: str) -> str | None:
        kind = record.get(f"{prefix}asset_type")
        if kind is None:
            return None
        return "native" if kind == "native" else f"{record[f'{prefix}asset_code']}:{record[f'{prefix}asset_issuer']}"

    def _account_stream(self, account: str, stream: str, query: dict[str, str]) -> list[dict[str, Any]]:
        if account not in (A, B, C, D, S, ISSUER):
            raise _Problem(404)
        if stream == "transactions":
            records = self.ledger.account_transactions.get(account, [])
        elif stream == "operations":
            records = [
                record for record in self.ledger.account_operations.get(account, [])
                if query.get("include_failed") == "true" or record["transaction_successful"]
            ]
        elif stream == "effects":
            records = self.ledger.account_effects.get(account, [])
        else:
            records = self.ledger.account_trades.get(account, [])
        return [record for record in records if self._kept(record)]

    def _transaction(self, name: str) -> dict[str, Any]:
        if len(name) != 64 or not all(char in "0123456789abcdef" for char in name):
            raise _Problem(400)
        record = self.ledger.transactions.get(name) or self.ledger.by_inner_hash.get(name)
        if record is None or name in self.hidden_transactions:
            raise _Problem(404)
        if record["ledger"] < self.elder:
            raise _Problem(410)
        return record

    def _operation(self, name: str) -> dict[str, Any]:
        if not name.isdigit() or len(name) > 19:
            raise _Problem(400)
        record = self.ledger.operations.get(name)
        if record is None or name in self.hidden_operations:
            raise _Problem(404)
        return record

    def _ledger(self, sequence: int) -> dict[str, Any]:
        if sequence < self.elder:
            raise _Problem(410)
        if sequence > self.latest:
            raise _Problem(404)
        if sequence not in self.ledger.ledgers:
            # A closed ledger without transactions is still a ledger.
            self.ledger.ledgers[sequence] = {
                "id": digest(f"ledger {sequence}"), "paging_token": str(sequence << 32), "sequence": sequence,
                "closed_at": iso(close_time(sequence)), "hash": digest(f"ledger {sequence}"),
                "successful_transaction_count": 0, "failed_transaction_count": 0, "operation_count": 0,
            }
        return self.ledger.ledgers[sequence]

    def _aggregations(self, query: dict[str, str]) -> dict[str, Any]:
        """Buckets of a market, in the orientation asked for, as the source
        aggregates them: count, both volumes, the average price as counter
        volume over base volume, and high, low, open and close from the
        prices the trades were made at."""
        base, counter = self._asset(query, "base_"), self._asset(query, "counter_")
        start, end = int(query["start_time"]), int(query["end_time"])
        resolution = int(query.get("resolution", 86400000))
        buckets: dict[int, list[tuple[int, int, Fraction, int, int]]] = {}  # (base, counter, price, operation, order)
        for record in self._trades(query):
            moment = int(datetime.strptime(record["ledger_close_time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).timestamp() * 1000)
            if not start <= moment < end:
                continue
            amounts = (amount_to_stroops(record["base_amount"]), amount_to_stroops(record["counter_amount"]))
            if self._asset(record, "base_") != base:
                amounts = (amounts[1], amounts[0])
            operation, _, order = record["paging_token"].partition("-")
            price = Fraction(int(record["price"]["n"]), int(record["price"]["d"]))
            buckets.setdefault(moment - moment % resolution, []).append((*amounts, price, int(operation), int(order or 0)))
        offset, limit = int(query.get("offset", 0)), int(query.get("limit", 10))
        stamps = sorted(buckets)[offset : offset + limit]
        return {"_embedded": {"records": [_bucket(stamp, buckets[stamp]) for stamp in stamps]}}

    # -- pages and problems ---------------------------------------------------

    def _page(
        self, path: str, query: dict[str, str], kind: str, records: list[dict[str, Any]], *, by_address: bool = False
    ) -> httpx.Response:
        """A page of a collection. ``by_address``: the accounts, which are
        ordered by address and have no place in time. An asset is ordered by
        its code and its issuer, and its cursor names both."""
        key = _asset_place if kind == "asset" else (lambda record: (record["account_id"],)) if by_address else (lambda record: key_of(record, kind))
        order, limit = query.get("order", "asc"), int(query.get("limit", 10))
        cursor = query.get("cursor", "")
        view = sorted((record for record in records if record.get("id") not in self.gone), key=key)
        if cursor:
            if kind == "asset":
                position = tuple(cursor.split("_", 1))
            else:
                position = (cursor,) if by_address else position_of(cursor, kind)
            # A cursor at or below the elder ledger asks for what is not kept.
            # Reading backwards that is an error; reading forwards the source
            # starts where its history does, without a word.
            if kind != "asset" and not by_address and position[0] <= self.elder << 32:
                if order == "desc":
                    return self._problem(410)
                position = (self.elder << 32, INF) if kind in ("effect", "trade") else (self.elder << 32,)
            view = [record for record in view if _after(key(record), position, order)]
        if order == "desc":
            view = view[::-1]
        page_records = view[:limit]
        next_cursor = token_of(page_records[-1]) if page_records else cursor
        following = {**query, "cursor": next_cursor}
        body = {
            "_embedded": {"records": page_records},
            "_links": {
                "self": {"href": self.origin + self.prefix + path + "?" + urlencode(query)},
                "next": {"href": self.origin + self.prefix + path + "?" + urlencode(following)},
                "prev": {"href": self.origin + self.prefix + path + "?" + urlencode({**query, "order": "asc" if order == "desc" else "desc"})},
            },
        }
        if self.tamper is not None:
            body = self.tamper(path, body)
        return self._json(body)

    def _json(self, body: dict[str, Any]) -> httpx.Response:
        return httpx.Response(
            200, headers={"content-type": "application/hal+json; charset=utf-8", **self.headers}, json=body
        )

    def _problem(self, status: int, headers: dict[str, str] | None = None) -> httpx.Response:
        titles = {400: "Bad Request", 404: "Resource Missing", 410: "Data Requested Is Before Recorded History", 429: "Too Many Requests", 503: "Service Unavailable"}
        types = {400: "bad_request", 404: "not_found", 410: "before_history", 429: "rate_limit_exceeded", 503: "service_unavailable"}
        return httpx.Response(
            status,
            headers={"content-type": "application/problem+json; charset=utf-8", **self.headers, **(headers or {})},
            json={"type": f"https://stellar.org/horizon-errors/{types[status]}", "title": titles[status], "status": status, "detail": "fake answer"},
        )


def _after(key: tuple[float, ...], position: tuple[float, ...], order: str) -> bool:
    """Whether a record lies strictly beyond a cursor."""
    return key > position if order == "asc" else key < position


def _bucket(stamp: int, trades: list[tuple[int, int, Fraction, int, int]]) -> dict[str, Any]:
    """One bucket of the source's answer for trades given as (base, counter,
    price, operation, order)."""
    volume_base = sum(trade[0] for trade in trades)
    volume_counter = sum(trade[1] for trade in trades)
    record: dict[str, Any] = {
        "timestamp": str(stamp),
        "trade_count": str(len(trades)),
        "base_volume": stroops_to_amount(volume_base),
        "counter_volume": stroops_to_amount(volume_counter),
        "avg": _seven_decimals(Fraction(volume_counter, volume_base)),
    }
    in_order = sorted(trades, key=lambda trade: (trade[3], trade[4]))
    extremes = {
        "high": max(in_order, key=lambda trade: trade[2]),
        "low": min(in_order, key=lambda trade: trade[2]),
        "open": in_order[0],
        "close": in_order[-1],
    }
    for name, trade in extremes.items():
        record[name] = _seven_decimals(trade[2])
        record[f"{name}_r"] = {"n": str(trade[2].numerator), "d": str(trade[2].denominator)}
    return record


def _seven_decimals(value: Fraction) -> str:
    """A price with seven decimals, ties away from zero."""
    scaled = value * 10**7
    whole, rest = divmod(scaled.numerator, scaled.denominator)
    if rest * 2 >= scaled.denominator:
        whole += 1
    return f"{whole // 10**7}.{whole % 10**7:07d}"
