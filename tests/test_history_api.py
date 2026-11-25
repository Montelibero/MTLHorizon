"""The history API against archives on a real PostgreSQL: the history of the
fake Horizon loaded by the backfill, and saved answers of the live server
stored as they came.

No test reaches a server: the fixtures of ``conftest.py`` refuse every
connection that is not the database, and everything compared with the live
Horizon is compared with what was saved of it.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterator, Mapping
from datetime import timedelta
from fractions import Fraction
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit

import psycopg
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError, InternalError, OperationalError
from sqlalchemy.orm import Session
from stellar_sdk import Account, Keypair, Network, TransactionBuilder

from app.backfill import store
from app.backfill.jobs import asset_params
from app.backfill.loader import Window
from app.config import Settings
from app.history_api import build_router
from app.history_api import streams as reading
from app.history_api.paging import EFFECT, OPERATION, TRADE, TRANSACTION, parse
from app.sources import horizon_records as records
from app.sources.horizon_cursors import to_total_order_id
from app.storage import history_jobs
from app.storage.models import (
    HistoryAccount,
    HistoryAsset,
    HistoryEffect,
    HistoryLedger,
    HistoryOperation,
    HistoryOperationParticipant,
    HistoryTrade,
    HistoryTransaction,
    HistoryTransactionParticipant,
)
from app.storage.upsert import mark_context
from tests import factories as f
from tests.fake_horizon import A, B, C, D, EUR, ISSUER, MTL, POOL, RECT, S, T0, FakeHorizon, paid
from tests.test_backfill import HISTORY_KEPT, Pilot

FIXTURES = Path(__file__).resolve().parent / "fixtures"
HOUR_MS, MINUTE_MS = 3_600_000, 60_000
T0_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z: when the first ledger of the fake history closes
NETWORK = Network.PUBLIC_NETWORK_PASSPHRASE
STREAMS = ("transactions", "operations", "effects", "trades")
HOUR_PERIOD = f"start_time={T0_MS}&end_time={T0_MS + HOUR_MS}&resolution={HOUR_MS}"  # the first hour, in hours

# -- the saved answers of the live server ---------------------------------------

OUTER = json.loads((FIXTURES / "live" / "transaction_fee_bump_outer.json").read_text())
INNER = json.loads((FIXTURES / "live" / "transaction_fee_bump_by_inner_hash.json").read_text())
OUTER_HASH, INNER_HASH, LIVE_SOURCE = OUTER["hash"], INNER["hash"], OUTER["source_account"]
LIVE_OPERATIONS_PAGE = json.loads((FIXTURES / "live" / "transaction_operations.json").read_text())
LIVE_OPERATIONS = LIVE_OPERATIONS_PAGE["_embedded"]["records"]
LIVE_EFFECTS_PAGE = json.loads((FIXTURES / "live" / "transaction_effects.json").read_text())
LIVE_EFFECTS = LIVE_EFFECTS_PAGE["_embedded"]["records"]
LIVE_OPERATION = json.loads((FIXTURES / "live" / "operation_path_payment_in_fee_bump.json").read_text())
LIVE_STREAM = json.loads((FIXTURES / "live" / "trades_of_transaction.json").read_text())  # /trades: stored as EURMTL first
LIVE_MARKET = json.loads((FIXTURES / "live" / "trades_of_operation_market_orientation.json").read_text())
LIVE_OPERATION_ID = LIVE_OPERATIONS[0]["id"]
LIVE_RECT = f"{LIVE_MARKET[0]['base_asset_code']}:{LIVE_MARKET[0]['base_asset_issuer']}"
LIVE_EUR = f"{LIVE_MARKET[0]['counter_asset_code']}:{LIVE_MARKET[0]['counter_asset_issuer']}"


# -- small things the tests keep using ------------------------------------------


def toid(ledger: int, order: int = 0, operation: int = 0) -> int:
    return to_total_order_id(ledger, order, operation)


def without_links(record: Mapping[str, Any]) -> dict[str, Any]:
    return {name: value for name, value in record.items() if name != "_links"}


def targets(links: Mapping[str, Any]) -> dict[str, Any]:
    """Links as path and query: where they lead, wherever the server is."""
    out: dict[str, Any] = {}
    for name, link in links.items():
        parts = urlsplit(link["href"])
        shown = parts.path + (f"?{parts.query}" if parts.query else "")
        out[name] = {"href": shown, **{key: value for key, value in link.items() if key != "href"}}
    return out


def hrefs(value: Any) -> Iterator[str]:
    if isinstance(value, dict):
        for name, item in value.items():
            if name == "_links" and isinstance(item, dict):
                yield from (link["href"] for link in item.values())
            else:
                yield from hrefs(item)
    elif isinstance(value, list):
        for item in value:
            yield from hrefs(item)


def market(base: str, counter: str) -> str:
    return urlencode(sorted({**asset_params("base_", base), **asset_params("counter_", counter)}.items()))


def with_query(path: str, **params: Any) -> str:
    return path + ("&" if "?" in path else "?") + urlencode(params)


def relative(href: str) -> str:
    parts = urlsplit(href)
    return parts.path + (f"?{parts.query}" if parts.query else "")


def seven(numerator: int, denominator: int) -> str:
    """A price as Horizon prints one: seven decimals, ties away from zero."""
    whole, rest = divmod(numerator * 10**7, denominator)
    if rest * 2 >= denominator:
        whole += 1
    return f"{whole // 10**7}.{whole % 10**7:07d}"


def amount(stroops: int) -> str:
    return f"{stroops // 10**7}.{stroops % 10**7:07d}"


def tokens(records: list[dict[str, Any]]) -> list[str]:
    return [str(record["paging_token"]) for record in records]


def rationals(record: Mapping[str, Any]) -> dict[str, Any]:
    """A bucket with its fractions as numbers, to compare with one that prints them reduced."""
    return {
        name: Fraction(int(value["n"]), int(value["d"])) if name.endswith("_r") else value
        for name, value in record.items()
    }


def holds_a_float(value: Any) -> bool:
    """Amounts and prices are strings in Horizon; a float anywhere is a bug."""
    if isinstance(value, float):
        return True
    if isinstance(value, dict):
        return any(holds_a_float(item) for item in value.values())
    if isinstance(value, list):
        return any(holds_a_float(item) for item in value)
    return False


def transaction_of(pilot: Pilot, ledger: int, order: int = 0) -> Any:
    return next(tx for tx in pilot.fake.history if (tx.ledger, tx.order) == (ledger, order))


def every_path(pilot: Pilot) -> dict[str, str]:
    fee_bump = transaction_of(pilot, 104, 1)
    operation = toid(104, 1, 1)
    return {
        "account transactions": f"/accounts/{A}/transactions",
        "account operations": f"/accounts/{A}/operations",
        "account effects": f"/accounts/{B}/effects",
        "account trades": f"/accounts/{B}/trades",
        "transaction": f"/transactions/{fee_bump.hash}",
        "transaction by its inner hash": f"/transactions/{fee_bump.inner_hash}",
        "operations of a transaction": f"/transactions/{fee_bump.hash}/operations",
        "effects of a transaction": f"/transactions/{fee_bump.hash}/effects",
        "operation": f"/operations/{operation}",
        "effects of an operation": f"/operations/{operation}/effects",
        "all transactions": "/transactions",
        "all operations": "/operations",
        "all effects": "/effects",
        "all trades": "/trades",
        "trades of a market": f"/trades?{market(RECT, EUR)}",
        "trade aggregations": f"/trade_aggregations?{market(RECT, EUR)}&{HOUR_PERIOD}",
        "coverage of an account": f"/v1/coverage?account={A}",
        "coverage of a market": f"/v1/coverage?base_asset={RECT}&counter_asset={EUR}",
        "coverage of a transaction": f"/v1/coverage?transaction={fee_bump.hash}",
    }


PAGES = (
    "account transactions", "account operations", "account effects", "account trades",
    "operations of a transaction", "effects of a transaction", "effects of an operation",
    "all transactions", "all operations", "all effects", "all trades", "trades of a market",
)  # fmt: skip
SINGLES = ("transaction", "transaction by its inner hash", "operation")


# -- what the tests run against --------------------------------------------------


def mounted(engine: Engine | None, **options: Any) -> TestClient:
    app = FastAPI()
    app.include_router(build_router(engine, Settings(database_url="", **options)))
    return TestClient(app)


def records_of(api: TestClient, path: str) -> list[dict[str, Any]]:
    response = api.get(path)
    assert response.status_code == 200, f"GET {path}: {response.status_code} {response.text[:300]}"
    assert response.headers["content-type"].startswith("application/hal+json")
    return response.json()["_embedded"]["records"]


def walk(api: TestClient, path: str, limit: int) -> list[dict[str, Any]]:
    """Every record of a stream: page after page of ``limit``, each asked for
    by the ``next`` link of the one before, until a page comes short."""
    collected: list[dict[str, Any]] = []
    url = with_query(path, limit=limit)
    for _ in range(500):
        response = api.get(url)
        assert response.status_code == 200, f"GET {url}: {response.text[:300]}"
        found = response.json()["_embedded"]["records"]
        collected.extend(found)
        if len(found) < limit:
            return collected
        url = relative(response.json()["_links"]["next"]["href"])
    raise AssertionError(f"{path}: the stream does not end")


def stored(engine: Engine, *transactions: tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]) -> None:
    """Store transactions given as (transaction, its operations, its effects)
    by the rules of the backfill."""
    with Session(engine) as session, session.begin():
        for transaction, operations, effects in transactions:
            transaction_id = store.store_transaction(session, transaction)
            for record in operations:
                store.store_operation(session, record)
            named = records.operation_participants(operations)
            for operation_id, accounts in named.items():
                store.link_operation(session, operation_id, accounts)
            store.link_transaction(session, transaction_id, records.transaction_participants(transaction, named))
            for record in effects:
                store.store_effect(session, record)
            mark_context(session, transaction_id, "operations", *(("effects",) if effects else ()))


@pytest.fixture
def pilot(make_database: Callable[[], str]) -> Iterator[Pilot]:
    instance = Pilot(make_database())
    yield instance
    instance.close()


def partial_parents(fake: FakeHorizon) -> None:
    """Add a transaction of an untracked source paying a tracked account and
    an untracked one: of its two operations, one reaches the archive."""
    fake.close(paid(125, S, A, D), latest=130)


def load(
    make_database: Callable[[], str],
    *,
    context: str = "full",
    prepare: Callable[[FakeHorizon], None] | None = None,
) -> Pilot:
    """A private database with the window of the fake history loaded as the
    backfill loads it. The caller closes the pilot."""
    instance = Pilot(make_database())
    try:
        if prepare is not None:
            prepare(instance.fake)
        reports = instance.run(context=context)
        assert {report.status for report in reports} == {"verified"}, [report.error for report in reports]
    except BaseException:
        instance.close()
        raise
    return instance


@pytest.fixture
def loaded(make_database: Callable[[], str]) -> Iterator[Pilot]:
    """The window of the fake history loaded whole, every transaction with its context."""
    instance = load(make_database)
    yield instance
    instance.close()


@pytest.fixture
def api(loaded: Pilot) -> TestClient:
    """The API, open to anyone, over the loaded window."""
    return mounted(loaded.engine, api_access="open")


@pytest.fixture
def parents(make_database: Callable[[], str]) -> Iterator[Pilot]:
    """The window loaded as the history queue loads it by default: the streams
    of the account, and only the parents a record cannot be stored without."""
    instance = load(make_database, context="parents", prepare=partial_parents)
    yield instance
    instance.close()


@pytest.fixture
def live_archive(make_database: Callable[[], str]) -> Iterator[Engine]:
    """The saved answers of the live server, stored by the rules of the backfill."""
    engine = create_engine(make_database())
    stored(engine, (OUTER, LIVE_OPERATIONS, LIVE_EFFECTS))
    with Session(engine) as session, session.begin():
        for record in LIVE_STREAM:
            store.store_trade(session, record)
        mark_context(session, int(OUTER["paging_token"]), "trades")
    yield engine
    engine.dispose()



# -- what is answered ------------------------------------------------------------


def test_every_route_answers(loaded: Pilot) -> None:
    api = mounted(loaded.engine, api_access="open")
    for name, path in every_path(loaded).items():
        response = api.get(path)
        assert response.status_code == 200, name
        assert response.headers["cache-control"] == "no-cache, no-store, max-age=0"
        body = response.json()
        if name in PAGES:
            assert response.headers["content-type"].startswith("application/hal+json"), name
            assert set(body) == {"_links", "_embedded", "scope"}, name
            assert set(body["_links"]) == {"self", "next", "prev"}, name
            assert body["_embedded"]["records"], f"{name}: nothing in it"
            for record in body["_embedded"]["records"]:
                assert record["_links"] and "paging_token" in record, name
        elif name in SINGLES:
            assert response.headers["content-type"].startswith("application/hal+json"), name
            assert "_embedded" not in body and "scope" not in body, name
            assert "_links" in body and "paging_token" in body, name
        elif name == "trade aggregations":
            assert response.headers["content-type"].startswith("application/json"), name
            assert set(body) == {"_embedded", "scope"}, name
            assert body["_embedded"]["records"], name
        else:
            assert response.headers["content-type"].startswith("application/json"), name
            assert {"subject", "statement", "incomplete"} <= set(body), name


def test_records_are_what_the_live_server_answered(live_archive: Engine) -> None:
    """Stored as they came from the live server, the records come back as the
    live server sent them: the same members, the same values, the same
    cursors. Only the links differ, and they lead to the same paths."""
    api = mounted(live_archive, api_access="open")

    outer = api.get(f"/transactions/{OUTER_HASH}").json()
    assert without_links(outer) == without_links(OUTER)
    assert targets(outer["_links"]) == targets(OUTER["_links"])

    # Asked by the inner hash of the fee-bump: id, hash and signatures are the inner ones.
    inner = api.get(f"/transactions/{INNER_HASH}").json()
    assert without_links(inner) == without_links(INNER)
    assert targets(inner["_links"]) == targets(INNER["_links"])
    assert inner["fee_bump_transaction"]["hash"] == OUTER_HASH

    for asked in (OUTER_HASH, INNER_HASH):
        served = records_of(api, f"/transactions/{asked}/operations?limit=200&order=asc")
        assert [without_links(record) for record in served] == [without_links(record) for record in LIVE_OPERATIONS]
        assert [record["transaction_hash"] for record in served] == [OUTER_HASH]
    page = api.get(f"/transactions/{OUTER_HASH}/operations?limit=200&order=asc").json()
    assert targets(page["_links"]) == targets(LIVE_OPERATIONS_PAGE["_links"])
    assert [targets(record["_links"]) for record in page["_embedded"]["records"]] == [
        targets(record["_links"]) for record in LIVE_OPERATIONS
    ]

    for path in (f"/transactions/{OUTER_HASH}/effects", f"/operations/{LIVE_OPERATION_ID}/effects"):
        served = records_of(api, path)
        assert [without_links(record) for record in served] == [without_links(record) for record in LIVE_EFFECTS]
    page = api.get(f"/transactions/{OUTER_HASH}/effects?limit=200&order=asc").json()
    assert targets(page["_links"]) == targets(LIVE_EFFECTS_PAGE["_links"])
    assert [targets(record["_links"]) for record in page["_embedded"]["records"]] == [
        targets(record["_links"]) for record in LIVE_EFFECTS
    ]

    operation = api.get(f"/operations/{LIVE_OPERATION_ID}").json()
    assert without_links(operation) == without_links(LIVE_OPERATION)
    assert targets(operation["_links"]) == targets(LIVE_OPERATION["_links"])

    # The market asked the other way round: the same trades, base and counter exchanged.
    asked = records_of(api, f"/trades?{market(LIVE_RECT, LIVE_EUR)}")
    assert [without_links(record) for record in asked] == [without_links(record) for record in LIVE_MARKET]
    assert [targets(record["_links"]) for record in asked] == [targets(record["_links"]) for record in LIVE_MARKET]
    asked = records_of(api, f"/trades?{market(LIVE_EUR, LIVE_RECT)}")
    assert [without_links(record) for record in asked] == [without_links(record) for record in LIVE_STREAM[1:]]
    asked = records_of(api, "/trades")
    assert [without_links(record) for record in asked] == [without_links(record) for record in LIVE_STREAM]
    assert records_of(api, f"/trades?{market(LIVE_EUR, LIVE_EUR)}") == []  # a market of one asset with itself
    # The source of the path payment is a side of all four trades.
    asked = records_of(api, f"/accounts/{LIVE_SOURCE}/trades")
    assert [without_links(record) for record in asked] == [without_links(record) for record in LIVE_STREAM]

    # result_meta_xdr was never served by the source: it is not made up here.
    assert "result_meta_xdr" not in outer
    assert "result_meta_xdr" not in api.get(f"/accounts/{LIVE_SOURCE}/transactions").text


def test_buckets_have_the_members_the_live_server_served(api: TestClient) -> None:
    live = json.loads((FIXTURES / "history_api" / "trade_aggregations_live.json").read_text())
    for base, counter, name in ((EUR, RECT, "EURMTL_MTLRECT"), (RECT, EUR, "MTLRECT_EURMTL")):
        served = api.get(
            "/trade_aggregations",
            params={
                **asset_params("base_", base),
                **asset_params("counter_", counter),
                "start_time": T0_MS,
                "end_time": T0_MS + 24 * HOUR_MS,
                "resolution": HOUR_MS,
                "limit": 200,
            },
        ).json()["_embedded"]["records"]
        assert served
        for bucket, live_bucket in zip(served, live[name]):
            assert list(bucket) == list(live_bucket)
            for key, value in bucket.items():
                if key.endswith("_r"):
                    assert list(value) == ["n", "d"] and all(isinstance(part, str) for part in value.values())
                else:
                    assert isinstance(value, str)


def test_what_is_stored_is_served_and_nothing_else(api: TestClient) -> None:
    """The transaction with meta in the fake history has it; the others have
    none, and none is invented for them."""
    with_meta = str(toid(110, 2))
    shown = {record["paging_token"]: record for record in records_of(api, "/transactions?include_failed=true")}
    assert shown[with_meta]["result_meta_xdr"] == "AAAAAA=="
    assert [token for token, record in shown.items() if "result_meta_xdr" in record] == [with_meta]
    assert len(shown) == 6


# -- cursors ------------------------------------------------------------------------


def streams_of(pilot: Pilot) -> dict[str, str]:
    fee_bump = transaction_of(pilot, 104, 1)
    return {
        "account transactions": f"/accounts/{A}/transactions?include_failed=true",
        "fee source transactions": f"/accounts/{S}/transactions",
        "account operations": f"/accounts/{A}/operations?include_failed=true",
        "account effects": f"/accounts/{B}/effects",  # five of them share one operation id
        "account trades": f"/accounts/{C}/trades",
        "all transactions": "/transactions?include_failed=true",
        "all operations": "/operations?include_failed=true",
        "all effects": "/effects",
        "all trades": "/trades",
        "market, stored way round": f"/trades?{market(RECT, EUR)}",
        "market, the other way round": f"/trades?{market(EUR, RECT)}",
        "operations of a transaction": f"/transactions/{fee_bump.inner_hash}/operations",
    }


@pytest.mark.parametrize("limit", [1, 2, 3, 7])
def test_paging_loses_nothing_and_repeats_nothing(loaded: Pilot, limit: int) -> None:
    """Page after page of ``limit`` records is the same stream as one page, in
    both directions, the links alone deciding where to go on."""
    api = mounted(loaded.engine, api_access="open")
    for name, path in streams_of(loaded).items():
        whole = walk(api, path, 200)
        assert whole, name
        assert len(tokens(whole)) == len(set(tokens(whole))), name
        assert tokens(walk(api, path, limit)) == tokens(whole), name
        assert walk(api, with_query(path, order="desc"), limit) == list(reversed(whole)), name


def test_effects_of_one_operation_page_by_their_order(live_archive: Engine) -> None:
    """Nine effects of one operation, all with the same operation id: the
    cursor has to tell them apart by the order inside the operation."""
    api = mounted(live_archive, api_access="open")
    path = f"/operations/{LIVE_OPERATION_ID}/effects"
    whole = [record["paging_token"] for record in LIVE_EFFECTS]
    assert whole == tokens(records_of(api, path))
    for limit in (1, 2, 4, 8, 9):
        assert tokens(walk(api, path, limit)) == whole
        assert tokens(walk(api, with_query(path, order="desc"), limit)) == whole[::-1]


def test_links_say_where_they_go(api: TestClient) -> None:
    first = api.get(f"/accounts/{A}/transactions", params={"limit": 2, "include_failed": "true"}).json()
    second = api.get(relative(first["_links"]["next"]["href"])).json()
    assert second["_links"]["self"] == first["_links"]["next"]
    assert not set(tokens(second["_embedded"]["records"])) & set(tokens(first["_embedded"]["records"]))
    # The filters travel with the links.
    for link in second["_links"].values():
        query = dict(parse_qsl(urlsplit(link["href"]).query))
        assert query["include_failed"] == "true" and query["limit"] == "2", link
    back = api.get(relative(second["_links"]["prev"]["href"])).json()
    assert tokens(back["_embedded"]["records"]) == tokens(first["_embedded"]["records"])[::-1]

    narrowed = api.get(f"/accounts/{A}/operations", params={"type": "manage_data", "limit": 1}).json()
    assert "type=manage_data" in narrowed["_links"]["next"]["href"]
    of_market = api.get(f"/trades?{market(EUR, RECT)}").json()
    for link in of_market["_links"].values():
        assert "base_asset_code=EURMTL" in link["href"] and "counter_asset_code=MTLRECT" in link["href"], link
    typed = api.get(f"/accounts/{B}/trades?trade_type=orderbook").json()
    assert "trade_type=orderbook" in typed["_links"]["next"]["href"]


def test_an_empty_page_and_the_now_cursor(loaded: Pilot, api: TestClient) -> None:
    never_seen = f.address(4242)
    empty = api.get(f"/accounts/{never_seen}/transactions").json()
    root = f"http://testserver/accounts/{never_seen}/transactions?"
    assert empty["_embedded"]["records"] == []
    assert empty["_links"] == {
        "self": {"href": root + "cursor=&limit=10&order=asc"},
        "next": {"href": root + "cursor=&limit=10&order=asc"},
        "prev": {"href": root + "cursor=&limit=10&order=desc"},
    }
    assert empty["scope"]["coverage"] == f"/v1/coverage?account={never_seen}"
    assert "nothing more is in this archive" in empty["scope"]["note"]

    for name, path in streams_of(loaded).items():
        newest = walk(api, with_query(path, cursor="now", order="desc"), 3)
        assert newest == walk(api, with_query(path, order="desc"), 3), name
        assert records_of(api, with_query(path, cursor="now")) == [], name



# -- failed transactions, unknown types, fee bumps, markets ---------------------------


def test_failed_transactions_are_hidden_unless_asked_for(loaded: Pilot) -> None:
    api = mounted(loaded.engine, api_access="open")
    failed, failed_operation = transaction_of(loaded, 108).hash, str(toid(108, 0, 1))
    assert tokens(records_of(api, f"/accounts/{A}/transactions")) == [str(toid(100)), str(toid(110, 2)), str(toid(118))]
    assert tokens(records_of(api, f"/accounts/{A}/transactions?include_failed=true")) == [
        str(toid(100)), str(toid(108)), str(toid(110, 2)), str(toid(118))
    ]
    assert failed not in api.get("/transactions").text
    assert failed in api.get("/transactions", params={"include_failed": "true"}).text

    # The operations of a failed transaction: hidden in the streams, served when it is asked for.
    assert failed_operation not in tokens(records_of(api, f"/accounts/{A}/operations"))
    assert failed_operation not in tokens(records_of(api, "/operations"))
    served = records_of(api, f"/transactions/{failed}/operations")
    assert tokens(served) == [failed_operation] and served[0]["transaction_successful"] is False
    assert records_of(api, f"/transactions/{failed}/effects") == []
    assert api.get(f"/transactions/{failed}").json()["successful"] is False
    for flag in ("false", "0", "False"):
        assert failed_operation not in tokens(records_of(api, f"/accounts/{A}/operations?include_failed={flag}"))
    for flag in ("true", "1", "TRUE"):
        assert failed_operation in tokens(records_of(api, f"/accounts/{A}/operations?include_failed={flag}"))


def test_an_unknown_operation_type_stays(loaded: Pilot) -> None:
    api = mounted(loaded.engine, api_access="open")
    unknown = api.get(f"/operations/{toid(110, 2, 1)}").json()
    assert unknown["type"] == "unknown" and unknown["type_i"] == 99
    assert unknown["transaction_hash"] == transaction_of(loaded, 110, 2).hash
    assert unknown["transaction_successful"] is True
    shown = {record["id"]: record for record in records_of(api, f"/accounts/{A}/operations")}
    assert shown[str(toid(110, 2, 1))]["type"] == "unknown"
    assert shown[str(toid(110, 2, 2))]["type"] == "payment"


def test_a_fee_bump_is_found_by_either_hash_and_shown_as_upstream_does(loaded: Pilot) -> None:
    api = mounted(loaded.engine, api_access="open")
    fee_bump = transaction_of(loaded, 104, 1)
    outer = api.get(f"/transactions/{fee_bump.hash}").json()
    inner = api.get(f"/transactions/{fee_bump.inner_hash}").json()
    assert outer["id"] == outer["hash"] == fee_bump.hash
    assert inner["id"] == inner["hash"] == fee_bump.inner_hash
    assert outer["signatures"] == [f"sig-of-{S[:6]}"] and inner["signatures"] == ["inner"]
    assert outer["fee_bump_transaction"] == {"hash": fee_bump.hash, "signatures": outer["signatures"]}
    assert inner["fee_bump_transaction"] == outer["fee_bump_transaction"]
    assert outer["inner_transaction"]["hash"] == fee_bump.inner_hash
    assert outer["max_fee"] == "20000" and outer["inner_transaction"]["max_fee"] == "10000"
    assert outer["paging_token"] == inner["paging_token"] == str(toid(104, 1))
    assert outer["fee_account"] == S and outer["source_account"] == B
    assert outer["_links"]["self"]["href"] == f"http://testserver/transactions/{fee_bump.hash}"
    assert inner["_links"]["self"]["href"] == f"http://testserver/transactions/{fee_bump.inner_hash}"

    # In the streams of both of its accounts it is the outer transaction.
    for account in (B, S):
        shown = [r for r in records_of(api, f"/accounts/{account}/transactions") if r["paging_token"] == str(toid(104, 1))]
        assert [record["hash"] for record in shown] == [fee_bump.hash]

    # Its operations and effects are served under either hash; they name the outer transaction.
    for asked in (fee_bump.hash, fee_bump.inner_hash):
        served = records_of(api, f"/transactions/{asked}/operations")
        assert [record["transaction_hash"] for record in served] == [fee_bump.hash]
        assert served[0]["_links"]["self"]["href"] == f"http://testserver/operations/{toid(104, 1, 1)}"
        assert tokens(records_of(api, f"/transactions/{asked}/effects")) == [
            f"{toid(104, 1, 1)}-{order}" for order in range(1, 6)
        ]
    effect = records_of(api, f"/transactions/{fee_bump.inner_hash}/effects")[0]
    assert targets(effect["_links"])["operation"]["href"] == f"/operations/{toid(104, 1, 1)}"


def test_the_market_is_shown_the_way_it_was_asked(api: TestClient) -> None:
    stored_trades = {record["id"]: record for record in records_of(api, "/trades")}
    pool, book, sale = f"{toid(104, 1, 1)}-0", f"{toid(104, 1, 1)}-1", f"{toid(112, 0, 1)}-0"
    assert list(stored_trades) == [pool, book, sale]
    through_pool = stored_trades[pool]
    assert through_pool["trade_type"] == "liquidity_pool" and through_pool["liquidity_pool_fee_bp"] == 30
    assert through_pool["base_liquidity_pool_id"] == POOL and "base_account" not in through_pool
    assert through_pool["counter_account"] == B and through_pool["counter_offer_id"] == "4893653146851139585"
    assert through_pool["price"] == {"n": "7126809730", "d": "49128652874"}
    assert through_pool["_links"]["base"] == {"href": f"http://testserver/liquidity_pools/{POOL}"}

    # Stored as EURMTL/MTLRECT and MTLRECT/EURMTL; asked as MTLRECT/EURMTL.
    rect_eur = records_of(api, f"/trades?{market(RECT, EUR)}")
    assert [record["id"] for record in rect_eur] == [book, sale]
    assert without_links(rect_eur[1]) == without_links(stored_trades[sale])
    asked = rect_eur[0]
    assert (asked["base_asset_code"], asked["base_account"], asked["base_amount"]) == ("MTLRECT", C, "4.9325436")
    assert (asked["counter_asset_code"], asked["counter_account"], asked["counter_amount"]) == ("EURMTL", B, "23.1829549")
    assert asked["base_is_seller"] is True and asked["price"] == {"n": "47", "d": "10"}
    assert (asked["base_offer_id"], asked["counter_offer_id"]) == ("1867248625", "4893653146851139585")
    assert asked["_links"]["base"] == {"href": f"http://testserver/accounts/{C}"}
    assert asked["_links"]["counter"]["href"] == f"http://testserver/accounts/{B}"

    # The other way round: the same trades, the same ids.
    eur_rect = records_of(api, f"/trades?{market(EUR, RECT)}")
    assert [record["id"] for record in eur_rect] == [book, sale]
    assert without_links(eur_rect[0]) == without_links(stored_trades[book])
    asked = eur_rect[1]
    assert (asked["base_asset_code"], asked["base_account"], asked["base_is_seller"]) == ("EURMTL", D, False)
    assert (asked["base_amount"], asked["counter_amount"]) == ("23.1829549", "4.9325436")
    assert asked["price"] == {"n": "10", "d": "47"}
    assert (asked["base_offer_id"], asked["counter_offer_id"]) == ("112", "1804182570")

def test_trades_by_market_filter_and_by_account(api: TestClient) -> None:
    pool, book, sale = f"{toid(104, 1, 1)}-0", f"{toid(104, 1, 1)}-1", f"{toid(112, 0, 1)}-0"
    assert [r["id"] for r in records_of(api, f"/trades?{market(EUR, MTL)}")] == [pool]  # a pool trade is a trade
    assert [r["id"] for r in records_of(api, f"/trades?{market(MTL, EUR)}")] == [pool]
    assert records_of(api, f"/trades?{market(EUR, MTL)}&trade_type=orderbook") == []
    assert [r["id"] for r in records_of(api, f"/trades?{market(RECT, EUR)}&trade_type=orderbook")] == [book, sale]
    assert records_of(api, f"/trades?{market(RECT, MTL)}") == []  # never traded here: an empty page, not an error
    assert [r["id"] for r in records_of(api, f"/accounts/{B}/trades")] == [pool, book]
    assert [r["id"] for r in records_of(api, f"/accounts/{C}/trades")] == [book, sale]
    assert [r["id"] for r in records_of(api, f"/accounts/{D}/trades")] == [sale]
    assert records_of(api, f"/accounts/{A}/trades") == []
    assert [r["id"] for r in records_of(api, f"/accounts/{B}/trades?trade_type=liquidity_pool")] == [pool]


def test_a_trade_of_an_account_with_itself_comes_once(make_database: Callable[[], str]) -> None:
    engine = create_engine(make_database())
    with Session(engine) as session, session.begin():
        transaction = f.add_transaction(session, 10, source_account=f.ALICE)
        operation = f.add_operation(session, transaction)
        f.add_trade(session, operation, 0, base_account=f.ALICE, counter_account=f.ALICE)
        f.add_trade(session, operation, 1, base_account=f.ALICE, counter_account=f.BOB)
        f.add_trade(session, operation, 2, base_account=f.CAROL, counter_account=f.ALICE)
    try:
        api = mounted(engine, api_access="open")
        whole = records_of(api, f"/accounts/{f.ALICE}/trades")
        assert [record["id"] for record in whole] == [f"{operation}-0", f"{operation}-1", f"{operation}-2"]
        assert tokens(walk(api, f"/accounts/{f.ALICE}/trades", 1)) == tokens(whole)
        middle = records_of(api, f"/accounts/{f.ALICE}/trades?cursor={operation}-0&limit=1")
        assert [record["id"] for record in middle] == [f"{operation}-1"]
    finally:
        engine.dispose()



# -- scope: what the archive has, and what it does not ---------------------------------


def test_pages_say_what_kind_of_view_they_are(loaded: Pilot) -> None:
    api = mounted(loaded.engine, api_access="open")
    account = api.get(f"/accounts/{A}/operations").json()["scope"]
    assert account["kind"] == "account" and account["coverage"] == f"/v1/coverage?account={A}"
    assert "not that nothing happened on the network" in account["note"]
    of_market = api.get(f"/trades?{market(RECT, EUR)}").json()["scope"]
    assert of_market["kind"] == "market"
    assert "base_asset=MTLRECT%3A" in of_market["coverage"] and "counter_asset=EURMTL%3A" in of_market["coverage"]
    for path in ("/transactions", "/operations", "/effects", "/trades"):
        whole = api.get(path).json()["scope"]
        assert set(whole) == {"kind", "note"} and whole["kind"] == "archive"
        assert "not the stream of the network" in whole["note"]
    aggregated = api.get(every_path(loaded)["trade aggregations"]).json()["scope"]
    assert aggregated["kind"] == "market" and "counter_asset=EURMTL%3A" in aggregated["coverage"]


def test_a_list_of_one_transaction_says_whether_it_is_whole(make_database: Callable[[], str]) -> None:
    """Loaded with parents only, the operations and the effects of a
    transaction are those that a stream brought: the list says it may be
    incomplete. Loaded with its context, the list says it is whole."""
    parents, whole_window = load(make_database, context="parents", prepare=partial_parents), load(make_database)
    try:
        api = mounted(parents.engine, api_access="open")
        partial = transaction_of(parents, 125)
        operations = api.get(f"/transactions/{partial.hash}/operations").json()
        assert tokens(operations["_embedded"]["records"]) == [str(toid(125, 0, 1))]
        assert operations["scope"] == {
            "kind": "transaction",
            "context_loaded": False,
            "note": operations["scope"]["note"],
            "coverage": f"/v1/coverage?transaction={partial.hash}",
        }
        assert "may be incomplete" in operations["scope"]["note"]
        effects = api.get(f"/transactions/{partial.hash}/effects").json()
        assert effects["scope"]["context_loaded"] is False
        assert tokens(effects["_embedded"]["records"]) == [f"{toid(125, 0, 1)}-1"]
        of_operation = api.get(f"/operations/{toid(125, 0, 1)}/effects").json()
        assert of_operation["scope"]["kind"] == "operation" and of_operation["scope"]["context_loaded"] is False
        assert of_operation["scope"]["coverage"] == f"/v1/coverage?transaction={partial.hash}"

        whole = mounted(whole_window.engine, api_access="open")
        sale = transaction_of(whole_window, 112)
        operations = whole.get(f"/transactions/{sale.hash}/operations").json()
        assert operations["scope"]["context_loaded"] is True
        assert "as the source serves them" in operations["scope"]["note"]
        assert whole.get(f"/transactions/{sale.hash}/effects").json()["scope"]["context_loaded"] is True
    finally:
        parents.close()
        whole_window.close()


def test_coverage_of_a_transaction_says_what_was_loaded(make_database: Callable[[], str]) -> None:
    parents, whole_window = load(make_database, context="parents", prepare=partial_parents), load(make_database)
    try:
        told_of_partial(parents)
        told_whole(whole_window)
    finally:
        parents.close()
        whole_window.close()


def told_of_partial(parents: Pilot) -> None:
    partial = transaction_of(parents, 125)
    told = mounted(parents.engine, api_access="open").get(f"/v1/coverage?transaction={partial.hash}").json()
    assert told["subject"] == {
        "type": "transaction",
        "hash": partial.hash,
        "transaction_hash": partial.hash,
        "inner_transaction_hash": None,
        "id": str(toid(125)),
        "ledger": 125,
        "successful": True,
    }
    assert told["context_loaded"] == []
    assert told["context"] == {
        "operations": {"complete": False, "stored": 1},
        "effects": {"complete": False, "stored": 1},
        "trades": {"complete": False, "stored": 0},
    }
    assert told["incomplete"] == [
        "operations: not loaded as a whole; 1 are stored, and there may be more",
        "effects: not loaded as a whole; 1 are stored, and there may be more",
        "trades: not loaded as a whole; none are stored, and there may be some",
    ]



def told_whole(loaded: Pilot) -> None:
    api = mounted(loaded.engine, api_access="open")
    fee_bump = transaction_of(loaded, 104, 1)
    told = api.get(f"/v1/coverage?transaction={fee_bump.inner_hash}").json()
    assert told["subject"]["hash"] == fee_bump.inner_hash
    assert told["subject"]["transaction_hash"] == fee_bump.hash
    assert told["subject"]["inner_transaction_hash"] == fee_bump.inner_hash
    assert told["context_loaded"] == ["operations", "effects", "trades"]
    assert told["context"] == {
        "operations": {"complete": True, "stored": 1},
        "effects": {"complete": True, "stored": 5},
        "trades": {"complete": True, "stored": 2},
    }
    assert told["incomplete"] == []
    failed = api.get(f"/v1/coverage?transaction={transaction_of(loaded, 108).hash}").json()
    assert failed["subject"]["successful"] is False
    assert failed["incomplete"] == ["the transaction failed: it has no effects and no trades on the network"]


def test_coverage_says_what_a_partial_import_left(pilot: Pilot) -> None:
    """A stream that reached only part of its window: the page is short, and
    the coverage says which ledgers were not read."""
    pilot.run(page_size=2, stop_after=6)  # A's transactions: ledgers 111–130 read, 100–110 pending
    api = mounted(pilot.engine, api_access="open")
    assert tokens(records_of(api, f"/accounts/{A}/transactions")) == [str(toid(110, 2)), str(toid(118))]
    told = api.get(f"/v1/coverage?account={A}").json()
    assert told["complete"] is False
    assert told["subject"] == {"type": "account", "id": A}
    policy = told["streams"]["transactions"]["policies"][0]
    assert policy["policy"] == "FULL" and policy["policy_version"] == 1
    assert policy["window"] == {"from_ledger": 100, "to_ledger": 130}
    assert policy["ledgers"] == {"complete": 20, "empty": 0, "pending": 11, "unverifiable": 0}
    assert [(r["from_ledger"], r["to_ledger"], r["state"]) for r in policy["ranges"]] == [
        (100, 110, "pending"),
        (111, 130, "complete"),
    ]
    assert policy["ranges"][0]["verified_at"] is None and policy["ranges"][1]["verified_at"] is not None
    assert set(policy["ranges"][1]) == {"from_ledger", "to_ledger", "state", "verified_at", "detail", "source"}
    assert "transactions: pending, not read yet — ledgers 100–110" in told["incomplete"]
    assert "transactions: nothing is claimed below ledger 100 or above ledger 130" in told["incomplete"]
    assert all(any(line.startswith(f"{stream}:") for line in told["incomplete"]) for stream in STREAMS)
    assert told["tracked"] == {
        "enabled": True,
        "origin": "yaml",
        "discovered_by": None,
        "watch_since_ledger": None,
        "boundary_due": True,  # its import is planned, not finished
    }
    assert told["import"] is None
    assert told["archive"] == {"oldest_ledger": 110, "newest_ledger": 118}
    history_jobs.plan(pilot.engine, kind="account", subject=A, to_ledger=130)
    told = api.get(f"/v1/coverage?account={A}").json()
    assert told["import"]["state"] == "waiting" and told["import"]["to_ledger"] == 130
    assert told["import"]["context"] == "parents"


def test_coverage_names_what_the_source_could_no_longer_answer(pilot: Pilot) -> None:
    pilot.run(Window(95, 130))  # below ledger 100 the source keeps nothing any more
    api = mounted(pilot.engine, api_access="open")
    told = api.get(f"/v1/coverage?account={A}").json()
    assert told["complete"] is False
    for stream in STREAMS:
        assert any(line.startswith(f"{stream}: unverifiable — ledgers 95–99 (") for line in told["incomplete"]), stream
    effects = told["streams"]["effects"]["policies"][0]
    assert [(r["from_ledger"], r["to_ledger"], r["state"]) for r in effects["ranges"]] == [
        (95, 99, "unverifiable"),
        (100, 130, "complete"),
    ]
    assert effects["ranges"][0]["detail"] == HISTORY_KEPT + " 100 (history_latest_ledger 130)"
    assert effects["ranges"][0]["verified_at"] is not None
    assert effects["ledgers"] == {"complete": 31, "empty": 0, "pending": 0, "unverifiable": 5}


def test_coverage_of_a_market(loaded: Pilot) -> None:
    api = mounted(loaded.engine, api_access="open")
    told = api.get(f"/v1/coverage?base_asset={RECT}&counter_asset={EUR}").json()
    assert told["subject"] == {"type": "market", "id": f"{EUR}>{RECT}", "assets": [EUR, RECT]}  # lower asset first
    trades = told["streams"]["trades"]
    assert trades["stored"] == {"first_ledger": 104, "last_ledger": 112}
    policy = trades["policies"][0]
    assert policy["policy"] == "MARKET" and policy["window"] == {"from_ledger": 100, "to_ledger": 130}
    assert policy["ledgers"]["pending"] == policy["ledgers"]["unverifiable"] == 0
    assert policy["ledgers"]["complete"] + policy["ledgers"]["empty"] == 31
    assert told["tracked"] == {"enabled": True}
    assert "trades: nothing is claimed below ledger 100 or above ledger 130" in told["incomplete"]
    assert told["complete"] is False
    assert api.get(f"/v1/coverage?base_asset={EUR}&counter_asset={RECT}").json() == told  # either way round
    quiet = api.get(f"/v1/coverage?base_asset={RECT}&counter_asset={MTL}").json()
    assert quiet["streams"]["trades"] == {"stored": None, "policies": []}
    assert quiet["incomplete"] == ["trades: nothing is verified; no range of this stream was planned or read for this market"]
    assert quiet["tracked"] is None and quiet["import"] is None


def test_coverage_of_what_the_archive_never_met(api: TestClient) -> None:
    stranger = f.address(777)
    told = api.get(f"/v1/coverage?account={stranger}").json()
    assert told["tracked"] is None and told["import"] is None
    assert all(told["streams"][stream] == {"stored": None, "policies": []} for stream in STREAMS)
    assert told["incomplete"] == [
        f"{stream}: nothing is verified; no range of this stream was planned or read for this account"
        for stream in STREAMS
    ]
    assert told["archive"] == {"oldest_ledger": 100, "newest_ledger": 118}
    assert records_of(api, f"/accounts/{stranger}/operations") == []


# -- manage_data: what an operation does to a data entry ---------------------------------


def effect_record(operation_id: int, account: str, kind: str, type_i: int, created: str, **members: Any) -> dict[str, Any]:
    """An effect of the first place of ``operation_id``, as Horizon answers it."""
    return {
        "id": f"{operation_id:019d}-{1:010d}",
        "paging_token": f"{operation_id}-1",
        "account": account,
        "type": kind,
        "type_i": type_i,
        "created_at": created,
        **members,
    }


def manage_data_history(ledger: int, *, fee_bump: bool = False) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    """A transaction of three manage_data operations — a value, an empty
    value, a deletion — with a real envelope, in the shape Horizon answers.

    The operations show what Horizon shows: a deletion and an empty value are
    both ``value: ""``.
    """
    source, payer = Keypair.random(), Keypair.random()
    builder = (
        TransactionBuilder(source_account=Account(source.public_key, 1), network_passphrase=NETWORK, base_fee=100)
        .set_timeout(30)
        .append_manage_data_op("config", b"hello")
        .append_manage_data_op("blank", b"")
        .append_manage_data_op("gone", None)
    )
    created = (T0 + timedelta(seconds=5 * (ledger - 100))).strftime("%Y-%m-%dT%H:%M:%SZ")
    inner = builder.build()
    envelope = (
        builder.build_fee_bump_transaction(payer.public_key, 600, inner, network_passphrase=NETWORK) if fee_bump else inner
    )
    transaction_hash = envelope.hash().hex()
    transaction: dict[str, Any] = {
        "id": transaction_hash,
        "paging_token": str(toid(ledger)),
        "successful": True,
        "hash": transaction_hash,
        "ledger": ledger,
        "created_at": created,
        "source_account": source.public_key,
        "source_account_sequence": "1",
        "fee_account": payer.public_key if fee_bump else source.public_key,
        "fee_charged": "300",
        "max_fee": "600" if fee_bump else "300",
        "operation_count": 3,
        "envelope_xdr": envelope.to_xdr(),
        "result_xdr": "AAAAAAAAAGQ=",
        "fee_meta_xdr": "AAAAAg==",
        "memo_type": "none",
        "signatures": [],
    }
    if fee_bump:
        transaction["fee_bump_transaction"] = {"hash": transaction_hash, "signatures": []}
        transaction["inner_transaction"] = {"hash": inner.hash().hex(), "signatures": [], "max_fee": "300"}
    operations = [
        {
            "id": str(toid(ledger, 0, number)),
            "paging_token": str(toid(ledger, 0, number)),
            "transaction_successful": True,
            "source_account": source.public_key,
            "type": "manage_data",
            "type_i": 10,
            "created_at": created,
            "transaction_hash": transaction_hash,
            "name": name,
            "value": value,
        }
        for number, (name, value) in enumerate((("config", "aGVsbG8="), ("blank", ""), ("gone", "")), start=1)
    ]
    return transaction, operations, source.public_key


@pytest.mark.parametrize("fee_bump", [False, True])
def test_manage_data_is_told_by_the_envelope(make_database: Callable[[], str], fee_bump: bool) -> None:
    engine = create_engine(make_database())
    try:
        transaction, operations, source = manage_data_history(200, fee_bump=fee_bump)
        stored(engine, (transaction, operations, []))
        api = mounted(engine, api_access="open")
        shown = records_of(api, f"/accounts/{source}/operations")
        assert [record["value"] for record in shown] == ["aGVsbG8=", "", ""]  # as upstream shows them
        assert [record["data_change"] for record in shown] == [
            {"request": "set", "told_by": "xdr", "value": "aGVsbG8=", "effect": None},
            {"request": "set", "told_by": "xdr", "value": "", "effect": None},  # an empty value ...
            {"request": "remove", "told_by": "xdr", "effect": None},  # ... and a deletion: apart in the envelope
        ]
        for number in (1, 2, 3):
            single = api.get(f"/operations/{toid(200, 0, number)}").json()
            assert single["data_change"] == shown[number - 1]["data_change"]
        of_transaction = records_of(api, f"/transactions/{transaction['hash']}/operations")
        assert [record["data_change"] for record in of_transaction] == [record["data_change"] for record in shown]
    finally:
        engine.dispose()


def test_manage_data_is_told_by_its_effect_when_the_envelope_cannot(make_database: Callable[[], str]) -> None:
    engine = create_engine(make_database())
    try:
        created = (T0 + timedelta(seconds=5 * 200)).strftime("%Y-%m-%dT%H:%M:%SZ")
        source = f.ALICE
        transaction = {
            "id": f.digest(1),
            "paging_token": str(toid(300)),
            "successful": True,
            "hash": f.digest(1),
            "ledger": 300,
            "created_at": created,
            "source_account": source,
            "source_account_sequence": "1",
            "fee_account": source,
            "fee_charged": "400",
            "max_fee": "400",
            "operation_count": 4,
            "envelope_xdr": "AAAA" + "not-an-envelope",
            "result_xdr": "AAAAAAAAAGQ=",
            "fee_meta_xdr": "AAAAAg==",
            "memo_type": "none",
            "signatures": [],
        }
        entries = (("gone", ""), ("blank", ""), ("changed", "eA=="), ("lost", ""))
        operations = [
            {
                "id": str(toid(300, 0, number)),
                "paging_token": str(toid(300, 0, number)),
                "transaction_successful": True,
                "source_account": source,
                "type": "manage_data",
                "type_i": 10,
                "created_at": created,
                "transaction_hash": f.digest(1),
                "name": name,
                "value": value,
            }
            for number, (name, value) in enumerate(entries, start=1)
        ]
        effects = [
            effect_record(toid(300, 0, 1), source, "data_removed", 41, created, name="gone"),
            effect_record(toid(300, 0, 2), source, "data_created", 40, created, name="blank", value=""),
            effect_record(toid(300, 0, 3), source, "data_updated", 42, created, name="changed", value="eA=="),
        ]
        stored(engine, (transaction, operations, effects))
        api = mounted(engine, api_access="open")
        shown = records_of(api, f"/accounts/{source}/operations")
        assert [record["data_change"] for record in shown] == [
            {"request": "remove", "told_by": "effect", "effect": "data_removed"},
            {"request": "set", "told_by": "effect", "value": "", "effect": "data_created"},
            {"request": "set", "told_by": "effect", "value": "eA==", "effect": "data_updated"},
            {"request": "unknown", "told_by": None, "effect": None},  # neither can tell
        ]
    finally:
        engine.dispose()


def test_what_cannot_be_told_is_unknown(api: TestClient) -> None:
    """The manage_data operation of a failed transaction in the fake history:
    no effects (it failed), and an envelope that is not one."""
    shown = records_of(api, f"/accounts/{A}/operations?include_failed=true&type=manage_data")
    assert len(shown) == 1
    assert (shown[0]["name"], shown[0]["value"], shown[0]["transaction_successful"]) == ("gone", "", False)
    assert shown[0]["data_change"] == {"request": "unknown", "told_by": None, "effect": None}
    assert "data_change" not in api.get(f"/operations/{toid(100, 0, 1)}").json()


def test_manage_data_the_envelope_and_the_effect_disagree_about_is_unknown(make_database: Callable[[], str]) -> None:
    engine = create_engine(make_database())
    try:
        transaction, operations, source = manage_data_history(400)
        effects = [effect_record(toid(400, 0, 1), source, "data_removed", 41, transaction["created_at"], name="config")]
        stored(engine, (transaction, operations, effects))
        shown = records_of(mounted(engine, api_access="open"), f"/accounts/{source}/operations")
        assert shown[0]["data_change"] == {"request": "unknown", "told_by": None, "effect": "data_removed"}
        assert shown[1]["data_change"] == {"request": "set", "told_by": "xdr", "value": "", "effect": None}
    finally:
        engine.dispose()


def test_manage_data_is_filtered_by_account_type_name_and_value(make_database: Callable[[], str]) -> None:
    engine = create_engine(make_database())
    try:
        transaction, operations, source = manage_data_history(200)
        stored(engine, (transaction, operations, []))
        api = mounted(engine, api_access="open")
        path = f"/accounts/{source}/operations"

        def ids(**query: Any) -> list[str]:
            return [record["id"] for record in records_of(api, with_query(path, **query))]

        first, second, third = (str(toid(200, 0, number)) for number in (1, 2, 3))
        assert ids() == [first, second, third]
        assert ids(type="manage_data") == ids(type="10") == [first, second, third]
        assert ids(type="payment") == []
        assert ids(data_name="config") == [first]
        assert ids(data_name="gone") == [third]
        assert ids(data_name="never") == []
        assert ids(data_value="") == [second, third]  # the empty string: deletions and empty values alike
        assert ids(data_value="aGVsbG8=") == [first]
        narrowed = api.get(with_query(path, type="manage_data", data_name="blank", data_value="", limit=1)).json()
        assert [record["id"] for record in narrowed["_embedded"]["records"]] == [second]
        for link in narrowed["_links"].values():
            assert "type=manage_data" in link["href"] and "data_name=blank" in link["href"]
            assert "data_value=" in link["href"]
        # Another account sees none of them: the source is the only participant of a manage_data operation.
        assert records_of(api, f"/accounts/{B}/operations?type=manage_data") == []
    finally:
        engine.dispose()


# -- trade aggregations -----------------------------------------------------------------


def buckets(api: TestClient, base: str, counter: str, **params: Any) -> dict[str, Any]:
    """The hourly buckets of the first hour of the fake history, unless told otherwise."""
    response = api.get(
        "/trade_aggregations",
        params={
            **asset_params("base_", base),
            **asset_params("counter_", counter),
            "start_time": T0_MS,
            "end_time": T0_MS + HOUR_MS,
            "resolution": HOUR_MS,
            **params,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def priced(
    stamp: int, base_volume: int, counter_volume: int, trades: int, prices: dict[str, tuple[int, int]]
) -> dict[str, Any]:
    """A bucket as it is expected: volumes in stroops, every price as numerator and denominator."""
    bucket: dict[str, Any] = {
        "timestamp": str(stamp),
        "trade_count": str(trades),
        "base_volume": amount(base_volume),
        "counter_volume": amount(counter_volume),
        "avg": seven(counter_volume, base_volume),
    }
    for name, (numerator, denominator) in prices.items():
        bucket[name], bucket[f"{name}_r"] = seven(numerator, denominator), {"n": str(numerator), "d": str(denominator)}
    return bucket


BOOK = {"high": (47, 10), "low": (47, 10), "open": (47, 10), "close": (47, 10)}


def test_buckets_are_what_the_trades_add_up_to(api: TestClient) -> None:
    served = buckets(api, RECT, EUR)
    assert served["scope"]["kind"] == "market" and "counter_asset=EURMTL%3A" in served["scope"]["coverage"]
    # Two trades of 4.9325436 MTLRECT for 23.1829549 EURMTL, at 4.7.
    assert served["_embedded"]["records"] == [priced(T0_MS, 2 * 49325436, 2 * 231829549, 2, BOOK)]

    # The same market the other way round: the volumes exchange places, the prices are the reciprocals.
    turned = {"high": (10, 47), "low": (10, 47), "open": (10, 47), "close": (10, 47)}
    assert buckets(api, EUR, RECT)["_embedded"]["records"] == [priced(T0_MS, 2 * 231829549, 2 * 49325436, 2, turned)]

    # A trade through a liquidity pool: the price is what was bought for what was sold.
    exact = (7126809730, 49128652874)
    pool = {name: exact for name in BOOK}
    assert buckets(api, EUR, MTL)["_embedded"]["records"] == [
        priced(T0_MS, 49128652874, 7126809730, 1, pool)  # the fraction as stored, not reduced
    ]
    reciprocal = {name: (exact[1], exact[0]) for name in BOOK}
    assert buckets(api, MTL, EUR)["_embedded"]["records"] == [priced(T0_MS, 7126809730, 49128652874, 1, reciprocal)]
    assert buckets(api, RECT, MTL)["_embedded"]["records"] == []  # nothing was ever traded here
    # A day holds the same two trades in one bucket: it starts on midnight UTC.
    assert buckets(api, RECT, EUR, resolution=86_400_000, end_time=T0_MS + 86_400_000)["_embedded"]["records"] == [
        priced(T0_MS, 2 * 49325436, 2 * 231829549, 2, BOOK)
    ]


def test_buckets_are_what_the_fake_source_aggregates(loaded: Pilot) -> None:
    """The same trades through the formulas of the fake server, which were
    written from the source's own."""
    api = mounted(loaded.engine, api_access="open")
    for base, counter in ((RECT, EUR), (EUR, RECT), (EUR, MTL), (MTL, EUR)):
        params = {
            **asset_params("base_", base),
            **asset_params("counter_", counter),
            "start_time": str(T0_MS),
            "end_time": str(T0_MS + HOUR_MS),
            "resolution": str(HOUR_MS),
            "limit": "200",
        }
        theirs = loaded.fake._aggregations(params)["_embedded"]["records"]
        mine = api.get("/trade_aggregations", params=params).json()["_embedded"]["records"]
        assert mine and [rationals(record) for record in mine] == [rationals(record) for record in theirs], (base, counter)
        if MTL not in (base, counter):
            assert mine == theirs  # book prices: the fake reduces what is reduced already


def test_buckets_are_cut_on_the_resolution_and_only_where_trades_are(api: TestClient) -> None:
    period = {"start_time": T0_MS, "end_time": T0_MS + 3 * MINUTE_MS, "resolution": MINUTE_MS}
    stamps = [record["timestamp"] for record in buckets(api, RECT, EUR, **period)["_embedded"]["records"]]
    # A trade 20 seconds after the first ledger, another 60: two buckets of a minute.
    assert stamps == [str(T0_MS), str(T0_MS + MINUTE_MS)]
    served = buckets(api, RECT, EUR, **period, limit=1)["_embedded"]["records"]
    assert [record["timestamp"] for record in served] == stamps[:1]
    served = buckets(api, RECT, EUR, **period, limit=1, offset=1)["_embedded"]["records"]
    assert [record["timestamp"] for record in served] == stamps[1:]
    served = buckets(api, RECT, EUR, **period, order="desc")["_embedded"]["records"]
    assert [record["timestamp"] for record in served] == stamps[::-1]
    # No bucket is invented for an interval in which nothing was traded.
    quiet = buckets(api, RECT, EUR, start_time=T0_MS + 2 * MINUTE_MS, end_time=T0_MS + 3 * MINUTE_MS, resolution=MINUTE_MS)
    assert quiet["_embedded"]["records"] == []
    for resolution in (5 * MINUTE_MS, 15 * MINUTE_MS, HOUR_MS, 86_400_000, 604_800_000):
        assert len(buckets(api, RECT, EUR, resolution=resolution)["_embedded"]["records"]) == 1


def test_open_close_high_and_low_are_the_prices_in_the_order_of_the_operations(make_database: Callable[[], str]) -> None:
    engine = create_engine(make_database())
    try:
        with Session(engine) as session, session.begin():
            first = f.add_transaction(session, 10, source_account=f.ALICE)
            second = f.add_transaction(session, 11, source_account=f.ALICE)
            f.add_operation(session, first)
            f.add_operation(session, second)
            f.add_trade(session, first + 1, 0, price=(5, 1), base_amount=10_000_000, counter_amount=50_000_000)
            f.add_trade(session, first + 1, 1, price=(3, 1), base_amount=20_000_000, counter_amount=60_000_000)
            f.add_trade(session, second + 1, 0, price=(4, 1), base_amount=30_000_000, counter_amount=120_000_000)
        start = int(f.GENESIS_TIME.timestamp() * 1000)  # a whole minute: both ledgers close inside it
        api = mounted(engine, api_access="open")

        def ask(base: str, counter: str) -> list[dict[str, Any]]:
            response = api.get(
                "/trade_aggregations",
                params={
                    **asset_params("base_", base),
                    **asset_params("counter_", counter),
                    "start_time": start,
                    "end_time": start + MINUTE_MS,
                    "resolution": MINUTE_MS,
                },
            )
            assert response.status_code == 200, response.text
            return response.json()["_embedded"]["records"]

        assert ask(f.EUR, f.USD) == [
            priced(start, 60_000_000, 230_000_000, 3, {"high": (5, 1), "low": (3, 1), "open": (5, 1), "close": (4, 1)})
        ]
        # The other way round every price is the reciprocal: the high of one is the low of the other.
        assert ask(f.USD, f.EUR) == [
            priced(start, 230_000_000, 60_000_000, 3, {"high": (1, 3), "low": (1, 5), "open": (1, 5), "close": (1, 4)})
        ]
    finally:
        engine.dispose()


def test_a_bucket_with_nothing_on_the_base_side_has_no_average(make_database: Callable[[], str]) -> None:
    """The average is counter over base. A bucket whose base side is all zeros
    has none, and no prices are invented for it."""
    engine = create_engine(make_database())
    try:
        with Session(engine) as session, session.begin():
            transaction = f.add_transaction(session, 10, source_account=f.ALICE)  # closes 45 s after the start
            operation = f.add_operation(session, transaction)
            f.add_trade(session, operation, 0, base_amount=0, counter_amount=5_000_000)
        start = int(f.GENESIS_TIME.timestamp() * 1000)
        response = mounted(engine, api_access="open").get(
            "/trade_aggregations",
            params={
                **asset_params("base_", f.EUR),
                **asset_params("counter_", f.USD),
                "start_time": start,
                "end_time": start + MINUTE_MS,
                "resolution": MINUTE_MS,
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()["_embedded"]["records"] == [
            {"timestamp": str(start), "trade_count": "1", "base_volume": "0.0000000", "counter_volume": "0.5000000"}
        ]
    finally:
        engine.dispose()


# -- who reads -----------------------------------------------------------------------------


def test_open_access_lets_anyone_in(loaded: Pilot) -> None:
    api = mounted(loaded.engine, api_access="open")
    for name, path in every_path(loaded).items():
        response = api.get(path)
        assert response.status_code == 200, name
        assert "www-authenticate" not in response.headers


def test_a_configured_token_lets_its_holder_in(loaded: Pilot) -> None:
    api = mounted(loaded.engine, api_access="token", api_tokens=("first-secret", "second-secret"))
    for name, path in every_path(loaded).items():
        for header in ("Bearer second-secret", "bearer second-secret", "Bearer first-secret"):
            response = api.get(path, headers={"Authorization": header})
            assert response.status_code == 200, (name, header)
            assert "secret" not in response.text


def test_a_wrong_or_missing_token_is_refused(loaded: Pilot) -> None:
    api = mounted(loaded.engine, api_access="token", api_tokens=("the-secret",))
    refused = {
        "type": "about:blank",
        "title": "Unauthorized",
        "status": 401,
        "detail": "the history API is read with a token: Authorization: Bearer <token>",
    }
    for name, path in every_path(loaded).items():
        for headers in (
            {},
            {"Authorization": "Bearer wrong-secret"},
            {"Authorization": "Bearer wrong-secret extra"},
            {"Authorization": "Bearer"},
            {"Authorization": "Basic the-secret"},
            {"Authorization": "the-secret"},
        ):
            response = api.get(path, headers=headers)
            assert response.status_code == 401, (name, headers)
            assert response.headers["www-authenticate"] == 'Bearer realm="history-api"'
            assert response.json() == refused
            assert "the-secret" not in response.text
        # A token is taken from its header, and from nowhere else.
        assert api.get(with_query(path, token="the-secret"), headers={"X-Access-Token": "the-secret"}).status_code == 401
        assert api.get(with_query(path, access_token="the-secret")).status_code == 401


def test_token_access_without_a_token_configured_is_closed(loaded: Pilot) -> None:
    closed = {
        "type": "about:blank",
        "title": "Forbidden",
        "status": 403,
        "detail": "the history API needs a token and none is configured (MTL_HORIZON_API_TOKENS), so it is closed",
    }
    # Token access is the default; with no token it lets nobody in, whoever asks.
    for options in ({"api_access": "token"}, {"api_access": "token", "api_tokens": ()}, {}):
        api = mounted(loaded.engine, **options)
        for name, path in every_path(loaded).items():
            for headers in ({}, {"Authorization": "Bearer anything"}, {"Authorization": "Bearer "}):
                response = api.get(path, headers=headers)
                assert response.status_code == 403, (name, options, headers)
                assert response.json() == closed


def test_without_a_database_every_route_answers_503(loaded: Pilot) -> None:
    for options in ({"api_access": "open"}, {"api_access": "token", "api_tokens": ("a-secret",)}):
        api = mounted(None, **options)
        for name, path in every_path(loaded).items():
            response = api.get(path, headers={"Authorization": "Bearer a-secret"})
            assert response.status_code == 503, (name, options)
            assert response.headers["content-type"].startswith("application/problem+json")
            assert response.json() == {
                "type": "https://stellar.org/horizon-errors/service_unavailable",
                "title": "Service Unavailable",
                "status": 503,
                "detail": "there is no database to read history from",
            }


# -- where the links lead ------------------------------------------------------------------


def live_paths() -> list[str]:
    return [
        f"/transactions/{OUTER_HASH}",
        f"/transactions/{INNER_HASH}",
        f"/transactions/{INNER_HASH}/operations",
        f"/transactions/{OUTER_HASH}/effects",
        f"/operations/{LIVE_OPERATION_ID}",
        f"/operations/{LIVE_OPERATION_ID}/effects",
        f"/accounts/{LIVE_SOURCE}/transactions",
        f"/accounts/{LIVE_SOURCE}/operations",
        f"/accounts/{LIVE_SOURCE}/effects",
        f"/accounts/{LIVE_SOURCE}/trades",
        "/transactions",
        "/operations",
        "/effects",
        "/trades",
        f"/trades?{market(LIVE_RECT, LIVE_EUR)}",
        f"/v1/coverage?transaction={OUTER_HASH}",
        f"/v1/coverage?account={LIVE_SOURCE}",
        f"/v1/coverage?base_asset={LIVE_EUR}&counter_asset={LIVE_RECT}",
    ]


def test_no_link_names_the_server_the_history_came_from(loaded: Pilot, live_archive: Engine) -> None:
    """The stored answers carry the links of the server that gave them; none
    of those links is served."""
    archives = (
        (loaded.engine, list(every_path(loaded).values()), "horizon.test"),
        (live_archive, live_paths(), "horizon.stellar.org"),
    )
    for engine, paths, origin in archives:
        if engine is live_archive:
            with engine.connect() as connection:
                stored_links = connection.execute(
                    text("SELECT count(*) FROM history_transactions WHERE raw::text LIKE :origin"), {"origin": f"%{origin}%"}
                ).scalar()
            assert stored_links, "the archive holds no link of the source; the test would prove nothing"
        api = mounted(engine, api_access="open")
        for path in paths:
            response = api.get(path)
            assert response.status_code == 200, (origin, path)
            for href in hrefs(response.json()):
                assert href == "" or href.startswith("http://testserver/"), (origin, path, href)
            if not path.startswith("/v1/coverage"):
                assert origin not in response.text, (origin, path)


def test_links_name_the_public_origin_when_one_is_set(loaded: Pilot) -> None:
    api = mounted(loaded.engine, api_access="open", public_origin="https://horizon.example.org")
    for name, path in every_path(loaded).items():
        response = api.get(path)
        assert response.status_code == 200, name
        assert "testserver" not in response.text, name
        for href in hrefs(response.json()):
            assert href == "" or href.startswith("https://horizon.example.org/"), (name, href)
        scope = response.json().get("scope", {})
        if "coverage" in scope:
            assert scope["coverage"].startswith("/v1/coverage?")  # a path: the host is wherever this was asked


def test_no_answer_holds_a_float(loaded: Pilot, live_archive: Engine) -> None:
    """Amounts and prices are strings, as Horizon serves them."""
    for engine, paths in ((loaded.engine, every_path(loaded).values()), (live_archive, live_paths())):
        api = mounted(engine, api_access="open")
        for path in paths:
            response = api.get(path)
            assert response.status_code == 200 and not holds_a_float(response.json()), path


# -- what is refused ------------------------------------------------------------------------


def test_what_the_archive_does_not_have_is_a_404(api: TestClient) -> None:
    """A 404 says the archive does not have it. It does not say the network
    does not: the answer says so itself."""
    nowhere = "0" * 64
    for path in (
        f"/transactions/{nowhere}",
        f"/transactions/{nowhere}/operations",
        f"/transactions/{nowhere}/effects",
        "/operations/1",
        f"/operations/{toid(999_999, 0, 1)}",
        f"/operations/{toid(100)}/effects",  # a transaction's id is not an operation's
        f"/v1/coverage?transaction={nowhere}",
    ):
        response = api.get(path)
        assert response.status_code == 404, path
        assert response.headers["content-type"].startswith("application/problem+json")
        body = response.json()
        assert (body["type"], body["title"], body["status"]) == (
            "https://stellar.org/horizon-errors/not_found",
            "Resource Missing",
            404,
        )
        assert "not in this archive" in body["detail"] and "nothing about the network" in body["detail"]
        assert set(body) == {"type", "title", "status", "detail"}


def test_what_cannot_be_read_is_a_400_naming_the_parameter(api: TestClient) -> None:
    hour = f"{market(RECT, EUR)}&start_time={T0_MS}&end_time={T0_MS + HOUR_MS}"
    credit = f"base_asset_code=MTLRECT&base_asset_issuer={ISSUER}"
    refused = [
        ("/accounts/not-an-address/transactions", "account_id"),
        (f"/accounts/{A.lower()}/transactions", "account_id"),
        (f"/accounts/{A}/transactions?cursor=abc", "cursor"),
        (f"/accounts/{A}/transactions?cursor=-1", "cursor"),
        (f"/accounts/{A}/transactions?cursor=1-2", "cursor"),
        (f"/accounts/{A}/transactions?cursor=99999999999999999999", "cursor"),
        (f"/accounts/{A}/effects?cursor=1-2-3", "cursor"),
        (f"/accounts/{A}/trades?cursor=-", "cursor"),
        (f"/accounts/{A}/trades?cursor=12a", "cursor"),
        (f"/accounts/{A}/transactions?limit=0", "limit"),
        (f"/accounts/{A}/transactions?limit=201", "limit"),
        (f"/accounts/{A}/transactions?limit=-5", "limit"),
        (f"/accounts/{A}/transactions?limit=many", "limit"),
        (f"/accounts/{A}/transactions?order=sideways", "order"),
        (f"/accounts/{A}/operations?include_failed=maybe", "include_failed"),
        (f"/accounts/{A}/trades?trade_type=swap", "trade_type"),
        (f"/accounts/{A}/operations?type=Manage_Data", "type"),
        (f"/accounts/{A}/operations?data_name=", "data_name"),
        (f"/accounts/{A}/operations?data_value=***", "data_value"),
        (f"/transactions/{OUTER_HASH.upper()}", "transaction_hash"),
        (f"/transactions/{OUTER_HASH[:-1]}", "transaction_hash"),
        (f"/transactions/{'0' * 64}/operations?cursor=1-2", "cursor"),
        ("/operations/twelve", "id"),
        (f"/operations/{2**64}", "id"),
        (f"/operations/{LIVE_OPERATION_ID}/effects?cursor=1-2-3", "cursor"),
        ("/trades?base_asset_type=native", "base_asset_type"),
        ("/trades?counter_asset_type=native", "base_asset_type"),
        (f"/trades?base_asset_code=MTL&base_asset_issuer={ISSUER}&counter_asset_type=native", "base_asset_type"),
        (f"/trades?base_asset_type=pool&{credit}&counter_asset_type=native", "base_asset_type"),
        ("/trades?base_asset_type=native&base_asset_code=XLM&counter_asset_type=native", "base_asset_type"),
        (f"/trades?base_asset_type=credit_alphanum4&{credit}&counter_asset_type=native", "base_asset_code"),
        (f"/trades?base_asset_type=credit_alphanum4&base_asset_code=MTL&counter_asset_type=native", "base_asset_code"),
        (f"/trades?base_asset_type=credit_alphanum12&base_asset_code=MTLRECT&base_asset_issuer=GARBAGE&counter_asset_type=native", "base_asset_issuer"),  # fmt: skip
        (f"/trades?base_asset_type=native&base_asset_issuer={ISSUER}&counter_asset_type=native", "base_asset_type"),
        ("/trade_aggregations", "base_asset_type"),
        (f"/trade_aggregations?{market(RECT, EUR)}", "start_time"),
        (f"/trade_aggregations?{market(RECT, EUR)}&start_time={T0_MS}", "end_time"),
        (f"/trade_aggregations?{market(RECT, EUR)}&start_time={T0_MS}&end_time={T0_MS}", "end_time"),
        (f"/trade_aggregations?{hour}", "resolution"),
        (f"/trade_aggregations?{hour}&resolution=12345", "resolution"),
        (f"/trade_aggregations?{hour}&resolution={MINUTE_MS}&limit=0", "limit"),
        (f"/trade_aggregations?{hour}&resolution={MINUTE_MS}&offset=-1", "offset"),
        (f"/trade_aggregations?{hour}&resolution={MINUTE_MS}&order=down", "order"),
        ("/v1/coverage", "subject"),
        (f"/v1/coverage?account={A}&transaction={OUTER_HASH}", "subject"),
        (f"/v1/coverage?base_asset={RECT}", "subject"),
        (f"/v1/coverage?account=not-an-address", "account_id"),
        ("/v1/coverage?transaction=zz", "transaction_hash"),
        ("/v1/coverage?base_asset=MTL&counter_asset=native", "asset"),
        ("/v1/coverage?base_asset=MTL:X&counter_asset=native", "asset"),
    ]
    for path, field in refused:
        response = api.get(path)
        assert response.status_code == 400, path
        assert response.headers["content-type"].startswith("application/problem+json")
        body = response.json()
        assert (body["type"], body["title"], body["status"]) == (
            "https://stellar.org/horizon-errors/bad_request",
            "Bad Request",
            400,
        )
        assert body["extras"] == {"invalid_field": field}, path
        assert body["detail"]


# -- what a read costs, and what a failure looks like ---------------------------------------------


def test_reads_are_read_only_and_bounded_in_time(loaded: Pilot, monkeypatch: pytest.MonkeyPatch) -> None:
    engine = loaded.engine
    with pytest.raises(DBAPIError) as refused:
        with reading.reading(engine) as connection:
            connection.execute(text("UPDATE history_ledgers SET close_time = close_time"))
    assert refused.value.orig.sqlstate == "25006"  # read-only transaction

    monkeypatch.setattr(reading, "STATEMENT_TIMEOUT_MS", 1)
    with pytest.raises(OperationalError) as stopped:
        with reading.reading(engine) as connection:
            connection.execute(text("SELECT pg_sleep(0.2)"))
    assert reading.timed_out(stopped.value)

    # The settings were the transaction's: the connection went back to the pool as it was.
    with engine.connect() as connection:
        assert connection.execute(text("SHOW statement_timeout")).scalar() == "0"
        connection.execute(text("UPDATE history_ledgers SET close_time = close_time WHERE false"))


def test_a_stopped_query_and_a_failed_one_leave_as_problems(loaded: Pilot, monkeypatch: pytest.MonkeyPatch) -> None:
    api = mounted(loaded.engine, api_access="open")
    cancelled = OperationalError("SELECT ...", {}, psycopg.OperationalError("canceling statement due to statement timeout"))
    cancelled.orig.sqlstate = "57014"
    monkeypatch.setattr(reading, "rows", raising(cancelled))
    response = api.get("/effects")
    assert response.status_code == 503
    assert response.json()["type"] == "https://stellar.org/horizon-errors/service_unavailable"
    assert "Ask more narrowly" in response.json()["detail"]

    monkeypatch.setattr(reading, "rows", raising(InternalError("SELECT ...", {}, RuntimeError("boom"))))
    response = api.get("/effects")
    assert response.status_code == 500
    assert response.json() == {
        "type": "https://stellar.org/horizon-errors/internal_server_error",
        "title": "Internal Server Error",
        "status": 500,
        "detail": "the request could not be answered",
    }
    assert "boom" not in response.text


def test_a_database_that_does_not_answer_is_a_503() -> None:
    """Nothing listens on this port. The answer says it is the database, and
    nothing of the way to it."""
    engine = create_engine("postgresql+psycopg://user:not-a-password@127.0.0.1:1/mtl_horizon")
    try:
        response = mounted(engine, api_access="open").get("/transactions")
        assert response.status_code == 503
        assert response.json() == {
            "type": "https://stellar.org/horizon-errors/service_unavailable",
            "title": "Service Unavailable",
            "status": 503,
            "detail": "the database does not answer at the moment; ask again later",
        }
        assert "not-a-password" not in response.text
    finally:
        engine.dispose()


def raising(error: Exception) -> Callable[..., Any]:
    def refuse(*args: Any, **options: Any) -> Any:
        raise error

    return refuse


# -- what a page costs: the plans -------------------------------------------------------------


PAD = "Q" * 100  # a stored answer is a few kilobytes: XDR and JSON. A page of the table is as full as a real one.
FIRST_LEDGER, BULK_LEDGERS = 1000, 1500  # 1500 ledgers: 7500 transactions, as many operations, 15000 effects, 3000 trades


def bulk_history(engine: Engine, ledgers: int = BULK_LEDGERS) -> None:
    """A few thousand rows as the archive holds them, written in bulk: every
    ledger closes five transactions of five accounts, each with one operation
    and two effects, and two trades are made on two of the markets. The rows
    are as wide as real ones: the plans depend on how many pages a table is."""
    crowd = [f.address(1000 + number) for number in range(50)]
    ledger_rows: list[dict[str, Any]] = []
    transactions: list[dict[str, Any]] = []
    operations: list[dict[str, Any]] = []
    effects: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    of_transaction: list[dict[str, Any]] = []
    of_operation: list[dict[str, Any]] = []
    for index in range(ledgers):
        sequence = 1000 + index
        closed = f.close_time(sequence)
        ledger_rows.append({"sequence": sequence, "close_time": closed})
        # Account A is the source of one transaction in every tenth ledger; the rest is other accounts.
        sources = [A if index % 10 == 0 and number == 0 else crowd[(index + number) % 50] for number in range(5)]
        for order, source in enumerate(sources):
            values = f.transaction_values(
                sequence, order, source_account=source, transaction_hash=f.digest(sequence * 8 + order)
            )
            values.update(
                envelope_xdr=PAD * 12,
                result_xdr=PAD * 4,
                fee_meta_xdr=PAD * 12,
                raw={"id": values["transaction_hash"], "memo_type": "none", "preconditions": PAD * 15},
            )
            destination = crowd[(index + order + 11) % 50]
            transactions.append(values)
            for account in (source, destination):
                of_transaction.append({"transaction_id": values["id"], "account_id": account})
            operation = {
                "id": values["id"] + 1,
                "transaction_id": values["id"],
                "application_order": 1,
                "source_account": source,
                "type": "payment",
                "type_i": 1,
                "ledger_close_time": closed,
                "raw": {
                    "id": str(values["id"] + 1),
                    "type": "payment",
                    "amount": "1.0000000",
                    **asset_params("", EUR),
                    "padding": PAD * 3,
                },
            }
            operations.append(operation)
            for account in (source, destination):
                of_operation.append({"operation_id": operation["id"], "account_id": account})
            for number, account, kind, type_i in ((1, destination, "account_credited", 2), (2, source, "account_debited", 3)):
                effects.append(
                    {
                        "id": f.effect_id(operation["id"], number),
                        "paging_token": f.pair_paging_token(operation["id"], number),
                        "operation_id": operation["id"],
                        "effect_order": number,
                        "account_id": account,
                        "type": kind,
                        "type_i": type_i,
                        "ledger_close_time": closed,
                        "raw": {"id": f.effect_id(operation["id"], number), "type": kind, "padding": PAD},
                    }
                )
        # Both trades are made by the operation of the ledger's first
        # transaction; A is a side of the second every fifth ledger.
        operation_id = operations[-5]["id"]
        for number, (base, counter) in enumerate(((RECT, EUR), (EUR, MTL))):
            seller = crowd[(index + number) % 50]
            buyer = A if number == 1 and index % 5 == 0 else crowd[(index + number + 17) % 50]
            trades.append(
                {
                    "id": f.pair_paging_token(operation_id, number),
                    "operation_id": operation_id,
                    "trade_order": number,
                    "ledger_close_time": closed,
                    "base_asset_id": base,
                    "counter_asset_id": counter,
                    "base_amount": 10_000_000 + number,
                    "counter_amount": 30_000_000 + number,
                    "base_account_id": seller,
                    "counter_account_id": buyer,
                    "base_liquidity_pool_id": None,
                    "counter_liquidity_pool_id": None,
                    "base_offer_id": 100 + number,
                    "counter_offer_id": None,
                    "base_is_seller": True,
                    "price_n": 3,
                    "price_d": 1,
                    "trade_type": "orderbook",
                    "liquidity_pool_fee_bp": None,
                    "raw": {
                        "id": f.pair_paging_token(operation_id, number),
                        **asset_params("base_", base),
                        **asset_params("counter_", counter),
                        "padding": PAD * 3,
                    },
                }
            )
    with engine.begin() as connection:
        connection.execute(insert(HistoryAccount), [{"account_id": account} for account in {A, *crowd}])
        connection.execute(
            insert(HistoryAsset), [{"asset_id": asset, **store.asset_parts(asset)} for asset in (RECT, EUR, MTL)]
        )
        for table, rows in (
            (HistoryLedger, ledger_rows),
            (HistoryTransaction, transactions),
            (HistoryOperation, operations),
            (HistoryTransactionParticipant, of_transaction),
            (HistoryOperationParticipant, of_operation),
            (HistoryEffect, effects),
            (HistoryTrade, trades),
        ):
            connection.execute(insert(table), rows)


def explain(connection: Connection, statement: Any) -> str:
    """The plan PostgreSQL ran, with what it actually did."""
    compiled = statement.compile(dialect=connection.dialect, compile_kwargs={"literal_binds": True})
    lines = connection.execute(text("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) " + str(compiled))).all()
    return "\n".join(line[0] for line in lines)


MIDDLE = FIRST_LEDGER + BULK_LEDGERS // 2  # a ledger in the middle of the bulk history
WAYS = ("forwards", "forwards from a cursor", "backwards", "backwards from a cursor")


def arguments(kind: str, way: str, limit: int) -> dict[str, Any]:
    """The page arguments of one page of a stream: ``way`` says where it starts
    and where it goes. A cursor in the middle is the first operation of the
    middle ledger, or that ledger's second transaction."""
    at = f"{toid(MIDDLE, 0, 1)}-0" if kind in (EFFECT, TRADE) else str(toid(MIDDLE, 2))
    size = str(limit)
    return {
        "forwards": {"limit": size},
        "forwards from a cursor": {"limit": size, "cursor": at},
        "backwards": {"limit": size, "order": "desc"},
        "backwards from a cursor": {"limit": size, "order": "desc", "cursor": at},
    }[way]


def page_statements(limit: int) -> list[tuple[str, bool, Any]]:
    """(name, whether it is the stream of an account, the statement): a page of
    ``limit`` records of every stream, each of the four ways."""
    out: list[tuple[str, bool, Any]] = []
    for way in WAYS:
        out.append(
            (
                f"account transactions, {way}",
                True,
                reading.transactions_statement(
                    parse(TRANSACTION, arguments(TRANSACTION, way, limit)), account=A, include_failed=True
                ),
            )
        )
        out.append(
            (
                f"account operations, {way}",
                True,
                reading.operations_statement(
                    parse(OPERATION, arguments(OPERATION, way, limit)), account=A, transaction_id=None, include_failed=True
                ),
            )
        )
        out.append(
            (
                f"account effects, {way}",
                True,
                reading.effects_statement(parse(EFFECT, arguments(EFFECT, way, limit)), account=A, span=None),
            )
        )
        out.append(
            (
                f"account trades, {way}",
                True,
                reading.trades_statement(
                    parse(TRADE, arguments(TRADE, way, limit)), account=A, pair=None, bound=None, trade_type=None
                ),
            )
        )
    newest = {"limit": str(limit), "order": "desc"}
    at = f"{toid(MIDDLE, 0, 1)}-0"
    closed = f.close_time(MIDDLE)
    out.extend(
        [
            (
                "all transactions, backwards",
                False,
                reading.transactions_statement(parse(TRANSACTION, newest), account=None, include_failed=True),
            ),
            (
                "all operations, backwards",
                False,
                reading.operations_statement(
                    parse(OPERATION, newest), account=None, transaction_id=None, include_failed=True
                ),
            ),
            ("all effects, backwards", False, reading.effects_statement(parse(EFFECT, newest), account=None, span=None)),
            (
                "all trades, forwards from a cursor",
                False,
                reading.trades_statement(
                    parse(TRADE, {"limit": str(limit), "cursor": at}), account=None, pair=None, bound=None, trade_type=None
                ),
            ),
            (
                "market trades, backwards from the top",
                False,
                reading.trades_statement(
                    parse(TRADE, newest), account=None, pair=(RECT, EUR), bound=None, trade_type=None
                ),
            ),
            (
                "market trades, forwards from a cursor",
                False,
                reading.trades_statement(
                    parse(TRADE, {"limit": str(limit), "cursor": at}),
                    account=None,
                    pair=(RECT, EUR),
                    bound=closed,
                    trade_type=None,
                ),
            ),
            (
                "market trades, the other way round, backwards",
                False,
                reading.trades_statement(
                    parse(TRADE, newest), account=None, pair=(EUR, RECT), bound=None, trade_type=None
                ),
            ),
            (
                "aggregation of a day",  # reads every trade of its period: the one statement with no page
                False,
                reading.pair_trades_statement(RECT, EUR, closed - timedelta(hours=12), closed + timedelta(hours=12)),
            ),
        ]
    )
    return out


def test_a_page_is_a_walk_of_an_index_as_long_as_the_page(make_database: Callable[[], str]) -> None:
    """Several thousand rows, as wide as real ones; a page of every stream —
    from the start, from a cursor in the middle, forwards and backwards — is
    a walk of an index that reads a page's worth of rows, at the default limit
    and at the largest. The plans are printed: they are what the documentation
    reports."""
    engine = create_engine(make_database())
    try:
        bulk_history(engine)
        with engine.connect() as connection:
            plans = {}
            for limit in (10, 200):
                for name, of_an_account, statement in page_statements(limit):
                    label = f"[limit {limit}] {name}"
                    if of_an_account:
                        with reading.ordered(connection):
                            plans[label] = explain(connection, statement)
                    else:
                        plans[label] = explain(connection, statement)
            connection.rollback()
    finally:
        engine.dispose()
    for label, plan in plans.items():
        print(f"\n== {label}\n{plan}")
    for label, plan in plans.items():
        assert "Seq Scan" not in plan and "Bitmap" not in plan, label
    for label, plan in plans.items():
        if label.endswith("aggregation of a day"):
            continue
        limit = int(label.split("]")[0].split()[-1])
        read = max(int(count) for count in re.findall(r"actual rows=(\d+)", plan))
        # Both sides of an account's trades are walked: twice the page, and a little over.
        assert read <= 2 * (limit + 2) + 4, (label, read)
