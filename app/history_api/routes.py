"""The routes of the history API and what guards them all.

A Horizon-compatible, read-only subset of the history this installation has
collected, answered from PostgreSQL and from nowhere else: no Horizon is
asked, whatever the request. The paths are upstream's, so a client written
for Horizon finds what it knows:

    /accounts/{account_id}/transactions|operations|effects|trades
    /transactions[/{transaction_hash}[/operations|effects]]
    /operations[/{id}[/effects]]
    /trades                              /trade_aggregations
    /v1/coverage                         (of this installation, not upstream)

What a route answers is what the archive has. An empty or short page says that
nothing more is *here*; a 404 says that the archive does not have the thing.
Neither says anything about the network. Every page carries a ``scope``
member that says so, and points to ``/v1/coverage`` for specifics.

Three things hold for every route, in this order (:class:`Guarded`): without a
database every answer is a 503; a request that the access policy does not let
in gets a 401 or a 403 and nothing else; and a failure leaves as a problem
document, never as a half answer. The routes are read-only twice over: they
only call reads, and the reads run in read-only transactions.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from fastapi.routing import APIRoute
from sqlalchemy.engine import Connection, Engine, Row
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.backfill.jobs import asset_params
from app.config import Settings
from app.history_api import access, aggregations, coverage, manage_data, params, streams
from app.history_api.paging import EFFECT, OPERATION, TRADE, TRANSACTION, Paging, page, parse
from app.history_api.problems import NO_STORE, Problem, bad_request, internal_error, not_found, service_unavailable
from app.history_api.records import effect_record, operation_record, trade_record, transaction_record

logger = logging.getLogger(__name__)

Reader = Callable[[Connection, Paging], Sequence[Row]]
Found = Sequence[Row]

_NOTES = {
    "account": (
        "Only what this installation collected for this account is here. An empty or short page means that "
        "nothing more is in this archive, not that nothing happened on the network. What has been read, and "
        "what has not, is told by the coverage link."
    ),
    "market": (
        "Only what this installation collected for this market is here. An empty or short page means that "
        "nothing more is in this archive, not that nothing was traded on the network. What has been read, and "
        "what has not, is told by the coverage link."
    ),
    "archive": (
        "A local sample: what this installation collected for its watched accounts and markets, in the order "
        "of the cursor. It is not the stream of the network; what never came through a watched account or "
        "market is not here."
    ),
}
_NOT_WHOLE = (
    "The transaction was stored without its whole context: this list may be incomplete. What is stored, and "
    "what is not, is told by the coverage link."
)


def build_router(engine: Engine | None, settings: Settings) -> APIRouter:
    """The routes of the history API, to be included at the root of the
    application (their paths are upstream's, complete).

    ``engine`` is the database of the archive; None when the process has none,
    and then every route answers 503. ``settings`` says who may read
    (:mod:`app.history_api.access`) and where this installation is reached
    from (``public_origin``; without it, the base URL of the request).
    """

    class Guarded(APIRoute):
        """What every route passes before it runs and after it fails."""

        def get_route_handler(self) -> Callable[[Request], Any]:
            route = super().get_route_handler()

            async def guarded(request: Request) -> Response:
                if engine is None:
                    return service_unavailable("there is no database to read history from").response()
                refused = access.denied(request, settings)
                if refused is not None:
                    return refused.response()
                try:
                    return await route(request)
                except Problem as problem:
                    return problem.response()
                except OperationalError as error:
                    # The database did not answer, or stopped the statement. It
                    # is not the request that is wrong, and it may be asked again.
                    if streams.timed_out(error):
                        logger.warning("history API: GET %s was stopped by the statement timeout", request.url.path)
                        return service_unavailable(
                            "the question was stopped: it takes longer than this installation allows a query. "
                            "Ask more narrowly: a smaller page, a shorter period or a coarser resolution."
                        ).response()
                    logger.warning(
                        "history API: GET %s could not reach the database (%s)",
                        request.url.path,
                        type(error.orig).__name__,
                    )
                    return service_unavailable("the database does not answer at the moment; ask again later").response()
                except SQLAlchemyError:
                    logger.exception("history API: a query failed on GET %s", request.url.path)
                    return internal_error().response()
                except StarletteHTTPException:
                    raise
                except Exception:  # noqa: BLE001 - whatever it was, the answer is the same and the log has it
                    logger.exception("history API: GET %s could not be answered", request.url.path)
                    return internal_error().response()

            return guarded

    router = APIRouter(route_class=Guarded)

    # -- what the routes share ------------------------------------------------

    def base_url(request: Request) -> str:
        return settings.public_origin.rstrip("/") if settings.public_origin else str(request.base_url).rstrip("/")

    def hal(body: Mapping[str, Any]) -> JSONResponse:
        return JSONResponse(dict(body), media_type="application/hal+json", headers={"Cache-Control": NO_STORE})

    def plain(body: Mapping[str, Any]) -> JSONResponse:
        return JSONResponse(dict(body), headers={"Cache-Control": NO_STORE})

    def resolved(kind: str, query: Mapping[str, str], connection: Connection) -> Paging:
        paging = parse(kind, query)
        return paging.at_now(streams.latest_ledger(connection)) if paging.asks_now else paging

    def echo(query: Mapping[str, str], *names: str) -> dict[str, str]:
        """The parameters of the request that its links have to keep."""
        return {name: query[name] for name in names if name in query}

    def scope(kind: str, coverage_query: str | None = None) -> dict[str, Any]:
        member: dict[str, Any] = {"kind": kind, "note": _NOTES[kind]}
        if coverage_query is not None:
            member["coverage"] = coverage_query
        return {"scope": member}

    def market_query(wanted: tuple[str, str]) -> str:
        return urlencode(sorted({"base_asset": wanted[0], "counter_asset": wanted[1]}.items()))

    def of_one_transaction(asked: str, *, kind: str, part: str, whole: bool) -> dict[str, Any]:
        """The ``scope`` of a list that belongs to one transaction: whole, or
        only what the archive happened to store of it."""
        return {
            "scope": {
                "kind": kind,
                "context_loaded": whole,
                "note": f"The {part} of the transaction as the source serves them." if whole else _NOT_WHOLE,
                "coverage": f"/v1/coverage?transaction={asked}",
            }
        }

    def stream_page(
        request: Request,
        *,
        kind: str,
        path: str,
        filters: Mapping[str, str],
        scope_member: Mapping[str, Any],
        read: Reader,
        render: Callable[[Connection, Found], list[dict[str, Any]]],
    ) -> JSONResponse:
        query = params.first_values(request.query_params.multi_items())
        base = base_url(request)
        with streams.reading(engine) as connection:
            paging = resolved(kind, query, connection)
            found = read(connection, paging)
            records = render(connection, found[: paging.limit])
        return hal(page(base, path, filters, paging, records, scope_member))

    def include_failed(query: Mapping[str, str]) -> bool:
        return params.flag(query, "include_failed") is True

    def transaction_records(base: str, found: Found) -> list[dict[str, Any]]:
        # In a stream a transaction is the outer one, as upstream shows it.
        return [transaction_record(base, row, row.transaction_hash) for row in found]

    def operation_records(connection: Connection, base: str, found: Found) -> list[dict[str, Any]]:
        changes = manage_data.changes(connection, found)
        return [
            operation_record(base, row, changes[row.id].as_member() if row.id in changes else None) for row in found
        ]

    def effect_records(base: str, found: Found) -> list[dict[str, Any]]:
        return [effect_record(base, row) for row in found]

    def trade_records(base: str, found: Found, wanted: tuple[str, str] | None) -> list[dict[str, Any]]:
        if wanted is None:
            return [trade_record(base, row) for row in found]
        return [trade_record(base, row, reversed_side=row.base_asset_id != wanted[0]) for row in found]

    def find_transaction(connection: Connection, asked: str) -> Row:
        row = streams.transaction_by_hash(connection, asked)
        if row is None:
            raise not_found("the transaction is not in this archive; this says nothing about the network")
        return row

    def find_operation(connection: Connection, asked: int) -> Row:
        row = streams.operation(connection, asked)
        if row is None:
            raise not_found("the operation is not in this archive; this says nothing about the network")
        return row

    # -- of an account --------------------------------------------------------

    @router.get("/accounts/{account_id}/transactions")
    def account_transactions(request: Request, account_id: str) -> JSONResponse:
        account = params.account_id(account_id)
        query = params.first_values(request.query_params.multi_items())
        shown = include_failed(query)
        return stream_page(
            request,
            kind=TRANSACTION,
            path=f"/accounts/{account}/transactions",
            filters=echo(query, "include_failed"),
            scope_member=scope("account", f"/v1/coverage?account={account}"),
            read=lambda c, p: streams.transactions(c, p, account=account, include_failed=shown),
            render=lambda c, found: transaction_records(base_url(request), found),
        )

    @router.get("/accounts/{account_id}/operations")
    def account_operations(request: Request, account_id: str) -> JSONResponse:
        account = params.account_id(account_id)
        query = params.first_values(request.query_params.multi_items())
        shown = include_failed(query)
        sql_filters = streams.operation_filters(params.operation_filter(query), *params.data_filter(query))
        return stream_page(
            request,
            kind=OPERATION,
            path=f"/accounts/{account}/operations",
            filters=echo(query, "include_failed", "type", "data_name", "data_value"),
            scope_member=scope("account", f"/v1/coverage?account={account}"),
            read=lambda c, p: streams.operations(c, p, account=account, include_failed=shown, filters=sql_filters),
            render=lambda c, found: operation_records(c, base_url(request), found),
        )

    @router.get("/accounts/{account_id}/effects")
    def account_effects(request: Request, account_id: str) -> JSONResponse:
        account = params.account_id(account_id)
        return stream_page(
            request,
            kind=EFFECT,
            path=f"/accounts/{account}/effects",
            filters={},
            scope_member=scope("account", f"/v1/coverage?account={account}"),
            read=lambda c, p: streams.effects(c, p, account=account),
            render=lambda c, found: effect_records(base_url(request), found),
        )

    @router.get("/accounts/{account_id}/trades")
    def account_trades(request: Request, account_id: str) -> JSONResponse:
        account = params.account_id(account_id)
        query = params.first_values(request.query_params.multi_items())
        return stream_page(
            request,
            kind=TRADE,
            path=f"/accounts/{account}/trades",
            filters=echo(query, "trade_type"),
            scope_member=scope("account", f"/v1/coverage?account={account}"),
            read=lambda c, p: streams.trades(c, p, account=account, trade_type=params.trade_type(query)),
            render=lambda c, found: trade_records(base_url(request), found, None),
        )

    # -- one transaction ------------------------------------------------------

    @router.get("/transactions/{transaction_hash}")
    def one_transaction(request: Request, transaction_hash: str) -> JSONResponse:
        asked = params.transaction_hash(transaction_hash)
        with streams.reading(engine) as connection:
            record = transaction_record(base_url(request), find_transaction(connection, asked), asked)
        return hal(record)

    def of_a_transaction(request: Request, asked: str, part: str) -> JSONResponse:
        query = params.first_values(request.query_params.multi_items())
        base = base_url(request)
        with streams.reading(engine) as connection:
            # What the request asks is read before anything is looked for: a
            # request that cannot be understood is that, whether or not the
            # transaction is here.
            paging = resolved(OPERATION if part == "operations" else EFFECT, query, connection)
            row = find_transaction(connection, asked)
            loaded = row.context_loaded or []
            if part == "operations":
                # All of them, as many as the transaction says it has, is whole
                # too, whatever stream brought them.
                whole = "operations" in loaded or streams.operations_stored(connection, row.id) >= row.operation_count
                found = streams.operations(connection, paging, transaction_id=row.id)
                records = operation_records(connection, base, found[: paging.limit])
            else:
                whole = "effects" in loaded
                found = streams.effects(connection, paging, transaction=row)
                records = effect_records(base, found[: paging.limit])
        member = of_one_transaction(asked, kind="transaction", part=part, whole=whole)
        return hal(page(base, f"/transactions/{asked}/{part}", {}, paging, records, member))

    @router.get("/transactions/{transaction_hash}/operations")
    def transaction_operations(request: Request, transaction_hash: str) -> JSONResponse:
        return of_a_transaction(request, params.transaction_hash(transaction_hash), "operations")

    @router.get("/transactions/{transaction_hash}/effects")
    def transaction_effects(request: Request, transaction_hash: str) -> JSONResponse:
        return of_a_transaction(request, params.transaction_hash(transaction_hash), "effects")

    # -- one operation --------------------------------------------------------

    @router.get("/operations/{operation_id}")
    def one_operation(request: Request, operation_id: str) -> JSONResponse:
        with streams.reading(engine) as connection:
            row = find_operation(connection, params.operation_id(operation_id))
            record = operation_records(connection, base_url(request), [row])[0]
        return hal(record)

    @router.get("/operations/{operation_id}/effects")
    def operation_effects(request: Request, operation_id: str) -> JSONResponse:
        asked = params.operation_id(operation_id)
        query = params.first_values(request.query_params.multi_items())
        base = base_url(request)
        with streams.reading(engine) as connection:
            paging = resolved(EFFECT, query, connection)
            row = find_operation(connection, asked)
            found = streams.effects(connection, paging, operation_id=row.id)
            records = effect_records(base, found[: paging.limit])
            whole = "effects" in (streams.context_loaded(connection, row.transaction_id) or [])
        member = of_one_transaction(row.transaction_hash, kind="operation", part="effects", whole=whole)
        return hal(page(base, f"/operations/{asked}/effects", {}, paging, records, member))

    # -- the whole archive ----------------------------------------------------

    @router.get("/transactions")
    def all_transactions(request: Request) -> JSONResponse:
        query = params.first_values(request.query_params.multi_items())
        shown = include_failed(query)
        return stream_page(
            request,
            kind=TRANSACTION,
            path="/transactions",
            filters=echo(query, "include_failed"),
            scope_member=scope("archive"),
            read=lambda c, p: streams.transactions(c, p, account=None, include_failed=shown),
            render=lambda c, found: transaction_records(base_url(request), found),
        )

    @router.get("/operations")
    def all_operations(request: Request) -> JSONResponse:
        query = params.first_values(request.query_params.multi_items())
        shown = include_failed(query)
        return stream_page(
            request,
            kind=OPERATION,
            path="/operations",
            filters=echo(query, "include_failed"),
            scope_member=scope("archive"),
            read=lambda c, p: streams.operations(c, p, include_failed=shown),
            render=lambda c, found: operation_records(c, base_url(request), found),
        )

    @router.get("/effects")
    def all_effects(request: Request) -> JSONResponse:
        return stream_page(
            request,
            kind=EFFECT,
            path="/effects",
            filters={},
            scope_member=scope("archive"),
            read=streams.effects,
            render=lambda c, found: effect_records(base_url(request), found),
        )

    @router.get("/trades")
    def all_trades(request: Request) -> JSONResponse:
        query = params.first_values(request.query_params.multi_items())
        base = base_url(request)
        pair = params.requested_pair(query)
        kind_of_trade = params.trade_type(query)
        if pair is None:
            return stream_page(
                request,
                kind=TRADE,
                path="/trades",
                filters=echo(query, "trade_type"),
                scope_member=scope("archive"),
                read=lambda c, p: streams.trades(c, p, trade_type=kind_of_trade),
                render=lambda c, found: trade_records(base, found, None),
            )
        wanted = (pair[0].asset_id, pair[1].asset_id)
        return stream_page(
            request,
            kind=TRADE,
            path="/trades",
            filters={
                **echo(query, "trade_type"),
                **asset_params("base_", wanted[0]),
                **asset_params("counter_", wanted[1]),
            },
            scope_member=scope("market", "/v1/coverage?" + market_query(wanted)),
            # A market of one asset with itself has no trades: nothing to read.
            read=lambda c, p: (
                [] if wanted[0] == wanted[1] else streams.trades(c, p, pair=wanted, trade_type=kind_of_trade)
            ),
            render=lambda c, found: trade_records(base, found, wanted),
        )

    @router.get("/trade_aggregations")
    def trade_aggregations(request: Request) -> JSONResponse:
        query = params.first_values(request.query_params.multi_items())
        pair = params.requested_pair(query)
        if pair is None:
            raise bad_request("a market is named by both a base and a counter asset", field="base_asset_type")
        start = params.integer(query, "start_time", default=None, minimum=0)
        end = params.integer(query, "end_time", default=None, minimum=0)
        if end <= start:
            raise bad_request("end_time must be after start_time", field="end_time")
        resolution = params.integer(query, "resolution", default=None, minimum=1)
        if resolution not in aggregations.RESOLUTIONS:
            raise bad_request(
                "resolution must be one of " + ", ".join(str(value) for value in aggregations.RESOLUTIONS),
                field="resolution",
            )
        limit = params.integer(query, "limit", default=params.DEFAULT_LIMIT, minimum=1, maximum=params.MAX_LIMIT)
        offset = params.integer(query, "offset", default=0, minimum=0)
        descending = params.order(query) == "desc"
        wanted = (pair[0].asset_id, pair[1].asset_id)
        with streams.reading(engine) as connection:
            found = aggregations.buckets(connection, wanted[0], wanted[1], start, end, resolution)
        if descending:
            found.reverse()
        return plain(
            {
                "_embedded": {"records": found[offset : offset + limit]},
                **scope("market", "/v1/coverage?" + market_query(wanted)),
            }
        )

    # -- coverage -------------------------------------------------------------

    @router.get("/v1/coverage")
    def coverage_of(request: Request) -> JSONResponse:
        query = params.first_values(request.query_params.multi_items())
        chosen = [name for name in ("account", "transaction", "base_asset", "counter_asset") if query.get(name)]
        if chosen not in (["account"], ["transaction"], ["base_asset", "counter_asset"]):
            raise bad_request(
                "one subject: account, or transaction, or a market as base_asset and counter_asset",
                field="subject",
            )
        with streams.reading(engine) as connection:
            if chosen == ["account"]:
                body: Any = coverage.account(connection, params.account_id(query["account"]))
            elif chosen == ["transaction"]:
                body = coverage.transaction(connection, params.transaction_hash(query["transaction"]))
                if body is None:
                    raise not_found("the transaction is not in this archive; this says nothing about the network")
            else:
                base = params.asset_id(query["base_asset"]).asset_id
                counter = params.asset_id(query["counter_asset"]).asset_id
                body = coverage.market(connection, base, counter)
        return plain(body)

    return router
