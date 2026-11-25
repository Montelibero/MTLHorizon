"""The mini-explorer: what Horizon says now of one account — its lines and
balances — and of the assets one issuer issued; to help set the watch lists
up, and nothing more.

A page opens without a word to any server. Only an explicit ask — a form
with an address — reads Horizon, through the pool every reader uses (the
shared pace, the check of the network), from a client opened for that
reading and closed after it. Two paths are asked and no others, and an
address is checked before anything is sent; a page's cursor is an opaque
string. Nothing of what is read is stored. This is the source's current
answer at the moment shown — not a history, and not a blockchain explorer.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, TypeVar
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.engine import Engine

from app import cli_common
from app.admin import auth, lists, support
from app.admin.areas.discovery import MOST_REQUESTS, budget_of
from app.admin.door import Door
from app.config import ConfigError
from app.sources.horizon import HorizonError, Page
from app.sources.pool import HorizonPool
from app.storage import pace, service_jobs

logger = logging.getLogger(__name__)

PAGE = 50  # assets on one page of an issuer
# A page's cursor: opaque here — anything short and printable without a space.
CURSOR = re.compile(r"[!-~]{1,128}")
# How many pages back the links of an issuer's assets keep.
MOST_BACK = 50
# How much of Horizon's own words a page shows of an error.
SAID = 400
# What an entry brought in from here is said to be watched for.
REASON = "добавлен из обозревателя"

NATIVE, CREDIT, POOL_SHARE, OTHER = "native", "credit", "pool_share", "other"

T = TypeVar("T")


@dataclass(frozen=True)
class Reading:
    """One reading of Horizon: when it was tried, what it asked, who
    answered — and why nothing could be shown, when nothing could."""

    at: datetime
    requests: int
    servers: str
    error: str | None = None


@dataclass(frozen=True)
class Line:
    """One balance line of an account, as Horizon tells it now."""

    asset_id: str | None  # None: a line this code cannot name
    balance: str
    limit: str | None
    kind: str  # native | credit | pool_share | other
    type: str  # what Horizon calls it
    authorized: str
    changed_at_ledger: int | None
    pool_id: str | None

    @property
    def issuer(self) -> str | None:
        return None if self.asset_id is None or self.kind != CREDIT else self.asset_id.partition(":")[2]


@dataclass(frozen=True)
class AccountView:
    reading: Reading
    address: str
    xlm: Line | None
    lines: list[Line]
    sequence: str | None
    changed_at_ledger: int | None
    # The account's row in the watch list — None when it is not watched — and
    # the rules of those of its assets that are watched.
    watched: lists.Account | None
    flags: dict[str, tuple[bool, bool]]


@dataclass(frozen=True)
class Issued:
    """An asset as ``/assets`` tells of it. The rules are None when the asset
    is not watched here."""

    asset_id: str
    type: str
    amount: str | None
    accounts: str | None
    enabled: bool | None
    discover: bool | None


@dataclass(frozen=True)
class IssuerView:
    reading: Reading
    issuer: str
    assets: list[Issued]
    # To the pages after and before this one, through this page's own address.
    next_href: str | None
    previous_href: str | None
    number: int


# -- what was read, made fit to show ------------------------------------------------


def _shown(value: Any) -> str | None:
    """What was read, as one short line of text: it is shown and never
    trusted to be anything."""
    if value is None:
        return None
    return " ".join(str(value).split())[:80] or None


def _number(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _yes(value: Any) -> str:
    if isinstance(value, bool):
        return "да" if value else "нет"
    return "—"


def _line(shown: Mapping[str, Any]) -> Line:
    """One balance as Horizon tells it. A line is read for what it says it
    is, and one of a kind this code does not know is shown as that, not
    dropped."""
    code, issuer, pool_id = shown.get("asset_code"), shown.get("asset_issuer"), None
    kind, asset_id = OTHER, None
    if shown.get("asset_type") == "native":
        kind = NATIVE
    elif shown.get("asset_type") in ("credit_alphanum4", "credit_alphanum12") and isinstance(code, str) and isinstance(issuer, str):
        kind, asset_id = CREDIT, f"{code}:{issuer}"
    elif shown.get("asset_type") == "liquidity_pool_shares":
        kind, pool_id = POOL_SHARE, _shown(shown.get("liquidity_pool_id"))
    return Line(
        asset_id,
        _shown(shown.get("balance")) or "—",
        _shown(shown.get("limit")),
        kind,
        _shown(shown.get("asset_type")) or "—",
        _yes(shown.get("is_authorized")),
        _number(shown.get("last_modified_ledger")),
        pool_id,
    )


def _lines(record: Mapping[str, Any]) -> tuple[Line | None, list[Line]]:
    """The balances of an account record: XLM apart, then its trustlines and
    pool shares."""
    xlm: Line | None = None
    lines: list[Line] = []
    balances = record.get("balances")
    for balance in balances if isinstance(balances, list) else ():
        if not isinstance(balance, Mapping):
            continue
        told = _line(balance)
        if told.kind == NATIVE and xlm is None:
            xlm = told
        else:
            lines.append(told)
    return xlm, lines


def _issued(record: Mapping[str, Any], flags: Mapping[str, tuple[bool, bool]]) -> Issued | None:
    """An asset record of ``/assets``; None when it does not name an asset."""
    code, issuer = record.get("asset_code"), record.get("asset_issuer")
    if not isinstance(code, str) or not isinstance(issuer, str):
        return None
    watched = flags.get(f"{code}:{issuer}")
    return Issued(
        f"{code}:{issuer}",
        _shown(record.get("asset_type")) or "—",
        _shown(record.get("amount")),
        _shown(record.get("num_accounts")),
        None if watched is None else watched[0],
        None if watched is None else watched[1],
    )


# -- one reading ----------------------------------------------------------------------


def _names(pool: HorizonPool) -> str:
    """The servers that answered, by the names they are shown under: a name
    holds nothing of an address that may be secret."""
    return ", ".join(sorted(pool.stats.by_server))


def _read(door: Door, ask: Callable[[HorizonPool], T]) -> tuple[Reading, T | None]:
    """One explicit reading through the pool: a client is opened for it and
    closed after it, and the shared pace and the network check of the pool
    apply. When Horizon could not answer, the reading says why and when."""
    reading = Reading(datetime.now(UTC), 0, "")
    with cli_common.make_client(door.settings, door.engine, share=pace.BACKGROUND) as pool:
        try:
            found = ask(pool)
        except (HorizonError, ConfigError) as error:
            logger.warning("the explorer asked Horizon and could not read: %s", error)
            # What the pool says of it, and what it keeps of each server that failed it.
            said = " ".join(str(error).split())[:SAID]
            causes = [f"{server.name}: {server.last_error}" for server in pool.servers if server.last_error]
            if causes:
                said += " — " + " ".join("; ".join(causes).split())[:SAID]
            return replace(reading, requests=pool.stats.requests, servers=_names(pool), error=said), None
        return replace(reading, requests=pool.stats.requests, servers=_names(pool)), found


def _account(door: Door, address: str) -> AccountView:
    engine: Engine = door.engine
    reading, record = _read(door, lambda pool: pool.get(f"/accounts/{address}"))
    if record is None:
        return AccountView(reading, address, None, [], None, None, None, {})
    xlm, lines = _lines(record)
    return AccountView(
        reading,
        address,
        xlm,
        lines,
        _shown(record.get("sequence")),
        _number(record.get("last_modified_ledger")),
        lists.account(engine, address),
        lists.flags_of(engine, [line.asset_id for line in lines if line.asset_id is not None]),
    )


def trail_of(cursor: str, back: Sequence[str]) -> tuple[str, list[str]]:
    """Where this page of an issuer's assets begins, and where the pages
    before it did: opaque strings as this page's own links gave them. A
    ValueError when they are not — nothing is asked of Horizon then."""
    if (cursor and not CURSOR.fullmatch(cursor)) or len(back) > MOST_BACK or any(
        given and not CURSOR.fullmatch(given) for given in back
    ):
        raise ValueError("курсор страницы не такой, каким его дала эта страница: откройте эмитента заново")
    return cursor, list(back)


def issuer_href(issuer: str, cursor: str, back: Sequence[str] = ()) -> str:
    """This page's own address for a page of an issuer's assets: the issuer,
    the opaque cursor the page begins with, and the trail of those before."""
    pairs: list[tuple[str, str]] = [("issuer", issuer)]
    if cursor:
        pairs.append(("cursor", cursor))
    pairs.extend(("back", given) for given in back)
    return "/admin/explore?" + urlencode(pairs)


def _issuer(door: Door, issuer: str, cursor: str, back: list[str]) -> IssuerView:
    engine: Engine = door.engine

    def ask(pool: HorizonPool) -> Page:
        # The source's one list of what an issuer issued; the parameters are
        # its address, the page's size and the opaque cursor, and no others.
        return pool.page("/assets", resource_type="asset", order="asc", cursor=cursor or None, limit=PAGE, asset_issuer=issuer)

    reading, page = _read(door, ask)
    if page is None:
        return IssuerView(reading, issuer, [], None, None, len(back) + 1)
    records = [record for record in page.records if isinstance(record, Mapping)]
    named = [record for record in records if isinstance(record.get("asset_code"), str) and isinstance(record.get("asset_issuer"), str)]
    flags = lists.flags_of(engine, [f"{record['asset_code']}:{record['asset_issuer']}" for record in named])
    assets = [asset for record in records if (asset := _issued(record, flags)) is not None]
    following = issuer_href(issuer, page.next_cursor, [*back, cursor]) if page.next_cursor else None
    behind = issuer_href(issuer, back[-1], back[:-1]) if back else None
    return IssuerView(reading, issuer, assets, following, behind, len(back) + 1)


# -- the pages --------------------------------------------------------------------------


def build(door: Door) -> APIRouter:
    router = APIRouter(prefix="/explore")

    def refused(request: Request, login: Any, why: str, status_code: int) -> Any:
        return support.render(
            request, "refused.html", login=login, csrf=auth.csrf_token(login.csrf), active="explore", why=why, status_code=status_code
        )

    def back(said: str, *, error: bool = False, **extra: Any) -> RedirectResponse:
        query = {"error" if error else "notice": " ".join(said.split())[:400], **extra}
        return RedirectResponse(f"/admin/explore?{urlencode(query)}", status_code=303)

    @router.get("")
    def explore_page(request: Request, login: Any = Depends(door.open)) -> Any:
        """The form on its own — no server is asked — or the one reading its
        address in the query asks for."""
        account = " ".join(request.query_params.get("account", "").split())
        issuer = " ".join(request.query_params.get("issuer", "").split())
        kind, view = "", None
        try:
            if account and issuer:
                raise ValueError("укажите что-то одно: аккаунт или эмитента")
            checked = lists.address_of(account or issuer) if account or issuer else ""
            cursor, before = trail_of(request.query_params.get("cursor", ""), request.query_params.getlist("back")) if issuer else ("", [])
        except ValueError as error:  # not an address, or not a cursor of this page: nothing was asked
            return refused(request, login, str(error), 400)
        # Only what was checked goes into the reading: no address but the owner's, no cursor but this page's.
        if account:
            kind, view = "account", _account(door, checked)
        elif issuer:
            kind, view = "issuer", _issuer(door, checked, cursor, before)
        # The task a form of this page has just put in the queue, to point at it.
        asked = request.query_params.get("task", "")
        return support.page(
            request, "explore.html", login, "explore",
            kind=kind, view=view, started=service_jobs.get(door.engine, int(asked)) if asked.isdigit() else None,
            most=MOST_REQUESTS, reason=REASON, page_size=PAGE,
        )

    @router.post("/run")
    async def run(request: Request) -> Any:
        """A dry run of discovery over an asset of an issuer's list, put in
        the queue as a task with the budget written. Nothing is read here, and
        nothing is written but the task: a dry run writes nothing."""
        login, form = await door.form_of(request)
        try:
            asset_id = lists.asset_of(form.get("asset", ""))
            budget = budget_of(form.get("max_requests"))
        except ValueError as error:
            return back(str(error), error=True)
        job, made = service_jobs.enqueue(
            door.engine,
            "discovery",
            {"asset": asset_id, "dry_run": True, "resume": False, "max_requests": budget},
            one_at_a_time=asset_id,
            requested_by="admin",
        )
        said = "Сухой проход" + (" поставлен в очередь" if made else " по этому активу уже стоит в очереди или идёт — ничего не добавлено")
        said += f": задача #{job.id}, бюджет {budget} запросов. Он ничего не записывает; его результат — на странице задачи."
        return back(said, task=job.id)

    return router
