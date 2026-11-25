"""Markets of the watch list: a pair of two assets whose trades are
collected, and the import of all the trades there are of the pair.

A market is an unordered pair. It is stored once — the lower asset first —
and asked for in either order; the page shows both readings. Its import is
planned from here: the request puts the job in the queue, with the latest
ledger the pool is known to have, and the worker's pass raises that boundary
to the latest ledger and makes it final. Planning again never resets
progress. Switching a market off pauses its import — the worker's pass does
that — and touches nothing else.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.admin import auth, lists, support
from app.admin.door import Door
from app.backfill.jobs import market_key

SWITCHED = {
    True: "Рынок включён. Подписку на его актив заведёт проход воркера, если её ещё нет.",
    False: "Рынок выключен. История сделок остаётся; проход воркера поставит его импорт на паузу. "
    "Активы, аккаунты и чужие подписки не тронуты.",
}


def _back(path: str, said: str, *, error: bool = False) -> RedirectResponse:
    """To a page, with a notice — or an error — in its query."""
    return RedirectResponse(f"{path}?{'error' if error else 'notice'}={quote(' '.join(said.split())[:400])}", status_code=303)


def _on(given: str | None) -> bool:
    """What a form's ``enabled`` field says. A ValueError when it says nothing."""
    if given == "1":
        return True
    if given == "0":
        return False
    raise ValueError("не сказано, включить рынок или выключить")


def _ticked(form: dict[str, str], name: str) -> bool:
    """Whether a form's box was ticked: a box sends its name only when it is."""
    return form.get(name) == "1"


def build(door: Door) -> APIRouter:
    router = APIRouter(prefix="/markets")

    def refused(request: Request, login: Any, why: str, status_code: int) -> Any:
        return support.render(
            request, "refused.html", login=login, csrf=auth.csrf_token(login.csrf), active="markets", why=why, status_code=status_code
        )

    def pair_page(low: str, high: str) -> str:
        """The market's page, by the pair as it is stored, when the market is
        watched — where a form that failed is to be tried again; the list
        otherwise."""
        if lists.market(door.engine, low, high) is None:
            return "/admin/markets"
        return f"/admin/markets/{quote(low, safe='')}/{quote(high, safe='')}"

    @router.get("")
    def markets_page(request: Request, login: Any = Depends(door.open)) -> Any:
        engine: Engine = door.engine
        search = " ".join(request.query_params.get("q", "").split())[:100]
        state = request.query_params.get("enabled", "")
        page, per_page, offset = support.page_of(request)
        rows, total = lists.markets(engine, search=search, enabled={"1": True, "0": False}.get(state), limit=per_page, offset=offset)
        return support.page(
            request, "markets.html", login, "markets", rows=rows, total=total, page=page, per_page=per_page, q=search, state=state
        )

    @router.get("/{base}/{counter}")
    def market_page(request: Request, base: str, counter: str, login: Any = Depends(door.open)) -> Any:
        try:
            base, counter = lists.asset_of(base), lists.asset_of(counter)
        except ValueError:
            return refused(request, login, "Это не рынок: нужны два актива, «native» или «КОД:ЭМИТЕНТ».", 404)
        row = lists.market(door.engine, base, counter)
        if row is None:
            return refused(request, login, "Этого рынка нет в списке наблюдения.", 404)
        return support.page(request, "market.html", login, "markets", row=row)

    @router.post("/add")
    async def add(request: Request) -> Any:
        login, form = await door.form_of(request)
        try:
            with Session(door.engine) as session, session.begin():
                low, high, added = lists.add_market(
                    session, form.get("base", ""), form.get("counter", ""), enabled=_ticked(form, "enabled"), reason=form.get("reason", "")
                )
        except ValueError as error:
            return _back("/admin/markets", str(error), error=True)
        page = pair_page(low, high)
        said = "Рынок добавлен в список наблюдения и " + ("включён." if _ticked(form, "enabled") else "оставлен выключенным.")
        if not added:
            said = "Рынок уже был в списке наблюдения: ничего не изменено."
        if _ticked(form, "history"):
            try:
                job, made = lists.enqueue_history(door.engine, lists.MARKET, market_key(low, high))
            except ValueError as error:
                said += " Загрузить историю не вышло: " + str(error) + "."
            else:
                said += (
                    f" Загрузка истории сделок {'поставлена' if made else 'уже стояла — границы расширены, прогресс не сброшен'}: "
                    f"задание #{job.id}, до леджера {job.to_ledger}."
                )
        return _back(page, said)

    @router.post("/{base}/{counter}/enabled")
    async def enabled(request: Request, base: str, counter: str) -> Any:
        login, form = await door.form_of(request)
        try:
            low, high = lists.asset_of(base), lists.asset_of(counter)
        except ValueError as error:
            return _back("/admin/markets", str(error), error=True)
        page = pair_page(low, high)
        try:
            on = _on(form.get("enabled"))
            with Session(door.engine) as session, session.begin():
                lists.set_market_enabled(session, base, counter, enabled=on)
        except ValueError as error:
            return _back(page, str(error), error=True)
        said = SWITCHED[on]
        if on:
            # An import there already goes back to work now: the pause the pass
            # made for the market being switched off is lifted, and its
            # boundary is taken again to cover the time it was off.
            if lists.import_of(door.engine, lists.MARKET, market_key(low, high)) is not None:
                lists.enqueue_history(door.engine, lists.MARKET, market_key(low, high))
                said += " Импорт, вставший на паузу при выключении, снова в очереди."
        return _back(page, said)

    @router.post("/{base}/{counter}/history")
    async def history(request: Request, base: str, counter: str) -> Any:
        """All the trades there are of the pair, from as far back as the pool
        keeps: planned, never reset."""
        login, form = await door.form_of(request)
        try:
            low, high = lists.asset_of(base), lists.asset_of(counter)
        except ValueError as error:
            return _back("/admin/markets", str(error), error=True)
        page = pair_page(low, high)
        try:
            job, made = lists.enqueue_history(door.engine, lists.MARKET, market_key(low, high))
        except ValueError as error:
            return _back(page, str(error), error=True)
        said = "Загрузка истории сделок " + ("поставлена" if made else "уже стояла — границы расширены, прогресс не сброшен")
        said += (
            f": задание #{job.id}, до леджера {job.to_ledger}. Проход воркера поднимет границу до последнего леджера пула "
            "и сделает её завершённой; читает историю служба history в фоне."
        )
        return _back(page, said)

    return router
