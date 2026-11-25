"""Assets of the watch list, and the discovery of who holds them.

An asset has two rules, apart from each other: ``enabled`` — its new events
are brought in — and ``discover`` — the holders of its trustline are looked
for and brought into the watch list. Each is switched from its own form.

A discovery pass is never run by a request. The request puts a task in the
queue (:mod:`app.tasks`): a dry run, which writes nothing anywhere, or a pass
with the budget the owner wrote; the worker's background thread takes it. The
page reads the passes and the tasks from the database, and says in plain
words what a pass is and is not.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote, urlencode

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.admin import auth, lists, support
from app.admin.areas import discovery
from app.admin.door import Door
from app.storage import service_jobs

MOST_REQUESTS, budget_of = discovery.MOST_REQUESTS, discovery.budget_of
TASKS_SHOWN, RUNS_SHOWN = 10, 5

# What switching a rule came to, in words for the notice.
SWITCHED = {
    ("enabled", True): "Сбор новых событий по активу включён. Подписку на актив заведёт проход воркера, если её ещё нет.",
    ("enabled", False): "Сбор новых событий по активу выключен. Уже собранная история остаётся; discovery и рынки, "
    "в которых есть этот актив, не тронуты.",
    ("discover", True): "Актив помечен для discovery: держателей его trustline ищут проходы. Первый запуск явный — "
    "кнопками на этой странице; подписку по правилу discovery заведёт проход воркера.",
    ("discover", False): "Актив больше не помечен для discovery: проходы его держателей не ищут. "
    "Найденные раньше аккаунты остаются в списке.",
}


def _back(path: str, said: str, *, error: bool = False, **extra: Any) -> RedirectResponse:
    """To a page, with a notice — or an error — in its query."""
    query = {"error" if error else "notice": " ".join(said.split())[:400], **extra}
    return RedirectResponse(f"{path}?{urlencode(query)}", status_code=303)


def _on(given: str | None) -> bool:
    """What a form's field says. A ValueError when it says nothing."""
    if given == "1":
        return True
    if given == "0":
        return False
    raise ValueError("не сказано, включить правило или выключить")


def _ticked(form: dict[str, str], name: str) -> bool:
    """Whether a form's box was ticked: a box sends its name only when it is."""
    return form.get(name) == "1"


def build(door: Door) -> APIRouter:
    router = APIRouter(prefix="/assets")

    def refused(request: Request, login: Any, why: str, status_code: int) -> Any:
        return support.render(
            request, "refused.html", login=login, csrf=auth.csrf_token(login.csrf), active="assets", why=why, status_code=status_code
        )

    def page_of(asset_id: str) -> str:
        """The asset's page, when the asset is watched — where a form that
        failed is to be tried again; the list otherwise."""
        return f"/admin/assets/{quote(asset_id, safe='')}" if lists.asset(door.engine, asset_id) else "/admin/assets"

    @router.get("")
    def assets_page(request: Request, login: Any = Depends(door.open)) -> Any:
        engine: Engine = door.engine
        search = " ".join(request.query_params.get("q", "").split())[:100]
        state = request.query_params.get("enabled", "")
        marked = request.query_params.get("discover", "")
        page, per_page, offset = support.page_of(request)
        rows, total = lists.assets(
            engine,
            search=search,
            enabled={"1": True, "0": False}.get(state),
            discover={"1": True, "0": False}.get(marked),
            limit=per_page,
            offset=offset,
        )
        return support.page(
            request, "assets.html", login, "assets",
            rows=rows, total=total, page=page, per_page=per_page, q=search, state=state, marked=marked,
            counts=lists.asset_counts(engine), figures=discovery.figures, most=MOST_REQUESTS,
        )

    @router.get("/{asset_id}")
    def asset_page(request: Request, asset_id: str, login: Any = Depends(door.open)) -> Any:
        engine: Engine = door.engine
        try:
            asset_id = lists.asset_of(asset_id)
        except ValueError:
            return refused(request, login, "Это не актив: нужно «native» или «КОД:ЭМИТЕНТ».", 404)
        row = lists.asset(engine, asset_id)
        if row is None:
            return refused(request, login, "Этого актива нет в списке наблюдения.", 404)
        runs = lists.runs(engine, asset_id, limit=RUNS_SHOWN)
        # The task a form of this page has just put in the queue, to point at it.
        asked = request.query_params.get("task", "")
        started = service_jobs.get(engine, int(asked)) if asked.isdigit() else None
        if started is not None and (started.kind != "discovery" or (started.params or {}).get("asset") != asset_id):
            started = None
        return support.page(
            request, "asset.html", login, "assets",
            row=row, started=started, stopped=discovery.where_stopped(engine, asset_id, runs[:1][0] if runs else None),
            runs=runs, tasks=lists.tasks(engine, asset_id, limit=TASKS_SHOWN),
            figures=discovery.figures, how_far=discovery.how_far, stopped_because=discovery.stopped_because, most=MOST_REQUESTS,
        )

    @router.post("/add")
    async def add(request: Request) -> Any:
        login, form = await door.form_of(request)
        try:
            asset_id = lists.asset_of(form.get("asset", ""))
        except ValueError as error:
            return _back("/admin/assets", str(error), error=True)
        on, discover = _ticked(form, "enabled"), _ticked(form, "discover")
        try:
            with Session(door.engine) as session, session.begin():
                added = lists.add_asset(session, asset_id, enabled=on, discover=discover, reason=form.get("reason", ""))
        except ValueError as error:
            return _back(page_of(asset_id), str(error), error=True)
        if not added:
            said = "Актив уже был в списке наблюдения: ничего не изменено. Его правила меняются на этой странице, по одному."
        else:
            said = "Актив добавлен в список наблюдения: " + ("включён" if on else "выключен") + ", " + ("помечен для discovery." if discover else "не помечен для discovery.")
            said += (
                " Держателей он начнёт искать только после явного прохода — сухого или настоящего: кнопки на этой странице."
                if discover
                else " Ничего само не запускается: пометить его для discovery и запустить проход можно на этой странице."
            )
        return _back(page_of(asset_id), said)

    async def switched(request: Request, asset_id: str, rule: str) -> Any:
        """One rule of an asset switched on or off, and no other."""
        login, form = await door.form_of(request)
        try:
            asset_id = lists.asset_of(asset_id)
        except ValueError as error:
            return _back("/admin/assets", str(error), error=True)
        try:
            on = _on(form.get(rule))
            with Session(door.engine) as session, session.begin():
                lists.set_asset_flags(session, asset_id, **{rule: on})
        except ValueError as error:
            return _back(page_of(asset_id), str(error), error=True)
        return _back(page_of(asset_id), SWITCHED.get((rule, on), ""))

    @router.post("/{asset_id}/enabled")
    async def enabled(request: Request, asset_id: str) -> Any:
        return await switched(request, asset_id, "enabled")

    @router.post("/{asset_id}/discover")
    async def discover(request: Request, asset_id: str) -> Any:
        return await switched(request, asset_id, "discover")

    @router.post("/{asset_id}/run")
    async def run(request: Request, asset_id: str) -> Any:
        """A discovery pass of this asset put in the queue: a dry run or a
        pass, with its budget. One at a time for an asset."""
        login, form = await door.form_of(request)
        engine: Engine = door.engine
        try:
            asset_id = lists.asset_of(asset_id)
        except ValueError as error:
            return _back("/admin/assets", str(error), error=True)
        try:
            if lists.asset(engine, asset_id) is None:
                raise ValueError("этого актива нет в списке наблюдения: добавьте его прежде")
            budget = budget_of(form.get("max_requests"))
            dry = form.get("dry_run") == "1"
            job, made = service_jobs.enqueue(
                engine,
                "discovery",
                {"asset": asset_id, "dry_run": dry, "resume": not dry and _ticked(form, "resume"), "max_requests": budget},
                one_at_a_time=asset_id,
                requested_by="admin",
            )
        except ValueError as error:
            return _back(page_of(asset_id), str(error), error=True)
        said = "Сухой проход" if dry else "Проход discovery"
        said += " поставлен в очередь" if made else " по этому активу уже стоит в очереди или идёт — ничего не добавлено"
        said += f": задача #{job.id}, бюджет {budget} запросов. Запрос только поставил задачу — выполнит её воркер в фоне; состояние и результат на странице задачи."
        return _back(page_of(asset_id), said, task=job.id)

    return router
