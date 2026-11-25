"""The accounts of the watch list, as the owner sees and changes them: who is
watched and why, how each one came, and where it is in its life; accounts
brought in by hand, switched off and on; and an account that was here before
stage 05 given the history import it never had.

Nothing that follows from a change is done by the request. A start boundary,
a history import, a subscription — these are the worker's pass, as they are
for an account a command adds; the notice after a change says so. A page
reads the database and asks no server.
"""

from __future__ import annotations

from collections import Counter
from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.admin import auth, lists, support
from app.admin.door import Door
from app.watch import accounts as watched

ORIGINS = (
    (watched.LEGACY, "унаследованный"),
    (watched.YAML, "из файла списка"),
    (watched.MANUAL, "вручную"),
    (watched.DISCOVERY, "найден discovery"),
)
# What adding an account came to, in words for a notice.
ADDED_WORDS = {
    watched.ADDED: "добавлен",
    watched.ENABLED: "уже был включён — не изменён",
    watched.DISABLED: "был выключен вручную — оставлен выключенным",
}
# ...and what switching one came to, as ``lists.set_account_enabled`` says of it.
SWITCHED = {
    "on": "Аккаунт включён снова и ждёт новую границу начала: проход воркера возьмёт её, "
    "поставит загрузку пропущенного и вернёт подписку.",
    "off": "Аккаунт выключен. История остаётся; проход воркера поставит его импорт на паузу "
    "и снимет его подписку. Активы, рынки и чужие подписки не тронуты.",
    "unchanged": "Аккаунт уже был в этом состоянии: ничего не изменено.",
}


def origin_in_words(origin: str) -> str:
    return dict(ORIGINS).get(origin, origin)


def _back(path: str, said: str, *, error: bool = False) -> RedirectResponse:
    """To a page, with a notice — or an error — in its query."""
    kept = " ".join(said.split())[:400]
    return RedirectResponse(f"{path}?{'error' if error else 'notice'}={quote(kept)}", status_code=303)


def _on(given: str | None) -> bool:
    """What a form's ``enabled`` field says. A ValueError when it says nothing."""
    if given == "1":
        return True
    if given == "0":
        return False
    raise ValueError("не сказано, включить аккаунт или выключить")


def build(door: Door) -> APIRouter:
    router = APIRouter(prefix="/accounts")

    def refused(request: Request, login: Any, why: str, status_code: int) -> Any:
        return support.render(
            request, "refused.html", login=login, csrf=auth.csrf_token(login.csrf), active="accounts", why=why, status_code=status_code
        )

    def page_of(address: str) -> str:
        """The account's page, when ``address`` is of an account that is
        watched — where a form that failed is to be tried again; the list
        otherwise."""
        if not lists.ADDRESS.fullmatch(address) or lists.account(door.engine, address) is None:
            return "/admin/accounts"
        return f"/admin/accounts/{address}"

    @router.get("")
    def accounts_page(request: Request, login: Any = Depends(door.open)) -> Any:
        engine: Engine = door.engine
        try:
            search = lists.address_start(request.query_params.get("q", ""))
        except ValueError as error:  # not the start of an address: said, and nothing is shown in its place
            return refused(request, login, str(error), 400)
        state = request.query_params.get("enabled", "")
        origin = request.query_params.get("origin", "")
        page, per_page, offset = support.page_of(request)
        rows, total = lists.accounts(
            engine,
            search=search,
            enabled={"1": True, "0": False}.get(state),
            origin=origin if origin in watched.ORIGINS else None,
            limit=per_page,
            offset=offset,
        )
        return support.page(
            request, "accounts.html", login, "accounts",
            rows=rows, total=total, page=page, per_page=per_page, q=search, state=state, origin=origin,
            counts=lists.account_counts(engine), origins=ORIGINS, origin_in_words=origin_in_words,
        )

    @router.get("/{address}")
    def account_page(request: Request, address: str, login: Any = Depends(door.open)) -> Any:
        try:
            checked = lists.address_of(address)
        except ValueError:
            checked = ""
        row = lists.account(door.engine, checked) if checked else None
        if row is None:
            return refused(request, login, "Этот аккаунт не в списке наблюдения.", 404)
        return support.page(request, "account.html", login, "accounts", row=row, origin_in_words=origin_in_words)

    @router.post("/add")
    async def add(request: Request) -> Any:
        login, form = await door.form_of(request)
        try:
            addresses = lists.addresses_of(form.get("addresses", ""))
            reason = lists.reason_of(form.get("reason", ""), "аккаунт")
            with Session(door.engine) as session, session.begin():
                changes = [watched.add_account(session, address, reason=reason, origin=watched.MANUAL) for address in addresses]
        except ValueError as error:
            return _back("/admin/accounts", str(error), error=True)
        if len(changes) == 1:
            change = changes[0]
            said = {
                watched.ADDED: "Аккаунт добавлен: включён и ждёт границу начала. Дальше — проход воркера, в тишине между событиями: "
                "он возьмёт границу, поставит загрузку всей доступной истории и зарегистрирует подписку. Запускать ничего не нужно.",
                watched.ENABLED: "Аккаунт уже в списке и включён: ничего не изменено.",
                watched.DISABLED: "Аккаунт уже в списке и был выключен вручную: оставлен выключенным. "
                "Включить его можно на этой странице.",
            }[change.outcome]
            return _back(f"/admin/accounts/{change.account_id}", said)
        came = Counter(change.outcome for change in changes)
        said = f"Адресов обработано {len(changes)}, добавлено новых {came[watched.ADDED]}"
        left = [f"{number} — {ADDED_WORDS[outcome]}" for outcome, number in came.items() if outcome != watched.ADDED]
        if left:
            said += "; уже бывшие в списке не изменены: " + ", ".join(left)
        return _back("/admin/accounts", said + ". Новые ждут границу начала; остальное сделает проход воркера.")

    @router.post("/{address}/enabled")
    async def enabled(request: Request, address: str) -> Any:
        login, form = await door.form_of(request)
        try:
            address = lists.address_of(address)
            on = _on(form.get("enabled"))
            with Session(door.engine) as session, session.begin():
                change = lists.set_account_enabled(session, address, enabled=on)
        except ValueError as error:
            return _back(page_of(address), str(error), error=True)
        return _back(f"/admin/accounts/{address}", SWITCHED[change])

    @router.post("/{address}/reason")
    async def reason(request: Request, address: str) -> Any:
        login, form = await door.form_of(request)
        try:
            address = lists.address_of(address)
            why = lists.reason_of(form.get("reason", ""), "аккаунт")
            with Session(door.engine) as session, session.begin():
                lists.set_account_reason(session, address, reason=why)
        except ValueError as error:
            return _back(page_of(address), str(error), error=True)
        return _back(f"/admin/accounts/{address}", "Причина изменена.")

    @router.post("/{address}/history")
    async def history(request: Request, address: str) -> Any:
        """An account that was here before stage 05: its history import, and
        the road to a start boundary and a subscription check."""
        login, form = await door.form_of(request)
        try:
            address = lists.address_of(address)
            job, _ = lists.enqueue_history(door.engine, lists.ACCOUNT, address)
        except ValueError as error:
            return _back(page_of(address), str(error), error=True)
        said = f"Загрузка истории стоит: задание #{job.id}, до леджера {job.to_ledger}. "
        row = lists.account(door.engine, address)
        if row is not None and row.boundary_due:
            said += (
                "Аккаунт ждёт границу начала: проход воркера, в тишине между событиями, возьмёт её и поставит импорт по ней, "
                "займётся подпиской — проверит её и поднимет верхнюю границу задания, когда она подтвердится."
            )
        else:
            said += "Задание уже было или граница начала взята: границы расширены, прогресс не сброшен."
        return _back(f"/admin/accounts/{address}", said)

    return router
