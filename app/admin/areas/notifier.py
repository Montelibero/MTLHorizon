"""The Notifier as this installation sees it: what it wants to be told of,
what is really registered and confirmed at Operations Notifier, and what is
left for a person to decide.

The page reads the database and nothing else. What is wanted comes from the
watch lists; what is confirmed, from the rows the passes wrote. Whether those
rows are right can be learned from the Notifier alone, and that is a task:
«Что там сейчас?» says what a pass would do, «Сверить» does it — the worker
runs either in the background, and a request only puts it in the queue. A
page opened by a refresh asks no server.

A subscription made by hand is adopted by its id and is never deleted here:
there is no way on this page to delete one. What the passes retire is only
what this installation made itself.

The reasons the passes and the Notifier's client write are kept as they
wrote them: a row says what went wrong in the words it happened in, and this
page does not retell them. The page's own words are the owner's, Russian.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode, urlsplit

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from stellar_sdk import StrKey

from app.admin import support
from app.admin.door import Door, Refused
from app.backfill import config as watch_lists
from app.config import Settings, shown_url
from app.notifier.client import NotifierError, NotifierRejected
from app.storage import service_jobs
from app.storage.models import ServiceJob, WatchSubscription
from app.subscriptions import desired, reconcile
from app.subscriptions.desired import ACCOUNT, ASSET, Want
from app.watch import provision

STATES = (reconcile.WANTED, reconcile.ACTIVE, reconcile.ERROR, reconcile.AMBIGUOUS, reconcile.RETIRING, reconcile.MANUAL, reconcile.RETIRED)
# What waits for a person comes first, what is over and done with last.
ORDER = {reconcile.ERROR: 0, reconcile.AMBIGUOUS: 1, reconcile.WANTED: 2, reconcile.RETIRING: 3, reconcile.ACTIVE: 4, reconcile.MANUAL: 5, reconcile.RETIRED: 6}
WAITS = (reconcile.WANTED, reconcile.ERROR, reconcile.RETIRING)  # these have a next attempt
# The tasks with the Notifier go one at a time, as the worker's own passes do.
QUEUE = "subscriptions"
# How many of the latest tasks are looked through for the last look and the last pass.
MOST_TASKS = 100
# How often the page asks for itself again while a task with the Notifier runs (it asks no server).
REFRESH = 10
RULES = {"account enabled": "аккаунт включён", "history of the asset": "история актива", "discovery": "обнаружение держателей"}
WHY = {
    "gone from the Notifier (deleted or switched off there)": "у Notifier её больше нет: удалена там или выключена им самим",
    "made by hand, and gone from the Notifier: nothing is made in its place": "сделана руками, и у Notifier её больше нет: взамен ничего не создаётся",
    "nothing wants it any more": "её больше не хочет ни одно правило",
    "no longer wanted at the Notifier": "она больше не нужна",
    "wanted, and not confirmed yet": "нужна, а ещё не подтверждена",
}
SEVERAL_FIT = re.compile(r"a person decides: (\d+) fit")
ANOTHER = re.compile(r"subscription (\S+) is no longer what this installation registers")
REMOTE = re.compile(r"[A-Za-z0-9_-]{1,64}")
DIGITS = re.compile(r"\d{1,18}")
KINDS = ((ACCOUNT, "account — аккаунт"), (ASSET, "asset — актив"))


# -- reading ---------------------------------------------------------------------


def _plain(row: WatchSubscription) -> WatchSubscription:
    """The row as plain data, alive after its session is gone."""
    return WatchSubscription(**{column.name: getattr(row, column.name) for column in WatchSubscription.__table__.columns})


def _rule(rule: str) -> str:
    """A rule that wants a subscription, in words for the owner; the rule
    itself when it is none this page knows."""
    if rule in RULES:
        return RULES[rule]
    if rule.startswith("market "):
        # The pair by its codes: the issuers would be a line of their own.
        return "рынок " + " > ".join(asset.partition(":")[0] for asset in rule[7:].split(">"))
    return rule


def _what(what: Any) -> str:
    """What a pass would do to one subscription, in words for the owner; the
    step's own words when they are none this page knows."""
    text = str(what)
    if text == "create":
        return "создать у Notifier"
    if text.startswith("take "):
        return "взять ту, что уже есть у Notifier (" + text[5:] + ")"
    if text == "register again":
        return "зарегистрировать заново"
    if text == "retire":
        return "снять у Notifier"
    if text == "forget":
        return "забыть здесь, у Notifier не трогать"
    if text == "a person decides":
        return "решает человек"
    several = SEVERAL_FIT.fullmatch(text)
    if several:
        return "решает человек: подходят подписок — " + several[1]
    return text


def _why(why: Any) -> str:
    """Why a pass would do it, in words for the owner where the reason is one
    the pass is known to write; the reason itself otherwise."""
    text = str(why)
    if text in WHY:
        return WHY[text]
    another = ANOTHER.fullmatch(text)
    if another:
        return f"подписка {another[1]} у Notifier уже не такая, какую регистрирует эта установка (другой адрес или другие фильтры)"
    rules = text.split(", ")
    if all(rule in RULES or rule.startswith("market ") for rule in rules):
        return "хотят правила: " + ", ".join(_rule(rule) for rule in rules)
    return text


def _due(row: WatchSubscription, state: str, now: datetime) -> bool:
    """Whether the row's turn has come: its next attempt is not in the future."""
    at = row.next_attempt_at
    return state in WAITS and at is not None and at.replace(tzinfo=at.tzinfo or UTC) <= now


def lines(wanted: Mapping[tuple[str, str], Want], rows: Sequence[WatchSubscription], now: datetime) -> list[dict[str, Any]]:
    """Wanted against kept: a line for every kind and key of either, wanted
    or kept alone. Those that wait for a person come first."""
    kept = {(row.kind, row.key): row for row in rows}
    found = []
    for key in sorted(set(wanted) | set(kept)):
        want, row = wanted.get(key), kept.get(key)
        state = row.state if row is not None else reconcile.WANTED
        found.append(
            {
                "kind": key[0], "key": key[1], "row": row, "state": state, "wanted": want is not None,
                "rules": [{"said": _rule(rule), "rule": rule} for rule in want.rules] if want is not None else [],
                "due": row is not None and _due(row, state, now),
            }
        )
    return sorted(found, key=lambda line: (ORDER.get(line["state"], len(ORDER)), line["kind"], line["key"]))


def _callback(url: str) -> str:
    """The callback address as it is shown: scheme, host, port and path — the
    path included, because this is the whole address the Notifier is told to
    post to, and by which this installation's subscriptions are told apart. A
    user name, a password, a query or a fragment in it are left out."""
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        return "<не читается>"
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return ""
    path = parts.path.rstrip("/")
    return f"{parts.scheme}://{parts.hostname}" + ("" if port is None else f":{port}") + path


def _api(settings: Settings) -> dict[str, Any]:
    """What is configured for talking to the Notifier — and no secret of it.
    The token is only said to be set or not set."""
    return {
        "url": shown_url(settings.notifier_api_url) if settings.notifier_api_url else "",
        "token": bool(settings.notifier_api_token),
        "callback": _callback(settings.notifier_callback_url),
        "signers": len(settings.notifier_signer_keys),
    }


def _result(job: ServiceJob | None, *, look: bool) -> dict[str, Any] | None:
    """An ended task with the Notifier as the page shows it. ``look``: the
    dry run, which says what a pass would do; otherwise the pass itself."""
    if job is None:
        return None
    result: dict[str, Any] = dict(job.result or {})
    shown: dict[str, Any] = {"job": job, "done": job.state == service_jobs.DONE, "error": job.last_error}
    if job.state != service_jobs.DONE:
        return shown
    if look:
        at = result.get("at")
        try:
            looked = datetime.fromisoformat(at) if isinstance(at, str) else None
        except ValueError:
            looked = None
        shown.update(
            at=looked,
            signer=result.get("signer"),
            # None: no keys are configured, so the question was not asked.
            signer_known=None if result.get("signer_known") is None else bool(result.get("signer_known")),
            at_notifier=int(result.get("at_notifier") or 0),
            foreign=int(result.get("foreign") or 0),
            wanted=int(result.get("wanted") or 0),
            would_do_nothing=bool(result.get("would_do_nothing")),
            steps=[
                {**step, "what_said": _what(step.get("what")), "why_said": _why(step.get("why"))}
                for step in result.get("steps") or []
                if isinstance(step, dict)
            ],
        )
        shown["ours"] = shown["at_notifier"] - shown["foreign"]
        return shown
    shown["figures"] = [
        (name, result.get(name))
        for name in ("wanted", "created", "taken", "confirmed", "failed", "ambiguous", "retired")
    ]
    shown["problems"] = [str(problem) for problem in (result.get("problems") or []) if problem]
    shown["unreachable"] = result.get("unreachable")
    return shown


def _tasks(engine: Engine, job: ServiceJob | None) -> dict[str, Any]:
    """The newest tasks with the Notifier — the last look, the last pass, the
    one queued or running now — and the task the page was sent back to."""
    jobs = service_jobs.recent(engine, kind="reconcile", limit=MOST_TASKS)

    def ended(look: bool) -> ServiceJob | None:
        return next(
            (one for one in jobs if one.state not in service_jobs.ACTIVE and bool((one.params or {}).get("dry_run")) is look), None
        )

    return {
        "job": job,
        "active": next((one for one in jobs if one.state in service_jobs.ACTIVE), None),
        "look": _result(ended(True), look=True),
        "pass": _result(ended(False), look=False),
    }


def _row(engine: Engine, asked: str) -> WatchSubscription | None:
    """A row of ``watch_subscription`` by its id, as plain data; None when
    there is none — or ``asked`` is no number."""
    if not DIGITS.fullmatch(asked):
        return None
    with Session(engine) as session:
        row = session.get(WatchSubscription, int(asked))
        return None if row is None else _plain(row)


def _job_of(engine: Engine, asked: str) -> ServiceJob | None:
    """A task by its id; None when there is none — or ``asked`` is no number."""
    return service_jobs.get(engine, int(asked)) if DIGITS.fullmatch(asked) else None


def _not_taken(error: NotifierRejected, remote: str, kind: str, key: str) -> str:
    """Why the Notifier would not let a subscription be adopted, in words."""
    if error.status == 404:
        return f"У Notifier нет активной подписки с id {remote}: ничего не принято. Проверьте id — или её сделали не там, или её уже сняли."
    if error.status == 409:
        return (
            f"Подписка {remote} у Notifier — не то, что регистрирует эта установка для {kind} {key}: "
            "другой адрес вебхука или другие фильтры. Ничего не принято."
        )
    return f"Notifier отказал ({error.status}): {error.detail}. Ничего не принято."


# -- the page --------------------------------------------------------------------


def build(door: Door) -> APIRouter:
    router = APIRouter(prefix="/notifier")

    def back(notice: str | None = None, *, error: str | None = None, job: ServiceJob | None = None, at: str | None = None, **query: str) -> RedirectResponse:
        """To the page, saying what was done — as a notice, or as what went
        wrong — which task it concerns, and, with ``at``, the card it is told in."""
        parts = {"notice": notice, "error": error, "job": str(job.id) if job is not None else None, **query}
        where = "/admin/notifier?" + urlencode(sorted((name, value) for name, value in parts.items() if value))
        return RedirectResponse(where + (f"#{at}" if at else ""), status_code=303)

    def start(look: bool, key: str | None = None) -> RedirectResponse:
        """Put a task with the Notifier in the queue: the dry run, or the
        pass. Never run here."""
        if not door.settings.notifier_api_url:
            raise Refused(
                "API Notifier не настроен (MTL_HORIZON_NOTIFIER_API_URL): спросить его не с чем. "
                "Подписки делаются у Notifier вручную и принимаются здесь по id."
            )
        job, made = service_jobs.enqueue(door.engine, "reconcile", {"dry_run": look}, one_at_a_time=QUEUE)
        name = "взгляд на Notifier" if look else "сверка"
        if not made and bool((job.params or {}).get("dry_run")) is not look:
            running = "сверка" if look else "взгляд на Notifier (сухой прогон)"
            return back(
                f"Ничего не добавлено: с Notifier уже работает задача {job.id} — это {running}. "
                "Нажмите ещё раз, когда она закончится.", job=job,
            )
        told = {"at": "last-look" if look else "last-pass"}
        if not made:
            return back(f"Такой {name} уже стоит в очереди или идёт (задача {job.id}): ничего не добавлено.", job=job, **told)
        said = (
            "Задача поставлена в очередь: она прочтёт у Notifier список и скажет, что сделал бы проход. "
            "Ничего не меняется — ни здесь, ни у Notifier."
            if look else
            "Сверка поставлена в очередь: выполнит её воркер в фоне — зарегистрирует, подтвердит, снимет лишнее. Итог — на странице задачи."
        )
        return back(said, job=job, **told, **({"q": key} if key else {}))

    @router.get("")
    def notifier_page(request: Request, login: Any = Depends(door.open)) -> Any:
        engine: Engine = door.engine
        now = datetime.now(UTC)
        with Session(engine) as session:
            wanted = desired.wanted(session)
            rows = [_plain(row) for row in session.scalars(select(WatchSubscription))]
        every = lines(wanted, rows, now)
        state = request.query_params.get("state", "")
        state = state if state in STATES else ""
        search = request.query_params.get("q", "").strip()[:200]
        shown = [
            line for line in every
            if (not state or line["state"] == state) and (not search or search.lower() in line["key"].lower())
        ]
        page, per_page, offset = support.page_of(request)
        counts = dict.fromkeys(STATES, 0)
        for line in every:
            counts[line["state"]] += 1
        asked = request.query_params.get("job", "")
        job = _job_of(engine, asked)
        if job is not None and job.kind != "reconcile":
            job = None
        kind = request.query_params.get("kind", "")
        tasks = _tasks(engine, job)
        return support.page(
            request, "notifier.html", login, "notifier",
            lines=shown[offset : offset + per_page], total=len(shown), page=page, per_page=per_page,
            state=state, states=STATES, q=search,
            links={name: "/admin/notifier?" + urlencode(sorted({"state": name, **({"q": search} if search else {})}.items())) for name in STATES},
            counts=counts, wanted=len(wanted),
            confirmed=sum(1 for line in every if line["wanted"] and line["state"] in reconcile.CONFIRMED),
            due=reconcile.due(engine, now=now),
            tasks=tasks, api=_api(door.settings), kinds=KINDS,
            adopt={"kind": kind if kind in (ACCOUNT, ASSET) else ACCOUNT, "key": request.query_params.get("key", "")[:200] or search},
            # While a task with the Notifier is queued or running, the page comes back by itself to show how it ended.
            refresh=REFRESH if tasks["active"] is not None else None,
        )

    @router.post("/look")
    async def look(request: Request) -> Any:
        await door.form_of(request)
        return start(look=True)

    @router.post("/reconcile")
    async def reconcile_now(request: Request) -> Any:
        await door.form_of(request)
        return start(look=False)

    @router.post("/retry")
    async def retry(request: Request) -> Any:
        """«Повторить» for a row that is not confirmed. One in ``error`` is
        retried by the pass, as every pass retries it. One in ``ambiguous``
        is a person's to decide and no pass touches it: what is repeated
        there is the look at the Notifier, which says what is there now."""
        _, form = await door.form_of(request)
        row = _row(door.engine, form.get("row", ""))
        if row is None:
            raise Refused("Такой строки подписок нет.")
        if row.state not in (reconcile.ERROR, reconcile.AMBIGUOUS):
            raise Refused(f"Строке {row.id} повтор не нужен: её состояние — {row.state}.")
        if row.state == reconcile.ERROR:
            # The pause of the row is not waited out: the pass that was just
            # asked for takes this row now.
            reconcile.try_now(door.engine, row.id)
        return start(look=row.state == reconcile.AMBIGUOUS, key=row.key)

    @router.post("/adopt")
    async def adopt(request: Request) -> Any:
        """Take a subscription made by hand at the Notifier as this
        installation's. With an API configured it is read back there first
        and refused when it is not what this installation registers; without
        one it is taken at its word, and the notice says so."""
        _, form = await door.form_of(request)
        kind = form.get("kind", "")
        if kind not in (ACCOUNT, ASSET):
            raise Refused("Тип подписки — account (аккаунт) или asset (актив).")
        key = form.get("key", "").strip()
        if kind == ACCOUNT:
            try:
                an_account = StrKey.is_valid_ed25519_public_key(key)
            except Exception:  # noqa: BLE001 - whatever is wrong with it, it is not an address
                an_account = False
            if not an_account:
                raise Refused("Для account ключ — адрес аккаунта (G…).")
        else:
            errors: list[str] = []
            asset = watch_lists.parse_asset_id(key, "key", errors)
            if asset is None:
                raise Refused("Для asset ключ — native или актив в виде CODE:ISSUER (1–12 букв или цифр, двоеточие, адрес эмитента).")
            key = asset
        remote = form.get("id", "").strip()
        if not REMOTE.fullmatch(remote):
            raise Refused("id подписки у Notifier — от 1 до 64 знаков: буквы, цифры, «-» или «_».")
        settings = door.settings
        with Session(door.engine) as session:
            wanted = desired.wanted(session)
        api = provision.make_api(settings, engine=door.engine)
        try:
            row_id = reconcile.adopt(door.engine, kind, key, remote, callback_url=settings.notifier_callback_url, api=api)
        except NotifierRejected as error:
            return back(error=_not_taken(error, remote, kind, key), q=key)
        except NotifierError as error:
            return back(error=f"Принять не получилось: с Notifier не удалось поговорить, ничего не принято и не изменено. ({error})", q=key)
        finally:
            if api is not None:
                api.close()
        said = f"Подписка {remote} принята как сделанная вручную ({kind} {key}, строка {row_id}). Эту установку она никогда не удалит."
        if api is None:
            said += " Принята на слово: API Notifier не настроен, и у Notifier её не сверяли."
        elif not settings.notifier_callback_url:
            said += " Адрес вебхуков в настройках не задан: у Notifier проверено только, что подписка есть, — не её адрес и фильтры."
        else:
            said += " Она прочитана у Notifier и совпадает с тем, что регистрирует эта установка."
        if (kind, key) not in wanted:
            # The asset codes are case-sensitive, as they are on the network: another case is another asset.
            said += " Внимание: сейчас ни одно правило наблюдения этого не хочет — следующий проход забудет строку (retired), у Notifier подписка останется."
        return back(said, q=key)

    @router.post("/{row_id}/release")
    async def release(request: Request, row_id: str) -> Any:
        """Forget here a subscription adopted by hand. It stays at the
        Notifier: nothing is deleted there."""
        await door.form_of(request)
        row = _row(door.engine, row_id)
        if row is None:
            raise Refused(f"Строки подписок {row_id} нет.")
        if row.state != reconcile.MANUAL:
            raise Refused(
                f"Строка {row.id} в состоянии {row.state}, а не manual: забыть здесь можно только подписку, принятую вручную."
            )
        try:
            reconcile.release(door.engine, row.id)
        except ValueError as error:
            raise Refused(f"Забыть не получилось: {error}") from error
        return back(
            f"Подписка {row.remote_id} ({row.kind} {row.key}) забыта здесь. У Notifier она осталась: "
            "если её нужно снять, сделайте это там — здесь этого сделать нельзя."
        )

    return router
