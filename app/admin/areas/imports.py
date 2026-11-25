"""The pages of the history import: what the imports came to for every
account and every market, the queue as a whole, and one job — and what a
person does about it by hand.

A page reads the database and asks no server — neither Horizon nor the
Notifier — and reads it under a time limit (:mod:`app.stats.imports`): a
database that has no time for a page is said so on the page, and is left to
the worker. What a form changes is what the commands of :mod:`app.history`
change, through the same functions: a pause, a retry, a priority, a window.
What takes time — a comparison with the pool, a new reading of a window — is
never done in the request: it is put in the queue of tasks
(:mod:`app.storage.service_jobs`) and the worker's background thread runs it.

The words of the figures — why a job waits, what a state means — are said
here in Russian, by the code the statistics give. The errors of the servers
and of the worker are shown as they were kept.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.admin import support
from app.admin.door import Door, Refused
from app.stats import imports
from app.stats.imports import TooSlow
from app.storage import heartbeats, history_jobs, service_jobs
from app.storage.models import HistoryJob, ServiceJob, TrackedAccount, TrackedMarket
from app.tasks import kinds

logger = logging.getLogger(__name__)

# Who is said to have paused the whole queue, or lifted its pause: the admin
# UI, as a task says ``admin`` of whoever asked for it. (The command line
# says ``python -m app.history``.)
BY = "admin"
# The page asks for itself again while it is looked at (``admin.js``): the
# import moves, and a page received an hour ago would go on saying "alive" of
# a service that stopped since.
REFRESH_SECONDS = 15
# How long a reason may be, how long a search, and how many tasks are looked
# at for one subject.
LONGEST_REASON = 200
LONGEST_SEARCH = 200
RECENT_TASKS = 50
# A ledger as a form sends it: digits only, and not so many that it is not one.
LEDGER = re.compile(r"[0-9]{1,12}")
# The points of priority a form may ask for.
PRIORITIES = (0, 1, 2, 3)
# The most a comparison may ask of the pool: a mistyped budget is not to
# become a task of a month.
MOST_REQUESTS = 1_000_000
# What is said when the database had no time for a page: it is the worker's
# first, and a page is shown again soon enough.
BUSY = "база занята — попробуйте ещё раз"
# Why a window is not read again while the queue may be reading the same
# streams, and why a job is not woken while its window is read again.
IN_HANDS = (
    "Задание сейчас в работе: сначала поставьте его на паузу, затем перечитывайте окно — "
    "воркер очереди и перечитывание не должны читать одни потоки одновременно."
)
WHILE_READING = (
    "Окно этого субъекта перечитывается (задача {task}): пока задача не кончилась, задание не будят — "
    "иначе воркер очереди и перечитывание прочитают одни потоки одновременно."
)

# The states the queue takes jobs from. The page asks for itself again while
# a job is in them, and does not let a window be read again under the worker's hand.
ACTIVE = (history_jobs.WAITING, history_jobs.RUNNING)

# Why a job waits, or is in the state it is, in the words of the page — by the
# code ``app.stats.imports`` gives.
WHY = {
    imports.WORKING: "читается",
    imports.PAUSED: "на паузе",
    imports.GAVE_UP: "отказалось и ждёт человека",
    imports.FINISHED: "готово: доступное окно проверено",
    imports.NOT_FINAL: "готово до границы, которая ещё не окончательная",
    imports.GLOBAL_PAUSE: "вся очередь на паузе",
    imports.SERVERS: "серверы пула не отвечают",
    imports.RATE_LIMIT: "сервер просит подождать (429)",
    imports.POOL_SILENT: "молчит сервер, где может быть более старая история",
    imports.POOL_BEHIND: "пул не дошёл до границы задания",
    imports.BACKOFF: "пауза после неудачного среза",
    imports.ITS_TURN: "ждёт своего среза",
}
SAID_TASK = {"verify": "Сверка с пулом поставлена в очередь", "recover": "Перечитывание окна поставлено в очередь"}

# The words of the page, by what the database keeps.
WORDS: dict[str, Any] = {
    "titles": {"account": "Аккаунты", "market": "Рынки"},
    "kinds": {"account": "аккаунт", "market": "рынок"},
    # What the subjects of a share are called: counted, and shared by.
    "every": {"account": "аккаунты", "market": "рынки"},
    "of_all": {"account": "аккаунтов", "market": "рынков"},
    "states": {
        history_jobs.WAITING: "ждёт своего среза или времени следующей попытки",
        history_jobs.RUNNING: "его читает воркер службы истории",
        history_jobs.DONE: "доступное окно проверено",
        history_jobs.PARTIAL: "часть потоков проверена, остальные не читаются: ждёт человека",
        history_jobs.ERROR: "ничего не прочитано: ждёт человека",
        history_jobs.PAUSED: "остановлено: субъект выключен, или так решил человек",
    },
    "why": WHY,
    # Why a waiting job waits, in the order the page says it.
    "waiting": [(code, WHY[code]) for code in imports.WAITING_REASONS],
    "contexts": {
        history_jobs.PARENTS: "parents — родители записи, без которых её не сохранить",
        history_jobs.FULL: "full — весь контекст каждой транзакции",
    },
    # The reason the queue itself pauses a job on: its subject was switched off.
    "switched_off": {"account disabled": "аккаунт выключен", "market disabled": "рынок выключен"},
    "streams": {"transactions": "транзакции", "operations": "операции", "effects": "эффекты", "trades": "сделки"},
    "coverage": {
        "pending": "запланировано, не прочитано",
        "complete": "прочитано",
        "empty": "прочитано: записей нет",
        "unverifiable": "проверить нельзя: ниже того, что хранит пул",
    },
    # What the history service says of itself, after the badge that says it is alive.
    "service": {"idle": "и ждёт работы", "working": "— читает задание", "paused": "— вся очередь на паузе"},
    # A verification or a new reading, by the mode the task is given.
    "modes": {"verify": "Сверка с пулом", "recover": "Перечитывание окна"},
}


# -- how things are said ----------------------------------------------------------------


def percent(share: Any) -> str:
    """A share as the page says it. Rounded down: a page is not to say 100%
    of what is not all there. ``неизвестно`` when there is no share — a
    window that is not defined yet has none."""
    if not isinstance(share, (int, float)):
        return "неизвестно"
    if share >= 1:
        return "100%"
    if 0 < share < 0.01:
        return "меньше 1%"
    return f"{int(share * 100)}%"


def length(seconds: Any) -> str:
    """How long, in a few words: less than a second, then seconds, minutes,
    hours and days."""
    if not isinstance(seconds, (int, float)):
        return "—"
    if 0 < seconds < 1:
        return "меньше секунды"
    seconds = max(0, int(seconds))
    if seconds < 120:
        return f"{seconds} с"
    if seconds < 3600:
        return f"{seconds // 60} мин"
    if seconds < 86400:
        return f"{seconds // 3600} ч {seconds % 3600 // 60} мин"
    return f"{seconds // 86400} дн {seconds % 86400 // 3600} ч"


def ahead(seconds: Any) -> str:
    """A moment so many seconds from now; one that has passed is said so."""
    if not isinstance(seconds, (int, float)):
        return "—"
    return f"через {length(seconds)}" if seconds >= 0 else f"должна была быть {length(-seconds)} назад"


def paused_because(item: dict[str, Any]) -> str:
    """Why a job is paused, in Russian when the queue's own reason says it."""
    kept = item.get("paused_because") or ""
    return WORDS["switched_off"].get(kept, kept)


def how_bottom(window: dict[str, Any]) -> str:
    """How the bottom of a job's window is known: the words the statistics
    keep are for a log, these are for the page."""
    if window["bottom"] is None:
        return "не известен: ни один срез ещё не нашёл, где начинается история пула"
    if "asked for" in window["bottom_how"]:
        return "задан при постановке задания"
    return "найден первым срезом: там начиналась история пула"


def how_cursor(cursor: dict[str, Any]) -> str:
    """What says where a stream stands: its checkpoint, or the end of what is
    verified."""
    if cursor["at_ledger"] is None:
        return "ещё не читался"
    return "по чекпоинту" if cursor["how"] == "its checkpoint" else "там, где кончается проверенное"


def _unknown(_: str | None) -> str:
    """Why a share is unknown, in Russian: the window of the job is not
    defined until a slice of it found where the pool's history begins."""
    return "окно ещё не определено: задание спрашивают настолько далеко, насколько пул хранит, и ни один его срез ещё не нашёл, где история пула начинается"


# The names under which the statistics keep a moment as text, besides those
# that end in ``_at``.
MOMENTS = ("at", "ledger_time", "reached_ledger_time")


def _moments(value: Any, key: str = "") -> Any:
    """The plain data of :mod:`app.stats.imports` with its moments read back
    from text: the templates show them with the filters every page shares. A
    text under such a name that is not a moment is left as it is."""
    if isinstance(value, dict):
        return {name: _moments(item, name) for name, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_moments(item, key) for item in value]
    if isinstance(value, str) and (key.endswith("_at") or key in MOMENTS):
        try:
            moment = datetime.fromisoformat(value)
        except ValueError:
            return value
        return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)
    return value


# -- what a form says -------------------------------------------------------------------


def _filters(request: Request) -> tuple[str | None, str | None, str | None]:
    """What the table is asked to show: a kind, a state and a part of a
    subject. What is none of these is no filter — not an error."""
    kind, state = request.query_params.get("kind", ""), request.query_params.get("state", "")
    sought = request.query_params.get("q", "").strip()
    return (
        kind if kind in imports.STREAMS else None,
        state if state in history_jobs.STATES else None,
        sought[:LONGEST_SEARCH] or None,
    )


def _reason(form: dict[str, str]) -> str:
    """Why a pause was made, as the person wrote it."""
    text = (form.get("because") or "").strip()
    if not text:
        raise Refused("Скажите причину паузы: она хранится с ней и показывается на странице.")
    if len(text) > LONGEST_REASON:
        raise Refused(f"Причина длиннее {LONGEST_REASON} знаков: скажите её короче.")
    return text


def _ledger(form: dict[str, str], name: str, what: str) -> int:
    """A ledger as a form sends it: a whole positive number, and nothing else
    is taken for one."""
    text = (form.get(name) or "").strip()
    if not LEDGER.fullmatch(text) or int(text) < 1:
        raise Refused(f"{what}: целое положительное число — номер леджера.")
    return int(text)


def _window(form: dict[str, str]) -> tuple[int, int]:
    """The range a form asks to compare or to read again."""
    low, high = _ledger(form, "from_ledger", "Нижняя граница"), _ledger(form, "to_ledger", "Верхняя граница")
    if low > high:
        raise Refused(f"Нижняя граница ({low}) выше верхней ({high}): укажите диапазон от меньшего леджера к большему.")
    return low, high


def _budget(form: dict[str, str], usual: int) -> int:
    """How many requests a task may make: what was said, or the usual."""
    text = (form.get("max_requests") or "").strip()
    if not text:
        return usual
    if not LEDGER.fullmatch(text) or not 1 <= int(text) <= MOST_REQUESTS:
        raise Refused(f"Бюджет: целое число запросов от 1 до {MOST_REQUESTS}, или пустое — тогда {usual}.")
    return int(text)


# -- what a page reads besides the statistics --------------------------------------------


def _service(engine: Engine, now: datetime) -> dict[str, Any]:
    """The history service's last word of itself, and whether it is stale:
    an empty queue and a service that is not running look the same in the
    jobs."""
    word = heartbeats.read(engine, heartbeats.HISTORY)
    age = heartbeats.age(word, now)
    return {
        "word": None if word is None else _moments(word),
        "stale": heartbeats.is_stale(word, now),
        "age_seconds": None if age is None else age.total_seconds(),
    }


def _watched(engine: Engine, kind: str, subject: str) -> bool:
    """Whether the job's subject is on its watch list and switched on — what
    the ``enqueue`` command asks before it plans. A switched-off subject's
    job is paused again at its next slice: planning it would be for nothing."""
    with Session(engine) as session:
        row = session.get(TrackedAccount, subject) if kind == "account" else session.get(TrackedMarket, tuple(subject.split(">")))
    return row is not None and bool(row.enabled)


def _job(engine: Engine, job_id: int) -> HistoryJob:
    job = history_jobs.get(engine, job_id)
    if job is None:
        raise Refused(f"Задания {job_id} нет.")
    return job


def _tasks(engine: Engine, kind: str, subject: str) -> list[ServiceJob]:
    """The comparisons and the new readings that were started for a subject:
    what is queued or running, then the newest that ended."""
    of_the_subject = (
        ServiceJob.kind == "verify", ServiceJob.params["kind"].astext == kind, ServiceJob.params["subject"].astext == subject
    )
    with Session(engine) as session:
        going = session.scalars(select(ServiceJob).where(*of_the_subject, ServiceJob.state.in_(service_jobs.ACTIVE)).order_by(ServiceJob.id))
        ended = session.scalars(
            select(ServiceJob)
            .where(*of_the_subject, ServiceJob.state.not_in(service_jobs.ACTIVE))
            .order_by(ServiceJob.created_at.desc(), ServiceJob.id.desc())
            .limit(RECENT_TASKS)
        )
        return [*going, *ended]


def _reading_again(engine: Engine, kind: str, subject: str) -> ServiceJob | None:
    """The new reading of a subject's window that is queued or running, if
    there is one. Nothing serialises it with a slice of the queue's job over
    the same streams — two readers would move one checkpoint each their own
    way, and a job could be declared done over coverage turned back to
    pending — so the page keeps the two apart: the window is read again only
    of a job the queue does not take, and such a job is not woken meanwhile.
    The command lines are not held by this; a worker's or a task's own guard
    would be."""
    with Session(engine) as session:
        return session.scalar(
            select(ServiceJob).where(
                ServiceJob.kind == "verify",
                ServiceJob.params["kind"].astext == kind,
                ServiceJob.params["subject"].astext == subject,
                ServiceJob.params["mode"].astext == "recover",
                ServiceJob.state.in_(service_jobs.ACTIVE),
            )
        )


def _not_woken(engine: Engine, job: HistoryJob) -> None:
    """Refuse to wake a job whose window is being read again."""
    reading = _reading_again(engine, job.kind, job.subject)
    if reading is not None:
        raise Refused(WHILE_READING.format(task=reading.id))


def _where(path: str, **said: Any) -> RedirectResponse:
    """To a page, with what was done said on it: only what is not empty."""
    asked = [(name, str(value)) for name, value in said.items() if value not in (None, "")]
    return RedirectResponse(path + ("?" + "&".join(f"{name}={quote(value)}" for name, value in asked) if asked else ""), status_code=303)


def build(door: Door) -> APIRouter:
    router = APIRouter(prefix="/import")
    said = {
        "words": WORDS, "percent": percent, "length": length, "ahead": ahead, "paused_because": paused_because,
        "how_bottom": how_bottom, "how_cursor": how_cursor, "unknown": _unknown,
    }

    def back(job_id: int, **what: Any) -> RedirectResponse:
        return _where(f"/admin/import/jobs/{job_id}", **what)

    @router.get("")
    def imports_page(request: Request, login: Any = Depends(door.open)) -> Any:
        engine: Engine = door.engine
        now = datetime.now(UTC)
        kind, state, sought = _filters(request)
        page, per_page, _ = support.page_of(request)
        whole = table = None
        trouble: list[str] = []
        # Two readings, each on its own: a database with no time for one may
        # well have time for the other.
        try:
            whole = _moments(imports.overview(engine, now=now).as_dict())
        except TooSlow as slow:
            logger.warning("the imports could not be counted for a page: %s", slow)
            trouble.append(f"Общих цифр нет: {BUSY}.")
        try:
            table = _moments(
                imports.jobs(engine, kind=kind, state=state, q=sought, page=page, per_page=per_page, now=now).as_dict()
            )
        except TooSlow as slow:
            logger.warning("the jobs could not be read for a page: %s", slow)
            trouble.append(f"Таблицы заданий нет: {BUSY}.")
        # ``refresh`` is not a name the template knows: it reaches ``render``,
        # which makes the page ask for itself again while it is looked at.
        return support.page(
            request, "imports.html", login, "import", refresh=REFRESH_SECONDS,
            whole=whole, table=table, trouble=trouble, pause=_moments(history_jobs.pause_state(engine)),
            service=_service(engine, now), measured_at=now,
            kind=kind or "", state=state or "", q=sought or "", page=page, per_page=per_page,
            states=history_jobs.STATES, stale_after=heartbeats.STALE_AFTER, **said,
        )

    @router.get("/jobs/{job_id}")
    def job_page(request: Request, job_id: int, login: Any = Depends(door.open)) -> Any:
        engine: Engine = door.engine
        now = datetime.now(UTC)
        try:
            detail = imports.job_detail(engine, job_id, now=now)
        except TooSlow as slow:
            logger.warning("job %s could not be read for a page: %s", job_id, slow)
            return support.render(request, "refused.html", login=login, why=f"Задание не показать: {BUSY}.", status_code=503)
        if detail is None:
            return support.render(request, "refused.html", login=login, why=f"Задания {job_id} нет.", status_code=404)
        found = _moments(detail.as_dict())
        job = found["job"]
        asked = request.query_params.get("task", "")
        again = _reading_again(engine, job["kind"], job["subject"])
        return support.page(
            request, "import_job.html", login, "import", refresh=REFRESH_SECONDS if job["state"] in ACTIVE else None,
            detail=found, job=job, tasks=_tasks(engine, job["kind"], job["subject"]),
            # What says the job may not be woken: a new reading of its window.
            again=None if again is None else again.id,
            asked_task=int(asked) if asked.isdigit() else None,
            # The lower boundary as the job was asked for it: None is "as far
            # back as the pool keeps", and no form can go below that.
            asked_low=_job(engine, job_id).from_ledger, recent=RECENT_TASKS,
            pause=_moments(history_jobs.pause_state(engine)), watched=_watched(engine, job["kind"], job["subject"]),
            usual=kinds.settings_of(door.settings)["verify_budget"], priorities=PRIORITIES, **said,
        )

    # -- the queue as a whole ------------------------------------------------------------

    @router.post("/pause")
    async def pause_queue(request: Request) -> Any:
        login, form = await door.form_of(request)
        history_jobs.pause_all(door.engine, because=_reason(form), by=BY)
        return _where(
            "/admin/import",
            notice="Вся очередь на паузе: ни один воркер истории не возьмёт задание, пока её не снимут. Живой сбор событий это не останавливает.",
        )

    @router.post("/resume")
    async def resume_queue(request: Request) -> Any:
        login, form = await door.form_of(request)
        if not history_jobs.is_paused(door.engine):
            return _where("/admin/import", error="Очередь и так не на паузе: ничего не изменилось.")
        history_jobs.resume_all(door.engine, by=BY)
        return _where("/admin/import", notice="Пауза всей очереди снята: воркеры истории снова берут задания.")

    # -- one job --------------------------------------------------------------------------

    @router.post("/jobs/{job_id}/pause")
    async def pause_job(request: Request, job_id: int) -> Any:
        login, form = await door.form_of(request)
        engine: Engine = door.engine
        if _job(engine, job_id).state == history_jobs.DONE:
            return back(job_id, error=f"Задание {job_id} уже готово: ставить его на паузу нечего.")
        history_jobs.pause(engine, ids=[job_id], because=_reason(form))
        return back(job_id, notice=f"Задание {job_id} на паузе. Срез, который читается, остановится перед следующим запросом; прочитанное сохранено.")

    @router.post("/jobs/{job_id}/resume")
    async def resume_job(request: Request, job_id: int) -> Any:
        login, form = await door.form_of(request)
        engine: Engine = door.engine
        job = _job(engine, job_id)
        _not_woken(engine, job)
        if job.state != history_jobs.PAUSED:
            return back(job_id, error=f"Задание {job_id} не на паузе ({job.state}): ничего не изменилось.")
        history_jobs.resume(engine, ids=[job_id])
        return back(job_id, notice=f"Задание {job_id} снова в очереди: счёт попыток начинается заново; прочитанное раньше не читается снова.")

    @router.post("/jobs/{job_id}/retry")
    async def retry_job(request: Request, job_id: int) -> Any:
        login, form = await door.form_of(request)
        engine: Engine = door.engine
        job = _job(engine, job_id)
        _not_woken(engine, job)
        if job.state not in history_jobs.GAVE_UP:
            return back(job_id, error=f"Задание {job_id} не отказывалось ({job.state}): повторять нечего.")
        history_jobs.retry(engine, ids=[job_id])
        return back(job_id, notice=f"Задание {job_id} снова в очереди: счёт попыток начинается заново; прочитанное раньше не читается снова.")

    @router.post("/jobs/{job_id}/priority")
    async def priority_job(request: Request, job_id: int) -> Any:
        login, form = await door.form_of(request)
        engine: Engine = door.engine
        _job(engine, job_id)
        points = (form.get("priority") or "").strip()
        if points not in {str(number) for number in PRIORITIES}:
            raise Refused(f"Приоритет — одно из {', '.join(str(number) for number in PRIORITIES)}.")
        history_jobs.set_priority(engine, priority=int(points), ids=[job_id])
        # A point is worth this many minutes of going first after each slice.
        minutes = int(history_jobs.PRIORITY_STEP.total_seconds()) // 60
        if points == "0":
            return back(job_id, notice=f"Приоритет задания {job_id} сброшен: оно идёт в очередь как все.")
        return back(
            job_id,
            notice=(
                f"Приоритет задания {job_id}: {points}. После каждого среза оно идёт в очередь так, как будто его обслужили "
                f"на {int(points) * minutes} мин раньше: ему достаётся больше срезов, а остальные всё равно получают свои."
            ),
        )

    @router.post("/jobs/{job_id}/plan")
    async def plan_job(request: Request, job_id: int) -> Any:
        login, form = await door.form_of(request)
        engine: Engine = door.engine
        was = _job(engine, job_id)
        if not _watched(engine, was.kind, was.subject):
            raise Refused("Субъект задания выключен или убран из списка наблюдения: его история не читается. Включите его на странице аккаунтов или рынков.")
        _not_woken(engine, was)
        top = _ledger(form, "to_ledger", "Верхняя граница")
        low = _ledger(form, "from_ledger", "Нижняя граница") if (form.get("from_ledger") or "").strip() else None
        if low is not None and low > top:
            raise Refused(f"Нижняя граница ({low}) выше верхней ({top}): укажите диапазон от меньшего леджера к большему.")
        job, _ = history_jobs.plan(engine, kind=was.kind, subject=was.subject, to_ledger=top, from_ledger=low, resume=True)
        if (job.to_ledger, job.from_ledger, job.state) == (was.to_ledger, was.from_ledger, was.state):
            return back(job_id, error="Окно не изменилось: границы двигаются только наружу — ниже и выше.")
        from_ = f"от леджера {job.from_ledger}" if job.from_ledger is not None else "от того, насколько хватает истории пула"
        notice = f"Окно задания {job_id}: {from_} до леджера {job.to_ledger}. Прогресс не сброшен: что проверено, то проверено."
        if job.state == history_jobs.PAUSED:
            notice += " Задание осталось на паузе."
        elif job.state == history_jobs.WAITING and was.state != history_jobs.WAITING:
            notice += " Задание вернулось в очередь."
        return back(job_id, notice=notice)

    async def a_reading(request: Request, job_id: int, mode: str) -> RedirectResponse:
        """Put a comparison, or a new reading, of the job's subject over a
        range in the queue of tasks. Nothing is read here: the task does it,
        within its budget, and one such task of a subject is queued or
        running at a time. A comparison stores nothing and is asked of any
        job; a new reading only of one the queue is not working on."""
        login, form = await door.form_of(request)
        engine: Engine = door.engine
        job = _job(engine, job_id)
        if not _watched(engine, job.kind, job.subject):
            raise Refused("Субъект задания выключен или убран из списка наблюдения: сверять и перечитывать его историю не для чего.")
        if mode == "recover" and job.state in ACTIVE:
            raise Refused(IN_HANDS)
        low, high = _window(form)
        budget = _budget(form, kinds.settings_of(door.settings)["verify_budget"])
        task, made = service_jobs.enqueue(
            engine,
            "verify",
            {"kind": job.kind, "subject": job.subject, "from_ledger": low, "to_ledger": high, "mode": mode, "max_requests": budget},
            one_at_a_time=job.subject,
        )
        where = f"задача {task.id} (/admin/tasks/{task.id})"
        if not made:
            return back(job_id, notice=f"Сверка или перечитывание этого субъекта уже стоит в очереди или идёт: {where}. Ничего не добавлено.", task=task.id)
        return back(job_id, notice=f"{SAID_TASK[mode]}: {where}. Выполнит её фоновый поток воркера — не этот запрос.", task=task.id)

    @router.post("/jobs/{job_id}/verify")
    async def verify_job(request: Request, job_id: int) -> Any:
        return await a_reading(request, job_id, "verify")

    @router.post("/jobs/{job_id}/recover")
    async def recover_job(request: Request, job_id: int) -> Any:
        return await a_reading(request, job_id, "recover")

    return router
