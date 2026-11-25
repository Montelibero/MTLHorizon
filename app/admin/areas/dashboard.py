"""The dashboard of the admin UI: the whole installation on one page.

Everything shown is :func:`app.stats.dashboard.snapshot`'s, read from
PostgreSQL when the page is asked for and from nothing else — not Horizon,
not the Notifier, no log file. What this module adds is the saying of it.

Each service — the live worker and the history service — is said in one of
three sentences, so that an empty queue is never taken for a service that is
not running: it works and nothing waits; it works, with what is in progress
and what waits; or it is not heard of, and then what its queue holds is said
as well. Every figure says whether it was counted exactly or is an estimate,
and when it was measured.

While the page is looked at it refreshes itself, and that is the browser
asking for the same page again. The one expensive thing — counting large
tables — is kept in a cache of the process for a few minutes: several
browsers refreshing one dashboard share it, and the worker is not kept
waiting behind a page.

The dashboard is the door's own first page, ``/admin``: its route is put on
the door's router by :func:`build`, which returns a router of its own with
nothing in it.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, Request
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from app.admin import support
from app.admin.areas.tasks import duration, figures
from app.admin.door import Door
from app.stats import dashboard as stats
from app.storage import service_jobs
from app.tasks.kinds import describe

# How often the page asks for itself again while it is looked at.
REFRESH_SECONDS = 30
ACTIVE = service_jobs.ACTIVE
ENDED = ("done", "failed", "cancelled")
# The largest tables shown one by one; the rest are summed.
TABLES_SHOWN = 12

# What the rows of the tables of distinct records are called.
UNIQUE = {"transactions": "транзакции", "operations": "операции", "effects": "эффекты", "trades": "сделки"}

# What the queue of events is as a whole, said apart from what the worker says
# of itself. The same queue is another thing under a worker that takes it,
# under a worker that is not there, and under one that says it is idle while
# work is due.
QUEUE_SAID = {
    stats.QUEUE_EMPTY: "Очередь пуста: ничего не ждёт и ничего не делается.",
    stats.QUEUE_WORKING: "Работа есть, и воркер её берёт.",
    stats.QUEUE_NOT_TAKEN: "Работа есть, а брать её некому: воркера не слышно.",
    stats.QUEUE_IDLE_WITH_DUE_WORK: "Работа давно ждёт, а воркер говорит, что простаивает: возможно, застрял.",
}

# On what a figure rests, as the statistics write it, and the same in the
# owner's words. A basis made of several pieces is translated whole, the
# longest piece first; a piece that is not here stays as it was written
# rather than being guessed at.
SAID = (
    ("; the exact count ran out of time", "; точный подсчёт не успел за отведённое время"),
    ("not known: the table's rows are not counted", "число неизвестно: строки таблицы никто не пересчитывает"),
    ("counted: the table's rows less the other states", "посчитано: строки таблицы минус остальные состояния"),
    ("the table's rows, counted earlier", "строки таблицы, посчитанные раньше"),
    ("less the other states counted now", "минус остальные состояния, посчитанные сейчас"),
    (
        stats.NEVER_ANALYZED,
        "неизвестно: таблицу не анализировали (ANALYZE даст планировщику её число), а для страницы она слишком велика, чтобы считать",
    ),
    (stats.RELTUPLES, "оценка планировщика PostgreSQL"),
    ("partial and dead", "partial и dead вместе"),
    (stats.COUNTED, "посчитано в базе"),
)


def now() -> datetime:
    """The moment the page is read at."""
    return datetime.now(UTC)


def size(number: int | None) -> str:
    """A number of bytes in a few characters: 823 Б, 112 КБ, 9,9 МБ."""
    if number is None:
        return "—"
    value = float(number)
    unit = "Б"
    for unit in ("Б", "КБ", "МБ", "ГБ", "ТБ"):
        if value < 1024 or unit == "ТБ":
            break
        value /= 1024
    if unit == "Б":
        return f"{int(value):,}".replace(",", " ") + " Б"
    return f"{value:.1f}".replace(".", ",").removesuffix(",0") + " " + unit


def how(figure: stats.Figure) -> str:
    """On what a figure rests, in the owner's words."""
    said = figure.basis
    for written, words in sorted(SAID, key=lambda piece: -len(piece[0])):
        said = said.replace(written, words)
    return said


def _span(seconds: float) -> str:
    """A length of time in a few words, by the steps of the ``ago`` filter."""
    if seconds < 60:
        return f"{int(seconds)} с"
    if seconds < 3600:
        return f"{int(seconds // 60)} мин"
    if seconds < 86400:
        return f"{int(seconds // 3600)} ч"
    return f"{int(seconds // 86400)} дн"


def till(at: datetime | None, moment: datetime) -> str:
    """How far a moment is from ``moment``, either way: ``через 5 мин``,
    ``5 мин назад``, or ``сейчас``. «—» when there is no moment."""
    if at is None:
        return "—"
    if at.tzinfo is None:
        at = at.replace(tzinfo=UTC)
    seconds = (at.astimezone(UTC) - moment).total_seconds()
    if seconds > 30:
        return f"через {_span(seconds)}"
    if seconds >= -30:
        return "сейчас"
    return f"{_span(-seconds)} назад"


def service_said(
    beat: stats.Heartbeat, *, state: str, working: int | None, waiting: int | None, paused: bool = False
) -> dict[str, Any]:
    """A service in words.

    ``state`` is what the statistics made of its heartbeat (idle, working,
    paused, not_running or stale), ``working`` and ``waiting`` what its queue
    holds. The saying is one of three: the service is heard of and its queue
    is empty; it is heard of and works through a queue — or stands over it
    on pause; or it is not heard of, and then the queue is said too, because
    a queue that waits for nobody is another matter than a queue with
    nothing in it, and the numbers alone do not tell them apart.
    """
    working, waiting = int(working or 0), int(waiting or 0)
    last = {
        "idle": "простаивает",
        "working": "на задании " + (f"#{beat.job_id}" if beat.job_id is not None else "без номера"),
    }.get(beat.state or "", "делает неизвестно что")
    if beat.stale:
        tone, word = "bad", "не слышно"
        silence = (
            "не слышно ни разу" if beat.at is None or beat.age_seconds is None else f"не слышно уже {_span(beat.age_seconds)}"
        )
        saying = f"{silence} — служба не работает или застряла"
        queue = "очередь пуста" if working + waiting == 0 else f"ждут {waiting}, в работе {working}"
        doing = f"в последний раз — {last}"
    elif paused or state == "paused":
        tone, word = "wait", "пауза"
        saying = f"работает, но очередь на паузе: в работе {working}, ждут {waiting}"
        queue, doing = "", "на паузе" if beat.state != "working" else last
    elif working + waiting == 0:
        tone, word = "ok", "на связи"
        saying, queue, doing = "работает, очередей нет", "", last
    else:
        tone, word = "work", "на связи"
        saying, queue, doing = f"работает: в работе {working}, ждут {waiting}", "", last
    return {"beat": beat, "tone": tone, "word": word, "saying": saying, "queue": queue, "doing": doing}


def build(door: Door) -> APIRouter:
    # One cache for this application: what was expensive to count is kept for
    # a few minutes, and every browser looking at the dashboard shares it.
    cache = stats.TtlCache(ttl_seconds=stats.COUNT_CACHE_SECONDS)

    def dashboard_page(request: Request, login: Any = Depends(door.open)) -> Any:
        engine: Engine = door.engine
        at = now()
        try:
            shown = stats.snapshot(engine, now=at, cache=cache)
        except OperationalError as error:
            if not stats.timed_out(error):
                raise
            # The database was let go: a page that is too slow says so, and holds nothing.
            return support.refused(
                request,
                f"База не успела ответить за {stats.STATEMENT_TIMEOUT_SECONDS:g} с — дашборд не построен, ничего не держится. Обновите страницу.",
                status_code=503,
            )
        largest = sorted(shown.database.tables, key=lambda table: -table.total_bytes)
        return support.page(
            request,
            "dashboard.html",
            login,
            "dashboard",
            refresh=REFRESH_SECONDS,
            shown=shown,
            now=at,
            worker=service_said(
                shown.worker.heartbeat,
                state=shown.worker.state,
                working=shown.queue.running.value,
                waiting=shown.queue.pending.value,
            ),
            history=service_said(
                shown.history.heartbeat,
                state=shown.history.status,
                working=shown.history.jobs["running"].value,
                waiting=shown.history.jobs["waiting"].value,
                paused=shown.history.queue_paused,
            ),
            queue_said=QUEUE_SAID[shown.queue.status],
            tasks_running=service_jobs.recent(engine, states=ACTIVE, limit=25),
            ended=service_jobs.recent(engine, states=ENDED, limit=1),
            # The largest tables one by one; the rest are summed into one line.
            tables=largest[:TABLES_SHOWN],
            other_tables=len(largest[TABLES_SHOWN:]),
            other_bytes=sum(table.total_bytes for table in largest[TABLES_SHOWN:]),
            # For each asset marked for discovery, the pass it had last — or none.
            passes=[
                (asset, next((item for item in shown.discovery.last_passes if item["asset_id"] == asset), None))
                for asset in shown.discovery.assets
            ],
            # The distinct records of the installation: what each is called
            # and where it is kept.
            unique=[
                (UNIQUE[name], table, shown.database.rows.get(table)) for name, table in stats.UNIQUE_TABLES.items()
            ],
            paused_at=stats.read_moment(shown.history.pause.get("at")) if shown.history.queue_paused else None,
            errors_shown=stats.ERRORS_SHOWN,
            problems_shown=stats.PROBLEMS_SHOWN,
            till=till,
            oldest=_span,
            describe=describe,
            figures=figures,
            duration=duration,
            how=how,
            size=size,
            queue_states=stats.QUEUE_STATES,
            history_states=stats.HISTORY_STATES,
            subscription_states=stats.SUBSCRIPTION_STATES,
        )

    # The dashboard is the door's own first page: at ``""`` on the door's
    # router, which is ``/admin`` once that router is put on the application
    # under its prefix. It cannot be in a router of its own for the door to
    # include: FastAPI refuses an empty path to a router that is included
    # without a prefix. What is returned carries no route of its own.
    door.router.get("")(dashboard_page)
    return APIRouter()
