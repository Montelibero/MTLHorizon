"""The activity of the watched accounts: one table, and one thing the owner
can do from a row of it.

Everything counted is :mod:`app.stats.activity`'s: what each watched account
took part in over a period, by the time of the ledger — never by the time a
record was imported — against the window of the same length before it;
which of its operations dealt with its own assets and which with other
people's, where the database proves it, and «неизвестно» where it does not;
and how much data that is, approximately. The installation's own figures
stand apart, counted on their own and never as a sum over accounts. So does
the speed of the import: the one thing here that is counted by the time of
storing.

The page shows, and lets the owner switch the watch of an account off by
hand. It blocks nothing by itself, raises no alarm and deletes nothing. An
account switched off keeps its history, and nothing else of it is touched
here: what follows from it — its import paused, its subscription retired —
is done by the worker.

What the statistics say in English is said here in the owner's words; what
is not met stays as the statistics wrote it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.admin import support
from app.admin.areas.dashboard import size
from app.admin.door import Door, Refused
from app.stats import activity as stats
from app.stats.dashboard import COUNT_CACHE_SECONDS, TtlCache, timed_out
from app.watch import accounts as watched

HOME = "/admin/activity"
ON, OFF = "on", "off"
DEFAULT_ORDER = "desc"
# The speed of the import is shown over the last day.
IMPORT_HOURS = 24
# More than this of a reason is not kept from a form.
REASON_LIMIT = 1000
_ADDRESS = re.compile(r"G[A-Z0-9]{55}")

PERIODS = (("day", "сутки"), ("week", "неделя"), ("month", "месяц"), ("range", "с даты по дату"))
KINDS = (("transactions", "Транзакции"), ("operations", "Операции"), ("effects", "Эффекты"), ("trades", "Сделки"))
SORTS = (*KINDS, ("account", "по адресу"))
BY_COUNT = (("desc", "больше первыми"), ("asc", "меньше первыми"))
BY_ADDRESS = (("asc", "с начала алфавита"), ("desc", "с конца алфавита"))

VOLUME_FORMULA = (
    "объём = транзакции × (байтов на строку history_transaction_participants + байтов на строку "
    "history_transactions) + операции × (байтов на строку history_operation_participants + байтов на "
    "строку history_operations) + эффекты × байтов на строку history_effects + сделки × байтов на строку "
    "history_trades — по строкам окна. Байтов на строку таблицы = (её данные + то, что хранится вне основной "
    "кучи (TOAST) + её индексы) / число строк, которое планировщик считает у неё имеющимся (pg_class.reltuples). "
    "Это число — оценка, и всё, что из него сделано, — тоже."
)
VOLUME_WARNING = (
    "Записи двух наблюдаемых аккаунтов общие и считаются каждому: транзакция, в которой участвовали два "
    "наблюдаемых аккаунта, входит в объём обоих. Объёмы аккаунтов складывать нельзя."
)
ASSETS_RULE = (
    "Активы операции читаются из её сохранённого тела: каждый актив, который она называет. Операция аккаунта — "
    "своя, если каждый актив, который она называет, выпущен самим аккаунтом или это актив, на который у аккаунта "
    "по доказательствам была trustline в тот момент: сохранённый эффект trustline_created аккаунта по этому "
    "активу в леджере не позже леджера операции. Чужая — если она называет актив, который ни то и ни другое. "
    "Native (XLM) никем не выпущен и trustline не требует: он чужой для каждого аккаунта. Операция без актива "
    "считается отдельно. Операция оценивается только там, где эффекты аккаунта проверены без пропусков "
    "(sync_coverage: complete или empty) от сохранённого эффекта account_created аккаунта — ровно одного в "
    "сохранённой истории — до леджера операции: только там отсутствие trustline_created значит, что trustline не "
    "было. Вне этого операции не оцениваются, и аккаунт без такого доказательства получает «неизвестно»."
)
IMPORT_NOTE = (
    "Время строки — когда она попала в базу (created_at записей, first_seen_at реестров). Последний час ещё не "
    "кончился. Таблицы-связки аккаунтов с транзакциями и операциями собственного времени не имеют и не "
    "считаются. Таблица слишком большая, чтобы читать её целиком ради страницы, пропускается и говорит об этом."
)


@dataclass(frozen=True)
class Period:
    """What period was asked for: what the statistics take (``asked``), its
    name, the two dates as they were written — for the controls to show them
    again — and, when the asking could not be followed, what was wrong with
    it: then the default period is shown and the trouble said with it."""

    asked: Any
    name: str
    start: str = ""
    end: str = ""
    trouble: str = ""


def now() -> datetime:
    """The moment the page is read at; for a day, a week or a month, the end
    of its window."""
    return datetime.now(UTC)


def _date(written: str) -> datetime:
    """A date as the controls send it (YYYY-MM-DD): the start of that day, UTC."""
    try:
        return datetime.strptime(written, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError as error:
        raise ValueError("даты периода пишутся как ГОД-МЕСЯЦ-ДЕНЬ") from error


def period_of(request: Request) -> Period:
    """The period the page is asked for: a day, a week or a month, or a range
    by two dates in UTC, the last day not included. What cannot be read is
    not guessed at: the week is shown, and the trouble said."""
    name = request.query_params.get("period") or stats.DEFAULT_PERIOD
    start, end = request.query_params.get("from", "").strip(), request.query_params.get("to", "").strip()
    if name in stats.PERIODS:
        return Period(name, name, start, end)
    try:
        if name != "range":
            raise ValueError(f"период «{name[:40]}» не понят: это сутки, неделя, месяц или две даты")
        low, high = _date(start), _date(end)
        if low >= high:
            raise ValueError("у периода по датам конец должен быть позже начала")
    except ValueError as error:
        return Period(stats.DEFAULT_PERIOD, stats.DEFAULT_PERIOD, start, end, f"{error}. Показана неделя.")
    return Period((low, high), "range", start, end)


def decimal(value: float | None) -> str:
    """A number with a fraction as the page writes it: ``150``, ``4,33``;
    «—» when there is none."""
    if value is None:
        return "—"
    return f"{value:.2f}".rstrip("0").rstrip(".").replace(".", ",")


def percent(value: float | None, *, signed: bool = False) -> str:
    """A number of percents as the page writes it: ``150 %``, ``33,33 %``;
    with a sign, ``+150 %``. «—» when there is none."""
    if value is None:
        return "—"
    return ("+" if signed and value > 0 else "") + decimal(value) + " %"


def share(part: float | None) -> str:
    """A share (0 to 1) in percents: ``50 %``. «—» when there is nothing to share."""
    return "—" if part is None else percent(round(100 * part, 2))


def num(number: int) -> str:
    """A whole number as the pages write it, with its thousands apart."""
    return f"{number:,}".replace(",", " ")


def why(assets: stats.Assets) -> str:
    """Why the operations of an account were not judged, or judged only in
    part: the reason of the statistics in the owner's words."""
    reason = assets.reason
    # The ledger the reason names, written as the pages write numbers.
    ledger = [num(int(found)) for found in re.findall(r"ledger (\d+)", reason)]
    if reason == stats.NO_CREATION_STORED:
        return (
            "в сохранённой истории нет эффекта account_created этого аккаунта: trustline, открытую до того, как "
            "история начала сохраняться, не отличить от той, которой не было"
        )
    if reason == stats.CREATED_MORE_THAN_ONCE:
        return "аккаунт создавался в сохранённой истории больше одного раза (слияние и создание заново): правило к нему не применяется"
    if ledger and reason.startswith(stats.NOT_VERIFIED_FROM_CREATION.split("{ledger}")[0]):
        return (
            f"эффекты аккаунта не проверены без пропусков от сохранённого account_created до леджера {ledger[0]}: "
            "эффект, который не сохранён, сделал бы trustline отсутствующей"
        )
    if ledger and reason.startswith(stats.VERIFIED_ONLY_UP_TO.split("{ledger}")[0]):
        return f"оценено только до леджера {ledger[0]} — докуда эффекты аккаунта проверены без пропусков от его создания"
    if reason == stats.UNREADABLE_LINE:
        return "у сохранённого эффекта trustline_created аккаунта не читается, на какой актив"
    if reason == stats.RAN_OUT_OF_TIME:
        return "запрос, который оценивает операции, не успел за отведённое время"
    return reason


def volume_trouble(table: stats.ActivityPage) -> str:
    """Why the volume of an account's data is not known, when it is not: no
    text when every row of the page has one."""
    if all(row.volume_bytes is not None for row in table.rows):
        return ""
    missing = [name for name, width in table.widths.items() if width.bytes_per_row is None]
    if not missing:
        return "Объём неизвестен: не из всех таблиц формулы есть данные"
    return (
        "Объём неизвестен: у планировщика нет числа строк для "
        + ", ".join(missing)
        + " — их не анализировали (ANALYZE даёт планировщику его число; после большой загрузки он делает это сам)"
    )


def why_not(speed: stats.TableSpeed) -> str:
    """Why a table was not measured for the speed of the import."""
    reason = speed.reason
    if reason.startswith("not measured: the table is larger"):
        return "не измерено: время сохранения строки не индексировано, и страницу нельзя читать через всю таблицу такого размера"
    if "ran out of its time" in reason:
        return "не измерено: чтение таблицы целиком не успело за отведённое время"
    if "no such table" in reason:
        return "не измерено: такой таблицы нет"
    return reason


def back_to(where: str | None, notice: str) -> str:
    """Where a form came from — only this page, with whatever it was showing
    — with the notice in its query. Anything else is the page itself."""
    parts = urlsplit(where or "")
    kept = [(name, value) for name, value in parse_qsl(parts.query) if name not in ("notice", "error")]
    path = parts.path if parts.path == HOME else HOME
    return path + "?" + urlencode([*kept, ("notice", notice)])


def build(door: Door) -> APIRouter:
    router = APIRouter()
    # One cache for this application: how wide a row of each table is, and
    # the speed of the import, are the same for every page and expensive.
    cache = TtlCache(ttl_seconds=COUNT_CACHE_SECONDS)

    @router.get("/activity")
    def activity_page(request: Request, login: Any = Depends(door.open)) -> Any:
        engine: Engine = door.engine
        at = now()
        period = period_of(request)
        enabled = {ON: True, OFF: False}.get(request.query_params.get("enabled", ""))
        sort = request.query_params.get("sort") or stats.DEFAULT_SORT
        sort = sort if sort in stats.SORTS else stats.DEFAULT_SORT
        descending = request.query_params.get("order", DEFAULT_ORDER) != "asc"
        try:
            asked = int(request.query_params.get("per_page", ""))
        except ValueError:
            asked = 0
        number, per_page, _ = support.page_of(request, per_page=asked or None)
        try:
            table = stats.table(
                engine,
                period=period.asked,
                now=at,
                enabled=enabled,
                sort=sort if sort in stats.SORTS else stats.DEFAULT_SORT,
                descending=descending,
                page=number,
                per_page=per_page,
                cache=cache,
            )
            speed = stats.import_speed(engine, now=at, hours=IMPORT_HOURS, cache=cache)
        except OperationalError as error:
            if not timed_out(error):
                raise
            # The database was let go: a page that is too slow says so, and holds nothing.
            return support.refused(
                request,
                "База не успела посчитать активность за отведённое время — ничего не держится. Возьмите период короче или обновите страницу.",
                status_code=503,
            )
        return support.page(
            request,
            "activity.html",
            login,
            "activity",
            table=table,
            speed=speed,
            now=at,
            period=period,
            period_words=dict(PERIODS)[period.name],
            # Where the page is, for a form to bring the owner back to what it showed.
            back=request.url.path + support.query(request),
            busiest=max((hour.rows for hour in speed.series), default=0),
            enabled=request.query_params.get("enabled", ""),
            sort=sort,
            descending=descending,
            per_page=per_page,
            periods=PERIODS,
            kinds=KINDS,
            sorts=SORTS,
            orders=BY_ADDRESS if sort == "account" else BY_COUNT,
            default_per_page=support.PER_PAGE,
            volume_formula=VOLUME_FORMULA,
            volume_warning=VOLUME_WARNING,
            assets_rule=ASSETS_RULE,
            import_note=IMPORT_NOTE,
            volume_trouble=volume_trouble(table),
            why=why,
            why_not=why_not,
            percent=percent,
            decimal=decimal,
            share=share,
            size=size,
            reason_limit=REASON_LIMIT,
        )

    @router.post("/activity/{address}/disable")
    async def disable(request: Request, address: str) -> RedirectResponse:
        """Switch the watch of an account off, from its row. Its history
        stays; nothing else is touched here."""
        _, form = await door.form_of(request)
        if not _ADDRESS.fullmatch(address):
            raise Refused("Это не адрес аккаунта.")
        reason = (form.get("reason") or "").strip()
        if len(reason) > REASON_LIMIT:
            raise Refused(f"Причина длиннее {REASON_LIMIT} знаков: сократите её.")
        with Session(door.engine) as session, session.begin():
            change = watched.set_enabled(session, address, enabled=False, reason=reason)
        short = address[:6] + "…"
        if change.outcome == "not watched":
            raise Refused(f"Аккаунт {short} не наблюдается: нечего отключать.")
        if change.outcome == watched.DISABLED:
            notice = f"Наблюдение за аккаунтом {short} отключено. История осталась на месте." + (
                f" Причина: {reason}" if reason else ""
            )
        else:
            notice = f"Аккаунт {short} уже был отключён: ничего не изменилось."
        return RedirectResponse(back_to(form.get("back"), notice), status_code=303)

    return router
