"""A chart of a market drawn from what the archive holds: price and volume
by buckets of time, as an SVG made here, by the server.

The buckets are the history API's own — :func:`app.history_api.aggregations.buckets`,
the formulas stage 03 verified against the source — read from PostgreSQL in a
read-only transaction. Nothing is asked of any server, and behind the door no
token is needed. An interval without trades gets no bucket and no mark: what
is quiet and what was merely never read would look the same, did the page not
show next to the price what the archive has verified of the market
(:mod:`app.history_api.coverage`) — its ranges, what is still pending and
what can no longer be verified.

A period is bounded. The resolution is chosen for the period or given, and a
period that would make more than a few thousand buckets is refused before a
trade is read.

The SVG is made of numbers: every value in it is a count, an amount or a
moment that the database gave as digits, and the only words are this module's
own labels. Nothing a person typed and nothing stored as text is put into it,
so it is the one thing a page of this UI shows unescaped. Amounts and prices
are read as decimals and stay exact; a float appears only where a number
becomes a place on the picture.
"""

from __future__ import annotations

import calendar
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, NamedTuple
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Request
from markupsafe import escape
from sqlalchemy import func, select
from sqlalchemy.engine import Connection
from sqlalchemy.exc import OperationalError

from app.admin import auth, support
from app.admin.door import Door
from app.backfill import config as watch_lists
from app.backfill.jobs import asset_params, market_key
from app.history_api import aggregations, coverage, streams
from app.storage.models import HistoryLedger

MINUTE, HOUR, DAY, WEEK = 60_000, 3_600_000, 86_400_000, 604_800_000
RESOLUTIONS = aggregations.RESOLUTIONS
NAMES = {MINUTE: "1 минута", 5 * MINUTE: "5 минут", 15 * MINUTE: "15 минут", HOUR: "час", DAY: "сутки", WEEK: "неделя"}
PERIODS = (("day", "последние сутки"), ("week", "последняя неделя"), ("month", "последний месяц"), ("own", "свой период"))
UNITS = (("time", "по времени, UTC"), ("ledger", "по леджерам"))
RESOLUTION_CHOICES = (("auto", "автоматически"),) + tuple((str(size), NAMES[size]) for size in RESOLUTIONS)
# The finer of two resolutions that keeps this many buckets is the one chosen
# for a period; more than MOST_BINS is refused, however the resolution came.
AUTO_BINS, MOST_BINS = 300, 2000
MOST_ROWS = 300  # rows of the table under the chart
MOST_BANDS = 600  # coverage ranges placed on the picture; beyond that they are only listed
# Ledgers close on whole seconds and a trade carries its ledger's close time:
# one second past that is past everything of the ledger and of none after it.
SECOND = 1000
EPOCH = aggregations.EPOCH
TIME_FORMATS = ("%Y-%m-%d", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S")
FIELDS = ("base", "counter", "period", "unit", "from", "to", "resolution")
DIGITS = re.compile(r"\d+(?:\.\d+)?")
LEDGER = re.compile(r"[1-9]\d{0,17}")

# What a coverage range is painted as on the strip under the prices.
STATE_CLASSES = {"complete": "cx-complete", "empty": "cx-empty", "pending": "cx-pending", "unverifiable": "cx-unverifiable"}

# -- the picture -----------------------------------------------------------------

WIDTH, MARGIN_LEFT, MARGIN_RIGHT = 960, 96, 14
TITLE_TOP, PRICE_TOP, PRICE_HEIGHT = 16, 30, 280
STRIP_TOP, STRIP_HEIGHT = PRICE_TOP + PRICE_HEIGHT + 26, 12
# Under the strip: its own room for the two lines the volumes are named in.
VOLUME_TOP, VOLUME_HEIGHT = STRIP_TOP + STRIP_HEIGHT + 52, 110
AXIS_TOP = VOLUME_TOP + VOLUME_HEIGHT + 6
HEIGHT = AXIS_TOP + 28
SVG_TITLE = "График рынка из архива этой установки: цена и объём по корзинам времени"
STYLE = (
    ".cx text{font-size:12px;fill:#566577}"
    ".cx .cx-title{fill:#16202b;font-weight:600}"
    ".cx .cx-frame{fill:none;stroke:#d9dee5}"
    ".cx .cx-grid{stroke:#e6eaef;stroke-width:1}"
    ".cx .cx-tick{stroke:#9aa6b2;stroke-width:1}"
    ".cx .cx-up{stroke:#1f7a3f;fill:#1f7a3f}"
    ".cx .cx-down{stroke:#b42318;fill:#b42318}"
    ".cx .cx-base{fill:#1f5fbf}"
    ".cx .cx-counter{fill:#a9c3ea}"
    ".cx .cx-none{fill:#eceff3}"
    ".cx .cx-complete{fill:#1f7a3f}"
    ".cx .cx-empty{fill:#b7dfc3}"
    ".cx .cx-pending{fill:#e0a400}"
    ".cx .cx-unverifiable{fill:#b42318}"
)
_TIME_STEPS = (
    ("minute", 1), ("minute", 5), ("minute", 15), ("minute", 30), ("hour", 1), ("hour", 3), ("hour", 6), ("hour", 12),
    ("day", 1), ("day", 2), ("week", 1), ("month", 1), ("month", 3), ("year", 1), ("year", 2), ("year", 5), ("year", 10),
    ("year", 25), ("year", 100),
)
_FIXED_STEP = {"minute": MINUTE, "hour": HOUR, "day": DAY, "week": WEEK}
_SHOWN_AS = {"minute": "%m-%d %H:%M", "hour": "%m-%d %H:%M", "day": "%Y-%m-%d", "week": "%Y-%m-%d", "month": "%Y-%m", "year": "%Y"}


class Refused(Exception):
    """What was asked for cannot be drawn. The words are for the owner."""


@dataclass(frozen=True)
class Asked:
    """The period as it was asked: its shape is checked, nothing is placed on
    the clock yet."""

    period: str  # day | week | month | own
    unit: str  # time | ledger: how the ends of an own period are given
    start: str
    to: str
    resolution: str  # auto, or a number of milliseconds


@dataclass(frozen=True)
class Period:
    start_ms: int
    end_ms: int  # not part of the period: trades from the start up to it
    resolution_ms: int
    bins: int
    kind: str  # the period, in words
    bounds: str  # how its ends were found, in words


class Read(NamedTuple):
    """One bucket as the picture reads it: all of it numbers."""

    stamp: int
    count: str
    base: Decimal
    counter: Decimal
    price: dict[str, Decimal] | None  # None: the bucket has no prices (nothing on the base side)


# -- what was asked --------------------------------------------------------------


def _shown(text: str) -> str:
    """A person's input as a message may repeat it: short."""
    text = text.strip()
    return text if len(text) <= 48 else text[:48] + "…"


def _asset(value: str, name: str) -> str:
    """``native`` or ``CODE:ISSUER``, checked as the watch list file checks
    one; the canonical id."""
    errors: list[str] = []
    asset = watch_lists.parse_asset_id(value.strip(), name, errors)
    if asset is None:
        raise Refused(
            f"Актив «{_shown(value)}» ({name}) не прочитан: нужен native или актив в виде CODE:ISSUER "
            "— 1–12 букв или цифр, двоеточие, адрес эмитента."
        )
    return asset


def _form(query: Mapping[str, str]) -> dict[str, str]:
    """What the form sent, to be shown again in it."""
    return {name: (query.get(name) or "")[:200] for name in FIELDS}


def _asked(form: Mapping[str, str]) -> Asked:
    period = form["period"].strip() or "day"
    if period not in dict(PERIODS):
        raise Refused("Период — последние сутки, неделя, месяц или свой.")
    unit = form["unit"].strip() or "time"
    if unit not in dict(UNITS):
        raise Refused("Концы своего периода задаются по времени или по леджерам.")
    resolution = form["resolution"].strip() or "auto"
    if resolution != "auto" and not (resolution.isdigit() and int(resolution) in RESOLUTIONS):
        raise Refused("Разрешение — одно из: " + ", ".join(NAMES[name] for name in RESOLUTIONS) + ".")
    return Asked(period, unit, form["from"], form["to"], resolution)


def _moment(text: str, name: str) -> datetime:
    """A moment in UTC: a day, or a day with a time."""
    raw = text.strip().removesuffix("Z").strip()
    for shape in TIME_FORMATS:
        try:
            return datetime.strptime(raw, shape).replace(tzinfo=UTC)
        except ValueError:
            continue
    raise Refused(f"Время «{_shown(text)}» ({name}) не прочитано: нужен UTC в виде 2026-01-31 или 2026-01-31 12:30.")


def _ms(moment: datetime) -> int:
    """Whole milliseconds since the epoch, counted in integers."""
    away = moment.replace(tzinfo=moment.tzinfo or UTC).astimezone(UTC) - EPOCH
    return away.days * 86_400_000 + away.seconds * 1000 + away.microseconds // 1000


def _at(ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=ms)


def _ledger(text: str, name: str) -> int:
    if not LEDGER.fullmatch(text.strip()):
        raise Refused(f"Леджер «{_shown(text)}» ({name}) — не номер: нужно целое число от 1.")
    return int(text)


def _newest(connection: Connection) -> tuple[int, datetime] | None:
    """The newest ledger the archive has a close time for, or None."""
    row = connection.execute(
        select(HistoryLedger.sequence, HistoryLedger.close_time)
        .order_by(HistoryLedger.close_time.desc(), HistoryLedger.sequence.desc())
        .limit(1)
    ).first()
    return None if row is None else (int(row[0]), row[1])


def _span_of_ledgers(connection: Connection, low: int, high: int) -> tuple[int, int, int, int] | None:
    """Where in time the ledgers the archive knows between ``low`` and ``high``
    lie: the first and the last of them by number, the close time of the
    first, and one second past the close time of the last. None when the
    archive knows none in the range: it has neither their times nor anything
    that was in them."""
    first, last = connection.execute(
        select(func.min(HistoryLedger.sequence), func.max(HistoryLedger.sequence))
        .where(HistoryLedger.sequence >= low, HistoryLedger.sequence <= high)
    ).one()
    if first is None:
        return None
    times = dict(
        connection.execute(
            select(HistoryLedger.sequence, HistoryLedger.close_time).where(HistoryLedger.sequence.in_((first, last)))
        ).all()
    )
    return int(first), int(last), _ms(times[first]), _ms(times[last]) + SECOND


def _month_earlier(ms: int) -> int:
    """The same moment a calendar month before: the day kept, or the last
    day of that month."""
    moment = _at(ms)
    year, month = (moment.year - 1, 12) if moment.month == 1 else (moment.year, moment.month - 1)
    return _ms(moment.replace(year=year, month=month, day=min(moment.day, calendar.monthrange(year, month)[1])))


def _placed(asked: Asked, connection: Connection) -> tuple[int, int, str]:
    """The ends of the period in milliseconds, and how they were found, in
    words: the ledger period says which ledgers' close times were taken."""

    def anchor() -> tuple[int, str]:
        """Where a period with no end of its own stops: just past the newest
        ledger of the archive, so that the last of what was collected is in."""
        newest = _newest(connection)
        if newest is not None:
            closed = _ms(newest[1])
            return closed + SECOND, f"конец — сразу после последнего леджера архива №{newest[0]} (закрыт {_at(closed):%Y-%m-%d %H:%M:%S} UTC)"
        return _ms(datetime.now(UTC)), "в архиве нет ни одного леджера со временем закрытия: конец — сейчас"

    if asked.period == "own" and asked.unit == "ledger":
        low, high = _ledger(asked.start, "от"), _ledger(asked.to, "до")
        if low > high:
            raise Refused("Первый леджер периода больше последнего.")
        found = _span_of_ledgers(connection, low, high)
        if found is None:
            raise Refused(f"В архиве нет ни одного леджера {low}–{high}: нет здесь ни их времени закрытия, ни сделок.")
        first, last, start_ms, end_ms = found
        return start_ms, end_ms, f"по леджерам {low}–{high}; время взято от леджеров {first} и {last}, какие архив знает"

    if asked.to.strip():
        end_ms, bounds = _ms(_moment(asked.to, "конец")), "конец — как задан (не включая)"
    else:
        end_ms, bounds = anchor()
        if asked.period == "own":
            bounds = "конец не задан: " + bounds
    if asked.period == "day":
        start_ms = end_ms - DAY
    elif asked.period == "week":
        start_ms = end_ms - WEEK
    elif asked.period == "month":
        start_ms = _month_earlier(end_ms)
    else:
        if not asked.start.strip():
            raise Refused("У своего периода должно быть начало.")
        start_ms = _ms(_moment(asked.start, "начало"))
    if start_ms >= end_ms:
        raise Refused("Конец периода должен быть позже начала.")
    return start_ms, end_ms, bounds


def _period(asked: Asked, start_ms: int, end_ms: int, bounds: str) -> Period:
    span = end_ms - start_ms
    if asked.resolution == "auto":
        resolution = next((size for size in RESOLUTIONS if -(-span // size) <= AUTO_BINS), RESOLUTIONS[-1])
    else:
        resolution = int(asked.resolution)
    bins = -(-span // resolution)
    if bins > MOST_BINS:
        raise Refused(
            f"Период слишком велик для разрешения «{NAMES[resolution]}»: вышло бы {bins} корзин, а рисуется не больше {MOST_BINS}. "
            "Возьмите разрешение крупнее или период короче."
        )
    return Period(start_ms, end_ms, resolution, bins, dict(PERIODS)[asked.period], bounds)


# -- what is drawn ---------------------------------------------------------------


def _digits(value: Any) -> str:
    """A value of a bucket as it goes into the picture: digits and a point,
    and nothing else — a number is all that is ever put there."""
    text = str(value)
    if not DIGITS.fullmatch(text):
        raise ValueError("a chart is made of numbers only")
    return text


def _read(bucket: Mapping[str, Any]) -> Read:
    price = None
    if all(name in bucket for name in ("open", "high", "low", "close")):
        price = {name: Decimal(_digits(bucket[name])) for name in ("open", "high", "low", "close")}
    return Read(
        int(_digits(bucket["timestamp"])), _digits(bucket["trade_count"]),
        Decimal(_digits(bucket["base_volume"])), Decimal(_digits(bucket["counter_volume"])), price,
    )


def _number(value: Decimal | int | float) -> str:
    """A place on the picture: two decimals of a pixel."""
    return f"{float(value):.2f}"


def _attrs(**attributes: Any) -> str:
    """Attributes with their values escaped, whatever these are — a second
    lock on a door that is already shut."""
    return "".join(
        f' {name.rstrip("_").replace("_", "-")}="{escape(str(value))}"' for name, value in attributes.items() if value is not None
    )


def _tag(name: str, text: str | None = None, **attributes: Any) -> str:
    """One element of the SVG."""
    inside = _attrs(**attributes)
    return f"<{name}{inside}>{escape(text)}</{name}>" if text is not None else f"<{name}{inside}/>"


def _ticks(low: Decimal, high: Decimal, *, most: int = 6) -> tuple[list[Decimal], int]:
    """Round prices from just above ``low`` up to ``high`` — steps of 1, 2 or
    5 times a power of ten, exact in decimal — and how many decimals a step
    has, which is how many its labels need."""
    if high <= low:
        return [], 0
    rough = (high - low) / Decimal(most)
    unit = Decimal(1).scaleb(rough.adjusted())
    step = next((times for times in (1, 2, 5, 10) if unit * times >= rough), 10) * unit
    value, ticks = (low // step) * step + step, []  # low is not negative here, so this is the floor
    while value < high and len(ticks) <= most:
        ticks.append(value)
        value += step
    return ticks, max(0, -step.normalize().as_tuple().exponent)


def _months(moment: datetime, count: int) -> datetime:
    year, month = divmod(moment.year * 12 + moment.month - 1 + count, 12)
    return moment.replace(year=year, month=month + 1)


def _time_ticks(start: int, end: int, *, most: int = 7) -> list[tuple[int, str]]:
    """Moments of the period to write under the picture, each with how it is
    written: whole minutes, hours, days or weeks from the epoch (a UTC
    midnight), or whole months or years of the UTC calendar — the finest of
    the steps that gives no more than ``most``."""
    found: list[tuple[int, str]] = []
    for unit, amount in _TIME_STEPS:
        found = []
        if unit in _FIXED_STEP:
            step = amount * _FIXED_STEP[unit]
            moment = start - start % step + (step if start % step else 0)
            while moment < end and len(found) <= most:
                found.append((moment, _at(moment).strftime(_SHOWN_AS[unit])))
                moment += step
        else:
            moment = _at(start).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if unit == "year":
                moment = moment.replace(month=1)
            while _ms(moment) < start:
                moment = _months(moment, amount)
            while _ms(moment) < end and len(found) <= most:
                found.append((_ms(moment), moment.strftime(_SHOWN_AS[unit])))
                moment = _months(moment, amount)
        if len(found) <= most:
            return found
    return found


def _svg(buckets: Sequence[Mapping[str, Any]], period: Period, bands: Sequence[Mapping[str, Any]]) -> str:
    """The chart: prices above, the archive's coverage as a strip under them,
    volumes below that, time along the bottom. A bucket without prices keeps
    its volumes and gets no candle."""
    start, end, resolution = period.start_ms, period.end_ms, period.resolution_ms
    span = end - start
    left, right = MARGIN_LEFT, WIDTH - MARGIN_RIGHT
    plot = right - left
    body = max(1.0, min(resolution * plot / span * 0.7, 14.0))
    half = max(0.5, body / 2)

    def x(ms: int) -> float:
        return left + (ms - start) * plot / span

    # Opened and closed by hand: an <svg/> that closes itself is an empty picture to a browser.
    out: list[str] = [
        f"<svg{_attrs(
            xmlns='http://www.w3.org/2000/svg', viewBox=f'0 0 {WIDTH} {HEIGHT}', class_='chart cx', role='img',
            data_start=start, data_end=end, data_resolution=resolution, data_buckets=len(buckets),
        )}>",
        _tag("title", SVG_TITLE),
        _tag("style", STYLE),
    ]
    add = out.append

    def text(at: float, down: float, words: str, *, anchor: str | None = None, class_: str | None = None) -> None:
        add(_tag("text", words, x=_number(at), y=_number(down), text_anchor=anchor, class_=class_))

    read = [_read(bucket) for bucket in buckets]
    prices = [one.price for one in read if one.price is not None]

    # -- prices ------------------------------------------------------------------
    low = min((price["low"] for price in prices), default=Decimal(0))
    high = max((price["high"] for price in prices), default=Decimal(1))
    room = (high - low) * Decimal("0.06") or low * Decimal("0.06") or Decimal(1)
    low, high = max(Decimal(0), low - room), high + room
    whole = high - low

    def y(price: Decimal) -> float:
        return PRICE_TOP + float((high - price) / whole) * PRICE_HEIGHT

    text(left, TITLE_TOP, "цена: counter за base (открытие — максимум — минимум — закрытие)", class_="cx-title")
    add(_tag("rect", class_="cx-frame", x=_number(left), y=PRICE_TOP, width=plot, height=PRICE_HEIGHT))
    ticks, decimals = _ticks(low, high)
    for tick in ticks:
        add(_tag("line", class_="cx-grid", x1=_number(left), y1=_number(y(tick)), x2=_number(right), y2=_number(y(tick))))
        text(left - 7, y(tick) + 4, f"{tick:.{decimals}f}", anchor="end")
    if not prices:
        text(left + plot / 2, PRICE_TOP + PRICE_HEIGHT / 2, "в этом периоде нет корзин с ценами")

    # -- coverage ----------------------------------------------------------------
    text(left, STRIP_TOP - 6, "что архив проверил по этому рынку (серое — ничего не утверждается)", class_="cx-title")
    add(_tag("rect", class_="cx-none", x=_number(left), y=STRIP_TOP, width=plot, height=STRIP_HEIGHT))
    for band in bands:
        add(
            _tag(
                "rect", class_=STATE_CLASSES.get(band.get("state"), "cx-none"), x=_number(x(band["from_ms"])), y=STRIP_TOP,
                width=_number(max(0.0, x(band["to_ms"]) - x(band["from_ms"]))), height=STRIP_HEIGHT,
            )
        )

    # -- volumes -----------------------------------------------------------------
    most_base = max((one.base for one in read), default=Decimal(0))
    most_counter = max((one.counter for one in read), default=Decimal(0))

    def rise(value: Decimal, most: Decimal) -> Decimal:
        """How high a volume stands: each series from its own maximum, so
        that a small unit is not lost next to a large one."""
        return Decimal(VOLUME_HEIGHT) * value / most if most else Decimal(0)

    text(
        left, VOLUME_TOP - 34, f"объём по корзинам: тёмнее — base (самый большой {most_base:.7f}), светлее — counter (самый большой {most_counter:.7f})",
        class_="cx-title",
    )
    text(left, VOLUME_TOP - 16, "каждый ряд — от своего максимума; высоты рядов друг с другом не сравниваются")
    add(_tag("rect", class_="cx-frame", x=_number(left), y=VOLUME_TOP, width=plot, height=VOLUME_HEIGHT))

    # -- one group per bucket: its candle, its volumes, what it holds ---------
    for one, bucket in zip(read, buckets, strict=True):
        center = x(one.stamp + resolution // 2)
        kept = {
            "data_ts": one.stamp, "data_count": one.count,
            "data_base": _digits(bucket["base_volume"]), "data_counter": _digits(bucket["counter_volume"]),
        }
        said = f"{_at(one.stamp):%Y-%m-%d %H:%M:%S} UTC · сделок {one.count} · base {kept['data_base']} · counter {kept['data_counter']}"
        if "avg" in bucket:
            kept["data_avg"] = _digits(bucket["avg"])
            said += " · средняя " + kept["data_avg"]
        if one.price is not None:
            for name in ("open", "high", "low", "close"):
                kept["data_" + name] = _digits(bucket[name])
                said += f" · {name} {kept['data_' + name]}"
        # A bucket at either end of the period keeps its candle inside the frame.
        candle = min(max(center - half, left), right - body)
        add(f"<g{_attrs(class_='cx-bucket', **kept)}>")
        add(_tag("title", said))
        if one.price is not None:
            class_ = "cx-up" if one.price["close"] >= one.price["open"] else "cx-down"
            add(_tag("line", class_=class_, x1=_number(center), y1=_number(y(one.price["high"])), x2=_number(center), y2=_number(y(one.price["low"]))))
            top, bottom = y(max(one.price["open"], one.price["close"])), y(min(one.price["open"], one.price["close"]))
            add(_tag("rect", class_=class_, x=_number(candle), y=_number(top), width=_number(body), height=_number(max(1.0, bottom - top))))
        for class_, at, value, most in (("cx-base", max(candle, left), one.base, most_base), ("cx-counter", center, one.counter, most_counter)):
            stood = float(rise(value, most))
            add(_tag("rect", class_=class_, x=_number(at), y=_number(VOLUME_TOP + VOLUME_HEIGHT - stood), width=_number(half), height=_number(stood)))
        add("</g>")

    # -- time --------------------------------------------------------------------
    for moment, shown in _time_ticks(start, end):
        at = x(moment)
        add(_tag("line", class_="cx-tick", x1=_number(at), y1=VOLUME_TOP + VOLUME_HEIGHT, x2=_number(at), y2=VOLUME_TOP + VOLUME_HEIGHT + 5))
        if left + 26 <= at <= right - 26:
            text(at, AXIS_TOP + 16, shown, anchor="middle")
    add("</svg>")
    return "\n".join(out)


def _bands(connection: Connection, covered: Mapping[str, Any]) -> tuple[list[dict[str, Any]], bool]:
    """The coverage ranges of the market as stretches of time, to lie under
    the prices on the same axis of time.

    A range is placed by the ledgers the archive knows inside it: from the
    close of the first of them to one second past the close of the last. A
    trade carries the close time of its ledger, so every trade that is stored
    lies on the range its ledger belongs to, exactly. Where two neighbouring
    ranges meet, the time of the ledger that divides them is taken by its
    number between the closest ledgers known on either side — ledgers close
    at an even pace — and the two meet there without a gap; a range with no
    ledger of its own in the archive gets the stretch between its two
    boundaries. What has no known ledger on either side is left unplaced:
    nothing that is stored is there, and nothing is claimed by the picture.

    The second value says whether all of it was placed: past MOST_BANDS the
    picture gets none, and the page says so."""
    policies = covered["streams"]["trades"]["policies"]
    if sum(len(policy["ranges"]) for policy in policies) > MOST_BANDS:
        return [], False
    bands = []
    for policy in policies:
        bands.extend(_tile(connection, sorted(policy["ranges"], key=lambda range_: range_["from_ledger"]), policy["policy"]))
    return bands, True


def _tile(connection: Connection, ranges: Sequence[Mapping[str, Any]], policy: str) -> list[dict[str, Any]]:
    """The ranges of one policy, lowest ledger first, placed on the clock."""
    if not ranges:
        return []
    # Per range: the first and the last ledger the archive knows in it, and when each closed.
    known = [_span_of_ledgers(connection, range_["from_ledger"], range_["to_ledger"]) for range_ in ranges]

    def boundary(after: int) -> int | None:
        """When the ledger that starts range ``after`` closed — where the
        range before it ends. None when no ledger is known on either side."""
        left = next((known[number] for number in range(after - 1, -1, -1) if known[number] is not None), None)
        right = next((known[number] for number in range(after, len(ranges)) if known[number] is not None), None)
        if left is None:
            return right[2] if right is not None else None  # when the first ledger known after it closed
        if right is None:
            return left[3]  # one second past the last ledger known before it
        last_ledger, last_ms = left[1], left[3] - SECOND
        first_ledger, first_ms = right[0], right[2]
        if first_ledger <= last_ledger:
            return left[3]
        return last_ms + (first_ms - last_ms) * (ranges[after]["from_ledger"] - last_ledger) // (first_ledger - last_ledger)

    # edges[number]: where range number - 1 ends and range number begins.
    edges: list[int | None] = [None] * (len(ranges) + 1)
    for number in range(1, len(ranges)):
        if ranges[number]["from_ledger"] == ranges[number - 1]["to_ledger"] + 1:
            edges[number] = boundary(number)  # neighbours; a gap between two ranges stays: nothing is claimed in it
    placed = []
    for number, range_ in enumerate(ranges):
        found = known[number]
        start = edges[number] if edges[number] is not None else (found[2] if found is not None else None)
        end = edges[number + 1] if edges[number + 1] is not None else (found[3] if found is not None else None)
        if start is not None and end is not None and start < end:
            placed.append({**range_, "policy": policy, "from_ms": start, "to_ms": end})
    return placed


def _again(form: Mapping[str, str], **changes: str) -> str:
    """The same question with some of it changed: the period kept."""
    kept = {name: form[name] for name in FIELDS if form.get(name) and name not in ("base", "counter")}
    return "/admin/chart?" + urlencode(sorted({**kept, **changes}.items()))


def _policies(covered: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The coverage of the market's trades as the page lists it: per policy
    its window, how many of its ledgers are in each state, and its ranges —
    MOST_ROWS of them, each with when it was verified."""
    found = []
    for policy in covered["streams"]["trades"]["policies"]:
        ranges = policy["ranges"]
        rows = []
        for range_ in ranges[:MOST_ROWS]:
            at = range_.get("verified_at")
            try:
                verified = datetime.fromisoformat(at) if isinstance(at, str) else None
            except ValueError:
                verified = None
            rows.append(
                {"from": range_["from_ledger"], "to": range_["to_ledger"], "state": range_["state"], "at": verified, "detail": range_.get("detail")}
            )
        found.append(
            {"policy": policy["policy"], "window": policy["window"], "ledgers": policy["ledgers"], "ranges": rows, "more": max(0, len(ranges) - MOST_ROWS)}
        )
    return found


def _counted(found: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """What the buckets add up to, in exact decimals: trades and volumes."""
    read = [_read(bucket) for bucket in found]
    return {
        "trades": sum(int(one.count) for one in read),
        "base": f"{sum((one.base for one in read), Decimal(0)):.7f}",
        "counter": f"{sum((one.counter for one in read), Decimal(0)):.7f}",
    }


def build(door: Door) -> APIRouter:
    router = APIRouter(prefix="/chart")

    def answer(request: Request, login: auth.Login, status_code: int = 200, **context: Any) -> Any:
        return support.render(
            request, "chart.html", login=login, csrf=auth.csrf_token(login.csrf), active="chart", status_code=status_code,
            periods=PERIODS, units=UNITS, resolutions=RESOLUTION_CHOICES, most_bins=MOST_BINS, **context,
        )

    def refusal(request: Request, login: auth.Login, form: Mapping[str, str], why: str, status_code: int = 400) -> Any:
        """The page with the question still in its form, and what is wrong
        with it said. For what cannot be drawn nothing has been read."""
        return answer(request, login, status_code, asked=form, error=why)

    @router.get("")
    def chart_page(request: Request, login: Any = Depends(door.open)) -> Any:
        form = _form(request.query_params)
        if not form["base"].strip() and not form["counter"].strip():
            return answer(request, login, asked=form)  # nothing asked yet: the form alone
        try:
            # What is wrong is said before anything is read for it.
            base, counter = _asset(form["base"], "base"), _asset(form["counter"], "counter")
            if base == counter:
                raise Refused("Рынок — это два разных актива, а не один дважды.")
            asked = _asked(form)
            with streams.reading(door.engine) as connection:
                start_ms, end_ms, bounds = _placed(asked, connection)
                period = _period(asked, start_ms, end_ms, bounds)
                found = aggregations.buckets(connection, base, counter, period.start_ms, period.end_ms, period.resolution_ms)
                covered = coverage.market(connection, base, counter)
                bands, placed = _bands(connection, covered)
        except Refused as why:
            return refusal(request, login, form, str(why))
        except OperationalError as error:
            if not streams.timed_out(error):
                raise
            return refusal(
                request, login, form,
                "Чтение остановлено: за этот период слишком много сделок, чтобы прочитать их за отведённое время. "
                "Возьмите период короче или разрешение крупнее.", status_code=503,
            )
        read = [_read(bucket) for bucket in found]
        rows = []  # the last MOST_ROWS buckets, oldest first; the rest are only counted
        for one, bucket in list(zip(read, found, strict=True))[-MOST_ROWS:]:
            rows.append({
                "at": _at(one.stamp), "count": one.count, "base": bucket["base_volume"], "counter": bucket["counter_volume"],
                "avg": bucket.get("avg"), **{name: bucket.get(name) for name in ("open", "high", "low", "close")},
            })
        query = {**asset_params("base_", base), **asset_params("counter_", counter)}
        return answer(
            request, login, asked=form,
            chart={
                "base": base, "counter": counter, "key": market_key(base, counter),
                "period": period, "start": _at(period.start_ms), "end": _at(period.end_ms), "resolution": NAMES[period.resolution_ms],
                "kind": period.kind[0].upper() + period.kind[1:],
                "svg": _svg(found, period, [band for band in bands if band["from_ms"] < period.end_ms and band["to_ms"] > period.start_ms]),
                "rows": rows, "found": len(found), "priced": sum(1 for one in read if one.price is not None),
                "unpriced": sum(1 for one in read if one.price is None), "totals": _counted(found),
                "coverage": covered, "bands": bands, "placed": placed, "policies": _policies(covered),
                "flip": _again(form, base=counter, counter=base),
                "api": "/trade_aggregations?" + urlencode(
                    sorted({**query, "start_time": str(period.start_ms), "end_time": str(period.end_ms),
                            "resolution": str(period.resolution_ms), "limit": "200"}.items())
                ),
                "coverage_query": "/v1/coverage?" + urlencode(sorted(query.items())),
            },
        )

    return router
