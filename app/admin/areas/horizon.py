"""The Horizon pool as it is configured and as this installation knows it: a
list to look at, and a check to ask for.

The servers come from the settings, shown without what may be secret in
their addresses; nothing here edits them. Of each server the page shows what
is kept of it in the database — what the last check found, and what the
running processes last saw of it — and it asks no server when it opens. A
check is a task: the worker runs it, through the pool's common pace, as real
requests for history and not as a ping.

Which server is the active one is said as of the last check and no later.
Every process has a pool of its own, and which server answers is decided at
every request; nothing here knows that for a fact.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.admin import support
from app.admin.door import Door, Refused
from app.config import server_name
from app.pool import check as checking
from app.storage import pool_state, service_jobs
from app.storage.models import HorizonNodeState, ServiceJob

# The checks of the pool go one at a time.
QUEUE = "pool"
MOST_TASKS = 20
# How often the page asks for itself again while a check runs.
REFRESH = 10
REASONS = {
    "its root does not answer": "корень сервера не отвечает",
    "it serves another network than this installation keeps": "обслуживает другую сеть, не ту, что хранит эта установка",
    "no ledger could be asked for": "не нашлось леджера, который можно было бы спросить",
}


def _plain(row: HorizonNodeState) -> HorizonNodeState:
    """The row as plain data, alive after its session is gone."""
    return HorizonNodeState(**{column.name: getattr(row, column.name) for column in HorizonNodeState.__table__.columns})


def _moment(value: Any) -> datetime | None:
    """A moment as a check's result keeps it (ISO 8601), or None."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _reason(value: Any) -> str:
    """Why a server is not available, in words for the owner where it is one
    the check is known to write; the check's words otherwise."""
    text = str(value or "")
    if text in REASONS:
        return REASONS[text]
    if text.startswith("it does not answer for ledger "):
        return "не отвечает за леджер " + text[len("it does not answer for ledger ") :]
    return text


def _line(samples: Sequence[Mapping[str, Any]] | None, median: int | None) -> str:
    """What the check's requests of one kind came to, in words. Only counted
    here; what a sample says of itself stays where it is."""
    if not samples:
        return "не спрошено"
    answered = [sample for sample in samples if sample.get("status") is not None]
    statuses = sorted({int(sample["status"]) for sample in answered})
    refused = sum(1 for sample in answered if not 200 <= int(sample["status"]) < 300 or sample.get("error"))
    said = f"спрошено {len(samples)}, отвечено {len(answered)}"
    if median is not None:
        said += f", медиана {median} мс"
    if statuses:
        said += ", статус " + ", ".join(str(status) for status in statuses)
    if refused:
        said += f", с ошибкой {refused}"
    if len(samples) - len(answered):
        said += f", без ответа {len(samples) - len(answered)}"
    return said


def _aware(moment: datetime) -> datetime:
    """The moment with its zone said: UTC when the database gave none."""
    return moment.replace(tzinfo=moment.tzinfo or UTC)


def _server(url: str, position: int, kept: HorizonNodeState | None, active: str | None, now: datetime) -> dict[str, Any]:
    """One server of the settings with what is kept of it. The kept row is
    found by the address as it is shown, which is the address it is kept by."""
    address = server_name(url)
    report: dict[str, Any] = dict((kept.report if kept is not None else None) or {})
    check = None
    if kept is not None and kept.checked_at is not None:
        ledger = report.get("ledger")
        check = {
            "at": kept.checked_at, "available": bool(kept.available), "reason": _reason(report.get("reason")),
            "network": kept.network_passphrase, "network_ok": report.get("network_ok"), "version": kept.horizon_version,
            "latest": kept.latest_ledger, "elder": kept.elder_ledger, "closed_at": kept.latest_closed_at, "lag": kept.lag_ledgers,
            "root": _line(report.get("root"), kept.root_ms),
            "ledger": ledger if isinstance(ledger, int) else None,
            "history": _line(report.get("history"), kept.history_ms),
            # What the server sent, cut short: it is the server's text, shown escaped.
            "limits": {str(name)[:40]: str(value)[:80] for name, value in (kept.rate_limit_headers or {}).items()},
        }
    cooling = kept.cooling_until if kept is not None else None
    return {
        "url": address, "position": position, "check": check, "active": active is not None and active == address,
        "kept": kept is not None,
        "last_answer_at": kept.last_answer_at if kept is not None else None,
        "last_error": kept.last_error if kept is not None else None,
        "last_error_at": kept.last_error_at if kept is not None else None,
        "cooling_until": cooling,
        # Left alone until then by the processes that saw it fail.
        "cooling": cooling is not None and _aware(cooling) > now,
    }


def _active(result: Mapping[str, Any] | None, kept: Mapping[str, HorizonNodeState], addresses: Sequence[str]) -> dict[str, Any]:
    """The server the pool would have asked first — as of the last check
    only: said so in ``when``. Known from the last check that ended well;
    without one, from what each server's own last check said of it."""
    if result is not None:
        at = _moment(result.get("checked_at"))
        address = result.get("active")
        return {"url": address if isinstance(address, str) and address in addresses else None, "at": at, "how": "check"}
    for address in addresses:
        row = kept.get(address)
        if row is not None and row.checked_at is not None and row.available:
            return {"url": address, "at": row.checked_at, "how": "servers"}
    return {"url": None, "at": None, "how": "none"}


def _job_of(engine: Engine, asked: str) -> ServiceJob | None:
    """A task by its id; None when there is none — or ``asked`` is no number."""
    return service_jobs.get(engine, int(asked)) if asked.isdigit() and len(asked) <= 18 else None


def _tasks(engine: Engine, asked: str) -> dict[str, Any]:
    """The newest checks of the pool: the one queued or running, the last
    that ended, and the one the page was sent back to."""
    job = _job_of(engine, asked)
    jobs = service_jobs.recent(engine, kind="pool_check", limit=MOST_TASKS)
    last = next((one for one in jobs if one.state not in service_jobs.ACTIVE), None)
    result: dict[str, Any] = dict(last.result or {}) if last is not None and last.state == service_jobs.DONE else {}
    checked = [server for server in result.get("servers") or [] if isinstance(server, dict)]
    return {
        "job": job if job is not None and job.kind == "pool_check" else None,
        "running": next((one for one in jobs if one.state in service_jobs.ACTIVE), None),
        "last": last,
        "result": result or None,
        "requests": result.get("requests"),
        "available": sum(1 for server in checked if server.get("available")),
        "checked": len(checked),
        "unavailable": [str(server.get("url") or "")[:200] for server in checked if not server.get("available")],
    }


def build(door: Door) -> APIRouter:
    router = APIRouter(prefix="/horizon")

    def back(said: str, job: ServiceJob) -> RedirectResponse:
        return RedirectResponse("/admin/horizon?" + urlencode({"notice": said, "job": str(job.id)}), status_code=303)

    @router.get("")
    def horizon_page(request: Request, login: Any = Depends(door.open)) -> Any:
        engine: Engine = door.engine
        with Session(engine) as session:
            kept = {row.url: _plain(row) for row in pool_state.rows(session)}
        addresses = [server_name(url) for url in door.settings.horizon_urls]
        now = datetime.now(UTC)
        tasks = _tasks(engine, request.query_params.get("job", ""))
        active = _active(tasks["result"], kept, addresses)
        return support.page(
            request, "horizon.html", login, "horizon",
            servers=[
                _server(url, position, kept.get(address), active["url"], now)
                for position, (url, address) in enumerate(zip(door.settings.horizon_urls, addresses, strict=True), start=1)
            ],
            # Remembered from before an address was taken out of the settings: not asked any more.
            gone=[kept[address] for address in sorted(kept) if address not in addresses],
            first_choice=active, tasks=tasks, pace=door.settings.horizon_requests_per_second,
            repeats=list(range(1, checking.MAX_REPEATS + 1)), most=checking.MAX_REPEATS,
            # While a check is queued or running, the page asks for itself again: it asks no server.
            refresh=REFRESH if tasks["running"] is not None else None,
        )

    @router.post("/check")
    async def check(request: Request) -> Any:
        """«Проверить все»: put the check in the queue. The worker asks every
        server, on its own, as often as ``repeats`` says — up to three of
        each request, so up to six requests a server — never here."""
        _, form = await door.form_of(request)
        asked = form.get("repeats", "")
        if asked not in {str(number) for number in range(1, checking.MAX_REPEATS + 1)}:
            raise Refused(f"Число повторов — целое от 1 до {checking.MAX_REPEATS}.")
        job, made = service_jobs.enqueue(door.engine, "pool_check", {"repeats": int(asked)}, one_at_a_time=QUEUE)
        if not made:
            return back(
                f"Проверка уже стоит в очереди или идёт (задача {job.id}, повторов: {(job.params or {}).get('repeats')}) — "
                "ничего не добавлено. С другим числом повторов она получится, когда эта закончится.", job,
            )
        return back(
            f"Проверка поставлена в очередь (задача {job.id}): воркер спросит каждый сервер сам по себе — его root и один "
            f"леджер, до {asked} раз каждый запрос, через общий темп. Ничего в истории и в курсорах она не меняет; итог — на странице задачи.", job,
        )

    return router
