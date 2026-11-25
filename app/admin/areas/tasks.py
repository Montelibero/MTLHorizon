"""The tasks of the admin UI: what was started, how far it got, what it came
to; stopped or started again from here.

A task is never run by a request: the request puts it in the queue
(:mod:`app.storage.service_jobs`) and the worker's background thread takes
it. The page reads the queue, and nothing else.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.engine import Engine

from app.admin import support
from app.admin.door import Door, Refused
from app.storage import service_jobs
from app.storage.models import ServiceJob
from app.tasks.kinds import describe

STATES = service_jobs.STATES
ACTIVE = ("queued", "running")


def figures(job: ServiceJob) -> dict[str, str]:
    """The few numbers that say how a task went, by its kind: from what it
    says of itself while it runs, and from what it came to."""
    progress, result = dict(job.progress or {}), dict(job.result or {})
    if job.kind == "discovery":
        source = result or progress
        said = {
            "страниц": source.get("pages"),
            "найдено": source.get("found"),
            "новых": source.get("added"),
            "уже включены": source.get("already", source.get("already_enabled")),
            "отключены": source.get("disabled"),
        }
        if result:
            said["итог"] = result.get("stopped_because") or result.get("state") or ""
        return {name: value for name, value in said.items() if value not in (None, "")}
    if job.kind == "pool_check":
        servers = result.get("servers") or []
        if servers:
            ok = sum(1 for server in servers if server.get("available"))
            return {"серверов": f"{ok} из {len(servers)} доступны", "запросов": result.get("requests")}
        return {"повторов": progress.get("repeats")} if progress else {}
    if job.kind == "reconcile":
        if result.get("dry_run"):
            return {"шагов": len(result.get("steps") or []), "у Notifier": result.get("at_notifier")}
        return {
            name: result.get(name)
            for name in ("wanted", "created", "confirmed", "failed", "ambiguous", "retired")
            if result.get(name) not in (None, 0)
        } or ({"всё в шаге": "да"} if result else {})
    if job.kind == "verify":
        said: dict[str, Any] = {}
        if result.get("mode") == "recover":
            said["прочитано заново"] = "да" if result.get("read_again") else "нет"
        elif "matches" in result:
            said["совпадает"] = "да" if result.get("matches") else "нет"
        if result.get("stopped"):
            said["остановлено"] = "бюджет"
        if result.get("requests") is not None:
            said["запросов"] = result["requests"]
        return said
    return {}


def duration(job: ServiceJob) -> str:
    if job.started_at is None:
        return "—"
    end = job.finished_at or job.started_at
    seconds = (end - job.started_at).total_seconds()
    return f"{int(seconds)} с" if seconds < 120 else f"{seconds / 60:.1f} мин"


def build(door: Door) -> APIRouter:
    router = APIRouter()

    @router.get("/tasks")
    def tasks_page(request: Request, login: Any = Depends(door.open)) -> Any:
        engine: Engine = door.engine
        state = request.query_params.get("state", "")
        kind = request.query_params.get("kind", "") or None
        page, per_page, offset = support.page_of(request, per_page=100)
        states = (state,) if state in STATES else None
        # The active ones first, the newest of the rest after them: in two reads, each bounded.
        active = service_jobs.recent(engine, states=ACTIVE, kind=kind, limit=per_page)
        ended = [] if state in ACTIVE else service_jobs.recent(engine, states=(state,) if state else ("done", "failed", "cancelled"), kind=kind, limit=per_page)
        jobs = active + [job for job in ended if job.id not in {other.id for other in active}]
        counts = service_jobs.counts(engine)
        return support.page(
            request, "tasks.html", login, "tasks", jobs=jobs, counts=counts, state=state, kind=kind or "",
            figures=figures, duration=duration, describe=describe,
        )

    @router.get("/tasks/{job_id}")
    def task_page(request: Request, job_id: int, login: Any = Depends(door.open)) -> Any:
        job = service_jobs.get(door.engine, job_id)
        if job is None:
            return support.render(request, "refused.html", login=login, why=f"Задачи {job_id} нет.", status_code=404)
        return support.page(
            request, "task.html", login, "tasks", job=job, figures=figures(job), duration=duration(job), describe=describe,
        )

    def back(job_id: int, notice: str) -> RedirectResponse:
        return RedirectResponse(f"/admin/tasks/{job_id}?notice={quote(notice)}", status_code=303)

    @router.post("/tasks/{job_id}/cancel")
    async def cancel(request: Request, job_id: int) -> Any:
        await door.form_of(request)
        try:
            state = service_jobs.request_cancel(door.engine, job_id)
        except ValueError as error:
            raise Refused(str(error)) from error
        said = {
            "cancelled": "Задача остановлена.",
            "running": "Задаче сказано остановиться: она закончит перед следующим запросом.",
        }.get(state, "Задача уже закончилась: ничего не изменилось.")
        return back(job_id, said)

    @router.post("/tasks/{job_id}/again")
    async def again(request: Request, job_id: int) -> Any:
        await door.form_of(request)
        job = service_jobs.get(door.engine, job_id)
        if job is None:
            raise Refused(f"Задачи {job_id} нет.")
        new, made = service_jobs.enqueue(door.engine, job.kind, dict(job.params or {}), one_at_a_time=job.one_at_a_time)
        return back(
            new.id,
            "Задача поставлена заново." if made else "Такая задача уже стоит в очереди или идёт: ничего не добавлено.",
        )

    return router
