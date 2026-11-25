"""Puts the admin UI on the application: its static files, its pages, and
what its door raises turned into answers.

The UI is there whether or not a secret is configured; without one it is
closed — the login page says so, and every other page leads to it.
"""

from __future__ import annotations

from importlib import import_module

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.engine import Engine

from app.admin import support
from app.admin.door import LOGIN, Door, Refused, ToLogin
from app.config import Settings

# The groups of pages, by the names of their modules in ``app.admin.areas``,
# in the order their routers are put on: a group's first page may be ``""``.
AREAS = (
    "tasks",
    "dashboard",
    "accounts",
    "assets",
    "markets",
    "discovery",
    "imports",
    "activity",
    "notifier",
    "horizon",
    "chart",
    "explore",
)


def install(app: FastAPI, settings: Settings, engine: Engine | None) -> Door:
    door = Door(settings, engine)
    app.mount("/admin/static", StaticFiles(directory=str(support.STATIC)), name="admin-static")
    for name in AREAS:
        # An area that is not written yet is not there: the rest are.
        try:
            area = import_module(f"app.admin.areas.{name}")
        except ModuleNotFoundError as missing:
            if missing.name == f"app.admin.areas.{name}":
                continue
            raise
        door.router.include_router(area.build(door))
    app.include_router(door.router, prefix="/admin")

    async def to_login(request: Request, _: ToLogin) -> RedirectResponse:
        return RedirectResponse(LOGIN, status_code=303)

    async def refused(request: Request, error: Refused) -> object:
        return support.refused(request, str(error), status_code=403)

    app.add_exception_handler(ToLogin, to_login)
    app.add_exception_handler(Refused, refused)
    return door
