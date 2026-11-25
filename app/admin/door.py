"""The door of the admin UI as requests meet it: the login and the logout,
and what every other page — and every form that changes something — stands
behind.

With no secret configured the admin UI is closed: the login page says so and
no secret opens it (fail closed). A login that failed is counted; past the
allowance the address is told until when it is blocked, and the secret is not
so much as compared meanwhile.

The login form itself has no session yet and so no form token: what protects
it is the cookie's ``SameSite=Strict``, and — with a public origin set — the
check that the request came from there. Nothing is to be gained from making
somebody log into this installation but into their own.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qsl

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.engine import Engine

from app.admin import auth, support
from app.config import Settings

logger = logging.getLogger(__name__)

LOGIN = "/admin/login"
HOME = "/admin"
# A form of this UI is a few fields of text, never a file: this is all that
# is read of a body, and nothing that is not a plain form is read at all.
FORM = "application/x-www-form-urlencoded"
FORM_LIMIT = 64 * 1024


class NotAForm(Exception):
    """What was posted is not a form of this UI: too large, or of another
    kind. Nothing of it was read as a form."""


async def read_form(request: Request) -> dict[str, str]:
    """The fields of a posted form, by name. The body is read only up to
    ``FORM_LIMIT`` and only when it is what a browser sends for a form
    without files; anything else is :class:`NotAForm`."""
    kind = request.headers.get("content-type", "").split(";")[0].strip().lower()
    body = bytearray()
    async for chunk in request.stream():
        body += chunk
        if len(body) > FORM_LIMIT:
            raise NotAForm("форма слишком велика")
    if not body:
        return {}  # nothing posted: a form without fields, and its token is not there either
    if kind != FORM:
        raise NotAForm("это не форма")
    fields: dict[str, str] = {}
    for name, value in parse_qsl(body.decode("utf-8", errors="replace"), keep_blank_values=True):
        fields[name] = value
    return fields


class ToLogin(Exception):
    """There is no open session: the request is sent to the login page."""


class Refused(Exception):
    """A request that changes something did not show that it came from a page
    of this installation: it is refused, and nothing is done."""


class Door:
    def __init__(self, settings: Settings, engine: Engine | None) -> None:
        self.settings, self.engine = settings, engine
        self.router = APIRouter()
        self.router.get("/login")(self.login_page)
        self.router.post("/login")(self.login)
        self.router.post("/logout")(self.logout)

    # -- what pages and forms stand behind ------------------------------------

    def open(self, request: Request) -> auth.Login:
        """The session of the request, or the request goes to the login page.
        A closed installation has no pages: it goes there too, and that page
        says why."""
        if self.engine is None or not auth.secret_is_set(self.settings):
            raise ToLogin
        login = auth.session_of(self.engine, request.cookies.get(auth.COOKIE))
        if login is None:
            raise ToLogin
        return login

    async def form_of(self, request: Request) -> tuple[auth.Login, Any]:
        """For a request that changes something: an open session, a request
        from where this installation is, and the form's token. Gives the
        session and the form; raises :class:`Refused` and does nothing when
        any of it is missing."""
        login = self.open(request)
        if not auth.origin_ok(self.settings, request.headers.get("origin")):
            logger.warning("a request to %s came from another origin; nothing was done", request.url.path)
            raise Refused("Запрос пришёл не от этой установки — ничего не сделано.")
        try:
            form = await read_form(request)
        except NotAForm as error:
            raise Refused(f"Присланное — не форма админки ({error}); ничего не сделано.") from error
        if not auth.csrf_ok(login.csrf, form.get("csrf")):
            logger.warning("a request to %s came without the form's token; nothing was done", request.url.path)
            raise Refused("У формы нет верного токена. Откройте страницу заново и повторите.")
        return login, form

    # -- the login and the logout ----------------------------------------------

    def login_page(self, request: Request) -> Any:
        if self.engine is not None and auth.session_of(self.engine, request.cookies.get(auth.COOKIE)) is not None:
            return RedirectResponse(HOME, status_code=303)
        return support.render(
            request,
            "login.html",
            status_code=200 if auth.secret_is_set(self.settings) else 503,
            configured=auth.secret_is_set(self.settings),
            trouble=request.query_params.get("trouble", ""),
        )

    async def login(self, request: Request) -> Any:
        if not auth.secret_is_set(self.settings) or self.engine is None:
            return support.render(request, "login.html", status_code=503, configured=False, trouble="")
        if not auth.origin_ok(self.settings, request.headers.get("origin")):
            return support.refused(request, "Запрос пришёл не от этой установки.")
        try:
            form = await read_form(request)
        except NotAForm as error:
            return support.refused(request, f"Присланное — не форма админки ({error}).")
        address = request.client.host if request.client else None
        keys, now = auth.keys_for(address), datetime.now(UTC)
        until = auth.blocked_until(self.engine, keys, now=now)
        if until is not None:
            logger.warning("a login from %s was refused: blocked until %s", address, until.isoformat(timespec="seconds"))
            return support.render(request, "login.html", status_code=429, configured=True, blocked_until=until, trouble="")
        if auth.secret_matches(self.settings, str(form.get("secret") or "")):
            auth.succeeded(self.engine, address, now=now)
            session, token = auth.open_session(self.engine, self.settings, address=address, now=now)
            logger.info("the admin UI was opened from %s", address)
            # Only to a page of the admin UI: a ``next`` from elsewhere is not followed.
            asked = request.query_params.get("next") or ""
            answer = RedirectResponse(asked if asked.startswith("/admin") and not asked.startswith(LOGIN) else HOME, status_code=303)
            answer.set_cookie(**auth.cookie(token=token, settings=self.settings))
            return answer
        until = auth.failed(self.engine, keys, now=now)
        logger.warning("a login from %s gave a wrong secret", address)
        return support.render(
            request, "login.html", status_code=429 if until else 401, configured=True, blocked_until=until, trouble="Секрет неверен."
        )

    async def logout(self, request: Request) -> Any:
        login, _ = await self.form_of(request)
        auth.close_session(self.engine, request.cookies.get(auth.COOKIE))
        answer = RedirectResponse(LOGIN, status_code=303)
        answer.delete_cookie(auth.COOKIE, path="/admin")
        return answer
