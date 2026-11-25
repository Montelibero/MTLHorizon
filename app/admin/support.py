"""What the pages of the admin UI share: the templates and their filters, the
door every page but the login stands behind, the token every form that
changes something carries, a notice after an action, and pagination.

Times are shown in UTC with the zone said — and a reader who wants their own
zone turns it on in the page's footer (``admin.js`` changes what is shown,
never what is kept).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode
from typing import Any

from fastapi import Request
from fastapi.templating import Jinja2Templates

from app import __version__
from app.admin import auth

HERE = Path(__file__).resolve().parent
TEMPLATES = HERE / "templates"
STATIC = HERE / "static"

# What a page shows at once of a long list, and the most it may be asked for.
PER_PAGE = 50
MOST_PER_PAGE = 200

templates = Jinja2Templates(directory=str(TEMPLATES))

# The navigation, in its order: where every page can be reached from.
NAVIGATION = (
    ("Дашборд", "/admin", "dashboard"),
    ("Аккаунты", "/admin/accounts", "accounts"),
    ("Активы", "/admin/assets", "assets"),
    ("Рынки", "/admin/markets", "markets"),
    ("Discovery", "/admin/discovery", "discovery"),
    ("Импорт истории", "/admin/import", "import"),
    ("График", "/admin/chart", "chart"),
    ("Активность", "/admin/activity", "activity"),
    ("Notifier", "/admin/notifier", "notifier"),
    ("Horizon", "/admin/horizon", "horizon"),
    ("Задачи", "/admin/tasks", "tasks"),
    ("Обозреватель", "/admin/explore", "explore"),
)


def _when(value: Any) -> str:
    """A moment with its zone said: UTC, always to the second."""
    if not isinstance(value, datetime):
        return "—"
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _ago(value: Any, now: datetime | None = None) -> str:
    """How long ago a moment was, in a few words; «никогда» when there was none."""
    if not isinstance(value, datetime):
        return "никогда"
    seconds = ((now or datetime.now(UTC)) - value.replace(tzinfo=value.tzinfo or UTC).astimezone(UTC)).total_seconds()
    if seconds < 0:
        return "в будущем"
    if seconds < 60:
        return f"{int(seconds)} с назад"
    if seconds < 3600:
        return f"{int(seconds // 60)} мин назад"
    if seconds < 86400:
        return f"{int(seconds // 3600)} ч назад"
    return f"{int(seconds // 86400)} дн назад"


def _short(value: Any, kept: int = 8) -> str:
    text = "—" if value is None else str(value)
    return text if len(text) <= kept + 2 else text[:kept] + "…"


def _num(value: Any) -> str:
    return "—" if value is None else f"{value:,}".replace(",", " ")


def _yesno(value: Any) -> str:
    return "да" if value else "нет"


templates.env.filters["when"] = _when
templates.env.filters["ago"] = _ago
templates.env.filters["short"] = _short
templates.env.filters["num"] = _num
templates.env.filters["yesno"] = _yesno
templates.env.autoescape = True
templates.env.globals["version"] = __version__
templates.env.globals["navigation"] = NAVIGATION


def render(request: Request, name: str, *, status_code: int = 200, refresh: int | None = None, **context: Any) -> Any:
    """A page: the notice or the error the request carries in its query (after
    a redirect), the time now, and what was given."""
    notice, error = request.query_params.get("notice"), request.query_params.get("error")
    return templates.TemplateResponse(
        request,
        name,
        {
            "notice": notice,
            "error": error,
            "refresh": refresh,
            "now": datetime.now(UTC),
            "here": request.url.path,
            **context,
        },
        status_code=status_code,
    )


def refused(request: Request, why: str, status_code: int = 403) -> Any:
    """The page that says the request was not allowed, and why."""
    return render(request, "refused.html", why=why, status_code=status_code)


def page_of(request: Request, *, per_page: int | None = None) -> tuple[int, int, int]:
    """The page asked for (``?page=N``, from 1), how many on a page, and where
    that page begins. What is not a number is the first page."""
    wanted = max(1, min(int(per_page or PER_PAGE), MOST_PER_PAGE))
    try:
        page = int(request.query_params.get("page", "1"))
    except ValueError:
        page = 1
    page = max(1, page)
    return page, wanted, (page - 1) * wanted


def pages_of(total: int, per_page: int) -> int:
    return max(1, -(-total // per_page))


def query(request: Request, **changes: str | None) -> str:
    """The request's query with some of it changed or dropped (None): for a
    link to the same page with another sort, or a notice left out."""
    kept = {name: value for name, value in request.query_params.items() if name not in ("notice", "error")}
    for name, value in changes.items():
        if value is None:
            kept.pop(name, None)
        else:
            kept[name] = value
    return ("?" + urlencode(sorted(kept.items()))) if kept else ""


def page(request: Request, name: str, login: auth.Login, active: str, /, **context: Any) -> Any:
    """A page behind the door: it knows the session (for the form's token in
    every form and for the logout) and which item of the navigation it is.

    ``login`` and ``active`` are positional on purpose: a page that has a
    variable of either name gives it another, instead of colliding with
    these."""
    clashing = sorted(set(context) & {"login", "active", "csrf"})
    if clashing:
        raise TypeError(f"page() takes {', '.join(clashing)} from the door and the navigation, not from the page: name your variable otherwise")
    return render(request, name, login=login, csrf=auth.csrf_token(login.csrf), active=active, **context)


templates.env.filters["query"] = query
