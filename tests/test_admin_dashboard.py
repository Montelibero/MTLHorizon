"""The dashboard and the activity page as a browser meets them: every figure
is the statistics' own and says whether it was counted or estimated; each
service is said in words that keep an empty queue apart from a service that
is not running; the activity is counted by ledger time with the speed of the
import apart; a watched account is switched off from its row and loses
nothing; and no page asks a server when it opens."""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import NamedTuple
from urllib.parse import quote_plus
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from markupsafe import escape
from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.admin.areas import activity as act
from app.admin.areas import dashboard as page
from app.api.app import create_app
from app.stats import activity as counting
from app.stats import dashboard as stats
from app.storage import heartbeats, service_jobs
from app.storage.models import (
    DiscoveryRun,
    HistoryJob,
    ServiceJob,
    ServiceState,
    TrackedAccount,
    TrackedAsset,
    WatchSubscription,
)
from tests import factories, test_activity
from tests.fake_horizon import A, B, C, D, ISSUER, FakeHorizon, serve
from tests.test_admin_web import EVIL, SECRET, csrf_of, login, settings_for
from tests.test_dashboard_stats import HOUR, MINUTE, NOW, Yard, stored_history

SECOND = timedelta(seconds=1)

# What a figure looks like on the dashboard: its path in the snapshot, and
# under it its value — or that it is not known.
FIGURE = re.compile(r'data-figure="([^"]+)"[^>]*>(?:<span class="fig-value">([^<]*)</span>|неизвестно)')
NOT_A_FRONTIER = "последний леджер среди обработанных заданий — не фронтир"
NOT_HEARD = "— служба не работает или застряла"


# -- what the pages are opened with ---------------------------------------------------


def num(value: Any) -> str:
    """A number as the pages write it."""
    return "—" if value is None else f"{value:,}".replace(",", " ")


def written(moment: datetime) -> str:
    """A moment as the pages write it: UTC, to the second."""
    return moment.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


@contextmanager
def opened(url: str) -> Iterator[TestClient]:
    """A browser that has logged into the admin UI of the installation at
    ``url``. The application is a new one each time, as a restart makes it:
    what a process keeps for itself — the cache — starts empty."""
    with TestClient(create_app(settings_for(url))) as client:
        assert login(client).status_code == 303
        yield client


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[datetime]:
    """The moment the dashboard is read at: NOW, until a test moves it."""
    at = [NOW]
    monkeypatch.setattr(page, "now", lambda: at[0])
    return at


class Statements:
    """What every engine of the process sends to PostgreSQL."""

    def __init__(self) -> None:
        self.seen: list[str] = []

    def __call__(self, conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: Any) -> None:
        self.seen.append(statement)

    def of(self, part: str) -> list[str]:
        """The statements that contain ``part``."""
        return [statement for statement in self.seen if part in statement]

    def counting(self) -> list[str]:
        """The statements that count a whole table's rows: the expensive ones."""
        return [statement for statement in self.seen if statement.startswith("SELECT count(*) FROM ") and " WHERE " not in statement]


@pytest.fixture
def statements() -> Iterator[Statements]:
    seen = Statements()
    event.listen(Engine, "before_cursor_execute", seen)
    yield seen
    event.remove(Engine, "before_cursor_execute", seen)


def figures_of(node: Any, prefix: str = "") -> Iterator[tuple[str, dict[str, Any]]]:
    """Every figure of a snapshot, by where it is in it."""
    for name, item in node.items():
        path = f"{prefix}.{name}" if prefix else name
        if isinstance(item, dict) and "value" in item and "exact" in item:
            yield path, item
        elif isinstance(item, dict):
            yield from figures_of(item, path)
        elif isinstance(item, list):
            for index, element in enumerate(item):
                if isinstance(element, dict):
                    yield from figures_of(element, f"{path}.{index}")


def shown_figures(html: str) -> dict[str, str]:
    """The figures of a dashboard page, by path: a value, or «неизвестно»."""
    found = [(found.group(1), found.group(2) or "неизвестно") for found in FIGURE.finditer(html)]
    paths = [path for path, _ in found]
    assert len(paths) == len(set(paths)), "a figure is shown once under its path"
    return dict(found)


def path_of(table: str) -> str:
    """Where the rows of one of the tables of distinct records are."""
    return f"database.rows.{stats.UNIQUE_TABLES[table]}"


def the(html: str, path: str) -> str:
    """What stands under a figure of a card: how it was got, and when it was measured."""
    found = re.search(r'data-figure="' + re.escape(path) + r'"[^>]*>.*?</div>\s*<div class="sub">(.*?)</div>', html, re.S)
    assert found, f"{path} is a figure of a card"
    return re.sub(r"</?time[^>]*>", "", found.group(1))


def mark(html: str, path: str) -> str:
    """Whether the figure at ``path`` is shown as counted exactly or not."""
    found = re.search(r'data-figure="' + re.escape(path) + r'"[^>]*data-exact="([01])"', html)
    assert found, f"{path} is on the page"
    return found.group(1)


WORKING = ("работает, очередей нет", "работает:", "работает, но")


def works(said: str) -> bool:
    """Whether a service's block says that the service is heard of and working."""
    return any(word in said for word in WORKING)


def block(html: str, title: str) -> str:
    """What a service's card says: from its title to the end of its lines."""
    found = re.search(r"<h2>" + re.escape(title) + r".*?</dl>", html, re.S)
    assert found, f"the page has the block {title}"
    return found.group(0)


def section(html: str, start: str, end: str) -> str:
    """The page from one heading up to what follows the block."""
    assert html.count(f"<h2>{start}") == 1, start
    return html[html.index(f"<h2>{start}") : html.index(end, html.index(f"<h2>{start}"))]


# The tags at which a line of text begins or ends.
_LINES = re.compile(r"</(?:div|p|dd|dt|tr|td|th|h[1-6]|li|table|dl|form|label)>|<(?:div|p|h[1-6]|tr|li|table|dl|form|label|br)\b[^>]*>")


def plain(html: str) -> str:
    """The text of a page as the eye reads it: without its tags, so that a
    moment or an ``ago`` stands in its sentence as the page writes it; each
    block on its own line; runs of blanks made one."""
    lines = _LINES.sub("\n", html)
    return re.sub(r"[ \t]+", " ", re.sub(r"<[^>]*>", "", lines)).strip()


def tells_nothing_it_must_not(html: str, url: str) -> None:
    assert SECRET not in html and url not in html, "neither the secret nor the database's address is shown"
    assert "<script>alert" not in html, "what a page shows is escaped"


# -- what a running installation leaves in the database --------------------------------


def history_jobs(yard: Yard, states: dict[str, int]) -> None:
    """History jobs in each state; the earlier ones have waited longest."""
    made = 0

    def make(session: Session) -> None:
        nonlocal made
        for state, count in states.items():
            for _ in range(count):
                made += 1
                session.add(
                    HistoryJob(
                        kind="account", subject=factories.address(2000 + made), state=state, to_ledger=50_000,
                        created_at=NOW - made * MINUTE,
                    )
                )

    yard.write(make)


def subscriptions(yard: Yard, states: dict[str, int], problems: list[dict[str, Any]]) -> None:
    made = 0

    def make(session: Session) -> None:
        nonlocal made
        for state, count in states.items():
            for _ in range(count):
                made += 1
                session.add(
                    WatchSubscription(
                        kind="account", key=factories.address(3000 + made), state=state, remote_id=f"remote-{made}"
                    )
                )
        for problem in problems:
            session.add(WatchSubscription(**problem))

    yard.write(make)


def discovery(yard: Yard) -> None:
    """Two assets marked for discovery, each with a pass; accounts by origin."""
    issuer, other = factories.ISSUER_A, factories.ISSUER_B
    eur, rect, usd = f"EURMTL:{issuer}", f"MTLRECT:{issuer}", f"USD:{other}"
    origins = (("discovery", True, 7), ("discovery", False, 2), ("manual", True, 3), ("legacy", True, 1))
    made = 0

    def accounts(session: Session) -> None:
        nonlocal made
        for origin, enabled, count in origins:
            for _ in range(count):
                made += 1
                account = factories.address(4000 + made)
                factories.add_account(session, account)
                session.add(TrackedAccount(account_id=account, enabled=enabled, reason="test", origin=origin))
        for asset in (eur, rect, usd):
            factories.add_asset(session, asset)
            session.add(TrackedAsset(asset_id=asset, enabled=True, discover=asset != usd, reason="test"))

    yard.write(accounts)
    yard.write(
        lambda session: session.add_all(
            [
                DiscoveryRun(
                    asset_id=eur, state="incomplete", started_at=NOW - 5 * MINUTE, finished_at=NOW - 4 * MINUTE,
                    pages=2, found=400, with_line=400, added=300, already=100, last_error=EVIL,
                ),
                DiscoveryRun(asset_id=rect, state="running", started_at=NOW - 20 * MINUTE),
            ]
        )
    )


def a_full_installation(yard: Yard) -> tuple[int, int]:
    """An installation with something in every state the dashboard shows.
    Returns the ids of its two tasks: one ended, one in the queue."""
    yard.beat(stats.WORKER_HEARTBEAT, NOW, name="worker-1", state="working", job_id=7, done=12, failed=1)
    yard.beat(stats.HISTORY_HEARTBEAT, NOW, name="history-1", state="working", job_id=9, slices_done=40)
    yard.write(stored_history)
    yard.jobs("done", 4, ledger=lambda index: 1000 + index)
    yard.jobs("pending", 3, created=NOW - 40 * MINUTE, due=NOW - 3 * MINUTE)
    yard.jobs("running", lease_until=NOW + MINUTE)
    yard.jobs("partial", error="503 on the trades of the transaction, 8 tries", finished=NOW - 2 * MINUTE)
    yard.jobs("dead", 2, error=EVIL, finished=NOW - 30 * MINUTE)
    yard.event(NOW - MINUTE, ledger=701, state="unmatched")
    history_jobs(yard, {"waiting": 3, "running": 1, "done": 10, "partial": 2, "error": 1, "paused": 4})
    subscriptions(
        yard,
        {"wanted": 2, "active": 5, "retiring": 1, "manual": 1, "retired": 3},
        [
            {"kind": "account", "key": factories.address(3500), "state": "error", "last_error": EVIL, "attempts": 3,
             "next_attempt_at": NOW + 30 * MINUTE},
            {"kind": "asset", "key": f"MTLRECT:{factories.ISSUER_B}", "state": "ambiguous", "last_error": "two subscriptions fit",
             "attempts": 1, "next_attempt_at": NOW + MINUTE},
        ],
    )
    discovery(yard)
    ended, _ = service_jobs.enqueue(yard.engine, "pool_check", {"repeats": 1}, one_at_a_time="pool", now=NOW)
    taken = service_jobs.take(yard.engine, "runner-1", now=NOW)
    assert taken is not None and taken.id == ended.id
    service_jobs.finish(yard.engine, taken, result={"available": True}, now=NOW)
    queued, made = service_jobs.enqueue(yard.engine, "pool_check", {"repeats": 1}, one_at_a_time="pool", now=NOW)
    assert made
    with Session(yard.engine) as session, session.begin():
        session.get(ServiceJob, queued.id).last_error = EVIL  # type: ignore[union-attr]
    return ended.id, queued.id


# -- the dashboard: what it says of the services ---------------------------------------


def test_an_installation_where_nothing_ever_ran_is_two_things_said_apart(
    make_database: Callable[[], str], clock: list[datetime], monkeypatch: pytest.MonkeyPatch
) -> None:
    url = make_database()
    yard = Yard(url)
    fake = FakeHorizon()
    serve(monkeypatch, fake)
    try:
        with opened(url) as client:
            answer = client.get("/admin")
        assert answer.status_code == 200
        assert fake.requests == [], "the dashboard asks no server when it opens"
        html = answer.text
        # Neither service ever said anything: said so for each — and what the queue holds, apart.
        assert html.count("не слышно ни разу " + NOT_HEARD) == 2
        assert html.count("(очередь пуста)") == 2
        assert not works(block(html, "Живой воркер")) and not works(block(html, "Служба истории"))
        assert "Очередь пуста: ничего не ждёт и ничего не делается." in html
        # The page refreshes itself while it is looked at: the browser asks for it again.
        assert 'data-refresh="30"' in html and "обновлять каждые 30 с" in html
        assert "Заданий, которые отдались, нет." in html and "Уведомлений ещё не приходило." in html
        assert "Подписок в error и ambiguous нет." in html and "Проходов discovery ещё не было." in html
        assert "Задач из админки ещё не запускали." in html
        # Every block leads to its page.
        for href in ("/admin/import", "/admin/notifier", "/admin/discovery", "/admin/tasks"):
            assert f'href="{href}"' in html, href
        tells_nothing_it_must_not(html, url)
    finally:
        yard.close()


SERVICES = {
    "worker": (stats.WORKER_HEARTBEAT, "Живой воркер"),
    "history": (stats.HISTORY_HEARTBEAT, "Служба истории"),
}


@pytest.mark.parametrize("service", SERVICES)
def test_a_service_is_said_in_one_of_three_sentences_and_its_queue_apart(
    make_database: Callable[[], str], clock: list[datetime], service: str
) -> None:
    record, title = SERVICES[service]
    url = make_database()
    yard = Yard(url)
    try:

        def holds(running: int, waiting: int) -> None:
            with yard.engine.begin() as connection:
                connection.execute(text("DELETE FROM processing_job"))
                connection.execute(text("DELETE FROM history_job"))
            if service == "worker":
                if running:
                    yard.jobs("running", running, lease_until=NOW + MINUTE)
                if waiting:
                    yard.jobs("pending", waiting, due=NOW + 5 * MINUTE)
            else:
                history_jobs(yard, {state: count for state, count in (("running", running), ("waiting", waiting)) if count})

        def said() -> str:
            with opened(url) as client:
                answer = client.get("/admin")
            assert answer.status_code == 200
            return block(answer.text, title)

        # Heard of ten seconds ago, idle, nothing in the queue: it works, and there is no queue.
        holds(0, 0)
        yard.beat(record, NOW - 10 * SECOND, name="service-1", state="idle", job_id=None)
        idle = said()
        assert "работает, очередей нет" in idle and "простаивает" in idle and "очередь пуста" not in idle
        assert written(NOW - 10 * SECOND) in idle and "10 с назад" in idle and "service-1" in idle

        # Heard of now, on a job: what is in progress and what waits.
        holds(1, 3)
        heartbeats.beat(yard.engine, record, now=NOW, name="service-1", state="working", job_id=7)
        working = said()
        assert "работает: в работе 1, ждут 3" in working and "на задании #7" in working
        assert "не слышно" not in working and "очередей нет" not in working

        # Not heard of for five minutes. The queue is the same; the saying is not.
        heartbeats.beat(yard.engine, record, now=NOW - 5 * MINUTE, name="service-1", state="idle", job_id=None)
        stale = said()
        assert "не слышно уже 5 мин " + NOT_HEARD in stale
        assert "(ждут 3, в работе 1)" in stale and "в последний раз — простаивает" in stale
        assert not works(stale)

        # Not heard of, and nothing in the queue: the empty queue does not pass for a working service.
        holds(0, 0)
        silent = said()
        assert "не слышно уже 5 мин " + NOT_HEARD in silent and "(очередь пуста)" in silent
        assert "ждут" not in silent and not works(silent)
    finally:
        yard.close()


def test_a_worker_that_says_idle_while_work_is_due_and_work_that_waits_for_nobody(
    make_database: Callable[[], str], clock: list[datetime]
) -> None:
    url = make_database()
    yard = Yard(url)
    try:
        yard.jobs("pending", due=NOW - MINUTE)
        with opened(url) as client:
            # Idle, said half a minute ago: it may simply not have got to the job yet — the queue says so.
            yard.beat(stats.WORKER_HEARTBEAT, NOW - 30 * SECOND, name="worker-1", state="idle")
            answer = client.get("/admin")
            assert answer.status_code == 200
            assert "работает: в работе 0, ждут 1" in block(answer.text, "Живой воркер")
            assert "Работа давно ждёт, а воркер говорит, что простаивает: возможно, застрял." in answer.text
            # Said it an hour ago: nobody is taking the work.
            heartbeats.beat(yard.engine, stats.WORKER_HEARTBEAT, now=NOW - HOUR, name="worker-1", state="idle")
            answer = client.get("/admin")
            assert "Работа есть, а брать её некому: воркера не слышно." in answer.text
            assert "не слышно уже 1 ч " + NOT_HEARD in block(answer.text, "Живой воркер")
    finally:
        yard.close()


def test_a_history_queue_paused_by_hand_is_said_to_be_working_and_paused(
    make_database: Callable[[], str], clock: list[datetime]
) -> None:
    url = make_database()
    yard = Yard(url)
    try:
        history_jobs(yard, {"waiting": 3, "paused": 2, "done": 5})
        yard.beat(stats.HISTORY_HEARTBEAT, NOW, name="history-1", state="paused")
        yard.write(
            lambda session: session.add(
                ServiceState(
                    name=stats.HISTORY_PAUSE,
                    value={"paused": True, "because": "too many requests", "by": "owner", "at": (NOW - HOUR).isoformat()},
                )
            )
        )
        with opened(url) as client:
            answer = client.get("/admin")
        assert answer.status_code == 200
        history = block(answer.text, "Служба истории")
        assert "работает, но очередь на паузе: в работе 0, ждут 3" in history and "очередей нет" not in history
        said = plain(section(answer.text, "Импорт истории", "Подписки"))
        assert "Вся очередь истории на паузе. Причина: too many requests. Поставил: owner " + written(NOW - HOUR) in said
        assert shown_figures(section(answer.text, "Импорт истории", "Подписки"))["history.jobs.paused"] == "2"
    finally:
        yard.close()


def test_a_history_service_that_says_of_itself_it_is_paused_is_said_to_be(
    make_database: Callable[[], str], clock: list[datetime]
) -> None:
    url = make_database()
    yard = Yard(url)
    try:
        history_jobs(yard, {"waiting": 4, "done": 5})
        # The service's own word of itself is "paused"; nobody paused the whole queue by hand.
        yard.beat(stats.HISTORY_HEARTBEAT, NOW, name="history-1", state="paused")
        with opened(url) as client:
            answer = client.get("/admin")
        history = block(answer.text, "Служба истории")
        assert "работает, но очередь на паузе: в работе 0, ждут 4" in history and "на паузе" in history
        assert "Вся очередь истории на паузе" not in answer.text, "the pause of the whole queue is another thing, and was not asked"
    finally:
        yard.close()


# -- the dashboard: what it shows -----------------------------------------------------


def test_every_figure_on_the_page_is_the_statistics_own(make_database: Callable[[], str], clock: list[datetime]) -> None:
    url = make_database()
    yard = Yard(url)
    try:
        ended, queued = a_full_installation(yard)
        expected = dict(figures_of(stats.snapshot(yard.engine, now=NOW).as_dict()))
        with opened(url) as client:
            answer = client.get("/admin")
        assert answer.status_code == 200
        html = answer.text
        found = shown_figures(html)
        assert found, "the page shows figures"
        for path, shown in found.items():
            assert shown == num(expected[path]["value"]), path
            assert mark(html, path) == ("1" if expected[path]["exact"] else "0"), path
        # Everything the dashboard is asked to count is there, under its place in the snapshot.
        for path in (
            *(f"queue.{state}" for state in stats.QUEUE_STATES),
            "queue.pending_due", "queue.running_past_lease", "queue.waiting_for_a_person",
            *(f"history.jobs.{state}" for state in stats.HISTORY_STATES),
            *(f"subscriptions.counts.{state}" for state in stats.SUBSCRIPTION_STATES),
            "discovery.marked", "discovery.accounts", "discovery.accounts_enabled",
            "discovery.accounts_by_origin.discovery.total", "discovery.accounts_by_origin.discovery.enabled",
            "discovery.accounts_by_origin.manual.total", "discovery.accounts_by_origin.legacy.enabled",
            *(path_of(table) for table in stats.UNIQUE_TABLES),
        ):
            assert path in found, path
        assert [found[f"queue.{state}"] for state in stats.QUEUE_STATES] == ["3", "1", "4", "1", "2"]
        assert found["queue.pending_due"] == "3" and found["queue.waiting_for_a_person"] == "3"
        assert found["history.jobs.paused"] == "4" and found["subscriptions.counts.active"] == "5"
        assert (found["discovery.marked"], found["discovery.accounts"], found["discovery.accounts_enabled"]) == ("2", "13", "11")
        # The blocks say what they hold, each in the words of its own page.
        services = section(html, "Службы", "Очередь событий")
        assert "работает: в работе 1, ждут 3" in block(services, "Живой воркер")
        assert "на задании #7" in block(services, "Живой воркер") and "на задании #9" in block(services, "Служба истории")
        assert "<dt>Срезов сделано</dt><dd>40</dd>" in block(services, "Служба истории")
        assert "Работа есть, и воркер её берёт." in html
        freshness = plain(section(html, "Свежесть данных", "Импорт истории"))
        assert written(NOW - MINUTE) in freshness and "1 мин назад" in freshness and "unmatched" in freshness and "701" in freshness
        assert num(1003) in freshness, "the greatest ledger among the newest finished jobs"
        imports = section(html, "Подписки", "Discovery")
        assert "&lt;script&gt;" in imports and "two subscriptions fit" in imports and "через 30 мин" in imports
        passes = section(html, "Discovery", "PostgreSQL")
        assert "incomplete" in passes and "running" in passes and "&lt;script&gt;" in passes
        assert "Страницы этой очереди в админке нет." in html
        tasks = section(html, "Задачи", "</main>")
        for job_id in (ended, queued):
            assert f'href="/admin/tasks/{job_id}"' in tasks and str(job_id) in tasks
        assert "done" in tasks and "queued" in tasks and "&lt;script&gt;" in tasks
        tells_nothing_it_must_not(html, url)
    finally:
        yard.close()


def test_the_errors_of_the_jobs_that_gave_up_and_the_oldest_that_waits(
    make_database: Callable[[], str], clock: list[datetime]
) -> None:
    url = make_database()
    yard = Yard(url)
    try:
        yard.beat(stats.WORKER_HEARTBEAT, NOW, name="worker-1", state="idle")
        oldest = NOW - 40 * MINUTE
        yard.jobs("pending", 2, due=NOW - 3 * MINUTE, created=oldest)
        (partial,) = yard.jobs("partial", error="503 on the trades of the transaction, 8 tries", finished=NOW - 2 * MINUTE)
        (dead,) = yard.jobs("dead", error=EVIL, finished=NOW - MINUTE)
        with opened(url) as client:
            answer = client.get("/admin")
        html = answer.text
        queue = section(html, "Очередь событий", "Свежесть данных")
        assert f"Старейшее ждущее задание #1 (с {written(oldest)}) ждёт 40 мин." in plain(queue)
        # The newest first, each with the error of its last try.
        rows = re.findall(r"<tr>\s*<td>(\d+)</td>\s*<td><span class=\"badge badge-bad\">(\w+)</span>", queue)
        assert rows == [(str(dead), "dead"), (str(partial), "partial")]
        assert escape(EVIL) in queue and "503 on the trades of the transaction, 8 tries" in queue
        assert "503 on the trades of the transaction, 8 tries" in queue
        tells_nothing_it_must_not(html, url)
        # Where they are seen in full: the commands, for there is no page of this queue.
        assert "<code>python -m app.worker status</code>" in queue and "<code>python -m app.worker retry</code>" in queue
    finally:
        yard.close()


def test_a_queue_too_large_to_read_the_oldest_job_by_its_row_says_a_lower_bound(
    make_database: Callable[[], str], clock: list[datetime], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stats, "OLDEST_AGE_EXACT_LIMIT", 1)
    url = make_database()
    yard = Yard(url)
    try:
        yard.jobs("pending", 3, due=NOW - 90 * MINUTE)
        with opened(url) as client:
            answer = client.get("/admin")
        assert "Старейшее ждущее задание ждёт не меньше, чем 1 ч." in answer.text
    finally:
        yard.close()


def test_the_last_ledger_among_finished_jobs_is_never_called_a_frontier(
    make_database: Callable[[], str], clock: list[datetime]
) -> None:
    url = make_database()
    yard = Yard(url)
    try:
        yard.event(NOW - MINUTE, ledger=701, state="unmatched")
        yard.jobs("done", 3, ledger=lambda index: (5, 9, 7)[index], finished=lambda index: NOW - index * MINUTE)
        with opened(url) as client:
            answer = client.get("/admin")
        html = answer.text
        assert NOT_A_FRONTIER in html and "из него не следует, что всё ниже увидено" in html
        assert "Последний леджер среди обработанных заданий</dt><dd><strong>9</strong>" in html
        assert "среди 3 новейших завершённых заданий" in html
        # The word is used of it only to deny it; nothing on the page calls it one.
        assert all(html[max(0, found.start() - 3) : found.start()] == "не " for found in re.finditer("фронтир", html, re.I))
        assert not re.search("frontier", html, re.I)
        # What the last delivery was, and when.
        assert written(NOW - MINUTE) + " · 1 мин назад" in plain(html) and "Последнее уведомление" in html
        assert "unmatched" in html and "леджер 701" in html
    finally:
        yard.close()


def test_a_figure_says_whether_it_was_counted_or_estimated_and_when(
    make_database: Callable[[], str], clock: list[datetime], monkeypatch: pytest.MonkeyPatch
) -> None:
    url = make_database()
    yard = Yard(url)
    try:
        yard.write(stored_history)
        yard.jobs("pending", 2)
        with opened(url) as client:
            counted = client.get("/admin").text
        assert "Уникальные эффекты</div><div class=\"value\"><span class=\"fig\"" in counted
        assert mark(counted, path_of("effects")) == "1" and mark(counted, "queue.pending") == "1"
        assert the(counted, path_of("effects")) == f'<span class="badge badge-ok">точно</span> · измерено {written(NOW)}'
        assert 'data-exact="0"' not in section(counted, "PostgreSQL", "Задачи"), "every figure of a small table is counted"
        # The tables grew: the planner's number stands in for a count, and is said to.
        yard.analyze()
        monkeypatch.setattr(stats, "EXACT_COUNT_LIMIT", 1)
        with opened(url) as client:
            estimated = client.get("/admin").text
        database = section(estimated, "PostgreSQL", "Задачи")
        for table in stats.UNIQUE_TABLES:
            assert mark(estimated, path_of(table)) == "0", table
            assert the(estimated, path_of(table)).startswith(
                '<span class="badge badge-wait">оценка</span> · измерено ' + written(NOW) + " · оценка планировщика PostgreSQL"
            ), table
        estimate = '<span class="fig-value">2</span> <span class="hint">оценка</span>'
        assert estimate in database  # effects: two rows, as an estimate
        # What is read through an index is counted, however large the tables grew. ``done`` is
        # not read at all — it is the table's rows less the other states — and says what it rests on.
        queue = section(estimated, "Очередь событий", "Свежесть данных")
        assert mark(estimated, "queue.pending") == "1" and mark(estimated, "queue.running") == "1"
        assert mark(estimated, "queue.done") == "0" and "минус остальные состояния, посчитанные сейчас" in queue
    finally:
        yard.close()


def test_a_table_that_cannot_be_counted_for_a_page_says_so(
    make_database: Callable[[], str], clock: list[datetime], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stats, "NEVER_ANALYZED_PAGES_LIMIT", 0)
    url = make_database()
    yard = Yard(url)
    try:
        yard.write(stored_history)
        with opened(url) as client:
            html = client.get("/admin").text
        for table in stats.UNIQUE_TABLES:
            assert shown_figures(html)[path_of(table)] == "неизвестно" and mark(html, path_of(table)) == "0"
            assert "таблицу не анализировали" in the(html, path_of(table))
    finally:
        yard.close()


def test_the_cache_spares_the_database_the_expensive_counts_and_a_figure_says_when_it_was_measured(
    make_database: Callable[[], str], clock: list[datetime], monkeypatch: pytest.MonkeyPatch, statements: Statements
) -> None:
    monkeypatch.setattr(stats, "COUNT_CACHE_SECONDS", 0.5)
    url = make_database()
    yard = Yard(url)
    try:
        yard.write(stored_history)
        with opened(url) as client:
            first = client.get("/admin").text
            again = client.get("/admin").text
            assert len(statements.counting()) == 5, "the five counted tables, counted once"
            # Another opening, another minute by the clock: the figure still says when it was really measured.
            clock[0] = NOW + MINUTE
            later = client.get("/admin").text
            assert len(statements.counting()) == 5, "and not again while the cache holds them"
            assert written(NOW) in the(later, path_of("effects"))
            time.sleep(0.6)
            after = client.get("/admin").text
        assert len(statements.counting()) == 10, "counted again once the cache let them go"
        assert written(NOW + MINUTE) in the(after, path_of("effects"))
        assert shown_figures(first) == shown_figures(again) == shown_figures(after)
    finally:
        yard.close()


def test_a_snapshot_that_runs_out_of_time_says_so_and_holds_nothing(
    make_database: Callable[[], str], clock: list[datetime], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stats, "STATEMENT_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(
        stats, "_QUEUE_COUNTS",
        text("SELECT state, count(*) FROM processing_job WHERE pg_sleep(0.3) IS NOT NULL GROUP BY state"),
    )
    url = make_database()
    yard = Yard(url)
    try:
        yard.jobs("pending", 2)
        with opened(url) as client:
            answer = client.get("/admin")
        assert answer.status_code == 503
        assert "База не успела ответить" in answer.text and "дашборд не построен" in answer.text
        assert 'data-figure' not in answer.text
    finally:
        yard.close()

# -- the activity of the accounts ------------------------------------------------------


LOADED = test_activity.NOW  # when the last ledger of the fake's history closed
KINDS = counting.KINDS
STORED = datetime(2026, 6, 1, 10, 30, tzinfo=UTC)  # a moment, long after, at which everything was stored


class Installation(NamedTuple):
    url: str
    yard: test_activity.Yard


@pytest.fixture
def loaded(make_database: Callable[[], str]) -> Iterator[Installation]:
    """A private database with the watch list, and what the fake Horizon holds
    loaded into it by the backfill — by the rules of the backfill."""
    url = make_database()
    yield Installation(url, test_activity.Yard(url, test_activity.history()))


@pytest.fixture
def ledger_clock(monkeypatch: pytest.MonkeyPatch) -> list[datetime]:
    """The moment the activity page is read at: LOADED, until a test moves it."""
    at = [LOADED]
    monkeypatch.setattr(act, "now", lambda: at[0])
    return at


def row_of(html: str, account: str) -> str:
    """The row of one account of the table."""
    found = re.search(r'<tr data-account="' + re.escape(account) + r'".*?</tr>', html, re.S)
    assert found, f"the table has the row of {account}"
    return found.group(0)


def accounts_in(html: str) -> list[str]:
    """The accounts of the table's rows, in their order."""
    return re.findall(r'<tr data-account="([^"]+)"', html)


def counted(row: str, kind: str) -> tuple[str, str]:
    """What a count says: how many in the window, and — under it — the change
    against the window before, and how many there were."""
    found = re.search(r'data-count="' + kind + r'"><strong>([^<]*)</strong><div class="hint">([^<]*)</div>', row)
    assert found, f"the row counts {kind}"
    return found.group(1), found.group(2)


def assets_of(row: str) -> tuple[str, str]:
    """The cell of a row that tells own assets from foreign: its state, and what it says."""
    found = re.search(r'<td data-assets="(\w+)">(.*?)</td>', row, re.S)
    assert found, "the row has the cell of the assets"
    return found.group(1), found.group(2)


def figure(html: str, path: str) -> str:
    """The value shown under a name of a figure of the page."""
    found = re.search(r'data-figure="' + re.escape(path) + r'"[^>]*>([^<]*)<', html)
    assert found, f"the page shows {path}"
    return found.group(1)


def totals(html: str) -> list[str]:
    """The installation's own figures, as the page shows them."""
    return [figure(html, f"totals.{kind}") for kind in KINDS]


def speed_block(html: str) -> str:
    """The part of the page that speaks of the speed of the import."""
    return html[html.index("<h2>Скорость загрузки") : html.index("<h3>По часам")]


def test_a_week_of_every_watched_account_by_ledger_time_and_the_installation_apart(
    loaded: Installation, ledger_clock: list[datetime]
) -> None:
    url, yard = loaded
    asked = len(yard.fake.requests)
    with opened(url) as client:
        answer = client.get("/admin/activity")
    assert answer.status_code == 200
    assert len(yard.fake.requests) == asked, "the page asks no server when it opens"
    html = answer.text
    # A week is the default, and it ends now; the window before is of the same length.
    said = plain(html)
    assert "Период — неделя: с " + written(LOADED - 7 * counting.DAY) + " по " + written(LOADED) in said
    assert "прежнее окно той же длины: с " + written(LOADED - 14 * counting.DAY) + " по " + written(LOADED - 7 * counting.DAY) in said
    assert set(accounts_in(html)) == set(test_activity.WATCHED)
    alfa = row_of(html, A)
    # Each count, against the window before: by how much and in percents, and how many there were.
    assert (counted(alfa, "transactions"), counted(alfa, "operations")) == (("5", "+3 (+150 %) · было 2"),) * 2
    assert counted(row_of(html, B), "effects") == ("5", "+3 (+150 %) · было 2")
    delta = row_of(html, D)
    assert counted(delta, "transactions") == ("2", "+1 (+100 %) · было 1")
    assert counted(delta, "trades") == ("1", "+1 (—) · было 0")  # the window before had none: a share of zero is no number
    assert counted(row_of(html, ISSUER), "trades") == ("0", "0 (—) · было 0")
    assert counted(row_of(html, test_activity.S), "operations") == ("1", "+1 (—) · было 0")
    # The last event of an account is of all its time, by the ledger's clock. A's newest record is
    # five seconds after now: of no window, and its last event all the same.
    assert written(test_activity.closed(45, 1)) in plain(alfa) and "\nв будущем · леджер " + num(test_activity.day(45, 1)) in plain(alfa)
    assert written(test_activity.closed(43)) in delta
    assert f'href="/admin/accounts/{A}"' in alfa and f'data-copy="{A}"' in alfa, "a link to the account's page"
    # The installation's figures are its own: a transaction of two watched accounts is one record
    # there, and a row of each account here.
    assert totals(html) == ["9", "10", "17", "1"]
    assert sum(int(counted(row_of(html, account), "transactions")[0]) for account in test_activity.WATCHED) == 16
    assert sum(int(counted(row_of(html, account), "operations")[0]) for account in test_activity.WATCHED) == 17
    assert "никогда</strong> как сумма по аккаунтам" in html
    # The share of own and foreign assets is told only where the database proves it.
    issuer = assets_of(row_of(html, ISSUER))
    assert issuer[0] == "classified" and ">неизвестно<" not in issuer[1]
    assert "50 %" in issuer[1] and "свои 1 · чужие 1" in issuer[1]
    assert "50 %" in assets_of(delta)[1]
    for account in (A, B, C, test_activity.S):
        state, cell = assets_of(row_of(html, account))
        assert (state, ">неизвестно<" in cell, "нет эффекта account_created" in cell) == ("unknown", True, True), account
    # The volume of data: not known until the planner has its numbers — and said to be not known, never guessed.
    assert all('data-volume="unknown"' in row_of(html, account) for account in test_activity.WATCHED)
    assert "Объём неизвестен" in said and "ANALYZE" in said
    assert "Записи двух наблюдаемых аккаунтов общие, и объёмы складывать нельзя." in said
    assert "объём = транзакции ×" in said and "Native (XLM) никем не выпущен" in said
    tells_nothing_it_must_not(html, url)


def test_the_period_controls_change_the_numbers(loaded: Installation, ledger_clock: list[datetime]) -> None:
    url, _ = loaded
    with opened(url) as client:
        day = client.get("/admin/activity?period=day").text
        month = client.get("/admin/activity?period=month").text
        chosen = client.get("/admin/activity?period=range&from=2026-01-20&to=2026-02-01").text
        kept = client.get("/admin/activity?period=day&sort=trades&enabled=on").text
    assert totals(day) == ["1", "1", "2", "0"] and "Период — сутки: с " + written(LOADED - counting.DAY) in plain(day)
    assert counted(row_of(day, A), "operations") == ("1", "+1 (—) · было 0")
    assert counted(row_of(day, D), "trades") == ("0", "-1 (-100 %) · было 1")  # the trade was the day before
    assert all(counted(row_of(day, account), "operations")[0] == "0" for account in test_activity.WATCHED if account not in (A, B))
    assert totals(month) == ["16", "18", "31", "1"] and "с 2026-01-15 00:00:00 UTC" in plain(month)
    assert counted(row_of(month, D), "operations") == ("6", "+6 (—) · было 0")
    # A paid B once in January, before the month: the window before, and the change against it.
    assert counted(row_of(month, A), "transactions") == ("8", "+7 (+700 %) · было 1")
    # Two dates: a window of the owner's choosing, and the one of the same length before it.
    assert totals(chosen) == ["2", "3", "4", "0"]
    assert "Период — с даты по дату: с 2026-01-20 00:00:00 UTC по 2026-02-01 00:00:00 UTC" in plain(chosen)
    assert "прежнее окно той же длины: с 2026-01-08 00:00:00 UTC по 2026-01-20 00:00:00 UTC" in plain(chosen)
    assert counted(row_of(chosen, D), "operations") == ("2", "+1 (+100 %) · было 1")
    assert counted(row_of(chosen, test_activity.S), "operations") == ("0", "-2 (-100 %) · было 2")
    assert 'name="from" value="2026-01-20"' in chosen and 'name="to" value="2026-02-01"' in chosen, "the controls show what was asked"
    # What else the page shows travels with the period.
    assert 'name="period" id="period-day" value="day" checked' in kept and 'value="on" selected' in kept
    assert "Сделки ↓" in kept and "Операции ↓" not in kept
    tells_nothing_it_must_not(chosen, url)


def test_a_period_that_cannot_be_read_is_not_guessed_at(loaded: Installation, ledger_clock: list[datetime]) -> None:
    url, _ = loaded
    with opened(url) as client:
        backwards = client.get("/admin/activity?period=range&from=2026-02-05&to=2026-02-01")
        unreadable = client.get("/admin/activity?period=range&from=whenever&to=2026-02-01")
        unknown = client.get("/admin/activity?period=fortnight")
    for answer in (backwards, unreadable, unknown):
        assert answer.status_code == 200
        assert "Показана неделя." in plain(answer.text) and "Период — неделя" in plain(answer.text)
        assert totals(answer.text) == ["9", "10", "17", "1"], "the week is shown, and said to be"
    assert "конец должен быть позже начала" in backwards.text
    assert "ГОД-МЕСЯЦ-ДЕНЬ" in unreadable.text and 'value="whenever"' in unreadable.text
    assert "«fortnight» не понят" in unknown.text


def test_sorting_by_any_count_the_filter_of_enabled_and_the_pages(loaded: Installation, ledger_clock: list[datetime]) -> None:
    url, yard = loaded
    by_address = sorted(test_activity.WATCHED)

    def rows_of(path: str) -> list[str]:
        with opened(url) as client:
            return accounts_in(client.get(path).text)

    def as_counted(**asked: Any) -> list[str]:
        """The order the statistics give the accounts under ``asked``."""
        return [row.account_id for row in counting.table(yard.engine, now=LOADED, **asked).rows]

    # Most first; ties are told apart by the address, so a page is always the same.
    assert rows_of("/admin/activity") == as_counted()
    for kind in KINDS:
        assert rows_of(f"/admin/activity?sort={kind}") == as_counted(sort=kind), kind
        assert rows_of(f"/admin/activity?sort={kind}&order=asc") == as_counted(sort=kind, descending=False), kind
    assert set(rows_of("/admin/activity?sort=trades")[:2]) == {D, test_activity.S}, "the two sides of the week's trade"
    assert rows_of("/admin/activity?sort=account&order=asc") == by_address and rows_of("/admin/activity?sort=account") == by_address[::-1]
    # The pages: bounded, whatever is asked for.
    with opened(url) as client:
        first = client.get("/admin/activity?sort=account&order=asc&per_page=4").text
        second = client.get("/admin/activity?sort=account&order=asc&per_page=4&page=2").text
        beyond = client.get("/admin/activity?sort=account&order=asc&per_page=4&page=3").text
        most = client.get("/admin/activity?sort=account&order=asc&per_page=5000").text
    assert accounts_in(first) == by_address[:4] and accounts_in(second) == by_address[4:] and accounts_in(beyond) == []
    assert "страница 1 из 2" in first and "страница 2 из 2" in second and "Наблюдаемых аккаунтов с такими условиями нет." in beyond
    assert "page=2" in first and "sort=account" in first and "per_page=4" in first, "the next page keeps what else was asked"
    assert accounts_in(most) == by_address and 'name="per_page" value="200"' in most, "and no more than 200 on a page"
    # Only those switched on, or only those switched off — and the installation's figures follow the filter.
    with yard.engine.begin() as connection:
        connection.execute(text("UPDATE tracked_accounts SET enabled = false WHERE account_id = ANY(:off)"), {"off": [test_activity.S, ISSUER]})
    with opened(url) as client:
        on = client.get("/admin/activity?enabled=on").text
        off = client.get("/admin/activity?enabled=off").text
    assert accounts_in(on) == as_counted(enabled=True) and accounts_in(off) == as_counted(enabled=False)
    assert set(accounts_in(off)) == {ISSUER, test_activity.S} and len(accounts_in(on)) == 4
    assert totals(on) == ["8", "9", "14", "1"] and totals(off) == ["3", "3", "3", "1"]
    assert ">отключён<" not in on and ">включён<" not in off and "Отключить наблюдение" not in off


def test_the_share_that_cannot_be_told_is_unknown_with_its_reason(loaded: Installation, ledger_clock: list[datetime]) -> None:
    url, yard = loaded
    yard.forget(D, test_activity.day(21), test_activity.day(21))  # one ledger of D's effects is no longer verified
    with opened(url) as client:
        month = client.get("/admin/activity?period=month").text
        week = client.get("/admin/activity").text
    # In the month D is judged up to where its effects are verified; beyond that nothing is claimed.
    state, cell = assets_of(row_of(month, D))
    assert state == "partial" and "100 %" in cell and "свои 2 · чужие 0 · без актива 1" in cell
    assert "не оценено операций: 3" in cell and "оценено только до леджера " + num(test_activity.day(21) - 1) in cell
    # In the week none of it is verified from its creation: unknown, and why.
    state, cell = assets_of(row_of(week, D))
    assert state == "unknown" and ">неизвестно<" in cell and "не оценено операций: 2" in cell
    assert "эффекты аккаунта не проверены без пропусков" in cell
    assert assets_of(row_of(month, ISSUER))[0] == "classified", "the gap of one account touches no other"


def test_the_volume_of_data_is_known_once_the_planner_has_its_numbers(
    loaded: Installation, ledger_clock: list[datetime]
) -> None:
    url, yard = loaded
    with yard.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        connection.execute(text("ANALYZE"))  # as autovacuum gathers them
    expected = {row.account_id: row.volume_bytes for row in counting.table(yard.engine, now=LOADED, per_page=100).rows}
    assert all(value is not None and value > 0 for value in expected.values())
    with opened(url) as client:
        html = client.get("/admin/activity").text
    for account in test_activity.WATCHED:
        row = row_of(html, account)
        assert 'data-volume="known"' in row and page.size(expected[account]) in row, account
    assert "Объём неизвестен" not in html
    assert "примерно" in html and "объёмы складывать нельзя" in html


def test_the_speed_of_the_import_is_apart_from_the_activity_by_ledger_time(
    loaded: Installation, ledger_clock: list[datetime], statements: Statements
) -> None:
    url, yard = loaded
    with yard.engine.begin() as connection:
        for table, column in counting.IMPORTED:
            connection.execute(text(f"UPDATE {table} SET {column} = :at"), {"at": STORED})
    # By the time of the ledger there is activity in February — and nothing was stored in that day.
    with opened(url) as client:
        first = client.get("/admin/activity").text
        again = client.get("/admin/activity").text
    assert totals(first) == ["9", "10", "17", "1"]
    assert (figure(first, "speed.rows"), figure(first, "speed.rows_per_hour")) == ("0", "0")
    readings = len(statements.of("date_trunc('hour'"))
    assert readings == len(counting.IMPORTED), "each table read through once for the speed"
    assert speed_block(again) == speed_block(first), "kept in the cache for the next opening"
    assert len(statements.of("date_trunc('hour'")) == readings
    # A moment after the storing: no activity by the time of the ledger — and the speed shows what was stored.
    ledger_clock[0] = STORED + 2 * HOUR
    speed = counting.import_speed(yard.engine, now=ledger_clock[0])
    assert speed.rows == sum(table.rows for table in speed.tables.values()) > 0
    with opened(url) as client:
        html = client.get("/admin/activity").text
    assert totals(html) == ["0", "0", "0", "0"]
    assert (figure(html, "speed.rows"), figure(html, "speed.rows_per_hour")) == (num(speed.rows), act.decimal(speed.rows_per_hour))
    hours = dict(re.findall(r'data-hour="([^"]+)">([^<]*)</td>', html))
    assert len(hours) == 24 and hours == {written(hour.hour): num(hour.rows) for hour in speed.series}
    assert hours[written(STORED.replace(minute=0, second=0, microsecond=0))] == num(speed.rows)
    assert html.count('style="width: 100%"') == 1, "the busiest hour is the whole bar"
    said = plain(speed_block(html))
    assert "по времени сохранения" in said and "скорость импорта, другое, чем активность" in said
    assert "Последний час ещё не кончился" in said and "измерено " + written(ledger_clock[0]) in said


def test_an_account_is_switched_off_from_its_row_and_keeps_its_history_and_its_row(
    loaded: Installation, ledger_clock: list[datetime]
) -> None:
    url, yard = loaded
    asked = len(yard.fake.requests)
    history = yard.one("SELECT count(*) FROM history_transactions")
    of_alfa = yard.one("SELECT count(*) FROM history_transaction_participants WHERE account_id = :account", account=A)
    the_account = "SELECT enabled, reason FROM tracked_accounts WHERE account_id = :account"

    def disabled(token: str) -> Any:
        return client.post(f"/admin/activity/{A}/disable", data={"csrf": token}, follow_redirects=False)

    with opened(url) as client:
        shown = client.get("/admin/activity?sort=trades")
        alfa = row_of(shown.text, A)
        form = re.search(r'<form method="post" action="/admin/activity/' + re.escape(A) + r'/disable".*?</form>', alfa, re.S)
        assert form and 'data-confirm="Отключить наблюдение за ' in form.group(0) and "История остаётся" in form.group(0)
        assert 'name="reason"' in form.group(0) and 'name="back" value="/admin/activity?sort=trades"' in form.group(0)
        token = csrf_of(shown)
        # A form without its token changes nothing.
        assert client.post(f"/admin/activity/{A}/disable", data={"reason": "no token"}, follow_redirects=False).status_code == 403
        assert yard.rows(the_account, account=A) == [(True, "for the test")]
        # With it: the watch is switched off, and the owner comes back to what the page showed.
        done = client.post(
            f"/admin/activity/{A}/disable", data={"csrf": token, "reason": EVIL, "back": "/admin/activity?sort=trades"},
            follow_redirects=False,
        )
        notice = "Наблюдение за аккаунтом " + A[:6] + "… отключено. История осталась на месте. Причина: " + EVIL
        assert done.status_code == 303 and done.headers["location"].endswith("notice=" + quote_plus(notice))
        back = client.get(done.headers["location"])
        assert back.status_code == 200 and escape(EVIL) in back.text, "the reason comes back as it was written, escaped"
        alfa = row_of(back.text, A)
        assert 'data-enabled="off"' in alfa and ">отключён<" in alfa and "/disable" not in alfa
        assert counted(alfa, "operations") == ("5", "+3 (+150 %) · было 2"), "its history is still its activity"
        assert "/disable" in row_of(back.text, B), "another account is left as it was"
        assert accounts_in(client.get("/admin/activity?enabled=off").text) == [A]
        assert A not in accounts_in(client.get("/admin/activity?enabled=on").text)
        # Once more: nothing changes, and it is said. No new reason: the one it has is kept.
        token = csrf_of(client.get("/admin/activity"))
        twice = disabled(token)
        assert twice.status_code == 303 and quote_plus("уже был отключён") in twice.headers["location"]
        assert yard.rows(the_account, account=A) == [(False, EVIL)]
        # What is not watched cannot be switched off; what is not an address is refused.
        for address in (factories.address(10), "not-an-address"):
            answer = client.post(f"/admin/activity/{address}/disable", data={"csrf": token}, follow_redirects=False)
            assert answer.status_code == 403
        assert "не наблюдается" in client.post(f"/admin/activity/{factories.address(10)}/disable", data={"csrf": token}).text
        # A reason too long for a form is refused, and nothing changes.
        too_long = client.post(f"/admin/activity/{B}/disable", data={"csrf": token, "reason": "x" * (act.REASON_LIMIT + 1)}, follow_redirects=False)
        assert too_long.status_code == 403 and yard.one(the_account, account=B) is True
        # The form cannot send the owner anywhere but this page; the change is made all the same.
        away = client.post(
            f"/admin/activity/{B}/disable", data={"csrf": token, "back": "https://evil.example.org/admin"}, follow_redirects=False
        )
        assert away.status_code == 303 and away.headers["location"].startswith("/admin/activity?notice=")
        assert "evil.example.org" not in away.headers["location"]
        assert yard.rows(the_account, account=B) == [(False, "for the test")], "no reason given: the one it came with is kept"
        nowhere = client.post(f"/admin/activity/{B}/disable", data={"csrf": token, "back": ""}, follow_redirects=False)
        assert nowhere.status_code == 303 and nowhere.headers["location"].startswith("/admin/activity?notice=")
    assert len(yard.fake.requests) == asked, "nothing is asked of any server: what follows is the worker's"
    # The history stays, and nothing else is touched here.
    assert yard.rows(the_account, account=A) == [(False, EVIL)]
    assert yard.one("SELECT count(*) FROM history_transactions") == history
    assert yard.one("SELECT count(*) FROM history_transaction_participants WHERE account_id = :account", account=A) == of_alfa > 0
    assert yard.one("SELECT count(*) FROM tracked_accounts") == len(test_activity.WATCHED)
    assert yard.one("SELECT count(*) FROM watch_subscription") == 0 and yard.one("SELECT count(*) FROM history_job") == 0
    tells_nothing_it_must_not(back.text, url)


def test_a_page_that_runs_out_of_time_says_so_and_holds_nothing(
    loaded: Installation, ledger_clock: list[datetime], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(counting, "STATEMENT_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(
        counting, "_LEDGER_RANGE",
        text("SELECT min(sequence), max(sequence), pg_sleep(0.3) FROM history_ledgers WHERE close_time >= :start AND close_time < :end"),
    )
    url, _ = loaded
    with opened(url) as client:
        answer = client.get("/admin/activity")
    assert answer.status_code == 503
    assert "База не успела посчитать активность" in answer.text and "data-account=" not in answer.text
