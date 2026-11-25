"""The pages of the history import as a browser meets them: what every account
and every market came to, the queue and its service, one job with its streams
— read from the database alone; and every action of the page doing, behind the
form's token, what the command does."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from html.parser import HTMLParser
from typing import Any
from urllib.parse import unquote

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import insert, text
from sqlalchemy.orm import Session

from app.admin.areas import imports as area
from app.api.app import create_app
from app.backfill import config as watch_lists
from app.backfill.jobs import market_key
from app.config import PUBLIC_NETWORK_PASSPHRASE, Settings
from app.stats.imports import TooSlow
from app.storage import heartbeats, history_jobs, service_jobs
from app.storage.models import HistoryJob, HorizonNodeState
from app.tasks import kinds
from tests.factories import address
from tests.fake_horizon import A, B, C, D, EUR, ISSUER, MTL, RECT, S, FakeHorizon, close_time, serve
from tests.test_history_queue import (
    DONE,
    ERROR,
    MARKET,
    PAUSED,
    PARTIAL,
    RUNNING,
    STREAMS,
    WAITING,
    Yard,
    finish,
    ranges_of,
    verified,
)

SECRET = "a-secret-of-the-admin"
EVIL = "<script>alert('x')</script>"
EUR_MTL = market_key(EUR, MTL)
EUR_NATIVE = market_key(EUR, "native")
RECT_NATIVE = market_key(RECT, "native")
REFUSED = "trades: RequestFailed: Horizon answered 503 to GET /trades"
# Accounts the fake Horizon knows nothing of. Their jobs meet the worker only
# where the servers' state is what ends the slice — a 429, a pool that has not
# got as far — and are put in every other state by the queue's own word.
P1, P2, P3, P4, P5, P6, P7, NOJOB = (address(number) for number in range(51, 59))
ANY = ...  # an attribute that is there, whatever it says
POSTS = (
    "pause", "resume", "jobs/{id}/pause", "jobs/{id}/resume", "jobs/{id}/retry", "jobs/{id}/priority", "jobs/{id}/plan",
    "jobs/{id}/verify", "jobs/{id}/recover",
)


# -- reading a page ------------------------------------------------------------------------


class Leaf:
    """An element of a page as a test reads it: its attributes, and its text —
    with the text of all that is inside it, blanks squeezed."""

    def __init__(self, attrs: dict[str, str | None], parent: "Leaf | None" = None) -> None:
        self.attrs, self.parent, self.within, self.words = attrs, parent, [], []

    def text(self) -> str:
        return " ".join("".join(self.words).split())

    def matches(self, **where: Any) -> bool:
        return all(name in self.attrs and (wanted is ANY or self.attrs[name] == wanted) for name, wanted in where.items())

    def walk(self) -> Iterator["Leaf"]:
        yield self
        for leaf in self.within:
            yield from leaf.walk()

    def all(self, **where: Any) -> list["Leaf"]:
        """Every element inside this one, outermost first, that says ``where``."""
        found: list[Leaf] = []
        for leaf in self.within:
            if leaf.matches(**where):
                found.append(leaf)  # what is inside a found element is not found again
            else:
                found += leaf.all(**where)
        return found

    def has(self, **where: Any) -> bool:
        return bool(self.all(**where))

    def one(self, *where: dict[str, Any], **named: Any) -> "Leaf":
        """The element that says the first of ``where``; inside it one that
        says the next; and so on. What is named as keywords is said by the
        first element too."""
        steps = list(where) or [{}]
        steps[0] = {**named, **steps[0]}
        leaf = self
        for attrs in steps:
            leaf = next((inner for inner in leaf.walk() if inner.matches(**attrs)), None)  # type: ignore[assignment]
            assert leaf is not None, f"the page does not say {attrs}"
        return leaf

    def said(self, *where: dict[str, Any], **named: Any) -> str:
        return self.one(*where, **named).text()


class Reading(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.root = Leaf({})
        self.opened = [self.root]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        leaf = Leaf(dict(attrs), self.opened[-1])
        self.opened[-1].within.append(leaf)
        if tag not in ("area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"):
            self.opened.append(leaf)

    def handle_endtag(self, tag: str) -> None:
        if len(self.opened) > 1:
            self.opened.pop()

    def handle_data(self, data: str) -> None:
        for leaf in self.opened:
            leaf.words.append(data)


def read(page: Any) -> Leaf:
    assert page.status_code == 200, page.text
    parser = Reading()
    parser.feed(page.text)
    parser.close()
    return parser.root


# -- the queue, the service and the pages on one database -----------------------------------


class Pages:
    """A private database with the queue of history imports over a fake
    Horizon, and the admin UI on the same database."""

    def __init__(self, url: str, monkeypatch: pytest.MonkeyPatch) -> None:
        self.yard = Yard(url, FakeHorizon())
        # Whatever would build a pool reads this fake — and is counted by it.
        serve(monkeypatch, self.yard.fake)
        self.app = create_app(
            Settings(
                **{  # type: ignore[arg-type]
                    "database_url": self.yard.engine.url.render_as_string(hide_password=False),
                    "network_passphrase": PUBLIC_NETWORK_PASSPHRASE,
                    "admin_secret": SECRET,
                }
            )
        )

    @property
    def engine(self) -> Any:
        return self.yard.engine

    @property
    def fake(self) -> FakeHorizon:
        return self.yard.fake

    def close(self) -> None:
        self.yard.close()


@pytest.fixture
def pages(make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch) -> Iterator[Pages]:
    instance = Pages(make_database(), monkeypatch)
    yield instance
    instance.close()


class Browser:
    """A browser that is logged in, with the token of its forms."""

    def __init__(self, client: TestClient) -> None:
        self.client, self._token = client, None

    @property
    def token(self) -> str:
        if self._token is None:
            found = re.search(r'name="csrf" value="([0-9a-f]{64})"', self.client.get("/admin/import").text)
            assert found, "the page carries the form's token"
            self._token = found.group(1)
        return self._token

    def post(self, path: str, **fields: Any) -> Any:
        """A form sent as a browser sends it: with the token."""
        return self.client.post(path, data={"csrf": self.token, **fields}, follow_redirects=False)

    def send(self, path: str, **fields: Any) -> Any:
        """The same form, as sent by somebody who has no token."""
        return self.client.post(path, data=fields, follow_redirects=False)


@contextmanager
def opened(pages: Pages) -> Iterator[Browser]:
    with TestClient(pages.app) as client:
        assert client.post("/admin/login", data={"secret": SECRET}, follow_redirects=False).status_code == 303
        yield Browser(client)


def notice_of(answer: Any) -> str:
    return unquote(answer.headers["location"])


def kept(engine: Any) -> list[Any]:
    """What the actions change, as it is kept: to be compared before and after."""
    with engine.connect() as connection:
        jobs = connection.execute(
            text("SELECT id, state, priority, attempts, paused_because, from_ledger, to_ledger, slices FROM history_job ORDER BY id")
        ).all()
        pause = connection.execute(text("SELECT value FROM service_state WHERE name = 'history_paused'")).all()
        tasks = connection.execute(text("SELECT id, kind, state, params, one_at_a_time FROM service_job ORDER BY id")).all()
    return [*jobs, *pause, *tasks]


def the(engine: Any, job_id: int) -> HistoryJob:
    job = history_jobs.get(engine, job_id)
    assert job is not None
    return job


def agree(yard: Yard) -> None:
    """Move the yard's clock past the real now. A page writes the time of a
    next try by the real clock; the worker goes by the yard's."""
    yard.wall.now = datetime.now(UTC) + timedelta(seconds=30)


# -- the state of the import, as the queue and a worker leave it ----------------------------


def watch_market(yard: Yard, base: str, counter: str) -> str:
    """One more market on the watch list, switched on; its key."""
    lists = watch_lists.parse({"markets": [{"base": base, "counter": counter, "reason": "for the test"}]}, "test")
    with Session(yard.engine) as session, session.begin():
        watch_lists.apply(session, lists)
    return market_key(base, counter)


def craft(yard: Yard, kind: str, subject: str, outcome: str, error: str | None = None, **said: Any) -> HistoryJob:
    """A job put in a state by the queue itself, as a slice that ended so
    would have left it: planned, taken, its slice ended with ``outcome``. No
    server is asked. The job is to be the only one due."""
    job, _ = history_jobs.plan(yard.engine, kind=kind, subject=subject, to_ledger=130, now=yard.wall())
    taken = history_jobs.take(yard.engine, "craft", lease_seconds=60, now=yard.wall())
    assert taken is not None and taken.subject == subject, "the job planned now is the one taken now"
    history_jobs.slice_done(yard.engine, taken, outcome=outcome, error=error, max_attempts=3, now=yard.wall(), **said)
    return job


def slices_of(yard: Yard, job: HistoryJob, waits: tuple[int, ...]) -> HistoryJob:
    """Give the job one slice after each of ``waits``."""
    for wait in waits:
        yard.wall.wait(wait)
        assert yard.worker.once() is True
    return the(yard.engine, job.id)


def arranged(pages: Pages) -> dict[str, HistoryJob]:
    """Accounts and markets in every state there is, each for a reason of its
    own — read by the worker over the fake in small slices, or ended by the
    queue's own word; paused and prioritised by hand — with what the last
    check of the pool found. Returns the jobs, by a name for each."""
    yard, engine = pages.yard, pages.engine
    for who in (D, S, ISSUER, P1, P2, P3, P4, P5, P6, P7, NOJOB):
        yard.watch(who)
    watch_market(yard, EUR, MTL)
    watch_market(yard, EUR, "native")
    watch_market(yard, RECT, "native")
    read: dict[str, HistoryJob] = {}

    # Read by the worker in small slices. Each is the job that is due: what
    # waits for its next try is not due for minutes.
    yard.fake.fail("/trades", 503, times=1000)
    read["ISSUER"], _ = yard.plan(ISSUER)
    assert slices_of(yard, read["ISSUER"], (0, 31, 61)).state == PARTIAL, "a stream that keeps failing"
    yard.fake.fail(f"/accounts/{S}/", 503, times=1000)
    read["S"], _ = yard.plan(S)
    assert slices_of(yard, read["S"], (0, 31, 61)).state == ERROR, "nothing of it could be read"
    yard.fake.faults.clear()
    yard.fake.fail(f"/accounts/{P1}/effects", 429, times=1000, retry_after="100000")
    read["P1"], _ = yard.plan(P1)
    assert slices_of(yard, read["P1"], (0,)).state == WAITING, "a server asking to wait is not the job's failure"
    yard.fake.faults.clear()
    read["P2"], _ = yard.plan(P2, to_ledger=135)  # above the ledger the pool has got to
    assert slices_of(yard, read["P2"], (0,)).state == WAITING
    for name, who, options in (("A", A, {}), ("B", B, {"from_ledger": 90})):
        read[name], _ = yard.plan(who, **options)
        assert finish(yard.worker) >= 1 and the(engine, read[name].id).state == DONE
    for name, pair, options in (("MARKET", MARKET, {}), ("RECT_NATIVE", RECT_NATIVE, {"boundary_final": True})):
        read[name], _ = history_jobs.plan(engine, kind="market", subject=pair, to_ledger=130, now=yard.wall(), **options)
        assert finish(yard.worker) == 1 and the(engine, read[name].id).state == DONE
    yard.fake.fail("/trades", 503, times=1000)
    read["EUR_NATIVE"], _ = history_jobs.plan(engine, kind="market", subject=EUR_NATIVE, to_ledger=130, now=yard.wall())
    assert slices_of(yard, read["EUR_NATIVE"], (0, 31, 61)).state == ERROR
    yard.fake.faults.clear()

    # One page of one record is read, and the job is then held, as a worker
    # holds it.
    yard.worker = yard.new_worker("sliced", page_size=1, slice_requests=1)
    read["D"], _ = yard.plan(D)
    assert yard.worker.once() is True
    held = history_jobs.take(engine, "holder", lease_seconds=600, now=yard.wall())
    assert held is not None and held.subject == D
    yard.worker = yard.new_worker("whole")

    # Ended by the queue's own word, while each is the only job that is due:
    # what it takes an outage, or a silent server, to leave.
    read["P3"] = craft(
        yard, "account", P3, history_jobs.DEFERRED,
        "ledgers below 100 are not read: https://deep.test did not say of itself just now and may keep them", delay=120,
    )
    read["P4"] = craft(
        yard, "account", P4, history_jobs.DEFERRED, "Unavailable: no server of the pool answers: https://a.test is down", delay=120
    )
    read["P5"] = craft(yard, "account", P5, history_jobs.FAILED, REFUSED, delay=30)
    read["P6"] = craft(yard, "account", P6, history_jobs.PROGRESS, result={"window": [100, 130]})
    assert history_jobs.set_priority(engine, priority=2, ids=[read["P6"].id]) == 1

    read["P7"], _ = yard.plan(P7)
    assert history_jobs.pause(engine, ids=[read["P7"].id], because="maintenance") == 1
    read["C"], _ = yard.plan(C)  # on the watch list, switched off
    assert history_jobs.pause(engine, ids=[read["C"].id], because="account disabled") == 1
    read["EUR_MTL"], _ = history_jobs.plan(engine, kind="market", subject=EUR_MTL, to_ledger=130, now=yard.wall())

    # What the last check of the pool found: two servers, one of them down.
    now = datetime.now(UTC)
    with Session(engine) as session, session.begin():
        session.add(
            HorizonNodeState(
                url="https://a.test", position=1, available=True, elder_ledger=102, latest_ledger=130, checked_at=now - timedelta(minutes=5)
            )
        )
        session.add(
            HorizonNodeState(
                url="https://b.test", position=2, available=False, elder_ledger=110, latest_ledger=128, lag_ledgers=2,
                last_error="connection refused", checked_at=now - timedelta(hours=1),
            )
        )
    return read


def rows(tree: Leaf) -> list[Leaf]:
    return tree.all(**{"data-job": ANY})


# -- the pages that only show ----------------------------------------------------------------


def test_the_list_shows_accounts_and_markets_apart_and_a_job_in_every_state(pages: Pages) -> None:
    jobs = arranged(pages)
    heartbeats.beat(pages.engine, heartbeats.HISTORY, name="worker-1", state="idle", job_id=None, slices_done=9, slices_failed=6)
    with opened(pages) as browser:
        asked = len(pages.fake.requests)
        tree = read(browser.client.get("/admin/import"))
        for job in jobs.values():
            read(browser.client.get(f"/admin/import/jobs/{job.id}"))
        assert len(pages.fake.requests) == asked, "no page asks any server"
    assert "Цифры измерены" in tree.text() and "UTC" in tree.text()

    accounts = tree.one({"data-kind": "account"})

    def account(figure: str) -> str:
        """What the accounts say under a figure: its label, its number, and what is under it."""
        return tree.one({"data-kind": "account"}, {"data-figure": figure}).parent.text()

    assert account("subjects") == "Нужна история 13 с заданием: 12 · без задания: 1", "every switched-on account, with a job or without"
    assert account("finished").startswith("Завершили доступное окно 2 из них без недоступного: 1; с недоступными леджерами: 1"), (
        "how many finished the window that was available, and how many have ledgers that were not"
    )
    assert "всего таких леджеров: 10" in accounts.text()
    assert account("not_final").startswith("Граница ещё не окончательная 12")
    assert account("share").startswith("Доля завершённых — доля аккаунтов, не записей 15%"), "a share of accounts: 2 of 13"
    assert "2 из 13" in account("share"), "the denominator is named"
    for state, number in ((WAITING, 6), (RUNNING, 1), (PAUSED, 1), (DONE, 2), (PARTIAL, 1), (ERROR, 1)):
        said = tree.said({"data-kind": "account"}, {"data-state": state})
        assert said.startswith(state) and said.endswith(str(number)), (state, said)
    for code, number in (
        ("global_pause", 0), ("servers_cannot_answer", 1), ("rate_limited", 1), ("pool_silent", 1),
        ("pool_behind", 1), ("backoff_after_failure", 1), ("its_turn", 1),
    ):
        assert tree.said({"data-kind": "account"}, {"data-reason": code}).endswith(str(number)), code
    assert "maintenance: 1" in tree.said({"data-kind": "account"}, {"data-paused-reasons": "account"})
    assert "paused 1" in accounts.text(), "the job of the switched-off account is not counted among the subjects"

    # And the markets on their own: never one percentage with the accounts.
    markets = tree.one({"data-kind": "market"})

    def market(figure: str) -> str:
        return tree.one({"data-kind": "market"}, {"data-figure": figure}).parent.text()

    assert market("subjects") == "Нужна история 4 с заданием: 4 · без задания: 0"
    assert market("share").startswith("Доля завершённых — доля рынков, не записей 50%") and "2 из 4" in market("share")
    assert market("finished").startswith("Завершили доступное окно 2")
    for state, number in ((WAITING, 1), (DONE, 2), (ERROR, 1)):
        said = tree.said({"data-kind": "market"}, {"data-state": state})
        assert said.startswith(state) and said.endswith(str(number)), (state, said)
    assert "одного общего процента" in tree.text()

    # The pool as its last check saw it, and the windows.
    assert tree.one({"data-part": "pool"}, {"data-figure": "elder"}).parent.text().startswith("Насколько глубоко хранит пул 102")
    assert tree.one({"data-part": "pool"}, {"data-figure": "latest"}).parent.text().startswith("Последний леджер пула 130")
    pool = tree.one({"data-part": "pool"})
    assert "Horizon не спрашивался" in pool.text() and "Retention движется" in pool.text()
    assert "https://b.test" in tree.said({"data-part": "pool"}, {"data-servers": "unavailable"})
    assert len(pool.all(**{"data-server": ANY})) == 2
    window = pool.one({"data-window": "account"}).text()
    assert "135" in window and "90" in window, "the lowest and the highest ledger of the accounts' windows"

    # The table: what is active first, and every job with the reason of its state.
    assert [leaf.attrs["data-state"] for leaf in rows(tree)] == [RUNNING] + [WAITING] * 7 + [PAUSED] * 2 + [PARTIAL] + [ERROR] * 2 + [DONE] * 4
    by_name = {name: tree.one({"data-job": str(job.id)}) for name, job in jobs.items()}
    for name, job in jobs.items():
        row = by_name[name]
        assert row.one(**{"href": f"/admin/import/jobs/{job.id}"}), f"job {name} links to its page"
        assert row.one(**{"data-copy": job.subject}), f"the subject of job {name} is copyable"
    assert {name: row.attrs["data-reason"] for name, row in by_name.items()} == {
        "D": "working", "P1": "rate_limited", "P2": "pool_behind", "P3": "pool_silent", "P4": "servers_cannot_answer",
        "P5": "backoff_after_failure", "P6": "its_turn", "EUR_MTL": "its_turn", "P7": "paused", "C": "paused",
        "ISSUER": "gave_up", "S": "gave_up", "EUR_NATIVE": "gave_up", "A": "finished_boundary_not_final",
        "B": "finished_boundary_not_final", "MARKET": "finished_boundary_not_final", "RECT_NATIVE": "finished",
    }
    held = by_name["D"].text()
    assert "holder" in held and "112" in held, "who reads it, and where it got to"
    assert close_time(112).strftime("%Y-%m-%d %H:%M:%S UTC") in held, "the time of the ledger it is at, when known"
    assert "приоритет 2" in by_name["P6"].text()
    assert "maintenance" in by_name["P7"].text() and "аккаунт выключен" in by_name["C"].text()
    assert REFUSED in by_name["P5"].text(), "the error as it was kept"
    assert "parents" in by_name["A"].text() and "граница не окончательная" in by_name["A"].text()
    assert "граница окончательная" in by_name["RECT_NATIVE"].text()
    assert "transactions: 100–130" in by_name["B"].text() and "все потоки проверены" in by_name["B"].text()
    assert "окно не определено" in by_name["EUR_MTL"].text() and "? – 130" in by_name["EUR_MTL"].text()
    assert "135" in by_name["P2"].text(), "the top of its window, above what the pool has got to"
    assert "https://horizon.test" in by_name["MARKET"].text(), "the servers that read it"
    for name in ("D", "P1", "P2", "P3", "P4", "P5", "P6", "EUR_MTL", "P7", "C", "ISSUER", "S", "EUR_NATIVE"):
        assert by_name[name].one(**{"class": "why", "href": f"/admin/import/jobs/{jobs[name].id}"}), f"from {name}'s reason to its page"
    assert by_name["P7"].one(**{"action": f"/admin/import/jobs/{jobs['P7'].id}/resume"})
    assert by_name["ISSUER"].one(**{"action": f"/admin/import/jobs/{jobs['ISSUER'].id}/retry"})

    # The queue goes; the service is alive by its last word — an empty queue
    # is not mistaken for a stopped service; and the pause asks for a reason.
    assert tree.all(**{"data-paused": "no"}) and "очередь идёт" in tree.said({"data-paused": "no"})
    alive = tree.one({"data-service": "alive"}).text()
    assert "жива" in alive and "worker-1" in alive
    form = tree.one(**{"action": "/admin/import/pause", "data-confirm": ANY})
    assert form.one(**{"name": "csrf"}) and form.one(**{"name": "because"})
    queue = tree.one(**{"data-part": "queue"}).text()
    assert "переживает перезапуск" in queue and "живой сбор событий (worker) она не останавливает" in queue


def test_at_a_thousand_subjects_the_table_is_turned_by_pages_filtered_and_asks_nothing(pages: Pages) -> None:
    """A thousand subjects, none of them read: the page shows fifty at a time,
    finds one by a part of its name — and asks no server while the queue is
    idle."""
    yard, engine = pages.yard, pages.engine
    with engine.begin() as connection:
        connection.execute(
            insert(HistoryJob),
            [
                {"kind": "account", "subject": address(1000 + number), "state": WAITING, "to_ledger": 130, "next_attempt_at": yard.wall()}
                for number in range(1000)
            ],
        )
    a_market, _ = history_jobs.plan(engine, kind="market", subject=MARKET, to_ledger=130, now=yard.wall())
    assert a_market.id == 1001, "the thousand planned are the first thousand"
    with opened(pages) as browser:
        before = kept(engine)
        first, twentieth, last, beyond, markets, none, found, nonsense = (
            read(browser.client.get(path))
            for path in (
                "/admin/import", "/admin/import?page=20", "/admin/import?page=21", "/admin/import?page=22", "/admin/import?kind=market",
                "/admin/import?state=error", f"/admin/import?q={address(1303)[-12:]}", "/admin/import?kind=asset&state=finished&page=nothing",
            )
        )
        assert kept(engine) == before, "looking changes nothing"
    assert pages.fake.requests == [], "no page asks any server"

    def ids(tree: Leaf) -> list[str]:
        return [leaf.attrs["data-job"] for leaf in rows(tree)]

    assert ids(first) == [str(number) for number in range(1, 51)], "fifty on a page, however many there are"
    assert "страница 1 из 21 · 1 001" in first.one(**{"class": "pager"}).text()
    assert first.one(**{"href": "/admin/import?page=2"})
    assert ids(twentieth) == [str(number) for number in range(951, 1001)]
    assert ids(last) == [str(a_market.id)] and last.one(**{"href": "/admin/import?page=20"})
    assert rows(beyond) == [] and "Заданий с такими условиями нет" in beyond.text()
    assert ids(markets) == [str(a_market.id)]
    assert rows(none) == []
    assert ids(found) == ["304"], "a subject is found by a part of it"
    assert ids(nonsense) == ids(first), "what is no filter is dropped, not an error"


def test_a_silent_service_is_shown_as_not_running_and_a_fresh_one_as_alive(pages: Pages) -> None:
    job, _ = pages.yard.plan(A)
    with opened(pages) as browser:
        never = read(browser.client.get("/admin/import"))
        assert [leaf.attrs["data-service"] for leaf in never.all(**{"data-service": ANY})] == ["never"]
        assert "не запущена" in never.said({"data-service": "never"})
        heartbeats.beat(
            pages.engine, heartbeats.HISTORY, now=datetime.now(UTC) - timedelta(minutes=5), name="worker-1", state="idle", job_id=None
        )
        silent = read(browser.client.get("/admin/import"))
        said = silent.said({"data-service": "stale"})
        assert "не работает или зависла" in said and "не слышно 5 мин" in said
        assert not silent.all(**{"data-service": "alive"})
        heartbeats.beat(pages.engine, heartbeats.HISTORY, name="live", state="working", job_id=job.id, slices_done=3, slices_failed=1)
        alive = read(browser.client.get("/admin/import")).one({"data-service": "alive"})
        assert alive.text().startswith("жива — читает задание") and alive.one(**{"href": f"/admin/import/jobs/{job.id}"})
    assert pages.fake.requests == []


def test_one_job_shows_its_streams_their_shares_and_what_was_read_apart_from_what_was_stored(pages: Pages) -> None:
    jobs = arranged(pages)
    b = the(pages.engine, jobs["B"].id)
    with opened(pages) as browser:
        asked = len(pages.fake.requests)
        tree = read(browser.client.get(f"/admin/import/jobs/{b.id}"))
        fresh = read(browser.client.get(f"/admin/import/jobs/{jobs['EUR_MTL'].id}"))
        market = read(browser.client.get(f"/admin/import/jobs/{jobs['MARKET'].id}"))
        assert len(pages.fake.requests) == asked
    assert b.state == DONE and b.records > 0
    assert [leaf.attrs["data-stream"] for leaf in tree.all(**{"data-stream": ANY})] == list(STREAMS), "the four streams of an account"
    transactions = tree.one({"data-stream": "transactions"})
    assert "75%: 31 из 41" in tree.said({"data-stream": "transactions"}, {"data-share": "known"})
    assert "проверенные леджеры потока" in transactions.text() and "поделены на леджеры окна" in transactions.text()
    gone = transactions.one({"data-state": "unverifiable"})
    assert gone.text().startswith("90 99 unverifiable"), "the ledgers it lies over, and its state"
    assert "source keeps history from ledger 100" in gone.text(), "why it cannot be verified is kept"
    verified_ = transactions.one({"data-state": "complete"})
    assert verified_.text().startswith("100 130 complete") and "https://horizon.test" in verified_.text(), "and who served it"
    assert "90–99" in tree.said({"data-stream": "transactions"}, {"data-gaps": "unverifiable"})
    assert "нет" in tree.said({"data-stream": "transactions"}, {"data-gaps": "pending"})
    assert "там, где кончается проверенное" in transactions.text(), "a finished stream stands where its verified coverage ends"

    share = tree.one({"data-part": "share"}).text()
    assert "Доля задания: 75%" in share and "31 из 41 леджеров окна" in share and "проверенные в каждом потоке" in share

    # Read apart from stored, and met again apart from both: no ETA anywhere.
    counters = tree.one({"data-part": "counters"}).text()
    was_read = tree.one({"data-counters": "read"}).text()
    assert was_read.startswith(f"Прочитано у пула {b.records}")
    assert was_read.endswith(f"записей; запросов {b.requests}; страниц {b.pages}")
    inserted = tree.one({"data-counters": "inserted"}).text()
    assert inserted.startswith(f"Сохранено: новых записей {sum(b.inserted.values())}") and "транзакции" in inserted
    assert tree.one({"data-counters": "repeated"}).text().startswith(f"Встречено снова: уже было сохранено {sum(b.repeated.values())}")
    speed = f"{round(b.records / (b.time_spent_ms / 60_000), 1):,}".replace(",", " ")
    assert f"Скорость: {speed} записей" in counters and "страниц в минуту" in counters
    assert "Оценки времени окончания здесь нет" in counters and "ETA" not in tree.text()
    assert "done значит: доступное окно проверено — не вся жизнь адреса" in tree.text()

    # A job that has not read: no window, no share — said so, with the reason.
    assert [leaf.attrs["data-stream"] for leaf in fresh.all(**{"data-stream": ANY})] == ["trades"]
    unknown = fresh.one({"data-part": "share"}).text()
    assert "Доля задания: неизвестно" in unknown and "окно ещё не определено" in unknown
    assert "неизвестно" in fresh.said({"data-stream": "trades"}, {"data-share": "unknown"})
    # A market: one stream, the whole window verified.
    assert "Доля задания: 100%" in market.one({"data-part": "share"}).text()
    assert "100%: 31 из 31" in market.said({"data-stream": "trades"}, {"data-share": "known"})


# -- what the pages change ------------------------------------------------------------------


def test_one_job_is_paused_and_resumed_through_the_page_and_a_reason_is_escaped(pages: Pages) -> None:
    yard, engine = pages.yard, pages.engine
    failing = craft(yard, "account", S, history_jobs.FAILED, EVIL, delay=30)  # crafted while it is the only job
    done = craft(yard, "account", B, history_jobs.FINISHED, result={"window": [100, 130]})
    job, _ = yard.plan(A)
    with opened(pages) as browser:
        where = f"/admin/import/jobs/{job.id}"
        no_reason = browser.post(f"{where}/pause")
        assert no_reason.status_code == 403 and "причину паузы" in no_reason.text and the(engine, job.id).state == WAITING
        answer = browser.post(f"{where}/pause", because=f"  {EVIL}  ")
        assert answer.status_code == 303 and notice_of(answer).startswith(f"{where}?notice=Задание {job.id} на паузе")
        paused = the(engine, job.id)
        assert (paused.state, paused.paused_because) == (PAUSED, EVIL), "what the command keeps, with the reason"
        detail, listing = browser.client.get(where), browser.client.get("/admin/import")
        assert "&lt;script&gt;" in detail.text and "<script>alert" not in detail.text
        assert "&lt;script&gt;" in listing.text and "<script>alert" not in listing.text
        # What a browser shows is the reason itself — as text.
        assert EVIL in read(detail).one({"data-part": "reason"}).text()
        assert EVIL in read(listing).one({"data-job": str(job.id)}).text()
        assert EVIL in read(listing).one({"data-job": str(failing.id)}).text(), "and the error of a job, as it was kept"
        twice = browser.post(f"{where}/pause", because="once more")
        assert twice.status_code == 303 and the(engine, job.id).paused_because == "once more", "a paused job takes the reason given now"
        resumed = browser.post(f"{where}/resume")
        assert resumed.status_code == 303 and "снова в очереди" in notice_of(resumed)
        back = the(engine, job.id)
        assert (back.state, back.attempts, back.paused_because) == (WAITING, 0, None), "its tries start over"
        late = browser.post(f"{where}/resume")
        assert late.status_code == 303 and "не на паузе" in notice_of(late), "nothing changed the second time"
        finished = browser.post(f"/admin/import/jobs/{done.id}/pause", because="why")
        assert finished.status_code == 303 and "уже готово" in notice_of(finished) and the(engine, done.id).state == DONE
        nothing = browser.post(f"{where}/retry")
        assert nothing.status_code == 303 and "повторять нечего" in notice_of(nothing)


def test_a_job_that_gave_up_is_retried_prioritised_and_planned_again_through_the_page(pages: Pages) -> None:
    yard, engine = pages.yard, pages.engine
    yard.fake.fail("/trades", 503, times=1000)
    partial, _ = yard.plan(B)
    assert slices_of(yard, partial, (0, 31, 61)).state == PARTIAL
    yard.fake.faults.clear()
    where = f"/admin/import/jobs/{partial.id}"
    with opened(pages) as browser:
        retried = browser.post(f"{where}/retry")
        assert retried.status_code == 303
        again = the(engine, partial.id)
        assert (again.state, again.attempts) == (WAITING, 0), "its tries start over"
        agree(yard)
        assert finish(yard.worker) >= 1 and the(engine, partial.id).state == DONE, "what was verified is not read again"
        twice = browser.post(f"{where}/retry")
        assert twice.status_code == 303 and "повторять нечего" in notice_of(twice)

        # The priority: one of 0 to 3, and nothing else of the job is touched.
        refused = browser.post(f"{where}/priority", priority="7")
        assert refused.status_code == 403 and the(engine, partial.id).priority == 0
        raised = browser.post(f"{where}/priority", priority="3")
        assert raised.status_code == 303 and the(engine, partial.id).priority == 3
        assert "30 мин" in notice_of(raised) and "остальные всё равно получают свои" in notice_of(raised)
        plain = browser.post(f"{where}/priority", priority="0")
        assert plain.status_code == 303 and the(engine, partial.id).priority == 0

        # Planning again only widens — and never starts over.
        given, _ = yard.plan(A, to_ledger=125, from_ledger=110)
        assert finish(yard.worker) >= 1 and the(engine, given.id).state == DONE
        before = the(engine, given.id)
        coverage = ranges_of(engine, "account", A)
        widened = browser.post(f"/admin/import/jobs/{given.id}/plan", to_ledger="125", from_ledger="100")
        assert widened.status_code == 303 and "Прогресс не сброшен" in notice_of(widened)
        after = the(engine, given.id)
        assert (after.from_ledger, after.to_ledger, after.state, after.slices) == (100, 125, WAITING, before.slices), (
            "a job that had nothing left goes back to the queue"
        )
        assert (after.records, after.pages) == (before.records, before.pages) and ranges_of(engine, "account", A) == coverage

        # A paused job stays paused, its window widened all the same.
        paused = browser.post(f"/admin/import/jobs/{given.id}/pause", because="later")
        assert paused.status_code == 303 and the(engine, given.id).state == PAUSED
        again = browser.post(f"/admin/import/jobs/{given.id}/plan", to_ledger="130")
        assert again.status_code == 303 and "осталось на паузе" in notice_of(again)
        held = the(engine, given.id)
        assert (held.state, held.from_ledger, held.to_ledger, held.slices) == (PAUSED, 100, 130, before.slices)
        assert yard.worker.once() is False, "a paused job is not worked on, however wide its window"

        # What is not a window is refused, and nothing moves.
        plan = f"/admin/import/jobs/{given.id}/plan"
        upside = browser.post(plan, to_ledger="110", from_ledger="120")
        assert upside.status_code == 403 and "Нижняя граница (120) выше верхней (110)" in upside.text
        words = browser.post(plan, to_ledger="later")
        assert words.status_code == 403 and "номер леджера" in words.text
        inside = browser.post(plan, to_ledger="120")
        assert inside.status_code == 303 and "Окно не изменилось" in notice_of(inside)
        still = the(engine, given.id)
        assert (still.state, still.from_ledger, still.to_ledger) == (PAUSED, 100, 130)

        # Resumed, it goes on from where it stood: the new ledgers are read,
        # and what was verified is not read again.
        browser.post(f"/admin/import/jobs/{given.id}/resume")
        agree(yard)
        assert finish(yard.worker) >= 1 and the(engine, given.id).state == DONE
        assert verified(ranges_of(engine, "account", A), 100, 130)
        assert the(engine, given.id).records > before.records


def test_a_comparison_or_a_new_reading_is_queued_not_run_and_one_for_a_subject(pages: Pages) -> None:
    yard, engine = pages.yard, pages.engine
    job, _ = yard.plan(B)
    assert finish(yard.worker) >= 1
    where = f"/admin/import/jobs/{job.id}"
    with opened(pages) as browser:
        asked = len(pages.fake.requests)
        before = kept(engine)
        upside = browser.post(f"{where}/verify", from_ledger="130", to_ledger="100")
        words = browser.post(f"{where}/verify", from_ledger="later", to_ledger="130")
        nothing = browser.post(f"{where}/verify", from_ledger="100", to_ledger="130", max_requests="0")
        for answer in (upside, words, nothing):
            assert answer.status_code == 403, answer.text
        assert kept(engine) == before, "what is refused puts nothing in the queue"

        answer = browser.post(f"{where}/verify", from_ledger="100", to_ledger="130", max_requests="50")
        assert answer.status_code == 303
        task = service_jobs.recent(engine, kind="verify", limit=1)[0]
        assert notice_of(answer).startswith(f"{where}?notice=Сверка с пулом поставлена в очередь: задача {task.id} (/admin/tasks/{task.id})")
        assert task.state == "queued" and task.one_at_a_time == B, "one such task of a subject at a time"
        assert task.params == {"kind": "account", "subject": B, "from_ledger": 100, "to_ledger": 130, "mode": "verify", "max_requests": 50}
        assert len(pages.fake.requests) == asked, "queued, not run"
        # The page names the task and links to it.
        detail = read(browser.client.get(answer.headers["location"]))
        assert detail.one(**{"href": f"/admin/tasks/{task.id}"})
        said = detail.one({"data-task": str(task.id)}).text()
        assert "Сверка с пулом" in said and "queued" in said and "100–130" in said
        assert "Чего она не сравнивает" in detail.text() and "контекст каждой транзакции" in detail.text()

        # One at a time for a subject: a second comparison, or a new reading,
        # adds nothing — and gives back the task that is there.
        for answer in (
            browser.post(f"{where}/verify", from_ledger="100", to_ledger="125"),
            browser.post(f"{where}/recover", from_ledger="100", to_ledger="125"),
        ):
            assert answer.status_code == 303 and f"уже стоит в очереди или идёт: задача {task.id}" in notice_of(answer)
            assert "Ничего не добавлено" in notice_of(answer)
        assert [kept_.id for kept_ in service_jobs.recent(engine, kind="verify")] == [task.id]

        # Ended: the way is free for a new reading, with the usual budget.
        running = service_jobs.take(engine, "runner", lease_seconds=60, now=datetime.now(UTC))
        assert running is not None and running.id == task.id
        service_jobs.finish(engine, running, result={"mode": "verify", "matches": True}, now=datetime.now(UTC))
        answer = browser.post(f"{where}/recover", from_ledger="100", to_ledger="130")
        fresh = service_jobs.recent(engine, kind="verify", limit=1)[0]
        assert answer.status_code == 303 and fresh.id != task.id
        assert (fresh.state, fresh.one_at_a_time) == ("queued", B)
        assert fresh.params == {
            "kind": "account", "subject": B, "from_ledger": 100, "to_ledger": 130, "mode": "recover", "max_requests": kinds.VERIFY_BUDGET,
        }
        assert "Перечитывание окна поставлено в очередь" in notice_of(answer)
        detail = read(browser.client.get(answer.headers["location"]))
        said = detail.one({"data-task": str(task.id)}).text()
        assert "совпадает: да" in said, "what a comparison came to"
        assert len(pages.fake.requests) == asked, "nothing was read by a request"
        assert the(engine, job.id).state == DONE, "the job itself is not touched"

        # While the window is read again, the page wakes nothing: a job set
        # going would be read by the worker and by the task at once.
        page = read(browser.client.get(where))
        assert page.one({"data-reading": str(fresh.id)})
        actions = page.one(**{"data-part": "actions"})
        assert not actions.has(action=f"{where}/resume") and not actions.has(action=f"{where}/retry")
        assert page.one(**{"data-part": "plan"}).has(**{"data-not-woken": "yes"})
        for refused in (
            browser.post(f"{where}/resume"), browser.post(f"{where}/retry"), browser.post(f"{where}/plan", to_ledger="140"),
        ):
            assert refused.status_code == 403 and f"Окно этого субъекта перечитывается (задача {fresh.id})" in refused.text
        assert the(engine, job.id).to_ledger == 130
        running = service_jobs.take(engine, "runner", lease_seconds=60, now=datetime.now(UTC))
        assert running is not None and running.id == fresh.id
        service_jobs.finish(engine, running, result={"mode": "recover", "read_again": True}, now=datetime.now(UTC))
        allowed = browser.post(f"{where}/plan", to_ledger="125")
        assert allowed.status_code == 303, "ended: nothing holds the job any more (the window did not change, is all)"


def test_a_window_is_read_again_only_of_a_job_the_queue_does_not_take(pages: Pages) -> None:
    """A new reading and a slice of the queue's job over the same streams are
    serialised nowhere: two readers would move one checkpoint each their own
    way. The page keeps them apart — a window is read again only of a job the
    queue does not take, and such a job is not woken meanwhile."""
    yard, engine = pages.yard, pages.engine
    job, _ = yard.plan(A)
    where = f"/admin/import/jobs/{job.id}"
    with opened(pages) as browser:
        held = history_jobs.take(engine, "somebody", lease_seconds=600, now=yard.wall())
        assert held is not None and held.id == job.id, "the job is in a worker's hands"
        refused = browser.post(f"{where}/recover", from_ledger="100", to_ledger="130")
        assert refused.status_code == 403 and "сначала поставьте его на паузу" in refused.text
        assert service_jobs.recent(engine, kind="verify") == []
        assert read(browser.client.get(where)).one(**{"data-reading-refused": "yes"}), "and the page says why the form is closed"
        compared = browser.post(f"{where}/verify", from_ledger="100", to_ledger="130")
        assert compared.status_code == 303, "a comparison stores nothing: it is asked of a job in hand all the same"

        # Paused by the page — and then the window may be read again.
        running = service_jobs.take(engine, "runner", lease_seconds=60, now=datetime.now(UTC))
        assert running is not None and running.params["mode"] == "verify"
        service_jobs.finish(engine, running, result={"mode": "verify", "matches": True}, now=datetime.now(UTC))
        browser.post(f"{where}/pause", because="reading the window again")
        assert the(engine, job.id).state == PAUSED
        assert not read(browser.client.get(where)).has(**{"data-reading-refused": "yes"})
        again = browser.post(f"{where}/recover", from_ledger="100", to_ledger="130")
        task = service_jobs.recent(engine, kind="verify", limit=1)[0]
        assert again.status_code == 303 and (task.state, task.params["mode"]) == ("queued", "recover")

        page = read(browser.client.get(where))
        assert page.one({"data-reading": str(task.id)}) and "возобновляют вручную" in page.text()
        actions = page.one(**{"data-part": "actions"})
        assert not actions.has(action=f"{where}/resume") and actions.has(**{"data-not-woken": "yes"})
        for refused in (
            browser.post(f"{where}/resume"), browser.post(f"{where}/retry"), browser.post(f"{where}/plan", to_ledger="140"),
        ):
            assert refused.status_code == 403 and f"перечитывается (задача {task.id})" in refused.text
        still = the(engine, job.id)
        assert (still.state, still.to_ledger) == (PAUSED, 130)

        # Ended: the job is resumed by hand, and goes back to the queue.
        running = service_jobs.take(engine, "runner", lease_seconds=60, now=datetime.now(UTC))
        assert running is not None and running.id == task.id
        service_jobs.finish(engine, running, result={"mode": "recover", "read_again": True}, now=datetime.now(UTC))
        resumed = browser.post(f"{where}/resume")
        assert resumed.status_code == 303 and the(engine, job.id).state == WAITING
    assert pages.fake.requests == [], "nothing was read by a request"


def test_the_pause_of_the_whole_queue_is_set_and_lifted_through_the_page_and_outlives_an_engine(pages: Pages) -> None:
    yard, engine = pages.yard, pages.engine
    job, _ = yard.plan(B)
    with opened(pages) as browser:
        no_reason = browser.post("/admin/import/pause")
        assert no_reason.status_code == 403 and "причину паузы" in no_reason.text and not history_jobs.is_paused(engine)
        answer = browser.post("/admin/import/pause", because=f"  {EVIL}  ")
        assert answer.status_code == 303 and notice_of(answer).startswith("/admin/import?notice=Вся очередь на паузе")
        state = history_jobs.pause_state(engine)
        assert (state["paused"], state["because"], state["by"]) == (True, EVIL, "admin"), "what the command keeps"
        listing = browser.client.get(answer.headers["location"])
        tree = read(listing)
        assert "&lt;script&gt;" in listing.text and "<script>alert" not in listing.text
        queue = tree.one(**{"data-part": "queue"}).text()
        assert EVIL in queue and "поставил: admin" in queue, "who paused, and why, as it is kept"
        assert tree.one({"data-paused": "yes"}).text().startswith("вся очередь на паузе с")
        assert tree.one(**{"action": "/admin/import/resume"})
        assert tree.one({"data-kind": "account"}).one({"data-reason": "global_pause"}).text().endswith("1"), "what every waiting job waits for"
        assert tree.one({"data-job": str(job.id)}).attrs["data-reason"] == "global_pause"
        assert the(engine, job.id).state == WAITING, "no job is touched"
        assert yard.worker.once() is False and pages.fake.requests == [], "and nothing is taken"

        # Another engine, as after a restart: the pause is still there.
        yard.restart()
        assert history_jobs.pause_state(pages.engine)["paused"] and pages.fake.requests == []
        assert read(browser.client.get("/admin/import")).one({"data-paused": "yes"})
        lifted = browser.post("/admin/import/resume")
        assert lifted.status_code == 303 and history_jobs.pause_state(pages.engine)["paused"] is False
        after = read(browser.client.get(lifted.headers["location"]))
        going = after.one({"data-paused": "no"}).text()
        assert "очередь идёт" in going and EVIL in going, "what the last pause said stays, with who lifted it"
        assert after.one({"data-kind": "account"}).one({"data-reason": "its_turn"})
        assert yard.worker.once() is True, "the worker takes a job again"

        twice = browser.post("/admin/import/resume")
        assert twice.status_code == 303 and "и так не на паузе" in notice_of(twice)


def test_without_the_form_s_token_no_post_changes_anything(pages: Pages) -> None:
    job, _ = pages.yard.plan(A)
    with opened(pages) as browser:
        before = kept(pages.engine)
        for path in POSTS:
            fields = {"because": "a reason", "priority": "3", "from_ledger": "100", "to_ledger": "140", "max_requests": "5"}
            stranger = browser.send(f"/admin/import/{path.format(id=job.id)}", **fields)
            forged = browser.send(f"/admin/import/{path.format(id=job.id)}", csrf="0" * 64, **fields)
            assert stranger.status_code == 403 and "токен" in stranger.text, path
            assert forged.status_code == 403, path
        assert kept(pages.engine) == before, "and nothing changed"
    with TestClient(pages.app) as client:
        assert client.get("/admin/import", follow_redirects=False).headers["location"] == "/admin/login"
        assert client.get(f"/admin/import/jobs/{job.id}", follow_redirects=False).status_code == 303
    assert pages.fake.requests == []


def test_a_reading_that_took_too_long_is_said_on_the_page_and_is_never_a_500(pages: Pages, monkeypatch: pytest.MonkeyPatch) -> None:
    job, _ = pages.yard.plan(A)

    def slow(*_: Any, **__: Any) -> Any:
        raise TooSlow("reading the imports took longer than 5s: the page cannot be built now")

    with opened(pages) as browser:
        monkeypatch.setattr(area.imports, "overview", slow)
        tree = read(browser.client.get("/admin/import"))
        assert tree.text().count("база занята — попробуйте ещё раз") == 1
        assert tree.one(**{"data-job": str(job.id)}), "what could be read is shown all the same"
        assert tree.has(**{"data-service": ANY}) and not tree.has(**{"data-kind": ANY}), "the queue is read apart from the figures"
        monkeypatch.setattr(area.imports, "jobs", slow)
        tree = read(browser.client.get("/admin/import"))
        assert tree.text().count("база занята — попробуйте ещё раз") == 2 and not rows(tree)
        monkeypatch.setattr(area.imports, "job_detail", slow)
        detail = browser.client.get(f"/admin/import/jobs/{job.id}")
        assert detail.status_code == 503 and "база занята — попробуйте ещё раз" in detail.text
    assert pages.fake.requests == []


def test_what_is_switched_off_is_not_planned_or_read_again_from_the_page(pages: Pages) -> None:
    yard, engine = pages.yard, pages.engine
    job, _ = yard.plan(C)  # on the watch list, switched off
    with opened(pages) as browser:
        where = f"/admin/import/jobs/{job.id}"
        assert read(browser.client.get(where)).all(**{"data-watched": "no"})
        plan = browser.post(f"{where}/plan", to_ledger="140")
        verify = browser.post(f"{where}/verify", from_ledger="100", to_ledger="130")
        for answer in (plan, verify):
            assert answer.status_code == 403 and "выключен" in answer.text
        assert the(engine, job.id).to_ledger == 130 and service_jobs.recent(engine, kind="verify") == []


def test_a_job_that_is_not_there_is_said_so(pages: Pages) -> None:
    with opened(pages) as browser:
        assert browser.client.get("/admin/import/jobs/9999").status_code == 404
        missing = browser.post("/admin/import/jobs/9999/pause", because="why")
        assert missing.status_code == 403 and "Задания 9999 нет" in missing.text
        assert browser.client.get("/admin/import/jobs/later").status_code == 422
