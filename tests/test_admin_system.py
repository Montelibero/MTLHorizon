"""The pages of Operations Notifier, of the Horizon pool and of the chart of a
market, as a browser meets them behind the door of the admin UI: what they
show of a database that the services themselves filled, what their forms put
in the queue (and never run), and what they never do — ask a server when they
open, tell a secret, change anything by a GET, or delete what a person made
by hand.

The database is filled the way the worker fills it: by the reconciliation
passes, by a check of the pool through the task runner, by the backfill over
the fake Horizon. No page is given anything but what the services keep.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.admin import support
from app.api.app import create_app
from app.backfill import config as watch_lists
from app.backfill.jobs import market_key
from app.config import PUBLIC_NETWORK_PASSPHRASE, Settings
from app.history_api import aggregations, coverage, streams
from app.notifier.client import subscription_params
from app.sources.horizon import HorizonSettings
from app.sources.pool import HorizonPool, PoolSettings
from app.storage import coverage as coverage_store
from app.storage import pool_state, service_jobs
from app.storage.models import ServiceJob, TrackedAccount, WatchSubscription
from app.subscriptions import reconcile
from app.subscriptions.desired import ACCOUNT, ASSET
from app.tasks.runner import TaskRunner
from tests.factories import address
from tests.fake_horizon import A, B, EUR, ISSUER, MTL, RECT, FakeHorizon
from tests.fake_notifier_api import FakeNotifierApi
from tests.test_backfill import WATCH_LIST, Pilot
from tests.test_pool import SETTINGS, Clock
from tests.test_subscriptions import Site

SECRET = "a-secret-of-the-admin"
TOKEN = "the-token-of-the-notifier-api"
CALLBACK = "https://mtl-horizon.test/webhooks/notifier"
EVIL = "<script>alert('x')</script>"
FIRST, SECOND = "https://a.horizon.test", "https://b.horizon.test"
E = address(10)  # an account that is in no watch list
T0_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z: when the first ledger of the fake history closes
SHOWN_MARKET = market_key(RECT, EUR).replace(">", "&gt;")  # the key of a market as a page shows it in full
SHORT_MARKET = "EURMTL &gt; MTLRECT"  # … and by its codes, where there is no room for the issuers
# A part of B's address that is in none of the other keys (their beginnings are all alike).
PART_OF_B = next(B[-size:] for size in range(4, 40) if not any(B[-size:] in other for other in (A, EUR, RECT, E)))


# -- how the pages are reached ---------------------------------------------------------


def settings_for(url: str, **changes: Any) -> Settings:
    return Settings(**{"database_url": url, "network_passphrase": PUBLIC_NETWORK_PASSPHRASE, "admin_secret": SECRET, **changes})


def csrf_of(page: Any) -> str:
    assert page.status_code == 200, page.text[:600]
    found = re.search(r'name="csrf" value="([0-9a-f]{64})"', page.text)
    assert found, "the page carries the form's token"
    return found[1]


@contextmanager
def behind_the_door(settings: Settings) -> Iterator[TestClient]:
    """A browser that logged into the admin UI of this installation."""
    with TestClient(create_app(settings)) as client:
        assert client.post("/admin/login", data={"secret": SECRET}, follow_redirects=False).status_code == 303
        yield client


def page_of(client: TestClient, path: str, status: int = 200) -> str:
    page = client.get(path)
    assert page.status_code == status, f"GET {path}: {page.status_code} {page.text[:600]}"
    return page.text


def follows(client: TestClient, path: str, data: dict[str, Any], token: str | None = None) -> tuple[int, dict[str, list[str]], str]:
    """Post a form and follow where it sends: the status, the query of the
    place it sent to, and the page there — or, when it was refused, the
    refusal."""
    answer = client.post(path, data={"csrf": token, **data} if token is not None else data, follow_redirects=False)
    if answer.status_code != 303:
        return answer.status_code, {}, answer.text
    return 303, parse_qs(urlsplit(answer.headers["location"]).query), client.get(answer.headers["location"]).text


def card_of(page: str, title: str) -> str:
    """The card whose heading starts with ``title``, up to the next card."""
    start = page.index(title)
    end = page.find('<div class="card">', start)
    return page[start : end if end != -1 else len(page)]


# -- the Notifier ---------------------------------------------------------------------


def a_site(make_database: Callable[[], str], **changes: Any) -> Site:
    """A private database with the watch list, and a Notifier whose API
    wants the admin token."""
    site = Site(make_database())
    with Session(site.engine) as session, session.begin():
        watch_lists.apply(session, WATCH_LIST)
    site.api_fake = FakeNotifierApi(site.signer.public_key, authorization="token", admin_token=TOKEN)
    site.settings = settings_for(site.engine.url.render_as_string(hide_password=False), **{
        "notifier_signer_keys": (site.signer.public_key,), "notifier_api_url": "http://notifier.test",
        "notifier_api_token": TOKEN, "notifier_callback_url": CALLBACK, **changes,
    })
    return site


def a_pass(site: Site, api: Any) -> reconcile.Report:
    site.now += timedelta(minutes=2)
    return reconcile.run(site.engine, api, CALLBACK, now=site.now)


def run_tasks(site: Site, how_many: int = 1) -> list[ServiceJob]:
    """What the worker's runner does with what is queued: take and run."""
    site.now += timedelta(minutes=1)
    api = site.api()
    runner = TaskRunner(site.engine, site.settings, client=None, api=api, clock=lambda: site.now, name="runner-1")
    try:
        return [job for job in (runner.once() for _ in range(how_many)) if job is not None]
    finally:
        api.close()


def rows_of(site: Site) -> dict[tuple[str, str], WatchSubscription]:
    """Every row of ``watch_subscription``, retired ones included."""
    with Session(site.engine) as session:
        rows = list(session.scalars(select(WatchSubscription)))
        return {(row.kind, row.key): WatchSubscription(**{c.name: getattr(row, c.name) for c in WatchSubscription.__table__.columns}) for row in rows}


def states_of(site: Site) -> dict[tuple[str, str], str]:
    return {key: row.state for key, row in rows_of(site).items()}


def row_of(page: str, key: str) -> str:
    """The line of the subscriptions table that shows ``key``."""
    return [part for part in page.split("<tr>") if f'data-copy="{key}"' in part][0]


def cells_of(line: str) -> list[str]:
    """The cells of a line of the subscriptions table, in their order."""
    return [cell.split("</td>")[0] for cell in re.split(r"<td[^>]*>", line)[1:]]


def said(cell: str) -> str:
    """What a cell says, without its markup."""
    return re.sub(r"<[^>]+>", "", cell).strip()


def test_the_notifier_page_lists_wanted_against_confirmed_with_states_and_reasons(make_database: Callable[[], str]) -> None:
    site = a_site(make_database)
    try:
        with behind_the_door(site.settings) as client:
            page = page_of(client, "/admin/notifier")
            # Wanted by the rules, and no row yet: a pass makes the rows.
            assert 'Нужно подписок</div><div class="value">4</div>' in page
            assert 'Из них подтверждено</div><div class="value">0</div>' in page
            assert page.count(">wanted</span>") == 4 and page.count("строки ещё нет") == 4
            assert "аккаунт включён" in page and "история актива" in page and f"рынок {SHORT_MARKET}" in page
            assert f'title="market {SHOWN_MARKET}"' in page, "the rule itself is kept with its words"

            # Before the first pass: two subscriptions fit RECT — a person's to decide — and one was made by
            # hand for B as this installation would have made it, so it is taken. The rest is registered.
            with site.api() as api:
                api.create(subscription_params(ASSET, RECT, CALLBACK))
                api.create(subscription_params(ASSET, RECT, CALLBACK))
                api.create(subscription_params(ACCOUNT, B, CALLBACK))
                first = a_pass(site, api)
            assert (first.created, first.taken, first.ambiguous) == (2, 1, 1)
            page = page_of(client, "/admin/notifier")
            assert page.count(">error</span>") == 2 and page.count(">ambiguous</span>") == 1 and page.count(">active</span>") == 1
            assert page.count("registered, not confirmed yet") == 2
            line = row_of(page, RECT)
            cells = cells_of(line)
            assert ">ambiguous</span>" in cells[4] and "2 subscriptions fit" in cells[6]
            assert "Повторить" in cells[7] and "принять верную по id" in cells[7] and "решает человек" in cells[7]
            line = row_of(page, A)
            assert ">error</span>" in cells_of(line)[4] and "registered, not confirmed yet" in cells_of(line)[6]

            with site.api() as api:
                second = a_pass(site, api)
            assert second.confirmed == 2
            page = page_of(client, "/admin/notifier")
            assert page.count(">active</span>") == 3 and page.count(">ambiguous</span>") == 1
            assert 'Из них подтверждено</div><div class="value">3</div>' in page
            for (kind, key), row in rows_of(site).items():
                cells = cells_of(row_of(page, key))
                assert f">{row.state}</span>" in cells[4], (key, row.state)
                assert kind in cells[0] and f"попыток: {row.attempts}" in said(cells[4])
                assert row.remote_id in cells[3] if row.remote_id else said(cells[3]).startswith("—"), "the id at the Notifier, where there is one"
                # The version of the Notifier's matching rules, as the row keeps it (written when it was confirmed).
                assert said(cells[3]).endswith("filter version: " + ("2" if row.filter_version == 2 else "—")), key
                if row.state == "active":
                    confirmed = row.confirmed_at.strftime("%Y-%m-%d %H:%M:%S UTC")
                    assert row.attempts == 0
                    assert f"подтверждена {confirmed}" in said(cells[5]), "when it was confirmed, by the pass's clock"
                    assert "трогалась " in said(cells[5]) and "следующая попытка" not in said(cells[5])
                    assert said(cells[6]) == "—" and "<button" not in cells[7]
                else:
                    times = said(cells[5])
                    assert "подтверждена" not in times and "следующая попытка" not in times, "never confirmed, and no next attempt"

            # By state, and by a part of the key.
            page = page_of(client, "/admin/notifier?state=ambiguous")
            assert page.count(">ambiguous</span>") == 1 and ">active</span>" not in page and RECT in page
            page = page_of(client, "/admin/notifier?state=active")
            assert page.count(">active</span>") == 3 and ">ambiguous</span>" not in page
            page = page_of(client, f"/admin/notifier?q={PART_OF_B}")
            assert B in page and A not in page and EUR not in page and RECT not in page

            # Made by hand and adopted: manual, and never deleted here.
            with site.api() as api:
                made = api.create(subscription_params(ACCOUNT, E, CALLBACK))
                manual_id = reconcile.adopt(site.engine, ACCOUNT, E, made["id"], callback_url=CALLBACK, api=api, now=site.now)
            cells = cells_of(row_of(page_of(client, f"/admin/notifier?q={E}"), E))
            assert ">manual</span>" in cells[4] and "сделана руками" in cells[3] and made["id"] in cells[3]
            assert f'action="/admin/notifier/{manual_id}/release"' in cells[7] and "Забыть здесь" in cells[7]
            assert "У Notifier она останется" in cells[7]
            assert "<button" not in cells_of(row_of(page_of(client, f"/admin/notifier?q={A}"), A))[7], "what was not adopted is not forgotten"

            # The owner switches an account off: nothing wants its subscription, and the pass retires it.
            with Session(site.engine) as session, session.begin():
                session.get(TrackedAccount, A).enabled = False  # type: ignore[union-attr]
            with site.api() as api:
                third = a_pass(site, api)
            assert third.retired == 1
            cells = cells_of(row_of(page_of(client, f"/admin/notifier?q={A}"), A))
            assert ">retired</span>" in cells[4] and "уже не хотим" in cells[2] and "<button" not in cells[7]
            assert "Нужно подписок</div><div class=\"value\">3" in page_of(client, "/admin/notifier")
    finally:
        site.close()


def test_a_long_list_is_paginated_and_looking_changes_nothing(make_database: Callable[[], str]) -> None:
    site = a_site(make_database)
    try:
        with Session(site.engine) as session, session.begin():
            for number in range(60):
                session.add(WatchSubscription(kind=ACCOUNT, key=address(5000 + number), next_attempt_at=site.now))
        with behind_the_door(site.settings) as client:
            first = page_of(client, "/admin/notifier?state=wanted")
            later = page_of(client, "/admin/notifier?state=wanted&page=2")
            assert "страница 1 из 2 · 64" in first and "страница 2 из 2 · 64" in later, "the rows, and the four wished for without one"

            def shown(text: str) -> set[str]:  # the keys of the lines a page shows
                return set(re.findall(r'data-copy="([^"]+)"', text))

            assert len(shown(first)) == support.PER_PAGE and not shown(first) & shown(later)
            assert shown(first) | shown(later) == {address(5000 + number) for number in range(60)} | {A, B, EUR, RECT}
            assert ">wanted</span>" not in page_of(client, "/admin/notifier?state=wanted&page=30"), "past the end: nothing"
            assert len(rows_of(site)) == 60 and service_jobs.recent(site.engine) == [], "a GET changed nothing"
    finally:
        site.close()


def test_the_buttons_put_tasks_in_the_queue_lead_to_them_and_never_run_them(make_database: Callable[[], str]) -> None:
    site = a_site(make_database)
    try:
        with site.api() as api:
            api.create({"reaction_url": "https://someone-else.test/hook", "account": E})  # somebody else's: left as it is
            api.create(subscription_params(ASSET, RECT, CALLBACK))
            api.create(subscription_params(ASSET, RECT, CALLBACK))
        with behind_the_door(site.settings) as client:
            assert "ещё не смотрели" in page_of(client, "/admin/notifier")
            token = csrf_of(client.get("/admin/notifier"))
            asked = len(site.api_fake.requests)

            status, sent, page = follows(client, "/admin/notifier/look", {}, token)
            look = service_jobs.get(site.engine, int(sent["job"][0]))
            assert status == 303 and "Ничего не меняется" in sent["notice"][0]
            assert look is not None and (look.state, look.kind, look.params, look.one_at_a_time) == ("queued", "reconcile", {"dry_run": True}, "subscriptions")
            assert len(site.api_fake.requests) == asked, "the task was put in the queue, not run"
            assert f'href="/admin/tasks/{look.id}"' in page and ">queued</span>" in page
            assert 'data-refresh="10"' in page, "while the task runs, the page comes back by itself"

            # While the look is queued, a pass is not put next to it: one at a time with the Notifier.
            status, sent, _ = follows(client, "/admin/notifier/reconcile", {}, token)
            assert status == 303 and int(sent["job"][0]) == look.id and "Ничего не добавлено" in sent["notice"][0] and "это взгляд на Notifier (сухой прогон)" in sent["notice"][0]
            assert len(service_jobs.recent(site.engine)) == 1

            # The worker runs the look: what a pass would do, read from the Notifier's own list.
            done = run_tasks(site)[0]
            assert done.id == look.id and done.state == "done"
            page = page_of(client, "/admin/notifier")
            assert "Что показал последний взгляд на Notifier" in page and "data-refresh" not in page
            assert site.signer.public_key in page and "да: вебхуки от этого Notifier проходят проверку" in page
            assert page.count("создать у Notifier") == 4 and "хотят правила: аккаунт включён" in page and "хотят правила: история актива" in page
            assert f"хотят правила: история актива, рынок {SHORT_MARKET}" in page
            assert "Подписок у Notifier</dt><dd>3 — с адресом этой установки 2, чужих 1" in page
            assert len(site.api_fake.subscriptions) == 3, "a look makes nothing there"

            # Now the pass goes in; the worker runs it.
            status, sent, page = follows(client, "/admin/notifier/reconcile", {}, token)
            passed = service_jobs.get(site.engine, int(sent["job"][0]))
            assert status == 303 and passed is not None and passed.id != look.id and passed.params == {"dry_run": False}
            assert len(site.api_fake.requests) == asked + 2, "the pass was queued, not run"  # only the look read the status and the list
            assert len(site.api_fake.subscriptions) == 3, "queued, not run"
            assert run_tasks(site)[0].id == passed.id
            page = page_of(client, "/admin/notifier")
            assert "Как прошла последняя сверка" in page and "создано</dt><dd>3</dd>" in page and "неоднозначных</dt><dd>1</dd>" in page

            # «Повторить» for a row in error is the pass; for one that is a person's to decide, the look.
            rows = rows_of(site)
            error_id = next(row.id for (_, key), row in rows.items() if key == A)
            status, sent, page = follows(client, "/admin/notifier/retry", {"row": str(error_id)}, token)
            retry = service_jobs.get(site.engine, int(sent["job"][0]))
            assert status == 303 and retry is not None and (retry.kind, retry.params, retry.state) == ("reconcile", {"dry_run": False}, "queued")
            assert sent["q"] == [A] and f'href="/admin/tasks/{retry.id}"' in page
            run_tasks(site)
            ambiguous_id = next(row.id for (_, key), row in rows_of(site).items() if key == RECT)
            status, sent, _ = follows(client, "/admin/notifier/retry", {"row": str(ambiguous_id)}, token)
            retry = service_jobs.get(site.engine, int(sent["job"][0]))
            assert status == 303 and retry is not None and retry.params == {"dry_run": True} and sent["q"] == [RECT]
            status, _, page = follows(client, "/admin/notifier/retry", {"row": str(ambiguous_id + 1000)}, token)
            assert status == 403 and "Такой строки" in page
    finally:
        site.close()


def test_adopt_with_an_api_reads_the_subscription_back_at_the_notifier_and_refuses_what_is_not_ours(make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch) -> None:
    site = a_site(make_database)
    monkeypatch.setattr("app.watch.provision.make_api", lambda settings, **options: site.api())
    try:
        with site.api() as api:
            theirs = api.create({"reaction_url": "https://someone-else.test/hook", "account": A})
            ours = api.create(subscription_params(ACCOUNT, A, CALLBACK))
        at_notifier = [sub["id"] for sub in site.api_fake.subscriptions]
        asked = len(site.api_fake.requests)
        with behind_the_door(site.settings) as client:
            assert "API настроен: подписка сначала читается у Notifier" in page_of(client, "/admin/notifier")
            token = csrf_of(client.get("/admin/notifier"))

            # Not at the Notifier at all: refused, and nothing is written.
            status, sent, page = follows(client, "/admin/notifier/adopt", {"kind": "account", "key": A, "id": "0" * 24}, token)
            assert status == 303 and "нет активной подписки" in sent["error"][0] and "ничего не принято" in sent["error"][0]
            assert (ACCOUNT, A) not in rows_of(site) and ">manual</span>" not in page
            # There, but somebody else's: not what this installation registers.
            status, sent, _ = follows(client, "/admin/notifier/adopt", {"kind": "account", "key": A, "id": theirs["id"]}, token)
            assert status == 303 and "не то, что регистрирует эта установка" in sent["error"][0] and (ACCOUNT, A) not in rows_of(site)
            assert [call[:2] for call in site.api_fake.requests[asked:]] == [("GET", "/api/subscription")] * 2, "read back, and nothing else asked"

            status, sent, page = follows(client, "/admin/notifier/adopt", {"kind": "account", "key": A, "id": ours["id"]}, token)
            assert status == 303 and "прочитана у Notifier и совпадает" in sent["notice"][0] and "никогда не удалит" in sent["notice"][0]
            assert sent["q"] == [A] and ">manual</span>" in page and ours["id"] in page and "сделана руками" in page
        row = rows_of(site)[(ACCOUNT, A)]
        assert (row.state, row.managed, row.remote_id) == ("manual", False, ours["id"])
        assert [sub["id"] for sub in site.api_fake.subscriptions] == at_notifier, "the page made and removed nothing there"
    finally:
        site.close()


def test_adopt_without_an_api_takes_the_subscription_at_its_word_and_the_page_says_so(make_database: Callable[[], str]) -> None:
    site = a_site(make_database, notifier_api_url="")
    try:
        with behind_the_door(site.settings) as client:
            page = page_of(client, "/admin/notifier")
            assert "принимается <strong>на слово</strong>" in page and "API Notifier не настроен" in page
            assert "/admin/notifier/look" not in page and "/admin/notifier/reconcile" not in page, "there is nothing to reconcile with"
            assert "Токен API</dt><dd>задан" in page and TOKEN not in page
            token = csrf_of(client.get("/admin/notifier"))

            status, sent, page = follows(client, "/admin/notifier/adopt", {"kind": "asset", "key": RECT, "id": "made-by-hand"}, token)
            assert status == 303 and "Принята на слово" in sent["notice"][0] and "не сверяли" in sent["notice"][0]
            assert "никогда не удалит" in sent["notice"][0] and "ни одно правило" not in sent["notice"][0]
            assert ">manual</span>" in page and "made-by-hand" in page and RECT in page
            row = rows_of(site)[(ASSET, RECT)]
            assert (row.state, row.managed, row.filter_version) == ("manual", False, None) and site.api_fake.requests == []

            # An asset code is case-sensitive, as on the network: the same letters in another case are another
            # asset, and nothing watches it — said so.
            status, sent, _ = follows(client, "/admin/notifier/adopt", {"kind": "asset", "key": f"mtlrect:{ISSUER}", "id": "made-by-hand"}, token)
            assert status == 303 and "ни одно правило наблюдения этого не хочет" in sent["notice"][0]
            assert rows_of(site)[(ASSET, f"mtlrect:{ISSUER}")].state == "manual"

            # What is not an account or an asset is refused, and nothing of it reaches a row.
            for kind, key in (("account", "not-an-address"), ("asset", "MTL"), ("asset", EVIL), ("market", A)):
                status, _, page = follows(client, "/admin/notifier/adopt", {"kind": kind, "key": key, "id": "abc"}, token)
                assert status == 403 and "<script>alert" not in page, (kind, key)
            status, _, _ = follows(client, "/admin/notifier/adopt", {"kind": "account", "key": A, "id": "with spaces and <>"}, token)
            assert status == 403 and (ACCOUNT, A) not in rows_of(site) and len(rows_of(site)) == 2

            # Without an API the buttons of the tasks refuse too: there is nobody to ask.
            status, _, page = follows(client, "/admin/notifier/look", {}, token)
            assert status == 403 and "спросить его не с чем" in page and service_jobs.recent(site.engine) == []
    finally:
        site.close()


def test_release_forgets_an_adopted_subscription_and_deletes_nothing_at_the_notifier(make_database: Callable[[], str]) -> None:
    site = a_site(make_database)
    try:
        with site.api() as api:
            made = api.create(subscription_params(ACCOUNT, E, CALLBACK))
        before = site.api_fake.subscriptions
        manual_id = reconcile.adopt(site.engine, ACCOUNT, E, made["id"], callback_url=CALLBACK, api=None, now=site.now)
        with Session(site.engine) as session, session.begin():
            session.add(WatchSubscription(kind=ACCOUNT, key=B, state="active", remote_id="ab" * 12, next_attempt_at=site.now))
        active_id = rows_of(site)[(ACCOUNT, B)].id
        with behind_the_door(site.settings) as client:
            page = page_of(client, f"/admin/notifier?q={E}")
            assert "У Notifier она останется" in page and "нет способа удалить подписку" in page
            assert "delete" not in page.lower()
            token = csrf_of(client.get(f"/admin/notifier?q={E}"))

            # Not adopted by hand — or not there: not forgotten here.
            status, _, page = follows(client, f"/admin/notifier/{active_id}/release", {}, token)
            assert status == 403 and "а не manual" in page
            status, _, page = follows(client, "/admin/notifier/9999/release", {}, token)
            assert status == 403 and "Строки подписок 9999 нет" in page

            status, sent, page = follows(client, f"/admin/notifier/{manual_id}/release", {}, token)
            assert status == 303 and "забыта здесь" in sent["notice"][0] and "осталась" in sent["notice"][0]
            assert ">retired</span>" in page and "/release" not in page.split("<h2>Хотим")[1].split("<details>")[0]
            # And there is no way from the page to delete one, here or there.
            for path in (f"/admin/notifier/{manual_id}/delete", f"/admin/notifier/{manual_id}/retire", "/admin/notifier/delete", "/admin/notifier/forget"):
                assert client.post(path, data={"csrf": token}, follow_redirects=False).status_code == 404
        assert site.api_fake.subscriptions == before, "nothing was deleted at the Notifier"
        assert states_of(site)[(ACCOUNT, E)] == "retired" and states_of(site)[(ACCOUNT, B)] == "active"
    finally:
        site.close()


def test_the_notifier_page_tells_no_secret(make_database: Callable[[], str]) -> None:
    site = a_site(
        make_database,
        notifier_api_url="http://a-user:not-a-password@notifier.test/api?key=a-query-secret",
        notifier_callback_url="http://cb-user:cb-pass@mtl-horizon.test/webhooks/notifier?token=cb-query#frag",
        api_tokens=("history-api-token",),
    )
    try:
        with behind_the_door(site.settings) as client:
            page = page_of(client, "/admin/notifier")
            assert "http://notifier.test" in page and "http://mtl-horizon.test/webhooks/notifier" in page
            assert "Токен API</dt><dd>задан" in page
            for secret in (TOKEN, SECRET, "not-a-password", "a-user", "a-query-secret", "cb-pass", "cb-user", "cb-query", "history-api-token",
                           site.engine.url.password or "~~"):
                assert secret not in page, secret
    finally:
        site.close()


def test_the_limits_are_shown_as_requirements_and_not_as_the_service_s_values(make_database: Callable[[], str]) -> None:
    site = a_site(make_database)
    try:
        with behind_the_door(site.settings) as client:
            page = page_of(client, "/admin/notifier")
            assert "MAX_ACTIVE_SUBSCRIPTIONS</code> и <code>MAX_ACTIVE_SUBSCRIPTIONS_PER_USER" in page
            assert "должны быть достаточно велики" in page and "по умолчанию 100" in page
            assert "нужно <strong>4</strong> активных подписок" in page and "фактические значения здесь <strong>неизвестны</strong>" in page
            assert "<code>disabled</code>" in page and "<code>token</code>" in page and "ed25519" in page and "не поддерживается" in page
            assert "MTL_HORIZON_NOTIFIER_KEYS</code>) и токен API (<code>MTL_HORIZON_NOTIFIER_API_TOKEN" in page and "две разные вещи" in page
            assert "не гарантирует" in page and "выключает подписку" in page
            assert "Ключи подписи вебхуков</dt><dd>задано: 1" in page
            assert "лимит Notifier" not in page.lower() and "лимит: 100" not in page
    finally:
        site.close()


def test_what_is_stored_or_sent_is_shown_escaped(make_database: Callable[[], str]) -> None:
    site = a_site(make_database)
    try:
        with Session(site.engine) as session, session.begin():
            session.add(WatchSubscription(kind=ACCOUNT, key=EVIL, state="error", last_error=EVIL, remote_id="ab" * 12, next_attempt_at=site.now))
        job, _ = service_jobs.enqueue(site.engine, "reconcile", {"dry_run": True}, one_at_a_time="subscriptions", now=site.now)
        with Session(site.engine) as session, session.begin():
            kept = session.get(ServiceJob, job.id)
            assert kept is not None
            kept.state, kept.last_error = "failed", EVIL
        with behind_the_door(site.settings) as client:
            for path in ("/admin/notifier", f"/admin/notifier?q={urlencode({'q': EVIL})}", "/admin/notifier?kind=<script>&job=" + str(job.id)):
                page = page_of(client, path)
                assert "&lt;script&gt;" in page and "<script>alert" not in page, path
    finally:
        site.close()


# -- the pool -------------------------------------------------------------------------


def refused() -> httpx.ConnectError:
    return httpx.ConnectError("connection refused")


def a_pool(engine: Engine, clock: Clock, *fakes: FakeHorizon) -> HorizonPool:
    """A pool as a running process has it: it notes what it sees of a server."""
    return HorizonPool(
        tuple(fake.origin for fake in fakes),
        HorizonSettings(**SETTINGS),
        network_passphrase=PUBLIC_NETWORK_PASSPHRASE,
        remember=lambda server: pool_state.note(engine, server.as_dict()),
        pool_settings=PoolSettings(health_check_interval=60, failover_cooldown=3600),
        transports={fake.origin: httpx.MockTransport(fake._handle) for fake in fakes},
        clock=clock,
        sleeper=clock.sleep,
    )


def run_one(engine: Engine, settings: Settings, pool: HorizonPool) -> ServiceJob:
    """What the worker's runner does with the task that is queued: take and run."""
    done = TaskRunner(engine, settings, client=pool, clock=lambda: datetime.now(UTC), name="runner-1").once()
    assert done is not None and done.state == "done", (done and done.last_error)
    return done


def test_the_pool_page_shows_the_servers_of_the_settings_with_what_is_kept_of_them_and_asks_nothing(make_database: Callable[[], str]) -> None:
    url = make_database()
    engine = create_engine(url)
    one, two = FakeHorizon(origin=FIRST), FakeHorizon(origin=SECOND)
    one.headers = {"X-RateLimit-Limit": "17200", "X-RateLimit-Remaining": "17199", "X-RateLimit-Reset": "5"}
    pool = a_pool(engine, Clock(), one, two)
    settings = settings_for(url, horizon_urls=(FIRST + "/", SECOND))
    try:
        with behind_the_door(settings) as client:
            page = page_of(client, "/admin/horizon")
            assert FIRST in page and SECOND in page and page.count("Ещё не проверялся") == 2
            assert "ни одна проверка ещё не закончилась" in page and page.count("Процессы о нём ещё ничего не сказали") == 2
            assert "1.0 запросов в секунду" in page and "MTL_HORIZON_REQUESTS_PER_SECOND" in page and "429" in page
            assert "до трёх повторов" in page and "не ping" in page and "не больше шести запросов на сервер" in page

            # The worker's check: the first server answers, the second does not.
            two.down = refused()
            service_jobs.enqueue(engine, "pool_check", {"repeats": 2}, one_at_a_time="pool")
            checked = run_one(engine, settings, pool)
            # And what a running process saw: the first answered, then failed and was left alone; the second took over.
            assert pool.get("/ledgers/105")["sequence"] == 105
            one.down, two.down = refused(), None
            assert pool.get("/ledgers/106")["sequence"] == 106
            one.requests.clear(), two.requests.clear()
            asked = pool.stats.requests

            page = page_of(client, "/admin/horizon")
            assert one.requests == [] and two.requests == [] and pool.stats.requests == asked, "the page asked no server"
            first, second = card_of(page, "<h3>1. " + FIRST), card_of(page, "<h3>2. " + SECOND)
            assert "активный на последней проверке" in first and "активный" not in second
            assert ">доступен</span>" in first and ">недоступен</span>" in second and "корень сервера не отвечает" in second
            assert "30.0.1-fake" in first and "та, что хранит установка" in first
            assert "Последний леджер</dt><dd>130" in first and "Старейший леджер</dt><dd>100" in first and "Отставание</dt><dd>0" in first
            assert "спрошено 2, отвечено 2, медиана" in first and "<code>/ledgers/129</code>" in first
            assert "X-RateLimit-Limit=17200" in first and "X-RateLimit-Remaining=17199" in first
            assert "спрошено 2, отвечено 0" in second and "сервер не прислал" in second
            assert "no answer to its root" in first and "Оставлен до</dt><dd><time" in first and "время ещё не пришло" in first
            assert "Последний ответ</dt><dd><time" in first and "не оставлялся" in second
            assert "Последняя проверка пула (" in page and "активным был <strong>" + FIRST in page and "У каждого процесса свой пул" in page
            assert "Причина: корень сервера не отвечает." in second and "проверен <time" in first
            assert f"Запросов: {checked.result['requests']} · серверов доступно: 1 из 2 · недоступны: <code>{SECOND}</code>" in page
            assert f'href="/admin/tasks/{checked.id}"' in page

            # As of the last check only: that the second answers now and the first does not is not known here.
            again = page_of(client, "/admin/horizon")
            assert "активный на последней проверке" in card_of(again, "<h3>1. " + FIRST) and one.requests == [] and two.requests == []
    finally:
        pool.close()
        engine.dispose()


def test_check_all_puts_the_task_in_the_queue_and_leads_to_it(make_database: Callable[[], str]) -> None:
    url = make_database()
    engine = create_engine(url)
    one = FakeHorizon()
    settings = settings_for(url, horizon_urls=(one.origin,))
    pool = a_pool(engine, Clock(), one)
    try:
        with behind_the_door(settings) as client:
            assert "Из админки пул ещё не проверяли" in page_of(client, "/admin/horizon")
            token = csrf_of(client.get("/admin/horizon"))

            status, sent, page = follows(client, "/admin/horizon/check", {"repeats": "2"}, token)
            job = service_jobs.get(engine, int(sent["job"][0]))
            assert status == 303 and job is not None and (job.state, job.kind, job.params, job.one_at_a_time) == ("queued", "pool_check", {"repeats": 2}, "pool")
            assert one.requests == [], "queued, not run"
            assert f'href="/admin/tasks/{job.id}"' in page and ">queued</span>" in page and 'data-refresh="10"' in page

            status, sent, _ = follows(client, "/admin/horizon/check", {"repeats": "3"}, token)
            assert status == 303 and int(sent["job"][0]) == job.id and "ничего не добавлено" in sent["notice"][0] and "повторов: 2" in sent["notice"][0]
            assert [other.id for other in service_jobs.recent(engine)] == [job.id]

            # The worker runs it with what was asked: two of each request to the one server.
            assert run_one(engine, settings, pool).id == job.id
            assert [path for path, _ in one.requests if path == "/"] == ["/", "/"]
            one.requests.clear()
            page = page_of(client, "/admin/horizon")
            assert "доступно: 1 из 1" in page and "спрошено 2, отвечено 2" in page and one.requests == []
            assert "data-refresh" not in page

            for asked in ("0", "4", "три", "", "-1"):
                status, _, page = follows(client, "/admin/horizon/check", {"repeats": asked}, token)
                assert status == 403 and "Число повторов" in page, asked
            assert len(service_jobs.recent(engine)) == 1
    finally:
        pool.close()
        engine.dispose()


def test_an_address_is_shown_without_its_user_name_and_password(make_database: Callable[[], str]) -> None:
    url = make_database()
    engine = create_engine(url)
    settings = settings_for(
        url, horizon_urls=("https://a-user:not-a-password@a.horizon.test:8443/", "http://b-user@b.horizon.test"), horizon_requests_per_second=0.5
    )
    try:
        with behind_the_door(settings) as client:
            page = page_of(client, "/admin/horizon")
            assert "https://a.horizon.test:8443" in page and "http://b.horizon.test" in page
            for secret in ("not-a-password", "a-user", "b-user", SECRET, engine.url.password or "~~"):
                assert secret not in page, secret
            assert "0.5 запросов в секунду" in page and "один для всех процессов и всех серверов" in page
    finally:
        engine.dispose()


# -- the chart -------------------------------------------------------------------------


def loaded(make_database: Callable[[], str]) -> Pilot:
    """A private database with the window of the fake history in it, as the backfill loads it."""
    pilot = Pilot(make_database())
    try:
        reports = pilot.run()
    except BaseException:
        pilot.close()
        raise
    assert {report.status for report in reports} == {"verified"}, [report.error for report in reports]
    return pilot


def chart_of(client: TestClient, base: str, counter: str, status: int = 200, **asked: Any) -> str:
    return page_of(client, "/admin/chart?" + urlencode({"base": base, "counter": counter, **asked}), status)


def drawn(page: str) -> list[dict[str, str]]:
    """What the picture says it drew: every bucket with the numbers it holds."""
    return [dict(re.findall(r'data-([a-z]+)="([^"]*)"', group)) for group in re.findall(r'<g class="cx-bucket"([^>]*)>', page)]


def theirs(engine: Engine, base: str, counter: str, start: int, end: int, resolution: int) -> list[dict[str, str]]:
    """The buckets of the history API's own aggregation, named as the picture carries them."""
    with streams.reading(engine) as connection:
        found = aggregations.buckets(connection, base, counter, start, end, resolution)
    names = {
        "ts": "timestamp", "count": "trade_count", "base": "base_volume", "counter": "counter_volume",
        "avg": "avg", "open": "open", "high": "high", "low": "low", "close": "close",
    }
    return [{field: bucket[name] for field, name in names.items() if name in bucket} for bucket in found]


THREE_MINUTES = {"period": "own", "from": "2026-01-01 00:00", "to": "2026-01-01 00:03", "resolution": "60000"}


def test_the_chart_is_the_aggregation_of_the_stored_trades_and_empty_intervals_are_not_drawn(make_database: Callable[[], str]) -> None:
    pilot = loaded(make_database)
    try:
        asked = len(pilot.fake.requests)
        with behind_the_door(settings_for(pilot.engine.url.render_as_string(hide_password=False))) as client:
            page = chart_of(client, RECT, EUR, **THREE_MINUTES)
            assert drawn(page) == theirs(pilot.engine, RECT, EUR, T0_MS, T0_MS + 180_000, 60_000)
            # A trade 20 seconds after the start and another at 60: two buckets of a minute, one trade in each.
            assert [(one["ts"], one["count"]) for one in drawn(page)] == [(str(T0_MS), "1"), (str(T0_MS + 60_000), "1")]
            assert "корзин в периоде 3, из них со сделками 2" in page and "конец — как задан (не включая)" in page
            assert 'class="chart cx"' in page and "4.9325436" in page and "23.1829549" in page and "4.7000000" in page

            # The same market the other way round: the volumes exchange places and the prices are the reciprocals.
            page = chart_of(client, EUR, RECT, **THREE_MINUTES)
            assert drawn(page) == theirs(pilot.engine, EUR, RECT, T0_MS, T0_MS + 180_000, 60_000)
            assert drawn(page)[0]["base"] == "23.1829549" and drawn(page)[0]["close"] == "0.2127660"
            assert f'data-copy="{SHOWN_MARKET}"' in page and "Каноническая пара" in page and "Перевернуть пару" in page

            # A trade through a liquidity pool is a trade of its market like any other.
            page = chart_of(client, EUR, MTL, **THREE_MINUTES)
            assert drawn(page) == theirs(pilot.engine, EUR, MTL, T0_MS, T0_MS + 180_000, 60_000) and [one["count"] for one in drawn(page)] == ["1"]

            # A quiet minute has no bucket and no mark: nothing is invented for it.
            page = chart_of(client, RECT, EUR, period="own", **{"from": "2026-01-01 00:02", "to": "2026-01-01 00:03"}, resolution="60000")
            assert drawn(page) == [] and "нет сделок этого рынка" in page and "нет корзин с ценами" in page
            assert 'class="chart cx"' in page, "the frame and the coverage are drawn all the same"

            # In a day the two trades are one bucket: days are cut on midnight UTC.
            page = chart_of(client, RECT, EUR, period="own", **{"from": "2026-01-01", "to": "2026-01-02"}, resolution="86400000")
            assert [(one["ts"], one["count"]) for one in drawn(page)] == [(str(T0_MS), "2")]
            assert "объём base 9.8650872" in page and "объём counter 46.3659098" in page
            assert len(pilot.fake.requests) == asked, "the chart asked nothing of the source"
    finally:
        pilot.close()


def test_a_period_is_placed_on_the_clock_by_the_archive(make_database: Callable[[], str]) -> None:
    pilot = loaded(make_database)
    try:
        asked = len(pilot.fake.requests)
        with behind_the_door(settings_for(pilot.engine.url.render_as_string(hide_password=False))) as client:
            # The day ends just past the newest ledger the archive has a close time for (118: the last with a
            # transaction in it); the resolution is chosen for it.
            page = chart_of(client, RECT, EUR)
            assert "последние сутки" in page and "сразу после последнего леджера архива №118 (закрыт 2026-01-01 00:01:30 UTC)" in page
            assert [(one["ts"], one["count"]) for one in drawn(page)] == [(str(T0_MS), "2")] and "корзина 5 минут" in page
            assert "последняя неделя" in chart_of(client, RECT, EUR, period="week") and "корзина час" in chart_of(client, RECT, EUR, period="week")
            assert "последний месяц" in chart_of(client, RECT, EUR, period="month") and "корзина сутки" in chart_of(client, RECT, EUR, period="month")
            # With an end of its own: the end is not part of the period.
            page = chart_of(client, RECT, EUR, **{"to": "2026-01-01T00:01:00Z"})
            assert "конец — как задан (не включая)" in page and [one["ts"] for one in drawn(page)] == [str(T0_MS)]

            # By ledgers: the time of the first and the last ledger of the range that the archive knows.
            page = chart_of(client, RECT, EUR, period="own", unit="ledger", **{"from": "104", "to": "112"}, resolution="60000")
            assert "по леджерам 104–112; время взято от леджеров 104 и 112" in page
            assert [(one["ts"], one["count"]) for one in drawn(page)] == [(str(T0_MS), "1"), (str(T0_MS + 60_000), "1")]
            assert len(pilot.fake.requests) == asked
            # Nothing asked yet: the form alone.
            assert 'name="base"' in page_of(client, "/admin/chart") and "cx-bucket" not in page_of(client, "/admin/chart")
    finally:
        pilot.close()


@pytest.mark.parametrize(
    "base, counter, asked, words",
    [
        (RECT, "", {}, "не прочитан"),
        ("MTL", EUR, {}, "не прочитан"),
        (f"{ISSUER}<script>alert('x')</script>", RECT, {}, "не прочитан"),
        (RECT, RECT, {}, "два разных актива"),
        (RECT, EUR, {"period": "fortnight"}, "Период —"),
        (RECT, EUR, {"resolution": "12345"}, "Разрешение —"),
        (RECT, EUR, {"period": "own"}, "должно быть начало"),
        (RECT, EUR, {"period": "own", "from": "2026-02-01", "to": "2026-01-01"}, "позже начала"),
        (RECT, EUR, {"period": "own", "from": "вчера"}, "не прочитано"),
        (RECT, EUR, {"period": "own", "unit": "ledger", "from": "5", "to": "9"}, "нет ни одного леджера 5–9"),
        (RECT, EUR, {"period": "own", "unit": "ledger", "from": "112", "to": "104"}, "больше последнего"),
        (RECT, EUR, {"period": "week", "resolution": "60000"}, "слишком велик для разрешения"),
        (RECT, EUR, {"period": "month", "resolution": "300000"}, "корзин"),
    ],
)
def test_what_cannot_be_drawn_is_refused_before_any_work(
    make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch, base: str, counter: str, asked: dict[str, Any], words: str
) -> None:
    pilot = loaded(make_database)
    wanted = len(pilot.fake.requests)

    def never(*_: Any, **__: Any) -> None:
        raise AssertionError("something was read for a chart that cannot be drawn")

    monkeypatch.setattr(aggregations, "buckets", never)
    monkeypatch.setattr(coverage, "market", never)
    try:
        with behind_the_door(settings_for(pilot.engine.url.render_as_string(hide_password=False))) as client:
            page = chart_of(client, base, counter, 400, **asked)
            assert words in page and "cx-bucket" not in page and "<script>alert" not in page
            assert 'name="base"' in page, "the form is still there, to be put right"
        assert len(pilot.fake.requests) == wanted
    finally:
        pilot.close()


def test_the_coverage_of_the_market_is_shown_next_to_the_chart(make_database: Callable[[], str]) -> None:
    pilot = loaded(make_database)
    try:
        asked = len(pilot.fake.requests)
        with behind_the_door(settings_for(pilot.engine.url.render_as_string(hide_password=False))) as client:
            page = chart_of(client, RECT, EUR)
            assert "Покрытие: что архив проверил по этому рынку" in page
            assert "Наблюдается</dt><dd>да, включён" in page and "Правило MARKET: окно леджеров 100–130" in page
            assert "от леджера 104 до 112" in page and 'class="cx-complete"' in page
            assert "Не прочитано</dt><dd>0" in page and "Недоступно для проверки</dt><dd>0" in page
            assert "не обязательно тихий рынок" in page and "/v1/coverage?" in page and "/trade_aggregations?" in page
            assert "docs/history-api.md" in page and 'href="/docs"' in page

            # A market nobody watches: said so, and where to add it. What is here of it — one trade through a
            # pool — came with a watched account; nothing of this market is verified.
            page = chart_of(client, MTL, EUR)
            assert "не наблюдается установкой" in page and 'href="/admin/markets"' in page
            assert "Наблюдается</dt><dd>нет" in page and "По этому рынку ничего не проверено" in page
            assert "от леджера 104 до 104" in page and [one["count"] for one in drawn(page)] == ["1"]
            assert 'class="cx-none"' in page and 'class="cx-complete"' not in page
            # One nobody watches and nothing brought: not a trade, not a range.
            page = chart_of(client, RECT, MTL)
            assert "не наблюдается установкой" in page and "нет ни одной" in page and drawn(page) == []
            assert "нет сделок этого рынка" in page and 'class="cx-complete"' not in page

            # A period out of the coverage: the verified window is still said, and nothing is claimed for the period.
            page = chart_of(client, RECT, EUR, period="own", **{"from": "2025-06-01", "to": "2025-06-02"})
            assert "окно леджеров 100–130" in page and "нет сделок этого рынка" in page and drawn(page) == []
            assert 'class="cx-complete"' not in page
            assert len(pilot.fake.requests) == asked
    finally:
        pilot.close()


def test_the_chart_says_what_of_the_market_is_not_read_yet(make_database: Callable[[], str]) -> None:
    """Ledgers 100–110 of the market are to be read again: a trade that is
    stored there is drawn, and the strip under it says that nothing is
    claimed for those ledgers."""
    pilot = loaded(make_database)
    try:
        with Session(pilot.engine) as session, session.begin():
            market = pilot.jobs("market")[0].key
            ranges = coverage_store.invalidate(session, market, "horizon:pool", 100, 110)
        assert [(piece.from_ledger, piece.to_ledger, piece.state) for piece in ranges] == [(100, 110, "pending"), (111, 130, "complete")]
        with behind_the_door(settings_for(pilot.engine.url.render_as_string(hide_password=False))) as client:
            page = chart_of(client, RECT, EUR, period="own", **{"from": "2026-01-01", "to": "2026-01-02"}, resolution="60000")
            assert "Не прочитано</dt><dd>11" in page and "Проверено</dt><dd>20" in page
            assert ">pending</span>" in page and ">complete</span>" in page and "100–110" in page and "111–130" in page
            assert 'class="cx-pending"' in page and 'class="cx-complete"' in page
            # The trade of ledger 104 is drawn — the bucket that starts at the period's start — over the pending part.
            pending = re.search(r'<rect class="cx-pending" x="([\d.]+)"[^>]*width="([\d.]+)"', page)
            candle = re.search(r'<g class="cx-bucket" data-ts="' + str(T0_MS) + r'"[^>]*>.*?<rect class="cx-(?:up|down)" x="([\d.]+)"', page, re.S)
            assert pending and candle and float(pending[1]) <= float(candle[1]) < float(pending[1]) + float(pending[2])
            assert "по номеру разделяющего их леджера" in page and "Сделка всегда стоит на полосе своего диапазона" in page
    finally:
        pilot.close()


# -- the door ----------------------------------------------------------------------------


def test_no_post_of_these_pages_does_anything_without_the_form_s_token(make_database: Callable[[], str]) -> None:
    site = a_site(make_database)
    one = FakeHorizon()
    try:
        with TestClient(create_app(site.settings)) as client:
            for path in ("/admin/notifier", "/admin/horizon", "/admin/chart"):
                assert client.get(path, follow_redirects=False).status_code == 303, "behind the door"
            assert client.post("/admin/login", data={"secret": SECRET}, follow_redirects=False).status_code == 303
            for path, data in (
                ("/admin/notifier/look", {}), ("/admin/notifier/reconcile", {}), ("/admin/notifier/retry", {"row": "1"}),
                ("/admin/notifier/adopt", {"kind": "account", "key": A, "id": "abc"}), ("/admin/notifier/1/release", {}),
                ("/admin/horizon/check", {"repeats": "1"}),
            ):
                for sent in ({}, {"csrf": "0" * 64}):
                    answer = client.post(path, data={**data, **sent}, follow_redirects=False)
                    assert answer.status_code == 403 and "токен" in answer.text, (path, sent)
            assert service_jobs.recent(site.engine) == [] and rows_of(site) == {} and one.requests == []
            # With it, the same post is heard — and still only queued.
            token = csrf_of(client.get("/admin/horizon"))
            assert client.post("/admin/horizon/check", data={"repeats": "1", "csrf": token}, follow_redirects=False).status_code == 303
            assert len(service_jobs.recent(site.engine)) == 1 and one.requests == []
    finally:
        site.close()
