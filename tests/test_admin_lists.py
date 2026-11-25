"""The watch lists and the mini-explorer as a browser meets them: accounts,
assets and markets brought in by hand, each rule switched by its own form;
discovery passes put in the queue and never run by a request; a legacy
account given its import; and what Horizon says now of an account or of an
issuer, read only when it is asked for and stored nowhere.
"""

from __future__ import annotations

import html
import re
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from os.path import commonprefix
from typing import Any
from urllib.parse import parse_qsl, parse_qs, quote, urlsplit

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app import cli_common
from app.admin import lists
from app.admin.areas import explore as explorer
from app.api.app import create_app
from app.config import PUBLIC_NETWORK_PASSPHRASE, Settings, shown_url
from app.storage import service_jobs
from app.storage.models import (
    DiscoveryRun,
    HistoryJob,
    HorizonNodeState,
    NotifierEvent,
    ServiceJob,
    TrackedAccount,
    TrackedAsset,
    TrackedMarket,
)
from app.subscriptions import desired
from app.tasks.runner import TaskRunner
from tests.factories import add_operation, add_participants, add_transaction, address
from tests.fake_horizon import A, B, C, D, ISSUER, LATEST, MTL, POOL, RECT, EUR, FakeHorizon, serve
from tests.test_tasks import runner_for
from tests.test_watch_lifecycle import Life

SECRET = "a-secret-of-the-admin"
EVIL = "<script>alert('x')</script>"
ADDRESS = re.compile(r"G[A-Z0-9]{55}")
AT = datetime(2026, 11, 25, 12, 0, tzinfo=UTC)

WHERE_EUR = f"/admin/assets/{quote(EUR, safe='')}"
WHERE_MTL = f"/admin/assets/{quote(MTL, safe='')}"
# The market of the watch list, by its canonical pair (the lower asset first).
WHERE_PAIR = f"/admin/markets/{quote(EUR, safe='')}/{quote(RECT, safe='')}"
MARKET = f"{EUR}>{RECT}"


def settings_for(url: str, fake: FakeHorizon, **changes: Any) -> Settings:
    return Settings(
        **{
            "database_url": url,
            "network_passphrase": PUBLIC_NETWORK_PASSPHRASE,
            "admin_secret": SECRET,
            "horizon_urls": (fake.origin,),
            "horizon_requests_per_second": 1000,
            **changes,
        }
    )


def login(client: TestClient) -> Any:
    return client.post("/admin/login", data={"secret": SECRET}, follow_redirects=False)


def csrf_of(page: Any) -> str:
    assert page.status_code == 200, page.text
    found = re.search(r'name="csrf" value="([0-9a-f]{64})"', page.text)
    assert found, "the page carries the form's token"
    return found.group(1)


def post(client: TestClient, token: str, path: str, **fields: str) -> Any:
    """A form with its token, not followed: what came of it is in the answer."""
    return client.post(path, data={"csrf": token, **fields}, follow_redirects=False)


def said(answer: Any) -> str:
    """The notice — or the error — a redirect carries in its query."""
    assert answer.status_code == 303, answer.text
    query = parse_qs(urlsplit(answer.headers["location"]).query)
    return (query.get("notice") or query.get("error") or [""])[0]


def accounts_by_address(monkeypatch: pytest.MonkeyPatch, fake: FakeHorizon, pool_shares: dict[str, str] | None = None) -> None:
    """The fake Horizon, answering also what the explorer asks of one account:
    ``/accounts/{id}`` — the record the fake builds for its lists (XLM and the
    trustlines), with a share of a pool where ``pool_shares`` names its pool.
    Everything else is answered as the fake answers it."""

    def handle(request: httpx.Request) -> httpx.Response:
        url = urlsplit(str(request.url))
        parts = [part for part in url.path.split("/") if part]
        if len(parts) != 2 or parts[0] != "accounts" or not ADDRESS.fullmatch(parts[1]):
            return fake._handle(request)
        account = parts[1]
        fake.requests.append((url.path, dict(parse_qsl(url.query))))
        pool_id = (pool_shares or {}).get(account)
        if pool_id is None and account not in (A, B, C, D, ISSUER) and all(account not in lines for lines in fake.trustlines.values()):
            return fake._problem(404)  # no such account, as the source says so
        record = fake._account_record(account)
        if pool_id is not None:
            record["balances"].append(
                {
                    "balance": "1.0000000",
                    "limit": "922337203685.4775807",
                    "last_modified_ledger": fake.latest - 1,
                    "asset_type": "liquidity_pool_shares",
                    "liquidity_pool_id": pool_id,
                }
            )
        return httpx.Response(200, headers={"content-type": "application/hal+json; charset=utf-8"}, json=record)

    monkeypatch.setattr(cli_common, "TRANSPORTS", {shown_url(fake.origin): httpx.MockTransport(handle)})


class Site:
    """A private database with the watch list — its accounts ``legacy``, as an
    installation from before stage 05 has them — a fake Horizon and everything
    a pass needs; and the admin UI on it, opened."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, life: Life, **settings: Any) -> None:
        self.life, self.fake = life, life.fake
        serve(monkeypatch, self.fake)  # as a process reads the server: the explorer's pool reads it so
        self.settings = settings_for(life.engine.url.render_as_string(hide_password=False), self.fake, **settings)
        self.client = TestClient(create_app(self.settings))
        assert login(self.client).status_code == 303

    @property
    def engine(self) -> Any:
        return self.life.engine

    def close(self) -> None:
        self.client.close()

    def token(self, path: str = "/admin/accounts") -> str:
        return csrf_of(self.client.get(path))

    def tables(self) -> dict[str, int]:
        """How many rows every table has: to see that a page moved none."""
        with self.engine.connect() as connection:
            names = [name for (name,) in connection.execute(text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'"))]
            return {name: connection.execute(text(f'SELECT count(*) FROM "{name}"')).scalar_one() for name in names}

    def assets(self) -> dict[str, tuple[bool, bool, str]]:
        """The assets of the list with their two rules and their reason."""
        with Session(self.engine) as session:
            rows = session.scalars(select(TrackedAsset).order_by(TrackedAsset.asset_id))
            return {row.asset_id: (row.enabled, row.discover, row.reason) for row in rows}

    def markets(self) -> dict[tuple[str, str], tuple[bool, str]]:
        with Session(self.engine) as session:
            rows = session.scalars(select(TrackedMarket).order_by(TrackedMarket.base_asset_id))
            return {(row.base_asset_id, row.counter_asset_id): (row.enabled, row.reason) for row in rows}

    def jobs(self, kind: str) -> dict[str, HistoryJob]:
        """The imports of a kind, by subject, as plain data."""
        with Session(self.engine) as session:
            rows = session.scalars(select(HistoryJob).where(HistoryJob.kind == kind))
            return {row.subject: HistoryJob(**{c.name: getattr(row, c.name) for c in HistoryJob.__table__.columns}) for row in rows}

    def tasks(self) -> list[ServiceJob]:
        return service_jobs.recent(self.engine, limit=200)

    def asked(self) -> list[str]:
        return [path for path, _ in self.fake.requests]


@pytest.fixture
def site(make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch) -> Iterator[Site]:
    life = Life(make_database())
    opened = Site(monkeypatch, life)
    try:
        yield opened
    finally:
        opened.close()
        life.close()


def an_event(site: Site, account: str, *, ledger: int, at: datetime, state: str = "queued", name: str = "") -> None:
    """A delivery of the Notifier that names ``account``, kept as the journal
    keeps one."""
    with Session(site.engine) as session, session.begin():
        session.add(
            NotifierEvent(
                id=name or f"event-{ledger}-{account[-6:]}", subscription="sub-1", type="operation", received_at=at,
                operation_type="payment", account=account, accounts=[account], ledger_sequence=ledger, matched=[], state=state,
                payload={}, headers={},
            )
        )


def a_stored_transaction(site: Site, ledger: int, *accounts: str) -> None:
    with Session(site.engine) as session, session.begin():
        transaction = add_transaction(session, ledger, source_account=accounts[0])
        add_participants(session, transaction, add_operation(session, transaction), *accounts)


def asset_rows(page: Any) -> int:
    """How many assets a page of the assets list shows: one row an asset."""
    assert page.status_code == 200, page.text
    return sum(1 for chunk in page.text.split("<tr>") if 'href="/admin/assets/' in chunk)


def runner(site: Site) -> TaskRunner:
    """A runner of tasks, as a worker's background thread is."""
    return runner_for(site.life)


def listed(page: Any) -> dict[str, str]:
    """Of a page of accounts: each account's row of the table, by address."""
    rows: dict[str, str] = {}
    for chunk in page.text.split("<tr>"):
        found = re.search(r'href="/admin/accounts/(G[A-Z0-9]{55})"', chunk)
        if found:
            rows.setdefault(found.group(1), chunk)
    return rows


# -- accounts ------------------------------------------------------------------------


def test_an_account_comes_in_by_hand_with_a_reason_and_a_wrong_address_brings_in_nothing(site: Site) -> None:
    new, other, token = address(20), address(21), site.token()
    before = site.tables()
    wrong = post(site.client, token, "/admin/accounts/add", addresses=f"{new}\nnot-an-address", reason="why")
    assert wrong.status_code == 303 and wrong.headers["location"].startswith("/admin/accounts?error=")
    assert "не адрес аккаунта" in said(wrong) and site.tables() == before, "one wrong address among several: nothing is added"
    for given in ("", "  "):
        assert "не указан ни один адрес" in said(post(site.client, token, "/admin/accounts/add", addresses=given, reason="why"))
    assert "нужна причина" in said(post(site.client, token, "/admin/accounts/add", addresses=new, reason=" "))
    assert site.tables() == before

    added = post(site.client, token, "/admin/accounts/add", addresses=f"{new}\n{other}, {new} ", reason="pays us")
    assert added.headers["location"].startswith("/admin/accounts?notice=")
    assert "добавлено новых 2" in said(added) and "проход воркера" in said(added)
    for account in (new, other):
        tracked = site.life.account(account)
        assert tracked is not None and (tracked.enabled, tracked.origin, tracked.boundary_due) == (True, "manual", True)
        assert (tracked.reason, tracked.watch_since_ledger, tracked.discovered_by) == ("pays us", None, None)

    again = post(site.client, token, "/admin/accounts/add", addresses=new, reason="once more")
    assert again.headers["location"].startswith(f"/admin/accounts/{new}?notice=") and "уже в списке и включён" in said(again)
    kept = site.life.account(new)
    assert kept is not None and kept.reason == "pays us", "what is in the list already is left as it is"
    third = address(22)
    one = post(site.client, token, "/admin/accounts/add", addresses=third, reason="by hand")
    assert one.headers["location"].startswith(f"/admin/accounts/{third}?notice=") and "Аккаунт добавлен" in said(one)

    def shown(path: str) -> set[str]:
        page = site.client.get(path)
        assert page.status_code == 200, page.text
        return set(listed(page))

    assert shown("/admin/accounts") == {A, B, C, new, other, third}
    start = new[: len(commonprefix([new, other])) + 1]
    assert shown(f"/admin/accounts?q={start}") == {new}, "by the start of the address"
    assert shown("/admin/accounts?origin=manual") == {new, other, third}
    assert shown("/admin/accounts?enabled=0") == {C}
    assert shown("/admin/accounts?origin=legacy") == {A, B, C}
    assert shown("/admin/accounts?enabled=1&origin=legacy") == {A, B}
    refused = site.client.get("/admin/accounts?q=alice")
    assert refused.status_code == 400 and "не начало адреса" in refused.text
    assert site.client.get(f"/admin/accounts/{new}").status_code == 200
    missing = site.client.get(f"/admin/accounts/{address(99)}")
    assert missing.status_code == 404 and "не в списке наблюдения" in missing.text
    assert site.client.get("/admin/accounts/not-an-address").status_code == 404

    # A long list is shown a page at a time.
    many = [address(1000 + number) for number in range(55)]
    assert "добавлено новых 55" in said(post(site.client, token, "/admin/accounts/add", addresses=" ".join(many), reason="a page"))
    second = site.client.get("/admin/accounts?page=2")
    assert second.status_code == 200 and "страница 2 из 2" in second.text and "61" in second.text
    rest, first = set(listed(second)), set(listed(site.client.get("/admin/accounts")))
    assert len(rest) == 11 and not rest & first, "the second page holds the rest, and only the rest"
    assert rest | first == {A, B, C, new, other, third, *many}


def test_a_switched_off_account_keeps_its_history_and_a_pass_pauses_its_import_and_retires_its_own_subscription(
    site: Site,
) -> None:
    # A has an import, a subscription and stored history; so has B; the assets and the market are as they were.
    token = site.token(f"/admin/accounts/{A}")
    assert post(site.client, token, f"/admin/accounts/{A}/history").status_code == 303
    lists.enqueue_history(site.engine, lists.ACCOUNT, B)
    for _ in range(4):
        site.life.pass_()
    a_stored_transaction(site, 118, A, B)
    a_stored_transaction(site, 104, A)
    an_event(site, A, ledger=110, at=AT)
    an_event(site, B, ledger=118, at=AT + timedelta(seconds=5))
    assets, markets, before = site.assets(), site.markets(), site.tables()

    off = post(site.client, token, f"/admin/accounts/{A}/enabled", enabled="0")
    assert "Аккаунт выключен" in said(off)
    tracked = site.life.account(A)
    assert tracked is not None and tracked.enabled is False and tracked.boundary_due is False
    assert site.life.job(A) is not None and site.life.job(A).state != "paused", "the request changed the flag alone"
    assert site.tables() == before, "nothing is deleted: the history stays"

    site.life.pass_()
    site.life.pass_()
    job = site.life.job(A)
    assert job is not None and (job.state, job.paused_because) == ("paused", "account disabled")
    assert site.life.rows_state(A) in (set(), {"retired"}) and not any(
        sub.get("account") == A for sub in site.life.api_fake.subscriptions
    )
    assert site.life.rows_state(B) == {"active"} and site.life.job(B) is not None and site.life.job(B).state != "paused"
    assert site.assets() == assets and site.markets() == markets, "assets and markets are independent of the account"
    assert site.tables() == before

    page = site.client.get(f"/admin/accounts/{A}")
    assert page.status_code == 200 and "выключен" in page.text and "paused" in page.text
    assert "payment" in page.text and "Новый леджер в базе" in page.text and "<dd>118 <" in page.text

    token = csrf_of(page)
    on = post(site.client, token, f"/admin/accounts/{A}/enabled", enabled="1")
    assert "включён снова" in said(on)
    tracked = site.life.account(A)
    assert tracked is not None and tracked.enabled and tracked.boundary_due
    assert "уже был в этом состоянии" in said(post(site.client, token, f"/admin/accounts/{A}/enabled", enabled="1"))
    assert "не сказано" in said(post(site.client, token, f"/admin/accounts/{A}/enabled", enabled="maybe"))
    stranger = post(site.client, token, f"/admin/accounts/{address(99)}/enabled", enabled="1")
    assert stranger.headers["location"].startswith("/admin/accounts?error=") and "не в списке наблюдения" in said(stranger)


def test_the_reason_of_an_account_changes_and_nothing_else_of_it(site: Site) -> None:
    before = site.life.account(A)
    assert before is not None
    listing = site.client.get("/admin/accounts")
    assert f'action="/admin/accounts/{A}/reason"' in listed(listing)[A], "the reason changes where the account is listed"
    token = csrf_of(listing)
    assert "нужна причина" in said(post(site.client, token, f"/admin/accounts/{A}/reason", reason=""))
    assert "Причина изменена" in said(post(site.client, token, f"/admin/accounts/{A}/reason", reason="  pays   the rent "))
    after = site.life.account(A)
    assert after is not None and after.reason == "pays the rent"
    columns = [c.name for c in TrackedAccount.__table__.columns if c.name not in ("reason", "updated_at")]
    assert {name: getattr(after, name) for name in columns} == {name: getattr(before, name) for name in columns}
    assert "pays the rent" in site.client.get(f"/admin/accounts/{A}").text


def test_a_legacy_account_is_given_its_import_by_hand_and_the_pass_gives_it_the_whole_life(site: Site) -> None:
    legacy = site.life.account(A)
    assert legacy is not None and legacy.origin == "legacy" and site.life.job(A) is None and site.life.rows_state(A) == set()
    page = site.client.get(f"/admin/accounts/{A}")
    assert f'action="/admin/accounts/{A}/history"' in page.text and "Поставить историческую загрузку" in page.text

    # What the pool is known to have kept at its last check; nothing newer is asked for here.
    with Session(site.engine) as session, session.begin():
        session.add(HorizonNodeState(url=shown_url(site.fake.origin), position=1, latest_ledger=125, checked_at=AT))
    answer = post(site.client, csrf_of(page), f"/admin/accounts/{A}/history")
    assert "задание #1" in said(answer) and "ждёт границу начала" in said(answer)
    tracked = site.life.account(A)
    assert tracked is not None and tracked.boundary_due is True and tracked.watch_since_ledger is None
    job = site.life.job(A)
    assert job is not None and (job.id, job.state, job.to_ledger, job.boundary_final) == (1, "waiting", 125, False)
    assert site.asked() == [], "the request asked no server"
    waiting = site.client.get(f"/admin/accounts/{A}").text
    assert 'action="/admin/accounts/{A}/history"' not in waiting and "ждёт границу начала" in waiting

    site.life.pass_()
    tracked, job = site.life.account(A), site.life.job(A)
    assert tracked is not None and job is not None
    assert (tracked.watch_since_ledger, tracked.boundary_due) == (LATEST, False)
    assert (job.id, job.to_ledger, job.boundary_final) == (1, LATEST, False), "the same import, its boundary raised"
    assert site.life.rows_state(A) == {"error"}  # registered — and on the word of a create nothing is active yet
    site.life.pass_()
    assert site.life.rows_state(A) == {"active"}
    site.life.pass_()
    job = site.life.job(A)
    assert job is not None and job.boundary_final, "told of: the upper boundary is taken"

    # Pressed again: the import is planned again and nothing is reset; a boundary once taken is not taken again.
    again = post(site.client, site.token(f"/admin/accounts/{A}"), f"/admin/accounts/{A}/history")
    assert "границы расширены, прогресс не сброшен" in said(again)
    kept = site.life.account(A)
    assert site.life.job(A) is not None and site.life.job(A).id == 1 and site.life.job(A).boundary_final
    assert kept is not None and (kept.boundary_due, kept.watch_since_ledger) == (False, LATEST)
    assert 'action="/admin/accounts/{A}/history"' not in site.client.get(f"/admin/accounts/{A}").text

    # An account that came in by hand has no such button: it had this road from the start.
    new = address(20)
    post(site.client, site.token(), "/admin/accounts/add", addresses=new, reason="by hand")
    assert site.life.account(new) is not None and site.life.account(new).origin == "manual"  # type: ignore[union-attr]
    assert "/history\"" not in site.client.get(f"/admin/accounts/{new}").text
    # A switched-off account is not given an import.
    off = post(site.client, site.token(), f"/admin/accounts/{C}/history")
    assert off.headers["location"].startswith(f"/admin/accounts/{C}?error=") and "выключен" in said(off)
    assert site.life.job(C) is None


def test_an_account_shows_its_newest_stored_ledger_and_its_last_events_from_among_the_newest_deliveries(site: Site) -> None:
    a_stored_transaction(site, 118, A, B)
    a_stored_transaction(site, 104, A)
    an_event(site, A, ledger=110, at=AT, state="queued")
    an_event(site, A, ledger=100, at=AT - timedelta(hours=1), state="unmatched")
    an_event(site, C, ledger=128, at=AT + timedelta(minutes=1))
    rows = listed(site.client.get("/admin/accounts"))
    assert "operation · леджер 110 · queued" in rows[A] and "operation · леджер 100 · unmatched" in rows[A]
    assert "operation · леджер 128" in rows[C] and "леджер 128" not in rows[A] and "леджер 100" not in rows[C]
    assert "нет среди последних" in rows[B], "B is named by no delivery: it says so"
    detail = site.client.get(f"/admin/accounts/{A}").text
    assert "Новый леджер в базе" in detail and "<dd>118 <" in detail
    assert "<td>payment</td>" in detail and '<td class="num">110</td>' in detail

    # The journal is looked through only as far as its newest deliveries: an account nothing recent names shows fewer.
    with Session(site.engine) as session, session.begin():
        session.add_all(
            NotifierEvent(
                id=f"flood-{number}", subscription="sub-1", type="operation", received_at=AT + timedelta(hours=2, microseconds=number),
                account=B, accounts=[B], matched=[], state="queued", payload={}, headers={},
            )
            for number in range(lists.EVENT_WINDOW + 1)
        )
    rows = listed(site.client.get("/admin/accounts"))
    assert "operation · леджер 110" not in rows[A] and "нет среди последних" in rows[A]
    assert "operation · леджер" in rows[B], "the newest deliveries are B's"
    assert "Среди последних 2000 полученных уведомлений" in site.client.get(f"/admin/accounts/{A}").text


# -- assets and discovery ------------------------------------------------------------


def test_an_asset_is_added_with_its_rules_each_changes_apart_and_native_is_never_discovered(site: Site) -> None:
    token = site.token("/admin/assets")
    refused = post(site.client, token, "/admin/assets/add", asset="native", discover="1", reason="x")
    assert refused.headers["location"].startswith("/admin/assets?error=") and "native (XLM) нельзя пометить для discovery" in said(refused)
    for given, words in (
        ("", "не указан актив"), ("FOO", "не актив"), ("eurmtl:xyz", "не адрес эмитента"), (f"EURMTL:{ISSUER}:x", "не адрес эмитента"),
        (f"eu rmtl:{ISSUER}", "не код актива"),
    ):
        answer = post(site.client, token, "/admin/assets/add", asset=given, reason="x")
        assert answer.headers["location"].startswith("/admin/assets?error=") and words in said(answer), given
    assert "нужна причина" in said(post(site.client, token, "/admin/assets/add", asset=MTL, reason=""))
    before = site.assets()
    assert "native" not in before and MTL not in before

    # Discovery only: the rule of history is left off, as the form says.
    added = post(site.client, token, "/admin/assets/add", asset=MTL, discover="1", reason="who holds it")
    assert added.headers["location"].startswith(WHERE_MTL + "?notice=") and "не помечен" not in said(added)
    assert site.assets()[MTL] == (False, True, "who holds it") and {name: value for name, value in site.assets().items() if name != MTL} == before
    with Session(site.engine) as session:
        want = desired.wanted(session)[("asset", MTL)]
    assert want.rules == ("discovery",), "a discovery-only asset needs its subscription too"
    assert asset_rows(site.client.get("/admin/assets?discover=1")) == 1, "the two flags are filtered apart"
    assert asset_rows(site.client.get("/admin/assets?enabled=0")) == 1
    page = site.client.get(WHERE_MTL)
    assert page.status_code == 200 and "правило discovery" in page.text
    assert f'action="{WHERE_MTL}/run"' in page.text and "Предпросмотр (dry run)" in page.text and "Полнота не обещается" in page.text
    assert "Найти / обновить держателей (запустить проход)" in page.text
    assert "Найти / обновить держателей" in site.client.get("/admin/assets").text, "and from the list, for this one asset"

    token = csrf_of(page)
    on = post(site.client, token, WHERE_MTL + "/enabled", enabled="1")
    assert "включён" in said(on) and site.assets()[MTL] == (True, True, "who holds it"), "the other rule is not touched"
    off = post(site.client, token, WHERE_MTL + "/discover", discover="0")
    assert "больше не помечен" in said(off) and site.assets()[MTL] == (True, False, "who holds it")
    assert "не сказано" in said(post(site.client, token, WHERE_MTL + "/discover", discover="maybe"))
    assert site.assets()[EUR] == before[EUR], "another asset is not touched"
    missing = post(site.client, token, f"/admin/assets/{quote(f'AAA:{ISSUER}', safe='')}/enabled", enabled="0")
    assert "нет в списке наблюдения" in said(missing)
    # Adding what is there changes nothing — neither rule.
    again = post(site.client, token, "/admin/assets/add", asset=MTL, enabled="1", discover="1", reason="other")
    assert "уже был в списке наблюдения" in said(again) and site.assets()[MTL] == (True, False, "who holds it")

    # Native: watched for its history, never for discovery.
    assert post(site.client, token, "/admin/assets/add", asset="native", enabled="1", reason="lumens").status_code == 303
    assert site.assets()["native"] == (True, False, "lumens")
    never = post(site.client, csrf_of(site.client.get("/admin/assets/native")), "/admin/assets/native/discover", discover="1")
    assert "native (XLM) нельзя пометить для discovery" in said(never) and site.assets()["native"] == (True, False, "lumens")
    native = site.client.get("/admin/assets/native").text
    assert 'action="/admin/assets/native/discover"' not in native and "/run\"" not in native

    page = site.client.get("/admin/assets").text
    assert f'href="/admin/assets/{quote(MTL, safe="")}"' in page and 'href="/admin/assets/native"' in page
    assert "правило «включён»" in page, "MTL is enabled now, and no longer discovered"
    assert asset_rows(site.client.get("/admin/assets?q=mtl")) == 3, "MTL, MTLRECT and EURMTL have it in their code"
    assert asset_rows(site.client.get("/admin/assets?discover=1")) == 0, "nothing is marked now"
    assert asset_rows(site.client.get("/admin/assets?enabled=0")) == 0, "and nothing is switched off"
    assert site.client.get(f"/admin/assets/{quote('foo:bar', safe='')}").status_code == 404
    assert site.client.get(f"/admin/assets/{quote(f'ZZZ:{ISSUER}', safe='')}").status_code == 404


def test_a_dry_run_and_a_pass_are_put_in_the_queue_by_the_buttons_and_never_run_by_the_request(site: Site) -> None:
    token = site.token(WHERE_EUR)
    for bad in ("0", "abc", "", "1001", "2.5", "-1"):
        refused = post(site.client, token, WHERE_EUR + "/run", dry_run="1", max_requests=bad)
        assert refused.headers["location"].startswith(WHERE_EUR + "?error=") and "бюджет" in said(refused), bad
    assert site.tasks() == [] and site.asked() == []

    dry = post(site.client, token, WHERE_EUR + "/run", dry_run="1", max_requests="5")
    location = dry.headers["location"]
    assert location.startswith(WHERE_EUR + "?notice=") and "task=1" in location
    assert "Сухой проход поставлен в очередь" in said(dry) and "задача #1" in said(dry)
    (task,) = site.tasks()
    assert (task.kind, task.state, task.params, task.one_at_a_time, task.requested_by) == (
        "discovery", "queued", {"asset": EUR, "dry_run": True, "resume": False, "max_requests": 5}, EUR, "admin"
    )
    page = site.client.get(location)  # the asset page the notice leads to
    assert page.status_code == 200 and 'href="/admin/tasks/1"' in page.text and "стоит в очереди" in page.text
    assert site.asked() == [], "the request ran nothing"

    # One at a time for an asset: a pass asked for now gives back the task that is there.
    again = post(site.client, token, WHERE_EUR + "/run", dry_run="0", max_requests="7")
    assert "task=1" in again.headers["location"] and "уже стоит в очереди или идёт" in said(again)
    assert [job.id for job in site.tasks()] == [1]

    # The dry run is out of the way: a pass goes in, with what the form said. (A number not taken is never used again.)
    service_jobs.request_cancel(site.engine, 1)
    a_pass = post(site.client, token, WHERE_EUR + "/run", dry_run="0", max_requests="7", resume="1")
    number = int(re.search(r"task=(\d+)", a_pass.headers["location"]).group(1))
    assert number != 1 and "Проход discovery поставлен" in said(a_pass)
    job = service_jobs.get(site.engine, number)
    assert job is not None and job.params == {"asset": EUR, "dry_run": False, "resume": True, "max_requests": 7}
    page = site.client.get(a_pass.headers["location"])
    assert f'href="/admin/tasks/{number}"' in page.text and "бюджет 7 запросов" in page.text and 'href="/admin/tasks/1"' in page.text
    assert site.asked() == [] and site.tables()["discovery_run"] == 0
    # What is not watched cannot be run from an asset's page.
    stranger = post(site.client, token, WHERE_MTL + "/run", dry_run="1", max_requests="5")
    assert "нет в списке наблюдения" in said(stranger) and [job.id for job in site.tasks()] == [number, 1]


def test_a_pass_started_from_the_page_adds_new_holders_and_leaves_a_switched_off_account_off(site: Site) -> None:
    holder, late, found = address(600), address(601), D
    site.fake.trustlines[EUR] = {holder: 10**7, late: 5, found: 0, ISSUER: 10**9}
    token = site.token()
    post(site.client, token, "/admin/accounts/add", addresses=found, reason="found long ago")
    post(site.client, site.token(f"/admin/accounts/{found}"), f"/admin/accounts/{found}/enabled", enabled="0")
    post(site.client, token, WHERE_EUR + "/discover", discover="1")
    assert site.assets()[EUR][:2] == (True, True), "the rule of history and the rule of discovery are apart, and both are on"
    answer = post(site.client, site.token(WHERE_EUR), WHERE_EUR + "/run", dry_run="0", max_requests="10", resume="1")
    (task,) = site.tasks()
    assert "task=1" in answer.headers["location"] and task.params == {"asset": EUR, "dry_run": False, "resume": True, "max_requests": 10}
    assert site.asked() == [] and site.life.account(holder) is None

    done = runner(site).once()
    assert done is not None and done.id == task.id and done.state == "done" and (done.result or {})["state"] == "complete"
    new = site.life.account(holder)
    assert new is not None and (new.enabled, new.origin, new.boundary_due, new.discovered_by) == (True, "discovery", True, EUR)
    assert site.life.account(late) is not None and site.life.account(late).enabled  # type: ignore[union-attr]
    kept = site.life.account(found)
    assert kept is not None and kept.enabled is False and (kept.discovered_by, kept.reason) == (EUR, "found long ago")
    assert site.life.account(ISSUER) is None, "the issuer is not added"
    page = site.client.get(WHERE_EUR).text
    assert "complete" in page and f'href="/admin/tasks/{task.id}"' in page and "<dd>3 <" in page  # found by it

    # A pass again changes no decision.
    post(site.client, site.token(WHERE_EUR), WHERE_EUR + "/run", dry_run="0", max_requests="10")
    again = runner(site).once()
    assert again is not None and again.state == "done" and (again.result or {})["added"] == 0
    kept = site.life.account(found)
    assert kept is not None and kept.enabled is False and kept.reason == "found long ago"
    assert site.life.account(holder) is not None and site.life.account(holder).created_at == new.created_at  # type: ignore[union-attr]

    # A found-by count is by the whole asset, not by the start of its id.
    long_code = f"AAAA:{ISSUER}"
    with Session(site.engine) as session, session.begin():
        session.get(TrackedAccount, late).discovered_by = f"{EUR},{long_code}"  # type: ignore[union-attr]
    post(site.client, site.token(), "/admin/assets/add", asset=f"AAA:{ISSUER}", reason="a code that is the start of another")
    found_by = {row.asset_id: row.found_by for row in lists.assets(site.engine, limit=100, offset=0)[0]}
    assert found_by[EUR] == 3 and found_by[f"AAA:{ISSUER}"] == 0


def test_the_discovery_page_shows_the_marked_assets_their_passes_the_tasks_and_the_schedule(
    make_database: Callable[[], str], site: Site, monkeypatch: pytest.MonkeyPatch
) -> None:
    page = site.client.get("/admin/discovery")
    assert page.status_code == 200 and "Ни один актив" in page.text
    assert "Расписание:" in page.text and "выключено" in page.text and "MTL_HORIZON_DISCOVERY_INTERVAL" in page.text
    post(site.client, site.token(), WHERE_EUR + "/discover", discover="1")
    page = site.client.get("/admin/discovery").text
    assert "EURMTL" in page and 'href="/admin/discovery"' in page
    assert page.count(f'action="{WHERE_EUR}/run"') == 2, "a dry run and a pass, both put in the queue"
    assert 'href="/admin/assets/' in page and "страница" in page

    post(site.client, site.token(WHERE_EUR), WHERE_EUR + "/run", dry_run="0", max_requests="10")
    task = runner(site).once()
    assert task is not None and task.state == "done"
    assert "продолжить" not in site.client.get(WHERE_EUR).text, "the last pass was whole: there is nothing to continue"
    with Session(site.engine) as session, session.begin():
        # The last pass over the asset did not finish: it is the newest one, after the pass that ran.
        session.add(
            DiscoveryRun(
                asset_id=EUR, state="incomplete", dry_run=False, started_at=datetime.now(UTC), finished_at=datetime.now(UTC),
                last_address=address(777), last_error="the pass used its 10 requests"
            )
        )
    page = site.client.get("/admin/discovery")
    assert "complete" in page.text and "incomplete" in page.text and "the pass used its 10 requests" in page.text
    assert f'href="/admin/tasks/{task.id}"' in page.text and "запросов" in page.text
    assert "продолжить" in page.text and address(777)[:8] in page.text, "where the pass that did not finish stopped"

    life = Life(make_database())
    timed = Site(monkeypatch, life, discovery_interval_seconds=3600)
    try:
        page = timed.client.get("/admin/discovery").text
        assert "раз в 1 ч" in page and "не редактируется" in page
    finally:
        timed.close()
        life.close()


# -- markets --------------------------------------------------------------------------


def test_a_market_is_one_pair_whichever_way_it_is_written_and_can_load_its_history_at_once(site: Site) -> None:
    token = site.token("/admin/markets")
    before = site.markets()
    existing = post(site.client, token, "/admin/markets/add", base=RECT, counter=EUR, reason="the other way round")
    assert existing.headers["location"].startswith(WHERE_PAIR + "?notice=") and "уже был в списке наблюдения" in said(existing)
    assert site.markets() == before and site.jobs("market") == {}
    for base, counter, words in ((EUR, EUR, "два разных актива"), ("foo", EUR, "не актив"), ("native", "native", "два разных актива")):
        answer = post(site.client, token, "/admin/markets/add", base=base, counter=counter, reason="x")
        assert answer.headers["location"].startswith("/admin/markets?error=") and words in said(answer), (base, counter)
    assert "нужна причина" in said(post(site.client, token, "/admin/markets/add", base="native", counter=MTL, reason=" "))

    where = f"/admin/markets/{quote(MTL, safe='')}/native"
    added = post(site.client, token, "/admin/markets/add", base="native", counter=MTL, enabled="1", history="1", reason="for lumens")
    assert added.headers["location"].startswith(where + "?notice=")
    assert "Рынок добавлен" in said(added) and "Загрузка истории сделок поставлена" in said(added)
    assert site.markets()[(MTL, "native")] == (True, "for lumens") and site.markets()[(EUR, RECT)] == before[(EUR, RECT)]
    job = site.jobs("market").get(f"{MTL}>native")
    assert job is not None and (job.state, job.to_ledger, job.boundary_final) == ("waiting", 1, False), "nothing is known of the pool yet"
    # The two assets are registered; neither is brought into the asset list by it.
    assert MTL not in site.assets() and "native" not in site.assets()

    for path in (where, f"/admin/markets/native/{quote(MTL, safe='')}", WHERE_PAIR, f"/admin/markets/{quote(RECT, safe='')}/{quote(EUR, safe='')}"):
        page = site.client.get(path)
        assert page.status_code == 200, path
        assert "то же, что" in page.text or "Записанный иначе" in page.text
    text = site.client.get(where).text
    assert f"<code>{MTL}&gt;native</code>" in text and "XLM / MTL" in text and "MTL / XLM" in text
    assert 'action="' + where + '/history"' in text and "включён" in text.lower()
    assert "Что покрывает подписка рынка" in text and "Operations Notifier здесь не меняется" in text
    assert site.client.get(f"/admin/markets/{quote(EUR, safe='')}/native").status_code == 404
    assert site.client.get("/admin/markets/foo/bar").status_code == 404

    page = site.client.get("/admin/markets").text
    assert 'href="' + WHERE_PAIR + '"' in page and f'href="{where}"' in page
    only = site.client.get("/admin/markets?q=native").text
    assert f'href="{where}"' in only and WHERE_PAIR not in only
    assert site.client.get("/admin/markets?enabled=0").text.count('href="/admin/markets/') == 0
    assert site.asked() == []


def test_switching_a_market_off_pauses_its_import_by_a_pass_and_touches_nothing_else(site: Site) -> None:
    page = site.client.get(WHERE_PAIR)
    token = csrf_of(page)
    answer = post(site.client, token, WHERE_PAIR + "/history")
    assert "Загрузка истории сделок поставлена" in said(answer) and "задание #1" in said(answer)
    assert site.asked() == []
    site.life.pass_()  # the pool is asked for its latest ledger, and the boundary is raised to it and made final
    job = site.jobs("market")[MARKET]
    assert (job.to_ledger, job.boundary_final) == (LATEST, True)
    with Session(site.engine) as session, session.begin():
        kept = session.get(HistoryJob, job.id)
        assert kept is not None
        kept.requests, kept.slices = 40, 3  # what it read before, as a slice would have counted it
    assets, before = site.assets(), site.tables()

    off = post(site.client, token, WHERE_PAIR + "/enabled", enabled="0")
    assert "Рынок выключен" in said(off)
    assert site.markets()[(EUR, RECT)][0] is False and site.jobs("market")[MARKET].state == "waiting", "the flag alone"
    page = site.client.get(WHERE_PAIR).text
    assert "Загрузить всю доступную историю сделок" not in page and "Включить рынок" in page
    site.life.pass_()
    job = site.jobs("market")[MARKET]
    assert (job.state, job.paused_because, job.requests, job.slices) == ("paused", "market disabled", 40, 3), "paused, with the reason of a market; what it read is kept"
    assert site.assets() == assets and site.tables() == before
    assert all(site.life.job(account) is None for account in (A, B, C)), "no account is touched"

    on = post(site.client, csrf_of(site.client.get(WHERE_PAIR)), WHERE_PAIR + "/enabled", enabled="1")
    assert "Рынок включён" in said(on) and "снова в очереди" in said(on)
    assert site.jobs("market")[MARKET].state == "waiting", "what the pass paused for the market being off goes back to work"
    again = post(site.client, site.token(WHERE_PAIR), WHERE_PAIR + "/history")
    assert "уже стояла — границы расширены, прогресс не сброшен" in said(again) and "задание #1" in said(again)
    job = site.jobs("market")[MARKET]
    assert (job.id, job.state, job.requests, job.slices, job.to_ledger) == (1, "waiting", 40, 3, LATEST)
    another = post(site.client, site.token(), "/admin/markets/add", base="native", counter=MTL, enabled="1", reason="one more")
    assert "Рынок добавлен" in said(another) and site.markets()[(MTL, "native")][0] is True
    site.life.pass_()
    assert site.jobs("market")[MARKET].boundary_final, "still final: planning again narrowed nothing"


# -- the explorer ---------------------------------------------------------------------


def test_the_explorer_shows_what_horizon_says_now_of_an_account_asks_it_once_and_says_how_it_is_watched(
    site: Site, monkeypatch: pytest.MonkeyPatch
) -> None:
    accounts_by_address(monkeypatch, site.fake, {B: POOL})
    site.fake.trustlines[EUR] = {B: 10**7, D: 10**7}
    site.fake.trustlines[RECT] = {B: 0}
    site.fake.trustlines[MTL] = {D: 5}
    page = site.client.get(f"/admin/explore?account={B}")
    assert page.status_code == 200
    assert site.asked() == ["/", f"/accounts/{B}"], "the server's root, and the account once"
    text = page.text
    for shown in ("XLM (native)", "10.0000000", "EURMTL", "1.0000000", "MTLRECT", "0.0000000", "доля пула ликвидности", POOL):
        assert shown in text, shown
    assert "Прочитано" in text and " UTC" in text and "не история" in text
    assert "<strong>да</strong>" in text and "включён" in text and f'href="/admin/accounts/{B}"' in text
    # What is watched here is the installation's word, asset by asset: both lines of B are watched, none discovered.
    rule = "«включён»: <span class=\"badge badge-ok\">да</span> · discovery: <span class=\"badge badge-mute\">нет</span>"
    assert text.count(rule) == 2 and 'action="/admin/accounts/add"' not in text
    assert f'href="/admin/explore?issuer={ISSUER}"' in text, "the way to the issuer"
    assert 'name="max_requests"' not in text and 'action="/admin/explore/run"' not in text

    # D holds an asset nobody watches here, and is not watched itself.
    page = site.client.get(f"/admin/explore?account={D}")
    text = page.text
    assert page.status_code == 200 and "<strong>нет</strong>" in text and 'action="/admin/accounts/add"' in text
    assert "Добавить актив" in text and f'value="{MTL}"' in text and "MTL" in text
    assert site.asked().count(f"/accounts/{D}") == 1
    token = csrf_of(page)
    answer = post(site.client, token, "/admin/accounts/add", addresses=D, reason=explorer.REASON)
    assert answer.headers["location"].startswith(f"/admin/accounts/{D}?notice=")
    watched_ = site.life.account(D)
    assert watched_ is not None and (watched_.enabled, watched_.origin, watched_.reason) == (True, "manual", explorer.REASON)
    added = post(site.client, token, "/admin/assets/add", asset=MTL, enabled="1", reason=explorer.REASON)
    assert added.headers["location"].startswith(WHERE_MTL + "?notice=") and site.assets()[MTL] == (True, False, explorer.REASON)

    # An address that is nobody's: the source says so, and the page says what the source said.
    page = site.client.get(f"/admin/explore?account={address(99)}")
    assert page.status_code == 200 and "не ответил" in page.text and "404" in page.text
    assert site.asked()[-1] == f"/accounts/{address(99)}"


def test_an_issuers_assets_are_read_page_by_page_through_this_pages_own_links(site: Site, monkeypatch: pytest.MonkeyPatch) -> None:
    site.fake.issued[ISSUER] = {"AAA", "BBB", "CCC", "DDD", "EURMTL"}
    site.fake.issued[address(77)] = {"ZZZ"}
    monkeypatch.setattr(explorer, "PAGE", 2)

    def codes(page: Any) -> list[str]:
        assert page.status_code == 200, page.text
        return re.findall(r'data-copy="([A-Z0-9]+):', page.text)

    def link(page: Any, words: str) -> str:
        found = re.search(rf'<a href="([^"]+)">{words}</a>', page.text)
        assert found, words
        return html.unescape(found.group(1))

    first = site.client.get(f"/admin/explore?issuer={ISSUER}")
    assert codes(first) == ["AAA", "BBB"] and "страница 1" in first.text and "← назад" not in first.text
    second = site.client.get(link(first, "дальше →"))
    assert codes(second) == ["CCC", "DDD"] and "страница 2" in second.text
    third = site.client.get(link(second, "дальше →"))
    assert codes(third) == ["EURMTL"] and "страница 3" in third.text and "дальше →" not in third.text
    assert "«включён»" in third.text, "watched here: said by the installation, not by the source"
    back = site.client.get(link(third, "← назад"))
    assert codes(back) == ["CCC", "DDD"] and "страница 2" in back.text
    start = site.client.get(link(back, "к первой странице"))
    assert codes(start) == ["AAA", "BBB"] and "страница 1" in start.text

    asked = site.fake.asked("/assets")
    assert [query.get("cursor") for query in asked] == [None, f"BBB_{ISSUER}", f"DDD_{ISSUER}", f"BBB_{ISSUER}", None]
    assert all(query == {"order": "asc", "limit": "2", "asset_issuer": ISSUER, **({"cursor": query["cursor"]} if "cursor" in query else {})} for query in asked)
    assert "ZZZ" not in first.text + second.text + third.text
    assert f'href="/admin/explore?account={ISSUER}"' in first.text


def test_a_dry_run_is_put_in_the_queue_from_an_issuers_list_and_nothing_is_read_or_written(
    site: Site, monkeypatch: pytest.MonkeyPatch
) -> None:
    site.fake.issued[ISSUER] = {"AAA", "BBB"}
    page = site.client.get(f"/admin/explore?issuer={ISSUER}")
    asked, before = len(site.fake.requests), site.tables()
    asset = f"AAA:{ISSUER}"
    bad = post(site.client, csrf_of(page), "/admin/explore/run", asset=asset, max_requests="0")
    assert bad.headers["location"].startswith("/admin/explore?error=") and "бюджет" in said(bad)
    answer = post(site.client, csrf_of(page), "/admin/explore/run", asset=asset, max_requests="3")
    location = answer.headers["location"]
    assert location.startswith("/admin/explore?") and "task=1" in location and "Сухой проход поставлен" in said(answer)
    (task,) = site.tasks()
    assert (task.state, task.params, task.one_at_a_time) == ("queued", {"asset": asset, "dry_run": True, "resume": False, "max_requests": 3}, asset)
    after = site.tables()
    assert {name: count for name, count in after.items() if name != "service_job"} == {
        name: count for name, count in before.items() if name != "service_job"
    }
    assert after["service_job"] == before["service_job"] + 1
    shown = site.client.get(location)
    assert shown.status_code == 200 and 'href="/admin/tasks/1"' in shown.text and 'data-copy="' not in shown.text
    assert len(site.fake.requests) == asked, "neither the form nor the page it leads to read anything"


def test_what_is_not_an_address_is_refused_before_anything_is_asked(site: Site, monkeypatch: pytest.MonkeyPatch) -> None:
    accounts_by_address(monkeypatch, site.fake)
    unknown = address(7)[:-1] + "A"  # a key whose checksum does not match: an address that is nobody's
    for query in (
        "account=abc", "account=G", "account=" + unknown, "issuer=" + unknown, f"account={A}&issuer={ISSUER}",
        f"issuer={ISSUER}&cursor=" + "x" * 200, f"issuer={ISSUER}&cursor=with%20space", f"issuer={ISSUER}&back=" + "%00",
        "account=" + A + "%00", "issuer=%2Fetc%2Fpasswd", "account=..%2Faccounts", "issuer=" + ISSUER + "&cursor=" + quote("a_b", safe="") + "&back=x&back=" + "y" * 200,
    ):
        page = site.client.get("/admin/explore?" + query)
        assert page.status_code == 400 and "Запрос не выполнен" in page.text, query
    assert site.fake.requests == []


def test_a_page_that_merely_opens_asks_no_server(site: Site, monkeypatch: pytest.MonkeyPatch) -> None:
    accounts_by_address(monkeypatch, site.fake, {B: POOL})
    site.fake.trustlines[EUR] = {B: 10**7}
    site.fake.issued[ISSUER] = {"AAA"}
    pages = (
        "/admin/accounts", f"/admin/accounts/{A}", "/admin/assets", WHERE_EUR, "/admin/markets", WHERE_PAIR,
        "/admin/discovery", "/admin/explore", "/admin/explore?account=&issuer=", "/admin/explore?task=1", "/admin/explore?notice=hi",
        "/admin/accounts?enabled=0&origin=manual", "/admin/assets?discover=1", "/admin/discovery?notice=hi",
    )
    for path in pages:
        page = site.client.get(path)
        assert page.status_code == 200, path
    assert site.fake.requests == []


def test_nothing_of_what_is_read_is_stored(site: Site, monkeypatch: pytest.MonkeyPatch) -> None:
    accounts_by_address(monkeypatch, site.fake, {B: POOL})
    site.fake.trustlines[EUR] = {D: 10**7}
    site.fake.trustlines[RECT] = {B: 0}
    site.fake.issued[ISSUER] = {"AAA", "BBB"}
    before = site.tables()
    assert site.client.get(f"/admin/explore?account={B}").status_code == 200
    assert site.client.get(f"/admin/explore?account={D}").status_code == 200
    assert site.client.get(f"/admin/explore?issuer={ISSUER}").status_code == 200
    after = site.tables()
    assert set(after) == set(before) | {"http_pace"}
    # The one row that moved is the pool's own pace, which every request takes a slot in.
    assert {name: count for name, count in after.items() if name != "http_pace"} == {
        name: count for name, count in before.items() if name != "http_pace"
    }
    assert D not in site.client.get("/admin/accounts").text and MTL not in site.assets()


def test_a_horizon_that_is_down_is_said_with_the_moment_of_the_attempt(make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeHorizon(down=httpx.ConnectError("no route to the server"))
    life = Life(make_database(), fake)
    site = Site(monkeypatch, life)
    try:
        page = site.client.get(f"/admin/explore?account={A}")
        assert page.status_code == 200
        assert "Horizon не ответил" in page.text and "no route to the server" in page.text
        shown = re.sub(r"<[^>]+>", "", page.text)  # the moment stands inside its <time>
        assert re.search(r"Попытка в \d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC", shown), "the error carries its time"
        assert [path for path, _ in fake.requests] == ["/"], "its root was asked for once, and then it was left alone"
        issuer = site.client.get(f"/admin/explore?issuer={ISSUER}")
        assert issuer.status_code == 200 and "Horizon не ответил" in issuer.text
    finally:
        site.close()
        life.close()


# -- every form and every page ---------------------------------------------------------


def test_no_form_changes_anything_without_its_token_or_for_a_stranger(site: Site) -> None:
    forms = [
        ("/admin/accounts/add", {"addresses": address(30), "reason": "x"}),
        (f"/admin/accounts/{A}/enabled", {"enabled": "0"}),
        (f"/admin/accounts/{A}/reason", {"reason": "x"}),
        (f"/admin/accounts/{A}/history", {}),
        ("/admin/assets/add", {"asset": MTL, "enabled": "1", "reason": "x"}),
        (WHERE_EUR + "/enabled", {"enabled": "0"}),
        (WHERE_EUR + "/discover", {"discover": "1"}),
        (WHERE_EUR + "/run", {"dry_run": "1", "max_requests": "5"}),
        ("/admin/markets/add", {"base": "native", "counter": MTL, "enabled": "1", "reason": "x", "history": "1"}),
        (WHERE_PAIR + "/enabled", {"enabled": "0"}),
        (WHERE_PAIR + "/history", {}),
        ("/admin/explore/run", {"asset": EUR, "max_requests": "5"}),
    ]
    before = site.tables()
    for path, fields in forms:
        for given in ({}, {"csrf": "0" * 64}):
            answer = site.client.post(path, data={**fields, **given}, follow_redirects=False)
            assert answer.status_code == 403, (path, given)
    assert site.tables() == before
    stranger = TestClient(create_app(site.settings))
    with stranger:
        for path, fields in forms:
            answer = stranger.post(path, data=fields, follow_redirects=False)
            assert answer.status_code == 303 and answer.headers["location"] == "/admin/login", path
        for path in ("/admin/accounts", "/admin/assets", "/admin/markets", "/admin/discovery", "/admin/explore", f"/admin/accounts/{A}"):
            answer = stranger.get(path, follow_redirects=False)
            assert answer.status_code == 303 and answer.headers["location"] == "/admin/login", path
    assert site.tables() == before


def test_every_page_escapes_what_it_is_given(site: Site) -> None:
    token = site.token()
    where = f"/admin/markets/{quote(MTL, safe='')}/native"
    post(site.client, token, "/admin/accounts/add", addresses=address(40), reason=EVIL)
    post(site.client, token, f"/admin/accounts/{A}/reason", reason=EVIL)
    post(site.client, token, "/admin/assets/add", asset=MTL, enabled="1", reason=EVIL)
    post(site.client, token, WHERE_MTL + "/discover", discover="1")  # to be on the discovery page too
    post(site.client, token, "/admin/assets/add", asset="native", enabled="1", reason=EVIL)
    post(site.client, token, "/admin/markets/add", base="native", counter=MTL, reason=EVIL)
    with Session(site.engine) as session, session.begin():
        session.get(TrackedAccount, B).reason = "<script>alert('x')</script>"  # type: ignore[union-attr]
    pages = [
        "/admin/accounts", f"/admin/accounts/{A}", f"/admin/accounts/{address(40)}", "/admin/assets", WHERE_MTL, "/admin/assets/native",
        "/admin/markets", where, "/admin/discovery", "/admin/accounts?q=GAAAA", f"/admin/accounts?notice={quote(EVIL)}",
        f"/admin/assets?error={quote(EVIL)}", f"/admin/explore?notice={quote(EVIL)}", f"/admin/markets?error={quote(EVIL)}",
        f"/admin/discovery?notice={quote(EVIL)}&task=1",
    ]
    for path in pages:
        page = site.client.get(path)
        assert page.status_code == 200, path
        assert EVIL not in page.text, path
        assert "&lt;script&gt;" in page.text, path
    # What a form refused is said back the same way.
    refused = post(site.client, token, "/admin/accounts/add", addresses=EVIL, reason="x")
    page = site.client.get(refused.headers["location"])
    assert page.status_code == 200 and EVIL not in page.text and "&lt;script&gt;" in page.text
    # And what is not an address, on the page that refuses it.
    page = site.client.get(f"/admin/explore?account={quote(EVIL)}")
    assert page.status_code == 400 and EVIL not in page.text and "&lt;script&gt;" in page.text
