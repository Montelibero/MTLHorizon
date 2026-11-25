"""The whole path of stage 06 through the pages, on fakes: an asset is marked
for discovery from the browser → its holders are looked for by a task → the
new accounts get their subscriptions and their history imports → the pass is
run again → everything is restarted → a notification comes through the
webhook (twice) and its transaction is brought in → the history import is
widened over the same ledger and stores nothing twice → the API answers, the
chart is drawn → and the account is switched off from its page while the
asset's subscription stays. Every step is what the owner does, in the order
they do it; nothing is asked of any server the fakes do not stand for.

With ``MTL_HORIZON_UI_DUMPS=<directory>`` the main pages are written there as
they were rendered, to be looked at.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.api.app import create_app
from app.config import Settings
from app.history.runner import HistoryWorker
from app.notifier import format as notifier_format
from app.storage import history_jobs, service_jobs
from app.storage.models import HistoryJob
from app.tasks.runner import TaskRunner
from app.watch import provision
from app.worker.loader import Worker
from tests.fake_horizon import A, D, EUR, RECT, S, paid, serve
from tests.fake_notifier import notification
from tests.test_watch_lifecycle import Life

SECRET = "the-secret-of-this-test"
TOKEN = "the-token-of-this-test"
# Two holders the fake Horizon knows and the watch list does not; the second
# holds nothing of the asset — a holder all the same.
HOLDERS = (D, S)


class Browser:
    """The owner at the keyboard: logs in, opens pages, sends forms."""

    def __init__(self, settings: Settings, dumps: Path | None) -> None:
        self.client = TestClient(create_app(settings))
        self.client.__enter__()
        self.dumps = dumps
        self.number = 0
        answer = self.client.post("/admin/login", data={"secret": SECRET}, follow_redirects=False)
        assert answer.status_code == 303 and answer.headers["location"] == "/admin"

    def close(self) -> None:
        self.client.__exit__(None, None, None)

    def token(self, page: str) -> str:
        answer = self.client.get(page)
        assert answer.status_code == 200, (page, answer.status_code, answer.text[:300])
        found = re.search(r'name="csrf" value="([0-9a-f]{64})"', answer.text)
        assert found, f"the page {page} carries the form's token"
        return found.group(1)

    def send(self, action: str, fields: dict[str, str], *, page: str = "/admin") -> str:
        """A form sent from ``page``: where it leads, with its notice read."""
        answer = self.client.post(action, data={"csrf": self.token(page), **fields}, follow_redirects=False)
        assert answer.status_code == 303, (action, answer.status_code, answer.text[:400])
        return str(answer.headers["location"])

    def page(self, name: str, path: str, **must: str) -> str:
        answer = self.client.get(path)
        assert answer.status_code == 200, (path, answer.status_code, answer.text[:400])
        for words in must.values():
            assert words in answer.text, f"{path}: «{words}» is not on the page"
        assert "Traceback" not in answer.text
        if self.dumps is not None:
            self.number += 1
            (self.dumps / f"{self.number:02d}-{name}.html").write_text(answer.text)
        return answer.text


def passes(life: Life, count: int, api: Any = None) -> None:
    for number in range(count):
        life.wall.wait(120)
        provision.run(life.engine, life.settings, client=life.another_pool(), api=api if api is not None else life.api(), now=life.wall())


def until_history_done(life: Life, ids: list[int], *, slices: int = 200) -> dict[int, Any]:
    with HistoryWorker(life.pool, life.engine, name="history", slice_requests=20, max_attempts=5, clock=life.wall) as history:
        for number in range(slices):
            history.once()
            with Session(life.engine) as session:
                states = {job_id: session.get(HistoryJob, job_id).state for job_id in ids}
            if all(state in ("done", "partial", "error", "paused") for state in states.values()):
                break
            life.wall.wait(1)
    return states


def jobs_of(life: Life, kind: str, subject: str) -> HistoryJob | None:
    with Session(life.engine) as session:
        return session.scalar(select(HistoryJob).where(HistoryJob.kind == kind, HistoryJob.subject == subject))


def test_the_whole_path_through_the_pages(make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch) -> None:
    life = Life(make_database())
    serve(monkeypatch, life.fake)
    dumps = Path(os.environ["MTL_HORIZON_UI_DUMPS"]) if os.environ.get("MTL_HORIZON_UI_DUMPS") else None
    if dumps is not None:
        dumps.mkdir(parents=True, exist_ok=True)
        for old in dumps.glob("*.html"):
            old.unlink()
    life.settings = Settings(**{**life.settings.__dict__, "admin_secret": SECRET, "api_tokens": (TOKEN,)})
    life.fake.trustlines[EUR] = {HOLDERS[0]: 10**7, HOLDERS[1]: 0}
    where = quote(EUR, safe="")
    try:
        owner = Browser(life.settings, dumps)
        runner = TaskRunner(life.engine, life.settings, client=life.another_pool(), api=life.api(), clock=life.wall, name="hand")

        # 1. The asset, marked for discovery from its page.
        where_to = owner.send("/admin/assets/add", {"asset": EUR, "enabled": "1", "discover": "1", "reason": "держатели евромарки"}, page="/admin/assets")
        assert where_to.startswith(f"/admin/assets/{where}")
        owner.page("asset", f"/admin/assets/{where}", **{"discovery": "discovery"})

        # 2. A dry run is a task, never a scan in the request.
        asked = len(life.fake.requests)
        where_to = owner.send(f"/admin/assets/{where}/run", {"dry_run": "1", "max_requests": "20"}, page=f"/admin/assets/{where}")
        assert len(life.fake.requests) == asked, "the request itself asked no server"
        dry_id = int(re.search(r"[?&]task=(\d+)", where_to).group(1))
        task = runner.once()
        assert task is not None and task.id == dry_id and task.state == "done" and task.result is not None
        assert task.result["dry_run"] is True and task.result["found"] == 2
        owner.page("task-dry-run", f"/admin/tasks/{dry_id}", **{"dry": "dry"})
        with life.engine.connect() as connection:
            assert connection.execute(text("SELECT count(*) FROM tracked_accounts WHERE origin = 'discovery'")).scalar() == 0

        # 3. The pass itself: the new accounts are in the watch list, enabled.
        where_to = owner.send(f"/admin/assets/{where}/run", {"dry_run": "0", "max_requests": "20"}, page=f"/admin/assets/{where}")
        run_id = int(re.search(r"[?&]task=(\d+)", where_to).group(1))
        task = runner.once()
        assert task is not None and task.id == run_id and task.state == "done" and task.result is not None
        assert task.result["found"] == 2 and task.result["added"] == 2 and task.result["already_enabled"] == 0
        owner.page("discovery", "/admin/discovery", **{"found": "discovery"})
        owner.page("accounts", "/admin/accounts?origin=discovery", **{"the holder is listed": HOLDERS[0][:12]})
        for who in HOLDERS:
            account = life.account(who)
            assert account is not None and account.enabled and account.origin == "discovery" and account.boundary_due

        # 4. Their subscriptions and their imports — with the Notifier down at first.
        life.api_fake.hang(100)
        passes(life, 2)
        down = owner.page("notifier-down", "/admin/notifier")
        assert '<span class="badge badge-bad">error</span>' in down and "could not be reached" in down, "the page says what is wrong and why"
        life.api_fake._hangs = 0
        passes(life, 4)
        for who in HOLDERS:
            assert life.rows_state(who) == {"active"} and life.job(who) is not None and life.job(who).boundary_final  # type: ignore[union-attr]
        owner.page("notifier", "/admin/notifier")
        assert {str(sub.get("account")) for sub in life.api_fake.subscriptions if sub.get("account")} >= set(HOLDERS)

        # 5. Their history: read in slices by the queue, to the end of the window.
        ids = [life.job(who).id for who in HOLDERS if life.job(who) is not None]  # type: ignore[union-attr]
        states = until_history_done(life, ids)
        assert set(states.values()) == {"done"}, states
        owner.page("import", "/admin/import", **{"done": "done"})
        owner.page("import-job", f"/admin/import/jobs/{ids[0]}")

        # 6. The pass again: nothing new, no decision changed.
        where_to = owner.send(f"/admin/assets/{where}/run", {"dry_run": "0", "max_requests": "20"}, page=f"/admin/assets/{where}")
        again_id = int(re.search(r"[?&]task=(\d+)", where_to).group(1))
        task = runner.once()
        assert task is not None and task.id == again_id and task.result is not None and task.result["added"] == 0

        # 7. A restart: a new application, new workers — and everything is where it was.
        owner.close()
        owner = Browser(life.settings, dumps)
        owner.page("dashboard", "/admin", **{"done": "done"})
        with HistoryWorker(life.another_pool(), life.engine, name="history-again", slice_requests=20, clock=life.wall) as history:
            assert history.once() is False, "nothing is left to import: the queue is empty, not forgotten"

        # 8. A notification through the webhook — sent twice, as the Notifier does when a delivery is repeated.
        # The webhook stamps the delivery with the real clock: the clocks of the test follow it from here.
        life.fake.close(paid(131, A, HOLDERS[0]), latest=132)
        notice = notifier_format.read(notification(life.fake, str((131 << 32) | 1), subscription="asset-sub"))
        from tests.test_webhook import post

        from datetime import UTC, datetime

        for number in range(2):
            answer = post(owner.client, life.signer, notification(life.fake, str((131 << 32) | 1), subscription="asset-sub"))
            assert answer.status_code == 200 and answer.json()["state"] == "queued"
        life.wall.now = datetime.now(UTC) + timedelta(minutes=1)
        with Worker(life.another_pool(), life.engine, name="w", max_attempts=3, clock=life.wall) as worker:
            assert worker.once() is True
        with life.engine.connect() as connection:
            stored = connection.execute(text("SELECT count(*) FROM history_transactions WHERE ledger_sequence = 131")).scalar()
        assert stored == 1

        # 9. The import widened over that ledger: read again, and nothing stored twice.
        page = f"/admin/import/jobs/{ids[0]}"
        owner.send(f"/admin/import/jobs/{ids[0]}/plan", {"to_ledger": "132"}, page=page)
        states = until_history_done(life, ids[:1])
        assert states == {ids[0]: "done"}
        with life.engine.connect() as connection:
            operations = connection.execute(text("SELECT count(*), count(DISTINCT id) FROM history_operations WHERE id >> 32 = 131")).one()
            effects = connection.execute(text("SELECT count(*), count(DISTINCT id) FROM history_effects WHERE operation_id >> 32 = 131")).one()
            assert operations[0] == operations[1] and effects[0] == effects[1], (operations, effects)

        # 10. The API answers from the archive; the chart is drawn.
        headers = {"Authorization": f"Bearer {TOKEN}"}
        answer = owner.client.get(f"/accounts/{HOLDERS[0]}/transactions?limit=5", headers=headers)
        assert answer.status_code == 200 and answer.json()["_embedded"]["records"]
        assert owner.client.get(f"/v1/coverage?account={HOLDERS[0]}", headers=headers).status_code == 200
        owner.send(f"/admin/markets/{quote(EUR, safe='')}/{quote(RECT, safe='')}/history", {}, page="/admin/markets")
        passes(life, 2)
        market = jobs_of(life, "market", f"{min(EUR, RECT)}>{max(EUR, RECT)}")
        assert market is not None
        assert until_history_done(life, [market.id]) == {market.id: "done"}
        owner.page("chart", f"/admin/chart?base={quote(EUR, safe='')}&counter={quote(RECT, safe='')}&period=month", **{"svg": "<svg"})
        parts = []
        for side, asset in (("base", EUR), ("counter", RECT)):
            code, issuer = asset.split(":")
            parts += [f"{side}_asset_type=credit_alphanum12", f"{side}_asset_code={code}", f"{side}_asset_issuer={issuer}"]
        with life.engine.connect() as connection:
            trades = connection.execute(text("SELECT count(*) FROM history_trades")).scalar()
        assert trades >= 1, "the trades of the market are in the archive"
        first = 1_767_225_600_000  # 2026-01-01T00:00:00Z, when the fake's first ledger closes
        # The hour of the trades in hours; and a year of daily buckets around them — times are in milliseconds.
        for bounds in (
            f"start_time={first}&end_time={first + 3_600_000}&resolution={3_600_000}",
            f"start_time={first - 86_400_000}&end_time={first + 365 * 86_400_000}&resolution={86_400_000}",
        ):
            answer = owner.client.get(f"/trade_aggregations?{'&'.join(parts)}&{bounds}", headers=headers)
            assert answer.status_code == 200, answer.text[:300]
            assert answer.json()["_embedded"]["records"], ("bucketed for a client with the token", bounds, answer.text[:600])

        # 11. The account is switched off from its page: its subscription goes, the asset's stays, the history stays.
        owner.send(f"/admin/accounts/{HOLDERS[0]}/enabled", {"enabled": "0", "reason": "ушёл на время"}, page=f"/admin/accounts/{HOLDERS[0]}")
        passes(life, 3)
        account = life.account(HOLDERS[0])
        assert account is not None and not account.enabled
        assert life.job(HOLDERS[0]) is not None and life.job(HOLDERS[0]).state == "paused"  # type: ignore[union-attr]
        assert not any(sub.get("account") == HOLDERS[0] for sub in life.api_fake.subscriptions), "its subscription is gone from the Notifier"
        assert any(sub.get("account") == HOLDERS[1] for sub in life.api_fake.subscriptions), "and nobody else's is"
        asset_subs = [sub for sub in life.api_fake.subscriptions if sub.get("asset_code") == "EURMTL"]
        assert len(asset_subs) == 1, "the asset's subscription stays: discovery is a rule of its own"
        # Switching off keeps the account's reason (a reason is changed by its own form).
        owner.page("account-off", f"/admin/accounts/{HOLDERS[0]}", **{"switched off": "выключен", "the reason kept": "discovered: holds"})
        with life.engine.connect() as connection:
            kept = connection.execute(text("SELECT count(*) FROM history_transactions WHERE ledger_sequence = 131")).scalar()
        assert kept == 1
        assert life.fake.requests and all("horizon.stellar.org" not in str(path) for path, _ in life.fake.requests)
        owner.close()
    finally:
        life.close()
