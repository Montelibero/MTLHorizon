"""Five thousand subjects, on fakes: what it costs to plan their imports,
that each gets its turn, that nobody is asked anything while there is
nothing to do, and how much room the lists and the queue take. Measured, with
the numbers printed, so that they can be put in the report."""

from __future__ import annotations

import time
from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.history.runner import HistoryWorker
from app.storage import heartbeats, history_jobs
from app.watch import accounts as watched
from tests.factories import address
from tests.fake_horizon import A, FakeHorizon, paid
from tests.test_watch_lifecycle import Life

SUBJECTS = [address(number) for number in range(100, 5100)]


def test_five_thousand_subjects_are_planned_served_in_turn_and_ask_for_nothing_when_idle(
    make_database: Callable[[], str]
) -> None:
    life = Life(make_database())
    try:
        with Session(life.engine) as session, session.begin():
            for who in SUBJECTS:
                watched.add_account(session, who, reason="five thousand, for the measure", origin=watched.MANUAL)
        life.fake.close(paid(129, A, *SUBJECTS), latest=130)

        started = time.monotonic()
        ids = [history_jobs.plan(life.engine, kind="account", subject=who, to_ledger=130, boundary_final=True, now=life.wall())[0].id for who in SUBJECTS]
        planning = time.monotonic() - started
        with life.engine.connect() as connection:
            queued = connection.execute(text("SELECT count(*), count(DISTINCT (kind, subject)) FROM history_job")).one()
            size = connection.execute(text("SELECT pg_total_relation_size('history_job'), pg_total_relation_size('tracked_accounts')")).one()
        assert tuple(queued) == (5000, 5000)

        served: list[str] = []
        # The fake Horizon answers only for its own six accounts: to these
        # five thousand it says 404, so what is measured is the queue's own
        # cost — taking a job, a slice, the failure and the pause — and not
        # the reading.
        with HistoryWorker(life.pool, life.engine, name="scale", slice_requests=1, max_attempts=3, retry_base=1, retry_cap=2, clock=life.wall) as history:
            started = time.monotonic()
            for number in range(100):
                assert history.once() is True
                assert history.last is not None
                served.append(history.last.subject)
            working = time.monotonic() - started
            # Nothing due — the queue is paused by hand — is asked of nobody.
            history_jobs.pause(life.engine, all=True, because="for the measure of an idle moment")
            idle_before = len(life.fake.requests)
            started = time.monotonic()
            for _ in range(20):
                assert history.once() is False
            idle = time.monotonic() - started
        assert len(set(served)) == 100 and served == SUBJECTS[:100], "the first hundred of the list: each once, in the order planned"
        assert len(life.fake.requests) == idle_before, "an idle moment asks no server for anything"
        with life.engine.connect() as connection:
            tried = connection.execute(text("SELECT count(*) FROM history_job WHERE attempts = 1")).scalar()
            untouched = connection.execute(text("SELECT count(*) FROM history_job WHERE attempts = 0")).scalar()
            paused = connection.execute(text("SELECT count(*) FROM history_job WHERE state = 'paused'")).scalar()
        assert (tried, untouched, paused) == (100, 4900, 5000), "each of the hundred had one try; all are paused now"
        beat = heartbeats.read(life.engine, heartbeats.HISTORY)
        assert beat is not None and beat["state"] == "idle"
        print(
            f"\n5000 subjects: planning {planning:.1f}s ({planning * 1000 / 5000:.1f} ms a job); "
            f"100 slices (a root and a page each, answered 404) in {working:.1f}s ({working * 1000 / 100:.0f} ms a slice); "
            f"20 looks at an idle queue in {idle * 1000:.0f} ms, 0 requests; "
            f"queue table {size[0] // 1024} kB, watch list {size[1] // 1024} kB"
        )
    finally:
        life.close()


def test_the_pages_of_a_list_of_five_thousand_stay_quick(make_database: Callable[[], str]) -> None:
    """Five thousand watched accounts: the pages that list them answer in a
    bounded time and show a page, not the list; a search by the beginning of
    an address finds its account among them."""
    from fastapi.testclient import TestClient

    from app.api.app import create_app
    from app.config import PUBLIC_NETWORK_PASSPHRASE, Settings

    life = Life(make_database())
    try:
        with Session(life.engine) as session, session.begin():
            for who in SUBJECTS:
                watched.add_account(session, who, reason="five thousand, for the measure", origin=watched.MANUAL)
        settings = Settings(
            **{  # type: ignore[arg-type]
                "database_url": life.engine.url.render_as_string(hide_password=False),
                "network_passphrase": PUBLIC_NETWORK_PASSPHRASE,
                "admin_secret": "a-secret-for-the-measure",
            }
        )
        timings = {}
        with TestClient(create_app(settings)) as client:
            assert client.post("/admin/login", data={"secret": "a-secret-for-the-measure"}, follow_redirects=False).status_code == 303
            for name, path in (
                ("accounts p1", "/admin/accounts"),
                ("accounts p50", "/admin/accounts?page=50"),
                ("accounts search", f"/admin/accounts?q={SUBJECTS[4321]}"),  # the fake's addresses share a long prefix
                ("dashboard", "/admin"),
                ("activity", "/admin/activity"),
                ("import", "/admin/import"),
            ):
                started = time.monotonic()
                answer = client.get(path)
                timings[name] = time.monotonic() - started
                assert answer.status_code == 200 and "Traceback" not in answer.text, (name, answer.status_code)
            found = client.get(f"/admin/accounts?q={SUBJECTS[4321]}").text
            assert SUBJECTS[4321] in found and found.count("<tr>") <= 3, "the one account looked for"
            assert client.get("/admin/accounts").text.count("<tr>") <= 60, "a page of the list, not the list"
        print("\n5000 accounts, pages: " + ", ".join(f"{name} {seconds * 1000:.0f} ms" for name, seconds in timings.items()))
        assert max(timings[name] for name in ("accounts p1", "accounts p50", "accounts search")) < 2.0, timings
    finally:
        life.close()
