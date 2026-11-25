"""Tasks: what the admin UI starts and what takes time — put in the queue by
the request, taken by a runner under a lease, stopped on request, and never
twice for one thing at a time. And what the four kinds of task do."""

from __future__ import annotations

import threading
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.discovery import scan
from app.storage import pool_state, service_jobs
from app.storage.models import DiscoveryRun, TrackedAsset
from app.tasks import kinds
from app.tasks.runner import TaskRunner
from tests.factories import address
from tests.fake_horizon import A, EUR, LATEST, FakeHorizon
from tests.test_watch_lifecycle import CALLBACK, E, Life


def runner_for(life: Life, name: str = "runner-1") -> TaskRunner:
    return TaskRunner(life.engine, life.settings, client=life.another_pool(), api=life.api(), clock=life.wall, name=name)


# -- the queue ------------------------------------------------------------------------


def test_a_task_is_queued_once_taken_under_a_lease_and_finished_with_its_result(make_database: Callable[[], str]) -> None:
    life = Life(make_database())
    try:
        job, made = service_jobs.enqueue(life.engine, "pool_check", {"repeats": 1}, one_at_a_time="pool", now=life.wall())
        assert made and (job.id, job.state, job.kind, job.params) == (1, "queued", "pool_check", {"repeats": 1})
        again, made = service_jobs.enqueue(life.engine, "pool_check", {"repeats": 3}, one_at_a_time="pool", now=life.wall())
        assert not made and again.id == job.id, "one at a time: the second request gives back the first"
        assert service_jobs.enqueue(life.engine, "pool_check", {}, one_at_a_time="pool", now=life.wall())[1] is False
        taken = service_jobs.take(life.engine, "runner-1", lease_seconds=60, now=life.wall())
        assert taken is not None and (taken.id, taken.state, taken.leased_by, taken.attempts) == (1, "running", "runner-1", 1)
        assert service_jobs.take(life.engine, "runner-2", lease_seconds=60, now=life.wall()) is None
        assert service_jobs.progress(life.engine, taken, repeats=1, servers=2)
        state = service_jobs.finish(life.engine, taken, result={"available": True}, now=life.wall())
        assert state == "done"
        kept = service_jobs.get(life.engine, 1)
        assert kept is not None and kept.result == {"available": True} and kept.progress == {"repeats": 1, "servers": 2}
        assert kept.leased_by is None and kept.finished_at == life.wall()
        # Ended: the next request for one makes a new one.
        _, made = service_jobs.enqueue(life.engine, "pool_check", {}, one_at_a_time="pool", now=life.wall())
        assert made
        with pytest.raises(ValueError, match="kind is one of"):
            service_jobs.enqueue(life.engine, "clean_up", {})
    finally:
        life.close()


def test_a_runner_that_dies_loses_its_task_to_the_next_one_and_writes_nothing_more(make_database: Callable[[], str]) -> None:
    life = Life(make_database())
    try:
        service_jobs.enqueue(life.engine, "pool_check", {}, one_at_a_time="pool", now=life.wall())
        first = service_jobs.take(life.engine, "dead-runner", lease_seconds=60, now=life.wall())
        assert first is not None
        life.wall.wait(61)  # its lease ran out: it did not come back
        second = service_jobs.take(life.engine, "next-runner", lease_seconds=60, now=life.wall())
        assert second is not None and (second.leased_by, second.attempts) == ("next-runner", 2)
        assert "did not come back" in (second.last_error or "")
        # The dead one comes to after all: nothing it says is written any more.
        assert not service_jobs.renew(life.engine, first, lease_seconds=60, now=life.wall())
        assert not service_jobs.progress(life.engine, first, late=True)
        assert service_jobs.finish(life.engine, first, result={"late": True}) == "running"
        kept = service_jobs.get(life.engine, 1)
        assert kept is not None and kept.result is None and kept.progress is None and kept.state == "running"
        assert service_jobs.finish(life.engine, second, result={"ok": True}) == "done"
    finally:
        life.close()


def test_a_task_whose_runners_keep_dying_is_failed_after_the_tries(make_database: Callable[[], str]) -> None:
    life = Life(make_database())
    try:
        service_jobs.enqueue(life.engine, "pool_check", {}, now=life.wall())
        for try_ in range(3):
            life.wall.wait(1000)
            job = service_jobs.take(life.engine, f"runner-{try_}", lease_seconds=60, now=life.wall())
            assert job is not None and job.attempts == try_ + 1
        life.wall.wait(1000)
        assert service_jobs.take(life.engine, "late", lease_seconds=60, now=life.wall()) is None
        kept = service_jobs.get(life.engine, 1)
        assert kept is not None and kept.state == "failed" and "did not come back" in (kept.last_error or "")
        assert service_jobs.counts(life.engine) == {"queued": 0, "running": 0, "done": 0, "failed": 1, "cancelled": 0}
        assert [row.id for row in service_jobs.recent(life.engine)] == [1]
    finally:
        life.close()


def test_a_task_is_cancelled_before_it_runs_and_told_to_stop_while_it_runs(make_database: Callable[[], str]) -> None:
    life = Life(make_database())
    try:
        service_jobs.enqueue(life.engine, "pool_check", {}, one_at_a_time="pool", now=life.wall())
        assert service_jobs.request_cancel(life.engine, 1, now=life.wall()) == "cancelled"
        kept = service_jobs.get(life.engine, 1)
        assert kept is not None and kept.state == "cancelled" and kept.finished_at == life.wall()
        assert service_jobs.take(life.engine, "runner-1", lease_seconds=60, now=life.wall()) is None
        # Ended: the way is free for a new one.
        assert service_jobs.enqueue(life.engine, "pool_check", {}, one_at_a_time="pool", now=life.wall())[1]
        job = service_jobs.take(life.engine, "runner-1", lease_seconds=60, now=life.wall())
        assert job is not None and job.id == 2
        assert service_jobs.request_cancel(life.engine, job.id, now=life.wall()) == "running"
        assert service_jobs.cancel_requested(life.engine, job.id)
        # The handler comes to an end anyway — a cancel wins over what it found.
        assert service_jobs.finish(life.engine, job, result={"found": 1}) == "cancelled"
        kept = service_jobs.get(life.engine, job.id)
        assert kept is not None and kept.state == "cancelled" and kept.result == {"found": 1}
    finally:
        life.close()


# -- what the kinds do, through the runner ----------------------------------------------


def test_a_discovery_task_runs_the_scan_and_says_how_far_it_got(make_database: Callable[[], str]) -> None:
    life = Life(make_database())
    try:
        with Session(life.engine) as session, session.begin():
            session.get(TrackedAsset, EUR).discover = True  # type: ignore[union-attr]
        life.fake.trustlines[EUR] = {address(600): 10**7, address(601): 0}
        job, made = service_jobs.enqueue(
            life.engine, "discovery", {"asset": EUR, "max_requests": 50}, one_at_a_time=EUR, now=life.wall()
        )
        assert made
        assert service_jobs.enqueue(life.engine, "discovery", {"asset": EUR}, one_at_a_time=EUR, now=life.wall())[1] is False
        task = runner_for(life).once()
        assert task is not None and task.state == "done" and task.result is not None
        assert task.result["state"] == "complete" and task.result["found"] == 2 and task.result["added"] == 2
        assert (task.progress or {})["pages"] >= 1 and task.progress["found"] == 2
        assert life.account(address(600)) is not None and life.account(address(601)) is not None
        run_id = (task.result or {}).get("run")
        with Session(life.engine) as session:
            run = session.get(DiscoveryRun, run_id) if run_id else None
            assert run is not None and run.state == "complete" and run.pages >= 1
        # A dry run says the same and writes nothing.
        watched_rows = life.engine.connect().execute(text("SELECT count(*) FROM tracked_accounts")).scalar()
        dry, _ = service_jobs.enqueue(life.engine, "discovery", {"asset": EUR, "dry_run": True, "max_requests": 50}, now=life.wall())
        task = runner_for(life).once()
        assert task is not None and task.id == dry.id and task.state == "done"
        assert task.result is not None and task.result["dry_run"] is True and task.result["already_enabled"] == 2
        assert life.engine.connect().execute(text("SELECT count(*) FROM tracked_accounts")).scalar() == watched_rows
    finally:
        life.close()


def test_a_discovery_task_stopped_on_request_keeps_what_it_read(make_database: Callable[[], str]) -> None:
    life = Life(make_database(), FakeHorizon(stall=0.3))
    timer = None
    try:
        with Session(life.engine) as session, session.begin():
            session.get(TrackedAsset, EUR).discover = True  # type: ignore[union-attr]
        life.fake.trustlines[EUR] = {address(700 + number): 10**7 for number in range(450)}  # three pages, each slow
        job, _ = service_jobs.enqueue(life.engine, "discovery", {"asset": EUR, "max_requests": 50}, one_at_a_time=EUR, now=life.wall())
        # After the root and the first page are in.
        timer = threading.Timer(0.75, lambda: service_jobs.request_cancel(life.engine, job.id, now=life.wall()))
        timer.start()
        task = runner_for(life).once()
        assert task is not None and task.state == "cancelled"
        assert task.result is not None and task.result["state"] == "incomplete" and "stopped on request" in task.result["stopped_because"]
        kept = life.engine.connect().execute(text("SELECT count(*) FROM tracked_accounts WHERE origin = 'discovery'")).scalar()
        assert kept and kept < 450, "the pages it read by then are kept, and no more"
        assert scan.stopped_at(life.engine, EUR), "and the pass says where it stopped, to be resumed"
    finally:
        if timer is not None:
            timer.cancel()
        life.close()


def test_a_check_of_the_pool_task_keeps_what_the_command_keeps(make_database: Callable[[], str]) -> None:
    life = Life(make_database())
    try:
        service_jobs.enqueue(life.engine, "pool_check", {"repeats": 1}, one_at_a_time="pool", now=life.wall())
        task = runner_for(life).once()
        assert task is not None and task.state == "done" and task.result is not None
        assert len(task.result["servers"]) == 1 and task.result["servers"][0]["available"] is True
        assert task.result["unavailable"] == [] and task.result["requests"] <= 2
        with Session(life.engine) as session:
            rows = pool_state.rows(session)
            assert len(rows) == 1 and rows[0].available and rows[0].checked_at is not None
    finally:
        life.close()


def test_a_reconcile_task_runs_the_pass_or_only_says_what_it_would_do(make_database: Callable[[], str]) -> None:
    life = Life(make_database())
    try:
        service_jobs.enqueue(life.engine, "reconcile", {"dry_run": True}, one_at_a_time="subscriptions", now=life.wall())
        task = runner_for(life).once()
        assert task is not None and task.state == "done" and task.result is not None
        assert task.result["dry_run"] is True and task.result["signer_known"] is True and task.result["would_do_nothing"] is False
        assert len(task.result["steps"]) == 4, "four are wanted, none is registered yet"
        assert life.api_fake.subscriptions == [], "a dry run makes nothing there"
        made: list[Any] = []
        for number in range(3):
            life.wall.wait(10)
            service_jobs.enqueue(life.engine, "reconcile", {}, one_at_a_time="subscriptions", now=life.wall())
            task = runner_for(life).once()
            assert task is not None and task.state == "done"
            made.append(task.result or {})
        assert [report["created"] for report in made] == [4, 0, 0], "registered once, on the word of a create nothing is active"
        assert [report["confirmed"] for report in made] == [0, 4, 0] and len(life.api_fake.subscriptions) == 4
        life.settings = type(life.settings)(**{**life.settings.__dict__, "notifier_api_url": ""})
        service_jobs.enqueue(life.engine, "reconcile", {}, one_at_a_time="subscriptions", now=life.wall())
        task = TaskRunner(life.engine, life.settings, client=life.another_pool(), api=None, clock=life.wall, name="no-api").once()
        assert task is not None and task.state == "failed" and "no Notifier API is configured" in (task.last_error or "")
    finally:
        life.close()


def test_a_task_of_the_history_compares_or_reads_again_within_its_budget(make_database: Callable[[], str]) -> None:
    from app.backfill.jobs import stream_jobs
    from app.backfill.loader import Backfill, Window
    from app.cli_common import LOGICAL_SOURCE

    life = Life(make_database())
    try:
        with Session(life.engine) as session:
            streams = [stream for stream in stream_jobs(session)[0] if stream.key.subject_key == A]
        with Backfill(life.pool, life.engine, source=LOGICAL_SOURCE, page_size=3) as backfill:
            backfill.run(Window(100, LATEST), streams)
        # Compared: the stored against the pool, over the window.
        service_jobs.enqueue(
            life.engine, "verify", {"kind": "account", "subject": A, "from_ledger": 100, "to_ledger": LATEST}, one_at_a_time=A, now=life.wall()
        )
        task = runner_for(life).once()
        assert task is not None and task.state == "done" and task.result is not None
        assert task.result["mode"] == "verify" and task.result["matches"] is True and task.result["finished"] is True
        assert len(task.result["streams"]) == 4 and all(stream["matches"] for stream in task.result["streams"])
        assert task.result["requests"] > 0
        # Too small a budget: stopped where it stands, and never shown as a match.
        service_jobs.enqueue(
            life.engine,
            "verify",
            {"kind": "account", "subject": A, "from_ledger": 100, "to_ledger": LATEST, "max_requests": 1},
            now=life.wall(),
        )
        task = runner_for(life).once()
        assert task is not None and task.result is not None
        assert task.result["finished"] is False and task.result["matches"] is False and "budget" in task.result["stopped"]
        # Read again: the coverage of the window turned back and re-read, once more stored.
        before = life.engine.connect().execute(text("SELECT count(*) FROM history_operations")).scalar()
        service_jobs.enqueue(
            life.engine,
            "verify",
            {"kind": "account", "subject": A, "from_ledger": 100, "to_ledger": LATEST, "mode": "recover"},
            now=life.wall(),
        )
        task = runner_for(life).once()
        assert task is not None and task.result is not None
        assert task.result["mode"] == "recover" and task.result["read_again"] is True
        assert all(stream["status"] == "verified" for stream in task.result["streams"].values())
        assert life.engine.connect().execute(text("SELECT count(*) FROM history_operations")).scalar() == before
        # What is not watched cannot be verified: refused, with the reason, and not retried.
        service_jobs.enqueue(life.engine, "verify", {"kind": "account", "subject": E, "from_ledger": 100, "to_ledger": LATEST}, now=life.wall())
        task = runner_for(life).once()
        assert task is not None and task.state == "failed" and "is not watched here" in (task.last_error or "")
    finally:
        life.close()


def test_the_runner_says_that_it_is_alive(make_database: Callable[[], str]) -> None:
    from app.storage import heartbeats

    life = Life(make_database())
    try:
        runner = runner_for(life)
        service_jobs.enqueue(life.engine, "pool_check", {}, now=life.wall())
        runner.once()
        said = heartbeats.read(life.engine, heartbeats.BACKGROUND)
        assert said is not None and said["state"] == "idle" and said["name"] == "runner-1" and said["done"] == 1
        assert heartbeats.age(said, life.wall()) == timedelta(0)
    finally:
        life.close()


def test_every_handler_is_one_of_the_kinds_and_the_other_way_round() -> None:
    assert set(kinds.HANDLERS) == set(service_jobs.KINDS)
    assert kinds.DISCOVERY_BUDGET == scan.DEFAULT_MAX_REQUESTS
