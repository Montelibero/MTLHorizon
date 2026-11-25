"""Deadlocks: told from other failures, forced on a real database, and
survived — the work is asked again at once, and never for ever."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

import psycopg
import pytest
import sqlalchemy
from sqlalchemy import text

from app.storage import retry
from app.storage.locks import _key
from app.storage.writer import WRITER, HistoryWriter
from tests.fake_horizon import A, FakeHorizon
from tests.test_worker import T1, Shop


def test_a_deadlock_is_told_through_what_wraps_it() -> None:
    dead = psycopg.errors.DeadlockDetected("deadlock detected")
    assert retry.contention(sqlalchemy.exc.OperationalError("UPDATE x", {}, dead)) == "40P01"
    assert retry.contention(dead) == "40P01" and retry.said("40P01") == "deadlock detected"
    serial = psycopg.errors.SerializationFailure("could not serialize access due to concurrent update")
    try:
        raise ValueError("the try failed") from sqlalchemy.exc.InternalError("INSERT y", {}, serial)
    except ValueError as error:
        assert retry.contention(error) == "40001"
    assert retry.said("40001") == "could not serialize the transaction"
    for other in (None, ValueError("x"), sqlalchemy.exc.OperationalError("s", {}, psycopg.errors.NotNullViolation("null"))):
        assert retry.contention(other) is None
    assert retry.said(None).startswith("the database aborted")


def test_a_forced_deadlock_is_broken_by_the_database_and_the_work_is_asked_again(make_database: Callable[[], str]) -> None:
    """Two transactions, each holding what the other wants. The database
    breaks the cycle by aborting the one that was waiting when it looked for
    a cycle — here the writer of history, which takes its lock first and then
    waits for the row; the other transaction gets the lock a moment later
    without an error of its own. So the advisory lock is no proof that no
    cycle can exist, and the writer of history can be the one aborted: that
    is why such a try is asked again instead of being counted a failure.
    Asked again after the other transaction is gone, it commits."""
    shop = Shop(make_database(), FakeHorizon())
    try:
        shop.receive(shop.notice(T1))
        shop.worker.once()
        with shop.engine.connect() as connection:
            stored = connection.execute(text("SELECT id FROM history_transactions")).scalars().all()
        assert len(stored) == 1
        row_id = stored[0]
        update = text("UPDATE history_transactions SET operation_count = operation_count WHERE id = :id")

        other = shop.engine.connect()
        held = other.begin()
        other.execute(text("SELECT id FROM history_transactions WHERE id = :id FOR UPDATE"), {"id": row_id})

        waiting, finished, outcome = threading.Event(), threading.Event(), {}

        def write() -> None:
            waiting.set()
            try:
                with HistoryWriter(shop.engine).transaction() as session:
                    session.execute(update, {"id": row_id})
                outcome["writer"] = "committed"
            except Exception as error:  # noqa: BLE001 - the test reads what happened
                outcome["writer"] = error
            finally:
                finished.set()

        thread = threading.Thread(target=write)
        thread.start()
        assert waiting.wait(5)
        time.sleep(0.5)  # the writer holds its lock now, and waits for the row
        # The other asks for the writer's lock: the cycle is complete. It
        # returns once the database has broken the cycle — with no error here.
        other.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": _key(WRITER)})
        assert finished.wait(10), "the writer was let out of the cycle"
        thread.join(10)
        assert isinstance(outcome.get("writer"), Exception) and retry.contention(outcome["writer"]) == "40P01", outcome
        held.rollback()
        other.close()
        # The work is asked again, and nothing was lost.
        with HistoryWriter(shop.engine).transaction() as session:
            session.execute(update, {"id": row_id})
        with shop.engine.connect() as connection:
            kept = connection.execute(text("SELECT count(*) FROM history_transactions WHERE id = :id"), {"id": row_id}).scalar()
        assert kept == 1
    finally:
        shop.close()


def test_a_try_the_database_aborted_is_asked_again_at_once_and_is_not_counted_a_failure(
    make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch
) -> None:
    shop = Shop(make_database(), FakeHorizon())
    try:
        real = shop.worker.contexts.fetch_in_steps
        aborted: list[int] = []

        def flaky(*args: Any, **options: Any) -> None:
            aborted.append(1)
            if len(aborted) == 1:
                raise sqlalchemy.exc.OperationalError("UPDATE history_operations", {}, psycopg.errors.DeadlockDetected("deadlock detected"))
            return real(*args, **options)

        monkeypatch.setattr(shop.worker.contexts, "fetch_in_steps", flaky)
        shop.receive(shop.notice(T1))
        shop.worker.once()
        job = shop.job()
        assert job is not None and (job.state, job.attempts) == ("pending", 1)
        assert job.next_attempt_at == shop.wall(), "at once: no pause grows out of the database's doing"
        assert "deadlock detected" in (job.last_error or "") and shop.worker.failed == 0
        shop.worker.once()
        job = shop.job()
        assert job is not None and job.state == "done" and shop.worker.done == 1
        assert len(aborted) == 2
    finally:
        shop.close()


def test_tries_the_database_keeps_aborting_are_bounded(make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch) -> None:
    shop = Shop(make_database(), FakeHorizon(), max_attempts=2)
    try:

        def always(*args: Any, **options: Any) -> None:
            raise sqlalchemy.exc.OperationalError("UPDATE history_operations", {}, psycopg.errors.SerializationFailure("could not serialize"))

        monkeypatch.setattr(shop.worker.contexts, "fetch_in_steps", always)
        shop.receive(shop.notice(T1))
        for number in range(4):
            shop.worker.once()
        job = shop.job()
        assert job is not None and (job.state, job.attempts) == ("dead", 2), "two tries, and then it waits for a person"
        assert shop.worker.once() is False
    finally:
        shop.close()


def test_a_slice_the_database_aborted_is_asked_again_soon_and_the_import_goes_on(
    make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from datetime import timedelta

    from app.history import runner
    from app.history.runner import HistoryWorker
    from app.storage import history_jobs
    from tests.fake_horizon import LATEST
    from tests.test_watch_lifecycle import Life

    life = Life(make_database())
    try:
        job_id = history_jobs.plan(life.engine, kind="account", subject=A, to_ledger=LATEST, context="parents", boundary_final=True, now=life.wall())[0].id
        real = runner.Backfill.run
        aborted: list[int] = []

        def flaky(self: Any, *args: Any, **options: Any) -> Any:
            aborted.append(1)
            if len(aborted) == 1:
                raise sqlalchemy.exc.OperationalError("UPDATE history_effects", {}, psycopg.errors.DeadlockDetected("deadlock detected"))
            return real(self, *args, **options)

        monkeypatch.setattr(runner.Backfill, "run", flaky)
        with HistoryWorker(life.pool, life.engine, name="history", max_attempts=4, clock=life.wall) as history:
            history.once()
            job = history_jobs.get(life.engine, job_id)
            assert job is not None and (job.state, job.attempts) == ("waiting", 1)
            assert job.next_attempt_at == life.wall() + timedelta(seconds=runner.ABORTED_RETRY), "in a moment, not after a growing pause"
            assert "deadlock detected" in (job.last_error or "")
            assert history.once() is False, "not before its time"
            life.wall.wait(runner.ABORTED_RETRY)
            for _ in range(80):
                history.once()
                job = history_jobs.get(life.engine, job_id)
                if job is not None and job.state == "done":
                    break
                life.wall.wait(1)
        assert job is not None and job.state == "done" and len(aborted) >= 2
    finally:
        life.close()

