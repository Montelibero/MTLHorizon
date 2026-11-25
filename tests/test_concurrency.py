"""Several services at once, as an installation runs: two workers of events,
two of history, the pass that keeps accounts in step and a discovery pass —
on the same accounts, the same transactions, the same database. What is
stored is what one reading of the same window stores; nothing is stored
twice; nothing is left in nobody's hands."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.backfill import verify as checking
from app.backfill.jobs import stream_jobs
from app.backfill.loader import Backfill, Window
from app.cli_common import source_name
from app.discovery import scan
from app.history.runner import HistoryWorker
from app.notifier import format as notifier_format
from app.storage import history_jobs, jobs
from app.storage import events as journal
from app.storage.models import HistoryJob, ProcessingJob, TrackedAsset
from app.watch import provision
from app.worker.loader import Worker
from tests.factories import address
from tests.fake_horizon import A, B, EUR, FakeHorizon, paid, serve
from tests.fake_notifier import notification
from tests.test_backfill import WINDOW
from tests.test_watch_lifecycle import Life

SUBJECTS = (A, B)
# Payments to and fro between the two accounts, in the ledgers after the fake's own history.
WAYS = ((A, (B,)), (B, (A,)), (A, (B,)), (B, (A,)), (A, (B,)))


def notifications(life: Life) -> int:
    """Word of every operation of the two accounts, for two subscriptions
    each — and every third delivery sent once more — as the Notifier sends
    it. Returns how many deliveries were received."""
    sent = 0
    for ledger in range(100, 131):
        for order in range(1, 60):
            operation = str((ledger << 32) | order)
            try:
                notice = notifier_format.read(notification(life.fake, operation, subscription="first"))
            except Exception:  # noqa: BLE001 - no such operation: the fake has nothing to say of it
                continue
            if not (set(notice.accounts or ()) & set(SUBJECTS)):
                continue
            for subscription in ("first", "second", "first"):
                if subscription == "first" and sent % 3:
                    continue
                notice = notifier_format.read(notification(life.fake, operation, subscription=subscription))
                journal.receive(life.engine, notice, received_at=life.wall(), headers={}, signed_by="G-test")
                sent += 1
    return sent


def test_two_workers_of_events_two_of_history_and_the_pass_store_what_a_sequential_import_stores(
    make_database: Callable[[], str], monkeypatch: Any
) -> None:
    life, alone = Life(make_database()), Life(make_database())
    try:
        serve(monkeypatch, life.fake)
        # More traffic between the two accounts than the fake's own history has.
        extra = [paid(121 + number, source, *targets) for number, (source, targets) in enumerate(WAYS * 3)]
        for fake in (life.fake, alone.fake):
            fake.close(*extra, latest=130)
        # The reference: the same deliveries brought in by one worker, one
        # after another, and then the same window read once.
        sent = notifications(life)
        assert sent >= 20 and notifications(alone) == sent
        with Worker(alone.pool, alone.engine, name="alone", max_attempts=5, clock=alone.wall) as worker:
            for _ in range(1000):
                if worker.once() is False:
                    break
        with Session(alone.engine) as session:
            streams = [stream for stream in stream_jobs(session)[0] if stream.key.subject_key in SUBJECTS]
        with Backfill(alone.pool, alone.engine, source=source_name(alone.pool), page_size=3, context="parents") as backfill:
            backfill.run(Window(100, 130), streams)
        for who in SUBJECTS:
            history_jobs.plan(life.engine, kind="account", subject=who, to_ledger=130, context="parents", boundary_final=True, now=life.wall())
        with Session(life.engine) as session, session.begin():
            session.get(TrackedAsset, EUR).discover = True  # type: ignore[union-attr]
        life.fake.trustlines[EUR] = {address(800): 10**7}

        stop = threading.Event()
        failures: list[BaseException] = []
        api = life.api()

        def guarded(target: Callable[[], Any]) -> Callable[[], None]:
            def run() -> None:
                try:
                    target()
                except BaseException as error:  # noqa: BLE001 - read below
                    failures.append(error)
                    stop.set()

            return run

        def events(name: str) -> None:
            with Worker(life.another_pool(), life.engine, name=name, max_attempts=5, clock=life.wall) as worker:
                while not stop.is_set():
                    worker.once(stop=stop)

        def history(name: str) -> None:
            with HistoryWorker(life.another_pool(), life.engine, name=name, slice_requests=3, max_attempts=6, clock=life.wall) as worker:
                while not stop.is_set():
                    worker.once()

        def passes() -> None:
            while not stop.is_set():
                provision.run(life.engine, life.settings, client=life.another_pool(), api=api, now=life.wall())
                time.sleep(0.05)

        threads = [
            threading.Thread(target=guarded(lambda: events("events-1")), daemon=True),
            threading.Thread(target=guarded(lambda: events("events-2")), daemon=True),
            threading.Thread(target=guarded(lambda: history("history-1")), daemon=True),
            threading.Thread(target=guarded(lambda: history("history-2")), daemon=True),
            threading.Thread(target=guarded(passes), daemon=True),
        ]
        for thread in threads:
            thread.start()
        try:
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline and not failures:
                with life.engine.connect() as connection:
                    left = connection.execute(
                        text("SELECT count(*) FROM processing_job WHERE state IN ('pending', 'running')")
                    ).scalar() + connection.execute(
                        text("SELECT count(*) FROM history_job WHERE state IN ('waiting', 'running')")
                    ).scalar()
                if left == 0:
                    break
                time.sleep(0.2)
        finally:
            stop.set()
            for thread in threads:
                thread.join(30)
        assert not failures, failures[:3]

        # A discovery pass while the rest runs is the same scan; here it runs after, on its own pool.
        report = scan.scan(life.another_pool(), life.engine, EUR)
        assert report.state == "complete" and len(report.added) == 1

        assert checking.fingerprint(life.engine, WINDOW) == checking.fingerprint(alone.engine, WINDOW)
        with life.engine.connect() as connection:
            for table in ("history_operations", "history_effects", "history_trades"):
                counted = connection.execute(text(f"SELECT count(*), count(DISTINCT id) FROM {table}")).one()
                assert counted[0] == counted[1], table
            stuck = connection.execute(
                text("SELECT count(*) FROM processing_job WHERE state = 'running' AND lease_expires_at < now()")
            ).scalar()
            stuck_history = connection.execute(
                text("SELECT count(*) FROM history_job WHERE state = 'running' AND lease_expires_at < now()")
            ).scalar()
            given_up = connection.execute(text("SELECT count(*) FROM processing_job WHERE state IN ('partial', 'dead')")).scalar()
            history_states = dict(connection.execute(text("SELECT state, count(*) FROM history_job GROUP BY state")).all())
        assert (stuck, stuck_history, given_up) == (0, 0, 0)
        assert history_states.get("done") == 2 and sum(history_states.values()) == 2, history_states
    finally:
        life.close()
        alone.close()
