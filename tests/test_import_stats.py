"""What the pages of the admin read of the history imports: accounts and
markets counted apart, every number checked on data the test arranged
itself — and read from the database alone."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from app.backfill import config as watch_lists
from app.backfill.jobs import market_key
from app.storage import history_jobs
from app.storage.models import HistoryJob, HorizonNodeState
from app.stats import imports
from tests.factories import address
from tests.fake_horizon import A, B, C, D, EUR, ISSUER, MTL, S, FakeHorizon, close_time
from tests.test_history_queue import DONE, ERROR, MARKET, PAUSED, PARTIAL, RUNNING, STREAMS, WAITING, Yard, finish

EUR_MTL = market_key(EUR, MTL)
EUR_NATIVE = market_key(EUR, "native")
# Accounts that never meet a server: their jobs are put in a state by the queue's own functions.
P1, P2, P3, P4, P5, P6, P7, P8, P9 = (address(number) for number in range(51, 60))
NO_REASON = dict.fromkeys(imports.WAITING_REASONS, 0)


@pytest.fixture
def yard(make_database: Callable[[], str]) -> Iterator[Yard]:
    instance = Yard(make_database(), FakeHorizon())
    yield instance
    instance.close()


def watch_market(yard: Yard, base: str, counter: str) -> str:
    """One more market on the watch list, switched on; its key."""
    lists = watch_lists.parse({"markets": [{"base": base, "counter": counter, "reason": "for the test"}]}, "test")
    with Session(yard.engine) as session, session.begin():
        watch_lists.apply(session, lists)
    return market_key(base, counter)


def craft(
    yard: Yard,
    kind: str,
    subject: str,
    outcome: str,
    error: str | None = None,
    *,
    times: int = 1,
    planning: dict[str, Any] | None = None,
    **said: Any,
) -> Any:
    """A job put in a state by the queue itself, as a slice that ended so
    would have left it: planned, taken, and its slice ended with
    ``outcome``. No server is asked. The job is to be the only one due."""
    job = None
    for _ in range(times):
        job, _ = history_jobs.plan(
            yard.engine, kind=kind, subject=subject, to_ledger=130, now=yard.wall(), **(planning or {})
        )
        taken = history_jobs.take(yard.engine, "craft", lease_seconds=60, now=yard.wall())
        assert taken is not None and taken.subject == subject, "the job planned now is the one taken now"
        history_jobs.slice_done(
            yard.engine, taken, outcome=outcome, error=error, max_attempts=3, now=yard.wall(), **said
        )
    return job


def arrange(yard: Yard) -> dict[str, Any]:
    """Accounts and markets in every state there is, each for a reason of its
    own. Four of them were read from the fake; the rest is the queue's doing.
    Returns the jobs that were read."""
    for who in (D, S, ISSUER, P1, P2, P3, P4, P5, P6, P7, P8, P9):
        yard.watch(who)
    watch_market(yard, EUR, MTL)
    watch_market(yard, EUR, "native")

    read: dict[str, Any] = {}
    for who, options in ((A, {}), (B, {"from_ledger": 90})):
        read[who], _ = yard.plan(who, **options)
        assert finish(yard.worker) >= 1 and history_jobs.get(yard.engine, read[who].id).state == DONE
    job, _ = history_jobs.plan(yard.engine, kind="market", subject=MARKET, to_ledger=130, now=yard.wall())
    read[MARKET] = job
    assert finish(yard.worker) == 1 and history_jobs.get(yard.engine, job.id).state == DONE

    # D: one page of one record is read, and the job is then held, as a
    # worker holds it.
    yard.worker = yard.new_worker("sliced", page_size=1, slice_requests=1)
    read[D], _ = yard.plan(D)
    assert yard.worker.once() is True
    held = history_jobs.take(yard.engine, "holder", lease_seconds=600, now=yard.wall())
    assert held is not None and held.subject == D
    yard.worker = yard.new_worker("whole")

    # Each is crafted while it is the only job that is due: first those
    # that come to wait for a while, then those that are crafted to an end.
    craft(yard, "account", S, history_jobs.FAILED, "trades: RequestFailed: Horizon answered 503 to GET /trades", delay=30)
    refused = "trades: RequestFailed: Horizon answered 503 to GET /trades"
    craft(
        yard, "account", P1, history_jobs.DEFERRED,
        "ledgers below 100 are not read: https://deep.test did not say of itself just now and may keep them",
        delay=120,
    )
    craft(
        yard, "account", P2, history_jobs.DEFERRED,
        "effects: RateLimited: Horizon asks to wait longer than a request waits",
        delay=120,
    )
    craft(
        yard, "account", P3, history_jobs.DEFERRED,
        "Unavailable: no server of the pool answers: https://a.test is down",
        delay=120,
    )
    craft(
        yard, "account", P4, history_jobs.DEFERRED,
        "the pool has got only to ledger 130, and the boundary is ledger 131",
        delay=120,
    )
    craft(yard, "account", P7, history_jobs.FAILED, refused, times=3)
    craft(
        yard, "account", P8, history_jobs.ERROR,
        "transactions: RequestFailed: Horizon answered 503 to GET /transactions",
        times=3,
    )
    craft(yard, "market", EUR_NATIVE, history_jobs.ERROR, refused, times=3)
    craft(yard, "account", P5, history_jobs.PROGRESS, result={"window": [100, 130]})
    craft(yard, "account", P9, history_jobs.FINISHED, result={"window": [100, 130]}, planning={"boundary_final": True})

    paused, _ = yard.plan(P6)
    assert history_jobs.pause(yard.engine, ids=[paused.id], because="maintenance") == 1
    switched_off, _ = yard.plan(C)
    assert history_jobs.pause(yard.engine, ids=[switched_off.id], because="account disabled") == 1
    read[EUR_MTL], _ = history_jobs.plan(yard.engine, kind="market", subject=EUR_MTL, to_ledger=130, now=yard.wall())

    # What the last check of the pool found: two servers, one of them down.
    checked = yard.wall() - timedelta(minutes=5)
    with Session(yard.engine) as session, session.begin():
        session.add(
            HorizonNodeState(
                url="https://a.test", position=1, available=True, elder_ledger=102, latest_ledger=130, checked_at=checked
            )
        )
        session.add(
            HorizonNodeState(
                url="https://b.test", position=2, available=False, elder_ledger=110, latest_ledger=128, lag_ledgers=2,
                last_error="connection refused", checked_at=yard.wall() - timedelta(hours=1),
            )
        )
    return read


def at(moment: Any) -> str:
    return moment.isoformat(timespec="seconds")


def test_overview_counts_accounts_and_markets_apart_and_says_how(yard: Yard) -> None:
    arrange(yard)
    asked = len(yard.fake.requests)
    now = yard.wall() + timedelta(minutes=1)

    found = imports.overview(yard.engine, now=now)
    assert len(yard.fake.requests) == asked and yard.pool.stats.requests == asked, "a page asks no server"

    accounts = found.accounts
    assert accounts["subjects"] == 14 and (accounts["with_job"], accounts["no_job"]) == (13, 1), (
        "every switched-on account, with a job or without"
    )
    assert accounts["states"] == {WAITING: 6, RUNNING: 1, PAUSED: 1, DONE: 3, PARTIAL: 1, ERROR: 1}
    assert accounts["waiting_reasons"] == {
        **NO_REASON,
        "servers_cannot_answer": 1,
        "rate_limited": 1,
        "pool_silent": 1,
        "pool_behind": 1,
        "backoff_after_failure": 1,
        "its_turn": 1,
    }
    assert accounts["paused_reasons"] == {"maintenance": 1}
    assert accounts["boundary_not_final"] == {
        "total": 12,
        "by_state": {DONE: 2, RUNNING: 1, WAITING: 6, PAUSED: 1, PARTIAL: 1, ERROR: 1},
    }, "every job whose boundary is not vouched for yet, by state"
    # Three are done. One of them, B, had ten ledgers below what the pool
    # keeps: it finished the window that was available, and they are said
    # apart — never as read.
    assert accounts["finished"] == {"window_available": 3, "nothing_unavailable": 2, "with_unavailable": 1}
    assert accounts["unavailable"] == {"subjects": 1, "ledgers": 10}
    assert accounts["completion"] == {"completed": 3, "planned": 14, "share": 3 / 14}, "a share of accounts"
    assert accounts["completion_of_jobs"] == {"completed": 3, "planned": 13, "share": 3 / 13}
    assert accounts["not_counted"] == {WAITING: 0, RUNNING: 0, DONE: 0, PARTIAL: 0, ERROR: 0, PAUSED: 1}, (
        "the job of the account that is switched off is not a subject that needs history"
    )

    # Markets are their own count, never mixed with the accounts into one share.
    markets = found.markets
    assert (markets["subjects"], markets["with_job"], markets["no_job"]) == (3, 3, 0)
    assert markets["states"] == {WAITING: 1, RUNNING: 0, PAUSED: 0, DONE: 1, PARTIAL: 0, ERROR: 1}
    assert markets["waiting_reasons"] == {**NO_REASON, "its_turn": 1}
    assert markets["finished"] == {"window_available": 1, "nothing_unavailable": 1, "with_unavailable": 0}
    assert markets["completion"] == {"completed": 1, "planned": 3, "share": 1 / 3}
    assert markets["boundary_not_final"] == {"total": 3, "by_state": {WAITING: 1, ERROR: 1, DONE: 1}}

    # The pool as its last check saw it, and the windows the jobs are asked for.
    pool = found.pool
    assert (pool["elder_ledger"], pool["latest_ledger"]) == (102, 130), "the lowest elder and the highest latest"
    assert pool["unavailable"] == ["https://b.test"] and pool["silent"] == []
    assert pool["checked_at"] == at(yard.wall() - timedelta(minutes=5))
    assert "as of the last check" in pool["note"] and "was not asked" in pool["note"]
    assert found.windows == {
        "account": {
            "jobs": 14,  # the switched-off account's among them: a window is a job's
            "lowest_from": 90,
            "as_far_back_as_the_pool_keeps": 13,
            "highest_to": 130,
            "verified_starts_at": 100,
            "elder_above_verified_start": 2,
        },
        "market": {
            "jobs": 3,
            "lowest_from": None,
            "as_far_back_as_the_pool_keeps": 3,
            "highest_to": 130,
            "verified_starts_at": 100,
            "elder_above_verified_start": 2,
        },
    }
    assert found.queue["paused"] is False and found.queue["paused_for_seconds"] is None
    assert found.queue["worker"]["state"] == "idle" and found.queue["worker"]["age_seconds"] == 60, (
        "the worker last spoke a minute ago, by the clock of the page"
    )
    assert found.measured_at == now

    # Plain data: every moment in UTC as text, the whole of it writable as JSON.
    said = found.as_dict()
    assert said["measured_at"] == at(now) and said["accounts"] is found.accounts
    assert said["pool"]["servers"][0]["checked_at"] == at(yard.wall() - timedelta(minutes=5))
    assert json.dumps(said)
    assert set(said["method"]) >= {"subjects", "completion", "unavailable_ledgers", "pool", "share"}, (
        "every number says how it was measured"
    )


def test_the_pause_of_the_whole_queue_is_what_every_waiting_job_waits_for(yard: Yard) -> None:
    arrange(yard)
    history_jobs.pause_all(yard.engine, because="the servers are being moved", by="owner", now=yard.wall())
    now = yard.wall() + timedelta(seconds=90)

    found = imports.overview(yard.engine, now=now)
    assert found.accounts["waiting_reasons"] == {**NO_REASON, "global_pause": 6}
    assert found.markets["waiting_reasons"] == {**NO_REASON, "global_pause": 1}
    assert found.queue["paused"] is True and found.queue["because"] == "the servers are being moved"
    assert (found.queue["by"], found.queue["paused_for_seconds"]) == ("owner", 90)
    assert found.accounts["states"][PAUSED] == 1, "the jobs are not paused; the queue is"

    table = imports.jobs(yard.engine, state=WAITING, now=now)
    assert {item["reason"]["code"] for item in table.items} == {"global_pause"}
    text_ = {item["subject"]: item["reason"]["text"] for item in table.items}
    assert text_[S] == "the whole queue is paused; and before that: a slice failed, and the job waits before trying again"
    assert text_[P5] == "the whole queue is paused", "a job that only waited for its slice says nothing more"

    history_jobs.resume_all(yard.engine, by="owner", now=yard.wall())
    found = imports.overview(yard.engine, now=now)
    assert found.queue["paused"] is False and found.queue["resumed_by"] == "owner"
    assert found.queue["because"] == "the servers are being moved", "what the last pause said stays"
    assert found.accounts["waiting_reasons"] == {
        **NO_REASON,
        "backoff_after_failure": 1,
        "its_turn": 1,
        "pool_silent": 1,
        "rate_limited": 1,
        "servers_cannot_answer": 1,
        "pool_behind": 1,
    }


def test_the_table_puts_what_is_active_first_and_says_of_each_job(yard: Yard) -> None:
    arrange(yard)
    asked = len(yard.fake.requests)
    now = yard.wall() + timedelta(seconds=5)

    table = imports.jobs(yard.engine, now=now)
    assert len(yard.fake.requests) == asked
    assert table.total == 17 and len(table.items) == 17 and (table.page, table.pages, table.has_more) == (1, 1, False)
    states = [item["state"] for item in table.items]
    assert states == [RUNNING] + [WAITING] * 7 + [PAUSED] * 2 + [PARTIAL] + [ERROR] * 2 + [DONE] * 4, (
        "running, then waiting, then paused, partial, error, done"
    )
    waiting = [item for item in table.items if item["state"] == WAITING]
    assert waiting == sorted(waiting, key=lambda item: (item["next_attempt_at"], item["last_slice_at"] or "", item["id"])), (
        "waiting: by the time of the next try, then the job least recently served"
    )
    assert [item["subject"] for item in waiting] == [EUR_MTL, P5, S, P1, P2, P3, P4]

    held = table.items[0]
    assert (held["subject"], held["reason"], held["leased_by"]) == (
        D,
        {"code": "working", "text": "being read by holder"},
        "holder",
    )
    assert (held["priority"], held["context"], held["boundary_final"]) == (0, "parents", False)
    assert held["window"] == {
        "bottom": 100,
        "top": 130,
        "bottom_how": "where the pool's history began when the job first read",
        "read": [100, 130],
    }
    assert held["streams_left"] == list(STREAMS) and held["verified"]["share"] == 0.0
    assert (held["reached_ledger"], held["reached_ledger_time"]) == (112, at(close_time(112)))
    assert held["streams"]["transactions"]["cursor"]["how"] == "its checkpoint"
    assert held["streams"]["effects"]["cursor"] == {"token": None, "at_ledger": None, "ledger_time": None, "how": None}, (
        "a stream nothing was read of stands nowhere"
    )
    assert held["attempts"] == 0 and held["last_progress_at"] == at(yard.wall())
    # The market's job stored that transaction first, as a trade's parent:
    # D's job found it there, and added nothing.
    assert held["counters"]["inserted"] == dict.fromkeys(STREAMS, 0)
    assert held["counters"]["repeated"] == {"transactions": 1, "operations": 0, "effects": 0, "trades": 0}

    # The filters.
    only_markets = imports.jobs(yard.engine, kind="market", now=now)
    assert only_markets.total == 3 and {item["kind"] for item in only_markets.items} == {"market"}
    with pytest.raises(ValueError, match="kind is one of"):
        imports.jobs(yard.engine, kind="asset", now=now)
    with pytest.raises(ValueError, match="state is one of"):
        imports.jobs(yard.engine, state="finished", now=now)
    gave_up = imports.jobs(yard.engine, state=ERROR, now=now)
    assert [item["subject"] for item in gave_up.items] == [P8, EUR_NATIVE]
    assert gave_up.items[0]["reason"] == {
        "code": "gave_up",
        "text": "transactions: RequestFailed: Horizon answered 503 to GET /transactions",
    }
    assert gave_up.items[0]["attempts"] == 3 and gave_up.items[0]["next_attempt_in_seconds"] is None
    # Addresses made from small numbers share their beginning: the end tells them apart.
    found = imports.jobs(yard.engine, q=P3[-12:], now=now)
    assert [item["subject"] for item in found.items] == [P3], "a subject is found by a part of it"
    assert imports.jobs(yard.engine, q=P3[-12:].lower(), now=now).total == 1, "and in any case"
    assert imports.jobs(yard.engine, q="%_(", now=now).total == 0, "what is not in a subject is looked for as it is"

    by_hand = imports.jobs(yard.engine, state=PAUSED, now=now).items
    assert {item["subject"]: item["reason"]["text"] for item in by_hand} == {P6: "maintenance", C: "account disabled"}
    waits = {item["subject"]: item for item in waiting}
    assert [waits[who]["reason"]["code"] for who in (P1, P2, P3, P4, S, P5)] == [
        "pool_silent",
        "rate_limited",
        "servers_cannot_answer",
        "pool_behind",
        "backoff_after_failure",
        "its_turn",
    ]
    assert (waits[S]["next_attempt_in_seconds"], waits[S]["attempts"]) == (25, 1)
    assert waits[P5]["reason"] == {"code": "its_turn", "text": "waiting for its slice"}

    done = {item["subject"]: item for item in table.items if item["state"] == DONE}
    assert done[A]["reason"]["code"] == "finished_boundary_not_final"
    assert done[P9]["reason"]["code"] == "finished" and done[P9]["boundary_final"] is True
    assert done[B]["streams_left"] == [] and done[B]["verified"]["share"] == 31 / 41
    assert done[B]["window"]["bottom"] == 90 and done[B]["window"]["bottom_how"] == "the ledger the job was asked for"
    assert done[MARKET]["streams_left"] == [] and list(done[MARKET]["streams"]) == ["trades"]
    assert list(done[MARKET]["servers"]) == [yard.fake.origin], "the servers that read it"
    assert json.dumps(table.as_dict())


def test_a_page_of_the_table_never_has_more_than_a_page_and_loses_no_job(make_database: Callable[[], str]) -> None:
    yard = Yard(make_database(), FakeHorizon())
    try:
        planned = [
            history_jobs.plan(yard.engine, kind="account", subject=address(number), to_ledger=130, now=yard.wall())[0]
            for number in range(300, 357)
        ]
        now = yard.wall()
        whole = imports.jobs(yard.engine, per_page=1000, now=now)
        assert whole.per_page == 200 and len(whole.items) == 57, "the most a page has, however much is asked for"
        seen: list[int] = []
        page = 1
        while True:
            found = imports.jobs(yard.engine, page=page, per_page=7, now=now)
            assert len(found.items) <= 7 < found.total and found.page == page
            seen += [item["id"] for item in found.items]
            assert found.has_more == (page < found.pages)
            if not found.has_more:
                assert page == found.pages == 9
                break
            page += 1
        assert seen == [job.id for job in planned], "every job once, in the order of the table"
        beyond = imports.jobs(yard.engine, page=10, per_page=7, now=now)
        assert beyond.items == [] and beyond.total == 57
        least = imports.jobs(yard.engine, per_page=0, page=-3, now=now)
        assert (least.per_page, least.page, len(least.items)) == (1, 1, 1)
    finally:
        yard.close()


def test_one_job_says_of_each_stream_what_it_verified_and_what_it_cost(yard: Yard) -> None:
    read = arrange(yard)
    asked = len(yard.fake.requests)
    now = yard.wall() + timedelta(minutes=2)

    detail = imports.job_detail(yard.engine, read[B].id, now=now)
    assert len(yard.fake.requests) == asked
    assert detail is not None
    assert (detail.job["subject"], detail.job["state"], detail.job["finished_at"]) == (B, DONE, at(yard.wall()))
    assert detail.job["next_attempt_in_seconds"] is None, "nothing is next for a job that is done"
    assert (detail.job["reached_ledger"], detail.job["reached_ledger_time"]) == (100, at(close_time(100)))
    assert list(detail.streams) == list(STREAMS)
    transactions = detail.streams["transactions"]
    gone, kept = transactions["pieces"]
    assert transactions["pieces"] == [
        {
            "from": 90, "to": 99, "state": "unverifiable", "served_by": None,
            "verified_at": gone["verified_at"], "detail": gone["detail"], "in_window": [90, 99],
        },
        {
            "from": 100, "to": 130, "state": "complete", "served_by": yard.fake.origin,
            "verified_at": kept["verified_at"], "detail": None, "in_window": [100, 130],
        },
    ]
    assert "source keeps history from ledger 100" in gone["detail"], "why it cannot be verified is kept"
    assert gone["verified_at"] and kept["verified_at"], "when each was verified, as the database clock had it"
    assert transactions["gaps"] == {"pending": [], "unverifiable": [[90, 99]]}
    assert transactions["ledgers"] == {"pending": 0, "complete": 31, "empty": 0, "unverifiable": 10}
    assert transactions["verified"] == {"from": 100, "to": 130, "ledgers": 31, "spans": [[100, 130]]}
    assert transactions["share"] == {"verified": 31, "of": 41, "share": 31 / 41, "unknown_because": None}
    assert transactions["left"] == 0 and transactions["unverifiable_ledgers"] == 10
    assert transactions["cursor"] == {
        "token": None,
        "at_ledger": 100,
        "ledger_time": at(close_time(100)),
        "how": "verified down to here",
    }, "a finished stream has no checkpoint: it stands where its verified coverage ends"
    assert transactions["last_slice"]["status"] == "verified"
    assert detail.verified == {
        "verified": 31,
        "of": 41,
        "share": 31 / 41,
        "unknown_because": None,
        "method": "ledgers verified in every stream, over the ledgers of the job's window",
    }, "verified ledgers over the window, the method and the denominator said"
    assert "over the ledgers of that window" in detail.method["share"]

    job = history_jobs.get(yard.engine, read[B].id)
    assert job is not None
    counters = detail.counters
    assert set(counters) == {
        "requests", "pages", "records", "inserted", "repeated", "slices", "failed_slices",
        "time_spent_seconds", "records_per_minute", "pages_per_minute", "counts_what_was_read",
    }, "no ETA, nothing left to read in records, no date from an average ledger"
    assert counters["requests"] == job.requests and (counters["pages"], counters["records"]) == (job.pages, job.records)
    assert counters["inserted"] == job.inserted and counters["repeated"] == job.repeated
    assert counters["counts_what_was_read"] is True and counters["failed_slices"] == 0
    assert counters["time_spent_seconds"] == job.time_spent_ms / 1000 and counters["time_spent_seconds"] > 0
    minutes = job.time_spent_ms / 60_000
    assert counters["records_per_minute"] == round(job.records / minutes, 1)
    assert counters["pages_per_minute"] == round(job.pages / minutes, 1)

    market = imports.job_detail(yard.engine, read[MARKET].id, now=now)
    assert market is not None and list(market.streams) == ["trades"]
    assert market.verified["share"] == 1.0
    assert market.streams["trades"]["share"] == {"verified": 31, "of": 31, "share": 1.0, "unknown_because": None}
    assert imports.job_detail(yard.engine, 99999, now=now) is None
    assert json.dumps(detail.as_dict())


def test_a_job_that_has_not_read_has_no_window_and_no_share(yard: Yard) -> None:
    read = arrange(yard)
    now = yard.wall()

    detail = imports.job_detail(yard.engine, read[EUR_MTL].id, now=now)
    assert detail is not None
    trades = detail.streams["trades"]
    assert trades["left"] is None and trades["pieces"] == []
    assert trades["share"] == {"verified": None, "of": None, "share": None, "unknown_because": imports.UNKNOWN_WINDOW}
    assert "no slice of it has found" in trades["share"]["unknown_because"]
    assert detail.verified["share"] is None and detail.verified["unknown_because"] == imports.UNKNOWN_WINDOW
    assert detail.job["window"] == {"bottom": None, "top": 130, "bottom_how": imports.UNKNOWN_WINDOW, "read": None}
    assert detail.job["streams_left"] is None
    assert detail.job["reason"]["code"] == "its_turn" and detail.job["started_at"] is None

    # A window given is known before the first slice: none of it is verified
    # yet. (Inside what the fake has got to: a boundary above its latest
    # ledger is waited for, not read.)
    given, _ = yard.plan(ISSUER, to_ledger=125, from_ledger=110)
    known = imports.job_detail(yard.engine, given.id, now=now)
    assert known is not None
    assert known.job["window"] == {"bottom": 110, "top": 125, "bottom_how": "the ledger the job was asked for", "read": None}
    assert known.job["streams_left"] == list(STREAMS)
    assert known.streams["effects"]["share"] == {"verified": 0, "of": 16, "share": 0.0, "unknown_because": None}
    assert known.streams["effects"]["left"] == 16
    assert (known.verified["verified"], known.verified["of"], known.verified["share"]) == (0, 16, 0.0)

    # One slice of one page: the issuer has no transaction at all, so the
    # first stream is verified empty over the whole window, and the others
    # wait whole. Nothing of the issuer is verified in every stream yet.
    with Session(yard.engine) as session:
        in_the_way = [job.id for job in session.scalars(select(HistoryJob).where(HistoryJob.subject.in_([P5, EUR_MTL])))]
    assert history_jobs.pause(yard.engine, ids=in_the_way, because="out of the way of this slice") == 2
    yard.worker = yard.new_worker("sliced", page_size=1, slice_requests=1)
    assert yard.worker.once() is True and history_jobs.get(yard.engine, given.id).slices == 1
    now = yard.wall()
    moved = imports.job_detail(yard.engine, given.id, now=now)
    assert moved is not None
    assert moved.job["window"]["read"] == [110, 125]
    assert moved.job["streams_left"] == ["operations", "effects", "trades"]
    assert moved.job["last_progress_at"] == at(now), "it moved forward when it stored the page"
    first = moved.streams["transactions"]
    assert first["verified"] == {"from": 110, "to": 125, "ledgers": 16, "spans": [[110, 125]]}
    assert first["gaps"] == {"pending": [], "unverifiable": []}
    assert first["left"] == 0 and first["share"]["share"] == 1.0
    assert first["cursor"] == {
        "token": None,
        "at_ledger": 110,
        "ledger_time": at(close_time(110)),
        "how": "verified down to here",
    }, "it read down to the bottom of its window: when that ledger closed is known, a transaction of it being stored"
    assert moved.streams["effects"]["gaps"] == {"pending": [[110, 125]], "unverifiable": []}
    assert moved.verified["share"] == 0.0
    assert (moved.job["reached_ledger"], moved.job["reached_ledger_time"]) == (110, at(close_time(110)))
    # Addresses made of small numbers share their beginning: the end tells them apart.
    table = imports.jobs(yard.engine, q=ISSUER[-12:], kind="account", now=now).items
    assert [item["id"] for item in table] == [given.id] and table[0]["streams_left"] == ["operations", "effects", "trades"]


def test_the_pages_read_the_database_alone_and_only(yard: Yard, monkeypatch: pytest.MonkeyPatch) -> None:
    yard.plan(B)
    assert finish(yard.worker) == 1
    engine = yard.engine

    # Read-only: a page that would write is refused by the database.
    with pytest.raises(DBAPIError, match="read-only"):
        with imports._reading(engine) as session:
            session.execute(text("UPDATE history_job SET priority = 5"))
    with Session(engine) as session:
        assert session.scalar(text("SELECT priority FROM history_job")) == 0

    # And no statement may stand in the worker's way: one that runs too long
    # is let go of, and said so.
    monkeypatch.setattr(imports, "STATEMENT_TIMEOUT", 0.05)
    with pytest.raises(imports.TooSlow, match="cannot be built"):
        with imports._reading(engine) as session:
            session.execute(text("SELECT pg_sleep(0.5)"))
    monkeypatch.undo()
    with imports._reading(engine) as session:
        session.execute(text("SELECT pg_sleep(0.01)"))  # the limit is the module's again
