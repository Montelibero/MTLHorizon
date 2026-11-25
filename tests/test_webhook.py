"""The webhook: what it takes, what it keeps before it answers, and what it
makes of what it kept."""

from __future__ import annotations

import base64
import os
import threading
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from app.api.app import WEBHOOK_BODY_LIMIT, WEBHOOK_PATH, create_app
from app.backfill import config as watch_lists
from app.config import PUBLIC_NETWORK_PASSPHRASE, Settings
from app.notifier import format as format_module
from app.notifier.signature import HEADER
from app.storage import events, jobs
from app.storage.models import NotifierEvent, ProcessingJob
from tests.fake_horizon import A, D, EUR, ISSUER, RECT, FakeHorizon
from tests.fake_notifier import FakeNotifier, notification
from tests.test_backfill import WATCH_LIST

PATH = WEBHOOK_PATH
T1 = str((100 << 32) | 1)  # A pays B
T2 = str((100 << 32) | (3 << 12) | 1)  # C writes a data entry
T5 = str((110 << 32) | (2 << 12) | 1)  # an operation of a type nobody knows
T6 = str((110 << 32) | (2 << 12) | 2)  # A pays C, in the same transaction
T7 = str((112 << 32) | 1)  # C sells RECT for EUR; D is the other side
UNREACHABLE = "postgresql+psycopg://user:not-a-password@127.0.0.1:1/mtl_horizon"

Site = SimpleNamespace


@pytest.fixture
def site(make_database: Callable[[], str]) -> Any:
    url = make_database()
    engine = create_engine(url)
    with Session(engine) as session, session.begin():
        watch_lists.apply(session, WATCH_LIST)
    notifier = FakeNotifier()
    return Site(
        url=url,
        engine=engine,
        notifier=notifier,
        fake=FakeHorizon(),
        settings=Settings(
            database_url=url, network_passphrase=PUBLIC_NETWORK_PASSPHRASE, notifier_signer_keys=(notifier.public_key,)
        ),
    )


def post(client: TestClient, notifier: FakeNotifier, payload: dict[str, Any], **headers: str) -> Any:
    body, sent = notifier.post(payload)
    return client.post(PATH, content=body, headers={**sent, **headers})


def kept(site: Any) -> list[NotifierEvent]:
    with Session(site.engine) as session:
        return list(session.scalars(select(NotifierEvent).order_by(NotifierEvent.received_at, NotifierEvent.subscription)))


def queue(site: Any) -> list[ProcessingJob]:
    with Session(site.engine) as session:
        return list(session.scalars(select(ProcessingJob).order_by(ProcessingJob.id)))


# -- what is taken, and what is not ----------------------------------------------


def test_a_signed_notification_is_kept_and_its_transaction_queued_before_the_answer(site: Any) -> None:
    payload = notification(site.fake, T1)
    with TestClient(create_app(site.settings)) as client:
        answer = post(client, site.notifier, payload)

    assert answer.status_code == 200
    assert answer.json() == {"ok": True, "stored": True, "state": "queued", "job_id": 1}
    # Seen from outside, after the answer: it was committed, not merely made.
    with create_engine(site.url).connect() as outside:
        assert outside.execute(select(func.count()).select_from(NotifierEvent)).scalar() == 1
        assert outside.execute(select(func.count()).select_from(ProcessingJob)).scalar() == 1
    [event] = kept(site)
    [job] = queue(site)
    assert (event.id, event.subscription, event.state, event.job_id) == (payload["id"], payload["subscription"], "queued", job.id)
    assert [rule["rule"] for rule in event.matched] == ["account", "account", "asset"]  # A and B are watched, and so is EUR
    assert event.payload == payload and event.signed_by == site.notifier.public_key  # as it came
    assert HEADER not in event.headers and event.headers["User-Agent"] == "StellarNotifier/0.12.0"
    assert event.ledger_sequence == 100 and event.received_at is not None
    assert (job.state, job.transaction_hash, job.transaction_toid, job.ledger_sequence) == (
        "pending", payload["transaction"]["hash"], 100 << 32, 100
    )


def test_a_delivery_sent_again_changes_nothing_and_is_answered_all_the_same(site: Any) -> None:
    """The answer to the first was lost, or the Notifier sends again for its
    own reasons: one notification, one subscription, one row."""
    payload = notification(site.fake, T1)
    with TestClient(create_app(site.settings)) as client:
        assert post(client, site.notifier, payload).json()["stored"] is True
        for _ in range(3):
            again = post(client, site.notifier, payload)
            assert again.status_code == 200 and again.json()["stored"] is False
    assert len(kept(site)) == 1 and len(queue(site)) == 1


def test_deliveries_sent_at_the_same_time_are_kept_once(site: Any) -> None:
    payload = notification(site.fake, T1)
    answers: list[int] = []
    with TestClient(create_app(site.settings)) as client:

        def deliver() -> None:
            answers.append(post(client, site.notifier, payload).status_code)

        threads = [threading.Thread(target=deliver) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
    assert answers == [200] * 8
    assert len(kept(site)) == 1 and len(queue(site)) == 1


def test_one_transaction_in_several_notifications_makes_one_job_and_keeps_every_delivery(site: Any) -> None:
    """Two operations of one transaction, each delivered for two subscriptions:
    four deliveries kept, each with what matched it; one job."""
    with TestClient(create_app(site.settings)) as client:
        for operation, subscription in ((T5, "s1"), (T5, "s2"), (T6, "s1"), (T6, "s2")):
            assert post(client, site.notifier, notification(site.fake, operation, subscription=subscription)).status_code == 200
    events_kept, jobs_queued = kept(site), queue(site)
    assert len(events_kept) == 4 and len(jobs_queued) == 1
    assert {event.job_id for event in events_kept} == {jobs_queued[0].id}
    assert {(event.id, event.subscription) for event in events_kept}.__len__() == 4
    by_type = {event.operation_type: event.matched for event in events_kept}
    # The payment to C: A is watched and so is EUR; C is switched off and not named.
    assert [rule["key"] for rule in by_type["payment"]] == [A, EUR]
    assert [rule["key"] for rule in by_type["unknown"]] == [A]  # the operation nobody knows names an account that is watched


# -- what is not taken -----------------------------------------------------------


def test_what_is_not_signed_by_the_notifier_is_not_read(site: Any) -> None:
    payload = notification(site.fake, T1)
    body, headers = site.notifier.post(payload)
    stranger = FakeNotifier()
    forged = stranger.post(payload)[1]
    with TestClient(create_app(site.settings)) as client:
        assert client.post(PATH, content=body, headers=forged).status_code == 401  # signed, by somebody else
        tampered = dict(headers)
        tampered[HEADER] = site.notifier.post({**payload, "id": "another"})[1][HEADER]
        assert client.post(PATH, content=body.replace(b'"payment"', b'"account_merge"'), headers=tampered).status_code == 401
        without = {name: value for name, value in headers.items() if name != HEADER}
        assert client.post(PATH, content=body, headers=without).status_code == 401
        assert client.post(PATH, content=body, headers={**without, HEADER: "AAAA"}).status_code == 401
        assert client.post(PATH, json=payload).status_code == 401  # the body is right, unsigned
        assert client.post(PATH, content=b"", headers=headers).status_code == 401
    assert kept(site) == [] and queue(site) == []


def test_signed_and_not_a_notification_is_refused_and_nothing_kept(site: Any) -> None:
    """Signed by the Notifier, and not something a delivery can be kept as."""
    with TestClient(create_app(site.settings)) as client:
        for body in (b"[1, 2]", b"not json at all", b'{"id": "x"}', b'{"subscription": "s"}'):
            headers = {"Content-Type": "application/json"}
            headers[HEADER] = base64.b64encode(site.notifier.keypair.sign(body)).decode()  # signed, as over any bytes
            answer = client.post(PATH, content=body, headers=headers)
            assert answer.status_code == 400, body
    assert kept(site) == []


def test_a_body_too_large_is_refused_before_anything_is_read_of_it(site: Any) -> None:
    with TestClient(create_app(site.settings)) as client:
        answer = client.post(PATH, content=b"x" * (WEBHOOK_BODY_LIMIT + 1), headers={HEADER: "AAAA"})
        assert answer.status_code == 413
    assert kept(site) == []


def test_with_no_signing_key_configured_nothing_is_accepted(site: Any) -> None:
    settings = Settings(database_url=site.url, network_passphrase=PUBLIC_NETWORK_PASSPHRASE)
    payload = notification(site.fake, T1)
    with TestClient(create_app(settings)) as client:
        answer = post(client, site.notifier, payload)
        assert answer.status_code == 503 and "MTL_HORIZON_NOTIFIER_KEYS" in answer.json()["detail"]
    assert kept(site) == [] and queue(site) == []


def test_when_the_database_does_not_answer_nothing_is_confirmed(site: Any) -> None:
    settings = Settings(
        database_url=UNREACHABLE, network_passphrase=PUBLIC_NETWORK_PASSPHRASE, notifier_signer_keys=(site.notifier.public_key,)
    )
    payload = notification(site.fake, T1)
    with TestClient(create_app(settings)) as client:
        answer = post(client, site.notifier, payload)
        assert answer.status_code == 500 and "not-a-password" not in answer.text
    with create_engine(site.url).connect() as outside:  # the database that is there is untouched
        assert outside.execute(select(func.count()).select_from(NotifierEvent)).scalar() == 0


# -- what a delivery makes --------------------------------------------------------


def test_no_enabled_rule_means_no_job_and_a_diagnosis(site: Any) -> None:
    """C is switched off, and a data entry names no asset: kept, not worked."""
    with TestClient(create_app(site.settings)) as client:
        answer = post(client, site.notifier, notification(site.fake, T2))
    assert answer.status_code == 200 and answer.json()["state"] == "unmatched" and answer.json()["job_id"] is None
    [event] = kept(site)
    assert event.matched == [] and "no enabled account, asset or market" in (event.diagnosis or "")
    assert queue(site) == []


def test_an_enabled_asset_brings_a_transaction_in_whatever_the_accounts_status(site: Any) -> None:
    """C is switched off here; the market and its assets are on. A trade of C
    in that market is brought in — and so is D's side of it."""
    with TestClient(create_app(site.settings)) as client:
        answer = post(client, site.notifier, notification(site.fake, T7))
    assert answer.json()["state"] == "queued"
    [event] = kept(site)
    assert [rule["rule"] for rule in event.matched] == ["asset", "asset", "market"]
    assert {rule["key"] for rule in event.matched if rule["rule"] == "asset"} == {EUR, RECT}
    assert event.diagnosis is None and len(queue(site)) == 1


def test_a_market_rule_matches_on_its_own(make_database: Callable[[], str]) -> None:
    """Only the market is watched here: neither account nor asset."""
    url = make_database()
    engine = create_engine(url)
    lists = watch_lists.parse({"markets": [{"base": f"MTLRECT:{ISSUER}", "counter": f"EURMTL:{ISSUER}"}]}, "test")
    with Session(engine) as session, session.begin():
        watch_lists.apply(session, lists)
    fake, notifier = FakeHorizon(), FakeNotifier()
    settings = Settings(database_url=url, network_passphrase=PUBLIC_NETWORK_PASSPHRASE, notifier_signer_keys=(notifier.public_key,))
    with TestClient(create_app(settings)) as client:
        answer = post(client, notifier, notification(fake, T7))
    assert answer.json()["state"] == "queued"
    with Session(engine) as session:
        [event] = session.scalars(select(NotifierEvent))
    assert event.matched == [{"rule": "market", "key": f"{EUR} / {RECT}"}]  # an unordered pair, stored base first
    engine.dispose()


def test_what_cannot_be_told_is_not_dropped(site: Any) -> None:
    """An operation of a type this reader does not know, by an account nobody
    watches here: whether a rule matches cannot be said — and the Notifier
    matched it. Kept, queued, and the reason kept with it."""
    payload = notification(site.fake, T5)
    payload["operation"]["account"] = D
    with TestClient(create_app(site.settings)) as client:
        answer = post(client, site.notifier, payload)
    assert answer.status_code == 200 and answer.json()["state"] == "queued"
    [event] = kept(site)
    assert event.matched == [] and (event.diagnosis or "").startswith("queued although whether a watch rule matches could not be told")
    assert "operation type 'unknown'" in (event.diagnosis or "")
    assert len(queue(site)) == 1


def test_a_notification_of_another_kind_is_kept_and_not_worked(site: Any) -> None:
    payload = notification(site.fake, T1)
    payload["type"] = "transaction"
    with TestClient(create_app(site.settings)) as client:
        answer = post(client, site.notifier, payload)
    assert answer.status_code == 200 and answer.json()["state"] == "unsupported"
    [event] = kept(site)
    assert event.state == events.UNSUPPORTED and queue(site) == []


def test_word_that_comes_again_takes_a_job_that_had_given_up_back_into_the_queue_a_limited_number_of_times(
    site: Any,
) -> None:
    def gave_up() -> None:
        with Session(site.engine) as session, session.begin():
            job = session.get(ProcessingJob, 1)
            job.state, job.attempts = "partial", 8

    with TestClient(create_app(site.settings)) as client:
        post(client, site.notifier, notification(site.fake, T1))
        for number in range(events.MAX_REVIVALS):
            gave_up()
            post(client, site.notifier, notification(site.fake, T1, subscription=f"again-{number}"))
            assert queue(site)[0].state == "pending"
        gave_up()
        post(client, site.notifier, notification(site.fake, T1, subscription="once-too-many"))
    [job] = queue(site)
    assert job.state == "partial" and job.revivals == events.MAX_REVIVALS  # then only a person takes it back
    assert len(kept(site)) == events.MAX_REVIVALS + 2


# -- the module, without HTTP ----------------------------------------------------


def test_receive_keeps_a_delivery_once_and_makes_the_job_with_it(site: Any) -> None:
    from datetime import UTC, datetime

    notice = format_module.read(notification(site.fake, T1))
    first = events.receive(
        site.engine, notice, received_at=datetime.now(UTC), headers={"User-Agent": "StellarNotifier/0.12.0", HEADER: "secret-looking"}, signed_by="G-key"
    )
    again = events.receive(site.engine, notice, received_at=first.event.received_at, headers={}, signed_by="G-key")
    assert first.created and not again.created and again.event.id == first.event.id
    assert HEADER not in first.event.headers and first.event.headers == {"User-Agent": "StellarNotifier/0.12.0"}
    assert jobs.counts(site.engine) == {"pending": 1, "running": 0, "done": 0, "partial": 0, "dead": 0}


# -- stage 05: what this installation asked for itself, and discovery ------------------


def _payment_in(asset: str, account: str, *, subscription: str, notification_id: str | None = None) -> dict[str, Any]:
    """A notification of a payment in ``asset`` by ``account``, for a
    subscription of the Notifier: the fields the Notifier gives a payment."""
    from tests.factories import address

    from app.notifier.format import G_ADDRESS

    code, issuer = asset.split(":", 1)
    return {
        "id": notification_id or base64.b16encode(os.urandom(12)).decode().lower(),
        "subscription": subscription,
        "type": "operation",
        "created": "2026-11-25T12:00:00.000Z",
        "sent": "2026-11-25T12:00:01.000Z",
        "operation": {
            "type_i": 1, "type": "payment", "account": account, "destination": address(400),
            "amount": "1.0000000", "asset": {"asset_type": 1 if len(code) <= 4 else 2, "asset_code": code, "asset_issuer": issuer},
            "id": str((140 << 32) | (1 << 12) | 1),
        },
        "transaction": {"hash": "ab" * 32, "paging_token": str((140 << 32) | (1 << 12))},
    }


def _mark_asset(site: Any, asset: str, **flags: bool) -> None:
    from app.storage.models import TrackedAsset

    with Session(site.engine) as session, session.begin():
        tracked = session.get(TrackedAsset, asset)
        assert tracked is not None
        for name, value in flags.items():
            setattr(tracked, name, value)


def test_a_delivery_for_a_subscription_this_installation_manages_is_brought_in_whatever_the_local_rules_say(
    site: Any,
) -> None:
    """The owner registered an asset subscription (this installation did).
    A payment in it by an account nobody watches here is brought in: the
    subscription is the decision. Before, it was kept without a job."""
    from app.storage.models import WatchSubscription

    _mark_asset(site, RECT, enabled=False, discover=False)
    with Session(site.engine) as session, session.begin():
        session.add(WatchSubscription(kind="asset", key=RECT, remote_id="sub-of-ours", state="active", managed=True))
    payload = _payment_in(RECT, D, subscription="sub-of-ours")
    with TestClient(create_app(site.settings)) as client:
        answer = post(client, site.notifier, payload)
    assert answer.status_code == 200 and answer.json()["state"] == "queued" and answer.json()["job_id"] is not None
    [event] = kept(site)
    assert event.matched == [] and "queued for this installation's subscription (asset" in (event.diagnosis or "")
    assert len(queue(site)) == 1


def test_a_subscription_on_its_way_out_no_longer_decides(site: Any) -> None:
    from app.storage.models import WatchSubscription

    _mark_asset(site, RECT, enabled=False, discover=False)
    with Session(site.engine) as session, session.begin():
        session.add(WatchSubscription(kind="asset", key=RECT, remote_id="sub-going", state="retiring", managed=True))
    with TestClient(create_app(site.settings)) as client:
        answer = post(client, site.notifier, _payment_in(RECT, D, subscription="sub-going"))
    assert answer.json()["state"] == "unmatched" and queue(site) == []


def test_an_asset_marked_for_discovery_is_a_rule_of_its_own(site: Any) -> None:
    """The asset's history is not read (``enabled`` false), its holders are
    looked for (``discover`` true): an operation in it is brought in, so
    that a trustline opened on it is seen."""
    _mark_asset(site, RECT, enabled=False, discover=True)
    with TestClient(create_app(site.settings)) as client:
        answer = post(client, site.notifier, _payment_in(RECT, D, subscription="sub-of-ours"))
    assert answer.json()["state"] == "queued"
    [event] = kept(site)
    assert event.matched == [{"rule": "discovery", "key": RECT}] and len(queue(site)) == 1


def test_an_asset_nobody_reads_nor_discovers_makes_no_job(site: Any) -> None:
    _mark_asset(site, RECT, enabled=False, discover=False)
    with TestClient(create_app(site.settings)) as client:
        answer = post(client, site.notifier, _payment_in(RECT, D, subscription="somebody-elses"))
    assert answer.json()["state"] == "unmatched"
    assert "the subscription it came for is not one this installation manages" in kept(site)[0].diagnosis
    assert queue(site) == []
