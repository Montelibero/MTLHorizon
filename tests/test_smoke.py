"""The smoke test of the integration with a real Notifier, on the fake one:
one subscription, made by the test and deleted by it; a delivery of it
watched through the journal; nothing else touched; no request uncounted."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from sqlalchemy.orm import Session

from app.config import Settings
from app.notifier import format as notifier_format
from app.notifier.client import subscription_params
from app.smoke import cli as smoke_cli
from app.smoke import notifier as smoke
from app.storage import events as journal
from app.storage.models import TrackedAccount
from app.watch import accounts as watched
from app.worker.loader import Worker
from tests.fake_horizon import A, B, FakeHorizon, paid, serve
from tests.fake_notifier import notification
from tests.test_watch_lifecycle import CALLBACK, Life


def test_one_subscription_is_made_watched_and_deleted_and_nothing_else_is_touched(
    make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch
) -> None:
    life = Life(make_database())
    try:
        serve(monkeypatch, life.fake)
        api = life.api()
        asked = len(life.api_fake.requests)

        found = smoke.status(life.engine, life.settings, api)
        assert found["signer_known"] is True and found["signer"] == life.signer.public_key
        assert (found["at_notifier"], found["with_our_address"], found["test"], found["test_there"]) == (0, [], None, False)
        assert len(life.api_fake.requests) - asked == 2

        with pytest.raises(ValueError, match="G…"):
            smoke.subscribe(life.engine, life.settings, api, "not-an-account")
        without = Settings(**{**life.settings.__dict__, "notifier_signer_keys": ()})
        with pytest.raises(ValueError, match="not among MTL_HORIZON_NOTIFIER_KEYS"):
            smoke.subscribe(life.engine, without, api, A)

        made = smoke.subscribe(life.engine, life.settings, api, A, now=life.wall())
        assert made["created"] is True and made["id"]
        ours = life.api_fake.subscriptions
        assert len(ours) == 1 and ours[0]["account"] == A and ours[0]["reaction_url"] == CALLBACK and ours[0].get("filter_version") == 2
        again = smoke.subscribe(life.engine, life.settings, api, A, now=life.wall())
        assert again["created"] is False and again["id"] == made["id"] and len(life.api_fake.subscriptions) == 1, "not made twice"
        assert smoke.remembered(life.engine)["id"] == made["id"]
        found = smoke.status(life.engine, life.settings, api)
        assert found["test_there"] is True and found["with_our_address"] == [made["id"]]

        seen = smoke.watch(life.engine, timeout=0)
        assert (seen["arrived"], seen["finished"], seen["timed_out"], seen["good"]) == (0, False, True, False)

        # A transaction of the account: the Notifier delivers for the test subscription.
        life.fake.close(paid(131, A, B), latest=132)
        notice = notifier_format.read(notification(life.fake, str((131 << 32) | 1), subscription=made["id"]))
        journal.receive(life.engine, notice, received_at=life.wall(), headers={}, signed_by=life.signer.public_key)
        before = len(life.fake.requests)
        seen = smoke.watch(life.engine, timeout=0)
        assert (seen["arrived"], seen["finished"], seen["ledgers"]) == (1, False, [131]), "arrived, and its job is not done yet"
        with Worker(life.pool, life.engine, name="w", max_attempts=3, clock=life.wall) as worker:
            worker.once()
        seen = smoke.watch(life.engine, timeout=5)
        assert (seen["arrived"], seen["finished"], seen["timed_out"], seen["good"], seen["jobs"]) == (1, True, False, True, ["done"])
        assert len(life.fake.requests) > before, "the worker asked the fake; the watching itself asked nobody: it reads the journal"

        # Another subscription of this installation stays where it is.
        api.create(subscription_params("account", B, CALLBACK))
        assert len(life.api_fake.subscriptions) == 2
        gone = smoke.unsubscribe(life.engine, api)
        assert (gone["deleted"], gone["gone"], gone["id"]) == (True, True, made["id"])
        assert [sub["account"] for sub in life.api_fake.subscriptions] == [B]
        assert smoke.remembered(life.engine) is None
        nothing = smoke.unsubscribe(life.engine, api)
        assert (nothing["deleted"], nothing["id"], nothing["gone"]) == (False, None, True)
    finally:
        life.close()


def test_the_commands_as_its_owner_runs_them(make_database: Callable[[], str], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    life = Life(make_database())
    try:
        serve(monkeypatch, life.fake)
        monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", life.engine.url.render_as_string(hide_password=False))
        monkeypatch.setenv("MTL_HORIZON_NOTIFIER_API_URL", life.settings.notifier_api_url)
        monkeypatch.setenv("MTL_HORIZON_NOTIFIER_CALLBACK_URL", CALLBACK)
        monkeypatch.setenv("MTL_HORIZON_NOTIFIER_KEYS", life.signer.public_key)
        monkeypatch.setattr("app.smoke.cli.provision.make_api", lambda settings, engine=None, **options: life.api())
        assert smoke_cli.main(["status"]) == 0
        assert "signs with" in capsys.readouterr().out
        assert smoke_cli.main(["subscribe", "--account", A]) == 0
        assert "the test subscription is made" in capsys.readouterr().out
        assert smoke_cli.main(["watch", "--timeout", "0"]) == 1
        assert "nothing arrived in time" in capsys.readouterr().out
        assert smoke_cli.main(["unsubscribe"]) == 0
        assert "deleted, and seen to be gone" in capsys.readouterr().out
        assert smoke_cli.main(["watch", "--timeout", "0"]) == 1, "no test subscription: said so"
        assert life.api_fake.subscriptions == []
    finally:
        life.close()
