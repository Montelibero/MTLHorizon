"""Reading what Operations Notifier posts, and telling whether it is the
Notifier that posted it."""

from __future__ import annotations

import base64
from datetime import UTC, datetime
from typing import Any

import pytest
from stellar_sdk import Keypair

from app.notifier import format as notifier
from app.notifier.signature import HEADER, checked_key, signed_by
from tests.fake_horizon import A, B, C, D, EUR, ISSUER, MTL, RECT, FakeHorizon, sale
from tests.fake_notifier import FakeNotifier, notification

T1 = str((100 << 32) | 1)  # A pays B, ledger 100
T3 = str((104 << 32) | (1 << 12) | 1)  # the fee-bump path payment
T7 = str((112 << 32) | 1)  # C sells RECT for EUR
UNKNOWN = str((110 << 32) | (2 << 12) | 1)  # an operation type nobody knows


def read(**changes: Any) -> notifier.Notice:
    fake = FakeHorizon()
    payload = notification(fake, T1)
    payload["operation"].update(changes)
    return notifier.read(payload)


def test_a_payment_is_read_with_everything_it_names() -> None:
    notice = read()
    assert notice.told and notice.problems == ()
    assert notice.type == "operation" and notice.operation_type == "payment"
    assert notice.transaction_hash == FakeHorizon().ledger.operations[T1]["transaction_hash"]
    assert (notice.ledger, notice.operation_id, notice.transaction_toid) == (100, int(T1), 100 << 32)
    assert notice.accounts == {A, B} and notice.assets == {EUR} and notice.markets == frozenset()
    assert notice.created == datetime(2026, 11, 25, 12, 0, tzinfo=UTC)


def test_an_assets_number_becomes_its_name() -> None:
    assert notifier.asset_id({"asset_type": 0}) == "native"
    assert notifier.asset_id({"asset_type": 1, "asset_code": "MTL", "asset_issuer": ISSUER}) == MTL
    assert notifier.asset_id({"asset_type": 2, "asset_code": "EURMTL", "asset_issuer": ISSUER}) == EUR
    assert notifier.asset_id({"asset_type": 2, "asset_code": "EURMTL", "asset_issuer": "not an issuer"}) is None
    assert notifier.asset_id({"asset_type": 3, "asset_code": "X", "asset_issuer": ISSUER}) is None  # a kind this reader does not know
    assert notifier.asset_id("native") is None


def test_the_trades_of_an_offer_name_the_other_side_and_the_market() -> None:
    fake = FakeHorizon()
    notice = notifier.read(notification(fake, T7))
    trade = notice.operation["trades"][0]
    assert trade["type"] == "order_book" and trade["seller_id"] == C
    assert notice.markets == frozenset({(RECT, EUR)})
    # The buyer is named nowhere in the operation: only the offer's owner is, in the trade.
    assert notice.assets == {RECT, EUR} and notice.accounts == {C}
    assert notice.told


def test_an_operation_of_a_type_nobody_knows_is_kept_and_says_so() -> None:
    fake = FakeHorizon()
    notice = notifier.read(notification(fake, UNKNOWN))
    assert notice.operation_type == "unknown" and notice.accounts == {A}
    assert not notice.told
    assert notice.problems == ("operation type 'unknown' is not one this reader knows the fields of",)


def test_a_muxed_payment_is_read_by_its_base_accounts() -> None:
    notice = read(
        destination=B,
        destination_muxed="MBEPZKYCZBZV2AM4C4A6NFAC2RJKVHUZ6HRQ3JFMZGLK442LHBMK2SAAAAAAAAAAAHF5Y",
        destination_muxed_id="123456789",
    )
    assert notice.told and notice.accounts == {A, B}


def test_the_ledger_is_taken_from_the_transaction_and_checked_against_the_operation() -> None:
    fake = FakeHorizon()
    payload = notification(fake, T1)
    payload["transaction"]["paging_token"] = str(101 << 32)
    notice = notifier.read(payload)
    assert notice.ledger is None and not notice.told
    assert notice.problems[0].startswith("the operation lies in ledger 100, the transaction in ledger 101")

    payload = notification(fake, T1)
    del payload["transaction"]["paging_token"]
    notice = notifier.read(payload)
    assert notice.ledger == 100 and notice.transaction_toid is None and not notice.told
    assert "the transaction has no paging_token that is a total order id" in notice.problems


def test_an_operation_id_that_is_not_one_is_a_problem() -> None:
    notice = read(id="12")
    assert notice.operation_id is None and not notice.told
    notice = read(type="payment", type_i=13)
    assert any("does not go with its number" in problem for problem in notice.problems)


def test_what_cannot_be_told_from_a_delivery_is_refused_before_it_is_kept() -> None:
    for payload in ([], {}, {"subscription": "x"}, {"id": ""}, {"id": True, "subscription": "s"}):
        with pytest.raises(notifier.Unreadable):
            notifier.read(payload)


def test_a_notification_of_another_type_is_kept_and_says_so() -> None:
    fake = FakeHorizon()
    payload = notification(fake, T1)
    payload["type"] = "transaction"
    notice = notifier.read(payload)
    assert notice.type == "transaction" and not notice.told
    assert notice.problems[0] == "notification type 'transaction' is not one this reader knows"


def test_a_timestamp_without_a_zone_is_read_as_utc_and_a_bad_one_as_absent() -> None:
    fake = FakeHorizon()
    payload = notification(fake, T1)
    payload["created"], payload["sent"] = "2026-11-25T12:00:00", "yesterday"
    notice = notifier.read(payload)
    assert notice.created == datetime(2026, 11, 25, 12, 0, tzinfo=UTC) and notice.sent is None


def test_a_passive_offer_filled_by_somebody_else_names_its_owner_in_the_trades() -> None:
    """What v0.12.0 decodes: the owner of the passive offer is nowhere in the
    operation but in its trades."""
    fake = FakeHorizon()
    fake.close(sale(125, offer_owner=D), latest=126)
    notice = notifier.read(notification(fake, str((125 << 32) | 1)))
    assert D in notice.accounts and notice.markets == frozenset({(RECT, EUR)})


# -- the signature ---------------------------------------------------------------


def test_a_delivery_signed_by_a_known_key_is_the_notifier_s() -> None:
    notifier_key = FakeNotifier()
    other = FakeNotifier()
    body, headers = notifier_key.post({"id": "1", "subscription": "s"})
    assert signed_by([other.public_key, notifier_key.public_key], body, headers[HEADER]) == notifier_key.public_key
    assert signed_by([notifier_key.public_key], body + b" ", headers[HEADER]) is None  # tampered with
    assert signed_by([notifier_key.public_key], body, None) is None
    assert signed_by([notifier_key.public_key], body, "not base64!") is None
    assert signed_by([notifier_key.public_key], body, base64.b64encode(b"short").decode()) is None
    assert signed_by([], body, headers[HEADER]) is None  # no key configured: nothing is the Notifier's


def test_a_signing_key_is_a_stellar_public_key() -> None:
    key = Keypair.random().public_key
    assert checked_key(f" {key} ") == key
    with pytest.raises(ValueError):
        checked_key("not a key")
