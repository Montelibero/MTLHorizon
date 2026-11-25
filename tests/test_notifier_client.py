"""The client of the Notifier's management API against a fake of that API:
what it sends, what it makes of the answers, and what the fake says — which
is what the Notifier's code says at ``e4039fb`` (v0.12.0), also where that is
not what one would expect of it. No network and no database: the fake answers
in the process."""

from __future__ import annotations

import contextlib
import re
import traceback
from collections.abc import Callable, Iterator
from datetime import datetime
from typing import Any

import httpx
import pytest
from stellar_sdk import Keypair

from app import __version__
from app.notifier.client import (
    FILTER_VERSION,
    NotifierClient,
    NotifierError,
    NotifierMalformed,
    NotifierRejected,
    NotifierUnavailable,
    remote_id,
    same_subscription,
    subscription_params,
)
from tests.fake_notifier_api import ASSET_HELP, CANNOT_BE_MODIFIED, MAX_ACTIVE, OPERATION_TYPES_HELP, FakeNotifierApi

ORIGIN = "https://notifier.test"
CALLBACK = "https://horizon.test/webhook/notifier"
PUBLIC_KEY = Keypair.random().public_key
ACCOUNT = Keypair.random().public_key
OTHER_ACCOUNT = Keypair.random().public_key
ISSUER = Keypair.random().public_key
# Made up for these tests; nothing of the Notifier's own configuration.
ADMIN_TOKEN = "admin-token-made-up-for-tests"
USER_TOKEN = "user-token-made-up-for-tests"
OTHER_TOKEN = "other-user-token-made-up-for-tests"
# In none of the allowlists of these tests.
UNTAKEN_TOKEN = "a-token-that-no-notifier-takes-made-up-for-tests"
UNKNOWN_ID = "0123456789abcdef01234567"

# What the Notifier says of both limits. The error is named forbidden, but on POST it is sent as a 400.
LIMIT = "Forbidden. " + MAX_ACTIVE
NOT_MINE = "Forbidden"  # the reason phrase: a 403 of the error handler comes without a body


class Seen(httpx.BaseTransport):
    """What the client sent, kept as it was sent: the headers and the timeouts too."""

    def __init__(self, transport: httpx.BaseTransport) -> None:
        self.inner, self.requests = transport, []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self.inner.handle_request(request)

    def close(self) -> None:
        self.inner.close()


def connected(api: FakeNotifierApi, token: str = "", **options: Any) -> tuple[NotifierClient, Seen]:
    """A client of the fake, and what it sent. Not named ``client``: in a test
    that binds its result to ``client``, the name would be the local one."""
    seen = Seen(api.transport)
    return NotifierClient(ORIGIN, token=token, transport=seen, **options), seen


def of(api: FakeNotifierApi, token: str = "") -> NotifierClient:
    return NotifierClient(ORIGIN, token=token, transport=api.transport)


def tokens_api(**options: Any) -> FakeNotifierApi:
    """The Notifier in the ``token`` mode: two user tokens taken, and the admin."""
    return FakeNotifierApi(
        PUBLIC_KEY, authorization="token", admin_token=ADMIN_TOKEN, user_tokens=[USER_TOKEN, OTHER_TOKEN], **options
    )


def answering(status_code: int = 200, **body: Any) -> httpx.MockTransport:
    """A transport with one answer for everything: for answers the Notifier never gives."""
    return httpx.MockTransport(lambda request: httpx.Response(status_code, **body))


def account(address: str = ACCOUNT, callback: str = CALLBACK, **changes: Any) -> dict[str, Any]:
    """The parameters of an account subscription."""
    return {**subscription_params("account", address, callback), **changes}


def asset(code: str = "EURMTL", callback: str = CALLBACK) -> dict[str, Any]:
    return subscription_params("asset", f"{code}:{ISSUER}", callback)


def native(callback: str = CALLBACK) -> dict[str, Any]:
    return subscription_params("asset", "native", callback)


def listed(**fields: Any) -> dict[str, Any]:
    """A subscription as a listing shows it, made by hand: somebody else's, and with no filter unless written."""
    return {"id": UNKNOWN_ID, "status": 0, "pubkey": "somebody", **fields}


@contextlib.contextmanager
def refused(status: int, detail: str) -> Iterator[None]:
    """A ``NotifierRejected`` with that status and that detail — and nothing of the status left out of its message."""
    with pytest.raises(NotifierRejected) as caught:
        yield
    assert (caught.value.status, caught.value.detail) == (status, detail)
    assert str(status) in str(caught.value)


def ask(api: FakeNotifierApi, method: str, path: str, **options: Any) -> httpx.Response:
    """A request straight to the fake, the way no client of it would send."""
    return api.transport.handle_request(httpx.Request(method, ORIGIN + path, **options))


def test_the_token_is_sent_in_authorization_and_nothing_else_carries_it() -> None:
    api = FakeNotifierApi(PUBLIC_KEY, authorization="token", user_tokens=[USER_TOKEN])
    client, seen = connected(api, token=USER_TOKEN)
    client.status()
    (request,) = seen.requests
    assert request.headers["authorization"] == USER_TOKEN
    assert "x-access-token" not in request.headers
    assert request.headers["user-agent"] == f"mtl-horizon/{__version__}"
    assert request.headers["accept"] == "application/json"


def test_without_a_token_no_authorization_header_is_sent() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, seen = connected(api)
    client.list_subscriptions()
    assert "authorization" not in seen.requests[0].headers
    assert "x-access-token" not in seen.requests[0].headers


def test_with_authorization_disabled_no_token_is_needed() -> None:
    api = FakeNotifierApi(PUBLIC_KEY, admin_token=ADMIN_TOKEN)
    client, _ = connected(api)
    made = client.create(account())
    assert made["pubkey"] is None  # the anonymous admin owns it
    assert [remote_id(one) for one in client.list_subscriptions()] == [made["id"]]
    assert client.delete(made["id"]) is True
    assert client.status()["subscriptions"] == 0


def test_in_the_token_mode_a_token_that_is_not_taken_is_refused_with_nothing_said() -> None:
    api = FakeNotifierApi(PUBLIC_KEY, authorization="token", user_tokens=[USER_TOKEN])
    client, _ = connected(api, token=OTHER_TOKEN)
    # The token is judged before any route is: /api/status, which needs no token, is refused too.
    for asking in (client.status, client.list_subscriptions, lambda: client.create(account())):
        with refused(401, "Unauthorized"):
            asking()
    assert api.subscriptions == []


def test_in_the_token_mode_a_route_that_needs_a_user_refuses_a_request_without_a_token() -> None:
    api = FakeNotifierApi(PUBLIC_KEY, authorization="token", user_tokens=[USER_TOKEN])
    client, _ = connected(api)
    assert client.status()["version"] == "0.12.0"  # this one needs none
    with refused(401, "Unauthorized"):
        client.list_subscriptions()


@pytest.mark.parametrize("mode", ["disabled", "token", "ed25519"])
def test_the_admin_token_is_taken_in_every_mode(mode: str) -> None:
    api = FakeNotifierApi(PUBLIC_KEY, authorization=mode, admin_token=ADMIN_TOKEN, user_tokens=[USER_TOKEN])
    made = of(api, ADMIN_TOKEN).create(account())
    assert made["pubkey"] is None and remote_id(made)


def test_a_user_sees_only_its_own_subscriptions_and_the_admin_sees_all_of_them() -> None:
    api = tokens_api()
    user, other, admin = of(api, USER_TOKEN), of(api, OTHER_TOKEN), of(api, ADMIN_TOKEN)
    mine = [user.create(account()), user.create(account(OTHER_ACCOUNT))]
    theirs = other.create(asset())
    assert [one["id"] for one in user.list_subscriptions()] == [one["id"] for one in mine]
    assert all(one["pubkey"] == USER_TOKEN for one in user.list_subscriptions())  # a user is its token
    assert [one["id"] for one in other.list_subscriptions()] == [theirs["id"]]
    assert {one["id"] for one in admin.list_subscriptions()} == {theirs["id"], *(one["id"] for one in mine)}


def test_with_no_allowlist_every_token_is_a_user_of_its_own() -> None:
    api = FakeNotifierApi(PUBLIC_KEY, authorization="token")
    made = of(api, USER_TOKEN).create(account())
    assert made["pubkey"] == USER_TOKEN
    # The same parameters under another token are another owner's subscription.
    assert of(api, OTHER_TOKEN).create(account())["id"] != made["id"]


def test_the_scheme_of_the_credentials_is_taken_off_and_x_access_token_is_read_first() -> None:
    api = FakeNotifierApi(PUBLIC_KEY, authorization="token", user_tokens=[USER_TOKEN])
    for credentials in (USER_TOKEN, f"Bearer {USER_TOKEN}", f"token {USER_TOKEN}", f"ED25519 {USER_TOKEN}"):
        assert ask(api, "GET", "/api/subscription", headers={"Authorization": credentials}).status_code == 200
    both = {"X-Access-Token": USER_TOKEN, "Authorization": "not a token that is taken"}
    assert ask(api, "GET", "/api/subscription", headers=both).status_code == 200


def test_status_says_what_the_notifier_is_and_how_much_it_watches() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    empty = {
        "version": "0.12.0",
        "publicKey": PUBLIC_KEY,
        "observing": True,
        "subscriptions": 0,
        "lostNotifications": 0,
    }
    assert {name: client.status().get(name) for name in empty} == empty
    client.create(account())
    client.create(asset())
    assert client.status()["subscriptions"] == 2


def test_the_parameters_of_a_subscription_are_what_the_api_documents() -> None:
    assert account() == {"reaction_url": CALLBACK, "account": ACCOUNT, "filter_version": FILTER_VERSION}
    assert asset() == {
        "reaction_url": CALLBACK,
        "asset_code": "EURMTL",
        "asset_issuer": ISSUER,
        "filter_version": FILTER_VERSION,
    }
    four = {"reaction_url": CALLBACK, "asset_code": "EUR", "asset_issuer": ISSUER, "filter_version": FILTER_VERSION}
    assert asset("EUR") == four
    assert native() == {"reaction_url": CALLBACK, "asset_code": "XLM", "filter_version": FILTER_VERSION}
    assert account(filter_version=1)["filter_version"] == 1


@pytest.mark.parametrize(
    "kind, key",
    [
        ("account", "not an address"),
        ("account", ACCOUNT[:-1]),  # one character short
        ("account", "S" + ACCOUNT[1:]),  # not even shaped like a public key
        ("asset", "EURMTL"),
        ("asset", "EURMTL:not an issuer"),
        ("asset", "CODEWAYTOOLONG:" + ISSUER),
        ("asset", ""),
        ("offer", ACCOUNT),
        ("", "native"),
    ],
)
def test_what_cannot_be_subscribed_to_is_refused_here(kind: str, key: str) -> None:
    with pytest.raises(ValueError):
        subscription_params(kind, key, CALLBACK)


def test_one_subscription_is_posted_as_itself_and_comes_back_with_an_id() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    made = client.create(account())
    assert remote_id(made) and re.fullmatch("[0-9a-f]{24}", made["id"])
    assert (made["account"], made["reaction_url"], made["filter_version"], made["status"]) == (ACCOUNT, CALLBACK, 2, 0)
    assert api.requests == [("POST", "/api/subscription", account())]
    assert api.subscriptions == [made]


def test_a_batch_is_posted_as_an_array_at_the_root_and_comes_back_in_its_order() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    sent = [account(), account(OTHER_ACCOUNT), asset()]
    made = client.create(sent, batch=True)
    assert [one.get("account") for one in made] == [ACCOUNT, OTHER_ACCOUNT, None]
    assert len({one["id"] for one in made}) == 3
    assert api.requests == [("POST", "/api/subscription", sent)]
    assert [one["id"] for one in api.subscriptions] == [one["id"] for one in made]


def test_a_sequence_goes_as_a_batch_and_a_single_mapping_with_the_flag_too() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    assert len(client.create([account(), asset()])) == 2
    (one,) = client.create(native(), batch=True)
    assert one["asset_type"] == 0
    assert [request[2] for request in api.requests] == [[account(), asset()], [native()]]
    for wrong in ("a string", 42, [account(), "not a mapping"], object()):
        with pytest.raises(TypeError):
            client.create(wrong)  # type: ignore[arg-type]


def test_the_same_parameters_posted_twice_are_one_subscription() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    params = account()
    first, second = client.create(params), client.create(params)
    assert second["id"] == first["id"]
    assert len(api.subscriptions) == 1


@pytest.mark.parametrize(
    "other",
    [
        account(callback=CALLBACK + "/elsewhere"),
        account(OTHER_ACCOUNT),
        account(filter_version=1),
        account(operation_types=[0, 1]),
    ],
)
def test_what_differs_in_a_filter_is_another_subscription(other: dict[str, Any]) -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    assert client.create(other)["id"] != client.create(account())["id"]
    assert len(api.subscriptions) == 2


def test_over_the_total_limit_nothing_more_is_made_and_the_refusal_is_a_400() -> None:
    api = FakeNotifierApi(PUBLIC_KEY, max_active=2)
    client, _ = connected(api)
    client.create(account())
    client.create(account(OTHER_ACCOUNT))
    with refused(400, LIMIT):
        client.create(asset())
    assert len(api.subscriptions) == 2
    assert client.status()["subscriptions"] == 2


def test_over_the_per_user_limit_the_refusal_is_the_same_400_and_the_count_is_the_users_own() -> None:
    api = tokens_api(max_per_user=1)
    user, other = of(api, USER_TOKEN), of(api, OTHER_TOKEN)
    user.create(account())
    with refused(400, LIMIT):
        user.create(account(OTHER_ACCOUNT))
    assert remote_id(other.create(account(OTHER_ACCOUNT)))  # another user has a count of its own


def test_the_per_user_limit_holds_with_authorization_disabled_too() -> None:
    # observer.js checks it wherever `authorization` is true, and "disabled" is a true string.
    # Every subscription made that way is the anonymous admin's, and they all count.
    api = FakeNotifierApi(PUBLIC_KEY, max_per_user=1)
    client, _ = connected(api)
    client.create(account())
    with refused(400, LIMIT):
        client.create(account(OTHER_ACCOUNT))


def test_a_per_user_limit_of_zero_is_no_limit() -> None:
    api = FakeNotifierApi(PUBLIC_KEY, max_per_user=0)
    client, _ = connected(api)
    for _ in range(3):
        client.create(account(Keypair.random().public_key))
    assert len(api.subscriptions) == 3


def test_a_batch_is_counted_against_the_limit_as_a_whole() -> None:
    api = FakeNotifierApi(PUBLIC_KEY, max_active=2)
    client, _ = connected(api)
    with refused(400, LIMIT):
        client.create([account(), account(), account()], batch=True)  # three in the array, one to make
    assert api.subscriptions == []


def test_an_empty_batch_is_refused() -> None:
    client, _ = connected(FakeNotifierApi(PUBLIC_KEY))
    with refused(400, "Bad request. Expected a non-empty array of subscription params."):
        client.create([], batch=True)


# What the Notifier says of a parameter it refuses, for the documentation to quote.
NO_URL = "Bad request. Invalid parameter: reaction_url. Reaction URL is required."
NO_FILTER = "Bad request. No operation filter params were provided."
NOT_AN_ADDRESS = "Bad request. Invalid parameter: account. Invalid Stellar account address."
NOT_A_CONTRACT = "Bad request. Invalid parameter: contract. Invalid Soroban contract address."
LONG_MEMO = "Bad request. Invalid parameter: memo. Invalid memo format. String is too long."
BAD_TYPES = "Bad request. Invalid parameter: operation_types. " + OPERATION_TYPES_HELP
BAD_ASSET = "Bad request. Invalid parameter: asset. " + ASSET_HELP
BAD_VERSION = "Bad request. Invalid parameter: filter_version. Unsupported filter version. Valid values: 1..2."
BAD_DATE = "Bad request. Invalid parameter: expires. Invalid expiration date format."
PAST_DATE = "Bad request. Invalid parameter: expires. Expiration date cannot be less than current date."
# The same address with another last character: shaped like one, and the checksum does not hold.
WRONG_CHECKSUM = ACCOUNT[:-1] + ("B" if ACCOUNT[-1] != "B" else "A")

REFUSED_PARAMETERS = [
    ({"account": ACCOUNT}, NO_URL),
    ({"reaction_url": "https://", "account": ACCOUNT}, NO_URL),
    ({"reaction_url": CALLBACK}, NO_FILTER),
    ({"reaction_url": CALLBACK, "asset_issuer": ISSUER}, NO_FILTER),  # an issuer alone is no filter
    ({"reaction_url": CALLBACK, "operation_types": []}, NO_FILTER),
    ({"reaction_url": CALLBACK, "account": "not an address"}, NOT_AN_ADDRESS),
    ({"reaction_url": CALLBACK, "account": WRONG_CHECKSUM}, NOT_AN_ADDRESS),
    ({"reaction_url": CALLBACK, "contract": "not a contract"}, NOT_A_CONTRACT),
    ({"reaction_url": CALLBACK, "memo": "m" * 65}, LONG_MEMO),
    ({"reaction_url": CALLBACK, "operation_types": [27]}, BAD_TYPES),
    ({"reaction_url": CALLBACK, "operation_types": "0,abc"}, BAD_TYPES),
    ({"reaction_url": CALLBACK, "asset_code": "EURMTL"}, BAD_ASSET),
    ({"reaction_url": CALLBACK, "asset_code": "EURMTL", "asset_issuer": "not an issuer"}, BAD_ASSET),
    ({"reaction_url": CALLBACK, "asset_code": "TOOLONGFORANASSET", "asset_issuer": ISSUER}, BAD_ASSET),
    ({"reaction_url": CALLBACK, "account": ACCOUNT, "expires": "yesterday"}, BAD_DATE),
    ({"reaction_url": CALLBACK, "account": ACCOUNT, "expires": 1000}, PAST_DATE),
]
REFUSED_PARAMETERS += [
    ({"reaction_url": CALLBACK, "account": ACCOUNT, "filter_version": version}, BAD_VERSION)
    for version in (3, 0, -1, "abc", "1.5")
]


@pytest.mark.parametrize("params, detail", REFUSED_PARAMETERS)
def test_what_the_notifier_refuses_comes_back_with_its_reason(params: dict[str, Any], detail: str) -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    with refused(400, detail):
        client.create(params)
    assert api.subscriptions == []


def test_a_create_whose_answer_is_lost_may_have_been_made() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    params = account()
    api.create_but_hang(1)
    with pytest.raises(NotifierUnavailable):
        client.create(params)
    assert len(api.subscriptions) == 1  # made all the same; only the answer was lost
    again = client.create(params)
    assert again["id"] == api.subscriptions[0]["id"]
    assert len(api.subscriptions) == 1
    assert [one for one in client.list_subscriptions() if same_subscription(one, params)] == [again]


def test_an_asset_subscription_whose_answer_was_lost_is_duplicated_by_a_second_post() -> None:
    # The Notifier does not recognise an asset in the request it was sent: the stored subscription has an asset_type
    # the request has not (logic/observer.js, subscriptionHash). The retry that is safe for an account is not safe
    # for an asset; a listing read with same_subscription is what tells the two apart.
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    params = asset()
    api.create_but_hang(1)
    with pytest.raises(NotifierUnavailable):
        client.create(params)
    second = client.create(params)
    assert len(api.subscriptions) == 2
    assert {one["id"] for one in api.subscriptions} == {api.subscriptions[0]["id"], second["id"]}
    found = [one for one in client.list_subscriptions() if same_subscription(one, params)]
    assert {one["id"] for one in found} == {one["id"] for one in api.subscriptions}


def test_an_asset_is_made_anew_by_every_post_unless_the_request_carries_its_type() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    assert client.create(asset("EURMTL"))["id"] != client.create(asset("EURMTL"))["id"]
    # With the type in the request, it is the identity of what is stored, and the first is given back.
    typed = {**asset("EUR"), "asset_type": 1}
    assert client.create(typed)["id"] == client.create(typed)["id"]
    # Native is stored without a code and asked for with one: nothing makes the two meet.
    first, second = (client.create({**native(), "asset_type": 0}) for _ in range(2))
    assert first["id"] != second["id"]
    assert len(api.subscriptions) == 5


def test_the_id_of_a_subscription_is_read_from_whichever_field_holds_it() -> None:
    assert remote_id({"id": "a", "_id": "b"}) == "a"
    assert remote_id({"_id": 7}) == "7"
    assert remote_id({"reaction_url": CALLBACK}) is None
    assert remote_id({"id": ""}) is None


def test_a_refusal_raised_by_a_caller_reads_like_one_from_the_notifier() -> None:
    # app/subscriptions/reconcile.py raises one for a subscription that is not what was registered here.
    message = "subscription 0123 is not in the Notifier's list"
    mine = NotifierRejected(message, 404, "not found")
    assert (str(mine), mine.status, mine.detail) == (message, 404, "not found")
    assert isinstance(mine, NotifierError)


def test_a_listed_subscription_is_told_by_its_url_its_filters_and_the_version_of_its_rules() -> None:
    params = {"reaction_url": CALLBACK, "account": ACCOUNT, "memo": "hello", "operation_types": [2, 0, 1]}
    params["filter_version"] = 1
    mine = listed(reaction_url=CALLBACK, account=ACCOUNT, memo="hello", operation_types=[1, 2, 0], filter_version=1)
    assert same_subscription(mine, params)
    changed = (
        {"reaction_url": CALLBACK + "/elsewhere"},
        {"account": OTHER_ACCOUNT},
        {"memo": "bye"},
        {"operation_types": [0, 1]},
    )
    for other in changed:
        assert not same_subscription(mine, {**params, **other})
    assert same_subscription(mine, {**params, "operation_types": "2, 1, 0"})  # a string with commas, in any order
    # The owner is not compared.
    assert same_subscription({**mine, "pubkey": None}, params)
    assert same_subscription({**mine, "pubkey": "somebody else"}, params)


def test_the_first_rules_and_the_current_ones_are_different_subscriptions() -> None:
    before = listed(reaction_url=CALLBACK, account=ACCOUNT)  # no filter_version: made before v0.12.0
    current = listed(reaction_url=CALLBACK, account=ACCOUNT, filter_version=2)
    assert not same_subscription(before, account())
    assert same_subscription(before, account(filter_version=1))
    assert same_subscription(current, account())
    assert not same_subscription(current, account(filter_version=1))


def test_an_asset_is_compared_by_its_type_its_code_and_its_issuer() -> None:
    eurmtl = listed(reaction_url=CALLBACK, filter_version=2, asset_type=2, asset_code="EURMTL", asset_issuer=ISSUER)
    assert same_subscription(eurmtl, asset())
    assert not same_subscription(eurmtl, asset("EUR"))
    assert not same_subscription(eurmtl, account())
    assert not same_subscription(eurmtl, subscription_params("asset", f"EURMTL:{OTHER_ACCOUNT}", CALLBACK))
    four = listed(reaction_url=CALLBACK, filter_version=2, asset_type=1, asset_code="EUR", asset_issuer=ISSUER)
    assert same_subscription(four, asset("EUR"))
    # Native is the type alone.
    assert same_subscription(listed(reaction_url=CALLBACK, filter_version=2, asset_type=0), native())
    assert not same_subscription(listed(reaction_url=CALLBACK, filter_version=2), native())
    assert not same_subscription(listed(reaction_url=CALLBACK, filter_version=2, account=ACCOUNT), native())


def test_what_the_notifier_lists_is_found_by_the_same_reading() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    made = [client.create(asset()), client.create(asset()), client.create(account())]
    found = [one for one in client.list_subscriptions() if same_subscription(one, asset())]
    assert [one["id"] for one in found] == [made[0]["id"], made[1]["id"]]


def test_a_subscription_is_removed_once_and_after_that_it_is_not_there() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    made = client.create(account())
    assert client.delete(made["id"]) is True
    assert api.subscriptions == []
    assert client.delete(made["id"]) is False
    assert api.requests[-1][:2] == ("DELETE", f"/api/subscription/{made['id']}")
    assert client.create(account())["id"] != made["id"]  # the same rule can be subscribed to again


def test_the_removal_of_somebody_elses_subscription_is_refused() -> None:
    api = tokens_api()
    user, other, admin = of(api, USER_TOKEN), of(api, OTHER_TOKEN), of(api, ADMIN_TOKEN)
    theirs = other.create(account())
    with refused(403, NOT_MINE):
        user.delete(theirs["id"])
    assert other.get(theirs["id"]) == theirs  # still there
    admins_own = admin.create(account(OTHER_ACCOUNT))
    with refused(403, NOT_MINE):
        user.delete(admins_own["id"])  # the admin's own are the admin's to remove
    assert admin.delete(theirs["id"]) is True
    assert admin.delete(admins_own["id"]) is True
    assert api.subscriptions == []


def test_no_answer_and_a_server_out_of_order_are_both_unavailable() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    api.hang(1)
    with pytest.raises(NotifierUnavailable) as caught:
        client.status()
    assert isinstance(caught.value.__cause__, httpx.ReadTimeout)
    for status in (500, 502, 503):
        api.fail(1, status)
        with pytest.raises(NotifierUnavailable, match=str(status)):
            client.status()
    assert api.requests == [("GET", "/api/status", None)] * 4  # asked all the same


def test_whatever_is_asked_a_request_that_is_not_answered_is_unavailable() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    made = client.create(account())
    api.hang(len(asking_of(client)))
    for asking in asking_of(client):
        with pytest.raises(NotifierUnavailable):
            asking()
    assert len(api.subscriptions) == 1 and client.get(made["id"]) == made  # over, and answering again


def asking_of(client: NotifierClient) -> tuple[Callable[[], Any], ...]:
    return (
        client.status,
        client.list_subscriptions,
        lambda: client.create(account(OTHER_ACCOUNT)),
        lambda: client.get(UNKNOWN_ID),
        lambda: client.delete(UNKNOWN_ID),
    )


CALLS: dict[str, Callable[[NotifierClient], Any]] = {
    "status": lambda client: client.status(),
    "list": lambda client: client.list_subscriptions(),
    "create": lambda client: client.create(account()),
    "batch": lambda client: client.create([account(), asset()], batch=True),
}


@pytest.mark.parametrize(
    "status, body, call",
    [
        (200, {"json": []}, "status"),  # not an object
        (200, {"text": "<html>a gateway stands here</html>"}, "status"),  # not JSON
        (200, {"json": {}}, "list"),  # not a list
        (200, {"json": [{"reaction_url": CALLBACK}]}, "list"),  # a subscription without an id
        (200, {"json": [None]}, "list"),
        (200, {"json": []}, "create"),  # one was asked for
        (200, {"json": {"reaction_url": CALLBACK}}, "create"),  # and it came without an id
        (200, {"json": [{"id": UNKNOWN_ID}]}, "batch"),  # two were asked for
        (302, {}, "list"),  # a redirect: nothing the API documents
    ],
)
def test_an_answer_that_is_not_what_the_api_documents_is_malformed(
    status: int, body: dict[str, Any], call: str
) -> None:
    with pytest.raises(NotifierMalformed):
        CALLS[call](NotifierClient(ORIGIN, transport=answering(status, **body)))


def raised(asking: Callable[[], Any]) -> BaseException:
    with pytest.raises(NotifierError) as caught:
        asking()
    return caught.value


def test_the_token_is_in_nothing_that_can_be_shown() -> None:
    api = tokens_api()
    client, _ = connected(api, token=UNTAKEN_TOKEN)  # not a token the Notifier takes
    assert UNTAKEN_TOKEN not in repr(client)
    assert UNTAKEN_TOKEN not in str(client)
    # Refused, not answered, and answered by a server out of order: every error this client raises.
    errors = [raised(asking) for asking in asking_of(client)]
    for error in errors:
        assert isinstance(error, NotifierRejected) and (error.status, error.detail) == (401, "Unauthorized")
    api.hang(len(asking_of(client)))
    errors += [raised(asking) for asking in asking_of(client)]
    api.fail(len(asking_of(client)), 503)
    errors += [raised(asking) for asking in asking_of(client)]
    # A server that repeats the token back — in a refusal and in a failure.
    body = {"error": f"the token {UNTAKEN_TOKEN} is not taken", "details": [UNTAKEN_TOKEN]}
    repeating = answering(400, json=body)
    errors.append(raised(lambda: NotifierClient(ORIGIN, token=UNTAKEN_TOKEN, transport=repeating).create(account())))
    failing = answering(503, json={"error": f"down, said {UNTAKEN_TOKEN}"})
    errors.append(raised(lambda: NotifierClient(ORIGIN, token=UNTAKEN_TOKEN, transport=failing).status()))
    for error in errors:
        shown = repr(error) + str(error) + "".join(traceback.format_exception(error))
        assert UNTAKEN_TOKEN not in shown
    refused_back = errors[-2]
    assert isinstance(refused_back, NotifierRejected)
    assert refused_back.detail == 'the token [hidden] is not taken (["[hidden]"])'
    assert "[hidden]" in str(errors[-1])


def test_an_address_with_a_user_name_and_a_password_is_not_shown_and_the_token_still_arrives() -> None:
    api = FakeNotifierApi(PUBLIC_KEY, authorization="token", user_tokens=[USER_TOKEN])
    seen = Seen(api.transport)
    client = NotifierClient("https://operator:a-password@notifier.test/mtl", token=USER_TOKEN, transport=seen)
    assert repr(client) == "NotifierClient(base_url='https://notifier.test/mtl')"
    assert "a-password" not in str(client)
    assert client.create(account())["pubkey"] == USER_TOKEN
    (request,) = seen.requests
    assert request.url.path == "/mtl/api/subscription"
    # httpx sends the user name and the password of an address as basic authentication, in the token's own header.
    assert request.headers["authorization"].startswith("Basic ")
    assert request.headers["x-access-token"] == USER_TOKEN


def test_an_id_the_notifier_does_not_have_is_never_answered() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    with pytest.raises(NotifierUnavailable) as caught:
        client.get(UNKNOWN_ID)
    assert isinstance(caught.value.__cause__, httpx.ReadTimeout)
    assert api.requests == [("GET", f"/api/subscription/{UNKNOWN_ID}", None)]


def test_an_unknown_id_is_a_404_with_nothing_said_where_the_fake_is_told_to_answer() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    api.unknown_id_hangs = False
    client, _ = connected(api)
    with refused(404, "Not Found"):
        client.get(UNKNOWN_ID)


def test_one_subscription_is_shown_to_its_owner_only() -> None:
    api = tokens_api()
    user, other, admin = of(api, USER_TOKEN), of(api, OTHER_TOKEN), of(api, ADMIN_TOKEN)
    mine = user.create(account())
    assert user.get(mine["id"]) == mine
    for outsider in (other, admin):  # the admin lists everything, and asks for a user's subscription in vain
        with refused(404, f"Subscription {mine['id']} not found."):
            outsider.get(mine["id"])
    own = admin.create(account(OTHER_ACCOUNT))
    assert admin.get(own["id"]) == own


def test_every_request_is_sent_with_the_timeout() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, seen = connected(api, timeout_seconds=3.5)
    made = client.create(account())
    client.status()
    client.list_subscriptions()
    client.get(made["id"])
    client.delete(made["id"])
    timeout = {"connect": 3.5, "read": 3.5, "write": 3.5, "pool": 3.5}
    assert len(seen.requests) == 5
    assert all(request.extensions["timeout"] == timeout for request in seen.requests)
    for none in (0, -1, None):
        with pytest.raises(ValueError):
            NotifierClient(ORIGIN, timeout_seconds=none, transport=api.transport)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "base", ["https://notifier.test/mtl", "https://notifier.test/mtl/", "https://notifier.test/mtl/notify"]
)
def test_a_base_url_with_a_path_leads_every_request_under_that_path(base: str) -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    with NotifierClient(base, transport=api.transport) as client:
        made = client.create(account())
        assert client.list_subscriptions() == [made]
        assert client.get(made["id"]) == made
        assert client.delete(made["id"]) is True
    root = httpx.URL(base).path.rstrip("/")  # what the fake saw does not start with the scheme and the host
    assert [request[:2] for request in api.requests] == [
        ("POST", f"{root}/api/subscription"),
        ("GET", f"{root}/api/subscription"),
        ("GET", f"{root}/api/subscription/{made['id']}"),
        ("DELETE", f"{root}/api/subscription/{made['id']}"),
    ]


@pytest.mark.parametrize("base", ["notifier.test", "notifier.test:3000", "ftp://notifier.test"])
def test_an_address_that_is_no_http_server_is_refused_here(base: str) -> None:
    with pytest.raises(ValueError):
        NotifierClient(base)


def test_the_fake_refuses_a_put_with_the_documented_body_and_asks_for_no_token() -> None:
    api = tokens_api()
    for path in ("/api/subscription", f"/api/subscription/{UNKNOWN_ID}"):
        response = ask(api, "PUT", path, json=account())
        assert (response.status_code, response.json()) == (405, {"error": CANNOT_BE_MODIFIED})


def test_the_fake_refuses_with_no_body_where_the_error_handler_of_the_notifier_does() -> None:
    api = tokens_api()
    made = of(api, USER_TOKEN).create(account())
    mine, foreign = {"Authorization": USER_TOKEN}, {"Authorization": OTHER_TOKEN}
    no_token = ask(api, "GET", "/api/subscription")
    assert (no_token.status_code, no_token.content) == (401, b"")
    # The token is judged before any route, the one that needs none included.
    not_taken = ask(api, "GET", "/api/status", headers={"Authorization": "not a taken token"})
    assert (not_taken.status_code, not_taken.content) == (401, b"")
    gone = ask(api, "DELETE", f"/api/subscription/{UNKNOWN_ID}", headers=mine)
    assert (gone.status_code, gone.content) == (404, b"")
    not_mine = ask(api, "DELETE", f"/api/subscription/{made['id']}", headers=foreign)
    assert (not_mine.status_code, not_mine.content) == (403, b"")
    # An id MongoDB cannot read: an error with no code.
    unreadable = ask(api, "DELETE", "/api/subscription/not-an-id", headers=mine)
    assert (unreadable.status_code, unreadable.content) == (500, b"")
    assert len(api.subscriptions) == 1


def test_the_fake_shows_a_subscription_as_mongodb_does() -> None:
    made = of(FakeNotifierApi(PUBLIC_KEY)).create(asset())
    assert made["_id"] == made["id"] and re.fullmatch("[0-9a-f]{24}", made["id"])
    assert (made["status"], made["__v"]) == (0, 0)
    for name in ("created", "updated"):
        assert datetime.fromisoformat(made[name].replace("Z", "+00:00")).tzinfo is not None
    assert set(made) == {
        "_id", "id", "pubkey", "status", "asset_type", "asset_code", "asset_issuer", "filter_version", "reaction_url",
        "delivery_failures", "lost_notifications", "sent", "created", "updated", "__v",
    }


def test_the_fake_keeps_the_filters_as_the_notifier_keeps_them() -> None:
    client = of(FakeNotifierApi(PUBLIC_KEY))
    for code, kind in (("EUR", 1), ("EURMTL", 2), ("XLM", 1)):  # XLM with an issuer is a credit asset with that code
        made = client.create(subscription_params("asset", f"{code}:{ISSUER}", CALLBACK))
        assert (made["asset_type"], made["asset_code"], made["asset_issuer"]) == (kind, code, ISSUER)
    made = client.create(native())
    assert (made["asset_type"], "asset_code" in made, "asset_issuer" in made) == (0, False, False)
    # Operation types as they were sent, a memo as a string, a date as JavaScript writes it.
    sent = {"reaction_url": CALLBACK, "operation_types": "2,0,1", "memo": 42, "expires": "2100-01-01T00:00:00Z"}
    made = client.create(sent)
    assert (made["operation_types"], made["memo"]) == ([2, 0, 1], "42")
    assert made["expires"] == "2100-01-01T00:00:00.000Z"
    # parseInt() reads the number a value starts with, and "1" is the number 1.
    made = client.create({"reaction_url": CALLBACK, "operation_types": "12abc", "account": ACCOUNT})
    assert made["operation_types"] == [12]
    assert client.create(account(OTHER_ACCOUNT, filter_version="1"))["filter_version"] == 1


def test_a_batch_gives_back_what_exists_and_resolves_its_own_repeats_to_the_first() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    existing = client.create(account())
    made = client.create([account(), account(OTHER_ACCOUNT), account(), account(OTHER_ACCOUNT)], batch=True)
    assert [one["id"] for one in made] == [existing["id"], made[1]["id"], existing["id"], made[1]["id"]]
    assert len(api.subscriptions) == 2


def test_one_wrong_parameter_loses_the_whole_batch() -> None:
    api = FakeNotifierApi(PUBLIC_KEY)
    client, _ = connected(api)
    with refused(400, "Bad request. Invalid parameter: reaction_url. Reaction URL is required."):
        client.create([account(), {"account": ACCOUNT}], batch=True)
    assert api.subscriptions == []
