"""Managing this application's own subscriptions at Operations Notifier:
asking for them, creating them, telling an existing one from a new one, and
removing them.

Studied in the code of ``Montelibero/operations-notifier`` at revision
``e4039fb`` (v0.12.0). The routes are in ``api/observer-routes.js`` there, the
rules in ``logic/observer.js`` and ``logic/storage.js``, and what is written
below is what that code does — where it differs from the README, the code
wins.

* Authentication is a token in the ``Authorization`` header. Supported are
  the admin token (in every mode), a user token of the ``token`` mode, and the
  mode without authorization, where no token is needed. Not supported is the
  default mode of a per-request ed25519 signature over a nonce: this client
  signs nothing.
* ``GET /api/subscription/:id`` is never answered for an id the Notifier does
  not have: its handler has no catch for a missing subscription. So every
  request is sent with a timeout, and no answer is ``NotifierUnavailable``.
* A refusal is a 4xx. On POST every refusal — a wrong parameter, an empty
  array, a limit reached — is a 400 with a JSON body that says why in
  ``error``. A limit included: the Notifier's error for it is named forbidden,
  but it is sent as a 400 like the rest. The other refusals carry no body at
  all (401 when no token of the caller was accepted, 403 for the removal of a
  subscription that is not one's own, 404 for a removal of what is not there),
  so the reason phrase stands in for what was not said.
* A repeated POST of the same subscription returns the existing one — but only
  for filters the Notifier recognises in the request as it was sent: account,
  contract, memo, operation types. An asset filter is not recognised, because
  the stored subscription carries an ``asset_type`` the request never has. A
  second POST of an asset subscription therefore makes a duplicate. After a
  lost answer, look for the subscription with :func:`same_subscription`
  before asking again.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Any
from urllib.parse import quote

import httpx

from app import __version__
from app.notifier.format import G_ADDRESS
from app.sources import live

# The most complete matching rules the Notifier has (v0.12.0); 1 keeps the
# rules it had before.
FILTER_VERSION = 2

# An asset of this application, "CODE:ISSUER", with the code as the Notifier
# accepts it: one to twelve letters and digits.
CODE_WITH_ISSUER = re.compile(r"^([a-zA-Z0-9]{1,12}):(.+)$")

ASSET_FIELDS = ("asset_type", "asset_code", "asset_issuer")

USER_AGENT = f"mtl-horizon/{__version__}"


class NotifierError(Exception):
    """The Notifier could not be used as its API promises."""


class NotifierUnavailable(NotifierError):
    """No answer came, or none to stand on: the connection failed, the answer
    did not come in time, or the Notifier reported itself out of order.

    Nothing here says whether the request was carried out. A create may well
    have been stored and only its answer lost, so before asking again for
    something that must not exist twice, look for it."""


class NotifierRejected(NotifierError):
    """A 4xx: the Notifier understood the request and refused it.

    ``status`` is the code. ``detail`` is what the Notifier said, or the
    reason phrase where it said nothing. Made with the message first, as an
    error is, so that a caller can raise one of its own the same way."""

    def __init__(self, message: str, status: int, detail: str) -> None:
        super().__init__(message)
        self.status = status
        self.detail = detail


class NotifierMalformed(NotifierError):
    """An answer that is not what the API documents: not this Notifier, not
    this version of it, or something else answering in its place."""


def _refusal(status: int, detail: str, what: str) -> NotifierRejected:
    """The refusal of ``what``: the status with its reason, and what the
    Notifier said where it said more than the reason."""
    reason = httpx.codes.get_reason_phrase(status)
    said = f": {detail}" if detail and detail != reason else ""
    return NotifierRejected(f"the Notifier refused {what} with {status} {reason}{said}", status, detail)


def subscription_params(
    kind: str, key: str, callback_url: str, *, filter_version: int = FILTER_VERSION
) -> dict[str, Any]:
    """The body that creates a subscription for one watch rule.

    ``kind`` is ``"account"`` and ``key`` the account, or ``kind`` is
    ``"asset"`` and ``key`` is ``native`` or ``CODE:ISSUER`` as
    :func:`app.notifier.format.asset_id` writes them. The Notifier tracks
    native XLM by the code ``XLM`` and no issuer. ``filter_version`` is left
    for the Notifier to judge: it is the one that knows which it has.
    ``ValueError`` for anything else, here, rather than a round trip for it."""
    if kind == "account" and G_ADDRESS.match(key):
        return {"reaction_url": callback_url, "account": key, "filter_version": filter_version}
    if kind == "asset":
        if key == "native":
            return {"reaction_url": callback_url, "asset_code": "XLM", "filter_version": filter_version}
        found = CODE_WITH_ISSUER.match(key)
        if found and G_ADDRESS.match(found[2]):
            return {
                "reaction_url": callback_url,
                "asset_code": found[1],
                "asset_issuer": found[2],
                "filter_version": filter_version,
            }
    raise ValueError(f"not a {kind} to subscribe to: {key!r}")


def remote_id(subscription: Mapping[str, Any]) -> str | None:
    """The id of a subscription as the Notifier returns it, or None where
    there is none. A subscription stored in MongoDB comes with both ``id``
    and ``_id``; ``id`` is the one every version names."""
    for name in ("id", "_id"):
        value = subscription.get(name)
        if value is not None and value != "":
            return str(value)
    return None


def same_subscription(remote: Mapping[str, Any], params: Mapping[str, Any]) -> bool:
    """Whether a listed ``remote`` subscription is the one a POST of
    ``params`` is answered with: the same reaction URL, the same filters and
    the same filter version. A listed subscription without ``filter_version``
    was made with the first version of the rules; request parameters without
    it mean the current one.

    The owner is not compared. A caller that may see several owners — one
    that lists as the admin — must count the matches itself."""
    return _identity(remote, listed=True) == _identity(params, listed=False)


def _identity(subscription: Mapping[str, Any], *, listed: bool) -> tuple[Any, ...]:
    """What tells two subscriptions of one owner apart. A listed subscription
    carries its asset as stored, type included; request parameters carry only
    a code and an issuer, and the type follows from the length of the code."""
    asset = tuple(_field(subscription, name) for name in ASSET_FIELDS) if listed else _asset(subscription)
    return (
        _field(subscription, "reaction_url"),
        _field(subscription, "account"),
        _field(subscription, "contract"),
        _text(subscription.get("memo")),
        asset,
        _types(subscription.get("operation_types")),
        _version(subscription.get("filter_version"), 1 if listed else FILTER_VERSION),
    )


def _asset(params: Mapping[str, Any]) -> tuple[Any, ...]:
    code = _field(params, "asset_code")
    if not code:
        return ("", "", "")  # no asset filter at all
    issuer = _field(params, "asset_issuer")
    if code == "XLM" and not issuer:
        return (0, "", "")  # native, as the Notifier stores it: the type alone
    return (2 if len(str(code)) > 4 else 1, code, issuer)


def _field(subscription: Mapping[str, Any], name: str) -> Any:
    """A field as the Notifier tells it from an absent one: only missing and
    empty are the same. A zero is not — it is the type of the native asset."""
    value = subscription.get(name)
    return "" if value is None or value == "" else value


def _text(value: Any) -> str:
    return "" if not value else str(value)  # the Notifier stores a memo as a string, whatever was sent


def _types(value: Any) -> tuple[str, ...]:
    """``operation_types`` as a sorted tuple, from an array or from a string
    with commas, in any order."""
    if not value:
        return ()
    items = value.split(",") if isinstance(value, str) else list(value)
    numbers: list[str] = []
    for item in items:
        text = str(item).strip()
        try:
            text = str(int(text))
        except ValueError:
            pass  # not a number: it is compared as it is
        if text:
            numbers.append(text)
    return tuple(sorted(numbers))


def _version(value: Any, when_absent: Any) -> Any:
    if value is None or value == "":
        return when_absent
    try:
        return int(value)
    except (TypeError, ValueError):
        return value  # the Notifier refuses it with a reason of its own


def _is_subscription(value: Any) -> bool:
    return isinstance(value, dict) and remote_id(value) is not None


class NotifierClient:
    """The part of the Notifier's API that this application needs.

    ``base_url`` may carry a path, for a service that sits behind a proxy.
    ``token`` is a secret: it goes into the requests and into nothing else —
    not into the representation of this client, not into a message of any
    error raised here, not even when the other side repeats it back.
    ``transport`` is for tests; without it the requests leave the process."""

    def __init__(
        self,
        base_url: str,
        *,
        token: str = "",
        timeout_seconds: float = 20.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if timeout_seconds is None or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero: a request that is never answered has to end")
        try:
            base = httpx.URL(base_url)
        except httpx.InvalidURL:
            raise ValueError("base_url is not an address that can be read") from None
        if base.scheme not in ("http", "https") or not base.is_absolute_url:
            raise ValueError("base_url must be an absolute http or https address")
        self._token = token
        self._shown = str(base.copy_with(userinfo=b""))  # for repr(): the address without a user name or a password
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        if token:
            # httpx sends a user name and a password of the address as HTTP basic
            # authentication, which is the same header the token would go into.
            # The Notifier looks at X-Access-Token first of the two.
            named = bool(base.username or base.password)
            headers["X-Access-Token" if named else "Authorization"] = token
        self._http = httpx.Client(
            base_url=str(base),
            headers=headers,
            timeout=timeout_seconds,
            transport=transport if transport is not None else live.transport(),
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(base_url={self._shown!r})"

    def __enter__(self) -> NotifierClient:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    def status(self) -> dict[str, Any]:
        """What ``GET /api/status`` says: the version, the signing key, how
        many subscriptions are active, and so on. No token is needed."""
        return self._object(self._send("GET", "api/status"), "GET api/status")

    def list_subscriptions(self) -> list[dict[str, Any]]:
        """The active subscriptions: all of them for the admin token, the
        caller's own ones for a user token."""
        where = "GET api/subscription"
        listed = self._json(self._send("GET", "api/subscription"), where)
        if not isinstance(listed, list) or not all(_is_subscription(one) for one in listed):
            raise NotifierMalformed(f"{where} answered with what is not a list of subscriptions")
        return listed

    def get(self, remote_id: str) -> dict[str, Any]:
        """One subscription by its id, and only what the caller owns: for a
        user token its own subscriptions, for the admin token the ones that
        have no owner. Anything else is a 404, the same as if it did not
        exist.

        An id the Notifier does not have is not answered at all: on the real
        service this waits until ``timeout_seconds`` and then raises
        ``NotifierUnavailable``. Cheaper than a listing where one id is all
        that is wanted, at that price."""
        path = f"api/subscription/{quote(remote_id, safe='')}"
        return self._subscription(self._send("GET", path), f"GET {path}")

    def create(
        self, params: Mapping[str, Any] | Sequence[Mapping[str, Any]], *, batch: bool = False
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """Create a subscription from one mapping and get it back, or several
        from a sequence and get them back in the order sent. ``batch`` sends
        one mapping the way a batch goes — as an array at the root of the
        body — and then a list comes back too.

        What already exists is not made again where the Notifier can tell
        (see the module): the existing subscription is returned instead."""
        where = "POST api/subscription"
        one = isinstance(params, Mapping)
        if one:
            items: list[Mapping[str, Any]] = [params]
        elif isinstance(params, (str, bytes, bytearray)) or not isinstance(params, Sequence):
            raise TypeError("create takes one mapping or a sequence of mappings")
        else:
            items = list(params)
        if not all(isinstance(item, Mapping) for item in items):
            raise TypeError("create takes one mapping or a sequence of mappings")
        if one and not batch:
            return self._subscription(self._send("POST", "api/subscription", json=params), where)
        created = self._json(self._send("POST", "api/subscription", json=items), where)
        if not isinstance(created, list) or len(created) != len(items) or not all(_is_subscription(x) for x in created):
            raise NotifierMalformed(f"{where} answered with what is not a list of {len(items)} subscriptions")
        return created

    def delete(self, remote_id: str) -> bool:
        """Remove a subscription. True when it was removed; False when there
        was none with that id — removed before, perhaps by an earlier request
        whose answer was lost."""
        path = f"api/subscription/{quote(remote_id, safe='')}"
        try:
            self._send("DELETE", path)
        except NotifierRejected as refused:
            if refused.status != 404:
                raise
            return False
        return True

    def _send(self, method: str, path: str, **options: Any) -> httpx.Response:
        """One request with everything that can go wrong with it named. Only
        an answer that is a 2xx is returned."""
        try:
            response = self._http.request(method, path, **options)
        except httpx.TransportError as error:
            raise NotifierUnavailable(f"no answer to {method} {path}: {type(error).__name__}: {error}") from error
        status, where = response.status_code, f"{method} {path}"
        if status >= 500:
            said = self._said(response)
            raise NotifierUnavailable(f"the Notifier is out of order: {where} was answered with {status} {said}")
        if 400 <= status < 500:
            raise _refusal(status, self._said(response), where)
        if not 200 <= status < 300:
            raise NotifierMalformed(f"{where} was answered with {status}, which the API does not document")
        return response

    def _said(self, response: httpx.Response) -> str:
        """What came with a status: the ``error`` of a JSON body, with its
        ``details`` when there are any; the reason phrase when there is no
        such body. A body is the other side's text: the token is taken out of
        it, in case it is repeated there."""
        reason = httpx.codes.get_reason_phrase(response.status_code)
        try:
            body = response.json()
        except ValueError:
            return reason
        error = body.get("error") if isinstance(body, dict) else None
        if not isinstance(error, str) or not error:
            return reason
        details = body.get("details")
        if details is not None:
            shown = details if isinstance(details, str) else json.dumps(details, ensure_ascii=False)
            error = f"{error} ({shown})"
        return self._hidden(error)

    def _hidden(self, text: str) -> str:
        return text.replace(self._token, "[hidden]") if self._token else text

    def _json(self, response: httpx.Response, where: str) -> Any:
        try:
            return response.json()
        except ValueError:
            status = response.status_code
            raise NotifierMalformed(f"{where} was answered with {status} and a body that is not JSON") from None

    def _object(self, response: httpx.Response, where: str) -> dict[str, Any]:
        body = self._json(response, where)
        if not isinstance(body, dict):
            raise NotifierMalformed(f"{where} was answered with a body that is not a JSON object")
        return body

    def _subscription(self, response: httpx.Response, where: str) -> dict[str, Any]:
        body = self._object(response, where)
        if remote_id(body) is None:
            raise NotifierMalformed(f"{where} was answered with a subscription that has no id")
        return body
