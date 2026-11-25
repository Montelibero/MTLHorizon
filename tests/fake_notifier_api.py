"""A fake of what the Operations Notifier answers to the clients of its API,
made the way its code answers: the routes of ``api/observer-routes.js``, the
authorization of ``api/authorization-handler.js``, the rules of
``logic/observer.js`` and ``logic/storage.js`` and the errors of
``util/errors.js``, all at revision ``e4039fb`` (v0.12.0). Subscriptions are
kept in memory, in the shape MongoDB gives them back.

Where that code does not do what one would expect of it, the fake does what
the code does:

* Every refusal of a POST — a wrong parameter, an empty array, a limit
  reached — is answered with 400. The route answers an error with
  ``e.status || 400``, and the Notifier's errors carry a ``code``, never a
  ``status``. The refusal that is a 403 by its code, the limit, is a 400 on
  the wire like all the rest.
* The per-user limit is checked whenever ``authorization`` is true in
  JavaScript, and ``"disabled"`` is a true string. With authorization
  disabled every subscription belongs to the anonymous admin, and that admin
  has the per-user limit: 100 in the shipped configuration.
* A repeated POST returns the existing subscription only for filters the
  Notifier recognises in the request as it was sent: account, contract, memo,
  operation types. An asset is not recognised — the identity is taken before
  validation, from a request that has no ``asset_type``, and for native XLM
  no code — so every POST of an asset subscription makes a new one.
* ``GET /api/subscription/<id>`` is never answered for an id the Notifier
  does not have: the route has no catch. The fake raises ``httpx.ReadTimeout``
  there, which is what a client sees once its timeout is over.

Not made here: ``/health``, ``/api/delivery-log``, the user and the admin
routes, and the checking of ed25519 signatures — the default mode of
authorization — so nobody is known there without the admin token. Dates are
read from numbers and from ISO 8601 strings; a time without a zone is UTC.
"""

from __future__ import annotations

import copy
import json
import math
import re
import uuid
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from stellar_sdk import StrKey

FILTER_VERSION = 2
# Every operation type of the XDR definitions the Notifier is built with.
OPERATION_TYPES = range(27)
DEDUP_FIELDS = ("pubkey", "reaction_url", "account", "contract", "memo", "asset_type", "asset_code", "asset_issuer")
# The filter fields of a stored subscription, in the order of the schema.
FILTER_FIELDS = ("account", "asset_type", "asset_code", "asset_issuer", "memo", "contract", "operation_types")

MAX_ACTIVE = "Max active subscriptions exceeded."
CANNOT_BE_MODIFIED = "Subscription cannot be modified"
ASSET_HELP = (
    "Invalid asset format. Check "
    "https://www.stellar.org/developers/guides/concepts/assets.html#anchors-issuing-assets."
)
OPERATION_TYPES_HELP = (
    "Invalid operation type specified. Parameter operation_types should be an array of integers "
    f"matching existing operation types (valid values: {', '.join(str(value) for value in OPERATION_TYPES)})."
)

ID = re.compile("[0-9a-f]{24}")
# A reaction URL as the Notifier checks it. The characters of JavaScript's class, one by one: no backslash is needed.
REACTION_URL = re.compile("(?:https?://)[]A-Za-z0-9_.~:/?#[@!$&'()*+,;=-]+")
ASSET_CODE = re.compile("[a-zA-Z0-9]{1,12}")
EPOCH_MILLISECONDS = re.compile("[0-9]{1,13}")
WHOLE_NUMBER = re.compile("[+-]?[0-9]+")
NUMBER = re.compile("[+-]?(Infinity|[0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?")

# A property that is not there at all: JavaScript's undefined, as against null.
_ABSENT = object()
# The admin that the admin token names: it has no key of its own.
_ADMIN: dict[str, Any] = {"pubkey": None, "roles": ["admin"]}


class _Refused(Exception):
    """One of the Notifier's errors (``util/errors.js``): a message and a
    code. A code of zero is an error nobody expected, which has none."""

    def __init__(self, message: str, code: int = 0) -> None:
        super().__init__(message)
        self.message = message
        self.code = code


def _bad_request(details: Any = None) -> _Refused:
    return _Refused(_with_details("Bad request.", details), 400)


def _forbidden(details: Any = None) -> _Refused:
    return _Refused(_with_details("Forbidden.", details), 403)


def _validation(name: str, details: Any = None) -> _Refused:
    return _Refused(_with_details(f"Bad request. Invalid parameter: {name}.", details), 400)


def _with_details(message: str, details: Any) -> str:
    return message if not _truthy(details) else f"{message} {_string(details)}"


def _truthy(value: Any) -> bool:
    """Whether JavaScript holds a value to be true: an empty array or object
    is, a zero is not."""
    return value is not None and value is not _ABSENT and value is not False and value != 0 and value != ""


def _string(value: Any) -> str:
    """A value as JavaScript writes it into a string."""
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, list):
        return ",".join(_string(item) for item in value)
    if isinstance(value, dict):
        return "[object Object]"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _parse_int(value: Any) -> Any:
    """JavaScript's ``parseInt()``: the whole number a value starts with, or NaN."""
    found = WHOLE_NUMBER.match(_string(value).lstrip())
    return int(found[0]) if found else math.nan


def _number(value: Any) -> Any:
    """JavaScript's ``Number()``."""
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, list):
        return 0 if not value else _number(value[0]) if len(value) == 1 else math.nan
    if value is None or value is _ABSENT:
        return 0
    text = _string(value).strip()
    return float(text) if NUMBER.fullmatch(text) else math.nan


def _is_integer(value: Any) -> bool:
    """JavaScript's ``Number.isInteger()``."""
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and int(value) == value
    )


def _get(source: Any, name: str) -> Any:
    """A property of the parameters of a request, or ``_ABSENT``: whatever is
    not an object has none."""
    return source.get(name, _ABSENT) if isinstance(source, Mapping) else _ABSENT


def _is_account(value: Any) -> bool:
    try:
        # Anything may come: the answer for it is no.
        return StrKey.is_valid_ed25519_public_key(value)  # type: ignore[arg-type]
    except Exception:  # noqa: BLE001 - whatever is wrong with the value, it is not a key
        return False


def _is_contract(value: Any) -> bool:
    try:
        return StrKey.is_valid_contract(value)  # type: ignore[arg-type]
    except Exception:  # noqa: BLE001 - whatever is wrong with the value, it is not a contract
        return False


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(moment: datetime) -> str:
    """A moment as JavaScript's ``toISOString()`` writes it:
    ``2026-11-25T12:00:00.000Z``."""
    return moment.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _date(value: Any) -> datetime | None:
    """A value as ``new Date()`` reads it: milliseconds since the epoch, or a
    string in ISO 8601. None for a date that is not one."""
    try:
        if isinstance(value, (int, float)):
            return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=value)
        return datetime.fromisoformat(_string(value).replace("Z", "+00:00")).replace(tzinfo=UTC)
    except (ValueError, OverflowError, OSError):
        return None


def _elapsed(start: datetime, end: datetime) -> str:
    """``util/elapsed-time.js``: ``1d 2h 3m 4s``, with the seconds always there."""
    seconds = int(abs((end - start).total_seconds()))
    parts: list[str] = []
    for span, name in ((86400, "d"), (3600, "h"), (60, "m")):
        count, seconds = divmod(seconds, span)
        if count:
            parts.append(f"{count}{name}")
    parts.append(f"{seconds}s")
    return " ".join(parts)


def _asset_type(code: Any) -> int:
    """``normalizeAssetType()``: 0 for native, 1 for a code of four characters
    at most, 2 for a longer one."""
    if not _truthy(code):
        return 0
    return 2 if isinstance(code, (str, list)) and len(code) > 4 else 1


def _parse_asset(params: Any) -> dict[str, Any] | None:
    """``parseAsset()`` of ``util/asset-helper.js`` over the parameters of a
    request. None for what is not an asset."""
    asset: dict[str, Any] = {"asset_type": _ABSENT, "asset_code": _get(params, "asset_code")}
    asset["asset_issuer"] = _get(params, "asset_issuer")
    asset["asset_type"] = _asset_type(asset["asset_code"])
    if asset["asset_code"] == "XLM" and not _truthy(asset["asset_issuer"]):
        asset["asset_type"], asset["asset_code"] = 0, _ABSENT  # native: the code is not kept
    asset = {name: value for name, value in asset.items() if value is not _ABSENT}
    return asset if _is_valid_asset(asset) else None


def _is_valid_asset(asset: Mapping[str, Any]) -> bool:
    """``isValidAsset()``."""
    if asset["asset_type"] == 0 and not _truthy(asset.get("asset_code")) and not _truthy(asset.get("asset_issuer")):
        return True
    code = asset.get("asset_code")
    if not _is_account(asset.get("asset_issuer")):
        return False
    if not _truthy(code) or not ASSET_CODE.fullmatch(_string(code)):
        return False
    return _asset_type(code) == asset["asset_type"]


def _is_admin(user: Mapping[str, Any]) -> bool:
    return "admin" in (user.get("roles") or [])


def _posted(refused: _Refused) -> httpx.Response:
    """What ``processResponse()`` makes of an error: a 400 — it reads a status
    that the Notifier's errors do not have — and the message in a JSON body."""
    return httpx.Response(400, json={"error": refused.message})


def _passed(refused: _Refused) -> httpx.Response:
    """What the error handler makes of an error that reaches it through
    ``next()``: its code as the status, and no body at all."""
    return httpx.Response(refused.code or 500)


def _no_route(request: httpx.Request) -> httpx.Response:
    """What Express answers where it has no route: a 404."""
    return httpx.Response(404, text=f"Cannot {request.method} {request.url.path}")


def _json_body(request: httpx.Request) -> Any:
    """The body as ``bodyParser.json()`` leaves it: parsed where the content
    type says JSON, ``_ABSENT`` where there is nothing to read or it cannot be."""
    if "json" not in request.headers.get("content-type", "") or not request.content:
        return _ABSENT
    try:
        return json.loads(request.content)
    except ValueError:
        return _ABSENT


def _unreadable(request: httpx.Request) -> bool:
    """A body that says it is JSON and is not: body-parser's error for it
    carries no code, so it ends in a 500."""
    if _json_body(request) is not _ABSENT or not request.content:
        return False
    return "json" in request.headers.get("content-type", "")


def _milliseconds(value: Any) -> Any:
    """``expires`` as ``new Date()`` gets it: a number, or a string of digits,
    is read as milliseconds since the epoch."""
    if isinstance(value, (int, float)) or (isinstance(value, str) and EPOCH_MILLISECONDS.fullmatch(value)):
        return int(value)
    return value


class FakeNotifierApi:
    """The Notifier's API in memory, answering through ``httpx.MockTransport``
    (``.transport``). It is made as the Notifier is configured.

    ``authorization`` is its setting of that name: ``"disabled"``, ``"token"``,
    or anything else for the mode of signatures. ``admin_token`` is the token
    that names the admin in every mode. ``user_tokens`` is the allowlist of
    the ``token`` mode, which lets every token in where it is empty.
    ``max_active`` and ``max_per_user`` are its two limits; a per-user limit
    of zero is no limit.

    Three faults can be put in the way of the requests to come, and they are
    taken in this order:

    * ``fail(times, status)``: the next ``times`` requests are answered with
      that status and a problem body, whatever they ask.
    * ``hang(times)``: the next ``times`` requests are not answered at all —
      ``httpx.ReadTimeout``, as a client sees a dead server.
    * ``create_but_hang(times)``: the next ``times`` creates are made in
      memory all the same, and answered with a timeout — a subscription that
      exists and an answer that was lost. A create that is refused is
      answered as it always is: nothing was made, so nothing is lost.

    ``unknown_id_hangs`` says whether a GET of an id the Notifier does not
    have is left without an answer, as it is on the real service. Turned off,
    it is answered the way the error handler would: a 404 with no body."""

    def __init__(
        self,
        public_key: str,
        *,
        authorization: str = "disabled",
        admin_token: str = "",
        user_tokens: Sequence[str] = (),
        version: str = "0.12.0",
        max_active: int = 10000,
        max_per_user: int = 100,
    ) -> None:
        self.public_key = public_key
        self._authorization = authorization
        self._admin_token = admin_token
        self._user_tokens = list(user_tokens)
        self._version = version
        self._max_active = max_active
        self._max_per_user = max_per_user
        self._stored: list[dict[str, Any]] = []  # every subscription ever made here, whatever its status
        self._started = _now()
        # Every request as it arrived: the method, the path, and the body read as JSON, or None.
        self.requests: list[tuple[str, str, Any]] = []
        self._failures: list[int] = []
        self._hangs = 0
        self._lost_answers = 0
        self.unknown_id_hangs = True
        self._transport = httpx.MockTransport(self.handle)

    @property
    def transport(self) -> httpx.MockTransport:
        return self._transport

    @property
    def subscriptions(self) -> list[dict[str, Any]]:
        """The active subscriptions, as the API returns them."""
        return [copy.deepcopy(record) for record in self._active()]

    def switch_off(self, **where: Any) -> int:
        """What happens at the Notifier when it gives up on a subscription —
        ten deliveries in a row nobody answered — or when somebody deletes
        one by hand there: its status becomes 1 and it is gone from the list;
        of that nobody is told. ``where`` names the subscriptions (``id``, or
        ``account``, ``asset_code``…). Returns how many were switched off."""
        gone = 0
        for record in self._stored:
            if record.get("status", 0) == 0 and all(record.get(name) == value for name, value in where.items()):
                record["status"] = 1
                gone += 1
        return gone

    def fail(self, times: int, status: int) -> None:
        self._failures.extend([status] * times)

    def hang(self, times: int) -> None:
        self._hangs += times

    def create_but_hang(self, times: int) -> None:
        self._lost_answers += times

    def handle(self, request: httpx.Request) -> httpx.Response:
        """The handler of the transport: one request in, the Notifier's answer out."""
        self.requests.append((request.method, request.url.path, self._recorded(request)))
        if self._failures:
            status = self._failures.pop(0)
            return httpx.Response(status, json={"error": httpx.codes.get_reason_phrase(status), "status": status})
        if self._hangs:
            self._hangs -= 1
            raise httpx.ReadTimeout("the fake was told not to answer", request=request)
        if _unreadable(request):
            return httpx.Response(500)
        try:
            return self._route(request, self._user(request))
        except _Refused as refused:
            return _passed(refused)

    def _recorded(self, request: httpx.Request) -> Any:
        body = _json_body(request)
        return None if body is _ABSENT else body

    def _active(self) -> list[dict[str, Any]]:
        return [record for record in self._stored if record["status"] == 0]

    def _owned_by(self, user: Mapping[str, Any]) -> list[dict[str, Any]]:
        return [record for record in self._active() if record["pubkey"] == user["pubkey"]]

    def _user(self, request: httpx.Request) -> dict[str, Any] | None:
        """Who is asking, as ``userMiddleware`` works it out; None where nobody
        is. A token the ``token`` mode does not take is refused right here,
        whatever the route asked for."""
        if not self._authorization or self._authorization == "disabled":
            return _ADMIN
        token = self._token(request)
        if not token:
            return None
        if token == self._admin_token:
            return _ADMIN
        if self._authorization == "token":
            if self._user_tokens and token not in self._user_tokens:
                raise _Refused("Unauthorized.", 401)
            return {"pubkey": token, "roles": []}
        return None  # the mode of signatures: none is checked here, so nobody is known

    @staticmethod
    def _token(request: httpx.Request) -> str | None:
        """``getAuthToken()``: ``X-Access-Token`` first, then ``Authorization``,
        either with its scheme taken off."""
        token = request.headers.get("x-access-token")
        if token is None:
            token = request.headers.get("authorization")
        if token is None:
            return None
        for scheme in ("ed25519 ", "token ", "bearer "):
            if token.lower().startswith(scheme):
                return token[len(scheme) :]
        return token

    def _route(self, request: httpx.Request, user: dict[str, Any] | None) -> httpx.Response:
        """The routes of ``api/observer-routes.js``, told by the end of the
        path: a proxy in front of the Notifier may keep it under a prefix."""
        parts = [part for part in request.url.path.split("/") if part]
        method = request.method
        if parts[-2:] == ["api", "status"] and method == "GET":
            return httpx.Response(200, json=self._status())
        if parts[-2:] == ["api", "subscription"]:
            return self._collection(request, user)
        if len(parts) >= 3 and parts[-3:-1] == ["api", "subscription"]:
            return self._one(request, user, parts[-1])
        return _no_route(request)

    def _collection(self, request: httpx.Request, user: dict[str, Any] | None) -> httpx.Response:
        method = request.method
        if method == "PUT":
            return httpx.Response(405, json={"error": CANNOT_BE_MODIFIED})
        if user is None:
            raise _Refused("Unauthorized.", 401)
        if method == "GET":
            listed = self._active() if _is_admin(user) else self._owned_by(user)
            return httpx.Response(200, json=copy.deepcopy(listed))
        if method == "POST":
            body = _json_body(request)
            try:
                made = self._subscribe_batch(body, user) if isinstance(body, list) else self._subscribe(body, user)
            except _Refused as refused:
                return _posted(refused)
            if self._lost_answers:
                self._lost_answers -= 1
                raise httpx.ReadTimeout("the subscription was made, but the answer was lost", request=request)
            return httpx.Response(200, json=copy.deepcopy(made))
        return _no_route(request)

    def _one(self, request: httpx.Request, user: dict[str, Any] | None, subscription_id: str) -> httpx.Response:
        method = request.method
        if method == "PUT":
            return httpx.Response(405, json={"error": CANNOT_BE_MODIFIED})
        if user is None:
            raise _Refused("Unauthorized.", 401)
        if method == "GET":
            return self._shown_one(request, user, subscription_id)
        if method == "DELETE":
            return self._removed(subscription_id, user)
        return _no_route(request)

    def _shown_one(self, request: httpx.Request, user: Mapping[str, Any], subscription_id: str) -> httpx.Response:
        try:
            found = self._find_any(subscription_id)
        except _Refused:
            found = None  # a malformed id is refused like a missing one, and a missing one is never answered
        if found is None:
            if self.unknown_id_hangs:
                raise httpx.ReadTimeout("the Notifier has no such subscription, and it says nothing", request=request)
            # What the error handler would answer if the route passed the error on.
            return httpx.Response(404) if ID.fullmatch(subscription_id) else httpx.Response(500)
        if found["pubkey"] == user["pubkey"]:
            return httpx.Response(200, json=copy.deepcopy(found))
        return httpx.Response(404, json={"error": f"Subscription {subscription_id} not found."})

    def _removed(self, subscription_id: str, user: Mapping[str, Any]) -> httpx.Response:
        found = self._find_any(subscription_id)
        if found is None:
            raise _Refused("Not found.", 404)
        admin = _is_admin(user)
        if not _truthy(found["pubkey"]) and not admin:
            raise _forbidden()  # the admin's own: only the admin removes it
        if _truthy(found["pubkey"]) and not (admin or user["pubkey"] == found["pubkey"]):
            raise _forbidden()
        self._unsubscribe(subscription_id)
        return httpx.Response(200)

    def _status(self) -> dict[str, Any]:
        active = self._active()
        return {
            "version": self._version,
            "uptime": _elapsed(self._started, _now()),
            "publicKey": self.public_key,
            "observing": True,
            "subscriptions": len(active),
            "lostNotifications": sum(record["lost_notifications"] or 0 for record in active),
            "lastLedger": None,
            "ledgerLag": None,
            "lastIngestedTx": None,
            "stream": None,
            "notifier": {"inProgress": 0},
        }

    def _subscribe(self, params: Any, user: dict[str, Any]) -> dict[str, Any]:
        """``Observer.subscribe()``: the limits, then the identity, then the making."""
        if len(self._active()) + 1 > self._max_active:
            raise _forbidden(MAX_ACTIVE)
        if self._per_user_exceeded(user, 1):
            raise _forbidden(MAX_ACTIVE)
        key = self._hash(params, user)
        existing = self._find(self._active(), key)
        if existing is not None:
            return existing  # what exists is given back, and nothing is made
        return self._store(self._validate(params, user))

    def _subscribe_batch(self, batch: Any, user: dict[str, Any]) -> list[dict[str, Any]]:
        """``Observer.subscribeBatch()``: one answer for the whole array, in
        the order of the request. What exists is given back as it is, and a
        repeat inside the array as the one made first. Either all are made or
        none: the limits are counted for the whole array, the repetition in it
        included."""
        if not isinstance(batch, list) or not batch:
            raise _bad_request("Expected a non-empty array of subscription params.")
        if len(self._active()) + len(batch) > self._max_active:
            raise _forbidden(MAX_ACTIVE)
        if self._per_user_exceeded(user, len(batch)):
            raise _forbidden(MAX_ACTIVE)
        active = self._active()
        known = {self._hash(record) for record in active}
        results: list[Any] = []  # a subscription that exists, or the place of one to make
        to_create: list[Any] = []
        first_of: dict[Any, int] = {}
        for params in batch:
            key = self._hash(params, user)
            if key not in known:
                known.add(key)
                first_of[key] = len(to_create)
                results.append(len(to_create))
                to_create.append(params)
            else:
                existing = self._find(active, key)
                results.append(existing if existing is not None else first_of[key])
        # All are checked before any is made: one wrong parameter loses the whole array.
        checked = [self._validate(params, user) for params in to_create]
        made = [self._store(one) for one in checked]
        return [existing if isinstance(existing, dict) else made[existing] for existing in results]

    def _per_user_exceeded(self, user: Mapping[str, Any], more: int) -> bool:
        """The per-user limit as ``observer.js`` checks it: whenever
        ``authorization`` is true — ``"disabled"`` included — and counted
        with ``more`` subscriptions more than the user has."""
        limited = bool(self._authorization) and bool(self._max_per_user)
        return limited and len(self._owned_by(user)) + more > self._max_per_user

    def _find(self, records: Iterable[dict[str, Any]], key: tuple[Any, ...]) -> dict[str, Any] | None:
        return next((record for record in records if self._hash(record) == key), None)

    def _hash(self, source: Any, owner: Mapping[str, Any] | None = None) -> tuple[Any, ...]:
        """``subscriptionHash()`` of ``logic/observer.js``: what tells a
        subscription apart, over a stored one or over the parameters of a
        request as they were sent — which is where an asset is not recognised."""
        if source is None or source is _ABSENT:
            missing = "undefined" if source is _ABSENT else "null"
            raise _Refused(f"Cannot read properties of {missing} (reading 'id')")  # the TypeError behind a 400

        def get(name: str) -> Any:
            return _get(source, name)

        sent = get("id") is _ABSENT and get("status") is _ABSENT
        version = get("filter_version")
        if version is _ABSENT or version is None:
            # Parameters without it mean the current rules; a stored one, the first.
            version = FILTER_VERSION if sent else 1
        if sent:
            pubkey = (owner.get("pubkey") if owner else None) or ""
        else:
            pubkey = _or_empty(get("pubkey"))
        values = [pubkey if name == "pubkey" else _or_empty(get(name)) for name in DEDUP_FIELDS]
        return tuple(values + [self._types_of(get("operation_types")), version])

    @staticmethod
    def _types_of(types: Any) -> str:
        """``operation_types`` sorted and joined with commas, the way the identity writes them."""
        if not _truthy(types):
            return ""
        if not isinstance(types, (list, str)):
            raise _Refused(f"{_string(types)} is not iterable")
        items = list(types)  # a string is spread into its characters, as JavaScript spreads it
        return ",".join(sorted(_string(item) for item in items))

    def _find_any(self, subscription_id: str) -> dict[str, Any] | None:
        """A subscription by its id, whatever its status, as ``findById()``
        has it. A malformed id is refused before anything is looked for."""
        if not ID.fullmatch(subscription_id):
            raise _Refused(f'Cast to ObjectId failed for value "{subscription_id}" (type string) at path "_id"')
        return next((record for record in self._stored if record["id"] == subscription_id), None)

    def _unsubscribe(self, subscription_id: str) -> None:
        """``Observer.unsubscribe()``: found among the active ones, and set to deleted."""
        for record in self._active():
            if record["id"] == subscription_id:  # the intended non-strict comparison
                record["status"] = 1
                return
        raise _Refused("Not found.", 404)

    def _validate(self, params: Any, user: Mapping[str, Any]) -> dict[str, Any]:
        """``_validateSubscriptionParams()`` of ``logic/storage.js``, refusal
        for refusal, in the order of its checks."""
        if not _truthy(params):
            raise _bad_request("Subscription params were not provided.")
        reaction = _get(params, "reaction_url")
        if not _truthy(reaction) or not REACTION_URL.fullmatch(_string(reaction)):
            raise _validation("reaction_url", "Reaction URL is required.")
        subscription: dict[str, Any] = {
            "pubkey": user["pubkey"] if user else None,
            "reaction_url": reaction,
            "lost_notifications": 0,
        }
        filtered = False

        account = _get(params, "account")
        if _truthy(account):
            if not _is_account(account):
                raise _validation("account", "Invalid Stellar account address.")
            subscription["account"] = account
            filtered = True

        contract = _get(params, "contract")
        if _truthy(contract):
            if not _is_contract(contract):
                raise _validation("contract", "Invalid Soroban contract address.")
            subscription["contract"] = contract
            filtered = True

        memo = _get(params, "memo")
        if _truthy(memo):
            if isinstance(memo, str) and len(memo) > 64:
                raise _validation("memo", "Invalid memo format. String is too long.")
            subscription["memo"] = _string(memo)
            filtered = True

        types = _get(params, "operation_types")
        if _truthy(types):
            optypes = list(types) if isinstance(types, list) else _string(types).split(",")
            if optypes:
                optypes = [_parse_int(value) for value in optypes]
                if any(not _is_integer(value) or value not in OPERATION_TYPES for value in optypes):
                    raise _validation("operation_types", OPERATION_TYPES_HELP)
                subscription["operation_types"] = optypes
                filtered = True

        code = _get(params, "asset_code")
        if _truthy(code):
            asset = _parse_asset(params)
            if asset is None:
                raise _validation("asset", ASSET_HELP)
            subscription.update(asset)
            filtered = True

        if not filtered:
            raise _bad_request("No operation filter params were provided.")

        raw = _get(params, "filter_version")
        requested = FILTER_VERSION if raw is _ABSENT or raw is None or raw == "" else _number(raw)
        if isinstance(raw, bool) or not _is_integer(requested) or not 1 <= requested <= FILTER_VERSION:
            raise _validation("filter_version", f"Unsupported filter version. Valid values: 1..{FILTER_VERSION}.")
        subscription["filter_version"] = int(requested)

        expires = _get(params, "expires")
        if _truthy(expires):
            moment = _date(_milliseconds(expires))
            if moment is None:
                raise _validation("expires", "Invalid expiration date format.")
            if moment < _now():
                raise _validation("expires", "Expiration date cannot be less than current date.")
            subscription["expires"] = moment
        return subscription

    def _store(self, made: Mapping[str, Any]) -> dict[str, Any]:
        """A validated subscription as MongoDB gives it back: the defaults of
        the schema filled in, the id twice, and the time of its making."""
        identifier = uuid.uuid4().hex[:24]
        now = _iso(_now())
        record: dict[str, Any] = {"_id": identifier, "id": identifier, "pubkey": made["pubkey"], "status": 0}
        for name in FILTER_FIELDS:
            if name in made:
                record[name] = copy.deepcopy(made[name])
        record["filter_version"] = made["filter_version"]
        record["reaction_url"] = made["reaction_url"]
        record["delivery_failures"] = 0
        record["lost_notifications"] = made["lost_notifications"]
        record["sent"] = 0
        if "expires" in made:
            record["expires"] = _iso(made["expires"])
        record["created"] = now
        record["updated"] = now
        record["__v"] = 0
        self._stored.append(record)
        return record


def _or_empty(value: Any) -> Any:
    """A field of the identity as JavaScript's ``||`` makes it: what is false is an empty string."""
    return value if _truthy(value) else ""
