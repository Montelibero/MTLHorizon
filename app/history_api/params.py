"""Reading and checking what a request asks for, as upstream Horizon takes it.

A parameter that cannot be understood is a 400 naming the parameter, never a
guess: a cursor that is not a number, a limit outside 1..200, an asset that is
not one. A parameter that is well formed but matches nothing (an account the
archive never met, an operation type it never saw) is not an error: it makes
an empty page.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from stellar_sdk import StrKey

from app.history_api.problems import bad_request

INT64_MAX = 2**63 - 1
DEFAULT_LIMIT = 10
MAX_LIMIT = 200

_ACCOUNT = re.compile(r"G[0-9A-Z]{55}")
_HASH = re.compile(r"[0-9a-f]{64}")
_DIGITS = re.compile(r"[0-9]{1,19}")
_CODE = re.compile(r"[A-Za-z0-9]{1,12}")
_TYPE_NAME = re.compile(r"[a-z_]{1,64}")
_DATA_NAME = re.compile(r"[ -~]{1,64}")  # printable ASCII, as a data name is on the network
_BASE64 = re.compile(r"[A-Za-z0-9+/]*={0,2}")

ASSET_TYPES = {"credit_alphanum4": (1, 4), "credit_alphanum12": (5, 12)}
TRADE_TYPES = ("orderbook", "liquidity_pool")
# What strconv.ParseBool accepts, which is what upstream reads a flag with.
TRUTHY = {"1", "t", "T", "TRUE", "true", "True"}
FALSY = {"0", "f", "F", "FALSE", "false", "False"}


def first_values(params: Iterable[tuple[str, str]]) -> dict[str, str]:
    """The first value of every parameter, as Go's ``URL.Query().Get`` reads one."""
    values: dict[str, str] = {}
    for name, value in params:
        values.setdefault(name, value)
    return values


def account_id(value: str) -> str:
    """A Stellar account address. Muxed (M...) addresses are not taken: the
    archive keeps the base address, as a participant always is one."""
    if not _ACCOUNT.fullmatch(value) or not StrKey.is_valid_ed25519_public_key(value):
        raise bad_request("account_id must be a Stellar account address (G...)", field="account_id")
    return value


def transaction_hash(value: str) -> str:
    if not _HASH.fullmatch(value):
        raise bad_request("transaction_hash must be 64 hexadecimal characters", field="transaction_hash")
    return value


def operation_id(value: str) -> int:
    if not _DIGITS.fullmatch(value):
        raise bad_request("the operation id must be a total order id: an unsigned 64-bit integer", field="id")
    number = int(value)
    if number > INT64_MAX:
        raise bad_request("the operation id is outside the range of a 64-bit integer", field="id")
    return number


def integer(
    params: Mapping[str, str], name: str, *, default: int | None, minimum: int, maximum: int = INT64_MAX
) -> int:
    """An integer parameter. Absent or empty: ``default``, which may be required."""
    raw = params.get(name, "")
    if raw == "":
        if default is None:
            raise bad_request(f"{name} is required", field=name)
        return default
    if not _DIGITS.fullmatch(raw):
        raise bad_request(f"{name} must be a non-negative integer", field=name)
    number = int(raw)
    if not minimum <= number <= maximum:
        raise bad_request(f"{name} must be between {minimum} and {maximum}", field=name)
    return number


def flag(params: Mapping[str, str], name: str) -> bool | None:
    """A boolean parameter as upstream reads one; None when it was not given."""
    raw = params.get(name, "")
    if raw == "":
        return None
    if raw in TRUTHY:
        return True
    if raw in FALSY:
        return False
    raise bad_request(f"{name} must be true or false", field=name)


def order(params: Mapping[str, str]) -> str:
    """``order``: ``asc`` (the default) or ``desc``."""
    value = params.get("order", "") or "asc"
    if value not in ("asc", "desc"):
        raise bad_request("order must be asc or desc", field="order")
    return value


def trade_type(params: Mapping[str, str]) -> str | None:
    raw = params.get("trade_type", "")
    if raw == "":
        return None
    if raw not in TRADE_TYPES:
        raise bad_request("trade_type must be orderbook or liquidity_pool", field="trade_type")
    return raw


def operation_filter(params: Mapping[str, str]) -> tuple[str, str | int] | None:
    """``type``: an operation type as Horizon names it, or its ``type_i`` as a number."""
    raw = params.get("type", "")
    if raw == "":
        return None
    if _DIGITS.fullmatch(raw):
        return "type_i", int(raw)
    if _TYPE_NAME.fullmatch(raw):
        return "type", raw
    raise bad_request("type must be an operation type (manage_data, payment, ...) or its number", field="type")


def data_filter(params: Mapping[str, str]) -> tuple[str | None, str | None]:
    """``data_name`` and ``data_value`` of a manage_data operation, as stored.

    ``data_value`` that is given empty is a filter of its own: it asks for the
    operations whose stored value is the empty string, which are the deletions
    and the empty values alike.
    """
    name, value = params.get("data_name"), params.get("data_value")
    if name is not None and not _DATA_NAME.fullmatch(name):
        raise bad_request("data_name must be 1 to 64 printable characters", field="data_name")
    if value is not None and not _BASE64.fullmatch(value):
        raise bad_request("data_value must be base64, as Horizon shows a value", field="data_value")
    return name, value


# -- assets -------------------------------------------------------------------


@dataclass(frozen=True)
class Asset:
    """An asset by its canonical id: ``native``, or ``<code>:<issuer>``."""

    asset_id: str

    @property
    def native(self) -> bool:
        return self.asset_id == "native"


def asset_id(value: str) -> Asset:
    """A canonical asset id, as this installation names assets."""
    if value == "native":
        return Asset(value)
    code, sep, issuer = value.partition(":")
    if sep and _CODE.fullmatch(code) and _ACCOUNT.fullmatch(issuer) and StrKey.is_valid_ed25519_public_key(issuer):
        return Asset(value)
    raise bad_request("an asset is native or <code>:<issuer>", field="asset")


def requested_asset(params: Mapping[str, str], prefix: str) -> Asset | None:
    """The asset named by ``<prefix>asset_type`` (and ``_code``, ``_issuer``),
    or None when no parameter of it was given."""
    kind = params.get(f"{prefix}asset_type", "")
    code, issuer = params.get(f"{prefix}asset_code", ""), params.get(f"{prefix}asset_issuer", "")
    if not (kind or code or issuer):
        return None
    if not kind:
        raise bad_request(f"{prefix}asset_type is required with an asset", field=f"{prefix}asset_type")
    if kind == "native":
        if code or issuer:
            raise bad_request("a native asset has neither a code nor an issuer", field=f"{prefix}asset_type")
        return Asset("native")
    lengths = ASSET_TYPES.get(kind)
    if lengths is None:
        raise bad_request(
            f"{prefix}asset_type must be native, credit_alphanum4 or credit_alphanum12", field=f"{prefix}asset_type"
        )
    if not code or not issuer:
        raise bad_request(
            f"a {kind} asset needs {prefix}asset_code and {prefix}asset_issuer", field=f"{prefix}asset_code"
        )
    if not _CODE.fullmatch(code) or not lengths[0] <= len(code) <= lengths[1]:
        raise bad_request(
            f"{prefix}asset_code must be {lengths[0]} to {lengths[1]} letters or digits for {kind}",
            field=f"{prefix}asset_code",
        )
    if not _ACCOUNT.fullmatch(issuer) or not StrKey.is_valid_ed25519_public_key(issuer):
        raise bad_request(f"{prefix}asset_issuer must be a Stellar account address", field=f"{prefix}asset_issuer")
    return Asset(f"{code}:{issuer}")


def requested_pair(params: Mapping[str, str]) -> tuple[Asset, Asset] | None:
    """The base and counter asset of a trade request, or None without any.

    One without the other is not a pair and is refused.
    """
    base, counter = requested_asset(params, "base_"), requested_asset(params, "counter_")
    if base is None and counter is None:
        return None
    if base is None or counter is None:
        raise bad_request("a market is named by both a base and a counter asset", field="base_asset_type")
    return base, counter
