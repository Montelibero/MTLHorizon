"""Cursors and pages: where a stream starts, how it is ordered, and its links.

A cursor is a position in the stream, nothing else. It is read exactly as
upstream reads one (:mod:`app.sources.horizon_cursors` has the formats):

* transactions and operations: a total order id (SEP-35);
* effects and trades: ``<operation id>-<order>``. Given as one number, the
  order part is open: everything of that operation and above.

The same cursor means the same position whatever the order: ``asc`` answers
what lies after it, ``desc`` what lies before it. Without a cursor, ``asc``
starts at the oldest record and ``desc`` at the newest; ``now`` is the end of
the newest ledger the archive has.

The links of a page repeat the request with the cursor moved, as upstream
builds them: ``next`` goes on in the same order from the last record of the
page, ``prev`` goes back from the first one. Upstream attaches ``next`` to
every page, the last one included, so its presence says nothing about whether
more records exist; a page shorter than the limit is the last one.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

from sqlalchemy import ColumnElement, Tuple, asc, desc, tuple_

from app.history_api.params import DEFAULT_LIMIT, INT64_MAX, MAX_LIMIT, integer
from app.history_api.params import order as order_of
from app.history_api.problems import bad_request
from app.sources.horizon_cursors import MAX_ORDER, after_ledger, effect_id

TRANSACTION, OPERATION, EFFECT, TRADE = "transaction", "operation", "effect", "trade"
_DIGITS = re.compile(r"[0-9]{1,19}")
_OTHER = {"asc": "desc", "desc": "asc"}


@dataclass(frozen=True)
class Paging:
    """The page arguments of one request. ``after`` is the position records
    have to lie beyond, as a tuple in the order of the key; None means the end
    of the stream, which is where a stream starts when no cursor is given."""

    kind: str
    order: str
    limit: int
    # What the links repeat: the cursor as it was asked for.
    cursor: str
    after: tuple[int, ...] | None

    @property
    def ascending(self) -> bool:
        return self.order == "asc"

    @property
    def asks_now(self) -> bool:
        return self.cursor == "now"

    def at_now(self, latest_ledger: int | None) -> "Paging":
        """The same request with ``now`` resolved: after everything in
        ``latest_ledger``, the newest ledger the archive has (ledger 0 when it
        has none, which is before every record)."""
        value = after_ledger(latest_ledger or 0)
        after: tuple[int, ...] = (value,) if self.kind in (TRANSACTION, OPERATION) else (value, MAX_ORDER)
        return Paging(self.kind, self.order, self.limit, str(value), after)

    def condition(self, *columns: Any) -> ColumnElement[bool] | None:
        """``key > cursor`` (or ``<``): what narrows the stream to the page."""
        if self.after is None:
            return None
        key: Any = columns[0] if len(columns) == 1 else tuple_(*columns)
        bound: Any = self.after[0] if len(columns) == 1 else tuple_(*self.after)
        return key > bound if self.ascending else key < bound

    def ordering(self, *columns: Any) -> list[ColumnElement | Tuple]:
        direction = asc if self.ascending else desc
        return [direction(column) for column in columns]

    def text_condition(self, column: Any, key: str) -> ColumnElement[bool]:
        """``column > key`` on the stored text of an effect id, whose order is
        the order of the pair it encodes (zero-padded, both parts)."""
        return column > key if self.ascending else column < key


def parse(kind: str, params: Mapping[str, str]) -> Paging:
    """The page arguments of a request. What cannot be read is a 400."""
    order = order_of(params)
    limit = integer(params, "limit", default=DEFAULT_LIMIT, minimum=1, maximum=MAX_LIMIT)
    cursor = params.get("cursor", "")
    if cursor in ("", "now"):
        return Paging(kind, order, limit, cursor, None)

    parts = cursor.split("-")
    if len(parts) > (2 if kind in (EFFECT, TRADE) else 1) or not all(_DIGITS.fullmatch(part) for part in parts):
        raise _not_a_cursor(kind)
    first = int(parts[0])
    if first > INT64_MAX:
        raise _not_a_cursor(kind)
    if len(parts) == 1 and kind in (TRANSACTION, OPERATION):
        return Paging(kind, order, limit, cursor, (first,))
    # The order part is cut to what an operation can hold, as upstream cuts it;
    # missing, it is open.
    second = MAX_ORDER if len(parts) == 1 else min(MAX_ORDER, int(parts[1]))
    return Paging(kind, order, limit, cursor, (first, second))


def _not_a_cursor(kind: str) -> Exception:
    shape = "an '<operation id>-<order>' pair" if kind in (EFFECT, TRADE) else "a total order id"
    return bad_request(f"cursor is not a paging token of this stream: {shape}, or now", field="cursor")


def effect_key(after: tuple[int, ...]) -> str:
    """The stored text of a position in the effects: comparable with effect ids."""
    return effect_id(after[0], after[1])


def href(base: str, path: str, query: Mapping[str, str]) -> str:
    return f"{base}{path}?{urlencode(sorted(query.items()))}"


def page(
    base: str,
    path: str,
    filters: Mapping[str, str],
    paging: Paging,
    records: Sequence[Mapping[str, Any]],
    scope: Mapping[str, Any],
) -> dict[str, Any]:
    """A HAL page: its records, its three links, and what kind of a view it is.

    ``filters`` are the other parameters of the request, repeated in every
    link so that following one keeps them. An empty page repeats the cursor it
    was asked with.
    """
    tokens = [str(record["paging_token"]) for record in records]
    query = {**filters, "cursor": paging.cursor, "limit": str(paging.limit), "order": paging.order}
    first, last = (tokens[0], tokens[-1]) if tokens else (paging.cursor, paging.cursor)
    return {
        "_links": {
            "self": {"href": href(base, path, query)},
            "next": {"href": href(base, path, {**query, "cursor": last})},
            "prev": {"href": href(base, path, {**query, "order": _OTHER[paging.order], "cursor": first})},
        },
        "_embedded": {"records": list(records)},
        **dict(scope),
    }
