"""manage_data operations: what each of them does to a data entry.

Upstream shows the ``value`` of a manage_data operation as an empty string
both when the operation deletes the entry and when it sets an empty one: by
the operation record alone the two cannot be told apart. What is stored can:

* the envelope of the transaction (XDR; it is always stored) — the operation
  itself says whether it carries a value or deletes the entry;
* the effect of the operation, where the effects were collected and the
  transaction succeeded: ``data_removed`` against ``data_created`` and
  ``data_updated``.

The envelope is read first: it is the operation. Where it cannot be decoded
the effect speaks. Where the two disagree, or neither can tell, the answer is
``unknown`` and nothing is guessed.

What the member says is what the operation *asks for*. Whether it took effect
is the transaction's, and the record says it: ``transaction_successful``.
"""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import Connection, Row
from stellar_sdk.xdr import EnvelopeType, OperationType
from stellar_sdk.xdr import TransactionEnvelope as TransactionEnvelopeXdr

from app.storage.models import HistoryEffect, HistoryTransaction

# Horizon's type_i: of the operation, and of the three effects it can have.
MANAGE_DATA = 10
DATA_CREATED, DATA_REMOVED, DATA_UPDATED = 40, 41, 42
DATA_EFFECTS = {DATA_CREATED: "data_created", DATA_REMOVED: "data_removed", DATA_UPDATED: "data_updated"}
SET, REMOVE, UNKNOWN = "set", "remove", "unknown"


@dataclass(frozen=True)
class DataChange:
    """What is known of one manage_data operation.

    ``request``: ``set`` (a value, possibly an empty one), ``remove`` (the
    entry is deleted), or ``unknown``. ``told_by``: what told it, ``xdr`` or
    ``effect``. ``value``: base64, as Horizon shows a value; known for ``set``
    only, and not always. ``effect``: the data effect stored for the
    operation, if there is one.
    """

    request: str = UNKNOWN
    told_by: str | None = None
    value: str | None = None
    effect: str | None = None

    def as_member(self) -> dict[str, Any]:
        member: dict[str, Any] = {"request": self.request, "told_by": self.told_by}
        if self.request == SET and self.value is not None:
            member["value"] = self.value
        member["effect"] = self.effect
        return member


def changes(connection: Connection, operations: Sequence[Row]) -> dict[int, DataChange]:
    """The manage_data operations among ``operations`` (rows as the operations
    stream returns them), each by its id.

    Two lookups, both as long as the page: the envelopes of their
    transactions, and their data effects.
    """
    manage_data = [row for row in operations if row.type_i == MANAGE_DATA]
    if not manage_data:
        return {}
    operation_ids = [row.id for row in manage_data]
    envelopes = dict(
        connection.execute(
            select(HistoryTransaction.id, HistoryTransaction.envelope_xdr).where(
                HistoryTransaction.id.in_({row.transaction_id for row in manage_data})
            )
        ).all()
    )
    effects: dict[int, dict[str, Any]] = {}
    for operation_id, type_i, raw in connection.execute(
        select(HistoryEffect.operation_id, HistoryEffect.type_i, HistoryEffect.raw).where(
            HistoryEffect.type_i.in_(list(DATA_EFFECTS)), HistoryEffect.operation_id.in_(operation_ids)
        )
    ):
        effects.setdefault(operation_id, {})[type_i] = raw if isinstance(raw, Mapping) else None
    return {row.id: _change(row, envelopes.get(row.transaction_id), effects.get(row.id)) for row in manage_data}


def _change(
    operation: Row, envelope_xdr: str | None, effects: Mapping[int, Mapping[str, Any] | None] | None
) -> DataChange:
    effect = next(iter(DATA_EFFECTS[type_i] for type_i in effects), None) if effects else None
    name = operation.raw.get("name") if isinstance(operation.raw, Mapping) else None
    from_envelope = _from_envelope(envelope_xdr, operation.application_order, name)
    from_effect = _from_effects(effects)
    if from_envelope is not None:
        request, value = from_envelope
        # The effect of a successful transaction follows its operation. If it
        # does not, one of the two is not what it is stored as: nothing is said.
        if from_effect is not None and from_effect[0] != request:
            return DataChange(UNKNOWN, None, None, effect)
        return DataChange(request, "xdr", value, effect)
    if from_effect is not None:
        return DataChange(from_effect[0], "effect", from_effect[1], effect)
    return DataChange(UNKNOWN, None, None, effect)


def _from_envelope(envelope_xdr: str | None, application_order: int, name: str | None) -> tuple[str, str | None] | None:
    """``(set, value)`` or ``(remove, None)`` as the operation itself says, or
    None when the envelope cannot tell: it does not decode, the operation at
    that place is not a manage_data one, or it is not about this entry."""
    if not envelope_xdr:
        return None
    try:
        envelope = TransactionEnvelopeXdr.from_xdr(envelope_xdr)
        if envelope.type == EnvelopeType.ENVELOPE_TYPE_TX_FEE_BUMP:
            operations = envelope.fee_bump.tx.inner_tx.v1.tx.operations
        elif envelope.type == EnvelopeType.ENVELOPE_TYPE_TX:
            operations = envelope.v1.tx.operations
        elif envelope.type == EnvelopeType.ENVELOPE_TYPE_TX_V0:
            operations = envelope.v0.tx.operations
        else:
            return None
        if not 1 <= application_order <= len(operations):
            return None
        body = operations[application_order - 1].body
        if body.type != OperationType.MANAGE_DATA:
            return None
        manage_data = body.manage_data_op
        if name is not None and _text(manage_data.data_name) != name:
            return None
        stored = manage_data.data_value
        # In XDR the three cases are apart: bytes, empty bytes, and none.
        if stored is None:
            return REMOVE, None
        return SET, base64.b64encode(stored.data_value).decode("ascii")
    except Exception:  # noqa: BLE001 - whatever the SDK raises for what it cannot read means one thing: it cannot tell
        return None


def _text(value: Any) -> str:
    """A string as the SDK keeps it in XDR: an object over UTF-8 bytes."""
    raw = getattr(value, "string64", value)
    return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)


def _from_effects(effects: Mapping[int, Mapping[str, Any] | None] | None) -> tuple[str, str | None] | None:
    """The same from the stored data effect of a successful operation."""
    if not effects:
        return None
    kinds = {DATA_EFFECTS[type_i] for type_i in effects}
    if kinds == {"data_removed"}:
        return REMOVE, None
    if kinds and kinds <= {"data_created", "data_updated"}:
        raw = next(raw for type_i, raw in effects.items() if raw)
        value = raw.get("value")
        return SET, value if isinstance(value, str) else None
    return None
