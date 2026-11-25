"""What the source does when read forwards, checked on a small window.

A collector is trusted with a source only after this: that reading forwards
from a ledger position starts exactly above it, that a page continues
strictly after its last record, and that a position below the history the
source keeps is not an error but the start of that history.

About nine requests; nothing is stored.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.backfill.jobs import ACCOUNT_STREAM_FILTERS, asset_params
from app.sources import horizon_records as records
from app.sources.horizon import BeforeHistory, HorizonClient, OrderKey
from app.sources.horizon_cursors import after_ledger, order_key

ACCOUNT_STREAMS = ("transactions", "operations", "effects", "trades")


@dataclass(frozen=True)
class Finding:
    name: str
    ok: bool
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return {"check": self.name, "ok": self.ok, "detail": self.detail}


def ascending(
    client: HorizonClient,
    *,
    account: str,
    base_asset_id: str,
    counter_asset_id: str,
    window: int = 86400,
) -> list[Finding]:
    """Read every stream of one account, and one market, forwards from
    ``window`` ledgers below the source's latest ledger."""
    findings: list[Finding] = []
    root = client.root()
    latest, elder = int(root["history_latest_ledger"]), int(root["history_elder_ledger"])
    lower = max(latest - window + 1, elder)
    findings.append(Finding("source", True, f"latest ledger {latest}, history kept from {elder}, window from {lower}"))

    paths: list[tuple[str, str, dict[str, str]]] = [
        (f"/accounts/{account}/{stream}", stream.removesuffix("s"), dict(ACCOUNT_STREAM_FILTERS[stream]))
        for stream in ACCOUNT_STREAMS
    ]
    paths.append(("/trades", "trade", {**asset_params("base_", base_asset_id), **asset_params("counter_", counter_asset_id)}))
    for path, resource_type, filters in paths:
        name = path.rsplit("/", 1)[-1] if path != "/trades" else "market trades"
        try:
            page = client.page(path, resource_type=resource_type, order="asc", cursor=str(after_ledger(lower - 1)), limit=200, **filters)
        except BeforeHistory:
            findings.append(Finding(name, False, "410 on a position inside the history kept"))
            continue
        ledgers = [records.ledger_of(record, resource_type) for record in page.records]
        first = ledgers[0] if ledgers else None
        inside = all(ledger >= lower for ledger in ledgers)
        findings.append(
            Finding(
                f"{name}: forwards from ledger {lower}",
                inside,
                f"{len(page.records)} record(s)"
                + (f", first in ledger {first}, last in {ledgers[-1]}" if ledgers else "")
                + ("" if inside else f" — a record lies below ledger {lower}"),
            )
        )
        # Only where a pair cursor and a plain one are met: does a page go on
        # strictly after its last record?
        if page.records and name in ("transactions", "market trades", "trades"):
            token = records.paging_token(page.records[-1])
            boundary = order_key(resource_type, token)
            following = client.page(path, resource_type=resource_type, order="asc", cursor=token, limit=200, **filters)
            if not following.records:
                findings.append(Finding(f"{name}: after its last record", True, "no record follows: the stream ends here"))
                continue
            keys = [order_key(resource_type, records.paging_token(record)) for record in following.records]
            strict = all(later > earlier for earlier, later in zip([boundary, *keys], keys))
            findings.append(
                Finding(
                    f"{name}: after its last record",
                    strict,
                    f"{len(keys)} record(s), first in ledger {keys[0][0] >> 32}"
                    + ("" if strict else " — a record repeats or goes back"),
                )
            )

    # A position below the history kept: reading forwards starts where the
    # history does, without an error.
    try:
        page = client.page(
            f"/accounts/{account}/effects", resource_type="effect", order="asc",
            cursor=str(after_ledger(max(elder - 1000, 0))), limit=1,
        )
        ledgers = [records.ledger_of(record, "effect") for record in page.records]
        findings.append(
            Finding(
                "a position below the history kept",
                all(ledger >= elder for ledger in ledgers),
                f"200, {len(page.records)} record(s)" + (f", first in ledger {ledgers[0]}" if ledgers else "")
                + " (the source starts where its history does, without a word)",
            )
        )
    except BeforeHistory:
        findings.append(Finding("a position below the history kept", False, "410: the stream would fail, not skip"))
    return findings


def print_findings(findings: list[Finding]) -> bool:
    width = max(len(finding.name) for finding in findings)
    for finding in findings:
        print(f"  {'ok  ' if finding.ok else 'FAIL'}  {finding.name.ljust(width)}  {finding.detail}")
    return all(finding.ok for finding in findings)
