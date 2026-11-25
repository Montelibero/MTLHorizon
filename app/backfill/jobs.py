"""Which streams a backfill reads: every enabled account and every enabled market."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.storage.coverage import CoverageKey
from app.storage.models import TrackedAccount, TrackedMarket

# A policy is what gets collected. Its version changes when the meaning of a
# policy changes; coverage of an older version stays a true record of what
# was collected under it. "FULL" is the name the account policy was given
# when accounts still had modes; it means what it did — the four streams of
# the account — so the coverage collected under it stays valid.
POLICY_VERSIONS = {"FULL": 1, "MARKET": 1}

ACCOUNT_STREAMS = ("transactions", "operations", "effects", "trades")
# Everything the account has, failed transactions included.
ACCOUNT_STREAM_FILTERS: dict[str, Mapping[str, str]] = {
    "transactions": {"include_failed": "true"},
    "operations": {"include_failed": "true"},
    "effects": {},
    "trades": {},
}
# Operations whose result can contain trades.
TRADE_MAKING_OPERATIONS = frozenset(
    {"manage_sell_offer", "manage_buy_offer", "create_passive_sell_offer", "path_payment_strict_receive", "path_payment_strict_send"}
)


class StreamJob:
    """One stream of one tracked subject."""

    def __init__(
        self,
        key: CoverageKey,
        resource_type: str,
        stream_key: str,
        path: str,
        filters: Mapping[str, Any],
        participant: str | None,
    ) -> None:
        self.key = key
        # The collection (transactions) and the kind of record in it, which
        # is what its cursors are parsed as (transaction).
        self.stream = resource_type
        self.resource_type = resource_type.removesuffix("s")
        self.stream_key = stream_key
        self.path = path
        self.filters = dict(filters)
        # The account whose participation in what this stream returns is
        # stated by Horizon itself. None for a market.
        self.participant = participant

    @property
    def name(self) -> str:
        return f"{self.key.subject_type} {self.key.subject_key} / {self.key.data_type}"


def market_key(base_asset_id: str, counter_asset_id: str) -> str:
    """A market is an unordered pair; the key names the lower asset first."""
    low, high = sorted((base_asset_id, counter_asset_id))
    return f"{low}>{high}"


def asset_params(prefix: str, asset_id: str) -> dict[str, str]:
    """The Horizon query parameters naming an asset."""
    if asset_id == "native":
        return {f"{prefix}asset_type": "native"}
    code, issuer = asset_id.split(":", 1)
    asset_type = "credit_alphanum4" if len(code) <= 4 else "credit_alphanum12"
    return {f"{prefix}asset_type": asset_type, f"{prefix}asset_code": code, f"{prefix}asset_issuer": issuer}


def stream_jobs(session: Session) -> tuple[list[StreamJob], list[str]]:
    """The streams to read, and a line for each tracked subject left out."""
    jobs: list[StreamJob] = []
    left_out: list[str] = []
    for account in session.scalars(select(TrackedAccount).order_by(TrackedAccount.account_id)):
        if not account.enabled:
            left_out.append(f"account {account.account_id}: disabled; its history is not loaded")
            continue
        for stream in ACCOUNT_STREAMS:
            jobs.append(
                StreamJob(
                    CoverageKey("account", account.account_id, stream, "FULL", POLICY_VERSIONS["FULL"]),
                    stream,
                    account.account_id,
                    f"/accounts/{account.account_id}/{stream}",
                    ACCOUNT_STREAM_FILTERS[stream],
                    account.account_id,
                )
            )
    for market in session.scalars(select(TrackedMarket).order_by(TrackedMarket.base_asset_id)):
        if not market.enabled:
            left_out.append(f"market {market_key(market.base_asset_id, market.counter_asset_id)}: disabled")
            continue
        filters = {**asset_params("base_", market.base_asset_id), **asset_params("counter_", market.counter_asset_id)}
        jobs.append(
            StreamJob(
                CoverageKey(
                    "market",
                    market_key(market.base_asset_id, market.counter_asset_id),
                    "trades",
                    "MARKET",
                    POLICY_VERSIONS["MARKET"],
                ),
                "trades",
                market_key(market.base_asset_id, market.counter_asset_id),
                "/trades",
                filters,
                None,
            )
        )
    return jobs, left_out
