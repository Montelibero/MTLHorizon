"""What this installation wants to be told of, as subscriptions of
Operations Notifier — read from the watch lists, and from nothing else.

One subscription for each enabled account. One for each asset that is
enabled, or marked for discovery. A market is covered by a subscription on
one of its assets: every trade of the pair has both, and the Notifier's
rules (version 2) match an operation by the asset it deals in, the assets
on its path and the assets of its trades. Native is never subscribed for a
market's sake — nearly every operation of the network deals in it — and an
asset that is enabled is subscribed whether or not a market wants it.

A subscription is one for a kind and a key, whatever rules want it: the
rules are kept with it so that ``status`` can say why it is there, and so
that it is retired only when none of them wants it any more. Switching an
account off withdraws its account subscription and nothing else.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.backfill.jobs import market_key
from app.storage.models import TrackedAccount, TrackedAsset, TrackedMarket

ACCOUNT, ASSET = "account", "asset"


@dataclass(frozen=True)
class Want:
    kind: str
    key: str
    # The rules that want it, each once.
    rules: tuple[str, ...]


def asset_id(base: str, counter: str) -> str | None:
    """The one asset to subscribe for a market: either will do, native will
    not; the lower one is chosen so that the choice does not depend on the
    order the market was given in."""
    sides = sorted(side for side in (base, counter) if side != "native")
    return sides[0] if sides else None


def wanted(session: Session) -> dict[tuple[str, str], Want]:
    """Everything wanted, by (kind, key)."""
    wants: dict[tuple[str, str], list[str]] = {}

    def want(kind: str, key: str, rule: str) -> None:
        wants.setdefault((kind, key), [])
        if rule not in wants[(kind, key)]:
            wants[(kind, key)].append(rule)

    for account in session.scalars(select(TrackedAccount).where(TrackedAccount.enabled.is_(True))):
        want(ACCOUNT, account.account_id, "account enabled")
    for asset in session.scalars(select(TrackedAsset)):
        if asset.enabled:
            want(ASSET, asset.asset_id, "history of the asset")
        if asset.discover:
            want(ASSET, asset.asset_id, "discovery")
    for market in session.scalars(select(TrackedMarket).where(TrackedMarket.enabled.is_(True))):
        chosen = asset_id(market.base_asset_id, market.counter_asset_id)
        if chosen is not None:
            want(ASSET, chosen, f"market {market_key(market.base_asset_id, market.counter_asset_id)}")
    return {key: Want(key[0], key[1], tuple(rules)) for key, rules in wants.items()}


def is_native(key: str) -> bool:
    return key == "native"


def rules_of(want: Want | None) -> Iterable[str]:
    return want.rules if want is not None else ()
