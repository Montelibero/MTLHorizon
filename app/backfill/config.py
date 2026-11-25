"""Importing the watch lists from a YAML file.

One file names assets, markets and accounts. Importing it again changes
nothing unless the file did. An account's mode is changed only when asked
for. Nothing is removed because a later file leaves it out: history once
collected stays, and so does the record of what was collected.

::

    assets:
      - code: MTLRECT
        issuer: GACKTN5DAZGWXRWB2WLM6OPBDHAMT6SJNGLJZPQMEZBUR4JUGBX2UK7V
      - native: true
    markets:
      - base: MTLRECT:GACKTN5DAZGWXRWB2WLM6OPBDHAMT6SJNGLJZPQMEZBUR4JUGBX2UK7V
        counter: native
    accounts:
      - address: G...
        mode: FULL
        reason: why this account is watched
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Mapping
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy.orm import Session
from stellar_sdk import StrKey

from app.backfill.jobs import market_key
from app.backfill.store import asset_parts
from app.storage.models import (
    HistoryAccount,
    HistoryAsset,
    TrackedAccount,
    TrackedAsset,
    TrackedMarket,
)
from app.storage.upsert import upsert_row

MODES = ("FULL", "ASSETS_ONLY", "CANDIDATE", "DISABLED")
# Policies whose history the backfill reads. The others are stored as given
# and skipped: they are not quietly treated as FULL.
LOADED_BY_BACKFILL = frozenset({"FULL"})

_CODE = re.compile(r"[A-Za-z0-9]{1,12}")


class ConfigError(ValueError):
    """The file is not a usable watch list. Nothing is imported."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("; ".join(errors))


@dataclass(frozen=True)
class AssetRule:
    asset_id: str
    reason: str


@dataclass(frozen=True)
class MarketRule:
    base_asset_id: str
    counter_asset_id: str
    reason: str


@dataclass(frozen=True)
class AccountRule:
    address: str
    mode: str
    reason: str


@dataclass(frozen=True)
class TrackingConfig:
    assets: tuple[AssetRule, ...]
    markets: tuple[MarketRule, ...]
    accounts: tuple[AccountRule, ...]


@dataclass
class ImportResult:
    """What importing did, by kind of entry."""

    added: Counter[str] = field(default_factory=Counter)
    unchanged: Counter[str] = field(default_factory=Counter)
    changed: Counter[str] = field(default_factory=Counter)
    # Entries left as they were, with the reason.
    skipped: list[str] = field(default_factory=list)
    # Accounts stored with a mode the backfill does not load.
    not_loaded: list[str] = field(default_factory=list)

    def lines(self) -> list[str]:
        def count(kind: str) -> str:
            parts = [f"{self.added[kind]} added", f"{self.unchanged[kind]} unchanged"]
            if self.changed[kind]:
                parts.append(f"{self.changed[kind]} changed")
            return ", ".join(parts)

        return [
            f"assets:   {count('asset')}",
            f"markets:  {count('market')}",
            f"accounts: {count('account')}, {len(self.skipped)} skipped",
            *self.skipped,
            *self.not_loaded,
        ]


def load(path: Path) -> TrackingConfig:
    """Read and validate a watch list file. Raises ConfigError listing every
    problem found, so that one round of editing fixes them all."""
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ConfigError([f"{path}: {error.strerror}"]) from error
    except yaml.YAMLError as error:
        raise ConfigError([f"{path}: not valid YAML: {error}"]) from error
    return parse(document, str(path))


def parse(document: Any, source: str = "config") -> TrackingConfig:
    errors: list[str] = []
    if not isinstance(document, Mapping):
        raise ConfigError([f"{source}: the file is not a mapping"])
    errors += [f"{source}: unknown section {name!r}" for name in sorted(set(document) - set(SECTIONS))]

    rules = {
        name: parser(_section(document, name, errors, source), errors, source)
        for name, parser in (("assets", _assets), ("markets", _markets), ("accounts", _accounts))
    }
    if errors:
        raise ConfigError(errors)
    return TrackingConfig(tuple(rules["assets"]), tuple(rules["markets"]), tuple(rules["accounts"]))


SECTIONS = ("assets", "markets", "accounts")


def _section(document: Mapping[str, Any], name: str, errors: list[str], source: str) -> list[Any]:
    value = document.get(name, [])
    if value is None:
        return []
    if not isinstance(value, list):
        errors.append(f"{source}: section {name!r} must be a list")
        return []
    return value


def _entry(
    item: Any, index: int, name: str, allowed: set[str], errors: list[str], source: str
) -> Mapping[str, Any] | None:
    where = f"{source}: {name}[{index}]"
    if not isinstance(item, Mapping):
        errors.append(f"{where}: not a mapping")
        return None
    errors.extend(f"{where}: unknown key {key!r}" for key in sorted(set(item) - allowed))
    return item


def _reason(item: Mapping[str, Any], default: str) -> str:
    value = item.get("reason", default)
    return value.strip() if isinstance(value, str) and value.strip() else default


def _text(item: Mapping[str, Any], key: str, where: str, errors: list[str]) -> str | None:
    value = item.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    errors.append(f"{where}: {key} is missing or not text")
    return None


def asset_id(code: str, issuer: str, where: str, errors: list[str]) -> str | None:
    """Canonical id of an asset given as code and issuer."""
    if not _CODE.fullmatch(code):
        errors.append(f"{where}: asset code {code!r} is not 1 to 12 letters or digits")
        return None
    if not StrKey.is_valid_ed25519_public_key(issuer):
        errors.append(f"{where}: issuer {issuer!r} is not an account address")
        return None
    return f"{code}:{issuer}"


def parse_asset_id(value: str, where: str, errors: list[str]) -> str | None:
    """Canonical id of an asset given as one string: ``native`` or ``CODE:ISSUER``."""
    if value == "native":
        return value
    code, separator, issuer = value.partition(":")
    if not separator:
        errors.append(f"{where}: asset {value!r} is neither 'native' nor 'CODE:ISSUER'")
        return None
    return asset_id(code, issuer, where, errors)


def _assets(items: list[Any], errors: list[str], source: str) -> list[AssetRule]:
    rules: list[AssetRule] = []
    seen: set[str] = set()
    for index, raw in enumerate(items):
        where = f"{source}: assets[{index}]"
        entry = _entry(raw, index, "assets", {"code", "issuer", "native", "reason"}, errors, source)
        if entry is None:
            continue
        asset = None
        if entry.get("native") is True:
            if entry.get("code") or entry.get("issuer"):
                errors.append(f"{where}: native has neither code nor issuer")
            else:
                asset = "native"
        elif "native" in entry:
            errors.append(f"{where}: 'native' must be true or left out")
        else:
            code = _text(entry, "code", where, errors)
            issuer = _text(entry, "issuer", where, errors)
            if code is not None and issuer is not None:
                asset = asset_id(code, issuer, where, errors)
        if asset is None:
            continue
        if asset in seen:
            errors.append(f"{where}: asset {asset} is listed twice")
            continue
        seen.add(asset)
        rules.append(AssetRule(asset, _reason(entry, "imported from config")))
    return rules


def _markets(items: list[Any], errors: list[str], source: str) -> list[MarketRule]:
    rules: list[MarketRule] = []
    seen: dict[str, str] = {}
    for index, raw in enumerate(items):
        where = f"{source}: markets[{index}]"
        entry = _entry(raw, index, "markets", {"base", "counter", "reason"}, errors, source)
        if entry is None:
            continue
        given = _text(entry, "base", where, errors), _text(entry, "counter", where, errors)
        if None in given:
            continue
        base = parse_asset_id(given[0], where, errors)  # type: ignore[arg-type]
        counter = parse_asset_id(given[1], where, errors)  # type: ignore[arg-type]
        if base is None or counter is None:
            continue
        if base == counter:
            errors.append(f"{where}: a market needs two different assets, got {base} twice")
            continue
        key = market_key(base, counter)
        if key in seen:
            errors.append(f"{where}: market {base}/{counter} is {seen[key]} the other way round")
            continue
        seen[key] = f"{base}/{counter}"
        rules.append(MarketRule(base, counter, _reason(entry, "imported from config")))
    return rules


def _accounts(items: list[Any], errors: list[str], source: str) -> list[AccountRule]:
    rules: list[AccountRule] = []
    seen: set[str] = set()
    for index, raw in enumerate(items):
        where = f"{source}: accounts[{index}]"
        entry = _entry(raw, index, "accounts", {"address", "mode", "reason"}, errors, source)
        if entry is None:
            continue
        address = _text(entry, "address", where, errors)
        if address is None:
            continue
        if not StrKey.is_valid_ed25519_public_key(address):
            errors.append(f"{where}: {address!r} is not an account address")
            continue
        # Counted from here, so that a repeated address is reported even when
        # its first entry has other problems.
        repeated = address in seen
        seen.add(address)
        mode, reason = entry.get("mode"), entry.get("reason")
        if mode not in MODES:
            errors.append(f"{where}: mode {mode!r} is not one of {', '.join(MODES)}")
            continue
        if not isinstance(reason, str) or not reason.strip():
            errors.append(f"{where}: reason is missing: say why the account is watched")
            continue
        if repeated:
            errors.append(f"{where}: account {address} is listed twice")
            continue
        rules.append(AccountRule(address, mode, reason.strip()))
    return rules


def apply(session: Session, config: TrackingConfig, *, change_modes: bool = False) -> ImportResult:
    """Write the watch list.

    Runs in the caller's transaction: a failure anywhere leaves nothing of
    the file applied. An account's stored mode changes only with
    ``change_modes``; a file that leaves entries out removes nothing.
    """
    result = ImportResult()
    for rule in config.assets:
        if session.get(TrackedAsset, rule.asset_id) is None:
            _register_asset(session, rule.asset_id)
            upsert_row(session, TrackedAsset.__table__, {"asset_id": rule.asset_id}, {"reason": rule.reason})
            result.added["asset"] += 1
        else:
            result.unchanged["asset"] += 1

    for rule in config.markets:
        # A market is an unordered pair: it is stored with the lower asset first.
        base, counter = sorted((rule.base_asset_id, rule.counter_asset_id))
        key = {"base_asset_id": base, "counter_asset_id": counter}
        for asset_id in (base, counter):
            _register_asset(session, asset_id)
        if session.get(TrackedMarket, (base, counter)) is None:
            upsert_row(session, TrackedMarket.__table__, key, {"reason": rule.reason})
            result.added["market"] += 1
        else:
            result.unchanged["market"] += 1

    for rule in config.accounts:
        key = {"account_id": rule.address}
        tracked = session.get(TrackedAccount, rule.address)
        if tracked is None:
            upsert_row(session, HistoryAccount.__table__, key, {})
            upsert_row(session, TrackedAccount.__table__, key, {"mode": rule.mode, "reason": rule.reason})
            result.added["account"] += 1
        elif tracked.mode == rule.mode:
            result.unchanged["account"] += 1
        elif change_modes:
            tracked.mode, tracked.reason = rule.mode, rule.reason
            session.flush()
            result.changed["account"] += 1
        else:
            result.skipped.append(
                f"account {rule.address}: the file says {rule.mode}, the database says {tracked.mode}; "
                "left as it is (import again with --apply-changes to change it)"
            )
        if rule.mode not in LOADED_BY_BACKFILL:
            result.not_loaded.append(f"account {rule.address}: history is not loaded for mode {rule.mode}")
    return result


def _register_asset(session: Session, asset_id: str) -> None:
    upsert_row(session, HistoryAsset.__table__, {"asset_id": asset_id}, asset_parts(asset_id))
