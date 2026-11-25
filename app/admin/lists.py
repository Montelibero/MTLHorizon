"""The watch lists as their pages change and read them: accounts, assets and
markets brought in by hand, each rule switched on and off by itself — and
what the pages show of the lists.

Nothing is changed here in a way of its own. Every change is the function a
command of an earlier stage uses — :mod:`app.watch.accounts` for an account,
:mod:`app.backfill.config` for an asset or a market — called inside one
transaction, with what the owner wrote judged as those commands judge it.
What follows from a change (a start boundary, an import, a subscription) is
the worker's pass, as it is for anything a command adds; no server is asked
here.

What is never done: a counterparty or an issuer is not brought into the
watch list, and what the owner switched off is not switched on by anything
but the form that says so. An entry that is in a list already is left as it
is when the list is asked to add it again; a rule that is stored changes
only from the entry's own page, one rule at a time.

The reads give one page of a list at a time: each asks the database for a
bounded number of rows, and says in its docstring when it can show less than
there is.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Text, and_, bindparam, func, or_, select, text, type_coerce
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from stellar_sdk import StrKey

from app.backfill import config as watch_lists
from app.backfill.jobs import market_key
from app.storage import history_jobs, service_jobs
from app.storage.models import (
    DiscoveryRun,
    HistoryJob,
    HistoryTransaction,
    HistoryTransactionParticipant,
    HorizonNodeState,
    NotifierEvent,
    ServiceJob,
    TrackedAccount,
    TrackedAsset,
    TrackedMarket,
    WatchSubscription,
)
from app.subscriptions import desired
from app.watch import accounts as watched

LEGACY = watched.LEGACY
ACCOUNT, MARKET = "account", "market"
# How the worker's pass marks the import of a subject that was switched off —
# an account's or a market's.

# An account's address, and what a search for one may begin with.
ADDRESS = re.compile(r"G[A-Z0-9]{55}")
ADDRESS_START = re.compile(r"G[A-Z0-9]{0,55}")
# An asset's code, as ``app.backfill.config`` reads one.
CODE = re.compile(r"[A-Za-z0-9]{1,12}")
# A reason is a line, and not a page.
REASON_LENGTH = 500
# Accounts brought in from one form: at most this many at once.
MOST_AT_ONCE = 200
# Of what the owner wrote, quoted back in a message: no more than this.
QUOTED = 64

# The newest deliveries of the Notifier that are looked through for the last
# events of an account. The journal has no index by account and is never
# pruned, so a search that was not bounded would grow with it.
EVENT_WINDOW = 2000
EVENTS_OF_A_LIST, EVENTS_OF_A_PAGE = 3, 20


# -- what the owner wrote ----------------------------------------------------------


def _tidy(given: str) -> str:
    return " ".join(given.split())


def _shown(given: str) -> str:
    tidy = _tidy(given)
    return tidy if len(tidy) <= QUOTED else tidy[: QUOTED - 1] + "…"


def address_of(given: str) -> str:
    """An account's address as the owner wrote it, judged as the watch list
    file is (the checksum included). A ValueError, in words for the owner,
    when it is not one."""
    address = _tidy(given)
    if not address:
        raise ValueError("не указан адрес аккаунта")
    if not ADDRESS.fullmatch(address) or not StrKey.is_valid_ed25519_public_key(address):
        raise ValueError(f"«{_shown(address)}» — не адрес аккаунта: нужен G…-адрес целиком")
    return address


def address_start(given: str) -> str:
    """The start of an address to search the accounts by; "" for all of them."""
    start = _tidy(given)
    if start and not ADDRESS_START.fullmatch(start):
        raise ValueError(f"«{_shown(start)}» — не начало адреса: адрес начинается с G и состоит из заглавных латинских букв и цифр")
    return start


def asset_of(given: str) -> str:
    """The canonical id of an asset as the owner wrote it — ``native`` or
    ``CODE:ISSUER`` — judged as the watch list file is. A ValueError, in
    words for the owner, when it is not one."""
    asset = _tidy(given)
    if not asset:
        raise ValueError("не указан актив: нужно «native» или «КОД:ЭМИТЕНТ»")
    errors: list[str] = []
    judged = watch_lists.parse_asset_id(asset, "asset", errors)
    if judged is not None:
        return judged
    code, separator, issuer = asset.partition(":")
    if not separator:
        raise ValueError(f"«{_shown(asset)}» — не актив: нужно «native» или «КОД:ЭМИТЕНТ»")
    if not CODE.fullmatch(code):
        raise ValueError(f"«{_shown(code)}» — не код актива: от 1 до 12 латинских букв и цифр")
    raise ValueError(f"«{_shown(issuer)}» — не адрес эмитента: нужен G…-адрес целиком")


def addresses_of(given: str) -> list[str]:
    """The addresses written in a form's field — one a line, or apart by
    commas, semicolons or spaces — each checked, each once, in the order
    written. A ValueError, in words for the owner, naming the first that is
    not an address: nothing is added unless all of them are."""
    found: list[str] = []
    for word in re.split(r"[\s,;]+", given):
        if word and word not in found:
            found.append(address_of(word))
    if not found:
        raise ValueError("не указан ни один адрес: нужны G…-адреса, по одному в строке или через запятую")
    if len(found) > MOST_AT_ONCE:
        raise ValueError(f"за один раз добавляется не больше {MOST_AT_ONCE} адресов")
    return found


def reason_of(given: str, what: str) -> str:
    """Why an entry is watched, as one line of text; a ValueError when
    nothing was written."""
    reason = _tidy(given)
    if not reason:
        raise ValueError(f"нужна причина: зачем наблюдается этот {what}")
    if len(reason) > REASON_LENGTH:
        raise ValueError(f"причина длиннее {REASON_LENGTH} знаков: сократите её")
    return reason


def _pair(base: str, counter: str) -> tuple[str, str]:
    """The two assets of a market as it is stored: an unordered pair, the
    lower asset first — whichever way round the owner wrote them."""
    base, counter = asset_of(base), asset_of(counter)
    if base == counter:
        raise ValueError("рынку нужны два разных актива")
    return sorted((base, counter))


def _native_discoverable(discover: bool, asset_id: str) -> None:
    if discover and asset_id == "native":
        raise ValueError("native (XLM) нельзя пометить для discovery: его держит каждый аккаунт сети")


# -- changing the lists: one transaction each ------------------------------------


def add_asset(session: Session, asset_id: str, *, enabled: bool, discover: bool, reason: str) -> bool:
    """Bring an asset into the watch list with its two rules as given, and
    say whether it was added now.

    ``enabled`` is the rule of its new events; ``discover``, a rule apart,
    is to look for the holders of its trustline. An asset that is in the list
    already is left as it is, rules and reason alike, whatever is asked for
    here: so that nothing is switched on behind the owner's back. Native
    cannot be discovered.
    """
    asset_id = asset_of(asset_id)
    _native_discoverable(discover, asset_id)
    rule = watch_lists.AssetRule(asset_id, reason_of(reason, "актив"), enabled, discover)
    written = watch_lists.apply(session, watch_lists.TrackingConfig(assets=(rule,), markets=(), accounts=()))
    return written.added["asset"] == 1


def set_asset_flags(session: Session, asset_id: str, *, enabled: bool | None = None, discover: bool | None = None) -> None:
    """Change the rules asked for, and no other: a rule left out keeps its
    value. Switching ``discover`` off stops the looking; the accounts it
    found stay in the list. Switching ``enabled`` off withdraws the rule of
    the asset's history, and nothing else."""
    asset_id = asset_of(asset_id)
    _native_discoverable(bool(discover), asset_id)
    if enabled is None and discover is None:
        raise ValueError("не сказано, какое правило актива изменить")
    tracked = session.get(TrackedAsset, asset_id)
    if tracked is None:
        raise ValueError(f"актива {asset_id} нет в списке наблюдения: добавьте его прежде")
    if enabled is not None:
        tracked.enabled = enabled
    if discover is not None:
        tracked.discover = discover
    session.flush()


def add_market(session: Session, base: str, counter: str, *, enabled: bool, reason: str) -> tuple[str, str, bool]:
    """Bring a market into the watch list — two different assets, one
    unordered pair, whichever way round they are written — and give the pair
    as it is stored (the lower asset first) with whether it was added now. A
    market that is in the list already is left as it is."""
    low, high = _pair(base, counter)
    rule = watch_lists.MarketRule(low, high, reason_of(reason, "рынок"), enabled)
    written = watch_lists.apply(session, watch_lists.TrackingConfig(assets=(), markets=(rule,), accounts=()))
    return low, high, written.added["market"] == 1


def set_market_enabled(session: Session, base: str, counter: str, *, enabled: bool, reason: str | None = None) -> None:
    """Switch a market on or off. Switched off, nothing is deleted: the
    history of its trades stays, and the worker's pass pauses its import. No
    asset and nobody's subscription is touched by it. A reason given replaces
    the stored one."""
    low, high = _pair(base, counter)
    tracked = session.get(TrackedMarket, (low, high))
    if tracked is None:
        raise ValueError(f"рынка {low} / {high} нет в списке наблюдения: добавьте его прежде")
    tracked.enabled = enabled
    if reason is not None:
        tracked.reason = reason_of(reason, "рынок")
    session.flush()


def _watched(session: Session, address: str) -> TrackedAccount:
    tracked = session.get(TrackedAccount, address)
    if tracked is None:
        raise ValueError(f"аккаунт {_shown(address)} не в списке наблюдения: добавьте его прежде")
    return tracked


def set_account_enabled(session: Session, address: str, *, enabled: bool, reason: str | None = None) -> str:
    """Switch an account off or on, as ``python -m app.watch disable-account``
    and ``enable-account`` do, and say what came of it: ``on`` or ``off`` —
    what it is now, switched by this call — or ``unchanged``. (The service's
    own word for an account switched on is the same as for one found enabled
    already, so it is not read here.)

    Switched off, nothing is deleted: the history stays, and the worker's pass
    pauses its import and retires its subscription — nobody else's, and no
    asset or market. Switched on again, what was missed while it was off is
    read from a new start boundary."""
    address = address_of(address)
    if _watched(session, address).enabled == enabled:
        return "unchanged"
    watched.set_enabled(session, address, enabled=enabled, reason=None if reason is None else reason_of(reason, "аккаунт"))
    return "on" if enabled else "off"


def set_account_reason(session: Session, address: str, *, reason: str) -> None:
    """Say why an account is watched. Nothing else of it changes."""
    address = address_of(address)
    _watched(session, address).reason = reason_of(reason, "аккаунт")
    session.flush()


def enqueue_history(engine: Engine, kind: str, subject: str, *, now: datetime | None = None) -> tuple[HistoryJob, bool]:
    """Make sure the history of one subject is planned — all there is, as far
    back as the pool keeps — and give its job with whether it was made now:
    what ``python -m app.history enqueue`` plans. Planning again only widens
    the boundaries; it never resets progress.

    No server is asked. The upper boundary is the latest ledger the pool is
    known to have kept (its last check), and 1 until anything is known.
    Whatever it is, the worker's pass raises it to the latest ledger: for a
    market at once, and makes it final; for an account once it is told of.

    An account that was here before stage 05 (``legacy``) gets no import of
    its own: this is what it lacks. While it has no start boundary it is also
    marked as due for one, as an account added by hand is, and so gets the
    same life from the pass: the boundary, the import planned by that
    boundary, the subscription and its check, and the upper boundary once the
    subscription is confirmed. The mark is written before the import is
    planned: a crash between the two leaves the account due, and the pass
    plans the import. A boundary once taken is not taken again.

    An import the pass paused for its subject being switched off is taken
    back into the queue, the subject being switched on now.

    The subject must be watched and switched on: nothing of its history is
    read otherwise.
    """
    pair: tuple[str, str] | None = None
    if kind == ACCOUNT:
        subject = address_of(subject)
        name = f"аккаунт {subject[:6]}…"
    elif kind == MARKET:
        pair = _market(subject)
        subject, name = market_key(*pair), f"рынок {pair[0]} / {pair[1]}"
    else:
        raise ValueError(f"kind is one of {ACCOUNT}, {MARKET}, not {kind!r}")
    with Session(engine) as session, session.begin():
        tracked: Any
        if kind == ACCOUNT:
            tracked = session.get(TrackedAccount, subject)
            if tracked is not None and tracked.enabled and tracked.origin == LEGACY and tracked.watch_since_ledger is None:
                tracked.boundary_due = True
        else:
            tracked = session.get(TrackedMarket, pair)
        if tracked is None:
            raise ValueError(f"{name} не наблюдается: добавьте его в список прежде")
        if not tracked.enabled:
            raise ValueError(f"{name} выключен: его история не читается. Включите его и повторите")
        latest = session.scalar(select(func.max(HorizonNodeState.latest_ledger)))
    job, made = history_jobs.plan(
        engine, kind=kind, subject=subject, to_ledger=int(latest or 1), boundary_final=False, resume=True, now=now
    )
    return job, made


def import_of(engine: Engine, kind: str, subject: str) -> HistoryJob | None:
    """The history import of one subject, as plain data; None when none is
    planned."""
    with Session(engine) as session:
        row = session.scalar(select(HistoryJob).where(HistoryJob.kind == kind, HistoryJob.subject == subject))
        return None if row is None else _plain(row)


def _market(subject: str) -> tuple[str, str]:
    """The two assets of a market named as its jobs name it: ``A>B``."""
    low, separator, high = subject.partition(">")
    if not separator or ">" in high:
        raise ValueError(f"«{_shown(subject)}» — не рынок")
    return _pair(low, high)


# -- reading: one page of a list ---------------------------------------------------


@dataclass(frozen=True)
class Event:
    """One delivery of the Notifier that names an account, as a line says of it."""

    id: str
    type: str
    state: str
    received_at: datetime
    operation_type: str | None
    ledger: int | None


@dataclass(frozen=True)
class Account:
    account_id: str
    enabled: bool
    origin: str
    reason: str
    discovered_by: tuple[str, ...]
    watch_since_ledger: int | None
    boundary_due: bool
    created_at: datetime
    # The import of its history, and how its subscription stands.
    job: HistoryJob | None
    subscription: WatchSubscription | None
    # The newest ledger of a transaction of the account that is stored, and
    # when it closed.
    latest_ledger: int | None
    latest_ledger_at: datetime | None
    events: tuple[Event, ...]


@dataclass(frozen=True)
class Asset:
    asset_id: str
    enabled: bool
    discover: bool
    reason: str
    created_at: datetime
    # How the asset's subscription stands, and the rules that want it.
    subscription: WatchSubscription | None
    rules: tuple[str, ...]
    last_run: DiscoveryRun | None
    # Accounts of the list that the discovery of this asset found.
    found_by: int


@dataclass(frozen=True)
class Market:
    base_asset_id: str
    counter_asset_id: str
    key: str
    enabled: bool
    reason: str
    created_at: datetime
    # The import of the market's trades.
    job: HistoryJob | None
    # The one asset subscribed for the market, and how its subscription stands.
    subscribed: str | None
    subscription: WatchSubscription | None


@dataclass
class Counts:
    """How many entries a list has, by what they are."""

    total: int = 0
    on: int = 0
    due: int = 0  # accounts: waiting for their start boundary
    discover: int = 0  # assets: marked for discovery
    found: int = 0  # accounts that discovery brought in


def _plain(row: Any) -> Any:
    """A row as plain data, alive after the session that read it is gone."""
    return type(row)(**{column.name: getattr(row, column.name) for column in row.__table__.columns})


def _jobs(session: Session, kind: str, subjects: Sequence[str]) -> dict[str, HistoryJob]:
    """The history import of each subject named, by subject."""
    if not subjects:
        return {}
    rows = session.scalars(select(HistoryJob).where(HistoryJob.kind == kind, HistoryJob.subject.in_(subjects)))
    return {row.subject: _plain(row) for row in rows}


def _subscriptions(session: Session, kind: str, keys: Sequence[str]) -> dict[str, WatchSubscription]:
    """The subscription row of each key named, by key."""
    if not keys:
        return {}
    rows = session.scalars(select(WatchSubscription).where(WatchSubscription.kind == kind, WatchSubscription.key.in_(keys)))
    return {row.key: _plain(row) for row in rows}


def _latest_ledgers(session: Session, addresses: Sequence[str]) -> dict[str, tuple[int, datetime]]:
    """For each account named: the ledger of its newest stored transaction,
    and when that ledger closed. One row per account, read by its index."""
    if not addresses:
        return {}
    # The newest transaction of an account is the one with the greatest total
    # order id; the maximum is read by the index of (account, transaction).
    newest = (
        select(
            HistoryTransactionParticipant.account_id.label("account_id"),
            func.max(HistoryTransactionParticipant.transaction_id).label("id"),
        )
        .where(HistoryTransactionParticipant.account_id.in_(addresses))
        .group_by(HistoryTransactionParticipant.account_id)
        .subquery()
    )
    rows = session.execute(
        select(newest.c.account_id, HistoryTransaction.ledger_sequence, HistoryTransaction.ledger_close_time).join(
            HistoryTransaction, HistoryTransaction.id == newest.c.id
        )
    )
    return {account: (int(ledger), closed) for account, ledger, closed in rows}


def _events(session: Session, addresses: Sequence[str], per: int) -> dict[str, list[Event]]:
    """The newest deliveries of the Notifier that name each account, at most
    ``per`` of one. They are looked for among the newest ``EVENT_WINDOW``
    deliveries of all: an account nothing recent names shows fewer than it
    has."""
    if not addresses:
        return {}
    newest = (
        select(NotifierEvent.id, NotifierEvent.subscription)
        .order_by(NotifierEvent.received_at.desc(), NotifierEvent.id)
        .limit(EVENT_WINDOW)
        .subquery()
    )
    naming = or_(NotifierEvent.account.in_(addresses), NotifierEvent.accounts.op("?|")(type_coerce(addresses, ARRAY(Text))))
    found: dict[str, list[Event]] = {}
    for row in session.scalars(
        select(NotifierEvent)
        .join(newest, and_(NotifierEvent.id == newest.c.id, NotifierEvent.subscription == newest.c.subscription))
        .where(naming)
        .order_by(NotifierEvent.received_at.desc(), NotifierEvent.id)
    ):
        for address in addresses:
            if (row.account == address or address in (row.accounts or ())) and len(found.setdefault(address, [])) < per:
                found[address].append(Event(row.id, row.type, row.state, row.received_at, row.operation_type, row.ledger_sequence))
    return found


def _accounts_of(session: Session, tracked: Sequence[TrackedAccount], per: int) -> list[Account]:
    """The accounts read, each with what is kept of its import, its
    subscription, its newest stored ledger and its last events."""
    ids = [row.account_id for row in tracked]
    jobs = _jobs(session, ACCOUNT, ids)
    subscriptions = _subscriptions(session, desired.ACCOUNT, ids)
    latest = _latest_ledgers(session, ids)
    events = _events(session, ids, per)
    return [
        Account(
            row.account_id,
            row.enabled,
            row.origin,
            row.reason,
            tuple(asset for asset in (row.discovered_by or "").split(",") if asset),
            row.watch_since_ledger,
            row.boundary_due,
            row.created_at,
            jobs.get(row.account_id),
            subscriptions.get(row.account_id),
            *latest.get(row.account_id, (None, None)),
            tuple(events.get(row.account_id, ())),
        )
        for row in tracked
    ]


def accounts(
    engine: Engine, *, search: str = "", enabled: bool | None = None, origin: str | None = None, limit: int, offset: int = 0
) -> tuple[list[Account], int]:
    """One page of the accounts, by address — those enabled or of an origin
    when asked, and those whose address begins with ``search`` — and how many
    there are under the same conditions."""
    where = []
    if search:
        where.append(TrackedAccount.account_id.startswith(search, autoescape=True))
    if enabled is not None:
        where.append(TrackedAccount.enabled.is_(enabled))
    if origin:
        where.append(TrackedAccount.origin == origin)
    with Session(engine) as session:
        total = session.scalar(select(func.count()).select_from(TrackedAccount).where(*where)) or 0
        tracked = list(
            session.scalars(select(TrackedAccount).where(*where).order_by(TrackedAccount.account_id).limit(limit).offset(offset))
        )
        return _accounts_of(session, tracked, EVENTS_OF_A_LIST), int(total)


def account(engine: Engine, address: str) -> Account | None:
    """One account, with more of its last events than a list shows. None when
    it is not watched."""
    with Session(engine) as session:
        tracked = session.get(TrackedAccount, address)
        return None if tracked is None else _accounts_of(session, [tracked], EVENTS_OF_A_PAGE)[0]


def account_counts(engine: Engine) -> Counts:
    """The accounts: how many, how many switched on, how many waiting for
    their start boundary, how many discovery brought in."""
    with Session(engine) as session:
        grouped = session.execute(
            select(TrackedAccount.enabled, TrackedAccount.boundary_due, TrackedAccount.origin, func.count()).group_by(
                TrackedAccount.enabled, TrackedAccount.boundary_due, TrackedAccount.origin
            )
        ).all()
    counts = Counts()
    for on, due, origin, number in grouped:
        counts.total += number
        counts.on += number if on else 0
        counts.due += number if due else 0
        counts.found += number if origin == watched.DISCOVERY else 0
    return counts


_FOUND_BY = text(
    "SELECT found.asset AS asset_id, count(*) AS accounts "
    "FROM tracked_accounts, unnest(string_to_array(tracked_accounts.discovered_by, ',')) AS found(asset) "
    "WHERE found.asset IN :assets GROUP BY found.asset"
).bindparams(bindparam("assets", expanding=True))


def _found_by(session: Session, asset_ids: Sequence[str]) -> dict[str, int]:
    """For each asset named: how many accounts of the list carry it as the
    asset whose discovery found them. Counted by the database, in one read."""
    if not asset_ids:
        return {}
    return {asset_id: int(number) for asset_id, number in session.execute(_FOUND_BY, {"assets": list(asset_ids)})}


def _last_runs(session: Session, asset_ids: Sequence[str]) -> dict[str, DiscoveryRun]:
    """The last pass of each asset named, among those that were not dry runs."""
    if not asset_ids:
        return {}
    # The passes of each asset numbered from the newest: the first of each is
    # the one asked for.
    place = (
        func.row_number()
        .over(partition_by=DiscoveryRun.asset_id, order_by=(DiscoveryRun.started_at.desc(), DiscoveryRun.id.desc()))
        .label("place")
    )
    numbered = (
        select(DiscoveryRun.id.label("id"), place)
        .where(DiscoveryRun.asset_id.in_(asset_ids), DiscoveryRun.dry_run.is_(False))
        .subquery()
    )
    rows = session.scalars(select(DiscoveryRun).join(numbered, numbered.c.id == DiscoveryRun.id).where(numbered.c.place == 1))
    return {row.asset_id: _plain(row) for row in rows}


def _assets_of(session: Session, tracked: Sequence[TrackedAsset]) -> list[Asset]:
    ids = [row.asset_id for row in tracked]
    wanted = {key[1]: want.rules for key, want in desired.wanted(session).items() if key[0] == desired.ASSET}
    found_by = _found_by(session, ids)
    subscriptions = _subscriptions(session, desired.ASSET, ids)
    last = _last_runs(session, ids)
    return [
        Asset(
            row.asset_id,
            row.enabled,
            row.discover,
            row.reason,
            row.created_at,
            subscriptions.get(row.asset_id),
            wanted.get(row.asset_id, ()),
            last.get(row.asset_id),
            found_by.get(row.asset_id, 0),
        )
        for row in tracked
    ]


def assets(
    engine: Engine, *, search: str = "", enabled: bool | None = None, discover: bool | None = None, limit: int, offset: int = 0
) -> tuple[list[Asset], int]:
    """One page of the assets, by id — those enabled, or marked for
    discovery, when asked, and those whose code or issuer has ``search`` in
    it — and how many there are under the same conditions."""
    where = []
    if search:
        where.append(TrackedAsset.asset_id.icontains(search, autoescape=True))
    if enabled is not None:
        where.append(TrackedAsset.enabled.is_(enabled))
    if discover is not None:
        where.append(TrackedAsset.discover.is_(discover))
    with Session(engine) as session:
        total = session.scalar(select(func.count()).select_from(TrackedAsset).where(*where)) or 0
        tracked = list(session.scalars(select(TrackedAsset).where(*where).order_by(TrackedAsset.asset_id).limit(limit).offset(offset)))
        return _assets_of(session, tracked), int(total)


def asset(engine: Engine, asset_id: str) -> Asset | None:
    """One asset. None when it is not watched."""
    with Session(engine) as session:
        tracked = session.get(TrackedAsset, asset_id)
        return None if tracked is None else _assets_of(session, [tracked])[0]


def flags_of(engine: Engine, asset_ids: Sequence[str]) -> dict[str, tuple[bool, bool]]:
    """The two rules of each asset named that is watched: (enabled, discover).
    An asset left out is not in the list."""
    if not asset_ids:
        return {}
    with Session(engine) as session:
        rows = session.execute(
            select(TrackedAsset.asset_id, TrackedAsset.enabled, TrackedAsset.discover).where(TrackedAsset.asset_id.in_(asset_ids))
        )
        return {asset_id: (bool(on), bool(discover)) for asset_id, on, discover in rows}


def asset_counts(engine: Engine) -> Counts:
    """The assets: how many, how many enabled, how many marked for discovery."""
    with Session(engine) as session:
        grouped = session.execute(
            select(TrackedAsset.enabled, TrackedAsset.discover, func.count()).group_by(TrackedAsset.enabled, TrackedAsset.discover)
        ).all()
    counts = Counts()
    for on, discover, number in grouped:
        counts.total += number
        counts.on += number if on else 0
        counts.discover += number if discover else 0
    return counts


def _markets_of(session: Session, tracked: Sequence[TrackedMarket]) -> list[Market]:
    keys = [market_key(row.base_asset_id, row.counter_asset_id) for row in tracked]
    jobs = _jobs(session, MARKET, keys)
    # The one asset each market is covered by — never native — and its subscription.
    chosen = list(dict.fromkeys(desired.asset_id(row.base_asset_id, row.counter_asset_id) for row in tracked))
    subscriptions = _subscriptions(session, desired.ASSET, [asset_id for asset_id in chosen if asset_id is not None])
    return [
        Market(
            row.base_asset_id,
            row.counter_asset_id,
            key,
            row.enabled,
            row.reason,
            row.created_at,
            jobs.get(key),
            chosen[number],
            subscriptions.get(chosen[number] or ""),
        )
        for number, (row, key) in enumerate(zip(tracked, keys))
    ]


def markets(
    engine: Engine, *, search: str = "", enabled: bool | None = None, limit: int, offset: int = 0
) -> tuple[list[Market], int]:
    """One page of the markets, by their canonical pair — those enabled when
    asked, and those with ``search`` in either of their assets — and how many
    there are under the same conditions."""
    where = []
    if search:
        needle = f"%{_like(search)}%"
        where.append(or_(TrackedMarket.base_asset_id.ilike(needle, escape="\\"), TrackedMarket.counter_asset_id.ilike(needle, escape="\\")))
    if enabled is not None:
        where.append(TrackedMarket.enabled.is_(enabled))
    with Session(engine) as session:
        total = session.scalar(select(func.count()).select_from(TrackedMarket).where(*where)) or 0
        tracked = list(
            session.scalars(
                select(TrackedMarket).where(*where).order_by(TrackedMarket.base_asset_id, TrackedMarket.counter_asset_id).limit(limit).offset(offset)
            )
        )
        return _markets_of(session, tracked), int(total)


def market(engine: Engine, base: str, counter: str) -> Market | None:
    """One market, whichever way round its pair is written. None when it is
    not watched."""
    low, high = _pair(base, counter)
    with Session(engine) as session:
        tracked = session.get(TrackedMarket, (low, high))
        return None if tracked is None else _markets_of(session, [tracked])[0]


def _like(search: str) -> str:
    """A search as it goes into a LIKE pattern: its own wildcards are
    characters to look for."""
    return _tidy(search).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def runs(engine: Engine, asset_id: str, *, limit: int = 5) -> list[DiscoveryRun]:
    """The newest passes over an asset that were not dry runs — a dry run
    keeps no record of itself; its result is its task's."""
    with Session(engine) as session:
        rows = session.scalars(
            select(DiscoveryRun)
            .where(DiscoveryRun.asset_id == asset_id, DiscoveryRun.dry_run.is_(False))
            .order_by(DiscoveryRun.started_at.desc(), DiscoveryRun.id.desc())
            .limit(max(1, limit))
        )
        return [_plain(row) for row in rows]


def tasks(engine: Engine, asset_id: str | None = None, *, limit: int = 10) -> list[ServiceJob]:
    """The newest tasks of discovery — of every asset, or of one — as plain
    data. A task that is queued or running may be older than ``limit`` tasks
    that ended after it, and is then not among them: the tasks page has the
    active ones first."""
    chosen = select(ServiceJob).where(ServiceJob.kind == "discovery")
    if asset_id is not None:
        chosen = chosen.where(ServiceJob.params["asset"].astext == asset_id)
    with Session(engine) as session:
        rows = session.scalars(chosen.order_by(ServiceJob.created_at.desc(), ServiceJob.id.desc()).limit(max(1, limit)))
        return [_plain(row) for row in rows]


def active_tasks(engine: Engine) -> list[ServiceJob]:
    """The tasks of discovery that are queued or running."""
    return service_jobs.recent(engine, states=service_jobs.ACTIVE, kind="discovery", limit=200)
