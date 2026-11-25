"""One pass of keeping every account's life in step.

An account's life, from the moment it is added:

1. **The intent and the start boundary.** The account is in the watch list
   with ``boundary_due`` set. The pass asks the pool for its latest ledger
   and writes it to ``watch_since_ledger`` — from which ledger on nothing
   of the account is to be missed — and plans the import of the account's
   history up to it.
2. **Being told of it.** The subscription wanted for the account is
   registered at the Notifier, and confirmed by reading it back there.
3. **The upper boundary, after the confirmation.** The latest ledger is
   asked for again and the history import is extended up to it: whatever
   happened while the subscription was being registered is read by the
   import, and what the Notifier tells of from now on is stored by the
   worker. The two overlap, and overlap costs nothing: a record is stored
   once.

Each step is read from the database, not remembered between passes: an
account that needs a step is one whose row says so. A crash anywhere loses
nothing, because the next pass finds the same rows.

A disabled account has its import paused and its subscription retired; the
history stays. Switched on again, it is due for a new start boundary, and
the import that follows covers the time it was off.

The pass asks Horizon for nothing when every account is in step, and asks
the Notifier for nothing when there is nothing to register.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.backfill.jobs import market_key
from app.config import Settings
from app.discovery import events
from app.notifier.client import NotifierClient, NotifierError
from app.sources import live
from app.sources.horizon import HorizonClient
from app.storage import history_jobs
from app.storage.models import HistoryJob, TrackedAccount, TrackedAsset, TrackedMarket, WatchSubscription
from app.subscriptions import reconcile
from app.watch import accounts as watched

logger = logging.getLogger(__name__)



@dataclass
class PassReport:
    """What a pass did."""

    openings: int = 0  # trustline openings looked at in the stored history
    accounts_added: int = 0
    start_boundaries: int = 0
    imports_planned: int = 0
    upper_boundaries: int = 0
    imports_extended: int = 0
    imports_paused: int = 0
    # Accounts waiting for a boundary that could not be captured: Horizon
    # was not asked in this pass.
    waiting_for_a_boundary: int = 0
    subscriptions: reconcile.Report | None = None
    requests: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "openings": self.openings,
            "accounts_added": self.accounts_added,
            "start_boundaries": self.start_boundaries,
            "imports_planned": self.imports_planned,
            "upper_boundaries": self.upper_boundaries,
            "imports_extended": self.imports_extended,
            "imports_paused": self.imports_paused,
            "waiting_for_a_boundary": self.waiting_for_a_boundary,
            "subscriptions_wanted": None if self.subscriptions is None else self.subscriptions.wanted,
            "subscriptions_created": None if self.subscriptions is None else self.subscriptions.created,
            "subscriptions_confirmed": None if self.subscriptions is None else self.subscriptions.confirmed + self.subscriptions.taken,
            "subscriptions_retired": None if self.subscriptions is None else self.subscriptions.retired,
            "subscriptions_not_done": None if self.subscriptions is None else self.subscriptions.failed + self.subscriptions.ambiguous,
            "requests": self.requests,
        }

    def lines(self) -> list[str]:
        lines = [
            f"accounts: {self.accounts_added} added on an opened trustline ({self.openings} looked at), "
            f"{self.start_boundaries} start boundaries written, {self.imports_paused} import(s) paused"
        ]
        lines.append(
            f"history: {self.imports_planned} planned, {self.upper_boundaries} upper boundaries written "
            f"({self.imports_extended} extended)"
        )
        if self.waiting_for_a_boundary:
            lines.append(f"  {self.waiting_for_a_boundary} account(s) wait for a boundary: Horizon was not asked")
        if self.subscriptions is not None:
            lines += [f"  {line}" for line in self.subscriptions.lines()]
        lines.append(f"{self.requests} request(s) to Horizon")
        return lines


def run(
    engine: Engine,
    settings: Settings,
    *,
    client: HorizonClient | None = None,
    api: NotifierClient | None = None,
    now: datetime | None = None,
) -> PassReport:
    """One pass. ``client`` and ``api`` may be None: the pass then does what
    needs neither, and says what it left."""
    now = now or datetime.now(UTC)
    report = PassReport()
    first = 0 if client is None else client.stats.requests

    # 1. Trustlines opened on a discovery asset, in what the worker stored.
    with Session(engine) as session:
        assets = list(session.scalars(select(TrackedAsset.asset_id).where(TrackedAsset.discover.is_(True))))
    if assets:
        changes = events.find_and_add(engine, assets, now=now)
        report.openings = len(changes)
        report.accounts_added = sum(1 for change in changes if change.outcome == watched.ADDED)

    # 2. Every wanted subscription has its row. No word to the Notifier.
    wanted = reconcile.ensure(engine, settings.notifier_callback_url, now=now)

    # 3. Start boundaries: accounts added, or switched on again.
    due = _accounts_due(engine)
    report.waiting_for_a_boundary = len(due) if client is None else 0
    if due and client is not None:
        latest = client.history().latest
        # The import is planned first and the flag cleared after: a crash in
        # between leaves the account due, and the next pass plans its import
        # again — which changes nothing, as planning never resets progress.
        # The other way round, a crash would leave an account with a boundary
        # and no import, which nothing would notice.
        for account_id in due:
            history_jobs.plan(
                engine,
                kind="account",
                subject=account_id,
                to_ledger=latest,
                context=settings.history_context,
                boundary_final=False,
                resume=True,
                now=now,
            )
        with Session(engine) as session, session.begin():
            for account_id in due:
                tracked = session.get(TrackedAccount, account_id)
                assert tracked is not None
                tracked.watch_since_ledger, tracked.boundary_due = latest, False
        report.start_boundaries, report.imports_planned = len(due), len(due)
        logger.info("%d account(s) start at ledger %d; their history is planned up to it", len(due), latest)

    # 4. Subscriptions: register, confirm, retire — with the Notifier, once
    #    the webhook is sure to take what it sends.
    if api is not None:
        waiting = reconcile.due(engine, now)
        problem = _reception_problem(api, settings) if waiting else None
        if problem is not None:
            reconcile.postpone_all(engine, problem, now=now)
            report.subscriptions = reconcile.Report(wanted=len(wanted), unreachable=problem)
        else:
            report.subscriptions = reconcile.run(engine, api, settings.notifier_callback_url, now=now)

    # 5. The upper boundary: for an account once it is told of; for a market
    #    at once, for it is told of by the subscription of its asset — which
    #    is not this job's to wait for. A market's import is planned from the
    #    admin page with whatever ledger was last known; this makes it the
    #    latest one.
    told_of = _imports_awaiting_the_boundary(engine)
    markets = _market_imports_awaiting_the_boundary(engine, _enabled_markets(engine))
    if (told_of or markets) and client is not None:
        latest = client.history().latest
        for job_id, subject, to_ledger in told_of:
            history_jobs.extend_boundary(
                engine, job_id, to_ledger=max(latest, to_ledger), boundary_final=True, now=now
            )
        for job_id, subject, to_ledger in markets:
            # Planned again first: that lifts the pause the pass makes for a
            # market switched off, now that it is on — and widens nothing,
            # for the boundary asked is the one it has.
            history_jobs.plan(
                engine, kind="market", subject=subject, to_ledger=to_ledger, boundary_final=False, resume=True, now=now
            )
            history_jobs.extend_boundary(
                engine, job_id, to_ledger=max(latest, to_ledger), boundary_final=True, now=now
            )
        report.upper_boundaries, report.imports_extended = len(told_of) + len(markets), len(told_of) + len(markets)
        logger.info(
            "%d history import(s) now reach ledger %d, from where their subjects are told of", len(told_of) + len(markets), latest
        )

    # 6. Accounts and markets switched off: the import pauses, the history stays.
    paused = _pause_imports_of_disabled(engine)
    report.imports_paused = paused

    report.requests = 0 if client is None else client.stats.requests - first
    return report


def _reception_problem(api: NotifierClient, settings: Settings) -> str | None:
    """Why registering with this Notifier would be pointless right now: its
    webhooks would not get past this installation's own door. None when they
    would."""
    try:
        key = api.status().get("publicKey")
    except NotifierError as error:
        return None  # it cannot be asked now; the pass that follows says so
    if not settings.notifier_signer_keys:
        return "nothing is registered until MTL_HORIZON_NOTIFIER_KEYS names the key this Notifier signs with"
    if key not in settings.notifier_signer_keys:
        return f"nothing is registered: this Notifier signs with {key}, which is not among MTL_HORIZON_NOTIFIER_KEYS, so its webhooks would be refused"
    if not settings.notifier_callback_url:
        return "nothing is registered until MTL_HORIZON_NOTIFIER_CALLBACK_URL says where its webhooks are to be sent"
    return None


def _accounts_due(engine: Engine) -> list[str]:
    """Enabled accounts whose start boundary is not captured yet."""
    with Session(engine) as session:
        return list(
            session.scalars(
                select(TrackedAccount.account_id)
                .where(TrackedAccount.enabled.is_(True), TrackedAccount.boundary_due.is_(True))
                .order_by(TrackedAccount.account_id)
            )
        )


def _imports_awaiting_the_boundary(engine: Engine) -> list[tuple[int, str, int]]:
    """The imports of accounts that are told of (their subscription is
    confirmed or adopted) and whose upper boundary is not final yet:
    (job id, account, boundary)."""
    with Session(engine) as session:
        return [
            (int(job_id), account, int(to_ledger))
            for job_id, account, to_ledger in session.execute(
                select(HistoryJob.id, HistoryJob.subject, HistoryJob.to_ledger)
                .join(
                    WatchSubscription,
                    (WatchSubscription.kind == "account") & (WatchSubscription.key == HistoryJob.subject),
                )
                .join(TrackedAccount, TrackedAccount.account_id == HistoryJob.subject)
                .where(
                    HistoryJob.kind == "account",
                    HistoryJob.boundary_final.is_(False),
                    HistoryJob.state != "paused",
                    TrackedAccount.enabled.is_(True),
                    WatchSubscription.state.in_(reconcile.CONFIRMED),
                )
                .order_by(HistoryJob.id)
            )
        ]


def _enabled_markets(engine: Engine) -> list[str]:
    """The keys of the enabled markets, as a market's import is named by."""
    with Session(engine) as session:
        return [
            market_key(market.base_asset_id, market.counter_asset_id)
            for market in session.scalars(select(TrackedMarket).where(TrackedMarket.enabled.is_(True)))
        ]


def _market_imports_awaiting_the_boundary(engine: Engine, enabled: list[str]) -> list[tuple[int, str, int]]:
    """The imports of enabled markets whose upper boundary is not final yet,
    and those the pass paused for their market being switched off — which is
    on again: (job id, market, boundary)."""
    if not enabled:
        return []
    with Session(engine) as session:
        return [
            (int(job_id), market, int(to_ledger))
            for job_id, market, to_ledger in session.execute(
                select(HistoryJob.id, HistoryJob.subject, HistoryJob.to_ledger)
                .where(
                    HistoryJob.kind == "market",
                    HistoryJob.subject.in_(enabled),
                    (HistoryJob.boundary_final.is_(False) & (HistoryJob.state != "paused"))
                    | (HistoryJob.paused_because == history_jobs.switched_off("market")),
                )
                .order_by(HistoryJob.id)
            )
        ]


def _pause_imports_of_disabled(engine: Engine) -> int:
    """Pause the imports of accounts and markets that are switched off, or
    gone from the watch lists, that are not paused yet — each with the
    reason of its own kind, for that is what lifts the pause again when the
    subject is switched on (``history_jobs.plan`` with ``resume``). Returns
    how many were paused."""
    from sqlalchemy import true

    with Session(engine) as session:
        enabled_markets = [
            market_key(market.base_asset_id, market.counter_asset_id)
            for market in session.scalars(select(TrackedMarket).where(TrackedMarket.enabled.is_(True)))
        ]
        accounts_off = (HistoryJob.kind == "account") & (
            TrackedAccount.account_id.is_(None) | TrackedAccount.enabled.is_(False)
        )
        markets_off = (HistoryJob.kind == "market") & (
            HistoryJob.subject.notin_(enabled_markets) if enabled_markets else true()
        )
        rows = session.execute(
            select(HistoryJob.id, HistoryJob.kind)
            .outerjoin(TrackedAccount, TrackedAccount.account_id == HistoryJob.subject)
            .where(
                # Not the paused ones (they are) and not the done ones
                # (there is nothing left to stop).
                HistoryJob.state.notin_(("paused", "done")),
                accounts_off | markets_off,
            )
        ).all()
    ids = {"account": [], "market": []}
    for job_id, kind in rows:
        ids[kind].append(int(job_id))
    paused = 0
    for kind, kind_ids in ids.items():
        if kind_ids:
            history_jobs.pause(engine, ids=kind_ids, because=history_jobs.switched_off(kind))
            paused += len(kind_ids)
    return paused


def make_api(settings: Settings, *, transport: Any = None, engine: Engine | None = None) -> NotifierClient | None:
    """The client of the Notifier's API, or None when none is configured:
    subscriptions are then made by hand and adopted here by their ids."""
    if not settings.notifier_api_url:
        return None
    live.configure(settings.live_budget, engine)
    return NotifierClient(settings.notifier_api_url, token=settings.notifier_api_token, transport=transport)
