"""Keeping the Notifier and this installation in step: every subscription
that is wanted is registered and confirmed, and nothing else of this
installation's is left there.

Wanted comes from the watch lists (:mod:`app.subscriptions.desired`). What
there is at the Notifier is read from its API. Only subscriptions with this
installation's callback address are ever touched: they are the ones it made,
or the ones made for it; subscriptions of others are left alone, even where
they name the same account or asset.

A subscription is registered once. Before creating one, the Notifier's list
is read: a subscription with this installation's address and the same
filters is the one a create would be answered with — the Notifier
identifies subscriptions by owner, address, filters and the version of the
matching rules — so a create that timed out after it was done is found and
taken, not made twice. More than one that fits is a matter for a person:
the subscription is marked ``ambiguous`` and nothing is done to either.

Nothing is taken as active on the word of a create. The subscription is read
back from the Notifier first — it has to be in the Notifier's list with this
installation's address and filters; until then it is ``error`` with "not
confirmed yet", and the next pass looks for it there.

Failures are kept: the row says what went wrong and when it will be tried
again, with a pause that grows. A Notifier that is down, or one that
refuses (its limit of subscriptions per owner is 100 unless the owner
raised it), costs nothing but time: nothing is forgotten.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.notifier.client import (
    NotifierClient,
    NotifierError,
    NotifierRejected,
    NotifierUnavailable,
    remote_id,
    same_subscription,
    subscription_params,
)
from app.storage.locks import AdvisoryLock
from app.storage.models import WatchSubscription
from app.subscriptions import desired
from app.subscriptions.desired import ACCOUNT, ASSET, Want

logger = logging.getLogger(__name__)

WANTED, ACTIVE, ERROR, AMBIGUOUS, RETIRING, MANUAL, RETIRED = (
    "wanted", "active", "error", "ambiguous", "retiring", "manual", "retired",
)
# A subscription in one of these is as good as registered.
CONFIRMED = (ACTIVE, MANUAL)

BASE_PAUSE = timedelta(seconds=30)
MAX_PAUSE = timedelta(hours=1)
# How many subscriptions one pass registers, confirms or retires at most.
PASS_ACTIONS = 50
# One reconciliation at a time per database: the worker's passes and a
# command run by hand. Two at once could both find nothing and both
# register — and the Notifier does not merge two asset subscriptions made
# twice (it identifies subscriptions by the request as it was written, and
# an asset's type is written by the service, not by the request).
ONE_AT_A_TIME = "mtl-horizon:subscriptions"
WAIT_FOR_THE_OTHER = timedelta(seconds=2)


@dataclass
class Report:
    """What a pass of reconciliation found and did."""

    wanted: int = 0
    created: int = 0
    taken: int = 0          # found already there and taken for this installation's
    confirmed: int = 0
    failed: int = 0
    retired: int = 0
    ambiguous: int = 0
    # What could not be done, and why — the rows keep the details.
    problems: list[str] = field(default_factory=list)
    # Set when the Notifier could not be reached at all.
    unreachable: str | None = None

    def lines(self) -> list[str]:
        lines = [f"subscriptions: {self.wanted} wanted, {self.created} created, {self.taken} found there already, "
                 f"{self.confirmed} confirmed, {self.failed} not done, {self.ambiguous} ambiguous, {self.retired} retired"]
        if self.unreachable:
            lines.append(f"  the Notifier could not be reached: {self.unreachable}")
        lines += [f"  {problem}" for problem in self.problems[:10]]
        if len(self.problems) > 10:
            lines.append(f"  … and {len(self.problems) - 10} more")
        return lines


def ensure(engine: Engine, callback_url: str, *, now: datetime | None = None) -> dict[tuple[str, str], Want]:
    """Make sure every wanted subscription has its row, and that a row
    nothing wants any more is on its way out. No word to the Notifier."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        wanted = desired.wanted(session)
        # A row is one for a kind and a key for good; one that was retired is
        # brought back when the subscription is wanted again (an account
        # switched on again): the same row, a fresh start.
        rows = {(row.kind, row.key): row for row in session.scalars(select(WatchSubscription))}
        for key, want in wanted.items():
            row = rows.get(key)
            if row is None:
                session.add(WatchSubscription(kind=want.kind, key=want.key, state=WANTED, next_attempt_at=now))
            elif row.state == RETIRED:
                _revive(row, now)
        for key, row in rows.items():
            if key not in wanted and row.state != RETIRING:
                # Nothing wants it any more. What this installation made is
                # deleted at the Notifier; what it did not make — adopted by
                # hand — is only forgotten here, and left there.
                if row.managed and row.remote_id:
                    row.state, row.next_attempt_at = RETIRING, now
                else:
                    row.state = RETIRED
                    if row.remote_id:
                        row.last_error = "no longer wanted; made by hand, so left at the Notifier — delete it there if it is to go"
        session.flush()
    return wanted


def _revive(row: WatchSubscription, now: datetime) -> None:
    """A retired row back at the start: nothing is kept of what it was, for
    the subscription wanted now is a new one."""
    row.state, row.remote_id, row.managed = WANTED, None, True
    row.attempts, row.last_error, row.confirmed_at = 0, None, None
    row.filter_version, row.next_attempt_at = None, now


def run(
    engine: Engine,
    api: NotifierClient,
    callback_url: str,
    *,
    now: datetime | None = None,
    actions: int = PASS_ACTIONS,
) -> Report:
    """One pass: bring the Notifier and the wanted subscriptions in step, as
    far as ``actions`` registrations or retirements get. One at a time: a
    pass that finds another running says so and does nothing."""
    with AdvisoryLock(engine, ONE_AT_A_TIME) as lock:
        if not lock.acquire(wait=WAIT_FOR_THE_OTHER.total_seconds()):
            report = Report(unreachable="another reconciliation of subscriptions is running; nothing was done in this pass")
            return report
        try:
            return _run(engine, api, callback_url, now=now, actions=actions)
        finally:
            lock.release()


def _run(
    engine: Engine,
    api: NotifierClient,
    callback_url: str,
    *,
    now: datetime | None,
    actions: int,
) -> Report:
    now = now or datetime.now(UTC)
    report = Report()
    wanted = ensure(engine, callback_url, now=now)
    report.wanted = len(wanted)
    due = _due(engine, now, actions)
    if not due:
        return report
    try:
        listing = api.list_subscriptions()
    except NotifierUnavailable as error:
        report.unreachable = str(error)
        _postpone(engine, [row.id for row in due], now, "the Notifier could not be reached", error=str(error))
        return report
    except NotifierError as error:
        report.unreachable = str(error)
        report.problems.append(f"the Notifier's answer could not be read: {error}")
        _postpone(engine, [row.id for row in due], now, "the answer could not be read", error=str(error))
        return report

    for row in due:
        want = wanted.get((row.kind, row.key))
        try:
            if row.state == RETIRING:
                _retire(engine, api, row, now, report)
            elif want is None:
                # Stopped being wanted between the two reads: its turn to go comes in the next pass.
                continue
            elif row.state == WANTED or (row.state == ERROR and row.remote_id is None):
                _register(engine, api, row, want, callback_url, listing, now, report)
            elif row.state == ERROR and row.remote_id:
                _confirm(engine, row, callback_url, listing, now, report)
            elif row.state == ACTIVE:
                pass
        except NotifierUnavailable as error:
            report.failed += 1
            report.problems.append(f"{row.kind} {row.key[:12]}…: {error}")
            _failed(engine, row, now, str(error), retry=True)
        except NotifierRejected as error:
            report.failed += 1
            report.problems.append(f"{row.kind} {row.key[:12]}…: refused, {error.status}: {error.detail}")
            _failed(engine, row, now, f"refused, {error.status}: {error.detail}", retry=True)
        except NotifierError as error:
            report.failed += 1
            report.problems.append(f"{row.kind} {row.key[:12]}…: {error}")
            _failed(engine, row, now, str(error), retry=True)
    return report


def due(engine: Engine, now: datetime | None = None) -> int:
    """How many subscriptions are waiting for their turn with the Notifier."""
    now = now or datetime.now(UTC)
    with Session(engine) as session:
        return session.scalar(
            select(func.count()).select_from(WatchSubscription).where(
                WatchSubscription.state.in_((WANTED, ERROR, RETIRING)), WatchSubscription.next_attempt_at <= now
            )
        ) or 0


def postpone_all(engine: Engine, why: str, *, now: datetime | None = None) -> int:
    """Put off everything that is waiting, and say why: for when the
    Notifier can be reached but must not be talked to yet."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        rows = list(session.scalars(select(WatchSubscription).where(WatchSubscription.state.in_((WANTED, ERROR, RETIRING)))))
        for row in rows:
            row.last_error = why
            row.attempts += 1
            row.next_attempt_at = now + _pause(row.attempts)
    return len(rows)


def _due(engine: Engine, now: datetime, actions: int) -> list[WatchSubscription]:
    """The rows whose turn has come: retiring ones first, then the others."""
    with Session(engine) as session:
        rows = list(
            session.scalars(
                select(WatchSubscription)
                .where(
                    WatchSubscription.state.in_((WANTED, ERROR, RETIRING)),
                    WatchSubscription.next_attempt_at <= now,
                )
                .order_by(WatchSubscription.state != RETIRING, WatchSubscription.next_attempt_at, WatchSubscription.id)
                .limit(actions)
            )
        )
    return rows


def _register(
    engine: Engine,
    api: NotifierClient,
    row: WatchSubscription,
    want: Want,
    callback_url: str,
    listing: list[dict[str, Any]],
    now: datetime,
    report: Report,
) -> None:
    """Register one wanted subscription — or find that it is there."""
    params = subscription_params(want.kind, want.key, callback_url)
    fits = [sub for sub in listing if same_subscription(sub, params)]
    if len(fits) > 1:
        ids = ", ".join(str(remote_id(sub)) for sub in fits)
        with Session(engine) as session, session.begin():
            current = session.get(WatchSubscription, row.id)
            assert current is not None
            current.state, current.last_error, current.attempts = AMBIGUOUS, (
                f"{len(fits)} subscriptions fit ({ids}): a person decides which is this installation's"
            ), current.attempts + 1
        report.ambiguous += 1
        return
    if fits:
        with Session(engine) as session, session.begin():
            current = session.get(WatchSubscription, row.id)
            assert current is not None
            current.remote_id, current.state, current.attempts, current.last_error = str(remote_id(fits[0])), ACTIVE, 0, None
            current.confirmed_at, current.next_attempt_at = now, now
            current.filter_version = fits[0].get("filter_version") or 1
        report.taken += 1
        return
    made = api.create(params)
    assert isinstance(made, Mapping)
    with Session(engine) as session, session.begin():
        current = session.get(WatchSubscription, row.id)
        assert current is not None
        current.remote_id, current.managed, current.state = str(remote_id(made)), True, ERROR
        current.last_error, current.attempts = "registered, not confirmed yet", current.attempts + 1
        # Read back without delay: confirmation is the very next step.
        current.next_attempt_at = now
    report.created += 1


def _confirm(
    engine: Engine, row: WatchSubscription, callback_url: str, listing: list[dict[str, Any]], now: datetime, report: Report
) -> None:
    """Read the subscription back from the Notifier's list: active only if
    it is there with this installation's address and filters."""
    found = next((sub for sub in listing if str(remote_id(sub)) == row.remote_id), None)
    params = subscription_params(row.kind, row.key, callback_url)
    if found is None:
        # Gone, or never there: to be registered again.
        _set(engine, row.id, remote_id=None, state=WANTED, last_error="not found when confirmed: to be registered again", next_attempt_at=now)
    elif not same_subscription(found, params):
        _set(
            engine, row.id, state=AMBIGUOUS,
            last_error=f"subscription {row.remote_id} is not what this installation registers (another address or filters): a person decides",
        )
        report.ambiguous += 1
    else:
        _set(engine, row.id, state=ACTIVE, attempts=0, last_error=None, confirmed_at=now, next_attempt_at=now)
        report.confirmed += 1


def _retire(engine: Engine, api: NotifierClient, row: WatchSubscription, now: datetime, report: Report) -> None:
    try:
        api.delete(row.remote_id or "")
    except NotifierRejected as error:
        if error.status == 404:
            _retired(engine, row.id, now, None)
            report.retired += 1
            return
        if error.status == 403:
            # Not this installation's to delete after all: it is forgotten here, and left there.
            _retired(engine, row.id, now, "could not be deleted: the Notifier refuses (not this installation's)")
            report.retired += 1
            report.problems.append(f"subscription {row.remote_id} ({row.kind} {row.key[:12]}…) could not be deleted: refused; delete it by hand if it is to go")
            return
        raise
    _retired(engine, row.id, now, None)
    report.retired += 1


def _retired(engine: Engine, row_id: int, now: datetime, error: str | None) -> None:
    with Session(engine) as session, session.begin():
        current = session.get(WatchSubscription, row_id)
        assert current is not None
        current.state, current.last_error, current.next_attempt_at = RETIRED, error, now


def _failed(engine: Engine, row: WatchSubscription, row_now: datetime, error: str, *, retry: bool) -> None:
    """An attempt failed: the row says why, and when it will be tried again.
    A retiring row stays on its way out."""
    with Session(engine) as session, session.begin():
        current = session.get(WatchSubscription, row.id)
        assert current is not None
        current.last_error = error
        if retry:
            if current.state != RETIRING:
                current.state = ERROR
            current.attempts += 1
            current.next_attempt_at = row_now + _pause(current.attempts)
        else:
            current.state = RETIRED


def _postpone(engine: Engine, ids: list[int], now: datetime, why: str, *, error: str) -> None:
    with Session(engine) as session, session.begin():
        for row_id in ids:
            current = session.get(WatchSubscription, row_id)
            if current is not None:
                current.last_error = f"{why}: {error}" if error else why
                if current.state != RETIRING:
                    current.state = ERROR
                current.attempts += 1
                current.next_attempt_at = now + _pause(current.attempts)


def _pause(attempts: int) -> timedelta:
    return min(BASE_PAUSE * 2 ** max(0, attempts - 1), MAX_PAUSE)


def _set(engine: Engine, row_id: int, **changes: Any) -> None:
    with Session(engine) as session, session.begin():
        current = session.get(WatchSubscription, row_id)
        assert current is not None
        for name, value in changes.items():
            setattr(current, name, value)


def adopt(
    engine: Engine,
    kind: str,
    key: str,
    remote: str,
    *,
    callback_url: str = "",
    api: NotifierClient | None = None,
    now: datetime | None = None,
) -> int:
    """Take a subscription made by hand at the Notifier as this
    installation's, by its id there. Returns the row's id here.

    With ``api`` it is looked for in the Notifier's list first, and refused
    if it is not there or does not fit what this installation registers.
    What is adopted is never deleted by this installation (``managed`` stays
    False): a person made it, a person retires it.
    """
    now = now or datetime.now(UTC)
    if kind not in (ACCOUNT, ASSET):
        raise ValueError(f"kind is one of {ACCOUNT}, {ASSET}")
    if api is not None:
        found = next((sub for sub in api.list_subscriptions() if str(remote_id(sub)) == remote), None)
        if found is None:
            raise NotifierRejected(f"subscription {remote} is not in the Notifier's list", 404, "not found")
        if callback_url and not same_subscription(found, subscription_params(kind, key, callback_url)):
            raise NotifierRejected(
                f"subscription {remote} does not have the address and filters this installation registers for {kind} {key}",
                409, "another subscription",
            )
    with Session(engine) as session, session.begin():
        row = session.scalar(
            select(WatchSubscription).where(
                WatchSubscription.kind == kind, WatchSubscription.key == key, WatchSubscription.state != RETIRED
            )
        )
        if row is None:
            row = session.scalar(select(WatchSubscription).where(WatchSubscription.kind == kind, WatchSubscription.key == key))
            if row is None:
                row = WatchSubscription(kind=kind, key=key, next_attempt_at=now)
                session.add(row)
            else:
                _revive(row, now)
            session.flush()
        row.remote_id, row.state, row.managed = remote, MANUAL, False
        row.confirmed_at, row.last_error, row.attempts = now, None, 0
        session.flush()
        return int(row.id)


def release(engine: Engine, row_id: int, *, now: datetime | None = None) -> str:
    """Forget a subscription adopted by hand: it stays at the Notifier, and
    from here on it is not this installation's. Nothing is deleted there."""
    now = now or datetime.now(UTC)
    with Session(engine) as session, session.begin():
        row = session.get(WatchSubscription, row_id)
        if row is None:
            raise ValueError(f"no subscription row {row_id}")
        if row.state != MANUAL:
            raise ValueError(f"subscription row {row_id} is {row.state}, not adopted by hand")
        row.state, row.next_attempt_at = RETIRED, now
        return f"forgotten; it stays at the Notifier as {row.remote_id} — delete it there by hand if it is to go"
