"""One pass of discovery: the accounts that hold an asset, read from Horizon
page by page, and brought into the watch list.

The accounts are read with ``GET /accounts?asset=CODE:ISSUER``, in order of
address, through the pool like everything else. What counts is the line, not
the balance: an account that holds nothing of the asset still has its
trustline, and is a holder. Each record is checked for the line before it is
counted: the answer is not taken on trust.

A pass is not one snapshot of the network. It takes time, and a server may
be exchanged for another on the way, each with its own latest ledger. So
what is kept of a pass says which server answered each page and when, and a
pass that did not get to the end says so (``incomplete``) instead of looking
whole. Running a pass again is safe: an account is added once, and one that
is watched already is left as it is — switched off by hand or not.

The issuer is not added. Issuing an asset is not holding it, and the
issuer's own line to its asset is in every answer.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.sources.horizon import HorizonClient
from app.storage.models import DiscoveryRun, TrackedAccount, TrackedAsset
from app.watch import accounts as watched

logger = logging.getLogger(__name__)

PAGE = 200
# A pass stops here, whatever it has read: the rest is read by the next one,
# which goes on from where this one stopped.
DEFAULT_MAX_REQUESTS = 1000

RUNNING, COMPLETE, INCOMPLETE, FAILED = "running", "complete", "incomplete", "failed"
DRY_RUN = "dry-run"

NEW, ENABLED, DISABLED = "new", "enabled", "disabled"


class _Budget(Exception):
    """The pass used up the requests it was given."""


class _Stopped(Exception):
    """Somebody asked the pass to stop: it ends between two requests."""


@dataclass
class ScanReport:
    """What a pass found and did, and how far it got."""

    asset_id: str
    run_id: int | None = None
    # complete | incomplete | dry-run
    state: str = DRY_RUN
    dry_run: bool = False
    pages: int = 0
    requests: int = 0
    added: list[str] = field(default_factory=list)
    already: list[str] = field(default_factory=list)
    disabled: list[str] = field(default_factory=list)
    without_line: list[str] = field(default_factory=list)
    issuers: int = 0
    # What each page came from.
    servers: list[dict[str, Any]] = field(default_factory=list)
    last_address: str | None = None
    after_address: str | None = None
    stopped_because: str | None = None
    seconds: float = 0.0

    @property
    def found(self) -> int:
        """The accounts read, the issuer and those without a line apart."""
        return len(self.added) + len(self.already) + len(self.disabled) + len(self.without_line)

    @property
    def latest_ledger(self) -> int | None:
        known = [entry["latest_ledger"] for entry in self.servers if entry["latest_ledger"] is not None]
        return max(known) if known else None

    def as_dict(self) -> dict[str, Any]:
        return {
            "asset": self.asset_id,
            "run": self.run_id,
            "state": self.state,
            "dry_run": self.dry_run,
            "pages": self.pages,
            "requests": self.requests,
            "found": self.found,
            "with_line": len(self.added) + len(self.already) + len(self.disabled),
            "added": len(self.added),
            "already_enabled": len(self.already),
            "disabled": len(self.disabled),
            "without_line": len(self.without_line),
            "issuers_skipped": self.issuers,
            "added_accounts": self.added,
            "servers": self.servers,
            "latest_ledger": self.latest_ledger,
            "after_address": self.after_address,
            "last_address": self.last_address,
            "stopped_because": self.stopped_because,
            "seconds": round(self.seconds, 1),
        }

    def lines(self) -> list[str]:
        head = "dry run" if self.dry_run else f"run {self.run_id}"
        lines = [f"{head} over {self.asset_id}: {self.state}"]
        lines.append(
            f"  {self.found} account(s) read on {self.pages} page(s) with {self.requests} request(s) in {self.seconds:.1f}s"
        )
        lines.append(
            f"  {len(self.added)} new, {len(self.already)} enabled already, {len(self.disabled)} disabled and left so, "
            f"{len(self.without_line)} without a line for the asset, {self.issuers} the issuer"
        )
        if self.servers:
            names = ", ".join(sorted({str(entry["server"]) for entry in self.servers}))
            lines.append(
                f"  answered by {names}; the newest ledger they had: {self.latest_ledger or '-'}; "
                f"last address {self.last_address or '-'}"
            )
        if self.stopped_because:
            lines.append(f"  stopped: {self.stopped_because}")
            if not self.dry_run:
                lines.append("  the pass did not reach the end: `run --resume` goes on from where it stopped")
        return lines


def asset_parts(asset_id: str) -> tuple[str | None, str | None]:
    """(code, issuer) of an asset id; (None, None) for native."""
    if asset_id == "native":
        return None, None
    code, _, issuer = asset_id.partition(":")
    return code, issuer


def holds(record: Mapping[str, Any], asset_id: str) -> bool:
    """Whether an account record has a line for the asset. The line is what
    is looked for, at any balance: zero is a line too."""
    code, issuer = asset_parts(asset_id)
    balances = record.get("balances")
    if not isinstance(balances, list):
        return False
    for line in balances:
        if not isinstance(line, Mapping):
            continue
        if code is None:
            if line.get("asset_type") == "native":
                return True
        elif line.get("asset_code") == code and line.get("asset_issuer") == issuer:
            return True
    return False


def discovery_assets(engine: Engine) -> list[str]:
    """The assets whose holders are to be found, by id."""
    with Session(engine) as session:
        return list(
            session.scalars(
                select(TrackedAsset.asset_id).where(TrackedAsset.discover.is_(True)).order_by(TrackedAsset.asset_id)
            )
        )


def _in_watch_list(engine: Engine, addresses: list[str]) -> dict[str, str]:
    """What each account is in the watch list: new, enabled or disabled."""
    if not addresses:
        return {}
    with Session(engine) as session:
        tracked = {
            row.account_id: row.enabled
            for row in session.execute(select(TrackedAccount.account_id, TrackedAccount.enabled).where(TrackedAccount.account_id.in_(addresses)))
        }
    return {address: (NEW if address not in tracked else ENABLED if tracked[address] else DISABLED) for address in addresses}


def _latest_known(client: HorizonClient, served_by: str | None) -> int | None:
    """The newest ledger the server that answered is known to have."""
    for server in getattr(client, "servers", ()) or ():
        if server.name == served_by:
            return server.latest
    return None


def scan(
    client: HorizonClient,
    engine: Engine,
    asset_id: str,
    *,
    dry_run: bool = False,
    after_address: str | None = None,
    max_requests: int = DEFAULT_MAX_REQUESTS,
    clock: Callable[[], datetime] | None = None,
    should_stop: Callable[[], bool] | None = None,
    on_page: Callable[[ScanReport], None] | None = None,
) -> ScanReport:
    """Read the holders of ``asset_id`` and add the new ones to the watch
    list; say what was found and how far the pass got.

    With ``dry_run`` nothing is written anywhere: not a row of a run, not an
    account. The pass begins after ``after_address`` — where an earlier pass
    stopped — and stops itself at ``max_requests``. ``should_stop`` is asked
    before every request: True ends the pass where it stands, as one that
    did not get to the end (a page that was taken is kept: it is stored
    whole). ``on_page`` hears of the report after every page — for whoever
    shows how far the pass has got, a dry run included.
    """
    if asset_id == "native":
        raise ValueError("native cannot be discovered: every account of the network holds it")
    now = clock or (lambda: datetime.now(UTC))
    started = time.monotonic()
    report = ScanReport(asset_id=asset_id, dry_run=dry_run, after_address=after_address)
    _, issuer = asset_parts(asset_id)
    run_id: int | None = None
    if not dry_run:
        with Session(engine) as session, session.begin():
            run = DiscoveryRun(
                asset_id=asset_id, state=RUNNING, dry_run=False, after_address=after_address, started_at=now()
            )
            session.add(run)
            session.flush()
            run_id = report.run_id = run.id

    inner = client.before_request
    first = client.stats.requests

    def before_request() -> None:
        if inner is not None:
            inner()
        if client.stats.requests - first >= max_requests:
            raise _Budget()
        if should_stop is not None and should_stop():
            raise _Stopped()

    client.before_request = before_request
    try:
        for page in client.address_pages("/accounts", asset=asset_id, cursor=after_address, limit=PAGE):
            report.pages += 1
            report.servers.append(
                {
                    "page": report.pages,
                    "server": page.served_by,
                    "at": now().isoformat(timespec="seconds"),
                    "latest_ledger": _latest_known(client, page.served_by),
                    "records": len(page.records),
                }
            )
            holders: list[str] = []
            for record in page.records:
                address = str(record.get("account_id") or "")
                if address == issuer:
                    report.issuers += 1
                elif not holds(record, asset_id):
                    report.without_line.append(address)
                else:
                    holders.append(address)
            if page.records:
                report.last_address = str(page.records[-1].get("account_id"))
            known = _in_watch_list(engine, holders)
            for address, outcome in known.items():
                if outcome == ENABLED:
                    report.already.append(address)
                elif outcome == DISABLED:
                    report.disabled.append(address)
            new = [address for address, outcome in known.items() if outcome == NEW]
            if not dry_run:
                _keep(engine, run_id, asset_id, holders, new, report, now())
            report.added.extend(new)
            if on_page is not None:
                on_page(report)
    except _Budget:
        report.state = DRY_RUN if dry_run else INCOMPLETE
        report.stopped_because = f"the pass used its {max_requests} requests"
    except _Stopped:
        report.state = DRY_RUN if dry_run else INCOMPLETE
        report.stopped_because = "stopped on request"
    except Exception as error:  # noqa: BLE001 - the pass did not get to the end: that is said, not hidden
        report.state = DRY_RUN if dry_run else INCOMPLETE
        report.stopped_because = f"{type(error).__name__}: {error}"
        logger.warning("discovery of %s stopped: %s", asset_id, report.stopped_because)
    else:
        report.state = DRY_RUN if dry_run else COMPLETE
    finally:
        client.before_request = inner
        report.requests = client.stats.requests - first
        report.seconds = time.monotonic() - started
    if run_id is not None:
        _finish(engine, run_id, report, now())
    if report.stopped_because is None:
        logger.info("discovery of %s: %s", asset_id, report.lines()[1])
    return report


def _keep(
    engine: Engine,
    run_id: int,
    asset_id: str,
    holders: list[str],
    new: list[str],
    report: ScanReport,
    at: datetime,
) -> None:
    """Add what the page found and note how far the pass got. One transaction
    a page: a pass that dies leaves an honest record of itself behind."""
    with Session(engine) as session, session.begin():
        for address in new:
            watched.add_account(
                session,
                address,
                reason=f"discovered: holds {asset_id} (run {run_id}, {at.strftime('%Y-%m-%d')})",
                origin=watched.DISCOVERY,
                discovered_by=[asset_id],
            )
        # An account watched already keeps its flag; where it was found is kept.
        for address in holders:
            if address not in new:
                watched.add_account(
                    session, address, reason="", origin=watched.DISCOVERY, discovered_by=[asset_id]
                )
        run = session.get(DiscoveryRun, run_id)
        assert run is not None
        run.pages, run.last_address, run.servers = report.pages, report.last_address, report.servers
        run.found = report.found
        run.with_line = len(report.added) + len(report.already) + len(report.disabled)
        run.added, run.already, run.disabled = len(report.added), len(report.already), len(report.disabled)
        run.issuer_skipped = report.issuers
        run.latest_ledger = report.latest_ledger


def _finish(engine: Engine, run_id: int, report: ScanReport, at: datetime) -> None:
    with Session(engine) as session, session.begin():
        run = session.get(DiscoveryRun, run_id)
        assert run is not None
        run.state = COMPLETE if report.state == COMPLETE else INCOMPLETE
        run.finished_at = at
        run.last_error = report.stopped_because


def stopped_at(engine: Engine, asset_id: str) -> str | None:
    """Where the last pass over the asset stopped, when that pass did not
    finish: None when it did (a pass that is to go on from an old place
    would read what a whole pass after it has read already)."""
    with Session(engine) as session:
        run = session.scalars(
            select(DiscoveryRun)
            .where(DiscoveryRun.asset_id == asset_id, DiscoveryRun.dry_run.is_(False))
            .order_by(DiscoveryRun.started_at.desc(), DiscoveryRun.id.desc())
            .limit(1)
        ).first()
    if run is None or run.state != INCOMPLETE:
        return None
    return run.last_address or run.after_address


def run_all(
    client: HorizonClient,
    engine: Engine,
    assets: list[str] | None = None,
    *,
    dry_run: bool = False,
    resume: bool = False,
    max_requests: int = DEFAULT_MAX_REQUESTS,
    clock: Callable[[], datetime] | None = None,
) -> list[ScanReport]:
    """A pass over every asset marked for discovery (or the ones named)."""
    reports = []
    for asset_id in assets if assets is not None else discovery_assets(engine):
        after = stopped_at(engine, asset_id) if resume else None
        reports.append(
            scan(client, engine, asset_id, dry_run=dry_run, after_address=after, max_requests=max_requests, clock=clock)
        )
    return reports
