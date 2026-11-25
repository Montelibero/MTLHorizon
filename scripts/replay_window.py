"""Replay a stored window as if it were the source, to measure what reading it costs.

A database loaded with whole contexts keeps the raw records of everything
its streams answered. Served back under Horizon's paging rules, they are a
source that costs no request: the same backfill can be run again in another
mode — with parents only (``parents``) — and the two loads compared record by
record. Running ``full`` again reproduces the first load, which shows the
replay is faithful.

    python scripts/replay_window.py run     --baseline URL --scratch NAME --mode parents \\
                                             --from-ledger L1 --to-ledger L0 --elder E --report out.json
    python scripts/replay_window.py compare --baseline URL --full NAME --parents NAME \\
                                             --from-ledger L1 --to-ledger L0 --reports full.json parents.json \\
                                             --output compare.json

``run`` drops and recreates the scratch database. Nothing is asked of any
Horizon: the addresses come from the baseline's watch list, the records from
what the baseline stored.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit

import httpx
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).resolve().parents[1]

ORIGIN = "https://replay.test"
SOURCE = "horizon:replay"
NETWORK = "Public Global Stellar Network ; September 2015"
# A pair cursor given as one number means that operation and any order.
MAX_ORDER = 2**31
PAIR_KINDS = ("effect", "trade")
STREAMS = {"transactions": "transaction", "operations": "operation", "effects": "effect", "trades": "trade"}


class Problem(Exception):
    def __init__(self, status: int) -> None:
        self.status = status


def _parts() -> dict[str, list]:
    return {"operations": [], "effects": [], "trades": []}


def key_of(record: Mapping[str, Any], kind: str) -> tuple[int, ...]:
    first, _, second = str(record["paging_token"]).partition("-")
    return (int(first), int(second)) if second else (int(first), 0)


def position_of(cursor: str, kind: str) -> tuple[int, ...]:
    """Sort key of a cursor, of the shape ``key_of`` gives a record."""
    first, _, second = cursor.partition("-")
    if kind in PAIR_KINDS:
        return (int(first), int(second) if second else MAX_ORDER)
    return (int(first), 0)


def beyond(key: tuple[int, ...], position: tuple[int, ...], order: str) -> bool:
    return key > position if order == "asc" else key < position


def asset_id(record: Mapping[str, Any], side: str) -> str:
    if record.get(f"{side}asset_type") == "native":
        return "native"
    return f"{record[f'{side}asset_code']}:{record[f'{side}asset_issuer']}"


def turned(record: Mapping[str, Any]) -> dict[str, Any]:
    """The same trade with base and counter exchanged."""
    other = dict(record)
    for name in ("asset_type", "asset_code", "asset_issuer", "amount", "account", "liquidity_pool_id", "offer_id"):
        base, counter = f"base_{name}", f"counter_{name}"
        for target, source in ((base, counter), (counter, base)):
            other[target] = record.get(source)
            if other[target] is None:
                del other[target]
    other["base_is_seller"] = not record["base_is_seller"]
    other["price"] = {"n": record["price"]["d"], "d": record["price"]["n"]}
    return other


def requested_asset(query: Mapping[str, str], prefix: str) -> str | None:
    kind = query.get(f"{prefix}asset_type")
    if kind is None:
        return None
    if kind == "native":
        return "native"
    return f"{query[f'{prefix}asset_code']}:{query[f'{prefix}asset_issuer']}"


def iso(moment: datetime | None) -> str | None:
    return None if moment is None else moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class Replay:
    """Answers like Horizon would have, from what a load stored before.

    Every record is served as it was stored, in the orientation it was first
    met in; a market asked for the other way round gets it turned around.
    """

    def __init__(self, baseline: Engine, first: int, last: int, elder: int, latest: int) -> None:
        self.elder, self.latest = elder, latest
        self.requests: Counter[str] = Counter()
        with baseline.connect() as c:
            window = {"a": first, "b": last}
            transactions = [raw for (raw,) in c.execute(text("SELECT raw FROM history_transactions WHERE ledger_sequence BETWEEN :a AND :b"), window)]
            operations = {int(id_): raw for id_, raw in c.execute(text("SELECT id, raw FROM history_operations"))}
            effects = [raw for (raw,) in c.execute(text("SELECT raw FROM history_effects"))]
            trades = [raw for (raw,) in c.execute(text("SELECT raw FROM history_trades"))]
            tx_participants = c.execute(text("SELECT transaction_id, account_id FROM history_transaction_participants")).all()
            op_participants = c.execute(text("SELECT operation_id, account_id FROM history_operation_participants")).all()
            closed = dict(c.execute(text("SELECT sequence, close_time FROM history_ledgers")).all())
            self.accounts = [account for (account,) in c.execute(text("SELECT account_id FROM tracked_accounts WHERE mode = 'FULL'"))]
        self.closed = closed

        self.by_hash: dict[str, dict[str, Any]] = {}
        self.by_id: dict[int, dict[str, Any]] = {}
        for record in transactions:
            self.by_hash[record["hash"]] = record
            self.by_id[int(record["paging_token"])] = record
            inner = record.get("inner_transaction")
            if isinstance(inner, dict) and inner.get("hash"):
                self.by_hash[inner["hash"]] = record
        self.operations = operations

        own: dict[int, dict[str, list]] = {transaction_id: _parts() for transaction_id in self.by_id}
        for record in operations.values():
            own.setdefault(int(record["paging_token"]) & ~0xFFF, _parts())["operations"].append(record)
        for record in effects:
            own.setdefault(int(record["paging_token"].split("-", 1)[0]) & ~0xFFF, _parts())["effects"].append(record)
        for record in trades:
            own.setdefault(int(record["id"].split("-", 1)[0]) & ~0xFFF, _parts())["trades"].append(record)
        self.of_transaction = {self.by_id[transaction_id]["hash"]: parts for transaction_id, parts in own.items() if transaction_id in self.by_id}

        streams: dict[str, dict[str, list]] = {account: {**_parts(), "transactions": []} for account in self.accounts}
        for transaction_id, account in tx_participants:
            if account in streams:
                streams[account]["transactions"].append(self.by_id[transaction_id])
        for operation_id, account in op_participants:
            if account in streams:
                streams[account]["operations"].append(operations[operation_id])
        for record in effects:
            if record.get("account") in streams:
                streams[record["account"]]["effects"].append(record)
        for record in trades:
            for side in ("base_account", "counter_account"):
                if record.get(side) in streams:
                    streams[record[side]]["trades"].append(record)
        self.streams = streams
        self.trades = trades

    # -- the server ----------------------------------------------------------

    def client(self):
        from app.sources.horizon import HorizonClient, HorizonSettings

        settings = HorizonSettings(ORIGIN, requests_per_second=1000, max_attempts=4, backoff_base_seconds=0.0, backoff_cap_seconds=0.0)
        return HorizonClient(settings, transport=httpx.MockTransport(self.handle), sleeper=lambda seconds: None)

    def handle(self, request: httpx.Request) -> httpx.Response:
        url = urlsplit(str(request.url))
        query = dict(parse_qsl(url.query))
        path = url.path
        parts = [part for part in path.split("/") if part]
        self.requests[self.kind(path, parts, query)] += 1
        try:
            answer = self.answer(parts, query)
        except Problem as problem:
            return httpx.Response(problem.status, json={"status": problem.status, "title": "replay"})
        if isinstance(answer, dict):
            return httpx.Response(200, headers={"content-type": "application/hal+json"}, json=answer)
        kind, records = answer
        return self.page(path, query, kind, records)

    @staticmethod
    def kind(path: str, parts: list[str], query: Mapping[str, str]) -> str:
        if not parts:
            return "root"
        if parts[0] == "accounts":
            return f"page {parts[2]}"
        if parts[0] == "trades":
            return "page market trades" if "base_asset_type" in query else "trades of a transaction"
        if parts[0] == "transactions":
            return f"{parts[2]} of a transaction" if len(parts) == 3 else "transaction"
        if parts[0] == "operations" and len(parts) == 2:
            return "operation"
        if parts[0] == "ledgers":
            return "ledger"
        return "other"

    def answer(self, parts: list[str], query: dict[str, str]) -> Any:
        if not parts:
            return {
                "horizon_version": "replay",
                "network_passphrase": NETWORK,
                "history_latest_ledger": self.latest,
                "history_elder_ledger": self.elder,
                "history_latest_ledger_closed_at": iso(self.closed.get(self.latest)),
                "ingest_latest_ledger": self.latest,
                "core_latest_ledger": self.latest,
            }
        if parts[0] == "ledgers" and len(parts) == 2:
            sequence = int(parts[1])
            if sequence not in self.closed:
                raise Problem(404)
            return {"sequence": sequence, "paging_token": str(sequence << 32), "closed_at": iso(self.closed[sequence])}
        if parts[0] == "accounts" and len(parts) == 3 and parts[2] in STREAMS:
            stream = self.streams.get(parts[1], {}).get(parts[2], [])
            if query.get("include_failed") != "true":
                stream = [r for r in stream if r.get("successful", r.get("transaction_successful", True))]
            return STREAMS[parts[2]], list(stream)
        if parts[0] == "trades" and len(parts) == 1:
            base, counter = requested_asset(query, "base_"), requested_asset(query, "counter_")
            if base is None:
                return "trade", self.trades
            chosen = []
            for record in self.trades:
                if {asset_id(record, "base_"), asset_id(record, "counter_")} != {base, counter}:
                    continue
                chosen.append(record if asset_id(record, "base_") == base else turned(record))
            return "trade", chosen
        if parts[0] == "transactions" and len(parts) == 2:
            record = self.by_hash.get(parts[1])
            if record is None:
                raise Problem(404)
            if record["ledger"] < self.elder:
                raise Problem(410)
            return record
        if parts[0] == "transactions" and len(parts) == 3 and parts[2] in ("operations", "effects"):
            record = self.by_hash.get(parts[1])
            if record is None:
                raise Problem(404)
            return parts[2].removesuffix("s"), list(self.of_transaction.get(record["hash"], {}).get(parts[2], []))
        if parts[0] == "operations" and len(parts) == 2:
            record = self.operations.get(int(parts[1]))
            if record is None:
                raise Problem(404)
            return record
        raise Problem(404)

    def page(self, path: str, query: dict[str, str], kind: str, records: Sequence[Mapping[str, Any]]) -> httpx.Response:
        """A page as Horizon serves it: ordered by cursor, strictly beyond the
        cursor, with a ``next`` link on every page."""
        order = query.get("order", "asc")
        limit = int(query.get("limit", 10))
        cursor = query.get("cursor", "")
        view = sorted(records, key=lambda record: key_of(record, kind))
        if cursor:
            position = position_of(cursor, kind)
            if position[0] <= self.elder << 32:  # below the history kept
                if order == "desc":
                    raise Problem(410)
                position = (self.elder << 32, MAX_ORDER) if kind in PAIR_KINDS else (self.elder << 32,)
            view = [record for record in view if beyond(key_of(record, kind), position, order)]
        if order == "desc":
            view = view[::-1]
        page_records = view[:limit]
        # An empty page repeats the cursor asked for, as the source does.
        following = {**query, "cursor": page_records[-1]["paging_token"] if page_records else cursor}
        body = {
            "_embedded": {"records": page_records},
            "_links": {
                "self": {"href": ORIGIN + path + "?" + urlencode(query)},
                "next": {"href": ORIGIN + path + "?" + urlencode(following)},
            },
        }
        return httpx.Response(200, headers={"content-type": "application/hal+json"}, json=body)


# -- running the backfill against the replay ------------------------------------


def alembic_config() -> Config:
    config = Config(PROJECT_ROOT / "alembic.ini")
    config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    config.attributes["configure_logger"] = False
    return config


def run(args: argparse.Namespace) -> int:
    from app.backfill.jobs import stream_jobs
    from app.backfill.loader import FULL, PARENTS, Backfill, Window

    baseline = create_engine(args.baseline)
    control = create_engine(baseline.url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    with control.connect() as connection:
        connection.execute(text(f'DROP DATABASE IF EXISTS "{args.scratch}" WITH (FORCE)'))
        connection.execute(text(f'CREATE DATABASE "{args.scratch}"'))
    control.dispose()
    scratch_url = baseline.url.set(database=args.scratch).render_as_string(hide_password=False)
    os.environ["MTL_HORIZON_DATABASE_URL"] = scratch_url
    command.upgrade(alembic_config(), "head")
    scratch = create_engine(scratch_url)
    with baseline.connect() as source, scratch.begin() as target:
        # The watch list refers to accounts and assets: they come first.
        for table in ("history_assets", "history_accounts", "tracked_assets", "tracked_accounts", "tracked_markets"):
            rows = [dict(row) for row in source.execute(text(f"SELECT * FROM {table}")).mappings()]
            if rows:
                columns = ", ".join(rows[0])
                values = ", ".join(f":{name}" for name in rows[0])
                target.execute(text(f"INSERT INTO {table} ({columns}) VALUES ({values})"), rows)

    replay = Replay(baseline, args.from_ledger, args.to_ledger, args.elder, args.to_ledger + 100)
    mode = PARENTS if args.mode == "parents" else FULL
    with Session(scratch) as session:
        jobs, _ = stream_jobs(session)
    before = resource.getrusage(resource.RUSAGE_SELF)
    started = time.monotonic()
    with replay.client() as client, Backfill(client, scratch, source=SOURCE, page_size=200, context=mode) as backfill:
        reports = backfill.run(Window(args.from_ledger, args.to_ledger), jobs, concurrency=1)
    seconds, usage = time.monotonic() - started, resource.getrusage(resource.RUSAGE_SELF)
    tables = (
        "history_transactions", "history_operations", "history_effects", "history_trades",
        "history_transaction_participants", "history_operation_participants", "history_accounts", "history_assets", "history_ledgers",
    )
    report = {
        "mode": args.mode,
        "window": [args.from_ledger, args.to_ledger],
        "requests": sum(replay.requests.values()),
        "requests_by_kind": dict(sorted(replay.requests.items())),
        "seconds": round(seconds, 2),
        "cpu_seconds": round(usage.ru_utime + usage.ru_stime - before.ru_utime - before.ru_stime, 2),
        "max_rss_mb": round(usage.ru_maxrss / 1024, 1),
        "pages": backfill.limits.pages,
        "database": backfill.writer.as_dict(),
        "contexts": {"fetched": backfill.contexts.fetched, "reused": backfill.contexts.reused},
        "rows": counts(scratch, tables),
        "bytes": sizes(scratch),
        "streams": [report_.as_dict() for report_ in reports],
    }
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(json.dumps(report, indent=1, default=str) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key not in ("streams", "database", "bytes")}, indent=1))
    scratch.dispose()
    baseline.dispose()
    return 0 if all(report_.status == "verified" for report_ in reports) else 1


# -- comparing the loads --------------------------------------------------------

SET_OF = {
    "transactions": (
        "SELECT t.id::text FROM history_transactions t JOIN history_transaction_participants p ON p.transaction_id = t.id "
        "WHERE p.account_id = :a AND t.ledger_sequence BETWEEN :first AND :last"
    ),
    "operations": (
        "SELECT o.id::text FROM history_operations o JOIN history_operation_participants p ON p.operation_id = o.id "
        "WHERE p.account_id = :a AND o.id >> 32 BETWEEN :first AND :last"
    ),
    "effects": "SELECT e.id FROM history_effects e WHERE e.account_id = :a AND e.operation_id >> 32 BETWEEN :first AND :last",
    "trades": (
        "SELECT tr.id FROM history_trades tr WHERE (tr.base_account_id = :a OR tr.counter_account_id = :a) "
        "AND tr.operation_id >> 32 BETWEEN :first AND :last"
    ),
}
MARKET_SET = (
    "SELECT id FROM history_trades WHERE (base_asset_id = :m AND counter_asset_id = :n "
    "OR base_asset_id = :n AND counter_asset_id = :m) AND operation_id >> 32 BETWEEN :first AND :last"
)
# table, key column, what the digest leaves out beyond the bookkeeping columns
ROWS = {
    "transactions": ("history_transactions", "id::text", " - 'context_loaded'"),
    "operations": ("history_operations", "id::text", ""),
    "effects": ("history_effects", "id", ""),
    "trades": ("history_trades", "id", ""),
}


def mandatory_sets(engine: Engine, first: int, last: int) -> dict[str, tuple[str, list[str]]]:
    """The records every load of the watch list must have: per account its
    four streams, and the market's trades — each as (kind, ids)."""
    found: dict[str, tuple[str, list[str]]] = {}
    with engine.connect() as connection:
        accounts = [a for (a,) in connection.execute(text("SELECT account_id FROM tracked_accounts WHERE mode = 'FULL'"))]
        (base, counter), = connection.execute(text("SELECT base_asset_id, counter_asset_id FROM tracked_markets")).all()
        for account in accounts:
            for stream, statement in SET_OF.items():
                ids = connection.execute(text(statement), {"a": account, "first": first, "last": last}).scalars().all()
                found[f"{account[:6]}… {stream}"] = (stream, sorted(ids))
        ids = connection.execute(text(MARKET_SET), {"m": base, "n": counter, "first": first, "last": last}).scalars().all()
        found["market trades"] = ("trades", sorted(ids))
    return found


def field_digest(connection, stream: str, ids: Sequence[str]) -> str:
    """One digest over the fields of the given records: what was stored,
    without the bookkeeping columns, the raw JSON and the context flag."""
    if not ids:
        return ""
    table, key, leave = ROWS[stream]
    return connection.execute(
        text(
            f"SELECT md5(COALESCE(string_agg(md5((to_jsonb(t) - 'raw' - 'created_at' - 'updated_at'{leave})::text), '' "
            f"ORDER BY {key}), '')) FROM {table} t WHERE {key} = ANY(:ids)"
        ),
        {"ids": list(ids)},
    ).scalar()


def field_digests(engine: Engine, first: int, last: int) -> dict[str, str]:
    with engine.connect() as connection:
        return {
            name: field_digest(connection, stream, ids)
            for name, (stream, ids) in mandatory_sets(engine, first, last).items()
        }


def sizes(engine: Engine, only: Sequence[str] | None = None) -> dict[str, int]:
    """Bytes of each table with its indexes and TOAST."""
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT c.relname, pg_total_relation_size(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'public' AND c.relkind = 'r' ORDER BY 1"
            )
        ).all()
    return {name: size for name, size in rows if only is None or name in only}


def counts(engine: Engine, tables: Sequence[str]) -> dict[str, int]:
    with engine.connect() as connection:
        return {table: connection.scalar(text(f"SELECT count(*) FROM {table}")) for table in tables}


def compare(args: argparse.Namespace) -> int:
    from app.backfill.loader import Window
    from app.backfill.verify import fingerprint

    first, last = args.from_ledger, args.to_ledger
    engines = {
        "baseline": create_engine(args.baseline),
        "full": create_engine(args.full),
        "parents": create_engine(args.parents),
    }
    sets = {name: mandatory_sets(engine, first, last) for name, engine in engines.items()}
    fields = {name: field_digests(engine, first, last) for name, engine in engines.items()}
    prints = {name: fingerprint(engine, Window(first, last)) for name, engine in engines.items()}
    bytes_ = {name: sizes(engine) for name, engine in engines.items()}
    reports = {mode: json.loads(Path(path).read_text()) for mode, path in (("full", args.reports[0]), ("parents", args.reports[1]))}

    names = list(sets["baseline"])
    rows: dict[str, dict[str, Any]] = {}
    print(f"window [{first}, {last}]: the records every load must have")
    print(f"  {'set':24s} {'records':>8s}  full ids  parents ids  full fields  parents fields")
    same = True
    for name in names:
        ids = {load: sets[load][name][1] for load in engines}
        digests = {load: fields[load][name] for load in engines}
        row = {
            "records": len(ids["baseline"]),
            "full_ids": ids["full"] == ids["baseline"],
            "parents_ids": ids["parents"] == ids["baseline"],
            "full_fields": digests["full"] == digests["baseline"],
            "parents_fields": digests["parents"] == digests["baseline"],
            "missing_in_parents": sorted(set(ids["baseline"]) - set(ids["parents"]))[:5],
            "extra_in_parents": sorted(set(ids["parents"]) - set(ids["baseline"]))[:5],
        }
        same = same and all(row[key] for key in ("full_ids", "parents_ids", "full_fields", "parents_fields"))
        print(f"  {name:24s} {row['records']:8d}  {'yes' if row['full_ids'] else 'NO':8s}  {'yes' if row['parents_ids'] else 'NO':11s}  "
              f"{'yes' if row['full_fields'] else 'NO':11s}  {'yes' if row['parents_fields'] else 'NO':14s}")
        rows[name] = row

    print("\nrequests to the source, replayed")
    for mode in ("full", "parents"):
        print(f"  {mode:8s} {reports[mode]['requests']:6d}  {reports[mode]['requests_by_kind']}")
    print(f"  parents asks {reports['parents']['requests'] / reports['full']['requests']:.0%} of what full asks")

    print("\nrows in the window (all tables the fingerprint covers)")
    for table in prints["baseline"]:
        print(f"  {table:28s} " + "  ".join(f"{load}={prints[load][table]['rows']:6d}" for load in engines)
              + ("  same rows" if prints["full"][table] == prints["baseline"][table] else ""))

    print("\nbytes with indexes")
    for load in engines:
        print(f"  {load:9s} {sum(bytes_[load].values()) / 1e6:8.2f} MB")

    out = {
        "window": [first, last],
        "sets": rows,
        "requests": {mode: reports[mode]["requests_by_kind"] for mode in reports},
        "seconds": {mode: reports[mode]["seconds"] for mode in reports},
        "cpu_seconds": {mode: reports[mode]["cpu_seconds"] for mode in reports},
        "max_rss_mb": {mode: reports[mode]["max_rss_mb"] for mode in reports},
        "fingerprints": prints,
        "bytes": bytes_,
        "all_mandatory_records_alike": same,
        "full_reproduces_baseline": prints["full"] == prints["baseline"],
    }
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(out, indent=1) + "\n")
    for engine in engines.values():
        engine.dispose()
    print(f"\nfull reproduces the baseline: {out['full_reproduces_baseline']}")
    print(f"every record every load must have is alike in fields: {same}")
    return 0 if same else 1


# -- the command line -----------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts/replay_window.py", description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)

    sub = commands.add_parser("run", help="load the window from the replay into a scratch database")
    sub.set_defaults(handler=run)
    sub.add_argument("--baseline", required=True, help="database that holds the window loaded with whole contexts")
    sub.add_argument("--scratch", required=True, help="database to drop, recreate and load")
    sub.add_argument("--mode", choices=("full", "parents"), required=True)
    sub.add_argument("--from-ledger", type=int, required=True)
    sub.add_argument("--to-ledger", type=int, required=True)
    sub.add_argument("--elder", type=int, required=True, help="oldest ledger the source kept when the baseline was loaded")
    sub.add_argument("--report", required=True)

    sub = commands.add_parser("compare", help="compare the baseline with two loads made from the replay")
    sub.set_defaults(handler=compare)
    sub.add_argument("--baseline", required=True)
    sub.add_argument("--full", required=True)
    sub.add_argument("--parents", required=True)
    sub.add_argument("--from-ledger", type=int, required=True)
    sub.add_argument("--to-ledger", type=int, required=True)
    sub.add_argument("--reports", nargs=2, required=True, metavar=("FULL_JSON", "PARENTS_JSON"))
    sub.add_argument("--output", required=True)

    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
