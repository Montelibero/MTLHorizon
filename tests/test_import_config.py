"""`import-config`: the watch list file, through the command line, on PostgreSQL."""

from __future__ import annotations

import contextlib
import io
import pathlib

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.backfill.cli import main
from app.storage.models import HistoryAccount, HistoryAsset, TrackedAccount, TrackedAsset, TrackedMarket
from tests.fake_horizon import ISSUER, A, B, C

GOOD = f"""
assets:
  - code: EURMTL
    issuer: {ISSUER}
  - code: MTLRECT
    issuer: {ISSUER}
markets:
  - base: MTLRECT:{ISSUER}
    counter: EURMTL:{ISSUER}
accounts:
  - address: {A}
    enabled: true
    reason: swaps through the market
  - address: {B}
    enabled: false
    reason: passive market maker, not decided yet
"""

# The file as it was written while accounts had modes.
OLD_GOOD = GOOD.replace("    enabled: true\n    reason: swaps", "    mode: FULL\n    reason: swaps").replace(
    "    enabled: false\n    reason: passive", "    mode: CANDIDATE\n    reason: passive"
)


def flags(engine, model: type, key) -> bool:
    with Session(engine) as session:
        return session.get(model, key).enabled


def import_file(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, url: str, text: str, *args: str) -> tuple[int, str, str]:
    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", url)
    monkeypatch.delenv("MTL_HORIZON_NETWORK_PASSPHRASE", raising=False)
    file = tmp_path / "tracking.yaml"
    file.write_text(text, encoding="utf-8")
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main(["--log-level", "WARNING", "import-config", "--file", str(file), *args])
    return code, out.getvalue(), err.getvalue()


@pytest.fixture
def db(fresh_database_url: str):
    engine = create_engine(fresh_database_url)
    yield engine
    engine.dispose()


def tracked(engine, model: type) -> int:
    with Session(engine) as session:
        return len(list(session.scalars(select(model))))


def test_a_dry_run_writes_nothing(tmp_path, monkeypatch, db, fresh_database_url) -> None:
    code, out, _ = import_file(tmp_path, monkeypatch, fresh_database_url, GOOD, "--dry-run")
    assert code == 0
    assert "dry run: nothing is written" in out and "assets:   2 added" in out and "accounts: 2 added" in out
    assert tracked(db, TrackedAccount) == 0 and tracked(db, TrackedAsset) == 0 and tracked(db, TrackedMarket) == 0
    assert tracked(db, HistoryAsset) == 0 and tracked(db, HistoryAccount) == 0


def test_importing_the_same_file_again_changes_nothing(tmp_path, monkeypatch, db, fresh_database_url) -> None:
    code, first, _ = import_file(tmp_path, monkeypatch, fresh_database_url, GOOD)
    assert code == 0
    assert "assets:   2 added, 0 unchanged" in first
    assert "markets:  1 added, 0 unchanged" in first
    assert "accounts: 2 added, 0 unchanged, 0 skipped" in first

    code, again, _ = import_file(tmp_path, monkeypatch, fresh_database_url, GOOD)
    assert code == 0
    assert "assets:   0 added, 2 unchanged" in again
    assert "markets:  0 added, 1 unchanged" in again
    assert "accounts: 0 added, 2 unchanged, 0 skipped" in again
    assert (tracked(db, TrackedAccount), tracked(db, TrackedAsset), tracked(db, TrackedMarket)) == (2, 2, 1)
    assert (flags(db, TrackedAccount, A), flags(db, TrackedAccount, B)) == (True, False)


def test_one_bad_entry_keeps_the_whole_file_out(tmp_path, monkeypatch, db, fresh_database_url) -> None:
    broken = GOOD + f"""  - address: {C}
    enabled: maybe
    reason: a flag that is not a flag
"""
    code, _, err = import_file(tmp_path, monkeypatch, fresh_database_url, broken)
    assert code == 1
    assert "enabled must be true or false, got 'maybe'" in err
    assert tracked(db, TrackedAccount) == 0 and tracked(db, TrackedMarket) == 0 and tracked(db, TrackedAsset) == 0


def test_every_problem_is_reported_in_one_go(tmp_path, monkeypatch, db, fresh_database_url) -> None:
    broken = f"""
accounts:
  - address: {A}
    enabled: true
  - address: GAAAA
    enabled: true
    reason: not an address
  - address: {A}
    enabled: true
    reason: listed twice
"""
    code, _, err = import_file(tmp_path, monkeypatch, fresh_database_url, broken)
    assert code == 1
    assert "reason is missing" in err
    assert "'GAAAA' is not an account address" in err
    assert "listed twice" in err


def test_a_reversed_market_is_the_same_market_and_cannot_be_listed_twice(tmp_path, monkeypatch, db, fresh_database_url) -> None:
    market = f"""  - base: MTLRECT:{ISSUER}
    counter: EURMTL:{ISSUER}
"""
    twice = GOOD.replace(market, market + f"""  - base: EURMTL:{ISSUER}
    counter: MTLRECT:{ISSUER}
""")
    code, _, err = import_file(tmp_path, monkeypatch, fresh_database_url, twice)
    assert code == 1 and "the other way round" in err

    import_file(tmp_path, monkeypatch, fresh_database_url, GOOD)
    other_way = GOOD.replace(f"base: MTLRECT:{ISSUER}\n    counter: EURMTL:{ISSUER}", f"base: EURMTL:{ISSUER}\n    counter: MTLRECT:{ISSUER}")
    code, out, _ = import_file(tmp_path, monkeypatch, fresh_database_url, other_way)
    assert code == 0 and "markets:  0 added, 1 unchanged" in out
    assert tracked(db, TrackedMarket) == 1


def test_a_flag_changes_only_when_asked_for(tmp_path, monkeypatch, db, fresh_database_url) -> None:
    import_file(tmp_path, monkeypatch, fresh_database_url, GOOD)
    other_flag = GOOD.replace("    enabled: true\n    reason: swaps through the market", "    enabled: false\n    reason: swaps through the market")

    code, out, _ = import_file(tmp_path, monkeypatch, fresh_database_url, other_flag)
    assert code == 0
    assert "accounts: 0 added, 1 unchanged, 1 skipped" in out
    assert "the file says enabled=false, the database says true; left as it is" in out
    assert flags(db, TrackedAccount, A) is True

    code, out, _ = import_file(tmp_path, monkeypatch, fresh_database_url, other_flag, "--apply-changes")
    assert code == 0 and "accounts: 0 added, 1 unchanged, 1 changed" in out
    assert flags(db, TrackedAccount, A) is False


def test_a_file_with_former_modes_is_read_in_the_open(tmp_path, monkeypatch, db, fresh_database_url) -> None:
    code, out, _ = import_file(tmp_path, monkeypatch, fresh_database_url, OLD_GOOD)

    assert code == 0
    assert (flags(db, TrackedAccount, A), flags(db, TrackedAccount, B)) == (True, False)
    assert "accounts[0]: mode FULL read as enabled true" in out
    assert "accounts[1]: mode CANDIDATE read as enabled false" in out
    assert out.index("mode FULL read") < out.index("mode CANDIDATE read")  # every entry, in order


def test_an_inactive_account_is_not_switched_on_by_a_file_that_says_nothing(tmp_path, monkeypatch, db, fresh_database_url) -> None:
    import_file(tmp_path, monkeypatch, fresh_database_url, OLD_GOOD)  # B comes in as CANDIDATE
    silent = f"""
accounts:
  - address: {A}
    reason: swaps through the market
  - address: {B}
    reason: passive market maker, not decided yet
"""
    code, out, _ = import_file(tmp_path, monkeypatch, fresh_database_url, silent)

    assert code == 0 and "accounts: 0 added, 2 unchanged, 0 skipped" in out
    assert (flags(db, TrackedAccount, A), flags(db, TrackedAccount, B)) == (True, False)


def test_a_former_mode_and_a_flag_cannot_be_given_together(tmp_path, monkeypatch, db, fresh_database_url) -> None:
    both = GOOD.replace("    enabled: true\n    reason: swaps", "    enabled: true\n    mode: FULL\n    reason: swaps")

    code, _, err = import_file(tmp_path, monkeypatch, fresh_database_url, both)

    assert code == 1 and "'mode' and 'enabled' are alternatives" in err
    assert tracked(db, TrackedAccount) == 0


def test_an_unknown_former_mode_is_refused(tmp_path, monkeypatch, db, fresh_database_url) -> None:
    unknown = OLD_GOOD.replace("    mode: CANDIDATE\n", "    mode: SOMETIMES\n")

    code, _, err = import_file(tmp_path, monkeypatch, fresh_database_url, unknown)

    assert code == 1 and "mode 'SOMETIMES' is not one of FULL, ASSETS_ONLY, CANDIDATE, DISABLED" in err
    assert tracked(db, TrackedAccount) == 0


def test_assets_and_markets_have_a_flag_of_their_own(tmp_path, monkeypatch, db, fresh_database_url) -> None:
    off = GOOD.replace("  - code: MTLRECT\n", "  - code: MTLRECT\n    enabled: false\n").replace(
        "  - base: MTLRECT:" + ISSUER + "\n", "  - enabled: false\n    base: MTLRECT:" + ISSUER + "\n"
    )

    code, out, _ = import_file(tmp_path, monkeypatch, fresh_database_url, off)

    assert code == 0 and "assets:   2 added" in out and "markets:  1 added" in out
    assert (flags(db, TrackedAsset, "MTLRECT:" + ISSUER), flags(db, TrackedAsset, "EURMTL:" + ISSUER)) == (False, True)
    assert flags(db, TrackedMarket, ("EURMTL:" + ISSUER, "MTLRECT:" + ISSUER)) is False  # lower id first


def test_entries_left_out_of_a_later_file_are_not_removed(tmp_path, monkeypatch, db, fresh_database_url) -> None:
    import_file(tmp_path, monkeypatch, fresh_database_url, GOOD)
    smaller = f"""
accounts:
  - address: {A}
    enabled: true
    reason: swaps through the market
"""
    code, _, _ = import_file(tmp_path, monkeypatch, fresh_database_url, smaller)
    assert code == 0
    assert tracked(db, TrackedAccount) == 2 and tracked(db, TrackedMarket) == 1 and tracked(db, TrackedAsset) == 2


def test_a_missing_file_is_an_error(tmp_path, monkeypatch, db, fresh_database_url) -> None:
    monkeypatch.setenv("MTL_HORIZON_DATABASE_URL", fresh_database_url)
    with contextlib.redirect_stderr(io.StringIO()):
        assert main(["--log-level", "WARNING", "import-config", "--file", str(tmp_path / "nope.yaml")]) == 1
    assert tracked(db, TrackedAccount) == 0


def test_native_asset_and_an_undeclared_market_asset(tmp_path, monkeypatch, db, fresh_database_url) -> None:
    file = f"""
assets:
  - native: true
markets:
  - base: native
    counter: EURMTL:{ISSUER}
accounts: []
"""
    code, out, _ = import_file(tmp_path, monkeypatch, fresh_database_url, file)
    assert code == 0 and "markets:  1 added" in out
    with Session(db) as session:
        native = session.get(HistoryAsset, "native")
        assert (native.asset_type, native.asset_code, native.asset_issuer) == ("native", "", "")
        assert tracked(db, HistoryAsset) == 2  # the market registered EURMTL too
        assert session.get(TrackedMarket, ("EURMTL:" + ISSUER, "native")) is not None  # lower id first
