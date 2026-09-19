"""Collapsing the Beijing board's pre-rename codes onto the ones in use now.

The BSE renumbered 248 securities on 2025-09-30 and the vendors then served
each one's whole history under its new code. The lake kept both series, so
2016..2025 was stored twice — 215,433 daily_bars rows with all five OHLCV
fields byte-identical, and 248 securities counted twice by anything counting
Beijing.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.storage import bse_code_migration as mig

MAPPING = {"430090.BJ": "920090.BJ", "832278.BJ": "920278.BJ"}


@pytest.fixture(autouse=True)
def _mapping(monkeypatch):
    monkeypatch.setattr(mig, "_mapping", lambda: dict(MAPPING))


@pytest.fixture
def cfg(tmp_path):
    config = Config(data_root=tmp_path / "lake")
    (config.curated_root).mkdir(parents=True, exist_ok=True)
    (config.meta_root).mkdir(parents=True, exist_ok=True)
    return config


def _write(cfg, dataset: str, rows: list[dict], partition: str = "p") -> None:
    out = cfg.curated_root / dataset / partition
    out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(out / "part-merged.parquet")


def _read(cfg, dataset: str) -> pl.DataFrame:
    files = sorted((cfg.curated_root / dataset).glob("**/*.parquet"))
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")


def _bar(symbol: str, day: date, close: float = 10.0) -> dict:
    return {"symbol": symbol, "trade_date": day, "close": close, "volume": 100.0}


DAY = date(2024, 3, 1)


def test_a_row_whose_current_code_already_holds_it_is_dropped(cfg):
    _write(cfg, "daily_bars", [_bar("430090.BJ", DAY), _bar("920090.BJ", DAY)])

    report = mig.migrate_bse_legacy_codes(cfg, apply=True)

    assert report["datasets"]["daily_bars"]["rows_dropped"] == 1
    kept = _read(cfg, "daily_bars")
    assert kept.height == 1
    assert kept["symbol"][0] == "920090.BJ"


def test_a_row_the_current_code_does_not_hold_is_re_stamped_not_dropped(cfg):
    """`corporate_actions` holds 165 transfer records from 2016 that exist only
    under the old codes. Dropping them would lose real data."""
    _write(cfg, "daily_bars", [_bar("430090.BJ", DAY)])

    report = mig.migrate_bse_legacy_codes(cfg, apply=True)

    assert report["datasets"]["daily_bars"] == {
        "rows_dropped": 0,
        "rows_restamped": 1,
        "partitions_rewritten": 1,
    }
    kept = _read(cfg, "daily_bars")
    assert kept["symbol"].to_list() == ["920090.BJ"]


def test_the_twin_test_is_per_key_not_per_symbol(cfg):
    """One shared session does not make the whole series redundant."""
    _write(
        cfg,
        "daily_bars",
        [
            _bar("430090.BJ", DAY),
            _bar("430090.BJ", date(2024, 3, 2)),
            _bar("920090.BJ", DAY),
        ],
    )

    mig.migrate_bse_legacy_codes(cfg, apply=True)

    kept = _read(cfg, "daily_bars").sort("trade_date")
    assert kept["symbol"].to_list() == ["920090.BJ", "920090.BJ"]
    assert kept["trade_date"].to_list() == [DAY, date(2024, 3, 2)]


def test_a_code_the_exchange_never_migrated_is_left_alone(cfg):
    """832317, 833874 and 833994 last traded in 2021 and are in no mapping."""
    _write(cfg, "daily_bars", [_bar("833994.BJ", DAY)])

    report = mig.migrate_bse_legacy_codes(cfg, apply=True)

    assert report["datasets"]["daily_bars"]["rows_dropped"] == 0
    assert _read(cfg, "daily_bars")["symbol"].to_list() == ["833994.BJ"]


def test_the_registry_records_the_rename_instead_of_the_old_entry(cfg):
    _write(
        cfg,
        "instruments",
        [
            {"symbol": "430090.BJ", "name": "旧", "prev_symbol": None},
            {"symbol": "920090.BJ", "name": "新", "prev_symbol": None},
        ],
        partition=".",
    )

    report = mig.migrate_bse_legacy_codes(cfg, apply=True)

    assert report["datasets"]["instruments"] == {
        "entries_dropped": 1,
        "prev_symbol_recorded": 1,
    }
    kept = _read(cfg, "instruments")
    assert kept["symbol"].to_list() == ["920090.BJ"]
    assert kept["prev_symbol"].to_list() == ["430090.BJ"]


def test_a_rename_another_exchange_already_recorded_is_not_counted_again(cfg):
    """Otherwise the report claims renames this call never wrote."""
    _write(
        cfg,
        "instruments",
        [
            {"symbol": "600000.SH", "name": "沪", "prev_symbol": "600001.SH"},
            {"symbol": "430090.BJ", "name": "旧", "prev_symbol": None},
            {"symbol": "920090.BJ", "name": "新", "prev_symbol": None},
        ],
        partition=".",
    )

    report = mig.migrate_bse_legacy_codes(cfg, apply=True)

    assert report["datasets"]["instruments"]["prev_symbol_recorded"] == 1
    kept = _read(cfg, "instruments").sort("symbol")
    assert kept.filter(pl.col("symbol") == "600000.SH")["prev_symbol"][0] == "600001.SH"


def test_reporting_writes_nothing(cfg):
    _write(cfg, "daily_bars", [_bar("430090.BJ", DAY), _bar("920090.BJ", DAY)])

    report = mig.migrate_bse_legacy_codes(cfg)

    assert report["applied"] is False
    assert report["datasets"]["daily_bars"]["rows_dropped"] == 1
    assert _read(cfg, "daily_bars").height == 2, "the lake is untouched"


def test_a_partition_emptied_of_everything_is_removed(cfg):
    _write(cfg, "daily_bars", [_bar("430090.BJ", DAY)], partition="trade_date=2024-03-01")
    _write(cfg, "daily_bars", [_bar("920090.BJ", DAY)], partition="trade_date=other")

    mig.migrate_bse_legacy_codes(cfg, apply=True)

    assert _read(cfg, "daily_bars")["symbol"].to_list() == ["920090.BJ"]


def test_a_rename_recorded_as_a_delisting_is_removed(cfg, monkeypatch):
    """248 of `delisting_events`' 343 rows were renames: the old code stops
    answering on the switch date, which is what a delisting looks like to a
    probe. Anything studying how listings end was reading mostly renames."""
    out = cfg.derived_root / "delisting_events"
    out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {"symbol": "430090.BJ", "last_trade_date": date(2025, 9, 30)},
            {"symbol": "600000.SH", "last_trade_date": date(2024, 5, 1)},
        ]
    ).write_parquet(out / "part-merged.parquet")

    report = mig.migrate_bse_legacy_codes(cfg, apply=True)

    assert report["datasets"]["delisting_events"]["rows_dropped"] == 1
    kept = pl.read_parquet(out / "part-merged.parquet")
    assert kept["symbol"].to_list() == ["600000.SH"], "a real delisting stays"


def test_the_catalogue_and_the_ingest_ledger_forget_the_rename(cfg):
    """Otherwise the dedicated delisted fetch keeps chasing history for codes
    that no longer exist, and the rows go straight back next time."""
    import json

    state = cfg.meta_root / "state"
    state.mkdir(parents=True, exist_ok=True)
    (state / "delisted_catalog.json").write_text(
        json.dumps({"delisted": {"430090.BJ": "2025-09-30", "600000.SH": "2024-05-01"}})
    )
    (state / "delisted_ingested.json").write_text(
        json.dumps({"completed": ["430090.BJ", "600000.SH"]})
    )

    report = mig.migrate_bse_legacy_codes(cfg, apply=True)

    assert report["state"] == {"delisted_catalog.json": 1, "delisted_ingested.json": 1}
    catalog = json.loads((state / "delisted_catalog.json").read_text())
    assert list(catalog["delisted"]) == ["600000.SH"]
    ledger = json.loads((state / "delisted_ingested.json").read_text())
    assert ledger["completed"] == ["600000.SH"]


def test_the_apply_closes_its_own_run(tmp_path, monkeypatch):
    """An apply that finished must not read as a crash.

    `_publish` started a run and never finished it, so the reconciler swept
    every apply into `failed: worker exited without finish_run` — three in a
    row on 2026-09-18, on a migration that had completed and published.
    """
    import sqlite3

    from cnequity.config import Config
    from cnequity.storage.bse_code_migration import migrate_bse_legacy_codes
    from cnequity.storage.layout import init_data_layout

    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)

    migrate_bse_legacy_codes(cfg, apply=True)

    con = sqlite3.connect(cfg.manifest_path)
    rows = con.execute(
        "select status from ingestion_runs where job_name = 'maintenance:bse_code_migration'"
    ).fetchall()
    assert rows == [("success",)]
