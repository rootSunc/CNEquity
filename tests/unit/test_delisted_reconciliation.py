"""Safe repair path for vendor-polluted delisted catalogue terminals."""

import hashlib
import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.orchestrator.manifest import Manifest
from cn_market_lake.steps.delisted import (
    catalog_path,
    delisted_catalog_reconciliation_report,
    reconcile_delisted_catalog,
)


def _cfg(tmp_path) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    path = catalog_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "delisted": {
                    "300028.SZ": "2021-06-27",
                    "600001.SH": "2020-01-06",
                    "600002.SH": "2019-12-31",
                },
                "never_issued": [],
            }
        )
    )
    bars_root = cfg.curated_root / "daily_bars" / "trade_date=2020-01-01"
    bars_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["300028.SZ", "300028.SZ", "600001.SH"],
            "trade_date": [date(2020, 7, 31), date(2020, 8, 3), date(2020, 1, 3)],
            "volume": [100, 0, 100],
        }
    ).write_parquet(bars_root / "part-merged.parquet")
    anchor_root = cfg.curated_root / "daily_bars" / "trade_date=2022-01-10"
    anchor_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"symbol": ["600519.SH"], "trade_date": [date(2022, 1, 10)], "volume": [100]}
    ).write_parquet(anchor_root / "part-merged.parquet")
    instruments_root = cfg.curated_root / "instruments"
    instruments_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["300028.SZ", "600001.SH", "600002.SH"],
            "delist_date": pl.Series(
                [date(2020, 8, 3), date(2020, 1, 7), date(2020, 1, 2)], dtype=pl.Date
            ),
        }
    ).write_parquet(instruments_root / "part-merged.parquet")
    return cfg


def test_reconciliation_separates_safe_unresolved_and_missing(tmp_path):
    cfg = _cfg(tmp_path)

    report = delisted_catalog_reconciliation_report(cfg)

    assert report["read_only"] is True
    assert report["counts"] == {
        "catalogued": 3,
        "matching": 0,
        "safe_correction": 1,
        "unresolved_mismatch": 1,
        "missing_curated_bars": 1,
    }
    correction = report["safe_corrections"][0]
    assert correction["symbol"] == "300028.SZ"
    assert correction["proposed_last_traded"] == "2020-07-31"
    assert correction["catalog_after_formal_delist"] is True
    assert correction["catalog_not_trading_day"] is True


def test_apply_backs_up_catalog_and_writes_receipt(tmp_path):
    cfg = _cfg(tmp_path)

    result = reconcile_delisted_catalog(cfg)

    payload = json.loads(catalog_path(cfg).read_text())
    assert payload["delisted"]["300028.SZ"] == "2020-07-31"
    assert payload["delisted"]["600001.SH"] == "2020-01-06"
    assert result["applied"] == 1
    backup = Path(result["backup"])
    assert json.loads(backup.read_text())["delisted"]["300028.SZ"] == "2021-06-27"
    assert result["backup_sha256"] == hashlib.sha256(backup.read_bytes()).hexdigest()
    assert result["catalog_sha256"] == hashlib.sha256(catalog_path(cfg).read_bytes()).hexdigest()
    assert Path(result["receipt"]).exists()


def test_apply_refuses_while_an_ingestion_run_is_active(tmp_path):
    cfg = _cfg(tmp_path)
    Manifest(cfg.manifest_path).start_run("backfill", {"dataset": "top_holders"})

    with pytest.raises(RuntimeError, match="while ingestion runs are active"):
        reconcile_delisted_catalog(cfg)

    payload = json.loads(catalog_path(cfg).read_text())
    assert payload["delisted"]["300028.SZ"] == "2021-06-27"
    assert not (cfg.meta_root / "state" / "history").exists()
