"""Tests for valuation_metrics orphan-symbol purge."""

from __future__ import annotations

from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.storage.valuation_orphans import purge_valuation_orphan_symbols


def test_purge_drops_symbols_absent_from_bars(tmp_path):
    root = tmp_path / "data"
    bars = root / "curated" / "daily_bars" / "trade_date=2024-06-28"
    bars.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [date(2024, 6, 28)]}).write_parquet(
        bars / "part.parquet"
    )

    part_a = root / "curated" / "valuation_metrics" / "trade_date=2024-06-28"
    part_b = root / "curated" / "valuation_metrics" / "trade_date=2024-06-27"
    part_a.mkdir(parents=True)
    part_b.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "600193.SH"],
            "trade_date": [date(2024, 6, 28), date(2024, 6, 28)],
            "pe_ttm": [20.0, 1.0],
        }
    ).write_parquet(part_a / "part.parquet")
    pl.DataFrame(
        {
            "symbol": ["600193.SH"],
            "trade_date": [date(2024, 6, 27)],
            "pe_ttm": [1.0],
        }
    ).write_parquet(part_b / "part.parquet")

    summary = purge_valuation_orphan_symbols(Config(data_root=root))
    assert summary["orphan_symbols"] == ["600193.SH"]
    assert summary["rows_removed"] == 2
    assert summary["partitions_rewritten"] == 2

    kept = pl.read_parquet(part_a / "part.parquet")
    assert kept["symbol"].to_list() == ["600519.SH"]
    assert not (part_b / "part.parquet").exists()


def test_purge_noop_when_aligned(tmp_path):
    root = tmp_path / "data"
    bars = root / "curated" / "daily_bars" / "trade_date=2024-06-28"
    bars.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [date(2024, 6, 28)]}).write_parquet(
        bars / "part.parquet"
    )
    part = root / "curated" / "valuation_metrics" / "trade_date=2024-06-28"
    part.mkdir(parents=True)
    pl.DataFrame(
        {"symbol": ["600519.SH"], "trade_date": [date(2024, 6, 28)], "pe_ttm": [20.0]}
    ).write_parquet(part / "part.parquet")

    summary = purge_valuation_orphan_symbols(Config(data_root=root))
    assert summary == {
        "orphan_symbols": [],
        "partitions_rewritten": 0,
        "rows_removed": 0,
    }
