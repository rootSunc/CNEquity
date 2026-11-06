"""derive_adj_factors under month partitions.

The derive read and wrote ``trade_date=<day>`` directories. Under a month
layout its watermark would have read as the 1st of the month (every later
session re-derived daily), and its writer would have kept one directory per
day beside the month files.
"""

from __future__ import annotations

import dataclasses
from datetime import date

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.derive import adj_factors as derive
from cnequity.domain import datasets as registry
from cnequity.domain.schemas import with_provenance

JUNE = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)]


def _factors(days: list[date], symbols: list[str], factor: float = 1.0) -> pl.DataFrame:
    rows = [(s, d) for d in days for s in symbols]
    return with_provenance(
        pl.DataFrame(
            {
                "symbol": [r[0] for r in rows],
                "trade_date": [r[1] for r in rows],
                "adjust_type": ["hfq"] * len(rows),
                "factor": [factor] * len(rows),
            }
        ),
        source="sina",
        data_version="v1",
    )


@pytest.fixture
def month_cfg(tmp_path, monkeypatch):
    monkeypatch.setitem(
        registry.DATASETS,
        "adj_factors",
        dataclasses.replace(registry.DATASETS["adj_factors"], partition_granularity="month"),
    )
    cfg = Config(data_root=tmp_path / "data")
    part = cfg.derived_root / "adj_factors" / "trade_date=2024-06"
    part.mkdir(parents=True)
    frame = pl.concat(
        [_factors(JUNE[:2], ["600519.SH", "000001.SZ"]), _factors(JUNE[2:], ["600519.SH"])]
    )
    frame.write_parquet(part / "part-0.parquet")
    return cfg


def test_the_watermark_is_the_newest_session_inside_the_month(month_cfg):
    assert derive._adj_factors_watermark(month_cfg) == date(2024, 6, 5)


def test_partition_symbols_are_read_per_day_inside_a_month(month_cfg):
    assert derive._factor_partition_symbols(month_cfg, date(2024, 6, 4)) == {
        "600519.SH",
        "000001.SZ",
    }
    assert derive._factor_partition_symbols(month_cfg, date(2024, 6, 5)) == {"600519.SH"}
    assert derive._factor_partition_symbols(month_cfg, date(2024, 6, 6)) == set()


def test_an_append_lands_in_the_month_and_keeps_its_earlier_sessions(month_cfg):
    root = month_cfg.derived_root / "adj_factors"
    derive._write_adj_partitions(
        month_cfg, _factors([date(2024, 6, 6)], ["600519.SH", "000001.SZ"]), replace=False
    )
    assert sorted(p.name for p in root.iterdir()) == ["trade_date=2024-06"]
    stored = pl.concat([pl.read_parquet(f) for f in root.rglob("*.parquet")])
    assert stored.height == 5 + 2
    assert sorted(set(stored["trade_date"].to_list())) == [*JUNE, date(2024, 6, 6)]
    assert derive._adj_factors_watermark(month_cfg) == date(2024, 6, 6)


def test_a_full_replace_writes_whole_months(month_cfg):
    root = month_cfg.derived_root / "adj_factors"
    july = [date(2024, 7, 1), date(2024, 7, 2)]
    derive._write_adj_partitions(
        month_cfg, _factors(JUNE + july, ["600519.SH"], factor=2.0), replace=True
    )
    assert sorted(p.name for p in root.iterdir()) == ["trade_date=2024-06", "trade_date=2024-07"]
    june = pl.concat([pl.read_parquet(f) for f in (root / "trade_date=2024-06").rglob("*.parquet")])
    assert june.height == 3 and set(june["factor"].to_list()) == {2.0}


def test_the_day_layout_still_reads_its_own_directories(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    for day in JUNE:
        part = cfg.derived_root / "adj_factors" / f"trade_date={day}"
        part.mkdir(parents=True)
        _factors([day], ["600519.SH"]).write_parquet(part / "part-0.parquet")
    assert derive._adj_factors_watermark(cfg) == date(2024, 6, 5)
    assert derive._factor_partition_symbols(cfg, date(2024, 6, 4)) == {"600519.SH"}
