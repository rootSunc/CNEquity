"""Offline tests for C2 membership history expanders (SW industry / CNI index)."""

from __future__ import annotations

from datetime import date

import polars as pl

from cn_market_lake.adapters.cni.index_constituents_history import expand_cni_constituents_as_of
from cn_market_lake.adapters.sw.industry_history import expand_sw_industry_as_of
from cn_market_lake.domain.datasets import get_dataset
from cn_market_lake.domain.schemas import validate_dataframe


def test_industry_members_declares_sw_backfill_source():
    spec = get_dataset("industry_members")
    assert spec.fetch_semantics == "snapshot"
    assert spec.backfill_source == "sw"


def test_index_constituents_declares_cni_backfill_source():
    spec = get_dataset("index_constituents")
    assert spec.fetch_semantics == "snapshot"
    assert spec.backfill_source == "cni"


def test_expand_sw_industry_as_of_picks_latest_spell():
    intervals = pl.DataFrame(
        {
            "symbol": ["000001.SZ", "000001.SZ", "600519.SH"],
            "start_date": [date(2014, 2, 21), date(2021, 7, 30), date(2016, 1, 4)],
            "industry_code": ["480101", "480301", "240201"],
        }
    )
    out = expand_sw_industry_as_of(intervals, [date(2020, 6, 30), date(2022, 6, 30)])
    assert set(out["as_of_date"].to_list()) == {date(2020, 6, 30), date(2022, 6, 30)}
    early = out.filter(
        (pl.col("symbol") == "000001.SZ") & (pl.col("as_of_date") == date(2020, 6, 30))
    )
    late = out.filter(
        (pl.col("symbol") == "000001.SZ") & (pl.col("as_of_date") == date(2022, 6, 30))
    )
    assert early["industry_code"].to_list() == ["480101"]
    assert late["industry_code"].to_list() == ["480301"]
    assert out["classification_system"].unique().to_list() == ["sw"]
    validate_dataframe(
        out.with_columns(
            pl.lit("sw").alias("source"),
            pl.lit("v1").alias("data_version"),
            pl.lit(None, dtype=pl.Datetime(time_zone="UTC")).alias("fetched_at"),
        ),
        "industry_members",
    )


def test_expand_cni_constituents_as_of_uses_open_interval():
    adj = pl.DataFrame(
        {
            "index_symbol": ["399001.SZ", "399001.SZ", "399001.SZ"],
            "symbol": ["000001.SZ", "000002.SZ", "000001.SZ"],
            "start_date": [date(2021, 12, 13), date(2021, 12, 13), date(2022, 6, 13)],
            "end_date": [date(2022, 6, 13), date(2024, 12, 16), date(2024, 12, 16)],
            "adjust_type": ["OLD", "OLD", "+"],
        }
    )
    out = expand_cni_constituents_as_of(adj, [date(2022, 1, 28), date(2022, 6, 13)])
    jan = set(out.filter(pl.col("as_of_date") == date(2022, 1, 28))["symbol"].to_list())
    jun = set(out.filter(pl.col("as_of_date") == date(2022, 6, 13))["symbol"].to_list())
    assert jan == {"000001.SZ", "000002.SZ"}
    # 000001's first spell ends on 2022-06-13 (exclusive end); + spell starts same day.
    assert jun == {"000001.SZ", "000002.SZ"}
    validate_dataframe(
        out.with_columns(
            pl.lit("cni").alias("source"),
            pl.lit("v1").alias("data_version"),
            pl.lit(None, dtype=pl.Datetime(time_zone="UTC")).alias("fetched_at"),
        ),
        "index_constituents",
    )
