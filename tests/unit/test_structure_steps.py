"""Offline coverage for L5 structure steps (sector / index / industry)."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import Config
from cn_market_lake.steps import structure as st
from cn_market_lake.storage.state import StateStore


@pytest.fixture
def cfg(tmp_path):
    c = Config(data_root=tmp_path / "data")
    c.staging_root.mkdir(parents=True)
    return c


def test_month_end_trading_days(cfg, monkeypatch):
    days = [
        date(2024, 1, 30),
        date(2024, 1, 31),
        date(2024, 2, 28),
        date(2024, 2, 29),
    ]
    monkeypatch.setattr(st, "list_trading_dates", lambda *a, **k: days)
    assert st._month_end_trading_days(cfg, date(2024, 1, 1), date(2024, 2, 29)) == [
        date(2024, 1, 31),
        date(2024, 2, 29),
    ]


def test_existing_as_of_dates_empty_and_populated(cfg):
    assert st._existing_as_of_dates(cfg, "index_constituents") == set()
    part = cfg.curated_root / "index_constituents" / "as_of_date=2024-06"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "index_symbol": ["399001.SZ"],
            "symbol": ["000001.SZ"],
            "as_of_date": [date(2024, 6, 28)],
        }
    ).write_parquet(part / "part-000.parquet")
    assert st._existing_as_of_dates(cfg, "index_constituents") == {date(2024, 6, 28)}


def test_sector_members_disabled_source(cfg):
    cfg.sources["eastmoney"] = False
    with pytest.raises(RuntimeError, match="eastmoney source disabled"):
        st.step_sector_members(cfg, date(2024, 6, 28), "run-1", {})


def test_sector_members_writes_staging(cfg, monkeypatch):
    StateStore(cfg.meta_root).set_date("sector_members", date(2024, 6, 27))

    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "sector_code": ["BK0001"],
                "sector_name": ["白酒"],
                "as_of_date": [trade_date],
            }
        )

    monkeypatch.setattr(st, "fetch_sector_members", fake_fetch)
    result = st.step_sector_members(cfg, date(2024, 6, 28), "run-1", {})
    assert result["rows_written"] == 1
    assert list(cfg.staging_root.glob("sector_members/**/*.parquet"))


def test_index_constituents_daily_writes(cfg, monkeypatch):
    StateStore(cfg.meta_root).set_date("index_constituents", date(2024, 6, 27))

    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                "index_symbol": ["000300.SH"],
                "symbol": ["600519.SH"],
                "as_of_date": [trade_date],
                "weight": [1.0],
            }
        )

    monkeypatch.setattr(st, "fetch_index_constituents", fake_fetch)
    result = st.step_index_constituents(cfg, date(2024, 6, 28), "run-1", {})
    assert result["rows_written"] == 1


def test_index_constituents_backfill_already_present(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 1, 31)
    monkeypatch.setattr(st, "_month_end_trading_days", lambda *a, **k: [date(2024, 1, 31)])
    monkeypatch.setattr(st, "_existing_as_of_dates", lambda *a, **k: {date(2024, 1, 31)})
    result = st.step_index_constituents(cfg, date(2024, 1, 31), "run-bf", {})
    assert result["rows_written"] == 0
    assert "already present" in result["note"]


def test_index_constituents_backfill_writes_cni(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 1, 31)
    todo = [date(2024, 1, 31)]
    monkeypatch.setattr(st, "_month_end_trading_days", lambda *a, **k: todo)
    monkeypatch.setattr(st, "_existing_as_of_dates", lambda *a, **k: set())

    adj = pl.DataFrame({"index_symbol": ["399001.SZ"], "dummy": [1]})
    expanded = pl.DataFrame(
        {
            "index_symbol": ["399001.SZ"],
            "symbol": ["000001.SZ"],
            "as_of_date": [date(2024, 1, 31)],
            "weight": pl.Series("weight", [None], dtype=pl.Float64),
        }
    )

    def fake_adj(index_symbol):
        if index_symbol == "399001.SZ":
            return adj
        return pl.DataFrame()

    monkeypatch.setattr(st, "fetch_cni_index_adjustments", fake_adj)
    monkeypatch.setattr(st, "expand_cni_constituents_as_of", lambda a, days: expanded)
    result = st.step_index_constituents(cfg, date(2024, 1, 31), "run-cni", {})
    assert result["rows_written"] == 1
    assert result["as_of_dates"] == 1
    findings = result["context_updates"]["audit_findings"]
    assert findings[0]["code"] == "cni_index_backfill_incomplete"


def test_industry_members_disabled(cfg):
    cfg.sources["eastmoney"] = False
    with pytest.raises(RuntimeError, match="eastmoney source disabled"):
        st.step_industry_members(cfg, date(2024, 6, 28), "run-1", {})


def test_industry_members_daily_writes(cfg, monkeypatch):
    StateStore(cfg.meta_root).set_date("industry_members", date(2024, 6, 27))

    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "classification_system": ["em"],
                "industry_code": ["白酒"],
                "industry_name": ["白酒"],
                "as_of_date": [trade_date],
            }
        )

    monkeypatch.setattr(st, "fetch_industry_members", fake_fetch)
    result = st.step_industry_members(cfg, date(2024, 6, 28), "run-1", {})
    assert result["rows_written"] == 1


def test_industry_members_backfill_already_present(cfg, monkeypatch):
    cfg._backfill = True
    monkeypatch.setattr(st, "_month_end_trading_days", lambda *a, **k: [date(2024, 1, 31)])
    monkeypatch.setattr(st, "_existing_sw_as_of_dates", lambda *a, **k: {date(2024, 1, 31)})
    monkeypatch.setattr(st, "_existing_as_of_dates", lambda *a, **k: {date(2024, 1, 31)})
    result = st.step_industry_members(cfg, date(2024, 1, 31), "run-sw", {})
    assert "already present" in result["note"]


def test_industry_members_backfill_thin_month_warning(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 1, 31)
    monkeypatch.setattr(st, "_month_end_trading_days", lambda *a, **k: [date(2024, 1, 31)])
    monkeypatch.setattr(st, "_existing_sw_as_of_dates", lambda *a, **k: set())
    monkeypatch.setattr(st, "_existing_as_of_dates", lambda *a, **k: set())
    monkeypatch.setattr(st, "fetch_sw_industry_intervals", lambda: pl.DataFrame({"x": [1]}))

    # Far fewer than the 1000-name floor → soft warning finding.
    thin = pl.DataFrame(
        {
            "symbol": [f"{i:06d}.SH" for i in range(5)],
            "classification_system": ["sw"] * 5,
            "industry_code": ["240301"] * 5,
            "industry_name": ["铝"] * 5,
            "as_of_date": [date(2024, 1, 31)] * 5,
        }
    )
    monkeypatch.setattr(st, "expand_sw_industry_as_of", lambda intervals, todo: thin)
    result = st.step_industry_members(cfg, date(2024, 1, 31), "run-thin", {})
    assert result["rows_written"] == 5
    assert result["context_updates"]["audit_findings"][0]["code"] == "sw_industry_thin_months"


def test_existing_sw_as_of_dates_filters_source(cfg):
    part = cfg.curated_root / "industry_members" / "as_of_date=2024-06"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "classification_system": ["em", "sw"],
            "industry_code": ["白酒", "240301"],
            "industry_name": ["白酒", "铝"],
            "as_of_date": [date(2024, 6, 28), date(2024, 6, 28)],
            "source": ["eastmoney", "sw"],
            "data_version": ["v1", "v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"] * 2,
        }
    ).write_parquet(part / "part-000.parquet")
    assert st._existing_sw_as_of_dates(cfg) == {date(2024, 6, 28)}
