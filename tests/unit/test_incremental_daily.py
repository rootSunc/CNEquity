from datetime import date

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.steps import capital as cap
from cn_market_lake.steps.common import (
    SnapshotBackfillError,
    fetch_incremental_daily,
    incremental_trade_dates,
    list_trading_dates,
)
from cn_market_lake.storage.state import StateStore


def _seed_trading_calendar(cfg: Config, start: date, end: date) -> None:
    rows = []
    d = start
    while d <= end:
        rows.append({"trade_date": d, "is_trading": d.weekday() < 5})
        d = date.fromordinal(d.toordinal() + 1)
    path = cfg.curated_root / "trading_calendar" / "part-merged.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def test_incremental_trade_dates_uses_watermark(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("fund_flow", date(2024, 6, 25))

    dates = incremental_trade_dates(cfg, "fund_flow", date(2024, 6, 28))
    assert dates == [date(2024, 6, 26), date(2024, 6, 27), date(2024, 6, 28)]


def test_list_trading_dates_skips_weekends_without_calendar(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    dates = list_trading_dates(cfg, date(2024, 6, 28), date(2024, 6, 30))
    assert dates == [date(2024, 6, 28)]


def test_fetch_incremental_daily_loops_gap_days_for_by_date(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("margin_trading", date(2024, 6, 25))
    fetched: list[date] = []

    def _fetch(day: date) -> pl.DataFrame:
        fetched.append(day)
        return pl.DataFrame({"trade_date": [day], "symbol": ["600519.SH"], "value": [1.0]})

    df, findings = fetch_incremental_daily(cfg, "margin_trading", date(2024, 6, 28), _fetch)
    assert fetched == [date(2024, 6, 26), date(2024, 6, 27), date(2024, 6, 28)]
    assert df.height == 3
    assert findings == []


def test_fetch_incremental_daily_snapshot_only_fetches_run_day(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("fund_flow", date(2024, 6, 25))
    fetched: list[date] = []

    def _fetch(day: date) -> pl.DataFrame:
        fetched.append(day)
        return pl.DataFrame({"trade_date": [day], "symbol": ["600519.SH"], "value": [1.0]})

    df, findings = fetch_incremental_daily(cfg, "fund_flow", date(2024, 6, 28), _fetch)
    assert fetched == [date(2024, 6, 28)]
    assert df.height == 1
    assert len(findings) == 1
    assert findings[0]["check"] == "coverage_gap"
    assert findings[0]["gap_dates"] == ["2024-06-26", "2024-06-27"]


def test_fetch_incremental_daily_snapshot_rejects_backfill(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    cfg._backfill = True

    with pytest.raises(SnapshotBackfillError, match="fund_flow"):
        fetch_incremental_daily(
            cfg,
            "fund_flow",
            date(2024, 6, 28),
            lambda d: pl.DataFrame(),
        )


def test_step_fund_flow_snapshot_gap_only_fetches_run_day(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("fund_flow", date(2024, 6, 25))
    fetched: list[date] = []

    def fake_fetch(trade_date, **kwargs):
        fetched.append(trade_date)
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [trade_date],
                "main_net_inflow": [1.0],
                "super_large_net_inflow": [0.0],
                "large_net_inflow": [0.0],
                "medium_net_inflow": [0.0],
                "small_net_inflow": [0.0],
            }
        )

    monkeypatch.setattr(cap, "fetch_fund_flow", fake_fetch)
    cfg.staging_root.mkdir(parents=True)
    result = cap.step_fund_flow(cfg, date(2024, 6, 28), "run-gap", {})
    assert fetched == [date(2024, 6, 28)]
    assert result["rows_written"] == 1
    findings = result["context_updates"]["audit_findings"]
    assert findings[0]["check"] == "coverage_gap"


def test_step_fund_flow_single_day_when_caught_up(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    StateStore(cfg.meta_root).set_date("fund_flow", date(2024, 6, 27))
    fetched: list[date] = []

    def fake_fetch(trade_date, **kwargs):
        fetched.append(trade_date)
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [trade_date],
                "main_net_inflow": [1.0],
                "super_large_net_inflow": [0.0],
                "large_net_inflow": [0.0],
                "medium_net_inflow": [0.0],
                "small_net_inflow": [0.0],
            }
        )

    monkeypatch.setattr(cap, "fetch_fund_flow", fake_fetch)
    cfg.staging_root.mkdir(parents=True)
    result = cap.step_fund_flow(cfg, date(2024, 6, 28), "run-1", {})
    assert fetched == [date(2024, 6, 28)]
    assert result["rows_written"] == 1
    assert "context_updates" not in result
