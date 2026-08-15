"""Offline smoke for thin HTTP/derived step wrappers (disabled + empty + write)."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import Config
from cn_market_lake.steps import commodity, macro_risk, newsboard, research
from cn_market_lake.storage.state import StateStore


@pytest.fixture
def cfg(tmp_path):
    c = Config(data_root=tmp_path / "data")
    c.staging_root.mkdir(parents=True)
    return c


def test_research_disabled_and_empty(cfg, monkeypatch):
    cfg.sources["eastmoney"] = False
    with pytest.raises(RuntimeError, match="disabled"):
        research.step_institutional_holdings(cfg, date(2024, 6, 28), "r", {})
    with pytest.raises(RuntimeError, match="disabled"):
        research.step_analyst_consensus(cfg, date(2024, 6, 28), "r", {})

    cfg.sources["eastmoney"] = True
    monkeypatch.setattr(
        research,
        "fetch_institutional_holdings",
        lambda *a, **k: pl.DataFrame(),
    )
    monkeypatch.setattr(
        research,
        "fetch_analyst_consensus",
        lambda *a, **k: pl.DataFrame(),
    )
    assert (
        research.step_institutional_holdings(cfg, date(2024, 6, 28), "r", {})["rows_written"] == 0
    )
    assert research.step_analyst_consensus(cfg, date(2024, 6, 28), "r", {})["rows_written"] == 0


def test_research_writes(cfg, monkeypatch):
    monkeypatch.setattr(
        research,
        "fetch_institutional_holdings",
        lambda trade_date, backfill=False, config=None: pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "holder_type": ["fund"],
                "report_period": ["2024Q1"],
                "holding_shares": [1.0],
                "holding_ratio": [0.1],
                "holding_mv": [1.0],
            }
        ),
    )
    monkeypatch.setattr(
        research,
        "fetch_analyst_consensus",
        lambda trade_date, config=None: pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "forecast_date": [trade_date],
                "forecast_year": [2024],
                "eps_forecast": [1.0],
                "pe_forecast": [20.0],
                "target_price": [100.0],
                "rating": ["buy"],
                "analyst_count": [3],
            }
        ),
    )
    assert (
        research.step_institutional_holdings(cfg, date(2024, 6, 28), "r1", {})["rows_written"] == 1
    )
    assert research.step_analyst_consensus(cfg, date(2024, 6, 28), "r2", {})["rows_written"] == 1


def test_macro_risk_guards_and_writes(cfg, monkeypatch):
    cfg.sources["eastmoney"] = False
    with pytest.raises(RuntimeError, match="disabled"):
        macro_risk.step_share_unlock_schedule(cfg, date(2024, 6, 28), "r", {})
    cfg.sources["eastmoney"] = True
    cfg.sources["cninfo"] = False
    with pytest.raises(RuntimeError, match="disabled"):
        macro_risk.step_regulatory_events(cfg, date(2024, 6, 28), "r", {})
    cfg.sources["cninfo"] = True

    StateStore(cfg.meta_root).set_date("macro_indicators", date(2024, 6, 27))
    StateStore(cfg.meta_root).set_date("share_unlock_schedule", date(2024, 6, 27))
    StateStore(cfg.meta_root).set_date("regulatory_events", date(2024, 6, 27))

    monkeypatch.setattr(
        macro_risk,
        "fetch_macro_indicators",
        lambda d, config=None: pl.DataFrame(
            {
                "indicator_id": ["gdp"],
                "obs_date": [d],
                "value": [1.0],
                "frequency": ["q"],
            }
        ),
    )
    monkeypatch.setattr(
        macro_risk,
        "fetch_share_unlock_schedule",
        lambda d: pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "unlock_date": [d],
                "unlock_shares": [1.0],
                "unlock_ratio": [0.01],
                "unlock_type": ["restricted"],
            }
        ),
    )
    monkeypatch.setattr(
        macro_risk,
        "fetch_regulatory_events",
        lambda d, config=None: pl.DataFrame(
            {
                "event_id": ["e1"],
                "symbol": ["600519.SH"],
                "event_date": [d],
                "event_type": ["inquiry"],
                "title": ["t"],
            }
        ),
    )
    assert macro_risk.step_macro_indicators(cfg, date(2024, 6, 28), "r", {})["rows_written"] == 1
    assert (
        macro_risk.step_share_unlock_schedule(cfg, date(2024, 6, 28), "r", {})["rows_written"] == 1
    )
    assert macro_risk.step_regulatory_events(cfg, date(2024, 6, 28), "r", {})["rows_written"] == 1


def test_newsboard_and_commodity(cfg, monkeypatch):
    cfg.sources["eastmoney"] = False
    with pytest.raises(RuntimeError, match="disabled"):
        newsboard.step_flash_news_wire(cfg, date(2024, 6, 28), "r", {})
    with pytest.raises(RuntimeError, match="disabled"):
        newsboard.step_economic_calendar(cfg, date(2024, 6, 28), "r", {})
    cfg.sources["eastmoney"] = True
    cfg.sources["sina"] = False
    # eastmoney still on → commodity ok path
    StateStore(cfg.meta_root).set_date("commodity_bars", date(2024, 6, 27))
    monkeypatch.setattr(
        commodity,
        "fetch_commodity_bars",
        lambda d, config=None: pl.DataFrame(
            {
                "symbol": ["AU0.SHF"],
                "name": ["黄金主连"],
                "exchange": ["SHF"],
                "trade_date": [d],
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [1],
                "amount": [1.0],
                "open_interest": [1.0],
            }
        ),
    )
    assert commodity.step_commodity_bars(cfg, date(2024, 6, 28), "r", {})["rows_written"] == 1

    cfg.sources["eastmoney"] = False
    with pytest.raises(RuntimeError, match="both eastmoney and sina"):
        commodity.step_commodity_bars(cfg, date(2024, 6, 28), "r", {})

    cfg.sources["eastmoney"] = True
    monkeypatch.setattr(
        newsboard,
        "fetch_economic_calendar",
        lambda d: pl.DataFrame(
            {
                "event_id": ["e1"],
                "event_date": [d],
                "event_time": ["10:00"],
                "country": ["CN"],
                "indicator": ["CPI"],
                "importance": [1],
                "forecast": [1.0],
                "previous": [1.0],
                "actual": [1.0],
                "unit": ["%"],
            }
        ),
    )
    assert newsboard.step_economic_calendar(cfg, date(2024, 6, 28), "r", {})["rows_written"] == 1

    with pytest.raises(RuntimeError, match="no rows"):
        monkeypatch.setattr(newsboard, "fetch_economic_calendar", lambda d: pl.DataFrame())
        newsboard.step_economic_calendar(cfg, date(2024, 6, 28), "r", {})
