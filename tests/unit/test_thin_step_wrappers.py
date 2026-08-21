"""Offline smoke for thin HTTP/derived step wrappers (disabled + empty + write)."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.steps import commodity, macro_risk, newsboard, research, rotation
from cnequity.steps.common import SnapshotBackfillError
from cnequity.storage.state import StateStore


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
    with pytest.raises(RuntimeError, match="institutional_holdings: no rows returned"):
        research.step_institutional_holdings(cfg, date(2024, 6, 28), "r", {})
    with pytest.raises(RuntimeError, match="analyst_consensus: no rows returned"):
        research.step_analyst_consensus(cfg, date(2024, 6, 28), "r", {})


def test_research_writes(cfg, monkeypatch):
    monkeypatch.setattr(
        research,
        "fetch_institutional_holdings",
        lambda trade_date, backfill=False, config=None: pl.DataFrame(
            {
                "symbol": [f"{600000 + i:06d}.SH" for i in range(100)],
                "holder_type": ["fund"] * 100,
                "report_period": ["2024Q1"] * 100,
                "holding_shares": [1.0] * 100,
                "holding_ratio": [0.1] * 100,
                "holding_mv": [1.0] * 100,
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
        research.step_institutional_holdings(cfg, date(2024, 6, 28), "r1", {})["rows_written"]
        == 100
    )
    assert research.step_analyst_consensus(cfg, date(2024, 6, 28), "r2", {})["rows_written"] == 1


def test_institutional_holdings_rejects_partial_period(cfg, monkeypatch):
    monkeypatch.setattr(
        research,
        "fetch_institutional_holdings",
        lambda *args, **kwargs: pl.DataFrame(
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

    with pytest.raises(RuntimeError, match="institutional_holdings: incomplete quarterly snapshot"):
        research.step_institutional_holdings(cfg, date(2024, 6, 28), "r-partial", {})


def test_snapshot_steps_reject_backfill(cfg, monkeypatch):
    cfg._backfill = True
    calls = {"consensus": 0, "calendar": 0}

    def _consensus(*args, **kwargs):
        calls["consensus"] += 1
        return pl.DataFrame()

    def _calendar(*args, **kwargs):
        calls["calendar"] += 1
        return pl.DataFrame()

    monkeypatch.setattr(research, "fetch_analyst_consensus", _consensus)
    monkeypatch.setattr(newsboard, "fetch_economic_calendar", _calendar)

    with pytest.raises(SnapshotBackfillError, match="analyst_consensus"):
        research.step_analyst_consensus(cfg, date(2024, 6, 28), "r-consensus", {})
    with pytest.raises(SnapshotBackfillError, match="economic_calendar"):
        newsboard.step_economic_calendar(cfg, date(2024, 6, 28), "r-calendar", {})
    assert calls == {"consensus": 0, "calendar": 0}


def test_economic_calendar_does_not_use_a_future_event_watermark():
    from cnequity.domain.datasets import DATASETS

    assert DATASETS["economic_calendar"].watermark is False


def test_institutional_backfill_surfaces_missing_quarters(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2020, 1, 1)
    cfg._backfill_end = date(2020, 6, 30)
    monkeypatch.setattr(
        research,
        "fetch_institutional_holdings",
        lambda *args, **kwargs: pl.DataFrame(),
    )

    result = research.step_institutional_holdings(cfg, date(2026, 6, 30), "r-gap", {})

    assert result["status"] == "warning"
    assert result["missing_periods"] == 2
    assert result["context_updates"]["audit_findings"][0]["check"] == ("backfill_missing_quarters")


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
        lambda d, config=None, strict=False: pl.DataFrame(
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
        lambda d, config=None: pl.DataFrame(
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


def test_flash_news_wire_passes_config_to_fetcher(cfg, monkeypatch):
    seen = {}

    def fake_fetch(trade_date, config=None):
        seen["trade_date"] = trade_date
        seen["config"] = config
        return pl.DataFrame({"value": [1]})

    def fake_run(config, trade_date, run_id, dataset, fetch_fn, **kwargs):
        assert config is cfg
        assert trade_date == date(2024, 6, 28)
        assert run_id == "r"
        assert dataset == "flash_news_wire"
        assert kwargs["source"] == "eastmoney"
        assert kwargs["allow_empty"] is False
        frame = fetch_fn(trade_date)
        return {"rows_written": frame.height}

    monkeypatch.setattr(newsboard, "fetch_flash_news_wire", fake_fetch)
    monkeypatch.setattr(newsboard, "run_incremental_fetched", fake_run)

    assert newsboard.step_flash_news_wire(cfg, date(2024, 6, 28), "r", {}) == {"rows_written": 1}
    assert seen == {"trade_date": date(2024, 6, 28), "config": cfg}


def test_rotation_snapshot_steps_reject_empty_feeds(cfg, monkeypatch):
    monkeypatch.setattr(rotation, "fetch_hot_rank", lambda *a, **k: pl.DataFrame())
    monkeypatch.setattr(rotation, "fetch_sector_fund_flow", lambda *a, **k: pl.DataFrame())

    with pytest.raises(RuntimeError, match="hot_rank: no rows returned"):
        rotation.step_hot_rank(cfg, date(2024, 6, 28), "r-hot-empty", {})
    with pytest.raises(RuntimeError, match="sector_fund_flow: no rows returned"):
        rotation.step_sector_fund_flow(cfg, date(2024, 6, 28), "r-flow-empty", {})


def test_rotation_hot_rank_accepts_partial_snapshot(cfg, monkeypatch):
    monkeypatch.setattr(
        rotation,
        "fetch_hot_rank",
        lambda *a, **k: pl.DataFrame(
            {
                "symbol": ["600519.SH", "000001.SZ"],
                "trade_date": [date(2024, 6, 28), date(2024, 6, 28)],
                "rank": [1, 2],
                "rank_change": [0, 0],
                "hist_rank": [1, 2],
            }
        ),
    )

    result = rotation.step_hot_rank(cfg, date(2024, 6, 28), "r-hot-partial", {})
    assert result["rows_written"] == 2


def test_sector_fund_flow_rejects_missing_board_category(cfg, monkeypatch):
    monkeypatch.setattr(
        rotation,
        "fetch_sector_fund_flow",
        lambda *a, **k: pl.DataFrame(
            {
                "sector_code": ["BK0001"],
                "board_type": ["concept"],
                "trade_date": [date(2024, 6, 28)],
            }
        ),
    )

    with pytest.raises(RuntimeError, match=r"missing board type\(s\): industry"):
        rotation.step_sector_fund_flow(cfg, date(2024, 6, 28), "r-flow-partial", {})


def test_sector_fund_flow_rejects_thin_board_category(cfg, monkeypatch):
    monkeypatch.setattr(
        rotation,
        "fetch_sector_fund_flow",
        lambda *a, **k: pl.DataFrame(
            {
                "sector_code": [f"HY{i:04d}" for i in range(49)]
                + [f"BK{i:04d}" for i in range(100)],
                "board_type": ["industry"] * 49 + ["concept"] * 100,
                "trade_date": [date(2024, 6, 28)] * 149,
            }
        ),
    )

    with pytest.raises(RuntimeError, match=r"industry=49 \(minimum 50\)"):
        rotation.step_sector_fund_flow(cfg, date(2024, 6, 28), "r-flow-thin", {})
    cfg.sources["eastmoney"] = True
    cfg.sources["sina"] = False
    # eastmoney still on → commodity ok path
    StateStore(cfg.meta_root).set_date("commodity_bars", date(2024, 6, 27))
    monkeypatch.setattr(
        commodity,
        "fetch_commodity_bars",
        lambda d, config=None, strict=False: pl.DataFrame(
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

    monkeypatch.setattr(commodity, "fetch_commodity_bars", lambda *a, **k: pl.DataFrame())
    with pytest.raises(RuntimeError, match="commodity_bars: no rows returned"):
        commodity.step_commodity_bars(cfg, date(2024, 6, 28), "r-empty", {})

    cfg.sources["eastmoney"] = False
    with pytest.raises(RuntimeError, match="both eastmoney and sina"):
        commodity.step_commodity_bars(cfg, date(2024, 6, 28), "r", {})

    cfg.sources["eastmoney"] = True
    monkeypatch.setattr(
        newsboard,
        "fetch_economic_calendar",
        lambda d, config=None: pl.DataFrame(
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
        monkeypatch.setattr(
            newsboard, "fetch_economic_calendar", lambda d, config=None: pl.DataFrame()
        )
        newsboard.step_economic_calendar(cfg, date(2024, 6, 28), "r", {})
