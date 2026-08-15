"""Offline coverage for fundamentals step wrappers and valuation backfill."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import Config
from cn_market_lake.steps import fundamentals as fund


@pytest.fixture
def cfg(tmp_path):
    c = Config(data_root=tmp_path / "data")
    c.staging_root.mkdir(parents=True)
    return c


def test_financial_statement_items_disabled(cfg):
    cfg.sources["eastmoney"] = False
    with pytest.raises(RuntimeError, match="eastmoney source disabled"):
        fund.step_financial_statement_items(cfg, date(2024, 6, 28), "run-1", {})


def test_financial_statement_items_empty(cfg, monkeypatch):
    monkeypatch.setattr(
        fund,
        "fetch_financial_statement_items",
        lambda trade_date, backfill=False, config=None: pl.DataFrame(),
    )
    result = fund.step_financial_statement_items(cfg, date(2024, 6, 28), "run-1", {})
    assert result == {"rows_read": 0, "rows_written": 0}


def test_financial_statement_items_writes_staging(cfg, monkeypatch):
    seen = {}

    def fake_fetch(trade_date, backfill=False, config=None):
        seen["backfill"] = backfill
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "report_period": ["2024Q1"],
                "statement_type": ["income"],
                "item_code": ["roe"],
                "item_value": [0.12],
                "announce_date": [date(2024, 4, 20)],
            }
        )

    monkeypatch.setattr(fund, "fetch_financial_statement_items", fake_fetch)
    cfg._backfill = True
    result = fund.step_financial_statement_items(cfg, date(2024, 6, 28), "run-fsi", {})
    assert seen["backfill"] is True
    assert result["rows_written"] == 1
    assert list(cfg.staging_root.glob("financial_statement_items/**/*.parquet"))


def test_valuation_metrics_disabled(cfg):
    cfg.sources["eastmoney"] = False
    with pytest.raises(RuntimeError, match="eastmoney source disabled"):
        fund.step_valuation_metrics(cfg, date(2024, 6, 28), "run-1", {})


def test_backfill_valuation_locked_nothing_to_do(cfg, monkeypatch):
    monkeypatch.setattr(
        "cn_market_lake.storage.valuation_orphans.purge_valuation_orphan_symbols",
        lambda config: {"purged": 0},
    )
    monkeypatch.setattr(fund, "load_symbols", lambda config: ["600519.SH"])
    monkeypatch.setattr(fund, "load_bar_universe", lambda config: {"600519.SH"})
    monkeypatch.setattr(fund, "_symbols_needing_backfill", lambda config, universe: [])
    monkeypatch.setattr(fund, "_valuation_history_end", lambda config, trade_date: date(2024, 6, 1))
    result = fund._backfill_valuation_metrics_locked(cfg, date(2024, 6, 28), "run-v")
    assert result["rows_written"] == 0
    assert "already backfilled" in result["note"]


def test_backfill_valuation_locked_history_end_before_start(cfg, monkeypatch):
    monkeypatch.setattr(
        "cn_market_lake.storage.valuation_orphans.purge_valuation_orphan_symbols",
        lambda config: {"purged": 0},
    )
    monkeypatch.setattr(fund, "load_symbols", lambda config: ["600519.SH"])
    monkeypatch.setattr(fund, "load_bar_universe", lambda config: {"600519.SH"})
    monkeypatch.setattr(fund, "_symbols_needing_backfill", lambda config, universe: ["600519.SH"])
    monkeypatch.setattr(fund, "_valuation_history_end", lambda config, trade_date: date(2010, 1, 1))
    result = fund._backfill_valuation_metrics_locked(cfg, date(2024, 6, 28), "run-v")
    assert "history_end before backfill start" in result["note"]


def test_backfill_valuation_locked_writes_chunks(cfg, monkeypatch):
    monkeypatch.setattr(
        "cn_market_lake.storage.valuation_orphans.purge_valuation_orphan_symbols",
        lambda config: {"purged": 1},
    )
    monkeypatch.setattr(fund, "load_symbols", lambda config: ["600519.SH", "000001.SZ"])
    monkeypatch.setattr(fund, "load_bar_universe", lambda config: {"600519.SH", "000001.SZ"})
    monkeypatch.setattr(
        fund,
        "_symbols_needing_backfill",
        lambda config, universe: ["600519.SH", "000001.SZ"],
    )
    monkeypatch.setattr(fund, "_valuation_history_end", lambda config, trade_date: date(2024, 6, 1))

    def fake_history(batch, start, end, config=None):
        df = pl.DataFrame(
            {
                "symbol": batch,
                "trade_date": [date(2024, 1, 2)] * len(batch),
                "pe_ttm": [10.0] * len(batch),
                "pb": [1.0] * len(batch),
                "ps_ttm": [2.0] * len(batch),
                "total_mv": [1e9] * len(batch),
                "float_mv": [1e9] * len(batch),
            }
        )
        return df, []

    monkeypatch.setattr(
        "cn_market_lake.adapters.baostock.valuation.fetch_valuation_history",
        fake_history,
    )
    # Shrink chunk size so the loop body runs once with our tiny universe.
    monkeypatch.setattr(fund, "_VALUATION_BACKFILL_CHUNK", 50)
    result = fund._backfill_valuation_metrics_locked(cfg, date(2024, 6, 28), "run-chunk")
    assert result["rows_written"] == 2
    assert result["symbols_todo"] == 2
    assert list(cfg.staging_root.glob("valuation_metrics/**/batch-00000/*.parquet")) or list(
        cfg.staging_root.glob("valuation_metrics/**/*.parquet")
    )


def test_backfill_valuation_locked_aborts_on_runtime_error(cfg, monkeypatch):
    monkeypatch.setattr(
        "cn_market_lake.storage.valuation_orphans.purge_valuation_orphan_symbols",
        lambda config: {"purged": 0},
    )
    monkeypatch.setattr(fund, "load_symbols", lambda config: ["600519.SH"])
    monkeypatch.setattr(fund, "load_bar_universe", lambda config: {"600519.SH"})
    monkeypatch.setattr(fund, "_symbols_needing_backfill", lambda config, universe: ["600519.SH"])
    monkeypatch.setattr(fund, "_valuation_history_end", lambda config, trade_date: date(2024, 6, 1))

    def boom(*a, **k):
        raise RuntimeError("baostock banned")

    monkeypatch.setattr(
        "cn_market_lake.adapters.baostock.valuation.fetch_valuation_history",
        boom,
    )
    result = fund._backfill_valuation_metrics_locked(cfg, date(2024, 6, 28), "run-abort")
    assert result["rows_written"] == 0
    assert "baostock banned" in result["aborted"]
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["code"] == "baostock_backfill_incomplete"
