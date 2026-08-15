"""TDX tip gaps route through EastMoney clist (ADR-0005), not per-symbol kline."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cn_market_lake.adapters.eastmoney.bars import fetch_daily_bars_clist
from cn_market_lake.config import Config, FailoverDatasetSpec
from cn_market_lake.domain.schemas import with_provenance
from cn_market_lake.orchestrator.manifest import Manifest
from cn_market_lake.steps.bars import (
    _finish_daily_bars,
    _gapfill_tip_via_clist,
    _reject_preopen_placeholder,
    _staged_daily_bar_symbols,
)
from cn_market_lake.storage import StagingWriter
from cn_market_lake.storage.layout import init_data_layout


def _cfg(tmp_path) -> Config:
    cfg = Config(
        data_root=tmp_path / "data",
        workers=1,
        batch_size=10,
        tdx_allow_mock=True,
        failover_enabled=True,
        failover_datasets=[
            FailoverDatasetSpec(
                name="daily_bars",
                primary="tdx_protocol",
                backup="eastmoney",
            )
        ],
        sources={"eastmoney": True, "tdx_protocol": True, "sina": True},
    )
    init_data_layout(cfg)
    return cfg


def _bar_frame(symbols: list[str], d: date, *, volume: int = 100) -> pl.DataFrame:
    n = len(symbols)
    return with_provenance(
        pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [d] * n,
                "open": [10.0] * n,
                "high": [11.0] * n,
                "low": [9.0] * n,
                "close": [10.5] * n,
                "volume": [volume] * n,
                "amount": [1000.0] * n,
            }
        ),
        source="tdx_protocol",
        data_version="v1",
    )


def test_fetch_daily_bars_clist_stamps_trade_date(monkeypatch):
    raw = [
        {
            "f12": "600519",
            "f13": 1,
            "f17": 100.0,
            "f15": 102.0,
            "f16": 99.0,
            "f2": 101.0,
            "f5": 1000,
            "f6": 1e6,
        },
        {
            "f12": "000001",
            "f13": 0,
            "f17": 10.0,
            "f15": 11.0,
            "f16": 9.0,
            "f2": 10.5,
            "f5": 2000,
            "f6": 2e6,
        },
    ]

    class _Client:
        def close(self):
            pass

    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.bars.fetch_clist_pages",
        lambda client, fields: raw,
    )
    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.bars.EastMoneyClient",
        lambda **kwargs: _Client(),
    )
    tip = date(2026, 7, 24)
    df = fetch_daily_bars_clist(tip, symbols={"600519.SH"})
    assert df.height == 1
    assert df["symbol"].to_list() == ["600519.SH"]
    assert df["trade_date"].to_list() == [tip]
    assert df["open"].to_list() == [100.0]
    assert df["high"].to_list() == [102.0]
    assert df["low"].to_list() == [99.0]
    assert df["close"].to_list() == [101.0]


def test_tip_gapfill_writes_only_missing_keys(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core")
    tip = date(2026, 7, 24)
    # TDX already staged one symbol.
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-batch-0", _bar_frame(["600519.SH"], tip)
    )

    clist = pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ", "600000.SH"],
            "trade_date": [tip, tip, tip],
            "open": [1.0, 2.0, 3.0],
            "high": [1.0, 2.0, 3.0],
            "low": [1.0, 2.0, 3.0],
            "close": [1.0, 2.0, 3.0],
            "volume": [10, 20, 30],
            "amount": [100.0, 200.0, 300.0],
        }
    )
    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.bars.fetch_daily_bars_clist",
        lambda trade_date, symbols=None, client=None, config=None: clist,
    )

    out = _gapfill_tip_via_clist(
        cfg,
        tip,
        run_id,
        expected_symbols=["600519.SH", "000001.SZ", "600000.SH"],
    )
    assert out["filled"] is True
    assert out["rows_written"] == 2
    staged = _staged_daily_bar_symbols(cfg, run_id, tip)
    assert staged == {"600519.SH", "000001.SZ", "600000.SH"}
    # Gap-fill batch must not re-stage the TDX key.
    gap_files = list((cfg.staging_root / "daily_bars" / f"run_id={run_id}").rglob("*.parquet"))
    gap_only = [f for f in gap_files if "em-clist-gapfill" in str(f)]
    assert gap_only
    gap_df = pl.read_parquet(gap_only[0])
    assert set(gap_df["symbol"].to_list()) == {"000001.SZ", "600000.SH"}
    assert gap_df["source"].unique().to_list() == ["eastmoney"]


def test_tip_tdx_fail_clist_recovers_step(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 7, 24)
    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.bars.fetch_daily_bars_clist",
        lambda trade_date, symbols=None, client=None, config=None: pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [tip],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.5],
                "volume": [100],
                "amount": [1000.0],
            }
        ),
    )
    result = _finish_daily_bars(
        cfg,
        tip,
        run_id,
        start=tip,
        end=tip,
        expected_tdx_symbols=["600519.SH"],
        tdx_result={
            "rows_read": 0,
            "rows_written": 0,
            "had_error": True,
            "failed_symbols": ["600519.SH"],
        },
        sina_result=None,
    )
    assert result["rows_written"] == 1
    assert any(
        f["check"] == "daily_bars_clist_gapfill"
        for f in result["context_updates"]["audit_findings"]
    )


def test_multiday_uses_kline_not_clist(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 28)
    clist_calls: list = []
    kline_calls: list = []

    def _clist(*a, **k):
        clist_calls.append(1)
        return pl.DataFrame()

    def _kline(symbols, s, e, **k):
        kline_calls.append(list(symbols))
        return pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [s] * len(symbols),
                "open": [1.0] * len(symbols),
                "high": [1.0] * len(symbols),
                "low": [1.0] * len(symbols),
                "close": [1.0] * len(symbols),
                "volume": [1] * len(symbols),
                "amount": [1.0] * len(symbols),
            }
        )

    monkeypatch.setattr("cn_market_lake.adapters.eastmoney.bars.fetch_daily_bars_clist", _clist)
    monkeypatch.setattr("cn_market_lake.adapters.eastmoney.bars.fetch_daily_bars", _kline)

    result = _finish_daily_bars(
        cfg,
        end,
        run_id,
        start=start,
        end=end,
        expected_tdx_symbols=["600519.SH"],
        tdx_result={
            "rows_read": 0,
            "rows_written": 0,
            "had_error": True,
            "failed_symbols": ["600519.SH"],
        },
        sina_result=None,
    )
    assert clist_calls == []
    assert kline_calls == [["600519.SH"]]
    assert result["rows_written"] == 1


def test_preopen_placeholder_still_rejects_clist_flat_zeros(tmp_path):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 7, 24)
    flat = with_provenance(
        pl.DataFrame(
            {
                "symbol": ["600519.SH", "000001.SZ"],
                "trade_date": [tip, tip],
                "open": [10.0, 10.0],
                "high": [10.0, 10.0],
                "low": [10.0, 10.0],
                "close": [10.0, 10.0],
                "volume": [0, 0],
                "amount": [0.0, 0.0],
            }
        ),
        source="eastmoney",
        data_version="v1",
    )
    StagingWriter(cfg.staging_root).write_batch("daily_bars", run_id, "em-clist-gapfill", flat)
    with pytest.raises(RuntimeError, match="pre-open placeholders"):
        _reject_preopen_placeholder(cfg, run_id, tip)
