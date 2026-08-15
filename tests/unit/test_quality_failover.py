"""Failover snapshot helpers (quality/failover.py) — offline, mocked adapters."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cn_market_lake.config import Config, FailoverDatasetSpec
from cn_market_lake.quality import failover as fo


def _bars_df(symbol: str = "600519.SH", trade_date: date = date(2024, 6, 28)) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [symbol],
            "trade_date": [trade_date],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [1000],
            "amount": [1_000_000.0],
        }
    )


def _cfg(tmp_path, **overrides) -> Config:
    kwargs = {
        "data_root": tmp_path / "data",
        "tdx_enabled": False,
        "failover_enabled": True,
        "sources": {"eastmoney": True, "tdx_protocol": True},
        "failover_datasets": [
            FailoverDatasetSpec(name="daily_bars", primary="tdx_protocol", backup="eastmoney"),
            FailoverDatasetSpec(
                name="corporate_actions", primary="tdx_protocol", backup="eastmoney"
            ),
        ],
    }
    kwargs.update(overrides)
    return Config(**kwargs)


def test_failover_spec_disabled_returns_none(tmp_path):
    cfg = _cfg(tmp_path, failover_enabled=False)
    assert fo.failover_spec(cfg, "daily_bars") is None


def test_failover_spec_skips_non_matching_and_finds_match(tmp_path):
    cfg = _cfg(tmp_path)
    spec = fo.failover_spec(cfg, "corporate_actions")
    assert spec is not None
    assert spec.name == "corporate_actions"


def test_failover_spec_returns_none_when_unknown_dataset(tmp_path):
    cfg = _cfg(tmp_path)
    assert fo.failover_spec(cfg, "unknown_dataset") is None


def test_write_backup_snapshot_skips_empty_dataframe(tmp_path):
    cfg = _cfg(tmp_path)
    fo.write_backup_snapshot(
        cfg,
        "daily_bars",
        pl.DataFrame(),
        run_id="run-1",
        batch_id="batch-1",
        source="eastmoney",
    )
    assert not (cfg.meta_root / "source_snapshots").exists()


def test_write_backup_snapshot_handles_no_path_returned(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        "cn_market_lake.quality.failover.SnapshotStore.write", lambda self, *a, **k: None
    )
    # Must not raise even though the store reports it wrote nothing.
    fo.write_backup_snapshot(
        cfg,
        "daily_bars",
        _bars_df(),
        run_id="run-1",
        batch_id="batch-1",
        source="eastmoney",
    )


def test_snapshot_daily_bars_clist_noop_when_backup_disabled(tmp_path):
    cfg = _cfg(tmp_path, sources={"eastmoney": False, "tdx_protocol": True})
    out = fo.snapshot_daily_bars_clist(cfg, trade_date=date(2024, 6, 28), run_id="run-1")
    assert out.is_empty()


def test_snapshot_daily_bars_clist_returns_given_df_when_spec_missing(tmp_path):
    cfg = _cfg(tmp_path, failover_enabled=False)
    given = _bars_df()
    out = fo.snapshot_daily_bars_clist(cfg, trade_date=date(2024, 6, 28), run_id="run-1", df=given)
    assert out is given


def test_snapshot_daily_bars_clist_fetches_when_df_not_provided(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    calls: list = []
    monkeypatch.setattr(
        "cn_market_lake.quality.failover.fetch_daily_bars_clist",
        lambda trade_date, symbols=None, config=None: calls.append(1) or _bars_df(),
    )
    out = fo.snapshot_daily_bars_clist(cfg, trade_date=date(2024, 6, 28), run_id="run-1")
    assert calls == [1]
    assert out.height == 1
    assert out["source"].to_list() == ["eastmoney"]
    snap_dir = cfg.meta_root / "source_snapshots" / "daily_bars"
    assert snap_dir.exists()


def test_snapshot_daily_bars_clist_empty_fetch_returns_empty(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        "cn_market_lake.quality.failover.fetch_daily_bars_clist",
        lambda trade_date, symbols=None, config=None: pl.DataFrame(),
    )
    out = fo.snapshot_daily_bars_clist(cfg, trade_date=date(2024, 6, 28), run_id="run-1")
    assert out.is_empty()


def test_snapshot_daily_bars_backup_noop_when_spec_missing(tmp_path):
    cfg = _cfg(tmp_path, failover_enabled=False)
    fo.snapshot_daily_bars_backup(
        cfg,
        symbols=["600519.SH"],
        start=date(2024, 6, 1),
        end=date(2024, 6, 28),
        run_id="run-1",
        batch_id="batch-1",
    )
    assert not (cfg.meta_root / "source_snapshots").exists()


def test_snapshot_daily_bars_backup_noop_for_single_day_window(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        "cn_market_lake.quality.failover.fetch_em_daily_bars",
        lambda *a, **k: pytest.fail("must not fetch a single-day tip window"),
    )
    fo.snapshot_daily_bars_backup(
        cfg,
        symbols=["600519.SH"],
        start=date(2024, 6, 28),
        end=date(2024, 6, 28),
        run_id="run-1",
        batch_id="batch-1",
    )


def test_snapshot_daily_bars_backup_noop_when_fetch_empty(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        "cn_market_lake.quality.failover.fetch_em_daily_bars",
        lambda *a, **k: pl.DataFrame(),
    )
    fo.snapshot_daily_bars_backup(
        cfg,
        symbols=["600519.SH"],
        start=date(2024, 6, 1),
        end=date(2024, 6, 28),
        run_id="run-1",
        batch_id="batch-1",
    )
    assert not (cfg.meta_root / "source_snapshots").exists()


def test_snapshot_daily_bars_backup_writes_snapshot(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        "cn_market_lake.quality.failover.fetch_em_daily_bars",
        lambda symbols, start, end: _bars_df(trade_date=end),
    )
    fo.snapshot_daily_bars_backup(
        cfg,
        symbols=["600519.SH"],
        start=date(2024, 6, 1),
        end=date(2024, 6, 28),
        run_id="run-1",
        batch_id="batch-1",
    )
    assert (cfg.meta_root / "source_snapshots" / "daily_bars").exists()


def test_snapshot_corporate_actions_backup_noop_when_not_backfill(tmp_path):
    cfg = _cfg(tmp_path)
    fo.snapshot_corporate_actions_backup(
        cfg, trade_date=date(2024, 6, 28), run_id="run-1", backfill=False
    )
    assert not (cfg.meta_root / "source_snapshots").exists()


def test_snapshot_corporate_actions_backup_noop_when_spec_missing(tmp_path):
    cfg = _cfg(tmp_path, failover_enabled=False)
    fo.snapshot_corporate_actions_backup(
        cfg, trade_date=date(2024, 6, 28), run_id="run-1", backfill=True
    )
    assert not (cfg.meta_root / "source_snapshots").exists()


def test_snapshot_corporate_actions_backup_noop_when_fetch_empty(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        "cn_market_lake.quality.failover.fetch_corporate_actions_eastmoney",
        lambda trade_date, backfill, config=None: pl.DataFrame(),
    )
    fo.snapshot_corporate_actions_backup(
        cfg, trade_date=date(2024, 6, 28), run_id="run-1", backfill=True
    )
    assert not (cfg.meta_root / "source_snapshots").exists()


def test_snapshot_corporate_actions_backup_writes_snapshot(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    ca_df = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "ex_date": [date(2024, 6, 28)],
            "action_type": ["cash_dividend"],
            "cash_dividend": [1.0],
            "bonus_ratio": [0.0],
            "transfer_ratio": [0.0],
            "allotment_ratio": [None],
            "allotment_price": [None],
        },
        schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
    )
    monkeypatch.setattr(
        "cn_market_lake.quality.failover.fetch_corporate_actions_eastmoney",
        lambda trade_date, backfill, config=None: ca_df,
    )
    fo.snapshot_corporate_actions_backup(
        cfg, trade_date=date(2024, 6, 28), run_id="run-1", backfill=True
    )
    assert (cfg.meta_root / "source_snapshots" / "corporate_actions").exists()


def test_snapshot_corporate_actions_tdx_backup_noop_when_no_symbols(tmp_path):
    cfg = _cfg(tmp_path)
    fo.snapshot_corporate_actions_tdx_backup(
        cfg,
        trade_date=date(2024, 6, 28),
        symbols=[],
        run_id="run-1",
        rate_limit=None,
    )
    assert not (cfg.meta_root / "source_snapshots").exists()


def test_snapshot_corporate_actions_tdx_backup_noop_when_tdx_disabled(tmp_path):
    cfg = _cfg(tmp_path, tdx_enabled=False)
    fo.snapshot_corporate_actions_tdx_backup(
        cfg,
        trade_date=date(2024, 6, 28),
        symbols=["600519.SH"],
        run_id="run-1",
        rate_limit=None,
    )
    assert not (cfg.meta_root / "source_snapshots").exists()


def test_snapshot_corporate_actions_tdx_backup_noop_when_empty(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, tdx_enabled=True)
    monkeypatch.setattr(
        "cn_market_lake.adapters.tdx_protocol.client.quotes_client_factory",
        lambda config: lambda: None,
    )
    monkeypatch.setattr(
        "cn_market_lake.adapters.tdx_protocol.corporate_actions.fetch_corporate_actions_tdx",
        lambda symbols, **k: pl.DataFrame(),
    )
    fo.snapshot_corporate_actions_tdx_backup(
        cfg,
        trade_date=date(2024, 6, 28),
        symbols=["600519.SH"],
        run_id="run-1",
        rate_limit=None,
    )
    assert not (cfg.meta_root / "source_snapshots").exists()


def test_snapshot_corporate_actions_tdx_backup_writes_snapshot(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, tdx_enabled=True)
    tdx_df = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "ex_date": [date(2024, 6, 28)],
            "action_type": ["bonus"],
            "cash_dividend": [0.0],
            "bonus_ratio": [0.3],
            "transfer_ratio": [0.0],
            "allotment_ratio": [None],
            "allotment_price": [None],
        },
        schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
    )
    monkeypatch.setattr(
        "cn_market_lake.adapters.tdx_protocol.client.quotes_client_factory",
        lambda config: lambda: None,
    )
    monkeypatch.setattr(
        "cn_market_lake.adapters.tdx_protocol.corporate_actions.fetch_corporate_actions_tdx",
        lambda symbols, **k: tdx_df,
    )
    fo.snapshot_corporate_actions_tdx_backup(
        cfg,
        trade_date=date(2024, 6, 28),
        symbols=["600519.SH"],
        run_id="run-1",
        rate_limit=None,
    )
    assert (cfg.meta_root / "source_snapshots" / "corporate_actions").exists()
