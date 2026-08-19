"""trading_status failover coordinator + step degradation tests (offline)."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from cnequity import steps
from cnequity.quality import failover


def _spec(**kw):
    base = {
        "name": "trading_status",
        "primary": "eastmoney",
        "backup": "baostock",
        "compare_fields": None,
    }
    base.update(kw)
    return SimpleNamespace(**{k: v for k, v in base.items() if v is not None})


def _ts_frame(symbols, d):
    return pl.DataFrame(
        {
            "symbol": symbols,
            "trade_date": [d] * len(symbols),
            "is_trading": [True] * len(symbols),
            "status": ["normal"] * len(symbols),
        }
    )


D = date(2026, 8, 18)


def test_backup_refused_when_not_configured(monkeypatch):
    monkeypatch.setattr(failover, "failover_spec", lambda config, dataset: None)
    frame, meta = failover.fetch_trading_status_backup(object(), ["000001.SZ"], D)
    assert frame is None
    assert meta["failover_used"] is False


def test_backup_refused_when_source_disabled(monkeypatch):
    monkeypatch.setattr(failover, "failover_spec", lambda config, dataset: _spec())

    class _Cfg:
        sources = {"baostock": False}

    frame, meta = failover.fetch_trading_status_backup(_Cfg(), ["000001.SZ"], D)
    assert frame is None


def test_backup_refused_when_stale(monkeypatch):
    monkeypatch.setattr(failover, "failover_spec", lambda config, dataset: _spec())
    monkeypatch.setattr(failover, "_baostock_has_day", lambda config, trade_date: False)

    class _Cfg:
        sources = {"baostock": True}

    frame, meta = failover.fetch_trading_status_backup(_Cfg(), ["000001.SZ"], D)
    assert frame is None
    assert meta["freshness"] == "stale"


def test_backup_raises_when_baostock_also_fails(monkeypatch):
    monkeypatch.setattr(failover, "failover_spec", lambda config, dataset: _spec())
    monkeypatch.setattr(failover, "_baostock_has_day", lambda config, trade_date: True)

    def _boom(*_a, **_k):
        raise RuntimeError("baostock down")

    monkeypatch.setattr(failover, "fetch_trading_status_baostock", _boom)

    class _Cfg:
        sources = {"baostock": True}
        curated_root = object()

    with pytest.raises(RuntimeError, match="baostock down"):
        failover.fetch_trading_status_backup(_Cfg(), ["000001.SZ"], D)


def test_backup_split_sh_sz_fill_and_bj(monkeypatch):
    sh = ["600519.SH", "000001.SZ", "600984.SH"]
    bj = ["920001.BJ"]
    bs_df = _ts_frame(["600519.SH", "000001.SZ"], D)
    monkeypatch.setattr(failover, "failover_spec", lambda config, dataset: _spec())
    monkeypatch.setattr(failover, "_baostock_has_day", lambda config, trade_date: True)
    monkeypatch.setattr(
        failover, "fetch_trading_status_baostock", lambda symbols, day, config: bs_df
    )

    class _Cfg:
        sources = {"baostock": True}

    monkeypatch.setattr(
        failover,
        "_previous_statuses",
        lambda config, trade_date: {"600984.SH": "normal"},
    )
    # keep real _bj_rows but let it take the default path via fake EM failure
    monkeypatch.setattr(
        failover,
        "_fetch_suspended_symbols",
        lambda client, trade_date: (_ for _ in ()).throw(RuntimeError("em down")),
    )

    frame, meta = failover.fetch_trading_status_backup(_Cfg(), sh + bj, D)
    assert frame is not None
    assert set(frame.get_column("symbol").to_list()) == set(sh + bj)
    assert meta["failover_used"] is True
    assert meta["n_filled"] == 1  # 600984 filled normal
    assert meta["n_bj_defaulted"] == 1  # 920001 defaulted
    rows = {r["symbol"]: r for r in frame.iter_rows(named=True)}
    assert rows["600984.SH"]["status"] == "normal"
    assert rows["920001.BJ"]["status"] == "normal"


def test_backup_refused_on_fill_failure(monkeypatch):
    sh = ["600984.SH", "600519.SH"]
    bs_df = _ts_frame(["600519.SH"], D)
    monkeypatch.setattr(failover, "failover_spec", lambda config, dataset: _spec())
    monkeypatch.setattr(failover, "_baostock_has_day", lambda config, trade_date: True)
    monkeypatch.setattr(
        failover, "fetch_trading_status_baostock", lambda symbols, day, config: bs_df
    )
    monkeypatch.setattr(
        failover,
        "_previous_statuses",
        lambda config, trade_date: {"600984.SH": "suspended"},  # avoid wash
    )
    monkeypatch.setattr(failover, "_bj_rows", lambda config, bj, day: ([], 0))

    class _Cfg:
        sources = {"baostock": True}

    frame, meta = failover.fetch_trading_status_backup(_Cfg(), sh, D)
    assert frame is None
    assert meta["n_fill_failed"] == 1


def test_fill_missing_classification():
    previous = {"600984.SH": "suspended", "000002.SZ": "st", "600519.SH": "normal"}
    missing = ["600984.SH", "000002.SZ", "600519.SH", "300000.SZ"]
    fill_rows, failures, n = failover._fill_missing(missing, previous, D, threshold=10)
    assert failures == ["600984.SH", "000002.SZ"]
    assert n == 2  # 600519.SH normal + 300000.SZ no record


def test_step_marks_warning_and_baostock_provenance(monkeypatch, config):
    syms = ["000001.SZ", "600519.SH"]
    d = date(2026, 8, 18)

    def _incremental(cfg, dataset, trade_date, fetch_fn, allow_empty=False):
        return fetch_fn(d), []

    backup_df = _ts_frame(syms, d)
    meta = {
        "failover_used": True,
        "source": "baostock",
        "n_filled": 0,
        "n_bj_defaulted": 2,
        "n_fill_failed": 0,
        "freshness": "fresh",
    }

    monkeypatch.setattr(steps.reference, "fetch_incremental_daily", _incremental)
    monkeypatch.setattr(
        steps.reference,
        "fetch_trading_status",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("em down")),
    )
    monkeypatch.setattr(
        steps.reference, "fetch_trading_status_backup", lambda cfg, symbols, day: (backup_df, meta)
    )
    snapshot_calls: list = []
    monkeypatch.setattr(
        steps.reference,
        "snapshot_trading_status_backup",
        lambda cfg, *, df, run_id, trade_date: snapshot_calls.append((df, run_id)),
    )
    captured: dict = {}

    def _write(cfg, run_id, dataset, df):
        captured["df"] = df
        return {"rows_read": df.height, "rows_written": df.height}

    monkeypatch.setattr(steps.reference, "write_simple", _write)

    result = steps.reference.step_trading_status(config, d, "run-1", {"symbols": syms})

    assert result["status"] == "warning"
    findings = result["context_updates"]["audit_findings"]
    assert any(f["check"] == "failover_degraded" for f in findings)
    assert snapshot_calls, "backup snapshot must be written"
    assert set(captured["df"].get_column("source").to_list()) == {"baostock"}


def test_step_primary_success_uses_eastmoney_source(monkeypatch, config):
    syms = ["000001.SZ"]
    d = date(2026, 8, 18)
    frame = _ts_frame(syms, d)

    def _incremental(cfg, dataset, trade_date, fetch_fn, allow_empty=False):
        return fetch_fn(d), []

    monkeypatch.setattr(steps.reference, "fetch_incremental_daily", _incremental)
    monkeypatch.setattr(steps.reference, "fetch_trading_status", lambda *a, **k: frame)
    backup_calls: list = []
    monkeypatch.setattr(
        steps.reference,
        "fetch_trading_status_backup",
        lambda cfg, symbols, day: backup_calls.append((symbols, day)) or (None, {}),
    )
    captured: dict = {}

    def _write(cfg, run_id, dataset, df):
        captured["df"] = df
        return {"rows_read": df.height, "rows_written": df.height}

    monkeypatch.setattr(steps.reference, "write_simple", _write)
    result = steps.reference.step_trading_status(config, d, "run-2", {"symbols": syms})
    assert "status" not in result or result["status"] == "success"
    assert not backup_calls
    assert set(captured["df"].get_column("source").to_list()) == {"eastmoney"}


def test_backup_fills_scope_defaults_without_threshold(monkeypatch):
    sh = ["510300.SH", "600519.SH"]  # ETF missing from the A-share snapshot
    bs_df = _ts_frame(["600519.SH"], D)
    monkeypatch.setattr(failover, "failover_spec", lambda config, dataset: _spec())
    monkeypatch.setattr(failover, "_baostock_has_day", lambda config, trade_date: True)
    monkeypatch.setattr(failover, "fetch_trading_status_baostock", lambda symbols, day, config: bs_df)
    monkeypatch.setattr(failover, "_previous_statuses", lambda config, trade_date: {})
    monkeypatch.setattr(failover, "_bj_rows", lambda config, bj, day: ([], 0))

    class _Cfg:
        sources = {"baostock": True}

    frame, meta = failover.fetch_trading_status_backup(_Cfg(), sh, D)
    assert frame is not None
    assert meta["n_filled"] == 0
    assert meta["n_scope_defaults"] == 1
    rows = {r["symbol"]: r for r in frame.iter_rows(named=True)}
    assert rows["510300.SH"]["status"] == "normal"
