"""TDX tip gaps use clist first, then bounded kline recovery (ADR-0005)."""

from __future__ import annotations

import json
from datetime import date

import polars as pl
import pytest

from cnequity.adapters.eastmoney.bars import fetch_daily_bars_clist
from cnequity.config import Config, FailoverDatasetSpec
from cnequity.domain.schemas import with_provenance
from cnequity.orchestrator.manifest import Manifest
from cnequity.steps.bars import (
    _finish_daily_bars,
    _gapfill_complete_symbols_via_exchange,
    _gapfill_missing_keys_via_ths,
    _gapfill_multiday_via_kline,
    _gapfill_tip_via_clist,
    _mark_unresolved_daily_bar_batches,
    _reject_preopen_placeholder,
    _resolve_recovered_daily_batches,
    _staged_daily_bar_missing_keys,
    _staged_daily_bar_partial_symbols,
    _staged_daily_bar_symbols,
)
from cnequity.steps.common import (
    load_negative_evidence,
    record_negative_evidence,
)
from cnequity.storage import StagingWriter
from cnequity.storage.layout import init_data_layout


def _no_suspension_evidence(monkeypatch) -> None:
    """The chain now asks baostock which absences were suspensions. A test that
    does not care about that answer must still not reach the vendor for it."""
    monkeypatch.setattr(
        "cnequity.adapters.baostock.st_history.fetch_st_history",
        lambda symbols, start, end, **kwargs: (pl.DataFrame(), []),
    )


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


def test_szse_report_cannot_fill_auction_daily_bar_gaps(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.sources["exchange"] = True
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("backfill")
    first = date(2026, 7, 20)
    second = date(2026, 7, 21)
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars",
        run_id,
        "tdx-0000",
        _bar_frame(["000001.SZ"], first),
    )
    calls: list[date] = []

    def fetch_szse(day, *, config=None):
        calls.append(day)
        return _bar_frame(["000001.SZ"], day).drop("source", "data_version", "fetched_at")

    monkeypatch.setattr(
        "cnequity.adapters.exchange.daily_quotes.fetch_szse_daily_quotes",
        fetch_szse,
    )

    result = _gapfill_complete_symbols_via_exchange(
        cfg,
        run_id,
        symbols=["000001.SZ"],
        start=first,
        end=second,
    )

    assert calls == []
    assert result["complete_symbols"] == []
    assert result["rows_written"] == 0
    assert not (
        cfg.staging_root / "daily_bars" / f"run_id={run_id}" / "part-exchange-gapfill.parquet"
    ).exists()


def test_exchange_gapfill_skips_long_windows_before_network_calls(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.sources["exchange"] = True
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    monkeypatch.setattr(
        "cnequity.adapters.exchange.daily_quotes.fetch_szse_daily_quotes",
        lambda *args, **kwargs: pytest.fail("long windows must skip exchange daily files"),
    )

    result = _gapfill_complete_symbols_via_exchange(
        cfg,
        run_id,
        symbols=["000001.SZ"],
        start=date(2026, 1, 1),
        end=date(2026, 3, 31),
    )

    assert result["source_outcomes"]["exchange"]["status"] == "skipped_long_window"
    assert result["source_outcomes"]["exchange"]["requests"] == 0


def test_ths_final_fallback_writes_only_requested_missing_keys(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.sources["ths"] = True
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("backfill")
    first = date(2026, 7, 20)
    second = date(2026, 7, 21)

    monkeypatch.setattr(
        "cnequity.adapters.ths.stock_bars.fetch_stock_bars",
        lambda *args, **kwargs: [
            {
                "symbol": "000001.SZ",
                "trade_date": first,
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 100,
                "amount": 1000.0,
            },
            {
                "symbol": "000001.SZ",
                "trade_date": second,
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 100,
                "amount": 1000.0,
            },
        ],
    )

    result = _gapfill_missing_keys_via_ths(
        cfg,
        run_id,
        missing_keys={("000001.SZ", second)},
        start=first,
        end=second,
    )

    assert result["rows_written"] == 1
    staged = pl.read_parquet(
        cfg.staging_root / "daily_bars" / f"run_id={run_id}" / "part-ths-kline-gapfill.parquet"
    )
    assert staged.select("symbol", "trade_date").rows() == [("000001.SZ", second)]


def test_one_source_empty_is_not_enough_to_certify_no_data(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.sources.update({"exchange": False, "ths": False, "eastmoney": False})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    day = date(2026, 7, 21)
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbol_names": ["561833.SH"],
            "empty_symbol_names": ["561833.SH"],
        },
    )

    result = _gapfill_multiday_via_kline(
        cfg,
        run_id,
        symbols=["561833.SH"],
        start=day,
        end=day,
    )

    assert result["complete"] is False
    assert result["expected_no_data_symbols"] == []


def test_sina_rate_limit_and_eastmoney_disconnect_fall_through_to_ths(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.sources.update({"exchange": False, "ths": True, "eastmoney": True})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    day = date(2026, 7, 21)
    symbol = "600519.SH"
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbol_names": [symbol],
            "empty_symbol_names": [],
            "source_outcomes": {
                "sina": {
                    "status": "failed",
                    "failure_reasons": {"rate_limited": 1, "circuit_open": 1},
                    "empty_symbols": 0,
                }
            },
        },
    )

    def disconnected(symbols, start, end, *, diagnostics, **kwargs):
        diagnostics.update(
            {
                "failed_symbols": {symbol: "transport_error"},
                "empty_symbols": [],
                "route_outcomes": {
                    symbol: {
                        "proxy_failed": True,
                        "direct_failed": True,
                        "direct_succeeded": False,
                    }
                },
            }
        )
        return pl.DataFrame()

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars",
        disconnected,
    )
    monkeypatch.setattr(
        "cnequity.adapters.ths.stock_bars.fetch_stock_bars",
        lambda *args, **kwargs: [
            {
                "symbol": symbol,
                "trade_date": day,
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 100,
                "amount": 1000.0,
            }
        ],
    )

    result = _gapfill_multiday_via_kline(
        cfg,
        run_id,
        symbols=[symbol],
        start=day,
        end=day,
    )

    assert result["complete"] is True
    assert result["rows_written"] == 1
    assert result["source_outcomes"]["eastmoney"]["proxy_failed"] == 1
    assert result["source_outcomes"]["eastmoney"]["direct_failed"] == 1
    assert result["source_outcomes"]["ths"]["status"] == "success"
    staged = pl.read_parquet(
        cfg.staging_root / "daily_bars" / f"run_id={run_id}" / "part-ths-kline-gapfill.parquet"
    )
    assert staged["source"].unique().to_list() == ["ths"]


def test_interior_gaps_schedule_exact_single_day_retry_batches(tmp_path):
    cfg = _cfg(tmp_path)
    cfg.batch_size = 2
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("backfill")
    first = date(2026, 7, 20)
    second = date(2026, 7, 21)
    manifest.start_batch(
        run_id,
        "original-wide-batch",
        "daily_bars",
        "daily_bars",
        symbols=["000001.SZ", "000002.SZ", "000003.SZ", "600519.SH"],
        window_start=first.isoformat(),
        window_end=second.isoformat(),
    )
    manifest.finish_batch(
        run_id,
        "original-wide-batch",
        "failed",
        error_message="partial coverage",
    )

    _mark_unresolved_daily_bar_batches(
        cfg,
        run_id,
        {
            ("000001.SZ", first),
            ("000002.SZ", first),
            ("000003.SZ", first),
            ("600519.SH", second),
        },
    )

    batches = sorted(manifest.get_batches_for_run(run_id), key=lambda row: row["batch_id"])
    assert len(batches) == 4
    original = next(row for row in batches if row["batch_id"] == "original-wide-batch")
    assert original["status"] == "superseded"
    children = [row for row in batches if row["batch_id"] != "original-wide-batch"]
    assert all(row["status"] == "stale" for row in children)
    assert all(row["window_start"] == row["window_end"] for row in children)
    actual = {
        (row["window_start"], tuple(sorted(json.loads(row["symbols_json"])))) for row in children
    }
    assert actual == {
        (first.isoformat(), ("000001.SZ", "000002.SZ")),
        (first.isoformat(), ("000003.SZ",)),
        (second.isoformat(), ("600519.SH",)),
    }


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
        "cnequity.adapters.eastmoney.bars.fetch_clist_pages",
        lambda client, fields: raw,
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.EastMoneyClient",
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


def test_fetch_daily_bars_clist_drops_invalid_ohlcv_instead_of_zero(monkeypatch):
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_clist_pages",
        lambda client, fields: [{}],
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.clist_rows_to_symbols",
        lambda rows: [
            (
                "600519.SH",
                {"f17": "bad", "f15": 102.0, "f16": 99.0, "f2": 101.0, "f5": 1000},
            )
        ],
    )

    class _Client:
        def close(self):
            pass

    df = fetch_daily_bars_clist(date(2026, 7, 24), client=_Client())
    assert df.is_empty()


def test_fetch_daily_bars_clist_drops_zero_price_placeholder(monkeypatch):
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_clist_pages",
        lambda client, fields: [{}],
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.clist_rows_to_symbols",
        lambda rows: [
            (
                "600519.SH",
                {"f17": 0.0, "f15": 0.0, "f16": 0.0, "f2": 0.0, "f5": 1000},
            )
        ],
    )

    class _Client:
        def close(self):
            pass

    assert fetch_daily_bars_clist(date(2026, 7, 24), client=_Client()).is_empty()


def test_fetch_daily_bars_clist_drops_invalid_volume(monkeypatch):
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_clist_pages",
        lambda client, fields: [{}],
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.clist_rows_to_symbols",
        lambda rows: [
            (
                "600519.SH",
                {
                    "f17": 100.0,
                    "f15": 102.0,
                    "f16": 99.0,
                    "f2": 101.0,
                    "f5": 1e300,
                },
            )
        ],
    )

    class _Client:
        def close(self):
            pass

    assert fetch_daily_bars_clist(date(2026, 7, 24), client=_Client()).is_empty()


def test_fetch_daily_bars_clist_closes_owned_client_on_failure(monkeypatch):
    from cnequity.adapters.eastmoney import bars as em_bars

    created = []

    class _OwnedClient:
        closed = False

        def close(self):
            self.closed = True

    def _factory(**kwargs):
        client = _OwnedClient()
        created.append(client)
        return client

    monkeypatch.setattr(em_bars, "EastMoneyClient", _factory)
    monkeypatch.setattr(
        em_bars,
        "fetch_clist_pages",
        lambda client, fields: (_ for _ in ()).throw(RuntimeError("clist down")),
    )
    with pytest.raises(RuntimeError, match="clist down"):
        fetch_daily_bars_clist(date(2026, 7, 24))
    assert created[0].closed is True


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
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
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
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core")
    tip = date(2026, 7, 24)
    manifest.start_batch(
        run_id,
        "tdx-batch-0",
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=["600519.SH"],
        window_start=tip.isoformat(),
        window_end=tip.isoformat(),
    )
    manifest.finish_batch(run_id, "tdx-batch-0", "failed", error_message="TDX empty")
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
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
    assert manifest.get_batch(run_id, "tdx-batch-0")["status"] == "success"


def test_clean_primary_tip_still_captures_daily_peer_snapshot(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.failover_datasets[0].snapshot_cadence = "daily"
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 7, 24)
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-batch-0", _bar_frame(["600519.SH"], tip)
    )
    calls = []
    monkeypatch.setattr(
        "cnequity.quality.failover.snapshot_daily_bars_clist",
        lambda *args, **kwargs: calls.append(kwargs) or pl.DataFrame(),
    )

    result = _finish_daily_bars(
        cfg,
        tip,
        run_id,
        start=tip,
        end=tip,
        expected_tdx_symbols=["600519.SH"],
        tdx_result={"rows_read": 1, "rows_written": 1},
        sina_result=None,
    )

    assert result["rows_written"] == 1
    assert calls and calls[0]["symbols"] == ["600519.SH"]
    finding = next(
        item
        for item in result["context_updates"]["audit_findings"]
        if item["check"] == "backup_snapshot_unavailable"
    )
    assert finding["peer_unavailable"] is True
    assert finding["retryable"] is True


def test_peer_snapshot_failure_is_observable_but_does_not_invalidate_primary(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.failover_datasets[0].snapshot_cadence = "daily"
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 7, 24)
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-batch-0", _bar_frame(["600519.SH"], tip)
    )
    monkeypatch.setattr(
        "cnequity.quality.failover.snapshot_daily_bars_clist",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("peer down")),
    )

    result = _finish_daily_bars(
        cfg,
        tip,
        run_id,
        start=tip,
        end=tip,
        expected_tdx_symbols=["600519.SH"],
        tdx_result={"rows_read": 1, "rows_written": 1},
        sina_result=None,
    )

    findings = result["context_updates"]["audit_findings"]
    finding = next(item for item in findings if item["check"] == "backup_snapshot_unavailable")
    assert finding["severity"] == "warning"
    assert finding["peer_unavailable"] is True
    assert finding["retryable"] is True


def test_historical_tip_backfill_validates_staged_end_not_job_as_of(tmp_path, monkeypatch):
    """A weekend/as-of date must not hide a successfully staged past session."""
    cfg = _cfg(tmp_path)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("backfill")
    fetched = date(2026, 8, 21)
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-batch-0", _bar_frame(["600519.SH"], fetched)
    )
    monkeypatch.setattr(
        "cnequity.steps.bars._gapfill_tip_via_clist",
        lambda *args, **kwargs: {"rows_read": 0, "rows_written": 0, "audit_findings": []},
    )

    result = _finish_daily_bars(
        cfg,
        date(2026, 8, 23),
        run_id,
        start=fetched,
        end=fetched,
        expected_tdx_symbols=["600519.SH"],
        tdx_result={"rows_read": 1, "rows_written": 1},
        sina_result=None,
    )

    assert result["rows_written"] == 1


def test_tip_clist_leftover_uses_kline(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 8, 18)
    expected = ["600519.SH", "161728.SZ"]

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
        lambda *args, **kwargs: _bar_frame([expected[0]], tip),
    )
    kline_calls: list[list[str]] = []

    def kline(symbols, start, end, **kwargs):
        kline_calls.append(list(symbols))
        return _bar_frame(list(symbols), tip)

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", kline)

    result = _finish_daily_bars(
        cfg,
        tip,
        run_id,
        start=tip,
        end=tip,
        expected_tdx_symbols=expected,
        tdx_result={
            "rows_read": 0,
            "rows_written": 0,
            "had_error": True,
            "failed_symbols": expected,
        },
        sina_result=None,
    )

    assert kline_calls == [[expected[1]]]
    assert result["rows_written"] == 2
    assert _staged_daily_bar_symbols(cfg, run_id, tip) == set(expected)


def test_historical_tip_retry_uses_kline_not_live_clist(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core")
    historical = date(2026, 8, 18)
    current = date(2026, 8, 19)
    batch_id = "2026-08-18_2026-08-18-batch-0"
    manifest.start_batch(
        run_id,
        batch_id,
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=["161728.SZ"],
        window_start=historical.isoformat(),
        window_end=historical.isoformat(),
    )
    manifest.finish_batch(run_id, batch_id, "failed", error_message="TDX empty")

    calls: list[tuple] = []

    def no_clist(*args, **kwargs):
        calls.append(("clist", args, kwargs))
        return pl.DataFrame()

    def kline(symbols, start, end, **kwargs):
        calls.append(("kline", list(symbols), start, end))
        return _bar_frame(list(symbols), historical)

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist", no_clist)
    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", kline)

    result = _finish_daily_bars(
        cfg,
        current,
        run_id,
        start=historical,
        end=historical,
        expected_tdx_symbols=["161728.SZ"],
        tdx_result={
            "rows_read": 0,
            "rows_written": 0,
            "had_error": True,
            "failed_symbols": ["161728.SZ"],
        },
        sina_result=None,
    )

    assert "clist" not in [call[0] for call in calls]
    assert ("kline", ["161728.SZ"], historical, historical) in calls
    assert result["rows_written"] == 1
    assert _staged_daily_bar_symbols(cfg, run_id, historical) == {"161728.SZ"}
    assert manifest.get_batch(run_id, batch_id)["status"] == "success"


def test_retry_routes_non_tdx_symbols_to_fallback(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    calls: list[tuple[list[str], date, date, str]] = []
    captured: dict = {}

    def fake_fallback(config, symbols, start, end, run_id, *, batch_prefix):
        calls.append((symbols, start, end, batch_prefix))
        return {"rows_read": 0, "rows_written": 0, "failed_symbols": 0}

    def fake_finish(*args, **kwargs):
        captured.update(kwargs)
        return {"rows_read": 0, "rows_written": 0}

    monkeypatch.setattr("cnequity.steps.bars.fetch_bars_via_sina", fake_fallback)
    # Beijing history is asked of TDX first; here it answers nothing, so Sina
    # must still be handed the symbol rather than the session being dropped.
    monkeypatch.setattr(
        "cnequity.steps.bars._fetch_bj_history_via_tdx",
        lambda *a, **k: {"rows_read": 0, "rows_written": 0, "covered": set(), "requested": True},
    )
    monkeypatch.setattr("cnequity.steps.bars.fetch_daily_bars_parallel", pytest.fail)
    monkeypatch.setattr("cnequity.steps.bars._finish_daily_bars", fake_finish)
    monkeypatch.setattr("cnequity.steps.bars._merge_ownership_result", lambda out, *args: out)

    from cnequity.steps.bars import step_daily_bars

    step_daily_bars(
        cfg,
        date(2024, 6, 28),
        run_id,
        {"_retry_batch_specs": [("retry-0", ["920001.BJ"], date(2024, 6, 27), date(2024, 6, 28))]},
    )

    assert calls == [(["920001.BJ"], date(2024, 6, 27), date(2024, 6, 28), "retry-0-sina")]
    assert captured["expected_tdx_symbols"] == []
    assert captured["expected_fallback_symbols"] == ["920001.BJ"]


def test_a_symbol_tdx_answered_is_not_asked_of_sina_again(tmp_path, monkeypatch):
    """Sina costs one request per symbol per session; TDX answers a range in
    one call. Asking both would keep the cost the routing exists to avoid."""
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    calls: list = []

    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *a, **k: calls.append(a) or {"rows_read": 0, "rows_written": 0, "failed_symbols": 0},
    )
    monkeypatch.setattr(
        "cnequity.steps.bars._fetch_bj_history_via_tdx",
        lambda *a, **k: {
            "rows_read": 4,
            "rows_written": 4,
            "covered": {"920001.BJ"},
            "requested": True,
        },
    )
    monkeypatch.setattr("cnequity.steps.bars.fetch_daily_bars_parallel", pytest.fail)
    monkeypatch.setattr(
        "cnequity.steps.bars._finish_daily_bars",
        lambda *a, **k: {"rows_read": 0, "rows_written": 0},
    )
    monkeypatch.setattr("cnequity.steps.bars._merge_ownership_result", lambda out, *args: out)

    from cnequity.steps.bars import step_daily_bars

    step_daily_bars(
        cfg,
        date(2024, 6, 28),
        run_id,
        {"_retry_batch_specs": [("retry-0", ["920001.BJ"], date(2024, 6, 27), date(2024, 6, 28))]},
    )

    assert calls == []


def test_tip_total_loss_still_raises(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 7, 24)
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
        lambda trade_date, symbols=None, client=None, config=None: pl.DataFrame(),
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars",
        lambda *args, **kwargs: pl.DataFrame(),
    )
    with pytest.raises(RuntimeError, match="produced no staged tip rows"):
        _finish_daily_bars(
            cfg,
            tip,
            run_id,
            start=tip,
            end=tip,
            expected_tdx_symbols=["600519.SH", "000001.SZ"],
            tdx_result={
                "rows_read": 0,
                "rows_written": 0,
                "had_error": True,
                "failed_symbols": ["600519.SH", "000001.SZ"],
            },
            sina_result=None,
        )


def test_tip_partial_miss_after_gapfill_stays_strict_for_unknown_symbol(tmp_path, monkeypatch):
    # A market-sized response cannot prove that one remaining symbol had no
    # data.  Without listing/status/source-empty evidence the unknown key must
    # keep the checkpoint blocked; there is no market-level 5% allowance.
    cfg = _cfg(tmp_path)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core")
    tip = date(2026, 7, 24)
    manifest.start_batch(
        run_id,
        "tdx-partial",
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=["600519.SH", "000001.SZ"],
    )
    manifest.finish_batch(run_id, "tdx-partial", "failed", error_message="TDX partial")
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
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
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars",
        lambda *args, **kwargs: pl.DataFrame(),
    )
    with pytest.raises(RuntimeError, match="refusing to checkpoint"):
        _finish_daily_bars(
            cfg,
            tip,
            run_id,
            start=tip,
            end=tip,
            expected_tdx_symbols=["600519.SH", "000001.SZ"],
            tdx_result={
                "rows_read": 0,
                "rows_written": 0,
                "had_error": True,
                "failed_symbols": ["600519.SH", "000001.SZ"],
            },
            sina_result=None,
        )
    assert manifest.get_batch(run_id, "tdx-partial")["status"] == "failed"


def test_tip_large_partial_miss_blocks_checkpoint(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 7, 24)
    expected = [f"600{i:03d}.SH" for i in range(10)]
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
        lambda trade_date, symbols=None, client=None, config=None: pl.DataFrame(
            {
                "symbol": [expected[0]],
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
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars",
        lambda *args, **kwargs: pl.DataFrame(),
    )
    with pytest.raises(RuntimeError, match="refusing to checkpoint"):
        _finish_daily_bars(
            cfg,
            tip,
            run_id,
            start=tip,
            end=tip,
            expected_tdx_symbols=expected,
            tdx_result={
                "rows_read": 0,
                "rows_written": 0,
                "had_error": True,
                "failed_symbols": expected,
            },
            sina_result=None,
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
        days = [
            date(2024, 6, 20),
            date(2024, 6, 21),
            date(2024, 6, 24),
            date(2024, 6, 25),
            date(2024, 6, 26),
            date(2024, 6, 27),
            date(2024, 6, 28),
        ]
        rows = [
            {
                "symbol": symbol,
                "trade_date": day,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1,
                "amount": 1.0,
            }
            for symbol in symbols
            for day in days
        ]
        return pl.DataFrame(rows)

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist", _clist)
    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", _kline)
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": ["600519.SH"],
            "empty_symbol_names": [],
        },
    )

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
    assert result["rows_written"] == 7


def test_multiday_partial_miss_after_gapfill_stays_strict_for_unknown_symbol(tmp_path, monkeypatch):
    # Even a large multi-day response cannot certify one unresolved symbol
    # without symbol-level metadata/status/empty evidence.
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    start, end = date(2024, 6, 20), date(2024, 6, 21)
    days = [date(2024, 6, 20), date(2024, 6, 21)]
    expected = [f"600{i:03d}.SH" for i in range(20)]
    missing = expected[-1]

    def _kline(symbols, s, e, **k):
        rows = [
            {
                "symbol": symbol,
                "trade_date": day,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1,
                "amount": 1.0,
            }
            for symbol in symbols
            if symbol != missing
            for day in days
        ]
        return pl.DataFrame(rows)

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", _kline)
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": len(expected),
            "failed_symbol_names": expected,
            "empty_symbol_names": [],
        },
    )

    with pytest.raises(RuntimeError, match="refusing to checkpoint"):
        _finish_daily_bars(
            cfg,
            end,
            run_id,
            start=start,
            end=end,
            expected_tdx_symbols=expected,
            tdx_result={
                "rows_read": 0,
                "rows_written": 0,
                "had_error": True,
                "failed_symbols": expected,
            },
            sina_result=None,
        )


def test_multiday_single_symbol_scope_still_raises(tmp_path, monkeypatch):
    # The tolerance above must not apply to a narrow explicit scope — a
    # scoped backfill or a `cne run retry` batch of just one or two symbols,
    # where every symbol is the whole ask and "tolerate at least 1" would
    # make the run silently report success with nothing staged.
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    start, end = date(2024, 6, 20), date(2024, 6, 21)

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars",
        lambda *args, **kwargs: pl.DataFrame(),
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": ["600519.SH"],
            "empty_symbol_names": [],
        },
    )

    with pytest.raises(RuntimeError, match="refusing to checkpoint"):
        _finish_daily_bars(
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


def test_multiday_large_partial_miss_blocks_checkpoint(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    start, end = date(2024, 6, 20), date(2024, 6, 21)
    days = [date(2024, 6, 20), date(2024, 6, 21)]
    expected = [f"600{i:03d}.SH" for i in range(10)]

    def _kline(symbols, s, e, **k):
        rows = [
            {
                "symbol": symbol,
                "trade_date": day,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1,
                "amount": 1.0,
            }
            for symbol in symbols
            if symbol == expected[0]
            for day in days
        ]
        return pl.DataFrame(rows)

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", _kline)
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": len(expected),
            "failed_symbol_names": expected,
            "empty_symbol_names": [],
        },
    )

    with pytest.raises(RuntimeError, match="refusing to checkpoint"):
        _finish_daily_bars(
            cfg,
            end,
            run_id,
            start=start,
            end=end,
            expected_tdx_symbols=expected,
            tdx_result={
                "rows_read": 0,
                "rows_written": 0,
                "had_error": True,
                "failed_symbols": expected,
            },
            sina_result=None,
        )


def test_multiday_requires_two_independent_source_empty_observations(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.sources["ths"] = True
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 28)

    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": ["561833.SH"],
            "empty_symbol_names": ["561833.SH"],
        },
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars",
        lambda *args, **kwargs: pl.DataFrame(),
    )
    monkeypatch.setattr(
        "cnequity.adapters.ths.stock_bars.fetch_stock_bars",
        lambda *args, **kwargs: [],
    )

    result = _finish_daily_bars(
        cfg,
        end,
        run_id,
        start=start,
        end=end,
        expected_tdx_symbols=["561833.SH"],
        tdx_result={
            "rows_read": 0,
            "rows_written": 0,
            "had_error": True,
            "failed_symbols": ["561833.SH"],
        },
        sina_result=None,
    )

    assert result["rows_written"] == 0
    findings = result["context_updates"]["audit_findings"]
    assert any(f["check"] == "daily_bars_multi_source_no_data" for f in findings)


def test_resolve_recovered_daily_batches_does_not_close_unrelated_failures(tmp_path):
    cfg = _cfg(tmp_path)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("backfill")
    manifest.start_batch(
        run_id,
        "batch-a",
        "daily_bars",
        "daily_bars",
        symbols=["561833.SH"],
    )
    manifest.start_batch(
        run_id,
        "batch-b",
        "daily_bars",
        "daily_bars",
        symbols=["561834.SH"],
    )
    manifest.finish_batch(run_id, "batch-a", "failed", error_message="TDX empty")
    manifest.finish_batch(run_id, "batch-b", "failed", error_message="TDX empty")

    _resolve_recovered_daily_batches(cfg, run_id, resolved_symbols={"561833.SH"})

    batches = {row["batch_id"]: row for row in manifest.get_batches_for_run(run_id)}
    assert batches["batch-a"]["status"] == "success"
    assert batches["batch-b"]["status"] == "failed"


def test_multiday_partial_symbol_is_gapfilled_without_overwriting_primary_rows(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 24)
    symbol = "600519.SH"
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-start", _bar_frame([symbol], start)
    )
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-end", _bar_frame([symbol], end)
    )
    assert _staged_daily_bar_partial_symbols(cfg, run_id, [symbol], start, end) == {symbol}

    def _kline(symbols, s, e, **k):
        days = [date(2024, 6, 20), date(2024, 6, 21), date(2024, 6, 24)]
        return pl.concat([_bar_frame(symbols, day) for day in days])

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", _kline)
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": [symbol],
            "empty_symbol_names": [],
        },
    )
    result = _finish_daily_bars(
        cfg,
        end,
        run_id,
        start=start,
        end=end,
        expected_tdx_symbols=[symbol],
        tdx_result={
            "rows_read": 2,
            "rows_written": 2,
            "had_error": False,
            "failed_symbols": [],
        },
        sina_result=None,
    )
    assert result["rows_written"] == 3  # two primary rows + one recovered interior day
    assert _staged_daily_bar_symbols(cfg, run_id, None) == {symbol}


def test_multiday_partial_symbol_detects_leading_session_gap(tmp_path):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 24)
    symbol = "600519.SH"
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-late-start", _bar_frame([symbol], date(2024, 6, 21))
    )
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-end", _bar_frame([symbol], end)
    )

    assert _staged_daily_bar_partial_symbols(cfg, run_id, [symbol], start, end) == {symbol}


def test_multiday_partial_symbol_respects_listing_and_delisting_edges(tmp_path):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 24)
    symbol = "600519.SH"
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": [symbol],
            "list_date": [date(2024, 6, 21)],
            "delist_date": [date(2024, 6, 24)],
        }
    ).write_parquet(instruments / "part-merged.parquet")
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-listing", _bar_frame([symbol], date(2024, 6, 21))
    )
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-delisting", _bar_frame([symbol], end)
    )

    assert _staged_daily_bar_partial_symbols(cfg, run_id, [symbol], start, end) == set()


def test_multiday_fallback_failure_is_gapfilled_by_symbol(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 24)
    symbol = "920001.BJ"
    calls: list[list[str]] = []

    def _kline(symbols, s, e, **k):
        calls.append(list(symbols))
        days = [date(2024, 6, 20), date(2024, 6, 21), date(2024, 6, 24)]
        return pl.concat([_bar_frame(symbols, day) for day in days])

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", _kline)
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": [symbol],
            "empty_symbol_names": [],
        },
    )
    result = _finish_daily_bars(
        cfg,
        end,
        run_id,
        start=start,
        end=end,
        expected_tdx_symbols=[],
        expected_fallback_symbols=[symbol],
        tdx_result={"rows_read": 0, "rows_written": 0, "had_error": False},
        sina_result={
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": [symbol],
        },
    )

    assert calls == [[symbol]]
    assert result["rows_written"] == 3


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


def test_baostock_rescues_keys_when_eastmoney_history_is_unreachable(tmp_path, monkeypatch):
    """The measured outage: every `push2his` host drops the connection while
    the rest of the chain has nothing per-symbol left to try."""
    cfg = _cfg(tmp_path)
    cfg.sources.update({"exchange": False, "ths": False, "eastmoney": True, "baostock": True})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    day = date(2026, 9, 15)
    symbol = "600519.SH"

    def disconnected(symbols, start, end, *, diagnostics, **kwargs):
        diagnostics.update(
            {
                "failed_symbols": {symbol: "transport_error"},
                "empty_symbols": [],
                "route_outcomes": {},
            }
        )
        return pl.DataFrame()

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", disconnected)
    monkeypatch.setattr(
        "cnequity.adapters.baostock.delisted_bars.fetch_delisted_bars",
        lambda symbols, start, end, **kwargs: (
            [
                {
                    "symbol": symbol,
                    "trade_date": day,
                    "open": 1281.0,
                    "high": 1284.5,
                    "low": 1271.28,
                    "close": 1272.75,
                    "volume": 1376172,
                    "amount": 1.0e9,
                }
            ],
            [],
        ),
    )

    result = _gapfill_multiday_via_kline(
        cfg, run_id, symbols=[symbol], start=day, end=day, require_complete=False
    )

    assert result["complete"] is True
    assert result["source_outcomes"]["baostock"]["status"] == "success"
    staged = pl.read_parquet(
        cfg.staging_root / "daily_bars" / f"run_id={run_id}" / "part-baostock-kline-gapfill.parquet"
    )
    assert staged["source"].unique().to_list() == ["baostock"]


def test_baostock_empty_is_the_second_opinion_that_certifies_no_data(tmp_path, monkeypatch):
    """A suspended name needs two independent empties. With EastMoney's history
    host unreachable, THS was the only one left and could never get there."""
    _no_suspension_evidence(monkeypatch)
    cfg = _cfg(tmp_path)
    cfg.sources.update({"exchange": False, "ths": True, "eastmoney": True, "baostock": True})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    day = date(2026, 9, 15)
    symbol = "000016.SZ"

    def disconnected(symbols, start, end, *, diagnostics, **kwargs):
        diagnostics.update(
            {
                "failed_symbols": {symbol: "transport_error"},
                "empty_symbols": [],
                "route_outcomes": {},
            }
        )
        return pl.DataFrame()

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", disconnected)
    monkeypatch.setattr("cnequity.adapters.ths.stock_bars.fetch_stock_bars", lambda *a, **k: [])
    # Baostock reports a suspended session as no row, not as an error — which
    # is what makes it usable as the second empty.
    monkeypatch.setattr(
        "cnequity.adapters.baostock.delisted_bars.fetch_delisted_bars",
        lambda symbols, start, end, **kwargs: ([], []),
    )

    result = _gapfill_multiday_via_kline(
        cfg, run_id, symbols=[symbol], start=day, end=day, require_complete=False
    )

    assert result["expected_no_data_symbols"] == [symbol]
    assert result["complete"] is True
    assert result["source_outcomes"]["baostock"]["status"] == "empty"


def test_a_failed_baostock_symbol_is_not_evidence_of_anything(tmp_path, monkeypatch):
    """Answered-and-had-nothing certifies; never-got-an-answer does not."""
    _no_suspension_evidence(monkeypatch)
    cfg = _cfg(tmp_path)
    cfg.sources.update({"exchange": False, "ths": True, "eastmoney": False, "baostock": True})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    day = date(2026, 9, 15)
    symbol = "000016.SZ"

    monkeypatch.setattr("cnequity.adapters.ths.stock_bars.fetch_stock_bars", lambda *a, **k: [])
    monkeypatch.setattr(
        "cnequity.adapters.baostock.delisted_bars.fetch_delisted_bars",
        lambda symbols, start, end, **kwargs: ([], [symbol]),
    )

    result = _gapfill_multiday_via_kline(
        cfg, run_id, symbols=[symbol], start=day, end=day, require_complete=False
    )

    assert result["expected_no_data_symbols"] == []
    assert result["complete"] is False
    assert result["source_outcomes"]["baostock"]["status"] == "failed"


def test_the_slowest_link_refuses_a_residue_that_is_really_a_dead_primary(tmp_path, monkeypatch):
    """Baostock paces at 1 req/s: 5,000 stragglers is not a gap-fill, it is an
    hour spent proving the primary is down."""
    from cnequity.steps import bars as bars_mod

    cfg = _cfg(tmp_path)
    cfg.sources.update({"exchange": False, "ths": False, "eastmoney": False, "baostock": True})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    day = date(2026, 9, 15)
    symbols = [f"60{i:04d}.SH" for i in range(bars_mod._BAOSTOCK_GAPFILL_MAX_SYMBOLS + 1)]

    def _never(*args, **kwargs):
        raise AssertionError("the bounded link must not reach the network")

    monkeypatch.setattr("cnequity.adapters.baostock.delisted_bars.fetch_delisted_bars", _never)

    result = bars_mod._gapfill_missing_keys_via_baostock(
        cfg, run_id, missing_keys={(s, day) for s in symbols}, start=day, end=day
    )

    assert result["source_outcomes"]["baostock"]["status"] == "skipped"
    assert result["rows_written"] == 0
    assert "residue bound" in result["audit_findings"][0]["message"]


def test_beijing_keys_never_reach_baostock(tmp_path, monkeypatch):
    """Baostock serves SH/SZ only: a BJ code comes back as a retried failure,
    which costs the sweep and evidences nothing about the session."""
    from cnequity.steps import bars as bars_mod

    cfg = _cfg(tmp_path)
    cfg.sources.update({"baostock": True})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    day = date(2026, 9, 15)
    seen: dict = {}

    def _record(symbols, start, end, **kwargs):
        seen["symbols"] = list(symbols)
        return ([], [])

    monkeypatch.setattr("cnequity.adapters.baostock.delisted_bars.fetch_delisted_bars", _record)

    result = bars_mod._gapfill_missing_keys_via_baostock(
        cfg,
        run_id,
        missing_keys={("920002.BJ", day), ("600519.SH", day)},
        start=day,
        end=day,
    )

    assert seen["symbols"] == ["600519.SH"]
    # The Beijing name is not empty evidence either — it was never asked.
    assert result["empty_symbols"] == ["600519.SH"]


def test_an_all_beijing_residue_skips_the_link_entirely(tmp_path, monkeypatch):
    from cnequity.steps import bars as bars_mod

    cfg = _cfg(tmp_path)
    cfg.sources.update({"baostock": True})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    day = date(2026, 9, 15)

    def _never(*args, **kwargs):
        raise AssertionError("no SH/SZ key to ask about")

    monkeypatch.setattr("cnequity.adapters.baostock.delisted_bars.fetch_delisted_bars", _never)

    result = bars_mod._gapfill_missing_keys_via_baostock(
        cfg, run_id, missing_keys={("920002.BJ", day)}, start=day, end=day
    )
    assert result["source_outcomes"]["baostock"]["status"] == "skipped"
    assert result["empty_symbols"] == []


def test_a_satisfied_chain_does_not_report_the_last_link_as_disabled(tmp_path, monkeypatch):
    """`disabled` sends the operator to edit a setting. An earlier link having
    resolved everything is the chain working."""
    from cnequity.steps import bars as bars_mod

    cfg = _cfg(tmp_path)
    cfg.sources.update({"baostock": True})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")

    def _never(*args, **kwargs):
        raise AssertionError("nothing left to ask about")

    monkeypatch.setattr("cnequity.adapters.baostock.delisted_bars.fetch_delisted_bars", _never)

    result = bars_mod._gapfill_missing_keys_via_baostock(
        cfg, run_id, missing_keys=set(), start=date(2026, 9, 15), end=date(2026, 9, 15)
    )
    assert result["source_outcomes"]["baostock"]["status"] == "not_needed"

    cfg.sources.update({"baostock": False})
    off = bars_mod._gapfill_missing_keys_via_baostock(
        cfg,
        run_id,
        missing_keys={("600519.SH", date(2026, 9, 15))},
        start=date(2026, 9, 15),
        end=date(2026, 9, 15),
    )
    assert off["source_outcomes"]["baostock"]["status"] == "disabled"


def test_completeness_excuses_days_the_lake_knows_were_suspended(tmp_path, monkeypatch):
    """Counting every trading day as expected is how a backfill of a name with
    any suspension in its window could never report itself complete — which
    left the failed primary batch unresolved and compact skipping the dataset,
    so rows were fetched, staged and never published."""
    import datetime as dt

    import polars as pl

    from cnequity.steps import bars as bars_mod

    cfg = _cfg(tmp_path)
    sessions = [dt.date(2026, 9, 14), dt.date(2026, 9, 15), dt.date(2026, 9, 16)]
    monkeypatch.setattr(bars_mod, "list_trading_dates", lambda *a, **k: sessions)
    monkeypatch.setattr(
        bars_mod,
        "_instrument_spans",
        lambda config: {"600519.SH": (dt.date(2026, 9, 15), None, "stock")},
    )
    monkeypatch.setattr(
        bars_mod,
        "load_curated_trading_status",
        lambda *a, **k: pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [dt.date(2026, 9, 16)],
                "is_trading": [False],
            },
            schema_overrides={"trade_date": pl.Date},
        ),
    )

    keys = bars_mod._expected_session_keys(cfg, ["600519.SH"], sessions, sessions[0], sessions[-1])

    # 09-14 precedes the listing, 09-16 is a known suspension: one real key.
    assert keys == {("600519.SH", dt.date(2026, 9, 15))}


def test_completeness_still_expects_every_session_it_has_no_excuse_for(tmp_path, monkeypatch):
    import datetime as dt

    from cnequity.steps import bars as bars_mod

    cfg = _cfg(tmp_path)
    sessions = [dt.date(2026, 9, 14), dt.date(2026, 9, 15)]
    monkeypatch.setattr(bars_mod, "list_trading_dates", lambda *a, **k: sessions)
    monkeypatch.setattr(bars_mod, "_instrument_spans", lambda config: {})
    monkeypatch.setattr(bars_mod, "load_curated_trading_status", lambda *a, **k: None)

    keys = bars_mod._expected_session_keys(cfg, ["600519.SH"], sessions, sessions[0], sessions[-1])
    assert keys == {("600519.SH", sessions[0]), ("600519.SH", sessions[1])}


def test_the_step_learns_suspensions_itself_instead_of_demanding_a_second_command(
    tmp_path, monkeypatch
):
    """A pre-2016 window has no `trading_status` to excuse an interior gap with,
    so the bars would not publish until the operator knew to backfill
    trading_status first — for the same symbols and window, in an order nothing
    announced."""
    from cnequity.steps import bars as bars_mod

    cfg = _cfg(tmp_path)
    cfg.sources.update({"exchange": False, "ths": False, "eastmoney": False, "baostock": True})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    day = date(2010, 3, 30)
    symbol = "002118.SZ"
    asked: dict = {}

    def _st_history(symbols, start, end, **kwargs):
        asked["symbols"] = list(symbols)
        return (
            pl.DataFrame(
                {
                    "symbol": [symbol],
                    "trade_date": [day],
                    "is_trading": [False],
                    "status": ["suspended"],
                    "risk_warning": [False],
                },
                schema_overrides={"trade_date": pl.Date},
            ),
            [],
        )

    monkeypatch.setattr("cnequity.adapters.baostock.st_history.fetch_st_history", _st_history)
    staged: dict = {}

    def _write_fetched(config, rid, dataset, frame, **kwargs):
        staged[dataset] = frame.height
        return {"rows_written": frame.height}

    monkeypatch.setattr("cnequity.steps.http_common.write_fetched", _write_fetched)

    suspended, outcome = bars_mod._learn_suspensions_from_baostock(
        cfg, run_id, {(symbol, day)}, day, day
    )

    assert asked["symbols"] == [symbol]
    assert suspended == {(symbol, day)}
    assert outcome["status"] == "success"
    # Staged, so the next run does not have to ask again.
    assert staged["trading_status"] == 1
    # And carried on the config, because the gate below reads curated rows that
    # this run has not compacted yet.
    assert (symbol, day) in cfg._learned_suspensions["keys"]


def test_learning_is_skipped_when_there_is_nothing_unexplained(tmp_path, monkeypatch):
    from cnequity.steps import bars as bars_mod

    cfg = _cfg(tmp_path)
    cfg.sources.update({"baostock": True})

    def _never(*args, **kwargs):
        raise AssertionError("no unexplained key to ask about")

    monkeypatch.setattr("cnequity.adapters.baostock.st_history.fetch_st_history", _never)
    suspended, outcome = bars_mod._learn_suspensions_from_baostock(
        cfg, "run-x", set(), date(2010, 1, 1), date(2010, 12, 31)
    )
    assert suspended == set()
    assert outcome["status"] == "not_needed"


def test_beijing_keys_are_not_sent_to_a_vendor_without_beijing(tmp_path, monkeypatch):
    from cnequity.steps import bars as bars_mod

    cfg = _cfg(tmp_path)
    cfg.sources.update({"baostock": True})

    def _never(*args, **kwargs):
        raise AssertionError("baostock has no Beijing coverage")

    monkeypatch.setattr("cnequity.adapters.baostock.st_history.fetch_st_history", _never)
    suspended, outcome = bars_mod._learn_suspensions_from_baostock(
        cfg, "run-x", {("920002.BJ", date(2010, 3, 30))}, date(2010, 3, 30), date(2010, 3, 30)
    )
    assert suspended == set()
    assert outcome["status"] == "skipped"


def test_a_window_spent_entirely_halted_is_certified_from_positive_evidence(tmp_path, monkeypatch):
    """Two vendors returning nothing only says nobody had it. "Suspended on
    every session you asked about" is a statement about the market — and it is
    the only one that reaches a name halted for a whole restructuring."""
    # The final baostock link is not what either test is about, and reaching it
    # for real cost 37s of login deadline per test — and made the conclusion
    # depend on a live vendor: without that route the assertion below failed.
    monkeypatch.setattr(
        "cnequity.adapters.baostock.delisted_bars.fetch_delisted_bars",
        lambda requested, start, end, config=None: ({}, list(requested)),
    )
    from cnequity.steps import bars as bars_mod

    cfg = _cfg(tmp_path)
    cfg.sources.update({"exchange": False, "ths": False, "eastmoney": False, "baostock": True})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    sessions = [date(2010, 3, 29), date(2010, 3, 30)]
    symbol = "000156.SZ"

    monkeypatch.setattr(bars_mod, "list_trading_dates", lambda *a, **k: sessions)
    monkeypatch.setattr(
        bars_mod,
        "fetch_bars_via_sina",
        lambda *a, **k: {"rows_read": 0, "rows_written": 0, "empty_symbol_names": []},
    )
    monkeypatch.setattr(
        "cnequity.adapters.baostock.st_history.fetch_st_history",
        lambda symbols, start, end, **kwargs: (
            pl.DataFrame(
                {
                    "symbol": [symbol, symbol],
                    "trade_date": sessions,
                    "is_trading": [False, False],
                    "status": ["suspended", "suspended"],
                    "risk_warning": [False, False],
                },
                schema_overrides={"trade_date": pl.Date},
            ),
            [],
        ),
    )
    monkeypatch.setattr(
        "cnequity.steps.http_common.write_fetched",
        lambda config, rid, dataset, frame, **kw: {"rows_written": frame.height},
    )

    result = _gapfill_multiday_via_kline(
        cfg, run_id, symbols=[symbol], start=sessions[0], end=sessions[-1]
    )

    assert result["expected_no_data_symbols"] == [symbol]
    assert result["complete"] is True
    checks = {f["check"] for f in result["audit_findings"]}
    assert "daily_bars_window_fully_suspended" in checks
    # …and only that finding. A halted name is certified by the vendor's own
    # trading status, not by two sources returning nothing, so it must not be
    # reported under the rule that claims the latter — which also has no
    # `empty_evidence` entry for it and used to raise `KeyError` building one.
    multi = [f for f in result["audit_findings"] if f["check"] == "daily_bars_multi_source_no_data"]
    assert all(symbol not in f["symbols"] for f in multi), multi


def test_a_partly_halted_symbol_is_not_certified_as_having_no_data(tmp_path, monkeypatch):
    """It traded on the other sessions; only those are excused."""
    # The final baostock link is not what either test is about, and reaching it
    # for real cost 37s of login deadline per test — and made the conclusion
    # depend on a live vendor: without that route the assertion below failed.
    monkeypatch.setattr(
        "cnequity.adapters.baostock.delisted_bars.fetch_delisted_bars",
        lambda requested, start, end, config=None: ({}, list(requested)),
    )
    from cnequity.steps import bars as bars_mod

    cfg = _cfg(tmp_path)
    cfg.sources.update({"exchange": False, "ths": False, "eastmoney": False, "baostock": True})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    sessions = [date(2010, 3, 29), date(2010, 3, 30)]
    symbol = "000156.SZ"

    monkeypatch.setattr(bars_mod, "list_trading_dates", lambda *a, **k: sessions)
    monkeypatch.setattr(
        bars_mod,
        "fetch_bars_via_sina",
        lambda *a, **k: {"rows_read": 0, "rows_written": 0, "empty_symbol_names": []},
    )
    monkeypatch.setattr(
        "cnequity.adapters.baostock.st_history.fetch_st_history",
        lambda symbols, start, end, **kwargs: (
            pl.DataFrame(
                {
                    "symbol": [symbol],
                    "trade_date": [sessions[0]],
                    "is_trading": [False],
                    "status": ["suspended"],
                    "risk_warning": [False],
                },
                schema_overrides={"trade_date": pl.Date},
            ),
            [],
        ),
    )
    monkeypatch.setattr(
        "cnequity.steps.http_common.write_fetched",
        lambda config, rid, dataset, frame, **kw: {"rows_written": frame.height},
    )

    result = _gapfill_multiday_via_kline(
        cfg, run_id, symbols=[symbol], start=sessions[0], end=sessions[-1]
    )

    assert result["expected_no_data_symbols"] == []
    assert result["complete"] is False
def test_placeholder_only_symbol_certified_after_two_source_agreement(tmp_path, monkeypatch):
    """A volume=0 placeholder bar must not disqualify no-data certification.

    First-init dead funds (delisted/liquidated ETFs/LOFs absent from TDX and
    EastMoney history) stage exactly one pre-open placeholder on the tip day.
    The old "no staged rows at all" condition made the designed two-source
    escape hatch unreachable, which deadlocked bootstrap ``cne init``: the
    interior-gap gate raised before negative evidence could ever be recorded.
    """
    cfg = _cfg(tmp_path)
    cfg.sources.update({"exchange": False, "ths": False})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start = date(2026, 7, 20)
    tip = date(2026, 7, 22)
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-0000", _bar_frame(["158030.SZ"], tip, volume=0)
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbol_names": [],
            "empty_symbol_names": ["158030.SZ"],
        },
    )

    def empty(symbols, start, end, *, diagnostics, **kwargs):
        diagnostics.update(
            {"failed_symbols": {}, "empty_symbols": list(symbols), "route_outcomes": {}}
        )
        return pl.DataFrame()

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", empty)

    result = _gapfill_multiday_via_kline(
        cfg,
        run_id,
        symbols=["158030.SZ"],
        start=start,
        end=tip,
    )

    assert result["complete"] is True
    assert result["expected_no_data_symbols"] == ["158030.SZ"]


def test_truncated_symbol_missing_prefix_certified_segment_level(tmp_path, monkeypatch):
    """A symbol whose source history starts mid-window certifies its missing
    prefix per segment instead of being permanently unresolved.

    The old rule only certified symbols with NO staged rows at all; a fund
    with a real recent tail (source retention cut) kept its unreachable
    prefix in the missing-key set forever, so the interior-gap gate refused
    to checkpoint on every resume.
    """
    cfg = _cfg(tmp_path)
    cfg.sources.update({"exchange": False, "ths": False})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start = date(2026, 7, 20)
    tail = date(2026, 7, 22)
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-0000", _bar_frame(["161022.SZ"], tail)
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbol_names": [],
            "empty_symbol_names": ["161022.SZ"],
        },
    )

    def empty(symbols, start, end, *, diagnostics, **kwargs):
        diagnostics.update(
            {"failed_symbols": {}, "empty_symbols": list(symbols), "route_outcomes": {}}
        )
        return pl.DataFrame()

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", empty)

    result = _gapfill_multiday_via_kline(
        cfg,
        run_id,
        symbols=["161022.SZ"],
        start=start,
        end=tail,
    )

    assert result["complete"] is True
    keys = {tuple(k) for k in result["expected_no_data_keys"]}
    assert keys == {("161022.SZ", start), ("161022.SZ", date(2026, 7, 21))}
    # 负证据只覆盖缺失段，不覆盖有真实数据的尾日
    evidence = load_negative_evidence(cfg, "daily_bars")
    covered = [
        e
        for e in evidence
        if e["symbol"] == "161022.SZ" and e["reason"] == "source_empty"
    ]
    assert covered, "segment certification must persist negative evidence"
    assert all(str(e["window_end"]) <= "2026-07-21" for e in covered)


def test_missing_key_gate_skips_negative_evidence_keys(tmp_path):
    """The interior-gap gate must exclude keys already covered by live
    negative evidence — symmetric with its trading_status exclusion."""
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start = date(2026, 7, 20)
    missing_day = date(2026, 7, 21)
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-0000", _bar_frame(["600519.SH"], start)
    )
    record_negative_evidence(
        cfg,
        "daily_bars",
        {"600519.SH"},
        missing_day,
        missing_day,
        reason="source_empty",
        source="tdx_protocol",
    )

    keys = _staged_daily_bar_missing_keys(cfg, run_id, ["600519.SH"], start, missing_day)

    assert keys == set()


def test_segment_certification_survives_em_transport_failure(tmp_path, monkeypatch):
    """EastMoney transport failure must not block certification when Sina and
    THS both vote empty — the vote needs two independent sources, not two
    specific sources (em push2his outage is the common real-world case)."""
    cfg = _cfg(tmp_path)
    cfg.sources.update({"exchange": False})
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start = date(2026, 7, 20)
    tail = date(2026, 7, 22)
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-0000", _bar_frame(["161022.SZ"], tail)
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbol_names": [],
            "empty_symbol_names": ["161022.SZ"],
        },
    )

    def em_down(symbols, start, end, *, diagnostics, **kwargs):
        diagnostics.update(
            {
                "failed_symbols": {s: "transport_error" for s in symbols},
                "empty_symbols": [],
                "route_outcomes": {},
            }
        )
        return pl.DataFrame()

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", em_down)
    monkeypatch.setattr(
        "cnequity.steps.bars._gapfill_missing_keys_via_ths",
        lambda config, run_id, *, missing_keys, start, end: {
            "rows_read": 0,
            "rows_written": 0,
            "audit_findings": [],
            "empty_symbols": [s for s, _day in missing_keys],
        },
    )

    result = _gapfill_multiday_via_kline(
        cfg,
        run_id,
        symbols=["161022.SZ"],
        start=start,
        end=tail,
    )

    assert result["complete"] is True
    keys = {tuple(k) for k in result["expected_no_data_keys"]}
    assert keys == {("161022.SZ", start), ("161022.SZ", date(2026, 7, 21))}
