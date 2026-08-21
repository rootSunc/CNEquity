"""TDX tip gaps route through EastMoney clist (ADR-0005), not per-symbol kline."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cnequity.adapters.eastmoney.bars import fetch_daily_bars_clist
from cnequity.config import Config, FailoverDatasetSpec
from cnequity.domain.schemas import with_provenance
from cnequity.orchestrator.manifest import Manifest
from cnequity.steps.bars import (
    _finish_daily_bars,
    _gapfill_tip_via_clist,
    _reject_preopen_placeholder,
    _staged_daily_bar_partial_symbols,
    _staged_daily_bar_symbols,
)
from cnequity.storage import StagingWriter
from cnequity.storage.layout import init_data_layout


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
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 7, 24)
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


def test_tip_total_loss_still_raises(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 7, 24)
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
        lambda trade_date, symbols=None, client=None, config=None: pl.DataFrame(),
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_with_status",
        lambda symbols, start, end, client=None, config=None: (pl.DataFrame(), []),
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


def test_tip_partial_miss_after_gapfill_warns_instead_of_failing_step(tmp_path, monkeypatch):
    # A symbol can still be legitimately missing after both TDX and the clist
    # gap-fill had a shot at it (e.g. a trading halt/suspension). That must
    # not fail the whole market-wide tip — regression test for a check that
    # used to raise on any single missing key, every day some symbol halted.
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 7, 24)
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
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_with_status",
        lambda symbols, start, end, client=None, config=None: (pl.DataFrame(), []),
    )
    result = _finish_daily_bars(
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
    assert result["rows_written"] == 1
    findings = result["context_updates"]["audit_findings"]
    assert any(
        f["check"] == "daily_bars_tip_missing_symbols" and "000001.SZ" in f["message"]
        for f in findings
    )


def test_tip_batch_kline_failure_fails_instead_of_success(tmp_path, monkeypatch):
    # A large EastMoney kline miss (transport outage) must fail the tip step so
    # the retry job can recover the session, instead of being masked as a
    # successful run the way the handful of legitimate absences are.
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 8, 19)
    symbols = [f"6{i:05d}.SH" for i in range(10)]
    staged_symbol = symbols[0]
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
        lambda trade_date, symbols=None, client=None, config=None: pl.DataFrame(
            {
                "symbol": [staged_symbol],
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
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_with_status",
        lambda symbols, start, end, client=None, config=None: (pl.DataFrame(), symbols),
    )
    with pytest.raises(RuntimeError, match="failing step for retry"):
        _finish_daily_bars(
            cfg,
            tip,
            run_id,
            start=tip,
            end=tip,
            expected_tdx_symbols=symbols,
            tdx_result={
                "rows_read": 0,
                "rows_written": 0,
                "had_error": True,
                "failed_symbols": symbols,
                "failed_batch_ids": [],
            },
            sina_result=None,
        )


def test_tip_missing_etf_recovered_by_kline_and_batch_superseded(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core")
    tip = date(2026, 8, 18)
    batch_id = "2026-08-18_2026-08-18-batch-0"
    manifest.start_batch(
        run_id,
        batch_id,
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=["600519.SH", "161728.SZ"],
        window_start=tip.isoformat(),
        window_end=tip.isoformat(),
    )
    manifest.finish_batch(
        run_id,
        batch_id,
        "failed",
        error_message=(
            "daily_bars: TDX returned no rows for 1 requested symbol(s): 161728.SZ"
        ),
    )
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

    def _kline(symbols, start, end, **kwargs):
        return pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [tip] * len(symbols),
                "open": [1.5] * len(symbols),
                "high": [1.6] * len(symbols),
                "low": [1.4] * len(symbols),
                "close": [1.557] * len(symbols),
                "volume": [100] * len(symbols),
                "amount": [100.0] * len(symbols),
            }
        )

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_with_status",
        lambda symbols, start, end, **kwargs: (_kline(symbols, start, end, **kwargs), []),
    )

    result = _finish_daily_bars(
        cfg,
        tip,
        run_id,
        start=tip,
        end=tip,
        expected_tdx_symbols=["600519.SH", "161728.SZ"],
        tdx_result={
            "rows_read": 0,
            "rows_written": 0,
            "had_error": True,
            "failed_symbols": ["161728.SZ"],
            "failed_batch_ids": [batch_id],
        },
        sina_result=None,
    )

    assert _staged_daily_bar_symbols(cfg, run_id, tip) == {"600519.SH", "161728.SZ"}
    assert manifest.get_batch(run_id, batch_id)["status"] == "superseded"
    assert any(
        f["check"] == "daily_bars_kline_gapfill"
        for f in result["context_updates"]["audit_findings"]
    )


def test_historical_retry_uses_kline_not_live_clist(tmp_path, monkeypatch):
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
    manifest.finish_batch(
        run_id,
        batch_id,
        "failed",
        error_message=(
            "daily_bars: TDX returned no rows for 1 requested symbol(s): 161728.SZ"
        ),
    )

    calls: list[tuple] = []

    def _no_clist(*args, **kwargs):
        calls.append(("clist", args, kwargs))
        return pl.DataFrame()

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
        _no_clist,
    )

    def _kline(symbols, start, end, **kwargs):
        calls.append(("kline", symbols, start, end))
        return pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [historical] * len(symbols),
                "open": [1.5] * len(symbols),
                "high": [1.6] * len(symbols),
                "low": [1.4] * len(symbols),
                "close": [1.557] * len(symbols),
                "volume": [100] * len(symbols),
                "amount": [100.0] * len(symbols),
            }
        )

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_with_status",
        lambda symbols, start, end, **kwargs: (_kline(symbols, start, end, **kwargs), []),
    )

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
            "failed_batch_ids": [batch_id],
        },
        sina_result=None,
    )

    assert "clist" not in [call[0] for call in calls]
    assert ("kline", ["161728.SZ"], historical, historical) in calls
    assert _staged_daily_bar_symbols(cfg, run_id, historical) == {"161728.SZ"}
    assert manifest.get_batch(run_id, batch_id)["status"] == "superseded"
    assert any(
        f["check"] == "daily_bars_kline_gapfill"
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
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_with_status",
        lambda symbols, start, end, **kwargs: (_kline(symbols, start, end, **kwargs), []),
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

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_with_status",
        lambda symbols, start, end, **kwargs: (_kline(symbols, start, end, **kwargs), []),
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

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_with_status",
        lambda symbols, start, end, **kwargs: (_kline(symbols, start, end, **kwargs), []),
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
