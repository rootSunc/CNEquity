"""Beijing exchange has no TDX route — bars must come from the fallback vendor."""

import json
from datetime import date, timedelta

import httpx
import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.schemas import DAILY_BARS_SCHEMA
from cnequity.domain.symbols import is_tdx_servable, split_by_quote_source
from cnequity.steps.bars import (
    _resolve_daily_bar_scope,
    fetch_bars_via_sina,
    repair_bse_tip_amounts_from_curated,
)
from cnequity.steps.delisted import catalog_path
from cnequity.steps.reference import _merge_untdxable_instruments
from cnequity.storage.parquet import StagingWriter

_BAR_COLS = [c for c in DAILY_BARS_SCHEMA if c not in ("source", "data_version", "fetched_at")]


def _bars(symbol: str, days: list[date]) -> pl.DataFrame:
    n = len(days)
    return pl.DataFrame(
        {
            "symbol": [symbol] * n,
            "trade_date": days,
            "open": [1.0] * n,
            "high": [1.0] * n,
            "low": [1.0] * n,
            "close": [1.0] * n,
            "volume": [10] * n,
            "amount": [None] * n,
        },
        schema={c: DAILY_BARS_SCHEMA[c] for c in _BAR_COLS},
    )


def _staged(cfg, run_id) -> pl.DataFrame:
    files = StagingWriter(cfg.staging_root).list_run_files("daily_bars", run_id)
    if not files:
        return pl.DataFrame()
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")


# --- routing ----------------------------------------------------------------


def test_only_sh_and_sz_are_tdx_servable():
    """Not because the protocol cannot serve Beijing — it carries its daily
    bars under market id 2, and `_fetch_bj_history_via_tdx` uses that. The rest
    of the pipeline reads this predicate as "Baostock serves this symbol"
    (suspension evidence, ST history), and Baostock is Shanghai and Shenzhen
    only: a Beijing code there costs a retried failure and answers nothing."""
    assert is_tdx_servable("600519.SH") and is_tdx_servable("000001.SZ")
    assert not is_tdx_servable("920000.BJ")
    assert not is_tdx_servable("garbage")


def test_split_preserves_order_within_each_side():
    tdx, fallback = split_by_quote_source(["600519.SH", "920001.BJ", "000001.SZ", "920000.BJ"])

    assert tdx == ["600519.SH", "000001.SZ"]
    assert fallback == ["920001.BJ", "920000.BJ"]


# --- fallback fetch ---------------------------------------------------------


def test_fallback_bars_are_staged_with_their_own_provenance(tmp_path):
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})

    result = fetch_bars_via_sina(
        cfg,
        ["920000.BJ", "920001.BJ"],
        date(2026, 7, 20),
        date(2026, 7, 21),
        "run-1",
        fetch=lambda s, c: _bars(s, [date(2026, 7, 20), date(2026, 7, 21)]),
    )

    staged = _staged(cfg, "run-1")
    assert result["rows_written"] == 4
    assert set(staged["symbol"]) == {"920000.BJ", "920001.BJ"}
    assert staged["source"].unique().to_list() == ["sina"]


def test_bse_is_primary_for_a_current_single_session(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"sina": True, "bse": True},
        source_intervals={"bse": 0.0, "sina_bars": 0.0},
    )
    day = date(2026, 8, 21)
    bse = _bars("920000.BJ", [day]).with_columns(pl.lit(1234.5).alias("amount"))
    monkeypatch.setattr("cnequity.steps.bars.list_trading_dates", lambda *args: [day])
    monkeypatch.setattr(
        "cnequity.adapters.bse.daily_quotes.fetch_daily_quotes", lambda *args, **kwargs: bse
    )

    def no_sina(*args, **kwargs):
        raise AssertionError("Sina should not be called when BSE covers the BJ tip")

    monkeypatch.setattr("cnequity.adapters.sina.bars.fetch_daily_bars_sina", no_sina)
    result = fetch_bars_via_sina(
        cfg,
        ["920000.BJ"],
        day - timedelta(days=1),
        day,
        "run-bse",
    )

    staged = _staged(cfg, "run-bse")
    assert result["rows_written"] == 1
    assert staged["amount"].item() == 1234.5
    assert staged["source"].item() == "bse"
    assert result["context_updates"]["audit_findings"][0]["check"] == "daily_bars_bse_tip"


def test_bse_rows_are_counted_with_sina_residuals(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"sina": True, "bse": True},
        source_intervals={"bse": 0.0, "sina_bars": 0.0},
    )
    day = date(2026, 8, 21)
    bse = _bars("920000.BJ", [day]).with_columns(pl.lit(1234.5).alias("amount"))
    monkeypatch.setattr("cnequity.steps.bars.list_trading_dates", lambda *args: [day])
    monkeypatch.setattr(
        "cnequity.adapters.bse.daily_quotes.fetch_daily_quotes", lambda *args, **kwargs: bse
    )
    monkeypatch.setattr(
        "cnequity.adapters.sina.bars.fetch_daily_bars_sina",
        lambda symbol, **kwargs: _bars(symbol, [day]),
    )

    result = fetch_bars_via_sina(
        cfg,
        ["920000.BJ", "600519.SH"],
        day - timedelta(days=1),
        day,
        "run-mixed",
    )

    staged = _staged(cfg, "run-mixed")
    assert result["rows_written"] == 2
    assert set(staged["source"].unique().to_list()) == {"bse", "sina"}


def test_bse_tip_amount_requires_exact_sina_ohlcv(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"sina": True, "bse": True},
        source_intervals={"bse": 0.0},
    )
    day = date(2026, 8, 21)
    bse = _bars("920000.BJ", [day]).with_columns(pl.lit(1234.5).alias("amount"))
    monkeypatch.setattr(
        "cnequity.adapters.bse.daily_quotes.fetch_daily_quotes", lambda *a, **k: bse
    )

    result = fetch_bars_via_sina(
        cfg,
        ["920000.BJ"],
        day,
        day,
        "run-1",
        fetch=lambda s, c: _bars(s, [day]),
    )

    staged = _staged(cfg, "run-1")
    assert staged["amount"].item() == 1234.5
    assert staged["source"].item() == "bse"
    assert result["context_updates"]["audit_findings"][0]["check"] == (
        "daily_bars_bse_amount_supplement"
    )


def test_bse_tip_mismatch_keeps_sina_amount_null(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"sina": True, "bse": True},
        source_intervals={"bse": 0.0},
    )
    day = date(2026, 8, 21)
    bse = _bars("920000.BJ", [day]).with_columns(
        pl.lit(2.0).alias("close"),
        pl.lit(1234.5).alias("amount"),
    )
    monkeypatch.setattr(
        "cnequity.adapters.bse.daily_quotes.fetch_daily_quotes", lambda *a, **k: bse
    )

    result = fetch_bars_via_sina(
        cfg,
        ["920000.BJ"],
        day,
        day,
        "run-1",
        fetch=lambda s, c: _bars(s, [day]),
    )

    staged = _staged(cfg, "run-1")
    assert staged["amount"].item() is None
    assert staged["source"].item() == "sina"
    assert result["context_updates"]["audit_findings"][0]["check"] == (
        "daily_bars_bse_quote_mismatch"
    )


def test_scoped_daily_backfill_rejects_unknown_instrument(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr("cnequity.steps.bars.load_symbols", lambda config: ["920000.BJ"])

    with pytest.raises(RuntimeError, match="not present in instruments"):
        _resolve_daily_bar_scope(cfg, ["920000.BJ", "999999.BJ"])


def test_bse_curated_repair_does_not_call_sina(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"bse": True},
        source_intervals={"bse": 0.0},
    )
    day = date(2026, 8, 21)
    part = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
    part.mkdir(parents=True)
    _bars("920000.BJ", [day]).write_parquet(part / "part-merged.parquet")
    bse = _bars("920000.BJ", [day]).with_columns(pl.lit(1234.5).alias("amount"))
    monkeypatch.setattr("cnequity.steps.bars.load_symbols", lambda config: ["920000.BJ"])
    monkeypatch.setattr(
        "cnequity.adapters.bse.daily_quotes.fetch_daily_quotes", lambda *a, **k: bse
    )

    result = repair_bse_tip_amounts_from_curated(cfg, day, "run-repair", ["920000.BJ"])

    staged = _staged(cfg, "run-repair")
    assert result["rows_written"] == 1
    assert staged["amount"].item() == 1234.5
    assert staged["source"].item() == "bse"


def test_bse_curated_repair_does_not_claim_success_when_bse_is_unavailable(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"bse": True},
        source_intervals={"bse": 0.0},
    )
    day = date(2026, 8, 21)
    part = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
    part.mkdir(parents=True)
    _bars("920000.BJ", [day]).write_parquet(part / "part-merged.parquet")
    monkeypatch.setattr("cnequity.steps.bars.load_symbols", lambda config: ["920000.BJ"])
    monkeypatch.setattr(
        "cnequity.adapters.bse.daily_quotes.fetch_daily_quotes",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("BSE down")),
    )

    result = repair_bse_tip_amounts_from_curated(cfg, day, "run-repair", ["920000.BJ"])

    assert result["status"] == "warning"
    assert result["rows_written"] == 0
    assert result["context_updates"]["audit_findings"][0]["check"] == (
        "daily_bars_bse_amount_unavailable"
    )


def test_one_dead_symbol_does_not_cost_the_whole_board(tmp_path):
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})

    def flaky(symbol, client):
        if symbol == "920000.BJ":
            raise ConnectionError("reset")
        return _bars(symbol, [date(2026, 7, 21)])

    result = fetch_bars_via_sina(
        cfg, ["920000.BJ", "920001.BJ"], date(2026, 7, 21), date(2026, 7, 21), "run-1", fetch=flaky
    )

    assert result["rows_written"] == 1
    assert result["failed_symbols"] == 1
    assert result["failed_symbol_names"] == ["920000.BJ"]
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["check"] == "fallback_source_incomplete"
    assert "920000.BJ" in finding["message"]


def test_no_fallback_symbols_is_a_cheap_noop(tmp_path):
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})

    def must_not_be_called(symbol, client):
        raise AssertionError("fetched nothing-to-fetch")

    result = fetch_bars_via_sina(
        cfg, [], date(2026, 7, 21), date(2026, 7, 21), "run-1", fetch=must_not_be_called
    )
    assert result["rows_written"] == 0


def test_sina_bars_retries_transient_rate_limit(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"sina": True, "sina_bars": True},
        source_intervals={"sina_bars": 0.0},
        retry_backoff_seconds=5,
    )
    attempts = 0
    deferrals: list[tuple[str, float]] = []
    request = httpx.Request("GET", "https://example.test/sina")

    def flaky(symbol, client):
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            response = httpx.Response(456, request=request)
            raise httpx.HTTPStatusError("rate limited", request=request, response=response)
        return _bars(symbol, [date(2026, 7, 21)])

    monkeypatch.setattr(
        cfg, "defer_source", lambda source, seconds: deferrals.append((source, seconds))
    )
    result = fetch_bars_via_sina(
        cfg,
        ["920000.BJ"],
        date(2026, 7, 21),
        date(2026, 7, 21),
        "run-retry",
        fetch=flaky,
    )

    assert attempts == 2
    assert deferrals == [("sina_bars", 30.0)]
    assert result["rows_written"] == 1
    assert "failed_symbols" not in result
    assert result["source_outcomes"]["sina"]["failure_reasons"] == {}


def test_sina_bars_opens_circuit_after_repeated_rate_limit(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"sina": True, "sina_bars": True},
        source_intervals={"sina_bars": 0.0},
        source_concurrency={"sina_bars": 1},
    )
    attempts = 0
    deferrals: list[tuple[str, float]] = []
    request = httpx.Request("GET", "https://example.test/sina")

    def blocked(symbol, client):
        nonlocal attempts
        attempts += 1
        response = httpx.Response(456, request=request)
        raise httpx.HTTPStatusError("rate limited", request=request, response=response)

    monkeypatch.setattr(
        cfg, "defer_source", lambda source, seconds: deferrals.append((source, seconds))
    )
    result = fetch_bars_via_sina(
        cfg,
        ["920000.BJ", "920001.BJ"],
        date(2026, 7, 21),
        date(2026, 7, 21),
        "run-circuit",
        fetch=blocked,
    )

    assert attempts == 2
    assert deferrals == [("sina_bars", 30.0), ("sina_bars", 120.0)]
    assert result["failed_symbols"] == 2
    assert result["source_outcomes"]["sina"]["failure_reasons"] == {
        "circuit_open": 1,
        "rate_limited": 1,
    }


# --- instruments ------------------------------------------------------------


def _live_instruments(symbols):
    return pl.DataFrame(
        {
            "symbol": list(symbols),
            "name": ["x"] * len(symbols),
            "exchange": [s.split(".")[1] for s in symbols],
            "asset_type": ["stock"] * len(symbols),
            "list_date": pl.Series([None] * len(symbols), dtype=pl.Date),
            "delist_date": pl.Series([None] * len(symbols), dtype=pl.Date),
            "prev_symbol": [None] * len(symbols),
            "source": ["tdx_protocol"] * len(symbols),
            "data_version": ["v1"] * len(symbols),
        }
    )


def _cfg_with_catalog(tmp_path, entries: dict[str, str], bars_through: date):
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})
    path = catalog_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"delisted": entries, "never_issued": []}))
    part = cfg.curated_root / "daily_bars" / f"trade_date={bars_through.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [bars_through]}).write_parquet(
        part / "part-merged.parquet"
    )
    return cfg


def test_beijing_symbols_are_added_to_the_instrument_list(tmp_path):
    """Without this the daily step never sees them and BJ stays empty forever."""
    cfg = _cfg_with_catalog(
        tmp_path, {"920000.BJ": "2026-07-21", "600001.SH": "2009-12-15"}, date(2026, 7, 21)
    )

    out = _merge_untdxable_instruments(cfg, _live_instruments(["600519.SH"]))

    assert set(out["symbol"]) == {"600519.SH", "920000.BJ"}
    bj = out.filter(pl.col("symbol") == "920000.BJ")
    assert bj["delist_date"].item() is None, "a trading stock must not carry a delist_date"
    assert bj["source"].item() == "sina"


def test_delisted_names_are_not_added_by_this_path(tmp_path):
    """Historical delistings belong to the backfill, not the live instrument list."""
    cfg = _cfg_with_catalog(tmp_path, {"600001.SH": "2009-12-15"}, date(2026, 7, 21))

    out = _merge_untdxable_instruments(cfg, _live_instruments(["600519.SH"]))

    assert set(out["symbol"]) == {"600519.SH"}


def test_a_missing_catalogue_is_not_fatal(tmp_path):
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})
    live = _live_instruments(["600519.SH"])

    assert _merge_untdxable_instruments(cfg, live).equals(live)


def _board(symbols_to_names: dict[str, str]):
    symbols = sorted(symbols_to_names)
    return pl.DataFrame(
        {
            "symbol": symbols,
            "name": [symbols_to_names[s] for s in symbols],
            "exchange": ["BJ"] * len(symbols),
            "asset_type": ["stock"] * len(symbols),
            "list_date": pl.Series([None] * len(symbols), dtype=pl.Date),
            "delist_date": pl.Series([None] * len(symbols), dtype=pl.Date),
            "prev_symbol": pl.Series([None] * len(symbols), dtype=pl.Utf8),
        }
    )


def _patch_board(monkeypatch, result):
    import cnequity.adapters.bse.instruments as bse_instruments

    def _fetch(trade_date, *, client=None, config=None):
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(bse_instruments, "fetch_bse_instruments", _fetch)


def test_the_beijing_board_discovers_listings_the_sweep_has_never_seen(tmp_path, monkeypatch):
    """`_merge_untdxable_instruments` replays the last code-space sweep, so it
    can keep a known BJ name alive but can never find a new one. Sixteen
    Beijing stocks were trading on 2026-09-15 with no row in the lake at all."""
    from cnequity.steps.reference import _merge_bse_instruments

    cfg = Config(data_root=tmp_path / "data", sources={"bse": True})
    _patch_board(monkeypatch, _board({"920038.BJ": "森合高科", "920023.BJ": "*ST田野"}))

    out = _merge_bse_instruments(cfg, _live_instruments(["600519.SH"]), date(2026, 9, 15))

    assert set(out["symbol"]) == {"600519.SH", "920038.BJ", "920023.BJ"}
    bj = out.filter(pl.col("symbol") == "920023.BJ")
    assert bj["source"].item() == "bse"
    # Beijing's ST designation is in 证券简称 and nowhere else this reaches.
    assert bj["name"].item() == "*ST田野"
    assert bj["delist_date"].item() is None


def test_a_beijing_board_outage_leaves_the_snapshot_exactly_as_it_was(tmp_path, monkeypatch):
    """This path is additive. It must never be able to turn a bad Beijing day
    into a day that inferred delistings."""
    from cnequity.steps.reference import _merge_bse_instruments

    cfg = Config(data_root=tmp_path / "data", sources={"bse": True})
    live = _live_instruments(["600519.SH"])
    _patch_board(monkeypatch, RuntimeError("bse down"))

    assert _merge_bse_instruments(cfg, live, date(2026, 9, 15)).equals(live)


def test_a_disabled_bse_source_is_not_consulted(tmp_path, monkeypatch):
    from cnequity.steps.reference import _merge_bse_instruments

    cfg = Config(data_root=tmp_path / "data", sources={"bse": False})
    live = _live_instruments(["600519.SH"])
    _patch_board(monkeypatch, AssertionError("must not be called"))

    assert _merge_bse_instruments(cfg, live, date(2026, 9, 15)).equals(live)


def test_recovered_beijing_names_are_enriched_not_stranded(tmp_path, monkeypatch):
    """Discovery and enrichment have to run in that order.

    A Beijing row arrives with no list_date — neither TDX nor the BSE board
    carries one — and a symbol with no list_date and no traded bar is exactly
    what `classify_daily_bar_ownership` holds back as a not-yet-listed
    placeholder. Enriching before the merge therefore discovered sixteen names
    and then guaranteed none of them would ever be fetched.
    """
    import cnequity.adapters.eastmoney.instruments as em_instruments
    import cnequity.steps.reference as reference

    cfg = Config(data_root=tmp_path / "data", sources={"bse": True, "eastmoney": True})
    _patch_board(monkeypatch, _board({"920038.BJ": "森合高科"}))
    monkeypatch.setattr(
        em_instruments, "fetch_list_date_map", lambda **_: {"920038.BJ": date(2026, 8, 5)}
    )
    monkeypatch.setattr(
        reference, "fetch_instruments", lambda **_: _live_instruments(["600519.SH"])
    )
    monkeypatch.setattr(reference, "normalize_with_source", lambda df, *_a, **_k: df)
    written: dict = {}

    def _capture(config, run_id, dataset, df):
        written["df"] = df
        return {}

    monkeypatch.setattr(reference, "write_simple", _capture)

    reference.step_instruments(cfg, date(2026, 9, 15), "run-1", {})

    bj = written["df"].filter(pl.col("symbol") == "920038.BJ")
    assert bj.height == 1
    assert bj["list_date"].item() == date(2026, 8, 5)


def test_sina_circuit_survives_multiple_gapfill_passes_and_resets_next_run(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path, source_intervals={"sina_bars": 0.0})
    monkeypatch.setattr(cfg, "defer_source", lambda *args: None)
    calls = []
    request = httpx.Request("GET", "https://example.test/sina")

    def blocked(symbol, client):
        calls.append(symbol)
        raise httpx.HTTPStatusError(
            "rate limited", request=request, response=httpx.Response(456, request=request)
        )

    day = date(2026, 7, 21)
    fetch_bars_via_sina(cfg, ["600519.SH"], day, day, "same-run", fetch=blocked)
    later = fetch_bars_via_sina(cfg, ["000001.SZ"], day, day, "same-run", fetch=blocked)
    assert len(calls) == 2
    assert later["source_outcomes"]["sina"]["failure_reasons"] == {"circuit_open": 1}
    fetch_bars_via_sina(cfg, ["000001.SZ"], day, day, "next-run", fetch=blocked)
    assert len(calls) == 4
    # Config later crosses a process-pool boundary. Circuit state cannot hold
    # a thread lock, even though the request workers within this call are threads.
    import pickle

    monkeypatch.undo()
    # The normal limiter cache is intentionally process-local as well.
    cfg._rate_limiters = None
    pickle.dumps(cfg)


# --- Beijing history via TDX ------------------------------------------------


def test_the_tip_session_is_left_to_the_exchange_snapshot(tmp_path, monkeypatch):
    """TDX reports volume in lots — 3,085 of 3,086 measured rows land within
    100 shares of the exact figure — so the current session keeps coming from
    BSE, which publishes shares. TDX only fills the history behind it."""
    from cnequity.steps import bars

    cfg = Config(data_root=tmp_path / "data")
    windows: list[tuple[date, date]] = []
    monkeypatch.setattr(
        bars, "list_trading_dates", lambda _c, lo, hi: [date(2026, 9, d) for d in (15, 16, 17)]
    )
    monkeypatch.setattr(
        bars,
        "fetch_daily_bars_parallel",
        lambda _c, _s, lo, hi, *a, **k: (
            windows.append((lo, hi)) or {"rows_read": 0, "rows_written": 0, "failed_symbols": []}
        ),
    )
    monkeypatch.setattr(bars, "_bj_history_covered", lambda *a, **k: set())

    bars._fetch_bj_history_via_tdx(
        cfg, ["920001.BJ"], date(2026, 9, 15), date(2026, 9, 17), "r1", reserve_tip=True
    )

    assert windows == [(date(2026, 9, 15), date(2026, 9, 16))]


def test_a_chunked_backfill_has_no_tip_to_reserve(tmp_path, monkeypatch):
    from cnequity.steps import bars

    cfg = Config(data_root=tmp_path / "data")
    windows: list[tuple[date, date]] = []
    monkeypatch.setattr(
        bars, "list_trading_dates", lambda _c, lo, hi: [date(2026, 9, d) for d in (15, 16, 17)]
    )
    monkeypatch.setattr(
        bars,
        "fetch_daily_bars_parallel",
        lambda _c, _s, lo, hi, *a, **k: (
            windows.append((lo, hi)) or {"rows_read": 0, "rows_written": 0, "failed_symbols": []}
        ),
    )
    monkeypatch.setattr(bars, "_bj_history_covered", lambda *a, **k: set())

    bars._fetch_bj_history_via_tdx(
        cfg, ["920001.BJ"], date(2026, 9, 15), date(2026, 9, 17), "r1", reserve_tip=False
    )

    assert windows == [(date(2026, 9, 15), date(2026, 9, 17))]


def test_a_tip_only_window_asks_tdx_for_nothing(tmp_path, monkeypatch):
    from cnequity.steps import bars

    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(bars, "list_trading_dates", lambda _c, lo, hi: [date(2026, 9, 17)])
    monkeypatch.setattr(bars, "fetch_daily_bars_parallel", pytest.fail)

    out = bars._fetch_bj_history_via_tdx(
        cfg, ["920001.BJ"], date(2026, 9, 17), date(2026, 9, 17), "r1", reserve_tip=True
    )

    assert out == {"rows_read": 0, "rows_written": 0, "covered": set(), "requested": False}


def test_a_symbol_tdx_reported_failed_is_not_counted_as_covered(tmp_path, monkeypatch):
    """Otherwise Sina is never asked for it and the gap survives the run."""
    from cnequity.steps import bars

    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        bars, "list_trading_dates", lambda _c, lo, hi: [date(2026, 9, d) for d in (15, 16)]
    )
    monkeypatch.setattr(
        bars,
        "fetch_daily_bars_parallel",
        lambda *a, **k: {"rows_read": 0, "rows_written": 0, "failed_symbols": ["920001.BJ"]},
    )
    monkeypatch.setattr(bars, "_bj_history_covered", lambda *a, **k: {"920001.BJ", "920002.BJ"})

    out = bars._fetch_bj_history_via_tdx(
        cfg,
        ["920001.BJ", "920002.BJ"],
        date(2026, 9, 15),
        date(2026, 9, 16),
        "r1",
        reserve_tip=False,
    )

    assert out["covered"] == {"920002.BJ"}
