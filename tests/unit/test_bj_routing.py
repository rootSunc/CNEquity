"""Beijing exchange has no TDX route — bars must come from the fallback vendor."""

import json
from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.domain.schemas import DAILY_BARS_SCHEMA
from cn_market_lake.domain.symbols import is_tdx_servable, split_by_quote_source
from cn_market_lake.steps.bars import fetch_bars_via_sina
from cn_market_lake.steps.delisted import catalog_path
from cn_market_lake.steps.reference import _merge_untdxable_instruments
from cn_market_lake.storage.parquet import StagingWriter

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
    assert is_tdx_servable("600519.SH") and is_tdx_servable("000001.SZ")
    assert not is_tdx_servable("920000.BJ"), "TDX has no Beijing market id"
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
