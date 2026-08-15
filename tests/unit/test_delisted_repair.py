"""Repair path: instruments from existing bars + catalog, no re-fetch."""

import json
from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.domain.schemas import DAILY_BARS_SCHEMA
from cn_market_lake.steps.delisted import (
    _ingested_symbols,
    catalog_path,
    delisted_symbols_in_window,
    repair_delisted_instruments,
)
from cn_market_lake.storage.parquet import StagingWriter

_BAR_COLS = [c for c in DAILY_BARS_SCHEMA if c not in ("source", "data_version", "fetched_at")]


def _write_bars(cfg: Config, symbol: str, first: date, last: date) -> None:
    for d in (first, last):
        part = cfg.curated_root / "daily_bars" / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": [symbol],
                "trade_date": [d],
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [10],
                "amount": [None],
            },
            schema={c: DAILY_BARS_SCHEMA[c] for c in _BAR_COLS},
        ).write_parquet(part / "part-merged.parquet")


def _cfg(tmp_path, catalog: dict[str, str], live=("600519.SH",)):
    """``live`` entries are symbols, or ``(symbol, name)`` pairs."""
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})
    path = catalog_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"delisted": catalog, "never_issued": []}))
    inst = cfg.curated_root / "instruments"
    inst.mkdir(parents=True, exist_ok=True)
    symbols, name_list = [], []
    for item in live:
        if isinstance(item, tuple):
            symbols.append(item[0])
            name_list.append(item[1])
        else:
            symbols.append(item)
            name_list.append("live")
    pl.DataFrame(
        {
            "symbol": symbols,
            "name": name_list,
            "exchange": [s.split(".")[1] for s in symbols],
            "asset_type": ["etf" if n == "认购款" else "stock" for n in name_list],
            "list_date": pl.Series([date(2000, 1, 1)] * len(symbols), dtype=pl.Date),
            "delist_date": pl.Series([None] * len(symbols), dtype=pl.Date),
            "prev_symbol": [None] * len(symbols),
        }
    ).write_parquet(inst / "part-merged.parquet")
    # Anchor the lake's last session so classify_catalog's recency gate is stable.
    _write_bars(cfg, "600519.SH", date(2026, 7, 20), date(2026, 7, 24))
    return cfg


def _staged(cfg, dataset, run_id) -> pl.DataFrame:
    files = StagingWriter(cfg.staging_root).list_run_files(dataset, run_id)
    if not files:
        return pl.DataFrame()
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")


def test_repair_writes_instruments_from_existing_bar_spans(tmp_path):
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10"})
    _write_bars(cfg, "600070.SH", date(2016, 3, 1), date(2025, 4, 10))

    result = repair_delisted_instruments(cfg, "run-1")

    staged = _staged(cfg, "instruments", "run-1")
    row = staged.filter(pl.col("symbol") == "600070.SH")
    assert result["with_bars"] == 1
    assert row["list_date"].item() == date(2016, 3, 1)
    assert row["delist_date"].item() == date(2025, 4, 10)
    assert "600519.SH" in staged["symbol"].to_list()


def test_repair_marks_catalogued_symbols_with_bars_as_ingested(tmp_path):
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10", "600071.SH": "2024-01-01"})
    _write_bars(cfg, "600070.SH", date(2016, 3, 1), date(2025, 4, 10))

    result = repair_delisted_instruments(cfg, "run-1")

    assert "600070.SH" in _ingested_symbols(cfg)
    assert "600071.SH" not in _ingested_symbols(cfg)
    assert delisted_symbols_in_window(cfg, date(2016, 1, 1)) == ["600071.SH"]
    assert result["still_need_bars"] == ["600071.SH"]


def test_repair_purges_subscription_placeholders(tmp_path):
    cfg = _cfg(
        tmp_path,
        {"600070.SH": "2025-04-10"},
        live=("600519.SH", ("515844.SH", "认购款")),
    )
    _write_bars(cfg, "600070.SH", date(2016, 3, 1), date(2025, 4, 10))

    result = repair_delisted_instruments(cfg, "run-1")

    staged = _staged(cfg, "instruments", "run-1")
    assert "515844.SH" not in staged["symbol"].to_list()
    curated = pl.read_parquet(cfg.curated_root / "instruments" / "part-merged.parquet")
    assert "515844.SH" not in curated["symbol"].to_list()
    assert result["purged_placeholders"] == 1


def test_repair_picks_up_orphan_bars_not_in_the_catalog(tmp_path):
    """Baostock bars without a catalog entry still need a delist_date."""
    cfg = _cfg(tmp_path, {})
    # Last bar well before the lake's 2026-07-24 session (>180d gap).
    _write_bars(cfg, "000018.SZ", date(2016, 1, 4), date(2020, 1, 7))

    result = repair_delisted_instruments(cfg, "run-1")

    staged = _staged(cfg, "instruments", "run-1")
    row = staged.filter(pl.col("symbol") == "000018.SZ")
    assert result["from_orphan_bars"] == 1
    assert row["delist_date"].item() == date(2020, 1, 7)
    assert row["list_date"].item() == date(2016, 1, 4)


def test_instruments_rows_fills_null_list_date_from_recovery_spans(tmp_path):
    """A prior repair with no bars must not shadow a later backfill's list_date."""
    from cn_market_lake.steps.delisted import _instruments_rows

    cfg = _cfg(tmp_path, {})
    # Hollow row as left by repair when bars were missing.
    inst = cfg.curated_root / "instruments" / "part-merged.parquet"
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "600070.SH"],
            "name": ["live", None],
            "exchange": ["SH", "SH"],
            "asset_type": ["stock", "stock"],
            "list_date": pl.Series([date(2000, 1, 1), None], dtype=pl.Date),
            "delist_date": pl.Series([None, date(2025, 4, 10)], dtype=pl.Date),
            "prev_symbol": [None, None],
        }
    ).write_parquet(inst)

    out = _instruments_rows(cfg, {"600070.SH": (date(2016, 3, 1), date(2025, 4, 10))})
    row = out.filter(pl.col("symbol") == "600070.SH")
    assert row["list_date"].item() == date(2016, 3, 1)
    assert row["delist_date"].item() == date(2025, 4, 10)


def test_repair_respects_since_window_for_catalog(tmp_path):
    cfg = _cfg(
        tmp_path,
        {"600001.SH": "2009-12-15", "600070.SH": "2025-04-10"},
    )
    _write_bars(cfg, "600070.SH", date(2016, 3, 1), date(2025, 4, 10))

    result = repair_delisted_instruments(cfg, "run-1", start=date(2016, 1, 1))

    staged = _staged(cfg, "instruments", "run-1")
    assert "600070.SH" in staged["symbol"].to_list()
    assert "600001.SH" not in staged["symbol"].to_list()
    assert result["from_catalog"] == 1
