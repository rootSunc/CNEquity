"""Ingesting catalogued delistings into daily_bars + instruments."""

import json
from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.domain.schemas import DAILY_BARS_SCHEMA, with_provenance
from cn_market_lake.orchestrator.engine import JobEngine
from cn_market_lake.steps.delisted import (
    _ingested_symbols,
    backfill_delisted_bars,
    catalog_path,
    delisted_symbols_in_window,
)
from cn_market_lake.storage.parquet import StagingWriter

_START = date(2016, 1, 1)
_BAR_COLS = [c for c in DAILY_BARS_SCHEMA if c not in ("source", "data_version", "fetched_at")]


def _bars(symbol: str, first: date, last: date) -> pl.DataFrame:
    days = [first, last]
    return pl.DataFrame(
        {
            "symbol": [symbol] * 2,
            "trade_date": days,
            "open": [1.0, 2.0],
            "high": [1.0, 2.0],
            "low": [1.0, 2.0],
            "close": [1.0, 2.0],
            "volume": [10, 20],
            "amount": [None, None],
        },
        schema={c: DAILY_BARS_SCHEMA[c] for c in _BAR_COLS},
    )


def _cfg(tmp_path, catalog: dict[str, str], live=("600519.SH",)):
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})
    path = catalog_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"delisted": catalog, "never_issued": []}))
    inst = cfg.curated_root / "instruments"
    inst.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "symbol": list(live),
            "name": ["live"] * len(live),
            "exchange": [s.split(".")[1] for s in live],
            "asset_type": ["stock"] * len(live),
            "list_date": pl.Series([date(2000, 1, 1)] * len(live), dtype=pl.Date),
            "delist_date": pl.Series([None] * len(live), dtype=pl.Date),
            "prev_symbol": [None] * len(live),
        }
    )
    with_provenance(frame, source="test_seed", data_version="v1").write_parquet(
        inst / "part-merged.parquet"
    )
    return cfg


def _staged(cfg, dataset, run_id) -> pl.DataFrame:
    files = StagingWriter(cfg.staging_root).list_run_files(dataset, run_id)
    if not files:
        return pl.DataFrame()
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")


def test_only_delistings_overlapping_the_window_are_fetched(tmp_path):
    """A name gone before the lake starts contributes nothing to a backtest over it."""
    cfg = _cfg(tmp_path, {"600001.SH": "2009-12-15", "600070.SH": "2025-04-10"})

    assert delisted_symbols_in_window(cfg, _START) == ["600070.SH"]


def test_bars_are_staged_with_sina_provenance(tmp_path):
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10"})

    result = backfill_delisted_bars(
        cfg, "run-1", _START, fetch=lambda s, c: _bars(s, date(2016, 3, 1), date(2025, 4, 10))
    )

    staged = _staged(cfg, "daily_bars", "run-1")
    assert result["rows_written"] == 2
    assert staged["symbol"].unique().to_list() == ["600070.SH"]
    assert staged["source"].unique().to_list() == ["sina"]


def test_instruments_row_dates_the_delisting_from_the_last_bar(tmp_path):
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10"})

    backfill_delisted_bars(
        cfg, "run-1", _START, fetch=lambda s, c: _bars(s, date(2016, 3, 1), date(2025, 4, 10))
    )

    inst = _staged(cfg, "instruments", "run-1")
    row = inst.filter(pl.col("symbol") == "600070.SH")
    assert row["list_date"].item() == date(2016, 3, 1)
    assert row["delist_date"].item() == date(2025, 4, 10)
    assert row["asset_type"].item() == "stock"


def test_instruments_staging_keeps_the_live_snapshot(tmp_path):
    """Staging only recovered names would look like a mass delisting to compact."""
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10"}, live=("600519.SH", "000001.SZ"))

    backfill_delisted_bars(
        cfg, "run-1", _START, fetch=lambda s, c: _bars(s, date(2016, 3, 1), date(2025, 4, 10))
    )

    inst = _staged(cfg, "instruments", "run-1")
    assert set(inst["symbol"]) == {"600519.SH", "000001.SZ", "600070.SH"}
    # A live symbol must keep its null delist_date.
    assert inst.filter(pl.col("symbol") == "600519.SH")["delist_date"].item() is None


def test_spans_accumulate_across_staging_chunks(tmp_path):
    """Chunked staging must not drop earlier symbols from the instruments rows."""
    catalog = {f"6001{i:02d}.SH": "2025-04-10" for i in range(120)}
    cfg = _cfg(tmp_path, catalog)

    backfill_delisted_bars(
        cfg, "run-1", _START, fetch=lambda s, c: _bars(s, date(2016, 3, 1), date(2025, 4, 10))
    )

    inst = _staged(cfg, "instruments", "run-1")
    recovered = set(inst["symbol"]) - {"600519.SH"}
    assert len(recovered) == 120, "every chunk's symbols must reach instruments"


def test_a_failed_symbol_is_not_marked_done_so_a_rerun_retries_it(tmp_path):
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10", "600083.SH": "2025-01-16"})

    def flaky(symbol, client):
        if symbol == "600070.SH":
            raise ConnectionError("reset")
        return _bars(symbol, date(2016, 3, 1), date(2025, 1, 16))

    result = backfill_delisted_bars(cfg, "run-1", _START, fetch=flaky)

    assert result["failed_symbols"] == 1
    assert "600070.SH" not in _ingested_symbols(cfg)
    assert "600083.SH" in _ingested_symbols(cfg)
    assert delisted_symbols_in_window(cfg, _START) == ["600070.SH"]


def test_rerun_is_a_noop_once_everything_is_ingested(tmp_path):
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10"})
    backfill_delisted_bars(
        cfg, "run-1", _START, fetch=lambda s, c: _bars(s, date(2016, 3, 1), date(2025, 4, 10))
    )
    JobEngine(cfg).run_step("compact", date(2025, 4, 10), "run-1")

    def must_not_be_called(symbol, client):
        raise AssertionError(f"refetched {symbol}")

    result = backfill_delisted_bars(cfg, "run-2", _START, fetch=must_not_be_called)
    assert result["rows_written"] == 0
    assert result["coverage_pending_compact"] is True


def test_empty_fetch_without_terminal_evidence_stays_retryable(tmp_path):
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10"})

    result = backfill_delisted_bars(
        cfg,
        "run-1",
        _START,
        fetch=lambda s, c: pl.DataFrame(schema={c: DAILY_BARS_SCHEMA[c] for c in _BAR_COLS}),
        probe_last=lambda s, c: None,
    )

    assert result["recovered"] == 0
    assert result["status"] == "warning"
    assert result["unresolved_symbols"] == 1
    assert "600070.SH" not in _ingested_symbols(cfg)
    assert _staged(cfg, "instruments", "run-1").is_empty()


def test_empty_fetch_with_pre_window_terminal_is_expected_no_data(tmp_path):
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10"})

    result = backfill_delisted_bars(
        cfg,
        "run-1",
        _START,
        fetch=lambda s, c: pl.DataFrame(schema={c: DAILY_BARS_SCHEMA[c] for c in _BAR_COLS}),
        probe_last=lambda s, c: date(2009, 12, 15),
    )

    assert result["expected_no_data"] == 1
    assert result["coverage_pending_compact"] is True
    assert "600070.SH" not in _ingested_symbols(cfg)
