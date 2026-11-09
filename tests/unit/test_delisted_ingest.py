"""Ingesting catalogued delistings into daily_bars + instruments."""

import json
from datetime import date
from pathlib import Path

import httpx
import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.schemas import DAILY_BARS_SCHEMA, with_provenance
from cnequity.orchestrator.engine import JobEngine
from cnequity.steps.delisted import (
    _ingested_symbols,
    backfill_delisted_bars,
    catalog_path,
    delisted_symbols_in_window,
)
from cnequity.storage.parquet import StagingWriter

_START = date(2016, 1, 1)
_BAR_COLS = [
    c for c in DAILY_BARS_SCHEMA if c not in ("source", "data_version", "fetched_at", "pre_close")
]


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
    # Fetches are injected fixture functions. Keep source admission enabled
    # while avoiding real pacing waits (including in the mocked retry clock).
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"sina": True},
        source_intervals={"sina": 0, "sina_bars": 0},
    )
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


def test_zero_volume_terminal_is_not_recorded_as_last_traded_date(tmp_path):
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10"})
    bars = pl.concat(
        [
            _bars("600070.SH", date(2016, 3, 1), date(2025, 4, 10)),
            _bars("600070.SH", date(2025, 4, 11), date(2025, 4, 11)).with_columns(
                pl.lit(0).cast(pl.Int64).alias("volume")
            ),
        ],
        how="diagonal_relaxed",
    )

    result = backfill_delisted_bars(cfg, "run-1", _START, fetch=lambda s, c: bars)

    checkpoint = json.loads(Path(result["checkpoint"]).read_text(encoding="utf-8"))
    assert checkpoint["recovered_spans"]["600070.SH"]["last_trade_date"] == "2025-04-10"


def test_existing_curated_terminal_is_reused_without_provider_fetch(tmp_path):
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10", "600083.SH": "2025-01-16"})
    existing = _bars("600070.SH", date(2016, 3, 1), date(2025, 4, 10))
    for day in existing["trade_date"].unique().to_list():
        part = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        existing.filter(pl.col("trade_date") == day).write_parquet(part / "part.parquet")
    anchor_day = date(2026, 8, 14)
    anchor = cfg.curated_root / "daily_bars" / f"trade_date={anchor_day.isoformat()}"
    anchor.mkdir(parents=True, exist_ok=True)
    _bars("600519.SH", anchor_day, anchor_day).write_parquet(anchor / "anchor.parquet")
    fetched: list[str] = []

    def fetch(symbol, client):
        fetched.append(symbol)
        return _bars(symbol, date(2016, 3, 1), date(2025, 1, 16))

    result = backfill_delisted_bars(cfg, "run-1", _START, fetch=fetch)

    assert fetched == ["600083.SH"]
    assert result["reused_curated"] == 1
    assert result["to_fetch"] == 1
    assert result["recovered"] == 2


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


def test_delisted_sina_retries_transient_rate_limit(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10"})
    attempts = 0
    sleeps: list[float] = []
    request = httpx.Request("GET", "https://example.test/sina")

    def flaky(symbol, client):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            response = httpx.Response(456, request=request)
            raise httpx.HTTPStatusError("rate limited", request=request, response=response)
        return _bars(symbol, date(2016, 3, 1), date(2025, 4, 10))

    deferred: list[tuple[str, float]] = []
    monkeypatch.setattr("cnequity.domain.rate_limit.time.sleep", sleeps.append)
    monkeypatch.setattr(cfg, "defer_source", lambda source, s: deferred.append((source, s)))
    result = backfill_delisted_bars(cfg, "run-retry", _START, fetch=flaky)

    assert attempts == 3
    assert sleeps == []
    assert deferred == [("sina_bars", 30.0), ("sina_bars", 30.0)]
    assert result["rows_written"] == 2
    assert result["coverage_pending_compact"] is True


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
    assert result["status"] == "failed"
    assert result["usable_result"] is False
    assert result["reason_code"] == "source_unavailable"
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


def test_pre_window_no_data_evidence_is_reused_on_rerun(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10"})

    def empty(symbol, client):
        return pl.DataFrame(schema={c: DAILY_BARS_SCHEMA[c] for c in _BAR_COLS})

    monkeypatch.setattr(
        "cnequity.steps.delisted.load_delisted_catalog",
        lambda _config: {"600070.SH": date(2025, 4, 10)},
    )

    backfill_delisted_bars(
        cfg,
        "run-1",
        _START,
        fetch=empty,
        probe_last=lambda s, c: date(2009, 12, 15),
    )

    def must_not_be_called(symbol, client):
        raise AssertionError(f"refetched {symbol}")

    result = backfill_delisted_bars(
        cfg,
        "run-2",
        _START,
        fetch=must_not_be_called,
        probe_last=must_not_be_called,
    )
    assert result["rows_written"] == 0
    assert result["expected_no_data"] == 1
    assert result["coverage_pending_compact"] is True


def test_init_recovers_and_publishes_delisted_bars_before_legacy_ownership_retry(
    tmp_path, monkeypatch
):
    from cnequity.steps.delisted import delisted_recovery_covers

    start, end = date(2023, 9, 30), date(2025, 4, 10)
    cfg = _cfg(tmp_path, {"600070.SH": end.isoformat(), "430001.BJ": end.isoformat()})
    cfg.ingest_universe = "all_a_sh_sz"
    cfg._backfill_start = start
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("init", {})
    engine.manifest.start_batch(
        run_id,
        "ownership",
        dataset="daily_bars",
        task_id="daily_bars_ownership",
        blocks_compaction=True,
    )
    engine.manifest.finish_batch(run_id, "ownership", "warning")
    calls = []

    def fetch(symbol, **kwargs):
        calls.append(symbol)
        assert kwargs["start"] == start
        assert kwargs["end"] == end
        return _bars(symbol, date(2023, 10, 9), end)

    monkeypatch.setattr("cnequity.adapters.sina.bars.fetch_daily_bars_sina", fetch)
    engine._recover_init_delisted_bars(run_id, end, {})

    assert calls == ["600070.SH"]
    assert delisted_recovery_covers(cfg, start, end, ["600070.SH"])
    # Recovery's compact must not overwrite or bypass the parent's old warning.
    assert engine.manifest.get_batch(run_id, "ownership")["status"] == "warning"
    engine._recover_init_delisted_bars(run_id, end, {})
    assert calls == ["600070.SH"]


def test_init_partial_recovery_publishes_progress_and_retries_only_missing_targets(
    tmp_path, monkeypatch
):

    start, end = date(2024, 6, 27), date(2025, 4, 10)
    cfg = _cfg(tmp_path, {"600070.SH": end.isoformat(), "600083.SH": end.isoformat()})
    cfg._backfill_start = start
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("init", {})
    calls = []
    failing = True

    def fetch(symbol, **kwargs):
        calls.append(symbol)
        if failing and symbol == "600083.SH":
            raise ConnectionError("fixture source unavailable")
        return _bars(symbol, start, end)

    monkeypatch.setattr("cnequity.adapters.sina.bars.fetch_daily_bars_sina", fetch)
    recovery = engine._recover_init_delisted_bars(run_id, end, {})
    assert recovery["coverage_status"] == "partial"
    assert calls == ["600070.SH", "600083.SH"]
    failing = False
    engine._recover_init_delisted_bars(run_id, end, {})
    assert calls == ["600070.SH", "600083.SH", "600083.SH"]


def test_init_accepts_published_pre_window_no_data_evidence(tmp_path, monkeypatch):
    from cnequity.steps.delisted import delisted_recovery_covers

    start, end = date(2024, 6, 27), date(2025, 4, 10)
    cfg = _cfg(tmp_path, {"600070.SH": end.isoformat()})
    cfg._backfill_start = start
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("init", {})
    calls = []

    def fetch(symbol, **kwargs):
        calls.append(symbol)
        return pl.DataFrame()

    monkeypatch.setattr("cnequity.adapters.sina.bars.fetch_daily_bars_sina", fetch)
    monkeypatch.setattr(
        "cnequity.adapters.sina.bars.symbol_exists", lambda *a, **k: date(2009, 12, 15)
    )
    engine._recover_init_delisted_bars(run_id, end, {})
    assert delisted_recovery_covers(cfg, start, end, ["600070.SH"])
    engine._recover_init_delisted_bars(run_id, end, {})
    assert calls == ["600070.SH"]


def test_init_recovery_preserves_a_returned_execution_failure(tmp_path, monkeypatch):
    end = date(2025, 4, 10)
    cfg = _cfg(tmp_path, {"600070.SH": end.isoformat()})
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("init", {})
    monkeypatch.setattr(
        "cnequity.steps.delisted.backfill_delisted_bars",
        lambda *a, **k: {"status": "failed", "reason_code": "invariant_violation"},
    )
    with pytest.raises(RuntimeError, match="recovery execution failed"):
        engine._recover_init_delisted_bars(run_id, end, {})
    child = engine.manifest.list_runs(job_name="delisted_backfill")[0]
    assert child["status"] == "failed"
