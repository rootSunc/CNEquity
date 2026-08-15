"""Resume + orchestration for the trading_status ST backfill step (C4).

The baostock fetch and the curated write are stubbed so the test isolates the
step's own logic: the todo set, the swept-symbol resume marker, and the
fail-loud finding on dropped symbols.
"""

from __future__ import annotations

import json
from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.orchestrator.engine import JobEngine
from cn_market_lake.quality.st_coverage import (
    build_st_scope,
    load_st_checkpoint,
    st_evidence_coverage_report,
)
from cn_market_lake.steps import reference
from cn_market_lake.steps.reference import _backfill_trading_status_st


def _write_instruments(config: Config, symbols: list[str]) -> None:
    root = config.curated_root / "instruments"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": symbols}).write_parquet(root / "part-merged.parquet")


def _st_row(symbol: str, d: date) -> dict:
    return {"symbol": symbol, "trade_date": d, "is_trading": True, "status": "st"}


def _patch(monkeypatch, *, returns):
    """Stub the network fetch and the curated write; return a captured-writes list."""
    written: list[pl.DataFrame] = []

    def fake_fetch(symbols, start, end, **kwargs):
        df, failed = returns
        return df, failed

    def fake_write(config, run_id, dataset, df, *, source, batch_id="batch-0"):
        written.append(df)
        return {"rows_read": df.height, "rows_written": df.height}

    monkeypatch.setattr("cn_market_lake.adapters.baostock.st_history.fetch_st_history", fake_fetch)
    monkeypatch.setattr(reference, "write_fetched", fake_write)
    return written


def test_writes_st_rows_and_marks_all_swept_symbols(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    _write_instruments(cfg, ["600000.SH", "600001.SH"])  # both all_a, no ST for 600001
    df = pl.DataFrame([_st_row("600000.SH", date(2020, 5, 6))])
    written = _patch(monkeypatch, returns=(df, []))

    result = _backfill_trading_status_st(cfg, date(2026, 7, 1), "run1")

    assert result["rows_written"] == 1
    assert written[0]["symbol"].to_list() == ["600000.SH"]
    scope = build_st_scope(
        ["600000.SH", "600001.SH"],
        date(2016, 1, 1),
        date(2026, 7, 1),
        universe="all_a",
    )
    checkpoint = load_st_checkpoint(cfg, scope)
    assert set(checkpoint["completed_symbols"]) == {"600000.SH", "600001.SH"}
    assert checkpoint["status"] == "complete"
    assert result["coverage_pending_compact"] is True


def test_new_run_rechecks_positive_rows_that_never_reached_storage(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    _write_instruments(cfg, ["600000.SH", "600001.SH"])
    _patch(monkeypatch, returns=(pl.DataFrame([_st_row("600000.SH", date(2020, 5, 6))]), []))
    _backfill_trading_status_st(cfg, date(2026, 7, 1), "run1")

    # The positive row was captured by a fake writer, so it never reached
    # staging/curated. A new run may reuse the zero-row query evidence for the
    # other symbol, but must re-fetch the missing positive facts.
    captured: dict = {}

    def fake_fetch(symbols, start, end, **kwargs):
        captured["symbols"] = symbols
        return pl.DataFrame(schema={"symbol": pl.Utf8}), []

    monkeypatch.setattr("cn_market_lake.adapters.baostock.st_history.fetch_st_history", fake_fetch)
    _backfill_trading_status_st(cfg, date(2026, 7, 1), "run2")

    assert captured["symbols"] == ["600000.SH"]


def test_failed_symbols_are_not_marked_and_surface_a_finding(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    _write_instruments(cfg, ["600000.SH", "600001.SH"])
    df = pl.DataFrame([_st_row("600000.SH", date(2020, 5, 6))])
    _patch(monkeypatch, returns=(df, ["600001.SH"]))  # 600001 dropped by throttling

    result = _backfill_trading_status_st(cfg, date(2026, 7, 1), "run1")

    scope = build_st_scope(
        ["600000.SH", "600001.SH"],
        date(2016, 1, 1),
        date(2026, 7, 1),
        universe="all_a",
    )
    checkpoint = load_st_checkpoint(cfg, scope)
    assert checkpoint["completed_symbols"] == ["600000.SH"]
    assert checkpoint["unresolved_symbols"] == ["600001.SH"]
    assert result["status"] == "warning"
    assert result["failed_symbols"] == 1
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["code"] == "baostock_st_backfill_incomplete"
    assert finding["severity"] == "warning"


def test_legacy_sparse_checkpoint_is_not_completion_evidence(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    _write_instruments(cfg, ["600000.SH"])
    legacy = cfg.meta_root / "state" / "trading_status_st_backfill.json"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text(json.dumps({"completed": ["600000.SH"]}))
    _written, calls = _patch_with_calls(monkeypatch, returns=(pl.DataFrame(), []))

    _backfill_trading_status_st(cfg, date(2026, 7, 1), "run1")

    assert calls == [["600000.SH"]]


def _patch_with_calls(monkeypatch, *, returns):
    calls: list[list[str]] = []

    def fake_fetch(symbols, start, end, **kwargs):
        calls.append(list(symbols))
        return returns

    monkeypatch.setattr("cn_market_lake.adapters.baostock.st_history.fetch_st_history", fake_fetch)
    monkeypatch.setattr(
        reference,
        "write_fetched",
        lambda *args, **kwargs: {"rows_read": 0, "rows_written": 0},
    )
    return [], calls


def test_receipt_is_published_only_after_successful_compact(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    _write_instruments(cfg, ["600000.SH"])
    monkeypatch.setattr(
        "cn_market_lake.adapters.baostock.st_history.fetch_st_history",
        lambda *args, **kwargs: (
            pl.DataFrame([_st_row("600000.SH", date(2020, 5, 6))]),
            [],
        ),
    )
    engine = JobEngine(cfg)
    result = engine.run_job(
        "backfill",
        date(2026, 7, 1),
        steps=["trading_status"],
        backfill=True,
        finalize_run=False,
    )

    assert result["status"] == "success"
    assert st_evidence_coverage_report(cfg, date(2016, 1, 1), date(2026, 7, 1))["verified"] is False

    compact = engine.run_step("compact", date(2026, 7, 1), result["run_id"])

    assert compact["coverage_receipts"]
    assert st_evidence_coverage_report(cfg, date(2016, 1, 1), date(2026, 7, 1))["verified"] is True


def test_partial_st_rows_do_not_compact_or_publish_coverage(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    _write_instruments(cfg, ["600000.SH", "600001.SH"])
    monkeypatch.setattr(
        "cn_market_lake.adapters.baostock.st_history.fetch_st_history",
        lambda *args, **kwargs: (
            pl.DataFrame([_st_row("600000.SH", date(2020, 5, 6))]),
            ["600001.SH"],
        ),
    )
    engine = JobEngine(cfg)
    result = engine.run_job(
        "backfill",
        date(2026, 7, 1),
        steps=["trading_status"],
        backfill=True,
        finalize_run=False,
    )

    assert result["status"] == "warning"
    assert engine.manifest.incomplete_batch_count(result["run_id"]) == 1
    assert engine.manifest.incomplete_batch_counts_by_dataset(result["run_id"]) == {
        "trading_status": 1
    }

    compact = engine.run_step("compact", date(2026, 7, 1), result["run_id"])

    assert compact["rows_written"] == 0
    assert "coverage_receipts" not in compact
    assert st_evidence_coverage_report(cfg, date(2016, 1, 1), date(2026, 7, 1))["verified"] is False
