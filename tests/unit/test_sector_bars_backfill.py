"""sector_bars backfill step — checkpoint resume and warning status."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.steps import rotation as rot
from cnequity.steps.rotation import (
    _backfill_sector_bars,
    _sector_bars_completed,
    clear_sector_bars_backfill_state,
    step_sector_bars,
)


def _patch_history(monkeypatch, *, returns):
    """Stand in for the 同花顺 sweep, which returns list[dict] rather than a frame."""

    def fake_sweep(start, end, *, config=None, boards=None, skip_sectors=None, on_batch=None):
        df, failed, succeeded = returns
        skip = skip_sectors or set()
        succeeded = [s for s in succeeded if s not in skip]
        failed = [s for s in failed if s not in skip]
        rows = df.filter(pl.col("sector_code").is_in(succeeded)).to_dicts() if succeeded else []
        # Real adapter hands every row to on_batch and returns an empty list.
        if on_batch is not None:
            if succeeded:
                on_batch(rows, succeeded)
            return [], failed, succeeded, []
        return rows, failed, succeeded, []

    written: list[pl.DataFrame] = []

    def fake_write(config, run_id, dataset, df, *, source, batch_id="batch-0"):
        assert source == "ths", f"sector_bars must be single-source; got {source!r}"
        written.append(df)
        return {"rows_read": df.height, "rows_written": df.height}

    monkeypatch.setattr("cnequity.adapters.ths.boards.sweep_board_bars", fake_sweep)
    monkeypatch.setattr(rot, "write_fetched", fake_write)
    return written


def test_marks_succeeded_boards_and_resumes(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    df = pl.DataFrame(
        [
            {
                "sector_code": "885611",
                "sector_name": "A",
                "board_type": "concept",
                "trade_date": date(2026, 7, 10),
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1,
                "amount": 1.0,
                "change_pct": 0.1,
            }
        ]
    )
    _patch_history(monkeypatch, returns=(df, [], ["885611"]))
    result = _backfill_sector_bars(cfg, date(2026, 7, 14), "run1")
    assert result["rows_written"] == 1
    assert _sector_bars_completed(cfg) == {"885611"}

    captured: dict = {}

    def fake_sweep(start, end, *, config=None, boards=None, skip_sectors=None, on_batch=None):
        captured["skip"] = skip_sectors
        return [], [], [], []

    monkeypatch.setattr("cnequity.adapters.ths.boards.sweep_board_bars", fake_sweep)
    again = _backfill_sector_bars(cfg, date(2026, 7, 14), "run2")
    assert "already swept" in again["note"]
    assert captured["skip"] == {"885611"}


def test_failed_boards_not_marked_and_emit_warning(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    df = pl.DataFrame(
        [
            {
                "sector_code": "885611",
                "sector_name": "A",
                "board_type": "concept",
                "trade_date": date(2026, 7, 10),
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1,
                "amount": 1.0,
                "change_pct": 0.1,
            }
        ]
    )
    _patch_history(monkeypatch, returns=(df, ["885612", "885613"], ["885611"]))
    result = _backfill_sector_bars(cfg, date(2026, 7, 14), "run1")

    assert _sector_bars_completed(cfg) == {"885611"}
    assert result["failed_sectors"] == 2
    assert result["status"] == "warning"
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["check"] == "sector_bars_sweep_incomplete"


def test_force_clears_checkpoint(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    cfg._sector_bars_force = True
    df = pl.DataFrame(
        [
            {
                "sector_code": "885611",
                "sector_name": "A",
                "board_type": "concept",
                "trade_date": date(2026, 7, 10),
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1,
                "amount": 1.0,
                "change_pct": 0.1,
            }
        ]
    )
    _patch_history(monkeypatch, returns=(df, [], ["885611"]))
    _backfill_sector_bars(cfg, date(2026, 7, 14), "run1")
    assert _sector_bars_completed(cfg) == {"885611"}

    clear_sector_bars_backfill_state(cfg)
    assert _sector_bars_completed(cfg) == set()


def test_narrow_gap_fill_does_not_satisfy_the_full_window(tmp_path, monkeypatch):
    """A 4-day gap-fill must not mark boards done for the 400-day backfill."""
    cfg = Config(data_root=tmp_path / "data")
    df = pl.DataFrame(
        [
            {
                "sector_code": "885611",
                "sector_name": "A",
                "board_type": "concept",
                "trade_date": date(2026, 7, 20),
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1,
                "amount": 1.0,
                "change_pct": 0.1,
            }
        ]
    )
    _patch_history(monkeypatch, returns=(df, [], ["885611"]))
    cfg._backfill_start = date(2026, 7, 15)
    cfg._backfill_end = date(2026, 7, 21)
    _backfill_sector_bars(cfg, date(2026, 7, 21), "run1")

    narrow = (date(2026, 7, 15), date(2026, 7, 21))
    assert _sector_bars_completed(cfg, narrow) == {"885611"}
    # The full backfill window is not covered by that sweep, so the board is
    # still owed its history — the checkpoint must not claim otherwise.
    assert _sector_bars_completed(cfg, (date(2025, 6, 16), date(2026, 7, 21))) == set()

    # And the wide sweep re-fetches it rather than reporting "already done".
    captured: dict = {}

    def fake_sweep(start, end, *, config=None, boards=None, skip_sectors=None, on_batch=None):
        captured["skip"] = skip_sectors
        return [], [], [], []

    monkeypatch.setattr("cnequity.adapters.ths.boards.sweep_board_bars", fake_sweep)
    cfg._backfill_start = None
    cfg._backfill_end = None
    _backfill_sector_bars(cfg, date(2026, 7, 21), "run2")
    assert captured["skip"] == set()


def test_step_sector_bars_applies_finality_guard_before_sweep(config, monkeypatch):
    events: list[str] = []

    def stop_before_sweep(*args, **kwargs):
        events.append("guard")
        raise RuntimeError("stop before sweep")

    monkeypatch.setattr(
        "cnequity.steps.bars._reject_unfinished_daily_bar_window",
        stop_before_sweep,
    )
    monkeypatch.setattr(
        "cnequity.steps.rotation._sweep_sector_bars",
        lambda *args, **kwargs: events.append("sweep"),
    )

    with pytest.raises(RuntimeError, match="stop before sweep"):
        step_sector_bars(config, date(2026, 8, 10), "run-sector-guard", {})

    assert events == ["guard"]


def test_a_retired_board_is_checkpointed_and_reported_as_info(tmp_path, monkeypatch):
    """同花顺 keeps retired seasonal boards in its catalog with no bars; that
    must not file a sweep-incomplete warning every day."""
    cfg = Config(data_root=tmp_path / "data")

    def fake_sweep(start, end, *, config=None, boards=None, skip_sectors=None, on_batch=None):
        if on_batch is not None:
            on_batch([], ["886109"])
        return [], [], [], ["886109"]

    monkeypatch.setattr("cnequity.adapters.ths.boards.sweep_board_bars", fake_sweep)
    result = _backfill_sector_bars(cfg, date(2026, 10, 26), "run1")

    assert _sector_bars_completed(cfg) == {"886109"}
    assert "failed_sectors" not in result
    assert result.get("status") != "warning"
    [finding] = result["context_updates"]["audit_findings"]
    assert finding["check"] == "sector_bars_board_retired"
    assert finding["severity"] == "info"
    assert "886109" in finding["message"]
