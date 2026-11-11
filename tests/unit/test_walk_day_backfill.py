"""walk_day_backfill — date walk, curated-date resume, chunked flush.

Generalizes _backfill_margin_trading (see test_margin_backfill.py) for
datasets whose adapter answers one day at a time and had no historical walk
before this: dragon_tiger, block_trades, market_breadth, regulatory_events,
announcement_index all route through this one function. Tests write against
market_breadth's schema (trade_date, metric_id, value) — the simplest real
schema in the registry — since the helper is dataset-agnostic and validates
against whatever DATASET_SCHEMAS says.
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.run_scope import set_backfill
from cnequity.steps.common import walk_day_backfill


def _fake_row(d: date, date_col: str) -> dict:
    return {date_col: d, "metric_id": "adv_pct", "value": 1.0}


def test_walks_range_and_stages_rows(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2026, 6, 1), end=date(2026, 6, 5))
    fetched: list[date] = []

    def fetch_one(d: date) -> pl.DataFrame:
        fetched.append(d)
        return pl.DataFrame([_fake_row(d, "trade_date")])

    out = walk_day_backfill(
        cfg, date(2026, 7, 1), "run-1", "market_breadth", fetch_one, source="derived"
    )

    # No curated calendar in a fresh tmp lake → Mon-Fri fallback: 5 weekdays.
    assert fetched == [date(2026, 6, d) for d in (1, 2, 3, 4, 5)]
    assert out["days_fetched"] == 5
    assert out["rows_written"] == 5
    staged = list((cfg.staging_root / "market_breadth").glob("**/*.parquet"))
    assert len(staged) == 1
    df = pl.read_parquet(staged[0])
    assert df.height == 5
    assert set(df["source"]) == {"derived"}


def test_skips_dates_already_curated(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2026, 6, 1), end=date(2026, 6, 3))
    fetched: list[date] = []

    def fetch_one(d: date) -> pl.DataFrame:
        fetched.append(d)
        return pl.DataFrame([_fake_row(d, "trade_date")])

    curated = cfg.curated_root / "market_breadth" / "trade_date=2026-06-02"
    curated.mkdir(parents=True)
    pl.DataFrame([_fake_row(date(2026, 6, 2), "trade_date")]).write_parquet(
        curated / "part-0.parquet"
    )

    out = walk_day_backfill(
        cfg, date(2026, 7, 1), "run-1", "market_breadth", fetch_one, source="derived"
    )

    assert date(2026, 6, 2) not in fetched
    assert out["days_fetched"] == 2
    assert out["days_skipped"] == 1


@pytest.mark.parametrize("reason", ["quarantined_rows", "source_unavailable"])
def test_existing_rows_do_not_hide_a_date_gap_or_clear_it_before_publication(tmp_path, reason):
    from cnequity.storage.state import StateStore

    cfg = Config(data_root=tmp_path / "data")
    day = date(2026, 6, 2)
    set_backfill(cfg, start=day, end=day)
    root = cfg.curated_root / "market_breadth" / f"trade_date={day}"
    root.mkdir(parents=True)
    pl.DataFrame([_fake_row(day, "trade_date")]).write_parquet(root / "part-0.parquet")
    state = StateStore(cfg.meta_root)
    state.record_missing_dates("market_breadth", [day], reason=reason)
    calls = []

    def fetch(d):
        calls.append(d)
        return pl.DataFrame([_fake_row(d, "trade_date")])

    result = walk_day_backfill(cfg, day, "repair", "market_breadth", fetch, source="derived")
    assert calls == [day]
    assert result["days_skipped"] == 0
    assert result["rows_written"] == 1
    assert state.get_missing_dates("market_breadth") == {day}


def test_resume_check_uses_the_configured_date_column(tmp_path):
    """regulatory_events keys on event_date, not trade_date — the resume-skip
    check has to read the same column the schema actually stores, or every
    rerun refetches everything the dataset already has."""
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2026, 6, 1), end=date(2026, 6, 2))
    fetched: list[date] = []

    def _row(d: date) -> dict:
        return {
            "event_id": f"e-{d}",
            "symbol": "600000.SH",
            "event_date": d,
            "event_type": "x",
            "title": "x",
        }

    def fetch_one(d: date) -> pl.DataFrame:
        fetched.append(d)
        return pl.DataFrame([_row(d)])

    curated = cfg.curated_root / "regulatory_events" / "event_date=2026-06-01"
    curated.mkdir(parents=True)
    pl.DataFrame([_row(date(2026, 6, 1))]).write_parquet(curated / "part-0.parquet")

    out = walk_day_backfill(
        cfg,
        date(2026, 7, 1),
        "run-1",
        "regulatory_events",
        fetch_one,
        source="cninfo",
        date_col="event_date",
    )

    assert fetched == [date(2026, 6, 2)]
    assert out["days_skipped"] == 1


def test_empty_days_reported_not_fatal(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2026, 6, 1), end=date(2026, 6, 2))

    def fetch_one(d: date) -> pl.DataFrame:
        return (
            pl.DataFrame() if d == date(2026, 6, 1) else pl.DataFrame([_fake_row(d, "trade_date")])
        )

    out = walk_day_backfill(
        cfg, date(2026, 7, 1), "run-1", "market_breadth", fetch_one, source="derived"
    )

    assert out["days_empty"] == 1
    assert out["days_fetched"] == 1
    assert out["rows_written"] == 1
    assert out["status"] == "warning"
    finding = out["context_updates"]["audit_findings"][0]
    assert finding["check"] == "backfill_empty_days"
    assert finding["severity"] == "warning"
    assert finding["sample_dates"] == ["2026-06-01"]


def test_rejects_rows_from_a_different_requested_date(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2026, 6, 1), end=date(2026, 6, 2))

    def fetch_one(d: date) -> pl.DataFrame:
        return pl.DataFrame(
            [_fake_row(d if d == date(2026, 6, 1) else d + timedelta(days=1), "trade_date")]
        )

    result = walk_day_backfill(
        cfg, date(2026, 7, 1), "run-1", "market_breadth", fetch_one, source="derived"
    )
    assert result["status"] == "warning"
    assert result["failed_days"] == 1

    staged = list((cfg.staging_root / "market_breadth").glob("**/*.parquet"))
    assert len(staged) == 1
    assert pl.read_parquet(staged[0])["trade_date"].to_list() == [date(2026, 6, 1)]


def test_rejects_rows_without_the_configured_date_column(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2026, 6, 1), end=date(2026, 6, 1))

    def fetch_one(d: date) -> pl.DataFrame:
        return pl.DataFrame([_fake_row(d, "trade_date")])

    with pytest.raises(RuntimeError, match="configured date column 'event_date'"):
        walk_day_backfill(
            cfg,
            date(2026, 7, 1),
            "run-1",
            "regulatory_events",
            fetch_one,
            source="cninfo",
            date_col="event_date",
        )


def test_end_clamped_to_trade_date(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2026, 6, 29), end=date(2026, 7, 10))
    fetched: list[date] = []

    def fetch_one(d: date) -> pl.DataFrame:
        fetched.append(d)
        return pl.DataFrame([_fake_row(d, "trade_date")])

    walk_day_backfill(
        cfg, date(2026, 6, 30), "run-1", "market_breadth", fetch_one, source="derived"
    )

    assert fetched == [date(2026, 6, 29), date(2026, 6, 30)]


def test_flushes_already_fetched_days_before_reraising(tmp_path):
    """Measured in production: announcement_index ran 9.6h, hit a DNS blip on
    one day mid-window, and landed zero new curated days — every day fetched
    since the last flush boundary was lost with the exception. A raise must
    honor the same "kill costs only the unflushed chunk" promise as a kill."""
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2026, 6, 1))
    set_backfill(cfg, end=date(2026, 6, 10))  # under one 60-day flush window
    poison = date(2026, 6, 5)

    def fetch_one(d: date) -> pl.DataFrame:
        if d == poison:
            raise RuntimeError("simulated DNS blip")
        return pl.DataFrame([_fake_row(d, "trade_date")])

    try:
        walk_day_backfill(
            cfg, date(2026, 7, 1), "run-1", "market_breadth", fetch_one, source="derived"
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected the poisoned day's error to propagate")

    staged = list((cfg.staging_root / "market_breadth").glob("**/*.parquet"))
    assert len(staged) == 1
    df = pl.read_parquet(staged[0])
    # Every weekday strictly before the poison day landed; the poison day and
    # anything after it never got the chance to fetch.
    assert set(df["trade_date"].to_list()) == {date(2026, 6, d) for d in (1, 2, 3, 4)}


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
def test_an_interrupt_stages_the_days_already_fetched(tmp_path, interrupt):
    """Ctrl-C is not an ``Exception``; it used to drop every unflushed day."""
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2026, 6, 1), end=date(2026, 6, 10))

    def fetch_one(d: date) -> pl.DataFrame:
        if d == date(2026, 6, 5):
            raise interrupt
        return pl.DataFrame([_fake_row(d, "trade_date")])

    with pytest.raises(interrupt):
        walk_day_backfill(
            cfg, date(2026, 7, 1), "run-1", "market_breadth", fetch_one, source="derived"
        )

    staged = list((cfg.staging_root / "market_breadth").glob("**/*.parquet"))
    assert len(staged) == 1
    df = pl.read_parquet(staged[0])
    assert set(df["trade_date"].to_list()) == {date(2026, 6, d) for d in (1, 2, 3, 4)}


def test_an_interrupt_is_not_masked_by_a_failed_flush(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2026, 6, 1), end=date(2026, 6, 10))

    def fetch_one(d: date) -> pl.DataFrame:
        if d == date(2026, 6, 3):
            raise KeyboardInterrupt
        return pl.DataFrame([_fake_row(d, "trade_date")])

    def publish(_part: pl.DataFrame, _batch_id: str) -> object:
        raise OSError("disk full")

    with pytest.raises(KeyboardInterrupt):
        walk_day_backfill(
            cfg,
            date(2026, 7, 1),
            "run-1",
            "market_breadth",
            fetch_one,
            source="derived",
            publish_fn=publish,
        )


def test_same_run_retry_keeps_earlier_successful_chunks(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2026, 6, 1), end=date(2026, 6, 5))
    calls: list[date] = []

    def first(d: date) -> pl.DataFrame:
        if d == date(2026, 6, 4):
            raise ConnectionError("first interruption")
        return pl.DataFrame([_fake_row(d, "trade_date")])

    result = walk_day_backfill(
        cfg, date(2026, 6, 5), "same-run", "market_breadth", first, source="derived"
    )
    assert result["status"] == "warning"

    def second(d: date) -> pl.DataFrame:
        calls.append(d)
        if d == date(2026, 6, 5):
            raise ConnectionError("second interruption")
        return pl.DataFrame([_fake_row(d, "trade_date")])

    result = walk_day_backfill(
        cfg, date(2026, 6, 5), "same-run", "market_breadth", second, source="derived"
    )
    assert result.get("status", "success") == "success"

    assert calls == [date(2026, 6, 4)]
    staged = list((cfg.staging_root / "market_breadth").glob("**/*.parquet"))
    assert len(staged) == 3
    assert {d for path in staged for d in pl.read_parquet(path)["trade_date"].to_list()} == {
        date(2026, 6, d) for d in (1, 2, 3, 4, 5)
    }


def test_start_falls_back_to_the_floor_argument(tmp_path):
    """No --start given: use the caller's floor, not the generic BACKFILL_START."""
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, end=date(2007, 1, 8))
    fetched: list[date] = []

    def fetch_one(d: date) -> pl.DataFrame:
        fetched.append(d)
        return pl.DataFrame([_fake_row(d, "trade_date")])

    walk_day_backfill(
        cfg,
        date(2026, 7, 1),
        "run-1",
        "market_breadth",
        fetch_one,
        source="derived",
        floor=date(2007, 1, 1),
    )

    assert fetched[0] == date(2007, 1, 1)


def test_flushes_in_chunks_not_one_giant_write(tmp_path):
    """A kill mid-sweep should cost only the unflushed chunk."""
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2026, 1, 1))
    set_backfill(cfg, end=date(2026, 6, 30))  # well over 60 weekdays

    def fetch_one(d: date) -> pl.DataFrame:
        return pl.DataFrame([_fake_row(d, "trade_date")])

    walk_day_backfill(
        cfg,
        date(2026, 7, 1),
        "run-1",
        "market_breadth",
        fetch_one,
        source="derived",
        flush_days=10,
    )

    staged = list((cfg.staging_root / "market_breadth").glob("**/*.parquet"))
    assert len(staged) > 1, "expected multiple chunked parts, not one write for the whole range"
