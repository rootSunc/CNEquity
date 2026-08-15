"""_backfill_share_unlock_schedule — strided walk, not daily.

Unlike the other by-date snapshots, share_unlock_schedule's PK is
(symbol, unlock_date) with no as-of/snapshot column: one call returns every
unlock in the next 180 days from its date, so the same event would be
re-fetched up to ~180 times by a daily walk before aging out of the window.
The backfill strides under the horizon instead.
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.steps.macro_risk import (
    _UNLOCK_STRIDE_DAYS,
    _backfill_share_unlock_schedule,
)


def _row(unlock_date: date, symbol: str = "600000.SH") -> dict:
    return {
        "symbol": symbol,
        "unlock_date": unlock_date,
        "unlock_shares": 1.0,
        "unlock_ratio": 0.01,
        "unlock_type": "限售股份",
    }


def test_strides_under_the_horizon_not_every_day(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    cfg._backfill_start = date(2026, 1, 1)
    cfg._backfill_end = date(2026, 6, 30)  # ~181 days: exactly one full stride
    calls: list[date] = []

    def fake_fetch(d: date, *, horizon_days: int = 180, **_kw) -> pl.DataFrame:
        calls.append(d)
        return pl.DataFrame([_row(d + timedelta(days=30))])

    monkeypatch.setattr("cn_market_lake.steps.macro_risk.fetch_share_unlock_schedule", fake_fetch)
    monkeypatch.setattr(cfg, "rate_limit", lambda source: None)

    _backfill_share_unlock_schedule(cfg, date(2026, 7, 1), "run-1")

    # 181-day window at a 150-day stride: two calls, at day 0 and day 150, not
    # 181 separate daily ones.
    assert set(calls) == {date(2026, 1, 1), date(2026, 1, 1) + timedelta(days=_UNLOCK_STRIDE_DAYS)}


def test_walks_newest_stride_first(tmp_path, monkeypatch):
    """RPT_LIFT_STAGE is sorted FREE_DATE descending, so a stride targeting a
    recent date pages only the ~7 pages nearest the top, while one targeting
    an old date has to page through nearly the whole 63-page report to reach
    it. Four consecutive production runs died on an early (old, expensive)
    stride and lost every cheap recent one queued behind it. Newest-first
    means a failure anywhere still leaves the years most likely to be queried
    already landed."""
    cfg = Config(data_root=tmp_path / "data")
    cfg._backfill_start = date(2010, 1, 1)
    cfg._backfill_end = date(2010, 1, 1) + timedelta(days=_UNLOCK_STRIDE_DAYS * 3)
    calls: list[date] = []

    def fake_fetch(d: date, *, horizon_days: int = 180, **_kw) -> pl.DataFrame:
        calls.append(d)
        return pl.DataFrame()

    monkeypatch.setattr("cn_market_lake.steps.macro_risk.fetch_share_unlock_schedule", fake_fetch)
    monkeypatch.setattr(cfg, "rate_limit", lambda source: None)

    _backfill_share_unlock_schedule(cfg, date(2026, 7, 1), "run-1")

    assert calls == sorted(calls, reverse=True)
    assert calls[0] > calls[-1]


def test_flushes_per_stride_not_once_at_the_end(tmp_path, monkeypatch):
    """A failure on a later stride must not cost the ones already fetched.

    Measured in production: a stride's page 28 hit an unretried EastMoney
    timeout 38 minutes into a ~40-stride sweep. The old single-batch-at-the-end
    write meant every prior stride was lost with it. Two strides here must
    land as two staged parts, not one — a compact-time PK dedup on the
    (symbol, unlock_date) overlap is a separate concern from this.
    """
    cfg = Config(data_root=tmp_path / "data")
    cfg._backfill_start = date(2026, 1, 1)
    cfg._backfill_end = date(2026, 6, 30)  # two strides: day 0 and day 150

    def fake_fetch(d: date, *, horizon_days: int = 180, **_kw) -> pl.DataFrame:
        return pl.DataFrame([_row(d + timedelta(days=30))])

    monkeypatch.setattr("cn_market_lake.steps.macro_risk.fetch_share_unlock_schedule", fake_fetch)
    monkeypatch.setattr(cfg, "rate_limit", lambda source: None)

    out = _backfill_share_unlock_schedule(cfg, date(2026, 7, 1), "run-1")

    staged = list((cfg.staging_root / "share_unlock_schedule").glob("**/*.parquet"))
    assert len(staged) == 2
    assert out["rows_written"] == sum(pl.read_parquet(f).height for f in staged)


def test_uses_the_patient_sweep_retry_budget_not_the_daily_default(tmp_path, monkeypatch):
    """Measured: three backfill runs each failed once, on three different
    pages (8, 27, 28) of the 63-page market-wide report every stride re-walks
    from page 1 — a transient-load pattern the daily call's 3/5s budget isn't
    sized for. The backfill must ask for the more patient one."""
    from cn_market_lake.steps.macro_risk import (
        _UNLOCK_SWEEP_BACKOFF_SECONDS,
        _UNLOCK_SWEEP_RETRIES,
    )

    cfg = Config(data_root=tmp_path / "data")
    cfg._backfill_start = date(2026, 1, 1)
    cfg._backfill_end = date(2026, 1, 2)  # a single stride
    seen: dict = {}

    def fake_fetch(d: date, *, horizon_days: int = 180, **kw) -> pl.DataFrame:
        seen.update(kw)
        return pl.DataFrame()

    monkeypatch.setattr("cn_market_lake.steps.macro_risk.fetch_share_unlock_schedule", fake_fetch)
    monkeypatch.setattr(cfg, "rate_limit", lambda source: None)

    _backfill_share_unlock_schedule(cfg, date(2026, 7, 1), "run-1")

    assert seen["max_retries"] == _UNLOCK_SWEEP_RETRIES
    assert seen["retry_backoff_seconds"] == _UNLOCK_SWEEP_BACKOFF_SECONDS


def test_empty_range_writes_nothing(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    cfg._backfill_start = date(2026, 1, 1)
    cfg._backfill_end = date(2026, 1, 2)  # under one stride — a single call

    monkeypatch.setattr(
        "cn_market_lake.steps.macro_risk.fetch_share_unlock_schedule",
        lambda d, **k: pl.DataFrame(),
    )
    monkeypatch.setattr(cfg, "rate_limit", lambda source: None)

    out = _backfill_share_unlock_schedule(cfg, date(2026, 7, 1), "run-1")

    assert out == {"rows_read": 0, "rows_written": 0}
    assert not (cfg.staging_root / "share_unlock_schedule").exists()
