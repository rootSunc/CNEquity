"""daily_bars must not let a pre-open capture reach curated.

A fetch that fires before the session opens returns the previous close on every
field — open==high==low==close, zero volume — for the whole universe. 2026-07-22
arrived that way. A handful of such rows on any day are genuine suspensions; a
whole day of them is a mis-timed run, and it must fail loudly (the step re-runs
after the close) rather than overwrite a good partition.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cn_market_lake.steps.bars import _reject_preopen_placeholder

TD = date(2026, 7, 22)


class _Staging:
    def __init__(self, path):
        self.staging_root = path


def _write(tmp_path, rows: list[dict]) -> None:
    run_dir = tmp_path / "daily_bars" / "run_id=r1"
    run_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(run_dir / "part-000.parquet")


def _bar(sym: str, *, flat: bool) -> dict:
    if flat:
        return {
            "symbol": sym,
            "trade_date": TD,
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "volume": 0,
        }
    return {
        "symbol": sym,
        "trade_date": TD,
        "open": 10.0,
        "high": 10.6,
        "low": 9.8,
        "close": 10.3,
        "volume": 12345,
    }


def test_all_placeholder_day_is_rejected(tmp_path):
    _write(tmp_path, [_bar(f"{i:06d}.SZ", flat=True) for i in range(50)])
    with pytest.raises(RuntimeError, match="pre-open placeholder"):
        _reject_preopen_placeholder(_Staging(tmp_path), "r1", TD)


def test_a_few_suspensions_pass(tmp_path):
    rows = [_bar(f"{i:06d}.SZ", flat=False) for i in range(48)]
    rows += [_bar("900001.SZ", flat=True), _bar("900002.SZ", flat=True)]
    _write(tmp_path, rows)
    # 2/50 flat — normal suspensions, must not raise.
    _reject_preopen_placeholder(_Staging(tmp_path), "r1", TD)


def test_half_placeholder_trips_the_guard(tmp_path):
    rows = [_bar(f"{i:06d}.SZ", flat=True) for i in range(25)]
    rows += [_bar(f"{i:06d}.SH", flat=False) for i in range(25)]
    _write(tmp_path, rows)
    with pytest.raises(RuntimeError, match="50%"):
        _reject_preopen_placeholder(_Staging(tmp_path), "r1", TD)


def test_no_staged_rows_is_noop(tmp_path):
    (tmp_path / "daily_bars").mkdir(parents=True)
    _reject_preopen_placeholder(_Staging(tmp_path), "r1", TD)


def test_only_the_target_day_is_judged(tmp_path):
    # A clean target day alongside an old flat day must still pass.
    rows = [_bar(f"{i:06d}.SZ", flat=False) for i in range(40)]
    rows += [
        {
            "symbol": "000001.SZ",
            "trade_date": date(2026, 7, 21),
            "open": 5.0,
            "high": 5.0,
            "low": 5.0,
            "close": 5.0,
            "volume": 0,
        }
    ]
    _write(tmp_path, rows)
    _reject_preopen_placeholder(_Staging(tmp_path), "r1", TD)
