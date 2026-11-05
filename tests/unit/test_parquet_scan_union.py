"""Mixed-schema scans plan one scan, not one per run of equal schemas.

daily_bars holds two schemas (with and without ``pre_close``) interleaved by
date into ~4,000 runs. Building a lazy scan per run and concatenating them
diagonally cost ~1.7 s of every one-symbol history read; reading 11k footers
again on every call cost ~1 s more.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import cnequity.query.parquet_scan as ps


def _write(path: Path, day: date, *, with_pre_close: bool, close_dtype=pl.Float64) -> None:
    path.mkdir(parents=True, exist_ok=True)
    columns = {
        "symbol": ["600519.SH", "000001.SZ"],
        "trade_date": [day, day],
        "close": pl.Series([10.0, 20.0], dtype=close_dtype),
    }
    if with_pre_close:
        columns["pre_close"] = [9.5, 19.5]
    pl.DataFrame(columns).write_parquet(path / "part-0.parquet")


def _interleaved(root: Path, days: int = 12) -> list[Path]:
    for offset in range(days):
        day = date(2024, 1, 1 + offset)
        _write(root / f"trade_date={day}", day, with_pre_close=offset % 3 != 0)
    return ps.partition_files_in_range(root, "trade_date", resolve=False)


def _diagonal(files: list[Path], *, hive: bool, monkeypatch) -> pl.DataFrame:
    with monkeypatch.context() as patch:
        patch.setattr(ps, "_additive_union", lambda schemas: None)
        return ps._scan_parquet_paths(files, hive=hive).collect()


@pytest.mark.parametrize("hive", [False, True])
def test_one_union_scan_returns_exactly_what_the_diagonal_union_did(tmp_path, monkeypatch, hive):
    files = _interleaved(tmp_path)
    expected = _diagonal(files, hive=hive, monkeypatch=monkeypatch)
    actual = ps._scan_parquet_paths(files, hive=hive).collect()
    assert_frame_equal(actual, expected, check_row_order=True, check_column_order=True)
    assert actual["pre_close"].null_count() == 8  # 4 old-schema files x 2 rows


def test_the_union_keeps_first_seen_column_order():
    first = pl.Schema({"a": pl.Int64, "b": pl.Utf8})
    second = pl.Schema({"a": pl.Int64, "c": pl.Float64, "b": pl.Utf8})
    union = ps._additive_union([first, second])
    assert list(union) == ["a", "b", "c"]


def test_a_dtype_conflict_keeps_the_diagonal_supertype_path(tmp_path, monkeypatch):
    _write(tmp_path / "trade_date=2024-01-01", date(2024, 1, 1), with_pre_close=False)
    _write(
        tmp_path / "trade_date=2024-01-02",
        date(2024, 1, 2),
        with_pre_close=True,
        close_dtype=pl.Float32,
    )
    files = ps.partition_files_in_range(tmp_path, "trade_date", resolve=False)
    schemas = [pl.read_parquet_schema(str(f)) for f in files]
    assert ps._additive_union(schemas) is None
    frame = ps._scan_parquet_paths(files, hive=False).collect()
    assert frame.height == 4 and frame.schema["close"] == pl.Float64


def test_a_rewritten_file_is_not_served_a_stale_schema(tmp_path):
    day = date(2024, 1, 1)
    target = tmp_path / f"trade_date={day}"
    _write(target, day, with_pre_close=False)
    path = str(target / "part-0.parquet")
    assert "pre_close" not in ps._footer_schema(path)
    stat = os.stat(path)
    _write(target, day, with_pre_close=True)
    # Same mtime on purpose: inode or size must still tell them apart.
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    assert "pre_close" in ps._footer_schema(path)
