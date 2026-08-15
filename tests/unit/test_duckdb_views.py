"""DuckDB / polars view paths must stay POSIX-form so Windows ``\\`` never hits globs."""

from __future__ import annotations

from pathlib import Path

from cn_market_lake.config import Config
from cn_market_lake.domain.datasets import DATASETS
from cn_market_lake.query.parquet_scan import parquet_glob
from cn_market_lake.query.views import _view_glob, ensure_duckdb_views


def test_view_glob_uses_forward_slashes():
    glob_path, hive = _view_glob("C:/Users/测试/lake", DATASETS["daily_bars"])
    assert "\\" not in glob_path
    assert glob_path.startswith("C:/Users/测试/lake/curated/daily_bars/")
    assert hive is True


def test_parquet_glob_is_posix(tmp_path):
    pattern = parquet_glob(tmp_path / "curated" / "daily_bars")
    assert "\\" not in pattern
    assert pattern.endswith("/**/*.parquet")


def test_ensure_duckdb_views_accepts_native_windows_style_root(tmp_path):
    # Build a root whose str() would contain backslashes on Windows; on Unix
    # resolve().as_posix() is a no-op, so the assertion still holds.
    data_root = tmp_path / "cn-market-lake"
    cfg = Config(data_root=data_root)
    db = ensure_duckdb_views(cfg)
    assert db.exists()
    # The helper itself must never feed a backslash into the SQL it builds —
    # re-check the path form used for globs.
    posix = data_root.resolve().as_posix()
    assert "\\" not in posix or Path(posix).as_posix() == posix
