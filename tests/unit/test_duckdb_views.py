"""DuckDB / polars view paths must stay POSIX-form so Windows ``\\`` never hits globs."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.datasets import DATASETS
from cnequity.query.canonical import dedupe_by_primary_key, dedupe_lazy_by_primary_key
from cnequity.query.parquet_scan import parquet_glob
from cnequity.query.views import _view_glob, ensure_duckdb_views


def test_view_glob_uses_forward_slashes():
    glob_path, hive = _view_glob("C:/Users/测试/lake", DATASETS["daily_bars"])
    assert "\\" not in glob_path
    assert glob_path.startswith("C:/Users/测试/lake/curated/daily_bars/")
    assert hive is True


def test_merge_style_view_glob_is_recursive():
    glob_path, hive = _view_glob("/tmp/lake", DATASETS["delisting_events"])
    assert glob_path.endswith("/derived/delisting_events/**/*.parquet")
    assert hive is False


def test_parquet_glob_is_posix(tmp_path):
    pattern = parquet_glob(tmp_path / "curated" / "daily_bars")
    assert "\\" not in pattern
    assert pattern.endswith("/**/*.parquet")


def test_ensure_duckdb_views_accepts_native_windows_style_root(tmp_path):
    # Build a root whose str() would contain backslashes on Windows; on Unix
    # resolve().as_posix() is a no-op, so the assertion still holds.
    data_root = tmp_path / "cnequity"
    cfg = Config(data_root=data_root)
    db = ensure_duckdb_views(cfg)
    assert db.exists()
    # The helper itself must never feed a backslash into the SQL it builds —
    # re-check the path form used for globs.
    posix = data_root.resolve().as_posix()
    assert "\\" not in posix or Path(posix).as_posix() == posix


def test_duckdb_views_dedupe_duplicate_primary_keys(tmp_path):
    data_root = tmp_path / "data"
    partition = data_root / "curated" / "daily_bars" / "trade_date=2024-06-28"
    partition.mkdir(parents=True)
    day = date(2024, 6, 28)
    base = {
        "symbol": ["600519.SH"],
        "trade_date": [day],
        "open": [1790.0],
        "high": [1810.0],
        "low": [1780.0],
        "close": [1800.0],
        "volume": [1000],
        "amount": [1_000_000.0],
        "source": ["tdx_protocol"],
        "data_version": ["v2"],
        "fetched_at": [datetime(2024, 6, 28, tzinfo=timezone.utc)],
    }
    pl.DataFrame(base).write_parquet(partition / "part-old.parquet")
    newer = {
        **base,
        "close": [1900.0],
        "fetched_at": [datetime(2024, 6, 28, 1, tzinfo=timezone.utc)],
    }
    pl.DataFrame(newer).write_parquet(partition / "part-new.parquet")

    db = ensure_duckdb_views(Config(data_root=data_root))
    with duckdb.connect(str(db)) as con:
        rows = con.execute("SELECT symbol, close FROM daily_bars ORDER BY symbol").fetchall()
    assert rows == [("600519.SH", 1900.0)]


def test_canonical_dedupe_prefers_primary_source_on_same_timestamp():
    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH"],
            "trade_date": [date(2024, 6, 28)] * 2,
            "close": [1800.0, 1900.0],
            "source": ["eastmoney", "tdx_protocol"],
            "data_version": ["v9", "v1"],
            "fetched_at": [datetime(2024, 6, 28, tzinfo=timezone.utc)] * 2,
        }
    )

    eager = dedupe_by_primary_key(frame, "daily_bars")
    lazy = dedupe_lazy_by_primary_key(frame.lazy(), "daily_bars").collect()

    assert eager.select("source", "close").to_dicts() == [
        {"source": "tdx_protocol", "close": 1900.0}
    ]
    assert lazy.select("source", "close").to_dicts() == [
        {"source": "tdx_protocol", "close": 1900.0}
    ]


def test_canonical_dedupe_prefers_primary_source_without_fetch_time():
    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH"],
            "trade_date": [date(2024, 6, 28)] * 2,
            "close": [1800.0, 1900.0],
            "source": ["tdx_protocol", "eastmoney"],
            "data_version": ["v1", "v9"],
        }
    )

    eager = dedupe_by_primary_key(frame, "daily_bars")
    lazy = dedupe_lazy_by_primary_key(frame.lazy(), "daily_bars").collect()

    assert eager.select("source", "close").to_dicts() == [
        {"source": "tdx_protocol", "close": 1800.0}
    ]
    assert lazy.select("source", "close").to_dicts() == [
        {"source": "tdx_protocol", "close": 1800.0}
    ]


def test_canonical_dedupe_does_not_let_null_fetch_time_override_known_row():
    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH"],
            "trade_date": [date(2024, 6, 28)] * 2,
            "close": [1900.0, 1800.0],
            "source": ["tdx_protocol", "eastmoney"],
            "data_version": ["v1", "v1"],
            "fetched_at": [None, datetime(2024, 6, 28, tzinfo=timezone.utc)],
        }
    )

    eager = dedupe_by_primary_key(frame, "daily_bars")
    lazy = dedupe_lazy_by_primary_key(frame.lazy(), "daily_bars").collect()

    assert eager.select("source", "close").to_dicts() == [{"source": "eastmoney", "close": 1800.0}]
    assert lazy.select("source", "close").to_dicts() == [{"source": "eastmoney", "close": 1800.0}]


def test_duckdb_views_dedupe_prefers_primary_source_on_same_timestamp(tmp_path):
    data_root = tmp_path / "data"
    partition = data_root / "curated" / "daily_bars" / "trade_date=2024-06-28"
    partition.mkdir(parents=True)
    common = {
        "symbol": ["600519.SH"],
        "trade_date": [date(2024, 6, 28)],
        "open": [1790.0],
        "high": [1810.0],
        "low": [1780.0],
        "volume": [1000],
        "amount": [1_000_000.0],
        "fetched_at": [datetime(2024, 6, 28, tzinfo=timezone.utc)],
    }
    pl.DataFrame(
        {**common, "close": [1800.0], "source": ["eastmoney"], "data_version": ["v9"]}
    ).write_parquet(partition / "part-backup.parquet")
    pl.DataFrame(
        {**common, "close": [1900.0], "source": ["tdx_protocol"], "data_version": ["v1"]}
    ).write_parquet(partition / "part-primary.parquet")

    db = ensure_duckdb_views(Config(data_root=data_root))
    with duckdb.connect(str(db), read_only=True) as con:
        rows = con.execute("SELECT source, close FROM daily_bars").fetchall()

    assert rows == [("tdx_protocol", 1900.0)]


def test_duckdb_views_dedupe_legacy_rows_by_source_without_fetch_time(tmp_path):
    data_root = tmp_path / "data"
    partition = data_root / "curated" / "daily_bars" / "trade_date=2024-06-28"
    partition.mkdir(parents=True)
    common = {
        "symbol": ["600519.SH"],
        "trade_date": [date(2024, 6, 28)],
        "open": [1790.0],
        "high": [1810.0],
        "low": [1780.0],
        "volume": [1000],
        "amount": [1_000_000.0],
        "data_version": ["v1"],
    }
    pl.DataFrame({**common, "close": [1800.0], "source": ["eastmoney"]}).write_parquet(
        partition / "part-backup.parquet"
    )
    pl.DataFrame({**common, "close": [1900.0], "source": ["tdx_protocol"]}).write_parquet(
        partition / "part-primary.parquet"
    )

    db = ensure_duckdb_views(Config(data_root=data_root))
    with duckdb.connect(str(db), read_only=True) as con:
        rows = con.execute("SELECT source, close FROM daily_bars").fetchall()

    assert rows == [("tdx_protocol", 1900.0)]


def test_duckdb_trading_status_view_accepts_legacy_text_fetch_time(tmp_path):
    data_root = tmp_path / "data"
    partition = data_root / "curated" / "trading_status" / "trade_date=2024-06"
    partition.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "is_trading": [True],
            "status": ["normal"],
            "risk_warning": [False],
            "source": ["eastmoney"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T08:00:00+00:00"],
        }
    ).write_parquet(partition / "legacy-text-time.parquet")

    db = ensure_duckdb_views(Config(data_root=data_root))
    with duckdb.connect(str(db), read_only=True) as con:
        rows = con.execute("SELECT symbol, fetched_at FROM trading_status").fetchall()

    assert rows == [("600519.SH", "2024-06-28T08:00:00+00:00")]


def test_duckdb_views_merge_optional_columns_by_name(tmp_path):
    data_root = tmp_path / "data"
    partition = data_root / "curated" / "daily_bars" / "trade_date=2024-06-28"
    partition.mkdir(parents=True)
    common = {
        "trade_date": [date(2024, 6, 28)],
        "open": [10.0],
        "high": [10.0],
        "low": [10.0],
        "close": [10.0],
        "volume": [100],
        "source": ["tdx_protocol"],
        "data_version": ["v2"],
        "fetched_at": [datetime(2024, 6, 28, tzinfo=timezone.utc)],
    }
    pl.DataFrame({"symbol": ["600519.SH"], "amount": [1000.0], **common}).write_parquet(
        partition / "part-with-amount.parquet"
    )
    pl.DataFrame({"symbol": ["000001.SZ"], **common}).write_parquet(
        partition / "part-without-amount.parquet"
    )

    db = ensure_duckdb_views(Config(data_root=data_root))
    with duckdb.connect(str(db)) as con:
        rows = con.execute("SELECT symbol, amount FROM daily_bars ORDER BY symbol").fetchall()
    assert rows == [("000001.SZ", None), ("600519.SH", 1000.0)]


def test_duckdb_qfq_views_use_the_correct_anchor(tmp_path):
    data_root = tmp_path / "data"
    (data_root / "curated" / "daily_bars").mkdir(parents=True)
    (data_root / "derived" / "adj_factors").mkdir(parents=True)
    day = [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]
    pl.DataFrame(
        {
            "symbol": ["600519.SH"] * 3,
            "trade_date": day,
            "open": [9.0, 11.0, 13.0],
            "high": [11.0, 13.0, 15.0],
            "low": [8.0, 10.0, 12.0],
            "close": [10.0, 12.0, 14.0],
            "volume": [100, 100, 100],
            "amount": [1000.0, 1200.0, 1400.0],
            "source": ["test"] * 3,
            "data_version": ["v1"] * 3,
            "fetched_at": [datetime(2024, 1, 4, tzinfo=timezone.utc)] * 3,
        }
    ).write_parquet(data_root / "curated" / "daily_bars" / "part.parquet")
    pl.DataFrame(
        {
            "symbol": ["600519.SH"] * 3,
            "trade_date": day,
            "adjust_type": ["hfq"] * 3,
            "factor": [2.0, 3.0, 4.0],
            "source": ["test"] * 3,
            "data_version": ["v1"] * 3,
            "fetched_at": [datetime(2024, 1, 4, tzinfo=timezone.utc)] * 3,
        }
    ).write_parquet(data_root / "derived" / "adj_factors" / "part.parquet")

    db = ensure_duckdb_views(Config(data_root=data_root))
    with duckdb.connect(str(db), read_only=True) as con:
        full = con.execute(
            "SELECT trade_date, qfq_close FROM daily_bars_adj ORDER BY trade_date"
        ).fetchall()
        bounded = con.execute(
            """
            SELECT trade_date, qfq_close
            FROM daily_bars_qfq(DATE '2024-01-01', DATE '2024-01-02')
            ORDER BY trade_date
            """
        ).fetchall()

    # The static view anchors to the latest bar in the lake (factor=4), while
    # the macro anchors to the latest bar in its explicit two-day window
    # (factor=3), matching load(..., adjust='qfq', end='2024-01-02').
    assert [row[0] for row in full] == day
    assert [row[1] for row in full] == pytest.approx([5.0, 9.0, 14.0])
    assert [row[0] for row in bounded] == day[:2]
    assert [row[1] for row in bounded] == pytest.approx([20.0 / 3.0, 12.0])
