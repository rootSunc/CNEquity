import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from cn_market_lake.config import Config, FailoverDatasetSpec
from cn_market_lake.quality.source_diff import diff_dataset
from cn_market_lake.storage.source_snapshots import (
    SnapshotStore,
    clean_source_snapshots,
)


def _bars_df(symbol: str, close: float, trade_date: date = date(2024, 6, 28)) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [symbol],
            "trade_date": [trade_date],
            "close": [close],
            "open": [close - 10.0],
            "high": [close + 10.0],
            "low": [close - 20.0],
            "volume": [1000],
            "amount": [1_000_000.0],
            "source": ["eastmoney"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    )


def test_snapshot_store_roundtrip(tmp_path):
    root = tmp_path / "data"
    store = SnapshotStore(root / "meta")
    path = store.write(
        "daily_bars",
        _bars_df("600519.SH", 1800.0),
        source="eastmoney",
        data_version="v1",
        run_id="run-1",
        batch_id="backup",
        trade_date=date(2024, 6, 28),
    )
    assert path is not None
    out = store.read_latest("daily_bars", source="eastmoney")
    assert out.height == 1


def test_read_latest_uses_newest_run_only(tmp_path):
    """read_latest must not concat every historical run_id (would grow unbounded)."""
    store = SnapshotStore(tmp_path / "meta")
    store.write(
        "daily_bars",
        _bars_df("600519.SH", 1800.0),
        source="eastmoney",
        data_version="v1",
        run_id="run-old",
        trade_date=date(2024, 6, 28),
    )
    old_dir = (
        tmp_path
        / "meta"
        / "source_snapshots"
        / "daily_bars"
        / "source=eastmoney"
        / "data_version=v1"
        / "run_id=run-old"
    )
    # Ensure mtime ordering: old run older than new.
    older = (datetime.now(timezone.utc) - timedelta(days=3)).timestamp()
    for path in [old_dir, *old_dir.rglob("*")]:
        if path.exists():
            Path(path).touch()
            os.utime(path, (older, older))

    store.write(
        "daily_bars",
        _bars_df("600519.SH", 1900.0),
        source="eastmoney",
        data_version="v1",
        run_id="run-new",
        trade_date=date(2024, 6, 29),
    )
    out = store.read_latest("daily_bars", source="eastmoney")
    assert out.height == 1
    assert out["close"][0] == 1900.0


def test_clean_source_snapshots_keeps_newest_and_recent(tmp_path):
    meta = tmp_path / "meta"
    store = SnapshotStore(meta)
    store.write(
        "daily_bars",
        _bars_df("600519.SH", 1800.0),
        source="eastmoney",
        data_version="v1",
        run_id="run-stale",
        trade_date=date(2024, 6, 1),
    )
    store.write(
        "daily_bars",
        _bars_df("600519.SH", 1900.0),
        source="eastmoney",
        data_version="v1",
        run_id="run-fresh",
        trade_date=date(2024, 6, 28),
    )
    stale = (
        meta
        / "source_snapshots"
        / "daily_bars"
        / "source=eastmoney"
        / "data_version=v1"
        / "run_id=run-stale"
    )
    old_ts = (datetime.now(timezone.utc) - timedelta(days=30)).timestamp()
    for path in [stale, *stale.rglob("*")]:
        os.utime(path, (old_ts, old_ts))

    result = clean_source_snapshots(meta, retention_days=14, dry_run=False)
    assert any("run-stale" in p for p in result.removed_run_dirs)
    assert any("run-fresh" in p for p in result.kept_run_dirs)
    assert not stale.exists()
    assert store.read_latest("daily_bars", source="eastmoney")["close"][0] == 1900.0


def test_source_diff_detects_price_drift(tmp_path):
    root = tmp_path / "data"
    curated = root / "curated" / "daily_bars" / "trade_date=2024-06-28"
    curated.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "close": [1800.0],
            "volume": [1000],
            "open": [1790.0],
            "high": [1810.0],
            "low": [1780.0],
            "amount": [1.0],
            "source": ["tdx_protocol"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    ).write_parquet(curated / "part-0.parquet")

    store = SnapshotStore(root / "meta")
    store.write(
        "daily_bars",
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 28)],
                "close": [1802.0],
                "volume": [1000],
                "open": [1790.0],
                "high": [1810.0],
                "low": [1780.0],
                "amount": [1.0],
                "source": ["eastmoney"],
                "data_version": ["v1"],
                "fetched_at": ["2024-06-28T00:00:00+00:00"],
            }
        ),
        source="eastmoney",
        data_version="v1",
        run_id="run-1",
        batch_id="backup",
        trade_date=date(2024, 6, 28),
    )

    cfg = Config(
        data_root=root,
        failover_enabled=True,
        failover_datasets=[
            FailoverDatasetSpec(
                name="daily_bars",
                primary="tdx_protocol",
                backup="eastmoney",
                compare_fields=["close", "volume"],
                price_tolerance_bps=10.0,
            )
        ],
    )
    diffs = diff_dataset(cfg, cfg.failover_datasets[0], trade_date=date(2024, 6, 28))
    price_diffs = [d for d in diffs if d.get("check") == "price_drift"]
    assert len(price_diffs) == 1
    assert price_diffs[0]["bps"] > 10.0
