import os
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone

import pytest

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import load_config
from cn_market_lake.config.bootstrap import path_for_toml
from cn_market_lake.orchestrator.engine import JobEngine
from cn_market_lake.orchestrator.manifest import Manifest
from cn_market_lake.orchestrator.worker_pool import fetch_daily_bars_parallel
from cn_market_lake.storage import StagingWriter
from cn_market_lake.storage.layout import init_data_layout


@pytest.fixture
def worker_config(tmp_path, monkeypatch):
    cfg_path = tmp_path / "test.toml"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[orchestrator]
workers = 1
batch_size = 1

[tdx_protocol]
allow_mock = true

[[job.daily.waves]]
name = "bars"
parallel = false
steps = ["daily_bars"]
"""
    )
    return load_config(cfg_path)


def test_worker_pool_records_symbol_batches(worker_config, monkeypatch):
    init_data_layout(worker_config)
    manifest = Manifest(worker_config.manifest_path)
    run_id = manifest.start_run("test")

    fetch_daily_bars_parallel(
        worker_config,
        ["600519.SH", "000001.SZ"],
        date(2024, 6, 27),
        date(2024, 6, 28),
        run_id,
        "daily_bars",
    )

    batches = manifest.get_batches_for_run(run_id)
    assert len(batches) == 2
    assert {b["batch_id"] for b in batches} == {
        "2024-06-27_2024-06-28-batch-0",
        "2024-06-27_2024-06-28-batch-1",
    }


def test_manifest_accepts_str_db_path(tmp_path):
    """Process-pool workers pass manifest_path as str across the boundary."""
    db = tmp_path / "meta" / "manifest.db"
    manifest = Manifest(str(db))
    assert manifest.db_path == db
    run_id = manifest.start_run("test")
    assert run_id


def test_manifest_connection_context_closes_connection(tmp_path):
    manifest = Manifest(tmp_path / "meta" / "manifest.db")

    conn = manifest._connect()
    with conn:
        conn.execute("SELECT 1")

    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        conn.execute("SELECT 1")


@pytest.mark.skipif(
    sys.platform.startswith("linux") and os.environ.get("CI") == "true",
    reason="ProcessPoolExecutor + SQLite manifest hangs on Linux GitHub runners",
)
def test_worker_pool_multiprocess_records_batches(worker_config, monkeypatch):
    """Regression: workers>1 used to crash Manifest(str) before start_batch."""
    worker_config.workers = 2
    init_data_layout(worker_config)
    manifest = Manifest(worker_config.manifest_path)
    run_id = manifest.start_run("test")

    fetch_daily_bars_parallel(
        worker_config,
        ["600519.SH", "000001.SZ"],
        date(2024, 6, 27),
        date(2024, 6, 28),
        run_id,
        "daily_bars",
    )

    batches = manifest.get_batches_for_run(run_id)
    assert len(batches) == 2
    assert all(b["status"] == "success" for b in batches)


def test_symbol_batch_ids_unique_per_window(worker_config, monkeypatch):
    init_data_layout(worker_config)
    manifest = Manifest(worker_config.manifest_path)
    run_id = manifest.start_run("test")

    fetch_daily_bars_parallel(
        worker_config,
        ["600519.SH"],
        date(2024, 6, 20),
        date(2024, 6, 21),
        run_id,
        "daily_bars",
    )
    fetch_daily_bars_parallel(
        worker_config,
        ["600519.SH"],
        date(2016, 1, 1),
        date(2024, 6, 28),
        run_id,
        "daily_bars",
    )

    batches = manifest.get_batches_for_run(run_id)
    assert len(batches) == 2
    assert len({b["batch_id"] for b in batches}) == 2


def test_retry_reruns_failed_symbol_batch_only(worker_config, monkeypatch):
    from cn_market_lake.adapters.tdx_protocol import client as tdx
    from cn_market_lake.derive.adj_factors import AdjFactorsResult

    calls: list[list[str]] = []

    attempts: dict[str, int] = {}

    def _fetch(symbols, start, end, **kwargs):
        calls.append(list(symbols))
        sym = symbols[0]
        attempts[sym] = attempts.get(sym, 0) + 1
        if sym == "600519.SH" and attempts[sym] == 1:
            raise tdx.TdxSourceError("simulated failure")
        return tdx._mock_bars(symbols, start, end)

    monkeypatch.setattr("cn_market_lake.orchestrator.worker_pool.fetch_daily_bars", _fetch)
    monkeypatch.setattr(
        "cn_market_lake.derive.adj_factors.compute_adj_factors",
        lambda *args, **kwargs: AdjFactorsResult(0, 0, [], []),
    )
    monkeypatch.setattr(
        "cn_market_lake.steps.bars.load_symbols",
        lambda _cfg: ["600519.SH", "000001.SZ"],
    )
    worker_config.tdx_allow_mock = False

    init_data_layout(worker_config)
    engine = JobEngine(worker_config)
    result = engine.run_job("daily", date(2024, 6, 28), steps=["daily_bars"])
    assert result["status"] == "failed"
    assert list(worker_config.staging_root.glob("daily_bars/**/*.parquet"))
    assert not list(worker_config.curated_root.glob("daily_bars/**/*.parquet"))

    retry = engine.run_job(
        "daily",
        date(2024, 6, 28),
        run_id=result["run_id"],
        retry_failed_only=True,
    )
    assert retry["retried"] >= 1
    assert retry["status"] == "success"
    assert calls[-1] == ["600519.SH"]
    curated = (
        worker_config.curated_root / "daily_bars" / "trade_date=2024-06-28" / "part-merged.parquet"
    )
    assert curated.exists()
    steps_run = {r.get("step") for r in retry["results"]}
    assert steps_run >= {
        "compact",
        "derive_adj_factors",
        "derive_industry_index",
        "audit",
    }


def test_retry_requeues_stale_running_batch(worker_config, monkeypatch):
    from cn_market_lake.adapters.tdx_protocol import client as tdx
    from cn_market_lake.derive.adj_factors import AdjFactorsResult

    calls: list[list[str]] = []
    worker_config.batch_stale_seconds = 60

    def _fetch(symbols, start, end, **kwargs):
        calls.append(list(symbols))
        return tdx._mock_bars(symbols, start, end)

    monkeypatch.setattr("cn_market_lake.orchestrator.worker_pool.fetch_daily_bars", _fetch)
    monkeypatch.setattr(
        "cn_market_lake.derive.adj_factors.compute_adj_factors",
        lambda *args, **kwargs: AdjFactorsResult(0, 0, [], []),
    )

    init_data_layout(worker_config)
    manifest = Manifest(worker_config.manifest_path)
    run_id = manifest.start_run("daily")
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars", symbols=["000001.SZ"])
    manifest.finish_batch(run_id, "batch-0", "success", rows_written=1)
    writer = StagingWriter(worker_config.staging_root)
    writer.write_batch(
        "daily_bars",
        run_id,
        "batch-0",
        tdx.normalize_with_source(
            tdx._mock_bars(["000001.SZ"], date(2024, 6, 28), date(2024, 6, 28))
        ),
    )
    manifest.start_batch(run_id, "batch-1", "daily_bars", "daily_bars", symbols=["600519.SH"])
    old_start = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    with manifest._connect() as conn:
        conn.execute(
            """
            UPDATE ingestion_batches
            SET started_at = ?, heartbeat_at = ?
            WHERE run_id = ? AND batch_id = ?
            """,
            (old_start, old_start, run_id, "batch-1"),
        )

    engine = JobEngine(worker_config)
    retry = engine.run_job(
        "daily",
        date(2024, 6, 28),
        run_id=run_id,
        retry_failed_only=True,
    )
    assert retry["stale_marked_failed"] == 2
    assert retry["batch_timeout"] == {"running_to_stale": 1, "stale_to_failed": 1}
    assert retry["retried"] == 1
    assert retry["status"] == "success"
    assert calls == [["600519.SH"]]
    curated = (
        worker_config.curated_root / "daily_bars" / "trade_date=2024-06-28" / "part-merged.parquet"
    )
    assert curated.exists()
