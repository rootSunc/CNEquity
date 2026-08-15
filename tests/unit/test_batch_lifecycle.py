from datetime import date, datetime, timedelta, timezone

import polars as pl

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import Config
from cn_market_lake.orchestrator.manifest import Manifest
from cn_market_lake.steps.finalize import step_compact
from cn_market_lake.storage import StagingWriter
from cn_market_lake.storage.state import StateStore


def _daily_bar_row(symbol: str, trade_date: date) -> dict:
    return {
        "symbol": symbol,
        "trade_date": trade_date,
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "volume": 1000,
        "amount": 10_500.0,
        "source": "mock",
        "data_version": "v1",
        "fetched_at": f"{trade_date.isoformat()}T00:00:00+00:00",
    }


def test_fresh_heartbeat_prevents_stale_promotion(tmp_path):
    manifest = Manifest(Config(data_root=tmp_path / "data").manifest_path)
    run_id = "run-heartbeat"
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars", symbols=["600519.SH"])
    manifest.touch_batch_heartbeat(run_id, "batch-0")

    promoted = manifest.promote_running_to_stale(run_id, stale_after_seconds=3600)
    assert promoted == 0
    assert manifest.get_batches_for_run(run_id)[0]["status"] == "running"


def test_running_to_stale_to_failed_lifecycle(tmp_path):
    manifest = Manifest(Config(data_root=tmp_path / "data").manifest_path)
    run_id = "run-lifecycle"
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars", symbols=["600519.SH"])

    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    with manifest._connect() as conn:
        conn.execute(
            """
            UPDATE ingestion_batches
            SET started_at = ?, heartbeat_at = ?
            WHERE run_id = ? AND batch_id = ?
            """,
            (old, old, run_id, "batch-0"),
        )

    timeout = manifest.advance_batch_timeouts(run_id, stale_after_seconds=60)
    assert timeout == {"running_to_stale": 1, "stale_to_failed": 1}

    batch = manifest.get_batches_for_run(run_id)[0]
    assert batch["status"] == "failed"
    assert batch["finished_at"] is not None


def test_compact_promotes_stale_running_batch(tmp_path):
    cfg = Config(data_root=tmp_path / "data", batch_stale_seconds=60)
    run_id = "run-compact-stale"
    trade_date = date(2024, 6, 28)
    manifest = Manifest(cfg.manifest_path)

    manifest.start_batch(run_id, "batch-ok", "daily_bars", "daily_bars", symbols=["000001.SZ"])
    manifest.finish_batch(run_id, "batch-ok", "success", rows_written=1)
    manifest.start_batch(run_id, "batch-stuck", "daily_bars", "daily_bars", symbols=["600519.SH"])

    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    with manifest._connect() as conn:
        conn.execute(
            """
            UPDATE ingestion_batches
            SET started_at = ?, heartbeat_at = ?
            WHERE run_id = ? AND batch_id = ?
            """,
            (old, old, run_id, "batch-stuck"),
        )

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "daily_bars",
        run_id,
        "batch-ok",
        pl.DataFrame([_daily_bar_row("000001.SZ", trade_date)]),
    )

    state = StateStore(cfg.meta_root)
    state.set_date("daily_bars", date(2024, 6, 27))

    step_compact(cfg, trade_date, run_id, {})

    batches = {b["batch_id"]: b["status"] for b in manifest.get_batches_for_run(run_id)}
    assert batches["batch-stuck"] == "stale"
    assert state.get_date("daily_bars") == date(2024, 6, 27)
    assert not (cfg.curated_root / "daily_bars" / "trade_date=2024-06-28").exists()
