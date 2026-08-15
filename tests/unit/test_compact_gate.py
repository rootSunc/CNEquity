from datetime import date, datetime, timedelta, timezone

import polars as pl

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import Config
from cn_market_lake.orchestrator.compact_gate import (
    compact_allowed,
    datasets_with_incomplete_batches,
)
from cn_market_lake.orchestrator.manifest import Manifest
from cn_market_lake.steps.finalize import step_audit, step_compact
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


def test_compact_skips_dataset_with_failed_batches(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    run_id = "run-gate"
    trade_date = date(2024, 6, 28)
    manifest = Manifest(cfg.manifest_path)

    manifest.start_batch(run_id, "batch-ok", "daily_bars", "daily_bars", symbols=["000001.SZ"])
    manifest.finish_batch(run_id, "batch-ok", "success", rows_written=1)
    manifest.start_batch(run_id, "batch-fail", "daily_bars", "daily_bars", symbols=["600519.SH"])
    manifest.finish_batch(run_id, "batch-fail", "failed", error_message="simulated")

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "daily_bars",
        run_id,
        "batch-ok",
        pl.DataFrame([_daily_bar_row("000001.SZ", trade_date)]),
    )

    state = StateStore(cfg.meta_root)
    state.set_date("daily_bars", date(2024, 6, 27))

    result = step_compact(cfg, trade_date, run_id, {})
    skipped = result.get("context_updates", {}).get("compact_skipped_datasets", [])
    assert skipped == [{"dataset": "daily_bars", "incomplete_batches": 1}]
    assert state.get_date("daily_bars") == date(2024, 6, 27)
    assert not (cfg.curated_root / "daily_bars" / "trade_date=2024-06-28").exists()


def test_compact_skips_dataset_with_running_batches(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    run_id = "run-running"
    trade_date = date(2024, 6, 28)
    manifest = Manifest(cfg.manifest_path)

    manifest.start_batch(run_id, "batch-ok", "daily_bars", "daily_bars", symbols=["000001.SZ"])
    manifest.finish_batch(run_id, "batch-ok", "success", rows_written=1)
    manifest.start_batch(run_id, "batch-stuck", "daily_bars", "daily_bars", symbols=["600519.SH"])

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "daily_bars",
        run_id,
        "batch-ok",
        pl.DataFrame([_daily_bar_row("000001.SZ", trade_date)]),
    )

    state = StateStore(cfg.meta_root)
    state.set_date("daily_bars", date(2024, 6, 27))

    result = step_compact(cfg, trade_date, run_id, {})
    skipped = result.get("context_updates", {}).get("compact_skipped_datasets", [])
    assert skipped == [{"dataset": "daily_bars", "incomplete_batches": 1}]
    assert state.get_date("daily_bars") == date(2024, 6, 27)
    assert not (cfg.curated_root / "daily_bars" / "trade_date=2024-06-28").exists()


def test_compact_advances_watermark_when_all_batches_succeed(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    run_id = "run-ok"
    trade_date = date(2024, 6, 28)
    manifest = Manifest(cfg.manifest_path)

    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars", symbols=["000001.SZ"])
    manifest.finish_batch(run_id, "batch-0", "success", rows_written=1)

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "daily_bars",
        run_id,
        "batch-0",
        pl.DataFrame([_daily_bar_row("000001.SZ", trade_date)]),
    )

    state = StateStore(cfg.meta_root)
    state.set_date("daily_bars", date(2024, 6, 27))

    step_compact(cfg, trade_date, run_id, {})
    assert state.get_date("daily_bars") == trade_date
    assert (
        cfg.curated_root / "daily_bars" / "trade_date=2024-06-28" / "part-merged.parquet"
    ).exists()


def test_compact_snapshot_watermark_uses_run_date_not_max_partition(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    run_id = "run-snapshot-wm"
    trade_date = date(2024, 6, 28)
    manifest = Manifest(cfg.manifest_path)

    manifest.start_batch(run_id, "batch-0", "fund_flow", "fund_flow")
    manifest.finish_batch(run_id, "batch-0", "success", rows_written=1)

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "fund_flow",
        run_id,
        "batch-0",
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [trade_date],
                "main_net_inflow": [1.0],
                "super_large_net_inflow": [0.0],
                "large_net_inflow": [0.0],
                "medium_net_inflow": [0.0],
                "small_net_inflow": [0.0],
                "source": ["eastmoney"],
                "data_version": ["v1"],
                "fetched_at": ["2024-06-28T00:00:00+00:00"],
            }
        ),
    )

    state = StateStore(cfg.meta_root)
    state.set_date("fund_flow", date(2024, 6, 25))

    step_compact(cfg, trade_date, run_id, {})
    assert state.get_date("fund_flow") == trade_date


def test_audit_emits_compact_skipped_warning(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    run_id = "run-audit"
    trade_date = date(2024, 6, 28)
    context = {"compact_skipped_datasets": [{"dataset": "daily_bars", "incomplete_batches": 2}]}

    step_audit(cfg, trade_date, run_id, context)

    findings_path = cfg.meta_root / "quality" / "findings" / f"{run_id}.json"
    assert findings_path.exists()
    import json

    payload = json.loads(findings_path.read_text(encoding="utf-8"))
    warnings = [f for f in payload["findings"] if f.get("check") == "compact_skipped"]
    assert len(warnings) == 1
    assert warnings[0]["severity"] == "warning"
    assert warnings[0]["incomplete_batches"] == 2


def test_datasets_with_incomplete_batches_and_compact_allowed_without_liveness_refresh(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = "run-gate-2"
    manifest.start_batch(run_id, "batch-fail", "daily_bars", "daily_bars")
    manifest.finish_batch(run_id, "batch-fail", "failed", error_message="boom")
    manifest.start_batch(run_id, "batch-ok", "index_bars", "index_bars")
    manifest.finish_batch(run_id, "batch-ok", "success", rows_written=1)

    incomplete = datasets_with_incomplete_batches(manifest, run_id)
    assert incomplete == frozenset({"daily_bars"})

    # stale_after_seconds=None skips the liveness refresh branch entirely.
    allowed, count = compact_allowed(manifest, run_id, "daily_bars")
    assert allowed is False
    assert count == 1

    allowed_ok, count_ok = compact_allowed(manifest, run_id, "index_bars")
    assert allowed_ok is True
    assert count_ok == 0


def test_mark_stale_running_batches_failed(tmp_path):
    cfg = Config(data_root=tmp_path / "data", batch_stale_seconds=60)
    manifest = Manifest(cfg.manifest_path)
    run_id = "run-stale"
    manifest.start_batch(run_id, "batch-stuck", "daily_bars", "daily_bars", symbols=["600519.SH"])
    old_start = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    with manifest._connect() as conn:
        conn.execute(
            """
            UPDATE ingestion_batches
            SET started_at = ?, heartbeat_at = ?
            WHERE run_id = ? AND batch_id = ?
            """,
            (old_start, old_start, run_id, "batch-stuck"),
        )

    marked = manifest.mark_stale_running_batches_failed(run_id, stale_after_seconds=60)
    assert marked == 2
    batches = manifest.get_batches_for_run(run_id)
    assert batches[0]["status"] == "failed"
    assert "heartbeat" in (batches[0]["error_message"] or "").lower()
