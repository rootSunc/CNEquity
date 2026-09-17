from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

import cnequity.steps  # noqa: F401
from cnequity.config import Config, FailoverDatasetSpec
from cnequity.orchestrator.compact_gate import compact_allowed, datasets_with_incomplete_batches
from cnequity.orchestrator.manifest import Manifest
from cnequity.steps.finalize import step_audit, step_compact
from cnequity.storage import StagingWriter
from cnequity.storage.revisions import RevisionStore
from cnequity.storage.source_snapshots import SnapshotStore
from cnequity.storage.state import StateStore


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


def _gate_bar_frame(trade_date: date, close: float, source: str) -> pl.DataFrame:
    row = _daily_bar_row("600519.SH", trade_date)
    row.update(
        {
            "open": close - 10.0,
            "high": close + 10.0,
            "low": close - 20.0,
            "close": close,
            "amount": close * 1000.0,
            "source": source,
        }
    )
    return pl.DataFrame([row])


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
    assert result["status"] == "warning"
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


def test_source_diff_gate_rolls_back_mutable_candidate_before_next_partition(tmp_path):
    root = tmp_path / "data"
    cfg = Config(
        data_root=root,
        failover_enabled=True,
        failover_datasets=[
            FailoverDatasetSpec(
                name="daily_bars",
                primary="tdx_protocol",
                backup="eastmoney",
                compare_fields=["close"],
                price_tolerance_bps=10.0,
                snapshot_cadence="daily",
                revision_gate=True,
            )
        ],
    )
    manifest = Manifest(cfg.manifest_path)
    writer = StagingWriter(cfg.staging_root)
    backup = SnapshotStore(cfg.meta_root)

    def stage(run_id: str, batch_id: str, day: date, frame: pl.DataFrame) -> None:
        manifest.start_batch(run_id, batch_id, "daily_bars", "daily_bars", symbols=["600519.SH"])
        manifest.finish_batch(run_id, batch_id, "success", rows_written=frame.height)
        writer.write_batch("daily_bars", run_id, batch_id, frame)

    def backup_day(run_id: str, day: date, close: float) -> None:
        backup.write(
            "daily_bars",
            _gate_bar_frame(day, close, "eastmoney"),
            source="eastmoney",
            data_version="v1",
            run_id=run_id,
            trade_date=day,
        )

    day_one = date(2024, 6, 28)
    day_bad = date(2024, 7, 1)
    day_next = date(2024, 7, 2)
    stage("run-good", "batch-good", day_one, _gate_bar_frame(day_one, 1800.0, "tdx_protocol"))
    backup_day("backup-good", day_one, 1800.0)
    first = step_compact(cfg, day_one, "run-good", {})
    assert first["dataset_revisions"]["daily_bars"]["revision"] == 1

    # The candidate contains the prior good partition plus this bad one.  The
    # gate must quarantine the whole mutable candidate and restore the pointer
    # generation, rather than leave day_bad to contaminate the next merge.
    stage("run-bad", "batch-bad", day_bad, _gate_bar_frame(day_bad, 1800.0, "tdx_protocol"))
    backup_day("backup-bad", day_bad, 1802.0)
    blocked = step_compact(cfg, day_bad, "run-bad", {})
    skipped = blocked["context_updates"]["compact_skipped_datasets"]
    assert skipped[0]["reason"] == "source_diff_gate"
    quarantine = Path(skipped[0]["quarantine"])
    assert quarantine.is_dir()
    assert list(quarantine.rglob("trade_date=2024-07-01/*.parquet"))

    revisions = RevisionStore(cfg.meta_root, cfg.curated_root)
    pointer = revisions.current_pointer("daily_bars")
    published = revisions.current_root("daily_bars")
    assert pointer is not None and pointer["revision"] == 1
    assert published is not None
    assert list(published.rglob("trade_date=2024-07-01/*.parquet")) == []
    mutable = cfg.curated_root / "daily_bars"
    assert list(mutable.rglob("trade_date=2024-07-01/*.parquet")) == []
    assert pl.read_parquet(next(published.rglob("*.parquet")))["trade_date"].to_list() == [day_one]

    # A later good partition must merge from the restored immutable revision,
    # not from the quarantined candidate.
    stage("run-next", "batch-next", day_next, _gate_bar_frame(day_next, 1805.0, "tdx_protocol"))
    backup_day("backup-next", day_next, 1805.0)
    next_result = step_compact(cfg, day_next, "run-next", {})
    assert next_result["dataset_revisions"]["daily_bars"]["revision"] == 2
    current = revisions.current_root("daily_bars")
    assert current is not None
    assert list(current.rglob("trade_date=2024-07-01/*.parquet")) == []
    assert sorted(
        pl.read_parquet(path)["trade_date"][0] for path in current.rglob("*.parquet")
    ) == [
        day_one,
        day_next,
    ]
    assert list(mutable.rglob("trade_date=2024-07-01/*.parquet")) == []


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


def _run_with_batch(tmp_path, step_result):
    """Run one fake step through the engine and return (manifest, run_id)."""
    from cnequity.config import Config
    from cnequity.orchestrator.engine import JobEngine
    from cnequity.orchestrator.registry import STEP_REGISTRY, StepEntry
    from cnequity.storage.layout import init_data_layout

    cfg = Config(data_root=tmp_path / "lake")
    init_data_layout(cfg)
    engine = JobEngine(cfg)
    original = STEP_REGISTRY.get("instruments")
    STEP_REGISTRY["instruments"] = StepEntry(
        fn=lambda *args: dict(step_result), group="core", requires_workers=False
    )
    try:
        out = engine.run_job(
            "backfill", date(2026, 7, 1), steps=["instruments"], finalize_run=False
        )
    finally:
        if original is not None:
            STEP_REGISTRY["instruments"] = original
    return engine.manifest, out["run_id"]


def test_a_step_may_warn_while_its_batch_settles(tmp_path):
    """`daily_bars` carrying a tolerated gap: the rows are staged and the
    shortfall is in the ledger, so nothing is waiting to be retried.

    Leaving the batch `warning` had compact skip the whole dataset for that one
    batch — a measured init staged 3,894,608 rows and published none of them.
    """
    manifest, run_id = _run_with_batch(
        tmp_path,
        {"rows_read": 1, "rows_written": 1, "status": "warning", "batch_settled": True},
    )

    assert manifest.incomplete_batch_counts_by_dataset(run_id) == {}


def test_a_warning_without_that_signal_still_blocks(tmp_path):
    """The opt-in half. `trading_status` with partial ST evidence must keep
    blocking, or the lake publishes a coverage receipt for a universe it never
    swept — which `test_partial_st_rows_do_not_compact_or_publish_coverage`
    holds from the other side."""
    manifest, run_id = _run_with_batch(
        tmp_path, {"rows_read": 1, "rows_written": 1, "status": "warning"}
    )

    assert manifest.incomplete_batch_counts_by_dataset(run_id) == {"instruments": 1}
