from datetime import date

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import Config
from cn_market_lake.orchestrator.engine import JobEngine
from cn_market_lake.orchestrator.manifest import Manifest
from cn_market_lake.orchestrator.run_lock import RunLockError, run_lock
from cn_market_lake.storage.layout import init_data_layout


def test_retry_pending_when_batches_still_running(tmp_path):
    cfg = Config(data_root=tmp_path / "data", tdx_allow_mock=True)
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily", {"trade_date": "2024-06-28"})
    manifest.start_batch(
        run_id,
        "batch-live",
        "daily_bars",
        "daily_bars",
        symbols=["600519.SH"],
        window_start="2024-06-28",
        window_end="2024-06-28",
    )
    manifest.finish_run(run_id, "failed")

    engine = JobEngine(cfg)
    result = engine.run_job(
        "retry",
        date(2024, 6, 28),
        run_id=run_id,
        retry_failed_only=True,
    )
    assert result["status"] == "pending"
    assert result["retried"] == 0
    assert result["incomplete_batches"] == 1
    assert result["incomplete_by_status"]["running"] == 1


def test_worker_batch_specs_reads_manifest_window(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    engine = JobEngine(cfg)
    manifest = engine.manifest
    run_id = manifest.start_run("daily", {})
    manifest.start_batch(
        run_id,
        "2016-01-01_2024-06-27-batch-0",
        "daily_bars",
        "daily_bars",
        symbols=["600519.SH"],
        window_start="2016-01-01",
        window_end="2024-06-27",
    )
    manifest.finish_batch(run_id, "2016-01-01_2024-06-27-batch-0", "failed", error_message="boom")

    specs = engine._worker_batch_specs(manifest.get_failed_batches(run_id), date(2024, 6, 28))
    assert specs == [
        (
            "2016-01-01_2024-06-27-batch-0",
            ["600519.SH"],
            date(2016, 1, 1),
            date(2024, 6, 27),
        )
    ]


def test_run_lock_blocks_concurrent_retry(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    run_id = "run-lock-test"
    with run_lock(cfg.meta_root, run_id):
        try:
            with run_lock(cfg.meta_root, run_id):
                raise AssertionError("should not acquire twice")
        except RunLockError:
            pass


def test_daily_ingestion_lock_blocks_overlapping_runs(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    engine = JobEngine(cfg)
    monkeypatch.setattr(engine, "_run_wave", lambda *args, **kwargs: ([], 0, 0, False, False))

    with run_lock(cfg.meta_root, "daily_ingestion"):
        try:
            engine.run_job("daily:core", date(2024, 6, 28), waves=[])
        except RunLockError:
            pass
        else:
            raise AssertionError("expected RunLockError")


def test_reconcile_orphaned_runs_closes_stale_running(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core", {})
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars")
    out = manifest.reconcile_orphaned_runs(stale_after_seconds=0)
    assert out["runs_closed"] == 1
    assert out["batches_closed"] == 1
    assert manifest.get_run(run_id)["status"] == "failed"


def test_reconcile_keeps_a_run_with_fresh_batch_heartbeat(tmp_path):
    """Long jobs must not be closed just because run.started_at is old."""
    from datetime import datetime, timedelta, timezone

    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("valuation_2001", {})
    # Backdate the run start so a started_at-only check would kill it.
    old = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
    with manifest._connect() as conn:
        conn.execute(
            "UPDATE ingestion_runs SET started_at = ? WHERE run_id = ?",
            (old, run_id),
        )
    manifest.start_batch(run_id, "batch-0", "valuation_metrics", "valuation_metrics")
    # Fresh heartbeat (just set by start_batch).
    out = manifest.reconcile_orphaned_runs(stale_after_seconds=3600)
    assert out["runs_closed"] == 0
    assert manifest.get_run(run_id)["status"] == "running"


def test_reconcile_skips_a_run_whose_lock_is_held(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core", {})
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars")
    with run_lock(cfg.meta_root, run_id):
        out = manifest.reconcile_orphaned_runs(stale_after_seconds=0, locks_root=cfg.meta_root)
    assert out["runs_closed"] == 0
    assert out["skipped_locked"] == 1
    assert manifest.get_run(run_id)["status"] == "running"


def test_reconcile_is_idempotent(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core", {})
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars")
    assert manifest.reconcile_orphaned_runs(stale_after_seconds=0)["runs_closed"] == 1
    assert manifest.reconcile_orphaned_runs(stale_after_seconds=0)["runs_closed"] == 0


def test_retry_finishes_a_zombie_run_with_all_batches_success(tmp_path):
    """Crash after the last batch left status=running; retry must close it."""
    cfg = Config(data_root=tmp_path / "data", tdx_allow_mock=True)
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core", {"trade_date": "2024-06-28"})
    manifest.start_batch(run_id, "batch-0", "instruments", "instruments")
    manifest.finish_batch(run_id, "batch-0", "success", rows_written=1)
    # Deliberately leave the run in 'running'.

    engine = JobEngine(cfg)
    result = engine.run_job(
        "retry",
        date(2024, 6, 28),
        run_id=run_id,
        retry_failed_only=True,
    )
    assert result["status"] == "success"
    assert manifest.get_run(run_id)["status"] == "success"


def test_run_job_reconciles_orphans_on_entry(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data", tdx_allow_mock=True, batch_stale_seconds=0)
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    zombie = manifest.start_run("valuation_2001", {})
    manifest.start_batch(zombie, "batch-0", "valuation_metrics", "valuation_metrics")

    engine = JobEngine(cfg)
    monkeypatch.setattr(engine, "_run_wave", lambda *args, **kwargs: ([], 0, 0, False, False))
    engine.run_job("daily:core", date(2024, 6, 28), waves=[])

    assert manifest.get_run(zombie)["status"] == "failed"


def test_retry_reconciles_peer_orphans_on_entry(tmp_path):
    """cml retry used to skip reconcile; peer zombies sat until the next daily."""
    cfg = Config(data_root=tmp_path / "data", tdx_allow_mock=True, batch_stale_seconds=0)
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    zombie = manifest.start_run("valuation_2001", {})
    manifest.start_batch(zombie, "batch-0", "valuation_metrics", "valuation_metrics")

    target = manifest.start_run("daily:core", {"trade_date": "2024-06-28"})
    manifest.start_batch(target, "batch-0", "instruments", "instruments")
    manifest.finish_batch(target, "batch-0", "success", rows_written=1)
    # Leave target running so retry's all-green path closes it.

    engine = JobEngine(cfg)
    engine.run_job(
        "retry",
        date(2024, 6, 28),
        run_id=target,
        retry_failed_only=True,
    )
    assert manifest.get_run(zombie)["status"] == "failed"
    assert manifest.get_run(target)["status"] == "success"
