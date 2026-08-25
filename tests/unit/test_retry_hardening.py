from datetime import date

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.run_lock import RunLockError, run_lock
from cnequity.storage.layout import init_data_layout


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


def test_retry_uses_the_original_runs_trade_date_not_today(tmp_path, monkeypatch):
    """`cne retry` has no trade_date of its own; run_job() defaults the
    parameter to today when the caller omits it, exactly as the CLI does. A
    run retried after its own trade_date has rolled over must still resume
    the session it was started for, or a backfill for a past date silently
    asks sources for data dated today, which never lands.
    """
    cfg = Config(data_root=tmp_path / "data", tdx_allow_mock=True)
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    stored_date = "2024-06-28"
    run_id = manifest.start_run("daily", {"trade_date": stored_date})
    manifest.start_batch(
        run_id,
        "batch-daily_bars",
        "daily_bars",
        "daily_bars",
        symbols=["600519.SH"],
        window_start=stored_date,
        window_end=stored_date,
    )
    manifest.finish_batch(run_id, "batch-daily_bars", "failed", error_message="boom")
    manifest.finish_run(run_id, "failed")

    engine = JobEngine(cfg)
    seen_dates: list[date] = []

    def fake_run_step(name, trade_date, run_id, context, *, retry_of=None):
        seen_dates.append(trade_date)
        manifest.start_batch(run_id, "batch-daily_bars", name, name)
        manifest.finish_batch(run_id, "batch-daily_bars", "success")
        return {"status": "success"}

    monkeypatch.setattr(engine, "_run_step", fake_run_step)
    monkeypatch.setattr(engine, "_run_finalize_steps", lambda *args, **kwargs: [])

    # No trade_date passed — mirrors `cne retry --run-id <id>` exactly.
    engine.run_job("retry", run_id=run_id, retry_failed_only=True)

    assert seen_dates == [date.fromisoformat(stored_date)]


def test_retry_automatically_repeats_with_persisted_budget(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        tdx_allow_mock=True,
        max_retries=3,
        retry_backoff_seconds=0,
    )
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily", {"trade_date": "2024-06-28"})
    manifest.start_batch(
        run_id,
        "batch-daily_bars",
        "daily_bars",
        "daily_bars",
        symbols=["600519.SH"],
    )
    manifest.finish_batch(run_id, "batch-daily_bars", "failed", error_message="timeout")

    engine = JobEngine(cfg)
    attempts = 0
    timeout_passes = iter(
        [
            {"running_to_stale": 1, "stale_to_failed": 0},
            {"running_to_stale": 0, "stale_to_failed": 2},
        ]
    )

    def flaky_run_step(name, trade_date, resumed_run_id, context, *, retry_of=None):
        nonlocal attempts
        attempts += 1
        manifest.start_batch(resumed_run_id, "batch-daily_bars", name, name)
        status = "success" if attempts == 2 else "failed"
        manifest.finish_batch(
            resumed_run_id,
            "batch-daily_bars",
            status,
            error_message=None if status == "success" else "TDX timeout",
        )
        return {"status": status}

    monkeypatch.setattr(engine, "_run_step", flaky_run_step)
    monkeypatch.setattr(engine, "_run_finalize_steps", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        engine.manifest, "advance_batch_timeouts", lambda *args, **kwargs: next(timeout_passes)
    )
    result = engine.run_job("retry", run_id=run_id, retry_failed_only=True)

    assert result["status"] == "success"
    assert result["retry_passes"] == 2
    assert result["retried"] == 2
    assert result["retry_exhausted"] == 0
    assert [item["status"] for item in result["results"]] == ["failed", "success"]
    assert result["stale_marked_failed"] == 3
    assert result["batch_timeout"] == {"running_to_stale": 1, "stale_to_failed": 2}
    assert manifest.get_batches_for_run(run_id)[0]["retry_count"] == 2


def test_retry_stops_at_configured_budget(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        tdx_allow_mock=True,
        max_retries=2,
        retry_backoff_seconds=0,
    )
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily", {"trade_date": "2024-06-28"})
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars")
    manifest.finish_batch(run_id, "batch-0", "failed", error_message="timeout")

    engine = JobEngine(cfg)
    attempts = 0

    def always_fails(name, trade_date, resumed_run_id, context, *, retry_of=None):
        nonlocal attempts
        attempts += 1
        manifest.start_batch(resumed_run_id, "batch-0", name, name)
        manifest.finish_batch(resumed_run_id, "batch-0", "failed", error_message="timeout")
        return {"status": "failed"}

    monkeypatch.setattr(engine, "_run_step", always_fails)
    result = engine.run_job("retry", run_id=run_id, retry_failed_only=True)

    assert attempts == 2
    assert result["status"] == "failed"
    assert result["retry_passes"] == 2
    assert result["retry_exhausted"] == 1
    assert manifest.get_batches_for_run(run_id)[0]["retry_count"] == 2

    repeated = engine.run_job("retry", run_id=run_id, retry_failed_only=True)
    assert attempts == 2
    assert repeated["status"] == "failed"
    assert repeated["retried"] == 0
    assert repeated["retry_exhausted"] == 1


def test_retry_does_not_automatically_repeat_non_worker_steps(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        tdx_allow_mock=True,
        max_retries=3,
        retry_backoff_seconds=0,
    )
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily", {"trade_date": "2024-06-28"})
    manifest.start_batch(run_id, "batch-0", "trading_status", "trading_status")
    manifest.finish_batch(run_id, "batch-0", "failed", error_message="network timeout")

    engine = JobEngine(cfg)
    attempts = 0

    def still_fails(name, trade_date, resumed_run_id, context, *, retry_of=None):
        nonlocal attempts
        attempts += 1
        return {"status": "failed"}

    monkeypatch.setattr(engine, "_run_step", still_fails)
    result = engine.run_job("retry", run_id=run_id, retry_failed_only=True)

    assert attempts == 1
    assert result["status"] == "failed"
    assert result["retry_passes"] == 1
    assert manifest.get_batches_for_run(run_id)[0]["retry_count"] == 0


def test_non_worker_retry_lineage_remains_recoverable(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        tdx_allow_mock=True,
        max_retries=1,
        retry_backoff_seconds=0,
    )
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily", {"trade_date": "2024-06-28"})
    manifest.start_batch(run_id, "batch-original", "trading_status", "trading_status")
    manifest.finish_batch(run_id, "batch-original", "failed", error_message="network timeout")

    engine = JobEngine(cfg)
    attempts = 0

    def retry_step(name, trade_date, resumed_run_id, context, *, retry_of=None):
        nonlocal attempts
        attempts += 1
        batch_id = f"batch-retry-{attempts}"
        manifest.start_batch(resumed_run_id, batch_id, name, name)
        status = "failed" if attempts == 1 else "success"
        manifest.finish_batch(resumed_run_id, batch_id, status, error_message="timeout")
        if status == "success":
            manifest.supersede_batches(
                resumed_run_id,
                retry_of or [],
                superseded_by=batch_id,
            )
        return {"status": status}

    monkeypatch.setattr(engine, "_run_step", retry_step)
    monkeypatch.setattr(engine, "_run_finalize_steps", lambda *args, **kwargs: [])

    first = engine.run_job("retry", run_id=run_id, retry_failed_only=True)
    assert first["status"] == "failed"
    second = engine.run_job("retry", run_id=run_id, retry_failed_only=True)

    assert second["status"] == "success"
    assert attempts == 2
    statuses = {
        batch["batch_id"]: batch["status"] for batch in manifest.get_batches_for_run(run_id)
    }
    assert statuses == {
        "batch-original": "superseded",
        "batch-retry-1": "superseded",
        "batch-retry-2": "success",
    }


def test_retry_routes_by_task_id_when_physical_dataset_differs(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        tdx_allow_mock=True,
        max_retries=1,
        retry_backoff_seconds=0,
    )
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("backfill", {"trade_date": "2024-06-28"})
    manifest.start_batch(
        run_id,
        "history-failed",
        task_id="daily_bars_history",
        dataset="daily_bars",
    )
    manifest.finish_batch(run_id, "history-failed", "failed", error_message="network timeout")

    engine = JobEngine(cfg)
    seen_steps: list[str] = []

    def recover(name, trade_date, resumed_run_id, context, *, retry_of=None):
        seen_steps.append(name)
        manifest.start_batch(resumed_run_id, "history-success", name, "daily_bars")
        manifest.finish_batch(resumed_run_id, "history-success", "success")
        manifest.supersede_batches(
            resumed_run_id,
            retry_of or [],
            superseded_by="history-success",
        )
        return {"status": "success"}

    monkeypatch.setattr(engine, "_run_step", recover)
    monkeypatch.setattr(engine, "_run_finalize_steps", lambda *args, **kwargs: [])
    result = engine.run_job("retry", run_id=run_id, retry_failed_only=True)

    assert result["status"] == "success"
    assert seen_steps == ["daily_bars_history"]
    batches = {batch["batch_id"]: batch for batch in manifest.get_batches_for_run(run_id)}
    assert batches["history-failed"]["status"] == "superseded"
    assert batches["history-failed"]["retry_count"] == 0


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
    """cne retry used to skip reconcile; peer zombies sat until the next daily."""
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
