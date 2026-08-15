from datetime import date, datetime, timedelta, timezone

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.config.bootstrap import path_for_toml
from cn_market_lake.file_lock import exclusive_lock
from cn_market_lake.orchestrator.manifest import Manifest
from cn_market_lake.storage import StagingWriter
from cn_market_lake.storage.staging_cleanup import (
    clean_staging,
    clean_stale_lock_files,
    list_staging_run_ids,
    run_ready_for_staging_cleanup,
)


def _bar_row(symbol: str, trade_date: date) -> dict:
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


def test_clean_removes_staging_for_successful_compacted_run(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily", {"trade_date": "2024-06-28"})
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars")
    manifest.finish_batch(run_id, "batch-0", "success", rows_written=1)
    manifest.start_batch(run_id, "compact-0", "compact", "compact")
    manifest.finish_batch(run_id, "compact-0", "success", rows_written=1)
    manifest.finish_run(run_id, "success")

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "daily_bars",
        run_id,
        "batch-0",
        pl.DataFrame([_bar_row("000001.SZ", date(2024, 6, 28))]),
    )
    assert run_id in list_staging_run_ids(cfg.staging_root)

    result = clean_staging(cfg)
    assert run_id in result.removed_run_ids
    assert run_id not in list_staging_run_ids(cfg.staging_root)


def test_clean_skips_incomplete_run(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily", {})
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars")
    manifest.finish_batch(run_id, "batch-0", "failed", error_message="err")
    manifest.finish_run(run_id, "failed")

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "daily_bars",
        run_id,
        "batch-0",
        pl.DataFrame([_bar_row("000001.SZ", date(2024, 6, 28))]),
    )

    result = clean_staging(cfg, orphan_retention_days=999)
    assert run_id in result.skipped_run_ids
    assert run_id in list_staging_run_ids(cfg.staging_root)


def test_clean_never_deletes_failed_run_staging_without_force(tmp_path):
    """A failed run's success batches live only in staging; age must not matter."""
    import os

    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily", {})
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars")
    manifest.finish_batch(run_id, "batch-0", "success", rows_written=1)
    manifest.start_batch(run_id, "batch-1", "daily_bars", "daily_bars")
    manifest.finish_batch(run_id, "batch-1", "failed", error_message="err")
    manifest.finish_run(run_id, "failed")

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "daily_bars",
        run_id,
        "batch-0",
        pl.DataFrame([_bar_row("000001.SZ", date(2024, 6, 28))]),
    )
    run_dir = cfg.staging_root / "daily_bars" / f"run_id={run_id}"
    old = datetime.now(timezone.utc) - timedelta(days=30)
    os.utime(run_dir, (old.timestamp(), old.timestamp()))

    result = clean_staging(cfg, orphan_retention_days=7)
    assert run_id in result.skipped_run_ids
    assert run_id in list_staging_run_ids(cfg.staging_root)


def test_clean_force_deletes_failed_run_and_demotes_success_batches(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily", {})
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars")
    manifest.finish_batch(run_id, "batch-0", "success", rows_written=1)
    manifest.start_batch(run_id, "batch-1", "daily_bars", "daily_bars")
    manifest.finish_batch(run_id, "batch-1", "failed", error_message="err")
    manifest.finish_run(run_id, "failed")

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "daily_bars",
        run_id,
        "batch-0",
        pl.DataFrame([_bar_row("000001.SZ", date(2024, 6, 28))]),
    )

    result = clean_staging(cfg, force=True)
    assert run_id in result.force_removed_run_ids
    assert run_id not in list_staging_run_ids(cfg.staging_root)
    # success batch demoted so retry refetches it instead of losing rows
    statuses = {b["batch_id"]: b["status"] for b in manifest.get_batches_for_run(run_id)}
    assert statuses["batch-0"] == "failed"
    assert statuses["batch-1"] == "failed"


def test_clean_force_dry_run_keeps_manifest_untouched(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily", {})
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars")
    manifest.finish_batch(run_id, "batch-0", "success", rows_written=1)
    manifest.finish_run(run_id, "failed")

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "daily_bars",
        run_id,
        "batch-0",
        pl.DataFrame([_bar_row("000001.SZ", date(2024, 6, 28))]),
    )

    result = clean_staging(cfg, force=True, dry_run=True)
    assert run_id in result.force_removed_run_ids
    assert run_id in list_staging_run_ids(cfg.staging_root)
    statuses = {b["batch_id"]: b["status"] for b in manifest.get_batches_for_run(run_id)}
    assert statuses["batch-0"] == "success"


def test_clean_removes_orphan_staging_without_manifest(tmp_path):
    import os

    cfg = Config(data_root=tmp_path / "data")
    run_id = "orphan-run"
    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "daily_bars",
        run_id,
        "batch-0",
        pl.DataFrame([_bar_row("000001.SZ", date(2024, 6, 28))]),
    )
    run_dir = cfg.staging_root / "daily_bars" / f"run_id={run_id}"
    old = datetime.now(timezone.utc) - timedelta(days=10)
    os.utime(run_dir, (old.timestamp(), old.timestamp()))

    result = clean_staging(cfg, orphan_retention_days=7)
    assert run_id in result.orphan_run_ids
    assert run_id not in list_staging_run_ids(cfg.staging_root)


def test_clean_removes_failed_run_when_compacted_and_settled(tmp_path):
    """Failed/warning terminal runs with compact + no incomplete batches are ready."""
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("valuation_2001", {})
    manifest.start_batch(run_id, "batch-0", "valuation_metrics", "valuation_metrics")
    manifest.finish_batch(run_id, "batch-0", "success", rows_written=1)
    manifest.start_batch(run_id, "compact-0", "compact", "compact")
    manifest.finish_batch(run_id, "compact-0", "success", rows_written=1)
    # All batches success but run marked failed (e.g. orphan reconcile).
    manifest.finish_run(run_id, "failed")

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "daily_bars",
        run_id,
        "batch-0",
        pl.DataFrame([_bar_row("000001.SZ", date(2024, 6, 28))]),
    )

    assert run_ready_for_staging_cleanup(manifest, run_id) is True
    result = clean_staging(cfg)
    assert run_id in result.removed_run_ids
    assert run_id not in list_staging_run_ids(cfg.staging_root)


def test_clean_skips_warning_run_without_compact(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("backfill", {})
    manifest.start_batch(run_id, "batch-0", "margin_trading", "margin_trading")
    manifest.finish_batch(run_id, "batch-0", "success", rows_written=1)
    manifest.finish_run(run_id, "warning")

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "daily_bars",
        run_id,
        "batch-0",
        pl.DataFrame([_bar_row("000001.SZ", date(2024, 6, 28))]),
    )

    assert run_ready_for_staging_cleanup(manifest, run_id) is False
    result = clean_staging(cfg, orphan_retention_days=999)
    assert run_id in result.skipped_run_ids


def test_clean_stale_lock_files_missing_dir_is_noop(tmp_path):
    assert clean_stale_lock_files(tmp_path / "meta") == 0


def test_clean_stale_lock_files_removes_old_unheld_lock(tmp_path):
    import os

    meta_root = tmp_path / "meta"
    lock_dir = meta_root / "locks"
    lock_dir.mkdir(parents=True)
    lock_path = lock_dir / "run-old.lock"
    lock_path.write_text("")
    old = datetime.now(timezone.utc) - timedelta(days=30)
    os.utime(lock_path, (old.timestamp(), old.timestamp()))

    removed = clean_stale_lock_files(meta_root, retention_days=7)
    assert removed == 1
    assert not lock_path.exists()


def test_clean_stale_lock_files_skips_recent_lock(tmp_path):
    meta_root = tmp_path / "meta"
    lock_dir = meta_root / "locks"
    lock_dir.mkdir(parents=True)
    lock_path = lock_dir / "run-fresh.lock"
    lock_path.write_text("")

    removed = clean_stale_lock_files(meta_root, retention_days=7)
    assert removed == 0
    assert lock_path.exists()


def test_clean_stale_lock_files_skips_held_lock(tmp_path):
    import os

    meta_root = tmp_path / "meta"
    lock_dir = meta_root / "locks"
    lock_dir.mkdir(parents=True)
    lock_path = lock_dir / "run-held.lock"
    lock_path.write_text("")
    old = datetime.now(timezone.utc) - timedelta(days=30)
    os.utime(lock_path, (old.timestamp(), old.timestamp()))

    with exclusive_lock(lock_path):
        removed = clean_stale_lock_files(meta_root, retention_days=7)
    assert removed == 0
    assert lock_path.exists()


def test_clean_stale_lock_files_dry_run_keeps_file(tmp_path):
    import os

    meta_root = tmp_path / "meta"
    lock_dir = meta_root / "locks"
    lock_dir.mkdir(parents=True)
    lock_path = lock_dir / "run-old.lock"
    lock_path.write_text("")
    old = datetime.now(timezone.utc) - timedelta(days=30)
    os.utime(lock_path, (old.timestamp(), old.timestamp()))

    removed = clean_stale_lock_files(meta_root, retention_days=7, dry_run=True)
    assert removed == 1
    assert lock_path.exists()


def test_engine_run_step_records_a_compact_batch(tmp_path):
    """`cml backfill` / `cml compact` route through the engine so cleanup can fire.

    Calling step_compact directly leaves no compact batch in the manifest, and
    run_ready_for_staging_cleanup then refuses that run's staging forever.
    """
    import cn_market_lake.steps  # noqa: F401 — register steps
    from cn_market_lake.orchestrator.engine import JobEngine

    cfg = Config(data_root=tmp_path / "data", daily_waves=[])
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("backfill", {})

    result = engine.run_step("compact", date(2026, 7, 21), run_id)
    engine.manifest.finish_run(run_id, "success")

    assert result["status"] == "success"
    batches = engine.manifest.get_batches_for_run(run_id)
    assert [(b["dataset"], b["status"]) for b in batches] == [("compact", "success")]
    assert run_ready_for_staging_cleanup(engine.manifest, run_id) is True


def test_backfill_finishes_run_only_after_compact(tmp_path, monkeypatch):
    """Backfill must not mark the run terminal before compact is recorded."""
    from click.testing import CliRunner

    import cn_market_lake.steps  # noqa: F401 — register steps
    from cn_market_lake.cli.main import cli
    from cn_market_lake.orchestrator import registry
    from cn_market_lake.orchestrator.engine import JobEngine

    cfg_path = tmp_path / "cn-market-lake.toml"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[orchestrator]
workers = 1
"""
    )

    order: list[str] = []
    real_run_job = JobEngine.run_job
    real_run_step = JobEngine.run_step
    real_finish = Manifest.finish_run

    def tracking_run_job(self, *args, **kwargs):
        assert kwargs.get("finalize_run") is False
        order.append("run_job")
        out = real_run_job(self, *args, **kwargs)
        order.append(f"run_job_status={out['status']}")
        # Run must still be non-terminal until backfill finishes after compact.
        assert self.manifest.get_run(out["run_id"])["status"] == "running"
        return out

    def tracking_run_step(self, name, trade_date, run_id, context=None):
        order.append(f"run_step:{name}")
        assert self.manifest.get_run(run_id)["status"] == "running"
        return real_run_step(self, name, trade_date, run_id, context)

    def tracking_finish(self, run_id, status, **kwargs):
        order.append(f"finish_run:{status}")
        assert any(b["dataset"] == "compact" for b in self.get_batches_for_run(run_id))
        return real_finish(self, run_id, status, **kwargs)

    monkeypatch.setattr(JobEngine, "run_job", tracking_run_job)
    monkeypatch.setattr(JobEngine, "run_step", tracking_run_step)
    monkeypatch.setattr(Manifest, "finish_run", tracking_finish)

    # Avoid network: stub the registered step to a no-op success.
    step = registry.get_step("trading_calendar")

    def _noop(config, trade_date, run_id, context):
        return {"rows_read": 0, "rows_written": 0}

    monkeypatch.setattr(step, "fn", _noop)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["backfill", "trading_calendar", "--config", str(cfg_path)],
    )
    assert result.exit_code == 0, result.output
    assert order == [
        "run_job",
        "run_job_status=success",
        "run_step:compact",
        "finish_run:success",
    ]
