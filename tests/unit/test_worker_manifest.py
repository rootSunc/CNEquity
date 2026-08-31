import os
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import cnequity.steps  # noqa: F401
from cnequity.config import load_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.worker_pool import fetch_daily_bars_parallel
from cnequity.storage import StagingWriter
from cnequity.storage.layout import init_data_layout


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


def test_programmatic_config_stays_in_process_with_effective_tdx_settings(tmp_path, monkeypatch):
    """A Config without a TOML path must not lose its runtime-only settings."""
    import polars as pl

    from cnequity.config import Config

    cfg = Config(
        data_root=tmp_path / "data",
        workers=1,
        batch_size=1,
        tdx_daily_workers=2,
        tdx_daily_backend="process",
        tdx_servers="fixture.example:7709",
        tdx_connect_timeout_sec=37,
        tdx_enabled=False,
        source_concurrency={"tdx_protocol": 1},
    )
    init_data_layout(cfg)
    run_id = Manifest(cfg.manifest_path).start_run("programmatic")
    seen: list[object] = []

    def _fetch(symbols, start, end, **kwargs):
        seen.append(kwargs["config"])
        return pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [start] * len(symbols),
                "open": [1.0] * len(symbols),
                "high": [1.0] * len(symbols),
                "low": [1.0] * len(symbols),
                "close": [1.0] * len(symbols),
                "volume": [100] * len(symbols),
                "amount": [100.0] * len(symbols),
            }
        )

    monkeypatch.setattr("cnequity.orchestrator.worker_pool.fetch_daily_bars", _fetch)
    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.ProcessPoolExecutor",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("process pool used")),
    )

    result = fetch_daily_bars_parallel(
        cfg,
        ["600519.SH", "000001.SZ"],
        date(2024, 6, 27),
        date(2024, 6, 27),
        run_id,
        "daily_bars",
    )

    assert result["had_error"] is False
    assert result["rows_written"] == 2
    assert seen and all(item is cfg for item in seen)
    assert cfg.tdx_servers == "fixture.example:7709"
    assert cfg.tdx_connect_timeout_sec == 37
    assert cfg.tdx_enabled is False
    assert cfg.source_concurrency_for("tdx_protocol") == 1


def test_manifest_accepts_str_db_path(tmp_path):
    """Process-pool workers pass manifest_path as str across the boundary."""
    db = tmp_path / "meta" / "manifest.db"
    manifest = Manifest(str(db))
    assert manifest.db_path == db
    run_id = manifest.start_run("test")
    assert run_id


def test_request_retries_are_separate_from_batch_retry_budget(tmp_path):
    db = tmp_path / "meta" / "manifest.db"
    manifest = Manifest(db)
    run_id = manifest.start_run("daily")
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars")
    manifest.finish_batch(run_id, "batch-0", "failed", request_retry_count=2)
    assert manifest.increment_batch_retry_counts(run_id, ["batch-0"]) == 1

    # A second worker reports its cumulative request-retry observation without
    # consuming another orchestrator retry slot.
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars")
    manifest.finish_batch(run_id, "batch-0", "success", request_retry_count=5)
    row = manifest.get_batch(run_id, "batch-0")
    assert row is not None
    assert row["retry_count"] == 1
    assert row["request_retry_count"] == 5
    assert manifest.get_retry_telemetry(run_id) == {
        "orchestrator_retries": 1,
        "request_retries": 5,
        "retries": 6,
    }


def test_worker_pool_persists_adapter_request_retries(worker_config, monkeypatch):
    import polars as pl

    init_data_layout(worker_config)
    manifest = Manifest(worker_config.manifest_path)
    run_id = manifest.start_run("daily")

    def _fetch(symbols, start, end, **kwargs):
        kwargs["metrics"]["retries"] = 2
        return pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [start] * len(symbols),
                "open": [1.0] * len(symbols),
                "high": [1.0] * len(symbols),
                "low": [1.0] * len(symbols),
                "close": [1.0] * len(symbols),
                "volume": [100] * len(symbols),
                "amount": [100.0] * len(symbols),
            }
        )

    monkeypatch.setattr("cnequity.orchestrator.worker_pool.fetch_daily_bars", _fetch)
    fetch_daily_bars_parallel(
        worker_config,
        ["600519.SH"],
        date(2024, 6, 28),
        date(2024, 6, 28),
        run_id,
    )

    row = manifest.get_batches_for_run(run_id)[0]
    assert row["retry_count"] == 0
    assert row["request_retry_count"] == 2


def test_self_recording_step_gets_engine_receipt_when_implementation_omits_it(tmp_path):
    from cnequity.config import Config

    cfg = Config(data_root=tmp_path / "data")
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("daily:core")
    engine._record_step_result(
        name="derive_adj_factors",
        entry=SimpleNamespace(group="finalize"),
        run_id=run_id,
        status="success",
        out={"rows_written": 7},
    )

    receipt = engine.manifest.get_dataset_result(run_id, "adj_factors", "derive")
    assert receipt is not None
    assert receipt["status"] == "success"
    assert receipt["rows_written"] == 7


def test_self_recording_retry_does_not_reuse_failed_receipt(tmp_path):
    from cnequity.config import Config

    engine = JobEngine(Config(data_root=tmp_path / "data"))
    run_id = engine.manifest.start_run("daily:core")
    engine.manifest.record_dataset_result(
        run_id,
        "adj_factors",
        "derive",
        "failed",
        criticality="core",
        rows_written=0,
    )

    engine._record_step_result(
        name="derive_adj_factors",
        entry=SimpleNamespace(group="finalize"),
        run_id=run_id,
        status="success",
        out={"rows_written": 11},
    )

    receipt = engine.manifest.get_dataset_result(run_id, "adj_factors", "derive")
    assert receipt is not None
    assert receipt["status"] == "success"
    assert receipt["rows_written"] == 11


def test_non_worker_retry_lineage_records_one_orchestrator_requeue(tmp_path, monkeypatch):
    from cnequity.config import Config

    cfg = Config(data_root=tmp_path / "data")
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("daily:core")
    entry = SimpleNamespace(
        group="core",
        requires_workers=False,
        fn=lambda *_args, **_kwargs: {"rows_read": 1, "rows_written": 1},
    )
    monkeypatch.setattr("cnequity.orchestrator.engine.get_step", lambda _name: entry)

    result = engine._run_step(
        "fixture_dataset",
        date(2024, 6, 28),
        run_id,
        {},
        retry_of=["failed-predecessor"],
    )

    assert result["status"] == "success"
    batch = engine.manifest.get_batches_for_run(run_id)[0]
    assert batch["retry_count"] == 1
    assert engine.manifest.get_retry_telemetry(run_id)["orchestrator_retries"] == 1


def test_cninfo_failure_metrics_reach_running_batch_retry_telemetry(tmp_path):
    from cnequity.config import Config
    from cnequity.steps.events import _record_cninfo_metrics

    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("backfill")
    manifest.start_batch(run_id, "batch-0", "announcement_index", "announcement_index")

    _record_cninfo_metrics(
        cfg,
        run_id,
        "announcement_index",
        {"requests": 4, "retries": 3, "status": "failed"},
        batch_id="batch-0",
    )
    # Re-recording an aggregate for the same attempt is monotonic, not
    # additive, so telemetry callbacks cannot double-count retries.
    _record_cninfo_metrics(
        cfg,
        run_id,
        "announcement_index",
        {"requests": 4, "retries": 3, "status": "failed"},
        batch_id="batch-0",
    )

    row = manifest.get_batch(run_id, "batch-0")
    assert row["request_retry_count"] == 3


def test_dataset_results_schema_migrates_and_round_trips(tmp_path):
    """A pre-stage-4 result table gains the new columns in place."""
    db = tmp_path / "meta" / "manifest.db"
    manifest = Manifest(db)
    with manifest._connect() as conn:
        conn.execute("DROP TABLE dataset_results")
        conn.execute(
            """
            CREATE TABLE dataset_results (
                run_id TEXT NOT NULL,
                dataset TEXT NOT NULL,
                stage TEXT NOT NULL,
                status TEXT NOT NULL
            )
            """
        )

    migrated = Manifest(db)
    run_id = migrated.start_run("daily")
    migrated.record_dataset_result(
        run_id,
        "daily_bars",
        "compact",
        "success",
        criticality="core",
        revision_id="rev-1",
        rows_written=7,
    )
    row = migrated.get_dataset_result(run_id, "daily_bars", "compact")
    assert row is not None
    assert dict(row)["revision_id"] == "rev-1"
    assert dict(row)["rows_written"] == 7
    with migrated._connect() as conn:
        columns = {item[1] for item in conn.execute("PRAGMA table_info(dataset_results)")}
    assert {
        "criticality",
        "revision_id",
        "rows_written",
        "error_code",
        "error_message",
    } <= columns


def test_batch_request_retry_column_migrates_old_manifest(tmp_path):
    db = tmp_path / "meta" / "manifest.db"
    manifest = Manifest(db)
    with manifest._connect() as conn:
        conn.execute("ALTER TABLE ingestion_batches RENAME TO ingestion_batches_new")
        conn.execute(
            """
            CREATE TABLE ingestion_batches (
                run_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                dataset TEXT NOT NULL,
                status TEXT NOT NULL,
                symbols_json TEXT DEFAULT '[]',
                window_start TEXT,
                window_end TEXT,
                rows_read INTEGER DEFAULT 0,
                rows_written INTEGER DEFAULT 0,
                retry_count INTEGER DEFAULT 0,
                started_at TEXT,
                finished_at TEXT,
                error_message TEXT,
                heartbeat_at TEXT,
                blocks_compaction INTEGER DEFAULT 1,
                PRIMARY KEY (run_id, batch_id)
            )
            """
        )
        conn.execute("DROP TABLE ingestion_batches_new")

    migrated = Manifest(db)
    with migrated._connect() as conn:
        columns = {item[1] for item in conn.execute("PRAGMA table_info(ingestion_batches)")}
    assert "request_retry_count" in columns

    run_id = migrated.start_run("daily")
    migrated.start_batch(run_id, "batch-0", "daily_bars", "daily_bars")
    migrated.finish_batch(run_id, "batch-0", "success", request_retry_count=2)
    assert migrated.get_batch(run_id, "batch-0")["request_retry_count"] == 2


def test_dataset_result_aggregate_distinguishes_degraded_from_core_failure(tmp_path):
    manifest = Manifest(tmp_path / "meta" / "manifest.db")
    run_id = manifest.start_run("daily")
    manifest.record_dataset_result(run_id, "daily_bars", "compact", "success", criticality="core")
    manifest.record_dataset_result(
        run_id,
        "adj_factors",
        "derive",
        "failed",
        criticality="research",
        error_code="source_down",
    )
    assert manifest.aggregate_run_status(run_id)["status"] == "degraded"

    manifest.record_dataset_result(
        run_id,
        "daily_bars",
        "fetch",
        "failed",
        criticality="core",
        error_code="source_down",
    )
    assert manifest.aggregate_run_status(run_id)["status"] == "failed"


def test_adj_failure_keeps_committed_daily_bars_revision(worker_config, monkeypatch):
    from cnequity.adapters.tdx_protocol.client import _mock_bars, normalize_with_source
    from cnequity.storage.revisions import RevisionStore

    init_data_layout(worker_config)
    manifest = Manifest(worker_config.manifest_path)
    run_id = manifest.start_run("daily")
    frame = normalize_with_source(
        _mock_bars(["600519.SH"], date(2024, 6, 28), date(2024, 6, 28)),
        dataset="daily_bars",
    )
    StagingWriter(worker_config.staging_root).write_batch("daily_bars", run_id, "batch-0", frame)
    manifest.start_batch(
        run_id,
        "batch-0",
        "daily_bars",
        "daily_bars",
        symbols=["600519.SH"],
        window_start="2024-06-28",
        window_end="2024-06-28",
    )
    manifest.finish_batch(run_id, "batch-0", "success", rows_written=frame.height)

    engine = JobEngine(worker_config)
    compact = engine.run_step("compact", date(2024, 6, 28), run_id)
    assert compact["status"] == "success"
    revision = RevisionStore(worker_config.meta_root, worker_config.curated_root).latest(
        "daily_bars"
    )
    assert revision is not None

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.compute_adj_factors",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("adj source down")),
    )
    derive = engine.run_step("derive_adj_factors", date(2024, 6, 28), run_id)
    assert derive["status"] == "failed"
    assert engine._overall_status(run_id, "success") == "degraded"
    daily_revision = manifest.get_dataset_result(run_id, "daily_bars", "publish_revision")
    assert daily_revision is not None
    assert daily_revision["revision_id"] == revision.revision_id
    adj_result = manifest.get_dataset_result(run_id, "adj_factors", "derive")
    assert adj_result is not None
    assert adj_result["status"] == "failed"


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


def test_partial_tdx_symbol_batch_keeps_valid_rows_but_stays_failed(worker_config, monkeypatch):
    import polars as pl

    from cnequity.domain.schemas import with_provenance

    worker_config.failover_enabled = False
    worker_config.batch_size = 2
    init_data_layout(worker_config)
    manifest = Manifest(worker_config.manifest_path)
    run_id = manifest.start_run("test")

    def _partial_fetch(symbols, start, end, **kwargs):
        return with_provenance(
            pl.DataFrame(
                {
                    "symbol": [symbols[0]],
                    "trade_date": [end],
                    "open": [1.0],
                    "high": [1.0],
                    "low": [1.0],
                    "close": [1.0],
                    "volume": [100],
                    "amount": [100.0],
                }
            ),
            source="tdx_protocol",
            data_version="v2",
        )

    monkeypatch.setattr("cnequity.orchestrator.worker_pool.fetch_daily_bars", _partial_fetch)
    result = fetch_daily_bars_parallel(
        worker_config,
        ["600519.SH", "000001.SZ"],
        date(2024, 6, 27),
        date(2024, 6, 28),
        run_id,
        "daily_bars",
    )

    assert result["had_error"] is True
    assert result["failed_symbols"] == ["000001.SZ"]
    assert result["rows_written"] == 0
    batch = manifest.get_batches_for_run(run_id)[0]
    assert batch["status"] == "failed"
    files = list(worker_config.staging_root.glob("daily_bars/**/*.parquet"))
    assert len(files) == 1
    staged = pl.read_parquet(files[0])
    assert staged["symbol"].to_list() == ["600519.SH"]


def test_wrong_date_tdx_batch_is_not_staged_as_success(worker_config, monkeypatch):
    import polars as pl

    from cnequity.domain.schemas import with_provenance

    worker_config.failover_enabled = False
    init_data_layout(worker_config)
    manifest = Manifest(worker_config.manifest_path)
    run_id = manifest.start_run("test")

    def _wrong_date_fetch(symbols, start, end, **kwargs):
        return with_provenance(
            pl.DataFrame(
                {
                    "symbol": list(symbols),
                    "trade_date": [end + timedelta(days=1)] * len(symbols),
                    "open": [1.0] * len(symbols),
                    "high": [1.0] * len(symbols),
                    "low": [1.0] * len(symbols),
                    "close": [1.0] * len(symbols),
                    "volume": [100] * len(symbols),
                    "amount": [100.0] * len(symbols),
                }
            ),
            source="tdx_protocol",
            data_version="v2",
        )

    monkeypatch.setattr("cnequity.orchestrator.worker_pool.fetch_daily_bars", _wrong_date_fetch)
    result = fetch_daily_bars_parallel(
        worker_config,
        ["600519.SH", "000001.SZ"],
        date(2024, 6, 27),
        date(2024, 6, 28),
        run_id,
        "daily_bars",
    )

    assert result["had_error"] is True
    assert result["rows_written"] == 0
    batch = manifest.get_batches_for_run(run_id)[0]
    assert batch["status"] == "failed"
    assert not list(worker_config.staging_root.glob("daily_bars/**/*.parquet"))


def test_retry_reruns_failed_symbol_batch_only(worker_config, monkeypatch):
    from cnequity.adapters.tdx_protocol import client as tdx
    from cnequity.derive.adj_factors import AdjFactorsResult

    calls: list[list[str]] = []

    attempts: dict[str, int] = {}

    def _fetch(symbols, start, end, **kwargs):
        calls.append(list(symbols))
        sym = symbols[0]
        attempts[sym] = attempts.get(sym, 0) + 1
        if sym == "600519.SH" and attempts[sym] == 1:
            raise tdx.TdxSourceError("simulated failure")
        return tdx._mock_bars(symbols, start, end)

    monkeypatch.setattr("cnequity.orchestrator.worker_pool.fetch_daily_bars", _fetch)
    monkeypatch.setattr(
        "cnequity.derive.adj_factors.compute_adj_factors",
        lambda *args, **kwargs: AdjFactorsResult(0, 0, [], []),
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.load_symbols",
        lambda _cfg: ["600519.SH", "000001.SZ"],
    )
    # This test exercises retry of a failed primary batch.  Keep the backup
    # vendors disabled so the first failure remains deterministic and does not
    # depend on a live Sina/EastMoney response.
    worker_config.sources.update({"sina": False, "eastmoney": False})
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


def test_partial_batch_retry_scope_is_reduced_to_missing_symbols(worker_config, monkeypatch):
    from cnequity.adapters.tdx_protocol import client as tdx
    from cnequity.derive.adj_factors import AdjFactorsResult

    worker_config.batch_size = 2
    worker_config.failover_enabled = False
    worker_config.sources.update({"sina": False, "eastmoney": False})
    calls: list[list[str]] = []
    attempts = 0

    def _fetch(symbols, start, end, **kwargs):
        nonlocal attempts
        attempts += 1
        calls.append(list(symbols))
        if attempts == 1:
            # The second symbol proves that the first one is the only missing
            # key; retry must not repeat the whole original batch.
            return tdx._mock_bars([symbols[1]], start, end)
        return tdx._mock_bars(symbols, start, end)

    monkeypatch.setattr("cnequity.orchestrator.worker_pool.fetch_daily_bars", _fetch)
    monkeypatch.setattr("cnequity.steps.bars.load_symbols", lambda _cfg: ["600519.SH", "000001.SZ"])
    # This test targets daily-bar retry scope.  Keep finalize deterministic and
    # offline so a transient Sina factor response cannot mask that contract.
    monkeypatch.setattr(
        "cnequity.derive.adj_factors.compute_adj_factors",
        lambda *args, **kwargs: AdjFactorsResult(0, 0, [], []),
    )
    worker_config.tdx_allow_mock = False

    init_data_layout(worker_config)
    engine = JobEngine(worker_config)
    result = engine.run_job("daily", date(2024, 6, 28), steps=["daily_bars"])
    assert result["status"] == "failed"
    failed = Manifest(worker_config.manifest_path).get_failed_batches(result["run_id"])
    assert len(failed) == 1
    assert failed[0]["symbols_json"] == '["600519.SH"]'

    retry = engine.run_job(
        "daily",
        date(2024, 6, 28),
        run_id=result["run_id"],
        retry_failed_only=True,
    )
    assert retry["status"] == "success"
    assert calls == [["600519.SH", "000001.SZ"], ["600519.SH"]]
    import polars as pl

    curated = worker_config.curated_root / "daily_bars" / "trade_date=2024-06-28"
    rows = pl.concat([pl.read_parquet(path) for path in curated.glob("*.parquet")])
    assert set(rows["symbol"].to_list()) == {"600519.SH", "000001.SZ"}


def test_retry_requeues_stale_running_batch(worker_config, monkeypatch):
    from cnequity.adapters.tdx_protocol import client as tdx
    from cnequity.derive.adj_factors import AdjFactorsResult

    calls: list[list[str]] = []
    worker_config.batch_stale_seconds = 60

    def _fetch(symbols, start, end, **kwargs):
        calls.append(list(symbols))
        return tdx._mock_bars(symbols, start, end)

    monkeypatch.setattr("cnequity.orchestrator.worker_pool.fetch_daily_bars", _fetch)
    monkeypatch.setattr(
        "cnequity.derive.adj_factors.compute_adj_factors",
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
