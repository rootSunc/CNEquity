from datetime import date

import pytest

import cn_market_lake.steps  # noqa: F401 — register steps
from cn_market_lake.config import validate_config
from cn_market_lake.orchestrator.engine import JobEngine
from cn_market_lake.orchestrator.manifest import Manifest
from cn_market_lake.storage.layout import init_data_layout

pytestmark = pytest.mark.integration


def test_validate_config(config):
    assert validate_config(config) == []


def test_init_layout(config):
    init_data_layout(config)
    assert config.manifest_path.exists()
    assert config.curated_root.exists()


def test_manifest_run(config):
    init_data_layout(config)
    m = Manifest(config.manifest_path)
    run_id = m.start_run("test")
    m.start_batch(run_id, "b1", "t1", "instruments")
    m.finish_batch(run_id, "b1", "success", rows_written=10)
    m.finish_run(run_id, "success", rows_written=10)
    summary = m.run_summary(run_id)
    assert summary["batch_counts"]["success"] == 1


def test_daily_job_mock(config):
    init_data_layout(config)
    engine = JobEngine(config)
    result = engine.run_job("daily", date(2024, 6, 28))
    assert result["run_id"]
    assert result["status"] in ("success", "failed")


def test_daily_job_fails_loudly_without_allow_mock(config, monkeypatch):
    """With allow_mock off, an unreachable TDX source must fail the batch —
    never silently fall back to fabricated data."""
    from cn_market_lake.adapters.tdx_protocol import client as tdx

    def _boom(_config=None):
        raise RuntimeError("simulated TDX outage")

    monkeypatch.setattr(tdx, "_quotes_client", _boom)
    config.tdx_allow_mock = False

    init_data_layout(config)
    engine = JobEngine(config)
    result = engine.run_job("daily", date(2024, 6, 28))

    assert result["status"] == "failed"
    fetch_results = {r["step"]: r for r in result["results"] if r["step"] == "instruments"}
    assert fetch_results["instruments"]["status"] == "failed"
    # Nothing fabricated may reach staging.
    staged = list(config.staging_root.glob("instruments/**/*.parquet"))
    assert staged == []
