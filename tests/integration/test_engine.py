from datetime import date

import polars as pl
import pytest

import cnequity.steps  # noqa: F401 — register steps
from cnequity.config import validate_config
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.manifest import Manifest
from cnequity.storage.layout import init_data_layout

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


def test_daily_job_mock(config, monkeypatch, caplog):
    from cnequity.adapters.tdx_protocol import client as tdx

    # TDX's mock gate covers its own data only. Beijing's current-session
    # snapshot and adjustment factors use separate upstream adapters, so they
    # need deterministic inputs too; a failed run is not a successful smoke test.
    config.sources["exchange"] = False
    monkeypatch.setattr(
        "cnequity.adapters.bse.instruments.fetch_bse_instruments",
        lambda trade_date, *, config: tdx._mock_instruments().filter(pl.col("exchange") == "BJ"),
    )
    monkeypatch.setattr(
        "cnequity.adapters.bse.daily_quotes.fetch_daily_quotes",
        lambda trade_date, *, symbols, config: tdx._mock_bars(
            sorted(symbols), trade_date, trade_date
        ),
    )
    monkeypatch.setattr(
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
        lambda symbol, adjust_type, *, client: pl.DataFrame(
            {"trade_date": [date(2010, 1, 1)], "factor": [1.0]}
        ),
    )
    init_data_layout(config)
    engine = JobEngine(config)
    result = engine.run_job("daily", date(2024, 6, 28))
    assert result["run_id"]
    assert result["execution_status"] == "completed", result
    # Mock provenance remains an explicit quality warning after successful
    # execution, rather than being made to look like real market evidence.
    assert result["status"] == "degraded", result
    assert all(row["status"] != "failed" for row in result["results"])

    from cnequity.query import load

    symbols = {"600519.SH", "000001.SZ", "300750.SZ", "920000.BJ"}
    # The run also fills the bars a fresh lake owes for earlier sessions.
    day = date(2024, 6, 28)
    bars = load("daily_bars", config=config, start=day, end=day)
    factors = load("adj_factors", config=config, start=day, end=day)
    assert set(bars["symbol"]) == symbols
    assert bars.height == len(symbols)
    assert set(factors["symbol"]) == symbols
    assert factors.height == len(symbols)
    assert factors["factor"].to_list() == [1.0] * len(symbols)
    assert "outbound" not in caplog.text


def test_daily_job_source_outage_finishes_without_fabricated_data(config, monkeypatch):
    """An unreachable TDX source completes with explicit gaps and fallback."""
    from cnequity.adapters.tdx_protocol import client as tdx

    def _boom(_config=None):
        raise RuntimeError("simulated TDX outage")

    monkeypatch.setattr(tdx, "_quotes_client", _boom)
    config.tdx_allow_mock = False

    init_data_layout(config)
    engine = JobEngine(config)
    result = engine.run_job("daily", date(2024, 6, 28))

    assert result["status"] == "degraded"
    assert result["execution_status"] == "completed"
    assert result["coverage_status"] == "partial"
    assert result["fallback"]
    fetch_results = {r["step"]: r for r in result["results"] if r["step"] == "instruments"}
    assert fetch_results["instruments"]["status"] == "degraded"
    # Nothing fabricated may reach staging.
    staged = list(config.staging_root.glob("instruments/**/*.parquet"))
    assert staged == []
