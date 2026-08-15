from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.quality.audit import run_audit
from cn_market_lake.quality.st_coverage import (
    build_st_scope,
    publish_st_coverage_receipt,
    write_st_checkpoint,
)
from cn_market_lake.query.universe import coverage_start_date, trading_status_coverage_start
from cn_market_lake.steps.finalize import step_audit


def _write_status_partition(
    cfg: Config,
    trade_date: date,
    status: str = "normal",
    *,
    source: str = "eastmoney",
) -> None:
    path = cfg.curated_root / "trading_status" / f"trade_date={trade_date.isoformat()}"
    path.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [trade_date],
            "is_trading": [status != "suspended"],
            "status": [status],
            "source": [source],
            "data_version": ["v1"],
            "fetched_at": [f"{trade_date.isoformat()}T00:00:00+00:00"],
        }
    ).write_parquet(path / "part-0.parquet")


def _write_bars_partition(cfg: Config, trade_date: date) -> None:
    path = cfg.curated_root / "daily_bars" / f"trade_date={trade_date.isoformat()}"
    path.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [trade_date],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [1000],
            "amount": [10_500.0],
            "source": ["tdx_protocol"],
            "data_version": ["v1"],
            "fetched_at": [f"{trade_date.isoformat()}T00:00:00+00:00"],
        }
    ).write_parquet(path / "part-0.parquet")


def _write_st_receipt(cfg: Config, start: date, end: date) -> None:
    root = cfg.curated_root / "instruments"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["600519.SH"]}).write_parquet(root / "part-merged.parquet")
    scope = build_st_scope(["600519.SH"], start, end, universe="all_a")
    checkpoint = {
        "schema_version": 1,
        "claim": "historical_st_evidence",
        "scope": scope,
        "status": "complete",
        "completed_symbols": ["600519.SH"],
        "evidence_rows_by_symbol": {"600519.SH": 1},
        "unresolved_symbols": [],
    }
    write_st_checkpoint(cfg, checkpoint)
    publish_st_coverage_receipt(cfg, checkpoint)


def test_trading_status_coverage_start_from_partitions(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_status_partition(cfg, date(2024, 6, 27))
    _write_status_partition(cfg, date(2024, 6, 28))
    assert trading_status_coverage_start(cfg) == date(2024, 6, 27)
    assert coverage_start_date(cfg, "daily_bars") is None


def test_audit_warns_when_st_labels_lag_bars(tmp_path):
    """Suspension covered historically, but ST labels only start late → warning."""
    cfg = Config(data_root=tmp_path / "data")
    _write_bars_partition(cfg, date(2016, 1, 4))
    _write_status_partition(cfg, date(2016, 1, 4), status="suspended")  # historical suspension
    _write_status_partition(cfg, date(2024, 6, 28), status="st")  # ST only recent
    run_id = "run-ts-coverage"
    trade_date = date(2024, 6, 28)

    run_audit(cfg, run_id, trade_date, {})

    import json

    findings = json.loads(
        (cfg.meta_root / "quality" / "findings" / f"{run_id}.json").read_text(encoding="utf-8")
    )["findings"]
    coverage = [f for f in findings if f.get("check") == "trading_status_coverage_start"]
    assert len(coverage) == 1
    assert coverage[0]["severity"] == "warning"
    assert coverage[0]["coverage_start"] == "2016-01-04"
    assert coverage[0]["st_coverage_start"] == "2024-06-28"
    assert "ST evidence" in coverage[0]["message"]


def test_audit_coverage_info_when_st_and_suspension_aligned(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_bars_partition(cfg, date(2024, 6, 28))
    _write_status_partition(cfg, date(2024, 6, 28), status="st", source="baostock")
    _write_st_receipt(cfg, date(2024, 6, 28), date(2024, 6, 28))
    run_id = "run-aligned"
    trade_date = date(2024, 6, 28)

    step_audit(cfg, trade_date, run_id, {})

    import json

    payload = json.loads(
        (cfg.meta_root / "quality" / "findings" / f"{run_id}.json").read_text(encoding="utf-8")
    )
    coverage = [f for f in payload["findings"] if f.get("check") == "trading_status_coverage_start"]
    assert len(coverage) == 1
    assert coverage[0]["severity"] == "info"
    assert coverage[0]["st_evidence_verified"] is True
