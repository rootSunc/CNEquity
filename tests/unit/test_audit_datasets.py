from datetime import date

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.domain.datasets import PARTITION_COLS
from cn_market_lake.domain.schemas import MOCK_SOURCE
from cn_market_lake.quality.audit import run_audit
from cn_market_lake.quality.dataset_checks import (
    audit_curated_dataset,
    check_partition_row_mutation,
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
        "source": "tdx_protocol",
        "data_version": "v1",
        "fetched_at": f"{trade_date.isoformat()}T00:00:00+00:00",
    }


def _write_daily_bars_partition(cfg: Config, trade_date: date, symbols: list[str]) -> None:
    path = cfg.curated_root / "daily_bars" / f"trade_date={trade_date.isoformat()}"
    path.mkdir(parents=True)
    pl.DataFrame([_bar_row(sym, trade_date) for sym in symbols]).write_parquet(
        path / "part-merged.parquet"
    )


def test_audit_checks_all_partition_col_datasets(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    run_id = "run-all-datasets"
    trade_date = date(2024, 6, 28)

    run_audit(cfg, run_id, trade_date, {})

    import json

    payload = json.loads(
        (cfg.meta_root / "quality" / "findings" / f"{run_id}.json").read_text(encoding="utf-8")
    )
    exists_checks = {f["dataset"] for f in payload["findings"] if f.get("check") == "exists"}
    assert exists_checks == set(PARTITION_COLS.keys())
    # Optional datasets (source not yet wired) must not fail lake health alone.
    optional_exists = [
        f
        for f in payload["findings"]
        if f.get("check") == "exists" and f.get("dataset") == "economic_calendar"
    ]
    assert optional_exists and optional_exists[0]["severity"] == "warning"
    required_exists = [
        f
        for f in payload["findings"]
        if f.get("check") == "exists" and f.get("dataset") == "daily_bars"
    ]
    assert required_exists and required_exists[0]["severity"] == "error"


def test_row_count_mutation_warns_on_partial_market_drop():
    finding = check_partition_row_mutation(
        "daily_bars",
        "trade_date",
        current_value=date(2024, 6, 28),
        previous_value=date(2024, 6, 27),
        current_stats={"rows": 2400, "symbols": 2400},
        previous_stats={"rows": 5000, "symbols": 5000},
    )
    assert finding is not None
    assert finding["check"] == "row_count_mutation"
    assert finding["severity"] == "warning"
    assert finding["row_ratio"] == pytest.approx(0.48)


def test_row_count_mutation_ignores_small_baselines():
    assert (
        check_partition_row_mutation(
            "trading_calendar",
            "trade_date",
            current_value=date(2024, 6, 28),
            previous_value=date(2024, 6, 27),
            current_stats={"rows": 1, "symbols": None},
            previous_stats={"rows": 1, "symbols": None},
        )
        is None
    )


def test_audit_row_count_mutation_detects_daily_bars_drop(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    trade_date = date(2024, 6, 28)
    prev_date = date(2024, 6, 27)

    prev_symbols = [f"600{i:03d}.SH" for i in range(100)]
    cur_symbols = [f"600{i:03d}.SH" for i in range(40)]
    _write_daily_bars_partition(cfg, prev_date, prev_symbols)
    _write_daily_bars_partition(cfg, trade_date, cur_symbols)

    findings = audit_curated_dataset(
        "daily_bars",
        "trade_date",
        cfg.curated_root / "daily_bars",
        trade_date,
    )
    mutation = [f for f in findings if f.get("check") == "row_count_mutation"]
    assert len(mutation) == 1
    assert mutation[0]["current_rows"] == 40
    assert mutation[0]["previous_rows"] == 100


def test_audit_flags_mock_rows_in_trade_date_partition(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    trade_date = date(2024, 6, 28)
    path = cfg.curated_root / "daily_bars" / f"trade_date={trade_date.isoformat()}"
    path.mkdir(parents=True)
    row = _bar_row("600519.SH", trade_date)
    row["source"] = MOCK_SOURCE
    pl.DataFrame([row]).write_parquet(path / "part-0.parquet")

    findings = audit_curated_dataset(
        "daily_bars",
        "trade_date",
        cfg.curated_root / "daily_bars",
        trade_date,
    )
    mock = [f for f in findings if f.get("check") == "mock_source"]
    assert len(mock) == 1
    assert mock[0]["severity"] == "error"
