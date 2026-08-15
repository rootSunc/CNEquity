"""Historical-universe validity stays strict and machine-readable."""

from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.quality.historical_validity import historical_universe_validity
from cn_market_lake.quality.st_coverage import (
    build_st_scope,
    publish_st_coverage_receipt,
    write_st_checkpoint,
)


def _partition(cfg: Config, dataset: str, day: date, frame: pl.DataFrame) -> None:
    root = cfg.curated_root / dataset / f"trade_date={day.isoformat()}"
    root.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(root / "part-0.parquet")


def _lake(tmp_path) -> Config:
    cfg = Config(data_root=tmp_path / "lake")
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600519.SH"]}).write_parquet(instruments / "part-merged.parquet")
    for day in (date(2020, 1, 2), date(2024, 12, 31)):
        _partition(
            cfg,
            "daily_bars",
            day,
            pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [day]}),
        )
    _partition(
        cfg,
        "trading_status",
        date(2019, 12, 31),
        pl.DataFrame(
            {
                "symbol": ["600001.SH"],
                "trade_date": [date(2019, 12, 31)],
                "status": ["st"],
            }
        ),
    )
    scope = build_st_scope(
        ["600519.SH"],
        date(2020, 1, 2),
        date(2024, 12, 31),
        universe="all_a",
    )
    checkpoint = {
        "schema_version": 1,
        "claim": "historical_st_evidence",
        "scope": scope,
        "status": "complete",
        "completed_symbols": ["600519.SH"],
        "evidence_rows_by_symbol": {"600519.SH": 0},
        "unresolved_symbols": [],
    }
    write_st_checkpoint(cfg, checkpoint)
    publish_st_coverage_receipt(cfg, checkpoint)
    return cfg


def _survivorship(*, verified: bool) -> dict:
    return {
        "verified": verified,
        "counts": {
            "pending_probe": 0 if verified else 2,
            "missing_bars": 0,
            "unknown_overlap": 0,
            "terminal_mismatch": 0,
            "missing_instrument": 0,
            "invalid_delist_date": 0,
        },
    }


def test_manifest_is_ready_only_when_all_universe_checks_pass(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)
    monkeypatch.setattr(
        "cn_market_lake.quality.historical_validity.delisted_coverage_report",
        lambda *args, **kwargs: _survivorship(verified=True),
    )

    report = historical_universe_validity(cfg, date(2020, 1, 2), date(2024, 12, 31))

    assert report["universe_ready"] is True
    assert report["blockers"] == []
    assert all(check["passed"] for check in report["checks"].values())


def test_manifest_explains_each_blocking_boundary(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)
    monkeypatch.setattr(
        "cn_market_lake.quality.historical_validity.delisted_coverage_report",
        lambda *args, **kwargs: _survivorship(verified=False),
    )

    report = historical_universe_validity(cfg, date(2019, 1, 1), date(2024, 12, 31))

    assert report["universe_ready"] is False
    assert {blocker["code"] for blocker in report["blockers"]} == {
        "daily_bars_window_incomplete",
        "historical_st_labels_incomplete",
        "delisted_universe_unverified",
    }
    assert all(blocker["remediation"] for blocker in report["blockers"])
