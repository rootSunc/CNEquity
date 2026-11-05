"""Tests for valuation_metrics orphan-symbol purge."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl

from cnequity.config import Config
from cnequity.domain.schemas import VALUATION_METRICS_SCHEMA
from cnequity.storage.repairs.valuation_orphans import purge_valuation_orphan_symbols
from cnequity.storage.revisions import RevisionStore

DAYS = [date(2024, 6, 27), date(2024, 6, 28)]


def _store(cfg: Config) -> RevisionStore:
    return RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)


def _valuation(symbol: str, day: date) -> dict:
    row = dict.fromkeys(VALUATION_METRICS_SCHEMA)
    row.update(
        symbol=symbol,
        trade_date=day,
        pe_ttm=20.0,
        source="baostock",
        data_version="v1",
        fetched_at=datetime(2024, 6, 28, tzinfo=timezone.utc),
    )
    return row


def _lake(tmp_path, valuation: dict[date, list[str]]) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    for dataset, rows in (
        (
            "daily_bars",
            {
                day: pl.DataFrame(
                    {
                        "symbol": ["600519.SH"],
                        "trade_date": [day],
                        "open": [10.0],
                        "high": [10.0],
                        "low": [10.0],
                        "close": [10.0],
                        "volume": [100.0],
                        "amount": [1000.0],
                    }
                )
                for day in DAYS
            },
        ),
        (
            "valuation_metrics",
            {
                day: pl.DataFrame(
                    [_valuation(s, day) for s in symbols], schema=VALUATION_METRICS_SCHEMA
                )
                for day, symbols in valuation.items()
            },
        ),
    ):
        for day, frame in rows.items():
            part = cfg.curated_root / dataset / f"trade_date={day}"
            part.mkdir(parents=True)
            frame.write_parquet(part / "part-0.parquet")
        _store(cfg).commit(
            dataset,
            run_id="seed",
            changed_files=sorted((cfg.curated_root / dataset).rglob("*.parquet")),
            schema_version=1,
            contract_fingerprint="contract",
        )
    return cfg


def _published_symbols(cfg: Config, revision: int | None = None) -> list[str]:
    root = _store(cfg).current_root("valuation_metrics", revision=revision)
    return sorted(pl.read_parquet(root, hive_partitioning=False)["symbol"].to_list())


def test_purge_publishes_a_revision_without_symbols_absent_from_bars(tmp_path):
    cfg = _lake(tmp_path, {DAYS[0]: ["600193.SH"], DAYS[1]: ["600519.SH", "600193.SH"]})

    summary = purge_valuation_orphan_symbols(cfg)
    assert summary["orphan_symbols"] == ["600193.SH"]
    assert summary["rows_removed"] == 2
    assert summary["partitions_rewritten"] == 2
    assert summary["revision"] == 2

    # Readers see the purge, the next compact's materialize keeps it, and the
    # previous revision still holds the removed rows.
    assert _published_symbols(cfg) == ["600519.SH"]
    _store(cfg).materialize_current("valuation_metrics")
    working = pl.read_parquet(cfg.curated_root / "valuation_metrics", hive_partitioning=False)
    assert working["symbol"].to_list() == ["600519.SH"]
    assert _published_symbols(cfg, revision=1) == ["600193.SH", "600193.SH", "600519.SH"]


def test_purge_noop_when_aligned(tmp_path):
    cfg = _lake(tmp_path, {DAYS[1]: ["600519.SH"]})

    summary = purge_valuation_orphan_symbols(cfg)
    assert summary == {
        "orphan_symbols": [],
        "partitions_rewritten": 0,
        "rows_removed": 0,
    }
    assert _store(cfg).latest("valuation_metrics").revision == 1
