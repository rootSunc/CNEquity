"""PIT vintages: a restatement must add a row, never overwrite the original."""

from datetime import date

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.domain.schemas import PRIMARY_KEYS
from cn_market_lake.query import load
from cn_market_lake.query.reader import ReaderError
from cn_market_lake.storage.parquet import StagingWriter, compact_dataset

_DATASET = "financial_statement_items"

# One fact — 000001.SZ 2024Q1 revenue — reported once, then restated downward.
_ORIGINAL = date(2024, 4, 20)
_RESTATED = date(2025, 3, 15)


def _row(announce: date, value: float, fetched: str) -> dict:
    return {
        "symbol": "000001.SZ",
        "report_period": "2024Q1",
        "statement_type": "income",
        "item_code": "revenue",
        "item_value": value,
        "announce_date": announce,
        "source": "eastmoney",
        "data_version": "v1",
        "fetched_at": fetched,
    }


def _frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows).with_columns(
        pl.col("announce_date").cast(pl.Date),
        pl.col("fetched_at").str.to_datetime(time_unit="us", time_zone="UTC"),
    )


def _write_curated(cfg: Config, rows: list[dict]) -> None:
    part = cfg.curated_root / _DATASET / "report_period=2024Q1"
    part.mkdir(parents=True, exist_ok=True)
    _frame(rows).write_parquet(part / "part-merged.parquet")


def test_announce_date_is_part_of_the_primary_key():
    assert "announce_date" in PRIMARY_KEYS[_DATASET]


def test_compact_keeps_both_vintages(tmp_path):
    """Without announce_date in the PK the restatement would erase the original."""
    cfg = Config(data_root=tmp_path / "data")
    StagingWriter(cfg.staging_root).write_batch(
        _DATASET,
        "run-1",
        "batch-0",
        _frame(
            [
                _row(_ORIGINAL, 100.0, "2024-04-20T09:00:00+00:00"),
                _row(_RESTATED, 90.0, "2025-03-15T09:00:00+00:00"),
            ]
        ),
    )

    compact_dataset(
        cfg.staging_root, cfg.curated_root, _DATASET, "run-1", partition_col="report_period"
    )

    out = pl.read_parquet(
        cfg.curated_root / _DATASET / "report_period=2024Q1" / "part-merged.parquet"
    )
    assert sorted(out["announce_date"].to_list()) == [_ORIGINAL, _RESTATED]


def test_as_of_before_restatement_returns_the_original_value(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_curated(
        cfg,
        [
            _row(_ORIGINAL, 100.0, "2024-04-20T09:00:00+00:00"),
            _row(_RESTATED, 90.0, "2025-03-15T09:00:00+00:00"),
        ],
    )

    df = load(_DATASET, as_of="2024-06-30", config=cfg)

    assert df.height == 1
    assert df["item_value"][0] == 100.0


def test_as_of_after_restatement_returns_the_revised_value_once(tmp_path):
    """Both vintages qualify on date; only the one current then may be returned."""
    cfg = Config(data_root=tmp_path / "data")
    _write_curated(
        cfg,
        [
            _row(_ORIGINAL, 100.0, "2024-04-20T09:00:00+00:00"),
            _row(_RESTATED, 90.0, "2025-03-15T09:00:00+00:00"),
        ],
    )

    df = load(_DATASET, as_of="2025-06-30", config=cfg)

    assert df.height == 1, "a restated fact must not be double-counted"
    assert df["item_value"][0] == 90.0


def test_all_vintages_exposes_the_revision_history(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_curated(
        cfg,
        [
            _row(_ORIGINAL, 100.0, "2024-04-20T09:00:00+00:00"),
            _row(_RESTATED, 90.0, "2025-03-15T09:00:00+00:00"),
        ],
    )

    df = load(_DATASET, as_of="2025-06-30", all_vintages=True, config=cfg)

    assert sorted(df["item_value"].to_list()) == [90.0, 100.0]


def test_vintages_are_collapsed_per_item_not_globally(tmp_path):
    """Two different items must both survive; only same-key vintages collapse."""
    cfg = Config(data_root=tmp_path / "data")
    net_profit = {**_row(_ORIGINAL, 12.0, "2024-04-20T09:00:00+00:00"), "item_code": "net_profit"}
    _write_curated(
        cfg,
        [
            _row(_ORIGINAL, 100.0, "2024-04-20T09:00:00+00:00"),
            _row(_RESTATED, 90.0, "2025-03-15T09:00:00+00:00"),
            net_profit,
        ],
    )

    df = load(_DATASET, as_of="2025-06-30", config=cfg)

    assert sorted(df["item_code"].to_list()) == ["net_profit", "revenue"]


def test_as_of_is_still_required(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_curated(cfg, [_row(_ORIGINAL, 100.0, "2024-04-20T09:00:00+00:00")])

    with pytest.raises(ReaderError, match="requires as_of"):
        load(_DATASET, config=cfg)
