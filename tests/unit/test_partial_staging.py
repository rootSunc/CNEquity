import dataclasses
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain import datasets as dataset_registry
from cnequity.domain.schemas import SchemaValidationError, with_provenance
from cnequity.quality.planned_coverage import ANSWER_CHECK, partial_days
from cnequity.query import load
from cnequity.steps.common import write_simple
from cnequity.steps.finalize import step_compact
from cnequity.storage.parquet import StagingWriter


def _bars(day: date, closes: list[float]) -> pl.DataFrame:
    count = len(closes)
    frame = pl.DataFrame(
        {
            "symbol": [f"{number:06d}.SZ" for number in range(count)],
            "trade_date": [day] * count,
            "open": [10.0] * count,
            "high": [11.0] * count,
            "low": [9.0] * count,
            "close": closes,
            "volume": [100.0] * count,
            "amount": [1000.0] * count,
        }
    )
    return with_provenance(frame, source="tdx_protocol", data_version="v2")


def test_bad_row_is_quarantined_without_losing_valid_rows_or_claiming_coverage(tmp_path):
    cfg = Config(data_root=tmp_path)
    day = date(2026, 1, 5)
    result = write_simple(cfg, "partial", "daily_bars", _bars(day, [10.0] * 100 + [-1.0]))
    assert result["rows_written"] == 100
    assert result["rows_rejected"] == 1
    assert result["status"] == "degraded"
    assert result["rejected_dates"] == [day.isoformat()]
    quarantine = Path(result["quarantine"])
    assert quarantine.parent == tmp_path / "_quarantine"
    assert pl.read_parquet(quarantine / "input.parquet").height == 101
    assert pl.read_parquet(quarantine / "rejected.parquet").height == 1
    (answer,) = [
        f for f in result["context_updates"]["audit_findings"] if f["check"] == ANSWER_CHECK
    ]
    assert answer["outcome"] == "partial"
    assert answer["periods"] == [day.isoformat()]
    step_compact(cfg, day, "partial", result["context_updates"])
    assert load("daily_bars", config=cfg).height == 100
    assert partial_days(cfg, "daily_bars") == {day}
    writer = StagingWriter(cfg.staging_root)
    assert len(writer.list_run_files("daily_bars", "partial")) == 1
    assert [r["rows_rejected"] for r in writer.quality_receipts("daily_bars", "partial")] == [1]


def test_clean_batch_writes_no_quality_receipt(tmp_path):
    cfg = Config(data_root=tmp_path)
    result = write_simple(cfg, "clean", "daily_bars", _bars(date(2026, 1, 5), [10.0, 11.0]))
    assert result == {"rows_read": 2, "rows_written": 2}
    assert StagingWriter(cfg.staging_root).quality_receipts("daily_bars", "clean") == []
    assert not (tmp_path / "_quarantine").exists()


def test_all_rows_invalid_still_fails_the_batch(tmp_path):
    cfg = Config(data_root=tmp_path)
    with pytest.raises(SchemaValidationError, match="all 2 rows rejected"):
        write_simple(cfg, "bad", "daily_bars", _bars(date(2026, 1, 5), [-1.0, -2.0]))
    assert StagingWriter(cfg.staging_root).list_run_files("daily_bars", "bad") == []


def test_whole_set_datasets_keep_all_or_nothing_staging(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path)
    strict = dataclasses.replace(dataset_registry.DATASETS["daily_bars"], partial_rows=False)
    monkeypatch.setitem(dataset_registry.DATASETS, "daily_bars", strict)
    with pytest.raises(SchemaValidationError):
        write_simple(cfg, "strict", "daily_bars", _bars(date(2026, 1, 5), [10.0, -1.0]))
    assert StagingWriter(cfg.staging_root).list_run_files("daily_bars", "strict") == []
    with pytest.raises(ValueError, match="does not accept partial batches"):
        StagingWriter(cfg.staging_root).write_usable_batch(
            "daily_bars", "strict", "b", _bars(date(2026, 1, 5), [10.0])
        )


def test_snapshot_datasets_are_not_registered_for_partial_rows():
    registry = dataset_registry.DATASETS
    for name in (
        "instruments",
        "etf_profiles",
        "trading_calendar",
        "index_constituents",
        "sector_members",
        "industry_members",
        "financial_statement_items",
    ):
        assert not registry[name].partial_rows, name
