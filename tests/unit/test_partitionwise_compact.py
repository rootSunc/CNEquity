"""Partition-wise compact writes exactly what the whole-run merge writes."""

from __future__ import annotations

import random
import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from cnequity.domain.datasets import PARTITION_COLS
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.storage import StagingWriter
from cnequity.storage import parquet as parquet_module
from cnequity.storage.parquet import _partitionwise_eligible, compact_dataset

# Spans months, a quarter and a year boundary so every granularity splits.
DAYS = [date(2024, 1, 2) + timedelta(days=37 * i) for i in range(6)] + [
    date(2025, 3, 3),
    date(2025, 3, 4),
]
SYMBOLS = ["600519.SH", "000001.SZ", "300750.SZ"]
CHOICES = {
    "report_period": ["2024Q1", "2024Q2", "2024Q3"],
    "statement_type": ["income", "balance"],
    "item_code": ["revenue", "net_profit"],
    "index_symbol": ["000300.SH", "000905.SH"],
    # Official and vendor books, primary and backup sources, an unranked one.
    "source": ["eastmoney", "exchange", "tdx_protocol", "sina"],
    "action_type": ["dividend"],
}
PRICES = {"open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5}
# A non-split corporate action carries the neutral unit factor.
FIXED = {"split_factor": 1.0}
ALWAYS_NULL = {"total_mv_basis", "float_mv_basis", "reference_price"}

DATASETS_UNDER_TEST = [
    "daily_bars",  # day; pre_close carry, zero-quantity evidence
    "fund_flow",  # day; plain recency
    "valuation_metrics",  # day; zero placeholders nulled before the merge
    "dragon_tiger",  # month
    "index_constituents",  # month; official book replaces vendor book
    "financial_statement_items",  # quarter; PIT vintages
    "index_bars",  # year
    "corporate_actions",  # year; payment evidence carried within a key
]


def _rows(dataset: str, n: int, rng: random.Random) -> pl.DataFrame:
    schema = DATASET_SCHEMAS[dataset]
    out = []
    for _ in range(n):
        row = {}
        for col, dtype in schema.items():
            if col in ALWAYS_NULL:
                row[col] = None
            elif col in FIXED:
                row[col] = FIXED[col]
            elif col in CHOICES:
                row[col] = rng.choice(CHOICES[col])
            elif col == "symbol":
                row[col] = rng.choice(SYMBOLS)
            elif col in PRICES:
                row[col] = PRICES[col]
            elif col == "pre_close":
                row[col] = 10.2 if rng.random() > 0.4 else None
            elif dtype == pl.Date:
                row[col] = rng.choice(DAYS)
            elif isinstance(dtype, pl.Datetime):
                # Few distinct stamps, so provenance ties really happen.
                row[col] = datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(
                    minutes=rng.randint(0, 4)
                )
            elif dtype.is_float():
                row[col] = 0.0 if rng.random() < 0.15 else round(rng.uniform(0.1, 5.0), 3)
            elif dtype.is_integer():
                row[col] = 0 if rng.random() < 0.15 else rng.randint(1, 1000)
            elif dtype == pl.Boolean:
                row[col] = rng.random() > 0.5
            else:
                row[col] = rng.choice(["a", "b"])
        out.append(row)
    return pl.DataFrame(out, schema=schema)


def _tree(root: Path) -> dict[str, pl.DataFrame]:
    return {
        path.relative_to(root).as_posix(): pl.read_parquet(path)
        for path in sorted(root.rglob("*.parquet"))
    }


def _compact(tmp: Path, dataset: str, seed: int, *, partitionwise: bool):
    """Commit a base run, then compact a second, overlapping run."""
    rng = random.Random(seed)
    lake = tmp / ("pw" if partitionwise else "whole")
    staging, curated = lake / "staging", lake / "curated"
    writer = StagingWriter(staging)
    writer.write_batch(dataset, "base", "b0", _rows(dataset, 60, rng))
    compact_dataset(
        staging,
        curated,
        dataset,
        "base",
        partition_col=PARTITION_COLS[dataset],
        partitionwise_min_bytes=None,
    )
    for index in range(4):
        writer.write_batch(dataset, "run", f"b{index}", _rows(dataset, 50, rng))
    changed: list[Path] = []
    log: list[dict] = []
    rows = compact_dataset(
        staging,
        curated,
        dataset,
        "run",
        partition_col=PARTITION_COLS[dataset],
        changed_files=changed,
        change_log=log,
        partitionwise_min_bytes=0 if partitionwise else None,
    )
    root = curated / dataset
    return (
        rows,
        sorted(Path(p).resolve().relative_to(root.resolve()).as_posix() for p in changed),
        sorted(log, key=lambda entry: str(entry["partition"])),
        _tree(root),
    )


@pytest.mark.parametrize("dataset", DATASETS_UNDER_TEST)
@pytest.mark.parametrize("seed", [1, 2, 3])
def test_partitionwise_writes_the_same_lake(tmp_path, dataset, seed):
    assert _partitionwise_eligible(dataset, PARTITION_COLS[dataset])
    whole = _compact(tmp_path, dataset, seed, partitionwise=False)
    pw = _compact(tmp_path, dataset, seed, partitionwise=True)
    assert pw[0] == whole[0], "rows written"
    assert pw[1] == whole[1], "changed files"
    assert pw[2] == whole[2], "change log (partition order may differ)"
    assert pw[3].keys() == whole[3].keys()
    assert len(pw[3]) > 1, "fixture must span several partitions"
    for relative, frame in whole[3].items():
        assert_frame_equal(pw[3][relative], frame, check_exact=True), relative


def test_a_column_only_some_files_carry_reaches_every_partition(tmp_path):
    """Staged files may disagree on optional columns.

    PIT storage columns survive validation only when a file already carries
    them, so one run can stage files with and without them. The whole-run
    concat gives every row the union of columns; a partition whose files all
    lack one must still get it, with the same dtype and position.
    """
    from cnequity.domain.pit import normalize_pit_storage_columns

    dataset = "financial_statement_items"
    outputs = {}
    for partitionwise in (False, True):
        rng = random.Random(11)
        lake = tmp_path / ("pw" if partitionwise else "whole")
        staged = []
        for index in range(3):
            frame = _rows(dataset, 40, rng)
            if index == 0:
                # Only this file carries the PIT columns, and only one period.
                frame = normalize_pit_storage_columns(
                    frame.filter(pl.col("report_period") == "2024Q1"), dataset
                )
            path = lake / "in" / f"{index}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            frame.write_parquet(path)
            staged.append(path)
        compact_dataset(
            lake / "staging",
            lake / "curated",
            dataset,
            "run",
            partition_col=PARTITION_COLS[dataset],
            staging_files=staged,
            partitionwise_min_bytes=0 if partitionwise else None,
        )
        outputs[partitionwise] = _tree(lake / "curated" / dataset)
    assert outputs[True].keys() == outputs[False].keys()
    for relative, frame in outputs[False].items():
        assert_frame_equal(outputs[True][relative], frame, check_exact=True), relative


def test_the_spill_directory_is_removed(tmp_path):
    _compact(tmp_path, "fund_flow", 7, partitionwise=True)
    leftovers = [p for p in (tmp_path / "pw" / "staging").iterdir() if p.name.startswith(".")]
    assert leftovers == []


@pytest.mark.parametrize(
    "dataset",
    ["announcement_index", "news_headlines", "regulatory_events", "trading_status"],
)
def test_datasets_needing_the_whole_run_are_not_eligible(dataset):
    assert not _partitionwise_eligible(dataset, PARTITION_COLS[dataset])


def test_small_runs_keep_the_whole_run_merge(tmp_path, monkeypatch):
    called = []
    monkeypatch.setattr(
        parquet_module, "_compact_partitionwise", lambda *a, **k: called.append(1) or 0
    )
    _compact(tmp_path, "fund_flow", 1, partitionwise=False)
    staging = tmp_path / "whole" / "staging"
    StagingWriter(staging).write_batch(
        "fund_flow", "r2", "b0", _rows("fund_flow", 5, random.Random(9))
    )
    compact_dataset(
        staging, tmp_path / "whole" / "curated", "fund_flow", "r2", partition_col="trade_date"
    )
    assert called == [], "a few KB of staging is far below STREAMING_COMPACT_MIN_BYTES"
    assert parquet_module.STREAMING_COMPACT_MIN_BYTES >= 64 * 1024 * 1024
    shutil.rmtree(tmp_path / "whole")
