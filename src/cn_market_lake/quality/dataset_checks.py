"""Curated dataset existence, integrity, and partition row-count sentinels."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from cn_market_lake.domain.datasets import (
    DATASETS,
    ROW_COUNT_MUTATION_MIN_BASELINE_ROWS,
    ROW_COUNT_MUTATION_MIN_RATIO,
)
from cn_market_lake.domain.partitions import granularity_of
from cn_market_lake.domain.schemas import MOCK_SOURCE, PRIMARY_KEYS
from cn_market_lake.query.parquet_scan import (
    dataset_has_parquet,
    lazy_mock_row_count,
    lazy_n_unique_symbol,
    lazy_row_count,
    list_partitions,
    scan_parquet_files,
    scan_parquet_root,
)

_AUDIT_SAMPLE_FILES = 20


def partition_parquet_files(root: Path, partition_col: str, partition_value: str) -> list[Path]:
    """Files in one partition directory. *partition_value* is the literal
    directory value — a day, month or year label depending on granularity."""
    part_dir = root / f"{partition_col}={partition_value}"
    if not part_dir.exists():
        return []
    return sorted(part_dir.glob("**/*.parquet"))


def partition_row_stats(files: list[Path]) -> dict[str, int | None]:
    if not files:
        return {"rows": 0, "symbols": None}
    lf = scan_parquet_files(files)
    return {
        "rows": lazy_row_count(lf),
        "symbols": lazy_n_unique_symbol(lf),
    }


def _sample_files(files: list[Path], limit: int = _AUDIT_SAMPLE_FILES) -> list[Path]:
    return files[:limit] if len(files) <= limit else files[:limit]


def _pk_duplicate_count(df: pl.DataFrame, dataset: str) -> int:
    pk = PRIMARY_KEYS.get(dataset, [])
    if not pk or not all(c in df.columns for c in pk):
        return 0
    return df.height - df.unique(subset=pk).height


def _mutation_ratio(current: int, baseline: float) -> float:
    if baseline <= 0:
        return 1.0
    return current / baseline


def period_elapsed_fraction(partition_value: str, granularity: str, as_of: date) -> float:
    """How much of *partition_value*'s period has happened by *as_of*.

    A month partition on the 8th holds eight days against a full prior month,
    so a straight period-over-period ratio reads ~26% and trips the shrink
    threshold — for every month-partitioned dataset, for most of every month.
    That is the alarm that teaches people to stop reading the audit. Scaling
    the baseline by this fraction compares like with like.

    Returns 1.0 for any period that is already over, and for day granularity,
    where a partition is whole the moment it exists.
    """
    if granularity == "day":
        return 1.0
    try:
        if granularity == "month":
            year, month = (int(p) for p in partition_value.split("-")[:2])
            start = date(year, month, 1)
            end = date(year + (month == 12), (month % 12) + 1, 1)
        elif granularity == "quarter":
            year, quarter = int(partition_value[:4]), int(partition_value[-1])
            start = date(year, 3 * (quarter - 1) + 1, 1)
            end = date(year + 1, 1, 1) if quarter == 4 else date(year, 3 * quarter + 1, 1)
        elif granularity == "year":
            year = int(partition_value[:4])
            start, end = date(year, 1, 1), date(year + 1, 1, 1)
        else:
            return 1.0
    except (ValueError, IndexError):
        return 1.0

    if as_of >= end:
        return 1.0
    if as_of < start:
        return 1.0
    total = (end - start).days
    elapsed = (as_of - start).days + 1
    return max(elapsed / total, 0.0) if total else 1.0


def check_partition_row_mutation(
    dataset: str,
    partition_col: str,
    *,
    current_value: str,
    previous_value: str,
    current_stats: dict[str, int | None],
    previous_stats: dict[str, int | None],
    elapsed_fraction: float = 1.0,
) -> dict | None:
    """Flag a partition that shrank sharply against the one before it.

    *elapsed_fraction* scales the baseline for a period still in progress —
    see :func:`period_elapsed_fraction`. Without it a month-partitioned dataset
    warns from the 1st to roughly the 20th, every month, forever.
    """
    prev_rows = int(previous_stats["rows"])
    cur_rows = int(current_stats["rows"])
    if prev_rows < ROW_COUNT_MUTATION_MIN_BASELINE_ROWS:
        return None

    fraction = min(max(elapsed_fraction, 0.0), 1.0) or 1.0
    row_baseline = prev_rows * fraction
    row_ratio = _mutation_ratio(cur_rows, row_baseline)
    row_triggered = row_ratio < ROW_COUNT_MUTATION_MIN_RATIO

    symbol_triggered = False
    symbol_ratio = None
    prev_symbols = previous_stats.get("symbols")
    cur_symbols = current_stats.get("symbols")
    if prev_symbols is not None and cur_symbols is not None:
        prev_symbols = int(prev_symbols)
        cur_symbols = int(cur_symbols)
        if prev_symbols >= ROW_COUNT_MUTATION_MIN_BASELINE_ROWS:
            # Prorated as well. Leaving this raw was the first attempt, on the
            # theory that a few days of daily snapshots already cover the whole
            # universe — true for valuation_metrics, false for every
            # event-driven dataset, where distinct names accumulate exactly like
            # rows. dragon_tiger, block_trades and sentiment_scores all kept
            # warning on the symbol ratio alone (26% / 28% / 46%) after the row
            # ratio was fixed. For a genuinely daily-snapshot dataset the
            # prorated symbol baseline is simply easy to clear, which is the
            # right outcome — the row check still covers it.
            symbol_ratio = _mutation_ratio(cur_symbols, prev_symbols * fraction)
            symbol_triggered = symbol_ratio < ROW_COUNT_MUTATION_MIN_RATIO

    if not row_triggered and not symbol_triggered:
        return None

    prorated = (
        "" if fraction >= 1.0 else f", prorated to {row_baseline:.0f} at {fraction:.0%} elapsed"
    )
    parts = [
        (
            f"partition {partition_col}={current_value} has {cur_rows} rows "
            f"vs {prev_rows} in {previous_value}{prorated} "
            f"({row_ratio:.0%} of expected)"
        )
    ]
    if symbol_ratio is not None:
        parts.append(f"symbols {cur_symbols} vs {prev_symbols} ({symbol_ratio:.0%} of prior)")
    return {
        "dataset": dataset,
        "severity": "warning",
        "check": "row_count_mutation",
        "message": "; ".join(parts),
        "partition_col": partition_col,
        "current_partition": current_value,
        "previous_partition": previous_value,
        "current_rows": cur_rows,
        "previous_rows": prev_rows,
        "row_ratio": round(row_ratio, 4),
        "current_symbols": cur_symbols,
        "previous_symbols": prev_symbols,
        "min_ratio_threshold": ROW_COUNT_MUTATION_MIN_RATIO,
    }


def audit_curated_dataset(
    dataset: str,
    partition_col: str | None,
    root: Path,
    trade_date: date,
) -> list[dict]:
    findings: list[dict] = []
    from cn_market_lake.domain.datasets import DATASETS

    required = DATASETS[dataset].required if dataset in DATASETS else True
    empty_severity = "error" if required else "warning"

    if not root.exists():
        findings.append(
            {
                "dataset": dataset,
                "severity": empty_severity,
                "check": "exists",
                "message": f"No curated data for {dataset}",
            }
        )
        return findings

    if not dataset_has_parquet(root):
        findings.append(
            {
                "dataset": dataset,
                "severity": empty_severity,
                "check": "non_empty",
                "message": f"Empty curated {dataset}",
            }
        )
        return findings

    audit_files: list[Path] | None = None
    partition_value: str | None = None
    previous_value: str | None = None
    audit_lf: pl.LazyFrame

    if partition_col is not None:
        # The audited unit is the partition holding trade_date, which under
        # month/year granularity is a period rather than the single day.
        partitions = list_partitions(root, partition_col)
        current = next((p for p in partitions if p.covers(trade_date)), None)
        if current is not None:
            partition_value = current.value
            prior = [p for p in partitions if p.start < current.start]
            previous_value = prior[-1].value if prior else None
            part_files = partition_parquet_files(root, partition_col, current.value)
            if part_files:
                audit_files = part_files
                audit_lf = scan_parquet_files(part_files)
            else:
                audit_lf = scan_parquet_root(
                    root,
                    partition_col=partition_col,
                    start=current.start,
                    end=current.end,
                )
        else:
            audit_lf = scan_parquet_root(root, partition_col=partition_col)
    else:
        audit_lf = scan_parquet_root(root, hive=False)

    sample_lf = (
        scan_parquet_files(_sample_files(audit_files))
        if audit_files is not None
        else audit_lf.limit(_AUDIT_SAMPLE_FILES)
    )
    sample_df = sample_lf.collect()
    row_count = lazy_row_count(audit_lf)
    mock_rows = lazy_mock_row_count(audit_lf, mock_source=MOCK_SOURCE)
    file_count = len(audit_files) if audit_files is not None else None

    if mock_rows:
        findings.append(
            {
                "dataset": dataset,
                "severity": "error",
                "check": "mock_source",
                "message": (
                    f"{mock_rows} fabricated rows (source={MOCK_SOURCE!r}) in curated {dataset}; "
                    "regenerate with a real source before using downstream"
                ),
            }
        )

    findings.append(
        {
            "dataset": dataset,
            "severity": "info",
            "check": "row_count",
            "message": (
                f"{row_count} rows"
                + (
                    f" in {partition_col}={partition_value}"
                    if partition_value is not None
                    else " across dataset"
                )
            ),
            "sample_columns": sample_df.columns[:10],
            "partition_col": partition_col,
            "partition_value": partition_value,
            "file_count": file_count,
        }
    )

    dupes = _pk_duplicate_count(sample_df, dataset)
    if dupes:
        findings.append(
            {
                "dataset": dataset,
                "severity": "error",
                "check": "pk_unique",
                "message": f"{dupes} duplicate PK rows in curated {dataset} sample",
            }
        )

    if dataset == "daily_bars" and "close" in sample_df.columns:
        null_close = sample_df.filter(pl.col("close").is_null()).height
        if null_close:
            findings.append(
                {
                    "dataset": dataset,
                    "severity": "warning",
                    "check": "null_close",
                    "message": f"{null_close} rows with null close in sample",
                }
            )

    if partition_col is not None and partition_value is not None and previous_value is not None:
        current_stats = partition_row_stats(
            partition_parquet_files(root, partition_col, partition_value)
        )
        previous_stats = partition_row_stats(
            partition_parquet_files(root, partition_col, previous_value)
        )
        granularity = DATASETS[dataset].partition_granularity if dataset in DATASETS else "day"
        mutation = check_partition_row_mutation(
            dataset,
            partition_col,
            current_value=partition_value,
            previous_value=previous_value,
            current_stats=current_stats,
            previous_stats=previous_stats,
            elapsed_fraction=period_elapsed_fraction(partition_value, granularity, trade_date),
        )
        if mutation is not None:
            findings.append(mutation)

    return findings


# A partition holding fewer rows than this is mostly Parquet footer: metadata
# costs ~1KB per file regardless of content, so the dataset spends its bytes and
# its file opens on overhead. Well below the smallest sensible daily partition.
PARTITION_FRAGMENTATION_MIN_ROWS = 50
# Only judge a dataset with enough partitions for the average to mean something.
PARTITION_FRAGMENTATION_MIN_PARTITIONS = 30

# Whole-dataset PK scan when mixed-granularity leftovers are present: the
# datasets that need a granularity flip are small; cap so a pathological lake
# cannot turn audit into a full-table scan of daily_bars.
_MIXED_GRANULARITY_PK_SCAN_MAX_FILES = 20_000


def check_mixed_partition_granularity(
    dataset: str,
    partition_col: str | None,
    root: Path,
) -> dict | None:
    """Error when on-disk partitions span a different period than the registry.

    Changing ``DatasetSpec.partition_granularity`` (day → year) makes new
    compact writes land in coarse directories, but the old fine directories stay
    put. Whole-layer scans then see the same primary key twice — once in
    ``trade_date=2016-01-04`` and again inside ``trade_date=2016`` — and the
    sampled ``pk_unique`` check (current period only) never notices.
    ``cml repartition`` (with PK dedupe) is the fix.
    """
    if partition_col is None or not dataset_has_parquet(root):
        return None
    spec = DATASETS.get(dataset)
    if spec is None:
        return None

    partitions = list_partitions(root, partition_col)
    if not partitions:
        return None

    configured = spec.partition_granularity
    by_gran: dict[str, list[str]] = {}
    for part in partitions:
        by_gran.setdefault(granularity_of(part), []).append(part.value)
    stale = {g: vals for g, vals in by_gran.items() if g != configured}
    if not stale:
        return None

    on_disk = sorted(by_gran)
    stale_count = sum(len(v) for v in stale.values())
    sample = []
    for vals in stale.values():
        sample.extend(vals[:5])
    sample = sample[:8]

    pk_dupes: int | None = None
    files = sorted(root.glob("**/*.parquet"))
    pk = PRIMARY_KEYS.get(dataset, [])
    if pk and len(files) <= _MIXED_GRANULARITY_PK_SCAN_MAX_FILES:
        df = scan_parquet_files(files, hive=False).select(pk).collect()
        if all(c in df.columns for c in pk):
            pk_dupes = df.height - df.unique(subset=pk).height

    msg = (
        f"{stale_count} partition(s) still at {[g for g in on_disk if g != configured]} "
        f"while registry wants {configured!r} (on disk: {on_disk}). "
        "Overlapping periods republish the same primary key across granularities; "
        f"quarantine the finer leftovers and run `cml repartition {dataset}`"
    )
    if pk_dupes:
        msg += f" — {pk_dupes} duplicate PK row(s) visible in a whole-dataset scan"

    return {
        "dataset": dataset,
        "severity": "error",
        "check": "mixed_partition_granularity",
        "message": msg,
        "configured_granularity": configured,
        "on_disk_granularities": on_disk,
        "stale_partitions": stale_count,
        "stale_sample": sample,
        "pk_duplicate_rows": pk_dupes,
    }


def check_partition_fragmentation(
    dataset: str,
    partition_col: str | None,
    root: Path,
) -> dict | None:
    """Flag a dataset partitioned far finer than its row volume justifies.

    Guards the granularity choice in the registry: a new dataset added with the
    default day partitioning, or an existing one whose volume never grew into
    it, otherwise quietly accumulates thousands of near-empty files that every
    scan has to open. ``cml repartition`` is the fix.
    """
    if partition_col is None or not dataset_has_parquet(root):
        return None
    partitions = list_partitions(root, partition_col)
    if len(partitions) < PARTITION_FRAGMENTATION_MIN_PARTITIONS:
        return None

    files = sorted(root.glob("**/*.parquet"))
    rows = lazy_row_count(scan_parquet_files(files))
    avg = rows / len(partitions)
    if avg >= PARTITION_FRAGMENTATION_MIN_ROWS:
        return None

    spec = DATASETS.get(dataset)
    granularity = spec.partition_granularity if spec else "day"
    total_bytes = sum(f.stat().st_size for f in files if f.is_file())
    return {
        "dataset": dataset,
        "severity": "warning",
        "check": "partition_fragmentation",
        "message": (
            f"{len(partitions)} partitions hold {rows} rows ({avg:.1f} per partition, "
            f"{total_bytes / 1e6:.1f}MB across {len(files)} files) — mostly parquet "
            f"metadata. Configured granularity is {granularity!r}; coarsen it in the "
            f"registry and run `cml repartition {dataset}`"
        ),
        "partitions": len(partitions),
        "rows": rows,
        "rows_per_partition": round(avg, 1),
        "files": len(files),
        "bytes": total_bytes,
        "granularity": granularity,
        "min_rows_threshold": PARTITION_FRAGMENTATION_MIN_ROWS,
    }
