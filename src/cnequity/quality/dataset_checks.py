"""Curated dataset existence, integrity, and partition row-count sentinels."""

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path

import polars as pl

from cnequity.domain.datasets import (
    DATASETS,
    ROW_COUNT_MUTATION_MIN_BASELINE_ROWS,
    ROW_COUNT_MUTATION_MIN_RATIO,
)
from cnequity.domain.partitions import granularity_of
from cnequity.domain.schemas import (
    DATASET_SCHEMAS,
    MOCK_SOURCE,
    PRIMARY_KEYS,
    SchemaValidationError,
    required_columns_for_dataset,
)
from cnequity.query.parquet_scan import (
    dataset_has_parquet,
    lazy_mock_row_count,
    lazy_n_unique_symbol,
    lazy_row_count,
    list_partitions,
    partition_files_in_range,
    scan_parquet_files,
    scan_parquet_root,
)

_AUDIT_SAMPLE_FILES = 20


def _schema_contract_scan(
    files: list[Path], dataset: str, root: Path
) -> tuple[dict | None, list[Path], list[Path]]:
    """Validate historical files one at a time without unbounded memory use.

    The normal writer path is already strict. This is the read-only counterpart
    for audit: a legacy file may omit a nullable column added later, but it may
    not omit a PK/provenance/core-bar field or contain values that violate the
    current numeric contract. Reading one file at a time keeps ``audit --full``
    bounded by the largest Parquet file rather than the whole dataset.
    """
    invalid: list[dict[str, str]] = []
    valid: list[Path] = []
    readable: list[Path] = []
    unreadable_count = 0
    for path in files:
        try:
            # Keep readable-but-invalid files in the secondary null/PK audit;
            # only an unreadable footer should be removed from that scan.
            pl.read_parquet_schema(path)
            readable.append(path)
            _validate_parquet_file(path, dataset)
            valid.append(path)
        except SchemaValidationError as exc:
            invalid.append(
                {
                    "file": path.relative_to(root).as_posix(),
                    "message": str(exc),
                }
            )
        except (OSError, pl.exceptions.PolarsError, ValueError) as exc:
            unreadable_count += 1
            invalid.append(
                {
                    "file": path.relative_to(root).as_posix(),
                    "message": f"unreadable parquet or schema: {exc}",
                }
            )

    if not invalid:
        return None, valid, readable
    shown = invalid[:_AUDIT_SAMPLE_FILES]
    suffix = f" (+{len(invalid) - len(shown)} more)" if len(invalid) > len(shown) else ""
    return (
        {
            "dataset": dataset,
            "severity": "error",
            "check": "schema_contract",
            "message": (
                f"{len(invalid)} parquet file(s) violate the stored schema contract in "
                f"curated {dataset}{suffix}"
            ),
            "files_checked": len(files),
            "invalid_files": len(invalid),
            "unreadable_files": unreadable_count,
            "sample": shown,
        },
        valid,
        readable,
    )


def _validate_parquet_file(path: Path, dataset: str) -> None:
    """Validate one Parquet file without materializing all of its rows.

    Full-lake health used to call ``read_parquet`` here. That is reasonable for
    a small daily file, but it turns a historical tick/minute partition into a
    multi-gigabyte temporary DataFrame before the actual audit even starts.
    Read the footer for the contract and run the value checks as streaming
    scalar aggregations instead. The resulting errors intentionally use the
    same domain exception as ``validate_dataframe`` so callers keep one
    actionable schema-contract finding.
    """
    schema = DATASET_SCHEMAS.get(dataset)
    if schema is None:
        pl.read_parquet_schema(path)
        return

    actual_schema = pl.read_parquet_schema(path)
    required = required_columns_for_dataset(dataset, schema)
    missing_required = [col for col in required if col not in actual_schema]
    if missing_required:
        raise SchemaValidationError(
            f"dataset '{dataset}': missing required columns {missing_required}"
        )

    columns = [col for col in schema if col in actual_schema]
    casts = []
    for col in columns:
        dtype = schema[col]
        if isinstance(dtype, pl.Datetime) and actual_schema[col] == pl.Utf8:
            casts.append(
                pl.col(col)
                .str.to_datetime(time_unit=dtype.time_unit, time_zone=dtype.time_zone, strict=False)
                .alias(col)
            )
        elif dtype == pl.Date and actual_schema[col] == pl.Utf8:
            casts.append(pl.col(col).str.to_date(strict=False).alias(col))
        else:
            casts.append(pl.col(col).cast(dtype, strict=False))

    normalized = pl.scan_parquet(path).select(casts)
    expressions = [pl.len().alias("_rows")]
    expressions.extend(pl.col(col).null_count().alias(f"_null_{col}") for col in required)

    string_required = [col for col in required if schema[col] == pl.Utf8]
    expressions.extend(
        pl.col(col).str.strip_chars().eq("").sum().alias(f"_blank_{col}") for col in string_required
    )

    float_columns = [col for col in columns if schema[col] in (pl.Float32, pl.Float64)]
    expressions.extend(
        (pl.col(col).is_not_null() & ~pl.col(col).is_finite()).sum().alias(f"_finite_{col}")
        for col in float_columns
    )

    bar_datasets = {
        "daily_bars",
        "index_bars",
        "minute_bars",
        "minute_bars_5m",
        "commodity_bars",
        "sector_bars",
        "trade_ticks",
    }
    price_columns = [col for col in ("open", "high", "low", "close", "price") if col in columns]
    semantic_checks = [pl.col(col) <= 0 for col in price_columns]
    if "volume" in columns:
        semantic_checks.append(pl.col("volume") < 0)
    if "amount" in columns:
        semantic_checks.append(pl.col("amount").is_not_null() & (pl.col("amount") < 0))
    if all(col in columns for col in ("open", "high", "low", "close")):
        ohlc_row = (
            pl.col("volume").is_null() | (pl.col("volume") > 0)
            if "volume" in columns
            else pl.lit(True)
        )
        semantic_checks.extend(
            [
                ohlc_row & (pl.col("high") < pl.col("open")),
                ohlc_row & (pl.col("high") < pl.col("close")),
                ohlc_row & (pl.col("low") > pl.col("open")),
                ohlc_row & (pl.col("low") > pl.col("close")),
                ohlc_row & (pl.col("low") > pl.col("high")),
            ]
        )
    if dataset in bar_datasets and semantic_checks:
        expressions.append(pl.any_horizontal(semantic_checks).sum().alias("_semantic_bad"))

    try:
        summary = normalized.select(expressions).collect(engine="streaming").row(0, named=True)
    except pl.exceptions.PolarsError as exc:
        raise SchemaValidationError(
            f"dataset '{dataset}': values cannot be cast to the registered schema: {exc}"
        ) from exc

    missing_values = {
        col: int(summary[f"_null_{col}"] or 0) for col in required if summary[f"_null_{col}"]
    }
    if missing_values:
        detail = ", ".join(f"{col}={count}" for col, count in missing_values.items())
        raise SchemaValidationError(
            f"dataset '{dataset}': required columns contain null or unparseable values: {detail}"
        )

    blank_values = {
        col: int(summary[f"_blank_{col}"] or 0)
        for col in string_required
        if summary[f"_blank_{col}"]
    }
    if blank_values:
        detail = ", ".join(f"{col}={count}" for col, count in blank_values.items())
        raise SchemaValidationError(
            f"dataset '{dataset}': required string columns cannot be blank: {detail}"
        )

    invalid_finite = {
        col: int(summary[f"_finite_{col}"] or 0)
        for col in float_columns
        if summary[f"_finite_{col}"]
    }
    if invalid_finite:
        detail = ", ".join(f"{col}={count}" for col, count in invalid_finite.items())
        raise SchemaValidationError(
            f"dataset '{dataset}': non-finite numeric values are not allowed: {detail}"
        )

    semantic_bad = int(summary.get("_semantic_bad") or 0)
    if semantic_bad:
        raise SchemaValidationError(
            f"dataset '{dataset}': {semantic_bad} row(s) violate numeric market-data invariants"
        )


def _schema_contract_findings(files: list[Path], dataset: str, root: Path) -> dict | None:
    """Return the schema finding while keeping the scan helper testable."""
    finding, _, _ = _schema_contract_scan(files, dataset, root)
    return finding


def _unreadable_parquet_finding(files: list[Path], dataset: str, root: Path) -> dict | None:
    """Check Parquet footers without validating every row in a normal audit."""
    unreadable: list[dict[str, str]] = []
    for path in files:
        try:
            pl.read_parquet_schema(path)
        except (OSError, pl.exceptions.PolarsError, ValueError) as exc:
            unreadable.append(
                {
                    "file": path.relative_to(root).as_posix(),
                    "message": f"unreadable parquet or schema: {exc}",
                }
            )
    if not unreadable:
        return None
    shown = unreadable[:_AUDIT_SAMPLE_FILES]
    suffix = f" (+{len(unreadable) - len(shown)} more)" if len(unreadable) > len(shown) else ""
    return {
        "dataset": dataset,
        "severity": "error",
        "check": "schema_contract",
        "message": (
            f"{len(unreadable)} parquet file(s) are unreadable in curated {dataset}{suffix}"
        ),
        "files_checked": len(files),
        "invalid_files": len(unreadable),
        "unreadable_files": len(unreadable),
        "sample": shown,
    }


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


def _lazy_pk_duplicate_count(lf: pl.LazyFrame, dataset: str) -> int:
    """Count duplicate PK rows across the whole audited partition."""
    pk = PRIMARY_KEYS.get(dataset, [])
    columns = set(lf.collect_schema().names())
    if not pk or not set(pk).issubset(columns):
        return 0
    result = (
        lf.select(pk)
        .group_by(pk)
        .agg(pl.len().alias("_pk_rows"))
        .filter(pl.col("_pk_rows") > 1)
        .select((pl.col("_pk_rows") - 1).sum().fill_null(0).alias("duplicate_rows"))
        .collect(engine="streaming")
    )
    return int(result["duplicate_rows"][0] or 0)


def _partitioned_pk_duplicate_count(
    files: list[Path], dataset: str, partition_col: str | None, root: Path
) -> int:
    """Count PK duplicates one on-disk partition at a time.

    A whole-lake ``group_by(PK)`` is needlessly expensive for high-volume
    intraday datasets: the partition column is already part of their PK, and
    a correctly laid out file cannot duplicate a row in another date
    partition. Keep the same exact check for files sharing one partition while
    bounding memory by the largest partition. DuckDB handles each partition
    with a bounded connection and can spill to a temporary directory; a
    Polars hash group over 600M ticks otherwise needs several gigabytes.
    Mixed or misplaced layouts are reported separately by
    ``check_mixed_partition_granularity`` and the writer's partition contract.
    """
    if not files:
        return 0
    prefix = f"{partition_col}="
    groups: dict[str, list[Path]] = {}
    for path in files:
        try:
            relative = path.relative_to(root)
        except ValueError:
            relative = path
        partition = next(
            (part for part in relative.parts if part.startswith(prefix)),
            "__root__",
        )
        groups.setdefault(partition, []).append(path)

    pk = PRIMARY_KEYS.get(dataset, [])
    if not pk:
        return 0

    import duckdb

    identifiers = ", ".join(f'"{column.replace(chr(34), chr(34) * 2)}"' for column in pk)
    duplicate_count = 0
    with tempfile.TemporaryDirectory(prefix="cnequity-pk-") as scratch:
        con = duckdb.connect(":memory:")
        try:
            con.execute("SET memory_limit='512MB'")
            con.execute("SET threads=1")
            con.execute("SET preserve_insertion_order=false")
            con.execute("SET temp_directory=?", [scratch])
            for group in groups.values():
                query = (
                    f"SELECT COUNT(*) - COUNT(DISTINCT ({identifiers})) "
                    "FROM read_parquet(?, hive_partitioning=false)"
                )
                duplicate_count += int(
                    con.execute(query, [[str(path) for path in group]]).fetchone()[0]
                )
        finally:
            con.close()
    return duplicate_count


def _full_scalar_stats(files: list[Path], dataset: str) -> tuple[int, int, dict[str, int]]:
    """Return row/mock/null totals with one bounded scalar Parquet query."""
    if not files:
        return 0, 0, {}

    import duckdb

    schema = DATASET_SCHEMAS.get(dataset, {})
    required = required_columns_for_dataset(dataset, schema)
    identifiers = {column: f'"{column.replace(chr(34), chr(34) * 2)}"' for column in required}
    expressions = ["COUNT(*) AS _rows"]
    if "source" in identifiers:
        expressions.append(f"SUM(CASE WHEN {identifiers['source']} = ? THEN 1 ELSE 0 END) AS _mock")
    else:
        expressions.append("0 AS _mock")
    expressions.extend(
        f'SUM(CASE WHEN {identifiers[column]} IS NULL THEN 1 ELSE 0 END) AS "_null_{column}"'
        for column in required
    )
    query = (
        "SELECT "
        + ", ".join(expressions)
        + " FROM read_parquet(?, union_by_name=true, hive_partitioning=false)"
    )

    con = duckdb.connect(":memory:")
    try:
        con.execute("SET memory_limit='512MB'")
        con.execute("SET threads=1")
        row = con.execute(query, [MOCK_SOURCE, [str(path) for path in files]]).fetchone()
    finally:
        con.close()

    if row is None:
        return 0, 0, {}
    row_count = int(row[0] or 0)
    mock_rows = int(row[1] or 0)
    nulls = {
        column: int(row[index + 2] or 0) for index, column in enumerate(required) if row[index + 2]
    }
    return row_count, mock_rows, nulls


def _required_null_counts(lf: pl.LazyFrame, dataset: str) -> dict[str, int]:
    schema = DATASET_SCHEMAS.get(dataset)
    if schema is None:
        return {}
    columns = set(lf.collect_schema().names())
    required = [col for col in required_columns_for_dataset(dataset, schema) if col in columns]
    if not required:
        return {}
    row = (
        lf.select([pl.col(col).null_count().alias(col) for col in required])
        .collect(engine="streaming")
        .row(0, named=True)
    )
    return {col: int(count) for col, count in row.items() if count}


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
    *,
    full: bool = False,
    stale: bool = False,
) -> list[dict]:
    """Audit the current partition, or every historical file when ``full``.

    ``stale`` says the dataset is already known to be behind. A partition that
    shrank because nothing has been fetched into it is not the defect
    ``row_count_mutation`` exists to catch, and reporting it as one made nine
    of sixteen audit warnings restatements of a staleness the freshness report
    had already named.

    Per-run audits stay bounded to the partition touched today. The explicit
    full-lake health path opts into a file-by-file historical schema scan and
    whole-dataset PK/null checks so old corruption cannot remain invisible.
    """
    findings: list[dict] = []
    from cnequity.domain.datasets import DATASETS

    spec = DATASETS.get(dataset)
    required = spec.required if spec is not None else True
    empty_severity = (
        spec.empty_severity
        if spec is not None and spec.empty_severity is not None
        else ("error" if required else "warning")
    )

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

    if full:
        audit_files = sorted(root.rglob("*.parquet"))
        # Historical files can straddle a nullable-column schema evolution.
        # The per-file contract scan below still validates each file; the
        # aggregate lazy checks only need a stable union for PK/null counts.
        audit_lf = scan_parquet_files(
            audit_files,
            missing_columns="insert",
            extra_columns="ignore",
        )
    elif partition_col is not None:
        # The audited unit is the partition holding trade_date, which under
        # month/year granularity is a period rather than the single day.
        partitions = list_partitions(root, partition_col)
        # No partition covers the audited day when a dataset is behind or its
        # capture is switched off. Falling back to the whole dataset made the
        # *bounded* daily audit the expensive one: `trade_ticks` stopped in
        # August, so every daily run counted rows across all 6 GB of it — 18
        # minutes for a dataset nobody is ingesting. Audit its newest
        # partition instead; the weekly `--full` sweep is what reads
        # everything.
        current = next((p for p in partitions if p.covers(trade_date)), None) or (
            partitions[-1] if partitions else None
        )
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

    contract_files = audit_files
    if contract_files is None:
        # A coarse partition can be selected through the date-aware scanner
        # without producing an explicit file list above. Reuse its exact
        # window for the schema scan; do not silently widen a normal audit to
        # the whole dataset.
        if partition_col is not None and partition_value is not None:
            matching = next(
                (p for p in list_partitions(root, partition_col) if p.value == partition_value),
                None,
            )
            contract_files = (
                partition_files_in_range(
                    root,
                    partition_col,
                    start=matching.start,
                    end=matching.end,
                )
                if matching is not None
                else []
            )
        else:
            contract_files = sorted(root.rglob("*.parquet"))
    contract, valid_contract_files, readable_contract_files = _schema_contract_scan(
        contract_files, dataset, root
    )
    if contract is not None:
        findings.append(contract)

        # A current partition can contain an unreadable file too. Keep the
        # structural finding, but run the remaining row/PK/null checks only on
        # files that the contract scan proved readable. Otherwise a quality
        # report turns the very corruption it is meant to surface into an
        # uncaught collect() exception.
        audit_files = valid_contract_files if full else readable_contract_files
        audit_lf = scan_parquet_files(
            audit_files,
            missing_columns="insert" if full else "raise",
            extra_columns="ignore" if full else "raise",
        )

    if not full:
        # A normal audit validates only the active partition, but the quality
        # checks below can scan the historical window. Inspect the other
        # footers first so one old corrupt file cannot abort those checks.
        current_files = set(contract_files)
        historical_unreadable = _unreadable_parquet_finding(
            [path for path in sorted(root.rglob("*.parquet")) if path not in current_files],
            dataset,
            root,
        )
        if historical_unreadable is not None:
            findings.append(historical_unreadable)

    if full:
        # Do not let one unreadable historical file abort the entire audit.
        # It remains an error finding, while valid files still contribute to
        # row/PK/null aggregates.
        audit_files = valid_contract_files
        audit_lf = scan_parquet_files(
            audit_files,
            missing_columns="insert",
            extra_columns="ignore",
        )

    # Sample rows, not sample files: a single intraday Parquet file can hold
    # millions of rows, so collecting the first twenty files would defeat the
    # bounded-memory contract of the full audit.
    sample_df = audit_lf.limit(_AUDIT_SAMPLE_FILES).collect(engine="streaming")
    if full and audit_files is not None:
        row_count, mock_rows, nulls = _full_scalar_stats(audit_files, dataset)
    else:
        row_count = lazy_row_count(audit_lf)
        mock_rows = lazy_mock_row_count(audit_lf, mock_source=MOCK_SOURCE)
        nulls = None
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

    if full and audit_files is not None:
        dupes = _partitioned_pk_duplicate_count(audit_files, dataset, partition_col, root)
    else:
        dupes = _lazy_pk_duplicate_count(audit_lf, dataset)
    if dupes:
        findings.append(
            {
                "dataset": dataset,
                "severity": "error",
                "check": "pk_unique",
                "message": (f"{dupes} duplicate PK rows in audited curated {dataset} partition"),
                "rows_checked": row_count,
            }
        )

    if nulls is None:
        nulls = _required_null_counts(audit_lf, dataset)
    if nulls:
        detail = ", ".join(f"{col}={count}" for col, count in nulls.items())
        findings.append(
            {
                "dataset": dataset,
                "severity": "error",
                "check": "required_non_null",
                "message": f"Required fields contain nulls in curated {dataset}: {detail}",
                "null_counts": nulls,
                "rows_checked": row_count,
            }
        )

    if dataset == "daily_bars" and "close" in sample_df.columns:
        null_close = sample_df.filter(pl.col("close").is_null()).height
        if null_close and not nulls:
            findings.append(
                {
                    "dataset": dataset,
                    "severity": "warning",
                    "check": "null_close",
                    "message": f"{null_close} rows with null close in sample",
                }
            )

    if partition_col is not None and partition_value is not None and previous_value is not None:
        try:
            current_stats = partition_row_stats(
                partition_parquet_files(root, partition_col, partition_value)
            )
            previous_stats = partition_row_stats(
                partition_parquet_files(root, partition_col, previous_value)
            )
        except (OSError, pl.exceptions.PolarsError, ValueError):
            # The contract finding above is the actionable result for a bad
            # file. Do not let the secondary period-over-period sentinel hide
            # it by aborting the whole audit.
            current_stats = previous_stats = None
        if current_stats is not None and previous_stats is not None:
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
                if stale:
                    # Keep the observation, drop the accusation: the rows are
                    # missing because the feed is behind, which `cne status
                    # --datasets` already reports with the schedule group that
                    # owns it.
                    mutation["severity"] = "info"
                    mutation["stale_dataset"] = True
                    mutation["message"] += " — dataset is stale; see `cne status --datasets`"
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
    ``scripts/repartition.py`` (with PK dedupe) is the fix.
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
        # Keep the whole-dataset overlap check lazy as well. Mixed layouts are
        # exactly the case where the old eager collect could pull a large
        # legacy lake into memory before the audit reported the layout error.
        pk_dupes = _lazy_pk_duplicate_count(
            scan_parquet_files(files, hive=False).select(pk),
            dataset,
        )

    msg = (
        f"{stale_count} partition(s) still at {[g for g in on_disk if g != configured]} "
        f"while registry wants {configured!r} (on disk: {on_disk}). "
        "Overlapping periods republish the same primary key across granularities; "
        f"quarantine the finer leftovers and run `python scripts/repartition.py {dataset}`"
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
    scan has to open. ``scripts/repartition.py`` is the fix.
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
            f"registry and run `python scripts/repartition.py {dataset}`"
        ),
        "partitions": len(partitions),
        "rows": rows,
        "rows_per_partition": round(avg, 1),
        "files": len(files),
        "bytes": total_bytes,
        "granularity": granularity,
        "min_rows_threshold": PARTITION_FRAGMENTATION_MIN_ROWS,
    }
