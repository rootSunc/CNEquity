"""Compile CNEquity's dataset registry into an :class:`equitylake_core.spec.Registry`.

Pure domain layer: reads only :mod:`cnequity.domain.datasets` and
:mod:`cnequity.domain.schemas`, producing market-neutral
:class:`equitylake_core.spec.DatasetSpec` and :class:`equitylake_core.spec.Registry`
objects consumed by :mod:`equitylake_core.read` and :mod:`equitylake_core.lake`.
"""

from __future__ import annotations

from collections.abc import Mapping
from functools import lru_cache

import polars as pl

from cnequity.domain.datasets import DATASETS
from cnequity.domain.schemas import DATASET_SCHEMAS, PRIMARY_KEYS
from equitylake_core.coverage import Coverage
from equitylake_core.spec import DatasetSpec as CoreDatasetSpec
from equitylake_core.spec import Partition, Registry


def polars_dtype_to_sql(dtype: pl.DataType | type[pl.DataType]) -> str:
    """Map a Polars schema dtype to the DuckDB SQL type declared in a Core spec."""
    resolved = dtype() if isinstance(dtype, type) and issubclass(dtype, pl.DataType) else dtype
    if resolved == pl.Date:
        return "DATE"
    if isinstance(resolved, pl.Datetime):
        return "TIMESTAMPTZ" if resolved.time_zone else "TIMESTAMP"
    if resolved in (pl.Utf8, pl.String):
        return "VARCHAR"
    if resolved == pl.Boolean:
        return "BOOLEAN"
    if resolved in (pl.Int64, pl.UInt64):
        return "BIGINT"
    if resolved in (pl.Int32, pl.UInt32):
        return "INTEGER"
    if resolved in (pl.Int16, pl.UInt16):
        return "SMALLINT"
    if resolved in (pl.Int8, pl.UInt8):
        return "TINYINT"
    if resolved == pl.Float64:
        return "DOUBLE"
    if resolved == pl.Float32:
        return "FLOAT"
    if isinstance(resolved, pl.Decimal):
        precision = resolved.precision or 38
        scale = resolved.scale or 0
        return f"DECIMAL({precision}, {scale})"
    return "VARCHAR"


def _partition_for(dataset: str, time_col: str | None) -> Partition:
    spec = DATASETS[dataset]
    if spec.partition_col is None or time_col is None:
        return "none"
    if spec.partition_col != time_col:
        return "none"
    gran = spec.partition_granularity
    if gran in ("year", "quarter", "month", "day"):
        return gran  # type: ignore[return-value]
    return "none"


def core_spec(
    dataset: str,
    *,
    columns: Mapping[str, str] | None = None,
    coverage: Coverage | None = None,
) -> CoreDatasetSpec:
    """Build an :class:`equitylake_core.spec.DatasetSpec` for CNEquity *dataset*."""
    if columns is None and coverage is None:
        return _default_core_spec(dataset)
    return _build_core_spec(dataset, columns=columns, coverage=coverage)


@lru_cache(maxsize=256)
def _default_core_spec(dataset: str) -> CoreDatasetSpec:
    return _build_core_spec(dataset, columns=None, coverage=None)


def _build_core_spec(
    dataset: str,
    *,
    columns: Mapping[str, str] | None = None,
    coverage: Coverage | None = None,
) -> CoreDatasetSpec:
    spec = DATASETS[dataset]
    if columns is not None:
        sql_columns = dict(columns)
    else:
        schema = DATASET_SCHEMAS[dataset]
        sql_columns = {col: polars_dtype_to_sql(dtype) for col, dtype in schema.items()}
    key = tuple(col for col in PRIMARY_KEYS.get(dataset, ()) if col in sql_columns)
    if not key:
        key = (next(iter(sql_columns)),)
    time_col = spec.query_date_col if spec.query_date_col in sql_columns else None
    if time_col is not None and sql_columns[time_col] not in (
        "DATE",
        "TIMESTAMP",
        "TIMESTAMPTZ",
        "TIMESTAMP WITH TIME ZONE",
    ):
        time_col = None
    partition = _partition_for(dataset, time_col)
    if coverage is not None and coverage.by != "write":
        placed = coverage.column or time_col
        if placed not in sql_columns or (
            coverage.entity is not None and coverage.entity not in sql_columns
        ):
            coverage = None
    return CoreDatasetSpec(
        name=dataset,
        columns=sql_columns,
        key=key,
        time=time_col,
        partition=partition,
        description=str(getattr(spec, "summary", None) or getattr(spec, "description", None) or ""),
        coverage=coverage,
    )


@lru_cache(maxsize=1)
def core_registry() -> Registry:
    """Every registered CNEquity dataset compiled into an :class:`equitylake_core.spec.Registry`."""
    return Registry(_default_core_spec(name) for name in sorted(DATASETS))
