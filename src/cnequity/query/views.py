"""DuckDB view layer — one view per dataset, generated from the registry."""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from cnequity.config import Config
from cnequity.domain.datasets import DATASETS, DatasetSpec
from cnequity.domain.partitions import uses_hive
from cnequity.domain.schemas import DATASET_SCHEMAS, PRIMARY_KEYS
from cnequity.domain.trading_status import evidence_rank_sql
from cnequity.storage.revisions import resolve_committed_root


def _duckdb_type(dtype: pl.DataType) -> str:
    if isinstance(dtype, pl.Datetime):
        return "TIMESTAMPTZ" if dtype.time_zone else "TIMESTAMP"
    if dtype == pl.Date:
        return "DATE"
    if dtype == pl.Boolean:
        return "BOOLEAN"
    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64):
        return "BIGINT"
    if dtype in (pl.Float32, pl.Float64):
        return "DOUBLE"
    return "VARCHAR"


def _empty_view_sql(name: str) -> str:
    schema = DATASET_SCHEMAS[name]
    cols = ",\n            ".join(
        f"CAST(NULL AS {_duckdb_type(dtype)}) AS {col}" for col, dtype in schema.items()
    )
    return f"""
        CREATE OR REPLACE VIEW {name} AS
        SELECT
            {cols}
        WHERE false
    """


def _view_glob(data_root: str, spec: DatasetSpec) -> tuple[str, bool]:
    # *data_root* is always POSIX-form (see ensure_duckdb_views): DuckDB's
    # read_parquet glob accepts `/` on every platform, and backslashes would
    # either escape the SQL string or fail to match files on Windows.
    layer_dir = "derived" if spec.layer == "derived" else "curated"
    logical = Path(data_root) / layer_dir / spec.name
    # DuckDB views are another public read path. Resolve the pointer before
    # constructing the glob so a refresh that rewrites several partitions
    # never leaves a view over a mixed mutable layout.
    # Keep path-only callers (including Windows-style paths inspected on a
    # POSIX host) purely lexical.  Resolving through RevisionStore would turn
    # ``C:/...`` into a cwd-relative POSIX path and would also create metadata
    # directories as a side effect when no pointer exists.
    pointer = Path(data_root) / "meta" / "revisions" / spec.name / "current.json"
    dataset_root = (
        resolve_committed_root(
            logical,
            dataset=spec.name,
            meta_root=Path(data_root) / "meta",
        )
        if pointer.is_file()
        else logical
    )
    if spec.partition_col is None:
        return f"{dataset_root.as_posix()}/**/*.parquet", False
    # Hive parsing only for day granularity: a `trade_date=2024` directory
    # cannot be read as the DATE column it sits beside. The real column is in
    # the file either way, so the view is identical apart from pruning.
    return (
        f"{dataset_root.as_posix()}/**/*.parquet",
        uses_hive(spec.partition_granularity),
    )


def _glob_has_files(pattern: str) -> bool:
    base = pattern.split("**")[0].split("*")[0].rstrip("/")
    p = Path(base)
    if not p.exists():
        return False
    return any(p.rglob("*.parquet"))


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _canonical_order_sql(name: str, columns: set[str]) -> str:
    """Return the DuckDB ordering used to select one canonical lake row.

    Mirrors :func:`cnequity.domain.canonical.dedupe_by_primary_key`, including
    the datasets whose evidence class outranks recency.
    """
    order_by: list[str] = []
    if name == "trading_status":
        evidence = evidence_rank_sql(columns)
        if evidence is not None:
            order_by.append(f"{evidence} DESC")
    if "fetched_at" in columns:
        order_by.append("fetched_at DESC NULLS LAST")
    if "source" in columns:
        spec = DATASETS.get(name)
        cases: list[str] = []
        if spec and spec.backup_source:
            cases.append(f"WHEN source = {_sql_literal(spec.backup_source)} THEN 1")
        if spec and spec.primary_source:
            cases.append(f"WHEN source = {_sql_literal(spec.primary_source)} THEN 2")
        if cases:
            order_by.append("CASE " + " ".join(cases) + " ELSE 0 END DESC")
        order_by.append("source DESC NULLS LAST")
    if "data_version" in columns:
        order_by.append("data_version DESC NULLS LAST")
    return ", ".join(order_by)


def _view_select_sql(
    name: str,
    glob_path: str,
    hive: bool,
    *,
    columns: set[str] | None = None,
) -> str:
    """Build a canonical dataset view over all parquet fragments.

    DuckDB is a separate read path from :func:`cnequity.query.reader.load`.
    Apply the same latest-by-``fetched_at`` PK rule here, otherwise an old
    fragment or overlapping retry can multiply rows in SQL joins while the
    Python API returns one canonical observation.
    """
    primary_key = PRIMARY_KEYS.get(name, [])
    source = (
        f"read_parquet('{glob_path}', hive_partitioning={str(hive).lower()}, union_by_name=true)"
    )
    # A few pre-schema-migration fragments in the wild (and lightweight
    # bootstrap fixtures) do not carry provenance. If no provenance field is
    # available, keep the raw view readable and let the schema/quality checks
    # report the malformed fragment. A source/version without fetched_at is
    # still enough to apply the same deterministic fallback as Python.
    if not primary_key or (
        columns is not None
        and (
            not set(primary_key).issubset(columns)
            or not {"fetched_at", "source", "data_version"}.intersection(columns)
        )
    ):
        return f"SELECT * FROM {source}"
    partition_by = ", ".join(primary_key)
    order_by = _canonical_order_sql(name, columns or set())
    return (
        "SELECT * FROM "
        f"{source} "
        "QUALIFY ROW_NUMBER() OVER ("
        f"PARTITION BY {partition_by} ORDER BY {order_by}"
        ") = 1"
    )


def ensure_duckdb_views(config: Config, *, require_data: bool = False) -> Path:
    db_path = config.duckdb_path or (config.data_root / "duckdb" / "cnequity.duckdb")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # as_posix() keeps Windows drive letters (`C:/…`) while turning `\` into
    # `/`, which is what the SQL literals below and DuckDB's glob both want.
    root = config.data_root.resolve().as_posix().replace("'", "''")

    con = duckdb.connect(str(db_path))
    con.execute(f"SET memory_limit='{config.duckdb_memory_limit}'")
    con.execute(f"SET threads={config.duckdb_threads}")

    for name, spec in sorted(DATASETS.items()):
        glob_path, hive = _view_glob(root, spec)
        if _glob_has_files(glob_path) or require_data:
            source = (
                f"read_parquet('{glob_path}', "
                f"hive_partitioning={str(hive).lower()}, union_by_name=true)"
            )
            columns = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()}
            con.execute(
                f"""
                CREATE OR REPLACE VIEW {name} AS
                {_view_select_sql(name, glob_path, hive, columns=columns)}
                """
            )
        else:
            con.execute(_empty_view_sql(name))

    # Adjusted bars per ADR-0004: only hfq factors are stored.
    #   hfq price = raw * factor
    #   qfq price = raw * factor / anchor   (anchor = latest factor on a bar date)
    # The static view is anchored to each symbol's latest bar in the lake.  A
    # bounded qfq query must use the table macro below, whose anchor is scoped
    # to its explicit [start_date, end_date] window.
    # adj_* keeps its historical qfq meaning; adj_is_exact mirrors the Python API.
    con.execute(
        """
        CREATE OR REPLACE VIEW daily_bars_adj AS
        WITH hfq AS (
            SELECT symbol, trade_date, factor
            FROM adj_factors
            WHERE adjust_type = 'hfq'
        ),
        bar_anchors AS (
            SELECT symbol, MAX(trade_date) AS anchor_date
            FROM daily_bars
            GROUP BY symbol
        ),
        anchors AS (
            SELECT h.symbol, h.factor AS hfq_anchor
            FROM hfq h
            JOIN bar_anchors b
              ON h.symbol = b.symbol AND h.trade_date <= b.anchor_date
            QUALIFY ROW_NUMBER() OVER (PARTITION BY h.symbol ORDER BY h.trade_date DESC) = 1
        )
        SELECT
            b.*,
            h.factor IS NOT NULL AS adj_is_exact,
            b.open  * COALESCE(h.factor, 1.0) AS hfq_open,
            b.high  * COALESCE(h.factor, 1.0) AS hfq_high,
            b.low   * COALESCE(h.factor, 1.0) AS hfq_low,
            b.close * COALESCE(h.factor, 1.0) AS hfq_close,
            b.open  * COALESCE(h.factor / a.hfq_anchor, 1.0) AS qfq_open,
            b.high  * COALESCE(h.factor / a.hfq_anchor, 1.0) AS qfq_high,
            b.low   * COALESCE(h.factor / a.hfq_anchor, 1.0) AS qfq_low,
            b.close * COALESCE(h.factor / a.hfq_anchor, 1.0) AS qfq_close,
            b.close * COALESCE(h.factor / a.hfq_anchor, 1.0) AS adj_close
        FROM daily_bars b
        LEFT JOIN hfq h
          ON b.symbol = h.symbol AND b.trade_date = h.trade_date
        LEFT JOIN anchors a
          ON b.symbol = a.symbol
        """
    )
    # DuckDB views cannot see the outer query's WHERE clause, so a static
    # qfq column cannot be correct for a historical sub-window.  This macro
    # is the SQL equivalent of load(..., adjust='qfq', start=..., end=...).
    con.execute(
        """
        CREATE OR REPLACE MACRO daily_bars_qfq(start_date, end_date) AS TABLE (
            WITH bars AS (
                SELECT *
                FROM daily_bars
                WHERE (start_date IS NULL OR trade_date >= CAST(start_date AS DATE))
                  AND (end_date IS NULL OR trade_date <= CAST(end_date AS DATE))
            ),
            bar_anchors AS (
                SELECT symbol, MAX(trade_date) AS anchor_date
                FROM bars
                GROUP BY symbol
            ),
            hfq AS (
                SELECT f.symbol, f.trade_date, f.factor
                FROM adj_factors f
                JOIN bar_anchors b
                  ON f.symbol = b.symbol AND f.trade_date <= b.anchor_date
                WHERE f.adjust_type = 'hfq'
            ),
            anchors AS (
                SELECT h.symbol, h.factor AS hfq_anchor
                FROM hfq h
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY h.symbol ORDER BY h.trade_date DESC
                ) = 1
            )
            SELECT
                b.*,
                h.factor IS NOT NULL AS adj_is_exact,
                b.open  * COALESCE(h.factor, 1.0) AS hfq_open,
                b.high  * COALESCE(h.factor, 1.0) AS hfq_high,
                b.low   * COALESCE(h.factor, 1.0) AS hfq_low,
                b.close * COALESCE(h.factor, 1.0) AS hfq_close,
                b.open  * COALESCE(h.factor / a.hfq_anchor, 1.0) AS qfq_open,
                b.high  * COALESCE(h.factor / a.hfq_anchor, 1.0) AS qfq_high,
                b.low   * COALESCE(h.factor / a.hfq_anchor, 1.0) AS qfq_low,
                b.close * COALESCE(h.factor / a.hfq_anchor, 1.0) AS qfq_close,
                b.close * COALESCE(h.factor / a.hfq_anchor, 1.0) AS adj_close
            FROM bars b
            LEFT JOIN hfq h
              ON b.symbol = h.symbol AND b.trade_date = h.trade_date
            LEFT JOIN anchors a
              ON b.symbol = a.symbol
        )
        """
    )
    con.close()
    return db_path
