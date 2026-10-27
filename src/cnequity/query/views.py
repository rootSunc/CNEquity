"""DuckDB view layer — one view per dataset, generated from the registry."""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

import duckdb
import polars as pl

from cnequity.config import Config
from cnequity.domain.action_evidence import evidence_level_sql
from cnequity.domain.canonical import OFFICIAL_INDEX_SOURCES
from cnequity.domain.datasets import DATASETS, DatasetSpec
from cnequity.domain.market_profile import OTC_SCOPED_DATASETS, otc_quote_sql
from cnequity.domain.partitions import uses_hive
from cnequity.domain.schemas import DATASET_SCHEMAS, PRIMARY_KEYS
from cnequity.domain.trading_status import evidence_rank_sql
from cnequity.storage.atomic import replace_with_retry
from cnequity.storage.revisions import resolve_committed_root

logger = logging.getLogger(__name__)


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


def _canonical_order_sql(name: str, columns: set[str] | dict[str, str]) -> str:
    """Return the DuckDB ordering used to select one canonical lake row.

    Mirrors :func:`cnequity.domain.canonical.dedupe_by_primary_key`, including
    the datasets whose evidence class outranks recency.
    """
    order_by: list[str] = []
    if name == "trading_status":
        evidence = evidence_rank_sql(columns)
        if evidence is not None:
            order_by.append(f"{evidence} DESC")
    elif name == "corporate_actions":
        evidence = evidence_level_sql(set(columns))
        if evidence is not None:
            order_by.append(f"{evidence} DESC")
    elif name == "financial_statement_items" and "source" in columns:
        order_by.append(
            "CASE WHEN source = 'cninfo_original' THEN 2 "
            "WHEN regexp_matches(coalesce(source, ''), 'backfill|reconstruct|snapshot') "
            "THEN 0 ELSE 1 END DESC"
        )
    elif name == "index_constituents" and "weight" in columns:
        order_by.append("(weight IS NOT NULL) DESC")
    elif name == "daily_bars" and {"volume", "amount"} <= set(columns):
        order_by.append("CASE WHEN volume = 0 AND amount > 0 THEN 0 ELSE 1 END DESC")
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
    columns: set[str] | dict[str, str] | None = None,
) -> str:
    """Build a canonical dataset view over all parquet fragments.

    DuckDB is a separate read path from :func:`cnequity.query.reader.load`.
    Apply the same latest-by-``fetched_at`` PK rule here, otherwise an old
    fragment or overlapping retry can multiply rows in SQL joins while the
    Python API returns one canonical observation.
    """
    primary_key = PRIMARY_KEYS.get(name, [])
    source = f"read_parquet({_sql_literal(glob_path)}, hive_partitioning={str(hive).lower()}, union_by_name=true)"
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
    payment_keys = primary_key + ["cash_dividend", "bonus_ratio", "transfer_ratio"]
    if name == "corporate_actions" and set(
        payment_keys + ["payment_date", "payment_source"]
    ) <= set(columns or ()):
        # Python carries the nearest preceding valid payment evidence within
        # identical economic terms. Descending order reverses its ascending
        # forward-fill, so the equivalent SQL frame looks forward. Select the
        # winner BEFORE enrichment: adding a date must not re-rank the rows.
        return f"""
            SELECT * EXCLUDE (__payment_evidence) REPLACE (
                CASE WHEN payment_date IS NOT NULL THEN payment_date
                     ELSE __payment_evidence.payment_date END AS payment_date,
                CASE WHEN payment_date IS NOT NULL THEN payment_source
                     ELSE __payment_evidence.payment_source END AS payment_source
            ) FROM (
                SELECT *, first_value(
                    CASE WHEN payment_date IS NOT NULL AND payment_date >= ex_date
                         THEN struct_pack(payment_date := payment_date,
                                          payment_source := payment_source)
                    END IGNORE NULLS
                ) OVER (
                    PARTITION BY {", ".join(payment_keys)} ORDER BY {order_by}
                    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                ) AS __payment_evidence
                FROM {source}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY {partition_by} ORDER BY {order_by}
                ) = 1
            )
        """
    canonical = (
        "SELECT * FROM "
        f"{source} "
        "QUALIFY ROW_NUMBER() OVER ("
        f"PARTITION BY {partition_by} ORDER BY {order_by}"
        ") = 1"
    )
    if name == "index_constituents" and {"source", "index_symbol", "as_of_date"} <= set(
        columns or ()
    ):
        # Mirrors domain/canonical.one_book_per_index_date: an official book
        # replaces, rather than merges with, a vendor's on the same index-date.
        official = ", ".join(_sql_literal(item) for item in OFFICIAL_INDEX_SOURCES)
        return (
            f"SELECT * FROM ({canonical}) "
            f"QUALIFY source IN ({official}) OR NOT bool_or(source IN ({official})) "
            "OVER (PARTITION BY index_symbol, as_of_date)"
        )
    return canonical


def views_connection(config: Config, *, require_data: bool = False) -> duckdb.DuckDBPyConnection:
    """An in-memory connection holding every lake view, for one reader.

    Readers used to rebuild the views inside ``{data.root}/duckdb/cnequity.duckdb``
    and then reopen it read-only. DuckDB lets one process hold that file while
    it writes, so two `cne query` at once, a query beside an MCP agent, or any
    query while a run finished its compact failed with "Could not set lock on
    file". Views are only definitions over the Parquet files, so each reader
    now keeps its own and never opens the shared file.
    """
    con = duckdb.connect(":memory:")
    try:
        _define_views(con, config, require_data=require_data)
    except BaseException:
        con.close()
        raise
    return con


def ensure_duckdb_views(config: Config, *, require_data: bool = False) -> Path:
    """Rewrite the on-disk view database for tools outside cnequity.

    Built beside the target and swapped in with one rename, so a DuckDB
    session or notebook holding the previous file keeps reading it and nothing
    here ever waits for, or fights over, that file's lock. Where the rename is
    refused (Windows, while the file is open) the previous views stay: they
    read Parquet by glob, so they still see every new partition.
    """
    db_path = config.duckdb_path or (config.data_root / "duckdb" / "cnequity.duckdb")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(dir=db_path.parent, prefix=f".{db_path.name}.", suffix=".tmp")
    os.close(handle)
    building = Path(name)
    building.unlink()  # DuckDB refuses to open an empty file as a database
    try:
        con = duckdb.connect(str(building))
        try:
            _define_views(con, config, require_data=require_data)
            con.execute("CHECKPOINT")
        finally:
            con.close()
        # A write-ahead log left by an older version's crash would be replayed
        # against the new file.
        Path(f"{db_path}.wal").unlink(missing_ok=True)
        try:
            replace_with_retry(building, db_path)
        except OSError as exc:
            if not db_path.exists():
                raise
            logger.warning(
                "Kept the previous DuckDB views at %s; replacing them failed: %s", db_path, exc
            )
    finally:
        building.unlink(missing_ok=True)
        Path(f"{building}.wal").unlink(missing_ok=True)
    return db_path


def _define_views(con: duckdb.DuckDBPyConnection, config: Config, *, require_data: bool) -> None:
    # as_posix() keeps Windows drive letters (`C:/…`) while turning `\` into
    # `/`, which is what the SQL literals below and DuckDB's glob both want.
    root = config.data_root.resolve().as_posix()

    con.execute(f"SET memory_limit='{config.duckdb_memory_limit}'")
    con.execute(f"SET threads={config.duckdb_threads}")

    for name, spec in sorted(DATASETS.items()):
        view_name = "flash_news_wire_legacy" if name == "flash_news_wire" else name
        if name in OTC_SCOPED_DATASETS:
            # The public view leaves out Beijing NEEQ quotes, as load() does;
            # it is defined below, once instruments exists to bind against.
            view_name = f"{name}_including_otc"
        glob_path, hive = _view_glob(root, spec)
        if _glob_has_files(glob_path) or require_data:
            source = (
                f"read_parquet({_sql_literal(glob_path)}, "
                f"hive_partitioning={str(hive).lower()}, union_by_name=true)"
            )
            # Types, not just names: the canonical ordering declines a
            # `fetched_at` that some fragment stored as text, and it can
            # only decline what it can see.
            columns = {
                row[0]: row[1] for row in con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
            }
            con.execute(
                f"""
                CREATE OR REPLACE VIEW {view_name} AS
                {_view_select_sql(name, glob_path, hive, columns=columns)}
                """
            )
        else:
            con.execute(_empty_view_sql(name).replace(f"VIEW {name} AS", f"VIEW {view_name} AS"))

    instrument_columns = {row[0] for row in con.execute("DESCRIBE instruments").fetchall()}
    # Without listing dates the cut is the select tier's opening alone, as in load().
    listing = (
        "SELECT symbol, MIN(list_date) AS list_date FROM instruments GROUP BY symbol"
        if {"symbol", "list_date"} <= instrument_columns
        else "SELECT NULL::VARCHAR AS symbol, NULL::DATE AS list_date WHERE FALSE"
    )
    for name in sorted(OTC_SCOPED_DATASETS):
        date_col = DATASETS[name].date_col or DATASETS[name].partition_col or "trade_date"
        con.execute(
            f"""
            CREATE OR REPLACE VIEW {name} AS
            SELECT b.*
            FROM {name}_including_otc b
            LEFT JOIN ({listing}) i ON b.symbol = i.symbol
            WHERE NOT COALESCE({otc_quote_sql("b.symbol", f"b.{date_col}", "i.list_date")}, FALSE)
            """
        )

    # Existing flash revisions remain readable, while every new EastMoney
    # item is stored only in news_headlines. Keep the wire's public columns and
    # hash contract in SQL, including the compatibility key's de-duplication.
    con.execute(
        """
        CREATE OR REPLACE VIEW flash_news_wire AS
        SELECT * FROM (
            SELECT wire_id, wire_source, item_hash, publish_date, publish_time,
                   title, summary, related_symbols, importance, channel,
                   source, data_version, fetched_at
            FROM flash_news_wire_legacy
            UNION ALL
            SELECT 'eastmoney:' || news_id AS wire_id,
                   'eastmoney' AS wire_source,
                   substr(sha256('eastmoney|' || CAST(publish_date AS VARCHAR)
                       || 'T' || coalesce(publish_time, '00:00:00') || '|'
                       || trim(title)), 1, 16) AS item_hash,
                   publish_date, publish_time, trim(title) AS title, summary,
                   related_symbols, CAST(NULL AS TINYINT) AS importance,
                   coalesce(channel, 'fast_news') AS channel,
                   source, data_version, fetched_at
            FROM news_headlines
            WHERE news_id IS NOT NULL AND trim(title) <> ''
        )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY wire_id, wire_source
            ORDER BY fetched_at DESC NULLS LAST
        ) = 1
        """
    )

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
    # SQL twin of load(..., adjust='total_return', start=..., end=...): hfq
    # with fund cash distributions reinvested at the close before each
    # ex-date. Stocks read as hfq. Window-scoped like daily_bars_qfq, because
    # each fund's level is anchored on its first bar in scope.
    con.execute(
        """
        CREATE OR REPLACE MACRO daily_bars_total_return(start_date, end_date) AS TABLE (
            WITH bars AS (
                SELECT *
                FROM daily_bars
                WHERE (start_date IS NULL OR trade_date >= CAST(start_date AS DATE))
                  AND (end_date IS NULL OR trade_date <= CAST(end_date AS DATE))
            ),
            funds AS (
                SELECT DISTINCT symbol FROM instruments WHERE asset_type = 'etf'
            ),
            payouts AS (
                SELECT symbol, ex_date, MAX(cash_dividend) AS cash
                FROM corporate_actions
                WHERE action_type = 'cash_dividend'
                  AND cash_dividend > 0
                  AND symbol IN (SELECT symbol FROM funds)
                  AND (start_date IS NULL OR ex_date >= CAST(start_date AS DATE))
                  AND (end_date IS NULL OR ex_date <= CAST(end_date AS DATE))
                GROUP BY symbol, ex_date
            ),
            first_payout_sessions AS (
                SELECT p.symbol, p.ex_date, p.cash,
                       MIN(b.trade_date) AS effective_session
                FROM payouts p
                JOIN bars b ON b.symbol = p.symbol AND b.trade_date >= p.ex_date
                GROUP BY p.symbol, p.ex_date, p.cash
            ),
            effective_payouts AS (
                SELECT symbol, effective_session, SUM(cash) AS cash
                FROM first_payout_sessions
                GROUP BY symbol, effective_session
            ),
            stepped AS (
                SELECT
                    b.*,
                    h.factor AS _hfq,
                    LAG(b.close) OVER (PARTITION BY b.symbol ORDER BY b.trade_date) AS _prev,
                    p.cash AS _cash
                FROM bars b
                LEFT JOIN adj_factors h
                  ON b.symbol = h.symbol AND b.trade_date = h.trade_date
                 AND h.adjust_type = 'hfq'
                LEFT JOIN effective_payouts p
                  ON b.symbol = p.symbol AND b.trade_date = p.effective_session
            ),
            multiplied AS (
                SELECT
                    *,
                    EXP(SUM(LN(
                        CASE WHEN _cash IS NOT NULL AND _prev > _cash
                             THEN _prev / (_prev - _cash) ELSE 1.0 END
                    )) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )) AS _mult
                FROM stepped
            )
            SELECT
                * EXCLUDE (_hfq, _prev, _cash, _mult),
                _hfq IS NOT NULL AS adj_is_exact,
                open  * COALESCE(_hfq, 1.0) * _mult AS tr_open,
                high  * COALESCE(_hfq, 1.0) * _mult AS tr_high,
                low   * COALESCE(_hfq, 1.0) * _mult AS tr_low,
                close * COALESCE(_hfq, 1.0) * _mult AS tr_close,
                close * COALESCE(_hfq, 1.0) * _mult AS adj_close
            FROM multiplied
        )
        """
    )
