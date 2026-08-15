"""Python read API — load curated datasets with adjustment, universe, and PIT filters."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Literal

import polars as pl

from cn_market_lake.config import Config, load_config
from cn_market_lake.derive.adj_factors import STORED_ADJUST_TYPE
from cn_market_lake.domain.datasets import (
    DATASETS,
    curated_dataset_names,
    derived_dataset_names,
    intraday_dataset_names,
    pit_dataset_names,
)
from cn_market_lake.domain.schemas import DATASET_SCHEMAS, PRIMARY_KEYS, validate_dataframe
from cn_market_lake.query.parquet_scan import (
    collect_parquet_root,
    dataset_has_parquet,
    partition_col_for_dataset,
    scan_parquet_root,
)
from cn_market_lake.query.universe import apply_universe_filter

logger = logging.getLogger(__name__)

AdjustType = Literal["qfq", "hfq"]
UniverseType = Literal["all_a"]

# All derived from the DatasetSpec registry (domain/datasets.py).
CURATED_DATASETS = curated_dataset_names()
DERIVED_DATASETS = derived_dataset_names()
DATE_COLUMNS: dict[str, str] = {
    name: spec.query_date_col for name, spec in DATASETS.items() if spec.query_date_col is not None
}
PIT_DATASETS = pit_dataset_names()
# Columns an adjustment factor multiplies. `price` is here for trade_ticks,
# which has no OHLC — without it the dataset would be in ADJUSTABLE_DATASETS
# and `load(adjust="hfq")` would return no adj_ columns at all, silently.
PRICE_COLS = ("open", "high", "low", "close", "price")
# Datasets carrying per-share prices that adj_factors can adjust. Intraday
# datasets come from the registry so a new frequency is adjustable the day it
# is registered. index_bars is in because its `frequency` column made it a bar
# dataset historically; its levels are not per-share, but that predates this.
# trade_ticks is listed by name rather than inherited from
# intraday_dataset_names(): it deliberately carries no `intraday_frequency`
# (see its DatasetSpec), but its prices still cross ex-dividend dates and a
# comparison spanning one would otherwise see a gap that is not a price move.
ADJUSTABLE_DATASETS = {"daily_bars", "index_bars", "trade_ticks"} | set(intraday_dataset_names())


class ReaderError(ValueError):
    """Raised when load() arguments or dataset state are invalid."""


def _parse_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def resolve_config(
    *,
    config: Config | None = None,
    data_root: str | Path | None = None,
) -> Config:
    if config is not None:
        return config
    if data_root is not None:
        return Config(data_root=Path(data_root).expanduser().resolve())
    path = Path("configs/cn-market-lake.toml")
    if path.exists():
        return load_config(path)
    raise ReaderError(
        "No config found: pass config= or data_root=, or create configs/cn-market-lake.toml"
    )


def _dataset_root(config: Config, dataset: str) -> Path:
    if dataset in DERIVED_DATASETS:
        return config.derived_root / dataset
    if dataset in CURATED_DATASETS:
        return config.curated_root / dataset
    raise ReaderError(f"unknown dataset {dataset!r}")


def _read_dataset(
    config: Config,
    dataset: str,
    *,
    start: date | None = None,
    end: date | None = None,
    symbols: list[str] | None = None,
) -> pl.DataFrame:
    root = _dataset_root(config, dataset)
    if not dataset_has_parquet(root):
        raise ReaderError(
            f"no parquet data for dataset {dataset!r} under {root} (data_root={config.data_root})"
        )

    partition_col = DATE_COLUMNS.get(dataset) or partition_col_for_dataset(dataset)
    try:
        df = collect_parquet_root(
            root,
            partition_col=partition_col,
            start=start,
            end=end,
            symbols=symbols,
        )
    except FileNotFoundError as exc:
        raise ReaderError(
            f"no parquet data for dataset {dataset!r} under {root} (data_root={config.data_root})"
        ) from exc
    if dataset in DATASET_SCHEMAS:
        return validate_dataframe(df, dataset)
    return df


def _apply_date_range(
    df: pl.DataFrame,
    dataset: str,
    start: date | None,
    end: date | None,
) -> pl.DataFrame:
    col = DATE_COLUMNS.get(dataset)
    if col is None or col not in df.columns or df.is_empty():
        return df
    if start is not None:
        df = df.filter(pl.col(col) >= start)
    if end is not None:
        df = df.filter(pl.col(col) <= end)
    return df


def _apply_symbol_filter(df: pl.DataFrame, symbols: list[str] | None) -> pl.DataFrame:
    if not symbols or df.is_empty() or "symbol" not in df.columns:
        return df
    return df.filter(pl.col("symbol").is_in(symbols))


def _hfq_anchor_factors(
    factors: pl.DataFrame,
    bars: pl.DataFrame,
    end: date | None,
) -> pl.DataFrame:
    """Per-symbol hfq factor at the qfq anchor date (latest date in scope)."""
    if end is not None:
        anchor_bars = bars.filter(pl.col("trade_date") <= end)
        anchor_factors = factors.filter(pl.col("trade_date") <= end)
    else:
        anchor_bars = bars
        anchor_factors = factors

    bar_anchors = anchor_bars.group_by("symbol").agg(
        pl.col("trade_date").max().alias("anchor_date")
    )
    return (
        anchor_factors.join(bar_anchors, on="symbol")
        .filter(pl.col("trade_date") == pl.col("anchor_date"))
        .select(["symbol", pl.col("factor").alias("hfq_anchor")])
    )


def _apply_adjustment(
    bars: pl.DataFrame,
    config: Config,
    adjust: AdjustType,
    start: date | None,
    end: date | None,
    *,
    strict_adj: bool = False,
) -> pl.DataFrame:
    if bars.is_empty():
        return bars

    factors = _read_dataset(config, "adj_factors", start=start, end=end)
    if factors.is_empty():
        out = bars.with_columns(
            *[pl.col(c).alias(f"adj_{c}") for c in PRICE_COLS if c in bars.columns],
            pl.lit(False).alias("adj_is_exact"),
        )
        if strict_adj:
            raise ReaderError("adj_factors dataset is empty; cannot compute exact adjusted prices")
        logger.warning("adj_factors missing; adj_is_exact=False for all rows")
        return out

    factors = factors.filter(pl.col("adjust_type") == STORED_ADJUST_TYPE)
    if factors.is_empty():
        raise ReaderError(
            f"adj_factors has no {STORED_ADJUST_TYPE!r} rows; re-run derive (ADR-0004)"
        )

    factors = factors.select(["symbol", "trade_date", "factor"])

    if adjust == "qfq":
        anchors = _hfq_anchor_factors(factors, bars, end)
        factors = factors.join(anchors, on="symbol", how="left")
        factors = factors.with_columns(
            (pl.col("factor") / pl.col("hfq_anchor")).alias("factor")
        ).drop("hfq_anchor")
    elif adjust != "hfq":
        raise ReaderError(f"unsupported adjust type {adjust!r}")

    joined = bars.join(factors, on=["symbol", "trade_date"], how="left")
    joined = joined.with_columns(pl.col("factor").is_not_null().alias("adj_is_exact"))
    inexact = joined.filter(~pl.col("adj_is_exact")).height
    if inexact:
        msg = f"{inexact} bar row(s) missing adj_factors for adjust={adjust!r}"
        if strict_adj:
            raise ReaderError(msg)
        logger.warning("%s; using factor=1.0 with adj_is_exact=False", msg)
    joined = joined.with_columns(pl.col("factor").fill_null(1.0))
    adj_exprs = [
        (pl.col(c) * pl.col("factor")).alias(f"adj_{c}") for c in PRICE_COLS if c in joined.columns
    ]
    return joined.with_columns(adj_exprs).drop("factor")


def _apply_pit_filters(
    df: pl.DataFrame,
    dataset: str,
    *,
    as_of: date,
    items: list[str] | None,
    all_vintages: bool,
) -> pl.DataFrame:
    if df.is_empty():
        return df
    if "announce_date" not in df.columns:
        raise ReaderError(f"{dataset} requires announce_date column (PIT contract)")
    df = df.filter(pl.col("announce_date") <= as_of)
    if items and "item_code" in df.columns:
        df = df.filter(pl.col("item_code").is_in(items))
    if all_vintages or df.is_empty():
        return df

    # announce_date is in the PK, so a restated fact keeps both its original and
    # its revised row. Filtering alone would return every vintage announced on or
    # before as_of and silently double-count the fact; collapse to the one that
    # was current on that date.
    key = [c for c in PRIMARY_KEYS.get(dataset, []) if c != "announce_date"]
    if not key or not all(c in df.columns for c in key):
        return df
    return df.sort("announce_date").group_by(key, maintain_order=True).last()


def load(
    dataset: str,
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    adjust: AdjustType | None = None,
    universe: UniverseType | None = None,
    as_of: str | date | None = None,
    items: list[str] | None = None,
    symbols: list[str] | None = None,
    strict_adj: bool = False,
    all_vintages: bool = False,
    config: Config | None = None,
    data_root: str | Path | None = None,
) -> pl.DataFrame:
    """Load a curated dataset with optional adjustment, universe, and PIT filters.

    Parameters
    ----------
    dataset:
        Curated dataset name (e.g. ``daily_bars``, ``financial_statement_items``).
    start, end:
        Inclusive date window on the dataset's primary date column.
    adjust:
        ``qfq`` or ``hfq`` — joins stored ``hfq`` ``adj_factors`` and adds
        ``adj_open`` … ``adj_close`` plus ``adj_is_exact``. ``qfq`` is derived
        at query time as ``hfq_factor / hfq_anchor`` (anchor = latest bar date
        in scope); only ``hfq`` is persisted (ADR-0004).
    universe:
        ``all_a`` — drop unlisted/delisted rows per day via ``instruments``, and
        drop ST/suspended rows when ``trading_status`` has data for that day.
        CDRs (SH 689xxx, e.g. 689009.SH) are excluded: they are depositary
        receipts with no adj-factor source coverage; query them via ``symbols=``
        without a universe if needed. Only valid for ``daily_bars`` (not
        ``index_bars``).

        **Limitation:** ``trading_status`` is daily-only (no historical ST
        backfill). Dates before the curated coverage start (see audit check
        ``trading_status_coverage_start``) are **not** ST/suspended-filtered;
        long backtests may include ST names in early windows.
    as_of:
        Point-in-time date for ``financial_statement_items``. Keeps only facts
        announced on or before this date, and — because a restatement stores a
        second vintage of the same fact rather than overwriting the first —
        returns the vintage that was current on that date.
    items:
        ``item_code`` filter for ``financial_statement_items``.
    all_vintages:
        Return every vintage announced on or before ``as_of`` instead of only
        the one current then. For studying revisions (a restatement's size and
        direction is itself a signal); not for cross-sectional screens, where
        multiple vintages of one fact would double-count it.
    symbols:
        Restrict to these symbols when the dataset has a ``symbol`` column.
    config, data_root:
        Lake location; auto-detects ``configs/cn-market-lake.toml`` when omitted.
        Raises ``ReaderError`` if config or dataset parquet files are missing.
    """
    cfg = resolve_config(config=config, data_root=data_root)
    if dataset not in CURATED_DATASETS | DERIVED_DATASETS:
        raise ReaderError(f"unknown dataset {dataset!r}")

    if universe and dataset == "index_bars":
        raise ReaderError(
            "universe filter applies to daily_bars only; index symbols are not in all_a"
        )

    start_d = _parse_date(start)
    end_d = _parse_date(end)
    as_of_d = _parse_date(as_of)

    if dataset in PIT_DATASETS:
        if as_of_d is None:
            raise ReaderError(f"{dataset} requires as_of= for point-in-time queries")
        df = _read_dataset(cfg, dataset, symbols=symbols)
        df = _apply_pit_filters(df, dataset, as_of=as_of_d, items=items, all_vintages=all_vintages)
        df = _apply_symbol_filter(df, symbols)
        sort_cols = [
            c for c in ("announce_date", "symbol", "report_period", "item_code") if c in df.columns
        ]
        return df.sort(sort_cols) if sort_cols else df

    df = _read_dataset(cfg, dataset, start=start_d, end=end_d, symbols=symbols)

    if universe and dataset == "daily_bars":
        date_col = DATE_COLUMNS[dataset]
        df = apply_universe_filter(df, cfg, universe=universe, date_col=date_col)

    # Intraday datasets join on (symbol, trade_date) like the daily bars do: a
    # corporate action applies to a whole session, so every bar in a day shares
    # that day's factor. Intraday prices are stored unadjusted for the same
    # reason daily ones are — the factor series can be recomputed, a price
    # written adjusted cannot be undone.
    if adjust and dataset in ADJUSTABLE_DATASETS:
        df = _apply_adjustment(df, cfg, adjust, start_d, end_d, strict_adj=strict_adj)

    # Intraday rows sort by symbol then timestamp: trade_date alone would leave
    # a session's 240 bars in whatever order the pages arrived, and grouping by
    # symbol first is what every resampling consumer wants.
    #
    # Transaction records need `tick_seq` rather than the timestamp, because
    # the timestamp has no seconds: sorting a session by `trade_time` leaves
    # the twenty records sharing a minute in file order, which is the one thing
    # this dataset exists to preserve.
    if "tick_seq" in df.columns:
        order = ("symbol", "trade_date", "tick_seq")
    elif "bar_time" in df.columns:
        order = ("symbol", "bar_time")
    else:
        order = (DATE_COLUMNS.get(dataset), "symbol")
    sort_cols = [c for c in order if c and c in df.columns]
    if sort_cols:
        df = df.sort(sort_cols)
    return df


def scan(
    dataset: str,
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    symbols: list[str] | None = None,
    config: Config | None = None,
    data_root: str | Path | None = None,
) -> pl.LazyFrame:
    """Return a LazyFrame over a dataset with hive partition pruning.

    Raw scan for heavy pipelines — no adjustment/universe/PIT semantics
    (use ``load`` for those); date window and symbol filters push down to
    the partition scan.
    """
    cfg = resolve_config(config=config, data_root=data_root)
    if dataset not in CURATED_DATASETS | DERIVED_DATASETS:
        raise ReaderError(f"unknown dataset {dataset!r}")
    root = _dataset_root(cfg, dataset)
    if not dataset_has_parquet(root):
        raise ReaderError(
            f"no parquet data for dataset {dataset!r} under {root} (data_root={cfg.data_root})"
        )
    return scan_parquet_root(
        root,
        partition_col=DATE_COLUMNS.get(dataset) or partition_col_for_dataset(dataset),
        start=_parse_date(start),
        end=_parse_date(end),
        symbols=symbols,
    )


def dataset_schema(dataset: str) -> dict[str, pl.DataType]:
    """Column contract (polars dtypes) for a dataset."""
    if dataset not in DATASET_SCHEMAS:
        raise ReaderError(f"unknown dataset {dataset!r}")
    return dict(DATASET_SCHEMAS[dataset])


def list_datasets(
    *,
    config: Config | None = None,
    data_root: str | Path | None = None,
) -> pl.DataFrame:
    """Catalog of all datasets: layer, history mode, coverage, and watermark.

    Uses hive partition directory names and ``meta/state`` watermarks — no
    parquet data is read, so this is cheap even on a 10-year lake.

    ``history_mode`` / ``backfill_source`` / ``coverage_start`` are the
    programmatic available-from contract for research consumers, and
    ``history_horizon_days`` is the ceiling on how far back that contract can
    ever reach (None = no source-imposed limit).
    """
    from cn_market_lake.domain.datasets import history_mode_for
    from cn_market_lake.query.parquet_scan import list_partitions
    from cn_market_lake.storage.state import StateStore

    cfg = resolve_config(config=config, data_root=data_root)
    state = StateStore(cfg.meta_root)
    rows = []
    for name, spec in sorted(DATASETS.items()):
        root = (cfg.derived_root if spec.layer == "derived" else cfg.curated_root) / name
        has_data = dataset_has_parquet(root)
        first_part = last_part = None
        if has_data and spec.partition_col:
            # Period bounds, not directory labels: for a month/year partition the
            # last directory's *start* would understate coverage by up to a year,
            # and lake_health treats coverage_end as a freshness fallback.
            parts = list_partitions(root, spec.partition_col)
            if parts:
                first_part, last_part = parts[0].start, parts[-1].end
        rows.append(
            {
                "dataset": name,
                "layer": spec.layer,
                "date_col": spec.query_date_col,
                "fetch_semantics": spec.fetch_semantics,
                "history_mode": history_mode_for(spec),
                "backfill_source": spec.backfill_source,
                # None = unbounded. A number means the source itself serves only
                # that many trading days back, so anything earlier is
                # unreachable rather than merely un-backfilled.
                "history_horizon_days": spec.history_horizon_days,
                "pit": spec.pit,
                "has_data": has_data,
                "coverage_start": first_part,
                "coverage_end": last_part,
                "watermarked": spec.watermark,
                "watermark": state.get_date(name) if spec.watermark else None,
            }
        )
    return pl.DataFrame(rows)
