"""Remove ``trading_status`` rows outside a code's listed life.

Two writers put rows there before their guards existed:

* Compaction once read "no bar history yet" as "stopped trading" and stamped a
  delisting on codes TDX published a few days ahead of their first session.
  The status step then wrote ``derived_delisted`` for them, so a code that
  listed on 2026-09-02 was ``delisted`` through 09-08. The instrument has
  since lost the date, but the rows stayed.
* EastMoney's suspension list answers ``normal`` for every code it does not
  list, including one not yet listed, so codes were stored as trading for
  weeks before their first session.

A ``derived_delisted`` row is kept only while the current instrument record
still dates a delisting on or before it (and not before the code listed). A
row of any source before the listing date goes, unless a traded bar precedes
that date — then the date is wrong, not the rows. Nothing is added: a dropped
key either has another source's row already, or has no status.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.domain.trading_status import (
    DELISTED_SOURCE,
    impossible_delistings,
    split_before_listing,
)
from cnequity.file_lock import lake_mutation_lock
from cnequity.query.parquet_scan import scan_parquet_root
from cnequity.storage.repairs.publish import publish_repair
from cnequity.storage.revisions import RevisionStore

DATASET = "trading_status"
_KEY = ["symbol", "trade_date", "source"]


def instrument_spans(store: RevisionStore, config: Config) -> pl.DataFrame:
    """``symbol / list_date / delist_date`` from the committed instruments."""
    schema = {"symbol": pl.Utf8, "list_date": pl.Date, "delist_date": pl.Date}
    root = store.current_root("instruments") or config.curated_root / "instruments"
    files = sorted(Path(root).rglob("*.parquet")) if Path(root).is_dir() else []
    if not files:
        return pl.DataFrame(schema=schema)
    frame = pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")
    for column in ("list_date", "delist_date"):
        if column not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Date).alias(column))
    return (
        frame.select(
            "symbol",
            pl.col("list_date").cast(pl.Date, strict=False),
            pl.col("delist_date").cast(pl.Date, strict=False),
        )
        .unique("symbol", keep="last")
        .cast(schema)
    )


def first_traded(store: RevisionStore, config: Config, symbols: list[str]) -> pl.DataFrame:
    """``symbol / first_traded`` from the committed ``daily_bars``."""
    schema = {"symbol": pl.Utf8, "first_traded": pl.Date}
    root = store.current_root("daily_bars") or config.curated_root / "daily_bars"
    if not symbols or not Path(root).is_dir():
        return pl.DataFrame(schema=schema)
    return (
        scan_parquet_root(
            Path(root), partition_col="trade_date", symbols=sorted(symbols), traded_only=True
        )
        .group_by("symbol")
        .agg(pl.col("trade_date").min().alias("first_traded"))
        .collect()
        .cast(schema)
    )


def listing_spans(store: RevisionStore, config: Config, *, listed_after: date) -> pl.DataFrame:
    """Spans of the codes listed after *listed_after*, with each one's first traded session."""
    spans = instrument_spans(store, config).filter(pl.col("list_date") > listed_after)
    return spans.join(
        first_traded(store, config, spans["symbol"].to_list()), on="symbol", how="left"
    )


def plan_listing_window(rows: pl.DataFrame, spans: pl.DataFrame) -> pl.DataFrame:
    """``symbol / trade_date / source / reason`` of the rows to drop."""
    impossible = impossible_delistings(spans)
    delisted = rows.filter(pl.col("source") == DELISTED_SOURCE).join(
        spans.select("symbol", "delist_date"), on="symbol", how="left"
    )
    unfounded = delisted.filter(
        pl.col("delist_date").is_null()
        | (pl.col("trade_date") < pl.col("delist_date"))
        | pl.col("symbol").is_in(sorted(impossible))
    ).select(*_KEY, pl.lit("delisting_not_on_record").alias("reason"))
    _, early = split_before_listing(rows.select(_KEY), spans)
    early = early.select(*_KEY, pl.lit("before_listing").alias("reason"))
    return pl.concat([unfounded, early], how="vertical").unique(
        _KEY, keep="first", maintain_order=True
    )


def repair_status_listing_window(config: Config, *, apply: bool = False) -> dict:
    """Plan (default) or publish the removal of rows outside each listed life."""
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _repair_locked(config, apply=apply)


def _repair_locked(config: Config, *, apply: bool) -> dict:
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    base = store.repair_base(DATASET, register=apply)
    report: dict = {"applied": False, "rows": 0}
    if base is None:
        report["note"] = "no trading_status data; nothing to repair"
        return report
    rows = scan_parquet_root(base, partition_col="trade_date").select(_KEY).collect()
    spans = instrument_spans(store, config)
    candidates = rows.join(spans.select("symbol", "list_date"), on="symbol").filter(
        pl.col("trade_date") < pl.col("list_date")
    )
    spans = spans.join(
        first_traded(store, config, candidates["symbol"].unique().to_list()),
        on="symbol",
        how="left",
    )
    plan = plan_listing_window(rows, spans)
    report["rows"] = plan.height
    report["symbols"] = plan["symbol"].n_unique()
    report["by_reason"] = dict(plan.group_by("reason").len().sort("reason").iter_rows())
    report["by_source"] = dict(plan.group_by("source").len().sort("source").iter_rows())
    report["by_month"] = {
        month: count
        for month, count in plan.group_by(pl.col("trade_date").dt.strftime("%Y-%m"))
        .len()
        .sort("trade_date")
        .iter_rows()
    }
    if not apply or plan.is_empty():
        return report
    marked = plan.select(_KEY).with_columns(pl.lit(True).alias("_outside"))

    def rebuild(frame: pl.DataFrame) -> pl.DataFrame:
        flagged = frame.join(marked, on=_KEY, how="left")
        hit = pl.col("_outside").fill_null(False)
        if flagged.filter(hit).is_empty():
            return frame
        return flagged.filter(~hit).drop("_outside")

    report.update(
        publish_repair(
            config,
            store,
            DATASET,
            "trade_date",
            base,
            keep=rebuild,
            reason="status_listing_window",
            metadata={"rows": plan.height, "by_reason": report["by_reason"]},
        )
    )
    report["applied"] = True
    return report
