"""Drop Shenwan ``industry_members`` rows dated on or after the stock's delisting.

The Shenwan file gives each stock's classes with start dates and no end dates.
Expanding it into monthly snapshots kept a delisted stock's last class in every
later month, so snapshots carried stocks gone for years. Those rows are this
lake's own expansion, not something Shenwan published for that date. EastMoney
rows are source observations and are left alone; ``cne verify --industry``
reports any that fall outside a listed life.
"""

from __future__ import annotations

from contextlib import nullcontext

import polars as pl

from cnequity.config import Config
from cnequity.file_lock import lake_mutation_lock
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.read_context import ReadContext
from cnequity.storage.repairs.publish import publish_repair
from cnequity.storage.revisions import RevisionStore

DATASET = "industry_members"


def _delisted(config: Config, context: ReadContext) -> pl.DataFrame:
    root = context.roots["instruments"]
    if not dataset_has_parquet(root, committed=False):
        return pl.DataFrame(schema={"symbol": pl.Utf8, "delist_date": pl.Date})
    from cnequity.domain.canonical import dedupe_lazy_by_primary_key

    return (
        dedupe_lazy_by_primary_key(scan_parquet_root(root, committed=False), "instruments")
        .filter(pl.col("delist_date").is_not_null())
        .select("symbol", "delist_date")
        .collect()
    )


def _after_delisting() -> pl.Expr:
    return (pl.col("classification_system") == "sw") & (
        pl.col("as_of_date") >= pl.col("delist_date")
    )


def drop_after_delisting(frame: pl.DataFrame, delisted: pl.DataFrame) -> pl.DataFrame:
    joined = frame.join(delisted, on="symbol", how="left")
    kept = joined.filter(~_after_delisting().fill_null(False))
    if kept.height == frame.height:
        return frame
    return kept.select(frame.columns)


def repair_sw_delisted(config: Config, *, apply: bool = False) -> dict:
    """Preview, or publish one gated revision without the post-delisting Shenwan rows."""
    with lake_mutation_lock(config.meta_root) if apply else nullcontext():
        store = RevisionStore(
            config.meta_root, config.curated_root, config.derived_root, create=apply
        )
        if apply:
            store.ensure_current(DATASET)
        context = ReadContext.capture(config, (DATASET, "instruments"), {})
        root = context.roots[DATASET]
        report: dict = {"dataset": DATASET, "applied": False, "rows": 0, "symbols": 0}
        if not dataset_has_parquet(root, committed=False):
            return report
        delisted = _delisted(config, context)
        hits = (
            scan_parquet_root(root, committed=False)
            .join(delisted.lazy(), on="symbol", how="inner")
            .filter(_after_delisting())
            .select("symbol")
            .collect()
        )
        report.update(rows=hits.height, symbols=hits["symbol"].n_unique())
        if not apply or not hits.height:
            return report
        report.update(
            publish_repair(
                config,
                store,
                DATASET,
                "as_of_date",
                root,
                keep=lambda part: drop_after_delisting(part, delisted),
                reason="sw_delisted",
                metadata={"rows": hits.height, "symbols": report["symbols"]},
            )
        )
        report["applied"] = True
        return report
