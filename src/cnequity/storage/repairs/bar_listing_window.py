"""Remove zero-volume ``daily_bars`` rows dated after a code's delisting.

baostock writes a zero-volume row on the removal date itself (摘牌日), the
first day a security is no longer listed. With ``delist_date`` held to the last
listed session, those rows sit outside the listed life: 206 delisted stocks
carried one each. They are placeholders, not prices, so they go. A traded row
after ``delist_date`` would contradict the date instead; it is reported and
kept.
"""

from __future__ import annotations

import polars as pl

from cnequity.config import Config
from cnequity.file_lock import lake_mutation_lock
from cnequity.query.parquet_scan import scan_parquet_root
from cnequity.storage.repairs.orphan_symbols import _publish
from cnequity.storage.repairs.status_listing_window import instrument_spans
from cnequity.storage.revisions import RevisionStore

DATASET = "daily_bars"
_KEY = ["symbol", "trade_date"]


def plan_bar_listing_window(rows: pl.DataFrame, spans: pl.DataFrame) -> pl.DataFrame:
    """``symbol / trade_date / volume`` of rows dated after the delisting."""
    return rows.join(
        spans.filter(pl.col("delist_date").is_not_null()).select("symbol", "delist_date"),
        on="symbol",
    ).filter(pl.col("trade_date") > pl.col("delist_date"))


def repair_bar_listing_window(config: Config, *, apply: bool = False) -> dict:
    """Plan (default) or publish the removal of placeholder bars after delisting."""
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _repair_locked(config, apply=apply)


def _repair_locked(config: Config, *, apply: bool) -> dict:
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    base = store.repair_base(DATASET, register=apply)
    report: dict = {"applied": False, "rows": 0}
    if base is None:
        report["note"] = "no daily_bars data; nothing to repair"
        return report
    spans = instrument_spans(store, config)
    delisted = spans.filter(pl.col("delist_date").is_not_null())["symbol"].to_list()
    rows = (
        scan_parquet_root(base, partition_col="trade_date", symbols=sorted(delisted))
        .select(*_KEY, "volume")
        .collect()
    )
    after = plan_bar_listing_window(rows, spans)
    plan = after.filter(pl.col("volume").fill_null(0) <= 0)
    traded = after.filter(pl.col("volume") > 0)
    report["rows"] = plan.height
    report["symbols"] = plan["symbol"].n_unique()
    report["traded_after_delisting"] = [
        {"symbol": symbol, "trade_date": day.isoformat()}
        for symbol, day in traded.select(_KEY).sort(_KEY).iter_rows()
    ]
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
        _publish(
            config,
            store,
            DATASET,
            "trade_date",
            base,
            keep=rebuild,
            reason="bar_listing_window",
            metadata={"rows": plan.height, "symbols": report["symbols"]},
        )
    )
    report["applied"] = True
    return report
