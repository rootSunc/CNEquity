"""Keep Shenwan classes of securities that never listed out of ``industry_members``.

Shenwan assigns a class when an IPO is pending, and its file has no end dates,
so a company whose IPO never completed stays in every later snapshot. On
2026-10-25, 12 Shenwan codes (classed between 2010 and 2023, e.g. 002525.SZ,
601206.SH, 874090.BJ) had no instrument, daily bar, trading-status row or
announcement anywhere in the lake, and EastMoney listed none of them.

A security is dropped only when all four are absent; one genuinely missing from
``instruments`` but trading still shows through its other records. The dropped
ones are recorded in ``meta/sw_never_listed.json`` so ``cne verify --industry``
keeps reporting them.
"""

from __future__ import annotations

from contextlib import nullcontext

import polars as pl

from cnequity.config import Config
from cnequity.file_lock import lake_mutation_lock
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.never_listed import (  # noqa: F401 — re-exported
    EVIDENCE,
    RECORD,
    load_never_listed,
    never_listed,
    record_never_listed,
)
from cnequity.storage.read_context import ReadContext
from cnequity.storage.repairs.publish import publish_repair
from cnequity.storage.revisions import RevisionStore

DATASET = "industry_members"


def repair_sw_never_listed(config: Config, *, apply: bool = False) -> dict:
    """Preview, or publish one gated revision without never-listed Shenwan rows."""
    with lake_mutation_lock(config.meta_root) if apply else nullcontext():
        store = RevisionStore(
            config.meta_root, config.curated_root, config.derived_root, create=apply
        )
        if apply:
            store.ensure_current(DATASET)
        context = ReadContext.capture(config, (DATASET, *EVIDENCE), {})
        root = context.roots[DATASET]
        report: dict = {"dataset": DATASET, "applied": False, "rows": 0, "symbols": []}
        if not dataset_has_parquet(root, committed=False):
            return report
        sw = (
            scan_parquet_root(root, committed=False)
            .filter(pl.col("classification_system") == "sw")
            .group_by("symbol")
            .agg(
                pl.len().alias("rows"),
                pl.col("as_of_date").min().alias("first_class_date"),
                pl.col("industry_code").sort_by("as_of_date").last(),
            )
            .collect()
        )
        gone = never_listed(config, sw["symbol"].to_list(), context)
        hits = sw.filter(pl.col("symbol").is_in(sorted(gone))).sort("symbol")
        report.update(rows=int(hits["rows"].sum()), symbols=hits["symbol"].to_list())
        if not apply or hits.is_empty():
            return report
        drop = set(hits["symbol"].to_list())

        def keep(part: pl.DataFrame) -> pl.DataFrame:
            mask = (pl.col("classification_system") == "sw") & pl.col("symbol").is_in(sorted(drop))
            kept = part.filter(~mask)
            return part if kept.height == part.height else kept

        report.update(
            publish_repair(
                config,
                store,
                DATASET,
                "as_of_date",
                root,
                keep=keep,
                reason="sw_never_listed",
                metadata={"symbols": sorted(drop)},
            )
        )
        record_never_listed(config, hits.select("symbol", "first_class_date", "industry_code"))
        report["applied"] = True
        return report
