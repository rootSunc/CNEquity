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
from datetime import datetime, timezone

import polars as pl

from cnequity.config import Config
from cnequity.file_lock import lake_mutation_lock
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.never_listed import (  # noqa: F401 — re-exported
    RECORD,
    _record_path,
    load_never_listed,
)
from cnequity.storage.read_context import ReadContext, read_root
from cnequity.storage.repairs.orphan_symbols import _publish
from cnequity.storage.revisions import RevisionStore

DATASET = "industry_members"
EVIDENCE = ("instruments", "daily_bars", "trading_status", "announcement_index")


def record_never_listed(config: Config, securities: pl.DataFrame) -> None:
    """Merge ``[symbol, first_class_date, industry_code]`` into the meta record."""
    if securities.is_empty():
        return
    known = {r["symbol"]: r for r in load_never_listed(config)}
    for row in securities.iter_rows(named=True):
        known[row["symbol"]] = {
            "symbol": row["symbol"],
            "first_class_date": str(row["first_class_date"]),
            "industry_code": row["industry_code"],
        }
    write_json_atomic(
        _record_path(config),
        {
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "rule": "no instrument, daily bar, trading-status or announcement record",
            "securities": sorted(known.values(), key=lambda r: r["symbol"]),
        },
        indent=2,
    )


def never_listed(
    config: Config, symbols: list[str], context: ReadContext | None = None
) -> set[str]:
    """Those of ``symbols`` with no record in any of ``EVIDENCE``."""
    left = set(symbols)
    for dataset in EVIDENCE:
        if not left:
            break
        try:
            root = read_root(config, dataset, context)
        except FileNotFoundError:
            continue
        if not dataset_has_parquet(root, committed=False):
            continue
        seen = (
            scan_parquet_root(root, committed=False, symbols=sorted(left))
            .filter(pl.col("symbol").is_in(sorted(left)))
            .select("symbol")
            .unique()
            .collect()["symbol"]
            .to_list()
        )
        left -= set(seen)
    return left


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
            _publish(
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
