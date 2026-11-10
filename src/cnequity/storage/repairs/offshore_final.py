"""Replace offshore ``commodity_bars`` rows stored before their CME session closed.

Sina updates the current session's row intraday, and the daily step used to
store whatever it read: an A-share evening run caught COMEX gold mid-session.
The writer now waits for the session to close and re-reads the last week, but
rows older than that kept their intraday value. On 2026-10-25, 23 of 1,758
stored ``GC0.CMX`` closes differed from Sina's final bars, by up to 3.5%
(2026-08-19: 4425.6 stored, 4580.7 final), and 2026-09-25 was missing.

This reads Sina's final bars once, replaces stored rows whose prices differ and
adds final bars missing inside the stored range. Rows that agree are untouched.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import nullcontext
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.file_lock import lake_mutation_lock
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.read_context import ReadContext
from cnequity.storage.repairs.publish import publish_repair
from cnequity.storage.revisions import RevisionStore

DATASET = "commodity_bars"
PRICES = ("open", "high", "low", "close")


def _offshore_symbols() -> list[str]:
    from cnequity.adapters.sina.global_futures import OFFSHORE_CONTRACTS

    return [c[0] for c in OFFSHORE_CONTRACTS]


def fetch_final_offshore(config: Config, start: date, end: date) -> pl.DataFrame:
    from cnequity.adapters.sina.global_futures import fetch_offshore_commodity_bars_range

    return fetch_offshore_commodity_bars_range(start, end, config=config, strict=True)


def _changes(stored: pl.DataFrame, final: pl.DataFrame) -> pl.DataFrame:
    """Final rows that differ from, or are missing in, ``stored`` within its range."""
    if final.is_empty():
        return final
    lo, hi = stored["trade_date"].min(), stored["trade_date"].max()
    final = final.filter(pl.col("trade_date").is_between(lo, hi))
    key = ["symbol", "trade_date"]
    joined = final.join(
        stored.select(*key, *(pl.col(c).alias(f"__{c}") for c in PRICES)), on=key, how="left"
    )
    differs = pl.any_horizontal(
        pl.col(f"__{c}").is_null() | ((pl.col(c) - pl.col(f"__{c}")).abs() > 1e-9) for c in PRICES
    )
    return joined.filter(differs).select(final.columns)


def repair_offshore_final(
    config: Config,
    *,
    apply: bool = False,
    fetch: Callable[[Config, date, date], pl.DataFrame] = fetch_final_offshore,
) -> dict:
    """Preview, or publish one gated revision with the offshore rows made final."""
    with lake_mutation_lock(config.meta_root) if apply else nullcontext():
        store = RevisionStore(
            config.meta_root, config.curated_root, config.derived_root, create=apply
        )
        if apply:
            store.ensure_current(DATASET)
        context = ReadContext.capture(config, (DATASET,), {})
        root = context.roots[DATASET]
        report: dict = {"dataset": DATASET, "applied": False, "rows": 0, "replaced": 0, "added": 0}
        if not dataset_has_parquet(root, committed=False):
            return report
        symbols = _offshore_symbols()
        stored = (
            scan_parquet_root(root, committed=False)
            .filter(pl.col("symbol").is_in(symbols))
            .select("symbol", "trade_date", *PRICES)
            .collect()
        )
        if stored.is_empty():
            return report
        final = fetch(config, stored["trade_date"].min(), stored["trade_date"].max())
        changes = _changes(stored, final)
        present = stored.select("symbol", "trade_date").with_columns(pl.lit(True).alias("_p"))
        marked = changes.join(present, on=["symbol", "trade_date"], how="left")
        report.update(
            rows=changes.height,
            replaced=marked.filter(pl.col("_p").is_not_null()).height,
            added=marked.filter(pl.col("_p").is_null()).height,
            sample=changes.select("symbol", pl.col("trade_date").cast(pl.Utf8), "close")
            .head(10)
            .to_dicts(),
        )
        if not apply or changes.is_empty():
            return report
        rows = with_provenance(
            changes.drop("source", strict=False),
            source="sina",
            data_version=data_version_for(DATASET),
        )

        def keep(part: pl.DataFrame) -> pl.DataFrame:
            days = part["trade_date"]
            mine = rows.filter(
                pl.col("trade_date").dt.year().is_in(days.dt.year().unique().to_list())
            )
            if mine.is_empty():
                return part
            key = ["symbol", "trade_date"]
            kept = part.join(mine.select(key), on=key, how="anti")
            return pl.concat([kept, mine.select(part.columns)], how="vertical_relaxed").sort(
                "trade_date", "symbol"
            )

        report.update(
            publish_repair(
                config,
                store,
                DATASET,
                "trade_date",
                root,
                keep=keep,
                reason="offshore_final",
                metadata={"replaced": report["replaced"], "added": report["added"]},
            )
        )
        report["applied"] = True
        return report
