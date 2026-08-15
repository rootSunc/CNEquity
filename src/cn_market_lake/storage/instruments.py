"""Merge-style compact for instruments (preserve delisted symbols)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from cn_market_lake.domain.schemas import INSTRUMENTS_SCHEMA, validate_dataframe
from cn_market_lake.storage.atomic import write_parquet_atomic
from cn_market_lake.storage.parquet import StagingWriter

# Refuse delist inference when too many symbols vanish from a snapshot — usually
# a partial TDX fetch, not a mass delisting event.
ABSENT_DELIST_THRESHOLD = 0.05


def compact_instruments(
    staging_root: Path,
    curated_root: Path,
    run_id: str,
    trade_date: date,
) -> tuple[int, list[dict]]:
    """Merge staging instruments into curated, retaining symbols missing from TDX."""
    staging = StagingWriter(staging_root)
    files = staging.list_run_files("instruments", run_id)
    if not files:
        return 0, []

    incoming = pl.concat(
        [validate_dataframe(pl.read_parquet(f), "instruments") for f in files],
        how="diagonal_relaxed",
    )
    incoming = incoming.sort("fetched_at").unique(subset=["symbol"], keep="last")

    out_path = curated_root / "instruments" / "part-merged.parquet"
    if out_path.exists():
        existing = validate_dataframe(pl.read_parquet(out_path), "instruments")
    else:
        existing = pl.DataFrame(schema=INSTRUMENTS_SCHEMA)

    incoming_symbols = incoming["symbol"].to_list()
    findings: list[dict] = []
    if not existing.is_empty():
        preserved = existing.filter(~pl.col("symbol").is_in(incoming_symbols))
        absent_count = preserved.height
        absent_ratio = absent_count / existing.height
        if absent_count and absent_ratio > ABSENT_DELIST_THRESHOLD:
            findings.append(
                {
                    "dataset": "instruments",
                    "severity": "error",
                    "check": "instruments_delist_suppressed",
                    "message": (
                        f"Refused to infer delist_date: {absent_count}/{existing.height} symbols "
                        f"({absent_ratio:.1%}) absent from snapshot (>{ABSENT_DELIST_THRESHOLD:.0%} "
                        "threshold); likely partial fetch"
                    ),
                    "absent_count": absent_count,
                    "existing_count": existing.height,
                    "absent_ratio": absent_ratio,
                }
            )
        else:
            preserved = preserved.with_columns(
                pl.when(pl.col("delist_date").is_null())
                .then(pl.lit(trade_date))
                .otherwise(pl.col("delist_date"))
                .alias("delist_date")
            )
        prior_dates = existing.select(
            [
                "symbol",
                pl.col("list_date").alias("_prior_list_date"),
                pl.col("delist_date").alias("_prior_delist_date"),
            ]
        )
        incoming = incoming.join(prior_dates, on="symbol", how="left")
        # Both dates are sticky: a live snapshot carries neither (TDX has no such
        # field), so coalescing is what keeps a delist_date — inferred from an
        # earlier absence or fetched from baostock — from being erased the next
        # day. Never resurrect a name a prior run buried.
        incoming = incoming.with_columns(
            pl.coalesce(pl.col("list_date"), pl.col("_prior_list_date")).alias("list_date"),
            pl.coalesce(pl.col("delist_date"), pl.col("_prior_delist_date")).alias("delist_date"),
        ).drop("_prior_list_date", "_prior_delist_date")
    else:
        preserved = pl.DataFrame(schema=INSTRUMENTS_SCHEMA)

    merged = pl.concat([incoming, preserved], how="diagonal_relaxed")
    merged = merged.sort("fetched_at").unique(subset=["symbol"], keep="last")

    write_parquet_atomic(out_path, merged, compression="zstd")
    return merged.height, findings
