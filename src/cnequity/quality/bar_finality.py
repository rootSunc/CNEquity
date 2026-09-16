"""Detect intraday observations left behind as completed daily bars."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.trading_status import SESSION_FINAL_AT
from cnequity.query.canonical import dedupe_lazy_by_primary_key
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root


def daily_bar_finality_findings(config: Config, end: date) -> list[dict]:
    root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(root):
        return []
    bars = scan_parquet_root(root, partition_col="trade_date", end=end)
    schema = bars.collect_schema()
    if not {"symbol", "trade_date", "volume", "fetched_at"}.issubset(schema.names()):
        return []  # Missing/invalid fields belong to the schema checks.
    fetched = pl.col("fetched_at")
    dtype = schema["fetched_at"]
    if dtype == pl.Utf8:
        fetched = fetched.str.to_datetime(time_zone="UTC", strict=False)
    elif isinstance(dtype, pl.Datetime):
        if dtype.time_zone is None:
            fetched = fetched.dt.replace_time_zone("UTC")
    else:
        return []
    local = fetched.dt.convert_time_zone("Asia/Shanghai")
    suspect = (
        dedupe_lazy_by_primary_key(bars, "daily_bars")
        .filter(
            (pl.col("volume") > 0)
            & (local.dt.date() == pl.col("trade_date"))
            & (local.dt.time() < SESSION_FINAL_AT)
        )
        .select("symbol", "trade_date", "fetched_at")
        .collect(engine="streaming")
    )
    if suspect.is_empty():
        return []
    return [
        {
            "dataset": "daily_bars",
            "severity": "warning",
            "check": "daily_bar_preclose_observation",
            "message": (
                f"{suspect.height} positive-volume daily bar(s) were observed before "
                "their session closed; refresh finalized values even when date keys "
                "and watermarks are complete"
            ),
            "rows": suspect.height,
            "symbols": suspect["symbol"].n_unique(),
            "sample": suspect.sort("trade_date", "symbol").head(8).to_dicts(),
        }
    ]
