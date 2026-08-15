"""Market breadth metrics computed from curated daily_bars."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from cn_market_lake.config import Config

_METRICS = (
    "advance_count",
    "decline_count",
    "flat_count",
    "limit_up_count",
    "limit_down_count",
    "advance_ratio",
    "total_count",
)


def _read_bars(root: Path, trade_date: date) -> pl.DataFrame:
    if not root.exists():
        return pl.DataFrame()
    files = list(root.glob(f"trade_date={trade_date.isoformat()}/**/*.parquet"))
    if not files:
        files = list(root.glob("**/*.parquet"))
    if not files:
        return pl.DataFrame()
    df = pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")
    return df.filter(pl.col("trade_date") == trade_date)


def _prev_trading_date(config: Config, trade_date: date) -> date | None:
    cal_root = config.curated_root / "trading_calendar"
    if not cal_root.exists():
        return None
    files = list(cal_root.glob("**/*.parquet"))
    if not files:
        return None
    cal = pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")
    prior = cal.filter((pl.col("trade_date") < trade_date) & pl.col("is_trading")).sort(
        "trade_date", descending=True
    )
    if prior.is_empty():
        return None
    return prior["trade_date"][0]


def compute_market_breadth(config: Config, trade_date: date) -> pl.DataFrame:
    bars_root = config.curated_root / "daily_bars"
    today = _read_bars(bars_root, trade_date)
    if today.is_empty():
        return pl.DataFrame()

    prev_date = _prev_trading_date(config, trade_date)
    if prev_date is None:
        return pl.DataFrame()

    prev = _read_bars(bars_root, prev_date)
    if prev.is_empty():
        return pl.DataFrame()

    joined = today.select(["symbol", "close", "trade_date"]).join(
        prev.select(["symbol", pl.col("close").alias("prev_close")]),
        on="symbol",
        how="inner",
    )
    joined = joined.with_columns(
        ((pl.col("close") - pl.col("prev_close")) / pl.col("prev_close")).alias("pct")
    )
    joined = joined.filter(pl.col("prev_close") > 0)

    total = joined.height
    advance = joined.filter(pl.col("pct") > 0).height
    decline = joined.filter(pl.col("pct") < 0).height
    flat = joined.filter(pl.col("pct") == 0).height
    limit_up = joined.filter(pl.col("pct") >= 0.095).height
    limit_down = joined.filter(pl.col("pct") <= -0.095).height
    ratio = advance / total if total else 0.0

    values = {
        "advance_count": float(advance),
        "decline_count": float(decline),
        "flat_count": float(flat),
        "limit_up_count": float(limit_up),
        "limit_down_count": float(limit_down),
        "advance_ratio": ratio,
        "total_count": float(total),
    }
    rows = [
        {"trade_date": trade_date, "metric_id": metric_id, "value": val}
        for metric_id, val in values.items()
    ]
    return pl.DataFrame(rows)
