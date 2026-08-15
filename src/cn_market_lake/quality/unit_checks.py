"""Traded-quantity unit checks — the guard against a 100× regression.

``daily_bars.volume`` is 股 for every source (:mod:`cn_market_lake.domain.units`),
but nothing in a vendor payload declares its unit, so an adapter that stops
converting, or a new one that never started, writes numbers that are wrong by
exactly 100 while looking entirely plausible. Row counts, PK uniqueness, OHLC
ordering and calendar coverage all still pass. That is how the break got in.

The identity that does notice is ``amount ≈ close × volume``: a share count
priced at the day's close should reproduce the day's turnover. It is not exact
— ``close`` is the last print, not the session VWAP — but across a whole
partition the median lands within a percent of 1.0, which leaves three orders
of magnitude of headroom before a unit error could hide in it. Measured over
the curated lake, per-source medians were 0.999 (ths), 1.000 (baostock) and
100.000 (tdx_protocol, pre-fix).

Grouped **by source**, deliberately: a mixed-unit column has a median near
neither 1 nor 100, and one bad adapter among four healthy ones can be
outvoted market-wide. Per source, the offender is named.

Two blind spots, both recorded rather than papered over:

* sina serves no ``amount``, so its rows are unmeasurable here. It is the one
  daily_bars path this check cannot see.
* index_bars and sector_bars are out of scope. Their ``close`` is an index
  level, not a per-share price, so the identity has no meaning there —
  running it anyway would produce a ratio of 36 on healthy data.
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.query.parquet_scan import dataset_has_parquet, scan_parquet_root

# Window scanned back from the audit date. Long enough that a quiet market or a
# short holiday still leaves a usable sample, short enough to stay cheap.
UNIT_CHECK_LOOKBACK_DAYS = 30

# Rows a source needs in the window before its median is worth judging. A tip
# gap-fill can contribute a handful of rows; a handful of ratios is noise.
UNIT_CHECK_MIN_ROWS = 200

# Median ratio band. The observed medians sit within 0.1% of 1.0, so ±20% is
# ~200× the real dispersion — it cannot fire on market conditions, and any
# power-of-100 mistake is far outside it.
UNIT_CHECK_RATIO_LOW = 0.8
UNIT_CHECK_RATIO_HIGH = 1.25


def _describe(ratio: float) -> str:
    """Name the likely mistake behind an off-band ratio."""
    if ratio >= 50.0:
        return "volume looks like 手 (lots) — a factor of ~100 too small"
    if ratio <= 0.02:
        return "volume looks ~100× too large for the turnover on record"
    return "volume does not reconcile against amount / close"


def daily_bars_volume_unit_findings(
    config: Config,
    trade_date: date,
    *,
    lookback_days: int = UNIT_CHECK_LOOKBACK_DAYS,
) -> list[dict]:
    """Flag any source whose ``daily_bars.volume`` is not in 股.

    One finding per offending source, at ``error`` — a unit break silently
    rescales every turnover and liquidity factor built on the column, so it is
    not something to warn about and move past.
    """
    findings: list[dict] = []
    root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(root):
        return findings

    start = trade_date - timedelta(days=lookback_days)
    lf = scan_parquet_root(root, partition_col="trade_date", start=start, end=trade_date)
    cols = lf.collect_schema().names()
    if not {"volume", "amount", "close", "source"}.issubset(cols):
        return findings

    stats = (
        lf.filter(
            (pl.col("volume") > 0)
            & (pl.col("amount") > 0)
            & (pl.col("close") > 0)
            & pl.col("amount").is_not_null()
            & pl.col("close").is_not_null()
        )
        .with_columns((pl.col("amount") / pl.col("close") / pl.col("volume")).alias("_ratio"))
        .group_by("source")
        .agg(
            pl.len().alias("rows"),
            pl.col("_ratio").median().alias("median_ratio"),
        )
        .collect()
    )

    for row in stats.sort("source").iter_rows(named=True):
        rows = int(row["rows"])
        ratio = row["median_ratio"]
        if rows < UNIT_CHECK_MIN_ROWS or ratio is None:
            continue
        ratio = float(ratio)
        if UNIT_CHECK_RATIO_LOW <= ratio <= UNIT_CHECK_RATIO_HIGH:
            continue
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "error",
                "check": "daily_bars_volume_unit",
                "message": (
                    f"source={row['source']}: median amount/close/volume = {ratio:.4f} "
                    f"over {rows} row(s) in {start.isoformat()}..{trade_date.isoformat()}; "
                    f"expected ~1.0 because daily_bars.volume is 股 — {_describe(ratio)}"
                ),
                "source": row["source"],
                "median_ratio": ratio,
                "rows": rows,
                "window_start": start.isoformat(),
                "window_end": trade_date.isoformat(),
            }
        )
    return findings
