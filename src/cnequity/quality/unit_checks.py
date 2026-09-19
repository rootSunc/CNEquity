"""Traded-quantity unit checks — the guard against a 100× regression.

``daily_bars.volume`` is 股 for every source (:mod:`cnequity.domain.units`),
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

from cnequity.config import Config
from cnequity.query.canonical import dedupe_lazy_by_primary_key
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

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

AMOUNT_COMPLETENESS_MIN_ROWS = 20


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
    lf = dedupe_lazy_by_primary_key(
        scan_parquet_root(root, partition_col="trade_date", start=start, end=trade_date),
        "daily_bars",
    )
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
        .collect(engine="streaming")
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


# A traded day's turnover divided by its share count is an average execution
# price, so it has to sit inside that day's own range. One percent of slack
# covers rounding in the published figures without admitting a real break.
IMPLIED_PRICE_SLACK = 0.01

# Per finding, not per row: 255 fund rows must not bury 8 stock ones.
IMPLIED_PRICE_SAMPLE = 6


def daily_bars_implied_price_findings(
    config: Config,
    trade_date: date,
    *,
    lookback_days: int = UNIT_CHECK_LOOKBACK_DAYS,
) -> list[dict]:
    """Rows whose own turnover, share count and range disagree.

    `daily_bars_volume_unit_findings` takes a median per source, which is the
    right shape for a source that rescaled every row and the wrong one for a
    source that got a few rows wrong: the median stays at 1.0 and the broken
    rows are invisible. This asks the question of each row on its own, and
    needs no second source to answer it.

    Measured over 2026-09-01..09-18: 255 of 16,949 ETF/LOF days and 8 of 77,324
    stock days. 160806.SZ on 2026-09-11 is the shape of it — amount 2,825.6
    agreeing to the cent with the minute stream, range 2.016..2.032, and volume
    154,400, an implied 0.0183 per share. The cross-source check that did see
    this read it as the *minute* data being wrong.
    """
    findings: list[dict] = []
    root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(root):
        return findings

    start = trade_date - timedelta(days=lookback_days)
    lf = dedupe_lazy_by_primary_key(
        scan_parquet_root(root, partition_col="trade_date", start=start, end=trade_date),
        "daily_bars",
    )
    cols = lf.collect_schema().names()
    if not {"symbol", "trade_date", "volume", "amount", "low", "high", "source"}.issubset(cols):
        return findings

    broken = (
        lf.filter(
            (pl.col("volume") > 0)
            & (pl.col("amount") > 0)
            & (pl.col("low") > 0)
            & (pl.col("high") >= pl.col("low"))
        )
        .with_columns((pl.col("amount") / pl.col("volume")).alias("_px"))
        .filter(
            (pl.col("_px") < pl.col("low") * (1 - IMPLIED_PRICE_SLACK))
            | (pl.col("_px") > pl.col("high") * (1 + IMPLIED_PRICE_SLACK))
        )
        .select("symbol", "trade_date", "low", "high", "volume", "amount", "_px", "source")
        .collect(engine="streaming")
    )
    if broken.is_empty():
        return findings

    instruments = _instrument_classes(config)
    if instruments is not None:
        broken = broken.join(instruments, on="symbol", how="left")
    else:
        broken = broken.with_columns(pl.lit(None, dtype=pl.Utf8).alias("asset_type"))
    broken = broken.with_columns(pl.col("asset_type").fill_null("unknown"))

    for asset_type in sorted(broken.get_column("asset_type").unique().to_list()):
        rows = broken.filter(pl.col("asset_type") == asset_type).sort(
            ["trade_date", "symbol"], descending=[True, False]
        )
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_implied_price",
                "message": (
                    f"{rows.height} {asset_type} day(s) in "
                    f"{start.isoformat()}..{trade_date.isoformat()} where amount/volume falls "
                    "outside that day's own low..high, so the row disagrees with itself; "
                    "turnover and price are corroborated by the minute stream where one "
                    "exists, which leaves volume as the column to doubt"
                ),
                "asset_type": asset_type,
                "rows": rows.height,
                "sources": sorted(rows.get_column("source").unique().to_list()),
                "window_start": start.isoformat(),
                "window_end": trade_date.isoformat(),
                "sample": [
                    {
                        "symbol": item["symbol"],
                        "trade_date": item["trade_date"].isoformat(),
                        "low": float(item["low"]),
                        "high": float(item["high"]),
                        "implied_price": round(float(item["_px"]), 4),
                        "volume": int(item["volume"]),
                        "source": item["source"],
                    }
                    for item in rows.head(IMPLIED_PRICE_SAMPLE).iter_rows(named=True)
                ],
            }
        )
    return findings


def _instrument_classes(config: Config) -> pl.DataFrame | None:
    """symbol -> asset_type, so one class's breakage cannot hide another's."""
    root = config.curated_root / "instruments"
    if not dataset_has_parquet(root):
        return None
    frame = scan_parquet_root(root).collect()
    if not {"symbol", "asset_type"}.issubset(frame.columns):
        return None
    return frame.select("symbol", "asset_type").unique(subset=["symbol"], keep="last")


def daily_bars_amount_completeness_findings(
    config: Config,
    trade_date: date,
    *,
    lookback_days: int = UNIT_CHECK_LOOKBACK_DAYS,
) -> list[dict]:
    """Report turnover coverage by source instead of treating null as zero.

    Sina intentionally has no amount field, but consumers still need to know
    that liquidity features are incomplete for those rows. Other sources with
    unexpected null turnover are surfaced by the same check.
    """
    findings: list[dict] = []
    root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(root):
        return findings

    start = trade_date - timedelta(days=lookback_days)
    lf = dedupe_lazy_by_primary_key(
        scan_parquet_root(root, partition_col="trade_date", start=start, end=trade_date),
        "daily_bars",
    )
    cols = lf.collect_schema().names()
    if not {"amount", "source"}.issubset(cols):
        return findings
    if "volume" in cols:
        # A suspension placeholder's amount=0 is a valid no-trade marker, not
        # evidence that the source supplied a complete turnover field. Keep
        # the sample-size gate tied to real traded rows, as the unit check is.
        lf = lf.filter(pl.col("volume") > 0)

    stats = (
        lf.group_by("source")
        .agg(
            pl.len().alias("rows"),
            pl.col("amount").is_null().sum().alias("missing_amount"),
        )
        .collect(engine="streaming")
    )
    for row in stats.sort("source").iter_rows(named=True):
        rows = int(row["rows"])
        missing = int(row["missing_amount"])
        if rows < AMOUNT_COMPLETENESS_MIN_ROWS or not missing:
            continue
        ratio = missing / rows
        source = str(row["source"])
        expected = (
            "expected for Sina (source does not publish turnover)"
            if source == "sina"
            else ("unexpected for this source")
        )
        findings.append(
            {
                "dataset": "daily_bars",
                # Sina's daily-kline contract has no turnover field. Keep the
                # completeness finding visible for downstream liquidity
                # consumers, but do not present a documented source limit as
                # an operational ingest warning. Any other source remains a
                # warning because its adapter is expected to supply amount.
                "severity": "info" if source == "sina" else "warning",
                "check": "daily_bars_amount_completeness",
                "message": (
                    f"source={source}: {missing}/{rows} row(s) have null amount "
                    f"({ratio:.1%}) over {start.isoformat()}..{trade_date.isoformat()}; {expected}"
                ),
                "source": source,
                "expected_missing": source == "sina",
                "rows": rows,
                "missing_amount": missing,
                "missing_ratio": ratio,
                "window_start": start.isoformat(),
                "window_end": trade_date.isoformat(),
                "source_limited": source == "sina",
            }
        )
    return findings


# A price-to-something ratio lives in single or low double digits. Even a loss
# -making company priced on a sliver of earnings rarely clears four figures, and
# nothing legitimate reaches the millions.
RATIO_PLAUSIBLE_MAX = 1000.0
# One outlier is a business fact; a majority is a wrong field.
RATIO_IMPLAUSIBLE_SHARE = 0.5


def valuation_ratio_unit_findings(config: Config, trade_date: date) -> list[dict]:
    """Valuation columns that hold an amount rather than a ratio.

    ``valuation_metrics.ps_ttm`` is stored per source, and one of them writes a
    figure in yuan: measured over the whole dataset, 96.7% of the EastMoney rows
    exceed 1000 with a median of 1.99e7, against 99.9% plausible and a median of
    3.2 from baostock. A licensed third source reads 600519.SH at 9.20 where the
    lake says 4.45e10. The adapter asks for EastMoney field ``f45``, which is a
    profit or revenue figure and not 市销率 at all.

    Grouped by source for the same reason ``daily_bars_volume_unit_findings``
    is: a mixed column has a median near neither, and one bad adapter among
    several healthy ones is otherwise outvoted.

    This is a shape test, not a value test, so it needs no second source to run
    — which is the point. The break was visible in the numbers themselves for as
    long as they have been stored.
    """
    root = config.curated_root / "valuation_metrics"
    if not dataset_has_parquet(root):
        return []
    columns = ("pe_ttm", "pb", "ps_ttm")
    frame = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(root, partition_col="trade_date", end=trade_date),
            "valuation_metrics",
        )
        .select("source", *columns)
        .collect()
    )
    if frame.is_empty():
        return []

    findings: list[dict] = []
    for column in columns:
        scored = (
            frame.select("source", column)
            .drop_nulls(column)
            .group_by("source")
            .agg(
                pl.len().alias("rows"),
                (pl.col(column).abs() > RATIO_PLAUSIBLE_MAX).mean().alias("share"),
                pl.col(column).abs().median().alias("median"),
            )
            .filter(pl.col("share") > RATIO_IMPLAUSIBLE_SHARE)
        )
        for row in scored.iter_rows(named=True):
            findings.append(
                {
                    "dataset": "valuation_metrics",
                    "severity": "error",
                    "check": "valuation_ratio_unit",
                    "message": (
                        f"{column} from {row['source']}: {row['share']:.1%} of "
                        f"{row['rows']:,} values exceed {RATIO_PLAUSIBLE_MAX:.0f} "
                        f"(median {row['median']:.4g}) — an amount, not a ratio"
                    ),
                    "column": column,
                    "source": row["source"],
                    "rows": int(row["rows"]),
                    "implausible_share": round(float(row["share"]), 4),
                    "median": float(row["median"]),
                }
            )
    return findings
