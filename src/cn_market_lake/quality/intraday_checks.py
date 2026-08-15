"""Intraday bar checks — session shape, and reconciliation against the daily bars.

A minute series fails differently from a daily one. A daily bar is either there
or missing and a coverage check sees it; a session that quietly lost 40 of its
240 bars, or gained bars at 12:15, still has rows on every trading day and
passes every dataset-level check the lake already runs. Resampling that series
to 5m produces plausible numbers that are wrong.

Three shape checks and one cross-check:

* ``minute_bars_off_session`` — bars outside continuous trading. The adapter
  drops these at parse time, so any that reach curated came from somewhere else
  or from a decode regression. Error.
* ``minute_bars_trade_date_mismatch`` — ``trade_date`` disagreeing with
  ``bar_time``. A-shares have no overnight session, so these cannot differ, and
  if they do the partition column is lying about where the row belongs. Error.
* ``minute_bars_session_coverage`` — symbol-days short of a full session.
  Warning: a genuine intraday halt produces exactly this shape, so it flags a
  pattern to look at rather than a defect.
* ``minute_bars_daily_reconciliation`` — the day's minute volume *and* turnover
  against ``daily_bars``. This is the one check that can catch a wrong
  frequency, a wrong symbol mapping, or a unit slip, because it compares the
  dataset against an independently fetched series rather than against itself.
  Both metrics, because they fail differently: ``volume`` is the column with a
  unit history and so catches a conversion slip, while ``amount`` is yuan from
  every source and so cannot be wrong for a unit reason — a break there means
  the wrong bars, not the wrong scale.
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from cn_market_lake.adapters.tdx_protocol.minute_bars import SESSIONS, bars_per_session
from cn_market_lake.config import Config
from cn_market_lake.domain.datasets import intraday_dataset_names
from cn_market_lake.query.parquet_scan import dataset_has_parquet, scan_parquet_root

# Window scanned back from the audit date — a few sessions is enough to catch a
# regression the day it lands, and keeps the scan off the whole lake.
INTRADAY_CHECK_LOOKBACK_DAYS = 7

# Symbol-days below this share of a full session are reported. Halts are real,
# so this is deliberately loose: it is looking for systematic truncation, not
# for the one stock that stopped trading at 10:00.
SESSION_COVERAGE_MIN_SHARE = 0.9

# Share of symbol-days that must be short before it reads as a pipeline problem
# rather than a handful of halts.
SESSION_COVERAGE_ALERT_SHARE = 0.2

# Minute volume vs daily volume. Wider than the unit check's band because the
# two series are fetched separately and attribute the auctions slightly
# differently; still three orders of magnitude tighter than a 100× unit slip.
RECONCILE_LOW = 0.95
RECONCILE_HIGH = 1.05
RECONCILE_MIN_SYMBOL_DAYS = 20

# daily_bars only stores 股 from data_version v2 on; v1 rows are 手 for some
# sources (cn_market_lake.domain.units). Comparing minute 股 against v1 手 would
# report a 100× break that is really just an un-migrated partition.
DAILY_BARS_SHARES_VERSION = "v2"


def _scan_intraday(config: Config, dataset: str, start: date, end: date) -> pl.LazyFrame | None:
    root = config.curated_root / dataset
    if not dataset_has_parquet(root):
        return None
    return scan_parquet_root(root, partition_col="trade_date", start=start, end=end)


def off_session_findings(lf: pl.LazyFrame, dataset: str, start: date, end: date) -> list[dict]:
    """Bars whose timestamp is not a legal closing minute."""
    legal = pl.lit(False)
    for lo, hi in SESSIONS:
        legal = legal | (
            (pl.col("bar_time").dt.time() >= pl.lit(lo))
            & (pl.col("bar_time").dt.time() <= pl.lit(hi))
        )
    bad = lf.filter(~legal).select("symbol", "bar_time").head(5).collect()
    if bad.is_empty():
        return []
    total = int(lf.filter(~legal).select(pl.len()).collect().item())
    examples = ", ".join(f"{r['symbol']}@{r['bar_time']}" for r in bad.iter_rows(named=True))
    return [
        {
            "dataset": dataset,
            "severity": "error",
            "check": "minute_bars_off_session",
            "message": (
                f"{total} bar(s) in {start}..{end} fall outside continuous trading "
                f"(09:31-11:30, 13:01-15:00); e.g. {examples}"
            ),
            "rows": total,
        }
    ]


def trade_date_mismatch_findings(
    lf: pl.LazyFrame, dataset: str, start: date, end: date
) -> list[dict]:
    """Rows whose partition date disagrees with their timestamp."""
    mismatched = lf.filter(pl.col("bar_time").dt.date() != pl.col("trade_date"))
    total = int(mismatched.select(pl.len()).collect().item())
    if not total:
        return []
    sample = mismatched.select("symbol", "trade_date", "bar_time").head(3).collect()
    examples = ", ".join(
        f"{r['symbol']} trade_date={r['trade_date']} bar_time={r['bar_time']}"
        for r in sample.iter_rows(named=True)
    )
    return [
        {
            "dataset": dataset,
            "severity": "error",
            "check": "minute_bars_trade_date_mismatch",
            "message": (
                f"{total} row(s) in {start}..{end} have trade_date != bar_time date; "
                f"A-shares have no overnight session, so the partition column is "
                f"wrong for these rows. e.g. {examples}"
            ),
            "rows": total,
        }
    ]


def session_coverage_findings(lf: pl.LazyFrame, dataset: str, start: date, end: date) -> list[dict]:
    """Symbol-days holding materially fewer bars than a full session."""
    counts = lf.group_by("symbol", "trade_date", "frequency").agg(pl.len().alias("bars")).collect()
    if counts.is_empty():
        return []

    findings: list[dict] = []
    for frequency in sorted(counts["frequency"].unique().to_list()):
        try:
            expected = bars_per_session(str(frequency))
        except KeyError:
            continue
        subset = counts.filter(pl.col("frequency") == frequency)
        short = subset.filter(pl.col("bars") < expected * SESSION_COVERAGE_MIN_SHARE)
        if short.is_empty():
            continue
        share = short.height / subset.height
        if share < SESSION_COVERAGE_ALERT_SHARE:
            continue
        worst = short.sort("bars").head(3)
        examples = ", ".join(
            f"{r['symbol']} {r['trade_date']} {r['bars']}/{expected}"
            for r in worst.iter_rows(named=True)
        )
        findings.append(
            {
                "dataset": dataset,
                "severity": "warning",
                "check": "minute_bars_session_coverage",
                "message": (
                    f"{frequency}: {short.height}/{subset.height} symbol-day(s) "
                    f"({share:.0%}) in {start}..{end} hold under {SESSION_COVERAGE_MIN_SHARE:.0%} "
                    f"of a {expected}-bar session; e.g. {examples}"
                ),
                "frequency": str(frequency),
                "short_symbol_days": short.height,
                "symbol_days": subset.height,
            }
        )
    return findings


def daily_reconciliation_findings(
    config: Config,
    lf: pl.LazyFrame,
    dataset: str,
    start: date,
    end: date,
) -> list[dict]:
    """Compare each session's minute volume *and* turnover against ``daily_bars``.

    Both, because they fail differently. ``volume`` is the column with a unit
    history (股 vs 手), so it catches a conversion slip; ``amount`` is in yuan
    from every source and so is the one number that cannot be wrong for a unit
    reason — a break there means the wrong bars, not the wrong scale.

    Only v2 daily rows take part: v1 predates the 股 normalisation and would
    report every symbol-day as a 100× break. Too few comparable rows is
    reported as ``info`` rather than skipped silently — a reconciliation that
    never runs is indistinguishable from one that always passes.
    """
    daily_root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(daily_root):
        return []

    daily = (
        scan_parquet_root(daily_root, partition_col="trade_date", start=start, end=end)
        .filter((pl.col("data_version") == DAILY_BARS_SHARES_VERSION) & (pl.col("volume") > 0))
        .select(
            "symbol",
            "trade_date",
            pl.col("volume").alias("daily_volume"),
            pl.col("amount").alias("daily_amount"),
        )
    )
    minute = (
        lf.group_by("symbol", "trade_date")
        .agg(
            pl.col("volume").sum().alias("minute_volume"),
            pl.col("amount").sum().alias("minute_amount"),
        )
        .filter(pl.col("minute_volume") > 0)
    )
    joined = (
        minute.join(daily, on=["symbol", "trade_date"], how="inner")
        .with_columns(
            (pl.col("minute_volume") / pl.col("daily_volume")).alias("volume_ratio"),
            pl.when(pl.col("daily_amount") > 0)
            .then(pl.col("minute_amount") / pl.col("daily_amount"))
            .alias("amount_ratio"),
        )
        .collect()
    )

    if joined.height < RECONCILE_MIN_SYMBOL_DAYS:
        return [
            {
                "dataset": dataset,
                "severity": "info",
                "check": "minute_bars_daily_reconciliation",
                "message": (
                    f"only {joined.height} symbol-day(s) in {start}..{end} could be "
                    f"reconciled against daily_bars (need {RECONCILE_MIN_SYMBOL_DAYS}); "
                    f"daily rows must be data_version={DAILY_BARS_SHARES_VERSION} to be "
                    "comparable in 股"
                ),
                "symbol_days": joined.height,
            }
        ]

    findings: list[dict] = []
    for column, label, unit in (
        ("volume_ratio", "volume", "股"),
        ("amount_ratio", "amount", "yuan"),
    ):
        series = joined[column].drop_nulls()
        if series.is_empty():
            continue
        median = float(series.median())
        if RECONCILE_LOW <= median <= RECONCILE_HIGH:
            continue
        hint = (
            f"minute {label} far exceeds the day's — check for duplicated bars or a wrong frequency"
            if median > RECONCILE_HIGH
            else f"minute {label} falls short of the day's — check for truncated sessions"
        )
        findings.append(
            {
                "dataset": dataset,
                "severity": "warning",
                "check": "minute_bars_daily_reconciliation",
                "message": (
                    f"median minute/daily {label} ({unit}) = {median:.4f} over "
                    f"{len(series)} symbol-day(s) in {start}..{end}; expected ~1.0 — {hint}"
                ),
                "metric": label,
                "median_ratio": median,
                "symbol_days": len(series),
            }
        )
    return findings


def dataset_findings(
    config: Config,
    dataset: str,
    trade_date: date,
    *,
    lookback_days: int = INTRADAY_CHECK_LOOKBACK_DAYS,
) -> list[dict]:
    """Every intraday check for one dataset, or nothing when it is not in use."""
    start = trade_date - timedelta(days=lookback_days)
    lf = _scan_intraday(config, dataset, start, trade_date)
    if lf is None:
        return []
    cols = lf.collect_schema().names()
    if not {"symbol", "trade_date", "bar_time", "frequency", "volume"}.issubset(cols):
        return []
    if lf.select(pl.len()).collect().item() == 0:
        return []

    findings: list[dict] = []
    findings.extend(off_session_findings(lf, dataset, start, trade_date))
    findings.extend(trade_date_mismatch_findings(lf, dataset, start, trade_date))
    findings.extend(session_coverage_findings(lf, dataset, start, trade_date))
    findings.extend(daily_reconciliation_findings(config, lf, dataset, start, trade_date))
    return findings


def minute_bars_findings(
    config: Config,
    trade_date: date,
    *,
    lookback_days: int = INTRADAY_CHECK_LOOKBACK_DAYS,
) -> list[dict]:
    """Every intraday check across every registered intraday dataset.

    Iterates the registry rather than a hardcoded name, so a newly registered
    frequency is audited without a second edit here — the failure mode this
    avoids is a dataset that collects rows nothing ever checks.
    """
    findings: list[dict] = []
    for dataset in sorted(intraday_dataset_names()):
        findings.extend(dataset_findings(config, dataset, trade_date, lookback_days=lookback_days))
    return findings
