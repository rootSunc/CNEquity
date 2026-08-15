"""Transaction-record checks — session shape, sequence integrity, and daily reconciliation.

``trade_ticks`` fails in ways neither the generic dataset checks nor the
intraday bar checks can see. The generic checks confirm rows exist on every
partition; a session that lost its whole morning still has rows. The intraday
checks key on ``bar_time`` and a bar count per session, and this dataset has
neither — which is exactly why it does not carry ``intraday_frequency`` and why
these live in their own module rather than being bolted onto
``intraday_checks``.

What can go wrong here that nowhere else can:

* ``trade_ticks_seq_gaps`` — ``tick_seq`` is the primary key *and* the row's
  meaning, so a hole in it is not a missing row, it is a session whose
  sequence numbers no longer describe the order they claim to. Error.
* ``trade_ticks_truncated_session`` — the wire pages backwards from the close,
  so a session that failed to assemble loses its **open**, not its end. A
  session whose first record is not the 09:25 auction is the signature. Warning:
  a name that genuinely first trades at 10:30 produces the same shape.
* ``trade_ticks_off_session`` — records outside the four windows the source was
  measured to use. Error: the adapter refuses to emit these, so any that reach
  curated came from a decode regression.
* ``trade_ticks_direction_mix`` — an unrecognised direction code, or a buy/sell
  split so lopsided it means the field moved rather than the market. Warning.
* ``trade_ticks_daily_reconciliation`` — the session's volume and turnover
  against ``daily_bars``. The one check that compares this dataset against an
  independently fetched series rather than against itself.
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from cn_market_lake.adapters.tdx_protocol.trade_ticks import (
    AFTER_HOURS,
    SESSIONS,
    UNKNOWN_DIRECTION,
)
from cn_market_lake.config import Config
from cn_market_lake.query.parquet_scan import dataset_has_parquet, scan_parquet_root

DATASET = "trade_ticks"

# Window scanned back from the audit date. A few sessions catches a regression
# the day it lands without scanning the whole lake.
TICK_CHECK_LOOKBACK_DAYS = 7

# Reconciliation bands. Tighter than the minute bars' ±5% because these numbers
# were measured: excluding after-hours rows, volume reconciled at 1.000000 on
# five of six symbols and 1.000018 at worst, and turnover within ±0.03%.
#
# Volume is the tighter of the two because the records sum to the day exactly.
# Turnover cannot: one representative price stands for every trade folded into
# a 3-second frame, so `price × volume` is close but never identical.
RECONCILE_VOLUME_LOW = 0.998
RECONCILE_VOLUME_HIGH = 1.002
RECONCILE_AMOUNT_LOW = 0.995
RECONCILE_AMOUNT_HIGH = 1.005
RECONCILE_MIN_SYMBOL_DAYS = 20

# daily_bars stores 股 only from data_version v2 on; v1 rows are 手 for some
# sources. Comparing against v1 would report a 100x break that is really an
# un-migrated partition.
DAILY_BARS_SHARES_VERSION = "v2"

# Share of symbol-days that must be missing their opening auction before it
# reads as a pipeline problem rather than a few late-starting names.
#
# All 38 symbols measured opened with a 09:25 record, the least-traded ordinary
# stock of that session included, so the honest threshold is close to zero. It
# sits at 10% instead because a truncation bug would hit most of a sweep at
# once, while a suspended or newly-listed name legitimately produces this shape
# and should not page anyone.
TRUNCATION_ALERT_SHARE = 0.10

# A session whose buy/sell split is beyond this is not a market, it is a bug.
# Measured spread across 36 symbols: 0.44 to 0.62.
DIRECTION_SKEW_LIMIT = 0.9


def _scan(config: Config, start: date, end: date) -> pl.LazyFrame | None:
    root = config.curated_root / DATASET
    if not dataset_has_parquet(root):
        return None
    return scan_parquet_root(root, partition_col="trade_date", start=start, end=end)


def seq_gap_findings(lf: pl.LazyFrame, start: date, end: date) -> list[dict]:
    """Symbol-days whose ``tick_seq`` is not a dense 0..n-1 run."""
    per_day = (
        lf.group_by("symbol", "trade_date")
        .agg(
            pl.len().alias("rows"),
            pl.col("tick_seq").min().alias("lo"),
            pl.col("tick_seq").max().alias("hi"),
            pl.col("tick_seq").n_unique().alias("distinct"),
        )
        .filter(
            (pl.col("lo") != 0)
            | (pl.col("hi") != pl.col("rows") - 1)
            | (pl.col("distinct") != pl.col("rows"))
        )
        .collect()
    )
    if per_day.is_empty():
        return []
    worst = per_day.head(3)
    examples = ", ".join(
        f"{r['symbol']} {r['trade_date']} ({r['rows']} rows, seq {r['lo']}..{r['hi']})"
        for r in worst.iter_rows(named=True)
    )
    return [
        {
            "dataset": DATASET,
            "severity": "error",
            "check": "trade_ticks_seq_gaps",
            "message": (
                f"{per_day.height} symbol-day(s) in {start}..{end} have a tick_seq that "
                f"is not a dense 0..n-1 run; the sequence no longer describes the order "
                f"it claims to. e.g. {examples}"
            ),
            "symbol_days": per_day.height,
        }
    ]


def off_session_findings(lf: pl.LazyFrame, start: date, end: date) -> list[dict]:
    """Records outside the windows the source was measured to use."""
    legal = pl.lit(False)
    for lo, hi in SESSIONS:
        legal = legal | (
            (pl.col("trade_time").dt.time() >= pl.lit(lo))
            & (pl.col("trade_time").dt.time() <= pl.lit(hi))
        )
    bad = lf.filter(~legal)
    total = int(bad.select(pl.len()).collect().item())
    if not total:
        return []
    sample = bad.select("symbol", "trade_time").head(3).collect()
    examples = ", ".join(f"{r['symbol']}@{r['trade_time']}" for r in sample.iter_rows(named=True))
    return [
        {
            "dataset": DATASET,
            "severity": "error",
            "check": "trade_ticks_off_session",
            "message": (
                f"{total} record(s) in {start}..{end} fall outside the trading windows "
                f"(09:25, 09:30-11:30, 13:00-15:00, 15:05-15:30); the adapter refuses to "
                f"emit these, so they came from a decode regression. e.g. {examples}"
            ),
            "rows": total,
        }
    ]


def trade_date_mismatch_findings(lf: pl.LazyFrame, start: date, end: date) -> list[dict]:
    """Rows whose partition date disagrees with their timestamp."""
    mismatched = lf.filter(pl.col("trade_time").dt.date() != pl.col("trade_date"))
    total = int(mismatched.select(pl.len()).collect().item())
    if not total:
        return []
    sample = mismatched.select("symbol", "trade_date", "trade_time").head(3).collect()
    examples = ", ".join(
        f"{r['symbol']} trade_date={r['trade_date']} trade_time={r['trade_time']}"
        for r in sample.iter_rows(named=True)
    )
    return [
        {
            "dataset": DATASET,
            "severity": "error",
            "check": "trade_ticks_trade_date_mismatch",
            "message": (
                f"{total} row(s) in {start}..{end} have trade_date != trade_time date; "
                f"A-shares have no overnight session, so the partition column is wrong "
                f"for these rows. e.g. {examples}"
            ),
            "rows": total,
        }
    ]


def truncation_findings(lf: pl.LazyFrame, start: date, end: date) -> list[dict]:
    """Sessions that do not begin with the opening auction.

    The direction of the failure is what makes this worth checking: pages walk
    backwards from the close, so an assembly that stopped short is missing the
    *morning*. A truncated session looks complete from the end.
    """
    opens = (
        lf.group_by("symbol", "trade_date")
        .agg(pl.col("trade_time").min().alias("first"), pl.len().alias("rows"))
        .collect()
    )
    if opens.is_empty():
        return []
    late = opens.filter(pl.col("first").dt.time() > pl.time(9, 25))
    if late.is_empty():
        return []
    share = late.height / opens.height
    if share < TRUNCATION_ALERT_SHARE:
        return []
    worst = late.sort("first", descending=True).head(3)
    examples = ", ".join(
        f"{r['symbol']} {r['trade_date']} opens {r['first'].time()} ({r['rows']} rows)"
        for r in worst.iter_rows(named=True)
    )
    return [
        {
            "dataset": DATASET,
            "severity": "warning",
            "check": "trade_ticks_truncated_session",
            "message": (
                f"{late.height}/{opens.height} symbol-day(s) ({share:.0%}) in {start}..{end} "
                f"do not open with the 09:25 auction. Pages walk backwards from the close, "
                f"so a short assembly loses the morning. e.g. {examples}"
            ),
            "symbol_days": late.height,
            "total_symbol_days": opens.height,
        }
    ]


def direction_findings(lf: pl.LazyFrame, start: date, end: date) -> list[dict]:
    """Unrecognised direction codes, or a buy/sell split that cannot be a market."""
    counts = lf.group_by("direction").agg(pl.len().alias("rows")).collect()
    if counts.is_empty():
        return []
    tally = dict(zip(counts["direction"], counts["rows"], strict=True))
    findings: list[dict] = []

    unknown = tally.get(UNKNOWN_DIRECTION, 0)
    if unknown:
        findings.append(
            {
                "dataset": DATASET,
                "severity": "warning",
                "check": "trade_ticks_direction_mix",
                "message": (
                    f"{unknown} record(s) in {start}..{end} carry a direction code the "
                    f"adapter does not recognise; the source's encoding may have gained "
                    f"a value (known: buy, sell, neutral, {AFTER_HOURS})"
                ),
                "rows": unknown,
            }
        )

    buys, sells = tally.get("buy", 0), tally.get("sell", 0)
    if buys + sells:
        share = buys / (buys + sells)
        if share > DIRECTION_SKEW_LIMIT or share < 1 - DIRECTION_SKEW_LIMIT:
            findings.append(
                {
                    "dataset": DATASET,
                    "severity": "warning",
                    "check": "trade_ticks_direction_mix",
                    "message": (
                        f"buy share is {share:.1%} of {buys + sells} directional record(s) "
                        f"in {start}..{end}; measured range across 36 symbols was 44-62%, "
                        f"so a split this lopsided suggests the field moved, not the market"
                    ),
                    "buy_share": share,
                }
            )
    return findings


def daily_reconciliation_findings(
    config: Config, lf: pl.LazyFrame, start: date, end: date
) -> list[dict]:
    """Session volume and turnover against ``daily_bars``.

    After-hours records are excluded first, and that is not a detail: they are
    real trades that the exchange's daily aggregate does not count, so leaving
    them in moves every ratio by 0.02-0.07% and turns a check that should read
    1.000000 into one that never quite does.

    Turnover is computed rather than stored — the source carries no amount, and
    a stored `price × volume` would look like a fact while being an
    approximation. Its band is correspondingly looser than volume's.
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
    ticks = (
        lf.filter(pl.col("direction") != AFTER_HOURS)
        .group_by("symbol", "trade_date")
        .agg(
            pl.col("volume").sum().alias("tick_volume"),
            (pl.col("price") * pl.col("volume")).sum().alias("tick_amount"),
        )
        .filter(pl.col("tick_volume") > 0)
    )
    joined = (
        ticks.join(daily, on=["symbol", "trade_date"], how="inner")
        .with_columns(
            (pl.col("tick_volume") / pl.col("daily_volume")).alias("volume_ratio"),
            pl.when(pl.col("daily_amount") > 0)
            .then(pl.col("tick_amount") / pl.col("daily_amount"))
            .alias("amount_ratio"),
        )
        .collect()
    )

    if joined.height < RECONCILE_MIN_SYMBOL_DAYS:
        # Reported rather than skipped silently: a reconciliation that never
        # runs is indistinguishable from one that always passes.
        return [
            {
                "dataset": DATASET,
                "severity": "info",
                "check": "trade_ticks_daily_reconciliation",
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
    for column, label, unit, low, high in (
        ("volume_ratio", "volume", "股", RECONCILE_VOLUME_LOW, RECONCILE_VOLUME_HIGH),
        ("amount_ratio", "amount", "yuan", RECONCILE_AMOUNT_LOW, RECONCILE_AMOUNT_HIGH),
    ):
        series = joined[column].drop_nulls()
        if series.is_empty():
            continue
        median = float(series.median())
        if low <= median <= high:
            continue
        hint = (
            f"tick {label} exceeds the day's — check for duplicated sessions, or for "
            "after-hours records leaking into the comparison"
            if median > high
            else f"tick {label} falls short of the day's — check for truncated sessions"
        )
        findings.append(
            {
                "dataset": DATASET,
                "severity": "warning",
                "check": "trade_ticks_daily_reconciliation",
                "message": (
                    f"median tick/daily {label} ({unit}) = {median:.6f} over "
                    f"{len(series)} symbol-day(s) in {start}..{end}; expected ~1.0 — {hint}"
                ),
                "metric": label,
                "median_ratio": median,
                "symbol_days": len(series),
            }
        )
    return findings


def trade_ticks_findings(
    config: Config,
    trade_date: date,
    *,
    lookback_days: int = TICK_CHECK_LOOKBACK_DAYS,
) -> list[dict]:
    """Every transaction-record check, or nothing when the dataset is not in use."""
    start = trade_date - timedelta(days=lookback_days)
    lf = _scan(config, start, trade_date)
    if lf is None:
        return []
    required = {"symbol", "trade_date", "tick_seq", "trade_time", "price", "volume", "direction"}
    if not required.issubset(lf.collect_schema().names()):
        return []
    if lf.select(pl.len()).collect().item() == 0:
        return []

    findings: list[dict] = []
    findings.extend(seq_gap_findings(lf, start, trade_date))
    findings.extend(off_session_findings(lf, start, trade_date))
    findings.extend(trade_date_mismatch_findings(lf, start, trade_date))
    findings.extend(truncation_findings(lf, start, trade_date))
    findings.extend(direction_findings(lf, start, trade_date))
    findings.extend(daily_reconciliation_findings(config, lf, start, trade_date))
    return findings
