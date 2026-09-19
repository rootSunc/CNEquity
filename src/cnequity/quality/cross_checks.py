"""Cross-dataset reconciliation checks.

Single-dataset integrity is in ``dataset_checks``. Here:

* ``daily_bars`` × ``trading_calendar`` — market-wide only (per-symbol gaps are
  often suspensions).
* ``valuation_metrics`` × ``daily_bars`` — coverage on shared days; skip absolute
  mcap sanity while baostock leaves ``total_mv``/``float_mv`` null.
* ``daily_bars`` × ``adj_factors`` × ``corporate_actions`` — hfq continuity vs
  recorded ex-events. Consecutive trading days only (spares suspension resumes).
* ``daily_bars`` × ``instruments`` — survivorship: does the lake still contain the
  names that stopped trading, and are they marked delisted?
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from cnequity.adapters.calendar.holidays_cn import CLOSED_DATES
from cnequity.adapters.eastmoney.corporate_actions import EASTMONEY_BACKFILL_FLOOR
from cnequity.adapters.exchange.st_lists import is_st_name
from cnequity.config import Config
from cnequity.domain.symbols import parse_symbol
from cnequity.domain.trading_status import risk_warning_expr
from cnequity.quality.ex_events import EX_EVENT_LOOKBACK_SESSIONS, unexplained_factor_steps
from cnequity.query.canonical import dedupe_by_primary_key, dedupe_lazy_by_primary_key
from cnequity.query.parquet_scan import (
    dataset_has_parquet,
    list_partitions,
    scan_parquet_root,
)

_SAMPLE = 8
# Flag when valuation covers less than this share of symbols with bars that day.
# Also the gate for watermark advance / baostock history tip isolation.
VALUATION_COVERAGE_WARN_RATIO = 0.7
_VALUATION_COVERAGE_WARN_RATIO = VALUATION_COVERAGE_WARN_RATIO

# Error: |adj_ret| and |adj_ret - raw_ret| both above this on consecutive TDs
# (beyond board limits; not a real ex-event).
ADJ_DISCONTINUITY_RET = 0.35

# Warning: adj continuous but raw diverges past board limit with no CA or known
# capital-structure adjustment on record.
MISSING_EVENT_MAX_ADJ_RET = 0.15
MISSING_EVENT_MIN_DIVERGENCE = 0.11

# A share-count restructuring can change the reference price without being a
# dividend/bonus/allotment event.  ``share_structure`` carries those events;
# keep the vocabulary deliberately narrow because ordinary issuance, unlocks,
# buybacks, and debt conversion do not make an ex-price adjustment by
# themselves.  Without this reconciliation, a verified ``缩股`` on 000887.SZ
# was incorrectly reported as an unrecorded corporate action.
_STRUCTURAL_ADJUSTMENT_RE = "缩股|减资|合股|并股|拆股"

_MAX_RECON_FINDINGS = 50
ADJ_RECON_LOOKBACK_DAYS = 30

# --- adjustment-factor coverage ---------------------------------------------
# adj_factors comes from Sina, daily_bars from TDX, and an append-only derive can
# leave the two at different coverage dates. Sina does serve 北交所, so a bar
# with no factor is an ingest gap rather than an expected exchange limitation.
# `load(adjust=…)` defaults to strict_adj=False, so that bar is returned at
# factor=1.0 — a raw price inside a result the caller asked to have adjusted,
# marked only by an `adj_is_exact` column most callers never select.
#
# The exact counts are lake- and window-dependent. Before this check existed,
# uncovered rows neither raised nor appeared in an audit — which is what this
# check is for. Reported per exchange, because
# "北交所 is uncovered" is one fact and 252 per-symbol findings is noise.
ADJ_COVERAGE_WARN_RATIO = 0.98

# --- survivorship -----------------------------------------------------------
# A symbol whose last bar precedes the lake's last bar by more than this has
# stopped trading (delisted, or suspended long enough to be untradable). Well
# past the longest routine suspension so ordinary halts are not counted.
RETIRED_GAP_DAYS = 180
# Only judge lakes spanning at least this long: over a short window a real
# market genuinely may retire nobody, so zero retirements proves nothing.
SURVIVORSHIP_MIN_SPAN_DAYS = 730


def _traded_bars(bars: pl.LazyFrame) -> pl.LazyFrame:
    """Keep real prints when the daily-bars schema exposes traded volume."""
    if "volume" in bars.collect_schema().names():
        # A diagonal scan inserts null for a legacy file that predates the
        # volume column. Curated current rows require a non-null volume, so a
        # null here is the compatibility marker rather than a traded value.
        return bars.filter((pl.col("volume") > 0) | pl.col("volume").is_null())
    # Minimal/legacy fixtures may not have volume; retain their row-based
    # semantics rather than making an otherwise readable lake unusable.
    return bars


def _canonical_traded_bars(bars: pl.LazyFrame) -> pl.LazyFrame:
    """Canonicalize daily-bar identity before applying traded-row semantics."""
    return _traded_bars(dedupe_lazy_by_primary_key(bars, "daily_bars"))


def _trading_days(config: Config, trade_date: date) -> set[date]:
    cal_root = config.curated_root / "trading_calendar"
    if not dataset_has_parquet(cal_root):
        return set()
    cal = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(cal_root, partition_col="trade_date", end=trade_date),
            "trading_calendar",
        )
        .filter(
            pl.col("is_trading")
            & (pl.col("trade_date").dt.weekday() <= 5)
            & ~pl.col("trade_date").dt.strftime("%Y-%m-%d").is_in(CLOSED_DATES)
        )
        .select("trade_date")
        .unique()
        .collect(engine="streaming")
    )
    return set(cal["trade_date"].to_list())


def daily_bars_calendar_findings(config: Config, trade_date: date) -> list[dict]:
    """Reconcile market-wide daily_bars trade dates against the calendar."""
    findings: list[dict] = []
    bars_root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(bars_root):
        return findings
    trading_days = _trading_days(config, trade_date)
    if not trading_days:
        return findings

    bars = dedupe_lazy_by_primary_key(
        scan_parquet_root(bars_root, partition_col="trade_date", end=trade_date),
        "daily_bars",
    )
    # Aggregate both calendar signals in one streaming pass. The old code
    # collected the same historical daily_bars scan twice (all rows, then
    # traded rows), which made a full health run needlessly amplify its peak
    # memory on a multi-decade lake.
    if "volume" in bars.collect_schema().names():
        traded_rows = (pl.col("volume") > 0) | pl.col("volume").is_null()
    else:
        traded_rows = pl.lit(True)
    date_stats = (
        bars.group_by("trade_date")
        .agg(
            pl.len().alias("_rows"),
            traded_rows.sum().alias("_traded_rows"),
        )
        .collect(engine="streaming")
    )
    if date_stats.is_empty():
        return findings
    bars_dates = set(date_stats["trade_date"].to_list())
    traded_dates = set(date_stats.filter(pl.col("_traded_rows") > 0)["trade_date"].to_list())

    # Bars on a closed calendar day.
    orphan = sorted(bars_dates - trading_days)
    if orphan:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "error",
                "check": "daily_bars_calendar_orphan",
                "message": (
                    f"{len(orphan)} trade date(s) have bars but are not calendar "
                    f"trading days (e.g. {', '.join(d.isoformat() for d in orphan[:_SAMPLE])})"
                ),
                "orphan_count": len(orphan),
                "orphan_sample": [d.isoformat() for d in orphan[:_SAMPLE]],
            }
        )

    # Trading days in the traded-data span with zero traded bars from any
    # symbol. A terminal partition containing only suspension placeholders is
    # still checked above for calendar anomalies, but must not extend the
    # market-wide coverage interval and create a false missing-day finding.
    if not traded_dates:
        return findings
    first, last = min(traded_dates), max(traded_dates)
    expected = {d for d in trading_days if first <= d <= last}
    missing = sorted(expected - traded_dates)
    if missing:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "error",
                "check": "daily_bars_calendar_missing_day",
                "message": (
                    f"{len(missing)} calendar trading day(s) in "
                    f"{first.isoformat()}..{last.isoformat()} have zero traded bars "
                    f"(e.g. {', '.join(d.isoformat() for d in missing[:_SAMPLE])})"
                ),
                "missing_count": len(missing),
                "missing_sample": [d.isoformat() for d in missing[:_SAMPLE]],
            }
        )
    return findings


def trading_calendar_horizon_findings(config: Config, trade_date: date) -> list[dict]:
    """Warn before the calendar starts guessing holidays.

    ``trading_calendar`` is written a year ahead of every run. Inside the
    bundled holiday table that is real; past its last date the fallback only
    strips weekends, so 春节 and 国庆 come back marked as sessions — silently,
    and a year of "trading days" that are not would land in every window,
    watermark and backtest built on them.

    Verified against the current table (ends 2027-10-07): asking for 2028-01-26,
    the first day of that 春节, returns is_trading=True.
    """
    from cnequity.adapters.calendar.holidays_cn import CLOSED_DATES

    cal_root = config.curated_root / "trading_calendar"
    if not dataset_has_parquet(cal_root) or not CLOSED_DATES:
        return []
    table_end = date.fromisoformat(max(CLOSED_DATES))

    written = (
        dedupe_lazy_by_primary_key(scan_parquet_root(cal_root), "trading_calendar")
        .filter(
            pl.col("is_trading")
            & (pl.col("trade_date").dt.weekday() <= 5)
            & ~pl.col("trade_date").dt.strftime("%Y-%m-%d").is_in(CLOSED_DATES)
        )
        .select(pl.col("trade_date").max().alias("last"))
        .collect(engine="streaming")
    )
    if written.is_empty() or written["last"][0] is None:
        return []
    last_written = written["last"][0]

    if last_written <= table_end:
        return []
    return [
        {
            "dataset": "trading_calendar",
            "severity": "warning",
            "check": "trading_calendar_beyond_holiday_table",
            "message": (
                f"calendar marks trading days through {last_written.isoformat()} but the "
                f"bundled holiday table ends {table_end.isoformat()}; dates past it only "
                "drop weekends, so public holidays are marked as sessions — refresh "
                "adapters/calendar/holidays_cn.py and the seed CSV"
            ),
            "calendar_last_trading_day": last_written.isoformat(),
            "holiday_table_end": table_end.isoformat(),
            "days_beyond": (last_written - table_end).days,
        }
    ]


def valuation_day_coverage_ratio(config: Config, trade_date: date) -> float | None:
    """``|valuation ∩ bars| / |bars|`` on *trade_date*, or None if either side empty."""
    val_root = config.curated_root / "valuation_metrics"
    bars_root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(val_root) or not dataset_has_parquet(bars_root):
        return None
    val_syms = set(
        scan_parquet_root(val_root, partition_col="trade_date", start=trade_date, end=trade_date)
        .select("symbol")
        .unique()
        .collect()["symbol"]
        .to_list()
    )
    bars_syms = set(
        _canonical_traded_bars(
            scan_parquet_root(
                bars_root, partition_col="trade_date", start=trade_date, end=trade_date
            )
        )
        .select("symbol")
        .unique()
        .collect()["symbol"]
        .to_list()
    )
    if not val_syms or not bars_syms:
        return None
    return len(val_syms & bars_syms) / len(bars_syms)


def last_dense_valuation_date(
    config: Config,
    *,
    min_ratio: float = VALUATION_COVERAGE_WARN_RATIO,
) -> date | None:
    """Newest valuation day whose symbol coverage vs bars is ≥ *min_ratio*.

    Walks partitions newest→oldest so a sparse tip (partial baostock refill)
    cannot pin the watermark or history-end past a complete EastMoney day.
    """
    from cnequity.query.parquet_scan import list_partitions

    val_root = config.curated_root / "valuation_metrics"
    if not dataset_has_parquet(val_root):
        return None
    parts = list_partitions(val_root, "trade_date")
    # A partially migrated valuation root can still contain loose legacy
    # parquet beside partition directories. The normal reader includes those
    # rows, so the dense-tip gate must include their dates too; otherwise a
    # complete legacy tip is mistaken for an older watermark.
    root_files = sorted(val_root.glob("*.parquet"))
    candidate_dates: set[date] = set()
    if parts and all(part.start == part.end for part in parts) and not root_files:
        candidate_dates = {part.end for part in parts}
    else:
        # Coarse or mixed layouts cannot use directory ends: a current month
        # may contain rows only through its middle, and the directory's last
        # calendar day has no valuation rows to measure.
        candidate_dates.update(
            scan_parquet_root(val_root, partition_col="trade_date")
            .select("trade_date")
            .drop_nulls()
            .unique()
            .collect()
            .get_column("trade_date")
            .to_list()
        )
    for d in sorted(candidate_dates, reverse=True):
        ratio = valuation_day_coverage_ratio(config, d)
        if ratio is not None and ratio >= min_ratio:
            return d
    return None


def last_complete_em_valuation_tip(
    config: Config,
    *,
    min_ratio: float = VALUATION_COVERAGE_WARN_RATIO,
) -> date | None:
    """Newest day with EastMoney valuation rows and coverage ≥ *min_ratio*.

    Baostock history must not write past this — those tip dates belong to the
    daily EastMoney snapshot. Returns None when no complete EM day exists yet.
    """
    val_root = config.curated_root / "valuation_metrics"
    if not dataset_has_parquet(val_root):
        return None
    em_days = (
        scan_parquet_root(val_root, partition_col="trade_date")
        .filter(pl.col("source") == "eastmoney")
        .select("trade_date")
        .unique()
        .collect()["trade_date"]
        .to_list()
    )
    for d in sorted(em_days, reverse=True):
        ratio = valuation_day_coverage_ratio(config, d)
        if ratio is not None and ratio >= min_ratio:
            return d
    return None


def _unique_symbols_and_dates(lf: pl.LazyFrame) -> tuple[set[str], set[date]]:
    """Collect the two small coverage dimensions in one streaming scan."""
    columns = set(lf.collect_schema().names())
    if "symbol" not in columns or "trade_date" not in columns:
        return set(), set()
    stats = lf.select(
        pl.col("symbol").drop_nulls().unique().implode().alias("_symbols"),
        pl.col("trade_date").drop_nulls().unique().implode().alias("_dates"),
    ).collect(engine="streaming")
    if stats.is_empty():
        return set(), set()
    symbols = stats["_symbols"][0]
    dates = stats["_dates"][0]
    return (
        set(symbols.to_list() if symbols is not None else []),
        set(dates.to_list() if dates is not None else []),
    )


def valuation_bars_coverage_findings(config: Config, trade_date: date) -> list[dict]:
    """valuation_metrics vs daily_bars: orphan symbols + one-day coverage ratio."""
    findings: list[dict] = []
    val_root = config.curated_root / "valuation_metrics"
    bars_root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(val_root) or not dataset_has_parquet(bars_root):
        return findings

    val_syms_all, val_dates = _unique_symbols_and_dates(
        scan_parquet_root(val_root, partition_col="trade_date", end=trade_date)
    )
    bars_syms_all, bars_dates = _unique_symbols_and_dates(
        _canonical_traded_bars(
            scan_parquet_root(bars_root, partition_col="trade_date", end=trade_date)
        )
    )
    if not val_syms_all or not bars_syms_all:
        return findings

    no_bar_ever = sorted(val_syms_all - bars_syms_all)
    if no_bar_ever:
        findings.append(
            {
                "dataset": "valuation_metrics",
                "severity": "warning",
                "check": "valuation_bars_orphan_symbol",
                "message": (
                    f"{len(no_bar_ever)} valuation symbol(s) have no daily_bars row "
                    f"anywhere (delisted/non-tradable; "
                    f"e.g. {', '.join(no_bar_ever[:_SAMPLE])}) — filter the valuation "
                    "step to the bar universe"
                ),
                "orphan_count": len(no_bar_ever),
                "orphan_sample": no_bar_ever[:_SAMPLE],
            }
        )

    shared = val_dates & bars_dates
    if not shared:
        findings.append(
            {
                "dataset": "valuation_metrics",
                "severity": "warning",
                "check": "valuation_bars_no_shared_date",
                "message": (
                    "valuation_metrics shares no trade date with daily_bars — "
                    "cannot reconcile symbol coverage"
                ),
            }
        )
        return findings

    anchor = max(shared)
    val_syms = set(
        scan_parquet_root(val_root, partition_col="trade_date", start=anchor, end=anchor)
        .select("symbol")
        .unique()
        .collect()["symbol"]
        .to_list()
    )
    bars_syms = set(
        _canonical_traded_bars(
            scan_parquet_root(bars_root, partition_col="trade_date", start=anchor, end=anchor)
        )
        .select("symbol")
        .unique()
        .collect()["symbol"]
        .to_list()
    )
    if not val_syms or not bars_syms:
        return findings

    covered = val_syms & bars_syms
    ratio = len(covered) / len(bars_syms)
    if ratio < _VALUATION_COVERAGE_WARN_RATIO:
        findings.append(
            {
                "dataset": "valuation_metrics",
                "severity": "warning",
                "check": "valuation_bars_low_coverage",
                "message": (
                    f"valuation covers {len(covered)}/{len(bars_syms)} "
                    f"({ratio:.0%}) of symbols with bars on {anchor.isoformat()} "
                    f"(< {_VALUATION_COVERAGE_WARN_RATIO:.0%})"
                ),
                "anchor_date": anchor.isoformat(),
                "covered_symbols": len(covered),
                "bars_symbols": len(bars_syms),
                "coverage_ratio": round(ratio, 4),
                "warn_ratio": _VALUATION_COVERAGE_WARN_RATIO,
            }
        )
    return findings


def _adjusted_returns(
    config: Config,
    trade_date: date,
    *,
    lookback_days: int | None = None,
) -> pl.DataFrame | None:
    """Per (symbol, day) hfq adj vs raw returns + previous bar date.

    The lake is partitioned by day, but the old implementation collected all
    history before joining bars to factors.  A full audit therefore needed
    several copies of ~19M rows in memory.  Process one calendar year at a
    time and carry only each symbol's last joined row across the boundary;
    retain only rows that can enter either reconciliation bucket.
    """
    bars_root = config.curated_root / "daily_bars"
    af_root = config.derived_root / "adj_factors"
    if not dataset_has_parquet(bars_root) or not dataset_has_parquet(af_root):
        return None
    bar_parts = list_partitions(bars_root, "trade_date")
    if not bar_parts:
        return None
    first_date = bar_parts[0].start
    window_start = first_date
    if lookback_days is not None:
        if lookback_days <= 0:
            raise ValueError("adjustment reconciliation lookback_days must be positive")
        window_start = max(first_date, trade_date - timedelta(days=lookback_days))
        # Carry one nearby historical print into the window. Pairs separated
        # by longer suspensions are filtered by the trading-calendar
        # successor check below, so there is no need to replay older history.
        first_date = max(first_date, window_start - timedelta(days=31))
    last_date = min(trade_date, bar_parts[-1].end)
    if first_date > last_date:
        return None

    carry = pl.DataFrame(
        schema={
            "symbol": pl.Utf8,
            "trade_date": pl.Date,
            "close": pl.Float64,
            "_adj": pl.Float64,
        }
    )
    interesting: list[pl.DataFrame] = []

    for year in range(first_date.year, last_date.year + 1):
        chunk_start = max(first_date, date(year, 1, 1))
        chunk_end = min(last_date, date(year, 12, 31))
        bars = (
            _traded_bars(
                dedupe_lazy_by_primary_key(
                    scan_parquet_root(
                        bars_root,
                        partition_col="trade_date",
                        start=chunk_start,
                        end=chunk_end,
                    ),
                    "daily_bars",
                )
            )
            .select("symbol", "trade_date", "close")
            .collect(engine="streaming")
        )
        if bars.is_empty():
            continue
        factors = (
            dedupe_lazy_by_primary_key(
                scan_parquet_root(
                    af_root,
                    partition_col="trade_date",
                    start=chunk_start,
                    end=chunk_end,
                ),
                "adj_factors",
            )
            .filter(pl.col("adjust_type") == "hfq")
            .select("symbol", "trade_date", "factor")
            .collect(engine="streaming")
        )
        if factors.is_empty():
            continue
        joined = (
            bars.join(factors, on=["symbol", "trade_date"], how="inner")
            .filter(pl.col("close").is_not_null() & (pl.col("close") > 0) & (pl.col("factor") > 0))
            .with_columns((pl.col("close") * pl.col("factor")).alias("_adj"))
            .select("symbol", "trade_date", "close", "_adj")
            .sort(["symbol", "trade_date"])
        )
        if joined.is_empty():
            continue

        combined = (
            pl.concat([carry, joined], how="vertical_relaxed")
            .sort(["symbol", "trade_date"])
            .with_columns(
                (pl.col("close") / pl.col("close").shift(1).over("symbol") - 1).alias("raw_ret"),
                (pl.col("_adj") / pl.col("_adj").shift(1).over("symbol") - 1).alias("adj_ret"),
                pl.col("trade_date").shift(1).over("symbol").alias("prev_trade_date"),
            )
        )
        chunk_returns = (
            combined.filter(pl.col("trade_date") >= window_start)
            .filter(pl.col("prev_trade_date").is_not_null())
            .with_columns((pl.col("adj_ret") - pl.col("raw_ret")).abs().alias("divergence"))
            .filter(
                (
                    (pl.col("adj_ret").abs() > ADJ_DISCONTINUITY_RET)
                    & (pl.col("divergence") > ADJ_DISCONTINUITY_RET)
                )
                | (
                    (pl.col("adj_ret").abs() <= MISSING_EVENT_MAX_ADJ_RET)
                    & (pl.col("divergence") > MISSING_EVENT_MIN_DIVERGENCE)
                )
            )
            .select(
                "symbol",
                "prev_trade_date",
                "trade_date",
                "raw_ret",
                "adj_ret",
                "divergence",
            )
        )
        if not chunk_returns.is_empty():
            interesting.append(chunk_returns)
        # Carry forward from the union of the running carry and this chunk,
        # not just this chunk: a symbol absent for an entire chunk (a
        # multi-quarter halt spanning a year boundary) must keep its older
        # carried row so the gap is still checked once the symbol resumes,
        # instead of silently losing its prior-row state at the boundary.
        carry = (
            pl.concat([carry, joined], how="vertical_relaxed")
            .sort(["symbol", "trade_date"])
            .group_by("symbol", maintain_order=True)
            .last()
            .select("symbol", "trade_date", "close", "_adj")
        )

    if not interesting:
        return pl.DataFrame()
    return pl.concat(interesting, how="vertical_relaxed")


def _capped_findings(
    ranked: pl.DataFrame, build_one, *, dataset: str, check: str, severity: str, noun: str
) -> list[dict]:
    """Emit one finding per row up to the cap, plus an overflow summary."""
    findings = [build_one(row) for row in ranked.head(_MAX_RECON_FINDINGS).iter_rows(named=True)]
    overflow = ranked.height - _MAX_RECON_FINDINGS
    if overflow > 0:
        findings.append(
            {
                "dataset": dataset,
                "severity": severity,
                "check": f"{check}_overflow",
                "message": (
                    f"{ranked.height} symbols have {noun}; {overflow} beyond the first "
                    f"{_MAX_RECON_FINDINGS} are not listed individually"
                ),
                "total_symbols": ranked.height,
                "listed": _MAX_RECON_FINDINGS,
            }
        )
    return findings


def _worst_per_symbol(df: pl.DataFrame, by: str) -> pl.DataFrame:
    return df.sort(by, descending=True).group_by("symbol", maintain_order=True).first()


def _structural_adjustments(
    config: Config, trade_date: date, *, symbols: list[str]
) -> pl.DataFrame | None:
    """Return share-count restructurings that can explain an ex-price move."""
    root = config.curated_root / "share_structure"
    if not dataset_has_parquet(root):
        return None
    structure = (
        scan_parquet_root(
            root,
            partition_col="change_date",
            end=trade_date,
            symbols=symbols,
        )
        .select("symbol", "change_date", "change_reason")
        .filter(pl.col("change_reason").fill_null("").str.contains(_STRUCTURAL_ADJUSTMENT_RE))
        .select("symbol", "change_date", "change_reason")
        .unique()
        .collect(engine="streaming")
    )
    return None if structure.is_empty() else structure


def _trading_day_successors(config: Config, trade_date: date) -> pl.DataFrame | None:
    """[prev_trade_date, next_td]. None if calendar missing (then no adjacency filter)."""
    cal_root = config.curated_root / "trading_calendar"
    if not dataset_has_parquet(cal_root):
        return None
    cal = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(cal_root, partition_col="trade_date", end=trade_date),
            "trading_calendar",
        )
        .filter(pl.col("is_trading"))
        .select("trade_date")
        .unique()
        .collect(engine="streaming")
        .sort("trade_date")
    )
    if cal.is_empty():
        return None
    return cal.with_columns(pl.col("trade_date").shift(-1).alias("next_td")).rename(
        {"trade_date": "prev_trade_date"}
    )


def _iso(value) -> str:
    return value.isoformat() if isinstance(value, date) else str(value)


def adj_factor_coverage_findings(config: Config, trade_date: date) -> list[dict]:
    """Flag exchanges whose priced symbols largely lack adjustment factors.

    See ``ADJ_COVERAGE_WARN_RATIO``. Scoped to ``asset_type`` in
    (``stock``, ``etf``): stocks and ETFs/LOFs both carry Sina hfq factor
    series, so a missing factor is a real coverage gap for either.
    """
    bars_root = config.curated_root / "daily_bars"
    fac_root = config.derived_root / "adj_factors"
    inst_root = config.curated_root / "instruments"
    if not (dataset_has_parquet(bars_root) and dataset_has_parquet(fac_root)):
        return []
    if not dataset_has_parquet(inst_root):
        return []

    instruments = dedupe_lazy_by_primary_key(scan_parquet_root(inst_root), "instruments").collect(
        engine="streaming"
    )
    if "asset_type" not in instruments.columns:
        return []
    priced_assets = set(
        instruments.filter(pl.col("asset_type").is_in(["stock", "etf"]))["symbol"].to_list()
    )
    if not priced_assets:
        return []

    bar_dates = _canonical_traded_bars(
        scan_parquet_root(
            bars_root,
            partition_col="trade_date",
            end=trade_date,
            symbols=sorted(priced_assets),
        )
    ).select("symbol", "trade_date")
    bar_summary = (
        bar_dates.group_by("symbol")
        .agg(
            pl.col("trade_date").min().alias("bar_first"),
            pl.col("trade_date").max().alias("bar_last"),
            pl.col("trade_date").unique().alias("_bar_dates"),
        )
        .collect(engine="streaming")
    )
    if bar_summary.is_empty():
        return []
    factor_rows = dedupe_lazy_by_primary_key(
        scan_parquet_root(
            fac_root,
            partition_col="trade_date",
            end=trade_date,
            symbols=sorted(priced_assets),
        ),
        "adj_factors",
    )
    factor_schema = set(factor_rows.collect_schema().names())
    # Only hfq is persisted and used by the reader. A qfq-only or malformed
    # factor row must not make the coverage check claim that an adjusted bar
    # has an exact factor. Older test/fixture lakes may lack the newer columns;
    # retain their historical row-presence semantics in that compatibility path.
    if "adjust_type" in factor_schema:
        factor_rows = factor_rows.filter(pl.col("adjust_type") == "hfq")
    if "factor" in factor_schema:
        factor_rows = factor_rows.filter(
            pl.col("factor").is_not_null() & pl.col("factor").is_finite() & (pl.col("factor") > 0)
        )
    factor_dates = factor_rows.select("symbol", "trade_date").unique()
    factor_summary = (
        factor_dates.group_by("symbol")
        .agg(
            pl.col("trade_date").min().alias("factor_first"),
            pl.col("trade_date").max().alias("factor_last"),
            pl.col("trade_date").unique().alias("_factor_dates"),
        )
        .collect(engine="streaming")
    )
    spans = bar_summary.join(factor_summary, on="symbol", how="left").with_columns(
        pl.when(pl.col("_factor_dates").is_null())
        .then(pl.col("_bar_dates").list.len())
        .otherwise(pl.col("_bar_dates").list.set_difference(pl.col("_factor_dates")).list.len())
        .alias("missing_factor_days")
    )

    from cnequity.storage.state import StateStore

    source_unavailable = StateStore(config.meta_root).get_string_set(
        "adj_factors", "source_unavailable_symbols"
    )
    findings: list[dict] = []
    by_exchange: dict[str, list[str]] = {}
    for symbol in priced_assets & set(spans["symbol"].to_list()):
        by_exchange.setdefault(symbol.rsplit(".", 1)[-1], []).append(symbol)

    for exchange, symbols in sorted(by_exchange.items()):
        total = len(symbols)
        if not total:
            continue
        span_by_symbol = {
            row["symbol"]: row
            for row in spans.filter(pl.col("symbol").is_in(symbols)).iter_rows(named=True)
        }
        fully_covered = {
            symbol
            for symbol, row in span_by_symbol.items()
            if (
                row["factor_first"] is not None
                and row["factor_last"] is not None
                and row["factor_first"] <= row["bar_first"]
                and row["factor_last"] >= row["bar_last"]
                and row["missing_factor_days"] == 0
            )
        }
        known_source_unavailable = sorted((set(symbols) - fully_covered) & source_unavailable)
        if known_source_unavailable:
            findings.append(
                {
                    "dataset": "adj_factors",
                    "severity": "info",
                    "check": "adj_factor_source_unavailable",
                    "exchange": exchange,
                    "message": (
                        f"{exchange}: {len(known_source_unavailable)} formally delisted "
                        "priced symbol(s) have explicit source-unavailable factor evidence; "
                        "no exact adjusted history is currently reconstructable"
                    ),
                    "symbols_unavailable": len(known_source_unavailable),
                    "sample": known_source_unavailable[:_SAMPLE],
                }
            )
        covered = len(fully_covered)
        ratio = covered / total
        if ratio >= ADJ_COVERAGE_WARN_RATIO:
            continue
        missing = total - covered
        without_factor = sum(
            1 for symbol in symbols if span_by_symbol[symbol]["factor_first"] is None
        )
        partial = missing - without_factor
        internal_gaps = sum(
            1
            for symbol in symbols
            if (
                span_by_symbol[symbol]["missing_factor_days"] > 0
                and span_by_symbol[symbol]["factor_first"] is not None
                and span_by_symbol[symbol]["factor_first"] <= span_by_symbol[symbol]["bar_first"]
                and span_by_symbol[symbol]["factor_last"] is not None
                and span_by_symbol[symbol]["factor_last"] >= span_by_symbol[symbol]["bar_last"]
            )
        )
        findings.append(
            {
                "dataset": "adj_factors",
                "severity": "warning",
                "check": "adj_factor_coverage",
                "exchange": exchange,
                "message": (
                    f"{exchange}: {missing} of {total} priced symbols lack a complete "
                    f"adjustment-factor span ({ratio:.0%} covered; "
                    f"{without_factor} with no factor, {partial} partial, "
                    f"{internal_gaps} with internal gaps). "
                    "load(adjust='hfq') returns uncovered bars unadjusted at factor=1.0 "
                    "unless strict_adj=True — check adj_is_exact"
                ),
                "symbols_total": total,
                "symbols_covered": covered,
                "symbols_missing": missing,
                "symbols_without_factor": without_factor,
                "symbols_partial": partial,
                "symbols_internal_gaps": internal_gaps,
                "coverage_ratio": round(ratio, 4),
                "sample": sorted(set(symbols) - fully_covered)[:_SAMPLE],
            }
        )
    return findings


def adj_factor_reconciliation_findings(
    config: Config,
    trade_date: date,
    *,
    lookback_days: int | None = None,
) -> list[dict]:
    """hfq continuity vs corporate_actions; errors/warnings capped per class."""
    rets = _adjusted_returns(config, trade_date, lookback_days=lookback_days)
    if rets is None or rets.is_empty():
        return []

    findings: list[dict] = []

    # Factor break on consecutive TDs only (suspension resumes false-flag otherwise).
    disc = rets.filter(
        (pl.col("adj_ret").abs() > ADJ_DISCONTINUITY_RET)
        & (pl.col("divergence") > ADJ_DISCONTINUITY_RET)
    )
    successors = _trading_day_successors(config, trade_date)
    if successors is not None and not disc.is_empty():
        disc = disc.join(successors, on="prev_trade_date", how="left").filter(
            pl.col("next_td") == pl.col("trade_date")
        )
    breaks = _worst_per_symbol(disc, by="divergence")
    break_syms = set(breaks["symbol"].to_list())
    if not breaks.is_empty():
        findings += _capped_findings(
            breaks.sort("divergence", descending=True),
            lambda row: {
                "dataset": "adj_factors",
                "symbol": row["symbol"],
                "severity": "error",
                "check": "adj_close_discontinuity",
                "message": (
                    f"{row['symbol']}: hfq adjusted return {row['adj_ret']:+.0%} on "
                    f"{_iso(row['trade_date'])} diverges {row['divergence']:.0%} from the "
                    f"raw move ({row['raw_ret']:+.0%}) on consecutive trading days — a "
                    "factor break, not a corporate action"
                ),
                "trade_date": _iso(row["trade_date"]),
                "prev_trade_date": _iso(row["prev_trade_date"]),
                "adj_ret": round(float(row["adj_ret"]), 4),
                "raw_ret": round(float(row["raw_ret"]), 4),
                "divergence": round(float(row["divergence"]), 4),
            },
            dataset="adj_factors",
            check="adj_close_discontinuity",
            severity="error",
            noun="a discontinuous hfq adjustment",
        )

    # Continuous adj but raw jumped with no CA; skip symbols already flagged.
    ca_root = config.curated_root / "corporate_actions"
    if not dataset_has_parquet(ca_root):
        return findings

    candidates = rets.filter(
        (pl.col("adj_ret").abs() <= MISSING_EVENT_MAX_ADJ_RET)
        & (pl.col("divergence") > MISSING_EVENT_MIN_DIVERGENCE)
        & ~pl.col("symbol").is_in(list(break_syms))
    ).sort(["symbol", "trade_date"])
    if successors is not None and not candidates.is_empty():
        candidates = candidates.join(successors, on="prev_trade_date", how="left").filter(
            pl.col("next_td") == pl.col("trade_date")
        )
    if candidates.is_empty():
        return findings

    ex_dates = (
        scan_parquet_root(ca_root, partition_col="ex_date", end=trade_date)
        .select("symbol", "ex_date")
        .unique()
        .collect(engine="streaming")
        .sort(["symbol", "ex_date"])
    )
    if ex_dates.is_empty():
        matched = candidates.with_columns(pl.lit(None, dtype=pl.Date).alias("_last_ex"))
    else:
        # Explained if some ex-date is in (t_prev, t].
        matched = candidates.join_asof(
            ex_dates.rename({"ex_date": "_last_ex"}),
            left_on="trade_date",
            right_on="_last_ex",
            by="symbol",
            strategy="backward",
            check_sortedness=False,
        )
    missing = _worst_per_symbol(
        matched.filter(
            pl.col("_last_ex").is_null() | (pl.col("_last_ex") <= pl.col("prev_trade_date"))
        ),
        by="divergence",
    )
    if missing.is_empty():
        return findings

    # Not every reference-price change is an ex-dividend event.  Reconcile
    # explicitly recorded share-count restructurings before filing a missing
    # corporate-action warning; otherwise a genuine capital reduction is
    # mislabelled as a missing dividend/bonus row.
    structural = _structural_adjustments(
        config,
        trade_date,
        symbols=missing["symbol"].unique().to_list(),
    )
    if structural is not None:
        explained = missing.join(
            structural,
            left_on=["symbol", "trade_date"],
            right_on=["symbol", "change_date"],
            how="inner",
        )
        if not explained.is_empty():
            findings.append(
                {
                    "dataset": "share_structure",
                    "severity": "info",
                    "check": "adjustment_explained_by_share_structure",
                    "message": (
                        f"{explained.height} raw/hfq return divergence(s) match a recorded "
                        "share-count restructuring rather than a dividend/bonus event"
                    ),
                    "events": [
                        {
                            "symbol": row["symbol"],
                            "trade_date": _iso(row["trade_date"]),
                            "change_reason": row["change_reason"],
                        }
                        for row in explained.select(
                            "symbol", "trade_date", "change_reason"
                        ).iter_rows(named=True)
                    ][:_SAMPLE],
                }
            )
            missing = missing.join(
                structural,
                left_on=["symbol", "trade_date"],
                right_on=["symbol", "change_date"],
                how="anti",
            )
            if missing.is_empty():
                return findings

    # Delisted names get their own bucket, not because the gap is fake, but
    # because it is a *different, already-diagnosed* gap. Both tdx_protocol
    # (xdxr) and the eastmoney backup were checked live against a sample of
    # these symbols and neither returns any corporate-action history for a
    # name once it drops off their live symbol list — a vendor behavior tied
    # to delisting, not a market-id or filter bug. The exact count is
    # lake-dependent, and filing one warning per delisted symbol for this
    # proven, unfixable-with-current-sources root cause would bury the handful
    # that are actually worth investigating.
    instruments = _instruments_frame(config)
    if instruments is not None and "delist_date" in instruments.columns:
        missing = missing.join(instruments.select("symbol", "delist_date"), on="symbol", how="left")
        delisted = missing.filter(pl.col("delist_date").is_not_null())
        active = missing.filter(pl.col("delist_date").is_null())
    else:
        delisted = missing.head(0)
        active = missing

    if not active.is_empty():
        findings += _capped_findings(
            active.sort("divergence", descending=True),
            lambda row: {
                "dataset": "corporate_actions",
                "symbol": row["symbol"],
                "severity": "warning",
                "check": "missing_corporate_action",
                "message": (
                    f"{row['symbol']}: raw return {row['raw_ret']:+.0%} on "
                    f"{_iso(row['trade_date'])} diverges {row['divergence']:.0%} from the "
                    f"hfq adjusted return ({row['adj_ret']:+.0%}) with no corporate action "
                    "on record for that day — an unrecorded ex-event"
                ),
                "trade_date": _iso(row["trade_date"]),
                "prev_trade_date": _iso(row["prev_trade_date"]),
                "adj_ret": round(float(row["adj_ret"]), 4),
                "raw_ret": round(float(row["raw_ret"]), 4),
                "divergence": round(float(row["divergence"]), 4),
            },
            dataset="corporate_actions",
            check="missing_corporate_action",
            severity="warning",
            noun="a raw move with no corporate action on record",
        )
    if not delisted.is_empty():
        divergence_on_or_before_delist = delisted.filter(
            pl.col("trade_date") <= pl.col("delist_date")
        ).height
        divergence_after_delist = delisted.filter(
            pl.col("trade_date") > pl.col("delist_date")
        ).height
        baostock_repairable = 0
        unsupported_exchange_counts: dict[str, int] = {}
        for symbol in delisted["symbol"].to_list():
            try:
                exchange = parse_symbol(str(symbol)).exchange
            except ValueError:
                continue
            if exchange in {"SH", "SZ"}:
                baostock_repairable += 1
            else:
                unsupported_exchange_counts[exchange] = (
                    unsupported_exchange_counts.get(exchange, 0) + 1
                )
        repairable_symbols = sorted(
            str(symbol)
            for symbol in delisted["symbol"].to_list()
            if str(symbol).upper().endswith((".SH", ".SZ"))
        )
        repair_hint = (
            " Explicit Baostock repair can cover "
            f"{baostock_repairable} delisted SH/SZ symbol(s); run "
            "`cne backfill corporate_actions --baostock-repair --symbols ...` "
            "for a scoped repair."
            if baostock_repairable
            else ""
        )
        unsupported_hint = (
            " Unsupported exchange counts: "
            + ", ".join(
                f"{exchange}={count}"
                for exchange, count in sorted(unsupported_exchange_counts.items())
            )
            + "; those symbols need an independent historical source."
            if unsupported_exchange_counts
            else ""
        )
        remediation_parts: list[str] = []
        if baostock_repairable:
            remediation_parts.append("Run a scoped Baostock repair for SH/SZ symbols")
        if unsupported_exchange_counts:
            remediation_parts.append(
                "obtain an independent historical corporate-action source for unsupported exchanges"
            )
        remediation = "; ".join(remediation_parts) or (
            "Review the delisted symbols against an independent historical corporate-action source"
        )
        findings.append(
            {
                "dataset": "corporate_actions",
                "severity": "info",
                "check": "missing_corporate_action_delisted",
                "message": (
                    f"{delisted.height} delisted symbol(s) have a raw/hfq return divergence "
                    "with no corporate action on record; these symbols later delisted "
                    f"({divergence_on_or_before_delist} on/before their delist_date, "
                    f"{divergence_after_delist} after). "
                    "Verified live against both tdx_protocol and the eastmoney backup: "
                    "neither serves corporate-action history for a name once it is gone "
                    "from their live symbol list. Not a market-id or filter bug — see "
                    "docs/datasets/sources.md#corporate_actions."
                    f"{repair_hint}{unsupported_hint}"
                ),
                "symbols_total": delisted.height,
                "sample": sorted(delisted["symbol"].to_list())[:_SAMPLE],
                "divergence_date_relation_counts": {
                    "on_or_before_delist_date": divergence_on_or_before_delist,
                    "after_delist_date": divergence_after_delist,
                },
                "baostock_repairable_symbols": baostock_repairable,
                "baostock_repairable_sample": repairable_symbols[:_SAMPLE],
                "unsupported_exchange_counts": unsupported_exchange_counts,
                "remediation": remediation + ".",
                "source_limited": True,
            }
        )
    return findings


def _symbol_last_bar(config: Config, trade_date: date) -> pl.DataFrame | None:
    """Per-symbol first/last traded date in ``daily_bars``. None if absent."""
    bars_root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(bars_root):
        return None
    bars = _canonical_traded_bars(
        scan_parquet_root(bars_root, partition_col="trade_date", end=trade_date)
    )
    # A suspended/delisted name can retain carried-forward OHLC placeholders
    # after its final print. Those rows must not hide a survivorship gap.
    out = (
        bars.group_by("symbol")
        .agg(
            pl.col("trade_date").min().alias("first_bar"),
            pl.col("trade_date").max().alias("last_bar"),
        )
        .collect(engine="streaming")
    )
    return None if out.is_empty() else out


def _instruments_frame(config: Config) -> pl.DataFrame | None:
    root = config.curated_root / "instruments"
    if not dataset_has_parquet(root):
        return None
    out = dedupe_by_primary_key(scan_parquet_root(root, hive=False).collect(), "instruments")
    return None if out.is_empty() else out


def universe_survivorship_findings(config: Config, trade_date: date) -> list[dict]:
    """Does the lake still hold the names that stopped trading?

    A history backfilled from *today's* listing snapshot contains only survivors:
    every delisted name — in A-shares typically after an 80–95% drawdown — is
    missing, so every backtest run on it overstates returns, and the bias lands
    hardest on exactly the small/value/distressed buckets a factor screen buys.

    The tell is structural rather than statistical: over a multi-year span a real
    market always retires names, so a lake where *no* symbol's series ever ends is
    proof the universe was pinned to the current listing, not evidence of an
    unusually healthy market. Retired names that ``instruments`` never marks
    delisted are the second half of the problem — ``universe="all_a"`` keeps
    treating them as listed forever.
    """
    last_bar = _symbol_last_bar(config, trade_date)
    if last_bar is None:
        return []

    instruments = _instruments_frame(config)
    if instruments is not None and "asset_type" in instruments.columns:
        # daily_bars also carries ETFs/LOFs for quote display. They commonly
        # end with zero-volume carried-forward rows and are deliberately
        # outside the research ``all_a`` universe, so they must not affect
        # either the retirement ratio or the missing-delist check. Keep the
        # minimal fixture behavior when older instrument fragments have no
        # asset_type column.
        research_symbols = set(
            instruments.filter(pl.col("asset_type") == "stock")["symbol"].to_list()
        )
        last_bar = last_bar.filter(pl.col("symbol").is_in(sorted(research_symbols)))
        if last_bar.is_empty():
            return []

    lake_first = last_bar["first_bar"].min()
    lake_last = last_bar["last_bar"].max()
    span_days = (lake_last - lake_first).days
    if span_days < SURVIVORSHIP_MIN_SPAN_DAYS:
        return []

    retired = last_bar.filter(
        (pl.lit(lake_last) - pl.col("last_bar")).dt.total_days() > RETIRED_GAP_DAYS
    )
    total = last_bar.height
    span_years = span_days / 365.25

    if retired.is_empty():
        return [
            {
                "dataset": "daily_bars",
                "severity": "error",
                "check": "universe_survivorship_absent",
                "message": (
                    f"all {total} symbols in daily_bars are still trading as of "
                    f"{lake_last.isoformat()} after {span_years:.1f} years — no name "
                    "ever leaves the lake, so history was backfilled from the current "
                    "listing snapshot. Every backtest is survivorship-biased; "
                    "backfill delisted symbols before trusting any return series"
                ),
                "symbols": total,
                "span_years": round(span_years, 2),
                "coverage_start": lake_first.isoformat(),
                "coverage_end": lake_last.isoformat(),
                "retired_gap_days": RETIRED_GAP_DAYS,
            }
        ]

    findings: list[dict] = [
        {
            "dataset": "daily_bars",
            "severity": "info",
            "check": "universe_survivorship",
            "message": (
                f"{retired.height}/{total} symbols stopped trading more than "
                f"{RETIRED_GAP_DAYS} days before {lake_last.isoformat()} "
                f"({retired.height / total:.1%} of the lake over {span_years:.1f} years)"
            ),
            "retired_symbols": retired.height,
            "total_symbols": total,
            "span_years": round(span_years, 2),
        }
    ]

    if instruments is None or "delist_date" not in instruments.columns:
        return findings

    unmarked = (
        retired.join(instruments.select(["symbol", "delist_date"]), on="symbol", how="left")
        .filter(pl.col("delist_date").is_null())
        .sort("last_bar")
    )
    if unmarked.is_empty():
        return findings

    sample = unmarked.head(_SAMPLE)
    findings.append(
        {
            "dataset": "instruments",
            "severity": "warning",
            "check": "retired_symbol_missing_delist_date",
            "message": (
                f"{unmarked.height} symbol(s) stopped producing bars but carry no "
                f"delist_date in instruments (e.g. "
                + ", ".join(
                    f"{r['symbol']} last bar {_iso(r['last_bar'])}"
                    for r in sample.iter_rows(named=True)
                )
                + ") — universe='all_a' keeps selecting them after they stopped trading"
            ),
            "unmarked_count": unmarked.height,
            "retired_symbols": retired.height,
            "sample": [
                {"symbol": r["symbol"], "last_bar": _iso(r["last_bar"])}
                for r in sample.iter_rows(named=True)
            ],
        }
    )
    return findings


# --- cross-source close verification ----------------------------------------
# A capture that fires before the session closes writes a bar that passes every
# single-source check: the PK is unique, the calendar day is real, the row count
# is normal. Only the close is wrong — and with it every return, every
# cross-sectional factor value, and the day's backtest P&L.
#
# A volume-vs-trailing-median heuristic cannot separate that from a genuinely
# quiet session: on this lake it flagged the 2016-01-07 circuit-breaker halt and
# the 2020-02-03 limit-down open alongside the one real defect. Comparing the
# close against an independent vendor does separate them — those four days
# matched Sina to the cent, the truncated one did not.
CLOSE_CROSSCHECK_SAMPLE = 12
# Prices carry 2dp; anything past 0.1% is a different print, not rounding.
CLOSE_CROSSCHECK_TOLERANCE = 0.001
# Above this share of the sample the cause is the capture, not one bad symbol.
CLOSE_CROSSCHECK_SYSTEMATIC_RATIO = 0.5


def _liquid_symbols_on(config: Config, trade_date: date, limit: int) -> list[str]:
    """Most-traded symbols that day — continuous prints, no stale-quote noise."""
    root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(root):
        return []
    lf = dedupe_lazy_by_primary_key(
        scan_parquet_root(root, partition_col="trade_date", start=trade_date, end=trade_date),
        "daily_bars",
    )
    lf = _traded_bars(lf)
    cols = lf.collect_schema().names()
    rank_col = "amount" if "amount" in cols else "volume"
    df = (
        lf.filter(pl.col("close").is_not_null() & (pl.col("close") > 0))
        .select("symbol", rank_col)
        .sort(rank_col, descending=True, nulls_last=True)
        .limit(limit)
        .collect(engine="streaming")
    )
    return df["symbol"].to_list()


def _curated_closes(config: Config, trade_date: date, symbols: list[str]) -> dict[str, float]:
    root = config.curated_root / "daily_bars"
    df = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(root, partition_col="trade_date", start=trade_date, end=trade_date),
            "daily_bars",
        )
        .filter(pl.col("symbol").is_in(symbols))
        .select("symbol", "close")
        .collect(engine="streaming")
    )
    return {r["symbol"]: float(r["close"]) for r in df.iter_rows(named=True)}


def _sina_closes(symbols: list[str], trade_date: date, *, config=None) -> dict[str, float]:
    import httpx

    from cnequity.adapters.sina.bars import fetch_daily_bars_sina

    out: dict[str, float] = {}
    with httpx.Client(timeout=20.0) as client:
        for sym in symbols:
            df = fetch_daily_bars_sina(
                sym,
                start=trade_date,
                end=trade_date,
                datalen=30,
                client=client,
                config=config,
            )
            if not df.is_empty():
                out[sym] = float(df["close"][0])
    return out


def daily_bars_close_crosscheck_findings(
    config: Config,
    trade_date: date,
    *,
    reference_closes=None,
) -> list[dict]:
    """Compare a liquid sample of that day's closes against an independent vendor.

    Runs only when ``[sources.sina]`` is enabled, so a lake configured without it
    (and every unit test) makes no network call. A source that is unreachable
    yields an info finding, never an audit failure — an unavailable second
    opinion is not evidence of bad data.

    ``reference_closes`` is injectable for tests.
    """
    if not config.sources.get("sina", False) and reference_closes is None:
        return []

    symbols = _liquid_symbols_on(config, trade_date, CLOSE_CROSSCHECK_SAMPLE)
    if not symbols:
        return []
    ours = _curated_closes(config, trade_date, symbols)
    if not ours:
        return []

    fetch = reference_closes or _sina_closes
    try:
        if reference_closes is None:
            theirs = fetch(symbols, trade_date, config=config)
        else:
            theirs = fetch(symbols, trade_date)
    except Exception as exc:  # noqa: BLE001 — a dead vendor must not fail the audit
        return [
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "close_crosscheck_unavailable",
                "message": f"could not reach the reference source for {trade_date.isoformat()}: {exc}",
            }
        ]

    compared: list[tuple[str, float, float]] = []
    for sym, ref in theirs.items():
        mine = ours.get(sym)
        if mine is None or ref <= 0:
            continue
        compared.append((sym, mine, ref))
    if not compared:
        return []

    mismatched = [
        (sym, mine, ref)
        for sym, mine, ref in compared
        if abs(mine - ref) / ref > CLOSE_CROSSCHECK_TOLERANCE
    ]
    if not mismatched:
        return []

    ratio = len(mismatched) / len(compared)
    systematic = ratio >= CLOSE_CROSSCHECK_SYSTEMATIC_RATIO
    sample = "; ".join(
        f"{sym} {mine:.2f} vs {ref:.2f} ({(mine - ref) / ref:+.2%})"
        for sym, mine, ref in mismatched[:_SAMPLE]
    )
    message = (
        f"{len(mismatched)}/{len(compared)} sampled closes on {trade_date.isoformat()} "
        f"disagree with the reference source ({sample})"
    )
    if systematic:
        message += (
            " — a whole-market disagreement, typically a capture that ran before "
            "the session closed; refetch the day"
        )
    return [
        {
            "dataset": "daily_bars",
            "severity": "error" if systematic else "warning",
            "check": "daily_bars_close_mismatch",
            "message": message,
            "trade_date": trade_date.isoformat(),
            "compared": len(compared),
            "mismatched": len(mismatched),
            "mismatch_ratio": round(ratio, 3),
            "symbols": [sym for sym, _, _ in mismatched[:_SAMPLE]],
        }
    ]


# --- ST label cross-check ----------------------------------------------------
# Flag only once the disagreement is past what naming lag explains: a name
# changes on the exchange the morning the label does, and `instruments` and
# `trading_status` are captured by different steps in the same run, so one or
# two names sitting on either side of that boundary is routine.
ST_CROSSCHECK_MAX_DISAGREEMENT = 3


def _st_from_names(instruments: pl.DataFrame) -> set[str] | None:
    """Symbols whose exchange short name carries an ST / *ST prefix.

    The short name is assigned by the exchange and travels with the security
    down a completely different pipe than the risk-warning board listing —
    ``instruments`` comes from the TDX binary protocol, ``trading_status`` from
    EastMoney HTTP. That makes the two genuinely independent readings of the
    same exchange fact, which is the property the retired AkShare union only
    appeared to have: it queried the same push2 endpoint with the same filter
    as the EastMoney adapter, so it could never disagree (issue #3 / #10).
    """
    if "name" not in instruments.columns or "symbol" not in instruments.columns:
        return None
    named = instruments.filter(pl.col("name").is_not_null())
    if "asset_type" in named.columns:
        # ST is a stock designation; an ETF that happens to carry those letters
        # in its short name would otherwise read as an unlabeled ST name.
        named = named.filter(pl.col("asset_type") == "stock")
    if named.is_empty():
        return None
    return {
        symbol for symbol, name in named.select(["symbol", "name"]).iter_rows() if is_st_name(name)
    }


def _active_instruments_on(instruments: pl.DataFrame, trade_date: date) -> pl.DataFrame:
    """Keep instruments that existed on the status observation date.

    The catalogue retains delisted names and their last exchange name. Using
    those historical names in a current-day ST cross-check creates a false
    disagreement because the risk-warning board no longer lists them.
    """
    active = instruments
    if "list_date" in active.columns:
        listed = pl.col("list_date").cast(pl.Date, strict=False)
        active = active.filter(listed.is_null() | (listed <= trade_date))
    if "delist_date" in active.columns:
        delisted = pl.col("delist_date").cast(pl.Date, strict=False)
        active = active.filter(delisted.is_null() | (delisted >= trade_date))
    return active


def st_label_crosscheck_findings(config: Config, trade_date: date) -> list[dict]:
    """``trading_status`` ST labels vs the ST prefix on the instrument's name.

    Reads only curated data — both sides are already fetched by the daily run,
    so this costs no requests. Measured on 2026-08-01 the two agreed exactly:
    205 names each, symmetric difference 0.
    """
    status_root = config.curated_root / "trading_status"
    if not dataset_has_parquet(status_root):
        return []
    instruments = _instruments_frame(config)
    if instruments is None:
        return []
    active_instruments = _active_instruments_on(instruments, trade_date)
    by_name = _st_from_names(active_instruments)
    if by_name is None:
        return []

    status = dedupe_lazy_by_primary_key(
        scan_parquet_root(
            status_root,
            partition_col="trade_date",
            start=trade_date,
            end=trade_date,
        ),
        "trading_status",
    )
    labeled = (
        status.filter(pl.col("trade_date") == pl.lit(trade_date))
        .filter(risk_warning_expr(status.collect_schema().names()))
        .select("symbol")
        .unique()
        .collect(engine="streaming")
    )
    if labeled.is_empty():
        # No ST rows for the day is a coverage question, not a disagreement —
        # `trading_status_st_coverage` in audit.py already owns that.
        return []
    by_board = set(labeled.get_column("symbol").to_list())

    # Only judge names the instrument list actually knows about; a symbol absent
    # from `instruments` is a universe gap, not an ST disagreement.
    known = set(active_instruments.get_column("symbol").to_list())
    by_board &= known

    board_only = sorted(by_board - by_name)
    name_only = sorted(by_name - by_board)
    total = len(board_only) + len(name_only)
    if total <= ST_CROSSCHECK_MAX_DISAGREEMENT:
        return []

    return [
        {
            "dataset": "trading_status",
            "severity": "warning",
            "check": "st_label_crosscheck",
            "message": (
                f"ST labels disagree with instrument names on {trade_date.isoformat()}: "
                f"{len(board_only)} labeled ST but not named ST, "
                f"{len(name_only)} named ST but not labeled — one of the two feeds is "
                "stale or the risk-warning board query changed shape"
            ),
            "trade_date": trade_date.isoformat(),
            "labeled_not_named": len(board_only),
            "named_not_labeled": len(name_only),
            "symbols": (board_only + name_only)[:_SAMPLE],
        }
    ]


def instrument_listing_order_findings(config: Config) -> list[dict]:
    """A security cannot stop trading before it starts.

    The delisting sweep infers a retirement from a Sina probe that answers the
    same way for a code that stopped trading and for one that was issued but has
    not opened yet. A fresh listing swept in that window is filed as a delisting,
    which then feeds ``delisting_events`` and makes the survivorship check above
    reason about names that never left. Eleven stored rows carried a delist_date
    earlier than their own list_date — all stamped 2026-09-01 against list dates
    of 2026-09-02..09-07, and their 'C'/'N' name prefixes mark them as new
    listings rather than retirements.

    Cheap, exact, and orthogonal to how the bad row arrived, so it holds even if
    a future source writes the pair some other way.
    """
    instruments = _instruments_frame(config)
    if instruments is None:
        return []
    if not {"symbol", "list_date", "delist_date"} <= set(instruments.columns):
        return []

    inverted = instruments.filter(
        pl.col("list_date").is_not_null()
        & pl.col("delist_date").is_not_null()
        & (pl.col("delist_date") < pl.col("list_date"))
    )
    if inverted.is_empty():
        return []

    sample = inverted.head(5).select("symbol", "list_date", "delist_date").to_dicts()
    return [
        {
            "dataset": "instruments",
            "severity": "error",
            "check": "instrument_listing_order",
            "message": (
                f"{inverted.height} instrument(s) carry a delist_date earlier than "
                "their list_date — a delisting was inferred for a security that had "
                "not started trading yet. These rows also reach delisting_events and "
                "the survivorship check"
            ),
            "rows": inverted.height,
            "sample": [
                {
                    "symbol": row["symbol"],
                    "list_date": row["list_date"].isoformat(),
                    "delist_date": row["delist_date"].isoformat(),
                }
                for row in sample
            ],
        }
    ]


def corporate_action_classification_findings(config: Config, start: date, end: date) -> list[dict]:
    """Two vendors describing one ex-date as different actions, or at 10x scale.

    ``action_type`` is in the primary key, so EastMoney filing 送 as ``transfer``
    while TDX files it as ``bonus`` produces two legal rows that describe one
    event. Thirty stored ex-dates look like that. Three more carry EastMoney at
    exactly ten times the TDX ratio — 603538.SH and 603585.SH read 0.4 against
    4.0 on 2026-07-09, 688557.SH 0.45 against 4.5 — a 每10股 / 每股 confusion.

    The unadjusted close settles which side is right: those three fell to 1/1.40,
    1/1.46 and 1/1.47 of the prior close, exactly a 送0.4/0.45 dilution. A real
    additional 转4.0 would have taken them to about a fifth.
    """
    root = config.curated_root / "corporate_actions"
    if not dataset_has_parquet(root):
        return []
    actions = dedupe_lazy_by_primary_key(
        scan_parquet_root(root, partition_col="ex_date", start=start, end=end),
        "corporate_actions",
    ).collect()
    needed = {"symbol", "ex_date", "bonus_ratio", "transfer_ratio", "source"}
    if actions.is_empty() or not needed <= set(actions.columns):
        return []

    bonus = actions.filter(pl.col("bonus_ratio").fill_null(0.0) > 0).select(
        "symbol", "ex_date", pl.col("bonus_ratio").alias("_b"), pl.col("source").alias("_b_src")
    )
    transfer = actions.filter(pl.col("transfer_ratio").fill_null(0.0) > 0).select(
        "symbol", "ex_date", pl.col("transfer_ratio").alias("_t"), pl.col("source").alias("_t_src")
    )
    paired = bonus.join(transfer, on=["symbol", "ex_date"], how="inner").filter(
        pl.col("_b_src") != pl.col("_t_src")
    )
    if paired.is_empty():
        return []

    duplicated = paired.filter((pl.col("_b") - pl.col("_t")).abs() < 1e-9)
    scaled = paired.filter((pl.col("_t") - pl.col("_b") * 10.0).abs() < 1e-9)
    # ``paired`` is a join, so a third source filing both columns multiplies the
    # rows for one event. Count events, not pairs.
    duplicated_events = duplicated.select("symbol", "ex_date").n_unique()
    scaled_events = scaled.select("symbol", "ex_date").n_unique()

    findings: list[dict] = []
    if duplicated_events:
        findings.append(
            {
                "dataset": "corporate_actions",
                "severity": "warning",
                "check": "corporate_action_duplicate_classification",
                "message": (
                    f"{duplicated_events} ex-date(s) carry the same ratio as 送 from one "
                    "source and 转 from another — one event stored twice. Anything that "
                    "adds bonus_ratio and transfer_ratio double-counts the dilution"
                ),
                "rows": duplicated_events,
            }
        )
    if scaled_events:
        findings.append(
            {
                "dataset": "corporate_actions",
                "severity": "error",
                "check": "corporate_action_ratio_scale",
                "message": (
                    f"{scaled_events} ex-date(s) carry a transfer_ratio exactly 10x the "
                    "bonus_ratio another source reports — a 每10股/每股 unit confusion"
                ),
                "rows": scaled_events,
                "sample": [
                    {
                        "symbol": row["symbol"],
                        "ex_date": row["ex_date"].isoformat(),
                        "bonus_ratio": row["_b"],
                        "transfer_ratio": row["_t"],
                        "bonus_source": row["_b_src"],
                        "transfer_source": row["_t_src"],
                    }
                    for row in scaled.unique(subset=["symbol", "ex_date"], keep="first")
                    .sort("symbol", "ex_date")
                    .head(5)
                    .to_dicts()
                ],
            }
        )
    return findings


# A balance sheet that does not balance is wrong no matter which vendor served
# it, so the tolerance is for rounding rather than for disagreement.
BALANCE_IDENTITY_TOLERANCE = 1e-4


# Where the identity breaks actually are. Measured 2026-09-19 over 286,689
# periods: 245 breaks, of which 181 are 2005 or earlier, 28 in 2006-2010, 18 in
# 2011-2015, 9 in 2016-2020 and 9 from 2021. An old annual report that does not
# foot is the published record and will never be restated; a recent one is
# worth a look, and there are few enough of them to actually look. The line
# sits where the concentration ends rather than at a round number.
MODERN_STATEMENT_YEAR = 2011


def balance_sheet_identity_findings(config: Config) -> list[dict]:
    """Assets = liabilities + equity, per (symbol, report_period).

    The one integrity test on a balance sheet that needs no second source: it
    holds by construction, so a breach is an upstream defect rather than a
    difference of opinion. Measured over the 2016-2024 backfill, 165,046 of
    165,064 periods held within a basis point; the failures are scale errors,
    such as 300885.SZ 2020Q1 reporting equity of 31.1bn against total assets of
    345m — a hundredfold slip on one field.
    """
    root = config.curated_root / "financial_statement_items"
    if not dataset_has_parquet(root):
        return []
    frame = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(root, partition_col="report_period"),
            "financial_statement_items",
        )
        .filter(
            (pl.col("statement_type") == "balance")
            & pl.col("item_code").is_in(["total_assets", "total_liabilities", "total_equity"])
        )
        .select("symbol", "report_period", "item_code", "item_value", "source")
        .collect()
    )
    if frame.is_empty():
        return []

    # The primary key carries announce_date, so a restated period holds several
    # rows per item; compare the latest reading of each.
    latest = (
        frame.group_by("symbol", "report_period", "item_code")
        .agg(pl.col("item_value").last().alias("value"), pl.col("source").last().alias("source"))
        .pivot(on="item_code", index=["symbol", "report_period"], values="value")
    )
    for column in ("total_assets", "total_liabilities", "total_equity"):
        if column not in latest.columns:
            return []
    checked = latest.drop_nulls(["total_assets", "total_liabilities", "total_equity"]).filter(
        pl.col("total_assets") != 0
    )
    if checked.is_empty():
        return []

    breached = checked.with_columns(
        (
            (pl.col("total_liabilities") + pl.col("total_equity") - pl.col("total_assets")).abs()
            / pl.col("total_assets").abs()
        ).alias("_rel")
    ).filter(pl.col("_rel") > BALANCE_IDENTITY_TOLERANCE)
    if breached.is_empty():
        return []

    def _sample(frame: pl.DataFrame) -> list[dict]:
        return [
            {
                "symbol": row["symbol"],
                "report_period": row["report_period"],
                "total_assets": row["total_assets"],
                "total_liabilities": row["total_liabilities"],
                "total_equity": row["total_equity"],
            }
            for row in frame.sort("_rel", descending=True).head(5).to_dicts()
        ]

    breached = breached.with_columns(
        pl.col("report_period").str.slice(0, 4).cast(pl.Int32, strict=False).alias("_year")
    )
    modern = breached.filter(pl.col("_year") >= MODERN_STATEMENT_YEAR)
    historical = breached.filter(
        pl.col("_year").is_null() | (pl.col("_year") < MODERN_STATEMENT_YEAR)
    )
    by_era = {
        str(year): count
        for year, count in sorted(
            breached.group_by("_year").len().iter_rows(),
            key=lambda item: (item[0] is None, item[0]),
        )
    }

    findings: list[dict] = []
    if not modern.is_empty():
        findings.append(
            {
                "dataset": "financial_statement_items",
                "severity": "warning",
                "check": "balance_sheet_identity",
                "message": (
                    f"{modern.height} of {checked.height} balance-sheet periods from "
                    f"{MODERN_STATEMENT_YEAR} on break assets = liabilities + equity by more "
                    "than a basis point"
                ),
                "rows": modern.height,
                "checked": checked.height,
                "since_year": MODERN_STATEMENT_YEAR,
                "sample": _sample(modern),
            }
        )
    if not historical.is_empty():
        findings.append(
            {
                "dataset": "financial_statement_items",
                "severity": "info",
                "check": "balance_sheet_identity_historical",
                "message": (
                    f"{historical.height} balance-sheet period(s) before "
                    f"{MODERN_STATEMENT_YEAR} break assets = liabilities + equity; these are "
                    "filings as published and will not be restated, so they are counted rather "
                    "than chased"
                ),
                "rows": historical.height,
                "checked": checked.height,
                "before_year": MODERN_STATEMENT_YEAR,
                "source_limited": True,
                "by_report_year": by_era,
                "sample": _sample(historical),
            }
        )
    return findings


def adj_factor_arbitration_findings(config: Config) -> list[dict]:
    """Ask a third source which side of an internal contradiction is wrong.

    ``adj_factors`` carries 19,088,826 rows from sina alone — no backup, no
    backfill, no failover entry. Every other check here compares the lake with
    itself, which cannot settle the case where its two internal series disagree:
    a recorded corporate action the factor never steps on, or a factor step with
    no recorded action. Measured 2026-09-09 there are 8,133 such (symbol, date)
    pairs, and the count rises every year.

    A 同花顺 snapshot of the same events breaks the tie. The verdict is
    directional rather than a pass/fail, because each combination points at a
    different series:

    * action recorded, factor still, peer agrees there was an event
      -> the factor missed a step
    * action recorded, factor still, peer has no event
      -> the recorded action is doubtful
    * factor stepped, no action recorded, peer has an event
      -> the lake is missing the action
    * factor stepped, no action recorded, peer has none either
      -> the factor step has no basis

    Silent without a snapshot, so a lake with no key is unaffected. Delisted
    securities cannot be arbitrated at all: the upstream refuses them, which is
    why the finding reports its own coverage.
    """
    from cnequity.storage.source_snapshots import SnapshotStore

    peer = SnapshotStore(config.meta_root).read_latest("corporate_actions", source="ths_official")
    if peer.is_empty() or not {"symbol", "ex_date"} <= set(peer.columns):
        return []

    factors_root = config.derived_root / "adj_factors"
    actions_root = config.curated_root / "corporate_actions"
    if not dataset_has_parquet(factors_root) or not dataset_has_parquet(actions_root):
        return []

    factors = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(factors_root, partition_col="trade_date"), "adj_factors"
        )
        .filter(pl.col("adjust_type") == "hfq")
        .select("symbol", "trade_date", "factor")
        .collect()
        .sort(["symbol", "trade_date"])
    )
    if factors.is_empty():
        return []
    jumps = (
        factors.with_columns(
            (pl.col("factor") / pl.col("factor").shift(1).over("symbol") - 1).abs().alias("_chg")
        )
        .filter(pl.col("_chg") > 1e-6)
        .select("symbol", pl.col("trade_date").alias("ex_date"))
    )
    if jumps.is_empty():
        return []

    floor = jumps.get_column("ex_date").min()
    actions = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(actions_root, partition_col="ex_date"), "corporate_actions"
        )
        .filter(pl.col("ex_date") >= floor)
        .select("symbol", "ex_date")
        .unique()
        .collect()
    )
    # Only compare where a comparison is meaningful: the symbol has a factor
    # series at all, and the date is one the market was open on.
    covered = factors.get_column("symbol").unique().to_list()
    sessions = _open_sessions(config)
    if sessions is not None:
        actions = actions.filter(pl.col("ex_date").is_in(sessions))
    actions = actions.filter(pl.col("symbol").is_in(covered))

    peer_dates = peer.select("symbol", "ex_date").unique()
    peer_symbols = peer.get_column("symbol").unique().to_list()

    silent = actions.join(jumps, on=["symbol", "ex_date"], how="anti")
    baseless = jumps.filter(pl.col("ex_date") >= floor).join(
        actions, on=["symbol", "ex_date"], how="anti"
    )

    def _split(frame: pl.DataFrame) -> tuple[pl.DataFrame, int, int]:
        in_scope = frame.filter(pl.col("symbol").is_in(peer_symbols))
        confirmed = in_scope.join(peer_dates, on=["symbol", "ex_date"], how="inner")
        return confirmed, in_scope.height - confirmed.height, frame.height - in_scope.height

    silent_confirmed, silent_no, silent_out = _split(silent)
    baseless_confirmed, baseless_no, baseless_out = _split(baseless)
    silent_yes = silent_confirmed.height
    baseless_yes = baseless_confirmed.height
    total = silent.height + baseless.height
    if not total:
        return []

    # Counts alone name no next step. The one bucket with a command behind it
    # is "the peer has the event and we do not": for an ex-date older than the
    # EastMoney backfill floor, the report still serves it by exact date, which
    # is the only route that reaches it. Later dates are ones the normal
    # sources already walked, so a gap there means no configured source
    # carries the event, not that a sweep was skipped.
    reachable = sorted(
        {
            value.isoformat()
            for value in baseless_confirmed.filter(pl.col("ex_date") < EASTMONEY_BACKFILL_FLOOR)
            .get_column("ex_date")
            .to_list()
        }
    )
    remediation = ""
    if reachable:
        shown = ",".join(reachable[:_SAMPLE])
        more = f" (+{len(reachable) - _SAMPLE} more)" if len(reachable) > _SAMPLE else ""
        remediation = (
            f" {len(reachable)} of the missing actions fall before the EastMoney backfill "
            f"floor {EASTMONEY_BACKFILL_FLOOR.isoformat()}, which the sweep cannot reach; ask "
            "the report for them by exact date with `cne backfill corporate_actions "
            f"--eastmoney-date-repair --ex-dates {shown}`{more}. It answers one date at a time "
            "and does not carry every older event, so a date it has nothing for stays open. "
            "The later dates were already walked by the configured sources."
        )
    against_factors = silent_yes + baseless_no
    against_actions = silent_no + baseless_yes
    unarbitrated = silent_out + baseless_out
    return [
        {
            "dataset": "adj_factors",
            "severity": "info" if not (against_factors or against_actions) else "warning",
            "check": "adj_factor_arbitration",
            "message": (
                f"{total} factor/action contradiction(s); the peer settles "
                f"{total - unarbitrated}: {against_factors} point at the factor series, "
                f"{against_actions} at the recorded actions, {unarbitrated} unarbitrated "
                "because the peer does not carry those securities"
                # Findings print their message and nothing else, so a
                # remediation nobody reads is a remediation nobody runs.
                f"{remediation}"
            ),
            "contradictions": total,
            "against_factor_series": against_factors,
            "against_recorded_actions": against_actions,
            "unarbitrated": unarbitrated,
            "factor_missed_a_step": silent_yes,
            "recorded_action_doubtful": silent_no,
            "missing_recorded_action": baseless_yes,
            "factor_step_without_basis": baseless_no,
            "missing_recorded_action_reachable_dates": reachable,
            "remediation": remediation,
        }
    ]


def unrecorded_ex_event_findings(
    config: Config,
    trade_date: date,
    *,
    exclude: set[tuple[str, str]] | None = None,
) -> list[dict]:
    """A recent factor step with no corporate action behind it.

    `missing_corporate_action` reads this from the price, and needs an 11%
    raw/adjusted divergence before it speaks — the right bar for a dividend,
    the wrong one for a fund unit split, which restates the reference price by
    exactly its ratio and can be far smaller than that. The factor series
    states the ratio outright, so ask it instead, and only over the window a
    run can still act on (`EX_EVENT_LOOKBACK_SESSIONS`); deep history stays
    with the price-based check, which is what keeps this quiet.

    Pairs already reported by the price-based check are excluded: two lines
    about one day is not twice the information.
    """
    steps = unexplained_factor_steps(config, upto=trade_date)
    if steps.is_empty():
        return []
    skip = exclude or set()
    rows = [
        row
        for row in steps.iter_rows(named=True)
        if (row["symbol"], _iso(row["ex_date"])) not in skip
    ]
    if not rows:
        return []
    return _capped_findings(
        pl.DataFrame(rows),
        lambda row: {
            "dataset": "corporate_actions",
            "symbol": row["symbol"],
            "severity": "warning",
            "check": "unrecorded_ex_event",
            "message": (
                f"{row['symbol']}: the hfq factor stepped x{row['factor_ratio']:.4f} on "
                f"{_iso(row['ex_date'])} with no corporate action on record; the daily "
                "dividend report carries no unit splits, so a 份额折算 arrives only from "
                "`cne backfill corporate_actions --symbols "
                f"{row['symbol']}`"
            ),
            "trade_date": _iso(row["ex_date"]),
            "factor_ratio": round(float(row["factor_ratio"]), 6),
            "lookback_sessions": EX_EVENT_LOOKBACK_SESSIONS,
        },
        dataset="corporate_actions",
        check="unrecorded_ex_event",
        severity="warning",
        noun="a factor step with no recorded action",
    )


def _open_sessions(config: Config) -> list[date] | None:
    root = config.curated_root / "trading_calendar"
    if not dataset_has_parquet(root):
        return None
    frame = scan_parquet_root(root, partition_col="trade_date").collect()
    if "is_trading" not in frame.columns:
        return None
    return frame.filter(pl.col("is_trading")).get_column("trade_date").unique().to_list()


# One tick on a ten-yuan share is 10bps, so a tie-break threshold below that
# would arbitrate rounding. `daily_bars` failover already uses 10bps.
BAR_ARBITRATION_TOLERANCE_BPS = 10.0


def daily_bars_arbitration_findings(config: Config) -> list[dict]:
    """When the two incumbents disagree on a close, ask a third.

    ``daily_bars`` from 2016 runs tdx against eastmoney with a revision gate.
    That gate has to decide something on a disagreement, and a binary comparison
    gives it nothing to decide with: it can block a good day on vendor noise or
    pass a real break, and there is no way to tell those apart from inside.

    A 同花顺 snapshot breaks the tie by siding with one incumbent or neither.
    "Neither" is itself informative — it means the two agreeing sources are both
    away from a third, which is the shape of a definitional difference rather
    than a defect.

    Silent without a snapshot, so a lake with no key keeps exactly the checks it
    had.
    """
    from cnequity.storage.source_snapshots import SnapshotStore

    store = SnapshotStore(config.meta_root)
    peer = store.read_latest("daily_bars", source="ths_official")
    backup = store.read_latest("daily_bars", source="eastmoney")
    needed = {"symbol", "trade_date", "close"}
    if peer.is_empty() or backup.is_empty():
        return []
    if not needed <= set(peer.columns) or not needed <= set(backup.columns):
        return []

    root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(root):
        return []
    dates = backup.get_column("trade_date").unique().to_list()
    primary = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(root, partition_col="trade_date", start=min(dates), end=max(dates)),
            "daily_bars",
        )
        .select("symbol", "trade_date", "close", "source")
        .collect()
    )
    if primary.is_empty():
        return []

    pair = primary.join(
        backup.select("symbol", "trade_date", pl.col("close").alias("_backup")),
        on=["symbol", "trade_date"],
        how="inner",
    ).filter(pl.col("close") != 0)
    if pair.is_empty():
        return []
    disputed = pair.with_columns(
        ((pl.col("_backup") - pl.col("close")).abs() / pl.col("close").abs() * 10_000).alias("_bps")
    ).filter(pl.col("_bps") > BAR_ARBITRATION_TOLERANCE_BPS)
    if disputed.is_empty():
        return []

    judged = disputed.join(
        peer.select("symbol", "trade_date", pl.col("close").alias("_peer")),
        on=["symbol", "trade_date"],
        how="inner",
    )
    if judged.is_empty():
        return [
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_arbitration",
                "message": (
                    f"{disputed.height} close disagreement(s) between the primary and its "
                    "backup, none covered by the third-source snapshot"
                ),
                "disputed": disputed.height,
                "arbitrated": 0,
            }
        ]

    judged = judged.with_columns(
        ((pl.col("_peer") - pl.col("close")).abs() / pl.col("close").abs() * 10_000).alias(
            "_to_primary"
        ),
        ((pl.col("_peer") - pl.col("_backup")).abs() / pl.col("_backup").abs() * 10_000).alias(
            "_to_backup"
        ),
    )
    backs_primary = judged.filter(
        (pl.col("_to_primary") <= BAR_ARBITRATION_TOLERANCE_BPS)
        & (pl.col("_to_backup") > BAR_ARBITRATION_TOLERANCE_BPS)
    ).height
    backs_backup = judged.filter(
        (pl.col("_to_backup") <= BAR_ARBITRATION_TOLERANCE_BPS)
        & (pl.col("_to_primary") > BAR_ARBITRATION_TOLERANCE_BPS)
    ).height
    backs_neither = judged.height - backs_primary - backs_backup
    return [
        {
            "dataset": "daily_bars",
            "severity": "warning" if backs_backup else "info",
            "check": "daily_bars_arbitration",
            "message": (
                f"{disputed.height} primary/backup close disagreement(s); the third source "
                f"settles {judged.height}: {backs_primary} for the primary, "
                f"{backs_backup} for the backup, {backs_neither} for neither"
            ),
            "disputed": disputed.height,
            "arbitrated": judged.height,
            "supports_primary": backs_primary,
            "supports_backup": backs_backup,
            "supports_neither": backs_neither,
            "tolerance_bps": BAR_ARBITRATION_TOLERANCE_BPS,
        }
    ]


# Statement values are reported to the yuan, so anything under 10bps is the two
# vendors rounding a restated figure differently rather than disagreeing.
STATEMENT_PEER_TOLERANCE = 1e-3


def financial_statement_peer_findings(config: Config) -> list[dict]:
    """Compare the statements the lake holds from one source against a peer.

    ``income`` and ``indicator`` come entirely from EastMoney — 1,624,060 and
    1,058,909 rows — and nothing checks them. A restated figure, a caliber
    change or a plain transcription error all look identical from inside a
    single-sourced dataset.

    Measured 2026-09-12 over 30 securities and nine years, the two agree on
    ``net_profit`` 958 times in 960 and on ``revenue`` 925 in 960. The other
    mapped codes matched exactly. So a breach here is worth reading: at these
    rates, disagreement is rare enough to be a signal.

    Reported per item code, because the rates differ by an order of magnitude
    between them and one aggregate number would hide that. Silent without a
    snapshot.
    """
    from cnequity.storage.source_snapshots import SnapshotStore

    peer = SnapshotStore(config.meta_root).read_latest(
        "financial_statement_items", source="ths_official"
    )
    needed = {"symbol", "report_period", "item_code", "item_value"}
    if peer.is_empty() or not needed <= set(peer.columns):
        return []

    root = config.curated_root / "financial_statement_items"
    if not dataset_has_parquet(root):
        return []
    periods = peer.get_column("report_period").unique().to_list()
    curated = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(root, partition_col="report_period"),
            "financial_statement_items",
        )
        .filter(pl.col("report_period").is_in(periods))
        .select("symbol", "report_period", "item_code", "item_value", "source")
        .collect()
    )
    if curated.is_empty():
        return []

    # The primary key carries announce_date, so a restated period holds several
    # rows per item; compare the latest reading of each.
    latest = curated.group_by("symbol", "report_period", "item_code").agg(
        pl.col("item_value").last().alias("_curated"), pl.col("source").last().alias("_source")
    )
    peer_latest = peer.group_by("symbol", "report_period", "item_code").agg(
        pl.col("item_value").last().alias("_peer")
    )
    pair = (
        latest.join(peer_latest, on=["symbol", "report_period", "item_code"], how="inner")
        .drop_nulls(["_curated", "_peer"])
        .filter(pl.col("_curated") != 0)
    )
    if pair.is_empty():
        return []

    scored = pair.with_columns(
        ((pl.col("_peer") - pl.col("_curated")).abs() / pl.col("_curated").abs()).alias("_rel")
    )
    by_code = (
        scored.group_by("item_code")
        .agg(
            pl.len().alias("compared"),
            (pl.col("_rel") > STATEMENT_PEER_TOLERANCE).sum().alias("disagreed"),
        )
        .filter(pl.col("disagreed") > 0)
        .sort("disagreed", descending=True)
    )
    if by_code.is_empty():
        return []

    total = int(by_code.get_column("disagreed").sum())
    return [
        {
            "dataset": "financial_statement_items",
            "severity": "warning",
            "check": "financial_statement_peer",
            "message": (
                f"{total} value(s) differ from the peer by more than "
                f"{STATEMENT_PEER_TOLERANCE:.1%} across {scored.height} compared"
            ),
            "compared": scored.height,
            "disagreed": total,
            "by_item_code": [
                {
                    "item_code": row["item_code"],
                    "compared": row["compared"],
                    "disagreed": row["disagreed"],
                }
                for row in by_code.to_dicts()
            ],
        }
    ]


def untraded_instrument_findings(config: Config, trade_date: date) -> list[dict]:
    """Securities in ``daily_bars`` that never print a trade.

    A price with no volume and no turnover behind it is a net asset value, not a
    quote. The open-end fund code space reached the lake this way: 519xxx on the
    SSE matched a "51" ETF prefix, and TDX answered with 436,533 NAV rows that
    every liquidity screen, turnover aggregate and tradable-universe filter then
    treated as market data.

    Structural rather than statistical, which is what makes it cheap: one real
    session is enough to tell a quoted security from an unquoted one, and a
    genuine security that is merely halted still carries prints either side of
    the halt. The window is a year so a thin-but-real name cannot be caught by a
    quiet fortnight.
    """
    root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(root):
        return []
    start = trade_date - timedelta(days=365)
    bars = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(root, partition_col="trade_date", start=start, end=trade_date),
            "daily_bars",
        )
        .select("symbol", "volume", "amount")
        .collect()
    )
    if bars.is_empty():
        return []

    per_symbol = bars.group_by("symbol").agg(
        pl.len().alias("rows"),
        pl.col("volume").fill_null(0).max().alias("_max_volume"),
        pl.col("amount").fill_null(0).max().alias("_max_amount"),
    )
    # A handful of rows proves nothing: a security listed last week can be quiet.
    untraded = per_symbol.filter(
        (pl.col("_max_volume") <= 0) & (pl.col("_max_amount") <= 0) & (pl.col("rows") >= 20)
    )
    if untraded.is_empty():
        return []

    sample = untraded.sort("rows", descending=True).head(5)
    return [
        {
            "dataset": "daily_bars",
            "severity": "warning",
            "check": "untraded_instruments",
            "message": (
                f"{untraded.height} symbol(s) carry {int(untraded.get_column('rows').sum())} "
                "bar(s) with no volume and no turnover in the last year — a NAV series "
                "rather than a quoted price"
            ),
            "symbols": untraded.height,
            "rows": int(untraded.get_column("rows").sum()),
            "sample": [{"symbol": row["symbol"], "rows": row["rows"]} for row in sample.to_dicts()],
        }
    ]


def _policy_base(source: str, registered: set[str]) -> str | None:
    """Resolve a provenance sub-label to the registered source it inherits from.

    Adapters refine provenance beyond the vendor — `eastmoney_cached`,
    `derived_bar_gap`, `exchange_calendar` — and those inherit the base label's
    terms. Longest match wins so `eastmoney_kline+sina_global` is not mistaken
    for plain `eastmoney`.
    """
    for candidate in sorted(registered, key=len, reverse=True):
        if source == candidate or source.startswith(f"{candidate}_"):
            return candidate
    return None


def undeclared_source_findings(config: Config) -> list[dict]:
    """Sources in the data that the compliance registry cannot speak for.

    ``policies_for_dataset`` answers "which terms apply to this data" from the
    ``DatasetSpec`` routing fields — primary, backup, backfill. Anything that
    reached curated another way is invisible to it, and a compliance matrix with
    a blind spot is worse than none, because it answers confidently.

    Two failures, reported separately because they need different responses:

    *Unregistered.* No policy covers the label or any base it inherits from, so
    its terms are simply unknown. ``bse`` is one: 654 rows in ``daily_bars`` and
    two sub-labels in ``trading_status``, and ``sources/SOURCES.yml`` has never
    carried it.

    *Registered but unrouted.* A policy exists and the dataset declares no
    route to the source at all, so ``policies_for_dataset`` omits terms that do
    apply — and ``ths_official``, the label this catches most often, is the
    policy whose redistribution field reads ``unknown``. A source reached only
    by an explicit repair command is declared as such (``repair_sources``) and
    carried in ``DatasetPolicy.repair``: the matrix speaks for it without the
    resilience report mistaking a manual command for a fallback.

    Reads what is stored rather than what the config intends, so it catches the
    next out-of-band writer whatever route it takes.
    """
    from cnequity.compliance.source_policy import load_source_policies
    from cnequity.domain.datasets import DATASETS

    try:
        registered = set(load_source_policies())
    except Exception:  # noqa: BLE001 — a missing registry is not this check's business
        return []

    unregistered: dict[str, list[str]] = {}
    unrouted: dict[str, list[str]] = {}
    for name, spec in sorted(DATASETS.items()):
        root = (config.derived_root if spec.layer == "derived" else config.curated_root) / name
        if not dataset_has_parquet(root):
            continue
        try:
            present = (
                scan_parquet_root(root, partition_col=spec.partition_col)
                .select("source")
                .unique()
                .collect()
                .get_column("source")
                .to_list()
            )
        except Exception:  # noqa: BLE001 — a dataset with no source column is fine
            continue
        declared = {
            value
            for value in (
                spec.primary_source,
                spec.backup_source,
                spec.backfill_source,
                *getattr(spec, "supplementary_sources", ()),
                # An operator-invoked repair is a declared writer too: its
                # terms are in the matrix (`DatasetPolicy.repair`), it is just
                # not part of any automatic route.
                *getattr(spec, "repair_sources", ()),
            )
            if value
        }
        for value in sorted(filter(None, present)):
            base = _policy_base(value, registered)
            if base is None:
                unregistered.setdefault(value, []).append(name)
            elif base not in declared and value not in declared:
                unrouted.setdefault(base, []).append(name)

    findings: list[dict] = []
    if unregistered:
        findings.append(
            {
                "dataset": "sources",
                "severity": "error",
                "check": "unregistered_source",
                "message": (
                    f"{len(unregistered)} source label(s) in curated have no policy entry and "
                    "no registered base, so their terms are undetermined: "
                    + ", ".join(sorted(unregistered))
                ),
                "sources": {key: sorted(set(value)) for key, value in sorted(unregistered.items())},
            }
        )
    if unrouted:
        findings.append(
            {
                "dataset": "sources",
                "severity": "warning",
                "check": "unrouted_source",
                "message": (
                    f"{len(unrouted)} registered source(s) wrote rows to datasets that do not "
                    "route to them, so policies_for_dataset omits their terms: "
                    + ", ".join(sorted(unrouted))
                ),
                "sources": {key: sorted(set(value)) for key, value in sorted(unrouted.items())},
            }
        )
    return findings
