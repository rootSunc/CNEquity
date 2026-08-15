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

from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.query.parquet_scan import dataset_has_parquet, scan_parquet_root

_SAMPLE = 8
# Flag when valuation covers less than this share of symbols with bars that day.
# Also the gate for watermark advance / baostock history tip isolation.
VALUATION_COVERAGE_WARN_RATIO = 0.7
_VALUATION_COVERAGE_WARN_RATIO = VALUATION_COVERAGE_WARN_RATIO

# Error: |adj_ret| and |adj_ret - raw_ret| both above this on consecutive TDs
# (beyond board limits; not a real ex-event).
ADJ_DISCONTINUITY_RET = 0.35

# Warning: adj continuous but raw diverges past board limit with no CA on record.
MISSING_EVENT_MAX_ADJ_RET = 0.15
MISSING_EVENT_MIN_DIVERGENCE = 0.11

_MAX_RECON_FINDINGS = 50

# --- adjustment-factor coverage ---------------------------------------------
# adj_factors comes from Sina, daily_bars from TDX, and the two do not cover the
# same market: Sina's factor series essentially skips 北交所. `load(adjust=…)`
# defaults to strict_adj=False, so a bar with no factor is returned at
# factor=1.0 — a raw price inside a result the caller asked to have adjusted,
# marked only by an `adj_is_exact` column most callers never select.
#
# Measured on a full lake: 260 of 6,128 stocks had no factor at all (252 BJ),
# and a one-year `universe="all_a"` hfq window carried 10,480 such rows, 10,461
# of them a real close>0. None of it raised, and none of it appeared in an
# audit — which is what this check is for. Reported per exchange, because
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


def _trading_days(config: Config, trade_date: date) -> set[date]:
    cal_root = config.curated_root / "trading_calendar"
    if not dataset_has_parquet(cal_root):
        return set()
    cal = (
        scan_parquet_root(cal_root, partition_col="trade_date", end=trade_date)
        .filter(pl.col("is_trading"))
        .select("trade_date")
        .unique()
        .collect()
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

    bars_dates = set(
        scan_parquet_root(bars_root, partition_col="trade_date", end=trade_date)
        .select("trade_date")
        .unique()
        .collect()["trade_date"]
        .to_list()
    )
    if not bars_dates:
        return findings

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

    # Trading days in the covered span with zero bars from any symbol.
    first, last = min(bars_dates), max(bars_dates)
    expected = {d for d in trading_days if first <= d <= last}
    missing = sorted(expected - bars_dates)
    if missing:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "error",
                "check": "daily_bars_calendar_missing_day",
                "message": (
                    f"{len(missing)} calendar trading day(s) in "
                    f"{first.isoformat()}..{last.isoformat()} have zero bars "
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
    from cn_market_lake.adapters.calendar.holidays_cn import CLOSED_DATES

    cal_root = config.curated_root / "trading_calendar"
    if not dataset_has_parquet(cal_root) or not CLOSED_DATES:
        return []
    table_end = date.fromisoformat(max(CLOSED_DATES))

    written = (
        scan_parquet_root(cal_root)
        .filter(pl.col("is_trading"))
        .select(pl.col("trade_date").max().alias("last"))
        .collect()
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
        scan_parquet_root(bars_root, partition_col="trade_date", start=trade_date, end=trade_date)
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
    from cn_market_lake.query.parquet_scan import list_partitions

    val_root = config.curated_root / "valuation_metrics"
    if not dataset_has_parquet(val_root):
        return None
    parts = list_partitions(val_root, "trade_date")
    for part in reversed(parts):
        # Day partitions: value is the date. Coarser periods: probe the max date
        # inside the period via coverage on that day only after we know it.
        if part.start == part.end:
            candidates = [part.start]
        else:
            ratio_end = valuation_day_coverage_ratio(config, part.end)
            if ratio_end is not None and ratio_end >= min_ratio:
                return part.end
            # Fall through: sample the period end only; full scan is for day layout.
            candidates = [part.end]
        for d in candidates:
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


def valuation_bars_coverage_findings(config: Config, trade_date: date) -> list[dict]:
    """valuation_metrics vs daily_bars: orphan symbols + one-day coverage ratio."""
    findings: list[dict] = []
    val_root = config.curated_root / "valuation_metrics"
    bars_root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(val_root) or not dataset_has_parquet(bars_root):
        return findings

    val_syms_all = set(
        scan_parquet_root(val_root, partition_col="trade_date", end=trade_date)
        .select("symbol")
        .unique()
        .collect()["symbol"]
        .to_list()
    )
    bars_syms_all = set(
        scan_parquet_root(bars_root, partition_col="trade_date", end=trade_date)
        .select("symbol")
        .unique()
        .collect()["symbol"]
        .to_list()
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

    val_dates = set(
        scan_parquet_root(val_root, partition_col="trade_date", end=trade_date)
        .select("trade_date")
        .unique()
        .collect()["trade_date"]
        .to_list()
    )
    bars_dates = set(
        scan_parquet_root(bars_root, partition_col="trade_date", end=trade_date)
        .select("trade_date")
        .unique()
        .collect()["trade_date"]
        .to_list()
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
        scan_parquet_root(bars_root, partition_col="trade_date", start=anchor, end=anchor)
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


def _adjusted_returns(config: Config, trade_date: date) -> pl.DataFrame | None:
    """Per (symbol, day) hfq adj vs raw returns + previous bar date. None if missing data."""
    bars_root = config.curated_root / "daily_bars"
    af_root = config.derived_root / "adj_factors"
    if not dataset_has_parquet(bars_root) or not dataset_has_parquet(af_root):
        return None

    bars = (
        scan_parquet_root(bars_root, partition_col="trade_date", end=trade_date)
        .select("symbol", "trade_date", "close")
        .collect()
    )
    factors = (
        scan_parquet_root(af_root, partition_col="trade_date", end=trade_date)
        .filter(pl.col("adjust_type") == "hfq")
        .select("symbol", "trade_date", "factor")
        .collect()
    )
    if bars.height < 2 or factors.is_empty():
        return None

    joined = bars.join(factors, on=["symbol", "trade_date"], how="inner").filter(
        pl.col("close").is_not_null() & (pl.col("close") > 0) & (pl.col("factor") > 0)
    )
    if joined.height < 2:
        return None

    return (
        joined.with_columns((pl.col("close") * pl.col("factor")).alias("_adj"))
        .sort(["symbol", "trade_date"])
        .with_columns(
            (pl.col("close") / pl.col("close").shift(1).over("symbol") - 1).alias("raw_ret"),
            (pl.col("_adj") / pl.col("_adj").shift(1).over("symbol") - 1).alias("adj_ret"),
            pl.col("trade_date").shift(1).over("symbol").alias("prev_trade_date"),
        )
        .filter(pl.col("prev_trade_date").is_not_null())
        .with_columns((pl.col("adj_ret") - pl.col("raw_ret")).abs().alias("divergence"))
        .select("symbol", "prev_trade_date", "trade_date", "raw_ret", "adj_ret", "divergence")
    )


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


def _trading_day_successors(config: Config, trade_date: date) -> pl.DataFrame | None:
    """[prev_trade_date, next_td]. None if calendar missing (then no adjacency filter)."""
    cal_root = config.curated_root / "trading_calendar"
    if not dataset_has_parquet(cal_root):
        return None
    cal = (
        scan_parquet_root(cal_root, partition_col="trade_date", end=trade_date)
        .filter(pl.col("is_trading"))
        .select("trade_date")
        .unique()
        .collect()
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
    """Flag exchanges whose stocks largely have no adjustment factor.

    See ``ADJ_COVERAGE_WARN_RATIO``. Scoped to ``asset_type='stock'``: ETFs and
    LOFs legitimately have no hfq factor series and would otherwise bury the
    signal this exists to raise.
    """
    bars_root = config.curated_root / "daily_bars"
    fac_root = config.derived_root / "adj_factors"
    inst_root = config.curated_root / "instruments"
    if not (dataset_has_parquet(bars_root) and dataset_has_parquet(fac_root)):
        return []
    if not dataset_has_parquet(inst_root):
        return []

    instruments = scan_parquet_root(inst_root).collect()
    if "asset_type" not in instruments.columns:
        return []
    stocks = set(instruments.filter(pl.col("asset_type") == "stock")["symbol"].to_list())
    if not stocks:
        return []

    priced = set(
        scan_parquet_root(bars_root).select("symbol").unique().collect()["symbol"].to_list()
    )
    with_factor = set(
        scan_parquet_root(fac_root).select("symbol").unique().collect()["symbol"].to_list()
    )

    findings: list[dict] = []
    by_exchange: dict[str, list[str]] = {}
    for symbol in stocks & priced:
        by_exchange.setdefault(symbol.rsplit(".", 1)[-1], []).append(symbol)

    for exchange, symbols in sorted(by_exchange.items()):
        total = len(symbols)
        if not total:
            continue
        covered = sum(1 for s in symbols if s in with_factor)
        ratio = covered / total
        if ratio >= ADJ_COVERAGE_WARN_RATIO:
            continue
        missing = total - covered
        findings.append(
            {
                "dataset": "adj_factors",
                "severity": "warning",
                "check": "adj_factor_coverage",
                "exchange": exchange,
                "message": (
                    f"{exchange}: {missing} of {total} priced stocks have no adjustment "
                    f"factor ({ratio:.0%} covered). load(adjust='hfq') returns those bars "
                    "unadjusted at factor=1.0 unless strict_adj=True — check adj_is_exact"
                ),
                "symbols_total": total,
                "symbols_covered": covered,
                "symbols_missing": missing,
                "coverage_ratio": round(ratio, 4),
                "sample": sorted(s for s in symbols if s not in with_factor)[:_SAMPLE],
            }
        )
    return findings


def adj_factor_reconciliation_findings(config: Config, trade_date: date) -> list[dict]:
    """hfq continuity vs corporate_actions; errors/warnings capped per class."""
    rets = _adjusted_returns(config, trade_date)
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
        .collect()
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

    # Delisted names get their own bucket, not because the gap is fake, but
    # because it is a *different, already-diagnosed* gap. Both tdx_protocol
    # (xdxr) and the eastmoney backup were checked live against a sample of
    # these symbols and neither returns any corporate-action history for a
    # name once it drops off their live symbol list — a vendor behavior tied
    # to delisting, not a market-id or filter bug (measured 2026-08: 109 of
    # 111 flagged symbols were delisted; the 2 still-listed ones were a genuine
    # stale-fetch gap and an isolated 2007 event). Filing 109 near-identical
    # "warning"s for one proven, unfixable-with-current-sources root cause
    # buries the handful that are actually worth investigating.
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
        findings.append(
            {
                "dataset": "corporate_actions",
                "severity": "info",
                "check": "missing_corporate_action_delisted",
                "message": (
                    f"{delisted.height} delisted symbol(s) have a raw/hfq return divergence "
                    "with no corporate action on record, all after their delist_date. "
                    "Verified live against both tdx_protocol and the eastmoney backup: "
                    "neither serves corporate-action history for a name once it is gone "
                    "from their live symbol list. Not a market-id or filter bug — see "
                    "docs/datasets/sources.md#corporate_actions"
                ),
                "symbols_total": delisted.height,
                "sample": sorted(delisted["symbol"].to_list())[:_SAMPLE],
            }
        )
    return findings


def _symbol_last_bar(config: Config, trade_date: date) -> pl.DataFrame | None:
    """Per-symbol first/last bar date in curated ``daily_bars``. None if absent."""
    bars_root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(bars_root):
        return None
    out = (
        scan_parquet_root(bars_root, partition_col="trade_date", end=trade_date)
        .group_by("symbol")
        .agg(
            pl.col("trade_date").min().alias("first_bar"),
            pl.col("trade_date").max().alias("last_bar"),
        )
        .collect()
    )
    return None if out.is_empty() else out


def _instruments_frame(config: Config) -> pl.DataFrame | None:
    root = config.curated_root / "instruments"
    if not dataset_has_parquet(root):
        return None
    out = scan_parquet_root(root, hive=False).collect()
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

    instruments = _instruments_frame(config)
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
    lf = scan_parquet_root(root, partition_col="trade_date", start=trade_date, end=trade_date)
    cols = lf.collect_schema().names()
    rank_col = "amount" if "amount" in cols else "volume"
    df = (
        lf.filter(pl.col("close").is_not_null() & (pl.col("close") > 0))
        .select("symbol", rank_col)
        .sort(rank_col, descending=True, nulls_last=True)
        .limit(limit)
        .collect()
    )
    return df["symbol"].to_list()


def _curated_closes(config: Config, trade_date: date, symbols: list[str]) -> dict[str, float]:
    root = config.curated_root / "daily_bars"
    df = (
        scan_parquet_root(root, partition_col="trade_date", start=trade_date, end=trade_date)
        .filter(pl.col("symbol").is_in(symbols))
        .select("symbol", "close")
        .collect()
    )
    return {r["symbol"]: float(r["close"]) for r in df.iter_rows(named=True)}


def _sina_closes(symbols: list[str], trade_date: date) -> dict[str, float]:
    import httpx

    from cn_market_lake.adapters.sina.bars import fetch_daily_bars_sina

    out: dict[str, float] = {}
    with httpx.Client(timeout=20.0) as client:
        for sym in symbols:
            df = fetch_daily_bars_sina(
                sym, start=trade_date, end=trade_date, datalen=30, client=client
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
    return set(
        named.filter(pl.col("name").str.replace_all(" ", "").str.to_uppercase().str.contains("ST"))
        .get_column("symbol")
        .to_list()
    )


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
    by_name = _st_from_names(instruments)
    if by_name is None:
        return []

    labeled = (
        scan_parquet_root(status_root, partition_col="trade_date", end=trade_date)
        .filter(pl.col("trade_date") == pl.lit(trade_date))
        .filter(pl.col("status") == "st")
        .select("symbol")
        .unique()
        .collect()
    )
    if labeled.is_empty():
        # No ST rows for the day is a coverage question, not a disagreement —
        # `trading_status_st_coverage` in audit.py already owns that.
        return []
    by_board = set(labeled.get_column("symbol").to_list())

    # Only judge names the instrument list actually knows about; a symbol absent
    # from `instruments` is a universe gap, not an ST disagreement.
    known = set(instruments.get_column("symbol").to_list())
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
