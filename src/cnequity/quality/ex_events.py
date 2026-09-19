"""Factor steps nobody recorded, over a bounded recent window.

The price-based check (`missing_corporate_action`) asks whether the raw and
adjusted returns diverge by more than 11%, which is the right question for a
dividend and the wrong one for a fund unit split: a 1-for-1.1 restatement moves
the reference price 9% and stays silent, while the factor series states the
ratio outright. So ask the factor instead — a step with no action on record is
an ex-event nobody wrote down, whatever its size.

The window is what makes that affordable. Asking the same question of all
history resurrects a thousand deep-history steps whose sources are long gone;
asking it of the last ten sessions surfaces exactly what a run can still go and
fetch, and lets anything unfixable fall out of scope on its own.
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.query.parquet_scan import (
    dataset_has_parquet,
    dedupe_lazy_by_primary_key,
    scan_parquet_root,
)

logger = logging.getLogger(__name__)

__all__ = ["EX_EVENT_LOOKBACK_SESSIONS", "unexplained_factor_steps"]

# Ten sessions is two trading weeks: long enough that a run interrupted over a
# weekend still gets its chance, short enough that a gap no source can fill
# stops being re-asked.
EX_EVENT_LOOKBACK_SESSIONS = 10

# The factor is stored as a float; equal values can differ in the last bits.
_STEP_EPSILON = 1e-6

_EMPTY = pl.DataFrame(schema={"symbol": pl.Utf8, "ex_date": pl.Date, "factor_ratio": pl.Float64})


def _recent_sessions(config: Config, upto: date, count: int) -> list[date]:
    """The last *count* open sessions up to and including *upto*."""
    root = config.curated_root / "trading_calendar"
    if not dataset_has_parquet(root):
        return []
    frame = scan_parquet_root(root, partition_col="trade_date", end=upto).collect()
    if frame.is_empty() or "is_trading" not in frame.columns:
        return []
    sessions = (
        frame.filter(pl.col("is_trading") & (pl.col("trade_date") <= upto))
        .get_column("trade_date")
        .unique()
        .sort()
        .to_list()
    )
    return sessions[-count:]


def unexplained_factor_steps(
    config: Config,
    *,
    upto: date,
    sessions: int = EX_EVENT_LOOKBACK_SESSIONS,
) -> pl.DataFrame:
    """(symbol, ex_date, factor_ratio) where the hfq factor stepped unrecorded.

    One extra session is loaded before the window so a step on its first day is
    measurable; the anchor day itself is never reported.
    """
    factors_root = config.derived_root / "adj_factors"
    if not dataset_has_parquet(factors_root):
        return _EMPTY.clone()
    window = _recent_sessions(config, upto, sessions + 1)
    if len(window) < 2:
        # No calendar, or a lake too young to have a step to compare against.
        return _EMPTY.clone()
    anchor, first = window[0], window[1]
    factors = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(factors_root, partition_col="trade_date", start=anchor, end=upto),
            "adj_factors",
        )
        .filter(pl.col("adjust_type") == "hfq")
        .select("symbol", "trade_date", "factor")
        .collect()
        .sort(["symbol", "trade_date"])
    )
    if factors.is_empty():
        return _EMPTY.clone()
    steps = (
        factors.with_columns(
            (pl.col("factor") / pl.col("factor").shift(1).over("symbol")).alias("factor_ratio")
        )
        .filter(
            pl.col("factor_ratio").is_not_null()
            & ((pl.col("factor_ratio") - 1.0).abs() > _STEP_EPSILON)
            & (pl.col("trade_date") >= first)
        )
        .select("symbol", pl.col("trade_date").alias("ex_date"), "factor_ratio")
    )
    if steps.is_empty():
        return _EMPTY.clone()
    actions_root = config.curated_root / "corporate_actions"
    if not dataset_has_parquet(actions_root):
        return steps.sort(["ex_date", "symbol"])
    recorded = (
        scan_parquet_root(actions_root, partition_col="ex_date", start=first, end=upto)
        .select("symbol", "ex_date")
        .unique()
        .collect()
    )
    return (
        steps.join(recorded, on=["symbol", "ex_date"], how="anti").sort(["ex_date", "symbol"])
        if not recorded.is_empty()
        else steps.sort(["ex_date", "symbol"])
    )
