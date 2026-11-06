"""Field semantics for ``valuation_metrics`` market capitalisation.

Market cap in the lake comes from three different kinds of evidence: a vendor
number, a number derived from the vendor's turnover ratio, and an estimate
from a share count that may be months old. They answer different questions,
so every stored value names its basis instead of hiding behind ``source``.
"""

from __future__ import annotations

import polars as pl

# Reported by the vendor as market cap (EastMoney push2 f20/f21, datacenter
# TOTAL_MARKET_CAP / NOTLIMITED_MARKETCAP_A).
MV_VENDOR_REPORTED = "vendor_reported"
# close × volume / (turnover% / 100): closing price times the float the
# vendor's turnover ratio implies (baostock).
FLOAT_MV_TURN_IMPLIED = "close_x_turn_implied_shares"
# A legacy amount/(turn/100) value (VWAP basis) rescaled by the bar's
# close / (amount / volume). Same shares as above, different price inputs.
FLOAT_MV_TURN_IMPLIED_REPAIRED = "close_x_turn_implied_shares_repaired"
# close × total shares effective on that session per ``share_structure``.
TOTAL_MV_SHARE_STRUCTURE = "close_x_share_structure"
# close × the last year-end total share count (baostock query_profit_data);
# misses intra-year issuance, buybacks and splits.
TOTAL_MV_YEAR_END_ESTIMATE = "close_x_year_end_shares_estimate"

_BASES = frozenset(
    {
        MV_VENDOR_REPORTED,
        FLOAT_MV_TURN_IMPLIED,
        FLOAT_MV_TURN_IMPLIED_REPAIRED,
        TOTAL_MV_SHARE_STRUCTURE,
        TOTAL_MV_YEAR_END_ESTIMATE,
    }
)
# Legacy baostock float_mv, amount / (turn / 100): the float the turnover ratio
# implies, priced at the session VWAP. Kept, labelled, where no consistent bar
# allows converting it to the close.
FLOAT_MV_VWAP_TURN_IMPLIED = "vwap_x_turn_implied_shares"

MV_BASES = _BASES | {FLOAT_MV_VWAP_TURN_IMPLIED}

# A VWAP outside the bar's own range (with this relative slack) means the
# bar's volume or amount is not on the same basis as its prices.
_VWAP_RANGE_SLACK = 0.01


def reconstruct_total_mv(frame: pl.DataFrame, shares: pl.DataFrame) -> pl.DataFrame:
    """Replace year-end estimates with ``close × shares effective that session``.

    *frame* needs ``symbol``, ``trade_date``, ``close`` and the valuation
    columns; *shares* needs ``symbol``, ``change_date`` and ``total_shares``
    (``share_structure``). A row is rebuilt only when its total_mv is absent
    or a year-end estimate and a share count dated on or before the session
    exists. Only baostock rows (or unlabelled adapter output) qualify; a
    vendor row is never given a derived value.
    """
    if frame.is_empty() or "close" not in frame.columns:
        return frame
    points = (
        shares.select("symbol", pl.col("change_date").cast(pl.Date), "total_shares")
        .filter(pl.col("total_shares") > 0)
        .drop_nulls(["symbol", "change_date"])
        .unique(subset=["symbol", "change_date"], keep="last")
        .sort("symbol", "change_date")
    )
    if points.is_empty():
        return frame
    order = frame.with_row_index("__row")
    matched = (
        order.select("__row", "symbol", "trade_date")
        .sort("symbol", "trade_date")
        .join_asof(
            points,
            left_on="trade_date",
            right_on="change_date",
            by="symbol",
            strategy="backward",
            check_sortedness=False,
        )
        .select("__row", "change_date", "total_shares")
    )
    out = order.join(matched, on="__row", how="left").sort("__row")
    # A value already rebuilt from share_structure is recomputed as well: it
    # is a pure function of close and shares, and share history gets corrected.
    eligible = (
        (
            (pl.col("total_mv_basis").is_null() & pl.col("total_mv").is_null())
            | pl.col("total_mv_basis").is_in([TOTAL_MV_YEAR_END_ESTIMATE, TOTAL_MV_SHARE_STRUCTURE])
        )
        & (pl.col("close") > 0)
        & pl.col("total_shares").is_not_null()
    )
    if "source" in out.columns:
        eligible = eligible & (pl.col("source") == "baostock")
    return out.with_columns(
        pl.when(eligible)
        .then(pl.col("close") * pl.col("total_shares"))
        .otherwise(pl.col("total_mv"))
        .alias("total_mv"),
        pl.when(eligible)
        .then(pl.lit(TOTAL_MV_SHARE_STRUCTURE))
        .otherwise(pl.col("total_mv_basis"))
        .alias("total_mv_basis"),
        pl.when(eligible)
        .then(pl.col("change_date"))
        .otherwise(pl.col("shares_as_of"))
        .alias("shares_as_of"),
    ).drop("__row", "change_date", "total_shares")


def repair_legacy_float_mv(frame: pl.DataFrame) -> pl.DataFrame:
    """Convert unlabelled baostock float_mv from VWAP to closing-price basis.

    *frame* needs the bar's ``close``, ``low``, ``high``, ``volume`` (shares)
    and ``amount`` (CNY). The legacy value is ``VWAP × float``; multiplying by
    ``close / VWAP`` gives ``close × float``. Where the bar is missing or its
    VWAP falls outside its own low..high, the legacy value is kept and
    labelled as VWAP-based instead of guessed.
    """
    if frame.is_empty():
        return frame
    vwap = pl.col("amount") / pl.col("volume")
    consistent = (
        (pl.col("volume") > 0)
        & (pl.col("amount") > 0)
        & (pl.col("close") > 0)
        & (vwap >= pl.col("low") * (1 - _VWAP_RANGE_SLACK))
        & (vwap <= pl.col("high") * (1 + _VWAP_RANGE_SLACK))
    ).fill_null(False)
    # A value still labelled VWAP-based is still VWAP-valued: convert it as
    # soon as a consistent bar exists (e.g. after a turnover repair).
    legacy = (
        (pl.col("source") == "baostock")
        & (
            pl.col("float_mv_basis").is_null()
            | (pl.col("float_mv_basis") == FLOAT_MV_VWAP_TURN_IMPLIED)
        )
        & pl.col("float_mv").is_not_null()
    )
    return frame.with_columns(
        pl.when(legacy & consistent)
        .then(pl.col("float_mv") * pl.col("close") / vwap)
        .otherwise(pl.col("float_mv"))
        .alias("float_mv"),
        pl.when(legacy & consistent)
        .then(pl.lit(FLOAT_MV_TURN_IMPLIED_REPAIRED))
        .when(legacy)
        .then(pl.lit(FLOAT_MV_VWAP_TURN_IMPLIED))
        .otherwise(pl.col("float_mv_basis"))
        .alias("float_mv_basis"),
    )


def label_legacy_total_mv(frame: pl.DataFrame) -> pl.DataFrame:
    """Name the basis of unlabelled baostock total_mv: a year-end estimate."""
    if frame.is_empty():
        return frame
    legacy = (
        (pl.col("source") == "baostock")
        & pl.col("total_mv").is_not_null()
        & pl.col("total_mv_basis").is_null()
    )
    return frame.with_columns(
        pl.when(legacy)
        .then(pl.lit(TOTAL_MV_YEAR_END_ESTIMATE))
        .otherwise(pl.col("total_mv_basis"))
        .alias("total_mv_basis")
    )


def split_dynamic_pe(frame: pl.DataFrame) -> pl.DataFrame:
    """Move push2's dynamic P/E out of ``pe_ttm`` and label vendor market caps.

    Rows labelled ``eastmoney`` came from the push2 clist, whose ``f9`` is the
    dynamic P/E. Rows already carrying ``pe_dynamic`` are left alone, so the
    step is idempotent.
    """
    if frame.is_empty():
        return frame
    push2 = pl.col("source") == "eastmoney"
    vendor = pl.col("source").is_in(["eastmoney", "eastmoney_datacenter"])
    move = push2 & pl.col("pe_dynamic").is_null() & pl.col("pe_ttm").is_not_null()
    return frame.with_columns(
        pl.when(move).then(pl.col("pe_ttm")).otherwise(pl.col("pe_dynamic")).alias("pe_dynamic"),
        pl.when(move)
        .then(pl.lit(None, dtype=pl.Float64))
        .otherwise(pl.col("pe_ttm"))
        .alias("pe_ttm"),
        *[
            pl.when(
                vendor & pl.col(f"{kind}_mv").is_not_null() & pl.col(f"{kind}_mv_basis").is_null()
            )
            .then(pl.lit(MV_VENDOR_REPORTED))
            .otherwise(pl.col(f"{kind}_mv_basis"))
            .alias(f"{kind}_mv_basis")
            for kind in ("total", "float")
        ],
    )
