"""Industry return indices computed from 申万 membership and hfq stock bars.

Why compute rather than fetch: a fetched board index and a separately fetched
membership list describe slightly different baskets, and the mismatch has to be
managed forever by a hand-maintained mapping that drifts as classifications
change. Computing the index from the membership we hold makes the two consistent
*by construction* — the seam disappears instead of being approximated.

This is also what makes the series backtestable. 申万 membership is stored as
monthly snapshots back to 2020-01, so each day's index uses the membership known
on that day, and a stock reclassified last month does not retroactively change
what its old industry did.

The 6-digit 申万 code is prefix-hierarchical (``240301`` 铝 -> ``2403`` 工业金属
-> ``24`` 有色金属), so all three levels come from the one membership series.

Two weightings are stored rather than one. Free-float market cap is the 申万
convention but ``valuation_metrics.float_mv`` is only ~69% populated across the
whole history, and weighting by a column that is null for a third of the universe
silently drops those names. ``equal`` and ``amount`` both come from ``daily_bars``
alone, so they cover everything; which one carries more signal is a question for
walk-forward, not for this module.

Rows carry ``n_members``/``n_priced``/``n_excluded`` because the index cannot
cover names without an adjustment factor — the 北交所 92 segment, ~5.6% of 申万
members overall but up to 43% of a few small industries. Excluding them quietly
would bias exactly those industries with no way to notice.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from cn_market_lake.config import Config

logger = logging.getLogger(__name__)

# Below one yuan a "turnover" is a feed artefact, not a trade.
_MIN_TRADED_AMOUNT = 1.0
# How far back to search for a prior trading day used only as a pct_change baseline.
_LOOKBACK_CALENDAR_DAYS = 21

LEVELS = {"L1": 2, "L2": 4, "L3": 6}
WEIGHTINGS = ("equal", "amount")


def _prior_trading_day(config: Config, day: date) -> date | None:
    """Latest trading day strictly before *day*, or None if the calendar is empty."""
    from cn_market_lake.steps.common import list_trading_dates

    window_start = day - timedelta(days=_LOOKBACK_CALENDAR_DAYS)
    prior = list_trading_dates(config, window_start, day - timedelta(days=1))
    return prior[-1] if prior else None


def _membership(config: Config) -> pl.DataFrame:
    """申万 snapshots only — `industry_members` also carries EastMoney board rows
    under 3/4-digit codes, which are a different taxonomy entirely."""
    from cn_market_lake.query.parquet_scan import dataset_has_parquet, parquet_glob

    root = config.curated_root / "industry_members"
    if not dataset_has_parquet(root):
        return pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "industry_code": pl.Utf8,
                "as_of_date": pl.Date,
            }
        )
    df = (
        pl.scan_parquet(parquet_glob(root))
        .filter(pl.col("source") == "sw")
        .select("symbol", "industry_code", "as_of_date")
        .collect()
    )
    return df.with_columns(pl.col("industry_code").cast(pl.Utf8))


def _priced_universe(config: Config) -> set[str]:
    """Symbols with an adjustment factor *somewhere* — see `_hfq_returns`.

    Coarse on purpose: this only removes names that have no hfq series at all.
    It cannot speak for individual sessions, which is why the row-level gap is
    handled after the load rather than here.
    """
    from cn_market_lake.query.parquet_scan import dataset_has_parquet, parquet_glob

    root = config.derived_root / "adj_factors"
    if not dataset_has_parquet(root):
        return set()
    return set(
        pl.scan_parquet(parquet_glob(root)).select("symbol").unique().collect()["symbol"].to_list()
    )


def _hfq_returns(config: Config, start: date, end: date, symbols: list[str]) -> pl.DataFrame:
    """Daily hfq returns and turnover per symbol.

    Filtering the universe by symbol was not enough. ``_priced_universe`` asks
    "does this name have a factor at all", while ``strict_adj=True`` asks for
    one on every single row — so a name that is priced for years but missing the
    newest session slipped through the first check and aborted the whole derive
    on the second. That is not hypothetical: ``adj_factors`` comes from Sina,
    whose series does not carry 北交所 names on the run date, so 45 BJ symbols
    had bars and no factor for exactly the day being derived. It failed the
    daily `core` group every run, which is the opposite of what putting this
    step on the daily path was for.

    So the gap is handled where it actually lives — per row. ``adj_is_exact``
    marks the rows the reader could not adjust; they are dropped rather than
    kept at factor=1.0, because a raw price inside an hfq return series is the
    silent corruption ``strict_adj`` exists to prevent. Dropping them costs
    those names one session in that day's cross-section and says so in the log.
    """
    from cn_market_lake.query.reader import load

    bars = load(
        "daily_bars",
        start=start,
        end=end,
        adjust="hfq",
        symbols=symbols,
        strict_adj=False,
        config=config,
    )
    if bars.is_empty():
        return bars
    if "adj_is_exact" in bars.columns:
        unpriced = bars.filter(~pl.col("adj_is_exact"))
        if not unpriced.is_empty():
            logger.info(
                "industry_index: dropping %d bar row(s) across %d symbol(s) with no "
                "adj_factor in [%s, %s] (newest: %s)",
                unpriced.height,
                unpriced["symbol"].n_unique(),
                start,
                end,
                unpriced["trade_date"].max(),
            )
            bars = bars.filter(pl.col("adj_is_exact"))
        if bars.is_empty():
            return bars
    return (
        bars.select("symbol", "trade_date", "close", "amount")
        .sort("symbol", "trade_date")
        .with_columns(pl.col("close").pct_change().over("symbol").alias("ret"))
        .drop_nulls("ret")
        # Turnover is either real or absent — a suspended name reports 0, and a
        # broken feed can report values like 5.9e-39 that are positive but not
        # money (2026-07-22 arrived that way for the whole universe). Anything
        # below a yuan is not a traded amount, and letting it through would make
        # the amount-weighted index a weighted average of noise.
        .with_columns(
            pl.when(pl.col("amount") >= _MIN_TRADED_AMOUNT)
            .then(pl.col("amount"))
            .otherwise(None)
            .alias("amount")
        )
    )


def _members_as_of(members: pl.DataFrame, days: list[date]) -> pl.DataFrame:
    """Point-in-time membership: each day takes the latest snapshot at or before it.

    A backward as-of join rather than the newest snapshot, so an industry's past
    is computed from the constituents it actually had, not from today's.
    """
    day_df = pl.DataFrame({"trade_date": days}).sort("trade_date")
    snaps = members.select("as_of_date").unique().sort("as_of_date")
    mapping = day_df.join_asof(
        snaps, left_on="trade_date", right_on="as_of_date", strategy="backward"
    ).drop_nulls("as_of_date")
    return mapping.join(members, on="as_of_date", how="inner")


def compute_industry_index(
    config: Config,
    start: date,
    end: date,
    *,
    levels: tuple[str, ...] = ("L1", "L2", "L3"),
) -> pl.DataFrame:
    members = _membership(config)
    if members.is_empty():
        logger.warning("industry_index: no 申万 membership rows")
        return pl.DataFrame()

    priced = _priced_universe(config)
    symbols = sorted(set(members["symbol"].to_list()) & priced)
    logger.info(
        "industry_index: %d 申万 members, %d with an adjustment factor",
        members["symbol"].n_unique(),
        len(symbols),
    )
    # pct_change needs the prior close: load one trading day before *start*, then
    # drop lookback-only rows so the emitted range stays [start, end].
    load_start = _prior_trading_day(config, start) or start
    rets = _hfq_returns(config, load_start, end, symbols)
    if rets.is_empty():
        logger.warning("industry_index: no priced bars in [%s, %s]", start, end)
        return pl.DataFrame()
    rets = rets.filter(pl.col("trade_date") >= start)
    if rets.is_empty():
        logger.warning("industry_index: no priced returns in [%s, %s]", start, end)
        return pl.DataFrame()

    days = sorted(rets["trade_date"].unique().to_list())
    panel = _members_as_of(members, days)
    priced_panel = panel.join(rets, on=["symbol", "trade_date"], how="inner")

    out: list[pl.DataFrame] = []
    for level, width in ((lvl, LEVELS[lvl]) for lvl in levels):
        key = pl.col("industry_code").str.slice(0, width).alias("industry_code")
        # Members known that day vs members that actually priced: the gap is the
        # names with no adjustment factor, and it is the distortion measure.
        known = (
            panel.with_columns(key)
            .group_by("trade_date", "industry_code")
            .agg(pl.col("symbol").n_unique().alias("n_members"))
        )
        agg = (
            priced_panel.with_columns(key)
            .group_by("trade_date", "industry_code")
            .agg(
                pl.col("ret").mean().alias("ret_equal"),
                # Null rather than a number when nothing in the group actually
                # traded: a weighted average over no weights is not zero.
                pl.when(pl.col("amount").is_not_null().any())
                .then((pl.col("ret") * pl.col("amount")).sum() / pl.col("amount").sum())
                .otherwise(None)
                .alias("ret_amount"),
                pl.col("symbol").n_unique().alias("n_priced"),
                pl.col("amount").sum().alias("amount"),
            )
            .join(known, on=["trade_date", "industry_code"], how="left")
            .with_columns(
                (pl.col("n_members") - pl.col("n_priced")).alias("n_excluded"),
                pl.lit(level).alias("level"),
            )
        )
        for weighting in WEIGHTINGS:
            out.append(
                agg.select(
                    "trade_date",
                    "industry_code",
                    "level",
                    pl.lit(weighting).alias("weighting"),
                    pl.col(f"ret_{weighting}").alias("ret"),
                    "n_members",
                    "n_priced",
                    "n_excluded",
                    "amount",
                )
            )
    frame = pl.concat(out).sort("trade_date", "level", "weighting", "industry_code")
    logger.info(
        "industry_index: %d rows | %d industries | %s .. %s",
        frame.height,
        frame["industry_code"].n_unique(),
        frame["trade_date"].min(),
        frame["trade_date"].max(),
    )
    return frame


def derive_industry_index(
    config: Config,
    *,
    start: date | None = None,
    end: date | None = None,
    full: bool = False,
) -> dict:
    """Compute and write ``industry_index``, partitioned by year.

    Incremental by default: recomputes from the day after the watermark. A
    membership snapshot only ever describes days at or after it, so already
    written days do not change — ``full`` is for a weighting or definition
    change, not for routine catch-up.
    """
    from cn_market_lake.file_lock import lake_mutation_lock

    # This derive merges existing yearly partitions and must share compact's
    # mutation lock with other curated/derived writers.
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _derive_industry_index_locked(config, start=start, end=end, full=full)


def _derive_industry_index_locked(
    config: Config,
    *,
    start: date | None = None,
    end: date | None = None,
    full: bool = False,
) -> dict:
    """Implementation of :func:`derive_industry_index` under the mutation lock."""
    from cn_market_lake.domain.schemas import with_provenance
    from cn_market_lake.storage.atomic import write_parquet_atomic
    from cn_market_lake.storage.state import StateStore

    state = StateStore(config.meta_root)
    if start is None:
        watermark = None if full else state.get_date("industry_index")
        start = (
            date(2020, 1, 1) if watermark is None else date.fromordinal(watermark.toordinal() + 1)
        )
    end = end or date.today()
    if start > end:
        return {"rows": 0, "note": f"industry_index already current through {end}"}

    frame = compute_industry_index(config, start, end)
    if frame.is_empty():
        return {"rows": 0, "note": f"no rows in [{start}, {end}]"}
    frame = with_provenance(frame, source="derived", data_version="v1")

    root = config.derived_root / "industry_index"
    written = 0
    for (year,), group in (
        frame.with_columns(pl.col("trade_date").dt.year().alias("_y"))
        .partition_by("_y", as_dict=True)
        .items()
    ):
        group = group.drop("_y")
        out_dir = root / f"trade_date={year}"
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / "part-000.parquet"
        if path.exists():
            # Same-year rerun: keep whatever the recompute did not cover.
            existing = pl.read_parquet(path)
            keep = existing.filter(~pl.col("trade_date").is_in(group["trade_date"].unique()))
            group = pl.concat([keep, group.select(existing.columns)]).sort(
                "trade_date", "level", "weighting", "industry_code"
            )
        write_parquet_atomic(path, group, compression="zstd")
        written += group.height
    state.update_max_date("industry_index", frame["trade_date"].max())
    return {
        "rows": frame.height,
        "rows_on_disk": written,
        "industries": frame["industry_code"].n_unique(),
        "first": str(frame["trade_date"].min()),
        "last": str(frame["trade_date"].max()),
    }
