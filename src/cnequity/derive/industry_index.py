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

from cnequity.domain.market_time import shanghai_today
from cnequity.query.canonical import dedupe_by_primary_key, dedupe_lazy_by_primary_key

if TYPE_CHECKING:
    from cnequity.config import Config

logger = logging.getLogger(__name__)

# Below one yuan a "turnover" is a feed artefact, not a trade.
_MIN_TRADED_AMOUNT = 1.0
# How far back to search for a prior trading day used only as a pct_change baseline.
_LOOKBACK_CALENDAR_DAYS = 21

LEVELS = {"L1": 2, "L2": 4, "L3": 6}
WEIGHTINGS = ("equal", "amount")


def _prior_trading_day(config: Config, day: date) -> date | None:
    """Latest trading day strictly before *day*, or None if the calendar is empty."""
    from cnequity.steps.common import list_trading_dates

    window_start = day - timedelta(days=_LOOKBACK_CALENDAR_DAYS)
    prior = list_trading_dates(config, window_start, day - timedelta(days=1))
    return prior[-1] if prior else None


def _membership(config: Config) -> pl.DataFrame:
    """申万 snapshots only — `industry_members` also carries EastMoney board rows
    under 3/4-digit codes, which are a different taxonomy entirely."""
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

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
        dedupe_lazy_by_primary_key(
            scan_parquet_root(root, partition_col="as_of_date", hive=False),
            "industry_members",
        )
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
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.derived_root / "adj_factors"
    if not dataset_has_parquet(root):
        return set()
    return set(
        scan_parquet_root(root, partition_col="trade_date", hive=False)
        .select("symbol")
        .unique()
        .collect()["symbol"]
        .to_list()
    )


def _hfq_returns(config: Config, start: date, end: date, symbols: list[str]) -> pl.DataFrame:
    """Daily hfq returns and turnover per symbol.

    Filtering the universe by symbol was not enough. ``_priced_universe`` asks
    "does this name have a factor at all", while ``strict_adj=True`` asks for
    one on every single row — so a name that is priced for years but missing the
    newest session slipped through the first check and aborted the whole derive
    on the second. That is not hypothetical: an append-only factor derive can
    temporarily leave BJ names without a factor for exactly the day being
    derived, even though Sina serves the exchange. It failed the daily `core`
    group every run, which is the opposite of what putting this step on the
    daily path was for.

    So the gap is handled where it actually lives — per row. ``adj_is_exact``
    marks the rows the reader could not adjust; they are dropped rather than
    kept at factor=1.0, because a raw price inside an hfq return series is the
    silent corruption ``strict_adj`` exists to prevent. Dropping them costs
    those names one session in that day's cross-section and says so in the log.
    """
    from cnequity.query.reader import load

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
    # A suspended security can still have a carried-forward close in
    # daily_bars, but the row is explicitly marked volume=0. Without this
    # guard its zero return enters the equal-weight industry mean and inflates
    # n_priced, while only the amount-weighted branch notices the missing
    # turnover later. Keep minimal historical/test frames without volume
    # compatible; curated daily_bars always carries the column.
    if "volume" in bars.columns:
        bars = bars.filter((pl.col("volume") > 0) | pl.col("volume").is_null())
        if bars.is_empty():
            return bars
    out = (
        bars.select("symbol", "trade_date", "close", "amount")
        .sort("symbol", "trade_date")
        .with_columns(
            pl.col("trade_date").shift(1).over("symbol").alias("_prev_trade_date"),
            pl.col("close").pct_change().over("symbol").alias("ret"),
        )
    )

    # pct_change() otherwise bridges a missing session: a symbol with bars on
    # Monday and Wednesday gets a two-session return labelled as Wednesday's
    # one-day return. That is especially damaging to an industry index because
    # the bridged move can look like a genuine cross-sectional signal. The
    # calendar is the authority here (not a one-day timedelta, since weekends
    # and Chinese holidays are valid gaps); drop the first row after a gap so
    # the missing session remains visible to the dense-coverage watermark.
    if hasattr(config, "curated_root") and not out.is_empty():
        from cnequity.steps.common import list_trading_dates

        days = sorted(out["trade_date"].unique().to_list())
        sessions = list_trading_dates(config, days[0], days[-1])
        if len(sessions) > 1:
            expected = pl.DataFrame(
                {
                    "trade_date": sessions[1:],
                    "_expected_prev_trade_date": sessions[:-1],
                }
            )
            out = (
                out.join(expected, on="trade_date", how="left")
                .filter(pl.col("_prev_trade_date") == pl.col("_expected_prev_trade_date"))
                .drop("_prev_trade_date", "_expected_prev_trade_date")
            )
        else:
            out = out.head(0).drop("_prev_trade_date")
    else:
        out = out.drop("_prev_trade_date")

    # A zero/invalid previous close can make pct_change() produce NaN rather
    # than null.  Letting it through poisons both weighting branches and only
    # surfaces much later in the derived-data audit; a non-finite return is no
    # more usable than a missing one, so exclude it at the row boundary.
    out = out.drop_nulls("ret").filter(pl.col("ret").is_finite())

    # Turnover is either real or absent — a suspended name reports 0, and a
    # broken feed can report values like 5.9e-39 that are positive but not
    # money (2026-07-22 arrived that way for the whole universe). Anything
    # below a yuan is not a traded amount, and letting it through would make
    # the amount-weighted index a weighted average of noise.
    return out.with_columns(
        pl.when(pl.col("amount") >= _MIN_TRADED_AMOUNT)
        .then(pl.col("amount"))
        .otherwise(None)
        .alias("amount")
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
        priced_agg = (
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
        )
        # Start from every membership group, not only groups with a priced
        # return. Otherwise an industry whose entire basket lacks an exact
        # adjustment factor disappears from the output and looks complete to
        # consumers that only see the remaining groups.
        agg = (
            known.join(priced_agg, on=["trade_date", "industry_code"], how="left")
            .with_columns(
                pl.col("n_members").cast(pl.UInt32),
                pl.col("n_priced").fill_null(0).cast(pl.UInt32),
                pl.col("amount").fill_null(0.0),
            )
            .with_columns(
                (pl.col("n_members") - pl.col("n_priced")).cast(pl.UInt32).alias("n_excluded"),
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
    from cnequity.file_lock import lake_mutation_lock

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
    from cnequity.domain.datasets import DATASETS
    from cnequity.domain.schemas import with_provenance
    from cnequity.quality.verify import last_contiguous_dense_date
    from cnequity.storage.parquet import write_partition_rows
    from cnequity.storage.state import StateStore

    state = StateStore(config.meta_root)
    if start is None:
        watermark = None if full else state.get_date("industry_index")
        start = (
            date(2020, 1, 1) if watermark is None else date.fromordinal(watermark.toordinal() + 1)
        )
    end = end or shanghai_today()
    if start > end:
        return {"rows": 0, "note": f"industry_index already current through {end}"}

    frame = compute_industry_index(config, start, end)
    if frame.is_empty():
        # Keep the empty-result reason actionable for the orchestrator.  A
        # daily-bars-only run legitimately has no 申万 input yet, whereas a
        # configured membership universe that yields no priced returns is a
        # retryable quality problem.
        note = (
            "no 申万 membership rows"
            if _membership(config).is_empty()
            else f"no rows in [{start}, {end}]"
        )
        return {"rows": 0, "note": note}
    frame = with_provenance(frame, source="derived", data_version="v1")
    frame = dedupe_by_primary_key(frame, "industry_index")

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
        stored = sorted(out_dir.glob("*.parquet"))
        if stored:
            # Keep one type per column across the partition's files: a rerun
            # computes Int64 counts where older files hold UInt32.
            schema = pl.read_parquet_schema(stored[0])
            casts = [
                pl.col(name).cast(dtype)
                for name, dtype in schema.items()
                if name in group.columns and group.schema[name] != dtype
            ]
            if casts:
                group = group.with_columns(casts)
        # A recomputed date replaces that whole date; a new date lands in a
        # file of its own instead of rewriting the year (~289k rows for ~1.2k).
        write_partition_rows(
            group,
            "industry_index",
            read_dir=out_dir,
            out_dir=out_dir,
            load=pl.read_parquet,
            dedupe=lambda frame: dedupe_by_primary_key(frame, "industry_index"),
            finish=lambda frame: frame.sort("trade_date", "level", "weighting", "industry_code"),
            replace_by=["trade_date"],
            incoming_first=False,
            merged_name="part-000.parquet",
        )
        written += group.height
    # This derive writes directly to the derived tree instead of going through
    # the generic compact step. Do not advance past an interior session gap:
    # the next incremental run starts at watermark + 1, so a raw max would make
    # a missing day permanently invisible.
    safe_watermark = last_contiguous_dense_date(config, DATASETS["industry_index"])
    if safe_watermark is not None:
        state.set_date("industry_index", safe_watermark)
    return {
        "rows": frame.height,
        "rows_on_disk": written,
        "industries": frame["industry_code"].n_unique(),
        "first": str(frame["trade_date"].min()),
        "last": str(frame["trade_date"].max()),
    }
