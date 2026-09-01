"""Reconstruct historical suspension status from daily_bars trading gaps.

For feeds that omit suspended rows, a listed symbol with no bar on a trading
day was suspended that day. Some lake sources retain an OHLC placeholder on a
suspended day, however; those rows carry ``volume=0``. A listed symbol with no
*traded* bar is therefore the actual evidence used here. This is authoritative
and covers the whole bar history — filling the trading_status gap that free ST
feeds (EastMoney's current-snapshot ST board) cannot reach.

The derived rows are only provisional suspension evidence
(``source=derived_bar_gap``, rank 1). They are staged into the
``trading_status`` dataset and published by compact (`staging → compact →
commit`) so committed readers see them and a later authority (rank 0) can
correct them.

Optional ``start`` / ``end`` bound the calendar cross-join so a 2001→today
rebuild can run year-by-year without OOM.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

import polars as pl

from cnequity.config import Config
from cnequity.domain.schemas import validate_dataframe, with_provenance
from cnequity.domain.trading_status import (
    DERIVED_BAR_GAP_SOURCE,
    STATUS_SUSPENDED,
)
from cnequity.query.canonical import dedupe_by_primary_key, dedupe_lazy_by_primary_key
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.steps.common import reject_unfinished_eod_window
from cnequity.storage import StagingWriter

logger = logging.getLogger(__name__)


def _suspended_pairs(
    config: Config,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    """(symbol, trade_date) that were trading days in a symbol's active range but have no bar."""
    bars_root = config.curated_root / "daily_bars"
    cal_root = config.curated_root / "trading_calendar"
    inst_root = config.curated_root / "instruments"
    if not (
        dataset_has_parquet(bars_root)
        and dataset_has_parquet(cal_root)
        and dataset_has_parquet(inst_root)
    ):
        return pl.DataFrame(schema={"symbol": pl.Utf8, "trade_date": pl.Date})

    # Lifetime bounds may include zero-volume suspension placeholders, but a
    # symbol represented only by placeholders must not get a synthetic active
    # range and be reported as suspended on every calendar day. The scanner
    # canonicalizes the daily-bar identity before filtering; legacy files
    # without volume retain row-based semantics when they coexist with current
    # files.
    bars_all_lf = scan_parquet_root(bars_root, partition_col="trade_date")
    traded_bars_lf = scan_parquet_root(bars_root, partition_col="trade_date", traded_only=True)
    traded_symbols = traded_bars_lf.select("symbol").unique()
    bars_lf = bars_all_lf.join(traded_symbols, on="symbol", how="semi")
    sym_range = (
        bars_lf.group_by("symbol")
        .agg(
            pl.col("trade_date").min().alias("bmin"),
            pl.col("trade_date").max().alias("bmax"),
        )
        .collect()
    )
    if sym_range.is_empty():
        return pl.DataFrame(schema={"symbol": pl.Utf8, "trade_date": pl.Date})

    # Anti-join only needs bars inside the derive window.
    # Suspended days may survive as an OHLC placeholder with volume=0. The
    # traded-only scan already removed those rows while retaining legacy rows
    # from files without volume.
    bars_lf = traded_bars_lf.select(["symbol", "trade_date"])
    if start is not None:
        bars_lf = bars_lf.filter(pl.col("trade_date") >= start)
    if end is not None:
        bars_lf = bars_lf.filter(pl.col("trade_date") <= end)
    bars = bars_lf.unique().collect()

    cal_lf = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(cal_root, partition_col="trade_date"),
            "trading_calendar",
        )
        .filter(pl.col("is_trading"))
        .select("trade_date")
    )
    if start is not None:
        cal_lf = cal_lf.filter(pl.col("trade_date") >= start)
    if end is not None:
        cal_lf = cal_lf.filter(pl.col("trade_date") <= end)
    cal = cal_lf.collect()
    if cal.is_empty():
        return pl.DataFrame(schema={"symbol": pl.Utf8, "trade_date": pl.Date})

    inst = dedupe_by_primary_key(
        scan_parquet_root(inst_root, hive=False)
        .select(["symbol", "list_date", "delist_date"])
        .collect(),
        "instruments",
    )

    active = inst.join(sym_range, on="symbol", how="inner").with_columns(
        pl.max_horizontal(pl.col("list_date").fill_null(pl.col("bmin")), pl.col("bmin")).alias(
            "astart"
        ),
        pl.min_horizontal(pl.col("delist_date").fill_null(pl.col("bmax")), pl.col("bmax")).alias(
            "aend"
        ),
    )
    if start is not None:
        active = active.with_columns(
            pl.max_horizontal(pl.col("astart"), pl.lit(start)).alias("astart")
        )
    if end is not None:
        active = active.with_columns(pl.min_horizontal(pl.col("aend"), pl.lit(end)).alias("aend"))
    active = active.filter(pl.col("astart") <= pl.col("aend"))
    if active.is_empty():
        return pl.DataFrame(schema={"symbol": pl.Utf8, "trade_date": pl.Date})

    expected = (
        active.select(["symbol", "astart", "aend"])
        .join(cal, how="cross")
        .filter(
            (pl.col("trade_date") >= pl.col("astart")) & (pl.col("trade_date") <= pl.col("aend"))
        )
        .select(["symbol", "trade_date"])
    )
    return expected.join(bars, on=["symbol", "trade_date"], how="anti")


def derive_suspension_history(
    config: Config,
    *,
    start: date | None = None,
    end: date | None = None,
    run_id: str = "derive",
    batch_id: str | None = None,
    now: datetime | None = None,
) -> int:
    """Stage derived ``suspended`` rows into trading_status. Returns row count.

    Rows are written through :class:`StagingWriter` into the ``trading_status``
    dataset under *run_id*; compact (in the same run) merges them by evidence
    rank and publishes a committed revision. Compact is responsible for
    partitioning by the month, deduplicating against existing authored rows,
    and keeping the derived row only when no authority contradicts it.

    When *start* / *end* are set, only that calendar window is considered —
    use yearly chunks for a full-history rebuild to keep the cross-join
    bounded. Callers that schedule the derive against the current session
    (``end == today``) must run it only once that session's bar is finalized;
    the same unfinished-session guard as daily_bars applies here, and a daily
    run should pass ``end`` strictly before the session whose bar is only
    staged (not yet committed) in the same run. ``now`` is injectable for a
    deterministic timezone boundary in tests.
    """
    if end is not None:
        reject_unfinished_eod_window(config, end, what="trading_status", now=now)
    pairs = _suspended_pairs(config, start=start, end=end)
    if pairs.is_empty():
        return 0

    rows = pairs.with_columns(
        pl.lit(False).alias("is_trading"),
        pl.lit(STATUS_SUSPENDED).alias("status"),
        # A missing bar proves the security did not trade. It proves nothing
        # about risk warning, so this stays null rather than asserting "clean";
        # `st_coverage` is what tracks where ST evidence is actually absent.
        pl.lit(None, dtype=pl.Boolean).alias("risk_warning"),
    )
    rows = with_provenance(rows, source=DERIVED_BAR_GAP_SOURCE, data_version="v1")
    rows = validate_dataframe(rows, "trading_status")
    StagingWriter(config.staging_root).write_batch(
        "trading_status",
        run_id,
        batch_id or "derived-suspension",
        rows,
    )
    logger.info(
        "derived %d historical suspension rows into trading_status staging for run %s",
        rows.height,
        run_id,
    )
    return rows.height
