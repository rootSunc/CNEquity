"""Reconstruct historical suspension status from daily_bars trading gaps.

For feeds that omit suspended rows, a listed symbol with no bar on a trading
day was suspended that day. Some lake sources retain an OHLC placeholder on a
suspended day, however; those rows carry ``volume=0``. A listed symbol with no
*traded* bar is therefore the actual evidence used here. It covers the whole
bar history — filling the trading_status gap that free ST feeds (EastMoney's
current-snapshot ST board) cannot reach.

Only sparse ``suspended`` rows are staged. Which row survives a primary-key
collision is decided by ``domain/trading_status``'s evidence ranking, not by
this module: an exchange record or a same-session board read still corrects a
derived row, while a current-state snapshot restated onto an older session no
longer erases one.

Optional ``start`` / ``end`` bound the calendar cross-join so a 2001→today
rebuild can run year-by-year without OOM.

Only *interior* gaps are reported. Each symbol's window is clamped to its own
first and last bar, so a session the bar feed has not delivered yet — today,
mid-run, or after a failed ingest — cannot be mistaken for a market-wide halt.
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.schemas import with_provenance
from cnequity.domain.trading_status import DERIVED_BAR_GAP_SOURCE, STATUS_SUSPENDED
from cnequity.query.canonical import dedupe_by_primary_key, dedupe_lazy_by_primary_key
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.parquet import StagingWriter

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
    run_id: str,
    *,
    start: date | None = None,
    end: date | None = None,
    batch_id: str = "derive-0",
) -> int:
    """Stage derived ``suspended`` rows for *run_id*. Returns the row count.

    The rows go through the ordinary ``staging -> compact -> commit`` channel
    rather than into the mutable curated directory. Writing curated directly
    published nothing: committed readers never saw the rows, and the next
    compact rebuilt the partition from the committed generation and dropped
    them again — so a `daily_bars` interior gap could never be excused by the
    suspension that explains it, however many times the derive was re-run.

    When *start* / *end* are set, only that calendar window is considered — use
    yearly chunks for a full-history rebuild to keep the cross-join bounded.
    """
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
    # Merging against what is already committed is compact's job, and it uses
    # the shared evidence ranking so a restated EastMoney snapshot cannot
    # overwrite these rows while a genuine authority still can.
    StagingWriter(config.staging_root).write_batch("trading_status", run_id, batch_id, rows)
    logger.info(
        "staged %d derived suspension row(s) for trading_status run %s", rows.height, run_id
    )
    return rows.height
