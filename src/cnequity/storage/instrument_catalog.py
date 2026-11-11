"""Read the committed security master without ingestion or network fallbacks."""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.storage.read_context import ReadContext, read_root


def load_curated_instruments(
    config: Config, read_context: ReadContext | None = None
) -> pl.DataFrame | None:
    """Read the merge-style instrument catalog across all surviving shards."""
    root = read_root(config, "instruments", read_context)
    if not root.exists() or not any(root.rglob("*.parquet")):
        return None
    from cnequity.domain.canonical import dedupe_by_primary_key
    from cnequity.query.parquet_scan import collect_parquet_root

    try:
        frame = collect_parquet_root(root, hive=False, committed=False)
    except FileNotFoundError:
        return None
    return dedupe_by_primary_key(frame, "instruments")


logger = logging.getLogger(__name__)


def listed_fund_symbols(
    instruments: pl.DataFrame, on: date, *, start: date | None = None
) -> list[str]:
    """Fund quote scope from the security master, independent of ETF eligibility.

    Unknown listing dates are still requested and reported as unknown by the
    coverage checker. A classification directory failure must not erase their
    quote history. Known prelisting and delisted names stay out of a live sweep.
    """
    if not {"symbol", "asset_type"} <= set(instruments.columns):
        raise ValueError("fund ingest requires instruments with symbol and asset_type")
    frame = instruments
    for column in ("list_date", "delist_date"):
        if column not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Date).alias(column))
    from cnequity.domain.symbols import is_subscription_placeholder

    frame = frame.filter(
        pl.col("asset_type").is_in(["etf", "lof"])
        & pl.col("symbol").str.contains(r"\.(SH|SZ)$")
        & (pl.col("list_date").is_null() | (pl.col("list_date") <= on))
        & (pl.col("delist_date").is_null() | (pl.col("delist_date") > (start or on)))
    )
    return sorted(
        row["symbol"]
        for row in frame.unique("symbol").iter_rows(named=True)
        if not is_subscription_placeholder(row.get("name"), row["symbol"])
    )


def load_curated_trading_status(
    config: Config,
    *,
    start: date | None = None,
    end: date | None = None,
    symbols: list[str] | None = None,
    include_inferred: bool = False,
    include_announced: bool = True,
    read_context: ReadContext | None = None,
) -> pl.DataFrame | None:
    """Load the available status evidence without making a network request.

    With a ``start``/``end`` window, halts the issuer announced (see
    :mod:`cnequity.domain.announced_halts`) are added as ``source="announcement"``
    rows on keys no first-hand row speaks for; ``include_announced=False``
    leaves them out.

    ``derived_bar_gap`` rows are left out unless ``include_inferred``: they
    are inferred from the very bars this evidence is used to judge, so a
    session that lost its Beijing bars in a failed run came back as 223
    "suspensions" that excused the gap, kept it out of the next fetch and out
    of the coverage gate (2026-08-07).

    ``trading_status`` is an advisory but independently fetched daily
    snapshot.  Daily-bar routing may use it to prove that a missing symbol was
    suspended; it must never synthesize a status by treating an absent row as
    ``normal``.  A corrupt or absent status root therefore returns ``None``
    and leaves the symbol in the strict unknown bucket.
    """
    root = read_root(config, "trading_status", read_context)
    if not root.exists() or not any(root.rglob("*.parquet")):
        return None
    try:
        from cnequity.domain.canonical import dedupe_by_primary_key
        from cnequity.query.parquet_scan import collect_parquet_root

        frame = collect_parquet_root(
            root,
            partition_col="trade_date",
            start=start,
            end=end,
            symbols=symbols,
            committed=False,
        )
    except (FileNotFoundError, OSError, pl.exceptions.PolarsError, ValueError) as exc:
        logger.warning("curated trading_status scan failed; status evidence unavailable: %s", exc)
        return None
    required = {"symbol", "trade_date", "is_trading"}
    if not required.issubset(frame.columns):
        logger.warning(
            "curated trading_status lacks required evidence columns: %s",
            sorted(required - set(frame.columns)),
        )
        return None
    if not include_inferred and "source" in frame.columns:
        from cnequity.domain.trading_status import DERIVED_BAR_GAP_SOURCE

        # Before the canonical pick, so an independent row on the same key
        # surfaces instead of the inference that outranked it.
        frame = frame.filter(
            pl.col("source").is_null() | (pl.col("source") != DERIVED_BAR_GAP_SOURCE)
        )
    frame = dedupe_by_primary_key(frame, "trading_status")
    if include_announced and start is not None and end is not None:
        frame = _with_announced_halts(config, frame, start, end, symbols, read_context)
    return frame


def _with_announced_halts(
    config: Config,
    status: pl.DataFrame,
    start: date,
    end: date,
    symbols: list[str] | None,
    read_context: ReadContext | None,
) -> pl.DataFrame:
    """Add halts the issuer announced where no first-hand row speaks for the key.

    A first-hand row on the key wins either way: a halt already proves itself,
    and a trading observation vetoes the announcement. Carried and derived rows
    on the key are replaced, since they cannot prove the halt themselves.
    """
    from datetime import timedelta

    from cnequity.domain.announced_halts import (
        ANNOUNCEMENT_SOURCE,
        LOOKBACK_DAYS,
        announced_halt_keys,
    )
    from cnequity.domain.trading_status import trusted_evidence_expr
    from cnequity.query.calendar import list_trading_dates
    from cnequity.query.parquet_scan import collect_parquet_root

    if read_context is not None and "announcement_index" not in read_context.roots:
        # The caller pinned a read without notices; mixing in a newer one
        # would break the consistency the pin exists for.
        return status
    root = read_root(config, "announcement_index", read_context)
    if not root.exists() or not any(root.rglob("*.parquet")):
        return status
    try:
        notices = collect_parquet_root(
            root,
            partition_col="announce_date",
            start=start - timedelta(days=LOOKBACK_DAYS),
            end=end + timedelta(days=LOOKBACK_DAYS),
            symbols=symbols,
            committed=False,
        )
        sessions = list_trading_dates(
            config, start - timedelta(days=LOOKBACK_DAYS), end + timedelta(days=LOOKBACK_DAYS)
        )
    except (FileNotFoundError, OSError, pl.exceptions.PolarsError, ValueError) as exc:
        logger.warning("announcement_index scan failed; announced halts unavailable: %s", exc)
        return status
    if not {"symbol", "announce_date", "title"}.issubset(notices.columns):
        return status
    keys = {(s, d) for s, d in announced_halt_keys(notices, sessions) if start <= d <= end}
    if not keys:
        return status
    key_schema = {"symbol": pl.Utf8, "trade_date": pl.Date}
    announced = pl.DataFrame(sorted(keys), schema=key_schema, orient="row")
    if {"source", "is_trading"}.issubset(status.columns):
        first_hand = status.filter(
            pl.col("is_trading").is_not_null() & trusted_evidence_expr(status.schema)
        ).select(*key_schema)
        announced = announced.join(first_hand, on=list(key_schema), how="anti")
    if announced.is_empty():
        return status
    rows = announced.with_columns(
        pl.lit(False).alias("is_trading"),
        pl.lit("suspended").alias("status"),
        pl.lit(ANNOUNCEMENT_SOURCE).alias("source"),
    )
    rows = rows.select([c for c in rows.columns if c in status.columns or c in key_schema])
    kept = status.join(announced, on=list(key_schema), how="anti")
    return pl.concat([kept, rows], how="diagonal_relaxed")


def load_bar_universe(config: Config) -> set[str]:
    """Symbols that carry at least one traded ``daily_bars`` row in the lake.

    A suspended/pre-open placeholder can still be present in ``daily_bars`` with
    ``volume=0``. It is not evidence that the symbol ever traded, so when the
    column exists require positive volume. This is the *tradable* universe as
    daily_bars actually realises it: delisted names (source returns no bars) and
    never-traded instrument placeholders (IPO listed but not yet trading) are
    absent. Live snapshots such as the EastMoney valuation clist return those
    dead names, so filtering to this set keeps valuation_metrics in lock-step
    with daily_bars coverage (audit check ``valuation_bars_orphan_symbol``). A
    genuine IPO enters this set the same day it first trades and gets a bar.

    Returns an empty set when no bars exist yet; callers must treat that as
    "cannot reconcile" and skip filtering rather than dropping every row.
    """
    bars_root = config.curated_root / "daily_bars"
    # Canonicalize the daily-bar identity before applying the volume contract.
    # Filtering each file first lets an old positive retry keep a symbol in the
    # valuation universe beside a newer zero-volume canonical row. The shared
    # scanner preserves row-based semantics for legacy files without volume.
    from cnequity.query.parquet_scan import scan_parquet_root

    try:
        scan = scan_parquet_root(
            bars_root,
            partition_col="trade_date",
            traded_only=True,
            dataset="daily_bars",
            meta_root=config.meta_root,
        )
    except FileNotFoundError:
        return set()
    if "symbol" not in scan.collect_schema().names():
        return set()
    return set(scan.select("symbol").unique().collect()["symbol"].to_list())
