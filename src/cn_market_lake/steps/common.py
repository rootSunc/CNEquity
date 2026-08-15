"""Shared helpers for step implementations."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta

import polars as pl

from cn_market_lake.adapters.tdx_protocol.client import fetch_instruments
from cn_market_lake.config import Config
from cn_market_lake.domain.datasets import fetch_semantics
from cn_market_lake.domain.schemas import data_version_for, with_provenance
from cn_market_lake.storage import StagingWriter
from cn_market_lake.storage.state import StateStore

logger = logging.getLogger(__name__)

INCREMENTAL_LOOKBACK_DAYS = 5
BACKFILL_START = date(2016, 1, 1)


class SnapshotBackfillError(RuntimeError):
    """Raised when backfill is requested for a snapshot-only dataset."""


def write_simple(
    config: Config,
    run_id: str,
    dataset: str,
    df: pl.DataFrame,
    *,
    batch_id: str = "batch-0",
) -> dict:
    writer = StagingWriter(config.staging_root)
    writer.write_batch(dataset, run_id, batch_id, df)
    return {"rows_read": df.height, "rows_written": df.height}


def incremental_window(config: Config, dataset: str, trade_date: date) -> date:
    """Start date for incremental fetch: day after watermark, or lookback window."""
    state = StateStore(config.meta_root)
    watermark = state.get_date(dataset)
    if watermark is not None:
        return min(watermark + timedelta(days=1), trade_date)
    return trade_date - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)


def _load_trading_calendar_df(
    config: Config,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame | None:
    """Load trading_calendar, preferring a lazy hive scan with optional date prune."""
    curated = config.curated_root / "trading_calendar"
    if curated.exists() and any(curated.rglob("*.parquet")):
        try:
            from cn_market_lake.query.parquet_scan import collect_parquet_root

            return collect_parquet_root(curated, partition_col="trade_date", start=start, end=end)
        except FileNotFoundError:
            pass
        files = list(curated.glob("**/*.parquet"))
        if files:
            lf = pl.scan_parquet([str(f) for f in files])
            if start is not None:
                lf = lf.filter(pl.col("trade_date") >= start)
            if end is not None:
                lf = lf.filter(pl.col("trade_date") <= end)
            return lf.collect()
    staging = list(config.staging_root.glob("trading_calendar/**/*.parquet"))
    if staging:
        latest = max(staging, key=lambda p: p.stat().st_mtime)
        df = pl.read_parquet(latest)
        if start is not None:
            df = df.filter(pl.col("trade_date") >= start)
        if end is not None:
            df = df.filter(pl.col("trade_date") <= end)
        return df
    return None


def list_trading_dates(config: Config, start: date, end: date) -> list[date]:
    """Trading days in [start, end] from curated/staging calendar, else Mon–Fri."""
    if start > end:
        return []
    cal = _load_trading_calendar_df(config, start=start, end=end)
    if cal is not None and not cal.is_empty() and "trade_date" in cal.columns:
        out = cal.filter(pl.col("is_trading"))["trade_date"].sort().to_list()
        if out:
            return out
    dates: list[date] = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            dates.append(d)
        d += timedelta(days=1)
    return dates


def incremental_trade_dates(config: Config, dataset: str, trade_date: date) -> list[date]:
    """Trading days to fetch for a daily dataset: [watermark+1, trade_date]."""
    start = incremental_window(config, dataset, trade_date)
    return list_trading_dates(config, start, trade_date)


def is_trading_day(config: Config, trade_date: date) -> bool:
    """Return whether *trade_date* is a trading day per curated calendar or seed."""
    cal = _load_trading_calendar_df(config, start=trade_date, end=trade_date)
    if cal is not None and not cal.is_empty():
        row = cal.filter(pl.col("trade_date") == trade_date)
        if not row.is_empty():
            return bool(row["is_trading"][0])

    from cn_market_lake.adapters.calendar.exchange_calendar import (
        build_trading_calendar,
        ensure_seed_csv,
    )

    seed_path = config.meta_root / "seeds" / "trading_calendar.csv"
    effective_seed = seed_path if seed_path.exists() else ensure_seed_csv()
    day_cal = build_trading_calendar(
        trade_date,
        trade_date,
        seed_path=effective_seed,
        curated_root=config.curated_root if config.curated_root.exists() else None,
    )
    if not day_cal.is_empty():
        return bool(day_cal["is_trading"][0])
    return trade_date.weekday() < 5


def _coverage_gap_findings(dataset: str, gap_dates: list[date]) -> list[dict]:
    if not gap_dates:
        return []
    gap_text = ", ".join(d.isoformat() for d in gap_dates)
    return [
        {
            "dataset": dataset,
            "severity": "warning",
            "check": "coverage_gap",
            "message": (
                f"{dataset}: skipped {len(gap_dates)} trading day(s) ({gap_text}) — "
                "snapshot fetch semantics cannot backfill historical values"
            ),
            "gap_dates": [d.isoformat() for d in gap_dates],
        }
    ]


def fetch_incremental_daily(
    config: Config,
    dataset: str,
    trade_date: date,
    fetch_fn: Callable[[date], pl.DataFrame],
    *,
    allow_empty: bool = False,
) -> tuple[pl.DataFrame, list[dict]]:
    """Fetch one or more trading days from watermark+1 through *trade_date*.

    Returns ``(dataframe, audit_findings)``. Snapshot datasets only fetch
    *trade_date*; missed days are reported as ``coverage_gap`` findings.
    """
    semantics = fetch_semantics(dataset)
    if getattr(config, "_backfill", False):
        if semantics == "snapshot":
            raise SnapshotBackfillError(
                f"{dataset}: backfill not supported — fetch semantics are snapshot "
                "(live page stamped with trade_date; historical values unavailable)"
            )
        return fetch_fn(trade_date), []

    dates = incremental_trade_dates(config, dataset, trade_date)
    if not dates:
        return pl.DataFrame(), []

    if semantics == "snapshot":
        gap_dates = [d for d in dates if d < trade_date]
        fetch_dates = [trade_date]
        findings = _coverage_gap_findings(dataset, gap_dates)
    else:
        fetch_dates = dates
        findings = []

    frames: list[pl.DataFrame] = []
    for d in fetch_dates:
        part = fetch_fn(d)
        if part.is_empty():
            if not allow_empty:
                raise RuntimeError(f"{dataset}: no rows returned for {d.isoformat()}")
            continue
        frames.append(part)
    if not frames:
        return pl.DataFrame(), findings
    return pl.concat(frames, how="diagonal_relaxed"), findings


def load_symbols(config: Config) -> list[str]:
    """Universe symbols: curated instruments first, then staging, then source."""
    curated = config.curated_root / "instruments" / "part-merged.parquet"
    staging_glob = list(config.staging_root.glob("instruments/run_id=*/part-*.parquet"))
    if curated.exists():
        return pl.read_parquet(curated)["symbol"].to_list()
    if staging_glob:
        latest = max(staging_glob, key=lambda p: p.stat().st_mtime)
        return pl.read_parquet(latest)["symbol"].to_list()
    df = fetch_instruments(
        rate_limit=config.tdx_rate_limit_spec(),
        allow_mock=config.tdx_allow_mock,
        config=config,
    )
    return df["symbol"].to_list()


def instrument_metadata(config: Config) -> pl.DataFrame:
    """Disk-only symbol/listing spans used for deterministic routing."""
    curated = config.curated_root / "instruments" / "part-merged.parquet"
    staged = list(config.staging_root.glob("instruments/run_id=*/part-*.parquet"))
    if curated.exists():
        frame = pl.read_parquet(curated)
    elif staged:
        frame = pl.read_parquet(max(staged, key=lambda path: path.stat().st_mtime))
    else:
        return pl.DataFrame(
            schema={"symbol": pl.Utf8, "list_date": pl.Date, "delist_date": pl.Date}
        )
    if "symbol" not in frame.columns:
        return pl.DataFrame(
            schema={"symbol": pl.Utf8, "list_date": pl.Date, "delist_date": pl.Date}
        )
    columns = [name for name in ("symbol", "list_date", "delist_date") if name in frame.columns]
    out = frame.select(columns)
    for name in ("list_date", "delist_date"):
        if name not in out.columns:
            out = out.with_columns(pl.lit(None, dtype=pl.Date).alias(name))
    return out


@dataclass
class DailyBarOwnership:
    """One window's explicit generic/dedicated/no-data ownership split."""

    generic: list[str] = field(default_factory=list)
    delegated_delisted: list[str] = field(default_factory=list)
    expected_no_data: list[str] = field(default_factory=list)


def classify_daily_bar_ownership(
    symbols: list[str],
    spans: dict[str, tuple[date | None, date | None]],
    start: date,
    end: date,
) -> DailyBarOwnership:
    """Route symbols without treating a silent exclusion as completion."""
    out = DailyBarOwnership()
    for symbol in symbols:
        list_date, delist_date = spans.get(symbol, (None, None))
        if list_date is not None and list_date > end:
            out.expected_no_data.append(symbol)
        elif delist_date is not None and delist_date < start:
            out.expected_no_data.append(symbol)
        elif delist_date is not None and delist_date <= end:
            out.delegated_delisted.append(symbol)
        else:
            out.generic.append(symbol)
    return out


def load_bar_universe(config: Config) -> set[str]:
    """Symbols that carry at least one ``daily_bars`` row anywhere in the lake.

    This is the *tradable* universe as daily_bars actually realises it: delisted
    names (source returns no bars) and never-traded instrument placeholders (IPO
    listed but not yet trading) are absent. Live snapshots such as the EastMoney
    valuation clist return those dead names, so filtering to this set keeps
    valuation_metrics in lock-step with daily_bars coverage (audit check
    ``valuation_bars_orphan_symbol``). A genuine IPO enters this set the same day
    it first trades and gets a bar.

    Returns an empty set when no bars exist yet; callers must treat that as
    "cannot reconcile" and skip filtering rather than dropping every row.
    """
    bars_root = config.curated_root / "daily_bars"
    files = list(bars_root.glob("**/*.parquet")) if bars_root.exists() else []
    if not files:
        return set()
    return set(pl.scan_parquet(files).select("symbol").unique().collect()["symbol"].to_list())


def _existing_dates(config: Config, dataset: str, date_col: str) -> set[date]:
    root = config.curated_root / dataset
    files = list(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        return set()
    return set(pl.scan_parquet(files).select(date_col).unique().collect()[date_col].to_list())


def walk_day_backfill(
    config: Config,
    trade_date: date,
    run_id: str,
    dataset: str,
    fetch_one: Callable[[date], pl.DataFrame],
    *,
    source: str,
    date_col: str = "trade_date",
    floor: date = BACKFILL_START,
    flush_days: int = 60,
) -> dict:
    """Walk trading days for a dataset whose fetch answers one day at a time.

    Generalizes ``_backfill_margin_trading`` (steps/capital.py): several
    by-date snapshot datasets — dragon_tiger, block_trades,
    share_unlock_schedule, announcement_index, regulatory_events,
    market_breadth — have an adapter that genuinely serves any historical
    *date_col* value, but until now nothing ever walked a range through it,
    so ``cml backfill <name> --start ...`` silently did nothing: the daily
    step only ever asked for ``trade_date``, never iterated a window.

    Resumable — days already in curated are skipped — and staged every
    *flush_days* so a kill costs only the unflushed chunk, not the sweep so
    far. Single-threaded on purpose: unlike margin_trading this has not been
    measured safe at higher concurrency for these sources, and getting a
    correct sweep once is worth more than a faster wrong one.
    """
    start = getattr(config, "_backfill_start", None) or floor
    end = getattr(config, "_backfill_end", None) or trade_date
    days = list_trading_dates(config, start, min(end, trade_date))
    have = _existing_dates(config, dataset, date_col)
    todo = [d for d in days if d not in have]
    if not todo:
        return {"rows_read": 0, "rows_written": 0, "days_skipped": len(days)}

    writer = StagingWriter(config.staging_root)
    frames: list[pl.DataFrame] = []
    rows_written = 0
    empty_days: list[date] = []
    n_parts = 0

    def flush() -> None:
        nonlocal frames, rows_written, n_parts
        if not frames:
            return
        part = with_provenance(
            pl.concat(frames, how="diagonal_relaxed"),
            source=source,
            data_version=data_version_for(dataset),
        )
        writer.write_batch(dataset, run_id, f"bf-{n_parts:04d}", part)
        n_parts += 1
        rows_written += part.height
        frames = []

    for i, d in enumerate(todo, 1):
        try:
            df = fetch_one(d)
        except Exception:
            # The docstring's "a kill costs only the unflushed chunk" promise
            # is empty if a raise skips this flush — measured in production:
            # announcement_index ran 9.6h and landed zero new days because the
            # failure hit mid-window, taking every already-fetched day with it.
            flush()
            raise
        if df.is_empty():
            empty_days.append(d)
        else:
            frames.append(df)
        if i % flush_days == 0:
            flush()
            logger.info(
                "%s backfill: %d/%d days (at %s, %d rows staged)",
                dataset,
                i,
                len(todo),
                d.isoformat(),
                rows_written,
            )
    flush()

    if empty_days:
        logger.warning(
            "%s backfill: %d trading day(s) returned no rows (e.g. %s) — "
            "left absent; a rerun retries them",
            dataset,
            len(empty_days),
            empty_days[0].isoformat(),
        )
    return {
        "rows_read": rows_written,
        "rows_written": rows_written,
        "days_fetched": len(todo) - len(empty_days),
        "days_skipped": len(days) - len(todo),
        "days_empty": len(empty_days),
    }
