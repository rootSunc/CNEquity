"""Shared helpers for step implementations."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta

import polars as pl

from cnequity.adapters.tdx_protocol.client import fetch_instruments
from cnequity.config import Config
from cnequity.domain.datasets import DATASETS, fetch_semantics
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.storage import StagingWriter
from cnequity.storage.state import StateStore

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
        from cnequity.query.canonical import dedupe_by_primary_key

        try:
            from cnequity.query.parquet_scan import collect_parquet_root

            return dedupe_by_primary_key(
                collect_parquet_root(curated, partition_col="trade_date", start=start, end=end),
                "trading_calendar",
            )
        except (FileNotFoundError, OSError, pl.exceptions.PolarsError, ValueError) as exc:
            logger.warning(
                "curated trading_calendar scan failed for %s; salvaging readable files: %s",
                curated,
                exc,
            )
        files = list(curated.glob("**/*.parquet"))
        if files:
            try:
                lf = pl.scan_parquet([str(f) for f in files])
                if start is not None:
                    lf = lf.filter(pl.col("trade_date") >= start)
                if end is not None:
                    lf = lf.filter(pl.col("trade_date") <= end)
                return dedupe_by_primary_key(lf.collect(), "trading_calendar")
            except (FileNotFoundError, OSError, pl.exceptions.PolarsError, ValueError) as exc:
                logger.warning(
                    "mixed curated trading_calendar scan failed for %s; reading files individually: %s",
                    curated,
                    exc,
                )
                frames: list[pl.DataFrame] = []
                for path in files:
                    try:
                        frames.append(pl.read_parquet(path))
                    except (
                        FileNotFoundError,
                        OSError,
                        pl.exceptions.PolarsError,
                        ValueError,
                    ) as file_exc:
                        logger.warning(
                            "skipping unreadable trading_calendar file %s: %s", path, file_exc
                        )
                if frames:
                    frame = pl.concat(frames, how="diagonal_relaxed")
                    if "trade_date" in frame.columns:
                        if start is not None:
                            frame = frame.filter(pl.col("trade_date") >= start)
                        if end is not None:
                            frame = frame.filter(pl.col("trade_date") <= end)
                    return dedupe_by_primary_key(frame, "trading_calendar")
    staging_root = config.staging_root / "trading_calendar"
    staging = sorted(staging_root.rglob("*.parquet")) if staging_root.exists() else []
    if staging:
        df = pl.concat([pl.read_parquet(path) for path in staging], how="diagonal_relaxed")
        if "trade_date" in df.columns:
            if "fetched_at" in df.columns:
                df = df.sort("fetched_at")
            df = df.unique(subset=["trade_date"], keep="last", maintain_order=True)
        if start is not None:
            df = df.filter(pl.col("trade_date") >= start)
        if end is not None:
            df = df.filter(pl.col("trade_date") <= end)
        return df
    return None


def load_curated_instruments(config: Config) -> pl.DataFrame | None:
    """Read the merge-style instrument catalog across all surviving shards."""
    root = config.curated_root / "instruments"
    if not root.exists() or not any(root.rglob("*.parquet")):
        return None
    from cnequity.query.canonical import dedupe_by_primary_key
    from cnequity.query.parquet_scan import collect_parquet_root

    try:
        frame = collect_parquet_root(root, hive=False)
    except FileNotFoundError:
        return None
    return dedupe_by_primary_key(frame, "instruments")


def _load_staged_instruments(config: Config) -> pl.DataFrame | None:
    """Read all recoverable instrument fragments when curated data is absent."""
    root = config.staging_root / "instruments"
    files = sorted(root.rglob("*.parquet")) if root.exists() else []
    if not files:
        return None
    from cnequity.query.canonical import dedupe_by_primary_key

    frame = pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")
    return dedupe_by_primary_key(frame, "instruments")


def list_trading_dates(config: Config, start: date, end: date) -> list[date]:
    """Trading days in [start, end] from curated data or the bundled calendar.

    Never fall back to plain weekdays for the CN market: that would classify
    Spring Festival and National Day as sessions when the lake has not yet
    materialized ``trading_calendar``.
    """
    if start > end:
        return []
    cal = _load_trading_calendar_df(config, start=start, end=end)
    if cal is not None and not cal.is_empty() and "trade_date" in cal.columns:
        covered = set(cal.get_column("trade_date").drop_nulls().to_list())
        expected_days = (end - start).days + 1
        if len(covered) == expected_days:
            out = cal.filter(pl.col("is_trading"))["trade_date"].sort().to_list()
            if out:
                return out
        else:
            logger.warning(
                "trading_calendar only covers %d/%d calendar day(s) in %s..%s; "
                "rebuilding from the seed and bar evidence",
                len(covered),
                expected_days,
                start.isoformat(),
                end.isoformat(),
            )
    from cnequity.adapters.calendar.exchange_calendar import (
        build_trading_calendar,
        ensure_seed_csv,
    )

    seed_path = config.meta_root / "seeds" / "trading_calendar.csv"
    effective_seed = seed_path if seed_path.exists() else ensure_seed_csv()
    calendar = build_trading_calendar(
        start,
        end,
        seed_path=effective_seed,
        curated_root=config.curated_root if config.curated_root.exists() else None,
    )
    return calendar.filter(pl.col("is_trading"))["trade_date"].sort().to_list()


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

    from cnequity.adapters.calendar.exchange_calendar import (
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
    if day_cal.is_empty():
        raise RuntimeError(
            f"trading calendar returned no row for {trade_date.isoformat()}; "
            "refusing to classify it by weekday"
        )
    return bool(day_cal["is_trading"][0])


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


def _backfill_empty_day_finding(dataset: str, empty_days: list[date]) -> dict:
    """Describe dates that were retried but returned no rows.

    Empty responses are non-fatal because some sources legitimately publish no
    rows on a session.  They must still be visible to the run audit; otherwise
    a backfill can report success while leaving permanent coverage holes.
    """
    return {
        "dataset": dataset,
        "severity": "warning",
        "check": "backfill_empty_days",
        "message": (
            f"{dataset}: {len(empty_days)} trading day(s) returned no rows; "
            "they remain absent and will be retried on the next backfill"
        ),
        "days_empty": len(empty_days),
        "sample_dates": [d.isoformat() for d in empty_days[:8]],
    }


def _dense_empty_day_finding(dataset: str, empty_days: list[date]) -> dict:
    """Keep an allowed empty response visible for a dense daily dataset."""
    return {
        "dataset": dataset,
        "severity": "warning",
        "check": "session_dense_empty_days",
        "message": (
            f"{dataset}: {len(empty_days)} trading day(s) returned no rows; "
            "the dense coverage watermark must not pass these sessions"
        ),
        "days_empty": len(empty_days),
        "sample_dates": [d.isoformat() for d in empty_days[:8]],
    }


def _validate_trade_date(
    df: pl.DataFrame,
    dataset: str,
    trade_date: date,
    *,
    date_col: str | None = None,
) -> None:
    """Reject a response whose date column names another day.

    The historical default checks ``trade_date`` only when present because a
    few period/snapshot datasets have another date key. Callers that fetch an
    exact day under a different key must pass it explicitly; then a missing
    date column is itself a malformed response.
    """
    column = date_col or "trade_date"
    if df.is_empty():
        return
    if column not in df.columns:
        if date_col is not None:
            raise RuntimeError(
                f"{dataset}: fetch for {trade_date.isoformat()} did not return "
                f"the configured date column {date_col!r}"
            )
        return
    parsed_dates = df.get_column(column).cast(pl.Date, strict=False)
    mismatched = int((parsed_dates.is_null() | (parsed_dates != trade_date).fill_null(True)).sum())
    if mismatched:
        raise RuntimeError(
            f"{dataset}: fetch for {trade_date.isoformat()} returned "
            f"{mismatched} row(s) with a different or invalid {column}"
        )


def fetch_incremental_daily(
    config: Config,
    dataset: str,
    trade_date: date,
    fetch_fn: Callable[[date], pl.DataFrame],
    *,
    allow_empty: bool = False,
    date_col: str | None = None,
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
        frame = fetch_fn(trade_date)
        if frame.is_empty() and not allow_empty:
            raise RuntimeError(f"{dataset}: no rows returned for {trade_date.isoformat()}")
        _validate_trade_date(frame, dataset, trade_date, date_col=date_col)
        return frame, []

    spec = DATASETS.get(dataset)
    if semantics == "snapshot" and spec is not None and not spec.watermark:
        # Rolling live windows (for example share_unlock_schedule) are not
        # incremental histories. Do not use a legacy state file to manufacture
        # gap dates; the current snapshot is the only honest request and its
        # future event dates must never become a watermark.
        dates = [trade_date]
    else:
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
    empty_days: list[date] = []
    for d in fetch_dates:
        part = fetch_fn(d)
        if part.is_empty():
            if not allow_empty:
                raise RuntimeError(f"{dataset}: no rows returned for {d.isoformat()}")
            spec = DATASETS.get(dataset)
            if spec is not None and spec.coverage_mode == "session_dense":
                empty_days.append(d)
            continue
        # A by-date adapter must not let a vendor page leak rows from a
        # neighbouring session into the requested partition. Validate here
        # before diagonal concatenation; once several days are merged the
        # offending response can no longer be attributed to one request.
        _validate_trade_date(part, dataset, d, date_col=date_col)
        frames.append(part)
    if empty_days:
        findings.append(_dense_empty_day_finding(dataset, empty_days))
    if not frames:
        return pl.DataFrame(), findings
    return pl.concat(frames, how="diagonal_relaxed"), findings


def load_symbols(config: Config) -> list[str]:
    """Universe symbols: curated instruments first, then staging, then source."""
    curated_frame = load_curated_instruments(config)
    staged_frame = _load_staged_instruments(config)
    if curated_frame is not None:
        return curated_frame["symbol"].drop_nulls().to_list()
    if staged_frame is not None:
        return staged_frame["symbol"].drop_nulls().to_list()
    df = fetch_instruments(
        rate_limit=config.tdx_rate_limit_spec(),
        allow_mock=config.tdx_allow_mock,
        config=config,
    )
    return df["symbol"].to_list()


def instrument_metadata(config: Config) -> pl.DataFrame:
    """Disk-only symbol/listing spans used for deterministic routing."""
    curated_frame = load_curated_instruments(config)
    staged_frame = _load_staged_instruments(config)
    empty = pl.DataFrame(
        schema={
            "symbol": pl.Utf8,
            "list_date": pl.Date,
            "delist_date": pl.Date,
            "asset_type": pl.Utf8,
        }
    )
    if curated_frame is not None:
        frame = curated_frame
    elif staged_frame is not None:
        frame = staged_frame
    else:
        return empty
    if "symbol" not in frame.columns:
        return empty
    columns = [
        name
        for name in ("symbol", "list_date", "delist_date", "asset_type")
        if name in frame.columns
    ]
    out = frame.select(columns)
    for name, dtype in (("list_date", pl.Date), ("delist_date", pl.Date), ("asset_type", pl.Utf8)):
        if name not in out.columns:
            out = out.with_columns(pl.lit(None, dtype=dtype).alias(name))
    return out


@dataclass
class DailyBarOwnership:
    """One window's explicit generic/dedicated/no-data ownership split."""

    generic: list[str] = field(default_factory=list)
    delegated_delisted: list[str] = field(default_factory=list)
    expected_no_data: list[str] = field(default_factory=list)


def classify_daily_bar_ownership(
    symbols: list[str],
    spans: dict[str, tuple[date | None, date | None, str | None]],
    start: date,
    end: date,
    *,
    bar_universe: set[str] | None = None,
) -> DailyBarOwnership:
    """Route symbols without treating a silent exclusion as completion."""
    out = DailyBarOwnership()
    for symbol in symbols:
        list_date, delist_date, asset_type = spans.get(symbol, (None, None, None))
        if list_date is not None and list_date > end:
            out.expected_no_data.append(symbol)
        elif delist_date is not None and delist_date < start:
            out.expected_no_data.append(symbol)
        elif delist_date is not None and delist_date <= end:
            out.delegated_delisted.append(symbol)
        elif (
            asset_type == "etf"
            and list_date is None
            and bar_universe is not None
            and symbol not in bar_universe
        ):
            # An ETF with no listing date and no traded bar in the lake is an
            # issued-but-not-yet-listed placeholder (e.g. 589430.SH). Requiring
            # TDX rows for it would fail the whole batch; it has no tradable
            # session to fetch.
            out.expected_no_data.append(symbol)
        else:
            out.generic.append(symbol)
    return out


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
    files = list(bars_root.glob("**/*.parquet")) if bars_root.exists() else []
    if not files:
        return set()
    # Apply the volume contract per file. A diagonal union would turn legacy
    # files without ``volume`` into nulls, and a global ``volume > 0`` filter
    # would then silently discard their valid row-based evidence.
    scans: list[pl.LazyFrame] = []
    for path in files:
        scan = pl.scan_parquet(str(path))
        if "volume" in scan.collect_schema().names():
            scan = scan.filter(pl.col("volume") > 0)
        scans.append(scan.select("symbol"))
    return set(
        pl.concat(scans, how="diagonal_relaxed")
        .select("symbol")
        .unique()
        .collect()["symbol"]
        .to_list()
    )


def _existing_dates(config: Config, dataset: str, date_col: str) -> set[date]:
    root = config.curated_root / dataset
    files = list(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        return set()
    from cnequity.query.parquet_scan import scan_parquet_files

    return set(scan_parquet_files(files).select(date_col).unique().collect()[date_col].to_list())


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
    existing_dates_fn: Callable[[list[date]], set[date]] | None = None,
) -> dict:
    """Walk trading days for a dataset whose fetch answers one day at a time.

    Generalizes ``_backfill_margin_trading`` (steps/capital.py): several
    by-date snapshot datasets — dragon_tiger, block_trades,
    share_unlock_schedule, announcement_index, regulatory_events,
    market_breadth — have an adapter that genuinely serves any historical
    *date_col* value, but until now nothing ever walked a range through it,
    so ``cne backfill <name> --start ...`` silently did nothing: the daily
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
    have = (
        existing_dates_fn(days)
        if existing_dates_fn is not None
        else _existing_dates(config, dataset, date_col)
    )
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
            if df.is_empty():
                empty_days.append(d)
            else:
                if date_col not in df.columns:
                    raise RuntimeError(
                        f"{dataset}: fetch for {d.isoformat()} did not return the "
                        f"configured date column {date_col!r}"
                    )
                parsed_dates = df.get_column(date_col).cast(pl.Date, strict=False)
                mismatched = int(
                    (parsed_dates.is_null() | (parsed_dates != d).fill_null(True)).sum()
                )
                if mismatched:
                    raise RuntimeError(
                        f"{dataset}: fetch for {d.isoformat()} returned {mismatched} row(s) "
                        f"with a different or invalid {date_col}"
                    )
                frames.append(df)
        except Exception:
            # The docstring's "a kill costs only the unflushed chunk" promise
            # is empty if a raise skips this flush — measured in production:
            # announcement_index ran 9.6h and landed zero new days because the
            # failure hit mid-window, taking every already-fetched day with it.
            # Response validation errors must preserve the same checkpoint.
            flush()
            raise
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
    result = {
        "rows_read": rows_written,
        "rows_written": rows_written,
        "days_fetched": len(todo) - len(empty_days),
        "days_skipped": len(days) - len(todo),
        "days_empty": len(empty_days),
    }
    if empty_days:
        # Keep direct step calls truthful as well as engine-managed calls. The
        # engine promotes ``days_empty`` to warning, but callers that invoke a
        # backfill step directly would otherwise receive ``success`` while
        # known dates remain absent and retryable.
        result["status"] = "warning"
        result["context_updates"] = {
            "audit_findings": [_backfill_empty_day_finding(dataset, empty_days)]
        }
    return result
