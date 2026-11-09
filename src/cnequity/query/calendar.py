"""Shared local calendar reads; staging is an explicit ingestion-only option."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import polars as pl

from cnequity.config import Config
from cnequity.domain.holidays_cn import CLOSED_DATES
from cnequity.domain.market_time import is_session_final
from cnequity.storage.read_context import read_root

logger = logging.getLogger(__name__)


def _load_trading_calendar_df(
    config: Config,
    *,
    start: date | None = None,
    end: date | None = None,
    include_staging: bool = False,
) -> pl.DataFrame | None:
    """Load trading_calendar, preferring a lazy hive scan with optional date prune."""
    curated = read_root(config, "trading_calendar")
    if curated.exists() and any(curated.rglob("*.parquet")):
        from cnequity.query.canonical import dedupe_by_primary_key

        try:
            from cnequity.query.parquet_scan import collect_parquet_root

            return dedupe_by_primary_key(
                collect_parquet_root(
                    curated, partition_col="trade_date", start=start, end=end, committed=False
                ),
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
    if not include_staging:
        return None
    staging_root = config.staging_root / "trading_calendar"
    staging = sorted(staging_root.rglob("*.parquet")) if staging_root.exists() else []
    if staging:
        from cnequity.query.canonical import dedupe_by_primary_key

        df = pl.concat([pl.read_parquet(path) for path in staging], how="diagonal_relaxed")
        if "trade_date" in df.columns:
            df = dedupe_by_primary_key(df, "trading_calendar")
        if start is not None:
            df = df.filter(pl.col("trade_date") >= start)
        if end is not None:
            df = df.filter(pl.col("trade_date") <= end)
        return df
    return None


def list_trading_dates(
    config: Config, start: date, end: date, *, include_staging: bool = False
) -> list[date]:
    """Trading days in [start, end] from curated data or the bundled calendar.

    Never fall back to plain weekdays for the CN market: that would classify
    Spring Festival and National Day as sessions when the lake has not yet
    materialized ``trading_calendar``.
    """
    if start > end:
        return []
    cal = _load_trading_calendar_df(config, start=start, end=end, include_staging=include_staging)
    if cal is not None and not cal.is_empty() and "trade_date" in cal.columns:
        covered = set(cal.get_column("trade_date").drop_nulls().to_list())
        expected_days = (end - start).days + 1
        if len(covered) == expected_days:
            out = (
                cal.filter(
                    pl.col("is_trading")
                    & (pl.col("trade_date").dt.weekday() <= 5)
                    & ~pl.col("trade_date").dt.strftime("%Y-%m-%d").is_in(CLOSED_DATES)
                )["trade_date"]
                .sort()
                .to_list()
            )
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
    return (
        calendar.filter(
            pl.col("is_trading")
            & (pl.col("trade_date").dt.weekday() <= 5)
            & ~pl.col("trade_date").dt.strftime("%Y-%m-%d").is_in(CLOSED_DATES)
        )["trade_date"]
        .sort()
        .to_list()
    )


def is_trading_day(config: Config, trade_date: date, *, include_staging: bool = False) -> bool:
    """Return whether *trade_date* is a trading day per curated calendar or seed."""
    if trade_date.weekday() >= 5 or trade_date.isoformat() in CLOSED_DATES:
        return False
    cal = _load_trading_calendar_df(
        config, start=trade_date, end=trade_date, include_staging=include_staging
    )
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


def last_trading_day(
    config: Config, day: date, *, include_staging: bool = False, now: datetime | None = None
) -> date:
    """The newest trading day on or before *day* whose session is final.

    *day* itself counts only once its session has closed (see
    ``is_session_final``; *now* defaults to the clock). Gives up after 15 calendar days, the longest A-share
    closure plus margin, and returns *day*. The CLI, the dashboard and the
    audit each had their own copy of this loop.
    """
    candidate = day if is_session_final(day, now) else day - timedelta(days=1)
    for _ in range(15):
        if is_trading_day(config, candidate, include_staging=include_staging):
            return candidate
        candidate -= timedelta(days=1)
    return day
