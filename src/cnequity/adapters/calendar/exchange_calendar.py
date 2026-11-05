"""Exchange trading calendar: bundled seed CSV + bar-derived fallback."""

from __future__ import annotations

import csv
import logging
from datetime import date, timedelta
from importlib import resources
from pathlib import Path

import polars as pl

from cnequity.domain.holidays_cn import CLOSED_DATES, EXTRA_TRADING_DATES

logger = logging.getLogger(__name__)

_SEED_START = date(2016, 1, 1)
_SEED_END = date(2027, 12, 31)
CALENDAR_FORWARD_COVERAGE_WARN_DAYS = 90


class CalendarCoverageError(RuntimeError):
    """Raised when the calendar cannot safely classify a requested date."""


def calendar_seed_end() -> date:
    """Last calendar date covered by bundled holiday seed data."""
    return _SEED_END


def calendar_forward_coverage_days(as_of: date) -> int:
    """Days from *as_of* (inclusive) through the bundled seed end date."""
    return (_SEED_END - as_of).days


def _is_trading_day(d: date) -> bool:
    iso = d.isoformat()
    if iso in EXTRA_TRADING_DATES:
        return True
    if d.weekday() >= 5:
        return False
    return iso not in CLOSED_DATES


def _generate_seed_rows(start: date, end: date) -> list[tuple[str, bool]]:
    rows: list[tuple[str, bool]] = []
    d = start
    while d <= end:
        rows.append((d.isoformat(), _is_trading_day(d)))
        d += timedelta(days=1)
    return rows


def _default_seed_path() -> Path:
    return Path(resources.files("cnequity.adapters.calendar") / "seeds" / "trading_calendar.csv")


def ensure_seed_csv(path: Path | None = None) -> Path:
    """Write bundled seed CSV if missing (idempotent)."""
    target = path or _default_seed_path()
    if target.exists():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["trade_date", "is_trading"])
        writer.writerows(_generate_seed_rows(_SEED_START, _SEED_END))
    return target


def load_seed_calendar(path: Path | None = None) -> pl.DataFrame:
    seed_path = path or ensure_seed_csv()
    if not seed_path.exists():
        raise FileNotFoundError(f"trading calendar seed not found: {seed_path}")
    df = pl.read_csv(
        seed_path,
        schema={"trade_date": pl.Utf8, "is_trading": pl.Boolean},
    )
    return df.with_columns(pl.col("trade_date").str.to_date(strict=False))


def _bar_dates(curated_root: Path, dataset: str) -> set[date]:
    """Return dates with usable rows from one curated bar dataset.

    A stock daily-bar placeholder (flat OHLC, ``volume=0``) means that the
    instrument did not trade; it must not create a pre-seed trading session.
    Older lakes predate the volume contract, so files without that column keep
    their historical row-presence semantics. Index bars are also kept row-based
    because index volume has different units and zero can be a valid quote.
    """
    root = curated_root / dataset
    if not root.exists():
        return set()
    if dataset == "daily_bars" and any(root.rglob("*.parquet")):
        # The per-file fallback below deliberately isolates corrupt fragments,
        # but filtering each fragment independently lets an old positive retry
        # create a session beside a newer zero-volume canonical row. Use the
        # partition-aware canonical scan on the normal readable path; retain
        # the isolated fallback so calendar derivation remains best-effort when
        # one historical fragment is unreadable.
        try:
            from cnequity.query.parquet_scan import scan_parquet_root

            scan = scan_parquet_root(root, partition_col="trade_date", traded_only=True)
            scan = scan.filter(
                (pl.col("trade_date").dt.weekday() <= 5)
                & ~pl.col("trade_date").dt.strftime("%Y-%m-%d").is_in(CLOSED_DATES)
            )
            values = scan.select("trade_date").collect().get_column("trade_date").drop_nulls()
            return set(values.to_list())
        except (FileNotFoundError, OSError, pl.exceptions.PolarsError, ValueError) as exc:
            logger.warning("Falling back to per-file %s bar dates: %s", dataset, exc)
    dates: set[date] = set()
    for path in root.glob("**/*.parquet"):
        try:
            scan = pl.scan_parquet(str(path))
            names = scan.collect_schema().names()
            if "trade_date" not in names:
                continue
            # SSE/SZSE sessions are Monday-Friday.  Historical source rows
            # must never turn a weekend into a market session while deriving
            # the pre-seed calendar.
            scan = scan.filter(
                (pl.col("trade_date").dt.weekday() <= 5)
                & ~pl.col("trade_date").dt.strftime("%Y-%m-%d").is_in(CLOSED_DATES)
            )
            if dataset == "daily_bars" and "volume" in names:
                scan = scan.filter(pl.col("volume") > 0)
            values = scan.select("trade_date").collect().get_column("trade_date").drop_nulls()
            dates.update(values.to_list())
        except (OSError, pl.exceptions.PolarsError, ValueError) as exc:
            # Calendar construction is also used by health/audit commands.
            # A bad bar file must be reported by the dataset audit, but should
            # not prevent the calendar from being built from the other files.
            logger.warning("Skipping unreadable %s bar file %s: %s", dataset, path, exc)
    return dates


def curated_bar_dates(curated_root: Path, dataset: str) -> set[date]:
    """Public helper for calendar/reference steps to share bar-date semantics."""
    if dataset not in {"daily_bars", "index_bars"}:
        raise ValueError(f"unsupported bar dataset: {dataset!r}")
    return _bar_dates(curated_root, dataset)


def _trading_days_from_bars(curated_root: Path) -> set[date]:
    """Dates any bar dataset recorded — a session nobody traded does not exist.

    Reads ``index_bars`` and ``daily_bars`` both. index_bars alone was enough
    while the lake started in 2016, but deep history reaches back to 2001 in
    daily_bars only, and without those dates every pre-2016 window resolves to
    zero trading days and a backtest over it fails outright. daily_bars is also
    the sounder signal of the two: a whole market trading is harder to
    misattribute than one index printing a bar.
    """
    days: set[date] = set()
    for dataset in ("index_bars", "daily_bars"):
        days |= curated_bar_dates(curated_root, dataset)
    return days


def build_trading_calendar(
    start: date,
    end: date,
    *,
    seed_path: Path | None = None,
    curated_root: Path | None = None,
) -> pl.DataFrame:
    """Return calendar rows for [start, end] from seed, extended by bar data.

    The seed is authoritative for every date it covers (2016 onward): bar
    derivation only fills dates outside its range — future dates past the bundled
    holiday schedule, and the deep history before it. That ordering keeps a
    spurious bar row from flipping a seed ``is_trading=False`` (a known holiday)
    to ``True``.
    """
    seed = load_seed_calendar(seed_path)
    seed = seed.filter((pl.col("trade_date") >= start) & (pl.col("trade_date") <= end))

    seed_dates = set(seed["trade_date"].to_list())
    bar_trading_days: set[date] = set()
    if curated_root is not None:
        # Only consider bar-derived trading days for dates the seed does not
        # cover; within the seed range the seed wins outright.
        bar_trading_days = _trading_days_from_bars(curated_root) - seed_dates

    if end > _SEED_END:
        unknown_weekdays: list[date] = []
        unknown_start = max(start, _SEED_END + timedelta(days=1))
        d = unknown_start
        while d <= end:
            if d.weekday() < 5 and d not in bar_trading_days:
                unknown_weekdays.append(d)
            d += timedelta(days=1)
        if unknown_weekdays:
            raise CalendarCoverageError(
                "trading calendar holiday seed ends at "
                f"{_SEED_END.isoformat()}; cannot classify {unknown_weekdays[0].isoformat()} "
                "without refreshed holidays_cn.py or curated bar evidence"
            )

    # Before the seed and the holiday table begin, bars are the only evidence
    # there is: `_is_trading_day` would only strip weekends, marking every
    # 春节/国庆 a session and inflating 2001-2008 to ~261 days against an actual
    # ~243. Inside this range a date with no bar anywhere is a closed market.
    bar_era_start = min(bar_trading_days) if bar_trading_days else None
    seed_start = min(seed_dates) if seed_dates else None

    rows: list[dict] = []
    d = start
    while d <= end:
        in_seed = seed.filter(pl.col("trade_date") == d)
        if not in_seed.is_empty():
            is_trading = bool(in_seed["is_trading"][0])
        elif d.isoformat() in CLOSED_DATES:
            is_trading = False
        elif d in bar_trading_days:
            is_trading = True
        elif (
            bar_era_start is not None
            and bar_era_start <= d
            and (seed_start is None or d < seed_start)
        ):
            is_trading = False  # inside the bar era, no bar means closed
        else:
            is_trading = _is_trading_day(d)
        rows.append({"trade_date": d, "is_trading": is_trading})
        d += timedelta(days=1)

    return pl.DataFrame(rows).sort("trade_date")
