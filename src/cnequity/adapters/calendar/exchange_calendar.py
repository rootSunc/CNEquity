"""Exchange trading calendar: holiday-table seed + bar-derived fallback."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from functools import cache
from pathlib import Path

import polars as pl

from cnequity.adapters.exchange.szse_calendar import month_days, parse_month
from cnequity.domain.holidays_cn import (
    CLOSED_DATES,
    HOLIDAY_TABLE_END,
    HOLIDAY_TABLE_START,
    exchange_may_open,
    exchange_may_open_expr,
)
from cnequity.meta_layout import SOURCE_CACHE

logger = logging.getLogger(__name__)

# The seed is the holiday table itself: it covers exactly the years whose
# official schedule is entered, and nothing past them.
_SEED_START = HOLIDAY_TABLE_START
_SEED_END = HOLIDAY_TABLE_END
CALENDAR_FORWARD_COVERAGE_WARN_DAYS = 90


class CalendarCoverageError(RuntimeError):
    """Raised when the calendar cannot safely classify a requested date."""


#: Where the trading_calendar step keeps SZSE's published months, under ``meta/``.
SZSE_CALENDAR_CACHE = SOURCE_CACHE / "szse_calendar"


def calendar_seed_end() -> date:
    """Last calendar date covered by bundled holiday seed data."""
    return _SEED_END


def calendar_forward_coverage_days(as_of: date, end: date | None = None) -> int:
    """Days from *as_of* (inclusive) through *end*, the holiday table's end by default."""
    return ((end or _SEED_END) - as_of).days


def szse_calendar_month_path(meta_root: Path, year: int, month: int) -> Path:
    return meta_root / SZSE_CALENDAR_CACHE / f"{year}-{month:02d}.json"


def load_official_calendar(meta_root: Path) -> dict[date, bool]:
    """``{day: is_session}`` from every cached SZSE month; a bad file is skipped."""
    days: dict[date, bool] = {}
    for path in sorted((meta_root / SZSE_CALENDAR_CACHE).glob("*.json")):
        try:
            year, month = (int(part) for part in path.stem.split("-"))
            days.update(parse_month(path.read_bytes(), year, month))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            logger.warning("Skipping unreadable SZSE calendar cache %s: %s", path, exc)
    return days


def confirmed_calendar_end(official: dict[date, bool]) -> date:
    """Last day the calendar can classify: the holiday table, then SZSE's published months.

    Only whole months that follow the table without a gap extend it; a month
    SZSE has not published yet ends the coverage there.
    """
    end = _SEED_END
    while True:
        following = end + timedelta(days=1)
        days = month_days(following.year, following.month)
        if not all(day in official for day in days):
            return end
        end = days[-1]


def calendar_coverage_end(meta_root: Path) -> date:
    """:func:`confirmed_calendar_end` over the lake's cached SZSE months."""
    return confirmed_calendar_end(load_official_calendar(meta_root))


_is_trading_day = exchange_may_open


def _generate_seed_rows(start: date, end: date) -> list[tuple[str, bool]]:
    rows: list[tuple[str, bool]] = []
    d = start
    while d <= end:
        rows.append((d.isoformat(), _is_trading_day(d)))
        d += timedelta(days=1)
    return rows


@cache
def _table_seed() -> pl.DataFrame:
    return pl.DataFrame(
        _generate_seed_rows(_SEED_START, _SEED_END),
        schema={"trade_date": pl.Utf8, "is_trading": pl.Boolean},
        orient="row",
    ).with_columns(pl.col("trade_date").str.to_date())


def load_seed_calendar(path: Path | None = None) -> pl.DataFrame:
    """Seed calendar rows: generated from the holiday table, or read from *path*.

    *path* is a pinned seed CSV (``trade_date,is_trading``), such as the one a
    restored research snapshot carries in ``meta/seeds``. Without it the seed
    is the holiday table, so there is no second copy to fall out of step.
    """
    if path is None:
        return _table_seed()
    if not path.exists():
        raise FileNotFoundError(f"trading calendar seed not found: {path}")
    df = pl.read_csv(
        path,
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
    from cnequity.query.parquet_scan import dataset_has_parquet, root_parquet_files

    root = curated_root / dataset
    if not dataset_has_parquet(root):
        return set()
    if dataset == "daily_bars":
        # The per-file fallback below deliberately isolates corrupt fragments,
        # but filtering each fragment independently lets an old positive retry
        # create a session beside a newer zero-volume canonical row. Use the
        # partition-aware canonical scan on the normal readable path; retain
        # the isolated fallback so calendar derivation remains best-effort when
        # one historical fragment is unreadable.
        try:
            from cnequity.query.parquet_scan import scan_parquet_root

            scan = scan_parquet_root(root, partition_col="trade_date", traded_only=True)
            scan = scan.filter(exchange_may_open_expr())
            values = scan.select("trade_date").collect().get_column("trade_date").drop_nulls()
            return set(values.to_list())
        except (FileNotFoundError, OSError, pl.exceptions.PolarsError, ValueError) as exc:
            logger.warning("Falling back to per-file %s bar dates: %s", dataset, exc)
    dates: set[date] = set()
    for path in root_parquet_files(root):
        try:
            scan = pl.scan_parquet(str(path))
            names = scan.collect_schema().names()
            if "trade_date" not in names:
                continue
            # SSE/SZSE sessions are Monday-Friday.  Historical source rows
            # must never turn a weekend into a market session while deriving
            # the pre-seed calendar.
            scan = scan.filter(exchange_may_open_expr())
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
    known: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Return calendar rows for [start, end] from published rows, seed, or bars.

    Published calendar rows take precedence. The seed comes next: bar
    derivation only fills dates outside its range — future dates past the bundled
    holiday schedule, and the deep history before it. That ordering keeps a
    spurious bar row from flipping a seed ``is_trading=False`` (a known holiday)
    to ``True``.
    """
    seed = load_seed_calendar(seed_path).filter(pl.col("trade_date").is_between(start, end))
    sessions = dict(seed.select("trade_date", "is_trading").iter_rows())
    if known is not None:
        sessions.update(
            known.filter(pl.col("trade_date").is_between(start, end))
            .select("trade_date", "is_trading")
            .iter_rows()
        )
    bar_trading_days: set[date] = set()
    if curated_root is not None and len(sessions) < (end - start).days + 1:
        # Only consider bar-derived trading days for dates the seed does not
        # cover; within the seed range the seed wins outright.
        bar_trading_days = _trading_days_from_bars(curated_root) - sessions.keys()

    if end > _SEED_END:
        unknown_weekdays: list[date] = []
        unknown_start = max(start, _SEED_END + timedelta(days=1))
        d = unknown_start
        while d <= end:
            if exchange_may_open(d) and d not in sessions and d not in bar_trading_days:
                unknown_weekdays.append(d)
            d += timedelta(days=1)
        if unknown_weekdays:
            raise CalendarCoverageError(
                "trading calendar holiday seed ends at "
                f"{_SEED_END.isoformat()}; cannot classify {unknown_weekdays[0].isoformat()} "
                "until SZSE publishes that year's calendar (the trading_calendar step "
                "fetches it) or its official holiday schedule is added to holidays_cn.py"
            )

    # Before the seed and the holiday table begin, bars are the only evidence
    # there is: `_is_trading_day` would only strip weekends, marking every
    # 春节/国庆 a session and inflating 2001-2008 to ~261 days against an actual
    # ~243. Inside this range a date with no bar anywhere is a closed market.
    bar_era_start = min(bar_trading_days) if bar_trading_days else None
    seed_start = seed["trade_date"].min()

    rows: list[dict] = []
    d = start
    while d <= end:
        if d in sessions:
            is_trading = bool(sessions[d])
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
