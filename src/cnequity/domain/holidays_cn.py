"""SSE/SZSE non-trading dates and makeup trading days.

Each year comes from its published schedule (see :data:`HOLIDAY_SCHEDULES`).
Weekends are handled separately; only weekday closures and weekend makeup
sessions are listed here. This module is the only copy of the table: the
bundled seed calendar is generated from it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import polars as pl

_EXCHANGE_SCHEDULE = "SSE/SZSE published holiday closures"
_NOTICE_2027 = (
    "国办发明电〔2026〕10号 (2026-11-05), "  # i18n-ignore: the notice's own number
    "https://www.gov.cn/zhengce/zhengceku/202611/content_7085102.htm"
)


@dataclass(frozen=True)
class HolidaySchedule:
    """One year's weekday market closures and the published schedule they come from."""

    source: str
    closed: frozenset[str]


#: Weekday closures by year (春节, 国庆, etc.). A year enters only once its
#: official schedule is published (the State Council notice each autumn, then
#: the exchanges' closure notices); never add guessed dates. The table covers
#: exactly the years listed, so a year that is not here is unknown and the
#: calendar refuses to classify it rather than guessing. 2027 was once entered
#: as a guess, and five closures were missing and two sessions marked closed
#: until the notice replaced it.
HOLIDAY_SCHEDULES: dict[int, HolidaySchedule] = {
    2016: HolidaySchedule(
        _EXCHANGE_SCHEDULE,
        frozenset(
            {
                "2016-01-01",
                "2016-02-08",
                "2016-02-09",
                "2016-02-10",
                "2016-02-11",
                "2016-02-12",
                "2016-04-04",
                "2016-05-02",
                "2016-06-09",
                "2016-06-10",
                "2016-09-15",
                "2016-09-16",
                "2016-10-03",
                "2016-10-04",
                "2016-10-05",
                "2016-10-06",
                "2016-10-07",
            }
        ),
    ),
    2017: HolidaySchedule(
        _EXCHANGE_SCHEDULE,
        frozenset(
            {
                "2017-01-02",
                "2017-01-27",
                "2017-01-30",
                "2017-01-31",
                "2017-02-01",
                "2017-02-02",
                "2017-04-03",
                "2017-04-04",
                "2017-05-01",
                "2017-05-29",
                "2017-05-30",
                "2017-10-02",
                "2017-10-03",
                "2017-10-04",
                "2017-10-05",
                "2017-10-06",
            }
        ),
    ),
    2018: HolidaySchedule(
        _EXCHANGE_SCHEDULE,
        frozenset(
            {
                "2018-01-01",
                "2018-02-15",
                "2018-02-16",
                "2018-02-19",
                "2018-02-20",
                "2018-02-21",
                "2018-04-05",
                "2018-04-06",
                "2018-04-30",
                "2018-05-01",
                "2018-06-18",
                "2018-09-24",
                "2018-10-01",
                "2018-10-02",
                "2018-10-03",
                "2018-10-04",
                "2018-10-05",
                "2018-12-31",
            }
        ),
    ),
    2019: HolidaySchedule(
        _EXCHANGE_SCHEDULE,
        frozenset(
            {
                "2019-01-01",
                "2019-02-04",
                "2019-02-05",
                "2019-02-06",
                "2019-02-07",
                "2019-02-08",
                "2019-04-05",
                "2019-05-01",
                "2019-05-02",
                "2019-05-03",
                "2019-06-07",
                "2019-09-13",
                "2019-10-01",
                "2019-10-02",
                "2019-10-03",
                "2019-10-04",
                "2019-10-07",
            }
        ),
    ),
    2020: HolidaySchedule(
        _EXCHANGE_SCHEDULE,
        frozenset(
            {
                "2020-01-01",
                "2020-01-24",
                "2020-01-27",
                "2020-01-28",
                "2020-01-29",
                "2020-01-30",
                "2020-01-31",
                "2020-04-06",
                "2020-05-01",
                "2020-05-04",
                "2020-05-05",
                "2020-06-25",
                "2020-06-26",
                "2020-10-01",
                "2020-10-02",
                "2020-10-05",
                "2020-10-06",
                "2020-10-07",
                "2020-10-08",
            }
        ),
    ),
    2021: HolidaySchedule(
        _EXCHANGE_SCHEDULE,
        frozenset(
            {
                "2021-01-01",
                "2021-02-11",
                "2021-02-12",
                "2021-02-15",
                "2021-02-16",
                "2021-02-17",
                "2021-04-05",
                "2021-05-03",
                "2021-05-04",
                "2021-05-05",
                "2021-06-14",
                "2021-09-20",
                "2021-09-21",
                "2021-10-01",
                "2021-10-04",
                "2021-10-05",
                "2021-10-06",
                "2021-10-07",
            }
        ),
    ),
    2022: HolidaySchedule(
        _EXCHANGE_SCHEDULE,
        frozenset(
            {
                "2022-01-03",
                "2022-01-31",
                "2022-02-01",
                "2022-02-02",
                "2022-02-03",
                "2022-02-04",
                "2022-04-04",
                "2022-04-05",
                "2022-05-02",
                "2022-05-03",
                "2022-05-04",
                "2022-06-03",
                "2022-09-12",
                "2022-10-03",
                "2022-10-04",
                "2022-10-05",
                "2022-10-06",
                "2022-10-07",
            }
        ),
    ),
    2023: HolidaySchedule(
        _EXCHANGE_SCHEDULE,
        frozenset(
            {
                "2023-01-02",
                "2023-01-23",
                "2023-01-24",
                "2023-01-25",
                "2023-01-26",
                "2023-01-27",
                "2023-04-05",
                "2023-05-01",
                "2023-05-02",
                "2023-05-03",
                "2023-06-22",
                "2023-06-23",
                "2023-09-29",
                "2023-10-02",
                "2023-10-03",
                "2023-10-04",
                "2023-10-05",
                "2023-10-06",
            }
        ),
    ),
    2024: HolidaySchedule(
        _EXCHANGE_SCHEDULE,
        frozenset(
            {
                "2024-01-01",
                "2024-02-09",
                "2024-02-12",
                "2024-02-13",
                "2024-02-14",
                "2024-02-15",
                "2024-02-16",
                "2024-04-04",
                "2024-04-05",
                "2024-05-01",
                "2024-05-02",
                "2024-05-03",
                "2024-06-10",
                "2024-09-16",
                "2024-09-17",
                "2024-10-01",
                "2024-10-02",
                "2024-10-03",
                "2024-10-04",
                "2024-10-07",
            }
        ),
    ),
    2025: HolidaySchedule(
        _EXCHANGE_SCHEDULE,
        frozenset(
            {
                "2025-01-01",
                "2025-01-28",
                "2025-01-29",
                "2025-01-30",
                "2025-01-31",
                "2025-02-03",
                "2025-02-04",
                "2025-04-04",
                "2025-05-01",
                "2025-05-02",
                "2025-05-05",
                "2025-06-02",
                "2025-10-01",
                "2025-10-02",
                "2025-10-03",
                "2025-10-06",
                "2025-10-07",
                "2025-10-08",
            }
        ),
    ),
    2026: HolidaySchedule(
        _EXCHANGE_SCHEDULE,
        frozenset(
            {
                "2026-01-01",
                "2026-01-02",
                "2026-02-16",
                "2026-02-17",
                "2026-02-18",
                "2026-02-19",
                "2026-02-20",
                "2026-02-23",
                "2026-04-06",
                "2026-05-01",
                "2026-05-04",
                "2026-05-05",
                "2026-06-19",
                "2026-09-25",
                "2026-10-01",
                "2026-10-02",
                "2026-10-05",
                "2026-10-06",
                "2026-10-07",
            }
        ),
    ),
    2027: HolidaySchedule(
        _NOTICE_2027,
        frozenset(
            {
                "2027-01-01",
                "2027-02-05",
                "2027-02-08",
                "2027-02-09",
                "2027-02-10",
                "2027-02-11",
                "2027-02-12",
                "2027-04-05",
                "2027-05-03",
                "2027-05-04",
                "2027-05-05",
                "2027-06-09",
                "2027-09-15",
                "2027-10-01",
                "2027-10-04",
                "2027-10-05",
                "2027-10-06",
                "2027-10-07",
            }
        ),
    ),
}

HOLIDAY_TABLE_START = date(min(HOLIDAY_SCHEDULES), 1, 1)
HOLIDAY_TABLE_END = date(max(HOLIDAY_SCHEDULES), 12, 31)

# Historical correction confirmed by the SSE 2001 fact book: the annual
# 240-session count excludes New Year's Day, while an old THS payload
# incorrectly carried two stock rows for this date.
_PRE_TABLE_CLOSURES: frozenset[str] = frozenset({"2001-01-01"})

#: Every known weekday closure.
CLOSED_DATES: frozenset[str] = _PRE_TABLE_CLOSURES.union(
    *(schedule.closed for schedule in HOLIDAY_SCHEDULES.values())
)


def _check_schedules() -> None:
    years = sorted(HOLIDAY_SCHEDULES)
    if years != list(range(years[0], years[-1] + 1)):
        raise ValueError(f"holiday schedules must cover consecutive years: {years}")
    for year, schedule in HOLIDAY_SCHEDULES.items():
        if not schedule.source:
            raise ValueError(f"holiday schedule {year} has no source")
        for iso in schedule.closed:
            day = date.fromisoformat(iso)
            if day.year != year or day.weekday() >= 5:
                raise ValueError(f"holiday schedule {year} lists {iso}, not a weekday of {year}")


_check_schedules()

# Intentionally empty. In mainland China, 调休 make-up workdays fall on
# weekends and apply to government/schools/companies — but the SSE and SZSE
# do NOT open on those weekends. The stock exchanges never hold weekend
# trading sessions. An earlier version of this file misapplied the national
# make-up-workday calendar here, which flagged ~70 closed weekends as trading
# days and shrank index/benchmark samples downstream. Verified against
# daily_bars/index_bars presence (2016–2026): every former entry had zero
# trades. Do not repopulate this with make-up weekends.
EXTRA_TRADING_DATES: frozenset[str] = frozenset()

#: The first session of the SSE, and of this lake's index history. No day before
#: it is a session: the calendar used to call every earlier weekday one, so a
#: plan from 1990-01-01 owed 252 sessions before the market existed.
FIRST_SESSION = date(1990, 12, 19)


def exchange_may_open(day: date) -> bool:
    """Whether SSE/SZSE can hold a session on *day*: a weekday not on the closure list,
    on or after :data:`FIRST_SESSION`.

    A listed make-up session counts too, though there are none (see above).
    This is the calendar-free rule; a stored ``trading_calendar`` row is a
    session when it also says ``is_trading`` (:func:`session_expr`).
    """
    if day < FIRST_SESSION:
        return False
    iso = day.isoformat()
    if iso in EXTRA_TRADING_DATES:
        return True
    return day.weekday() < 5 and iso not in CLOSED_DATES


def exchange_may_open_expr(column: str = "trade_date") -> pl.Expr:
    """:func:`exchange_may_open` over a date column."""
    iso = pl.col(column).dt.strftime("%Y-%m-%d")
    weekday = (pl.col(column).dt.weekday() <= 5) & ~iso.is_in(sorted(CLOSED_DATES))
    opened = pl.col(column) >= pl.lit(FIRST_SESSION)
    if not EXTRA_TRADING_DATES:
        return weekday & opened
    return (iso.is_in(sorted(EXTRA_TRADING_DATES)) | weekday) & opened


def session_expr(column: str = "trade_date") -> pl.Expr:
    """``trading_calendar`` rows that are sessions: ``is_trading`` on a day the exchange may open.

    The calendar is built from vendor rows, so this keeps a vendor's weekend or
    holiday row from becoming a session. Five readers used to write it out.
    """
    return pl.col("is_trading") & exchange_may_open_expr(column)
