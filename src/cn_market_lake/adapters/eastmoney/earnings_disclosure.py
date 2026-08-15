"""EastMoney scheduled earnings disclosure dates (预约披露时间表).

Both exchanges publish per-stock scheduled dates for annual/quarterly reports;
the EM datacenter report ``RPT_PUBLIC_BS_APPOIN`` mirrors them with the current
scheduled date, the first-ever scheduled date, and the actual publish date once
disclosed. Rows are CURRENT-STATE, not PIT: a revision overwrites
``APPOINT_PUBLISH_DATE`` in place (``FIRST_APPOINT_DATE`` keeps the original).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import polars as pl

from cn_market_lake.adapters.eastmoney.common import (
    report_period_from_date,
    symbol_from_secucode,
)
from cn_market_lake.adapters.eastmoney.datacenter import fetch_datacenter
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
from cn_market_lake.config import Config

logger = logging.getLogger(__name__)

_REPORT = "RPT_PUBLIC_BS_APPOIN"
_COLUMNS = (
    "SECUCODE,SECURITY_CODE,REPORT_DATE,APPOINT_PUBLISH_DATE,FIRST_APPOINT_DATE,ACTUAL_PUBLISH_DATE"
)

# Measured 2026-08: RPT_PUBLIC_BS_APPOIN returns real rows at 2006-12-31
# (1,262) and is empty at 2005-12-31 — 2016 was a guess, not a probed floor.
_BACKFILL_START_YEAR = 2006
_QUARTER_END_MMDD = ((3, 31), (6, 30), (9, 30), (12, 31))

# Annual reports may be disclosed up to 4 months after period end (Apr 30);
# keep a period active a bit longer so late filers still get actual_date.
_WINDOW_BACK_DAYS = 150
# The next period's timetable appears around period end (annual schedules in
# late December, half-year on the evening of Jun 30); look ahead one quarter.
_WINDOW_AHEAD_DAYS = 100


def _quarter_ends(start: date, end: date) -> list[str]:
    """Quarter-end report dates within [start, end], ascending ISO strings."""
    out: list[str] = []
    for year in range(start.year, end.year + 1):
        for mm, dd in _QUARTER_END_MMDD:
            d = date(year, mm, dd)
            if start <= d <= end:
                out.append(d.isoformat())
    return out


def _active_report_dates(trade_date: date) -> list[str]:
    """Report periods whose disclosure timetable is still moving around *trade_date*."""
    return _quarter_ends(
        trade_date - timedelta(days=_WINDOW_BACK_DAYS),
        trade_date + timedelta(days=_WINDOW_AHEAD_DAYS),
    )


def _backfill_report_dates(trade_date: date) -> list[str]:
    """Every quarter-end period from 2016 through the look-ahead window."""
    return _quarter_ends(
        date(_BACKFILL_START_YEAR, 1, 1),
        trade_date + timedelta(days=_WINDOW_AHEAD_DAYS),
    )


def _parse_date(raw: object) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def _parse_rows(raw: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for item in raw:
        # SECUCODE filters to A-share (drops B-share/NEEQ rows the report carries).
        sym = symbol_from_secucode(item.get("SECUCODE"))
        if not sym:
            continue
        report_period = report_period_from_date(item.get("REPORT_DATE"))
        if not report_period:
            continue
        scheduled = _parse_date(item.get("APPOINT_PUBLISH_DATE"))
        first = _parse_date(item.get("FIRST_APPOINT_DATE"))
        if scheduled is None:
            scheduled = first
        if first is None:
            first = scheduled
        if scheduled is None:
            continue
        rows.append(
            {
                "symbol": sym,
                "report_period": report_period,
                "scheduled_date": scheduled,
                "first_scheduled_date": first,
                "actual_date": _parse_date(item.get("ACTUAL_PUBLISH_DATE")),
            }
        )
    return rows


def fetch_earnings_disclosure_schedule(
    trade_date: date,
    *,
    backfill: bool = False,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    """Fetch the scheduled-disclosure timetable per report period.

    ``backfill=False`` (daily): periods whose disclosure window is open around
    *trade_date* — refreshes revisions and fills ``actual_date`` as reports
    land. ``backfill=True``: every quarter-end period 2016 → *trade_date*.
    A future period returns no rows until the exchanges publish its timetable.
    """
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)

    if backfill:
        report_dates = _backfill_report_dates(trade_date)
    else:
        report_dates = _active_report_dates(trade_date)

    rows: list[dict] = []
    try:
        for report_date in report_dates:
            if config is not None:
                config.rate_limit("eastmoney")
            raw = fetch_datacenter(
                client,
                _REPORT,
                _COLUMNS,
                filter_expr=f"(REPORT_DATE='{report_date}')",
                sort_columns="SECURITY_CODE",
                sort_types="1",
            )
            rows.extend(_parse_rows(raw))
    finally:
        if owns:
            client.close()

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "report_period"], keep="last")
