"""Macro indicators — daily rates and monthly series, each from its own publisher.

Rows carry their own ``source`` rather than taking the step's blanket value, so
a curated row always names the feed it came from.

Every series except 社融 comes from EastMoney's datacenter; 社融 comes from the
PBOC, which publishes it. AkShare used to supply the monthly block, but its PMI and 货币供应量
wrappers request the *same* ``datacenter-web.eastmoney.com`` endpoint this module
already talks to, so going direct removes a parsing layer without changing the
publisher — and picks up the project's own retry, throttle and TLS handling.
See issue #3.

Monthly observations are stamped at month end. EastMoney reports them at month
*start* (``REPORT_DATE = 2026-07-01``), so they are converted; changing this
convention would double-write every month already in curated under a second key.
"""

from __future__ import annotations

import calendar
import logging
import re
from datetime import date

import polars as pl

from cn_market_lake.adapters.eastmoney.datacenter import (
    EastMoneyDatacenterError,
    fetch_datacenter,
)
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient

logger = logging.getLogger(__name__)

_TREASURY_REPORT = "RPTA_WEB_TREASURYYIELD"
_TREASURY_COLUMNS = "SOLAR_DATE,EMM00166466"
_SHIBOR_REPORT = "RPT_IMP_INTRESTRATEN"
_SHIBOR_COLUMNS = "REPORT_DATE,IR_RATE"
_SHIBOR_FILTER = '(MARKET_CODE="001")(CURRENCY_CODE="CNY")(INDICATOR_ID="203")'
_LPR_REPORT = "RPTA_WEB_RATE"
_LPR_COLUMNS = "TRADE_DATE,LPR1Y"

# Monthly series published on EastMoney's 经济数据 pages, read directly.
#   pmi_manufacturing  制造业 PMI          data.eastmoney.com/cjsj/pmi.html
#   m2_yoy             M2 同比增长 (%)     data.eastmoney.com/cjsj/hbgyl.html
# `columns` is the full set the page requests; only `value_column` is kept, but
# asking for the page's own column list is what keeps the report accepting it.
_EM_MONTHLY_SERIES = {
    "pmi_manufacturing": {
        "report": "RPT_ECONOMY_PMI",
        "columns": "REPORT_DATE,TIME,MAKE_INDEX,MAKE_SAME,NMAKE_INDEX,NMAKE_SAME",
        "value_column": "MAKE_INDEX",
    },
    "m2_yoy": {
        "report": "RPT_ECONOMY_CURRENCY_SUPPLY",
        "columns": (
            "REPORT_DATE,TIME,BASIC_CURRENCY,BASIC_CURRENCY_SAME,"
            "CURRENCY,CURRENCY_SAME,FREE_CASH,FREE_CASH_SAME"
        ),
        # BASIC_CURRENCY is the M2 level; _SAME is its year-on-year change.
        "value_column": "BASIC_CURRENCY_SAME",
    },
}


def _parse_obs_date(value: object, fallback: date) -> date:
    if value is None:
        return fallback
    text = str(value)[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return fallback


_MONTH_RE = re.compile(r"^(\d{4})[-年/.](\d{1,2})月?")


def _parse_series_obs_date(value: object) -> date | None:
    """Parse a monthly observation date, mapping the month to its last day.

    Accepts ISO dates (EastMoney's ``2026-07-01 00:00:00``) and separated month
    forms (``2026年07月份``, ``2026-07``). Returns None when unparseable — the
    row is dropped rather than stamped with a fabricated date.
    """
    if value is None:
        return None
    if isinstance(value, date):
        return _to_month_end(value)
    text = str(value).strip()
    try:
        return _to_month_end(date.fromisoformat(text[:10]))
    except ValueError:
        pass
    match = _MONTH_RE.match(text)
    if match:
        year, month = int(match.group(1)), int(match.group(2))
        if 1 <= month <= 12:
            return date(year, month, calendar.monthrange(year, month)[1])
    return None


def _to_month_end(value: date) -> date:
    return value.replace(day=calendar.monthrange(value.year, value.month)[1])


def _eastmoney_daily(client: EastMoneyClient, trade_date: date) -> list[dict]:
    ds = trade_date.isoformat()
    rows: list[dict] = []

    try:
        treasury = fetch_datacenter(
            client,
            _TREASURY_REPORT,
            _TREASURY_COLUMNS,
            filter_expr=f"(SOLAR_DATE='{ds}')",
        )
    except EastMoneyDatacenterError as exc:
        logger.warning("EastMoney treasury indicator fetch skipped: %s", exc)
        treasury = []
    for item in treasury:
        val = item.get("EMM00166466")
        if val is not None:
            rows.append(
                {
                    "indicator_id": "cnbond_yield_10y",
                    "obs_date": _parse_obs_date(item.get("SOLAR_DATE"), trade_date),
                    "value": float(val),
                    "frequency": "daily",
                    "source": "eastmoney",
                }
            )

    try:
        shibor = fetch_datacenter(
            client,
            _SHIBOR_REPORT,
            _SHIBOR_COLUMNS,
            filter_expr=f"{_SHIBOR_FILTER}(REPORT_DATE='{ds}')",
        )
    except EastMoneyDatacenterError as exc:
        logger.warning("EastMoney SHIBOR indicator fetch skipped: %s", exc)
        shibor = []
    for item in shibor:
        val = item.get("IR_RATE")
        if val is not None:
            rows.append(
                {
                    "indicator_id": "shibor_3m",
                    "obs_date": _parse_obs_date(item.get("REPORT_DATE"), trade_date),
                    "value": float(val),
                    "frequency": "daily",
                    "source": "eastmoney",
                }
            )

    try:
        lpr = fetch_datacenter(
            client,
            _LPR_REPORT,
            _LPR_COLUMNS,
            filter_expr=f"(TRADE_DATE='{ds}')",
        )
    except EastMoneyDatacenterError as exc:
        logger.warning("EastMoney LPR indicator fetch skipped: %s", exc)
        lpr = []
    for item in lpr:
        val = item.get("LPR1Y")
        if val is not None:
            rows.append(
                {
                    "indicator_id": "lpr_1y",
                    "obs_date": _parse_obs_date(item.get("TRADE_DATE"), trade_date),
                    "value": float(val),
                    "frequency": "monthly",
                    "source": "eastmoney",
                }
            )

    return rows


def _eastmoney_monthly(client: EastMoneyClient, trade_date: date) -> list[dict]:
    """PMI and M2 straight from the EastMoney datacenter reports.

    Each report returns its whole published history (~220 months back to 2008)
    in one page. Everything up to ``trade_date`` is kept: monthly observations
    almost never land on the run day, and compact dedupes by
    ``(indicator_id, obs_date)``, so re-ingesting is idempotent and a first run
    backfills the series.
    """
    rows: list[dict] = []
    for indicator_id, spec in _EM_MONTHLY_SERIES.items():
        report = spec["report"]
        value_column = spec["value_column"]
        try:
            records = fetch_datacenter(
                client,
                report,
                spec["columns"],
                sort_columns="REPORT_DATE",
                sort_types="-1",
            )
        except EastMoneyDatacenterError as exc:
            # Non-fatal, and deliberately louder than the AkShare path it
            # replaced: a schema rejection here is a column rename worth seeing.
            logger.warning("EastMoney %s (%s) fetch skipped: %s", indicator_id, report, exc)
            continue

        for item in records:
            obs = _parse_series_obs_date(item.get("REPORT_DATE") or item.get("TIME"))
            if obs is None or obs > trade_date:
                continue
            val = item.get(value_column)
            if val is None:
                continue
            try:
                value = float(val)
            except (TypeError, ValueError):
                continue
            rows.append(
                {
                    "indicator_id": indicator_id,
                    "obs_date": obs,
                    "value": value,
                    "frequency": "monthly",
                    "source": "eastmoney",
                }
            )
    return rows


def _social_financing_rows(trade_date: date, *, config=None) -> list[dict]:
    """社融增量 from the PBOC's own statistical tables.

    Read from the publisher rather than a republisher. MOFCOM, which this
    replaced, was two release cycles behind *and* serving a superseded vintage
    (2026-04 as 6245 after the PBOC had revised it to 6238) — so it was not a
    safe backup either, only a quieter way to be wrong. See issue #10.
    """
    # Defaults on, like eastmoney: this is the primary feed for the indicator,
    # not a supplement.
    if config is not None and not config.sources.get("pboc", True):
        return []

    from cn_market_lake.adapters.pboc.social_financing import fetch_social_financing

    return [
        {
            "indicator_id": "social_financing",
            "obs_date": item["obs_date"],
            "value": item["value"],
            "frequency": "monthly",
            "source": "pboc",
        }
        for item in fetch_social_financing(config=config)
        if item["obs_date"] <= trade_date
    ]


def fetch_macro_indicators(
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config=None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)

    rows = _eastmoney_daily(client, trade_date)
    # Daily rates first, so an LPR row already published on the daily report
    # wins over a monthly restatement of the same (indicator_id, obs_date).
    seen = {(r["indicator_id"], r["obs_date"]) for r in rows}
    for item in _eastmoney_monthly(client, trade_date):
        key = (item["indicator_id"], item["obs_date"])
        if key not in seen:
            rows.append(item)
            seen.add(key)
    if owns:
        client.close()

    for item in _social_financing_rows(trade_date, config=config):
        key = (item["indicator_id"], item["obs_date"])
        if key not in seen:
            rows.append(item)
            seen.add(key)

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["indicator_id", "obs_date"], keep="last")
