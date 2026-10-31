"""Macro indicators — daily rates and monthly series, each from its own publisher.

Rows carry their own ``source`` rather than taking the step's blanket value, so
a curated row always names the feed it came from.

Every series except 社融 and 离岸人民币 comes from EastMoney's datacenter; 社融 comes from the
PBOC, which publishes it. AkShare used to supply the monthly block, but its PMI and 货币供应量
wrappers request the *same* ``datacenter-web.eastmoney.com`` endpoint this module
already talks to, so going direct removes a parsing layer without changing the
publisher — and picks up the project's own retry, throttle and TLS handling.
See issue #3.

The daily run asks each rate report for the run day only. The backfill reads
the two daily rates over its whole window instead (``fetch_daily_rates_range``).
离岸人民币 is the Sina forex day-kline close (``fx_susdcnh``). A failure there
is recorded and does not drop the rates. push2his is not consulted.

Monthly observations are stamped at month end. EastMoney reports them at month
*start* (``REPORT_DATE = 2026-07-01``), so they are converted; changing this
convention would double-write every month already in curated under a second key.
"""

from __future__ import annotations

import calendar
import logging
import math
import re
from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.datacenter import EastMoneyDatacenterError, fetch_datacenter
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.adapters.sina.usdcnh import fetch_usdcnh_closes

logger = logging.getLogger(__name__)

_TREASURY_REPORT = "RPTA_WEB_TREASURYYIELD"
_TREASURY_COLUMNS = "SOLAR_DATE,EMM00166466,EMG00001310"
_SHIBOR_REPORT = "RPT_IMP_INTRESTRATEN"
_SHIBOR_COLUMNS = "REPORT_DATE,IR_RATE"
_SHIBOR_FILTER = '(MARKET_CODE="001")(CURRENCY_CODE="CNY")(INDICATOR_ID="203")'
_LPR_REPORT = "RPTA_WEB_RATE"
_LPR_COLUMNS = "TRADE_DATE,LPR1Y,LPR5Y"

# The daily series a backfill reads as a window instead of one day at a time.
# Both reports take a `(COL>='…')(COL<='…')` range and page it — measured
# 2026-09-25: inclusive at both ends, 2016..2026 in six 500-row pages. LPR stays
# per-day: RPTA_WEB_RATE's LPR1Y is the old daily 贷款基础利率 until 2019-08 and
# the reformed monthly LPR after it, so a range would file a different
# benchmark under the same indicator.
#   (indicator_id, report, columns, base filter, date column, value column)
_DAILY_RANGE_SERIES = (
    ("cnbond_yield_10y", _TREASURY_REPORT, _TREASURY_COLUMNS, "", "SOLAR_DATE", "EMM00166466"),
    ("shibor_3m", _SHIBOR_REPORT, _SHIBOR_COLUMNS, _SHIBOR_FILTER, "REPORT_DATE", "IR_RATE"),
)

#: First published value of each daily series, measured 2026-09-25. The
#: treasury report has rows from 1990-12-19, but its 10Y column is null until
#: 2002-01-04; Shibor 3M starts with Shibor itself on 2006-10-08.
DAILY_SERIES_FIRST_OBS = {
    "cnbond_yield_10y": date(2002, 1, 4),
    "shibor_3m": date(2006, 10, 8),
}

_ROW_SCHEMA = {
    "indicator_id": pl.Utf8,
    "obs_date": pl.Date,
    "value": pl.Float64,
    "frequency": pl.Utf8,
    "source": pl.Utf8,
}

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
        # CURRENCY_SAME is M1 year-on-year change, read from the same response.
        "value_column": "BASIC_CURRENCY_SAME",
        "extra_columns": (("m1_yoy", "CURRENCY_SAME"),),
    },
    "cpi_yoy": {
        "report": "RPT_ECONOMY_CPI",
        "columns": "REPORT_DATE,TIME,NATIONAL_SAME,NATIONAL_BASE,NATIONAL_SEQUENTIAL",
        "value_column": "NATIONAL_SAME",
    },
    "ppi_yoy": {
        "report": "RPT_ECONOMY_PPI",
        "columns": "REPORT_DATE,TIME,BASE,BASE_SAME,BASE_ACCUMULATE",
        "value_column": "BASE_SAME",
    },
}


def _finite_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_obs_date(value: object) -> date | None:
    if value is None:
        return None
    text = str(value)[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


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


def _capture_series(unit: str, fetch, failures, completed):
    try:
        rows = fetch()
    except Exception as exc:
        if failures is None:
            raise
        failures.append((unit, str(exc)))
        return []
    if completed is not None:
        completed.add(unit)
    return rows


def _eastmoney_daily(
    client: EastMoneyClient, trade_date: date, *, config=None, failures=None, completed=None
) -> list[dict]:
    ds = trade_date.isoformat()
    rows: list[dict] = []

    treasury = _capture_series(
        "eastmoney:cnbond_yield_10y",
        lambda: fetch_datacenter(
            client, _TREASURY_REPORT, _TREASURY_COLUMNS, filter_expr=f"(SOLAR_DATE='{ds}')"
        ),
        failures,
        completed,
    )
    for item in treasury:
        obs = _parse_obs_date(item.get("SOLAR_DATE"))
        if obs != trade_date:
            continue
        for ind_id, col_name in (
            ("cnbond_yield_10y", "EMM00166466"),
            ("usbond_yield_10y", "EMG00001310"),
        ):
            val = _finite_float(item.get(col_name))
            if val is not None:
                rows.append(
                    {
                        "indicator_id": ind_id,
                        "obs_date": obs,
                        "value": val,
                        "frequency": "daily",
                        "source": "eastmoney",
                    }
                )

    shibor = _capture_series(
        "eastmoney:shibor_3m",
        lambda: fetch_datacenter(
            client,
            _SHIBOR_REPORT,
            _SHIBOR_COLUMNS,
            filter_expr=f"{_SHIBOR_FILTER}(REPORT_DATE='{ds}')",
        ),
        failures,
        completed,
    )
    for item in shibor:
        val = _finite_float(item.get("IR_RATE"))
        obs = _parse_obs_date(item.get("REPORT_DATE"))
        if val is not None and obs == trade_date:
            rows.append(
                {
                    "indicator_id": "shibor_3m",
                    "obs_date": obs,
                    "value": val,
                    "frequency": "daily",
                    "source": "eastmoney",
                }
            )

    lpr = _capture_series(
        "eastmoney:lpr_1y",
        lambda: fetch_datacenter(
            client, _LPR_REPORT, _LPR_COLUMNS, filter_expr=f"(TRADE_DATE='{ds}')"
        ),
        failures,
        completed,
    )
    for item in lpr:
        obs = _parse_obs_date(item.get("TRADE_DATE"))
        if obs != trade_date:
            continue
        for ind_id, col_name in (("lpr_1y", "LPR1Y"), ("lpr_5y", "LPR5Y")):
            val = _finite_float(item.get(col_name))
            if val is not None:
                rows.append(
                    {
                        "indicator_id": ind_id,
                        "obs_date": obs,
                        "value": val,
                        "frequency": "monthly",
                        "source": "eastmoney",
                    }
                )

    rows.extend(
        _capture_series(
            "sina:usdcnh",
            lambda: fetch_usdcnh_closes(trade_date, trade_date, config=config),
            failures,
            completed,
        )
    )
    return rows


def fetch_daily_rates_range(
    start: date,
    end: date,
    *,
    client: EastMoneyClient | None = None,
    config=None,
) -> pl.DataFrame:
    """cnbond_yield_10y, shibor_3m and usdcnh for every published day in ``[start, end]``.

    The backfill path. The per-day path is only ever asked for the run day, so
    history before the first daily run never landed; here one paged range query
    per report covers the whole window. Days a report
    carries without a value (treasury rows kept for US-only sessions and CN
    holidays) are dropped, as ``_eastmoney_daily`` drops them.

    A row outside the window means the report ignored the range filter, so the
    whole fetch fails rather than filing rows nobody asked for.
    """
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)

    lo, hi = start.isoformat(), end.isoformat()
    rows: list[dict] = []
    try:
        for indicator_id, report, columns, base, date_col, value_col in _DAILY_RANGE_SERIES:
            records = fetch_datacenter(
                client,
                report,
                columns,
                filter_expr=f"{base}({date_col}>='{lo}')({date_col}<='{hi}')",
                sort_columns=date_col,
                sort_types="1",
            )
            for item in records:
                obs = _parse_obs_date(item.get(date_col))
                val = _finite_float(item.get(value_col))
                if obs is None or val is None:
                    continue
                if not start <= obs <= end:
                    raise EastMoneyDatacenterError(
                        f"EastMoney datacenter {report} returned {date_col}={obs.isoformat()} "
                        f"for the range {lo}..{hi}; the range filter was not applied"
                    )
                rows.append(
                    {
                        "indicator_id": indicator_id,
                        "obs_date": obs,
                        "value": val,
                        "frequency": "daily",
                        "source": "eastmoney",
                    }
                )
        try:
            rows.extend(fetch_usdcnh_closes(start, end, config=config))
        except Exception:
            # Sina is a separate host from the rate reports. Losing it must
            # not drop a window of treasury and Shibor that already arrived.
            logger.warning("macro usdcnh range fetch failed", exc_info=True)
    finally:
        if owns:
            client.close()

    return (
        pl.DataFrame(rows, schema=_ROW_SCHEMA)
        .unique(subset=["indicator_id", "obs_date"], keep="last", maintain_order=True)
        .sort(["indicator_id", "obs_date"])
    )


def _eastmoney_monthly(
    client: EastMoneyClient, trade_date: date, *, failures=None, completed=None
) -> list[dict]:
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
        records = _capture_series(
            f"eastmoney:{indicator_id}",
            lambda report=report, columns=spec["columns"]: fetch_datacenter(
                client, report, columns, sort_columns="REPORT_DATE", sort_types="-1"
            ),
            failures,
            completed,
        )

        targets = [(indicator_id, value_column), *spec.get("extra_columns", ())]
        for item in records:
            obs = _parse_series_obs_date(item.get("REPORT_DATE") or item.get("TIME"))
            if obs is None or obs > trade_date:
                continue
            for target_id, col_name in targets:
                val = item.get(col_name)
                if val is None:
                    continue
                value = _finite_float(val)
                if value is None:
                    continue
                rows.append(
                    {
                        "indicator_id": target_id,
                        "obs_date": obs,
                        "value": value,
                        "frequency": "monthly",
                        "source": "eastmoney",
                    }
                )
    return rows


def _social_financing_rows(
    trade_date: date,
    *,
    config=None,
    failures: list[tuple[str, str]] | None = None,
    completed_units: set[str] | None = None,
) -> list[dict]:
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

    from cnequity.adapters.pboc.social_financing import fetch_social_financing

    year_failures: dict[int, str] = {}
    completed_years: set[int] = set()
    series = fetch_social_financing(
        config=config,
        strict=failures is None,
        failures_by_year=year_failures,
        completed_years=completed_years,
    )
    if failures is not None:
        failures.extend(
            (f"pboc:social_financing:{year or 'index'}", reason)
            for year, reason in sorted(year_failures.items())
        )
    if completed_units is not None:
        completed_units.update(f"pboc:social_financing:{year}" for year in completed_years)
        if not year_failures:
            completed_units.add("pboc:social_financing")

    return [
        {
            "indicator_id": "social_financing",
            "obs_date": item["obs_date"],
            "value": item["value"],
            "frequency": "monthly",
            "source": "pboc",
        }
        for item in series
        if item["obs_date"] <= trade_date
    ]


def fetch_macro_indicators(
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config=None,
    failures: list[tuple[str, str]] | None = None,
    completed_units: set[str] | None = None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)

    try:
        rows = _eastmoney_daily(
            client, trade_date, config=config, failures=failures, completed=completed_units
        )
        # Daily rates first, so an LPR row already published on the daily report
        # wins over a monthly restatement of the same (indicator_id, obs_date).
        seen = {(r["indicator_id"], r["obs_date"]) for r in rows}
        for item in _eastmoney_monthly(
            client, trade_date, failures=failures, completed=completed_units
        ):
            key = (item["indicator_id"], item["obs_date"])
            if key not in seen:
                rows.append(item)
                seen.add(key)
    finally:
        if owns:
            client.close()

    try:
        social = _social_financing_rows(
            trade_date, config=config, failures=failures, completed_units=completed_units
        )
    except Exception as exc:
        if failures is None:
            raise
        failures.append(("pboc:social_financing", str(exc)))
        social = []
    for item in social:
        key = (item["indicator_id"], item["obs_date"])
        if key not in seen:
            rows.append(item)
            seen.add(key)

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["indicator_id", "obs_date"], keep="last")
