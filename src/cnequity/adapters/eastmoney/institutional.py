"""EastMoney institutional holdings (季报 batch, keyed by REPORT_DATE).

``RPT_MAIN_ORGHOLD`` has no ``NOTICE_DATE`` column, so this fetches by quarterly
``REPORT_DATE``: daily runs refresh the latest quarter; backfill walks every
quarter-end from 2016.
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.common import (
    _to_float,
    report_period_from_date,
    symbol_from_secucode,
)
from cnequity.adapters.eastmoney.datacenter import fetch_datacenter
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient, rate_limit_if_unconfigured
from cnequity.config import Config
from cnequity.run_scope import backfill_of

logger = logging.getLogger(__name__)

_HOLD_REPORT = "RPT_MAIN_ORGHOLD"
_HOLD_COLUMNS = (
    "SECURITY_CODE,SECUCODE,REPORT_DATE,ORG_TYPE_NAME,HOULD_NUM,HOLD_VALUE,TOTALSHARES_RATIO"
)

# Measured 2026-08: RPT_MAIN_ORGHOLD still returns real rows at 2001-12-31
# (1,276) — 2016 was a guess, not a probed floor.
_BACKFILL_START_YEAR = 2001
_QUARTER_END_MMDD = (("03", "31"), ("06", "30"), ("09", "30"), ("12", "31"))

_HOLDER_TYPE_MAP = {
    "汇总": "summary",
    "基金": "fund",
    "QFII": "qfii",
    "社保": "social_security",
    "保险": "insurance",
    "券商": "broker",
    "信托": "trust",
    "银行": "bank",
    "一般法人": "corporate",
}


def _report_period(raw: str | None) -> str | None:
    return report_period_from_date(raw)


def _normalize_holder_type(raw: str | None) -> str:
    text = str(raw or "").strip()
    for key, value in _HOLDER_TYPE_MAP.items():
        if key in text:
            return value
    return text.lower().replace(" ", "_") if text else "other"


def _quarter_end_dates(
    trade_date: date,
    *,
    start: date | None = None,
    end: date | None = None,
) -> list[str]:
    """Quarter ends in the requested backfill window, newest first."""
    lower = date(_BACKFILL_START_YEAR, 1, 1)
    if start is not None:
        lower = max(lower, start)
    upper = trade_date if end is None else min(trade_date, end)
    if lower > upper:
        return []

    out: list[str] = []
    for year in range(lower.year, upper.year + 1):
        for mm, dd in _QUARTER_END_MMDD:
            ds = f"{year}-{mm}-{dd}"
            if lower <= date.fromisoformat(ds) <= upper:
                out.append(ds)
    return sorted(out, reverse=True)


def fetch_institutional_holdings(
    trade_date: date,
    *,
    backfill: bool = False,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    """Fetch institutional holdings by quarterly ``REPORT_DATE``.

    ``backfill=False``: the two most recent quarter-ends (the just-ended
    quarter fills in over ~2 months, so keep the last complete one fresh too).
    ``backfill=True``: every quarter-end from 2016 through *trade_date*.
    """
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)

    range_start = backfill_of(config).start if backfill else None
    range_end = backfill_of(config).end if backfill else None
    periods = _quarter_end_dates(trade_date, start=range_start, end=range_end)
    if not backfill:
        periods = periods[:2]

    rows: list[dict] = []
    try:
        for period in periods:
            rate_limit_if_unconfigured(client, config)
            raw = fetch_datacenter(
                client,
                _HOLD_REPORT,
                _HOLD_COLUMNS,
                filter_expr=f"(REPORT_DATE='{period}')",
            )
            expected_period = _report_period(period)
            period_rows = [
                item for item in raw if _report_period(item.get("REPORT_DATE")) == expected_period
            ]
            if raw and not period_rows:
                raise RuntimeError(
                    f"EastMoney institutional holdings response contains no "
                    f"REPORT_DATE row for {period}"
                )
            if len(period_rows) != len(raw):
                logger.warning(
                    "EastMoney institutional holdings dropped %d row(s) outside "
                    "requested REPORT_DATE %s",
                    len(raw) - len(period_rows),
                    period,
                )
            for item in period_rows:
                sym = symbol_from_secucode(item.get("SECUCODE"))
                if not sym:
                    continue
                report_period = _report_period(item.get("REPORT_DATE"))
                if not report_period:
                    continue
                rows.append(
                    {
                        "symbol": sym,
                        "holder_type": _normalize_holder_type(item.get("ORG_TYPE_NAME")),
                        "report_period": report_period,
                        "holding_shares": _to_float(item.get("HOULD_NUM")),
                        "holding_ratio": _to_float(item.get("TOTALSHARES_RATIO")),
                        "holding_mv": _to_float(item.get("HOLD_VALUE")),
                    }
                )
    finally:
        if owns:
            client.close()

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "holder_type", "report_period"], keep="last")
