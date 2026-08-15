"""EastMoney institutional holdings (季报 batch, keyed by REPORT_DATE).

``RPT_MAIN_ORGHOLD`` has no ``NOTICE_DATE`` column, so this fetches by quarterly
``REPORT_DATE``: daily runs refresh the latest quarter; backfill walks every
quarter-end from 2016.
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cn_market_lake.adapters.eastmoney.common import symbol_from_secucode
from cn_market_lake.adapters.eastmoney.datacenter import fetch_datacenter
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
from cn_market_lake.config import Config

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
    if not raw:
        return None
    text = str(raw)[:10]
    if len(text) < 7:
        return text
    year = text[:4]
    month = int(text[5:7])
    q = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}.get(month, "Q4")
    return f"{year}{q}"


def _normalize_holder_type(raw: str | None) -> str:
    text = str(raw or "").strip()
    for key, value in _HOLDER_TYPE_MAP.items():
        if key in text:
            return value
    return text.lower().replace(" ", "_") if text else "other"


def _quarter_end_dates(trade_date: date) -> list[str]:
    out: list[str] = []
    for year in range(_BACKFILL_START_YEAR, trade_date.year + 1):
        for mm, dd in _QUARTER_END_MMDD:
            ds = f"{year}-{mm}-{dd}"
            if date.fromisoformat(ds) <= trade_date:
                out.append(ds)
    return sorted(out, reverse=True)


def _num(value: object, default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


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

    periods = _quarter_end_dates(trade_date)
    if not backfill:
        periods = periods[:2]

    rows: list[dict] = []
    try:
        for period in periods:
            if config is not None:
                config.rate_limit("eastmoney")
            raw = fetch_datacenter(
                client,
                _HOLD_REPORT,
                _HOLD_COLUMNS,
                filter_expr=f"(REPORT_DATE='{period}')",
            )
            for item in raw:
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
                        "holding_shares": _num(item.get("HOULD_NUM")),
                        "holding_ratio": _num(item.get("TOTALSHARES_RATIO")),
                        "holding_mv": _num(item.get("HOLD_VALUE")),
                    }
                )
    finally:
        if owns:
            client.close()

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "holder_type", "report_period"], keep="last")
