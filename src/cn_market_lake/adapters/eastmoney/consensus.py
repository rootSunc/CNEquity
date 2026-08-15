"""EastMoney analyst consensus / earnings forecast (current-snapshot).

EastMoney retired the dated ``RPTA_WEB_RES_PROFIT`` report. ``RPT_WEB_RESPREDICT``
is a live per-stock consensus snapshot (no date filter), so this dataset is
snapshot semantics: stamped with ``forecast_date=trade_date``, no history.
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

_CONSENSUS_REPORT = "RPT_WEB_RESPREDICT"
_CONSENSUS_COLUMNS = (
    "SECUCODE,SECURITY_CODE,RATING_ORG_NUM,RATING_BUY_NUM,RATING_ADD_NUM,"
    "RATING_NEUTRAL_NUM,RATING_REDUCE_NUM,RATING_SALE_NUM,YEAR1,EPS1,"
    "DEC_AIMPRICEMAX,DEC_AIMPRICEMIN"
)

# rating bucket -> canonical label, in preference order for ties
_RATING_BUCKETS = (
    ("RATING_BUY_NUM", "buy"),
    ("RATING_ADD_NUM", "overweight"),
    ("RATING_NEUTRAL_NUM", "neutral"),
    ("RATING_REDUCE_NUM", "underweight"),
    ("RATING_SALE_NUM", "sell"),
)


def _num(value: object, default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _dominant_rating(item: dict) -> str:
    best_label = ""
    best_count = -1.0
    for field, label in _RATING_BUCKETS:
        count = _num(item.get(field), 0.0)
        if count > best_count:
            best_count = count
            best_label = label
    return best_label if best_count > 0 else ""


def fetch_analyst_consensus(
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    """Fetch the current analyst consensus snapshot, stamped with *trade_date*."""
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)

    try:
        if config is not None:
            config.rate_limit("eastmoney")
        raw = fetch_datacenter(client, _CONSENSUS_REPORT, _CONSENSUS_COLUMNS)
    finally:
        if owns:
            client.close()

    rows: list[dict] = []
    for item in raw:
        sym = symbol_from_secucode(item.get("SECUCODE"))
        if not sym:
            continue
        year = item.get("YEAR1")
        pmax = item.get("DEC_AIMPRICEMAX")
        pmin = item.get("DEC_AIMPRICEMIN")
        if pmax is not None and pmin is not None:
            target = (_num(pmax) + _num(pmin)) / 2.0
        else:
            target = _num(pmax if pmax is not None else pmin)
        rows.append(
            {
                "symbol": sym,
                "forecast_date": trade_date,
                "forecast_year": int(year) if year is not None else None,
                "eps_forecast": _num(item.get("EPS1")),
                "pe_forecast": 0.0,  # not exposed by RPT_WEB_RESPREDICT
                "target_price": target,
                "rating": _dominant_rating(item),
                "analyst_count": int(_num(item.get("RATING_ORG_NUM"))),
            }
        )

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "forecast_date"], keep="last")
