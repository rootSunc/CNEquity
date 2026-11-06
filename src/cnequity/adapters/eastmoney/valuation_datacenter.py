"""Valuation from EastMoney's datacenter report ``RPT_VALUEANALYSIS_DET``.

The same vendor's numbers as the push2 clist snapshot, served from a different
host: ``datacenter-web.eastmoney.com`` answered this IP throughout the 2026-09
push2 ban, and it is keyed by ``TRADE_DATE``, so a missed session can be read
back afterwards instead of being lost with the live snapshot. It carries
Beijing names too, which baostock does not.

Measured 2026-09-26 against the lake's push2 rows for 2026-09-21: ``PB_MRQ``,
``PS_TTM``, ``TOTAL_MARKET_CAP`` and ``NOTLIMITED_MARKETCAP_A`` match push2
f23/f130/f20/f21 exactly. ``PE_TTM`` does **not** match push2 f9 — f9 is the
dynamic (annualised latest-report) P/E, while ``PE_TTM`` equals baostock's
``peTTM`` to the cent (600519: 19.25 both on 2026-09-22). Rows from here are
therefore labelled ``eastmoney_datacenter`` so the two P/E definitions stay
distinguishable in the lake.

One day of the whole market is ~5,600 rows: two requests at pageSize 5000,
which this report honours.
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.common import _to_float, symbol_from_secucode
from cnequity.adapters.eastmoney.datacenter import fetch_datacenter
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.domain.valuation import MV_VENDOR_REPORTED
from cnequity.domain.value_rules import apply_value_rules

logger = logging.getLogger(__name__)

SOURCE = "eastmoney_datacenter"
_REPORT = "RPT_VALUEANALYSIS_DET"
_COLUMNS = "SECUCODE,TRADE_DATE,PE_TTM,PB_MRQ,PS_TTM,TOTAL_MARKET_CAP,NOTLIMITED_MARKETCAP_A"
_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "pe_ttm": pl.Float64,
    "pb": pl.Float64,
    "ps_ttm": pl.Float64,
    "total_mv": pl.Float64,
    "float_mv": pl.Float64,
    "total_mv_basis": pl.Utf8,
    "float_mv_basis": pl.Utf8,
}


def fetch_valuation_datacenter(
    start: date,
    end: date | None = None,
    *,
    client: EastMoneyClient | None = None,
    config=None,
) -> pl.DataFrame:
    """Every A-share's valuation for each session in ``[start, end]``.

    Rows dated outside the window, or for codes that are not A-shares, are
    dropped; a row without a date is an error, not something to guess about.
    """
    end = end or start
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    try:
        raw = fetch_datacenter(
            client,
            _REPORT,
            _COLUMNS,
            filter_expr=f"(TRADE_DATE>='{start.isoformat()}')(TRADE_DATE<='{end.isoformat()}')",
            page_size=5000,
            trust_page_size=True,
            sort_columns="TRADE_DATE,SECUCODE",
            sort_types="1,1",
        )
    finally:
        if owns:
            client.close()

    rows: list[dict] = []
    skipped = 0
    for item in raw:
        symbol = symbol_from_secucode(item.get("SECUCODE"))
        if symbol is None:
            skipped += 1
            continue
        text = str(item.get("TRADE_DATE") or "")[:10]
        if not text:
            raise RuntimeError(f"{_REPORT}: row for {symbol} has no TRADE_DATE")
        day = date.fromisoformat(text)
        if not start <= day <= end:
            continue
        rows.append(
            {
                "symbol": symbol,
                "trade_date": day,
                "pe_ttm": _to_float(item.get("PE_TTM")),
                "pb": _to_float(item.get("PB_MRQ")),
                "ps_ttm": _to_float(item.get("PS_TTM")),
                "total_mv": _to_float(item.get("TOTAL_MARKET_CAP")),
                "float_mv": _to_float(item.get("NOTLIMITED_MARKETCAP_A")),
                "total_mv_basis": MV_VENDOR_REPORTED,
                "float_mv_basis": MV_VENDOR_REPORTED,
            }
        )
    if skipped:
        logger.info("%s: skipped %d non-A-share row(s)", _REPORT, skipped)
    if not rows:
        return pl.DataFrame(schema=_SCHEMA)
    return apply_value_rules(
        pl.DataFrame(rows, schema=_SCHEMA).unique(
            subset=["symbol", "trade_date"], keep="last", maintain_order=True
        ),
        "valuation_metrics",
    )
