from __future__ import annotations

import json
import logging
import re
from typing import Literal

import httpx
import polars as pl

from cnequity.domain.symbols import parse_symbol

logger = logging.getLogger(__name__)

AdjustType = Literal["qfq", "hfq"]

_SINA_URLS = {
    "qfq": "https://finance.sina.com.cn/realstock/company/{symbol}/qfq.js",
    "hfq": "https://finance.sina.com.cn/realstock/company/{symbol}/hfq.js",
}
_SINA_FACTOR_COLS = {"qfq": "qfq_factor", "hfq": "hfq_factor"}


def to_sina_symbol(symbol: str) -> str:
    info = parse_symbol(symbol)
    prefix = {"SH": "sh", "SZ": "sz", "BJ": "bj"}.get(info.exchange, info.exchange.lower())
    return f"{prefix}{info.code}"


def _normalize_sina_rows(data: list) -> list[dict]:
    """Normalize Sina factor rows into ashare-lake factor columns.

    Stocks carry their factor in ``f``. ETFs/LOFs return ``f=1`` and put their
    factor in ``s``; the caller converts fund qfq ``s`` as ``1/s`` and uses fund
    hfq ``s`` directly as the multiplier.
    """
    rows: list[dict] = []
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            logger.warning("Sina adj factors: skipping non-object row %s", index)
            continue
        row = dict(item)
        if "d" in row and "date" not in row:
            row["date"] = row.pop("d")
        fund_factor = row.pop("s", None)
        price_factor = row.pop("f", None)
        if fund_factor is not None:
            row.setdefault("qfq_factor", fund_factor)
            row.setdefault("hfq_factor", fund_factor)
            row["_factor_mode"] = "fund"
        elif price_factor is not None:
            row.setdefault("qfq_factor", price_factor)
            row.setdefault("hfq_factor", price_factor)
            row["_factor_mode"] = "stock"
        rows.append(row)
    return rows


def _parse_sina_factor_payload(text: str) -> list[dict]:
    match = re.search(r"=\s*(\{.*\})", text, re.DOTALL)
    if not match:
        raise ValueError("Sina adj factor response missing JSON payload")
    payload = json.loads(match.group(1))
    if not isinstance(payload, dict):
        raise ValueError("Sina adj factor response payload is not an object")
    data = payload.get("data")
    if not data:
        raise ValueError("Sina adj factor response has empty data")
    if not isinstance(data, list):
        raise ValueError("Sina adj factor response data is not a list")
    rows = _normalize_sina_rows(data)
    if not rows:
        raise ValueError("Sina adj factor response has no valid rows")
    return rows


def fetch_adj_factor_series(
    symbol: str,
    adjust_type: AdjustType = "qfq",
    *,
    client: httpx.Client | None = None,
) -> pl.DataFrame:
    """Fetch external cumulative adjustment factors from Sina Finance.

    Sina stock qfq_factor is a divisor (adj_price = raw / qfq_factor).
    Sina stock hfq_factor is a multiplier (adj_price = raw * hfq_factor).
    Returned ``factor`` column matches cnequity schema: adj_price = raw * factor.

    ETFs/LOFs encode their factor in Sina's ``s`` field. For fund hfq rows,
    ``s`` is already a multiplier: ``adj_price = raw * s``. For fund qfq rows,
    ``s`` is a divisor: ``adj_price = raw / s``, so the returned qfq factor is
    ``1/s``.
    """
    sina_symbol = to_sina_symbol(symbol)
    url = _SINA_URLS[adjust_type].format(symbol=sina_symbol)
    owns_client = client is None
    if client is None:
        client = httpx.Client(timeout=20.0)

    try:
        response = client.get(url)
        response.raise_for_status()
        rows = _parse_sina_factor_payload(response.text)
        fund_mode = any(row.get("_factor_mode") == "fund" for row in rows)
    finally:
        if owns_client:
            client.close()

    if not rows:
        return pl.DataFrame(schema={"trade_date": pl.Date, "factor": pl.Float64})

    sina_col = _SINA_FACTOR_COLS[adjust_type]
    df = pl.DataFrame(rows).rename({"date": "trade_date", sina_col: "sina_factor"})
    df = df.with_columns(pl.col("trade_date").str.to_date("%Y-%m-%d", strict=False)).filter(
        pl.col("trade_date").is_not_null()
    )
    if df.is_empty():
        raise ValueError(f"Sina {adjust_type} factor series contains no valid trade dates")
    df = df.with_columns(pl.col("sina_factor").cast(pl.Float64))
    invalid = df.filter(
        pl.col("sina_factor").is_null()
        | ~pl.col("sina_factor").is_finite()
        | (pl.col("sina_factor") <= 0)
    )
    if not invalid.is_empty():
        raise ValueError(
            f"Sina {adjust_type} factor series contains "
            f"{invalid.height} non-positive or non-finite value(s)"
        )
    if fund_mode and adjust_type == "qfq":
        df = df.with_columns((1.0 / pl.col("sina_factor")).alias("factor"))
    elif fund_mode:
        df = df.with_columns(pl.col("sina_factor").alias("factor"))
    elif adjust_type == "qfq":
        df = df.with_columns((1.0 / pl.col("sina_factor")).alias("factor"))
    else:
        df = df.with_columns(pl.col("sina_factor").alias("factor"))
    return (
        df.select(["trade_date", "factor"])
        .unique(subset=["trade_date"], keep="last")
        .sort("trade_date")
    )
