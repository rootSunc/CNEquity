from __future__ import annotations

import json
import logging
import re
from typing import Literal

import httpx
import polars as pl

from cn_market_lake.domain.symbols import parse_symbol

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
    rows: list[dict] = []
    for item in data:
        row = dict(item)
        if "d" in row and "date" not in row:
            row["date"] = row.pop("d")
        factor_val = row.pop("f", None)
        if factor_val is not None:
            row.setdefault("qfq_factor", factor_val)
            row.setdefault("hfq_factor", factor_val)
        rows.append(row)
    return rows


def _parse_sina_factor_payload(text: str) -> list[dict]:
    match = re.search(r"=\s*(\{.*\})", text, re.DOTALL)
    if not match:
        raise ValueError("Sina adj factor response missing JSON payload")
    payload = json.loads(match.group(1))
    data = payload.get("data")
    if not data:
        raise ValueError("Sina adj factor response has empty data")
    return _normalize_sina_rows(data)


def fetch_adj_factor_series(
    symbol: str,
    adjust_type: AdjustType = "qfq",
    *,
    client: httpx.Client | None = None,
) -> pl.DataFrame:
    """Fetch external cumulative adjustment factors from Sina Finance.

    Sina qfq_factor is a divisor (adj_price = raw / qfq_factor).
    Sina hfq_factor is a multiplier (adj_price = raw * hfq_factor).
    Returned ``factor`` column matches cn-market-lake schema: adj_price = raw * factor.
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
    finally:
        if owns_client:
            client.close()

    if not rows:
        return pl.DataFrame(schema={"trade_date": pl.Date, "factor": pl.Float64})

    sina_col = _SINA_FACTOR_COLS[adjust_type]
    df = pl.DataFrame(rows).rename({"date": "trade_date", sina_col: "sina_factor"})
    df = df.with_columns(pl.col("trade_date").str.to_date("%Y-%m-%d"))
    df = df.with_columns(pl.col("sina_factor").cast(pl.Float64))
    if adjust_type == "qfq":
        df = df.with_columns((1.0 / pl.col("sina_factor")).alias("factor"))
    else:
        df = df.with_columns(pl.col("sina_factor").alias("factor"))
    return df.select(["trade_date", "factor"]).sort("trade_date")
