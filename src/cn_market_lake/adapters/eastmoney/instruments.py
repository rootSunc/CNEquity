"""EastMoney instrument metadata (list dates via push2 clist)."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import polars as pl

from cn_market_lake.adapters.eastmoney.clist import fetch_clist_pages
from cn_market_lake.adapters.eastmoney.common import symbol_from_em
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
from cn_market_lake.config import Config

logger = logging.getLogger(__name__)


def _parse_list_date(value: object) -> date | None:
    if value is None or value == "" or value == "-":
        return None
    try:
        num = int(float(value))
    except (TypeError, ValueError):
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            return None
    if num > 1_000_000_000_000:
        return datetime.fromtimestamp(num / 1000, tz=timezone.utc).date()
    text = str(num)
    if len(text) == 8:
        return date.fromisoformat(f"{text[:4]}-{text[4:6]}-{text[6:8]}")
    return None


def fetch_list_date_map(
    *, client: EastMoneyClient | None = None, config: Config | None = None
) -> dict[str, date]:
    """Return symbol -> list_date for all A-shares from EastMoney clist."""
    client = client or EastMoneyClient(config=config)
    rows = fetch_clist_pages(client, fields="f12,f13,f26")
    out: dict[str, date] = {}
    for item in rows:
        sym = symbol_from_em(str(item.get("f12", "")), int(item.get("f13") or 0))
        if not sym:
            continue
        list_date = _parse_list_date(item.get("f26"))
        if list_date is not None:
            out[sym] = list_date
    return out


def enrich_instrument_list_dates(config: Config, df: pl.DataFrame) -> pl.DataFrame:
    """Fill null list_date on *df* from EastMoney when enabled."""
    if df.is_empty() or not config.sources.get("eastmoney", True):
        return df
    if df.filter(pl.col("list_date").is_null()).is_empty():
        return df

    try:
        config.rate_limit("eastmoney")
        date_map = fetch_list_date_map(config=config)
    except Exception as exc:
        logger.warning("EastMoney instrument list_date enrichment failed: %s", exc)
        return df

    if not date_map:
        return df

    enrich = pl.DataFrame(
        {
            "symbol": list(date_map.keys()),
            "em_list_date": list(date_map.values()),
        }
    )
    merged = df.join(enrich, on="symbol", how="left")
    return merged.with_columns(
        pl.coalesce(pl.col("list_date"), pl.col("em_list_date")).alias("list_date")
    ).drop("em_list_date")
