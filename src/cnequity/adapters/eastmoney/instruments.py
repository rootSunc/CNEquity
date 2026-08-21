"""EastMoney instrument metadata (list dates via push2 clist)."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import polars as pl

from cnequity.adapters.eastmoney.clist import clist_rows_to_symbols, fetch_clist_pages
from cnequity.adapters.eastmoney.common import (
    ALL_A_FS,
    ETF_CLIST_FS,
    symbol_from_clist,
    symbol_from_clist_etf,
)
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.config import Config

logger = logging.getLogger(__name__)


def _parse_list_date(value: object) -> date | None:
    if value is None or value == "" or value == "-":
        return None
    try:
        num = int(float(value))
    except (TypeError, ValueError, OverflowError):
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            return None
    if num > 1_000_000_000_000:
        try:
            return datetime.fromtimestamp(num / 1000, tz=timezone.utc).date()
        except (OverflowError, OSError, ValueError):
            return None
    text = str(num)
    if len(text) == 8:
        try:
            return date.fromisoformat(f"{text[:4]}-{text[4:6]}-{text[6:8]}")
        except ValueError:
            return None
    return None


def fetch_list_date_map(
    *, client: EastMoneyClient | None = None, config: Config | None = None
) -> dict[str, date]:
    """Return symbol -> list_date for A-shares and ETFs from EastMoney clist."""
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    try:
        out: dict[str, date] = {}
        # The A-share board is the primary stock source and stays fail-loud so
        # a broken snapshot is never mistaken for "no stock list dates exist".
        rows = fetch_clist_pages(client, fields="f12,f13,f26", fs=ALL_A_FS)
        for sym, item in clist_rows_to_symbols(rows, symbol_resolver=symbol_from_clist):
            list_date = _parse_list_date(item.get("f26"))
            if list_date is not None:
                out[sym] = list_date
        # The ETF/LOF board is secondary enrichment; a failure there must not
        # discard the stock dates we already collected.
        try:
            etf_rows = fetch_clist_pages(client, fields="f12,f13,f26", fs=ETF_CLIST_FS)
        except Exception as exc:  # noqa: BLE001 — enrichment is best-effort
            logger.warning("EastMoney clist ETF/LOF list_date fetch failed: %s", exc)
            return out
        for sym, item in clist_rows_to_symbols(etf_rows, symbol_resolver=symbol_from_clist_etf):
            list_date = _parse_list_date(item.get("f26"))
            if list_date is not None:
                out[sym] = list_date
        return out
    finally:
        if owns:
            client.close()


def enrich_instrument_list_dates(config: Config, df: pl.DataFrame) -> pl.DataFrame:
    """Fill null list_date on *df* from EastMoney when enabled."""
    if df.is_empty() or not config.sources.get("eastmoney", True):
        return df
    if df.filter(pl.col("list_date").is_null()).is_empty():
        return df

    try:
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
