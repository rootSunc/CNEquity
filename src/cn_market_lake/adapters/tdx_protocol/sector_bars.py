"""TDX sector/board index bars (88xxxx) — optional offline helper.

Not used by ``sector_bars`` ingestion (pure EastMoney). Kept for ad-hoc probes
and experimental routing via ``cml derive sector_routing``.
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cn_market_lake.adapters.tdx_protocol.bars import fetch_bars_paginated
from cn_market_lake.adapters.tdx_protocol.client import TdxSourceError, _quotes_client
from cn_market_lake.adapters.tdx_protocol.session import TDX_SESSION_LOCK, close_quotes_client
from cn_market_lake.config import Config

logger = logging.getLogger(__name__)


def _sym_for_tdx_code(tdx_code: str) -> str:
    return f"{str(tdx_code).strip()}.SH"


def _rows_with_change_pct(rows: list[dict]) -> list[dict]:
    if not rows:
        return []
    df = pl.DataFrame(rows).sort("trade_date")
    closes = df["close"].to_list()
    change: list[float | None] = [None]
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        cur = closes[i]
        change.append((cur / prev - 1.0) * 100.0 if prev and prev > 0 else 0.0)
    df = df.with_columns(pl.Series("change_pct", change))
    return df.to_dicts()


def fetch_sector_index_bars(
    *,
    sector_code: str,
    sector_name: str,
    board_type: str,
    tdx_code: str,
    start: date,
    end: date,
    config: Config,
    backfill: bool = False,
) -> list[dict]:
    """Daily OHLC for one EastMoney BK via its mapped TDX 88xxxx index."""
    sym = _sym_for_tdx_code(tdx_code)
    rate_limit = config.tdx_rate_limit_spec()

    with TDX_SESSION_LOCK:
        client = _quotes_client(config)
        try:
            raw = fetch_bars_paginated(
                client,
                sym,
                start,
                end,
                rate_limit=rate_limit,
                backfill=backfill,
                is_index=True,
            )
        finally:
            close_quotes_client(client)

    if not raw:
        return []

    rows: list[dict] = []
    for r in raw:
        rows.append(
            {
                "sector_code": sector_code,
                "sector_name": sector_name,
                "board_type": board_type,
                "trade_date": r["trade_date"],
                "open": r["open"],
                "high": r["high"],
                "low": r["low"],
                "close": r["close"],
                "volume": r["volume"],
                "amount": r["amount"],
            }
        )
    return _rows_with_change_pct(rows)


def fetch_sector_index_bars_batch(
    routing: pl.DataFrame,
    start: date,
    end: date,
    *,
    config: Config,
    skip_sectors: set[str] | None = None,
    backfill: bool = False,
) -> tuple[pl.DataFrame, list[str], list[str]]:
    """Fetch TDX-routed boards. Returns ``(frame, failed_codes, succeeded_codes)``."""
    skip = skip_sectors or set()
    tdx_rows = routing.filter(pl.col("ohlc_source") == "tdx_protocol")
    if skip:
        tdx_rows = tdx_rows.filter(~pl.col("sector_code").is_in(list(skip)))

    all_rows: list[dict] = []
    failed: list[str] = []
    succeeded: list[str] = []

    for row in tdx_rows.iter_rows(named=True):
        code = row["sector_code"]
        tdx_code = row.get("tdx_code")
        if not tdx_code:
            failed.append(code)
            continue
        try:
            bars = fetch_sector_index_bars(
                sector_code=code,
                sector_name=row["sector_name"],
                board_type=row["board_type"],
                tdx_code=str(tdx_code),
                start=start,
                end=end,
                config=config,
                backfill=backfill,
            )
        except (TdxSourceError, Exception) as exc:
            logger.warning("TDX sector bars %s (%s) failed: %s", code, tdx_code, exc)
            failed.append(code)
            continue
        if not bars:
            failed.append(code)
            continue
        all_rows.extend(bars)
        succeeded.append(code)

    return (pl.DataFrame(all_rows) if all_rows else pl.DataFrame(), failed, succeeded)


def fetch_sector_index_snapshot(
    *,
    sector_code: str,
    sector_name: str,
    board_type: str,
    tdx_code: str,
    trade_date: date,
    config: Config,
) -> dict | None:
    """Latest TDX daily bar on or before ``trade_date``."""
    start = trade_date
    bars = fetch_sector_index_bars(
        sector_code=sector_code,
        sector_name=sector_name,
        board_type=board_type,
        tdx_code=tdx_code,
        start=start,
        end=trade_date,
        config=config,
        backfill=False,
    )
    if not bars:
        return None
    hit = [b for b in bars if b["trade_date"] == trade_date]
    return hit[-1] if hit else bars[-1]
