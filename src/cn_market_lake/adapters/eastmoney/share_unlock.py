"""EastMoney share-unlock (限售解禁) schedule."""

from __future__ import annotations

import logging
from datetime import date, timedelta

import polars as pl

from cn_market_lake.adapters.eastmoney.common import exchange_from_datacenter, symbol_from_em
from cn_market_lake.adapters.eastmoney.datacenter import fetch_datacenter
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient

logger = logging.getLogger(__name__)

_UNLOCK_REPORT = "RPT_LIFT_STAGE"
_UNLOCK_COLUMNS = (
    "SECURITY_CODE,FREE_DATE,ABLE_FREE_SHARES,FREE_RATIO,FREE_SHARES_TYPE,CURRENT_FREE_SHARES"
)


def _free_date(item: dict) -> date | None:
    raw = item.get("FREE_DATE")
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def fetch_share_unlock_schedule(
    trade_date: date,
    *,
    horizon_days: int = 180,
    client: EastMoneyClient | None = None,
    max_retries: int = 3,
    retry_backoff_seconds: float = 5.0,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient()

    # No FREE_DATE range predicate. EastMoney's datacenter rejects range
    # comparisons on date columns outright — "参数预处理错误:
    # org.antlr.v4.runtime.InputMismatchException (code=9501)" — which took this
    # step from working to failing every run with no code change on our side.
    # Equality still parses, but that is one request per day across the horizon.
    #
    # So: page the report newest-first and stop as soon as a page ends before
    # the window does. The report spans 2010..2035 in 63 pages of 500; the
    # 180-day window lives in the first ~7 of them descending, where reading it
    # ascending would walk all 63 to reach the same rows.
    start = trade_date
    end = trade_date + timedelta(days=horizon_days)

    # This walks a market-wide report (63 pages of 500), and the backfill's
    # ~40 strides each restart that walk from page 1 — so a transient EastMoney
    # timeout on any one page is common at this volume. Measured: three
    # failures across three backfill attempts, on three different pages
    # (8, 27, 28), not one specific broken request. The default 3 retries / 5s
    # backoff is sized for the single-page daily call; the backfill caller
    # passes a more patient budget.
    def _page_is_past_window(batch: list[dict]) -> bool:
        for item in reversed(batch):
            parsed = _free_date(item)
            if parsed is not None:
                return parsed < start
        return False

    raw = fetch_datacenter(
        client,
        _UNLOCK_REPORT,
        _UNLOCK_COLUMNS,
        sort_columns="FREE_DATE",
        sort_types="-1",
        stop_after=_page_is_past_window,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )
    if owns:
        client.close()

    rows: list[dict] = []
    for item in raw:
        code = str(item.get("SECURITY_CODE", "")).zfill(6)
        exch = exchange_from_datacenter(item)
        market_id = 1 if exch == "SH" else (2 if exch == "BJ" else 0)
        sym = symbol_from_em(code, market_id)
        if not sym:
            continue
        unlock_date = _free_date(item)
        if unlock_date is None or not (start <= unlock_date <= end):
            continue
        shares = item.get("ABLE_FREE_SHARES")
        if shares is None:
            shares = item.get("CURRENT_FREE_SHARES")
        rows.append(
            {
                "symbol": sym,
                "unlock_date": unlock_date,
                "unlock_shares": float(shares or 0),
                "unlock_ratio": float(item.get("FREE_RATIO") or 0),
                "unlock_type": str(item.get("FREE_SHARES_TYPE") or ""),
            }
        )

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "unlock_date"], keep="last")
