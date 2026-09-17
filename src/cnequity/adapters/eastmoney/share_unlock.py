"""EastMoney share-unlock (限售解禁) schedule."""

from __future__ import annotations

import logging
from datetime import date, timedelta

import polars as pl

from cnequity.adapters.eastmoney.common import _to_float, exchange_from_datacenter, symbol_from_em
from cnequity.adapters.eastmoney.datacenter import (
    EastMoneyDatacenterError,
    fetch_datacenter,
)
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient

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
    config=None,
    max_retries: int = 3,
    retry_backoff_seconds: float = 5.0,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)

    # Ask for the window; fall back to walking the report if the upstream
    # refuses. EastMoney rejected range comparisons on date columns outright at
    # one point — "参数预处理错误: org.antlr.v4.runtime.InputMismatchException
    # (code=9501)" — which took this step from working to failing with no code
    # change on our side, and the descending walk below is what it was rewritten
    # to. Re-measured 2026-09-17: the predicate is honoured exactly, 108 rows for
    # 2016-03 and 1,361 for 2016, zero dates outside the window.
    #
    # The difference is not cosmetic. The walk reads the whole 2010..2035 report
    # — 63 pages of 500 — on *every* call, so a backfill's strides each restart
    # it from page 1 and a transient timeout on any one page fails the run;
    # chunking the backfill would multiply those walks rather than shrink them.
    # Asking for the window reads one year in three pages.
    #
    # The walk stays as the fallback because this upstream has broken this way
    # before, and a slow answer beats a failed one.
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

    windowed = f"(FREE_DATE>='{start.isoformat()}')(FREE_DATE<='{end.isoformat()}')"
    try:
        try:
            raw = fetch_datacenter(
                client,
                _UNLOCK_REPORT,
                _UNLOCK_COLUMNS,
                filter_expr=windowed,
                sort_columns="FREE_DATE",
                sort_types="-1",
                max_retries=max_retries,
                retry_backoff_seconds=retry_backoff_seconds,
            )
        except EastMoneyDatacenterError as exc:
            if "9501" not in str(exc):
                raise
            logger.warning(
                "share_unlock_schedule: EastMoney refused the FREE_DATE range "
                "(%s); walking the report instead, which is slower but correct.",
                exc,
            )
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
    finally:
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
                "unlock_shares": _to_float(shares),
                "unlock_ratio": _to_float(item.get("FREE_RATIO")),
                "unlock_type": str(item.get("FREE_SHARES_TYPE") or ""),
            }
        )

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "unlock_date"], keep="last")
