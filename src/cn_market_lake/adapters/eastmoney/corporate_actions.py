"""EastMoney corporate actions (backup for TDX xdxr)."""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cn_market_lake.adapters.eastmoney.datacenter import (
    EastMoneyDatacenterError,
    fetch_datacenter,
)
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
from cn_market_lake.config import Config
from cn_market_lake.domain.symbols import format_symbol

logger = logging.getLogger(__name__)

_REPORT = "RPT_SHAREBONUS_DET"
# EastMoney renamed these columns (EX_DIV_DATE→EX_DIVIDEND_DATE,
# CASH_BTAX_RMB→PRETAX_BONUS_RMB, TRANSFER_RATIO→IT_RATIO) ~2026; the old names
# now 404 the whole report (code=9501). EM quotes amounts/ratios per-10-shares,
# pretax; _parse_row divides by 10 to honor the per-share CORPORATE_ACTIONS
# unit contract (schemas.py). Do NOT stage the raw per-10 values.
_EX_DATE_COL = "EX_DIVIDEND_DATE"
# Oldest ex-date a backfill walks back to when the caller names no start.
_BACKFILL_FLOOR = date(2016, 1, 1)
_COLUMNS = (
    "SECURITY_CODE,SECUCODE,EX_DIVIDEND_DATE,EQUITY_RECORD_DATE,PRETAX_BONUS_RMB,"
    "BONUS_RATIO,IT_RATIO,BONUS_IT_RATIO,IMPL_PLAN_PROFILE,ASSIGN_PROGRESS"
)


def _num(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _map_action_type(row: dict) -> str | None:
    impl = str(row.get("IMPL_PLAN_PROFILE") or "").lower()
    if "配" in impl:
        return "allotment"
    if "转" in impl:
        return "transfer"
    if "送" in impl:
        return "bonus"
    if "派" in impl or "息" in impl or "现金" in impl:
        return "cash_dividend"
    if _num(row.get("PRETAX_BONUS_RMB")) > 0:
        return "cash_dividend"
    if _num(row.get("IT_RATIO")) > 0:
        return "transfer"
    if _num(row.get("BONUS_RATIO")) > 0:
        return "bonus"
    return None


def _ex_date(row: dict) -> date | None:
    """Ex-date of one report row, or None when the row carries neither field."""
    raw = row.get(_EX_DATE_COL) or row.get("EQUITY_RECORD_DATE")
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def _parse_row(row: dict) -> dict | None:
    ex_date = _ex_date(row)
    if ex_date is None:
        return None

    secucode = str(row.get("SECUCODE") or "")
    code_part, _, suffix = secucode.partition(".")
    code = str(row.get("SECURITY_CODE") or code_part or "").zfill(6)
    if suffix in ("SH", "SZ", "BJ"):
        exchange = suffix
    elif code.startswith(("60", "68")):
        exchange = "SH"
    elif code.startswith(("43", "83", "87", "88", "92")):
        exchange = "BJ"
    else:
        exchange = "SZ"
    symbol = format_symbol(code, exchange)

    action_type = _map_action_type(row)
    if not action_type:
        return None

    # EM values are per-10-shares; divide by 10 for the per-share contract.
    cash = _num(row.get("PRETAX_BONUS_RMB")) / 10.0
    bonus = _num(row.get("BONUS_RATIO")) / 10.0
    transfer = _num(row.get("IT_RATIO")) / 10.0
    return {
        "symbol": symbol,
        "ex_date": ex_date,
        "action_type": action_type,
        "cash_dividend": cash if action_type == "cash_dividend" else 0.0,
        "bonus_ratio": bonus if action_type == "bonus" else 0.0,
        "transfer_ratio": transfer if action_type == "transfer" else 0.0,
        # RPT_SHAREBONUS_DET carries no allotment (配股) price/ratio columns;
        # allotment detail is a separate report, out of scope for daily ex-date.
        "allotment_ratio": None,
        "allotment_price": None,
    }


def fetch_corporate_actions_eastmoney(
    trade_date: date,
    *,
    backfill: bool = False,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)

    # No range predicate on the backfill path. EastMoney's datacenter rejects
    # range comparisons on date columns — "参数预处理错误: org.antlr.v4.runtime.
    # InputMismatchException (code=9501)" — the same change that broke
    # share_unlock_schedule and northbound_flows. Here it took out
    # `cml backfill corporate_actions` entirely. Equality still parses, so the
    # daily path keeps its exact-date filter; the backfill pages the report
    # newest-first (it is already sorted that way) and stops at the first page
    # that ends before the floor.
    stop_after = None
    if backfill:
        date_filter = ""
        floor = getattr(config, "_backfill_start", None) or _BACKFILL_FLOOR

        def stop_after(batch: list[dict]) -> bool:
            for item in reversed(batch):
                parsed = _ex_date(item)
                if parsed is not None:
                    return parsed < floor
            return False
    else:
        ds = trade_date.isoformat()
        date_filter = f"({_EX_DATE_COL}='{ds}')"

    retries = config.max_retries if config is not None else 3
    backoff = float(config.retry_backoff_seconds if config is not None else 5)

    try:
        if config is not None:
            config.rate_limit("eastmoney")
        raw = fetch_datacenter(
            client,
            _REPORT,
            _COLUMNS,
            filter_expr=date_filter,
            sort_columns=_EX_DATE_COL,
            sort_types="-1",
            max_retries=retries,
            retry_backoff_seconds=backoff,
            stop_after=stop_after,
        )
    except EastMoneyDatacenterError:
        if owns:
            client.close()
        raise
    except Exception as exc:
        if owns:
            client.close()
        raise EastMoneyDatacenterError(
            f"EastMoney corporate_actions failed for filter {date_filter!r}"
        ) from exc

    rows = []
    for item in raw:
        parsed = _parse_row(item)
        if parsed:
            rows.append(parsed)

    if owns:
        client.close()

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "ex_date", "action_type"], keep="last")
