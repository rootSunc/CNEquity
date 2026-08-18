"""Shared EastMoney response helpers."""

from __future__ import annotations

import math
from datetime import date, datetime

from cnequity.domain.symbols import (
    format_symbol,
    infer_exchange_from_code,
    is_all_a_symbol,
    is_etf_symbol,
)

ALL_A_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
# EastMoney clist ETF/LOF boards (see quote.eastmoney.com center/gridlist
# fund_etf). Kept separate from ALL_A_FS, which is equity-only, so instruments
# can still fill list_date on ETFs whose board the A-share filter omits.
ETF_CLIST_FS = "b:MK0021,b:MK0022,b:MK0023,b:MK0024,b:MK0827"
DATACENTER_BASE = "https://datacenter-web.eastmoney.com/api/data/v1/get"
PUSH2_CLIST_HOSTS = (
    "https://push2.eastmoney.com",
    "https://push2delay.eastmoney.com",
    # Numbered push2 host serving the ST board; useful when push2 is 502.
    "https://40.push2.eastmoney.com",
)
PUSH2_CLIST = f"{PUSH2_CLIST_HOSTS[0]}/api/qt/clist/get"
PUSH2HIS_KLINE_HOSTS = (
    "https://push2his.eastmoney.com",
    "https://91.push2his.eastmoney.com",
)


def parse_em_ymd(value: str) -> date:
    """Parse EastMoney compact ``YYYYMMDD`` (3.10 ``fromisoformat`` rejects this)."""
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return date.fromisoformat(text[:10])


def symbol_from_secucode(secucode: str | None) -> str | None:
    """Parse ``600519.SH`` / ``000001.SZ`` style codes from datacenter rows."""
    if not secucode:
        return None
    text = str(secucode).strip().upper()
    if "." not in text:
        return None
    code, exchange = text.split(".", 1)
    code = code.strip().zfill(6)
    if len(code) != 6 or not code.isdigit():
        return None
    if exchange not in {"SH", "SZ", "BJ"}:
        return None
    if not is_all_a_symbol(code, exchange):
        return None
    return format_symbol(code, exchange)


def report_period_from_date(raw: str | None) -> str | None:
    """Datacenter quarter-end date (``2026-06-30 00:00:00``) → ``2026Q2``."""
    if not raw:
        return None
    text = str(raw)[:10]
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        return None
    quarter_ends = {
        (3, 31): "Q1",
        (6, 30): "Q2",
        (9, 30): "Q3",
        (12, 31): "Q4",
    }
    quarter = quarter_ends.get((parsed.month, parsed.day))
    return f"{parsed.year}{quarter}" if quarter else None


def _to_float(value: object, default: float | None = None) -> float | None:
    """Parse a numeric field without turning bad input into a real zero.

    A zero is meaningful for several EastMoney fields (for example a halted
    security's volume), so missing, malformed, and non-finite values must stay
    distinguishable from an observed zero. Callers that require a value should
    reject ``None`` explicitly; nullable business fields can preserve it.
    """
    if value is None or value == "" or value == "-":
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _to_int(
    value: object,
    default: int | None = None,
    *,
    minimum: int = -(2**63),
    maximum: int = 2**63 - 1,
) -> int | None:
    """Parse an integer field without truncation or Int64 overflow.

    EastMoney sometimes serializes integer fields as floating-point values.
    Only finite, integral values within the range accepted by the curated
    schemas are valid; malformed values remain nullable instead of aborting a
    whole response or being silently truncated.
    """
    parsed = _to_float(value)
    if parsed is None or not parsed.is_integer() or parsed < minimum or parsed > maximum:
        return default
    return int(parsed)


def symbol_from_em(code: str, market_id: int) -> str | None:
    code = str(code).strip().zfill(6)
    if len(code) != 6 or not code.isdigit():
        return None
    exchange = "SH" if market_id == 1 else ("BJ" if market_id == 2 else "SZ")
    if not is_all_a_symbol(code, exchange):
        return None
    return format_symbol(code, exchange)


def symbol_from_clist(code: str, market_id: int) -> str | None:
    """Resolve clist ``f12``/``f13`` to a canonical symbol.

    Some EastMoney hosts (e.g. push2delay) return ``f13=0`` for every row;
    fall back to code-prefix exchange inference in that case.
    """
    code = str(code).strip().zfill(6)
    if len(code) != 6 or not code.isdigit():
        return None
    if market_id == 1:
        exchange = "SH"
    elif market_id == 2:
        exchange = "BJ"
    else:
        exchange = infer_exchange_from_code(code)
    if not is_all_a_symbol(code, exchange):
        return None
    return format_symbol(code, exchange)


def symbol_from_clist_etf(code: str, market_id: int) -> str | None:
    """Resolve a clist ETF/LOF row (``f12``/``f13``) to a canonical symbol.

    ETFs are excluded from ``is_all_a_symbol`` on purpose (they are not part of
    the all_a research universe), so the A-share resolver cannot represent them.
    The market id is unreliable on push2delay (returns 0 for every row), so fall
    back to the ETF code prefix when it is absent.
    """
    code = str(code).strip().zfill(6)
    if len(code) != 6 or not code.isdigit():
        return None
    if market_id == 1:
        exchange = "SH"
    elif market_id == 2:
        exchange = "BJ"
    elif is_etf_symbol(code, "SH"):
        exchange = "SH"
    elif is_etf_symbol(code, "SZ"):
        exchange = "SZ"
    else:
        return None
    if not is_etf_symbol(code, exchange):
        return None
    return format_symbol(code, exchange)


def exchange_from_datacenter(row: dict) -> str:
    market = str(row.get("MARKET_CODE") or row.get("TRADE_MARKET") or "").upper()
    code = str(row.get("SECURITY_CODE") or row.get("SECUCODE", "").split(".")[0]).zfill(6)
    if "SH" in market or code.startswith(("60", "68")):
        return "SH"
    if "BJ" in market or market in {"2", "BSE"} or infer_exchange_from_code(code) == "BJ":
        return "BJ"
    return infer_exchange_from_code(code)
