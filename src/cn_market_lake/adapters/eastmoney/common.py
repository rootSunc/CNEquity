"""Shared EastMoney response helpers."""

from __future__ import annotations

from datetime import date, datetime

from cn_market_lake.domain.symbols import format_symbol, is_all_a_symbol

ALL_A_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
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
    code = code.zfill(6)
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
    if len(text) < 7:
        return text
    year = text[:4]
    month = int(text[5:7])
    if month == 3:
        q = "Q1"
    elif month == 6:
        q = "Q2"
    elif month == 9:
        q = "Q3"
    else:
        q = "Q4"
    return f"{year}{q}"


def _to_float(value: object, default: float = 0.0) -> float:
    if value is None or value == "" or value == "-":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def symbol_from_em(code: str, market_id: int) -> str | None:
    code = str(code).zfill(6)
    exchange = "SH" if market_id == 1 else ("BJ" if market_id == 2 else "SZ")
    if not is_all_a_symbol(code, exchange):
        return None
    return format_symbol(code, exchange)


def symbol_from_clist(code: str, market_id: int) -> str | None:
    """Resolve clist ``f12``/``f13`` to a canonical symbol.

    Some EastMoney hosts (e.g. push2delay) return ``f13=0`` for every row;
    fall back to code-prefix exchange inference in that case.
    """
    code = str(code).zfill(6)
    if market_id == 1:
        exchange = "SH"
    elif market_id == 2:
        exchange = "BJ"
    else:
        if code.startswith(("60", "68")):
            exchange = "SH"
        elif code.startswith("92"):
            exchange = "BJ"
        else:
            exchange = "SZ"
    if not is_all_a_symbol(code, exchange):
        return None
    return format_symbol(code, exchange)


def exchange_from_datacenter(row: dict) -> str:
    market = str(row.get("MARKET_CODE") or row.get("TRADE_MARKET") or "").upper()
    code = str(row.get("SECURITY_CODE") or row.get("SECUCODE", "").split(".")[0]).zfill(6)
    if "SH" in market or code.startswith(("60", "68")):
        return "SH"
    if "BJ" in market or code.startswith("92"):
        return "BJ"
    return "SZ"
