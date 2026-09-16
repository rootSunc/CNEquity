"""EastMoney ST / suspension status for trading_status dataset."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.clist import clist_rows_to_symbols, fetch_clist_pages
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.domain.symbols import format_symbol, infer_exchange_from_code, is_all_a_symbol
from cnequity.domain.trading_status import STATUS_NORMAL, STATUS_SUSPENDED

# Risk-warning board (ST / *ST), the fs behind quote.eastmoney.com st_board.
# Do NOT use all-A market fs here.
_ST_FS = "m:0+f:4,m:1+f:4"
#: Markets `_ST_FS` actually selects: m:0 Shenzhen, m:1 Shanghai. Beijing has no
#: market code in it, so the board never looks there — and "not in the returned
#: set" then means "never examined", not "not ST". Read as the latter it
#: published `risk_warning=False` for all 343 live BJ names while the exchange
#: was listing *ST康乐, *ST田野 and *ST同辉, every single day. Outside these two
#: exchanges the answer is null: unknown, never a claim of "clean". The Beijing
#: board carries the designation in 证券简称 and the step reads it from there.
_ST_BOARD_EXCHANGES = frozenset({"SH", "SZ"})
# The old datacenter report now rejects otherwise valid requests with a
# server-side 9501 contract requiring undocumented MARKET/DATETIME values.
# This is the same feed used by EastMoney's public suspension page and keeps
# the market/date selector explicit instead of guessing the retired report's
# enum values.
_SUSPEND_LIST_URL = "https://datapc.eastmoney.com/emdatacenter/tfg/list2"
_SUSPEND_MARKET = 1  # all mainland markets; non-A rows are filtered below
# Smaller pages: large pz on push2 often 502s (esp. overseas).
_ST_PAGE_SIZE = 100


def _exchange_from_code(code: str) -> str:
    return infer_exchange_from_code(code)


def _st_board_covers(symbol: str) -> bool:
    """Whether `_ST_FS` selects the market this symbol trades on."""
    _, _, exchange = str(symbol).partition(".")
    return exchange.strip().upper() in _ST_BOARD_EXCHANGES


def _em_date(value: object) -> date | None:
    text = str(value or "").strip()
    if len(text) < 10:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _suspension_covers(item: dict, trade_date: date) -> bool:
    stop_date = _em_date(
        item.get("STOP_DATE") or item.get("SUSPEND_START_DATE") or item.get("SUSPEND_START_TIME")
    )
    if stop_date is None or stop_date > trade_date:
        return False

    resume_raw = str(item.get("RESUME_DATE") or item.get("SUSPEND_END_TIME") or "").strip().lower()
    if not resume_raw or resume_raw == "null":
        return True
    resume_date = _em_date(resume_raw)
    return resume_date is not None and resume_date >= trade_date


def _fetch_st_symbols(client: EastMoneyClient) -> set[str]:
    """Current ST-tagged symbols via clist (push2 → push2delay failover)."""
    # An empty ST set is valid; a failed request is not. Treating transport or
    # malformed responses as an empty set silently labels every ST name as
    # normal, which is materially worse than failing the snapshot.
    rows = fetch_clist_pages(
        client,
        fields="f12,f13,f14",
        fs=_ST_FS,
        page_size=_ST_PAGE_SIZE,
    )

    symbols: set[str] = set()
    symbols.update(sym for sym, _item in clist_rows_to_symbols(rows))
    return symbols


def _fetch_suspended_symbols(client: EastMoneyClient, trade_date: date) -> set[str]:
    response = client.get(
        _SUSPEND_LIST_URL,
        params={
            "mkt": _SUSPEND_MARKET,
            "st": "SUSPEND_START_DATE",
            "sr": -1,
            "fd": trade_date.isoformat(),
        },
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("EastMoney suspension response is not an object")
    if payload.get("success") is False:
        raise RuntimeError(
            "EastMoney suspension list failed: "
            f"{payload.get('message') or 'unknown response error'}"
        )
    result = payload.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("EastMoney suspension response without a result object")
    raw_rows = result.get("data")
    if raw_rows is None:
        rows: list[dict] = []
    elif isinstance(raw_rows, list):
        if any(not isinstance(item, dict) for item in raw_rows):
            raise RuntimeError("EastMoney suspension response contains a non-object row")
        rows = raw_rows
    else:
        raise RuntimeError("EastMoney suspension response data is not a list")

    matching_rows = [item for item in rows if _suspension_covers(item, trade_date)]
    if rows and not matching_rows:
        raise RuntimeError(
            f"EastMoney suspension response contains no row covering {trade_date.isoformat()}"
        )
    symbols: set[str] = set()
    for item in matching_rows:
        code = str(item.get("SECURITY_CODE", "")).zfill(6)
        exch = _exchange_from_code(code)
        if is_all_a_symbol(code, exch):
            symbols.add(format_symbol(code, exch))
    return symbols


def fetch_trading_status_eastmoney(
    symbols: list[str],
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config=None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(min_interval=0.3, config=config)

    try:
        st_set = _fetch_st_symbols(client)
        suspended = _fetch_suspended_symbols(client, trade_date)

        # Halting and risk warning are independent, so they are read
        # independently. This used to be an if/elif over one `status` column,
        # which meant a halted ST name was published as merely "suspended" and
        # its designation was lost for that day — see domain/trading_status.py.
        # Delisting is not knowable from these two boards; the step adds it
        # from `instruments`.
        rows = [
            {
                "symbol": sym,
                "trade_date": trade_date,
                "is_trading": sym not in suspended,
                "status": STATUS_SUSPENDED if sym in suspended else STATUS_NORMAL,
                "risk_warning": (sym in st_set if _st_board_covers(sym) else None),
            }
            for sym in symbols
        ]
        return pl.DataFrame(rows, schema_overrides={"risk_warning": pl.Boolean}).unique(
            subset=["symbol", "trade_date"], keep="last"
        )
    finally:
        if owns:
            client.close()
