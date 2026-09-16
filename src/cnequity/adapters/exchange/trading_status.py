"""Halt and ST status read from each exchange's own whole-board publication.

`trading_status` had no independent daily source. Its feed is EastMoney's
current-state board, reached through a TDX-named facade that only forwards to
EastMoney, and the in-step fallback was this lake's own previous snapshot —
which is not a second opinion, it is the same opinion one day older. The backup
gate said so once the registry stopped mislabelling the primary.

The exchanges publish what is needed already, and the daily-bar route reads it:
a board file carries every listed security with its OHLC and its 证券简称.

* A halted security is still listed, with open/high/low reported as ``0`` beside
  a non-zero reference close. That is the exchange stating it did not trade.
* ST and *ST are carried in the name, which is where `exchange/st_lists.py`
  already reads them from.

Measured against the EastMoney rows for 2026-09-15 over 5,219 comparable
symbols: ST agreed on **100.000%**, halts on 99.923%. All four disagreements
were EastMoney calling a name halted while the exchange published a full
session for it — ``301139.SZ``, ``301390.SZ``, ``002731.SZ``, ``000016.SZ`` —
so the exchange was right in every one.

Two requests for the market, 2.6s measured. Shanghai and Shenzhen only: the
Beijing board is not here, and `covered` says so rather than letting a caller
read absence as "not listed".
"""

from __future__ import annotations

import io
import logging
import warnings
from dataclasses import dataclass
from datetime import date

import polars as pl

from cnequity.adapters.exchange.daily_quotes import (
    _SSE_HEADERS,
    _SZSE_HEADERS,
    _TIMEOUT_SECONDS,
    SSE_CLOSE_TIME,
    SZSE_URL,
    _client,
    _keep_symbol,
)
from cnequity.adapters.exchange.st_lists import is_st_name
from cnequity.domain.rate_limit import source_request
from cnequity.domain.symbols import format_symbol
from cnequity.domain.trading_status import STATUS_NORMAL, STATUS_SUSPENDED

logger = logging.getLogger(__name__)

_SOURCE = "exchange"
#: `name` is the addition over the daily-bar route; the rest is the same board.
SSE_STATUS_SELECT = ("code", "name", "open", "high", "low", "last")
SSE_STATUS_URL = (
    "http://yunhq.sse.com.cn:32041/v1/sh1/list/exchange/equity"
    f"?select={','.join(SSE_STATUS_SELECT)}&begin=0&end=6000"
)
_SZSE_STATUS_COLUMNS = ("证券代码", "证券简称", "开盘", "最高", "最低", "今收")


@dataclass(frozen=True)
class ExchangeStatusResult:
    """Board status plus which exchanges actually answered.

    ``covered`` exists for the same reason it does on the quote route: one
    exchange answering is not the market answering, and a caller that ignores
    it will read a Shanghai-only reply as a verdict on Shenzhen too.
    """

    rows: pl.DataFrame
    covered: frozenset[str]
    failures: dict[str, str]

    @property
    def is_empty(self) -> bool:
        return self.rows.is_empty()


def _row(symbol: str, name: str, trade_date: date, ohl: tuple[float, float, float], close: float):
    # A limit-locked session is open == high == low == close, which is why the
    # test is "all three at zero beside a positive close" rather than "no range".
    halted = close > 0.0 and max(ohl) <= 0.0
    return {
        "symbol": symbol,
        "trade_date": trade_date,
        "is_trading": not halted,
        "status": STATUS_SUSPENDED if halted else STATUS_NORMAL,
        "risk_warning": is_st_name(name),
    }


def _float(value: object) -> float:
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _fetch_sse(trade_date: date, *, config=None) -> list[dict]:
    with source_request(config, _SOURCE):
        response = _client().get(
            SSE_STATUS_URL, headers=_SSE_HEADERS, impersonate="chrome", timeout=_TIMEOUT_SECONDS
        )
    response.raise_for_status()
    payload = response.json()
    raw_date = payload.get("date")
    try:
        snapshot_day = date(
            int(str(raw_date)[:4]), int(str(raw_date)[4:6]), int(str(raw_date)[6:8])
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"SSE status snapshot carried an unreadable date {raw_date!r}") from exc
    if snapshot_day != trade_date:
        # A current snapshot relabelled as another session would manufacture a
        # point-in-time fact the exchange never published.
        raise ValueError(f"SSE snapshot serves {snapshot_day}, not {trade_date}")
    snapshot_time = payload.get("time")
    if not isinstance(snapshot_time, int) or snapshot_time < SSE_CLOSE_TIME:
        raise ValueError(f"SSE snapshot is mid-session (time={snapshot_time})")

    rows = []
    for item in payload.get("list") or []:
        if not isinstance(item, (list, tuple)) or len(item) < len(SSE_STATUS_SELECT):
            continue
        code = str(item[0]).strip().zfill(6)
        if len(code) != 6 or not code.isdigit() or not _keep_symbol(code, "SH"):
            continue
        name = str(item[1] or "")
        o, h, low, close = (_float(v) for v in item[2:6])
        rows.append(_row(format_symbol(code, "SH"), name, trade_date, (o, h, low), close))
    return rows


def _fetch_szse(trade_date: date, *, config=None) -> list[dict]:
    import pandas as pd

    with source_request(config, _SOURCE):
        response = _client().get(
            SZSE_URL.format(day=trade_date.isoformat()),
            headers=_SZSE_HEADERS,
            impersonate="chrome",
            timeout=_TIMEOUT_SECONDS,
        )
    response.raise_for_status()
    if not response.content:
        raise ValueError(f"SZSE published no report for {trade_date}")
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Workbook contains no default style")
        frame = pd.read_excel(io.BytesIO(response.content), dtype=str)
    missing = [column for column in _SZSE_STATUS_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"SZSE report is missing {missing}")

    rows = []
    for record in frame[list(_SZSE_STATUS_COLUMNS)].to_dict("records"):
        code = str(record["证券代码"]).strip().zfill(6)
        if len(code) != 6 or not code.isdigit() or not _keep_symbol(code, "SZ"):
            continue
        # The export pads short names with spaces (``万  科Ａ``).
        name = str(record["证券简称"]).replace(" ", "").replace("　", "")
        ohl = (_float(record["开盘"]), _float(record["最高"]), _float(record["最低"]))
        rows.append(_row(format_symbol(code, "SZ"), name, trade_date, ohl, _float(record["今收"])))
    return rows


def fetch_trading_status_exchange(
    symbols: list[str] | set[str] | None,
    trade_date: date,
    *,
    config=None,
) -> ExchangeStatusResult:
    """Halt and ST status for the SH/SZ boards, from the exchanges themselves.

    Either exchange failing leaves its own symbols uncovered rather than
    failing the call: a Shenzhen outage must not cost Shanghai its reading.
    """
    rows: list[dict] = []
    covered: set[str] = set()
    failures: dict[str, str] = {}
    for label, fetch in (("sse", _fetch_sse), ("szse", _fetch_szse)):
        try:
            rows.extend(fetch(trade_date, config=config))
            covered.add(label)
        except Exception as exc:  # noqa: BLE001 — a partial board is still evidence
            failures[label] = f"{type(exc).__name__}: {exc}"
            logger.warning("exchange trading status unavailable from %s: %s", label, exc)

    frame = pl.DataFrame(rows, schema_overrides={"risk_warning": pl.Boolean})
    if symbols is not None and not frame.is_empty():
        frame = frame.filter(pl.col("symbol").is_in(sorted(set(symbols))))
    if not frame.is_empty():
        frame = frame.unique(subset=["symbol", "trade_date"], keep="last")
    return ExchangeStatusResult(rows=frame, covered=frozenset(covered), failures=failures)
