"""Halt and ST status for the Beijing board, from the exchange itself.

The SH/SZ boards list a halted security with its open/high/low at zero beside a
reference close. Beijing does not: its quotation board carries only the
securities that traded, so the halt is in the *absence*.

An absence is weaker evidence than a zeroed row, so it is only read as a halt
when the board was read completely: a partial page walk would otherwise report
the whole exchange as suspended. `complete` carries that, and a caller that
ignores it gets no halt rows at all.

Scope it to *live* names. Beijing renumbered its board in 2025 — 241 of the
lake's 580 catalogued BJ symbols are retired 43x/83x/87x codes — and a retired
code is absent from the board for good, so handing this the catalogued universe
manufactures hundreds of permanent halts. Measured for 2026-09-15: 327 live
names, all 327 on the board, none absent.

The ST designation is in 证券简称, and the board is the only source that has
it for this exchange. EastMoney's ST board does not cover Beijing: on
2026-09-15 it returned `risk_warning=False` for all 580 BJ names while the
exchange was publishing *ST康乐, *ST田野 and *ST同辉.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import polars as pl

from cnequity.adapters.bse.daily_quotes import read_board
from cnequity.adapters.exchange.st_lists import is_st_name
from cnequity.domain.symbols import format_symbol
from cnequity.domain.trading_status import STATUS_NORMAL, STATUS_SUSPENDED

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BseStatusResult:
    rows: pl.DataFrame
    #: Whether the page walk reached the board's advertised total. Necessary
    #: for reading an absence as a halt, and not sufficient — see the session
    #: guard in `fetch_trading_status_bse`.
    complete: bool
    listed: frozenset[str]

    @property
    def is_empty(self) -> bool:
        return self.rows.is_empty()


def _parse_date(value: object) -> date | None:
    raw = str(value or "").strip()
    try:
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
    except (TypeError, ValueError):
        return None


def board_names(trade_date: date, *, client=None, config=None) -> tuple[dict[str, str], bool]:
    """``{symbol: 证券简称}`` for everything the board published for *trade_date*.

    A row stamped for another session is dropped rather than restamped: the
    endpoint is a snapshot of the latest session, and keeping it would
    manufacture a point-in-time fact the exchange never published.
    """
    raw, total = read_board(client=client, config=config)
    listed: dict[str, str] = {}
    for item in raw:
        code = str(item.get("hqzqdm") or "").strip().zfill(6)
        if len(code) != 6 or not code.isdigit():
            continue
        if _parse_date(item.get("hqjsrq")) != trade_date:
            continue
        listed[format_symbol(code, "BJ")] = str(item.get("hqzqjc") or "")
    return listed, bool(total) and len(raw) >= total


def fetch_trading_status_bse(
    symbols: list[str] | set[str] | None,
    trade_date: date,
    *,
    client=None,
    config=None,
) -> BseStatusResult:
    """Halt and ST status for the Beijing board.

    ``symbols`` is the *live* scope — see the module docstring on retired codes.
    Names on the board are trading; names in the scope but off it are halted,
    and only when the walk was complete. A halted name's ST designation is not
    on the board either, so it stays ``None``: unknown, never a claim of
    "clean".
    """
    listed, complete = board_names(trade_date, client=client, config=config)

    scope = {str(s).strip().upper() for s in (symbols or ())} or set(listed)
    rows = [
        {
            "symbol": symbol,
            "trade_date": trade_date,
            "is_trading": True,
            "status": STATUS_NORMAL,
            "risk_warning": is_st_name(name),
        }
        for symbol, name in sorted(listed.items())
        if symbol in scope
    ]
    absent = sorted(scope - set(listed))
    # `complete` is a fact about the walk, not about the session. The board is
    # a snapshot of the *latest* session, so a run before Beijing publishes
    # gets a complete walk of yesterday — every row dropped as off-session,
    # every scope name "absent", and the whole exchange declared suspended.
    # An empty reading is no evidence; it must never become the strongest kind.
    if complete and listed:
        rows.extend(
            {
                "symbol": symbol,
                "trade_date": trade_date,
                "is_trading": False,
                "status": STATUS_SUSPENDED,
                # Off the board is off the ST list too.
                "risk_warning": None,
            }
            for symbol in absent
        )
    elif absent:
        logger.warning(
            "BSE board read cannot adjudicate absence for %s (complete=%s, listed=%d); "
            "%d absent symbol(s) left unjudged",
            trade_date,
            complete,
            len(listed),
            len(absent),
        )
    frame = pl.DataFrame(rows, schema_overrides={"risk_warning": pl.Boolean})
    return BseStatusResult(rows=frame, complete=complete, listed=frozenset(listed))
