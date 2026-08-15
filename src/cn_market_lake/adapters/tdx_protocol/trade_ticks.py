"""TDX transaction records (分笔) for one settled session, assembled whole.

**These are not individual trades.** A-share Level-1 is a 3-second snapshot, so
one record is however many real trades landed inside one frame: the same-session
command reports that count and it averages 6.3 for 600519 and 33.4 for 000001,
peaking at 1,217. That caps a session near 4,800 records (14,400 trading seconds
÷ 3) — measured 2026-07-31, the busiest name probed held 4,842 and the mean over
40 random stocks was 2,721. Anything downstream that says "tick" means "3-second
aggregate", and the docs have to keep saying so.

The wire timestamp carries **no seconds**, so up to 20 records a minute share a
timestamp and ``(symbol, trade_date, trade_time)`` cannot identify a row.
``tick_seq`` — position within the session, ascending in time — is what does,
and that is only meaningful if the whole session is in hand. Hence the contract
here: a session is returned complete or not at all. A page that fails mid-walk
raises rather than returning what it got, because a short assembly renumbers
every row after the hole.

That numbering is safe because a settled session is frozen: fetched twice,
600519 (4,308 rows) and 300750 (4,764) came back identical field for field.
There is no counterpart to the boundary jitter the minute bars have.

``start`` counts back from the session's *last* record, so pages arrive
newest-first and are prepended, and a short page marks the far edge.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date, datetime, time

from cn_market_lake.adapters.tdx_protocol._wire import MAX_TICK_PAGE
from cn_market_lake.adapters.tdx_protocol._wire.constants import SECURITY_COEFFICIENT
from cn_market_lake.adapters.tdx_protocol._wire.helper import get_security_type
from cn_market_lake.domain.rate_limit import RateLimitSpec, wait_spec

logger = logging.getLogger(__name__)

# Wire code → what lands in the `direction` column. Measured over 77k records
# across 36 symbols on 2026-07-31: only these four ever appear.
#
# 0/1 are the tick-rule guess at who crossed the spread — TDX's inference, not
# an exchange field, and they line up with the move from the previous frame
# about 70% of the time. 2 is the opening auction (exactly one record a session,
# at 09:25) plus the handful of frames the rule cannot call.
#
# 5 is the one nobody expects: after-hours fixed-price trading, 15:05–15:30,
# always at the session's last price. It is **not part of the exchange's daily
# volume** — reconciling against daily_bars gives 1.000363 with these rows and
# 1.000000 without — so it has to stay separable rather than folded into
# `neutral`, or every reconciliation carries a small permanent bias.
DIRECTIONS: dict[int, str] = {0: "buy", 1: "sell", 2: "neutral", 5: "after_hours"}
AFTER_HOURS = "after_hours"
UNKNOWN_DIRECTION = "unknown"

# Legal windows, inclusive. Measured: every session opens with one 09:25 record
# and nothing falls in the lunch break or between 15:00 and 15:05.
#
# Note how little this shares with `minute_bars.SESSIONS`, which is why it is
# not imported from there: bars are labelled by their closing minute, so the
# auction lives inside the 09:31 bar and 13:00 is not a bar at all. A trade at
# 09:25 or 13:00 is a real trade.
SESSIONS: tuple[tuple[time, time], ...] = (
    (time(9, 25), time(9, 25)),  # opening call auction
    (time(9, 30), time(11, 30)),
    (time(13, 0), time(15, 0)),
    (time(15, 5), time(15, 30)),  # after-hours fixed price
)

# A full session is ~4,800 records = 3 pages. Eight is loose enough that no
# real session reaches it and tight enough that a runaway walk stops being
# free. Reaching it means the far edge was never found, and the early session
# would be the part missing — so it fails rather than returning a truncated day.
MAX_SESSION_PAGES = 8


class TdxTradeTicksError(RuntimeError):
    """Raised when a session cannot be assembled whole."""


def in_session(stamp: datetime | time) -> bool:
    """Whether *stamp* falls in a window A-shares actually trade in."""
    clock = stamp.time() if isinstance(stamp, datetime) else stamp
    return any(lo <= clock <= hi for lo, hi in SESSIONS)


def price_divisor(symbol: str) -> int:
    """What the wire's integer price divides by to become yuan.

    Not a constant. Upstream divides by 100 unconditionally, which is the
    A-share stock coefficient; funds are 0.001 and bonds 0.0001. Measured, that
    shortcut decodes 159915 at 33.68 instead of 3.368 and puts 510300's
    reconciliation against its own daily turnover at 10.004.

    A divisor rather than the table's multiplier: 0.01 has no exact double, so
    ``135060 * 0.01`` is 1350.6000000000001 while ``135060 / 100`` is the same
    double as ``1350.6``. The prices are compared against daily_bars, and a
    reconciliation should not have to absorb noise this avoidable.

    Raises rather than falling back to the stock coefficient: a wrong scale is
    invisible in the data — the numbers all look like prices — and the fallback
    is exactly the 10× error.
    """
    code, _, exch = symbol.partition(".")
    market = 1 if exch == "SH" else 0
    try:
        return round(1 / SECURITY_COEFFICIENT[get_security_type(market, code)][0])
    except (NotImplementedError, KeyError):
        raise TdxTradeTicksError(
            f"{symbol}: no known price coefficient — refusing to guess, because "
            "the wrong scale produces plausible-looking prices"
        ) from None


def _row(symbol: str, trade_date: date, seq: int, raw: dict, divisor: int) -> dict:
    stamp = datetime(
        trade_date.year, trade_date.month, trade_date.day, int(raw["hour"]), int(raw["minute"])
    )
    code = int(raw["direction"])
    return {
        "symbol": symbol,
        "trade_date": trade_date,
        "tick_seq": seq,
        "trade_time": stamp,
        "price": int(raw["price_raw"]) / divisor,
        # The wire reports lots; the lake stores shares (domain.units). The
        # conversion is confirmed by reconciliation, not assumed: excluding the
        # after-hours rows, sum(vol) × 100 matches daily_bars.volume to
        # 1.000000 on five of six symbols measured.
        "volume": int(raw["vol"]) * 100,
        "direction": DIRECTIONS.get(code, UNKNOWN_DIRECTION),
    }


def fetch_trade_ticks(
    client,
    symbol: str,
    trade_date: date,
    *,
    rate_limit: RateLimitSpec | None = None,
    on_page: Callable[[], None] | None = None,
) -> list[dict]:
    """Every transaction record of *symbol* on *trade_date*, oldest first.

    Returns ``[]`` for a session the symbol did not trade — a suspension is a
    real answer, not a failure. Every other incomplete outcome raises.
    """
    code, _, exch = symbol.partition(".")
    if exch == "BJ":
        # The server answers with an empty list rather than an error, which is
        # indistinguishable from "did not trade" — and would quietly write a
        # Beijing name into the lake as permanently suspended.
        raise TdxTradeTicksError(f"{symbol}: TDX serves no Beijing-exchange transaction records")
    if exch not in ("SH", "SZ"):
        raise TdxTradeTicksError(f"{symbol}: unsupported exchange suffix {exch!r}")
    divisor = price_divisor(symbol)
    market = 1 if exch == "SH" else 0

    raw_rows: list[dict] = []
    start = 0
    for _page in range(MAX_SESSION_PAGES):
        wait_spec(rate_limit)
        try:
            page = client.ticks_history(
                code, trade_date, market=market, start=start, offset=MAX_TICK_PAGE
            )
        except Exception as exc:
            raise TdxTradeTicksError(
                f"{symbol} {trade_date}: transaction page failed at start={start}"
            ) from exc

        if not page:
            break
        # Pages walk backwards from the session's end, so each one goes in front.
        raw_rows = list(page) + raw_rows
        if on_page is not None:
            on_page()
        if len(page) < MAX_TICK_PAGE:
            break
        start += MAX_TICK_PAGE
    else:
        raise TdxTradeTicksError(
            f"{symbol} {trade_date}: still full pages after {MAX_SESSION_PAGES} "
            f"({len(raw_rows)} records) — the walk never reached the session's "
            "start, and the missing part would be the early session"
        )

    if not raw_rows:
        return []

    rows = [_row(symbol, trade_date, seq, raw, divisor) for seq, raw in enumerate(raw_rows)]

    off_session = [r for r in rows if not in_session(r["trade_time"])]
    if off_session:
        # Not dropped, the way an off-session minute bar is. There, a stray bar
        # is one bad row among 240 independent ones; here every later row's
        # tick_seq depends on this one existing, so a silent drop renumbers the
        # session. Measured, zero of 77k records fell outside these windows, so
        # one appearing means the layout moved, not that the market did.
        examples = ", ".join(str(r["trade_time"].time()) for r in off_session[:3])
        raise TdxTradeTicksError(
            f"{symbol} {trade_date}: {len(off_session)} record(s) outside trading "
            f"hours (e.g. {examples}) — the response layout changed"
        )

    stamps = [r["trade_time"] for r in rows]
    if any(later < earlier for earlier, later in zip(stamps, stamps[1:], strict=False)):
        raise TdxTradeTicksError(
            f"{symbol} {trade_date}: records are not in time order, so tick_seq "
            "would not be a sequence — pages were assembled wrongly"
        )

    unknown = sum(1 for r in rows if r["direction"] == UNKNOWN_DIRECTION)
    if unknown:
        # Kept rather than rejected: the price and volume are still good, and a
        # new direction code is news rather than corruption. The audit's
        # direction-mix check is what surfaces it.
        logger.warning(
            "%s %s: %d record(s) carry an unknown direction code", symbol, trade_date, unknown
        )
    return rows
