"""DCE futures from Sina, while DCE's own endpoints stay out of reach.

Every DCE endpoint this project has tried — ``dcereport/publicweb/dailystat/
dayQuotes``, ``publicweb/quotesdata/dayQuotesCh.html``, the export form —
answers with HTTP 412 and a JavaScript challenge, from browser and plain user
agents alike (measured 2026-09-25, overseas egress). The project does not work
around access controls, so DCE rows come from Sina, declared as a
supplementary source with its gaps written down (ADR-0013).

Two Sina endpoints, measured 2026-09-25:

- **Per-contract history** (``InnerFuturesNewService.getDailyKLine``): the
  whole life of one contract per request, with open/high/low/close, volume,
  open interest (``p``) and settlement (``s``). Contracts listed from about
  mid-2018 on (CU1906 answers, CU1901 does not; the same horizon holds for
  DCE). Sessions with no trade are **absent**, there is no turnover, and counts
  before 2020 are the exchange's double-sided figures, halved here like the
  exchanges' own files.
- **Batch quote** (``hq.sinajs.cn/list=nf_…``): after the close, every listed
  contract's session with settlement, previous settlement, volume and open
  interest, dated, a hundred contracts a request. Codes that do not exist come
  back empty, so candidate months can be asked for blindly. This covers the
  current session, including contracts that did not trade.

A backfill asks each contract once and serves every session from memory. The
daily run reads the batch quote, which also covers contracts with no trades.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date

import httpx
import polars as pl

from cnequity.adapters.futures_exchange.common import (
    ExchangeDay,
    FuturesDayUnavailable,
    drop_placeholders,
    fetch_bytes,
    parse_number,
    parse_price,
)
from cnequity.domain.derivatives import (
    CountingBasisError,
    parse_future_code,
    single_sided_count,
)

logger = logging.getLogger(__name__)

EXCHANGE = "DCE"
SOURCE = "sina"
#: The earliest session any Sina-served DCE contract reaches (M1809 lists 2018-09
#: or so; the first contracts answering start trading in mid-2018).
FIRST_SESSION = date(2018, 6, 1)

HISTORY_URL = (
    "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/x/"
    "InnerFuturesNewService.getDailyKLine"
)
QUOTE_URL = "https://hq.sinajs.cn/list={codes}"
_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn"}
_QUOTE_BATCH = 100
_QUOTE_RECENT_DAYS = 7

#: DCE futures products, upper case, as Sina and the lake spell them.
PRODUCTS: tuple[str, ...] = (
    "A", "B", "M", "Y", "P", "C", "CS", "JD", "L", "V", "PP", "EG", "EB", "PG",
    "I", "J", "JM", "RR", "LH", "FB", "BB", "LG", "BZ",
)  # fmt: skip

_ARRAY = re.compile(r"\[.*\]", re.S)
_QUOTE = re.compile(r'var hq_str_nf_([A-Z]+\d{4})="([^"]*)"')


def candidate_codes(day: date, *, months_ahead: int = 13) -> list[str]:
    """Every product × month that could be live on *day*."""
    codes = []
    year, month = day.year, day.month
    for _ in range(months_ahead):
        yymm = f"{year % 100:02d}{month:02d}"
        codes.extend(f"{product}{yymm}" for product in PRODUCTS)
        month += 1
        if month > 12:
            year, month = year + 1, 1
    return codes


def _client() -> httpx.Client:
    return httpx.Client(timeout=30.0, headers=_HEADERS, follow_redirects=True)


def _history_rows(code: str, payload: list[dict]) -> dict[date, dict]:
    contract = None
    out: dict[date, dict] = {}
    for item in payload:
        if not isinstance(item, dict) or not item.get("d"):
            continue
        day = date.fromisoformat(str(item["d"])[:10])
        contract = contract or parse_future_code(code, EXCHANGE, day)
        try:
            volume = single_sided_count(
                parse_number(item.get("v")),
                exchange=EXCHANGE,
                trade_date=day,
                field="volume",
            )
            open_interest = single_sided_count(
                parse_number(item.get("p")),
                exchange=EXCHANGE,
                trade_date=day,
                field="open_interest",
            )
        except CountingBasisError as exc:
            logger.warning("dce via sina: %s %s skipped: %s", code, day, exc)
            continue
        traded = bool(volume)
        out[day] = {
            "symbol": contract.symbol,
            "exchange": EXCHANGE,
            "exchange_code": code.lower(),
            "product": contract.product,
            "trade_date": day,
            "open": parse_price(item.get("o")) if traded else None,
            "high": parse_price(item.get("h")) if traded else None,
            "low": parse_price(item.get("l")) if traded else None,
            "close": parse_price(item.get("c")) if traded else None,
            "settle": parse_price(item.get("s")),
            # Absent sessions (no trades) make the previous row an unreliable
            # stand-in for the previous settlement, so it is left null.
            "pre_settle": None,
            "volume": volume,
            "amount": None,
            "open_interest": open_interest,
            # Same reason as pre_settle: a change needs the previous session.
            "oi_change": None,
            "source": SOURCE,
        }
    return out


class SinaDceHistory:
    """Per-contract histories backed by a bounded persistent response cache."""

    def __init__(self) -> None:
        pass

    def contract(self, code: str, *, config=None, client: httpx.Client) -> dict[date, dict]:
        # Both positive and negative results expire. Persistent exact-wire
        # cache survives CLI restarts; old delivery months have a longer TTL.
        from cnequity.domain.market_time import shanghai_today

        today = shanghai_today()
        delivery = date(2000 + int(code[-4:-2]), int(code[-2:]), 1)
        ttl = 30 * 86400 if (today - delivery).days > 62 else 3600
        body = fetch_bytes(
            HISTORY_URL,
            params={"symbol": code},
            config=config,
            source=SOURCE,
            ttl=ttl,
            client=client,
        )
        match = _ARRAY.search(body.decode("gbk", "replace"))
        payload = json.loads(match.group(0)) if match else None
        return _history_rows(code, payload) if payload else {}


_HISTORY = SinaDceHistory()


def quote_session(text: str) -> date | None:
    """The session the batch quote describes: the latest date it carries.

    A contract that did not trade keeps the date of its last trade, so the
    quote is only ever an answer for its latest session — never for an older
    day, where it would hold just the handful of contracts that stopped then.
    """
    dates = [
        body.split(",")[17] for _code, body in _QUOTE.findall(text) if len(body.split(",")) >= 18
    ]
    return max((date.fromisoformat(d) for d in dates if d), default=None)


def parse_quotes(text: str, trade_date: date) -> pl.DataFrame:
    """The batch quote's rows dated *trade_date*.

    Field positions, checked against the per-contract history for the same
    session (M2701, 2026-09-24): 2 open, 3 high, 4 low, 8 last, 9 settlement,
    10 previous settlement, 13 open interest, 14 volume, 17 date.
    """
    if quote_session(text) != trade_date:
        return pl.DataFrame()
    rows = []
    for code, body in _QUOTE.findall(text):
        fields = body.split(",")
        if len(fields) < 18 or fields[17] != trade_date.isoformat():
            continue
        contract = parse_future_code(code, EXCHANGE, trade_date)
        volume = single_sided_count(
            parse_number(fields[14]), exchange=EXCHANGE, trade_date=trade_date, field="v"
        )
        traded = bool(volume)
        rows.append(
            {
                "symbol": contract.symbol,
                "exchange": EXCHANGE,
                "exchange_code": code.lower(),
                "product": contract.product,
                "trade_date": trade_date,
                "open": parse_price(fields[2]) if traded else None,
                "high": parse_price(fields[3]) if traded else None,
                "low": parse_price(fields[4]) if traded else None,
                "close": parse_price(fields[8]) if traded else None,
                "settle": parse_price(fields[9]),
                "pre_settle": parse_price(fields[10]),
                "volume": volume,
                "amount": None,
                "open_interest": single_sided_count(
                    parse_number(fields[13]),
                    exchange=EXCHANGE,
                    trade_date=trade_date,
                    field="oi",
                ),
                "oi_change": None,
                "source": SOURCE,
            }
        )
    return (
        pl.DataFrame(drop_placeholders(rows), infer_schema_length=None) if rows else pl.DataFrame()
    )


def fetch_quotes(trade_date: date, *, config=None, client: httpx.Client) -> pl.DataFrame:
    codes = candidate_codes(trade_date)
    parts: list[str] = []
    for start in range(0, len(codes), _QUOTE_BATCH):
        batch = ",".join(f"nf_{code}" for code in codes[start : start + _QUOTE_BATCH])
        body = fetch_bytes(
            QUOTE_URL.format(codes=batch), config=config, source=SOURCE, ttl=60, client=client
        )
        parts.append(body.decode("gbk", "replace"))
    return parse_quotes("\n".join(parts), trade_date)


def fetch_dce_day(trade_date: date, *, config=None) -> ExchangeDay:
    """DCE futures for *trade_date*: the batch quote when it is dated that
    session, per-contract history otherwise."""
    if trade_date < FIRST_SESSION:
        raise FuturesDayUnavailable(f"Sina serves DCE contracts from {FIRST_SESSION}")
    from cnequity.domain.market_time import shanghai_today

    with _client() as client:
        frame = pl.DataFrame()
        # The quote only ever carries the latest session; asking it for an old
        # day in a backfill would cost three requests to learn nothing.
        from datetime import timedelta

        from cnequity.query.calendar import list_trading_dates

        today = shanghai_today()
        sessions = list_trading_dates(config, today - timedelta(days=21), today) if config else []
        latest = sessions[-1] if sessions else today
        if trade_date == latest:
            frame = fetch_quotes(trade_date, config=config, client=client)
        if frame.is_empty():
            rows = []
            for code in candidate_codes(trade_date):
                row = _HISTORY.contract(code, config=config, client=client).get(trade_date)
                if row is not None:
                    rows.append(row)
            frame = (
                pl.DataFrame(drop_placeholders(rows), infer_schema_length=None)
                if rows
                else pl.DataFrame()
            )
    if frame.is_empty():
        raise FuturesDayUnavailable(f"Sina has no DCE contracts for {trade_date.isoformat()}")
    return ExchangeDay(EXCHANGE, trade_date, futures=frame, options=pl.DataFrame())
