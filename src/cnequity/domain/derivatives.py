"""Futures and option contract identity, and the lake's counting basis for them.

Derivatives live beside the equity universe rather than inside it.
The equity symbol grammar in ``domain/symbols.py`` is deliberately not widened:
nothing here imports it, and nothing there knows these suffixes.

Canonical symbols
-----------------
Futures are ``{PRODUCT}{YYMM}.{EXCH}`` — ``CU2511.SHF``, ``TA2601.CZC``,
``IF2512.CFE``. Options add the side and the strike: ``CU2511C80000.SHF``,
``M2601C3000.DCE``, ``IO2512C4000.CFE``. The exchange suffixes are the ones
``commodity_bars`` already uses, plus ``CFE`` for CFFEX. The exchange's own
code is kept separately (``exchange_code``); this module only maps between the
two.

CZCE codes carry a single year digit (``TA601``), and the same code has named a
2006, a 2016 and a 2026 contract. The year is therefore resolved against the
trading day the code was observed on: a contract is never quoted after its
delivery month, and none is listed ten years ahead, so exactly one decade fits.

Counting basis
--------------
Until 2019-12-31 SHFE (INE with it), DCE and CZCE counted volume, open interest
and turnover on both sides of each trade; from 2020-01-01 on one side, which is
how CFFEX always counted. Measured across the switch: CZCE MA004 open interest
48 → 24 and SHFE rb2002 14,804 → 7,423 between the two sessions, and Sina
serves the same double-sided figures unchanged. The lake stores one side
throughout, converting at the adapter boundary. A double-sided count is two
copies of one trade, so it is always even; an odd one means the premise is
wrong for that row and the conversion refuses it rather than rounding.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

__all__ = [
    "CFFEX_INDEX_UNDERLYINGS",
    "EXCHANGES",
    "EXCHANGE_NAMES",
    "SINGLE_SIDED_SINCE",
    "PUBLISHED_FUTURES_ANOMALIES",
    "UNPUBLISHED_FUTURES_SESSIONS",
    "WAN_YUAN",
    "CountingBasisError",
    "FutureContract",
    "OptionContract",
    "format_strike",
    "is_double_sided",
    "parse_future_code",
    "parse_option_code",
    "percent_to_fraction",
    "single_sided_amount",
    "single_sided_count",
]

#: Lake suffix for every exchange that lists futures or options.
EXCHANGES: tuple[str, ...] = ("SHF", "INE", "DCE", "CZC", "GFE", "CFE")

EXCHANGE_NAMES: dict[str, str] = {
    "SHF": "上海期货交易所",  # i18n-ignore: official exchange name
    "INE": "上海国际能源交易中心",  # i18n-ignore: official exchange name
    "DCE": "大连商品交易所",  # i18n-ignore: official exchange name
    "CZC": "郑州商品交易所",  # i18n-ignore: official exchange name
    "GFE": "广州期货交易所",  # i18n-ignore: official exchange name
    "CFE": "中国金融期货交易所",  # i18n-ignore: official exchange name
}

#: CFFEX index options are written on the spot index, not on a futures
#: contract; these are the ``index_bars`` symbols they settle against.
CFFEX_INDEX_UNDERLYINGS: dict[str, str] = {
    "IO": "000300.SH",
    "MO": "000852.SH",
    "HO": "000016.SH",
}

#: Trading sessions an exchange's own futures archive has no file for. Each was
#: checked by hand (2026-09-26): the day answers 404 while the sessions either
#: side answer 200, and SHFE's retired ``data/dailydata/kx`` path is gone for
#: every day. CZCE's ``exchange/{y}/datadaily`` archive likewise 404s on three
#: 2010 sessions every other exchange traded (rechecked 2026-10-24; its old
#: ``jyxx/hq`` pages only answer 412). No backfill can fill these, so
#: completeness checks excuse them.
UNPUBLISHED_FUTURES_SESSIONS: dict[str, frozenset[date]] = {
    "SHF": frozenset({date(2004, 6, 25), date(2007, 6, 4)}),
    "CZC": frozenset({date(2010, 6, 21), date(2010, 7, 1), date(2010, 8, 13)}),
}

#: Rows an exchange's own file publishes in a form no bar can take. They stay
#: quarantined, but refetching serves the same bytes, so they must not hold the
#: session in debt. Keyed by session, not route: the SHFE and INE files both
#: carry INE contracts. INE kx20210728.dat: LU2108 traded one lot at 3180
#: (turnover 3.18 万元) yet carries CLOSEPRICE 3450, its settlement price.
PUBLISHED_FUTURES_ANOMALIES: dict[date, frozenset[str]] = {
    date(2021, 7, 28): frozenset({"LU2108.INE"}),
}

#: First session counted on one side at SHFE, INE, DCE and CZCE.
SINGLE_SIDED_SINCE = date(2020, 1, 1)
_DOUBLE_SIDED_EXCHANGES = frozenset({"SHF", "INE", "DCE", "CZC"})

#: The exchanges publish turnover in 万元; the lake stores 元.
WAN_YUAN = 10_000.0

_FUTURE_CODE = re.compile(r"^([A-Za-z]{1,3})(\d{3,4})$")
# cu2511C80000, sc2502C460, TA601C5400, m2601-C-3000, si2611-C-9000,
# IO2512-C-4000. The separators are optional and the side is one letter. CZCE
# also lists a second expiry series on some underlyings, marked with two
# letters before the side (CF701MSC14200, 2026); the tag is part of identity.
_OPTION_CODE = re.compile(r"^([A-Za-z]{1,3})(\d{3,4})([A-Z]{2})?-?([CP])-?(\d+(?:\.\d+)?)$")


class CountingBasisError(ValueError):
    """A double-sided count that cannot be two copies of one trade."""


@dataclass(frozen=True, order=True)
class FutureContract:
    """One delivery month of one product on one exchange."""

    exchange: str
    product: str
    year: int
    month: int

    def __post_init__(self) -> None:
        if self.exchange not in EXCHANGES:
            raise ValueError(f"unknown futures exchange suffix: {self.exchange!r}")
        if not 1 <= self.month <= 12:
            raise ValueError(f"invalid delivery month: {self.month!r}")

    @property
    def yymm(self) -> str:
        return f"{self.year % 100:02d}{self.month:02d}"

    @property
    def symbol(self) -> str:
        return f"{self.product}{self.yymm}.{self.exchange}"

    @property
    def delivery_month(self) -> date:
        return date(self.year, self.month, 1)


@dataclass(frozen=True)
class OptionContract:
    """One strike on one side of one expiry series."""

    series: FutureContract
    option_type: str
    strike: float
    #: Distinguishes a second expiry series on the same underlying ("MS").
    series_tag: str = ""

    def __post_init__(self) -> None:
        if self.option_type not in {"C", "P"}:
            raise ValueError(f"option_type must be 'C' or 'P', got {self.option_type!r}")
        if not self.strike > 0:
            raise ValueError(f"strike must be positive, got {self.strike!r}")

    @property
    def exchange(self) -> str:
        return self.series.exchange

    @property
    def product(self) -> str:
        return self.series.product

    @property
    def symbol(self) -> str:
        return (
            f"{self.series.product}{self.series.yymm}{self.series_tag}{self.option_type}"
            f"{format_strike(self.strike)}.{self.series.exchange}"
        )

    @property
    def underlying_symbol(self) -> str:
        """The futures contract (or, for CFFEX, the index) the option is on."""
        index = CFFEX_INDEX_UNDERLYINGS.get(self.series.product)
        if self.series.exchange == "CFE" and index is not None:
            return index
        return self.series.symbol

    @property
    def underlying_kind(self) -> str:
        if self.series.exchange == "CFE" and self.series.product in CFFEX_INDEX_UNDERLYINGS:
            return "index"
        return "future"


def format_strike(strike: float) -> str:
    """Render a strike without a trailing ``.0`` or float noise."""
    text = format(Decimal(str(round(float(strike), 6))).normalize(), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _resolve_year(digits: str, month: int, observed_on: date) -> int:
    if len(digits) == 2:
        return 2000 + int(digits)
    last = int(digits)
    for year in range(observed_on.year, observed_on.year + 10):
        if year % 10 == last and (year, month) >= (observed_on.year, observed_on.month):
            return year
    raise ValueError(f"no year ending in {last} fits month {month} on {observed_on}")


def _split_year_month(code_digits: str, observed_on: date) -> tuple[int, int]:
    month = int(code_digits[-2:])
    if not 1 <= month <= 12:
        raise ValueError(f"invalid delivery month in {code_digits!r}")
    return _resolve_year(code_digits[:-2], month, observed_on), month


def parse_future_code(code: str, exchange: str, observed_on: date) -> FutureContract:
    """Map an exchange futures code to its contract.

    ``observed_on`` is the trading day the code appeared on; it is only needed
    for CZCE's one-digit years, and is what makes that resolution exact.
    """
    match = _FUTURE_CODE.match(str(code).strip())
    if match is None:
        raise ValueError(f"not a futures contract code: {code!r}")
    product, digits = match.groups()
    if len(digits) == 3 and exchange != "CZC":
        raise ValueError(f"{exchange} codes carry a two-digit year: {code!r}")
    year, month = _split_year_month(digits, observed_on)
    return FutureContract(exchange=exchange, product=product.upper(), year=year, month=month)


def parse_option_code(code: str, exchange: str, observed_on: date) -> OptionContract:
    """Map an exchange option code to its contract."""
    match = _OPTION_CODE.match(str(code).strip())
    if match is None:
        raise ValueError(f"not an option contract code: {code!r}")
    product, digits, tag, side, strike = match.groups()
    if len(digits) == 3 and exchange != "CZC":
        raise ValueError(f"{exchange} codes carry a two-digit year: {code!r}")
    year, month = _split_year_month(digits, observed_on)
    series = FutureContract(exchange=exchange, product=product.upper(), year=year, month=month)
    return OptionContract(
        series=series, option_type=side, strike=float(strike), series_tag=tag or ""
    )


def is_double_sided(exchange: str, trade_date: date) -> bool:
    """Whether *exchange* published two-sided counts for *trade_date*."""
    return exchange in _DOUBLE_SIDED_EXCHANGES and trade_date < SINGLE_SIDED_SINCE


def single_sided_count(
    value: float | int | None, *, exchange: str, trade_date: date, field: str
) -> int | None:
    """Volume / open interest / its change, on the lake's one-sided basis."""
    if value is None:
        return None
    number = float(value)
    if not number.is_integer():
        raise CountingBasisError(f"{field}={value!r} is not a whole number of contracts")
    count = int(number)
    if not is_double_sided(exchange, trade_date):
        return count
    if count % 2:
        raise CountingBasisError(
            f"{exchange} {trade_date.isoformat()} {field}={count} is odd, so it cannot be a "
            "double-sided count"
        )
    return count // 2


def single_sided_amount(
    value_wan: float | None, *, exchange: str, trade_date: date
) -> float | None:
    """Turnover in 元 on one side, from the exchange's 万元 figure."""
    if value_wan is None:
        return None
    amount = float(value_wan) * WAN_YUAN
    return amount / 2 if is_double_sided(exchange, trade_date) else amount


def percent_to_fraction(value: float | None) -> float | None:
    """Implied volatility published in percent points (23.90) as a fraction."""
    return None if value is None else float(value) / 100.0
