"""What each exchange is, and which sources can speak for it.

Beijing needed the same facts in forty places: when the exchange started,
which sources carry it, which fields a source leaves out. Each place kept
its own copy, so a new source or a corrected date had to be found in every
one of them. They live here now; ingestion, derivation and the audit read
them from this module.

Coverage is named by *capability*, not by adapter: Baostock's k-data and its
ST history are one capability, TDX's daily bars and its intraday records are
two, because they cover different exchanges.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

import polars as pl

EXCHANGES = ("SH", "SZ", "BJ")


@dataclass(frozen=True)
class MarketEra:
    """A stretch of a market's history traded under one set of rules."""

    key: str
    start: date | None
    end: date | None
    # Daily move limit for an ordinary security, as a fraction. ``None`` means
    # the era had no single limit the data can be checked against.
    price_limit: float | None
    description: str

    def contains(self, day: date) -> bool:
        return (self.start is None or day >= self.start) and (self.end is None or day <= self.end)


@dataclass(frozen=True)
class MarketProfile:
    exchange: str
    eras: tuple[MarketEra, ...]
    # (source, dataset) -> fields that source never supplies for this exchange.
    field_gaps: dict[tuple[str, str], tuple[str, ...]]
    # Daily volume and amount include block trades, so a bar's VWAP can leave
    # its own high..low range until the block trades are netted out.
    bars_include_block_trades: bool = False

    def era_on(self, day: date) -> MarketEra:
        for era in self.eras:
            if era.contains(day):
                return era
        raise ValueError(f"{self.exchange}: no market era covers {day}")

    def era(self, key: str) -> MarketEra:
        for era in self.eras:
            if era.key == key:
                return era
        raise KeyError(f"{self.exchange}: unknown market era {key!r}")


_MAIN = MarketEra(
    "main",
    None,
    None,
    None,  # board- and status-dependent (10% / 20% / 5%), not one number
    "Shanghai / Shenzhen exchange trading",
)

BJ = MarketProfile(
    exchange="BJ",
    eras=(
        MarketEra(
            "neeq",
            None,
            date(2020, 7, 26),
            None,
            "NEEQ over-the-counter quotes before the select tier: market making and "
            "call auctions, no daily limit the bars can be checked against",
        ),
        MarketEra(
            "neeq_select",
            date(2020, 7, 27),
            date(2021, 11, 12),
            0.30,
            "NEEQ select tier (精选层), continuous auction with a 30% limit",  # i18n-ignore: Chinese term in an English description
        ),
        MarketEra(
            "bse",
            date(2021, 11, 15),
            None,
            0.30,
            "Beijing Stock Exchange, 30% limit (none on a listing's first session)",
        ),
    ),
    field_gaps={("sina", "daily_bars"): ("amount",)},
    bars_include_block_trades=True,
)

PROFILES: dict[str, MarketProfile] = {
    "SH": MarketProfile("SH", (_MAIN,), {}),
    "SZ": MarketProfile("SZ", (_MAIN,), {}),
    "BJ": BJ,
}

BSE_FIRST_SESSION = BJ.era("bse").start

# capability -> exchanges it covers. A capability that is not listed is not
# known here, and asking about it raises rather than guessing.
COVERAGE: dict[str, frozenset[str]] = {
    # k-data, adjust factors, valuation and historical ST (TCP API).
    "baostock": frozenset({"SH", "SZ"}),
    # TDX minute bars and transaction records; its daily path does serve BJ.
    "tdx_intraday": frozenset({"SH", "SZ"}),
    # SSE / SZSE official boards (listing, ST and suspension status).
    "exchange_boards": frozenset({"SH", "SZ"}),
    # The BSE quotation board: current listing, names, status and quotes.
    "bse_boards": frozenset({"BJ"}),
    # CNINFO's A-share issuer directory, used to look up issuer notices.
    "cninfo_issuer_directory": frozenset({"SH", "SZ"}),
    # THS public daily bars, the last gap-fill link. Not trusted for Beijing:
    # the one week it filled there (2026-09-07..14) held the only moves past
    # the exchange's limit in its history.
    "ths_daily_bars": frozenset({"SH", "SZ"}),
    # THS official deep daily history.
    "ths_official_deep_history": frozenset({"SH", "SZ"}),
    # THS bonus pages, used for delisted Beijing corporate actions.
    "ths_delisted_actions": frozenset({"BJ"}),
    # Optional Tushare ST history (stock_st / bak_basic), used for Beijing.
    "tushare_st_history": frozenset({"BJ"}),
}


def exchange_of(symbol: str) -> str:
    return str(symbol).rpartition(".")[2].upper()


def profile_for(symbol: str) -> MarketProfile:
    return PROFILES[exchange_of(symbol)]


def serves(capability: str, symbol: str) -> bool:
    return exchange_of(symbol) in COVERAGE[capability]


def served(capability: str, symbols: Iterable[str]) -> list[str]:
    covered = COVERAGE[capability]
    return [s for s in symbols if exchange_of(s) in covered]


def unserved(capability: str, symbols: Iterable[str]) -> list[str]:
    covered = COVERAGE[capability]
    return [s for s in symbols if exchange_of(s) not in covered]


def serves_expr(capability: str, column: str = "symbol") -> pl.Expr:
    """Polars predicate: the row's security is covered by ``capability``."""
    suffixes = [f".{exchange}" for exchange in sorted(COVERAGE[capability])]
    return pl.any_horizontal([pl.col(column).str.ends_with(s) for s in suffixes])


# Exchanges whose back-adjusted factor is computed from the lake's corporate
# actions rather than taken from Sina (which stays as a cross-check).
COMPUTED_FACTOR_EXCHANGES = frozenset({"BJ"})

# Datasets whose Beijing rows before a security's exchange start are NEEQ
# over-the-counter quotes: kept in the lake, left out of default reads.
OTC_SCOPED_DATASETS = frozenset({"daily_bars", "adj_factors"})

#: Market-wide daily history starts here: the primary vendor serves 2016 on,
#: and the deep-history step (同花顺 per-year files) reaches back to 2001.
HISTORY_BACKFILL_START = date(2001, 1, 1)


def exchange_start(list_date: pl.Expr) -> pl.Expr:
    """First day a Beijing security traded on an exchange rather than NEEQ OTC.

    The later of its listing date and the select tier's opening; a listing
    date recorded before 2020-07-27 is a NEEQ listing, not an exchange one.
    """
    floor = BJ.era("neeq_select").start
    return (
        pl.when(list_date.is_null() | (list_date < pl.lit(floor)))
        .then(pl.lit(floor))
        .otherwise(list_date)
    )


def otc_quote_expr(
    *, symbol: str = "symbol", day: str = "trade_date", list_date: str = "list_date"
) -> pl.Expr:
    """Polars predicate: a Beijing row from before its exchange start."""
    return pl.col(symbol).str.ends_with(f".{BJ.exchange}") & (
        pl.col(day) < exchange_start(pl.col(list_date))
    )


def otc_quote_sql(symbol: str, day: str, list_date: str) -> str:
    """The same predicate in DuckDB SQL, for the views."""
    floor = BJ.era("neeq_select").start.isoformat()
    return (
        f"({symbol} LIKE '%.{BJ.exchange}' AND {day} < "
        f"GREATEST(COALESCE({list_date}, DATE '{floor}'), DATE '{floor}'))"
    )


def block_trade_bars_expr(column: str = "symbol") -> pl.Expr:
    """Polars predicate: the security's daily bars include block trades."""
    suffixes = [f".{p.exchange}" for p in PROFILES.values() if p.bars_include_block_trades]
    if not suffixes:
        return pl.lit(False)
    return pl.any_horizontal([pl.col(column).str.ends_with(s) for s in suffixes])


def unserved_exchanges(capability: str) -> tuple[str, ...]:
    return tuple(e for e in EXCHANGES if e not in COVERAGE[capability])
