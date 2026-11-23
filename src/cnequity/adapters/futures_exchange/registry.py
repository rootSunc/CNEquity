"""Exchange capabilities and reader routes, independent of job scheduling."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

import polars as pl

from cnequity.adapters.futures_exchange import SUPPORTED_EXCHANGES, cffex, czce, dce, gfex, shfe
from cnequity.adapters.futures_exchange.common import ExchangeDay
from cnequity.adapters.sina import dce_futures
from cnequity.config import Config

Kind = str  # "futures" | "options"


@dataclass(frozen=True)
class ExchangeReader:
    """How to read one exchange, and from which session each kind exists."""

    exchange: str
    first_futures_session: date
    first_options_session: date | None
    fetch_day: Callable[..., ExchangeDay]
    fetch_reference: Callable[..., pl.DataFrame] | None = None

    reference_history: bool = True
    separate_kinds: bool = False

    def fetch(self, day: date, *, kind: Kind, config) -> ExchangeDay:
        if self.separate_kinds:
            return self.fetch_day(day, config=config, kind=kind)
        return self.fetch_day(day, config=config)

    def first_session(self, kind: Kind) -> date | None:
        return self.first_futures_session if kind == "futures" else self.first_options_session


READERS: dict[str, ExchangeReader] = {
    "SHF": ExchangeReader(
        exchange="SHF",
        first_futures_session=shfe.FIRST_SESSION,
        first_options_session=shfe.FIRST_OPTION_SESSION,
        fetch_day=shfe.fetch_shfe_day,
        separate_kinds=True,
        fetch_reference=shfe.fetch_shfe_reference,
    ),
    "CZC": ExchangeReader(
        exchange="CZC",
        first_futures_session=czce.FIRST_SESSION,
        first_options_session=czce.FIRST_OPTION_SESSION,
        fetch_day=czce.fetch_czce_day,
        separate_kinds=True,
        fetch_reference=czce.fetch_czce_reference,
    ),
    "GFE": ExchangeReader(
        exchange="GFE",
        first_futures_session=gfex.FIRST_SESSION,
        first_options_session=gfex.FIRST_OPTION_SESSION,
        fetch_day=gfex.fetch_gfex_day,
        separate_kinds=True,
        fetch_reference=gfex.fetch_gfex_reference,
        reference_history=False,
    ),
    # DCE's own endpoints answer with an access challenge, so the default route
    # is Sina: futures only, contracts listed from mid-2018.
    "DCE": ExchangeReader(
        exchange="DCE",
        first_futures_session=dce_futures.FIRST_SESSION,
        first_options_session=None,
        fetch_day=dce_futures.fetch_dce_day,
    ),
    "CFE": ExchangeReader(
        exchange="CFE",
        first_futures_session=cffex.FIRST_SESSION,
        first_options_session=cffex.FIRST_OPTION_SESSION,
        fetch_day=cffex.fetch_cffex_day,
        fetch_reference=cffex.fetch_cffex_params,
    ),
}

#: `[futures] dce_route = "official"`: DCE's own file, not yet seen answering.
DCE_OFFICIAL = ExchangeReader(
    exchange="DCE",
    first_futures_session=date(2000, 1, 4),
    first_options_session=date(2017, 3, 31),
    fetch_day=dce.fetch_dce_official_day,
    separate_kinds=True,
)


def reader(config: Config, exchange: str) -> ExchangeReader:
    """The reader for *exchange* under this config's routes."""
    if exchange == "DCE" and getattr(config, "futures_dce_route", "sina") == "official":
        return DCE_OFFICIAL
    return READERS[exchange]


def enabled_exchanges(config: Config) -> list[str]:
    """Configured exchanges, in publication order; empty config means all."""
    wanted = {
        "SHF" if e == "INE" else e
        for e in (getattr(config, "futures_exchanges", None) or SUPPORTED_EXCHANGES)
    }
    if getattr(config, "futures_dce_route", "sina") == "off":
        wanted.discard("DCE")
    return [e for e in SUPPORTED_EXCHANGES if e in wanted and e in READERS]


def expected_exchanges(config: Config, kind: Kind, day: date) -> list[str]:
    """Exchanges that should have a *kind* file for *day*."""
    out = []
    for exchange in enabled_exchanges(config):
        first = reader(config, exchange).first_session(kind)
        if first is not None and day >= first:
            out.append(exchange)
    return out


def earliest_session(config: Config, kind: Kind) -> date | None:
    firsts = [
        reader(config, e).first_session(kind)
        for e in enabled_exchanges(config)
        if reader(config, e).first_session(kind) is not None
    ]
    return min(firsts) if firsts else None


def capabilities() -> list[dict]:
    """Serializable source capabilities shared by documentation and clients.

    Dates describe adapter routing bounds, never verified lake coverage.
    """
    rows = []
    for key, route in [*READERS.items(), ("DCE", DCE_OFFICIAL)]:
        rows.append(
            {
                "exchange": key,
                "route": "official" if route is DCE_OFFICIAL or key != "DCE" else "sina",
                "futures_since": route.first_futures_session.isoformat(),
                "options_since": route.first_options_session.isoformat()
                if route.first_options_session
                else None,
                "reference": route.fetch_reference is not None,
                "reference_history": route.fetch_reference is not None and route.reference_history,
                "status": "experimental" if route is DCE_OFFICIAL else "supported",
                "limitations": "live payload mapping unverified"
                if route is DCE_OFFICIAL
                else "missing zero-trade days and turnover; no options"
                if key == "DCE"
                else "source bounds do not prove continuous historical coverage",
            }
        )
    return rows
