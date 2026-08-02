"""Fetch straight from the source for an agent that has no lake. Nothing is written.

WHAT THIS IS FOR. `asl mcp --live` lets someone try the agent surface without
first spending hours on `asl init`. It is the on-ramp, not the destination.

WHAT IT COSTS, AND WHY THAT IS SAID OUT LOUD. A live fetch is a vendor's current
answer to one question. It has:

* **no adjustment factors** — those are derived from a separate Sina series the
  lake stores; without them a price series crossing an ex-dividend date is wrong
  and there is no way to notice from the numbers.
* **no universe filter** — `trading_status` is a dataset, not a field on a bar,
  so nothing here can drop names that were suspended or ST that day.
* **no point-in-time** — the vendor returns today's view of a restated figure.
  There is no honest `as_of`, so fundamentals refuse to serve live at all rather
  than quietly answer a 2018 question with 2026 knowledge.
* **no write-time validation and no provenance** — the lake's schema check and
  its `source` / `fetched_at` columns happen on the way to disk, and nothing
  goes to disk here.

So the live path serves exactly two things — symbol lookup and unadjusted daily
bars — and every payload it produces carries ``origin: "live"`` and a warning
saying which of the guarantees above are absent. A model that cannot tell the
two apart will spend lake-grade confidence on vendor-grade data, and that is a
worse failure than having no data at all: it looks like an answer.

The caps below exist because an agent can loop. These are the same hosts a daily
pipeline depends on, and an MCP server that lets a model sweep the market on a
whim would earn the user a rate-limit ban for a question they did not ask.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import polars as pl

from ashare_lake.config import Config

logger = logging.getLogger(__name__)

# Per-call ceilings. Generous enough for the questions this path is meant to
# answer (a handful of names, a year or two), small enough that a runaway loop
# costs the vendor a rate limit rather than a block.
MAX_SYMBOLS = 50
MAX_DAYS = 800
DEFAULT_DAYS = 60

# What the live path can honestly produce. Everything else refuses by name, so
# the agent is told which tool to stop calling rather than getting an empty
# result it will read as "this did not happen".
SUPPORTED = ("resolve_symbol", "query_bars:daily_bars")

LIVE_WARNING = (
    "origin=live: fetched from the vendor just now and NOT stored. No adjustment "
    "factors (so any comparison across an ex-dividend or split date is wrong), no "
    "universe filter (suspended and ST names are included), no point-in-time "
    "guarantee, and no write-time validation or provenance. Treat as a quote "
    "screen, not as research data. Build a lake (`asl init`) for anything that "
    "needs a correct history."
)


class LiveUnavailable(RuntimeError):
    """This question cannot be answered honestly without a lake."""


def enabled(config: Config) -> bool:
    return bool(getattr(config, "_mcp_live", False))


def _guard(symbols: list[str] | None, start: date, end: date) -> None:
    if not symbols:
        raise LiveUnavailable(
            "live mode needs an explicit `symbols` list — there is no lake to scan, "
            "and sweeping the whole market on a model's initiative is how a user "
            "earns a rate-limit ban for a question they did not ask. "
            "Use resolve_symbol first."
        )
    if len(symbols) > MAX_SYMBOLS:
        raise LiveUnavailable(
            f"live mode fetches at most {MAX_SYMBOLS} symbols per call, got {len(symbols)}. "
            "Split the request, or build a lake for cross-sectional work."
        )
    span = (end - start).days
    if span > MAX_DAYS:
        raise LiveUnavailable(
            f"live mode fetches at most {MAX_DAYS} days per call, got {span}. "
            "Deep history is what a lake is for."
        )


def window(start: str | None, end: str | None) -> tuple[date, date]:
    """Resolve the requested window, defaulting to a short recent one.

    An unbounded default would have every casual question walk a decade of
    pages off a vendor that is paced at 100ms a request.
    """
    end_d = date.fromisoformat(end) if end else date.today()
    start_d = date.fromisoformat(start) if start else end_d - timedelta(days=DEFAULT_DAYS)
    return start_d, end_d


def daily_bars(
    config: Config,
    *,
    symbols: list[str] | None,
    start: str | None,
    end: str | None,
) -> pl.DataFrame:
    """Unadjusted daily bars, straight off the quote protocol."""
    from ashare_lake.adapters.tdx_protocol.client import fetch_daily_bars

    start_d, end_d = window(start, end)
    _guard(symbols, start_d, end_d)
    logger.info("live fetch: %d symbol(s) %s..%s", len(symbols), start_d, end_d)
    return fetch_daily_bars(
        symbols,
        start_d,
        end_d,
        rate_limit=config.tdx_rate_limit_spec(),
        allow_mock=config.tdx_allow_mock,
        config=config,
    )


def instruments(config: Config) -> pl.DataFrame:
    """The current security master. Live, so delisted names are simply absent.

    That absence is the survivorship gap this project exists to close, which is
    why the caller labels the result rather than presenting it as the universe.
    """
    from ashare_lake.adapters.tdx_protocol.client import fetch_instruments

    return fetch_instruments(
        rate_limit=config.tdx_rate_limit_spec(),
        allow_mock=config.tdx_allow_mock,
        config=config,
    )
