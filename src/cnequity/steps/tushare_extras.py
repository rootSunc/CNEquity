"""Optional Tushare-only datasets: convertible bonds, holder events, chip and top-10 data.

No schedule group lists these steps; they run through ``cne backfill <name>``
or an explicit ``cne run step``. Without a Tushare token each one sends nothing
and reports skipped.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.orchestrator.outcomes import SourceUnavailableError
from cnequity.orchestrator.registry import register_step
from cnequity.steps.http_common import write_fetched

_SKIPPED = {"rows_written": 0, "status": "skipped", "reason": "tushare not configured"}


def _by_day(
    config: Config,
    trade_date: date,
    run_id: str,
    dataset: str,
    fetch_one: Callable[[date], pl.DataFrame],
    *,
    floor: date,
    date_col: str = "trade_date",
    sparse: bool = False,
    calendar_days: bool | None = None,
) -> dict:
    """Backfill walks the window; a plain run fetches *trade_date* alone.

    *sparse* datasets (announcements, weekly pledge totals) have days without
    rows, so an empty day is normal for them; by default the walk then visits
    calendar days, since announcements also fall on non-trading days.
    """
    from cnequity.adapters.tushare.client import SOURCE
    from cnequity.steps.common import walk_day_backfill

    if getattr(config, "_backfill", False):
        return walk_day_backfill(
            config,
            trade_date,
            run_id,
            dataset,
            fetch_one,
            source=SOURCE,
            date_col=date_col,
            floor=floor,
            calendar_days=sparse if calendar_days is None else calendar_days,
            allow_empty_days=sparse,
        )
    df = fetch_one(trade_date)
    if df.is_empty():
        if sparse:
            return {"rows_written": 0, "status": "success", "reason": "no rows for the day"}
        raise SourceUnavailableError(f"{dataset}: tushare has no rows for {trade_date}")
    return write_fetched(config, run_id, dataset, df, source=SOURCE)


def _enabled(config: Config) -> bool:
    from cnequity.adapters.tushare.client import tushare_enabled

    return tushare_enabled(config)


@register_step(
    "convertible_bonds",
    group="research",
    description="Convertible and exchangeable bond terms (Tushare)",
)
def step_convertible_bonds(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """Re-read every bond's terms and offering from ``cb_basic`` and ``cb_issue``."""
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.convertible_bonds import SOURCE, fetch_bonds

    df = fetch_bonds(config)
    if df.is_empty():
        raise SourceUnavailableError("convertible_bonds: tushare returned no rows")
    return write_fetched(config, run_id, "convertible_bonds", df, source=SOURCE)


@register_step(
    "convertible_bond_bars",
    group="research",
    description="Daily convertible bond bars (Tushare)",
)
def step_convertible_bond_bars(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    """``cb_daily``, one session at a time."""
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.convertible_bonds import (
        CONVERTIBLE_BOND_BARS_FLOOR,
        fetch_bars,
    )

    return _by_day(
        config,
        trade_date,
        run_id,
        "convertible_bond_bars",
        lambda day: fetch_bars(day, config=config),
        floor=CONVERTIBLE_BOND_BARS_FLOOR,
    )


@register_step(
    "holder_trades",
    group="fundamentals",
    depends_on=["instruments"],
    description="Announced major-holder and executive trades (Tushare)",
)
def step_holder_trades(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """``stk_holdertrade`` by announcement date; codes outside ``instruments`` are dropped."""
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.holder_events import HOLDER_TRADES_FLOOR, fetch_holder_trades
    from cnequity.steps.common import load_symbols

    universe = sorted(set(load_symbols(config)))
    return _by_day(
        config,
        trade_date,
        run_id,
        "holder_trades",
        lambda day: fetch_holder_trades(day, config=config).filter(
            pl.col("symbol").is_in(universe)
        ),
        floor=HOLDER_TRADES_FLOOR,
        date_col="ann_date",
        sparse=True,
    )


@register_step(
    "share_repurchases",
    group="fundamentals",
    depends_on=["instruments"],
    description="Share buyback progress announcements (Tushare)",
)
def step_share_repurchases(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """``repurchase`` by announcement date; codes outside ``instruments`` are dropped."""
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.holder_events import (
        SHARE_REPURCHASES_FLOOR,
        fetch_share_repurchases,
    )
    from cnequity.steps.common import load_symbols

    universe = sorted(set(load_symbols(config)))
    return _by_day(
        config,
        trade_date,
        run_id,
        "share_repurchases",
        lambda day: fetch_share_repurchases(day, config=config).filter(
            pl.col("symbol").is_in(universe)
        ),
        floor=SHARE_REPURCHASES_FLOOR,
        date_col="ann_date",
        sparse=True,
    )


@register_step(
    "pledge_stats",
    group="macro_risk",
    depends_on=["instruments"],
    description="Weekly share-pledge totals per stock (Tushare)",
)
def step_pledge_stats(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """``pledge_stat`` by week-end date; other days are empty."""
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.holder_events import PLEDGE_STATS_FLOOR, fetch_pledge_stats
    from cnequity.steps.common import load_symbols

    universe = sorted(set(load_symbols(config)))
    return _by_day(
        config,
        trade_date,
        run_id,
        "pledge_stats",
        lambda day: fetch_pledge_stats(day, config=config).filter(pl.col("symbol").is_in(universe)),
        floor=PLEDGE_STATS_FLOOR,
        date_col="end_date",
        sparse=True,
        calendar_days=False,
    )


@register_step(
    "chip_distribution",
    group="capital",
    depends_on=["instruments"],
    description="Holding-cost distribution estimate per stock (Tushare)",
)
def step_chip_distribution(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """``cyq_perf``, one session at a time."""
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.market_stats import (
        CHIP_DISTRIBUTION_FLOOR,
        fetch_chip_distribution,
    )
    from cnequity.steps.common import load_symbols

    universe = sorted(set(load_symbols(config)))
    return _by_day(
        config,
        trade_date,
        run_id,
        "chip_distribution",
        lambda day: fetch_chip_distribution(day, config=config).filter(
            pl.col("symbol").is_in(universe)
        ),
        floor=CHIP_DISTRIBUTION_FLOOR,
    )


@register_step(
    "northbound_top10",
    group="capital",
    description="Ten most traded Connect stocks per channel (Tushare)",
)
def step_northbound_top10(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """``hsgt_top10``; Connect is closed on some A-share sessions, so empty days are normal."""
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.market_stats import (
        NORTHBOUND_TOP10_FLOOR,
        fetch_northbound_top10,
    )

    return _by_day(
        config,
        trade_date,
        run_id,
        "northbound_top10",
        lambda day: fetch_northbound_top10(day, config=config),
        floor=NORTHBOUND_TOP10_FLOOR,
        sparse=True,
        calendar_days=False,
    )
