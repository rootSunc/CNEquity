"""Optional Tushare-only datasets: bonds, holder events, market lists and disclosures.

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
from cnequity.run_scope import backfill_of
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

    if backfill_of(config).active:
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


@register_step(
    "limit_events",
    group="capital",
    description="Stocks at the price limit each session, with seal times (Tushare)",
)
def step_limit_events(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """``limit_list_d``, one session at a time; a quiet session can be empty."""
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.market_stats import LIMIT_EVENTS_FLOOR, fetch_limit_events

    return _by_day(
        config,
        trade_date,
        run_id,
        "limit_events",
        lambda day: fetch_limit_events(day, config=config),
        floor=LIMIT_EVENTS_FLOOR,
        sparse=True,
        calendar_days=False,
    )


@register_step(
    "futures_member_positions",
    group="research",
    description="Daily futures member volume and open-interest rankings (Tushare)",
)
def step_futures_member_positions(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    """``fut_holding``, one session at a time, every exchange."""
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.futures_positions import (
        FUTURES_MEMBER_POSITIONS_FLOOR,
        fetch_futures_member_positions,
    )

    return _by_day(
        config,
        trade_date,
        run_id,
        "futures_member_positions",
        lambda day: fetch_futures_member_positions(day, config=config),
        floor=FUTURES_MEMBER_POSITIONS_FLOOR,
    )


@register_step(
    "earnings_forecasts",
    group="fundamentals",
    depends_on=["instruments"],
    description="Company earnings forecasts (业绩预告) by announcement date (Tushare)",
)
def step_earnings_forecasts(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """``forecast`` by announcement date; codes outside ``instruments`` are dropped."""
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.disclosures import (
        EARNINGS_FORECASTS_FLOOR,
        fetch_earnings_forecasts,
    )
    from cnequity.steps.common import load_symbols

    universe = sorted(set(load_symbols(config)))
    return _by_day(
        config,
        trade_date,
        run_id,
        "earnings_forecasts",
        lambda day: fetch_earnings_forecasts(day, config=config).filter(
            pl.col("symbol").is_in(universe)
        ),
        floor=EARNINGS_FORECASTS_FLOOR,
        date_col="ann_date",
        sparse=True,
    )


@register_step(
    "investor_visits",
    group="fundamentals",
    depends_on=["instruments"],
    description="Institutional investor visits to listed companies (Tushare)",
)
def step_investor_visits(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """``stk_surv`` by visit date; codes outside ``instruments`` are dropped."""
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.disclosures import (
        INVESTOR_VISITS_FLOOR,
        fetch_investor_visits,
    )
    from cnequity.steps.common import load_symbols

    universe = sorted(set(load_symbols(config)))
    return _by_day(
        config,
        trade_date,
        run_id,
        "investor_visits",
        lambda day: fetch_investor_visits(day, config=config).filter(
            pl.col("symbol").is_in(universe)
        ),
        floor=INVESTOR_VISITS_FLOOR,
        date_col="visit_date",
        sparse=True,
    )


def quarter_ends(start: date, end: date) -> list[date]:
    """Report period ends (03-31, 06-30, 09-30, 12-31) in ``[start, end]``."""
    out = []
    for year in range(start.year, end.year + 1):
        for month, day in ((3, 31), (6, 30), (9, 30), (12, 31)):
            if start <= (period := date(year, month, day)) <= end:
                out.append(period)
    return out


@register_step(
    "business_segments",
    group="fundamentals",
    depends_on=["instruments"],
    description="Revenue, cost and gross profit by product, region and industry (Tushare)",
)
def step_business_segments(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """``fina_mainbz_vip`` per report period.

    A plain run re-reads the two latest period ends, which reports are still
    arriving for; a backfill walks every period end in its window.
    """
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.client import SOURCE, TushareClient
    from cnequity.adapters.tushare.disclosures import (
        BUSINESS_SEGMENTS_FLOOR,
        fetch_business_segments,
    )
    from cnequity.steps.common import load_symbols

    window = backfill_of(config)
    if window.active:
        periods = quarter_ends(
            max(window.start or BUSINESS_SEGMENTS_FLOOR, BUSINESS_SEGMENTS_FLOOR),
            window.end or trade_date,
        )
    else:
        periods = quarter_ends(date(trade_date.year - 1, 1, 1), trade_date)[-2:]
    universe = sorted(set(load_symbols(config)))
    written = 0
    with TushareClient(config) as client:
        for period in periods:
            df = fetch_business_segments(period, config=config, client=client).filter(
                pl.col("symbol").is_in(universe)
            )
            if df.height:
                result = write_fetched(config, run_id, "business_segments", df, source=SOURCE)
                written += int(result.get("rows_written", 0))
    return {"rows_written": written, "status": "success", "periods": len(periods)}
