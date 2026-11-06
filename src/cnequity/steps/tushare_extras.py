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


def _per_code(
    config: Config,
    run_id: str,
    dataset: str,
    codes: list[str],
    fetch_one: Callable[[str], pl.DataFrame],
    *,
    flush_every: int = 200,
) -> dict:
    """Fetch each code's whole history and stage it in chunks; for date-blind APIs."""
    from cnequity.adapters.tushare.client import SOURCE

    written, buffer = 0, []
    for i, code in enumerate(codes, 1):
        frame = fetch_one(code)
        if frame.height:
            buffer.append(frame)
        if buffer and (i % flush_every == 0 or i == len(codes)):
            result = write_fetched(config, run_id, dataset, pl.concat(buffer), source=SOURCE)
            written += int(result.get("rows_written", 0))
            buffer = []
    return {"rows_written": written, "status": "success", "codes": len(codes)}


def _stock_codes(config: Config) -> list[str]:
    from cnequity.steps.common import instrument_metadata

    meta = instrument_metadata(config)
    return sorted(meta.filter(pl.col("asset_type") == "stock")["symbol"].unique().to_list())


@register_step(
    "hot_money_traders",
    group="capital",
    description="Hot-money trader roster with the seats they use (Tushare)",
)
def step_hot_money_traders(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """Re-read the whole ``hm_list`` roster."""
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.hot_money import SOURCE, fetch_hot_money_traders

    df = fetch_hot_money_traders(config=config)
    if df.is_empty():
        raise SourceUnavailableError("hot_money_traders: tushare returned no rows")
    return write_fetched(config, run_id, "hot_money_traders", df, source=SOURCE)


@register_step(
    "hot_money_trades",
    group="capital",
    description="Hot-money traders' dragon-tiger trades per session (Tushare)",
)
def step_hot_money_trades(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """``hm_detail``, one session at a time."""
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.hot_money import HOT_MONEY_TRADES_FLOOR, fetch_hot_money_trades

    return _by_day(
        config,
        trade_date,
        run_id,
        "hot_money_trades",
        lambda day: fetch_hot_money_trades(day, config=config),
        floor=HOT_MONEY_TRADES_FLOOR,
        sparse=True,
        calendar_days=False,
    )


def _bond_events_step(dataset: str):
    def step(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
        if not _enabled(config):
            return dict(_SKIPPED)
        from cnequity.adapters.tushare.client import TushareClient
        from cnequity.adapters.tushare.convertible_bond_events import fetch_bond_events
        from cnequity.adapters.tushare.convertible_bonds import fetch_bonds

        with TushareClient(config) as client:
            codes = sorted(fetch_bonds(config, client=client)["symbol"].to_list())
            return _per_code(
                config,
                run_id,
                dataset,
                codes,
                lambda code: fetch_bond_events(dataset, code, config=config, client=client),
            )

    step.__doc__ = f"Every bond's whole ``{dataset}`` history, one bond per request."
    return step


for _name, _description in (
    ("convertible_bond_calls", "Convertible bond redemption announcements (Tushare)"),
    ("convertible_bond_conversions", "Convertible bond conversion reports (Tushare)"),
    ("convertible_bond_coupons", "Convertible bond coupon rates per interest year (Tushare)"),
):
    register_step(_name, group="research", description=_description)(_bond_events_step(_name))


def _sentiment_day_step(dataset: str, floor_name: str):
    def step(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
        if not _enabled(config):
            return dict(_SKIPPED)
        from cnequity.adapters.tushare import sentiment_lists

        return _by_day(
            config,
            trade_date,
            run_id,
            dataset,
            lambda day: sentiment_lists.fetch_day(dataset, day, config=config),
            floor=getattr(sentiment_lists, floor_name),
            sparse=True,
            calendar_days=False,
        )

    step.__doc__ = f"``{dataset}``, one session at a time."
    return step


for _name, _floor, _group, _description in (
    (
        "popularity_ranks",
        "POPULARITY_RANKS_FLOOR",
        "research",
        "THS and EastMoney popularity lists (Tushare)",
    ),
    (
        "limit_ladder",
        "LIMIT_LADDER_FLOOR",
        "capital",
        "Consecutive limit-up runs per session (Tushare)",
    ),
    (
        "strongest_sectors",
        "STRONGEST_SECTORS_FLOOR",
        "research",
        "THS concept boards ranked by limit-ups (Tushare)",
    ),
    (
        "kpl_limit_ups",
        "KPL_LIMIT_UPS_FLOOR",
        "capital",
        "开盘啦 limit-up list with seal times and reasons (Tushare)",
    ),
    (
        "fund_flow_ths_tushare",
        "FUND_FLOW_THS_TUSHARE_FLOOR",
        "capital",
        "THS net flow per stock by order size (Tushare)",
    ),
):
    register_step(_name, group=_group, description=_description)(_sentiment_day_step(_name, _floor))


@register_step(
    "investor_qa",
    group="research",
    depends_on=["instruments"],
    description="Investor questions and company answers on SSE/SZSE platforms (Tushare)",
)
def step_investor_qa(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """``irm_qa_sh`` and ``irm_qa_sz`` by publication day, calendar days included."""
    if not _enabled(config):
        return dict(_SKIPPED)
    from cnequity.adapters.tushare.company_people import INVESTOR_QA_FLOOR, fetch_investor_qa

    return _by_day(
        config,
        trade_date,
        run_id,
        "investor_qa",
        lambda day: fetch_investor_qa(day, config=config),
        floor=INVESTOR_QA_FLOOR,
        sparse=True,
    )


def _people_step(dataset: str, fetch_name: str):
    def step(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
        if not _enabled(config):
            return dict(_SKIPPED)
        from cnequity.adapters.tushare import company_people
        from cnequity.adapters.tushare.client import TushareClient

        fetch = getattr(company_people, fetch_name)
        codes = backfill_of(config).symbol_list() or _stock_codes(config)
        with TushareClient(config) as client:
            return _per_code(
                config,
                run_id,
                dataset,
                codes,
                lambda code: fetch(code, config=config, client=client),
            )

    step.__doc__ = f"Every stock's whole ``{dataset}`` history, one stock per request."
    return step


for _name, _fetch, _group, _description in (
    (
        "executives",
        "fetch_executives",
        "fundamentals",
        "Announced board members, supervisors and executives (Tushare)",
    ),
    (
        "executive_compensation",
        "fetch_executive_compensation",
        "fundamentals",
        "Executive pay and shareholdings per report period (Tushare)",
    ),
):
    register_step(_name, group=_group, depends_on=["instruments"], description=_description)(
        _people_step(_name, _fetch)
    )
