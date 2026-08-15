"""News wire + economic calendar archive steps (daily batch)."""

from __future__ import annotations

from datetime import date

from cn_market_lake.adapters.eastmoney.economic_calendar import fetch_economic_calendar
from cn_market_lake.adapters.eastmoney.news_wire import fetch_flash_news_wire
from cn_market_lake.config import Config
from cn_market_lake.orchestrator.registry import register_step
from cn_market_lake.steps.http_common import empty_ok, run_incremental_fetched, write_fetched


@register_step("flash_news_wire", group="research")
def step_flash_news_wire(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("flash_news_wire: eastmoney source disabled in config")
    # Fail-loud on empty: an empty success left the dataset unregistered in curated
    # and permanently failed lake_health (exists error) while the step looked green.
    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "flash_news_wire",
        fetch_flash_news_wire,
        source="eastmoney",
        allow_empty=False,
    )


@register_step("economic_calendar", group="macro_risk")
def step_economic_calendar(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("economic_calendar: eastmoney source disabled in config")
    df = fetch_economic_calendar(trade_date)
    empty_ok(df, "economic_calendar", trade_date)
    return write_fetched(config, run_id, "economic_calendar", df, source="eastmoney")
