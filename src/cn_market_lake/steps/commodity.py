"""Commodity futures continuous bars (L1-adjacent)."""

from __future__ import annotations

from datetime import date

from cn_market_lake.adapters.eastmoney.commodity_bars import fetch_commodity_bars
from cn_market_lake.config import Config
from cn_market_lake.orchestrator.registry import register_step
from cn_market_lake.steps.http_common import run_incremental_fetched


@register_step("commodity_bars", group="macro_risk")
def step_commodity_bars(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    em = bool(config.sources.get("eastmoney", True))
    sina = bool(config.sources.get("sina", True))
    if not em and not sina:
        raise RuntimeError("commodity_bars: both eastmoney and sina sources disabled")
    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "commodity_bars",
        lambda d: fetch_commodity_bars(d, config=config),
        # Row-level ``source`` is set by adapters (eastmoney / sina); this is
        # only the fallback stamp when a frame lacks the column.
        source="eastmoney",
        allow_empty=True,
    )
