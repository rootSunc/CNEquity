"""L3/L4/L7 research steps: institutional holdings, analyst consensus, sentiment."""

from __future__ import annotations

from datetime import date

from cn_market_lake.adapters.eastmoney.consensus import fetch_analyst_consensus
from cn_market_lake.adapters.eastmoney.institutional import fetch_institutional_holdings
from cn_market_lake.config import Config
from cn_market_lake.derive.sentiment_scores import compute_sentiment_scores
from cn_market_lake.orchestrator.registry import register_step
from cn_market_lake.steps.http_common import run_incremental_fetched, write_fetched


@register_step("institutional_holdings", group="research", depends_on=["instruments"])
def step_institutional_holdings(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("institutional_holdings: eastmoney source disabled in config")
    # Quarterly by REPORT_DATE: daily refreshes the latest quarter, backfill
    # walks all quarters from 2016.
    backfill = getattr(config, "_backfill", False)
    df = fetch_institutional_holdings(trade_date, backfill=backfill, config=config)
    if df.is_empty():
        return {"rows_read": 0, "rows_written": 0}
    return write_fetched(config, run_id, "institutional_holdings", df, source="eastmoney")


@register_step("analyst_consensus", group="research", depends_on=["instruments"])
def step_analyst_consensus(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("analyst_consensus: eastmoney source disabled in config")
    # Live consensus snapshot stamped with trade_date (no dated EM report).
    df = fetch_analyst_consensus(trade_date, config=config)
    if df.is_empty():
        return {"rows_read": 0, "rows_written": 0}
    return write_fetched(config, run_id, "analyst_consensus", df, source="eastmoney")


@register_step(
    "sentiment_scores",
    group="research",
    depends_on=["announcement_index", "news_headlines", "hot_rank"],
)
def step_sentiment_scores(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "sentiment_scores",
        lambda d: compute_sentiment_scores(config, d),
        source="derived",
        allow_empty=True,
    )
