"""L3/L4/L7 research steps: institutional holdings, broker forecasts, analyst consensus, sentiment."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from cnequity.adapters.eastmoney.consensus import fetch_analyst_consensus
from cnequity.adapters.eastmoney.institutional import fetch_institutional_holdings
from cnequity.adapters.tushare.broker_forecasts import (
    BROKER_FORECASTS_FLOOR,
    fetch_broker_forecasts,
)
from cnequity.adapters.tushare.client import SOURCE as TUSHARE_SOURCE
from cnequity.adapters.tushare.client import tushare_enabled
from cnequity.config import Config
from cnequity.derive.sentiment_scores import compute_sentiment_scores
from cnequity.orchestrator.outcomes import SourcePayloadError, SourceUnavailableError
from cnequity.orchestrator.registry import register_step
from cnequity.orchestrator.source_gaps import record_source_gap
from cnequity.steps.http_common import (
    call_with_run_id,
    empty_ok,
    run_incremental_fetched,
    verify_raw_archive,
    write_fetched,
)

_MIN_INSTITUTIONAL_HOLDING_ROWS_PER_PERIOD = 100


def _quarter_labels(config: Config, trade_date: date) -> set[str]:
    from cnequity.adapters.eastmoney.institutional import _quarter_end_dates

    periods = _quarter_end_dates(
        trade_date,
        start=getattr(config, "_backfill_start", None),
        end=getattr(config, "_backfill_end", None),
    )
    return {f"{period[:4]}Q{(int(period[5:7]) - 1) // 3 + 1}" for period in periods}


def _validate_institutional_holdings_snapshot(df):
    """Reject a non-empty but obviously truncated quarterly holdings response."""
    if df.is_empty():
        return df
    required = {"symbol", "holder_type", "report_period"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SourcePayloadError(
            "institutional_holdings: response is missing required column(s): " + ", ".join(missing)
        )
    counts = (
        df.unique(subset=["symbol", "holder_type", "report_period"])
        .group_by("report_period")
        .agg(pl.len().alias("_holding_rows"))
        .filter(pl.col("_holding_rows") < _MIN_INSTITUTIONAL_HOLDING_ROWS_PER_PERIOD)
    )
    if not counts.is_empty():
        details = ", ".join(
            f"{row['report_period']}={row['_holding_rows']}" for row in counts.iter_rows(named=True)
        )
        record_source_gap(
            "institutional_holdings",
            (
                "institutional_holdings: incomplete quarterly snapshot; each observed "
                f"period needs at least {_MIN_INSTITUTIONAL_HOLDING_ROWS_PER_PERIOD} "
                f"unique holding row(s) ({details})"
            ),
            frame=df,
        )
    return df


@register_step("institutional_holdings", group="research", depends_on=["instruments"])
def step_institutional_holdings(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    if not config.sources.get("eastmoney", True):
        raise SourceUnavailableError("institutional_holdings: eastmoney source disabled in config")
    # Quarterly by REPORT_DATE: daily refreshes the latest quarter, backfill
    # walks all quarters from 2016.
    backfill = getattr(config, "_backfill", False)
    df = _validate_institutional_holdings_snapshot(
        fetch_institutional_holdings(trade_date, backfill=backfill, config=config)
    )
    missing_periods: set[str] = set()
    if backfill:
        expected = _quarter_labels(config, trade_date)
        observed = (
            set(df.get_column("report_period").drop_nulls().to_list())
            if not df.is_empty() and "report_period" in df.columns
            else set()
        )
        missing_periods = expected - observed
    if backfill and not missing_periods and df.is_empty():
        return {"rows_read": 0, "rows_written": 0}
    if backfill and missing_periods:
        result: dict
        if df.is_empty():
            result = {"rows_read": 0, "rows_written": 0}
        else:
            result = write_fetched(config, run_id, "institutional_holdings", df, source="eastmoney")
        result["status"] = "warning"
        result["missing_periods"] = len(missing_periods)
        result["context_updates"] = {
            "audit_findings": [
                {
                    "dataset": "institutional_holdings",
                    "severity": "warning",
                    "check": "backfill_missing_quarters",
                    "message": (
                        f"institutional holdings missing {len(missing_periods)} requested "
                        f"quarter(s): {', '.join(sorted(missing_periods)[:8])}"
                    ),
                    "missing_periods": sorted(missing_periods),
                }
            ]
        }
        return result
    empty_ok(df, "institutional_holdings", trade_date)
    return write_fetched(config, run_id, "institutional_holdings", df, source="eastmoney")


# A daily run re-reads this many calendar days: reports land on weekends and
# the provider adds late rows; forecast_id makes the overlap idempotent.
_BROKER_FORECAST_LOOKBACK_DAYS = 7


@register_step("broker_forecasts", group="research", depends_on=["instruments"])
def step_broker_forecasts(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not tushare_enabled(config):
        raise SourceUnavailableError(
            "broker_forecasts: needs [sources.tushare] enabled with a token"
        )
    if getattr(config, "_backfill", False):
        from cnequity.steps.common import walk_day_backfill

        # Calendar days, not sessions: brokers publish on weekends too.
        return walk_day_backfill(
            config,
            trade_date,
            run_id,
            "broker_forecasts",
            lambda d: fetch_broker_forecasts(d, d, config=config),
            source=TUSHARE_SOURCE,
            date_col="report_date",
            floor=BROKER_FORECASTS_FLOOR,
            calendar_days=True,
            allow_empty_days=True,
        )
    start = trade_date - timedelta(days=_BROKER_FORECAST_LOOKBACK_DAYS - 1)
    df = fetch_broker_forecasts(start, trade_date, config=config)
    empty_ok(df, "broker_forecasts", trade_date)
    return write_fetched(config, run_id, "broker_forecasts", df, source=TUSHARE_SOURCE)


@register_step("analyst_consensus", group="research", depends_on=["instruments"])
def step_analyst_consensus(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not config.sources.get("eastmoney", True):
        raise SourceUnavailableError("analyst_consensus: eastmoney source disabled in config")
    # Live consensus snapshot stamped with trade_date (no dated EM report).
    # Use the common helper so snapshot backfill is rejected and missed daily
    # snapshots remain visible as coverage findings instead of looking complete.
    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "analyst_consensus",
        lambda d: call_with_run_id(
            fetch_analyst_consensus,
            d,
            pipeline_config=config,
            dataset="analyst_consensus",
            run_id=run_id,
            config=config,
        ),
        source="eastmoney",
        date_col="forecast_date",
        raw_archive_evidence_factory=lambda: verify_raw_archive(
            config,
            "analyst_consensus",
            run_id,
            source="eastmoney",
            request_scope=f"snapshot:{trade_date.isoformat()}",
        ),
    )


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
