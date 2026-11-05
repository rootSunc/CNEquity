"""Commodity futures continuous bars (L1-adjacent)."""

from __future__ import annotations

import logging
import uuid
from datetime import date, timedelta

import polars as pl

from cnequity.adapters.eastmoney.commodity_bars import (
    DEFAULT_BACKFILL_START,
    fetch_commodity_bars,
    fetch_commodity_bars_range,
)
from cnequity.adapters.sina.global_futures import (
    RECHECK_DAYS,
    fetch_offshore_commodity_bars_range,
)
from cnequity.config import Config
from cnequity.orchestrator.outcomes import SourceUnavailableError
from cnequity.orchestrator.registry import register_step
from cnequity.run_scope import backfill_of
from cnequity.steps.http_common import run_incremental_fetched, write_fetched

logger = logging.getLogger(__name__)


def _backfill_range(config: Config, trade_date: date, run_id: str) -> dict:
    """Write a whole backfill window from one range fetch.

    Both vendors answer a contract with its entire history in one request, so
    the adapter returns every session of the window at once. The generic
    backfill path accepts only rows dated the run day and rejected all of
    them, which is why no domestic history before the daily run ever landed.
    Rows are held to the requested window instead.
    """
    start = backfill_of(config).start or DEFAULT_BACKFILL_START
    end = min(backfill_of(config).end or trade_date, trade_date)
    if start > end:
        return {"rows_read": 0, "rows_written": 0}
    df = fetch_commodity_bars_range(start, end, config=config, strict=True)
    if df.is_empty():
        raise SourceUnavailableError(
            f"commodity_bars: no rows returned for {start.isoformat()}..{end.isoformat()}"
        )
    dates = df.get_column("trade_date").cast(pl.Date, strict=False)
    outside = int((dates.is_null() | (dates < start) | (dates > end)).sum())
    if outside:
        raise RuntimeError(
            f"commodity_bars: backfill {start.isoformat()}..{end.isoformat()} returned "
            f"{outside} row(s) with a trade_date outside the window"
        )
    return write_fetched(config, run_id, "commodity_bars", df, source="eastmoney")


@register_step("commodity_bars", group="macro_risk")
def step_commodity_bars(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    em = bool(config.sources.get("eastmoney", True))
    sina = bool(config.sources.get("sina", True))
    if not em and not sina:
        raise SourceUnavailableError("commodity_bars: both eastmoney and sina sources disabled")
    if backfill_of(config).active:
        return _backfill_range(config, trade_date, run_id)
    result = run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "commodity_bars",
        lambda d: fetch_commodity_bars(d, config=config, strict=True),
        # Row-level ``source`` is set by adapters (eastmoney / sina); this is
        # only the fallback stamp when a frame lacks the column.
        source="eastmoney",
        allow_empty=False,
    )
    if sina:
        _recheck_offshore(config, trade_date, run_id, result)
    return result


def _recheck_offshore(config: Config, trade_date: date, run_id: str, result: dict) -> None:
    """Rewrite recent offshore sessions with their final values.

    A COMEX session closes after the daily run, so the run-day row is either
    skipped or (historically) an intraday capture.  Re-reading the trailing
    window lets freshest-fetch dedupe replace it; the last session before a
    weekend or holiday is otherwise never rewritten.  Best-effort: the
    run-day result already stands on its own.
    """
    start = trade_date - timedelta(days=RECHECK_DAYS)
    end = trade_date - timedelta(days=1)
    try:
        df = fetch_offshore_commodity_bars_range(start, end, config=config)
        if df.is_empty():
            return
        recheck = write_fetched(
            config,
            run_id,
            "commodity_bars",
            df,
            source="sina",
            batch_id=f"offshore-recheck-{uuid.uuid4().hex}",
        )
    except Exception as exc:
        logger.warning(
            "commodity_bars: offshore recheck %s..%s failed: %s: %s",
            start,
            end,
            type(exc).__name__,
            exc,
        )
        return
    for key in ("rows_read", "rows_written"):
        if isinstance(recheck.get(key), int):
            result[key] = int(result.get(key) or 0) + recheck[key]
