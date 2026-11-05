"""Stage 同花顺 money flow on the days EastMoney's push2 cannot deliver it.

The ``fund_flow`` and ``sector_fund_flow`` steps call :func:`stage_ths_fallback`
when their EastMoney fetch raises, then re-raise: the EastMoney table really is
missing that day, and saying so keeps freshness and the gate honest. The
同花顺 rows go to their own datasets (``fund_flow_ths`` /
``sector_fund_flow_ths``, a different measure) and are compacted with the run.
A fallback that fails is logged and never masks the EastMoney error.
"""

from __future__ import annotations

import logging
from datetime import date

from cnequity.config import Config
from cnequity.orchestrator.registry import register_step
from cnequity.run_scope import backfill_of
from cnequity.steps.http_common import call_with_run_id, verify_raw_archive, write_fetched

logger = logging.getLogger(__name__)


def stage_ths_fallback(config: Config, trade_date: date, run_id: str, dataset: str) -> dict | None:
    if backfill_of(config).active or not config.sources.get("ths", True):
        # The pages are live and undated: there is nothing to backfill from.
        return None
    if _already_held(config, dataset, trade_date):
        # The 23:00 stale pass retries the failed EastMoney step; the day's
        # 同花顺 sweep (~115 pages) is already in the lake, so do not repeat it.
        logger.info("%s: %s already held; 同花顺 fallback not repeated", dataset, trade_date)
        return None
    try:
        result = _fetch_and_write(config, trade_date, run_id, dataset)
    except Exception as exc:  # noqa: BLE001 — the EastMoney failure is the one to report
        logger.warning("%s: 同花顺 fallback for %s failed: %s", dataset, trade_date, exc)
        return None
    logger.warning(
        "%s: EastMoney failed for %s; staged %s 同花顺 row(s) as %s",
        dataset.removesuffix("_ths"),
        trade_date,
        result.get("rows_written"),
        dataset,
    )
    return result


def _already_held(config: Config, dataset: str, trade_date: date) -> bool:
    import polars as pl

    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.curated_root / dataset
    if not dataset_has_parquet(root):
        return False
    held = (
        scan_parquet_root(root, partition_col="trade_date")
        .filter(pl.col("trade_date").cast(pl.Date) == trade_date)
        .select(pl.len())
        .collect()
        .item()
    )
    return bool(held)


def _fetch_and_write(config: Config, trade_date: date, run_id: str, dataset: str) -> dict:
    from cnequity.adapters.ths import fund_flow as ths

    fetch = {
        "fund_flow_ths": ths.fetch_fund_flow_ths,
        "sector_fund_flow_ths": ths.fetch_sector_fund_flow_ths,
    }[dataset]
    df = call_with_run_id(
        fetch, trade_date, pipeline_config=config, dataset=dataset, run_id=run_id, config=config
    )
    evidence = (
        verify_raw_archive(
            config,
            dataset,
            run_id,
            source=ths.SOURCE,
            request_scope=f"daily:{trade_date.isoformat()}",
        )
        if config.should_archive_raw(dataset)
        else None
    )
    return write_fetched(
        config, run_id, dataset, df, source=ths.SOURCE, raw_archive_evidence=evidence
    )


# In no schedule group: the EastMoney steps stage these themselves when push2
# fails, which is the only path that runs them. Registered so each dataset has
# its producing step (the registry contract), and so a job that plans them
# directly gets the same fetch — with its errors raised, not logged.
@register_step("fund_flow_ths", group="capital")
def step_fund_flow_ths(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    return _fetch_and_write(config, trade_date, run_id, "fund_flow_ths")


@register_step("sector_fund_flow_ths", group="research")
def step_sector_fund_flow_ths(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    return _fetch_and_write(config, trade_date, run_id, "sector_fund_flow_ths")
