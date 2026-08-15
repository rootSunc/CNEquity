"""L5 structure steps: sector members, index constituents, industry members."""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cn_market_lake.adapters.cni.index_constituents_history import (
    CNI_BACKFILL_INDICES,
    expand_cni_constituents_as_of,
    fetch_cni_index_adjustments,
)
from cn_market_lake.adapters.eastmoney.index_constituents import fetch_index_constituents
from cn_market_lake.adapters.eastmoney.industry import fetch_industry_members
from cn_market_lake.adapters.eastmoney.sectors import fetch_sector_members
from cn_market_lake.adapters.sw.industry_history import (
    expand_sw_industry_as_of,
    fetch_sw_industry_intervals,
)
from cn_market_lake.config import Config
from cn_market_lake.orchestrator.registry import register_step
from cn_market_lake.steps.common import BACKFILL_START, list_trading_dates
from cn_market_lake.steps.http_common import run_incremental_fetched, write_fetched

logger = logging.getLogger(__name__)

_INDUSTRY_HISTORY_START = date(2020, 1, 1)


def _month_end_trading_days(config: Config, start: date, end: date) -> list[date]:
    """Last trading day of each calendar month in [start, end]."""
    days = list_trading_dates(config, start, end)
    if not days:
        return []
    by_month: dict[tuple[int, int], date] = {}
    for d in days:
        by_month[(d.year, d.month)] = d
    return [by_month[k] for k in sorted(by_month)]


def _existing_as_of_dates(config: Config, dataset: str) -> set[date]:
    root = config.curated_root / dataset
    files = list(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        return set()
    return set(
        pl.scan_parquet(files).select("as_of_date").unique().collect()["as_of_date"].to_list()
    )


@register_step("sector_members", group="capital", depends_on=["instruments"])
def step_sector_members(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("sector_members: eastmoney source disabled in config")
    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "sector_members",
        fetch_sector_members,
        source="eastmoney",
    )


@register_step("index_constituents", group="fundamentals", depends_on=["instruments"])
def step_index_constituents(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        return _backfill_index_constituents(config, trade_date, run_id)
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("index_constituents: eastmoney source disabled in config")
    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "index_constituents",
        fetch_index_constituents,
        source="eastmoney",
    )


def _backfill_index_constituents(config: Config, trade_date: date, run_id: str) -> dict:
    """CNI adjustment history → as_of snapshots for 399001/399006 (C2)."""
    start = getattr(config, "_backfill_start", None) or date(2021, 12, 1)
    end = getattr(config, "_backfill_end", None) or trade_date
    have = _existing_as_of_dates(config, "index_constituents")
    # Prefer rebalance-month ends so as_of aligns with CNI spell boundaries.
    todo = [
        d for d in _month_end_trading_days(config, start, min(end, trade_date)) if d not in have
    ]
    if not todo:
        return {"rows_read": 0, "rows_written": 0, "note": "all CNI as_of months already present"}

    frames: list[pl.DataFrame] = []
    failed_indices: list[str] = []
    for index_symbol in CNI_BACKFILL_INDICES:
        adj = fetch_cni_index_adjustments(index_symbol)
        if adj.is_empty():
            failed_indices.append(index_symbol)
            continue
        frames.append(expand_cni_constituents_as_of(adj, todo))

    if not frames:
        raise RuntimeError(
            "index_constituents backfill: no CNI adjustment rows for "
            + ", ".join(CNI_BACKFILL_INDICES)
        )
    df = pl.concat([f for f in frames if not f.is_empty()])
    if df.is_empty():
        raise RuntimeError("index_constituents backfill: expansion produced 0 rows")

    result = write_fetched(config, run_id, "index_constituents", df, source="cni")
    if failed_indices:
        result.setdefault("context_updates", {})["audit_findings"] = [
            {
                "dataset": "index_constituents",
                "severity": "warning",
                "code": "cni_index_backfill_incomplete",
                "message": (
                    "CNI returned empty adjustment history for: "
                    + ", ".join(failed_indices)
                    + " (CSI 000300/000905 still EM-daily only)"
                ),
            }
        ]
    result["as_of_dates"] = len(todo)
    return result


@register_step("industry_members", group="fundamentals", depends_on=["instruments"])
def step_industry_members(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        return _backfill_industry_members(config, trade_date, run_id)
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("industry_members: eastmoney source disabled in config")
    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "industry_members",
        fetch_industry_members,
        source="eastmoney",
    )


def _backfill_industry_members(config: Config, trade_date: date, run_id: str) -> dict:
    """Shenwan classification intervals → monthly as_of snapshots from 2020 (C2)."""
    start = getattr(config, "_backfill_start", None) or max(BACKFILL_START, _INDUSTRY_HISTORY_START)
    end = getattr(config, "_backfill_end", None) or trade_date
    have = _existing_as_of_dates(config, "industry_members")
    # Skip eastmoney daily snapshots already in lake when choosing SW months —
    # SW rows use classification_system=sw and share as_of_date partitions, so
    # only skip dates that already contain sw rows.
    sw_have = _existing_sw_as_of_dates(config)
    todo = [
        d for d in _month_end_trading_days(config, start, min(end, trade_date)) if d not in sw_have
    ]
    if not todo:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "note": "all Shenwan monthly as_of dates already present",
            "eastmoney_as_of_dates": len(have),
        }

    intervals = fetch_sw_industry_intervals()
    df = expand_sw_industry_as_of(intervals, todo)
    if df.is_empty():
        raise RuntimeError("industry_members backfill: Shenwan expansion produced 0 rows")
    # Soft coverage floor — a month with far fewer names than typical means the
    # XLS window does not reach that as_of (fail-loud rather than ship a hole).
    counts = df.group_by("as_of_date").len().sort("as_of_date")
    thin = counts.filter(pl.col("len") < 1000)
    result = write_fetched(config, run_id, "industry_members", df, source="sw")
    result["as_of_dates"] = todo.__len__()
    if thin.height:
        result.setdefault("context_updates", {})["audit_findings"] = [
            {
                "dataset": "industry_members",
                "severity": "warning",
                "code": "sw_industry_thin_months",
                "message": (
                    f"{thin.height} month(s) have <1000 Shenwan members "
                    f"(sample {thin['as_of_date'].head(3).to_list()})"
                ),
            }
        ]
    return result


def _existing_sw_as_of_dates(config: Config) -> set[date]:
    root = config.curated_root / "industry_members"
    files = list(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        return set()
    return set(
        pl.scan_parquet(files)
        .filter(pl.col("classification_system") == "sw")
        .select("as_of_date")
        .unique()
        .collect()["as_of_date"]
        .to_list()
    )
