"""A daily check of new financial statements against 通达信's quarterly package.

Measured 2026-10-28 on the 2026Q3 package: of 34,111 values both sources held
for 1,712 newly reporting companies, 34,108 agreed within 0.1%, and the first
disclosure dates matched for 1,710. Disagreement is therefore rare enough to
read. The package also lists every company that has filed, which shows a filing
the EastMoney daily path missed.

Report only: nothing here writes rows. The package is fetched once per period
and kept until the index names a new MD5.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.orchestrator.registry import register_step

logger = logging.getLogger(__name__)

#: Filings disclosed this many days back are compared.
RECENT_DAYS = 14
TOLERANCE = 1e-3
_SAMPLE = 20


def _cached_package(config: Config, period: str, index: dict[str, str]) -> bytes | None:
    """The package for *period*, from the cache while its MD5 is unchanged."""
    from cnequity.adapters.tdx_fin.packages import fetch_package, package_name

    name = package_name(period)
    md5 = index.get(name)
    if md5 is None:
        return None
    folder = config.meta_root / "source_cache" / "tdx_fin"
    path = folder / f"{name}.{md5}"
    if path.is_file():
        return path.read_bytes()
    content = fetch_package(period, index=index, config=config)
    if content is None:
        return None
    folder.mkdir(parents=True, exist_ok=True)
    for stale in folder.glob(f"{name}.*"):
        stale.unlink(missing_ok=True)
    _write_atomic(path, content)
    return content


def _write_atomic(path: Path, content: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(content)
    tmp.replace(path)


def _lake_rows(config: Config, periods: list[str]) -> pl.DataFrame:
    """The latest stored reading per statement item for *periods*."""
    from cnequity.query.canonical import dedupe_lazy_by_primary_key
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.curated_root / "financial_statement_items"
    if not dataset_has_parquet(root):
        return pl.DataFrame()
    frame = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(root, partition_col="report_period"),
            "financial_statement_items",
        )
        .filter(pl.col("report_period").is_in(periods))
        .select(
            "symbol",
            "report_period",
            "statement_type",
            "item_code",
            "item_value",
            "announce_date",
            "source",
        )
        .collect()
    )
    keys = ["symbol", "report_period", "statement_type", "item_code"]
    return frame.sort([*keys, "announce_date"], nulls_last=False).unique(
        subset=keys, keep="last", maintain_order=True
    )


def compare(
    lake: pl.DataFrame,
    tdx: pl.DataFrame,
    trade_date: date,
    operating_income: pl.DataFrame | None = None,
) -> dict | None:
    """One finding: value agreement on recent filings, and filings the lake lacks.

    *operating_income* (``symbol / report_period / item_value``) is TDX's 营业收入,
    compared with the lake's ``revenue`` (营业总收入) one way only: the total
    includes a finance subsidiary's interest and fee income, so it may exceed
    营业收入 but never fall below it.
    """
    keys = ["symbol", "report_period", "statement_type", "item_code"]
    since = trade_date - timedelta(days=RECENT_DAYS)
    recent = lake.filter(
        (pl.col("announce_date") >= since) & ~pl.col("source").str.starts_with("tdx_fin")
    )
    peer = tdx.select(*keys, pl.col("item_value").alias("_tdx"))
    if operating_income is not None and not operating_income.is_empty():
        peer = pl.concat(
            [
                peer,
                operating_income.select(
                    "symbol",
                    "report_period",
                    pl.lit("income").alias("statement_type"),
                    pl.lit("revenue").alias("item_code"),
                    pl.col("item_value").alias("_tdx"),
                ),
            ],
            how="vertical_relaxed",
        )
    pairs = recent.join(peer, on=keys, how="inner").with_columns(
        pl.when((pl.col("item_code") == "revenue") & (pl.col("_tdx") <= pl.col("item_value")))
        .then(0.0)
        .otherwise(
            (pl.col("_tdx") - pl.col("item_value")).abs()
            / pl.col("item_value").abs().clip(lower_bound=1.0)
        )
        .alias("_rel")
    )
    differ = pairs.filter(pl.col("_rel") > TOLERANCE)
    by_item = (
        differ.group_by("item_code").len().sort("len", descending=True).rows()
        if differ.height
        else []
    )
    # A filing TDX dates before today should have reached the lake by now; one
    # from today may still be on its way.
    filed = tdx.filter(pl.col("announce_date") < trade_date).select(
        "symbol", "report_period", "announce_date"
    )
    have = lake.select("symbol", "report_period").unique()
    lacking = (
        filed.unique(subset=["symbol", "report_period"])
        .join(have, on=["symbol", "report_period"], how="anti")
        .sort("announce_date", "symbol")
    )
    if pairs.is_empty() and lacking.is_empty():
        return None
    return {
        "dataset": "financial_statement_items",
        "severity": "warning" if differ.height or lacking.height else "info",
        "check": "financial_statement_tdx_peer",
        "trade_date": trade_date.isoformat(),
        "message": (
            f"financial statements vs TDX: {pairs.height - differ.height} of {pairs.height} "
            f"recent value(s) agree within {TOLERANCE:.1%}; "
            f"{lacking.height} filing(s) TDX has that the lake lacks"
        ),
        "compared": pairs.height,
        "disagreed": differ.height,
        "by_item_code": [{"item_code": code, "disagreed": n} for code, n in by_item],
        "disagreed_sample": [
            f"{r['symbol']}@{r['report_period']}:{r['item_code']}"
            for r in differ.head(_SAMPLE).iter_rows(named=True)
        ],
        "lacking": lacking.height,
        "lacking_sample": [
            f"{r['symbol']}@{r['report_period']} ({r['announce_date']})"
            for r in lacking.head(_SAMPLE).iter_rows(named=True)
        ],
    }


@register_step(
    "financial_statement_tdx_peer",
    group="core",
    depends_on=["compact"],
    description="Check new financial statements against TDX's quarterly package",
)
def step_financial_statement_tdx_peer(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    """After compact, so the session's own filings are in the comparison."""
    from cnequity.adapters.tdx_fin.packages import (
        fetch_index,
        latest_periods,
        operating_income,
        package_rows,
    )

    if not config.sources.get("tdx_fin", True):
        return {"rows_written": 0, "status": "skipped", "reason": "tdx_fin disabled"}
    try:
        periods = latest_periods(trade_date)
        lake = _lake_rows(config, periods)
        if lake.is_empty():
            return {"rows_written": 0, "status": "skipped", "reason": "no statements"}
        since = trade_date - timedelta(days=RECENT_DAYS)
        # One package a day in practice; two when an annual report season and
        # a first quarter overlap.
        active = sorted(
            set(lake.filter(pl.col("announce_date") >= since)["report_period"]) or {periods[0]}
        )
        index = fetch_index(config=config)
        frames, incomes = [], []
        for period in active:
            content = _cached_package(config, period, index)
            if content is not None:
                frames.append(
                    package_rows(period, content, observed_on=trade_date, fresh_days=10_000)
                )
                incomes.append(
                    pl.DataFrame(
                        [
                            {"symbol": s, "report_period": period, "item_value": v}
                            for s, v in operating_income(period, content).items()
                        ],
                        schema={
                            "symbol": pl.Utf8,
                            "report_period": pl.Utf8,
                            "item_value": pl.Float64,
                        },
                    )
                )
        if not frames:
            return {"rows_written": 0, "status": "skipped", "reason": "no TDX package"}
        finding = compare(
            lake,
            pl.concat(frames, how="vertical_relaxed"),
            trade_date,
            pl.concat(incomes, how="vertical_relaxed"),
        )
    except Exception as exc:  # noqa: BLE001 — a peer check never fails the run
        logger.warning("financial_statement_tdx_peer: %s", exc)
        finding = {
            "dataset": "financial_statement_items",
            "severity": "warning",
            "check": "financial_statement_tdx_peer",
            "message": f"TDX financial statement check failed: {exc}",
        }
    result: dict = {"rows_written": 0, "rows_read": 0}
    if finding:
        result["context_updates"] = {"audit_findings": [finding]}
    return result
