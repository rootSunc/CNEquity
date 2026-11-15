"""Offline, retained revision repair for recorded fund share conversions."""

from contextlib import nullcontext
from datetime import datetime, timezone
from uuid import uuid4

import polars as pl

from cnequity.config import Config
from cnequity.derive.fund_factors import SOURCE, apply_fund_share_factors
from cnequity.domain.canonical import dedupe_lazy_by_primary_key
from cnequity.domain.schemas import with_provenance
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.instrument_catalog import load_curated_instruments
from cnequity.storage.lake_write import planned_on
from cnequity.storage.read_context import ReadContext
from cnequity.storage.repairs.publish import publish_staged
from cnequity.storage.revisions import RevisionStore


def repair_fund_factors(config: Config, *, apply: bool = False) -> dict:
    with planned_on(config) if apply else nullcontext():
        store = RevisionStore(
            config.meta_root, config.curated_root, config.derived_root, create=apply
        )
        context = ReadContext.capture(
            config, ("daily_bars", "instruments", "corporate_actions", "adj_factors"), {}
        )
        report = {
            "applied": False,
            "rows": 0,
            "symbols": 0,
            "source": SOURCE,
            "input_roots": {name: str(root) for name, root in context.roots.items()},
        }
        instruments = load_curated_instruments(config, context)
        root = context.roots["adj_factors"]
        if (
            instruments is None
            or "asset_type" not in instruments.columns
            or not dataset_has_parquet(root, committed=False)
        ):
            return report
        symbols = instruments.filter(pl.col("asset_type").is_in(["etf", "lof"]))["symbol"].to_list()
        before = (
            dedupe_lazy_by_primary_key(
                scan_parquet_root(
                    root, partition_col="trade_date", symbols=symbols, committed=False
                ),
                "adj_factors",
            )
            .filter(pl.col("adjust_type") == "hfq")
            .collect()
        )
        corrected, findings = apply_fund_share_factors(config, before, read_context=context)
        report["events"] = findings
        if "_vendor" not in corrected.columns:
            return report
        keys = ["symbol", "trade_date", "adjust_type"]
        changed = corrected.join(
            before.select(*keys, pl.col("factor").alias("_before")), on=keys
        ).filter(((pl.col("factor") / pl.col("_before")) - 1.0).abs() > 1e-12)
        report.update(
            rows=changed.height,
            symbols=changed["symbol"].n_unique(),
            changed_symbols=changed["symbol"].unique().sort().to_list(),
        )
        if not apply or changed.is_empty():
            return report
        rows = with_provenance(changed.select(*keys, "factor"), source=SOURCE, data_version="v1")
        run_id = f"fund-factors-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
        published = publish_staged(
            config,
            store,
            "adj_factors",
            rows,
            run_id=run_id,
            base_root=root,
            metadata={
                "reason": "fund_share_factors",
                **report,
            },
            symbols=frozenset(report["changed_symbols"]),
        )
        if published is None:
            raise RuntimeError("fund factor repair produced no revision")
        report.update(
            applied=True,
            revision=published["revision"],
            publication_audit=published["publication_audit"],
        )
        return report
