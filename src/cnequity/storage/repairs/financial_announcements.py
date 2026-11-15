"""Append explicitly reconstructed first disclosures without altering observed vintages."""

from contextlib import nullcontext
from datetime import datetime, timezone
from uuid import uuid4

import polars as pl

from cnequity.config import Config
from cnequity.domain.financial_statements import (
    DATASET,
    original_disclosures,
    reconstruct_comparatives,
    reconstruct_disclosures,
)
from cnequity.domain.schemas import PRIMARY_KEYS
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.lake_write import planned_on
from cnequity.storage.read_context import ReadContext
from cnequity.storage.repairs.publish import publish_staged
from cnequity.storage.revisions import RevisionStore


def repair_financial_announcements(config: Config, *, apply: bool = False) -> dict:
    with planned_on(config) if apply else nullcontext():
        store = RevisionStore(
            config.meta_root, config.curated_root, config.derived_root, create=apply
        )
        context = ReadContext.capture(config, (DATASET,), {})
        root = context.roots[DATASET]
        report = {"applied": False, "rows": 0, "symbols": 0, "source": "eastmoney_backfill"}
        if not dataset_has_parquet(root, committed=False):
            return report
        before = scan_parquet_root(root, committed=False).collect()
        dates = original_disclosures(before)
        originals = reconstruct_disclosures(before, dates)
        comparatives = reconstruct_comparatives(before, dates).join(
            originals.select(PRIMARY_KEYS[DATASET]), on=PRIMARY_KEYS[DATASET], how="anti"
        )
        rows = pl.concat([originals, comparatives], how="diagonal_relaxed")
        report.update(
            rows=rows.height,
            symbols=rows["symbol"].n_unique(),
            from_results_table=originals.height,
            from_comparatives=comparatives.height,
        )
        if not apply or rows.is_empty():
            return report
        run_id = (
            f"financial-announcements-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
        )
        published = publish_staged(
            config,
            store,
            DATASET,
            rows,
            run_id=run_id,
            base_root=root,
            metadata={
                "reason": "financial_original_disclosures",
                **report,
            },
            partition_col="report_period",
        )
        if published is None:
            raise RuntimeError("financial announcement repair produced no revision")
        report.update(
            applied=True,
            revision=published["revision"],
            publication_audit=published["publication_audit"],
        )
        return report
