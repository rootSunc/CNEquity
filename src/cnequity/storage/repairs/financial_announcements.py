"""Append explicitly reconstructed first disclosures without altering observed vintages."""

from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import polars as pl

from cnequity.config import Config
from cnequity.domain.contracts import contract_fingerprint, dataset_contract
from cnequity.domain.financial_statements import (
    DATASET,
    original_disclosures,
    reconstruct_comparatives,
    reconstruct_disclosures,
)
from cnequity.domain.schemas import PRIMARY_KEYS
from cnequity.file_lock import lake_mutation_lock
from cnequity.quality.publication import check_repair_publication
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.parquet import StagingWriter, compact_dataset
from cnequity.storage.read_context import ReadContext
from cnequity.storage.revisions import RevisionStore


def repair_financial_announcements(config: Config, *, apply: bool = False) -> dict:
    with lake_mutation_lock(config.meta_root) if apply else nullcontext():
        store = RevisionStore(
            config.meta_root, config.curated_root, config.derived_root, create=apply
        )
        if apply:
            store.ensure_current(DATASET)
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
        store.materialize_current(DATASET)
        changed: list[Path] = []
        with TemporaryDirectory(prefix="cne-financial-announcements-") as directory:
            staging = Path(directory)
            StagingWriter(staging).write_batch(DATASET, run_id, "repair-0", rows)
            compact_dataset(
                staging,
                config.curated_root,
                DATASET,
                run_id,
                partition_col="report_period",
                changed_files=changed,
                base_root=root,
            )
        publication = check_repair_publication(config, DATASET, run_id, changed)
        contract = dataset_contract(DATASET)
        revision = store.commit(
            DATASET,
            run_id=run_id,
            changed_files=changed,
            schema_version=int(contract["schema_version"]),
            contract_fingerprint=contract_fingerprint(contract),
            metadata={
                "reason": "financial_original_disclosures",
                **report,
                "publication_audit": publication["report_path"],
            },
        )
        if revision is None:
            raise RuntimeError("financial announcement repair produced no revision")
        report.update(
            applied=True, revision=revision.revision, publication_audit=publication["report_path"]
        )
        return report
