"""Reconcile the stored minute resamples after their inputs are published."""

from __future__ import annotations

from datetime import date
from functools import partial

from cnequity.config import Config
from cnequity.derive.minute_resample import derive_minute_resample, resample_has_gaps
from cnequity.domain.datasets import RESAMPLED_MINUTE_DATASETS
from cnequity.orchestrator.registry import register_step
from cnequity.orchestrator.run_lock import run_lock
from cnequity.steps.finalize import capture_derive_inputs, publish_derived_revision
from cnequity.storage.derived_output import DerivedOutput
from cnequity.storage.revisions import RevisionStore

MINUTE_RESAMPLE_STEPS = tuple(f"derive_{name}" for name in RESAMPLED_MINUTE_DATASETS)


def _reconcile(
    config: Config, trade_date: date, run_id: str, context: dict, *, dataset: str
) -> dict:
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    with run_lock(config.meta_root, f"derive-{dataset}", blocking=True):
        if store.current_pointer(dataset) is None:
            return {"status": "skipped", "note": f"{dataset} has not been materialized"}
        with DerivedOutput(config, dataset, run_id) as output:
            inputs = capture_derive_inputs(config, dataset, trade_date=trade_date)
            summary = derive_minute_resample(config, dataset, output=output)
            partial_coverage = resample_has_gaps(summary)
            revision = publish_derived_revision(
                config,
                dataset,
                run_id,
                trade_date,
                output,
                input_revisions=inputs,
                coverage_status="partial" if partial_coverage else "unknown",
            )

    result = {**summary, "rows_written": summary["rows"]}
    if revision is not None:
        result["dataset_revision"] = revision
        partial_coverage |= revision["coverage_status"] == "partial"
    if partial_coverage:
        result.update(status="degraded", coverage_status="partial")
    return result


for step, dataset in zip(MINUTE_RESAMPLE_STEPS, RESAMPLED_MINUTE_DATASETS, strict=True):
    register_step(
        step,
        group="finalize",
        depends_on=["compact"],
        parallelizable=False,
        # Either published minute input is enough; input_datasets would require both.
        outputs=(dataset,),
        description=f"Reconcile published {dataset} sessions with 1m and 5m inputs",
    )(partial(_reconcile, dataset=dataset))
