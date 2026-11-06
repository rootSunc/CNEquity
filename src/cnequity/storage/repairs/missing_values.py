"""Replace stored vendor placeholders with nulls in a retained, gated revision.

Which values are placeholders is the dataset's registry ``value_rules`` (see
``domain/value_rules.py``); writers apply the same rules, so this only rewrites
rows stored before a rule existed.
"""

from contextlib import nullcontext

import polars as pl

from cnequity.config import Config
from cnequity.domain.datasets import DATASETS
from cnequity.domain.value_rules import (
    apply_value_rules,
    placeholder_counts,
    placeholder_predicates,
)
from cnequity.file_lock import lake_mutation_lock
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.read_context import ReadContext
from cnequity.storage.repairs.orphan_symbols import _publish
from cnequity.storage.revisions import RevisionStore


def rewritable_datasets() -> list[str]:
    """Datasets whose value rules can turn a stored value into null."""
    return [
        name
        for name, spec in DATASETS.items()
        if spec.value_rules.rewrites and spec.partition_col is not None
    ]


def repair_missing_values(config: Config, dataset: str, *, apply: bool = False) -> dict:
    if dataset not in rewritable_datasets():
        raise ValueError(f"unsupported missing-value repair: {dataset}")
    partition_col = DATASETS[dataset].partition_col
    with lake_mutation_lock(config.meta_root) if apply else nullcontext():
        store = RevisionStore(
            config.meta_root, config.curated_root, config.derived_root, create=apply
        )
        if apply:
            store.ensure_current(dataset)
        context = ReadContext.capture(config, (dataset,), {})
        root = context.roots[dataset]
        report = {"dataset": dataset, "applied": False, "rows": 0, "fields": {}}
        if not dataset_has_parquet(root, committed=False):
            return report
        frame = scan_parquet_root(root, committed=False)
        counts = placeholder_counts(frame, dataset)
        predicates = placeholder_predicates(dataset, frame.collect_schema().names())
        rows = (
            int(frame.select(pl.any_horizontal(list(predicates.values())).sum()).collect().item())
            if predicates
            else 0
        )
        report.update(rows=rows, fields=counts)
        if not apply or not rows:
            return report
        report.update(
            _publish(
                config,
                store,
                dataset,
                partition_col,
                root,
                keep=lambda part: apply_value_rules(part, dataset),
                reason="missing_value_placeholders",
                metadata={"fields": counts},
            )
        )
        report["applied"] = True
        return report


def repair_all_missing_values(config: Config, *, apply: bool = False) -> dict:
    """Every rewritable dataset, one gated revision each."""
    return {
        dataset: repair_missing_values(config, dataset, apply=apply)
        for dataset in rewritable_datasets()
    }
