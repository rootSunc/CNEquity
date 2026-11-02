"""Merge partitions that incremental writes left in several files.

A write now appends rows of new keys as a file of their own instead of
rewriting the partition (see ``write_partition_rows``), so a partition gathers
files: one per gap fill, per appended day in a month or year partition. Each
extra file is another open for every scan. Merging them is the rewrite the
write avoided, done once for many writes and off the interactive path:
``cne run maintain`` calls it after the day's update.

The merge reads the committed generation, writes into the mutable tree and
publishes a revision like any repair. The rows do not change, except that a
legacy fragment's duplicate keys collapse and rows storage sanitizing rejects
are dropped — the same rules every compact applies to what it rewrites.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.datasets import DATASETS
from cnequity.domain.pit import normalize_pit_storage_columns
from cnequity.domain.schemas import sanitize_dataset_rows, validate_dataframe
from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.parquet import (
    MERGED_FILE,
    _business_equal,
    _dedupe_storage,
    _with_bucket_ids,
    bucket_column,
    emit_partition,
    is_base_file,
    parse_bucket,
)
from cnequity.storage.revisions import RevisionStore, _partition_of

#: A partition with at least this many files is merged.
MERGE_AT_FILES = 4

#: Derived datasets keep the merged-file name their writers always used.
_MERGED_NAMES = {"adj_factors": "part-0.parquet", "industry_index": "part-000.parquet"}


def merged_name(dataset: str) -> str:
    return _MERGED_NAMES.get(dataset, MERGED_FILE)


def merge_candidates(
    config: Config, dataset: str, *, min_files: int = MERGE_AT_FILES
) -> dict[str, list[Path]]:
    """Partitions of the committed generation holding at least *min_files* files."""
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    base = store.current_root(dataset)
    if base is None or not base.exists():
        return {}
    groups: dict[str, list[Path]] = {}
    for path in sorted(base.rglob("*.parquet")):
        groups.setdefault(_partition_of(path.relative_to(base)), []).append(path)
    name = merged_name(dataset)
    out = {}
    for part, files in groups.items():
        # The merged file and the buckets of a large partition count as one
        # base; what accumulates, and what this merges, are appended files.
        appended = [path for path in files if not is_base_file(path.name, name)]
        if len(appended) + (len(appended) < len(files)) >= min_files:
            out[part] = files
    return out


def _merge_group(files: list[Path], dataset: str, load) -> list[Path]:
    """The files a merge must load: all of them, or in a bucketed partition the
    appended files plus only the buckets their rows belong to."""
    name = merged_name(dataset)
    appended = [path for path in files if not is_base_file(path.name, name)]
    buckets = [(parse_bucket(path.name), path) for path in files if parse_bucket(path.name)]
    if not buckets or not appended:
        return files
    rows = pl.concat([load(path) for path in appended], how="diagonal_relaxed")
    column = bucket_column(rows, dataset)
    if column is None:
        return files
    count = max(parsed[1] for parsed, _ in buckets)
    wanted = set(_with_bucket_ids(rows.select(column), column, count)["__bucket__"].to_list())
    receiving = [path for parsed, path in buckets if parsed[1] == count and parsed[0] in wanted]
    return appended + receiving


def merge_dataset_files(
    config: Config, dataset: str, *, apply: bool = False, min_files: int = MERGE_AT_FILES
) -> dict:
    """Plan (default) or publish the merge of one dataset's many-file partitions."""
    spec = DATASETS.get(dataset)
    if spec is None:
        raise ValueError(f"unknown dataset {dataset!r}")
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _merge_locked(config, dataset, apply=apply, min_files=min_files)


def _load(dataset: str, layer: str):
    if layer == "curated":

        def load(path: Path) -> pl.DataFrame:
            return sanitize_dataset_rows(
                validate_dataframe(pl.read_parquet(path), dataset), dataset
            )

        return load
    return pl.read_parquet


def _merge(frames: list[pl.DataFrame], dataset: str, layer: str) -> pl.DataFrame:
    combined = pl.concat(frames, how="diagonal_relaxed")
    if layer == "curated":
        return normalize_pit_storage_columns(_dedupe_storage(combined, dataset), dataset)
    return dedupe_by_primary_key(combined, dataset)


def _merge_locked(config: Config, dataset: str, *, apply: bool, min_files: int) -> dict:
    spec = DATASETS[dataset]
    targets = merge_candidates(config, dataset, min_files=min_files)
    report: dict = {
        "dataset": dataset,
        "applied": False,
        "partitions": len(targets),
        "files_before": sum(len(files) for files in targets.values()),
    }
    if not targets or not apply:
        return report
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    base = store.current_root(dataset)
    assert base is not None
    store.materialize_current(dataset)
    layer = config.derived_root if spec.layer == "derived" else config.curated_root
    root = layer / dataset
    load = _load(dataset, spec.layer)
    name = merged_name(dataset)
    changed: list[Path] = []
    rows = 0
    content_changed = False
    for part, files in sorted(targets.items()):
        group = _merge_group(files, dataset, load)
        merged = _merge([load(path) for path in group], dataset, spec.layer)
        stored = pl.concat([pl.read_parquet(path) for path in group], how="diagonal_relaxed")
        content_changed = content_changed or not _business_equal(stored, merged)
        written, _ = emit_partition(
            merged,
            dataset,
            affected=group,
            existing=files,
            read_dir=base / part if part else base,
            out_dir=root / part if part else root,
            merged_name=name,
        )
        changed.extend(written)
        rows += merged.height

    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.quality.publication import check_repair_publication

    run_id = f"merge-files-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    contract = dataset_contract(dataset)
    # The gate audits the whole lake before and after (271 s of a 277 s merge
    # of 21 daily_bars partitions). A merge that publishes the stored rows
    # unchanged, only in fewer files, has nothing for it to judge; one that
    # collapsed a legacy duplicate or dropped a sanitized row goes through it.
    publication = (
        check_repair_publication(config, dataset, run_id, changed)
        if content_changed
        else {"report_path": None}
    )
    revision = store.commit(
        dataset,
        run_id=run_id,
        changed_files=changed,
        schema_version=int(contract["schema_version"]),
        contract_fingerprint=contract_fingerprint(contract),
        metadata={
            "reason": "merge_files",
            "partitions": len(targets),
            "files_before": report["files_before"],
            "content_changed": content_changed,
            "publication_audit": publication["report_path"],
        },
    )
    report.update(
        applied=True,
        run_id=run_id,
        rows=rows,
        content_changed=content_changed,
        revision=None if revision is None else revision.revision,
        publication_audit=publication["report_path"],
    )
    return report


def merge_all(
    config: Config, *, apply: bool = False, min_files: int = MERGE_AT_FILES
) -> list[dict]:
    """Every dataset with a many-file partition, merged (or planned)."""
    out = []
    for dataset in sorted(DATASETS):
        if not merge_candidates(config, dataset, min_files=min_files):
            continue
        out.append(merge_dataset_files(config, dataset, apply=apply, min_files=min_files))
    return out
