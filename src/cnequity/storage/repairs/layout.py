"""Move misplaced files of a partitioned dataset into its registered partitions.

Compaction merges one partition directory at a time. A root file beside the
partitions (or a directory keyed on another column) is never part of that
merge, so it can hold a second copy of keys that also live in a partition —
2026-09 lakes had 54,055 such futures bars, left by a repair that wrote a
root file directly. ``RevisionStore.commit`` now refuses such a layout; this
module is the way out of it.

The repair reads the committed generation, never the mutable tree. Rows of a
misplaced file join the partition their date belongs to. Where both copies of
a key agree on every non-null value, the kept row takes the other copy's
values for its own nulls: two observations carrying complementary fields
(one ``amount``, the other ``oi_change``) give one complete row. Where they
disagree, the canonical rule picks one row whole and nothing is mixed. Every
input row of a duplicated key is written to ``_quarantine`` as evidence, and
the previous generation stays retained, so nothing observed is lost.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.datasets import DATASETS, granularity_for_dataset
from cnequity.domain.pit import PIT_DATASET_NAMES
from cnequity.domain.schemas import PRIMARY_KEYS, sanitize_dataset_rows, validate_dataframe
from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.atomic import write_json_atomic, write_parquet_atomic
from cnequity.storage.locator import layer_root
from cnequity.storage.parquet import CuratedWriter, _partition_values
from cnequity.storage.revisions import RevisionStore, _partition_of, layout_violations

# Columns that describe an observation rather than the fact; they never make
# two copies of a key "disagree" and are taken from the kept row.
_OBSERVATION_COLUMNS = frozenset(
    {"source", "data_version", "fetched_at", "observed_at", "available_at", "revision_id"}
)
_FILE = "__layout_file"
_PART = "__layout_partition"


class LayoutRepairError(RuntimeError):
    """The dataset cannot be repaired without losing or inventing rows."""


def _read(path: Path, dataset: str, base: Path) -> pl.DataFrame:
    frame = sanitize_dataset_rows(validate_dataframe(pl.read_parquet(path), dataset), dataset)
    return frame.with_columns(pl.lit(path.relative_to(base).as_posix()).alias(_FILE))


def merge_complementary(frame: pl.DataFrame, dataset: str) -> tuple[pl.DataFrame, dict]:
    """One row per key; fill nulls only between copies that never disagree.

    Returns the merged frame (without helper columns) and counts of what was
    coalesced, what conflicted, and how many values each column gained.
    """
    pk = PRIMARY_KEYS.get(dataset, [])
    stats = {"duplicate_keys": 0, "coalesced_keys": 0, "conflicting_keys": 0, "filled": {}}
    if not pk or frame.is_empty():
        return frame.drop(_FILE, strict=False), stats
    counts = frame.group_by(pk).agg(pl.len().alias("__n"))
    duplicated = counts.filter(pl.col("__n") > 1).drop("__n")
    stats["duplicate_keys"] = duplicated.height
    canonical = dedupe_by_primary_key(frame, dataset)
    if duplicated.is_empty():
        return canonical.drop(_FILE, strict=False), stats

    facts = [c for c in frame.columns if c not in pk and c not in _OBSERVATION_COLUMNS | {_FILE}]
    dup_rows = frame.join(duplicated, on=pk, how="semi")
    distinct = dup_rows.group_by(pk).agg(
        [pl.col(c).drop_nulls().n_unique().alias(c) for c in facts]
    )
    conflict = pl.any_horizontal([pl.col(c) > 1 for c in facts]) if facts else pl.lit(False)
    agreeing = distinct.filter(~conflict).select(pk)
    stats["conflicting_keys"] = distinct.height - agreeing.height
    stats["coalesced_keys"] = agreeing.height
    if agreeing.is_empty() or not facts:
        return canonical.drop(_FILE, strict=False), stats

    # The one non-null value per column among agreeing copies.
    values = (
        dup_rows.join(agreeing, on=pk, how="semi")
        .group_by(pk)
        .agg([pl.col(c).drop_nulls().first().alias(f"__fill_{c}") for c in facts])
    )
    kept = canonical.join(values, on=pk, how="left")
    for column in facts:
        gained = kept.filter(pl.col(column).is_null() & pl.col(f"__fill_{column}").is_not_null())
        if gained.height:
            stats["filled"][column] = gained.height
    kept = kept.with_columns(
        [pl.coalesce(pl.col(c), pl.col(f"__fill_{c}")).alias(c) for c in facts]
    ).drop([f"__fill_{c}" for c in facts])
    return kept.select(canonical.columns).drop(_FILE, strict=False), stats


def repair_layout(config: Config, dataset: str, *, apply: bool = False) -> dict:
    """Plan (default) or publish the move of misplaced files into partitions."""
    spec = DATASETS.get(dataset)
    if spec is None:
        raise LayoutRepairError(f"unknown dataset {dataset!r}")
    if spec.partition_col is None:
        raise LayoutRepairError(f"{dataset} is not partitioned; nothing to move")
    if dataset in PIT_DATASET_NAMES:
        raise LayoutRepairError(f"{dataset} is point-in-time; its vintages are not merged here")
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _repair_locked(config, dataset, apply=apply)


def _repair_locked(config: Config, dataset: str, *, apply: bool) -> dict:
    spec = DATASETS[dataset]
    partition_col = spec.partition_col
    assert partition_col is not None
    granularity = granularity_for_dataset(dataset)
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    base = store.repair_base(dataset, register=apply)
    report: dict = {"dataset": dataset, "applied": False, "misplaced_files": []}
    if base is None:
        report["note"] = f"no {dataset} data; nothing to repair"
        return report
    files = sorted(base.rglob("*.parquet"))
    relatives = [path.relative_to(base) for path in files]
    bad = {problem.split(":", 1)[0] for problem in layout_violations(dataset, relatives)}
    # A root-only layout passes the commit guard until the first partition is
    # written beside it; move it now rather than at that failure.
    misplaced = [
        path
        for path, rel in zip(files, relatives, strict=True)
        if str(rel) in bad or not _partition_of(rel)
    ]
    report["misplaced_files"] = sorted(str(path.relative_to(base)) for path in misplaced)
    if not misplaced:
        return report

    moved = pl.concat([_read(path, dataset, base) for path in misplaced], how="diagonal_relaxed")
    affected_symbols = (
        frozenset(moved.get_column("symbol").drop_nulls().to_list())
        if dataset in {"corporate_actions", "adj_factors"} and "symbol" in moved.columns
        else frozenset()
    )
    if moved.get_column(partition_col).null_count():
        raise LayoutRepairError(f"{dataset}: misplaced rows without {partition_col}")
    moved = moved.with_columns(_partition_values(moved, partition_col, granularity).alias(_PART))
    groups = moved.partition_by(_PART, as_dict=True, include_key=False)
    del moved
    misplaced_set = set(misplaced)
    partition_files: dict[str, list[Path]] = {}
    for path, rel in zip(files, relatives, strict=True):
        if path not in misplaced_set:
            partition_files.setdefault(_partition_of(rel), []).append(path)

    run_id = f"layout-repair-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    layer = layer_root(config, spec)
    writer = CuratedWriter(layer)
    if apply:
        # Seed the mutable tree from the committed base; only the target
        # partitions and the misplaced files change below.
        store.materialize_current(dataset)
    totals: dict = {
        "rows_in": 0,
        "rows_out": 0,
        "duplicate_keys": 0,
        "coalesced_keys": 0,
        "conflicting_keys": 0,
        "filled": {},
    }
    changed: list[Path] = []
    evidence: list[pl.DataFrame] = []
    pk = PRIMARY_KEYS.get(dataset, [])
    for key in sorted(groups):
        value = str(key[0] if isinstance(key, tuple) else key)
        frames = [groups.pop(key)]
        frames += [
            _read(path, dataset, base)
            for path in partition_files.get(f"{partition_col}={value}", [])
        ]
        combined = pl.concat(frames, how="diagonal_relaxed")
        merged, stats = merge_complementary(combined, dataset)
        if stats["duplicate_keys"] and pk:
            repeated = combined.group_by(pk).agg(pl.len().alias("__n")).filter(pl.col("__n") > 1)
            evidence.append(combined.join(repeated.drop("__n"), on=pk, how="semi"))
        totals["rows_in"] += combined.height
        totals["rows_out"] += merged.height
        for name in ("duplicate_keys", "coalesced_keys", "conflicting_keys"):
            totals[name] += stats[name]
        for column, count in stats["filled"].items():
            totals["filled"][column] = totals["filled"].get(column, 0) + count
        report["partitions"] = report.get("partitions", 0) + 1
        if apply:
            changed.append(
                writer.write_partition(dataset, partition_col, value, merged, "part-merged.parquet")
            )
    report.update(totals)
    if (
        totals["rows_in"] - totals["rows_out"]
        != sum(frame.height for frame in evidence) - totals["duplicate_keys"]
    ):
        raise LayoutRepairError(f"{dataset}: merge dropped rows that were not duplicates")
    if not apply:
        return report

    if evidence:
        folder = config.data_root / "_quarantine" / f"{dataset}-{run_id}-{uuid.uuid4().hex}"
        folder.mkdir(parents=True)
        write_parquet_atomic(
            folder / "duplicate-observations.parquet",
            pl.concat(evidence, how="diagonal_relaxed"),
            compression="zstd",
        )
        write_json_atomic(folder / "report.json", report, indent=2)
        report["evidence"] = str(folder)
    root = layer / dataset
    for path in misplaced:
        stale = root / path.relative_to(base)
        if stale.exists():
            stale.unlink()
    from cnequity.domain.contracts import contract_fingerprint, dataset_contract

    contract = dataset_contract(dataset)
    from cnequity.quality.publication import check_repair_publication

    publication = check_repair_publication(
        config, dataset, run_id, changed, symbols=affected_symbols
    )
    revision = store.commit(
        dataset,
        run_id=run_id,
        changed_files=changed,
        schema_version=int(contract["schema_version"]),
        contract_fingerprint=contract_fingerprint(contract),
        metadata={
            "reason": "layout_repair",
            "misplaced_files": report["misplaced_files"],
            "merge": {name: report[name] for name in (*totals, "partitions")},
            "evidence": report.get("evidence"),
            "publication_audit": publication["report_path"],
        },
    )
    report.update(
        applied=True,
        run_id=run_id,
        revision=None if revision is None else revision.revision,
        revision_id=None if revision is None else revision.revision_id,
        publication_audit=publication["report_path"],
    )
    return json.loads(json.dumps(report, default=str))
