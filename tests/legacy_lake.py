"""Write a lake the way releases before 0.25.0 did: a generation, a receipt and a pointer.

Nothing in the package writes this layout any more; it only reads it, to upgrade
such a lake (``lake-catalog``, ``revision-records``) and to read research
snapshots. :func:`commit_generation` builds one for the tests of that reading.
It copies the dataset's whole hand-written tree (``curated/<dataset>`` or
``derived/<dataset>``) into a new generation, so the tree is what the revision
holds.
"""

from __future__ import annotations

import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

from cnequity.storage.revisions import DatasetRevision, RevisionFile, RevisionStore, sha256_file


def _partition(relative: Path) -> str:
    return relative.parts[0] if len(relative.parts) > 1 else ""


def commit_generation(
    store: RevisionStore,
    dataset: str,
    *,
    run_id: str,
    changed_files=(),
    schema_version: int = 1,
    contract_fingerprint: str = "test",
    metadata: dict | None = None,
    removed_partitions=(),
    _locked: bool = False,
) -> DatasetRevision | None:
    """Publish *dataset*'s tree as its next generation, in the pre-0.25.0 layout."""
    if not changed_files and not removed_partitions:
        return None
    tree = store._layer_root(dataset) / dataset
    pointer_path = store.root / dataset / "current.json"
    previous = json.loads(pointer_path.read_text()) if pointer_path.exists() else None
    if previous is None and tree.is_dir() and any(tree.rglob("*.parquet")):
        # The first commit adopted the tree as it stood as revision zero.
        baseline = store.root / "data" / dataset / "legacy"
        shutil.copytree(tree, baseline)
        previous = {
            "schema_version": 1,
            "dataset": dataset,
            "revision": 0,
            "revision_id": "legacy",
            "generation_path": baseline.relative_to(store.meta_root).as_posix(),
            "receipt": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        pointer_path.parent.mkdir(parents=True, exist_ok=True)
        pointer_path.write_text(json.dumps(previous, indent=2))
    revision = (previous or {}).get("revision", 0) + 1
    revision_id = uuid.uuid4().hex
    generation = store.root / "data" / dataset / revision_id
    generation.mkdir(parents=True)
    base = store.meta_root / previous["generation_path"] if previous else None
    changed = {
        _partition(Path(path).resolve().relative_to(tree.resolve())) for path in changed_files
    }
    changed |= {str(name) for name in removed_partitions}
    files: list[RevisionFile] = []
    for path in sorted(tree.rglob("*.parquet")) if tree.is_dir() else ():
        relative = path.relative_to(tree)
        target = generation / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        before = base / relative if base is not None else None
        if (
            before is not None
            and before.is_file()
            and _partition(relative) not in changed
            and sha256_file(before) == sha256_file(path)
        ):
            # An untouched file is shared with the previous generation, as before.
            os.link(before, target)
        else:
            shutil.copy2(path, target)
        files.append(
            RevisionFile(
                path=(Path(dataset) / relative).as_posix(),
                size_bytes=target.stat().st_size,
                sha256=sha256_file(target),
            )
        )
    changed_records = tuple(
        item for item in files if _partition(Path(item.path).relative_to(dataset)) in changed
    )
    committed_at = datetime.now(timezone.utc).isoformat()
    meta = store.meta_root
    receipt_path = store.root / dataset / f"{revision:08d}-{revision_id}.json"
    receipt = DatasetRevision(
        dataset=dataset,
        revision=revision,
        revision_id=revision_id,
        committed_at=committed_at,
        run_id=run_id,
        schema_version=schema_version,
        contract_fingerprint=contract_fingerprint,
        content_digest=store._content_digest(
            dataset, schema_version, contract_fingerprint, changed_records
        ),
        changed_partitions=tuple(
            sorted(f"{dataset}/{label}" if label else dataset for label in changed)
        ),
        files=changed_records,
        metadata={
            **dict(metadata or {}),
            "base_revision_id": (previous or {}).get("revision_id"),
        },
        generation_path=generation.relative_to(meta).as_posix(),
        generation_files=tuple(files),
        pointer_path=pointer_path.relative_to(meta).as_posix(),
    )
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt.to_dict(), indent=2))
    pointer_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset": dataset,
                "revision": revision,
                "revision_id": revision_id,
                "generation_path": receipt.generation_path,
                "receipt": receipt_path.relative_to(meta).as_posix(),
                "content_digest": receipt.content_digest,
                "committed_at": committed_at,
            },
            indent=2,
        )
    )
    with store.state.transaction(dataset) as state:
        state.update(
            {
                "revision": revision,
                "revision_id": revision_id,
                "revision_at": committed_at,
                "revision_run_id": run_id,
                "schema_version": schema_version,
                "contract_fingerprint": contract_fingerprint,
                "content_digest": receipt.content_digest,
                "revision_receipt": receipt_path.relative_to(meta).as_posix(),
                "revision_pointer": receipt.pointer_path,
                "changed_partitions": list(receipt.changed_partitions),
                "updated_at": committed_at,
            }
        )
    return receipt


def publish_generation(config, dataset: str, rows, *, run_id: str = "legacy") -> DatasetRevision:
    """Publish *rows* as their partitions' whole content, in the pre-0.25.0 layout.

    Each partition the rows fall into (by the dataset's period) is written as
    one file under the dataset's tree and the tree becomes the next generation.
    """
    import polars as pl

    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.domain.datasets import DATASETS, granularity_for_dataset
    from cnequity.storage.parquet import _partition_values

    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    tree = store._layer_root(dataset) / dataset
    column = DATASETS[dataset].partition_col
    changed = []
    if column is None:
        groups = {"": rows}
    else:
        labels = _partition_values(rows, column, granularity_for_dataset(dataset))
        groups = {
            f"{column}={value}": group.drop("__label__")
            for (value,), group in rows.with_columns(labels.alias("__label__"))
            .partition_by("__label__", as_dict=True, maintain_order=True)
            .items()
        }
    for label, group in groups.items():
        directory = tree / label if label else tree
        if directory.is_dir() and label:
            shutil.rmtree(directory)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "part-merged.parquet"
        pl.DataFrame(group).write_parquet(path)
        changed.append(path)
    contract = dataset_contract(dataset)
    return commit_generation(
        store,
        dataset,
        run_id=run_id,
        changed_files=changed,
        schema_version=int(contract["schema_version"]),
        contract_fingerprint=contract_fingerprint(contract),
    )
