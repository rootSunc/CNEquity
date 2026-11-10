"""The shared publish of repairs that filter stored rows (``publish_repair``).

It used to be a private ``_publish`` in ``orphan_symbols`` that ten repairs
imported. Because ``RevisionStore.commit`` read a change made only of deleted
partitions as "unchanged", it also rewrote an untouched partition to force the
publish; ``commit`` now takes the deleted partitions directly.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.storage.locator import layer_root
from cnequity.storage.parquet import CuratedWriter
from cnequity.storage.revisions import RevisionStore

logger = logging.getLogger(__name__)


def publish_repair(
    config: Config,
    store: RevisionStore,
    dataset: str,
    partition_col: str,
    base: Path,
    *,
    keep: Callable[[pl.DataFrame], pl.DataFrame],
    reason: str,
    metadata: dict,
) -> dict:
    """Rewrite the partitions ``keep`` changes and publish one gated revision.

    The shared end of the repairs that filter stored rows: every partition of
    *base* goes through *keep*; a partition it changes is rewritten, one it
    empties is deleted, and the result passes the publication gate before one
    revision is committed. A repair that changes nothing raises, so a no-op is
    never mistaken for a fix.
    """
    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.quality.publication import check_repair_publication

    store.materialize_current(dataset)
    layer = layer_root(config, dataset)
    writer = CuratedWriter(layer)
    changed: list[Path] = []
    removed: list[str] = []
    for directory in sorted(p for p in base.iterdir() if p.is_dir()):
        files = sorted(directory.rglob("*.parquet"))
        if not files:
            continue
        frame = pl.concat([pl.read_parquet(p) for p in files], how="diagonal_relaxed")
        kept = keep(frame)
        if kept is frame or (kept.height == frame.height and kept.equals(frame)):
            continue
        value = directory.name.partition("=")[2]
        if kept.is_empty():
            # The commit carries an undeclared partition only while it is
            # still present, so removing it here removes it from the revision.
            target = layer / dataset / directory.name
            for path in sorted(target.rglob("*"), reverse=True):
                path.unlink() if path.is_file() else path.rmdir()
            target.rmdir()
            removed.append(directory.name)
            continue
        changed.append(
            writer.write_partition(dataset, partition_col, value, kept, "part-merged.parquet")
        )
    run_id = f"{reason}-{dataset}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    publication = check_repair_publication(config, dataset, run_id, changed)
    contract = dataset_contract(dataset)
    revision = store.commit(
        dataset,
        run_id=run_id,
        changed_files=changed,
        removed_partitions=removed,
        schema_version=int(contract["schema_version"]),
        contract_fingerprint=contract_fingerprint(contract),
        metadata={
            "reason": reason,
            **metadata,
            "partitions_emptied": len(removed),
            "publication_audit": publication["report_path"],
        },
    )
    if revision is None:
        raise RuntimeError(f"{dataset}: {reason} produced no revision; nothing published")
    logger.info("%s: %s published revision %s", dataset, reason, revision.revision)
    return {
        "revision": revision.revision,
        "partitions_rewritten": len(changed),
        "partitions_emptied": len(removed),
        "publication_audit": publication["report_path"],
    }
