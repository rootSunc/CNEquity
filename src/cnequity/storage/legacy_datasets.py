"""Publish into the lake catalog the dataset folders an earlier release wrote without a revision.

Releases before 0.25.0 read a dataset straight from ``curated/<dataset>`` or
``derived/<dataset>`` when it had no revision pointer. The offline sample
lake wrote all of its datasets that way, and the demo lake its instruments.
The ``lake-catalog`` migration registers revisions only, so those folders
were left outside the catalog: reads still found them, but export, snapshots
and every write saw an empty dataset, and ``drop-generations`` (rightly) kept
them. The ``legacy-datasets`` migration publishes each such folder into the
catalog as one revision, checks the catalog holds the same rows, and then
deletes the folder.

A dataset the catalog already holds a revision for is left alone: its folder,
if any, is a leftover ``drop-generations`` reports.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl

MIGRATION_ID = "legacy-datasets"


def candidates(config) -> list[tuple[str, Path]]:
    """``(dataset, folder)`` for each dataset that has Parquet files but no revision."""
    from cnequity.domain.datasets import DATASETS
    from cnequity.storage.locator import layer_name
    from cnequity.storage.revisions import RevisionStore

    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    found = []
    for name in sorted(DATASETS):
        folder = Path(config.data_root) / layer_name(name) / name
        if not folder.is_dir() or folder.is_symlink():
            continue
        if next(folder.rglob("*.parquet"), None) is None:
            continue
        if store.current_pointer(name) is not None:
            continue
        found.append((name, folder))
    return found


def _rows(dataset: str, folder: Path) -> pl.DataFrame:
    from cnequity.domain.schemas import validate_dataframe

    files = sorted(folder.rglob("*.parquet"))
    frame = pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")
    # Files written before a nullable column existed stay valid without it.
    return validate_dataframe(frame, dataset, allow_missing_optional=True)


def adopt_legacy_datasets(config, *, apply: bool) -> dict:
    """Preview, or with *apply* publish and then delete, the folders :func:`candidates` finds."""
    from cnequity.storage.catalog_bridge import lake_root
    from cnequity.storage.lake_write import publish_frames
    from equitylake_core.catalog import Catalog, quote_identifier

    found = candidates(config)
    report: dict = {
        "applied": apply,
        "datasets": [name for name, _ in found],
        "rows_changed": len(found),
    }
    if not apply or not found:
        return report
    frames = {name: _rows(name, folder) for name, folder in found}
    # A sample lake's datasets keep the run id `cne init --profile sample`
    # gives them, which is how a later demo init recognises the lake.
    run_id = "sample" if getattr(config, "lake_profile", None) == "sample" else MIGRATION_ID
    publish_frames(config, frames, run_id=run_id, message=f"upgrade: {MIGRATION_ID}")
    with Catalog(lake_root(config), read_only=True) as catalog:
        held = {
            name: catalog.execute(f"SELECT count(*) FROM {quote_identifier(name)}").fetchone()[0]
            for name, _ in found
        }
    for name, folder in found:
        if held[name] != frames[name].height:
            raise RuntimeError(
                f"{name}: the catalog holds {held[name]} rows after the move, "
                f"the folder {frames[name].height}; {folder} was kept"
            )
        shutil.rmtree(folder)
        # The layer folder goes with its last dataset; one still holding anything stays.
        if not any(folder.parent.iterdir()):
            folder.parent.rmdir()
    report["rows"] = {name: frame.height for name, frame in frames.items()}
    return report
