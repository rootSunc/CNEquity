"""Public, cheap dataset identity for downstream caches and artifacts."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from cnequity.config import Config
from cnequity.domain.datasets import DATASETS


@dataclass(frozen=True)
class DatasetState:
    dataset: str
    revision: int | None
    revision_id: str | None
    revision_at: str | None
    run_id: str | None
    schema_version: int | None
    contract_fingerprint: str | None
    content_digest: str | None
    revision_receipt: str | None
    changed_partitions: tuple[str, ...]
    # When the writer last touched this dataset. Present in every state payload
    # this lake has ever written, unlike the revision fields, and it moves on a
    # repair of an old partition — which a max-covered-date watermark cannot
    # see. Consumers that need a cache identity before revisions are committed
    # can key on this.
    updated_at: str | None = None


def dataset_state(
    dataset: str,
    *,
    config: Config | None = None,
    data_root: str | Path | None = None,
) -> DatasetState:
    """Return the latest committed identity for *dataset* without reading Parquet."""
    if dataset not in DATASETS:
        raise ValueError(f"unknown dataset {dataset!r}")
    # Local import avoids a reader -> query package import cycle.
    from cnequity.query.reader import resolve_config

    cfg = resolve_config(config=config, data_root=data_root)
    # StateStore is a writer-oriented helper: its constructor creates
    # meta/state and get_payload creates a lock file. Dataset identity is a
    # public read API and must also work on a read-only mounted snapshot.
    from cnequity.storage.revisions import RevisionStore

    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root, create=False)
    if store.pointer_path(dataset).exists():
        receipt = store.latest(dataset)
        if receipt is not None:
            return DatasetState(
                dataset=dataset,
                revision=receipt.revision,
                revision_id=receipt.revision_id,
                revision_at=receipt.committed_at,
                run_id=receipt.run_id,
                schema_version=receipt.schema_version,
                contract_fingerprint=receipt.contract_fingerprint,
                content_digest=receipt.content_digest,
                revision_receipt=(
                    f"revisions/{dataset}/{receipt.revision:08d}-{receipt.revision_id}.json"
                ),
                changed_partitions=receipt.changed_partitions,
                updated_at=receipt.committed_at,
            )
    path = cfg.meta_root / "state" / f"{dataset}.json"
    payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    if not isinstance(payload, dict):
        raise ValueError(f"state for {dataset!r} must be an object")
    revision = payload.get("revision")
    if revision is not None and (
        isinstance(revision, bool) or not isinstance(revision, int) or revision < 1
    ):
        raise ValueError(f"state field {dataset}.revision must be a positive integer")
    partitions = payload.get("changed_partitions") or []
    if not isinstance(partitions, list) or not all(isinstance(item, str) for item in partitions):
        raise ValueError(f"state field {dataset}.changed_partitions must be a list of strings")
    return DatasetState(
        dataset=dataset,
        revision=revision,
        revision_id=payload.get("revision_id"),
        revision_at=payload.get("revision_at"),
        run_id=payload.get("revision_run_id"),
        schema_version=payload.get("schema_version"),
        contract_fingerprint=payload.get("contract_fingerprint"),
        content_digest=payload.get("content_digest"),
        revision_receipt=payload.get("revision_receipt"),
        changed_partitions=tuple(partitions),
        updated_at=payload.get("updated_at"),
    )


@dataclass(frozen=True)
class RevisionChange:
    revision: int
    revision_id: str
    committed_at: str
    contract_fingerprint: str
    changed_partitions: tuple[str, ...]


def dataset_revisions(
    dataset: str,
    *,
    after: str,
    config: Config | None = None,
    data_root: str | Path | None = None,
) -> tuple[RevisionChange, ...]:
    """Return the committed revisions after revision id *after*, oldest first.

    Raises ``ValueError`` when *after* is unknown or the history after it is
    incomplete; callers deciding whether evidence is still valid must then
    treat the dataset as changed.
    """
    if dataset not in DATASETS:
        raise ValueError(f"unknown dataset {dataset!r}")
    from cnequity.query.reader import resolve_config
    from cnequity.storage.revisions import RevisionConsistencyError, RevisionStore

    cfg = resolve_config(config=config, data_root=data_root)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root, create=False)
    try:
        receipts = store.history_after(dataset, after)
    except RevisionConsistencyError as exc:
        raise ValueError(str(exc)) from exc
    return tuple(
        RevisionChange(
            revision=r.revision,
            revision_id=r.revision_id,
            committed_at=r.committed_at,
            contract_fingerprint=r.contract_fingerprint,
            changed_partitions=r.changed_partitions,
        )
        for r in receipts
    )


def dataset_attempt(
    dataset: str,
    *,
    config: Config | None = None,
    data_root: str | Path | None = None,
) -> dict | None:
    """Read the latest dataset-stage outcome without opening the writer manifest.

    A published revision may remain available after a failed new fetch. This
    separate operational state lets consumers display that failure without
    changing, deleting or falsely refreshing the last good snapshot.
    """
    if dataset not in DATASETS:
        raise ValueError(f"unknown dataset {dataset!r}")
    from cnequity.query.reader import resolve_config

    cfg = resolve_config(config=config, data_root=data_root)
    path = cfg.manifest_path
    if not path.exists():
        return None
    if path.is_symlink():
        raise ValueError("manifest path must not be a symlink")
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute(
                """
                SELECT r.run_id, r.started_at, r.finished_at, r.status AS run_status,
                       d.stage, d.status, d.error_code, d.error_message
                FROM dataset_results d
                JOIN ingestion_runs r ON r.run_id = d.run_id
                WHERE d.dataset = ?
                ORDER BY r.started_at DESC, r.run_id DESC,
                    CASE d.status
                        WHEN 'failed' THEN 5 WHEN 'blocked' THEN 4
                        WHEN 'degraded' THEN 3 WHEN 'warning' THEN 2
                        WHEN 'success' THEN 1 ELSE 0
                    END DESC
                LIMIT 1
                """,
                (dataset,),
            ).fetchone()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return None
            raise
    return dict(row) if row is not None else None
