"""Committed dataset revisions and immutable change receipts.

Coverage watermarks answer how far a dataset reaches; they do not identify its
contents.  A repair to an old partition therefore needs a separate, monotonic
revision that downstream caches and research artifacts can pin.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import stat
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cnequity.config.retention import revision_keep
from cnequity.domain.datasets import DATASETS
from cnequity.file_lock import lake_mutation_lock
from cnequity.meta_layout import LEGACY_STATE, REVISION_DATA, REVISION_RECORDS, STORAGE_RECORD
from cnequity.storage.atomic import (
    fsync_dir,
)
from cnequity.storage.state import StateStore

logger = logging.getLogger(__name__)

#: ``storage`` of a revision published into the lake catalog instead of a generation.


class DiskSpaceError(RuntimeError):
    """A publish would not fit, or a write ran out of space.

    The unfinished generation is removed before this is raised. The current
    pointer stays on the previous generation.
    """


class RevisionConsistencyError(RuntimeError):
    """Raised when a committed-revision pointer or receipt is incomplete.

    A lake with no pointer is an old, supported layout and is read directly.
    Once a pointer exists, however, silently falling back to the mutable legacy
    directory could expose a half-published compact.  Readers therefore fail
    closed on a malformed pointer instead of mixing generations.
    """


_POINTER_SCHEMA_VERSION = 1
#: A revision whose rows only the lake catalog holds: its generation path is never created.
STORAGE_CATALOG = "catalog"
_LEGACY_REVISION_ID = "legacy"
# Room for the receipt and the directory entries around the new files. The
# receipt no longer carries the whole gap index, so this is a bound, not a
# measurement of that old payload.
_RECEIPT_ALLOWANCE_BYTES = 1024 * 1024


#: Receipt metadata a new revision inherits from the previous one when its
#: writer does not restate it. ``adj_factors``' reviewed source overrides and
#: event proofs describe the dataset, not one derive run.
_CARRIED_RECEIPT_METADATA: dict[str, tuple[str, ...]] = {
    "adj_factors": ("factor_source_overrides", "factor_event_proofs"),
}


def _reject_symlink_path(path: Path, *, label: str = "path") -> None:
    """Reject user-controlled symlink ancestors without rejecting macOS aliases.

    ``RevisionStore`` is deliberately lower-level than ``SnapshotStore`` and
    cannot import its path helpers without creating a cycle.  Keep the same
    lexical walk here: resolving first would make a configured ``meta_root``
    symlink indistinguishable from a real metadata root, while macOS's
    root-owned ``/var`` (and ``/tmp``) aliases are safe and expected for test
    and temporary lakes.
    """

    lexical = Path(path).expanduser()
    if not lexical.is_absolute():
        lexical = Path.cwd() / lexical
    cursor = Path(lexical.anchor)
    for index, part in enumerate(lexical.parts):
        if part == lexical.anchor:
            continue
        cursor = cursor / part
        try:
            info = cursor.lstat()
        except FileNotFoundError:
            # No descendant can exist below an absent component.  The caller
            # may create it after all existing ancestors have been checked.
            break
        trusted_system_alias = (
            cursor != lexical
            and cursor.parent == Path(lexical.anchor)
            and cursor.name in {"var", "tmp"}
            and getattr(info, "st_uid", -1) == 0
        )
        if stat.S_ISLNK(info.st_mode) and not trusted_system_alias:
            raise ValueError(f"{label} contains symlink ancestor: {cursor}")
        if trusted_system_alias:
            continue
        if index < len(lexical.parts) - 1 and not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"{label} ancestor is not a directory: {cursor}")


def sha256_file(path: Path) -> str:
    """Return a streaming SHA-256 digest for *path*."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class RevisionFile:
    """One curated file changed by a committed dataset mutation."""

    path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class DatasetRevision:
    """Immutable receipt for one committed dataset generation."""

    dataset: str
    revision: int
    revision_id: str
    committed_at: str
    run_id: str
    schema_version: int
    contract_fingerprint: str
    content_digest: str
    changed_partitions: tuple[str, ...]
    files: tuple[RevisionFile, ...]
    metadata: dict[str, Any]
    # The immutable copy-on-write generation selected by ``current.json``.
    # These fields default so receipts written by older releases remain
    # deserializable and continue to provide the old changed-file contract.
    generation_path: str = ""
    generation_files: tuple[RevisionFile, ...] = ()
    pointer_path: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _free_bytes(path: Path) -> int | None:
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return None


def _partition_of(relative: Path) -> str:
    """Top-level partition directory of a dataset-relative file ('' for root files)."""
    return relative.parts[0] if len(relative.parts) > 1 else ""


class RevisionStore:
    """Read a lake's published revisions, from its catalog or, before the upgrade, its files."""

    def __init__(
        self,
        meta_root: Path,
        curated_root: Path,
        derived_root: Path | None = None,
        *,
        create: bool = True,
    ):
        self.meta_root = Path(meta_root).expanduser()
        # ``root.mkdir`` below is the first filesystem mutation performed by
        # this store.  Validate the lexical metadata root before it so a user
        # symlink cannot redirect receipts, generations, or the mutation lock
        # outside the configured lake.
        _reject_symlink_path(self.meta_root, label="metadata root")
        self.curated_root = Path(curated_root).resolve()
        # Keep the historical two-argument constructor.  A store created for
        # a configured lake can still publish derived datasets by using the
        # sibling ``derived`` root; callers with a non-standard layout may
        # provide it explicitly as the third argument.
        self.derived_root = (
            Path(derived_root).resolve()
            if derived_root is not None
            else self.curated_root.parent / "derived"
        )
        _reject_symlink_path(self.meta_root / LEGACY_STATE, label="state root")
        self.root = self.meta_root / REVISION_RECORDS
        _reject_symlink_path(self.root, label="revision root")
        if create:
            self.root.mkdir(parents=True, exist_ok=True)
        # Leave the retired ``meta/state/`` directory alone. Constructing
        # ``StateStore`` creates it, and reading a missing dataset file creates
        # its lock. Open it only when an older release's file is still there.
        self._state = None

    @property
    def state(self) -> StateStore:
        if self._state is None:
            self._state = StateStore(self.meta_root)
        return self._state

    def _layer_root(self, dataset: str) -> Path:
        """Return the mutable layer root for *dataset*.

        ``resolve_committed_root`` constructs a store with the logical layer
        as its second argument (which may itself be ``derived``), while the
        finalize path constructs one with both configured roots.  Supporting
        both shapes keeps old callers working and makes a derived commit use
        the right source bytes rather than accidentally copying curated data.
        """

        spec = DATASETS.get(dataset)
        if spec is not None and spec.layer == "derived":
            if self.curated_root.name == "derived":
                return self.curated_root
            return self.derived_root
        return self.curated_root

    @staticmethod
    def _assert_regular(path: Path) -> os.stat_result:
        try:
            info = path.lstat()
        except FileNotFoundError:
            raise FileNotFoundError(path) from None
        if stat.S_ISLNK(info.st_mode):
            raise RevisionConsistencyError(f"revision path is a symlink: {path}")
        if not stat.S_ISREG(info.st_mode):
            raise RevisionConsistencyError(f"revision path is not a regular file: {path}")
        return info

    @classmethod
    def _walk_files(cls, root: Path) -> list[Path]:
        """List parquet files under *root* without following symlinks."""

        if not root.exists():
            return []
        try:
            root_info = root.lstat()
        except FileNotFoundError:
            return []
        if stat.S_ISLNK(root_info.st_mode):
            raise RevisionConsistencyError(f"revision dataset root is a symlink: {root}")
        if not stat.S_ISDIR(root_info.st_mode):
            raise RevisionConsistencyError(f"revision dataset root is not a directory: {root}")
        out: list[Path] = []

        def visit(directory: Path) -> None:
            try:
                with os.scandir(directory) as iterator:
                    entries = sorted(iterator, key=lambda entry: entry.name)
            except FileNotFoundError:
                raise RevisionConsistencyError(
                    f"revision dataset disappeared: {directory}"
                ) from None
            for entry in entries:
                info = entry.stat(follow_symlinks=False)
                path = Path(entry.path)
                if stat.S_ISLNK(info.st_mode):
                    raise RevisionConsistencyError(f"revision path is a symlink: {path}")
                if stat.S_ISDIR(info.st_mode):
                    visit(path)
                elif stat.S_ISREG(info.st_mode) and path.suffix == ".parquet":
                    out.append(path)
                elif not stat.S_ISREG(info.st_mode):
                    raise RevisionConsistencyError(f"revision path is not regular: {path}")

        visit(root)
        return out

    def pointer_path(self, dataset: str) -> Path:
        """Path to the atomic committed-generation pointer for *dataset*."""
        path = self.root / dataset / "current.json"
        _reject_symlink_path(path, label="revision pointer")
        return path

    def generation_root(self, dataset: str, revision_id: str) -> Path:
        """Return the immutable generation directory for a dataset revision."""
        path = self.root / "data" / dataset / revision_id
        _reject_symlink_path(path, label="revision generation")
        return path

    @staticmethod
    def _safe_relative(raw: str) -> Path:
        path = Path(raw)
        if path.is_absolute() or not path.parts or ".." in path.parts:
            raise RevisionConsistencyError(f"unsafe revision path: {raw}")
        return path

    def _read_pointer_file(self, dataset: str) -> dict[str, Any] | None:
        path = self.pointer_path(dataset)
        try:
            pointer_info = path.lstat()
        except FileNotFoundError:
            return None
        if stat.S_ISLNK(pointer_info.st_mode):
            raise RevisionConsistencyError(f"revision pointer is a symlink: {path}")
        if not stat.S_ISREG(pointer_info.st_mode):
            raise RevisionConsistencyError(f"revision pointer is not a regular file: {path}")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError) as exc:
            raise RevisionConsistencyError(f"invalid revision pointer: {path}") from exc
        if not isinstance(payload, dict):
            raise RevisionConsistencyError(f"revision pointer is not an object: {path}")
        if payload.get("schema_version") != _POINTER_SCHEMA_VERSION:
            raise RevisionConsistencyError(f"unsupported revision pointer schema: {path}")
        if payload.get("dataset") != dataset:
            raise RevisionConsistencyError(f"revision pointer dataset mismatch: {path}")
        revision = payload.get("revision")
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
            raise RevisionConsistencyError(f"invalid revision pointer number: {path}")
        root_raw = payload.get("generation_path") or payload.get("root")
        if not isinstance(root_raw, str) or not root_raw:
            raise RevisionConsistencyError(f"revision pointer has no generation path: {path}")
        relative = self._safe_relative(root_raw)
        if relative.parts[:3] != (REVISION_DATA / dataset).parts:
            raise RevisionConsistencyError(f"revision pointer generation mismatch: {path}")
        generation = self._safe_meta_path(relative, label="generation")
        try:
            generation.relative_to(self.meta_root.resolve())
        except ValueError as exc:
            raise RevisionConsistencyError(
                f"revision generation escapes meta root: {path}"
            ) from exc
        if not generation.is_dir() and payload.get("storage") != STORAGE_CATALOG:
            raise RevisionConsistencyError(f"revision generation is missing: {generation}")

        receipt_raw = payload.get("receipt")
        if revision > 0:
            revision_id = payload.get("revision_id")
            if not isinstance(revision_id, str) or not revision_id:
                raise RevisionConsistencyError(f"invalid revision pointer id: {path}")
            if not isinstance(receipt_raw, str) or not receipt_raw:
                raise RevisionConsistencyError(f"revision pointer has no receipt: {path}")
            receipt_relative = self._safe_relative(receipt_raw)
            if receipt_relative.parts[:2] != (REVISION_RECORDS / dataset).parts:
                raise RevisionConsistencyError(f"revision pointer receipt mismatch: {path}")
            receipt_path = self._safe_meta_path(receipt_relative, label="receipt")
            try:
                receipt_path.relative_to(self.meta_root.resolve())
            except ValueError as exc:
                raise RevisionConsistencyError(
                    f"revision receipt escapes meta root: {path}"
                ) from exc
            try:
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            except (OSError, ValueError, TypeError) as exc:
                raise RevisionConsistencyError(
                    f"revision pointer references unreadable receipt: {receipt_path}"
                ) from exc
            if not isinstance(receipt, dict):
                raise RevisionConsistencyError(f"revision receipt is not an object: {receipt_path}")
            if (
                receipt.get("dataset") != dataset
                or receipt.get("revision") != revision
                or receipt.get("revision_id") != revision_id
            ):
                raise RevisionConsistencyError(f"revision pointer and receipt disagree: {path}")
            receipt_generation = receipt.get("generation_path")
            if receipt_generation and receipt_generation != root_raw:
                raise RevisionConsistencyError(
                    f"revision pointer generation disagrees with receipt: {path}"
                )
            pointer_digest = payload.get("content_digest")
            receipt_digest = receipt.get("content_digest")
            if pointer_digest is not None and receipt_digest is not None:
                if pointer_digest != receipt_digest:
                    raise RevisionConsistencyError(
                        f"revision pointer content disagrees with receipt: {path}"
                    )
        elif payload.get("revision_id") != _LEGACY_REVISION_ID or receipt_raw not in (None, ""):
            raise RevisionConsistencyError(f"invalid legacy revision pointer: {path}")
        return payload

    def _safe_meta_path(self, relative: Path, *, label: str) -> Path:
        """Resolve an internal metadata path while rejecting link ancestors."""

        cursor = self.meta_root
        for part in relative.parts:
            cursor = cursor / part
            try:
                info = cursor.lstat()
            except FileNotFoundError:
                # The final existence/type check below produces the useful
                # missing-generation/receipt error at the call site.
                continue
            if stat.S_ISLNK(info.st_mode):
                raise RevisionConsistencyError(f"revision {label} path is a symlink: {cursor}")
        resolved = cursor.resolve()
        try:
            resolved.relative_to(self.meta_root.resolve())
        except ValueError as exc:
            raise RevisionConsistencyError(f"revision {label} escapes meta root: {cursor}") from exc
        return resolved

    def _catalog_records(self):
        """The catalog's record of this lake's revisions, or ``None`` while its files are read."""
        from cnequity.storage.revision_records import records_for

        return records_for(self.meta_root)

    def current_pointer(self, dataset: str) -> dict[str, Any] | None:
        """The current revision of *dataset*, in the shape of its ``current.json``.

        From the lake catalog once it records every revision (see
        :mod:`cnequity.storage.revision_records`); before that, the validated
        pointer file.
        """
        records = self._catalog_records()
        if records is None:
            return self._read_pointer_file(dataset)
        return records.pointer(dataset)

    _read_pointer = current_pointer

    def pointer_file(self, dataset: str) -> dict[str, Any] | None:
        """The validated ``current.json`` of *dataset*: what the revision store's files say."""
        return self._read_pointer_file(dataset)

    def current_root(self, dataset: str, *, revision: int | str | None = None) -> Path | None:
        """Resolve the immutable root selected for a query.

        ``revision=None`` is the current revision; a revision number or id
        pins an earlier one. Answered from the lake catalog once it records
        every revision, and from the revision store's files before that.
        """
        records = self._catalog_records()
        if records is None:
            return self.current_root_file(dataset, revision=revision)
        if revision is None:
            pointer = records.pointer(dataset)
            if pointer is None:
                return None
            return self._safe_meta_path(
                self._safe_relative(pointer["generation_path"]), label="generation"
            )
        if isinstance(revision, bool):
            raise ValueError("revision must be a non-negative integer or revision id")
        record = records.find(dataset, revision)
        if record is None:
            raise RevisionConsistencyError(f"unknown retained revision {revision!r} for {dataset}")
        generation = self._safe_meta_path(
            self._safe_relative(record.generation_path), label="generation"
        )
        in_catalog = record.metadata.get("storage") == STORAGE_CATALOG or (
            (dataset, record.revision_id) in records.archived()
        )
        if not generation.is_dir() and not in_catalog:
            raise RevisionConsistencyError(f"revision generation is missing: {generation}")
        return generation

    def current_root_file(self, dataset: str, *, revision: int | str | None = None) -> Path | None:
        """Resolve the immutable root selected for a query, from the revision store's files.

        ``revision=None`` resolves ``current.json``.  A historical integer or
        revision id resolves the matching receipt and generation, allowing a
        research query to pin one generation for its entire lazy plan.
        """
        pointer = self._read_pointer_file(dataset)
        if pointer is None:
            return None
        if revision is None:
            raw = pointer.get("generation_path") or pointer.get("root")
            return self._safe_meta_path(self._safe_relative(str(raw)), label="generation")
        if isinstance(revision, bool):
            raise ValueError("revision must be a non-negative integer or revision id")
        wanted_num: int | None = revision if isinstance(revision, int) else None
        wanted_id = str(revision) if wanted_num is None else None
        if wanted_num == 0 or wanted_id == _LEGACY_REVISION_ID:
            if pointer.get("revision") == 0:
                raw = pointer.get("generation_path") or pointer.get("root")
                return self._safe_meta_path(self._safe_relative(str(raw)), label="generation")
            # Revision zero is only available when the baseline pointer was
            # retained; it is not reconstructed from the mutable legacy root.
            raise RevisionConsistencyError(f"revision {revision!r} is not retained for {dataset}")
        receipt_dir = self.root / dataset
        try:
            receipt_dir_info = receipt_dir.lstat()
        except FileNotFoundError:
            receipt_dir_info = None
        if receipt_dir_info is not None and stat.S_ISLNK(receipt_dir_info.st_mode):
            raise RevisionConsistencyError(
                f"revision receipt directory is a symlink: {receipt_dir}"
            )
        for receipt_path in sorted(receipt_dir.glob("*.json")):
            if receipt_path.name == "current.json":
                continue
            self._assert_regular(receipt_path)
            try:
                payload = json.loads(receipt_path.read_text(encoding="utf-8"))
            except (OSError, ValueError, TypeError):
                continue
            if not isinstance(payload, dict):
                continue
            if (wanted_num is not None and payload.get("revision") != wanted_num) or (
                wanted_id is not None and payload.get("revision_id") != wanted_id
            ):
                continue
            number = payload.get("revision")
            published = pointer.get("revision")
            if isinstance(number, int) and isinstance(published, int) and number > published:
                # Newer than current.json: a publish that stopped after its receipt.
                raise RevisionConsistencyError(
                    f"revision {revision!r} of {dataset} was never published"
                )
            raw = payload.get("generation_path")
            if not isinstance(raw, str) or not raw:
                raise RevisionConsistencyError(f"receipt {receipt_path} has no retained generation")
            generation = self._safe_meta_path(self._safe_relative(raw), label="generation")
            if (
                not generation.is_dir()
                and payload.get("metadata", {}).get("storage") != STORAGE_CATALOG
            ):
                raise RevisionConsistencyError(f"revision generation is missing: {generation}")
            return generation
        raise RevisionConsistencyError(f"unknown retained revision {revision!r} for {dataset}")

    def publishes_to_catalog(self) -> bool:
        """Whether this lake publishes new revisions into its catalog (``meta/storage.json``)."""
        try:
            setting = json.loads((self.meta_root / STORAGE_RECORD).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return False
        return isinstance(setting, dict) and setting.get("publish") == STORAGE_CATALOG

    @staticmethod
    def _content_digest(
        dataset: str,
        schema_version: int,
        contract_fingerprint: str,
        files: tuple[RevisionFile, ...],
    ) -> str:
        payload = {
            "dataset": dataset,
            "schema_version": schema_version,
            "contract_fingerprint": contract_fingerprint,
            "files": [asdict(item) for item in files],
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()

    def next_catalog_revision(
        self,
        dataset: str,
        *,
        run_id: str,
        files: tuple[RevisionFile, ...],
        changed_partitions: tuple[str, ...],
        schema_version: int,
        contract_fingerprint: str,
        metadata: dict[str, Any] | None = None,
        identity: Mapping[str, Any] | None = None,
    ) -> DatasetRevision:
        """The revision the lake catalog is about to publish for *dataset*.

        Numbered after the highest revision the catalog records. Nothing is
        written here: the caller holds ``lake_mutation_lock`` and records the
        revision in the transaction that publishes its rows, so a publish that
        stops leaves no trace, then calls :meth:`index_catalog_revision`.

        *identity* republishes a revision another lake made (a snapshot being
        restored): its number, id, time, run and content digest are kept, so a
        result that pinned that revision reads it here too. It must be newer
        than every revision this catalog records for the dataset.
        """
        if schema_version < 1:
            raise ValueError("schema_version must be a positive integer")
        if not contract_fingerprint:
            raise ValueError("contract_fingerprint must not be empty")
        from cnequity.storage.revision_records import enabled

        if not enabled(self.meta_root):
            from cnequity.storage.lake_record import LakeMigrationRequired

            raise LakeMigrationRequired(
                "this lake keeps revision receipts its catalog does not record yet; "
                "run `cne upgrade --apply` once before anything writes to it"
            )
        records = self._catalog_records()
        previous = None if records is None else records.current.get(dataset)
        history = [] if records is None else records.history(dataset)
        newest = max((record.revision for record in history), default=0)
        revision = newest + 1
        revision_id = uuid.uuid4().hex
        committed_at = datetime.now(timezone.utc).isoformat()
        content_digest = self._content_digest(dataset, schema_version, contract_fingerprint, files)
        if identity is not None:
            revision, revision_id = int(identity["revision"]), str(identity["revision_id"])
            if revision <= newest or any(r.revision_id == revision_id for r in history):
                raise RevisionConsistencyError(
                    f"{dataset}: revision {revision} is not newer than this lake's {newest}"
                )
            committed_at = str(identity.get("committed_at") or committed_at)
            run_id = str(identity.get("run_id") or run_id)
            content_digest = str(identity.get("content_digest") or content_digest)
        receipt = DatasetRevision(
            dataset=dataset,
            revision=revision,
            revision_id=revision_id,
            committed_at=committed_at,
            run_id=run_id,
            schema_version=schema_version,
            contract_fingerprint=contract_fingerprint,
            content_digest=content_digest,
            changed_partitions=tuple(sorted(changed_partitions)),
            files=files,
            metadata={
                **dict(metadata or {}),
                "storage": STORAGE_CATALOG,
                "base_revision_id": None if previous is None else previous.revision_id,
            },
            generation_path=self.generation_root(dataset, revision_id)
            .relative_to(self.meta_root)
            .as_posix(),
            pointer_path=self.pointer_path(dataset).relative_to(self.meta_root).as_posix(),
        )
        if previous is not None:
            for key in _CARRIED_RECEIPT_METADATA.get(dataset, ()):
                if key not in receipt.metadata and key in previous.metadata:
                    receipt.metadata[key] = previous.metadata[key]
        return receipt

    def index_catalog_revision(self, receipt: DatasetRevision) -> None:
        """Bring the dataset's state file up to *receipt*, once the catalog has published it."""
        self._index_state(receipt, None)

    def _index_state(self, receipt: DatasetRevision, receipt_relative: str | None) -> None:
        if receipt_relative is None:
            # The catalog is this revision's only record; a copy in the state
            # file would only go stale. Drop one a release before this left.
            self._drop_state_revision(receipt.dataset)
            return
        with self.state.transaction(receipt.dataset) as state:
            state.update(
                {
                    "revision": receipt.revision,
                    "revision_id": receipt.revision_id,
                    "revision_at": receipt.committed_at,
                    "revision_run_id": receipt.run_id,
                    "schema_version": receipt.schema_version,
                    "contract_fingerprint": receipt.contract_fingerprint,
                    "content_digest": receipt.content_digest,
                    "changed_partitions": list(receipt.changed_partitions),
                    "updated_at": receipt.committed_at,
                }
            )
            if receipt_relative is None:
                # The catalog is this revision's record: it has no receipt or pointer file.
                state.pop("revision_receipt", None)
                state.pop("revision_pointer", None)
            else:
                state["revision_receipt"] = receipt_relative
                state["revision_pointer"] = receipt.pointer_path

    def latest(self, dataset: str) -> DatasetRevision | None:
        """The receipt of *dataset*'s current revision.

        From the lake catalog once it records every revision: the receipt's
        per-file lists are not kept there, and come back empty.
        """
        records = self._catalog_records()
        if records is None:
            return self.receipt_file(dataset)
        record = records.current.get(dataset)
        return None if record is None else self._revision_of(record)

    def recorded_revision(self, dataset: str, revision: int | str) -> DatasetRevision | None:
        """The revision of *dataset* with this number or id, where the lake catalog records it.

        ``None`` for a lake that still keeps its revisions in receipt files.
        """
        records = self._catalog_records()
        record = None if records is None else records.find(dataset, revision)
        return None if record is None else self._revision_of(record)

    def _revision_of(self, record) -> DatasetRevision:
        return DatasetRevision(
            dataset=record.dataset,
            revision=record.revision,
            revision_id=record.revision_id,
            committed_at=record.committed_at or "",
            run_id=record.run_id or "",
            schema_version=record.schema_version or 0,
            contract_fingerprint=record.contract_fingerprint or "",
            content_digest=record.content_digest or "",
            changed_partitions=record.changed_partitions,
            files=(),
            metadata=dict(record.metadata),
            generation_path=record.generation_path,
            pointer_path=self.pointer_path(record.dataset).relative_to(self.meta_root).as_posix(),
        )

    def _legacy_state_file(self, dataset: str) -> Path:
        """``meta/state/<dataset>.json`` from a release that copied the revision there."""
        return self.meta_root / LEGACY_STATE / f"{dataset}.json"

    def receipt_file(self, dataset: str) -> DatasetRevision | None:
        """Read the receipt file referenced by the current pointer file."""
        pointer = self._read_pointer_file(dataset)
        if pointer is None:
            # No pointer and no state file: nothing named a receipt. Don't
            # create the retired directory to confirm that.
            if not self._legacy_state_file(dataset).is_file():
                return None
            relative = self.state.get_payload(dataset).get("revision_receipt")
        else:
            relative = pointer.get("receipt")
        if not relative:
            return None
        path = self._safe_meta_path(self._safe_relative(str(relative)), label="receipt")
        return self._load_receipt(
            path, pointer_path=self.pointer_path(dataset).relative_to(self.meta_root).as_posix()
        )

    def _load_receipt(self, path: Path, *, pointer_path: str = "") -> DatasetRevision:
        self._assert_regular(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["files"] = tuple(RevisionFile(**item) for item in payload.get("files", []))
        payload["changed_partitions"] = tuple(payload.get("changed_partitions", []))
        payload["generation_files"] = tuple(
            RevisionFile(**item) for item in payload.get("generation_files", [])
        )
        payload.setdefault("generation_path", "")
        payload.setdefault("pointer_path", pointer_path)
        return DatasetRevision(**payload)

    def _receipt_paths(self, dataset: str) -> list[tuple[int, str, Path]]:
        """``(number, revision id, path)`` for every receipt file, published or not."""
        directory = self.root / dataset
        _reject_symlink_path(directory, label="revision directory")
        out = []
        for path in directory.glob("*-*.json") if directory.is_dir() else ():
            number, _, rid = path.stem.partition("-")
            if number.isdigit():
                out.append((int(number), rid, path))
        return out

    def history_after(self, dataset: str, revision_id: str) -> tuple[DatasetRevision, ...]:
        """Return every committed revision after *revision_id*, oldest first.

        Raises :class:`RevisionConsistencyError` when *revision_id* is not
        recorded, was never published, or the chain after it has a gap.
        """
        records = self._catalog_records()
        if records is None:
            return self._history_after_file(dataset, revision_id)
        history = records.history(dataset)
        by_id = {record.revision_id: record for record in history}
        if revision_id not in by_id:
            raise RevisionConsistencyError(
                f"revision {revision_id} of {dataset} is not in the receipt history"
            )
        start = by_id[revision_id].revision
        by_number: dict[int, list] = {}
        for record in history:
            by_number.setdefault(record.revision, []).append(record)
        current = records.current.get(dataset)
        out = []
        while current is not None and current.revision_id != revision_id:
            if current.revision <= start:
                raise RevisionConsistencyError(
                    f"revision {revision_id} of {dataset} was never published"
                )
            out.append(self._revision_of(current))
            base = current.metadata.get("base_revision_id")
            if base:
                current = by_id.get(str(base))
            else:
                candidates = by_number.get(current.revision - 1, [])
                current = candidates[0] if len(candidates) == 1 else None
            if current is None:
                raise RevisionConsistencyError(
                    f"the revision before {out[-1].revision} of {dataset} is missing"
                )
        return tuple(reversed(out))

    def _history_after_file(self, dataset: str, revision_id: str) -> tuple[DatasetRevision, ...]:
        """Return every committed receipt after *revision_id*, oldest first.

        Raises :class:`RevisionConsistencyError` when *revision_id* is not in
        the receipt directory, was never published, or the chain after it has
        a gap, so a caller can never mistake a partial history for "nothing
        changed".
        """
        by_id: dict[str, tuple[int, Path]] = {}
        by_number: dict[int, list[Path]] = {}
        for number, rid, path in self._receipt_paths(dataset):
            by_id[rid] = (number, path)
            by_number.setdefault(number, []).append(path)
        if revision_id not in by_id:
            raise RevisionConsistencyError(
                f"revision {revision_id} of {dataset} is not in the receipt history"
            )
        start = by_id[revision_id][0]
        current = self.receipt_file(dataset)
        out: list[DatasetRevision] = []
        # Walk back from the published revision along ``base_revision_id``, so
        # a receipt whose publish stopped short is never part of the history.
        # Receipts written before that link existed fall back to the previous
        # number, which a failed publish could not yet have taken.
        while current is not None and current.revision_id != revision_id:
            if current.revision <= start:
                raise RevisionConsistencyError(
                    f"revision {revision_id} of {dataset} was never published"
                )
            out.append(current)
            base = current.metadata.get("base_revision_id")
            if base:
                previous = by_id.get(str(base), (None, None))[1]
            else:
                candidates = by_number.get(current.revision - 1, [])
                previous = candidates[0] if len(candidates) == 1 else None
            if previous is None:
                raise RevisionConsistencyError(
                    f"the revision before {current.revision} of {dataset} is missing"
                )
            current = self._load_receipt(previous)
        return tuple(reversed(out))

    def _drop_state_revision(self, dataset: str) -> None:
        # The catalog already holds the revision. A missing file has nothing
        # to drop; opening it would create ``meta/state/`` and a lock.
        if not self._legacy_state_file(dataset).is_file():
            return
        if not any(key in self.state.get_payload(dataset) for key in STATE_REVISION_FIELDS):
            return
        with self.state.transaction(dataset) as state:
            for key in STATE_REVISION_FIELDS:
                state.pop(key, None)

    def sync_state(self, dataset: str) -> None:
        """Repair the denormalised state file from the current revision."""
        receipt = self.latest(dataset)
        if receipt is None or receipt.revision == 0:
            return
        relative = None
        if self._catalog_records() is None:
            pointer = self._read_pointer_file(dataset) or {}
            relative = pointer.get("receipt")
            if not relative:
                raise RevisionConsistencyError(f"current pointer has no receipt: {dataset}")
        self._index_state(receipt, relative)

    def sweep(self, *, apply: bool = False) -> dict[str, Any]:
        """Find, and with *apply* repair, a state file that names another revision.

        A publish records its revision in the transaction that publishes its
        rows and updates the dataset's state file afterwards; a process that
        stops between the two leaves the state file one revision behind.
        ``cne run maintain`` runs this daily.
        """
        from cnequity.file_lock import lake_mutation_lock

        report: dict[str, Any] = {"applied": apply, "state_synced": []}
        records = self._catalog_records()
        if records is None:
            return report
        with lake_mutation_lock(self.meta_root, blocking=True):
            records = self._catalog_records()
            for dataset, record in sorted((records.current if records else {}).items()):
                if record is None or record.revision == 0:
                    continue
                # The catalog keeps the revision; only a stale copy left in
                # the state file by an older release needs removing. A missing
                # file is not one, and opening it would recreate the lock.
                if not self._legacy_state_file(dataset).is_file():
                    continue
                if any(key in self.state.get_payload(dataset) for key in STATE_REVISION_FIELDS):
                    report["state_synced"].append(dataset)
                    if apply:
                        self.sync_state(dataset)
        return report


#: Revision identity the state file once repeated from the catalog.
STATE_REVISION_FIELDS = (
    "revision",
    "revision_id",
    "revision_at",
    "revision_run_id",
    "schema_version",
    "contract_fingerprint",
    "content_digest",
    "changed_partitions",
    "revision_receipt",
    "revision_pointer",
)


def resolve_committed_root(
    dataset_root: Path | str,
    *,
    dataset: str | None = None,
    meta_root: Path | str | None = None,
    revision: int | str | None = None,
) -> Path:
    """Resolve a logical dataset directory to one immutable generation.

    Old lakes have no ``meta/revisions/<dataset>/current.json`` and are
    returned unchanged.  New lakes are resolved through the validated pointer;
    the returned directory is safe to use with both Polars and DuckDB.  The
    helper intentionally returns the logical path for an absent pointer rather
    than requiring callers to know whether a deployment has been migrated.
    """
    logical = Path(dataset_root).expanduser().resolve()
    name = dataset or logical.name
    if meta_root is None:
        # ``.../curated/<dataset>`` and ``.../derived/<dataset>`` are the two
        # supported logical layouts, and any other ``<root>/<layer>/<dataset>``
        # resolves the same way: meta sits beside the layer directory. A caller
        # can pass meta_root explicitly for a custom root or a test fixture.
        meta = logical.parent.parent / "meta"
    else:
        # Preserve the lexical path until RevisionStore performs its lstat
        # boundary check.  Resolving first would turn a user-controlled
        # metadata symlink into an apparently ordinary directory.
        meta = Path(meta_root).expanduser()
    store = RevisionStore(meta, logical.parent, create=False)
    resolved = store.current_root(name, revision=revision)
    if resolved is None and revision is not None:
        raise RevisionConsistencyError(f"no retained revision {revision!r} for {name}")
    if resolved is not None:
        return resolved
    # A lake that keeps its revisions in its catalog reads only the catalog:
    # a dataset it records no revision for has no rows, whatever a folder
    # under curated/ or derived/ may still hold.
    return logical / UNPUBLISHED if catalog_only(meta) else logical


#: What a dataset without a revision resolves to on a lake that reads only its
#: catalog: a path that is never created, so every scan of it finds nothing.
UNPUBLISHED = ".unpublished"


class LakeFolderWrite(RuntimeError):
    """A write aimed at a dataset folder of a lake that reads only its catalog."""


def refuse_lake_folder_write(path: Path | str) -> None:
    """Raise :class:`LakeFolderWrite` when *path* is in a lake's ``curated/`` or ``derived/``.

    A lake on its catalog never reads those folders, so a file written there is
    lost without a word: the run reports rows written that no reader will see.
    Writers put rows in a checkout (``LakeWrite.checkout``) and publish it.
    Every other directory (a checkout, staging, a scratch folder, an earlier
    release's lake, which nothing writes) passes.
    """
    target = Path(path).expanduser().absolute()
    for folder in (target, *target.parents):
        if folder.name in ("curated", "derived") and catalog_only(folder.parent / "meta"):
            raise LakeFolderWrite(
                f"{target} is under {folder}, which this lake never reads: it reads only "
                "its catalog. Write into a checkout of the lake (LakeWrite.checkout) and "
                "publish it."
            )


def catalog_only(meta_root: Path | str) -> bool:
    """Whether the lake at *meta_root* reads its published rows only from its catalog.

    True once the catalog is the record of the lake's revisions (a new lake,
    or one past the ``revision-records`` migration). Before that, an earlier
    release's lake is still read from its folders, which needs no upgrade.
    """
    from cnequity.storage.revision_records import enabled

    try:
        return enabled(Path(meta_root))
    except (OSError, ValueError):
        return False


def committed_revision(
    dataset_root: Path | str,
    *,
    dataset: str | None = None,
    meta_root: Path | str | None = None,
) -> tuple[int, str] | None:
    """Return ``(revision, revision_id)`` selected by the current pointer."""
    logical = Path(dataset_root).expanduser().resolve()
    name = dataset or logical.name
    if meta_root is None:
        meta = logical.parent.parent / "meta"
    else:
        # Keep the configured spelling so RevisionStore can reject a symlink
        # before it is canonicalised.
        meta = Path(meta_root).expanduser()
    store = RevisionStore(meta, logical.parent, create=False)
    pointer = store.current_pointer(name)
    if pointer is None:
        return None
    return int(pointer["revision"]), str(pointer["revision_id"])


@dataclass(frozen=True)
class GenerationPruneResult:
    """One dataset's generations that retention removed, or would remove."""

    dataset: str
    removed_revision_ids: tuple[str, ...]
    kept_revision_ids: tuple[str, ...]
    freed_bytes: int
    candidate_revision_ids: tuple[str, ...] = ()
    logical_bytes_selected: int = 0


def preview_revision_retention(meta_root: Path | str, *, keep: int) -> list[GenerationPruneResult]:
    """What ``retain_revision_generations(keep=keep)`` would delete now; reads only.

    Holds, references, the current pointer and the newest *keep* generations
    (see ``LifecycleStore.inspect``) win over age.
    """
    from cnequity.storage.lifecycle import LifecycleStore

    if keep == 0:
        return []
    store = LifecycleStore(meta_root)
    store.retention_registry()
    report = store.inspect(keep=keep)
    candidates = [
        o for o in report["objects"] if not o["blocked_reasons"] and o.get("storage") != "catalog"
    ]
    results = []
    for dataset in sorted({o["dataset"] for o in candidates}):
        selected = [o for o in candidates if o["dataset"] == dataset]
        results.append(
            GenerationPruneResult(
                dataset=dataset,
                removed_revision_ids=(),
                kept_revision_ids=tuple(
                    o["revision_id"]
                    for o in report["objects"]
                    if o["dataset"] == dataset and o["blocked_reasons"]
                ),
                freed_bytes=0,
                candidate_revision_ids=tuple(o["revision_id"] for o in selected),
                logical_bytes_selected=sum(o["logical_bytes"] for o in selected),
            )
        )
    return results


def _exclusive_tree_bytes(root: Path) -> int:
    """Bytes that disappear when *root* is removed.

    A file hard-linked from a generation that is staying shares its inode, so
    only a link count of one is counted. Copy-on-write clones are separate
    inodes and are counted in full; this is not an APFS free-space promise.
    """
    total = 0
    for directory, dirnames, names in os.walk(root, followlinks=False):
        parent = Path(directory)
        for name in dirnames:
            path = parent / name
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise RevisionConsistencyError(f"generation contains a link: {path}")
        for name in names:
            path = parent / name
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                raise RevisionConsistencyError(f"generation contains a non-regular file: {path}")
            if info.st_nlink == 1:
                total += info.st_size
    return total


def _refuse_live_lake_deletion(meta: Path, path: Path, obj: dict, protected: set[str]) -> None:
    """Automatic retention may remove only a historical generation directory.

    The generation currently published, anything under staging, curated,
    derived, logs or source snapshots, and a path that is not the generation
    directory itself are refused. Staging is never deleted here; that takes a
    separate confirmation.
    """
    if obj.get("current") or obj.get("blocked_reasons") or obj["path"] in protected:
        raise RevisionConsistencyError(f"refusing to delete in-use generation: {obj['path']}")
    relative = Path(obj["path"])
    if (
        relative.parts[:3] != (REVISION_DATA / obj["dataset"]).parts
        or relative.name != obj["revision_id"]
    ):
        raise RevisionConsistencyError(
            f"refusing to delete unexpected generation path: {obj['path']}"
        )
    lake = meta.parent
    lexical = path
    for name in ("staging", "curated", "derived", "logs"):
        live = lake / name
        if lexical == live or live in lexical.parents:
            raise RevisionConsistencyError(f"refusing to delete in-use lake data: {path}")
    snapshots = meta / "source_snapshots"
    if lexical == snapshots or snapshots in lexical.parents:
        raise RevisionConsistencyError(f"refusing to delete source snapshots: {path}")


def retain_revision_generations(
    meta_root: Path | str,
    *,
    keep: int,
) -> list[GenerationPruneResult]:
    """Delete generation directories older than the newest *keep* per dataset.

    The current pointer, an explicit hold, and a generation whose receipt is
    missing or unusable are kept. Receipts stay, so the publication history
    remains; reading a removed generation fails instead of following a newer
    one. An unfinished purge deletes nothing.
    """
    keep = revision_keep(None, keep)
    if keep == 0:
        return []
    from cnequity.storage.lifecycle import LifecycleStore

    store = LifecycleStore(meta_root)
    with lake_mutation_lock(store.meta):
        store.retention_registry()
        report = store.inspect(keep=keep)
        revision_store = RevisionStore(store.meta, store.meta.parent / "curated", create=False)
        protected = set()
        for dataset in {obj["dataset"] for obj in report["objects"]}:
            pointer = revision_store.current_pointer(dataset)
            raw_path = pointer.get("generation_path") if pointer else None
            if isinstance(raw_path, str) and raw_path:
                protected.add(raw_path)
        chosen: dict[str, list[dict]] = {}
        for obj in report["objects"]:
            if obj.get("storage") == "catalog":
                continue
            if obj["blocked_reasons"] or obj["current"] or obj["path"] in protected:
                continue
            relative = Path(obj["path"])
            if relative.parts[:3] != (REVISION_DATA / obj["dataset"]).parts or (
                relative.name != obj["revision_id"]
            ):
                raise RevisionConsistencyError(
                    f"refusing to delete unexpected generation path: {obj['path']}"
                )
            chosen.setdefault(obj["dataset"], []).append(obj)
        results: list[GenerationPruneResult] = []
        for dataset, objs in sorted(chosen.items()):
            removed: list[str] = []
            freed = 0
            for obj in sorted(objs, key=lambda item: item["revision"]):
                path = store.meta / obj["path"]
                _reject_symlink_path(path, label="generation")
                _refuse_live_lake_deletion(store.meta, path, obj, protected)
                if not path.is_dir():
                    continue
                freed += _exclusive_tree_bytes(path)
                shutil.rmtree(path)
                removed.append(obj["revision_id"])
            parent = store.meta / REVISION_DATA / dataset
            if parent.is_dir():
                fsync_dir(parent)
            removed_ids = set(removed)
            results.append(
                GenerationPruneResult(
                    dataset=dataset,
                    removed_revision_ids=tuple(removed),
                    kept_revision_ids=tuple(
                        obj["revision_id"]
                        for obj in report["objects"]
                        if obj["dataset"] == dataset and obj["revision_id"] not in removed_ids
                    ),
                    freed_bytes=freed,
                    logical_bytes_selected=sum(
                        obj["logical_bytes"] for obj in objs if obj["revision_id"] in removed_ids
                    ),
                )
            )
        return results


def apply_configured_revision_retention(config) -> dict[str, Any] | None:
    """Apply ``[storage.retention]`` after a publishing command, or do nothing.

    ``keep_generations`` of 0 returns ``None`` without reading the lake. The
    default is 5, the same count the storage page uses for a recent
    generation. A failure is reported and does not undo the publication that
    just finished; the next backfill or daily run tries again.
    """
    keep = revision_keep(config)
    if keep == 0:
        return None
    from cnequity.storage.lifecycle import LifecycleError

    try:
        removed = retain_revision_generations(config.meta_root, keep=keep)
        cat_report: dict[str, Any] | None = None
        if RevisionStore(
            config.meta_root, config.curated_root, config.derived_root, create=False
        ).publishes_to_catalog():
            from cnequity.storage.catalog_bridge import catalog_retention

            cat_report = catalog_retention(config, apply=True)
    except (LifecycleError, RevisionConsistencyError, OSError, ValueError) as exc:
        logger.warning("revision retention failed: %s", exc)
        return {
            "status": "failed",
            "keep": keep,
            "error": f"{type(exc).__name__}: {exc}",
            "removed": 0,
            "freed_bytes": 0,
            "datasets": [],
        }
    datasets = [
        {
            "dataset": item.dataset,
            "removed_revision_ids": list(item.removed_revision_ids),
            "kept_revision_ids": list(item.kept_revision_ids),
            "freed_bytes": item.freed_bytes,
        }
        for item in removed
    ]
    out: dict[str, Any] = {
        "status": "ok",
        "keep": keep,
        "removed": sum(len(item.removed_revision_ids) for item in removed)
        + (int(cat_report.get("snapshots_expired", 0)) if cat_report else 0),
        "freed_bytes": sum(item.freed_bytes for item in removed),
        "datasets": datasets,
    }
    if cat_report is not None:
        out["catalog"] = cat_report
    return out
