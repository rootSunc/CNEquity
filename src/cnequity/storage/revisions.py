"""Committed dataset revisions and immutable change receipts.

Coverage watermarks answer how far a dataset reaches; they do not identify its
contents.  A repair to an old partition therefore needs a separate, monotonic
revision that downstream caches and research artifacts can pin.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import tempfile
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cnequity.domain.datasets import DATASETS
from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.file_copy import copy2_isolated
from cnequity.storage.state import StateStore


class RevisionConsistencyError(RuntimeError):
    """Raised when a committed-revision pointer or receipt is incomplete.

    A lake with no pointer is an old, supported layout and is read directly.
    Once a pointer exists, however, silently falling back to the mutable legacy
    directory could expose a half-published compact.  Readers therefore fail
    closed on a malformed pointer instead of mixing generations.
    """


_POINTER_SCHEMA_VERSION = 1
_LEGACY_REVISION_ID = "legacy"


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


class RevisionStore:
    """Publish revision receipts and advance dataset state under one lock."""

    def __init__(
        self,
        meta_root: Path,
        curated_root: Path,
        derived_root: Path | None = None,
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
        _reject_symlink_path(self.meta_root / "state", label="state root")
        self.root = self.meta_root / "revisions"
        _reject_symlink_path(self.root, label="revision root")
        self.root.mkdir(parents=True, exist_ok=True)
        self.state = StateStore(self.meta_root)

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

    def _read_pointer(self, dataset: str) -> dict[str, Any] | None:
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
        if relative.parts[:3] != ("revisions", "data", dataset):
            raise RevisionConsistencyError(f"revision pointer generation mismatch: {path}")
        generation = self._safe_meta_path(relative, label="generation")
        try:
            generation.relative_to(self.meta_root.resolve())
        except ValueError as exc:
            raise RevisionConsistencyError(
                f"revision generation escapes meta root: {path}"
            ) from exc
        if not generation.is_dir():
            raise RevisionConsistencyError(f"revision generation is missing: {generation}")

        receipt_raw = payload.get("receipt")
        if revision > 0:
            revision_id = payload.get("revision_id")
            if not isinstance(revision_id, str) or not revision_id:
                raise RevisionConsistencyError(f"invalid revision pointer id: {path}")
            if not isinstance(receipt_raw, str) or not receipt_raw:
                raise RevisionConsistencyError(f"revision pointer has no receipt: {path}")
            receipt_relative = self._safe_relative(receipt_raw)
            if receipt_relative.parts[:2] != ("revisions", dataset):
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

    def current_pointer(self, dataset: str) -> dict[str, Any] | None:
        """Return and validate the current pointer, if this is a revision lake."""
        return self._read_pointer(dataset)

    def current_root(self, dataset: str, *, revision: int | str | None = None) -> Path | None:
        """Resolve the immutable root selected for a query.

        ``revision=None`` resolves ``current.json``.  A historical integer or
        revision id resolves the matching receipt and generation, allowing a
        research query to pin one generation for its entire lazy plan.
        """
        pointer = self._read_pointer(dataset)
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
            raw = payload.get("generation_path")
            if not isinstance(raw, str) or not raw:
                raise RevisionConsistencyError(f"receipt {receipt_path} has no retained generation")
            generation = self._safe_meta_path(self._safe_relative(raw), label="generation")
            if not generation.is_dir():
                raise RevisionConsistencyError(f"revision generation is missing: {generation}")
            return generation
        raise RevisionConsistencyError(f"unknown retained revision {revision!r} for {dataset}")

    def _copy_generation(
        self, dataset: str, revision_id: str
    ) -> tuple[Path, tuple[RevisionFile, ...]]:
        """Copy the complete mutable dataset into an unpublished generation."""
        source = self._layer_root(dataset) / dataset
        destination = self.generation_root(dataset, revision_id)
        _reject_symlink_path(source, label="mutable dataset root")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix=f".{revision_id}-", dir=destination.parent))
        try:
            files: list[RevisionFile] = []
            source_info = None
            try:
                source_info = source.lstat()
            except FileNotFoundError:
                pass
            if source_info is not None and not stat.S_ISDIR(source_info.st_mode):
                raise RevisionConsistencyError(f"mutable dataset root is not a directory: {source}")
            if source_info is not None:
                for source_file in self._walk_files(source):
                    relative = source_file.relative_to(source)
                    stored = temporary / relative
                    stored.parent.mkdir(parents=True, exist_ok=True)
                    copy2_isolated(source_file, stored)
                    files.append(
                        RevisionFile(
                            path=(Path(dataset) / relative).as_posix(),
                            size_bytes=stored.stat().st_size,
                            sha256=sha256_file(stored),
                        )
                    )
            # Rename only after every file has been copied.  Readers never see
            # ``temporary`` because the pointer is published later.
            os.replace(temporary, destination)
            return destination, tuple(files)
        except Exception:
            shutil.rmtree(temporary, ignore_errors=True)
            shutil.rmtree(destination, ignore_errors=True)
            raise

    def ensure_current(self, dataset: str, *, _locked: bool = False) -> Path:
        """Materialise a legacy lake as revision-zero before a compact.

        This is called before any mutable compact write.  Consequently a
        query racing the first revision sees the immutable baseline rather
        than a partially rewritten legacy partition.  Existing revision lakes
        are left untouched.
        """
        if not _locked:
            with lake_mutation_lock(self.meta_root, blocking=True):
                return self.ensure_current(dataset, _locked=True)
        current = self.current_root(dataset)
        if current is not None:
            return current
        generation, _files = self._copy_generation(dataset, _LEGACY_REVISION_ID)
        pointer = {
            "schema_version": _POINTER_SCHEMA_VERSION,
            "dataset": dataset,
            "revision": 0,
            "revision_id": _LEGACY_REVISION_ID,
            "generation_path": generation.relative_to(self.meta_root).as_posix(),
            "receipt": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        write_json_atomic(self.pointer_path(dataset), pointer, ensure_ascii=False, indent=2)
        return generation

    def materialize_current(self, dataset: str) -> Path | None:
        """Reset the mutable compatibility tree to the committed generation.

        Derived writers still publish through their historical mutable paths.
        Before such a writer runs, seeding that path from ``current.json`` is
        important when an operator has removed the mutable tree: otherwise an
        append-only derive would write only its new partition and the next COW
        generation would silently lose the retained history.
        """

        current = self.current_root(dataset)
        if current is None:
            return None
        target = self._layer_root(dataset) / dataset
        target_parent = target.parent
        target_parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix=f".{dataset}-materialize-", dir=target_parent))
        staged = temporary / dataset
        try:
            shutil.copytree(current, staged, copy_function=copy2_isolated)
            existing = None
            try:
                existing = target.lstat()
            except FileNotFoundError:
                pass
            if existing is not None:
                if stat.S_ISLNK(existing.st_mode) or not stat.S_ISDIR(existing.st_mode):
                    raise RevisionConsistencyError(
                        f"mutable dataset path is not a directory: {target}"
                    )
                backup = target_parent / f".{dataset}-stale-{uuid.uuid4().hex}"
                os.replace(target, backup)
                try:
                    os.replace(staged, target)
                except BaseException:
                    os.replace(backup, target)
                    raise
                shutil.rmtree(backup, ignore_errors=True)
            else:
                os.replace(staged, target)
            return current
        finally:
            shutil.rmtree(temporary, ignore_errors=True)

    def quarantine_candidate(
        self,
        dataset: str,
        *,
        run_id: str,
        reason: str = "rejected",
    ) -> Path | None:
        """Move a rejected mutable candidate aside and restore the pointer tree.

        A source-diff gate runs after compact has written the legacy-compatible
        candidate.  Leaving that candidate in place would make the next run's
        merge inherit bytes that never passed the gate.  The rejected tree is
        retained below ``_quarantine`` for inspection, while the active path
        is reconstructed from the immutable pointer generation.
        """

        target = self._layer_root(dataset) / dataset
        try:
            info = target.lstat()
        except FileNotFoundError:
            info = None
        quarantine: Path | None = None
        if info is not None:
            quarantine_root = self.curated_root.parent / "_quarantine"
            quarantine_root.mkdir(parents=True, exist_ok=True)
            safe_reason = "".join(
                char if char.isalnum() or char in "._-" else "_" for char in str(reason)
            )[:48]
            safe_run = "".join(
                char if char.isalnum() or char in "._-" else "_" for char in str(run_id)
            )[:80]
            quarantine = quarantine_root / f"{dataset}-{safe_run}-{safe_reason}-{uuid.uuid4().hex}"
            os.replace(target, quarantine)

        current = self.current_root(dataset)
        if current is not None:
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = Path(tempfile.mkdtemp(prefix=f".{dataset}-rollback-", dir=target.parent))
            staged = temporary / dataset
            try:
                shutil.copytree(current, staged, copy_function=copy2_isolated)
                os.replace(staged, target)
            finally:
                shutil.rmtree(temporary, ignore_errors=True)
        return quarantine

    def _relative_file(self, path: Path, dataset: str) -> Path:
        resolved = path.resolve()
        layer_root = self._layer_root(dataset)
        try:
            return resolved.relative_to(layer_root)
        except ValueError as exc:
            label = "derived" if layer_root == self.derived_root else "curated"
            raise ValueError(f"revision file is outside {label} root: {path}") from exc

    def _file_records(self, files: list[Path], dataset: str) -> tuple[RevisionFile, ...]:
        records: list[RevisionFile] = []
        for path in sorted({Path(item) for item in files}, key=lambda item: str(item)):
            _reject_symlink_path(path, label="revision file")
            info = self._assert_regular(path)
            relative = self._relative_file(path, dataset)
            records.append(
                RevisionFile(
                    path=relative.as_posix(),
                    size_bytes=info.st_size,
                    sha256=sha256_file(path),
                )
            )
        return tuple(records)

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

    def commit(
        self,
        dataset: str,
        *,
        run_id: str,
        changed_files: list[Path],
        schema_version: int,
        contract_fingerprint: str,
        metadata: dict[str, Any] | None = None,
        _locked: bool = False,
    ) -> DatasetRevision | None:
        """Commit a new immutable generation, or return ``None`` if unchanged.

        Compaction still writes the backwards-compatible ``curated/<dataset>``
        layout.  This method snapshots that complete result into a private
        generation and publishes one ``current.json`` pointer only after all
        files and the receipt are durable.  A reader therefore observes either
        the previous complete generation or the next complete generation —
        never a mixture of partitions.  The mutable legacy copy is retained
        for old tools and can be reclaimed independently after migration.
        """
        if not _locked:
            with lake_mutation_lock(self.meta_root, blocking=True):
                return self.commit(
                    dataset,
                    run_id=run_id,
                    changed_files=changed_files,
                    schema_version=schema_version,
                    contract_fingerprint=contract_fingerprint,
                    metadata=metadata,
                    _locked=True,
                )

        if schema_version < 1:
            raise ValueError("schema_version must be a positive integer")
        if not contract_fingerprint:
            raise ValueError("contract_fingerprint must not be empty")
        files = self._file_records(changed_files, dataset)
        if not files:
            return None

        # A direct caller may commit a legacy dataset without first calling
        # ``ensure_current``.  Establish the baseline here as well; finalize
        # calls it earlier so a query cannot race its mutable compact.
        pointer = self._read_pointer(dataset)
        if pointer is None:
            self.ensure_current(dataset, _locked=True)
            pointer = self._read_pointer(dataset)
        assert pointer is not None
        current = pointer.get("revision", 0)
        if isinstance(current, bool) or not isinstance(current, int) or current < 0:
            raise RevisionConsistencyError(f"invalid current revision for {dataset}")
        revision = current + 1
        committed_at = datetime.now(timezone.utc).isoformat()
        revision_id = uuid.uuid4().hex
        generation, generation_files = self._copy_generation(dataset, revision_id)
        # File records are already POSIX; Path(str).parent would re-inject
        # Windows separators into the published partition identity.
        partitions = tuple(sorted({Path(item.path).parent.as_posix() for item in files}))
        receipt_path = self.root / dataset / f"{revision:08d}-{revision_id}.json"
        receipt_relative = receipt_path.relative_to(self.meta_root).as_posix()
        generation_relative = generation.relative_to(self.meta_root).as_posix()
        receipt = DatasetRevision(
            dataset=dataset,
            revision=revision,
            revision_id=revision_id,
            committed_at=committed_at,
            run_id=run_id,
            schema_version=schema_version,
            contract_fingerprint=contract_fingerprint,
            content_digest=self._content_digest(
                dataset, schema_version, contract_fingerprint, files
            ),
            changed_partitions=partitions,
            files=files,
            metadata=dict(metadata or {}),
            generation_path=generation_relative,
            generation_files=generation_files,
            pointer_path=self.pointer_path(dataset).relative_to(self.meta_root).as_posix(),
        )

        try:
            # Receipt first makes an interrupted copy/publish recoverable but
            # never makes it visible: current.json remains on the old root.
            write_json_atomic(receipt_path, receipt.to_dict(), indent=2)
            pointer_payload = {
                "schema_version": _POINTER_SCHEMA_VERSION,
                "dataset": dataset,
                "revision": revision,
                "revision_id": revision_id,
                "generation_path": generation_relative,
                "receipt": receipt_relative,
                "content_digest": receipt.content_digest,
                "committed_at": committed_at,
            }
            # current.json is the commit marker. It is replaced as one inode,
            # so readers that already opened the old pointer finish on the old
            # generation while new readers select the new complete generation.
            write_json_atomic(self.pointer_path(dataset), pointer_payload, indent=2)
        except Exception:
            # Do not remove an old generation.  The unpublished new generation
            # and receipt are harmless recovery artefacts and can be garbage
            # collected by ``gc`` after an operator inspects the failure.
            raise

        # State is an index/cache of the pointer.  Keep it in sync after the
        # commit marker is durable.  If this final metadata write is
        # interrupted, query resolution remains correct and the next commit
        # uses current.json's revision number; ``sync_state`` repairs it.
        with self.state.transaction(dataset) as state:
            pointer = self._read_pointer(dataset)
            assert pointer is not None
            state.update(
                {
                    "revision": revision,
                    "revision_id": revision_id,
                    "revision_at": committed_at,
                    "revision_run_id": run_id,
                    "schema_version": schema_version,
                    "contract_fingerprint": contract_fingerprint,
                    "content_digest": receipt.content_digest,
                    "revision_receipt": receipt_relative,
                    "revision_pointer": receipt.pointer_path,
                    "changed_partitions": list(partitions),
                    "updated_at": committed_at,
                }
            )
        return receipt

    def latest(self, dataset: str) -> DatasetRevision | None:
        """Read the receipt referenced by the current committed pointer."""
        pointer = self._read_pointer(dataset)
        if pointer is None:
            state = self.state.get_payload(dataset)
            relative = state.get("revision_receipt")
        else:
            relative = pointer.get("receipt")
        if not relative:
            return None
        path = self._safe_meta_path(self._safe_relative(str(relative)), label="receipt")
        self._assert_regular(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["files"] = tuple(RevisionFile(**item) for item in payload.get("files", []))
        payload["changed_partitions"] = tuple(payload.get("changed_partitions", []))
        payload["generation_files"] = tuple(
            RevisionFile(**item) for item in payload.get("generation_files", [])
        )
        payload.setdefault("generation_path", "")
        payload.setdefault(
            "pointer_path", self.pointer_path(dataset).relative_to(self.meta_root).as_posix()
        )
        return DatasetRevision(**payload)

    def sync_state(self, dataset: str) -> None:
        """Repair the denormalised state file from the committed pointer."""
        pointer = self._read_pointer(dataset)
        if pointer is None or pointer.get("revision", 0) == 0:
            return
        receipt = self.latest(dataset)
        if receipt is None:
            raise RevisionConsistencyError(f"current pointer has no receipt: {dataset}")
        with self.state.transaction(dataset) as state:
            state.update(
                {
                    "revision": receipt.revision,
                    "revision_id": receipt.revision_id,
                    "revision_at": receipt.committed_at,
                    "revision_run_id": receipt.run_id,
                    "schema_version": receipt.schema_version,
                    "contract_fingerprint": receipt.contract_fingerprint,
                    "content_digest": receipt.content_digest,
                    "revision_receipt": pointer.get("receipt"),
                    "revision_pointer": self.pointer_path(dataset)
                    .relative_to(self.meta_root)
                    .as_posix(),
                    "changed_partitions": list(receipt.changed_partitions),
                    "updated_at": receipt.committed_at,
                }
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
        # supported logical layouts.  A caller can pass meta_root explicitly
        # for a custom root or a test fixture.
        if logical.parent.name in {"curated", "derived"}:
            meta = logical.parent.parent / "meta"
        else:
            meta = logical.parent.parent / "meta"
    else:
        # Preserve the lexical path until RevisionStore performs its lstat
        # boundary check.  Resolving first would turn a user-controlled
        # metadata symlink into an apparently ordinary directory.
        meta = Path(meta_root).expanduser()
    store = RevisionStore(meta, logical.parent)
    resolved = store.current_root(name, revision=revision)
    return logical if resolved is None else resolved


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
    store = RevisionStore(meta, logical.parent)
    pointer = store.current_pointer(name)
    if pointer is None:
        return None
    return int(pointer["revision"]), str(pointer["revision_id"])


@dataclass(frozen=True)
class GenerationPruneResult:
    """What a generation prune removed, per dataset."""

    dataset: str
    removed_revision_ids: tuple[str, ...]
    kept_revision_ids: tuple[str, ...]
    freed_bytes: int


def _directory_size(path: Path) -> int:
    total = 0
    for entry in path.rglob("*"):
        try:
            info = entry.lstat()
        except OSError:
            continue
        if stat.S_ISREG(info.st_mode):
            total += info.st_size
    return total


def prune_revision_generations(
    meta_root: Path | str,
    *,
    keep: int = 5,
    dry_run: bool = False,
) -> list[GenerationPruneResult]:
    """Drop the stored bytes of all but the newest *keep* generations.

    A commit copies the whole dataset into a new immutable generation, so the
    store grows by the dataset's full size per commit and nothing ever removed
    one: 307 generations and 16 GB on a two-year lake, of which ``adj_factors``
    alone was 46 generations and 9.5 GB — larger than ``curated/`` itself.

    Receipts are deliberately kept. They are a few KB each and carry the
    lineage: run id, code version, config fingerprint and the per-file sha256
    of the generation. Pruning removes the ability to *read* an old revision,
    not the record that it existed or the evidence of what was in it.

    The generation named by ``current.json`` is always kept, whatever *keep*
    says, because it is what every reader resolves to.
    """
    root = Path(meta_root).expanduser() / "revisions"
    if not root.is_dir():
        return []
    keep = max(1, int(keep))
    results: list[GenerationPruneResult] = []

    for dataset_dir in sorted(p for p in root.iterdir() if p.is_dir() and p.name != "data"):
        dataset = dataset_dir.name
        generations_root = root / "data" / dataset
        if not generations_root.is_dir():
            continue

        # Receipts are named ``<zero-padded revision>-<revision_id>.json``, so
        # the filename alone orders them without opening every file.
        receipts: list[tuple[int, str]] = []
        for receipt in generations_root.parent.parent.joinpath(dataset).glob("*.json"):
            if receipt.name == "current.json":
                continue
            number, _, remainder = receipt.stem.partition("-")
            if not number.isdigit() or not remainder:
                continue
            receipts.append((int(number), remainder))
        # The pre-revision baseline has no receipt, so without this it would
        # be dropped whatever ``keep`` said. It is generation zero; order it
        # as one so ``keep`` covers it like any other.
        if (generations_root / _LEGACY_REVISION_ID).is_dir():
            receipts.append((0, _LEGACY_REVISION_ID))
        receipts.sort()

        protected = {revision_id for _, revision_id in receipts[-keep:]}
        current = _read_json_quietly(dataset_dir / "current.json")
        current_id = str(current.get("revision_id", "")).strip() if current else ""
        if current_id:
            protected.add(current_id)

        removed: list[str] = []
        freed = 0
        for generation in sorted(p for p in generations_root.iterdir() if p.is_dir()):
            if generation.name in protected:
                continue
            freed += _directory_size(generation)
            removed.append(generation.name)
            if not dry_run:
                shutil.rmtree(generation, ignore_errors=True)

        if removed:
            results.append(
                GenerationPruneResult(
                    dataset=dataset,
                    removed_revision_ids=tuple(removed),
                    kept_revision_ids=tuple(sorted(protected)),
                    freed_bytes=freed,
                )
            )
    return results


def _read_json_quietly(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
