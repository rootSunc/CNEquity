"""Portable, immutable lake snapshots with checksummed restore support."""

from __future__ import annotations

import contextlib
import gzip
import hashlib
import importlib.util
import json
import os
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cnequity.config import Config
from cnequity.domain.contracts import contract_fingerprint, dataset_contract
from cnequity.domain.datasets import DATASETS
from cnequity.file_lock import lake_mutation_lock
from cnequity.provenance import runtime_lineage
from cnequity.storage.atomic import write_json_atomic, write_parquet_atomic
from cnequity.storage.revisions import resolve_committed_root
from cnequity.storage.state import StateStore

_SNAPSHOT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")

# Archive extraction is an untrusted-input boundary.  Keep finite defaults
# even though normal snapshots are usually much smaller; callers handling a
# deliberately larger lake can opt into a reviewed larger limit per import.
MAX_ARCHIVE_MEMBERS = 100_000
MAX_ARCHIVE_MEMBER_BYTES = 1 << 30  # 1 GiB per member
MAX_ARCHIVE_TOTAL_BYTES = 8 << 30  # 8 GiB across the complete tar stream

# More explicit aliases for integrations that describe this as tar safety.
MAX_TAR_MEMBERS = MAX_ARCHIVE_MEMBERS
MAX_TAR_MEMBER_BYTES = MAX_ARCHIVE_MEMBER_BYTES
MAX_TAR_TOTAL_BYTES = MAX_ARCHIVE_TOTAL_BYTES


def _archive_name(name: str) -> str:
    """Derive a safe snapshot name from a conventional archive filename."""

    value = Path(name).name
    for suffix in (".tar.zst", ".tzst", ".tar.gz", ".tgz", ".tar"):
        if value.endswith(suffix):
            value = value[: -len(suffix)]
            break
    if not value:
        raise ValueError(f"cannot derive snapshot name from archive: {name}")
    return value


def _zstd_backend() -> str | None:
    """Return the available zstd implementation, preferring stdlib bindings."""

    try:
        if importlib.util.find_spec("compression.zstd") is not None:
            return "stdlib"
    except (ImportError, ModuleNotFoundError):
        pass
    try:
        import zstandard  # type: ignore[import-not-found]

        del zstandard
        return "package"
    except ImportError:
        return "command" if shutil.which("zstd") else None


@contextlib.contextmanager
def _archive_writer(path: Path, compression: str):
    """Yield a binary stream for a streaming tar writer.

    The context owns every compressor process/stream and waits for it before
    returning.  A failed compressor never leaves a seemingly complete archive
    because the caller writes to a ``.part`` file and publishes it with
    ``os.replace`` only after this context exits successfully.
    """

    raw = path.open("wb")
    stream = raw
    process: subprocess.Popen[bytes] | None = None
    try:
        if compression == "zstd":
            backend = _zstd_backend()
            if backend == "stdlib":
                import compression.zstd as zstd  # type: ignore[import-not-found]

                stream = zstd.open(raw, "wb", level=3)
            elif backend == "package":
                import zstandard  # type: ignore[import-not-found]

                compressor = zstandard.ZstdCompressor(level=3)
                stream = compressor.stream_writer(raw, closefd=False)
            elif backend == "command":
                process = subprocess.Popen(
                    ["zstd", "-q", "-T0", "-c"],
                    stdin=subprocess.PIPE,
                    stdout=raw,
                )
                assert process.stdin is not None
                stream = process.stdin
            else:
                raise RuntimeError(
                    "zstd compression requested but no zstd implementation is available"
                )
        elif compression == "gzip":
            stream = gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=6, mtime=0)
        elif compression == "none":
            stream = raw
        else:
            raise ValueError(f"unsupported archive compression: {compression}")
        yield stream
        stream.flush()
        if compression == "zstd" and process is not None:
            stream.close()
            return_code = process.wait()
            if return_code:
                raise OSError(f"zstd compressor exited with status {return_code}")
        elif stream is not raw:
            stream.close()
        raw.flush()
        os.fsync(raw.fileno())
    except BaseException:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        raise
    finally:
        if stream is not raw:
            try:
                stream.close()
            except Exception:
                pass
        raw.close()


@contextlib.contextmanager
def _archive_reader(path: Path, compression: str):
    """Yield a binary stream for a streaming tar reader."""

    raw = path.open("rb")
    stream = raw
    process: subprocess.Popen[bytes] | None = None
    try:
        if compression == "zstd":
            backend = _zstd_backend()
            if backend == "stdlib":
                import compression.zstd as zstd  # type: ignore[import-not-found]

                stream = zstd.open(raw, "rb")
            elif backend == "package":
                import zstandard  # type: ignore[import-not-found]

                stream = zstandard.ZstdDecompressor().stream_reader(raw, closefd=False)
            elif backend == "command":
                process = subprocess.Popen(
                    ["zstd", "-q", "-d", "-c", str(path)],
                    stdout=subprocess.PIPE,
                )
                assert process.stdout is not None
                stream = process.stdout
            else:
                raise RuntimeError(
                    "zstd archive requires stdlib compression.zstd, zstandard, or zstd"
                )
        elif compression == "gzip":
            stream = gzip.GzipFile(fileobj=raw, mode="rb")
        elif compression == "none":
            stream = raw
        else:
            raise ValueError(f"unsupported archive compression: {compression}")
        yield stream
        if process is not None:
            return_code = process.wait()
            if return_code:
                raise OSError(f"zstd decompressor exited with status {return_code}")
    except BaseException:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        raise
    finally:
        if stream is not raw:
            try:
                stream.close()
            except Exception:
                pass
        raw.close()


def _archive_compression(path: Path, requested: str) -> tuple[str, Path]:
    """Resolve compression and a conventional extension for an archive path."""

    mode = str(requested or "auto").strip().lower()
    suffix = path.name.lower()
    if mode == "auto":
        if suffix.endswith((".tar.zst", ".tzst")):
            mode = "zstd"
        elif suffix.endswith((".tar.gz", ".tgz", ".gz")):
            mode = "gzip"
        elif suffix.endswith(".tar"):
            mode = "none"
        else:
            mode = "zstd" if _zstd_backend() else "gzip"
            path = path.with_name(path.name + (".tar.zst" if mode == "zstd" else ".tar.gz"))
    elif mode == "zstd" and not suffix.endswith((".tar.zst", ".tzst")):
        path = path.with_name(path.name + ".tar.zst")
    elif mode == "gzip" and not suffix.endswith((".tar.gz", ".tgz", ".gz")):
        path = path.with_name(path.name + ".tar.gz")
    elif mode == "none" and not suffix.endswith(".tar"):
        path = path.with_name(path.name + ".tar")
    if mode not in {"zstd", "gzip", "none"}:
        raise ValueError("archive compression must be 'auto', 'zstd', 'gzip' or 'none'")
    if mode == "zstd" and _zstd_backend() is None:
        raise RuntimeError("zstd compression requested but no zstd implementation is available")
    return mode, path


def _archive_member_relative(name: str) -> Path:
    """Validate a tar member name before it is joined to an extraction root."""

    if not isinstance(name, str) or not name or "\\" in name:
        raise ValueError(f"unsafe archive member path: {name!r}")
    path = Path(name)
    if path.is_absolute() or not path.parts or ".." in path.parts or "." in path.parts:
        raise ValueError(f"unsafe archive member path: {name!r}")
    return path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    # ``Path.open`` follows a symlink between a prior lstat and the open.  The
    # snapshot/delta code is a file-transfer boundary, so use O_NOFOLLOW when
    # the host provides it and inspect the opened descriptor as well.
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        opened = os.fstat(fd)
        if not stat.S_ISREG(opened.st_mode):
            raise ValueError(f"snapshot path is not a regular file: {path}")
        with os.fdopen(fd, "rb", closefd=True) as handle:
            fd = -1
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    finally:
        if fd != -1:
            os.close(fd)
    return digest.hexdigest()


def _lstat(path: Path) -> os.stat_result | None:
    """Return an entry's metadata without following links.

    ``None`` means the entry does not exist.  Permission and other filesystem
    errors are deliberately propagated: a verification or apply operation
    must fail closed rather than treating an unreadable path as absent.
    """

    try:
        return path.lstat()
    except FileNotFoundError:
        return None


def _reject_symlink_path(path: Path, *, label: str = "path") -> None:
    """Reject a path whose lexical root or existing ancestor is a symlink.

    Calling ``Path.resolve()`` before this check loses the distinction between
    a configured lake root and the directory it points at.  Keep the lexical
    path for the lstat walk, including missing final components, so a dangling
    link is rejected before any later ``mkdir``/open can follow it.
    """

    lexical = Path(path).expanduser()
    if not lexical.is_absolute():
        lexical = Path.cwd() / lexical
    cursor = Path(lexical.anchor)
    parts = lexical.parts
    for index, part in enumerate(parts):
        if part == lexical.anchor:
            continue
        cursor = cursor / part
        info = _lstat(cursor)
        if info is None:
            # No descendant can exist below an absent component.  The caller
            # may create it, but all existing ancestors have been checked.
            break
        # macOS exposes standard temporary paths through root-owned aliases
        # such as /var -> /private/var and /tmp -> /private/tmp.  Those links
        # are outside the caller-controlled lake boundary and are required for
        # tempfile-based restore drills.  The configured path itself and every
        # non-root-level (therefore potentially lake-controlled) link remain
        # forbidden.
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
        if index < len(parts) - 1 and not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"{label} ancestor is not a directory: {cursor}")


def _reject_link_or_non_regular(path: Path, *, hardlink: bool = False) -> os.stat_result:
    """Validate one path before reading/copying it.

    Symlinks are rejected even when their target is inside the lake.  Hard
    links are also rejected for snapshot materialization: otherwise two
    manifest paths could silently share one mutable inode after extraction.
    """

    info = _lstat(path)
    if info is None:
        raise FileNotFoundError(path)
    if stat.S_ISLNK(info.st_mode):
        raise ValueError(f"unsupported symlink: {path}")
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"unsupported non-regular file: {path}")
    if hardlink and info.st_nlink > 1:
        raise ValueError(f"unsupported hard link: {path}")
    return info


def _tree_files_no_follow(
    root: Path,
    *,
    reject_hardlinks: bool = False,
) -> tuple[set[str], set[str]]:
    """Return ``(relative entries, unsafe entries)`` without following links.

    Every non-directory entry is included in the first set so callers can
    compare the actual file set to a manifest.  Symlinks, special files and
    hard links are included in the second set and are never opened.
    """

    root = Path(root)
    _reject_symlink_path(root, label="tree root")
    actual: set[str] = set()
    unsafe: set[str] = set()

    def visit(directory: Path, relative_parent: Path) -> None:
        try:
            with os.scandir(directory) as iterator:
                entries = sorted(iterator, key=lambda entry: entry.name)
        except FileNotFoundError:
            return
        for entry in entries:
            relative = (relative_parent / entry.name).as_posix()
            try:
                info = entry.stat(follow_symlinks=False)
            except FileNotFoundError:
                # A concurrently removed entry cannot be a valid exact
                # snapshot; record it as unsafe so verification fails.
                unsafe.add(relative)
                continue
            if stat.S_ISDIR(info.st_mode):
                if stat.S_ISLNK(info.st_mode):
                    actual.add(relative)
                    unsafe.add(relative)
                else:
                    visit(Path(entry.path), relative_parent / entry.name)
                continue
            actual.add(relative)
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                unsafe.add(relative)
            elif reject_hardlinks and info.st_nlink > 1:
                unsafe.add(relative)

    root_info = _lstat(root)
    if root_info is not None and stat.S_ISDIR(root_info.st_mode):
        visit(root, Path())
    return actual, unsafe


def _safe_target_path(root: Path, relative: Path) -> Path:
    """Validate all existing target ancestors and return a safe destination.

    ``Path.resolve`` alone is insufficient for a dangling symlink: a missing
    final component makes ``exists()`` false even though ``mkdir`` would still
    follow its symlink parent.  Walk with lstat first, then verify the resolved
    parent remains below the target root.
    """

    root = Path(root)
    _reject_symlink_path(root, label="delta target root")
    target = root.resolve(strict=False)
    destination = target / relative
    cursor = target
    for index, part in enumerate(relative.parts):
        cursor = cursor / part
        info = _lstat(cursor)
        if info is None:
            # Once an ancestor is absent, all later components are absent for
            # this preflight.  The parent resolve check below still catches a
            # pre-existing link if the filesystem changed during the walk.
            break
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(f"delta target contains symlink ancestor: {cursor}")
        if index < len(relative.parts) - 1 and not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"delta target ancestor is not a directory: {cursor}")

    parent = destination.parent
    try:
        resolved_parent = parent.resolve(strict=False)
        resolved_parent.relative_to(target)
    except ValueError as exc:
        raise ValueError(f"delta target parent escapes lake root: {relative}") from exc
    return destination


def _ensure_target_parent(root: Path, relative: Path) -> Path:
    """Create missing destination directories without accepting links."""

    destination = _safe_target_path(root, relative)
    target = Path(root).resolve(strict=False)
    parent = destination.parent
    missing: list[Path] = []
    cursor = parent
    while cursor != target:
        info = _lstat(cursor)
        if info is None:
            missing.append(cursor)
            cursor = cursor.parent
            continue
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(f"delta target contains symlink ancestor: {cursor}")
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"delta target ancestor is not a directory: {cursor}")
        break
    for directory in reversed(missing):
        try:
            directory.mkdir()
        except FileExistsError:
            pass
        info = _lstat(directory)
        if info is None or stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"delta target parent is not a real directory: {directory}")
    return destination


def _reject_symlink_ancestors(root: Path, relative: Path) -> None:
    """Reject links or non-directory ancestors below a canonical lake root."""

    root = Path(root)
    _reject_symlink_path(root, label="lake root")
    root = root.resolve(strict=False)
    cursor = root
    for index, part in enumerate(relative.parts):
        cursor = cursor / part
        info = _lstat(cursor)
        if info is None:
            # Missing components are safe to create only after the caller has
            # run its own destination preflight.  There cannot be a later
            # existing ancestor without an existing parent.
            break
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(f"lake path contains symlink ancestor: {cursor}")
        if index < len(relative.parts) - 1 and not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"lake path ancestor is not a directory: {cursor}")


def _safe_relative(raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise ValueError(f"unsafe snapshot path: {raw}")
    return path


def _safe_lake_relative(raw: str) -> Path:
    """Validate a path that may be applied to a lake root.

    Delta packages intentionally use a single canonical path namespace.  The
    namespace is narrower than a generic snapshot path: an update may touch
    curated/derived data and the metadata needed to identify that data, but it
    must never be able to write an arbitrary file below the target root.
    """

    path = _safe_relative(raw)
    if path.parts[0] not in {"curated", "derived", "meta"}:
        raise ValueError(f"unsupported lake path: {raw}")
    if path.parts[0] == "meta":
        allowed = {"state", "revisions", "adj_factors_cache", "applied-deltas", "raw"}
        if len(path.parts) < 2 or path.parts[1] not in allowed:
            raise ValueError(f"unsupported lake metadata path: {raw}")
    return path


def _is_current_pointer(relative: Path) -> bool:
    """Whether a lake-relative path is a COW commit pointer."""

    return (
        len(relative.parts) == 4
        and relative.parts[0] == "meta"
        and relative.parts[1] == "revisions"
        and relative.parts[3] == "current.json"
    )


def _file_digest_record(path: Path, relative: Path, *, dataset: str, layer: str) -> dict[str, Any]:
    """Build the stable content record used by snapshot and delta indexes."""

    info = _reject_link_or_non_regular(path)
    return {
        "path": relative.as_posix(),
        "dataset": dataset,
        "layer": layer,
        "size_bytes": info.st_size,
        "sha256": _sha256(path),
    }


def _index_digest(index: Mapping[str, Mapping[str, Any]]) -> str:
    """Hash a lake index without embedding machine-specific absolute paths."""

    payload = [
        {
            "path": path,
            "dataset": str(record.get("dataset", "")),
            "layer": str(record.get("layer", "")),
            "size_bytes": int(record.get("size_bytes", 0)),
            "sha256": str(record.get("sha256", "")),
        }
        for path, record in sorted(index.items())
    ]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _read_json_object(path: Path) -> dict[str, Any]:
    """Read a JSON object and fail with a useful corruption error."""

    _reject_symlink_path(path, label="JSON path")
    _reject_link_or_non_regular(path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        opened = os.fstat(fd)
        if not stat.S_ISREG(opened.st_mode):
            raise ValueError(f"JSON path is not a regular file: {path}")
        with os.fdopen(fd, "r", encoding="utf-8", closefd=True) as handle:
            fd = -1
            payload = json.load(handle)
    finally:
        if fd != -1:
            os.close(fd)
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def _manifest_contract_issues(manifest: Mapping[str, Any]) -> list[str]:
    """Return contract inconsistencies recorded in a snapshot manifest.

    The file hashes protect bytes, but the manifest itself also carries the
    registry contract and is intentionally not included in its own file list.
    Checking both the global registry identity and every selected dataset
    prevents a manifest-only edit from changing the meaning of an otherwise
    byte-identical snapshot.
    """

    issues: list[str] = []
    raw_datasets = manifest.get("datasets", [])
    if not isinstance(raw_datasets, list) or not raw_datasets:
        return ["datasets"]
    datasets = [str(item) for item in raw_datasets]
    if len(set(datasets)) != len(datasets):
        issues.append("datasets")

    global_expected = manifest.get("contract_fingerprint")
    if not isinstance(global_expected, str) or not global_expected:
        issues.append("contract_fingerprint")
    elif global_expected != contract_fingerprint():
        issues.append("contract_fingerprint")

    contracts = manifest.get("contracts")
    if not isinstance(contracts, Mapping):
        issues.append("contracts")
        return issues

    unknown_contracts = set(str(key) for key in contracts) - set(datasets)
    issues.extend(f"contract:{name}" for name in sorted(unknown_contracts))

    aliases = manifest.get("contract_fingerprints")
    if aliases is not None and not isinstance(aliases, Mapping):
        issues.append("contract_fingerprints")
        aliases = {}
    for dataset in datasets:
        try:
            expected = contracts.get(dataset)
            if not isinstance(expected, Mapping):
                issues.append(f"contract:{dataset}")
                continue
            actual = dataset_contract(dataset)
            expected_schema = expected.get("schema_version")
            if isinstance(expected_schema, bool) or expected_schema != actual.get("schema_version"):
                issues.append(f"contract:{dataset}:schema_version")
            if expected.get("fingerprint") != contract_fingerprint(dataset):
                issues.append(f"contract:{dataset}")
            if isinstance(aliases, Mapping) and dataset in aliases:
                if aliases.get(dataset) != contract_fingerprint(dataset):
                    issues.append(f"contract_fingerprints:{dataset}")
        except (KeyError, TypeError, ValueError):
            issues.append(f"contract:{dataset}")
    return issues


def _snapshot_path(snapshot: Path, raw: Any, *, label: str) -> Path:
    """Resolve a path stored inside a snapshot's ``meta`` namespace safely."""

    relative = _safe_relative(str(raw))
    target = snapshot / relative
    _reject_symlink_path(target, label=label)
    resolved = target.resolve(strict=False)
    try:
        resolved.relative_to(snapshot.resolve(strict=False))
    except ValueError as exc:
        raise ValueError(f"{label} escapes snapshot root: {raw}") from exc
    return resolved


def _snapshot_meta_path(snapshot: Path, raw: Any, *, label: str) -> Path:
    relative = _safe_relative(str(raw))
    if relative.parts[0] != "meta":
        raise ValueError(f"{label} must be below meta/: {raw}")
    return _snapshot_path(snapshot, relative, label=label)


def _manifest_state_issues(snapshot: Path, manifest: Mapping[str, Any]) -> list[str]:
    """Check state, pointer, receipt and watermark semantics in a snapshot.

    ``dataset_states`` is denormalised metadata and is not part of the byte
    records, so checksum verification alone cannot detect a forged watermark
    or a state/pointer disagreement.  This routine is deliberately read-only;
    restore calls the same verifier before writing anything to its target.
    """

    issues: list[str] = []
    raw_datasets = manifest.get("datasets", [])
    if not isinstance(raw_datasets, list):
        return ["datasets"]
    datasets = [str(item) for item in raw_datasets]
    raw_states = manifest.get("dataset_states", {})
    if not isinstance(raw_states, Mapping):
        return ["dataset_states"]
    missing_states = set(datasets) - set(str(key) for key in raw_states)
    issues.extend(f"state:{name}" for name in sorted(missing_states))
    unknown_states = set(str(key) for key in raw_states) - set(datasets)
    issues.extend(f"state:{name}" for name in sorted(unknown_states))

    # Build a map of data files once.  Data paths are the only files that can
    # establish a truthful ``last_success_trade_date`` watermark.
    records = manifest.get("files", [])
    data_files: dict[str, list[Path]] = {dataset: [] for dataset in datasets}
    if isinstance(records, list):
        for record in records:
            if not isinstance(record, Mapping):
                issues.append("files")
                continue
            dataset = str(record.get("dataset", ""))
            layer = str(record.get("layer", ""))
            raw_path = record.get("path")
            if dataset not in data_files or dataset not in DATASETS:
                issues.append(f"file:{raw_path}:dataset")
                continue
            if not isinstance(raw_path, str):
                issues.append(f"file:{raw_path}:path")
                continue
            try:
                relative = _safe_relative(raw_path)
            except ValueError:
                issues.append(f"file:{raw_path}:path")
                continue
            if relative.parts[0] == "data":
                expected_layer = str(DATASETS[dataset].layer)
                if (
                    layer != expected_layer
                    or len(relative.parts) < 3
                    or relative.parts[1] != expected_layer
                    or relative.parts[2] != dataset
                ):
                    issues.append(f"file:{raw_path}:dataset")
                    continue
                path = snapshot / relative
                if relative.parts[2] == dataset:
                    data_files[dataset].append(path)
            elif relative.parts[0] == "meta":
                if layer != "meta":
                    issues.append(f"file:{raw_path}:layer")
            else:
                issues.append(f"file:{raw_path}:root")

    # Lazy import keeps storage verification usable for callers that only work
    # with manifests and do not import the query stack at module load time.
    polars = None

    def max_data_date(dataset: str, column: str) -> Any:
        nonlocal polars
        values: list[Any] = []
        if polars is None:
            try:
                import polars as polars_module

                polars = polars_module
            except ImportError:
                return None
        for path in data_files.get(dataset, []):
            try:
                frame = polars.read_parquet(path, columns=[column])
                if column in frame.columns and not frame.is_empty():
                    value = frame[column].max()
                    if value is not None:
                        values.append(value)
            except Exception:
                # Byte/hash verification already reports an unreadable file;
                # avoid hiding that finding behind a second parser exception.
                continue
        return max(values) if values else None

    created_day: Any = None
    try:
        created_day = datetime.fromisoformat(str(manifest.get("created_at", ""))).date()
    except (TypeError, ValueError):
        pass

    for dataset in datasets:
        state = raw_states.get(dataset, {})
        if state == {}:
            state = {}
        if not isinstance(state, Mapping):
            issues.append(f"state:{dataset}")
            continue
        spec = DATASETS.get(dataset)
        if spec is None:
            issues.append(f"dataset:{dataset}")
            continue

        for field in ("last_success_trade_date", "last_snapshot_date"):
            if field not in state or state.get(field) in (None, ""):
                continue
            try:
                marker = datetime.fromisoformat(str(state[field])).date()
            except (TypeError, ValueError):
                issues.append(f"state:{dataset}:{field}")
                continue
            # A capture/watermark marker cannot claim a future observation
            # relative to the snapshot itself.  For trade-date watermarks also
            # require a row at or beyond the claimed date; a marker ahead of
            # the bytes is the classic forged-watermark failure.
            if created_day is not None and marker > created_day:
                issues.append(f"state:{dataset}:{field}")
            if field == "last_success_trade_date":
                date_column = spec.query_date_col or spec.date_col
                if date_column:
                    maximum = max_data_date(dataset, date_column)
                    if maximum is not None:
                        try:
                            maximum_day = maximum.date() if hasattr(maximum, "date") else maximum
                            if marker > maximum_day:
                                issues.append(f"state:{dataset}:{field}")
                        except (AttributeError, TypeError, ValueError):
                            issues.append(f"state:{dataset}:{field}")

        revision = state.get("revision")
        if revision is not None:
            if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
                issues.append(f"state:{dataset}:revision")
                revision = None
        revision_id = state.get("revision_id")
        if revision is not None and revision > 0 and not isinstance(revision_id, str):
            issues.append(f"state:{dataset}:revision_id")

        state_receipt: dict[str, Any] | None = None
        receipt_raw = state.get("revision_receipt")
        if receipt_raw:
            try:
                receipt_relative = _safe_relative(str(receipt_raw))
                if receipt_relative.parts[:2] != ("revisions", dataset):
                    raise ValueError("state receipt has wrong dataset")
                receipt_path = _snapshot_meta_path(
                    snapshot,
                    Path("meta") / receipt_relative,
                    label=f"state receipt:{dataset}",
                )
                state_receipt = _read_json_object(receipt_path)
                if state_receipt.get("dataset") not in (None, dataset):
                    issues.append(f"state:{dataset}:receipt")
                if revision is not None and state_receipt.get("revision") != revision:
                    issues.append(f"state:{dataset}:receipt")
                if revision_id is not None and state_receipt.get("revision_id") != revision_id:
                    issues.append(f"state:{dataset}:receipt")
                if state.get("content_digest") and state_receipt.get("content_digest"):
                    if state["content_digest"] != state_receipt["content_digest"]:
                        issues.append(f"state:{dataset}:content_digest")
                for field in ("schema_version", "contract_fingerprint"):
                    if state.get(field) is not None and state_receipt.get(field) is not None:
                        if state[field] != state_receipt[field]:
                            issues.append(f"state:{dataset}:{field}")
                changed = state.get("changed_partitions")
                receipt_changed = state_receipt.get("changed_partitions")
                if changed is not None and receipt_changed is not None:
                    if changed != receipt_changed:
                        issues.append(f"state:{dataset}:changed_partitions")
            except (OSError, TypeError, ValueError, json.JSONDecodeError):
                issues.append(f"state:{dataset}:receipt")

        pointer_path = snapshot / "meta" / "revisions" / dataset / "current.json"
        pointer: dict[str, Any] | None = None
        if _lstat(pointer_path) is not None:
            try:
                pointer = _read_json_object(pointer_path)
            except (OSError, TypeError, ValueError, json.JSONDecodeError):
                issues.append(f"pointer:{dataset}")
        elif state.get("revision_pointer"):
            # A state that explicitly advertises a pointer must not lose it in
            # a supposedly portable snapshot.  Old state files without this
            # field remain compatible with pre-COW snapshots.
            issues.append(f"pointer:{dataset}")

        if pointer is None:
            continue
        pointer_revision = pointer.get("revision")
        if (
            pointer.get("schema_version") != 1
            or pointer.get("dataset") != dataset
            or isinstance(pointer_revision, bool)
            or not isinstance(pointer_revision, int)
            or pointer_revision < 0
        ):
            issues.append(f"pointer:{dataset}")
            continue
        pointer_id = pointer.get("revision_id")
        if not isinstance(pointer_id, str) or not pointer_id:
            issues.append(f"pointer:{dataset}")
        if revision is not None and pointer_revision != revision:
            issues.append(f"state:{dataset}:pointer")
        if revision_id is not None and pointer_id != revision_id:
            issues.append(f"state:{dataset}:pointer")
        if pointer_revision > 0:
            if revision is None:
                issues.append(f"state:{dataset}:revision")
            if not isinstance(revision_id, str) or not revision_id:
                issues.append(f"state:{dataset}:revision_id")
            if not receipt_raw:
                issues.append(f"state:{dataset}:receipt")
        pointer_relative = Path("meta") / "revisions" / dataset / "current.json"
        # RevisionStore receipts historically store metadata paths relative
        # to ``meta_root`` (``revisions/...``), while snapshot manifests use
        # lake-root paths (``meta/revisions/...``).  Accept both spellings but
        # never accept a different dataset or an escaping path.
        pointer_relative_meta = Path("revisions") / dataset / "current.json"
        if state.get("revision_pointer"):
            try:
                if _safe_relative(str(state["revision_pointer"])) not in {
                    pointer_relative,
                    pointer_relative_meta,
                }:
                    issues.append(f"state:{dataset}:revision_pointer")
            except ValueError:
                issues.append(f"state:{dataset}:revision_pointer")
        generation_raw = pointer.get("generation_path") or pointer.get("root")
        try:
            generation_relative = _safe_relative(str(generation_raw))
            if generation_relative.parts[:3] != ("revisions", "data", dataset):
                raise ValueError("pointer generation has wrong dataset")
            generation_path = _snapshot_meta_path(
                snapshot,
                Path("meta") / generation_relative,
                label=f"pointer generation:{dataset}",
            )
            if not generation_path.is_dir():
                raise FileNotFoundError(generation_path)
            _tree_files_no_follow(generation_path, reject_hardlinks=True)
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            issues.append(f"pointer:{dataset}:generation")

        pointer_receipt = pointer.get("receipt")
        if pointer_revision > 0:
            if not pointer_receipt:
                issues.append(f"pointer:{dataset}:receipt")
            else:
                try:
                    pointer_receipt_relative = _safe_relative(str(pointer_receipt))
                    if pointer_receipt_relative.parts[:2] != ("revisions", dataset):
                        raise ValueError("pointer receipt has wrong dataset")
                    pointer_receipt_path = _snapshot_meta_path(
                        snapshot,
                        Path("meta") / pointer_receipt_relative,
                        label=f"pointer receipt:{dataset}",
                    )
                    pointer_receipt_payload = _read_json_object(pointer_receipt_path)
                    if (
                        pointer_receipt_payload.get("dataset") not in (None, dataset)
                        or pointer_receipt_payload.get("revision") != pointer_revision
                        or pointer_receipt_payload.get("revision_id") != pointer_id
                    ):
                        issues.append(f"pointer:{dataset}:receipt")
                    if (
                        generation_raw
                        and pointer_receipt_payload.get("generation_path")
                        and pointer_receipt_payload["generation_path"] != generation_raw
                    ):
                        issues.append(f"pointer:{dataset}:generation")
                    if (
                        pointer.get("content_digest") is not None
                        and pointer_receipt_payload.get("content_digest") is not None
                        and pointer.get("content_digest")
                        != pointer_receipt_payload.get("content_digest")
                    ):
                        issues.append(f"pointer:{dataset}:content_digest")
                    if state_receipt is not None:
                        if (
                            state_receipt.get("generation_path") is not None
                            and pointer_receipt_payload.get("generation_path") is not None
                            and state_receipt.get("generation_path")
                            != pointer_receipt_payload.get("generation_path")
                        ):
                            issues.append(f"state:{dataset}:pointer_receipt")
                        try:
                            state_receipt_relative = _safe_relative(str(receipt_raw))
                            pointer_receipt_relative = _safe_relative(str(pointer_receipt))
                            if state_receipt_relative != pointer_receipt_relative:
                                issues.append(f"state:{dataset}:pointer_receipt")
                        except ValueError:
                            issues.append(f"state:{dataset}:pointer_receipt")
                    if state_receipt is not None and pointer_receipt_payload != state_receipt:
                        # Receipts are immutable identities.  A state file may
                        # be a denormalised copy, but it must reference this
                        # exact receipt rather than a different revision.
                        if (
                            state_receipt.get("revision") != pointer_receipt_payload.get("revision")
                            or state_receipt.get("revision_id")
                            != pointer_receipt_payload.get("revision_id")
                            or state_receipt.get("content_digest")
                            != pointer_receipt_payload.get("content_digest")
                        ):
                            issues.append(f"state:{dataset}:pointer_receipt")
                except (OSError, TypeError, ValueError, json.JSONDecodeError):
                    issues.append(f"pointer:{dataset}:receipt")
        elif pointer.get("receipt") not in (None, ""):
            issues.append(f"pointer:{dataset}:receipt")

    return sorted(set(issues))


@dataclass(frozen=True)
class SnapshotFile:
    dataset: str
    layer: str
    path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class SnapshotVerification:
    snapshot: str
    passed: bool
    verified_files: int
    missing: tuple[str, ...]
    mismatched: tuple[str, ...]


@dataclass(frozen=True)
class DeltaVerification:
    """Result of checking the payload files in an immutable delta package.

    ``missing`` and ``mismatched`` contain package paths, while ``invalid``
    contains malformed change records.  Keeping this shape close to
    :class:`SnapshotVerification` lets callers use the same release gate for
    both a full baseline and a small incremental package.
    """

    delta: str
    passed: bool
    verified_files: int
    missing: tuple[str, ...]
    mismatched: tuple[str, ...]
    invalid: tuple[str, ...] = ()


@dataclass(frozen=True)
class DeltaChange:
    """One add/replace/delete operation in a delta manifest."""

    operation: str
    path: str
    package_path: str | None
    dataset: str
    layer: str
    size_bytes: int | None
    sha256: str | None
    old_size_bytes: int | None = None
    old_sha256: str | None = None
    allow_missing: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SnapshotStore:
    """Create and restore snapshots below ``meta/snapshots`` or an explicit root."""

    def __init__(self, config: Config, snapshot_root: Path | None = None):
        self.config = config
        self.root = Path(snapshot_root or config.meta_root / "snapshots")

    @staticmethod
    def _validate_name(name: str) -> str:
        if not _SNAPSHOT_NAME.fullmatch(name):
            raise ValueError(
                "snapshot name must be 1-80 characters using letters, digits, '.', '_' or '-'"
            )
        return name

    def path(self, name: str) -> Path:
        candidate = self.root / self._validate_name(name)
        _reject_symlink_path(candidate, label="snapshot path")
        return candidate

    def _source_root(self, dataset: str) -> tuple[str, Path]:
        spec = DATASETS[dataset]
        # Check configured roots before the revision resolver canonicalises
        # them.  Otherwise a data-root symlink is indistinguishable from the
        # real lake and a snapshot could silently read outside the configured
        # boundary.
        _reject_symlink_path(self.config.data_root, label="data root")
        _reject_symlink_path(self.config.meta_root, label="metadata root")
        base = self.config.derived_root if spec.layer == "derived" else self.config.curated_root
        logical = base / dataset
        _reject_symlink_path(base, label="lake source root")
        _reject_symlink_path(logical, label="dataset root")
        return spec.layer, resolve_committed_root(
            logical,
            dataset=dataset,
            meta_root=self.config.meta_root,
        )

    def create(self, name: str, datasets: list[str]) -> Path:
        """Copy selected datasets into a new immutable snapshot directory.

        Snapshot creation is a lake read transaction, not just a collection of
        independent file copies.  Compaction/revision publication uses the
        same mutation lock, so holding it from the state read through the
        final directory rename keeps the state, receipt, pointer and generation
        bytes in one view.  In particular, a snapshot cannot capture the old
        state alongside the new pointer (or vice versa) while a commit is in
        flight.
        """
        # Validate before acquiring the lock because lock acquisition creates
        # ``meta/locks``.  A user-controlled symlinked metadata root must not
        # be followed merely as a side effect of taking a read lock.
        _reject_symlink_path(self.config.meta_root, label="metadata root")
        _reject_symlink_path(self.config.data_root, label="data root")
        with lake_mutation_lock(self.config.meta_root, blocking=True):
            return self._create_locked(name, datasets)

    def _create_locked(self, name: str, datasets: list[str]) -> Path:
        """Implementation of :meth:`create`; caller owns the lake lock."""
        selected = sorted(set(datasets))
        if not selected:
            raise ValueError("at least one dataset is required")
        unknown = sorted(set(selected) - set(DATASETS))
        if unknown:
            raise ValueError(f"unknown dataset(s): {', '.join(unknown)}")
        # Reject configured roots before reading state or creating a snapshot
        # staging directory.  A symlinked data/meta root must never be
        # canonicalised into an apparently ordinary lake first.
        _reject_symlink_path(self.config.data_root, label="data root")
        _reject_symlink_path(self.config.meta_root, label="metadata root")
        _reject_symlink_path(self.config.meta_root / "state", label="state root")
        for dataset in selected:
            _reject_symlink_path(
                self.config.meta_root / "state" / f"{dataset}.json",
                label="dataset state",
            )
        destination = self.path(name)
        if destination.exists():
            raise FileExistsError(f"snapshot already exists: {destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp = Path(tempfile.mkdtemp(prefix=f".{name}-", dir=destination.parent))
        state = StateStore(self.config.meta_root)
        dataset_states: dict[str, dict] = {}
        records: list[SnapshotFile] = []
        runtime_state: dict[str, Any] = {}
        try:
            dataset_states = {dataset: state.get_payload(dataset) for dataset in selected}
            for dataset in selected:
                layer, source = self._source_root(dataset)
                if not source.is_dir():
                    raise FileNotFoundError(f"dataset has no stored files: {dataset}")
                source_entries, unsafe = _tree_files_no_follow(source, reject_hardlinks=True)
                if unsafe:
                    raise ValueError(
                        f"dataset contains unsupported link or special file(s): "
                        f"{', '.join(sorted(unsafe)[:8])}"
                    )
                files = sorted(
                    source / relative
                    for relative in map(Path, source_entries)
                    if relative.suffix == ".parquet"
                )
                if not files:
                    raise FileNotFoundError(f"dataset has no parquet files: {dataset}")
                for source_file in files:
                    _reject_link_or_non_regular(source_file, hardlink=True)
                    relative = source_file.relative_to(source)
                    stored = temp / "data" / layer / dataset / relative
                    stored.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source_file, stored)
                    records.append(
                        SnapshotFile(
                            dataset=dataset,
                            layer=layer,
                            path=(Path("data") / layer / dataset / relative).as_posix(),
                            size_bytes=stored.stat().st_size,
                            sha256=_sha256(stored),
                        )
                    )

            _reject_symlink_path(self.config.meta_root, label="metadata root")
            meta_root = self.config.meta_root.resolve(strict=False)
            for dataset, payload in dataset_states.items():
                receipt = payload.get("revision_receipt")
                if receipt:
                    receipt_logical = meta_root / _safe_relative(str(receipt))
                    _reject_symlink_path(receipt_logical, label="revision receipt")
                    receipt_source = receipt_logical.resolve(strict=False)
                    try:
                        receipt_relative = receipt_source.relative_to(meta_root)
                    except ValueError as exc:
                        raise ValueError(
                            f"revision receipt is outside meta root: {receipt}"
                        ) from exc
                    if not receipt_source.is_file():
                        raise FileNotFoundError(
                            f"dataset state references missing revision receipt: {receipt_source}"
                        )
                    _reject_link_or_non_regular(receipt_source, hardlink=True)
                    stored = temp / "meta" / receipt_relative
                    stored.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(receipt_source, stored)
                    records.append(
                        SnapshotFile(
                            dataset=dataset,
                            layer="meta",
                            path=(Path("meta") / receipt_relative).as_posix(),
                            size_bytes=stored.stat().st_size,
                            sha256=_sha256(stored),
                        )
                    )

                # A revision receipt points at an immutable generation under
                # meta/revisions/data. Include both the pointer and the
                # generation bytes, otherwise restoring a snapshot would
                # retain a dangling pointer and the query resolver would
                # correctly fail closed. Older snapshots without a pointer
                # continue through the legacy path above.
                pointer_source = meta_root / "revisions" / dataset / "current.json"
                _reject_symlink_path(pointer_source, label="revision pointer")
                if pointer_source.is_file():
                    pointer_relative = pointer_source.relative_to(meta_root)
                    stored_pointer = temp / "meta" / pointer_relative
                    stored_pointer.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(pointer_source, stored_pointer)
                    records.append(
                        SnapshotFile(
                            dataset=dataset,
                            layer="meta",
                            path=(Path("meta") / pointer_relative).as_posix(),
                            size_bytes=stored_pointer.stat().st_size,
                            sha256=_sha256(stored_pointer),
                        )
                    )
                    try:
                        pointer_payload = _read_json_object(pointer_source)
                        generation_relative = _safe_relative(
                            str(pointer_payload.get("generation_path", ""))
                        )
                        generation_logical = meta_root / generation_relative
                        _reject_symlink_path(generation_logical, label="revision generation")
                        generation_source = generation_logical.resolve(strict=False)
                        generation_source.relative_to(meta_root)
                    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
                        raise ValueError(
                            f"invalid current revision pointer for {dataset}: {pointer_source}"
                        ) from exc
                    if not generation_source.is_dir():
                        raise FileNotFoundError(
                            f"current revision generation is missing: {generation_source}"
                        )
                    generation_entries, unsafe = _tree_files_no_follow(
                        generation_source, reject_hardlinks=True
                    )
                    if unsafe:
                        raise ValueError(
                            f"revision generation contains unsupported link or special file(s): "
                            f"{', '.join(sorted(unsafe)[:8])}"
                        )
                    for generation_file in sorted(
                        generation_source / relative
                        for relative in map(Path, generation_entries)
                        if relative.suffix == ".parquet"
                    ):
                        _reject_link_or_non_regular(generation_file, hardlink=True)
                        relative = generation_file.relative_to(meta_root)
                        stored = temp / "meta" / relative
                        stored.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(generation_file, stored)
                        records.append(
                            SnapshotFile(
                                dataset=dataset,
                                layer="meta",
                                path=(Path("meta") / relative).as_posix(),
                                size_bytes=stored.stat().st_size,
                                sha256=_sha256(stored),
                            )
                        )

            # Adjustment-factor HTTP responses are a hot, sparse cache rather
            # than the query-facing derived table.  It is cheap to include it
            # when an adjustment snapshot is requested and doing so turns a
            # restored lake into a warm lake immediately.  Older snapshots
            # did not carry this directory; restore below can rebuild it from
            # the aligned ``derived/adj_factors`` rows instead.
            if "adj_factors" in selected:
                cache_source = self.config.meta_root / "adj_factors_cache"
                cache_entries, unsafe = (
                    _tree_files_no_follow(cache_source, reject_hardlinks=True)
                    if cache_source.is_dir()
                    else (set(), set())
                )
                if unsafe:
                    raise ValueError(
                        f"adjustment cache contains unsupported link or special file(s): "
                        f"{', '.join(sorted(unsafe)[:8])}"
                    )
                cache_files = sorted(
                    cache_source / relative
                    for relative in map(Path, cache_entries)
                    if relative.suffix == ".parquet" and len(relative.parts) == 1
                )
                copied_cache = 0
                for source_file in cache_files:
                    _reject_link_or_non_regular(source_file, hardlink=True)
                    relative = Path("meta") / "adj_factors_cache" / source_file.name
                    stored = temp / relative
                    stored.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source_file, stored)
                    records.append(
                        SnapshotFile(
                            dataset="adj_factors",
                            layer="meta",
                            path=relative.as_posix(),
                            size_bytes=stored.stat().st_size,
                            sha256=_sha256(stored),
                        )
                    )
                    copied_cache += 1
                runtime_state["adj_factors_cache"] = {
                    "mode": "included" if copied_cache else "rebuildable",
                    "files": copied_cache,
                    "rebuild_from": "derived/adj_factors",
                }

            # Critical source payload archives are part of a portable baseline
            # for the datasets that requested them. Keep the exact compressed
            # bytes and sidecars; no request credentials live in the archive.
            raw_root = self.config.meta_root / "raw"
            _reject_symlink_path(raw_root, label="raw archive root")
            for dataset in selected:
                source_dir = raw_root / dataset
                _reject_symlink_path(source_dir, label="raw dataset root")
                if not source_dir.is_dir():
                    continue
                source_entries, unsafe = _tree_files_no_follow(source_dir, reject_hardlinks=True)
                if unsafe:
                    raise ValueError(
                        f"raw archive contains unsupported link or special file(s): "
                        f"{', '.join(sorted(unsafe)[:8])}"
                    )
                for relative_source in sorted(map(Path, source_entries)):
                    source_file = source_dir / relative_source
                    # Sidecars are intentionally preserved regardless of
                    # extension; only directories are absent from the
                    # non-following file set.
                    _reject_link_or_non_regular(source_file, hardlink=True)
                    relative = Path("meta") / "raw" / dataset / source_file.relative_to(source_dir)
                    stored = temp / relative
                    stored.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source_file, stored)
                    records.append(
                        SnapshotFile(
                            dataset=dataset,
                            layer="meta",
                            path=relative.as_posix(),
                            size_bytes=stored.stat().st_size,
                            sha256=_sha256(stored),
                        )
                    )

            manifest = {
                "format": "cnequity.lake-snapshot",
                "format_version": 1,
                "name": name,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "datasets": selected,
                "dataset_states": dataset_states,
                "contracts": {
                    dataset: {
                        "schema_version": dataset_contract(dataset)["schema_version"],
                        "fingerprint": contract_fingerprint(dataset),
                    }
                    for dataset in selected
                },
                # Keep both names: ``contract_fingerprint`` is convenient for
                # a release gate, while the per-dataset map above remains the
                # backwards-compatible detailed contract surface.
                "contract_fingerprint": contract_fingerprint(),
                "contract_fingerprints": {
                    dataset: contract_fingerprint(dataset) for dataset in selected
                },
                "lineage": runtime_lineage(self.config),
                "runtime_state": runtime_state,
                "files": [asdict(item) for item in records],
            }
            write_json_atomic(temp / "manifest.json", manifest, indent=2, ensure_ascii=False)
            # Do not publish a package whose denormalised state disagrees with
            # its pointer/receipt, even if all copied bytes are individually
            # valid.  The same semantic verifier is used by ``verify`` and
            # ``restore``; running it before the final rename makes snapshot
            # creation itself a consistent transaction boundary.
            verification = self._verify_directory(name, temp, manifest)
            if not verification.passed:
                raise ValueError(
                    f"snapshot consistency check failed: missing={verification.missing}, "
                    f"mismatched={verification.mismatched}"
                )
            os.replace(temp, destination)
        except Exception:
            shutil.rmtree(temp, ignore_errors=True)
            raise
        return destination / "manifest.json"

    def _manifest(self, name: str) -> tuple[Path, dict]:
        snapshot = self.path(name)
        manifest = snapshot / "manifest.json"
        try:
            payload = _read_json_object(manifest)
        except FileNotFoundError:
            # Name what the operator typed. The bare manifest path reads as a
            # path they never wrote, on a command whose only argument was NAME.
            raise FileNotFoundError(f"no snapshot named {name!r} ({manifest})") from None
        if payload.get("format") != "cnequity.lake-snapshot" or payload.get("format_version") != 1:
            raise ValueError(f"unsupported snapshot manifest: {manifest}")
        return snapshot, payload

    def verify(self, name: str) -> SnapshotVerification:
        snapshot, manifest = self._manifest(name)
        return self._verify_directory(name, snapshot, manifest)

    @staticmethod
    def _verify_directory(
        name: str, snapshot: Path, manifest: Mapping[str, Any]
    ) -> SnapshotVerification:
        """Verify a materialized snapshot directory against its manifest."""

        _reject_symlink_path(snapshot, label="snapshot root")
        missing: list[str] = []
        mismatched: list[str] = []
        verified = 0
        records = manifest.get("files", [])
        if not isinstance(records, list):
            raise ValueError(f"snapshot files must be a list: {snapshot / 'manifest.json'}")
        actual_files, unsafe_files = _tree_files_no_follow(snapshot, reject_hardlinks=True)
        seen: set[str] = set()
        for record in records:
            if not isinstance(record, Mapping):
                mismatched.append(str(record))
                continue
            try:
                relative = _safe_relative(str(record["path"]))
                key = relative.as_posix()
                if key == "manifest.json":
                    mismatched.append(key)
                    continue
                if key in seen:
                    mismatched.append(key)
                    continue
                seen.add(key)
                expected_size = int(record["size_bytes"])
                expected_sha = str(record["sha256"])
            except (KeyError, TypeError):
                mismatched.append(str(record.get("path", record)))
                continue
            except ValueError as exc:
                # A manifest-controlled path is a security boundary.  Keep
                # malformed metadata reportable, but do not turn traversal
                # into an ordinary checksum mismatch that callers might
                # accidentally ignore.
                if "unsafe snapshot path" in str(exc):
                    raise
                mismatched.append(str(record.get("path", record)))
                continue
            if key in unsafe_files:
                mismatched.append(key)
                continue
            if key not in actual_files:
                missing.append(relative.as_posix())
                continue
            path = snapshot / relative
            if path.stat().st_size != expected_size or _sha256(path) != expected_sha:
                mismatched.append(relative.as_posix())
                continue
            verified += 1
        if "manifest.json" not in actual_files:
            missing.append("manifest.json")
        for path in sorted(unsafe_files):
            if path not in mismatched:
                mismatched.append(path)
        expected_files = {"manifest.json", *seen}
        for path in sorted(actual_files - expected_files):
            mismatched.append(path)
        mismatched.extend(_manifest_contract_issues(manifest))
        mismatched.extend(_manifest_state_issues(snapshot, manifest))
        # A malformed manifest field is reported once even when it also caused
        # an ordinary file-set mismatch.  Stable diagnostics make release and
        # restore tooling easier to consume.
        mismatched = list(dict.fromkeys(mismatched))
        return SnapshotVerification(
            snapshot=name,
            passed=not missing and not mismatched and verified == len(manifest.get("files", [])),
            verified_files=verified,
            missing=tuple(missing),
            mismatched=tuple(mismatched),
        )

    def export_archive(
        self,
        name: str,
        destination: Path | str | None = None,
        *,
        compression: str = "auto",
    ) -> Path:
        """Stream an immutable snapshot into one tar archive.

        ``tar.zst`` is preferred when Python's stdlib ``compression.zstd``,
        the optional ``zstandard`` package, or a ``zstd`` executable is
        available.  Gzip remains a portable fallback.  The destination is
        first written as a same-directory ``.part`` file and atomically
        renamed only after tar/compressor completion.
        """

        snapshot, manifest = self._manifest(name)
        verification = self._verify_directory(name, snapshot, manifest)
        if not verification.passed:
            raise ValueError(
                f"snapshot verification failed: missing={verification.missing}, "
                f"mismatched={verification.mismatched}"
            )
        expected_files = {
            "manifest.json",
            *(
                str(record["path"])
                for record in manifest.get("files", [])
                if isinstance(record, Mapping) and "path" in record
            ),
        }
        if destination is None:
            destination = self.root / "archives" / name
        archive_input = Path(destination).expanduser()
        _reject_symlink_path(archive_input, label="archive destination")
        archive = archive_input.resolve(strict=False)
        mode, archive = _archive_compression(archive, compression)
        try:
            archive.relative_to(snapshot.resolve())
        except ValueError:
            pass
        else:
            raise ValueError("archive destination must not be inside the source snapshot")
        archive.parent.mkdir(parents=True, exist_ok=True)
        if archive.exists():
            raise FileExistsError(f"archive already exists: {archive}")
        temporary = archive.with_name(f".{archive.name}.{os.getpid()}.{uuid.uuid4().hex}.part")
        try:
            actual_files, unsafe_files = _tree_files_no_follow(snapshot, reject_hardlinks=True)
            if unsafe_files:
                raise ValueError(
                    f"snapshot contains unsupported link or special file(s): "
                    f"{', '.join(sorted(unsafe_files)[:8])}"
                )
            extras = sorted(actual_files - expected_files)
            missing = sorted(expected_files - actual_files)
            if extras or missing:
                raise ValueError(
                    f"snapshot file set changed during export: extras={extras}, missing={missing}"
                )
            exported: set[str] = set()
            with _archive_writer(temporary, mode) as stream:
                with tarfile.open(fileobj=stream, mode="w|") as tar:
                    for raw_relative in sorted(actual_files):
                        relative = Path(raw_relative)
                        if raw_relative not in expected_files:
                            raise ValueError(f"snapshot contains unlisted file: {raw_relative}")
                        source = snapshot / relative
                        _reject_link_or_non_regular(source, hardlink=True)
                        exported.add(raw_relative)
                        info = tar.gettarinfo(str(source), arcname=relative.as_posix())
                        # Normalize metadata so archive bytes are reproducible
                        # across machines while the manifest remains the data
                        # integrity authority.
                        info.uid = 0
                        info.gid = 0
                        info.uname = ""
                        info.gname = ""
                        info.mtime = 0
                        if info.isreg():
                            with source.open("rb") as handle:
                                tar.addfile(info, handle)
                        else:
                            tar.addfile(info)
            if exported != expected_files:
                raise ValueError(
                    f"snapshot export file set mismatch: expected={sorted(expected_files)}, "
                    f"exported={sorted(exported)}"
                )
            os.replace(temporary, archive)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        return archive

    # Verb-first aliases used by CLI/integration callers.
    export = export_archive
    archive_export = export_archive
    export_tarball = export_archive

    def import_archive(
        self,
        archive: Path | str,
        *,
        name: str | None = None,
        overwrite: bool = False,
        max_members: int | None = None,
        max_member_bytes: int | None = None,
        max_total_bytes: int | None = None,
    ) -> Path:
        """Import and verify a streamed archive into the snapshot namespace.

        Every tar member is checked before extraction: absolute/parent paths,
        duplicates, links and device nodes are rejected.  The extracted
        directory is verified against its manifest before an atomic directory
        rename publishes it.  Existing snapshots are preserved unless the
        caller explicitly asks for ``overwrite``.
        """

        source_input = Path(archive).expanduser()
        _reject_symlink_path(source_input, label="archive source")
        source = source_input.resolve(strict=False)
        if not source.is_file():
            raise FileNotFoundError(source)
        explicit_name = name is not None
        snapshot_name = self._validate_name(name or _archive_name(source.name))
        destination = self.path(snapshot_name)
        if destination.exists() and not overwrite:
            raise FileExistsError(f"snapshot already exists: {destination}")
        suffix = source.name.lower()
        if suffix.endswith((".tar.zst", ".tzst")):
            mode = "zstd"
        elif suffix.endswith((".tar.gz", ".tgz", ".gz")):
            mode = "gzip"
        elif suffix.endswith(".tar"):
            mode = "none"
        else:
            raise ValueError("archive must end in .tar.zst, .tzst, .tar.gz, .tgz or .tar")

        member_limit = MAX_ARCHIVE_MEMBERS if max_members is None else int(max_members)
        member_bytes_limit = (
            MAX_ARCHIVE_MEMBER_BYTES if max_member_bytes is None else int(max_member_bytes)
        )
        total_bytes_limit = (
            MAX_ARCHIVE_TOTAL_BYTES if max_total_bytes is None else int(max_total_bytes)
        )
        if member_limit < 1 or member_bytes_limit < 1 or total_bytes_limit < 1:
            raise ValueError("archive extraction limits must be positive")

        self.root.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix=f".{snapshot_name}-import-", dir=self.root))
        extracted: set[str] = set()
        member_count = 0
        total_bytes = 0
        try:
            with _archive_reader(source, mode) as stream:
                with tarfile.open(fileobj=stream, mode="r|") as tar:
                    for member in tar:
                        member_count += 1
                        if member_count > member_limit:
                            raise ValueError(
                                f"archive contains too many members (limit={member_limit})"
                            )
                        relative = _archive_member_relative(member.name)
                        key = relative.as_posix()
                        if key in extracted:
                            raise ValueError(f"duplicate archive member: {key}")
                        extracted.add(key)
                        target = temporary / relative
                        try:
                            target.resolve().relative_to(temporary.resolve())
                        except ValueError as exc:
                            raise ValueError(f"archive member escapes import root: {key}") from exc
                        if member.isdir():
                            if member.size not in (0,):
                                raise ValueError(f"directory archive member has a payload: {key}")
                            _reject_symlink_path(target, label="archive extraction path")
                            target.mkdir(parents=True, exist_ok=True)
                            continue
                        if not member.isreg():
                            raise ValueError(f"unsupported archive member type: {key}")
                        declared_size = int(member.size)
                        if declared_size < 0 or declared_size > member_bytes_limit:
                            raise ValueError(
                                f"archive member exceeds size limit: {key} "
                                f"({declared_size} > {member_bytes_limit})"
                            )
                        if total_bytes + declared_size > total_bytes_limit:
                            raise ValueError(
                                "archive uncompressed size exceeds total limit: "
                                f"{total_bytes + declared_size} > {total_bytes_limit}"
                            )
                        _reject_symlink_path(target, label="archive extraction path")
                        target.parent.mkdir(parents=True, exist_ok=True)
                        handle = tar.extractfile(member)
                        if handle is None:
                            raise ValueError(f"archive member has no payload: {key}")
                        copied = 0
                        with handle, target.open("xb") as output:
                            while True:
                                chunk = handle.read(
                                    min(1024 * 1024, member_bytes_limit - copied + 1)
                                )
                                if not chunk:
                                    break
                                copied += len(chunk)
                                if copied > declared_size or copied > member_bytes_limit:
                                    raise ValueError(f"archive member payload exceeds limit: {key}")
                                if total_bytes + copied > total_bytes_limit:
                                    raise ValueError(
                                        "archive uncompressed size exceeds total limit: "
                                        f"{total_bytes + copied} > {total_bytes_limit}"
                                    )
                                output.write(chunk)
                        if copied != declared_size:
                            raise ValueError(
                                f"archive member size mismatch: {key} ({copied} != {declared_size})"
                            )
                        total_bytes += copied
            manifest_path = temporary / "manifest.json"
            if not manifest_path.is_file():
                raise ValueError("archive does not contain manifest.json")
            manifest = _read_json_object(manifest_path)
            if (
                manifest.get("format") != "cnequity.lake-snapshot"
                or manifest.get("format_version") != 1
            ):
                raise ValueError(f"unsupported snapshot manifest: {manifest_path}")
            if not explicit_name and manifest.get("name") not in {None, snapshot_name}:
                raise ValueError(
                    f"archive snapshot name mismatch: {manifest.get('name')!r} != {snapshot_name!r}"
                )
            verification = self._verify_directory(snapshot_name, temporary, manifest)
            if not verification.passed:
                raise ValueError(
                    f"archive verification failed: missing={verification.missing}, "
                    f"mismatched={verification.mismatched}"
                )
            expected = {"manifest.json"}
            expected.update(
                str(record["path"])
                for record in manifest.get("files", [])
                if isinstance(record, Mapping) and "path" in record
            )
            extras = sorted(extracted - expected)
            if extras:
                raise ValueError(f"archive contains unlisted file(s): {', '.join(extras[:8])}")

            backup: Path | None = None
            if destination.exists():
                if not overwrite:
                    raise FileExistsError(f"snapshot already exists: {destination}")
                backup = self.root / f".{snapshot_name}.replaced-{uuid.uuid4().hex}"
                os.replace(destination, backup)
            try:
                os.replace(temporary, destination)
            except BaseException:
                if backup is not None and not destination.exists():
                    os.replace(backup, destination)
                raise
            if backup is not None:
                shutil.rmtree(backup, ignore_errors=True)
            return destination
        except BaseException:
            shutil.rmtree(temporary, ignore_errors=True)
            raise

    import_ = import_archive
    archive_import = import_archive
    import_tarball = import_archive

    def restore(self, name: str, target_data_root: Path) -> Path:
        """Restore into a new or empty explicit target; existing data is never overwritten."""
        verification = self.verify(name)
        if not verification.passed:
            raise ValueError(
                f"snapshot verification failed: missing={verification.missing}, "
                f"mismatched={verification.mismatched}"
            )
        target_input = Path(target_data_root).expanduser()
        # Preserve the lexical target until every existing ancestor has been
        # lstat'ed.  Resolving first would turn both an external and a dangling
        # symlink into an apparently safe path and could redirect the restore.
        _reject_symlink_path(target_input, label="restore target")
        _reject_symlink_path(self.config.data_root, label="data root")
        target = target_input.resolve(strict=False)
        if target == self.config.data_root.resolve(strict=False):
            raise ValueError("restore target must not be the active data root")
        snapshot, manifest = self._manifest(name)
        # Restoring below the source package would mutate the very bytes being
        # verified (and below ``self.root`` would let an imported package or
        # archive become its own restore target).  Compare canonical paths only
        # after checking their lexical ancestors for user-controlled links.
        _reject_symlink_path(self.root, label="snapshot root")
        source_root = snapshot.resolve(strict=False)
        package_root = self.root.resolve(strict=False)
        for forbidden in (source_root, package_root):
            try:
                target.relative_to(forbidden)
            except ValueError:
                continue
            raise ValueError("restore target must not be inside the source snapshot/package")
        if target.exists() and any(target.iterdir()):
            raise FileExistsError(f"restore target is not empty: {target}")
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = Path(tempfile.mkdtemp(prefix=f".{target.name}-restore-", dir=target.parent))
        try:
            for record in manifest["files"]:
                relative = _safe_relative(str(record["path"]))
                if relative.parts[0] == "data":
                    # Stored data paths include a leading data/ while the
                    # restore root itself is the lake's data directory.
                    restored_relative = Path(*relative.parts[1:])
                elif relative.parts[0] == "meta":
                    restored_relative = relative
                else:
                    raise ValueError(f"unsupported snapshot file root: {relative}")
                destination = temp / restored_relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(snapshot / relative, destination)
            meta = temp / "meta"
            meta.mkdir(parents=True, exist_ok=True)
            for dataset, state in manifest.get("dataset_states", {}).items():
                if state:
                    write_json_atomic(
                        meta / "state" / f"{dataset}.json",
                        state,
                        indent=2,
                        ensure_ascii=False,
                    )
            self._rebuild_runtime_state(temp, manifest)
            write_json_atomic(
                meta / "restored-snapshot.json", manifest, indent=2, ensure_ascii=False
            )
            _reject_symlink_path(target, label="restore target")
            if target.exists():
                target.rmdir()
            os.replace(temp, target)
        except Exception:
            shutil.rmtree(temp, ignore_errors=True)
            raise
        return target

    @staticmethod
    def _rebuild_runtime_state(target: Path, manifest: Mapping[str, Any]) -> int:
        """Recreate runtime caches omitted by old snapshots.

        The adjustment cache is a source-response optimisation, not a second
        authority.  A snapshot made before cache files were included can still
        be restored without network access by converting the persisted aligned
        hfq table back into the cache shape (``trade_date``, ``factor``).
        Returning the number of files written keeps this helper useful to
        diagnostics while preserving the old ``restore() -> Path`` API.
        """

        runtime = manifest.get("runtime_state", {})
        cache_state = runtime.get("adj_factors_cache") if isinstance(runtime, Mapping) else None
        if not isinstance(cache_state, Mapping):
            # v1 snapshots predating ``runtime_state`` may still contain the
            # aligned derived table.  Treat that shape as rebuildable rather
            # than forcing the first restored derive to hit Sina again.
            if (target / "derived" / "adj_factors").is_dir():
                cache_state = {"mode": "rebuildable"}
            else:
                return 0
        if cache_state.get("mode") == "included":
            return 0

        derived = target / "derived" / "adj_factors"
        files = sorted(derived.rglob("*.parquet")) if derived.is_dir() else []
        if not files:
            return 0

        # Import lazily.  snapshots.py is part of storage's low-level import
        # surface and importing Polars/derive at module load would introduce a
        # storage -> query -> derive cycle for users that only verify hashes.
        try:
            import polars as pl
        except ImportError:  # pragma: no cover - Polars is a hard dependency
            return 0

        frames = []
        for path in files:
            try:
                frame = pl.read_parquet(path)
            except Exception:
                continue
            required = {"symbol", "trade_date", "factor"}
            if not required.issubset(frame.columns):
                continue
            if "adjust_type" in frame.columns:
                frame = frame.filter(pl.col("adjust_type") == "hfq")
            if not frame.is_empty():
                frames.append(frame.select(["symbol", "trade_date", "factor"]))
        if not frames:
            return 0

        all_factors = pl.concat(frames, how="diagonal_relaxed").unique(
            subset=["symbol", "trade_date"], keep="last"
        )
        written = 0
        for symbol, frame in all_factors.partition_by("symbol", as_dict=True).items():
            key = symbol[0] if isinstance(symbol, tuple) else symbol
            safe = str(key).replace(".", "_")
            destination = target / "meta" / "adj_factors_cache" / f"{safe}_hfq.parquet"
            write_parquet_atomic(destination, frame.sort("trade_date"), compression="zstd")
            written += 1
        return written

    # ------------------------------------------------------------------
    # Incremental packages
    # ------------------------------------------------------------------

    def delta_path(self, name: str) -> Path:
        """Return the canonical storage path for a delta package.

        Full snapshots and deltas have separate namespaces so a release can
        use the same human name for a baseline and its next update.  The
        fallback candidates in :meth:`_delta_manifest` keep packages made by
        early development builds (``root/name`` or ``root/delta-name``)
        readable.
        """

        _reject_symlink_path(self.root, label="snapshot root")
        path = self.root / "deltas" / self._validate_name(name)
        _reject_symlink_path(path, label="delta path")
        return path

    def _delta_candidates(self, name: str) -> tuple[Path, ...]:
        safe = self._validate_name(name)
        _reject_symlink_path(self.root, label="snapshot root")
        return (
            self.root / "deltas" / safe,
            self.root / f"delta-{safe}",
            self.root / safe,
        )

    def _delta_manifest(self, name: str) -> tuple[Path, dict[str, Any]]:
        for package in self._delta_candidates(name):
            _reject_symlink_path(package, label="delta package")
            manifest = package / "manifest.json"
            try:
                _reject_link_or_non_regular(manifest, hardlink=True)
            except FileNotFoundError:
                continue
            payload = _read_json_object(manifest)
            if payload.get("format") != "cnequity.lake-delta":
                continue
            if payload.get("format_version") != 1:
                raise ValueError(f"unsupported delta manifest: {manifest}")
            return package, payload
        raise FileNotFoundError(
            f"no delta package named {name!r} ({self.delta_path(name) / 'manifest.json'})"
        )

    @staticmethod
    def _dataset_layer(dataset: str) -> tuple[str, Path]:
        spec = DATASETS[dataset]
        layer = str(spec.layer)
        return layer, Path(layer) / dataset

    @classmethod
    def _lake_index(
        cls,
        lake_root: Path,
        datasets: list[str],
        *,
        include_runtime: bool = True,
    ) -> dict[str, dict[str, Any]]:
        """Index data and identity metadata for *datasets*.

        A delta must compare more than Parquet bytes: state watermarks and the
        referenced revision receipt are part of the published identity.  This
        index deliberately excludes locks, SQLite's operational attempt log,
        and unrelated dataset state so an update remains portable and
        composable with a newly restored lake.
        """

        _reject_symlink_path(lake_root, label="lake root")
        root = Path(lake_root).resolve(strict=False)
        _reject_symlink_path(root / "meta", label="metadata root")
        index: dict[str, dict[str, Any]] = {}

        def add(path: Path, relative: Path, dataset: str, layer: str) -> None:
            _reject_symlink_ancestors(root, relative)
            if _lstat(path) is None:
                return
            _reject_link_or_non_regular(path)
            canonical = _safe_lake_relative(relative.as_posix()).as_posix()
            index[canonical] = _file_digest_record(
                path,
                Path(canonical),
                dataset=dataset,
                layer=layer,
            )

        for dataset in sorted(set(datasets)):
            if dataset not in DATASETS:
                raise ValueError(f"unknown dataset(s): {dataset}")
            spec = DATASETS[dataset]
            layer = str(spec.layer)
            data_dir = root / layer / dataset
            data_info = _lstat(data_dir)
            if data_info is not None and stat.S_ISLNK(data_info.st_mode):
                raise ValueError(f"lake dataset root is a symlink: {data_dir}")
            if data_info is not None and stat.S_ISDIR(data_info.st_mode):
                data_entries, unsafe = _tree_files_no_follow(data_dir)
                if unsafe:
                    raise ValueError(
                        f"lake dataset contains unsupported link or special file(s): "
                        f"{', '.join(sorted(unsafe)[:8])}"
                    )
                for raw_relative in sorted(data_entries):
                    relative = Path(raw_relative)
                    if relative.suffix != ".parquet":
                        continue
                    path = data_dir / relative
                    add(path, Path(layer) / dataset / relative, dataset, layer)

            state_path = root / "meta" / "state" / f"{dataset}.json"
            add(state_path, Path("meta") / "state" / f"{dataset}.json", dataset, "meta")

            # Include the current receipt even though it is not a dataset
            # payload.  Without it a restored state would point to a dangling
            # revision_receipt and cache invalidation would be unverifiable.
            if state_path.is_file():
                try:
                    state = _read_json_object(state_path)
                    receipt = state.get("revision_receipt")
                    if receipt:
                        receipt_relative = _safe_relative(str(receipt))
                        if receipt_relative.parts[0] != "revisions":
                            raise ValueError(
                                f"dataset state references unsupported receipt: {receipt}"
                            )
                        add(
                            root / "meta" / receipt_relative,
                            Path("meta") / receipt_relative,
                            dataset,
                            "meta",
                        )
                    pointer_path = root / "meta" / "revisions" / dataset / "current.json"
                    _reject_symlink_ancestors(
                        root,
                        Path("meta") / "revisions" / dataset / "current.json",
                    )
                    if _lstat(pointer_path) is not None:
                        _reject_link_or_non_regular(pointer_path)
                        pointer_relative = Path("meta") / "revisions" / dataset / "current.json"
                        add(pointer_path, pointer_relative, dataset, "meta")
                        pointer_payload = _read_json_object(pointer_path)
                        generation_relative = _safe_relative(
                            str(pointer_payload.get("generation_path", ""))
                        )
                        generation_logical = root / "meta" / generation_relative
                        _reject_symlink_path(generation_logical, label="revision generation")
                        generation = generation_logical.resolve(strict=False)
                        try:
                            generation.relative_to((root / "meta").resolve())
                        except ValueError as exc:
                            raise ValueError(
                                f"revision generation escapes meta root: {pointer_path}"
                            ) from exc
                        if not generation.is_dir():
                            raise FileNotFoundError(
                                f"current revision generation is missing: {generation}"
                            )
                        generation_entries, unsafe = _tree_files_no_follow(generation)
                        if unsafe:
                            raise ValueError(
                                f"revision generation contains unsupported link or special file(s): "
                                f"{', '.join(sorted(unsafe)[:8])}"
                            )
                        for raw_relative in sorted(generation_entries):
                            relative = Path(raw_relative)
                            if relative.suffix != ".parquet":
                                continue
                            generation_file = generation / relative
                            meta_relative = Path("meta") / generation_file.relative_to(
                                root / "meta"
                            )
                            add(generation_file, meta_relative, dataset, "meta")

                    # COW generations are immutable research history.  The
                    # current pointer above is enough for a normal live read,
                    # but a byte-level delta must not interpret an older
                    # retained receipt/generation as a deletion merely
                    # because it is not selected by the target's current
                    # pointer.  Include every retained vintage in the identity
                    # index so applying a newer delta preserves fixed-revision
                    # queries and produces the same post-apply fingerprint.
                    receipt_dir = root / "meta" / "revisions" / dataset
                    receipt_dir_info = _lstat(receipt_dir)
                    if receipt_dir_info is not None and stat.S_ISLNK(receipt_dir_info.st_mode):
                        raise ValueError(f"revision receipt directory is a symlink: {receipt_dir}")
                    if receipt_dir_info is not None and stat.S_ISDIR(receipt_dir_info.st_mode):
                        for receipt_path in sorted(receipt_dir.glob("*.json")):
                            if receipt_path.name == "current.json":
                                continue
                            receipt_relative = receipt_path.relative_to(root / "meta")
                            add(
                                receipt_path,
                                Path("meta") / receipt_relative,
                                dataset,
                                "meta",
                            )
                            try:
                                historical = _read_json_object(receipt_path)
                                historical_relative = _safe_relative(
                                    str(historical.get("generation_path", ""))
                                )
                                historical_logical = root / "meta" / historical_relative
                                _reject_symlink_path(
                                    historical_logical, label="retained revision generation"
                                )
                                historical_generation = historical_logical.resolve(strict=False)
                                historical_generation.relative_to((root / "meta").resolve())
                            except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
                                raise ValueError(
                                    f"invalid retained revision receipt: {receipt_path}"
                                ) from exc
                            if not historical_generation.is_dir():
                                raise FileNotFoundError(
                                    f"retained revision generation is missing: "
                                    f"{historical_generation}"
                                )
                            historical_entries, unsafe = _tree_files_no_follow(
                                historical_generation
                            )
                            if unsafe:
                                raise ValueError(
                                    "retained revision generation contains unsupported link "
                                    "or special file(s): "
                                    f"{', '.join(sorted(unsafe)[:8])}"
                                )
                            for raw_relative in sorted(historical_entries):
                                relative = Path(raw_relative)
                                if relative.suffix != ".parquet":
                                    continue
                                historical_file = historical_generation / relative
                                meta_relative = Path("meta") / historical_file.relative_to(
                                    root / "meta"
                                )
                                add(historical_file, meta_relative, dataset, "meta")
                except (json.JSONDecodeError, OSError) as exc:
                    raise ValueError(f"invalid dataset state: {state_path}") from exc

            # A pointer is authoritative even when the denormalised state
            # cache was removed.  Keep indexing the generation and retained
            # receipts in that pointer-only layout so a delta neither misses
            # the immutable bytes nor treats them as deletions.
            if _lstat(state_path) is None:
                pointer_path = root / "meta" / "revisions" / dataset / "current.json"
                _reject_symlink_ancestors(
                    root,
                    Path("meta") / "revisions" / dataset / "current.json",
                )
                pointer_info = _lstat(pointer_path)
                if pointer_info is not None:
                    _reject_link_or_non_regular(pointer_path)
                    pointer_relative = Path("meta") / "revisions" / dataset / "current.json"
                    add(pointer_path, pointer_relative, dataset, "meta")
                    pointer_payload = _read_json_object(pointer_path)
                    generation_relative = _safe_relative(
                        str(pointer_payload.get("generation_path", ""))
                    )
                    generation_logical = root / "meta" / generation_relative
                    _reject_symlink_path(generation_logical, label="revision generation")
                    generation = generation_logical.resolve(strict=False)
                    generation.relative_to((root / "meta").resolve())
                    if not generation.is_dir():
                        raise FileNotFoundError(
                            f"current revision generation is missing: {generation}"
                        )
                    generation_entries, unsafe = _tree_files_no_follow(generation)
                    if unsafe:
                        raise ValueError(
                            "pointer generation contains unsupported link or special file(s): "
                            f"{', '.join(sorted(unsafe)[:8])}"
                        )
                    for raw_relative in sorted(generation_entries):
                        relative = Path(raw_relative)
                        if relative.suffix != ".parquet":
                            continue
                        generation_file = generation / relative
                        meta_relative = Path("meta") / generation_file.relative_to(root / "meta")
                        add(generation_file, meta_relative, dataset, "meta")

            if include_runtime and dataset == "adj_factors":
                cache_dir = root / "meta" / "adj_factors_cache"
                cache_info = _lstat(cache_dir)
                if cache_info is not None and stat.S_ISLNK(cache_info.st_mode):
                    raise ValueError(f"adjustment cache root is a symlink: {cache_dir}")
                if cache_info is not None and stat.S_ISDIR(cache_info.st_mode):
                    cache_entries, unsafe = _tree_files_no_follow(cache_dir)
                    if unsafe:
                        raise ValueError(
                            f"adjustment cache contains unsupported link or special file(s): "
                            f"{', '.join(sorted(unsafe)[:8])}"
                        )
                    for raw_relative in sorted(cache_entries):
                        relative = Path(raw_relative)
                        if relative.suffix != ".parquet" or len(relative.parts) != 1:
                            continue
                        path = cache_dir / relative
                        add(
                            path,
                            Path("meta") / "adj_factors_cache" / relative,
                            dataset,
                            "meta",
                        )

            raw_dir = root / "meta" / "raw" / dataset
            raw_info = _lstat(raw_dir)
            if raw_info is not None and stat.S_ISLNK(raw_info.st_mode):
                raise ValueError(f"raw archive root is a symlink: {raw_dir}")
            if raw_info is not None and stat.S_ISDIR(raw_info.st_mode):
                raw_entries, unsafe = _tree_files_no_follow(raw_dir)
                if unsafe:
                    raise ValueError(
                        f"raw archive contains unsupported link or special file(s): "
                        f"{', '.join(sorted(unsafe)[:8])}"
                    )
                for raw_relative in sorted(raw_entries):
                    relative = Path(raw_relative)
                    path = raw_dir / relative
                    add(
                        path,
                        Path("meta") / "raw" / dataset / relative,
                        dataset,
                        "meta",
                    )
        return index

    @staticmethod
    def _detect_datasets(*roots: Path) -> list[str]:
        found: set[str] = set()
        for root in roots:
            root = Path(root)
            for dataset, spec in DATASETS.items():
                data_dir = root / str(spec.layer) / dataset
                state = root / "meta" / "state" / f"{dataset}.json"
                if (data_dir.is_dir() and any(data_dir.rglob("*.parquet"))) or state.is_file():
                    found.add(dataset)
        return sorted(found)

    @staticmethod
    def _state_payloads(root: Path, datasets: list[str]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for dataset in datasets:
            path = Path(root) / "meta" / "state" / f"{dataset}.json"
            if path.is_file():
                out[dataset] = _read_json_object(path)
        return out

    @staticmethod
    def _contracts(datasets: list[str]) -> dict[str, dict[str, Any]]:
        return {
            dataset: {
                "schema_version": dataset_contract(dataset)["schema_version"],
                "fingerprint": contract_fingerprint(dataset),
            }
            for dataset in datasets
        }

    def _copy_delta_file(self, source: Path, temporary: Path, relative: str) -> str:
        """Copy one target file into a package and return its package path."""

        lake_relative = _safe_lake_relative(relative)
        package_relative = Path("data") / lake_relative
        destination = temporary / package_relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        _reject_link_or_non_regular(source, hardlink=True)
        shutil.copy2(source, destination)
        return package_relative.as_posix()

    def _materialize_snapshot_source(self, source: Path) -> Path | None:
        """Expose a full snapshot package as a temporary lake root.

        ``create_delta`` primarily compares live roots, but accepting a
        snapshot directory (or a snapshot name below ``snapshot_root``) makes
        the common "baseline snapshot → current lake" workflow one command.
        The temporary view is removed by the caller after package creation.
        """

        source = Path(source)
        _reject_symlink_path(source, label="snapshot source root")
        manifest_path = source / "manifest.json"
        try:
            _reject_link_or_non_regular(manifest_path, hardlink=True)
        except FileNotFoundError:
            return None
        manifest = _read_json_object(manifest_path)
        if manifest.get("format") != "cnequity.lake-snapshot":
            return None
        if manifest.get("format_version") != 1:
            raise ValueError(f"unsupported snapshot manifest: {manifest_path}")
        files = manifest.get("files", [])
        if not isinstance(files, list):
            raise ValueError(f"snapshot files must be a list: {manifest_path}")
        verification = self._verify_directory(source.name, source, manifest)
        if not verification.passed:
            raise ValueError(
                f"snapshot source verification failed: missing={verification.missing}, "
                f"mismatched={verification.mismatched}"
            )
        temporary = Path(tempfile.mkdtemp(prefix=f".{source.name}-delta-source-"))
        try:
            for record in files:
                if not isinstance(record, Mapping):
                    raise ValueError(f"invalid snapshot file record: {record}")
                package_relative = _safe_relative(str(record.get("path", "")))
                package_file = source / package_relative
                info = _reject_link_or_non_regular(package_file, hardlink=True)
                if info.st_size != int(record["size_bytes"]):
                    raise ValueError(f"snapshot file size mismatch: {package_relative}")
                if _sha256(package_file) != str(record["sha256"]):
                    raise ValueError(f"snapshot file digest mismatch: {package_relative}")
                if package_relative.parts[0] == "data":
                    lake_relative = Path(*package_relative.parts[1:])
                elif package_relative.parts[0] == "meta":
                    lake_relative = package_relative
                else:
                    raise ValueError(f"unsupported snapshot file root: {package_relative}")
                lake_relative = _safe_lake_relative(lake_relative.as_posix())
                destination = temporary / lake_relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(package_file, destination)
            for dataset, state in manifest.get("dataset_states", {}).items():
                if state:
                    write_json_atomic(
                        temporary / "meta" / "state" / f"{dataset}.json",
                        state,
                        indent=2,
                        ensure_ascii=False,
                    )
            self._rebuild_runtime_state(temporary, manifest)
            return temporary
        except Exception:
            shutil.rmtree(temporary, ignore_errors=True)
            raise

    def _write_delta_package(
        self,
        name: str,
        *,
        base_root: Path | None,
        target_root: Path,
        datasets: list[str],
        base_index: Mapping[str, Mapping[str, Any]],
        target_index: Mapping[str, Mapping[str, Any]],
        base_states: Mapping[str, Mapping[str, Any]],
        target_states: Mapping[str, Mapping[str, Any]],
        base_revisions: Mapping[str, int] | None = None,
        revision_only: bool = False,
        changed_paths: set[str] | None = None,
    ) -> Path:
        destination = self.delta_path(name)
        if destination.exists():
            raise FileExistsError(f"delta already exists: {destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix=f".{name}-", dir=destination.parent))
        changes: list[dict[str, Any]] = []
        try:
            paths = set(target_index) | set(base_index)
            if changed_paths is not None:
                paths = {path for path in paths if path in changed_paths}
                # Revision receipts can describe a removed path that is no
                # longer in the current index; retain it as a delete.
                paths |= changed_paths - set(target_index)
            for relative in sorted(paths):
                old = base_index.get(relative)
                new = target_index.get(relative)
                if (
                    old
                    and new
                    and old["sha256"] == new["sha256"]
                    and old["size_bytes"] == new["size_bytes"]
                ):
                    continue
                if new is None:
                    change = DeltaChange(
                        operation="delete",
                        path=relative,
                        package_path=None,
                        dataset=str((old or {}).get("dataset", "")),
                        layer=str((old or {}).get("layer", "")),
                        size_bytes=None,
                        sha256=None,
                        old_size_bytes=int(old["size_bytes"]) if old else None,
                        old_sha256=str(old["sha256"]) if old else None,
                        allow_missing=revision_only,
                    )
                else:
                    source = Path(target_root) / _safe_lake_relative(relative)
                    if not source.is_file():
                        raise FileNotFoundError(source)
                    package_path = self._copy_delta_file(source, temporary, relative)
                    change = DeltaChange(
                        operation="replace" if old else "add",
                        path=relative,
                        package_path=package_path,
                        dataset=str(new.get("dataset", "")),
                        layer=str(new.get("layer", "")),
                        size_bytes=int(new["size_bytes"]),
                        sha256=str(new["sha256"]),
                        old_size_bytes=int(old["size_bytes"]) if old else None,
                        old_sha256=str(old["sha256"]) if old else None,
                        # A revision receipt carries changed-file identity but
                        # not the previous bytes.  The revision number is the
                        # precondition, so an absent file is a valid upsert.
                        allow_missing=revision_only and old is None,
                    )
                changes.append(change.to_dict())

            contracts = self._contracts(datasets)
            target_fingerprint = _index_digest(target_index)
            base_fingerprint = _index_digest(base_index) if not revision_only else None
            manifest = {
                "format": "cnequity.lake-delta",
                "format_version": 1,
                "name": name,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "datasets": datasets,
                "contracts": contracts,
                "contract_fingerprint": contract_fingerprint(),
                "contract_fingerprints": {
                    dataset: item["fingerprint"] for dataset, item in contracts.items()
                },
                "lineage": runtime_lineage(self.config),
                "base": {
                    "lake_fingerprint": base_fingerprint,
                    "dataset_states": dict(base_states),
                    "revisions": dict(base_revisions or {}),
                },
                "target": {
                    "lake_fingerprint": target_fingerprint,
                    "dataset_states": dict(target_states),
                },
                # Top-level aliases make the manifest convenient for shell
                # tooling and preserve a clear contract for v1 consumers.
                "base_fingerprint": base_fingerprint,
                "target_fingerprint": target_fingerprint,
                "base_dataset_states": dict(base_states),
                "dataset_states": dict(target_states),
                "precondition": "revision" if revision_only else "lake_fingerprint",
                "changes": changes,
                "files": changes,
                "deletes": [item for item in changes if item["operation"] == "delete"],
            }
            write_json_atomic(temporary / "manifest.json", manifest, indent=2, ensure_ascii=False)
            os.replace(temporary, destination)
        except Exception:
            shutil.rmtree(temporary, ignore_errors=True)
            raise
        return destination / "manifest.json"

    def create_delta(
        self,
        name: str,
        baseline: Path | str | int | None = None,
        target: Path | str | None = None,
        datasets: list[str] | tuple[str, ...] | None = None,
        *,
        from_revision: int | Mapping[str, int] | None = None,
    ) -> Path:
        """Create a verifiable delta between two lake roots.

        ``baseline`` and ``target`` are data roots, not ``curated`` roots.
        ``target`` defaults to the configured active lake.  Passing an integer
        baseline (or ``from_revision=``) selects the revision-baseline mode;
        see :meth:`create_delta_from_revision` for its precondition semantics.
        """

        if isinstance(baseline, int) and from_revision is None:
            from_revision = baseline
            baseline = None
        if from_revision is not None:
            if baseline is not None:
                raise ValueError("baseline path and from_revision are mutually exclusive")
            return self.create_delta_from_revision(
                name,
                from_revision,
                list(datasets or []),
                target_data_root=Path(target) if target is not None else None,
            )
        if baseline is None:
            raise ValueError("baseline lake root is required")
        base_input = Path(baseline).expanduser()
        target_input = Path(target or self.config.data_root).expanduser()
        _reject_symlink_path(base_input, label="baseline lake root")
        _reject_symlink_path(target_input, label="target lake root")
        base_root = base_input.resolve(strict=False)
        target_root = target_input.resolve(strict=False)
        if base_root == target_root:
            raise ValueError("baseline and target lake roots must differ")
        # Permit a snapshot path as the baseline and the shorthand snapshot
        # name when it lives under this store.  A target snapshot is accepted
        # as well, which is useful for producing a delta entirely offline.
        source_temporaries: list[Path] = []
        for candidate_name, candidate in (("baseline", base_root), ("target", target_root)):
            if not candidate.exists() and _SNAPSHOT_NAME.fullmatch(
                str(baseline if candidate_name == "baseline" else target)
            ):
                shorthand = self.path(str(baseline if candidate_name == "baseline" else target))
                if shorthand.is_dir():
                    candidate = shorthand
                    if candidate_name == "baseline":
                        base_root = candidate
                    else:
                        target_root = candidate
            if not candidate.is_dir():
                raise FileNotFoundError(f"{candidate_name} lake root not found: {candidate}")
            materialized = self._materialize_snapshot_source(candidate)
            if materialized is not None:
                source_temporaries.append(materialized)
                if candidate_name == "baseline":
                    base_root = materialized
                else:
                    target_root = materialized
        selected = sorted(set(datasets or self._detect_datasets(base_root, target_root)))
        if not selected:
            raise ValueError("at least one dataset is required")
        unknown = sorted(set(selected) - set(DATASETS))
        if unknown:
            raise ValueError(f"unknown dataset(s): {', '.join(unknown)}")

        try:
            # The target may be the active lake.  Sharing compact's mutation
            # lock makes the index and copied bytes one stable observation,
            # while a detached target has its own lock namespace.
            _reject_symlink_path(target_root / "meta", label="metadata root")
            with lake_mutation_lock(target_root / "meta", blocking=True):
                base_index = self._lake_index(base_root, selected)
                target_index = self._lake_index(target_root, selected)
                return self._write_delta_package(
                    name,
                    base_root=base_root,
                    target_root=target_root,
                    datasets=selected,
                    base_index=base_index,
                    target_index=target_index,
                    base_states=self._state_payloads(base_root, selected),
                    target_states=self._state_payloads(target_root, selected),
                )
        finally:
            for temporary in source_temporaries:
                shutil.rmtree(temporary, ignore_errors=True)

    # Friendly aliases used by integrations that spell the operation in the
    # verb-first style of ``SnapshotStore.create``.
    delta_create = create_delta
    create_delta_package = create_delta
    create_delta_from_lakes = create_delta
    create_delta_from_states = create_delta

    def create_delta_from_snapshots(
        self,
        name: str,
        baseline: Path | str,
        target: Path | str,
        datasets: list[str] | tuple[str, ...] | None = None,
    ) -> Path:
        """Explicit alias for the snapshot-package/live-lake comparison path."""

        return self.create_delta(name, baseline, target, datasets)

    def _revision_receipts(self, root: Path, dataset: str, after: int) -> list[dict[str, Any]]:
        directory = Path(root) / "meta" / "revisions" / dataset
        if not directory.is_dir():
            return []
        receipts: list[dict[str, Any]] = []
        for path in sorted(directory.glob("*.json")):
            try:
                payload = _read_json_object(path)
                revision = int(payload.get("revision", 0))
            except (OSError, TypeError, ValueError, json.JSONDecodeError):
                continue
            if revision > after:
                receipts.append(payload)
        return sorted(receipts, key=lambda item: int(item.get("revision", 0)))

    def create_delta_from_revision(
        self,
        name: str,
        revision: int | Mapping[str, int],
        datasets: list[str] | tuple[str, ...] | None = None,
        *,
        target_data_root: Path | None = None,
    ) -> Path:
        """Create an update from one or more committed revision baselines.

        Revision receipts record changed files, not a second copy of the old
        lake.  Therefore this mode uses the baseline revision number as its
        precondition and emits ``replace`` operations with ``allow_missing``
        for files whose previous add/replace status cannot be inferred.  A
        normal two-root delta should be used when byte-level old hashes are
        required; it emits strict add/replace/delete preconditions.
        """

        target_input = Path(target_data_root or self.config.data_root).expanduser()
        _reject_symlink_path(target_input, label="target lake root")
        target_root = target_input.resolve(strict=False)
        selected = sorted(set(datasets or self._detect_datasets(target_root)))
        if not selected:
            raise ValueError("at least one dataset is required for a revision delta")
        unknown = sorted(set(selected) - set(DATASETS))
        if unknown:
            raise ValueError(f"unknown dataset(s): {', '.join(unknown)}")
        if not target_root.is_dir():
            raise FileNotFoundError(f"target lake root not found: {target_root}")
        _reject_symlink_path(target_root / "meta", label="metadata root")
        if isinstance(revision, Mapping):
            baseline_revisions = {dataset: int(revision[dataset]) for dataset in selected}
        else:
            baseline_revisions = {dataset: int(revision) for dataset in selected}
        if any(value < 0 for value in baseline_revisions.values()):
            raise ValueError("revision must be a non-negative integer")

        with lake_mutation_lock(target_root / "meta", blocking=True):
            target_states = self._state_payloads(target_root, selected)
            changed_paths: set[str] = set()
            for dataset in selected:
                current_state = target_states.get(dataset, {})
                current_raw = current_state.get("revision", 0)
                if isinstance(current_raw, bool) or not isinstance(current_raw, int):
                    raise ValueError(f"state field {dataset}.revision must be an integer")
                if current_raw < baseline_revisions[dataset]:
                    raise ValueError(
                        f"target revision for {dataset} ({current_raw}) is older than baseline "
                        f"{baseline_revisions[dataset]}"
                    )
                for receipt in self._revision_receipts(
                    target_root, dataset, baseline_revisions[dataset]
                ):
                    layer_root = Path(str(DATASETS[dataset].layer))
                    for item in receipt.get("files", []):
                        if not isinstance(item, Mapping) or not item.get("path"):
                            continue
                        relative = _safe_relative(str(item["path"]))
                        changed_paths.add((layer_root / relative).as_posix())
                    # Newer receipts may explicitly describe removals.  This
                    # is optional so old receipts remain fully readable.
                    for item in receipt.get("deleted_files", []):
                        raw = item.get("path") if isinstance(item, Mapping) else item
                        if raw:
                            changed_paths.add((layer_root / _safe_relative(str(raw))).as_posix())

                # State and its receipt are part of the update even when a
                # custom producer did not put metadata paths in its receipt.
                changed_paths.add((Path("meta") / "state" / f"{dataset}.json").as_posix())
                receipt = current_state.get("revision_receipt")
                if receipt:
                    receipt_relative = _safe_relative(str(receipt))
                    if receipt_relative.parts[0] != "revisions":
                        raise ValueError(f"unsupported revision receipt: {receipt}")
                    changed_paths.add((Path("meta") / receipt_relative).as_posix())

                # A revision-baseline lake normally already carries the old
                # receipt.  The target index only contains the latest receipt,
                # so explicitly delete the baseline receipt or the post-apply
                # fingerprint would retain a stale identity file.
                receipt_dir = target_root / "meta" / "revisions" / dataset
                if receipt_dir.is_dir():
                    for receipt_path in sorted(receipt_dir.glob("*.json")):
                        try:
                            receipt_payload = _read_json_object(receipt_path)
                            receipt_revision = int(receipt_payload.get("revision", 0))
                        except (OSError, TypeError, ValueError, json.JSONDecodeError):
                            continue
                        if receipt_revision == baseline_revisions[dataset]:
                            changed_paths.add(
                                (
                                    Path("meta") / "revisions" / dataset / receipt_path.name
                                ).as_posix()
                            )

                if dataset == "adj_factors":
                    # RevisionStore currently tracks curated commits while
                    # factor caches are a separate derive optimisation.  A
                    # revision delta still has to carry the warm state when
                    # the target has it, otherwise the post-apply identity
                    # would differ from the packaged target.
                    changed_paths.update(
                        path
                        for path in self._lake_index(target_root, [dataset])
                        if path.startswith("meta/adj_factors_cache/")
                    )

                # COW lakes publish a pointer and retain the generation under
                # meta/revisions/data.  A revision delta must carry those
                # bytes as well as the changed mutable files, otherwise the
                # post-apply fingerprint would leave the target pointing at a
                # generation that was never transferred.
                changed_paths.update(
                    path
                    for path in self._lake_index(target_root, [dataset])
                    if path.startswith(f"meta/revisions/{dataset}/")
                    or path.startswith(f"meta/revisions/data/{dataset}/")
                )

            base_index: dict[str, dict[str, Any]] = {}
            target_index = self._lake_index(target_root, selected)
            # For revision mode the old bytes are intentionally unknown.  The
            # package writer still sees the target index and emits only receipt
            # paths plus state/cache changes.
            return self._write_delta_package(
                name,
                base_root=None,
                target_root=target_root,
                datasets=selected,
                base_index=base_index,
                target_index=target_index,
                base_states={},
                target_states=target_states,
                base_revisions=baseline_revisions,
                revision_only=True,
                changed_paths=changed_paths,
            )

    delta_from_revision = create_delta_from_revision

    def verify_delta(self, name: str) -> DeltaVerification:
        """Verify every add/replace payload and every change path."""

        package, manifest = self._delta_manifest(name)
        missing: list[str] = []
        mismatched: list[str] = []
        invalid: list[str] = []
        verified = 0
        invalid.extend(_manifest_contract_issues(manifest))
        changes = manifest.get("changes", manifest.get("files", []))
        if not isinstance(changes, list):
            raise ValueError(f"delta changes must be a list: {package / 'manifest.json'}")
        actual_files, unsafe_files = _tree_files_no_follow(package, reject_hardlinks=True)
        if unsafe_files:
            invalid.extend(sorted(unsafe_files))
        expected_package_files = {"manifest.json"}
        seen_paths: set[str] = set()
        seen_package_paths: set[str] = set()
        for item in changes:
            if not isinstance(item, Mapping):
                invalid.append(str(item))
                continue
            operation = str(item.get("operation", ""))
            try:
                relative = _safe_lake_relative(str(item["path"]))
            except (KeyError, TypeError, ValueError) as exc:
                invalid.append(str(item.get("path", item)))
                if isinstance(exc, ValueError) and "unsafe" in str(exc):
                    # Keep the specific path in invalid rather than allowing a
                    # malformed package to become an arbitrary file write.
                    continue
                continue
            path_key = relative.as_posix()
            if path_key in seen_paths:
                invalid.append(path_key)
                continue
            seen_paths.add(path_key)
            if operation not in {"add", "replace", "delete"}:
                invalid.append(relative.as_posix())
                continue
            if operation == "delete":
                if item.get("package_path") not in (None, ""):
                    invalid.append(relative.as_posix())
                continue
            package_raw = item.get("package_path")
            if not isinstance(package_raw, str):
                invalid.append(relative.as_posix())
                continue
            try:
                package_relative = _safe_relative(package_raw)
            except ValueError:
                invalid.append(package_raw)
                continue
            if not package_relative.parts or package_relative.parts[0] != "data":
                invalid.append(package_raw)
                continue
            package_key = package_relative.as_posix()
            if package_key in seen_package_paths:
                invalid.append(package_key)
                continue
            seen_package_paths.add(package_key)
            expected_package_files.add(package_key)
            # A package payload must correspond exactly to its lake path.  A
            # detached payload with a renamed manifest entry is otherwise a
            # subtle but valid-looking data substitution.
            if Path(*package_relative.parts[1:]) != relative:
                invalid.append(relative.as_posix())
                continue
            path = package / package_relative
            try:
                _reject_link_or_non_regular(path, hardlink=True)
            except FileNotFoundError:
                missing.append(package_relative.as_posix())
                continue
            except ValueError:
                invalid.append(package_relative.as_posix())
                continue
            try:
                size = int(item["size_bytes"])
                digest = str(item["sha256"])
            except (KeyError, TypeError, ValueError):
                invalid.append(relative.as_posix())
                continue
            if path.stat().st_size != size or _sha256(path) != digest:
                mismatched.append(package_relative.as_posix())
                continue
            verified += 1
        for path in sorted(actual_files - expected_package_files):
            invalid.append(path)
        for path in sorted(expected_package_files - actual_files):
            if path == "manifest.json":
                invalid.append(path)
            else:
                missing.append(path)
        return DeltaVerification(
            delta=name,
            passed=not missing and not mismatched and not invalid,
            verified_files=verified,
            missing=tuple(missing),
            mismatched=tuple(mismatched),
            invalid=tuple(invalid),
        )

    delta_verify = verify_delta
    verify_delta_package = verify_delta

    @staticmethod
    def _current_contracts_match(manifest: Mapping[str, Any]) -> None:
        contracts = manifest.get("contracts", {})
        if not isinstance(contracts, Mapping) or not contracts:
            raise ValueError("delta manifest has no dataset contracts")
        expected_full = manifest.get("contract_fingerprint")
        if not isinstance(expected_full, str) or not expected_full:
            raise ValueError("delta manifest has no global contract fingerprint")
        if expected_full != contract_fingerprint():
            raise ValueError(
                "delta contract registry mismatch: "
                f"expected {expected_full}, running code has {contract_fingerprint()}"
            )
        datasets = manifest.get("datasets", [])
        if not isinstance(datasets, list) or not datasets:
            raise ValueError("delta manifest has no datasets")
        expected_datasets = {str(item) for item in datasets}
        if set(str(dataset) for dataset in contracts) != expected_datasets:
            raise ValueError("delta manifest dataset contracts do not match datasets")
        for dataset, expected in contracts.items():
            if dataset not in DATASETS:
                raise ValueError(f"delta references unknown dataset: {dataset}")
            if not isinstance(expected, Mapping):
                raise ValueError(f"invalid contract record for {dataset}")
            actual = contract_fingerprint(str(dataset))
            if actual != expected.get("fingerprint"):
                raise ValueError(
                    f"delta contract mismatch for {dataset}: expected {expected.get('fingerprint')}, "
                    f"running code has {actual}"
                )
            if expected.get("schema_version") != dataset_contract(str(dataset)).get(
                "schema_version"
            ):
                raise ValueError(f"delta schema contract mismatch for {dataset}")

    @staticmethod
    def _change_records(manifest: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        changes = manifest.get("changes", manifest.get("files", []))
        if not isinstance(changes, list):
            raise ValueError("delta changes must be a list")
        return [item for item in changes if isinstance(item, Mapping)]

    @staticmethod
    def _pointer_payload_from_delta(
        item: Mapping[str, Any],
        source: Path,
        target: Path,
        operations: list[tuple[Mapping[str, Any], Path, Path | None]],
    ) -> dict[str, Any]:
        """Validate a pointer payload before it can become visible.

        A COW pointer is the one file whose atomic replacement changes the
        reader's generation.  Checking its referenced receipt/generation
        against both the package and the planned target tree means a reader
        can only ever observe a complete old or complete new identity.
        """

        payload = _read_json_object(source)
        relative = _safe_lake_relative(str(item.get("path", "")))
        dataset = relative.parts[2] if len(relative.parts) >= 3 else ""
        revision = payload.get("revision")
        if (
            payload.get("schema_version") != 1
            or payload.get("dataset") != dataset
            or isinstance(revision, bool)
            or not isinstance(revision, int)
            or revision < 0
            or not isinstance(payload.get("revision_id"), str)
            or not payload.get("revision_id")
        ):
            raise ValueError(f"invalid revision pointer payload: {relative}")
        generation_raw = payload.get("generation_path") or payload.get("root")
        generation = _safe_relative(str(generation_raw))
        if generation.parts[:3] != ("revisions", "data", dataset):
            raise ValueError(f"revision pointer generation mismatch: {relative}")
        generation_target = target / "meta" / generation
        generation_prefix = (Path("meta") / generation).as_posix().rstrip("/") + "/"
        generation_in_package = any(
            op_source is not None
            and _safe_lake_relative(str(op_item.get("path", "")))
            .as_posix()
            .startswith(generation_prefix)
            for op_item, _destination, op_source in operations
        )
        if not generation_target.is_dir() and not generation_in_package:
            raise ValueError(f"revision pointer generation is not transferred: {relative}")

        pointer_receipt = payload.get("receipt")
        if revision == 0:
            if pointer_receipt not in (None, "") or payload.get("revision_id") != "legacy":
                raise ValueError(f"invalid legacy revision pointer payload: {relative}")
            return payload
        if not isinstance(pointer_receipt, str) or not pointer_receipt:
            raise ValueError(f"revision pointer has no receipt: {relative}")
        receipt = _safe_relative(pointer_receipt)
        if receipt.parts[:2] != ("revisions", dataset):
            raise ValueError(f"revision pointer receipt mismatch: {relative}")
        receipt_target = target / "meta" / receipt
        receipt_source: Path | None = None
        for op_item, _destination, op_source in operations:
            if op_source is None:
                continue
            op_path = _safe_lake_relative(str(op_item.get("path", "")))
            if op_path == Path("meta") / receipt:
                receipt_source = op_source
                break
        if receipt_source is None and not receipt_target.is_file():
            raise ValueError(f"revision pointer receipt is not transferred: {relative}")
        receipt_payload = (
            _read_json_object(receipt_source)
            if receipt_source is not None
            else _read_json_object(receipt_target)
        )
        if (
            receipt_payload.get("dataset") != dataset
            or receipt_payload.get("revision") != revision
            or receipt_payload.get("revision_id") != payload.get("revision_id")
        ):
            raise ValueError(f"revision pointer and receipt disagree: {relative}")
        if receipt_payload.get("generation_path") not in (None, generation_raw):
            raise ValueError(f"revision pointer and generation disagree: {relative}")
        return payload

    def apply_delta(
        self,
        name: str,
        target_data_root: Path | None = None,
        *,
        dry_run: bool = False,
    ) -> Path:
        """Verify and apply a delta to a non-empty lake root.

        Add/replace/delete operations are checked against the baseline lake
        fingerprint (or per-dataset revision for revision deltas).  Bytes are
        copied to same-directory temporary files and every overwritten file is
        backed up until the full change set and post-apply target fingerprint
        pass.  An exception rolls all individual changes back, so callers never
        observe a knowingly partial operation.
        """

        package, manifest = self._delta_manifest(name)
        verification = self.verify_delta(name)
        if not verification.passed:
            raise ValueError(
                f"delta verification failed: missing={verification.missing}, "
                f"mismatched={verification.mismatched}, invalid={verification.invalid}"
            )
        self._current_contracts_match(manifest)
        target_input = Path(target_data_root or self.config.data_root).expanduser()
        _reject_symlink_path(target_input, label="target lake root")
        target = target_input.resolve(strict=False)
        if not target.is_dir():
            raise FileNotFoundError(f"target lake root not found: {target}")
        if not any(target.iterdir()):
            raise FileExistsError(f"delta target is empty: {target}")
        datasets = manifest.get("datasets", [])
        if not isinstance(datasets, list) or not datasets:
            raise ValueError("delta manifest has no datasets")
        selected = [str(item) for item in datasets]
        # Validate the lock parent before acquiring it.  Otherwise a malicious
        # ``meta`` symlink could redirect even the lock file outside the lake.
        _safe_target_path(target, Path("meta") / "locks" / "compact.lock")

        with lake_mutation_lock(target / "meta", blocking=True):
            current_index = self._lake_index(target, selected)
            expected_target = manifest.get("target_fingerprint")
            if expected_target and _index_digest(current_index) == expected_target:
                # Applying the same package twice is a harmless no-op.  This
                # matters when a transfer job retries after the first process
                # committed but before it returned its receipt.
                return target

            precondition = str(manifest.get("precondition", "lake_fingerprint"))
            if precondition == "lake_fingerprint":
                expected_base = manifest.get("base_fingerprint")
                actual_base = _index_digest(current_index)
                if not expected_base or actual_base != expected_base:
                    raise ValueError(
                        f"delta base mismatch: expected {expected_base}, got {actual_base}"
                    )
            elif precondition == "revision":
                base = manifest.get("base", {})
                expected_revisions = base.get("revisions", {}) if isinstance(base, Mapping) else {}
                states = self._state_payloads(target, selected)
                for dataset in selected:
                    expected = expected_revisions.get(dataset)
                    if expected is None:
                        continue
                    actual = states.get(dataset, {}).get("revision", 0)
                    if actual != expected:
                        raise ValueError(
                            f"delta revision mismatch for {dataset}: expected {expected}, got {actual}"
                        )
            else:
                raise ValueError(f"unsupported delta precondition: {precondition}")

            changes = self._change_records(manifest)
            operations: list[tuple[Mapping[str, Any], Path, Path | None]] = []
            for item in changes:
                relative = _safe_lake_relative(str(item.get("path", "")))
                operation = str(item.get("operation", ""))
                if operation not in {"add", "replace", "delete"}:
                    raise ValueError(f"unsupported delta operation: {operation}")
                # Check every existing ancestor with lstat.  In particular,
                # ``exists()`` is false for a dangling symlink, but mkdir on
                # its child would still follow that link outside the lake.
                destination = _safe_target_path(target, relative)
                destination_info = _lstat(destination)
                if destination_info is not None and stat.S_ISLNK(destination_info.st_mode):
                    raise ValueError(f"delta target path is a symlink: {relative}")
                if destination_info is not None and not stat.S_ISREG(destination_info.st_mode):
                    raise ValueError(f"delta target path is not a regular file: {relative}")
                if operation == "delete":
                    if destination_info is not None:
                        expected_old = item.get("old_sha256")
                        if expected_old and _sha256(destination) != expected_old:
                            raise ValueError(f"delta old digest mismatch: {relative}")
                    elif not bool(item.get("allow_missing", False)):
                        raise ValueError(f"delta delete target is missing: {relative}")
                    operations.append((item, destination, None))
                    continue

                package_raw = item.get("package_path")
                if not isinstance(package_raw, str):
                    raise ValueError(f"delta payload path missing: {relative}")
                package_relative = _safe_relative(package_raw)
                source = package / package_relative
                _reject_link_or_non_regular(source, hardlink=True)
                if (
                    operation == "add"
                    and destination_info is not None
                    and not item.get("allow_missing")
                ):
                    raise ValueError(f"delta add target already exists: {relative}")
                expected_old = item.get("old_sha256")
                if (
                    destination_info is not None
                    and expected_old
                    and _sha256(destination) != expected_old
                ):
                    raise ValueError(f"delta old digest mismatch: {relative}")
                if (
                    destination_info is None
                    and operation == "replace"
                    and not item.get("allow_missing", False)
                ):
                    raise ValueError(f"delta replace target is missing: {relative}")
                operations.append((item, destination, source))

            # Materialise every generation, receipt, state and data payload
            # before replacing any COW pointer.  ``current.json`` is the
            # commit marker: making it the final visible operation guarantees
            # that lock-free readers select either the old complete generation
            # or the new complete generation, never a dangling intermediate.
            pointer_operations = [
                operation
                for operation in operations
                if _is_current_pointer(operation[1].relative_to(target))
            ]
            other_operations = [
                operation
                for operation in operations
                if not _is_current_pointer(operation[1].relative_to(target))
            ]
            if any(str(item.get("operation")) == "delete" for item, _, _ in pointer_operations):
                raise ValueError("delta cannot delete a current revision pointer")

            # A two-root delta may retire the generation or receipt selected by
            # the old pointer.  Those deletes/replacements are safe only after
            # the pointer has switched: removing an old generation first would
            # make a concurrent lock-free reader resolve a dangling pointer.
            # Keep the operations in a separate final phase.  The pointer's
            # ``os.replace`` remains the last visible replacement operation,
            # while retired bytes are cleaned up immediately afterwards.
            old_pointer_payloads: dict[Path, dict[str, Any] | None] = {}
            deferred_operations: list[tuple[Mapping[str, Any], Path, Path | None]] = []
            active_old_paths: dict[Path, set[Path]] = {}

            def path_below(path: Path, prefix: Path) -> bool:
                return path == prefix or prefix in path.parents

            for _item, destination, _source in pointer_operations:
                old_payload: dict[str, Any] | None = None
                info = _lstat(destination)
                if info is not None:
                    old_payload = _read_json_object(destination)
                    old_generation_raw = old_payload.get("generation_path") or old_payload.get(
                        "root"
                    )
                    old_generation = _safe_relative(str(old_generation_raw))
                    old_paths = {Path("meta") / old_generation}
                    old_receipt = old_payload.get("receipt")
                    if old_receipt:
                        old_paths.add(Path("meta") / _safe_relative(str(old_receipt)))
                    active_old_paths[destination] = old_paths
                old_pointer_payloads[destination] = old_payload

            for operation in other_operations:
                _item, destination, _source = operation
                relative = destination.relative_to(target)
                if any(
                    path_below(relative, prefix)
                    for paths in active_old_paths.values()
                    for prefix in paths
                ):
                    deferred_operations.append(operation)

            if deferred_operations:
                deferred_ids = {id(operation) for operation in deferred_operations}
                other_operations = [
                    operation for operation in other_operations if id(operation) not in deferred_ids
                ]
            operations = other_operations + pointer_operations + deferred_operations
            pointer_payloads: dict[Path, dict[str, Any]] = {}
            for item, destination, source in pointer_operations:
                assert source is not None
                pointer_payloads[destination] = self._pointer_payload_from_delta(
                    item, source, target, operations
                )

            if dry_run:
                return target

            transaction = uuid.uuid4().hex
            backup_root = Path(tempfile.mkdtemp(prefix=f".delta-{transaction}-", dir=target.parent))
            backups: list[tuple[Path, Path]] = []
            created: list[Path] = []
            published_pointers: set[Path] = set()
            application_receipt: Path | None = None
            receipt_relative = Path("meta") / "applied-deltas" / f"{name}-{transaction}.json"
            application_receipt = target / receipt_relative
            application_receipt_payload = {
                "format": "cnequity.lake-delta-application",
                "format_version": 1,
                "delta": name,
                "applied_at": datetime.now(timezone.utc).isoformat(),
                "target_fingerprint": manifest.get("target_fingerprint"),
                "changes": len(operations),
            }
            receipt_written = False

            def restore_atomically(destination: Path, backup: Path) -> None:
                """Restore one file without exposing a truncated JSON/data file."""

                relative = destination.relative_to(target)
                _ensure_target_parent(target, relative)
                fd, temporary_name = tempfile.mkstemp(
                    dir=destination.parent,
                    prefix=f".{destination.name}.{transaction}-rollback-",
                    suffix=".tmp",
                )
                os.close(fd)
                temporary = Path(temporary_name)
                try:
                    shutil.copy2(backup, temporary)
                    os.replace(temporary, destination)
                finally:
                    temporary.unlink(missing_ok=True)

            try:
                for item, destination, source in operations:
                    # The application receipt is operational metadata, but it
                    # still belongs before publication.  Writing it before
                    # the first pointer switch keeps ``current.json`` the
                    # commit marker and makes a failed switch easy to roll
                    # back without leaving a false "applied" record behind.
                    if (
                        not receipt_written
                        and pointer_operations
                        and destination in pointer_payloads
                    ):
                        _ensure_target_parent(target, receipt_relative)
                        write_json_atomic(
                            application_receipt,
                            application_receipt_payload,
                            indent=2,
                            ensure_ascii=False,
                        )
                        receipt_written = True
                    destination_info = _lstat(destination)
                    if destination_info is not None:
                        if stat.S_ISLNK(destination_info.st_mode) or not stat.S_ISREG(
                            destination_info.st_mode
                        ):
                            raise ValueError(
                                f"delta target path changed to unsafe entry: {destination}"
                            )
                        backup = backup_root / destination.relative_to(target)
                        backup.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(destination, backup)
                        backups.append((destination, backup))
                    if str(item.get("operation")) == "delete":
                        if destination_info is not None:
                            destination.unlink()
                        continue
                    assert source is not None
                    _ensure_target_parent(target, destination.relative_to(target))
                    fd, tmp_name = tempfile.mkstemp(
                        dir=destination.parent,
                        prefix=f".{destination.name}.{transaction}-",
                        suffix=".tmp",
                    )
                    os.close(fd)
                    temporary = Path(tmp_name)
                    try:
                        shutil.copy2(source, temporary)
                        os.replace(temporary, destination)
                    finally:
                        temporary.unlink(missing_ok=True)
                    if destination in pointer_payloads:
                        published_pointers.add(destination)
                    if not any(path == destination for path, _ in backups):
                        created.append(destination)

                if not receipt_written:
                    _ensure_target_parent(target, receipt_relative)
                    write_json_atomic(
                        application_receipt,
                        application_receipt_payload,
                        indent=2,
                        ensure_ascii=False,
                    )
                    receipt_written = True

                after = self._lake_index(target, selected)
                expected_target = manifest.get("target_fingerprint")
                if expected_target and _index_digest(after) != expected_target:
                    raise ValueError(
                        "delta post-apply fingerprint mismatch: "
                        f"expected {expected_target}, got {_index_digest(after)}"
                    )
            except Exception:
                # A pointer replacement is the publication boundary.  During
                # rollback, keep every generation/receipt selected by a newly
                # published pointer until that pointer has been switched back
                # (or removed); otherwise readers could resolve a pointer to a
                # half-restored generation.  Files from the old active view
                # are restored before switching back to that old pointer.
                protected_new_paths: set[Path] = set()
                for destination in published_pointers:
                    payload = pointer_payloads.get(destination)
                    if payload is None:
                        continue
                    protected_new_paths.add(destination.relative_to(target))
                    generation_raw = payload.get("generation_path") or payload.get("root")
                    protected_new_paths.add(Path("meta") / _safe_relative(str(generation_raw)))
                    receipt_raw = payload.get("receipt")
                    if receipt_raw:
                        protected_new_paths.add(Path("meta") / _safe_relative(str(receipt_raw)))

                def is_protected(relative: Path) -> bool:
                    return any(
                        relative == prefix or prefix in relative.parents
                        for prefix in protected_new_paths
                    )

                pointer_backup_paths = {
                    destination: backup
                    for destination, backup in backups
                    if destination in pointer_payloads
                }

                # Restore all non-pointer files first.  This includes any
                # deferred deletion/replacement of the old generation, while
                # leaving the new pointer's generation intact until the
                # pointer itself is no longer visible.
                for destination, backup in reversed(backups):
                    relative = destination.relative_to(target)
                    if destination in pointer_payloads or is_protected(relative):
                        continue
                    try:
                        restore_atomically(destination, backup)
                    except OSError:
                        pass
                for path in reversed(created):
                    relative = path.relative_to(target)
                    if path in pointer_payloads or is_protected(relative):
                        continue
                    try:
                        path.unlink(missing_ok=True)
                    except OSError:
                        pass

                # Restore an existing pointer with an atomic inode swap.  A
                # pointer that did not exist before the transaction is removed
                # only after the legacy mutable view has been restored above.
                for destination, backup in pointer_backup_paths.items():
                    try:
                        restore_atomically(destination, backup)
                    except OSError:
                        pass
                for destination in published_pointers:
                    if old_pointer_payloads.get(destination) is None:
                        try:
                            destination.unlink(missing_ok=True)
                        except OSError:
                            pass

                # Once no reader can select the new generation, clean up its
                # unpublished bytes and restore any pre-existing files that
                # occupied those paths.
                for destination, backup in reversed(backups):
                    relative = destination.relative_to(target)
                    if not is_protected(relative):
                        continue
                    try:
                        restore_atomically(destination, backup)
                    except OSError:
                        pass
                for path in reversed(created):
                    relative = path.relative_to(target)
                    if path not in pointer_payloads and is_protected(relative):
                        try:
                            path.unlink(missing_ok=True)
                        except OSError:
                            pass
                if application_receipt is not None:
                    try:
                        application_receipt.unlink(missing_ok=True)
                    except OSError:
                        pass
                raise
            finally:
                shutil.rmtree(backup_root, ignore_errors=True)
        return target

    delta_apply = apply_delta
    apply_delta_package = apply_delta
