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
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from cnequity.config import Config
from cnequity.domain.contracts import contract_fingerprint, dataset_contract
from cnequity.domain.datasets import DATASETS
from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.atomic import (
    replace_with_retry,
    swap_with_backup,
    write_json_atomic,
    write_parquet_atomic,
)
from cnequity.storage.lake_record import read_lake
from cnequity.storage.provenance import runtime_lineage
from cnequity.storage.raw_archive import RawArchiveError, RawPayloadArchive
from cnequity.storage.state import StateStore

#: What a snapshot keeps of each dataset's revision (the rest named files it no longer has).
_REVISION_FIELDS = (
    "revision",
    "revision_id",
    "committed_at",
    "run_id",
    "schema_version",
    "contract_fingerprint",
    "content_digest",
    "changed_partitions",
    "metadata",
)
#: The snapshot layout this release writes: each dataset's published files once,
#: its revision in the manifest. Version 1 also carried revision pointers,
#: receipts and a second copy of each dataset as a generation.
FORMAT_VERSION = 2
#: Where a restore puts the data files it then publishes into the new catalog.
_RESTORE_DIR = ".restore"
#: The migrations that move a lake onto its catalog; a restore does them itself.
_STORAGE_MIGRATIONS = ("lake-catalog", "revision-records", "drop-generations")

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

    Symlinks are rejected even when their target is inside the lake.  With
    ``hardlink=True`` hard links are rejected too, for snapshot packages:
    otherwise two manifest paths could silently share one mutable inode after
    extraction.  Live lake generations share unchanged files by hard link, so
    lake sources are read without that flag and copied as independent files.
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


def _safe_relative(raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise ValueError(f"unsafe snapshot path: {raw}")
    return path


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
    version = manifest.get("format_version", 1)
    revisions = manifest.get("revisions")
    if version >= 2 and (
        not isinstance(revisions, Mapping) or set(map(str, revisions)) != set(datasets)
    ):
        issues.append("revisions")
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
                if version >= 2 and relative.parts[1:2] == ("revisions",):
                    issues.append(f"file:{raw_path}:root")
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
            ahead_of_snapshot = created_day is not None and marker > created_day
            backed_by_bytes = False
            if field == "last_success_trade_date":
                date_column = spec.query_date_col or spec.date_col
                if date_column:
                    maximum = max_data_date(dataset, date_column)
                    if maximum is not None:
                        try:
                            maximum_day = maximum.date() if hasattr(maximum, "date") else maximum
                            if isinstance(maximum_day, str):
                                # A report period stored as text (``2025-12-31``).
                                maximum_day = date.fromisoformat(maximum_day[:10])
                            if marker > maximum_day:
                                issues.append(f"state:{dataset}:{field}")
                            else:
                                backed_by_bytes = True
                        except (AttributeError, TypeError, ValueError):
                            issues.append(f"state:{dataset}:{field}")
            # A watermark may run ahead of the snapshot day when the rows that
            # justify it do too: trading_calendar is written a year ahead on
            # purpose, and refusing that made every snapshot with a calendar
            # (`--research` among them) unverifiable.
            if ahead_of_snapshot and not backed_by_bytes:
                issues.append(f"state:{dataset}:{field}")

        revision = state.get("revision")
        if revision is not None:
            if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
                issues.append(f"state:{dataset}:revision")
                revision = None
        revision_id = state.get("revision_id")
        if revision is not None and revision > 0 and not isinstance(revision_id, str):
            issues.append(f"state:{dataset}:revision_id")

        if version >= 2:
            # The manifest holds the revision; there is no pointer or receipt.
            recorded = revisions.get(dataset) if isinstance(revisions, Mapping) else None
            number = recorded.get("revision") if isinstance(recorded, Mapping) else None
            if (
                not isinstance(recorded, Mapping)
                or isinstance(number, bool)
                or not isinstance(number, int)
                or number < 1
                or not isinstance(recorded.get("revision_id"), str)
                or not recorded["revision_id"]
            ):
                issues.append(f"revision:{dataset}")
                continue
            for field in (
                "revision",
                "revision_id",
                "content_digest",
                "schema_version",
                "contract_fingerprint",
            ):
                if state.get(field) is not None and state[field] != recorded.get(field):
                    issues.append(f"state:{dataset}:{field}")
            continue

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
class _PlannedCopy:
    """An immutable lake file to copy into a snapshot once the lake lock is released."""

    source: Path
    stored: Path
    dataset: str
    layer: str
    #: Size and modification time when the lake's view was read under the lock.
    stamp: tuple[int, int]


def _planned_copy(source: Path, stored: Path, dataset: str, layer: str) -> _PlannedCopy:
    status = source.stat()
    return _PlannedCopy(source, stored, dataset, layer, (status.st_size, status.st_mtime_ns))


@dataclass
class _SnapshotPlan:
    """What :meth:`SnapshotStore.create` read under the lake lock, and what is left to copy."""

    name: str
    selected: list[str]
    temp: Path
    destination: Path
    dataset_states: dict[str, dict]
    records: list[SnapshotFile]
    runtime_state: dict[str, Any]
    research_inputs: dict[str, Any]
    lifecycle_dependencies: Any
    copies: list[_PlannedCopy]
    revisions: dict[str, dict[str, Any]]
    lake_record: dict[str, Any]


@dataclass(frozen=True)
class SnapshotVerification:
    snapshot: str
    passed: bool
    verified_files: int
    missing: tuple[str, ...]
    mismatched: tuple[str, ...]


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

    def create(self, name: str, datasets: list[str], *, research: bool = False) -> Path:
        """Copy selected datasets into a new immutable snapshot directory.

        The lake's view is read as one transaction: compaction and revision
        publication use the same mutation lock, so the state, receipt and
        pointer of every dataset are read, and the files they name are listed,
        while it is held. A snapshot cannot capture the old state beside the
        new pointer while a commit is in flight.

        The bytes are copied after the lock is released. Generations and raw
        payloads are immutable once written, so the files listed still hold
        what the view named, and a copy that takes hours (a raw archive on a
        network drive) does not keep every other run waiting. A file that
        retention removed, or that changed, in the meantime fails the snapshot.
        """
        # Validate before acquiring the lock because lock acquisition creates
        # ``meta/locks``.  A user-controlled symlinked metadata root must not
        # be followed merely as a side effect of taking a read lock.
        _reject_symlink_path(self.config.meta_root, label="metadata root")
        _reject_symlink_path(self.config.data_root, label="data root")
        if research:
            from cnequity.storage.research_inputs import RESEARCH_DATASETS

            datasets = sorted(set(datasets) | RESEARCH_DATASETS)
        with lake_mutation_lock(self.config.meta_root, blocking=True):
            plan = self._plan_locked(name, datasets)
        return self._copy_and_publish(plan)

    def _plan_locked(self, name: str, datasets: list[str]) -> _SnapshotPlan:
        """Read the lake's view and copy its small mutable files; caller owns the lake lock."""
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
        copies: list[_PlannedCopy] = []
        runtime_state: dict[str, Any] = {}
        try:
            research_inputs = self._capture_research_inputs(temp, selected, records)
            from cnequity.storage.lifecycle import LifecycleStore
            from cnequity.storage.lifecycle.snapshot import DEPENDENCIES_PATH, export_dependencies

            lifecycle_dependencies = export_dependencies(LifecycleStore(self.config.meta_root))
            if lifecycle_dependencies is not None:
                stored = temp / "meta" / DEPENDENCIES_PATH
                write_json_atomic(stored, lifecycle_dependencies, indent=2)
                records.append(
                    SnapshotFile(
                        dataset=selected[0],
                        layer="meta",
                        path=stored.relative_to(temp).as_posix(),
                        size_bytes=stored.stat().st_size,
                        sha256=_sha256(stored),
                    )
                )
            dataset_states = {dataset: state.get_payload(dataset) for dataset in selected}
            for payload in dataset_states.values():
                # The catalog is each revision's record; it has no receipt or pointer file.
                payload.pop("revision_receipt", None)
                payload.pop("revision_pointer", None)
            from cnequity.storage.lake_write import export_tree, published_layout

            revisions = self._current_revisions(selected)
            for dataset in selected:
                layer = str(DATASETS[dataset].layer)
                layout = published_layout(self.config.data_root, dataset)
                if layout is None:
                    # Rows the catalog wrote itself, possibly several
                    # partitions to a file: write each partition out now.
                    source = temp / ".export" / layer / dataset
                    export_tree(self.config.data_root, dataset, source)
                    layout = {}
                    for path in sorted(source.rglob("*.parquet")):
                        label = path.parent.relative_to(source).as_posix()
                        layout.setdefault("" if label == "." else label, []).append(
                            (path, path.name)
                        )
                if not any(layout.values()):
                    raise FileNotFoundError(f"dataset has no parquet files: {dataset}")
                for label, files in sorted(layout.items()):
                    for source_file, stored_name in files:
                        _reject_link_or_non_regular(source_file)
                        stored = temp / "data" / layer / dataset / label / stored_name
                        copies.append(_planned_copy(source_file, stored, dataset, layer))

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
            raw_archive = RawPayloadArchive(self.config.meta_root)
            for dataset in selected:
                try:
                    source_entries = raw_archive.export_files(dataset)
                except (RawArchiveError, OSError) as exc:
                    raise ValueError(f"raw archive cannot be exported: {exc}") from exc
                for relative_source, source_file in sorted(source_entries.items()):
                    # Sidecars are intentionally preserved regardless of
                    # extension; only directories are absent from the
                    # non-following file set.
                    _reject_link_or_non_regular(source_file, hardlink=True)
                    stored = temp / "meta" / relative_source
                    copies.append(_planned_copy(source_file, stored, dataset, "meta"))

            # Evidence publishers can run independently of the compact lock.
            # Reject a changing set instead of shipping a silently mixed read.
            if research_inputs["source_fingerprint"] != self._research_evidence_identity():
                raise RuntimeError("research evidence changed during snapshot; retry creation")
        except Exception:
            shutil.rmtree(temp, ignore_errors=True)
            raise
        return _SnapshotPlan(
            name=name,
            selected=selected,
            temp=temp,
            destination=destination,
            dataset_states=dataset_states,
            records=records,
            runtime_state=runtime_state,
            research_inputs=research_inputs,
            lifecycle_dependencies=lifecycle_dependencies,
            copies=copies,
            revisions=revisions,
            lake_record=read_lake(self.config),
        )

    def _copy_and_publish(self, plan: _SnapshotPlan) -> Path:
        """Copy the planned files, then verify and publish the snapshot. Needs no lock."""
        name, selected, temp, records = plan.name, plan.selected, plan.temp, plan.records
        try:
            for item in plan.copies:
                item.stored.parent.mkdir(parents=True, exist_ok=True)
                try:
                    shutil.copy2(item.source, item.stored)
                    # Lake files are read-only; a snapshot's own copy is not, or
                    # deleting the snapshot would fail on Windows.
                    item.stored.chmod(0o644)
                    status = item.source.stat()
                except FileNotFoundError as exc:
                    raise RuntimeError(
                        f"lake file was removed during snapshot; retry creation: {item.source}"
                    ) from exc
                if (status.st_size, status.st_mtime_ns) != item.stamp:
                    raise RuntimeError(
                        f"lake file changed during snapshot; retry creation: {item.source}"
                    )
                records.append(
                    SnapshotFile(
                        dataset=item.dataset,
                        layer=item.layer,
                        path=item.stored.relative_to(temp).as_posix(),
                        size_bytes=item.stored.stat().st_size,
                        sha256=_sha256(item.stored),
                    )
                )
            manifest = {
                "format": "cnequity.lake-snapshot",
                "format_version": FORMAT_VERSION,
                "name": name,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "datasets": selected,
                "dataset_states": plan.dataset_states,
                # Each dataset's revision: a restore republishes it under this number and id.
                "revisions": plan.revisions,
                # The migrations the lake had: a restored lake owes what it owed.
                "lake_record": plan.lake_record,
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
                "runtime_state": plan.runtime_state,
                "research_inputs": plan.research_inputs,
                "lifecycle_dependencies": {
                    "path": "meta/lifecycle/snapshot-dependencies.json",
                    "external_bytes_materialized": False,
                    "requires_rebinding": True,
                }
                if plan.lifecycle_dependencies is not None
                else None,
                "files": [asdict(item) for item in records],
            }
            write_json_atomic(temp / "manifest.json", manifest, indent=2, ensure_ascii=False)
            # Do not publish a package whose denormalised state disagrees with
            # its pointer/receipt, even if all copied bytes are individually
            # valid.  The same semantic verifier is used by ``verify`` and
            # ``restore``; running it before the final rename makes snapshot
            # creation itself a consistent transaction boundary.
            # Exported catalog rows were copied into the snapshot like any
            # other source; their staging copy is not part of it.
            shutil.rmtree(temp / ".export", ignore_errors=True)
            verification = self._verify_directory(name, temp, manifest)
            if not verification.passed:
                raise ValueError(
                    f"snapshot consistency check failed: missing={verification.missing}, "
                    f"mismatched={verification.mismatched}"
                )
            if plan.destination.exists():
                raise FileExistsError(f"snapshot already exists: {plan.destination}")
            replace_with_retry(temp, plan.destination)
        except Exception:
            shutil.rmtree(temp, ignore_errors=True)
            raise
        return plan.destination / "manifest.json"

    def _current_revisions(self, datasets: list[str]) -> dict[str, dict[str, Any]]:
        """Each dataset's current revision as the lake catalog records it; caller holds the lock.

        A snapshot keeps the revision's number and id, so a restored lake
        answers a read pinned to it, and a result made against this lake can
        be reproduced from the snapshot.
        """
        from cnequity.storage.lake_record import LakeMigrationRequired
        from cnequity.storage.revision_records import enabled
        from cnequity.storage.revisions import RevisionStore

        if not enabled(self.config.meta_root):
            raise LakeMigrationRequired(
                "a snapshot is made of a lake on its catalog; run `cne upgrade --apply` first"
            )
        store = RevisionStore(
            self.config.meta_root,
            self.config.curated_root,
            self.config.derived_root,
            create=False,
        )
        revisions: dict[str, dict[str, Any]] = {}
        for dataset in datasets:
            receipt = store.latest(dataset)
            if receipt is None:
                raise FileNotFoundError(f"dataset has no published revision: {dataset}")
            revisions[dataset] = {
                key: value for key, value in receipt.to_dict().items() if key in _REVISION_FIELDS
            }
        return revisions

    def _research_evidence_identity(self) -> dict[str, str]:
        from cnequity.storage.research_inputs import CONTEXT_PATH, RESEARCH_METADATA

        identity = {}
        for relative in (*RESEARCH_METADATA, CONTEXT_PATH.as_posix()):
            root = self.config.meta_root / relative
            _reject_symlink_path(root, label="research evidence")
            if not root.exists():
                continue
            if root.is_dir():
                entries, unsafe = _tree_files_no_follow(root, reject_hardlinks=True)
                if unsafe:
                    raise ValueError(f"unsafe research evidence files: {sorted(unsafe)}")
                paths = [root / item for item in sorted(entries)]
            else:
                paths = [root]
            for path in paths:
                _reject_link_or_non_regular(path, hardlink=True)
                identity[path.relative_to(self.config.meta_root).as_posix()] = _sha256(path)
        return identity

    def _capture_research_inputs(self, target: Path, selected: list[str], records: list) -> dict:
        from cnequity.storage.research_inputs import (
            CONTEXT_PATH,
            RESEARCH_DATASETS,
            research_context,
        )

        identity = self._research_evidence_identity()
        paths = []
        for relative, digest in identity.items():
            if relative == CONTEXT_PATH.as_posix():
                continue
            stored = target / "meta" / relative
            stored.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.config.meta_root / relative, stored)
            if _sha256(stored) != digest:
                raise RuntimeError("research evidence changed while copying")
            paths.append(stored)
        context = target / "meta" / CONTEXT_PATH
        write_json_atomic(context, research_context(self.config, selected), indent=2)
        paths.append(context)
        for stored in paths:
            records.append(
                SnapshotFile(
                    dataset=selected[0],
                    layer="meta",
                    path=stored.relative_to(target).as_posix(),
                    size_bytes=stored.stat().st_size,
                    sha256=_sha256(stored),
                )
            )
        return {
            "schema_version": 1,
            "context_path": (Path("meta") / CONTEXT_PATH).as_posix(),
            "files": [path.relative_to(target).as_posix() for path in paths],
            "dependency_closure": RESEARCH_DATASETS <= set(selected),
            "source_fingerprint": identity,
        }

    def _manifest(self, name: str) -> tuple[Path, dict]:
        snapshot = self.path(name)
        manifest = snapshot / "manifest.json"
        try:
            payload = _read_json_object(manifest)
        except FileNotFoundError:
            # Name what the operator typed. The bare manifest path reads as a
            # path they never wrote, on a command whose only argument was NAME.
            raise FileNotFoundError(f"no snapshot named {name!r} ({manifest})") from None
        if payload.get("format") != "cnequity.lake-snapshot" or payload.get(
            "format_version"
        ) not in (1, FORMAT_VERSION):
            raise ValueError(f"unsupported snapshot manifest: {manifest}")
        return snapshot, payload

    def delete(self, name: str) -> dict[str, Any]:
        """Remove a snapshot and the retention references it contributed.

        The directory is renamed aside before removal, so an interrupted
        delete never leaves a half-snapshot under its own name. Only stale
        references whose citing file lay inside the snapshot are released;
        manual holds and every other citation stay.
        """
        from cnequity.storage.lifecycle import LifecycleStore

        snapshot, _ = self._manifest(name)
        meta = self.config.meta_root
        with lake_mutation_lock(meta):
            doomed = snapshot.with_name(f".deleting-{snapshot.name}-{uuid.uuid4().hex[:8]}")
            absolute = snapshot.absolute()
            replace_with_retry(snapshot, doomed)
            shutil.rmtree(doomed)
            store = LifecycleStore(meta)
            released = store.release_references_under(absolute, f"snapshot {name} deleted")
            if released:
                store.refresh_references()
        return {"snapshot": name, "path": str(absolute), "released": released}

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
        lifecycle = manifest.get("lifecycle_dependencies")
        dependency_path = "meta/lifecycle/snapshot-dependencies.json"
        if lifecycle is not None or dependency_path in seen:
            from cnequity.storage.lifecycle import LifecycleError
            from cnequity.storage.lifecycle.snapshot import read_dependencies

            if (
                lifecycle
                != {
                    "path": dependency_path,
                    "external_bytes_materialized": False,
                    "requires_rebinding": True,
                }
                or dependency_path not in seen
            ):
                mismatched.append("lifecycle_dependencies")
            try:
                if read_dependencies(snapshot / "meta") is None:
                    missing.append(dependency_path)
            except (LifecycleError, ValueError, OSError):
                mismatched.append(dependency_path)
        research = manifest.get("research_inputs")
        if research is not None:
            if not isinstance(research, Mapping) or research.get("schema_version") != 1:
                mismatched.append("research_inputs")
            else:
                declared = research.get("files", [])
                if (
                    not isinstance(declared, list)
                    or not all(isinstance(item, str) for item in declared)
                    or not set(declared) <= seen
                ):
                    mismatched.append("research_inputs.files")
                if research.get("context_path") not in seen:
                    mismatched.append("research_inputs.context_path")
                from cnequity.storage.research_inputs import (
                    CONTEXT_PATH,
                    RESEARCH_DATASETS,
                    load_research_context,
                )

                expected_context = (Path("meta") / CONTEXT_PATH).as_posix()
                if research.get("context_path") != expected_context:
                    mismatched.append("research_inputs.context_path")
                fingerprint = research.get("source_fingerprint", {})
                if not isinstance(fingerprint, Mapping):
                    mismatched.append("research_inputs.source_fingerprint")
                    fingerprint = {}
                for relative, digest in fingerprint.items():
                    if not isinstance(relative, str) or not isinstance(digest, str):
                        mismatched.append("research_inputs.source_fingerprint")
                        continue
                    if relative == CONTEXT_PATH.as_posix():
                        continue
                    path = _snapshot_meta_path(
                        snapshot, str(Path("meta") / relative), label="research evidence"
                    )
                    if not path.is_file() or _sha256(path) != digest:
                        mismatched.append(f"research_inputs:{relative}")
                if research.get("dependency_closure") and not RESEARCH_DATASETS <= set(
                    manifest.get("datasets", [])
                ):
                    mismatched.append("research_inputs.dependency_closure")
                try:
                    load_research_context(Config(data_root=snapshot))
                except (ValueError, OSError, TypeError):
                    mismatched.append("research_inputs.context")
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
            replace_with_retry(temporary, archive)
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
            if manifest.get("format") != "cnequity.lake-snapshot" or manifest.get(
                "format_version"
            ) not in (1, FORMAT_VERSION):
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
                swap_with_backup(temporary, destination, backup)
                shutil.rmtree(backup, ignore_errors=True)
            else:
                replace_with_retry(temporary, destination)
            return destination
        except BaseException:
            shutil.rmtree(temporary, ignore_errors=True)
            raise

    import_ = import_archive
    archive_import = import_archive
    import_tarball = import_archive

    def restore(self, name: str, target_data_root: Path) -> Path:
        """Restore into a new or empty explicit target; existing data is never overwritten.

        The target is a lake on its catalog, which every command can write. A
        snapshot of this format is published into a new catalog, each dataset
        under the revision number and id it had. A snapshot made before 0.25.0
        restores the lake it held and then moves it onto its catalog, as
        ``cne upgrade`` would; the other migrations that lake owes stay owed.
        """
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
        current = manifest.get("format_version") == FORMAT_VERSION
        # Where the data files land: a pre-0.25 lake's own layout, or a folder
        # they are published from into the new catalog.
        landing = Path(_RESTORE_DIR) if current else Path()
        try:
            for record in manifest["files"]:
                relative = _safe_relative(str(record["path"]))
                if relative.parts[0] == "data":
                    # Stored data paths include a leading data/ while the
                    # restore root itself is the lake's data directory.
                    restored_relative = landing / Path(*relative.parts[1:])
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
            self._rebuild_runtime_state(temp, manifest, data_root=temp / landing)
            if current:
                self._start_restored_lake(temp, manifest)
            write_json_atomic(
                meta / "restored-snapshot.json", manifest, indent=2, ensure_ascii=False
            )
            _reject_symlink_path(target, label="restore target")
            if target.exists():
                target.rmdir()
            replace_with_retry(temp, target)
        except Exception:
            shutil.rmtree(temp, ignore_errors=True)
            raise
        # The catalog is built where the lake will live: it records where its files are.
        if not current:
            self._upgrade_restored(target)
            return target
        try:
            self._publish_restored(target, manifest)
        except Exception:
            shutil.rmtree(target, ignore_errors=True)
            raise
        return target

    def _start_restored_lake(self, root: Path, manifest: Mapping[str, Any]) -> None:
        """Give the lake being restored the migration record of the lake it was taken from.

        A snapshot of this format comes from a lake on its catalog and holds no
        generations, so the migrations that move a lake there are not owed.
        """
        from dataclasses import replace

        from cnequity.storage.lake_record import _write_lake, package_version, start_on_catalog

        config = replace(self.config, data_root=root)
        record = dict(manifest.get("lake_record") or {})
        migrations = dict(record.get("migrations") or {})
        for migration_id in _STORAGE_MIGRATIONS:
            migrations.setdefault(
                migration_id,
                {
                    "status": "not_needed",
                    "affected": 0,
                    "at": datetime.now(timezone.utc).isoformat(),
                    "version": package_version(),
                },
            )
        record["migrations"] = migrations
        _write_lake(config, record)
        start_on_catalog(config)

    def _publish_restored(self, target: Path, manifest: Mapping[str, Any]) -> None:
        """Publish each dataset's restored files into the new lake's catalog as its revision."""
        from dataclasses import replace

        from cnequity.storage.lake_write import DatasetPublish, LakeWrite

        config = replace(self.config, data_root=target)
        landing = target / _RESTORE_DIR
        run_id = f"restore-{manifest.get('name', 'snapshot')}-{uuid.uuid4().hex[:8]}"
        publishes: dict[str, DatasetPublish] = {}
        with LakeWrite(config, run_id) as write:
            for dataset in manifest["datasets"]:
                root = landing / str(DATASETS[dataset].layer) / dataset
                partitions: dict[str, list[Path]] = {}
                for path in sorted(root.rglob("*.parquet")):
                    label = path.parent.relative_to(root).as_posix()
                    partitions.setdefault("" if label == "." else label, []).append(path)
                checkout = write.checkout(dataset)
                for label, files in partitions.items():
                    # Moved, not copied: the landing folder is this restore's own.
                    folder = checkout.partition(label)
                    for path in files:
                        replace_with_retry(path, folder / path.name)
                identity = dict(manifest["revisions"][dataset])
                metadata = {
                    key: value
                    for key, value in (identity.get("metadata") or {}).items()
                    # Facts about how the revision was written there, not here.
                    if key not in {"storage", "base_revision_id", "removed_partitions"}
                }
                publishes[dataset] = DatasetPublish(
                    schema_version=int(identity["schema_version"]),
                    contract_fingerprint=str(identity["contract_fingerprint"]),
                    metadata=metadata,
                    identity=identity,
                )
            write.publish(publishes, message=f"restore snapshot {manifest.get('name')}")
        shutil.rmtree(landing, ignore_errors=True)

    def _upgrade_restored(self, target: Path) -> None:
        """Move a restored pre-0.25 lake onto its catalog, as `cne upgrade` would.

        Only the migrations that change how the lake is stored run here; the
        repairs it may also owe can read sources and stay for ``cne upgrade``.
        A failure leaves the restored lake in place, as an older release left it.
        """
        from dataclasses import replace

        from cnequity.storage.catalog_migration import migrate_to_catalog
        from cnequity.storage.drop_generations import drop_generations
        from cnequity.storage.lake_record import record_migration
        from cnequity.storage.revision_records import import_receipts

        config = replace(self.config, data_root=target)
        steps = {
            "lake-catalog": migrate_to_catalog,
            "revision-records": import_receipts,
            "drop-generations": drop_generations,
        }
        for migration_id in _STORAGE_MIGRATIONS:
            if migration_id in (read_lake(config).get("migrations") or {}):
                continue
            try:
                report = steps[migration_id](config, apply=True)
            except Exception as exc:
                raise RuntimeError(
                    f"restored to {target}, but moving it onto its lake catalog failed "
                    f"({migration_id}: {exc}); run `cne upgrade --apply` there"
                ) from exc
            record_migration(config, migration_id, report)

    @staticmethod
    def _rebuild_runtime_state(
        target: Path, manifest: Mapping[str, Any], *, data_root: Path | None = None
    ) -> int:
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
            if ((data_root or target) / "derived" / "adj_factors").is_dir():
                cache_state = {"mode": "rebuildable"}
            else:
                return 0
        if cache_state.get("mode") == "included":
            return 0

        derived = (data_root or target) / "derived" / "adj_factors"
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
