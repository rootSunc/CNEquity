"""Immutable, replayable archives of selected source payloads.

The curated Parquet layer intentionally stores normalized rows only.  For a
small set of high-value or snapshot-only feeds we also retain the exact wire
payload, compressed and addressed by content hash.  The archive is an audit
artifact, not a request cache: it never stores authorization headers, cookies,
tokens, proxy settings, or a caller's complete HTTP client configuration.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import secrets
import stat
import tempfile
import threading
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from cnequity.storage.atomic import publish_once, write_json_atomic

_SENSITIVE_KEY = re.compile(
    r"(?:token|secret|password|passwd|authorization|cookie|proxy|api[_-]?key|access[_-]?key|signature|credential)",
    re.IGNORECASE,
)
_SAFE_SOURCE = re.compile(r"[^A-Za-z0-9._-]+")
_SAFE_DATASET = re.compile(r"[^A-Za-z0-9._-]+")
_ARCHIVE_SCHEMA_VERSION = 1
#: Payload formats that are already compressed internally.
_STORED_UNCOMPRESSED_FORMATS = frozenset({"pdf"})

# Receipt hand-off is deliberately in-memory and owner-scoped.  It is not a
# second archive index: the adapter's ``archive`` call returns a concrete
# ``RawPayloadRecord`` and this channel only lets the caller hand those exact
# records to the publisher after a normalized frame has been built.  A lock is
# required because some source adapters use several request lanes in one run.
_CAPTURE_REGISTRY_GUARD = threading.RLock()
_CAPTURE_REGISTRY_ATTR = "_raw_archive_capture_records"
_CAPTURE_REGISTRY_LOCK_ATTR = "_raw_archive_capture_lock"


@dataclass
class _CaptureBucket:
    """Ephemeral receipt bucket for exactly one logical source observation."""

    nonce: str
    records: list[Any] = field(default_factory=list)
    consumed: bool = False


class RawArchiveError(RuntimeError):
    """Raised for invalid archive metadata or an unavailable payload."""


def _capture_state(owner: object) -> tuple[dict[tuple[str, str, str, str], _CaptureBucket], Any]:
    """Return an owner-local receipt registry and its lock.

    Config is intentionally not given a persisted field for this state.  The
    registry exists only for one process/run and is never serialized into a
    manifest or checkpoint.
    """
    with _CAPTURE_REGISTRY_GUARD:
        registry = getattr(owner, _CAPTURE_REGISTRY_ATTR, None)
        if not isinstance(registry, dict):
            registry = {}
            setattr(owner, _CAPTURE_REGISTRY_ATTR, registry)
        lock = getattr(owner, _CAPTURE_REGISTRY_LOCK_ATTR, None)
        if lock is None:
            lock = threading.RLock()
            setattr(owner, _CAPTURE_REGISTRY_LOCK_ATTR, lock)
    return registry, lock


def _capture_record(
    owner: object,
    record: RawPayloadRecord,
    *,
    run_id: str | None,
    source: str | None,
    request_scope: str | None,
    capture_nonce: str | None,
) -> RawPayloadRecord:
    """Register one adapter-returned record for direct evidence hand-off."""
    registry, lock = _capture_state(owner)
    key = (
        record.dataset,
        str(run_id if run_id is not None else record.run_id or ""),
        str(source if source is not None else record.source),
        str(request_scope if request_scope is not None else record.request_scope or ""),
    )
    with lock:
        bucket = registry.get(key)
        if bucket is None:
            raise RawArchiveError(
                "raw archive capture is not active for the adapter's source/request scope"
            )
        if bucket.consumed:
            raise RawArchiveError("raw archive capture was already consumed")
        if capture_nonce is None and record.capture_nonce is None:
            raise RawArchiveError("raw archive capture is missing its nonce")
        if capture_nonce is not None and capture_nonce != bucket.nonce:
            raise RawArchiveError("raw archive capture was superseded by a newer fetch")
        if record.capture_nonce != bucket.nonce:
            record = replace(record, capture_nonce=bucket.nonce)
        # Retries can return the same immutable observation.  Keep one concrete
        # record per sidecar while preserving distinct observation ids.
        if not any(
            getattr(existing, "metadata_path", None) == record.metadata_path
            for existing in bucket.records
        ):
            bucket.records.append(record)
        return record


def begin_capture(
    owner: object,
    dataset: str,
    run_id: str | None,
    *,
    source: str,
    request_scope: str,
) -> str:
    """Start one logical fetch scope and discard an older receipt for it.

    This is intentionally separate from :class:`RawPayloadArchive` creation:
    a multi-report adapter may reuse one scope while constructing several
    archive helpers.  Top-level adapters call this once before their network
    walk; the resulting bucket is then populated by every exact response in
    that walk.  The lock makes a concurrent lane unable to interleave a reset
    with a record append.
    """
    registry, lock = _capture_state(owner)
    key = (str(dataset), str(run_id or ""), str(source), str(request_scope))
    with lock:
        nonce = secrets.token_hex(32)
        registry[key] = _CaptureBucket(nonce=nonce)
        return nonce


def capture_nonce(
    owner: object,
    dataset: str,
    run_id: str,
    *,
    source: str,
    request_scope: str,
) -> str | None:
    """Return the active nonce for one capture, if it still exists."""
    registry, lock = _capture_state(owner)
    key = (str(dataset), str(run_id), str(source), str(request_scope))
    with lock:
        bucket = registry.get(key)
        return bucket.nonce if bucket is not None else None


def captured_records(
    owner: object,
    dataset: str,
    run_id: str,
    *,
    source: str,
    request_scope: str,
) -> list[RawPayloadRecord]:
    """Return only records captured by this exact source/scope invocation."""
    registry, lock = _capture_state(owner)
    key = (str(dataset), str(run_id), str(source), str(request_scope))
    with lock:
        bucket = registry.get(key)
        return list(bucket.records) if bucket is not None else []


def capture_is_consumed(
    owner: object,
    dataset: str,
    run_id: str,
    *,
    source: str,
    request_scope: str,
    nonce: str,
) -> bool:
    """Return whether the current capture has already been published."""
    registry, lock = _capture_state(owner)
    key = (str(dataset), str(run_id), str(source), str(request_scope))
    with lock:
        bucket = registry.get(key)
        return bucket is None or bucket.nonce != str(nonce) or bucket.consumed


@contextmanager
def capture_publish(
    owner: object,
    dataset: str,
    run_id: str,
    *,
    source: str,
    request_scope: str,
    nonce: str,
):
    """Serialize validation/publish and consume a capture only on success."""
    registry, lock = _capture_state(owner)
    key = (str(dataset), str(run_id), str(source), str(request_scope))
    with lock:
        bucket = registry.get(key)
        if bucket is None or bucket.nonce != str(nonce):
            raise RawArchiveError("raw archive evidence capture is no longer active")
        if bucket.consumed:
            raise RawArchiveError("raw archive evidence capture was already consumed")
        try:
            yield
        except Exception:
            # A failed writer remains retryable with the same concrete source
            # observation; only a successful publish consumes the receipt.
            raise
        else:
            bucket.consumed = True


@dataclass(frozen=True)
class RawPayloadRecord:
    """Metadata for one immutable compressed source payload."""

    dataset: str
    source: str
    captured_at: str
    payload_sha256: str
    payload_bytes: int
    compressed_bytes: int
    compression: str
    payload_format: str
    payload_path: str
    metadata_path: str
    request_params: dict[str, Any]
    run_id: str | None = None
    url: str | None = None
    response_status: int | None = None
    # Optional transport and pagination context.  These are deliberately
    # separate from ``request_params`` so a replay can distinguish the form
    # sent to the source from metadata observed in its response.
    http_metadata: dict[str, Any] = field(default_factory=dict)
    pagination: dict[str, Any] = field(default_factory=dict)
    # A request observation may point at an already existing content-addressed
    # payload.  CNINFO needs one sidecar per page request even when two pages
    # happen to return byte-identical bodies (a common no-progress signal).
    observation_id: str | None = None
    # Stable logical fetch scope.  A run may contain several independent
    # requests for the same dataset (for example, one TDX chunk per worker or
    # one CNINFO date slice), so run_id alone is not an evidence identity.
    request_scope: str | None = None
    # Ephemeral in-process binding.  It is intentionally not persisted in the
    # sidecar: immutable payload observations may be reused by a later fetch,
    # but only the current capture bucket may turn them into publish evidence.
    capture_nonce: str | None = field(default=None, repr=False, compare=False)

    def to_dict(self) -> dict[str, Any]:
        payload = dict(self.__dict__)
        payload.pop("capture_nonce", None)
        return {
            "schema_version": _ARCHIVE_SCHEMA_VERSION,
            **payload,
        }


def _safe_component(value: object, fallback: str) -> str:
    text = _SAFE_SOURCE.sub("_", str(value or "").strip())[:100].strip("._")
    return text or fallback


def _lstat(path: Path) -> os.stat_result | None:
    try:
        return path.lstat()
    except FileNotFoundError:
        return None


def _reject_symlink_path(path: Path, *, label: str) -> None:
    """Reject a lexical path whose root or existing ancestor is a symlink."""

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
            # Descendants cannot exist below an absent component.  They may
            # be created later, but only after every existing ancestor above
            # this point has passed the no-follow check.
            break
        # Permit only root-owned filesystem aliases outside the archive
        # boundary (notably macOS /var and /tmp).  The configured metadata
        # root itself and all user-controlled descendants still fail closed.
        trusted_system_alias = (
            cursor != lexical
            and cursor.parent == Path(lexical.anchor)
            and getattr(info, "st_uid", -1) == 0
        )
        if stat.S_ISLNK(info.st_mode) and not trusted_system_alias:
            raise RawArchiveError(f"raw archive {label} contains symlink ancestor: {cursor}")
        if trusted_system_alias:
            continue
        if index < len(parts) - 1 and not stat.S_ISDIR(info.st_mode):
            raise RawArchiveError(f"raw archive {label} ancestor is not a directory: {cursor}")


def _safe_archive_path(root: Path, relative: Path, *, label: str) -> Path:
    """Join an internal archive path after a no-follow and containment check."""

    root = Path(root)
    _reject_symlink_path(root, label=f"{label} root")
    candidate = root / relative
    _reject_symlink_path(candidate, label=label)
    try:
        candidate.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError as exc:
        raise RawArchiveError(f"unsafe raw archive {label} path: {relative}") from exc
    return candidate


def _reject_regular_file(path: Path, *, label: str) -> os.stat_result:
    """Reject links/special files before a sensitive archive read or write."""

    _reject_symlink_path(path, label=label)
    info = _lstat(path)
    if info is None:
        raise FileNotFoundError(path)
    if stat.S_ISLNK(info.st_mode):
        raise RawArchiveError(f"raw archive {label} is a symlink: {path}")
    if not stat.S_ISREG(info.st_mode):
        raise RawArchiveError(f"raw archive {label} is not a regular file: {path}")
    return info


def _read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    """Read one archive sidecar through a no-follow file descriptor."""

    _reject_regular_file(path, label=label)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        with os.fdopen(fd, "r", encoding="utf-8", closefd=True) as handle:
            fd = -1
            payload = json.load(handle)
    finally:
        if fd != -1:
            os.close(fd)
    if not isinstance(payload, dict):
        raise RawArchiveError(f"invalid raw archive metadata: {path}")
    return payload


def _files_no_follow(root: Path) -> list[Path]:
    """Recursively list archive files without traversing directory links."""

    _reject_symlink_path(root, label="root")
    root_info = _lstat(root)
    if root_info is None:
        return []
    if not stat.S_ISDIR(root_info.st_mode):
        raise RawArchiveError(f"raw archive root is not a directory: {root}")
    files: list[Path] = []

    def visit(directory: Path) -> None:
        try:
            with os.scandir(directory) as iterator:
                entries = sorted(iterator, key=lambda entry: entry.name)
        except OSError as exc:
            raise RawArchiveError(f"raw archive directory cannot be read: {directory}") from exc
        for entry in entries:
            info = entry.stat(follow_symlinks=False)
            path = Path(entry.path)
            if stat.S_ISLNK(info.st_mode):
                raise RawArchiveError(f"raw archive path is a symlink: {path}")
            if stat.S_ISDIR(info.st_mode):
                visit(path)
            elif stat.S_ISREG(info.st_mode):
                files.append(path)
            else:
                raise RawArchiveError(f"raw archive path is not regular: {path}")

    visit(root)
    return files


def _redact(value: Any, *, key: str | None = None) -> Any:
    """Return JSON-compatible request metadata without secret material."""
    if key is not None and _SENSITIVE_KEY.search(key):
        return "<redacted>"
    if isinstance(value, Mapping):
        return {str(k): _redact(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_redact(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def sanitize_request_params(params: Mapping[str, Any] | None) -> dict[str, Any]:
    """Redact sensitive keys recursively and return a detached mapping."""
    if not params:
        return {}
    return dict(_redact(dict(params)))


def sanitize_url(url: str | None) -> str | None:
    """Keep a URL useful for replay while dropping sensitive query values."""
    if not url:
        return None
    try:
        parsed = urlsplit(str(url))
        # Never retain URL userinfo.  HTTP clients frequently encode proxy or
        # basic-auth credentials there, and those credentials are not needed
        # to identify the source endpoint during an offline replay.
        hostname = parsed.hostname or ""
        port = ""
        try:
            if parsed.port is not None:
                port = f":{parsed.port}"
        except ValueError:
            port = ""
        netloc = f"{hostname}{port}" if hostname else ""
        query = []
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            query.append((key, "<redacted>" if _SENSITIVE_KEY.search(key) else value))
        return urlunsplit((parsed.scheme, netloc, parsed.path, urlencode(query), ""))
    except ValueError:
        return "<redacted-url>"


def _payload_bytes(payload: Any, payload_format: str | None) -> tuple[bytes, str]:
    if isinstance(payload, bytes):
        return payload, payload_format or "bytes"
    if isinstance(payload, bytearray):
        return bytes(payload), payload_format or "bytes"
    if isinstance(payload, str):
        return payload.encode("utf-8"), payload_format or "text"
    try:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode(
            "utf-8"
        )
    except (TypeError, ValueError) as exc:
        raise RawArchiveError(f"payload is not serializable: {exc}") from exc
    return encoded, payload_format or "json"


def _write_bytes_once(path: Path, data: bytes) -> None:
    """Atomically create *path* without replacing an existing immutable file."""
    _reject_symlink_path(path, label="payload")
    existing = _lstat(path)
    if existing is not None:
        if stat.S_ISLNK(existing.st_mode):
            raise RawArchiveError(f"raw archive payload is a symlink: {path}")
        if not stat.S_ISREG(existing.st_mode):
            raise RawArchiveError(f"raw archive payload is not a regular file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink_path(path.parent, label="payload parent")
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        # An existing file with the same hash is the immutable payload already
        # present. Do not overwrite it; callers verify the bytes afterwards.
        publish_once(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _decompress_payload(compressed: bytes, compression: str, path: Path) -> bytes:
    """Decode one stored payload and normalize corruption to ``RawArchiveError``."""
    mode = str(compression or "").lower()
    if mode == "none":
        return compressed
    if mode != "gzip":
        raise RawArchiveError(f"unsupported raw payload compression {compression!r}: {path}")
    try:
        return gzip.decompress(compressed)
    except (OSError, EOFError, ValueError) as exc:
        raise RawArchiveError(f"raw payload cannot be decompressed: {path}") from exc


def _verify_stored_payload(
    path: Path,
    expected_compressed: bytes | None = None,
    *,
    digest: str | None = None,
    compression: str,
    payload_bytes: int | None = None,
    compressed_bytes: int | None = None,
) -> bytes:
    """Read, decode, and hash-check a payload every time it is reused.

    Content-addressed paths are not sufficient protection by themselves: an
    attacker (or a damaged filesystem) can replace a file with another byte
    sequence of the same length.  Re-reading and decoding the existing file
    on every archive hit makes that case observable before metadata is
    returned or a replay is allowed.
    """
    _reject_regular_file(path, label="payload")
    try:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags)
        try:
            opened = os.fstat(fd)
            if not stat.S_ISREG(opened.st_mode):
                raise RawArchiveError(f"raw archive payload is not a regular file: {path}")
            with os.fdopen(fd, "rb", closefd=True) as handle:
                fd = -1
                stored = handle.read()
        finally:
            if fd != -1:
                os.close(fd)
    except OSError as exc:
        raise RawArchiveError(f"raw payload cannot be read: {path}") from exc
    if compressed_bytes is not None and len(stored) != int(compressed_bytes):
        raise RawArchiveError(f"raw payload compressed size mismatch: {path}")
    if expected_compressed is not None and len(stored) != len(expected_compressed):
        raise RawArchiveError(f"immutable raw payload changed unexpectedly: {path}")
    if expected_compressed is not None and stored != expected_compressed:
        raise RawArchiveError(f"immutable raw payload bytes changed: {path}")
    raw = _decompress_payload(stored, compression, path)
    if payload_bytes is not None and len(raw) != int(payload_bytes):
        raise RawArchiveError(f"raw payload size mismatch after decompression: {path}")
    if digest and hashlib.sha256(raw).hexdigest() != str(digest):
        raise RawArchiveError(f"raw payload digest mismatch: {path}")
    return raw


class RawPayloadArchive:
    """Write and replay compressed source payloads under ``meta/raw``."""

    def __init__(
        self,
        meta_root: Path | str,
        *,
        enabled: bool = True,
        datasets: list[str] | tuple[str, ...] | set[str] | None = None,
        compression: str = "gzip",
        max_payload_bytes: int | None = None,
        capture_owner: object | None = None,
        capture_run_id: str | None = None,
        capture_source: str | None = None,
        capture_scope: str | None = None,
        capture_nonce: str | None = None,
    ):
        self.meta_root = Path(meta_root)
        self.root = self.meta_root / "raw"
        self.enabled = bool(enabled)
        self.datasets = None if datasets is None else frozenset(str(item) for item in datasets)
        self.compression = str(compression or "gzip").lower()
        self.max_payload_bytes = max_payload_bytes
        # ``capture_owner`` is an ephemeral in-process receipt channel.  It
        # lets a page-oriented adapter hand the exact records it just wrote to
        # its publish boundary without rescanning all sidecars for a run.
        self._capture_owner = capture_owner
        self._capture_run_id = str(capture_run_id) if capture_run_id is not None else None
        self._capture_source = str(capture_source) if capture_source is not None else None
        self._capture_scope = str(capture_scope) if capture_scope is not None else None
        self._capture_nonce = str(capture_nonce) if capture_nonce is not None else None
        if self.compression not in {"gzip", "none"}:
            raise ValueError("raw archive compression must be 'gzip' or 'none'")
        if max_payload_bytes is not None and int(max_payload_bytes) < 1:
            raise ValueError("raw archive max_payload_bytes must be >= 1")

    def should_archive(self, dataset: str) -> bool:
        return self.enabled and (self.datasets is None or dataset in self.datasets)

    def _validate_roots(self) -> None:
        """Check configured archive roots before any filesystem operation."""

        _reject_symlink_path(self.meta_root, label="metadata root")
        _reject_symlink_path(self.root, label="raw root")

    def external_locations(self) -> list[dict[str, Any]]:
        """Read lake-local locations; sidecars and logical paths stay portable."""
        self._validate_roots()
        path = self.meta_root / "raw_storage.json"
        if _lstat(path) is None:
            return []
        value = _read_json_object(path, label="storage locations")
        if value.get("schema_version") != 1 or not isinstance(value.get("locations"), list):
            raise RawArchiveError("invalid raw storage locations")
        seen = set()
        for location in value["locations"]:
            if not isinstance(location, dict):
                raise RawArchiveError("invalid raw storage location")
            prefix = Path(str(location.get("prefix", "")))
            root = Path(str(location.get("root", ""))).expanduser()
            formats = location.get("formats")
            if (
                len(prefix.parts) != 3
                or prefix.parts[0] != "raw"
                or _safe_component(prefix.parts[1], "") != prefix.parts[1]
                or not prefix.parts[2].startswith("source=")
                or _safe_component(prefix.parts[2][7:], "") != prefix.parts[2][7:]
                or prefix.as_posix() in seen
                or not root.is_absolute()
                or ".." in root.parts
                or not isinstance(formats, list)
                or not formats
                or any(not isinstance(f, str) or not re.fullmatch(r"[a-z_]+", f) for f in formats)
            ):
                raise RawArchiveError("invalid raw storage location")
            _reject_symlink_path(root, label="external root")
            lake = self.meta_root.absolute().parent
            if root == lake or lake in root.parents or root in self.meta_root.absolute().parents:
                raise RawArchiveError("external raw storage must be outside the lake")
            seen.add(prefix.as_posix())
        return value["locations"]

    def register_external(
        self, dataset: str, source: str, root: Path | str, *, formats: tuple[str, ...] = ("pdf",)
    ) -> dict[str, Any]:
        """Register an existing external folder without moving or deleting bytes.

        Registration affects new payloads of the selected formats. All readers,
        including ones constructed before registration, resolve old logical
        paths through the same lake-local file. Existing local bytes remain
        readable while an operator copies and verifies the external archive.
        """
        from cnequity.file_lock import lake_mutation_lock

        if (
            not dataset
            or not source
            or _safe_component(dataset, "") != dataset
            or _safe_component(source, "") != source
        ):
            raise RawArchiveError("invalid external archive dataset or source")
        root = Path(root).expanduser().absolute()
        if ".." in root.parts:
            raise RawArchiveError("external raw storage root must not contain '..'")
        _reject_symlink_path(root, label="external root")
        if not root.is_dir():
            raise RawArchiveError(f"external raw storage is unavailable: {root}")
        location = {
            "prefix": f"raw/{dataset}/source={source}",
            "root": str(root),
            "formats": list(formats),
        }
        # Validate a proposed location with exactly the same rules as readers.
        if not formats or any(
            not isinstance(f, str) or not re.fullmatch(r"[a-z_]+", f) for f in formats
        ):
            raise RawArchiveError("invalid external payload formats")
        lake = self.meta_root.absolute().parent
        if root == lake or lake in root.parents or root in self.meta_root.absolute().parents:
            raise RawArchiveError("external raw storage must be outside the lake")
        with lake_mutation_lock(self.meta_root):
            locations = self.external_locations()
            previous = next((x for x in locations if x["prefix"] == location["prefix"]), None)
            if previous is not None and previous != location:
                raise RawArchiveError(
                    "external location already registered; migrate it before changing"
                )
            if previous is None:
                locations.append(location)
                write_json_atomic(
                    self.meta_root / "raw_storage.json",
                    {"schema_version": 1, "locations": locations},
                    indent=2,
                )
        return location

    def payload_file(self, relative: str | Path, *, writing_format: str | None = None) -> Path:
        """Resolve a logical payload path without weakening no-follow checks."""
        relative = Path(relative)
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise RawArchiveError(f"unsafe raw archive path: {relative}")
        local = _safe_archive_path(self.meta_root, relative, label="payload")
        for location in self.external_locations():
            prefix = Path(location["prefix"])
            if prefix not in relative.parents:
                continue
            root = Path(location["root"])
            external = _safe_archive_path(root, relative, label="external payload")
            selected = writing_format in location["formats"] if writing_format else False
            if writing_format and not selected:
                return local
            # Readers and immutable re-archives must agree while old local
            # payloads are being copied. Never validate an external copy and
            # then return a receipt whose preferred local bytes are corrupt.
            if _lstat(local) is not None:
                return local
            if not root.is_dir():
                raise RawArchiveError(f"external raw storage is unavailable: {root}")
            return external
        return local

    def export_files(self, dataset: str) -> dict[Path, Path]:
        """Materialize the logical raw tree, including external payloads.

        Portable snapshots deliberately omit the machine's location registry.
        A missing external volume must fail instead of producing a baseline
        whose source sidecars silently point to absent evidence.
        """
        if not dataset or _safe_component(dataset, "") != dataset:
            raise RawArchiveError("invalid raw archive dataset")
        result = {p.relative_to(self.meta_root): p for p in _files_no_follow(self.root / dataset)}
        for location in self.external_locations():
            prefix = Path(location["prefix"])
            if prefix.parts[1] != dataset:
                continue
            root = Path(location["root"])
            if not root.is_dir():
                raise RawArchiveError(f"external raw storage is unavailable: {root}")
            for path in _files_no_follow(root / prefix):
                relative = path.relative_to(root)
                if relative in result:
                    existing = result[relative]
                    if existing.stat().st_size != path.stat().st_size:
                        raise RawArchiveError(f"conflicting raw payload copies: {relative}")
                    with existing.open("rb") as a, path.open("rb") as b:

                        def checksum(stream):
                            hashed = hashlib.sha256()
                            for block in iter(lambda: stream.read(1024 * 1024), b""):
                                hashed.update(block)
                            return hashed.digest()

                        if checksum(a) != checksum(b):
                            raise RawArchiveError(f"conflicting raw payload copies: {relative}")
                else:
                    result[relative] = path
        for relative, path in list(result.items()):
            if relative.suffix != ".json":
                continue
            record = _read_json_object(path, label="metadata")
            if record.get("schema_version") == _ARCHIVE_SCHEMA_VERSION and record.get(
                "payload_path"
            ):
                if Path(record["payload_path"]) not in result:
                    raise RawArchiveError(f"missing raw archive payload: {record['payload_path']}")
        return result

    def archive(
        self,
        dataset: str,
        payload: Any,
        *,
        source: str,
        request_params: Mapping[str, Any] | None = None,
        captured_at: datetime | None = None,
        run_id: str | None = None,
        url: str | None = None,
        response_status: int | None = None,
        payload_format: str | None = None,
        http_metadata: Mapping[str, Any] | None = None,
        pagination: Mapping[str, Any] | None = None,
        observation_id: str | None = None,
        request_scope: str | None = None,
    ) -> RawPayloadRecord | None:
        """Persist one payload and a redacted sidecar; repeated hashes are no-op."""
        if not self.should_archive(dataset):
            return None
        self._validate_roots()
        raw, fmt = _payload_bytes(payload, payload_format)
        if self.max_payload_bytes is not None and len(raw) > int(self.max_payload_bytes):
            raise RawArchiveError(
                f"raw payload for {dataset} is {len(raw)} bytes, exceeds configured limit "
                f"{self.max_payload_bytes}"
            )
        digest = hashlib.sha256(raw).hexdigest()
        source_part = _safe_component(source, "unknown")
        dataset_part = _safe_component(dataset, "dataset")
        stamp = (captured_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
        day = stamp.date().isoformat()
        base = self.root / dataset_part / f"source={source_part}" / f"captured_date={day}"
        compression = self.compression
        # gzip -9 shrank archived report PDFs by about 1% at real CPU cost.
        # A copy already stored compressed keeps its form, so a replayed
        # observation still matches its existing sidecar.
        if (
            compression == "gzip"
            and fmt in _STORED_UNCOMPRESSED_FORMATS
            and _lstat(self.payload_file((base / f"{digest}.gz").relative_to(self.meta_root)))
            is None
        ):
            compression = "none"
        compressed = gzip.compress(raw, mtime=0) if compression == "gzip" else raw
        suffix = ".gz" if compression == "gzip" else ".bin"
        relative_payload = (base / f"{digest}{suffix}").relative_to(self.meta_root)
        payload_path = self.payload_file(relative_payload, writing_format=fmt)
        # The payload itself remains content addressed.  A caller that needs
        # to account for two observations of the same bytes (CNINFO page 1
        # and page 2 can be identical) may request a deterministic observation
        # sidecar.  Replaying the exact same observation still resolves to the
        # same path and is a no-op.
        observation_suffix = ""
        if observation_id is not None:
            observation_digest = hashlib.sha256(str(observation_id).encode("utf-8")).hexdigest()
            observation_suffix = f".{observation_digest}"
        metadata_path = base / f"{digest}{observation_suffix}.json"
        _safe_archive_path(
            self.root,
            metadata_path.relative_to(self.root),
            label="metadata",
        )
        _write_bytes_once(payload_path, compressed)
        # Do not trust an existing content-addressed file merely because its
        # path and byte length match.  Re-decode and hash it on every archive
        # hit, including the same-size replacement case.
        _verify_stored_payload(
            payload_path,
            compressed,
            digest=digest,
            compression=compression,
            payload_bytes=len(raw),
            compressed_bytes=len(compressed),
        )
        relative_payload = relative_payload.as_posix()
        relative_metadata = metadata_path.relative_to(self.meta_root).as_posix()
        record = RawPayloadRecord(
            dataset=str(dataset),
            source=str(source),
            captured_at=stamp.isoformat(),
            payload_sha256=digest,
            payload_bytes=len(raw),
            compressed_bytes=len(compressed),
            compression=compression,
            payload_format=fmt,
            payload_path=relative_payload,
            metadata_path=relative_metadata,
            request_params=sanitize_request_params(request_params),
            run_id=str(run_id) if run_id is not None else None,
            url=sanitize_url(url),
            response_status=int(response_status) if response_status is not None else None,
            http_metadata=sanitize_request_params(http_metadata),
            pagination=sanitize_request_params(pagination),
            observation_id=str(observation_id) if observation_id is not None else None,
            request_scope=(
                str(request_scope) if request_scope is not None else self._capture_scope
            ),
            capture_nonce=self._capture_nonce,
        )
        metadata_info = _lstat(metadata_path)
        if metadata_info is not None:
            try:
                existing = _read_json_object(metadata_path, label="metadata")
            except (OSError, ValueError, RawArchiveError) as exc:
                raise RawArchiveError(f"invalid existing raw metadata: {metadata_path}") from exc
            if existing.get("payload_sha256") != digest:
                raise RawArchiveError(f"raw metadata hash mismatch: {metadata_path}")
            for key, expected in {
                "dataset": str(dataset),
                "source": str(source),
                "payload_bytes": len(raw),
                "compressed_bytes": len(compressed),
                "compression": compression,
                "payload_format": fmt,
                "payload_path": relative_payload,
                "metadata_path": relative_metadata,
                "http_metadata": sanitize_request_params(http_metadata),
                "pagination": sanitize_request_params(pagination),
                "observation_id": str(observation_id) if observation_id is not None else None,
                "request_scope": (
                    str(request_scope) if request_scope is not None else self._capture_scope
                ),
            }.items():
                if existing.get(key) != expected:
                    raise RawArchiveError(f"raw metadata {key} mismatch: {metadata_path}")
        else:
            _reject_symlink_path(metadata_path, label="metadata")
            write_json_atomic(metadata_path, record.to_dict(), indent=2, ensure_ascii=False)
        if self._capture_owner is not None:
            record = _capture_record(
                self._capture_owner,
                record,
                run_id=self._capture_run_id,
                source=self._capture_source,
                request_scope=record.request_scope,
                capture_nonce=self._capture_nonce,
            )
        return record

    # Descriptive aliases make adapter integration easy without exposing the
    # archive's implementation details.
    write = archive
    archive_payload = archive

    def records(self, dataset: str | None = None) -> list[RawPayloadRecord]:
        self._validate_roots()
        if dataset:
            root = _safe_archive_path(
                self.root,
                Path(_safe_component(dataset, "dataset")),
                label="dataset",
            )
        else:
            root = self.root
        if _lstat(root) is None:
            return []
        out: list[RawPayloadRecord] = []
        for path in sorted(path for path in _files_no_follow(root) if path.suffix == ".json"):
            _reject_regular_file(path, label="metadata")
            try:
                payload = _read_json_object(path, label="metadata")
                if payload.get("schema_version") != _ARCHIVE_SCHEMA_VERSION:
                    continue
                payload.pop("schema_version", None)
                record = RawPayloadRecord(**payload)
                # Listing an archive is also an integrity-sensitive read. Do
                # not hand callers metadata for a payload that was replaced
                # with a same-size or otherwise malformed file.
                self.read(record)
                out.append(record)
            except (OSError, ValueError, TypeError) as exc:
                raise RawArchiveError(f"invalid raw archive metadata: {path}") from exc
        return out

    def record(self, metadata_path: str | Path) -> RawPayloadRecord:
        """Load and integrity-check one sidecar without enumerating a dataset.

        Publish receipts use this narrow lookup so validation cannot discover
        a different observation merely because it shares ``dataset`` and
        ``run_id`` with the current fetch.
        """
        candidate = Path(metadata_path)
        if candidate.is_absolute():
            try:
                relative = candidate.relative_to(self.meta_root)
            except ValueError as exc:
                raise RawArchiveError(f"unsafe raw metadata path: {metadata_path}") from exc
        else:
            relative = candidate
        if not relative.parts or relative.is_absolute() or ".." in relative.parts:
            raise RawArchiveError(f"unsafe raw metadata path: {metadata_path}")
        path = _safe_archive_path(self.meta_root, relative, label="metadata")
        _reject_regular_file(path, label="metadata")
        try:
            payload = _read_json_object(path, label="metadata")
            if payload.get("schema_version") != _ARCHIVE_SCHEMA_VERSION:
                raise RawArchiveError(f"unsupported raw metadata schema: {path}")
            payload.pop("schema_version", None)
            record = RawPayloadRecord(**payload)
        except (OSError, ValueError, TypeError) as exc:
            raise RawArchiveError(f"invalid raw archive metadata: {path}") from exc
        expected_metadata = path.relative_to(self.meta_root).as_posix()
        if record.metadata_path != expected_metadata:
            raise RawArchiveError(f"raw metadata path mismatch: {path}")
        self.read(record)
        return record

    def read(self, record: RawPayloadRecord | Mapping[str, Any] | str | Path) -> bytes:
        self._validate_roots()
        expected_payload_bytes: int | None = None
        expected_compressed_bytes: int | None = None
        metadata_relative: str | None = None
        if isinstance(record, RawPayloadRecord):
            relative = record.payload_path
            metadata_relative = record.metadata_path
            digest = record.payload_sha256
            compression = record.compression
            expected_payload_bytes = record.payload_bytes
            expected_compressed_bytes = record.compressed_bytes
        elif isinstance(record, Mapping):
            relative = str(record.get("payload_path", ""))
            if record.get("metadata_path"):
                metadata_relative = str(record["metadata_path"])
            digest = str(record.get("payload_sha256", ""))
            compression = str(record.get("compression", "gzip"))
            if record.get("payload_bytes") is not None:
                try:
                    expected_payload_bytes = int(record["payload_bytes"])
                except (TypeError, ValueError) as exc:
                    raise RawArchiveError("invalid raw payload_bytes metadata") from exc
            if record.get("compressed_bytes") is not None:
                try:
                    expected_compressed_bytes = int(record["compressed_bytes"])
                except (TypeError, ValueError) as exc:
                    raise RawArchiveError("invalid raw compressed_bytes metadata") from exc
        else:
            candidate = Path(record)
            if candidate.is_absolute():
                try:
                    relative = candidate.relative_to(self.meta_root).as_posix()
                except ValueError as exc:
                    raise RawArchiveError(f"unsafe raw archive path: {candidate}") from exc
            else:
                relative = candidate.as_posix()
            digest = ""
            compression = "gzip" if relative.endswith(".gz") else "none"
        if not relative or Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise RawArchiveError(f"unsafe raw archive path: {relative}")
        if metadata_relative:
            metadata_path_value = Path(metadata_relative)
            if metadata_path_value.is_absolute() or ".." in metadata_path_value.parts:
                raise RawArchiveError(f"unsafe raw metadata path: {metadata_relative}")
            metadata_path = _safe_archive_path(
                self.meta_root,
                metadata_path_value,
                label="metadata",
            )
            if _lstat(metadata_path) is not None:
                _reject_regular_file(metadata_path, label="metadata")
        path = self.payload_file(relative)
        try:
            return _verify_stored_payload(
                path,
                digest=digest or None,
                compression=compression,
                payload_bytes=expected_payload_bytes,
                compressed_bytes=expected_compressed_bytes,
            )
        except (FileNotFoundError, RawArchiveError) as exc:
            # A verified migration may retire the local copy between resolve
            # and open. Retry only if it now resolves to a different location;
            # corruption is never hidden by falling back to another copy.
            if isinstance(exc, RawArchiveError) and not isinstance(
                exc.__cause__, FileNotFoundError
            ):
                raise
            relocated = self.payload_file(relative)
            if relocated == path:
                raise
            return _verify_stored_payload(
                relocated,
                digest=digest or None,
                compression=compression,
                payload_bytes=expected_payload_bytes,
                compressed_bytes=expected_compressed_bytes,
            )

    def replay(
        self,
        record: RawPayloadRecord | Mapping[str, Any] | str | Path,
        parser: Callable[[bytes], Any],
    ) -> Any:
        """Run a parser against archived bytes without network access."""
        if not callable(parser):
            raise TypeError("parser must be callable")
        return parser(self.read(record))


def archive_response(
    archive: RawPayloadArchive,
    dataset: str,
    response: Any,
    *,
    source: str,
    request_params: Mapping[str, Any] | None = None,
    run_id: str | None = None,
    url: str | None = None,
    request_scope: str | None = None,
) -> RawPayloadRecord | None:
    """Archive an httpx-like response without retaining request headers."""
    if not archive.enabled:
        return None
    content = getattr(response, "content", None)
    if isinstance(content, bytearray):
        content = bytes(content)
    elif isinstance(content, memoryview):
        content = content.tobytes()
    if not isinstance(content, bytes):
        raise RawArchiveError(
            "HTTP response has no exact wire bytes; refusing to create a replayable archive"
        )
    return archive.archive(
        dataset,
        content,
        source=source,
        request_params=request_params,
        run_id=run_id,
        url=url or str(getattr(response, "url", "") or "") or None,
        response_status=getattr(response, "status_code", None),
        payload_format="bytes",
        request_scope=request_scope,
    )


__all__ = [
    "RawArchiveError",
    "RawPayloadArchive",
    "RawPayloadRecord",
    "archive_response",
    "capture_is_consumed",
    "capture_nonce",
    "capture_publish",
    "captured_records",
    "begin_capture",
    "sanitize_request_params",
    "sanitize_url",
]


# --------------------------------------------------------------------------
# Sealed evidence that a run's exact wire bytes are in the archive. It lives
# here, not with the staging writer, so an adapter that verifies its own
# capture does not have to import the step layer to do it.
# --------------------------------------------------------------------------

#: How a source-evidence repair stages what it verified: the step passes
#: ``steps.http_common.write_fetched``, so the adapter neither imports the step
#: layer nor decides how staging is written.
StageWrite = Callable[..., dict]

_RAW_ARCHIVE_EVIDENCE_SEAL = object()


@dataclass(frozen=True, slots=True, init=False)
class RawArchiveEvidence:
    """A sealed receipt for exact wire observations already in the archive.

    Page-oriented adapters archive before they return their normalized frame,
    so ``write_fetched`` cannot receive source bytes a second time.  The
    receipt is intentionally not a boolean compatibility flag: it carries the
    concrete sidecar/hash identities and is revalidated at the publish
    boundary before any staging write is allowed.
    """

    dataset: str
    run_id: str
    source: str
    request_scope: str
    capture_nonce: str
    record_keys: tuple[tuple[str, str], ...]
    observation_ids: tuple[str, ...]
    _seal: object = field(repr=False, compare=False)

    def __init__(
        self,
        dataset: str,
        run_id: str,
        source: str,
        request_scope: str,
        capture_nonce: str,
        record_keys: tuple[tuple[str, str], ...],
        observation_ids: tuple[str, ...],
        *,
        _seal: object,
    ) -> None:
        if _seal is not _RAW_ARCHIVE_EVIDENCE_SEAL:
            raise TypeError("RawArchiveEvidence must be created by archive verification")
        object.__setattr__(self, "dataset", str(dataset))
        object.__setattr__(self, "run_id", str(run_id))
        object.__setattr__(self, "source", str(source))
        object.__setattr__(self, "request_scope", str(request_scope))
        object.__setattr__(self, "capture_nonce", str(capture_nonce))
        object.__setattr__(self, "record_keys", tuple(record_keys))
        object.__setattr__(self, "observation_ids", tuple(str(item) for item in observation_ids))
        object.__setattr__(self, "_seal", _seal)

    @classmethod
    def _verified(
        cls,
        dataset: str,
        run_id: str,
        source: str,
        request_scope: str,
        capture_nonce: str,
        record_keys: tuple[tuple[str, str], ...],
        observation_ids: tuple[str, ...],
    ) -> RawArchiveEvidence:
        return cls(
            dataset,
            run_id,
            source,
            request_scope,
            capture_nonce,
            record_keys,
            observation_ids,
            _seal=_RAW_ARCHIVE_EVIDENCE_SEAL,
        )


def _raw_archive(config, dataset: str) -> RawPayloadArchive:
    """Construct the strict archive view used for receipt validation."""
    return RawPayloadArchive(
        config.meta_root,
        enabled=True,
        datasets=[dataset],
        compression=config.raw_archive_compression,
        max_payload_bytes=config.raw_archive_max_payload_bytes,
    )


def verify_raw_archive(
    config,
    dataset: str,
    run_id: str,
    *,
    source: str,
    request_scope: str,
    records: list[RawPayloadRecord] | tuple[RawPayloadRecord, ...] | None = None,
) -> RawArchiveEvidence:
    """Verify and seal exact wire evidence for one run/dataset.

    This is the only factory for :class:`RawArchiveEvidence`.  Listing the
    archive also integrity-checks payload bytes; the additional metadata check
    rejects an adapter that wrote a normalized or otherwise non-wire payload.
    """
    if not run_id:
        raise RawArchiveError(f"{dataset}: raw archive evidence requires a non-empty run_id")
    if not source or not request_scope:
        raise RawArchiveError(f"{dataset}: raw archive evidence requires source and request_scope")
    nonce = capture_nonce(
        config,
        dataset,
        str(run_id),
        source=source,
        request_scope=request_scope,
    )
    if not nonce:
        raise RawArchiveError(
            f"{dataset}: archive capture is not active for source {source!r}, "
            f"scope {request_scope!r}"
        )
    if capture_is_consumed(
        config,
        dataset,
        str(run_id),
        source=source,
        request_scope=request_scope,
        nonce=nonce,
    ):
        raise RawArchiveError(f"{dataset}: archive capture was already consumed")
    archive = _raw_archive(config, dataset)
    current = captured_records(
        config,
        dataset,
        str(run_id),
        source=source,
        request_scope=request_scope,
    )
    if records is None:
        selected = current
    else:
        selected = list(records)
        current_keys = {
            (item.metadata_path, item.payload_sha256, item.observation_id) for item in current
        }
        if any(
            item.capture_nonce != nonce
            or (item.metadata_path, item.payload_sha256, item.observation_id) not in current_keys
            for item in selected
        ):
            raise RawArchiveError(
                f"{dataset}: supplied archive records are not from the active capture"
            )
    if not selected:
        raise RawArchiveError(
            f"{dataset}: archive is required but no exact wire observation exists "
            f"for run_id {run_id!r}, source {source!r}, scope {request_scope!r}"
        )
    keys: list[tuple[str, str]] = []
    observation_ids: list[str] = []
    for record in selected:
        if (
            record.dataset != str(dataset)
            or record.run_id != str(run_id)
            or record.source != str(source)
            or record.request_scope != str(request_scope)
            or record.capture_nonce != nonce
            or not record.observation_id
        ):
            raise RawArchiveError(
                f"{dataset}: archive observation is outside the current source/scope"
            )
        if (
            not isinstance(record.http_metadata, dict)
            or record.http_metadata.get("wire_exact") is not True
        ):
            raise RawArchiveError(
                f"{dataset}: archive observation {record.metadata_path!r} "
                "does not carry verified exact wire evidence"
            )
        # ``records`` already reads each payload, but keep this explicit so a
        # receipt is tied to the exact payload it names rather than just the
        # existence of a JSON sidecar.
        archive.record(record.metadata_path)
        keys.append((record.metadata_path, record.payload_sha256))
        observation_ids.append(str(record.observation_id))
    return RawArchiveEvidence._verified(
        dataset,
        str(run_id),
        str(source),
        str(request_scope),
        str(nonce),
        tuple(keys),
        tuple(observation_ids),
    )
