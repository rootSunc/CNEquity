"""Small JSON records under a directory of ``meta/``, one file per name, each under its lock.

The default directory is ``meta/state/``, which an older lake still has: the
``attempts-ledger`` migration reads its coverage fields from there once
(:meth:`StateStore.get_missing_dates`, :meth:`StateStore.get_outstanding_keys`).
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import IO

from cnequity.file_lock import exclusive_lock
from cnequity.storage.atomic import replace_with_retry


class StateStore:
    """JSON records named by key in ``meta/<area>/`` (``meta/state/`` by default)."""

    def __init__(self, meta_root: Path, area: Path | str = "state"):
        self.root = Path(meta_root) / area
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, dataset: str) -> Path:
        return self.root / f"{dataset}.json"

    def get_payload(self, dataset: str) -> dict:
        """Return a copy of the complete state payload for *dataset*.

        Watermarks are only one part of dataset identity.  Consumers that cache
        query results also need the committed dataset revision so a repair to an
        old partition invalidates their cache even when the maximum covered date
        does not move.
        """
        with self._dataset_lock(dataset):
            return dict(self._read_payload(self._path(dataset)))

    def get_revision(self, dataset: str) -> int | None:
        """Return the latest committed monotonic revision, when present."""
        value = self.get_payload(dataset).get("revision")
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"state field {dataset}.revision must be a positive integer")
        return value

    def _lock_path(self, dataset: str) -> Path:
        return self.root / f"{dataset}.lock"

    def _dataset_lock(self, dataset: str) -> AbstractContextManager[IO]:
        return exclusive_lock(self._lock_path(dataset))

    @contextmanager
    def transaction(self, dataset: str) -> Iterator[dict]:
        """Yield the dataset's mutable state payload under its exclusive lock.

        The yielded dict is written back atomically when the block exits
        normally; if the block raises, nothing is written. This is the
        supported way for another store to read the current state, do work
        that must happen inside the lock, and advance the payload in one step.
        Callers set their own timestamp fields.
        """
        path = self._path(dataset)
        with self._dataset_lock(dataset):
            payload = self._read_payload(path)
            yield payload
            self._write_payload(path, payload)

    def _read_payload(self, path: Path) -> dict:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _write_payload(self, path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            dir=path.parent,
            prefix=f".{path.stem}-",
            suffix=".tmp",
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
                handle.flush()
                os.fsync(handle.fileno())
            replace_with_retry(tmp_path, path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def get_date(self, dataset: str, field: str = "last_success_trade_date") -> date | None:
        with self._dataset_lock(dataset):
            value = self._read_payload(self._path(dataset)).get(field)
        if not value:
            return None
        return date.fromisoformat(str(value))

    def set_date(
        self,
        dataset: str,
        value: date,
        *,
        field: str = "last_success_trade_date",
    ) -> None:
        path = self._path(dataset)
        with self._dataset_lock(dataset):
            payload = self._read_payload(path)
            payload[field] = value.isoformat()
            payload["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._write_payload(path, payload)

    def update_max_date(
        self,
        dataset: str,
        candidate: date,
        *,
        field: str = "last_success_trade_date",
    ) -> None:
        path = self._path(dataset)
        with self._dataset_lock(dataset):
            payload = self._read_payload(path)
            current_raw = payload.get(field)
            current = date.fromisoformat(str(current_raw)) if current_raw else None
            if current is None or candidate > current:
                payload[field] = candidate.isoformat()
                payload["updated_at"] = datetime.now(timezone.utc).isoformat()
                self._write_payload(path, payload)

    def clear_date(
        self,
        dataset: str,
        *,
        field: str = "last_success_trade_date",
    ) -> None:
        """Remove a date watermark while preserving other dataset state."""
        path = self._path(dataset)
        with self._dataset_lock(dataset):
            payload = self._read_payload(path)
            if field not in payload:
                return
            payload.pop(field, None)
            payload["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._write_payload(path, payload)

    def get_string_set(self, dataset: str, field: str) -> set[str]:
        """Read a set-like string field from a dataset state payload."""
        with self._dataset_lock(dataset):
            value = self._read_payload(self._path(dataset)).get(field)
        if value is None:
            return set()
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ValueError(f"state field {dataset}.{field} must be a list of strings")
        return set(value)

    def set_string_set(self, dataset: str, field: str, values: Iterable[str]) -> None:
        """Atomically replace a set-like string field in a dataset state payload."""
        normalized = sorted(set(values))
        path = self._path(dataset)
        with self._dataset_lock(dataset):
            payload = self._read_payload(path)
            if normalized:
                payload[field] = normalized
            else:
                payload.pop(field, None)
            payload["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._write_payload(path, payload)

    def get_missing_dates(self, dataset: str) -> set[date]:
        """An older lake's failed by-date requests (``missing_ranges``)."""
        rows = self.get_payload(dataset).get("missing_ranges", [])
        if not isinstance(rows, list):
            raise ValueError(f"state field {dataset}.missing_ranges must be a list")
        days: set[date] = set()
        for row in rows:
            start = date.fromisoformat(str(row["start"]))
            end = date.fromisoformat(str(row.get("end") or row["start"]))
            if end < start:
                start, end = end, start
            for offset in range((end - start).days + 1):
                days.add(start + timedelta(days=offset))
        return days

    # Read once, from an older lake, by the ``attempts-ledger`` migration.
    def get_outstanding_keys(self, dataset: str) -> list[dict]:
        """An older lake's keys a tolerated gap left unfilled (``outstanding_keys``)."""
        with self._dataset_lock(dataset):
            payload = self._read_payload(self._path(dataset))
        rows = payload.get("outstanding_keys")
        return list(rows) if isinstance(rows, list) else []


def _utc_now(value: datetime | None = None) -> datetime:
    """Return an aware UTC clock value suitable for state comparisons."""
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        raise ValueError("negative evidence clock must be timezone-aware")
    return value.astimezone(timezone.utc)


def _parse_utc(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)
