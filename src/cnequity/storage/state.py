"""Per-dataset incremental watermarks under meta/state/."""

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


class StateStore:
    """Tracks last-success coverage per dataset (e.g. last trade_date)."""

    def __init__(self, meta_root: Path):
        self.root = meta_root / "state"
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
            os.replace(tmp_path, path)
        except Exception:
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

    # ------------------------------------------------------------------
    # Outstanding keys: what a tolerated gap still owes the lake.
    #
    # A sweep that loses a fraction of a percent no longer refuses the whole
    # checkpoint, which means the hole is now *inside* a published revision
    # rather than in front of a failed run. Nothing else would remember it: the
    # watermark has moved past those sessions, so no incremental run will ask
    # for them again. This is the ledger that does, and `cne backfill
    # <dataset> --outstanding` is what works it off.
    # ------------------------------------------------------------------

    def get_outstanding_keys(self, dataset: str) -> list[dict]:
        """Keys a tolerated gap left unfilled, oldest request first."""
        with self._dataset_lock(dataset):
            payload = self._read_payload(self._path(dataset))
        rows = payload.get("outstanding_keys")
        return list(rows) if isinstance(rows, list) else []

    def record_outstanding_keys(
        self,
        dataset: str,
        pairs: Iterable[tuple[str, date]],
        *,
        run_id: str,
        reason: str,
        now: datetime | None = None,
    ) -> int:
        """Merge (symbol, trade_date) pairs into the ledger. Returns the total.

        Merged by key rather than appended, so re-running a sweep that fails
        the same way twice does not grow the ledger without bound.
        """
        stamp = _utc_now(now).isoformat()
        incoming = {
            (symbol, day.isoformat() if hasattr(day, "isoformat") else str(day))
            for symbol, day in pairs
        }
        if not incoming:
            return len(self.get_outstanding_keys(dataset))
        path = self._path(dataset)
        with self._dataset_lock(dataset):
            payload = self._read_payload(path)
            existing = payload.get("outstanding_keys")
            merged: dict[tuple[str, str], dict] = {}
            if isinstance(existing, list):
                for row in existing:
                    if isinstance(row, dict) and row.get("symbol") and row.get("trade_date"):
                        merged[(row["symbol"], row["trade_date"])] = row
            for symbol, day in sorted(incoming):
                merged.setdefault(
                    (symbol, day),
                    {
                        "symbol": symbol,
                        "trade_date": day,
                        "reason": reason,
                        "run_id": run_id,
                        "recorded_at": stamp,
                    },
                )
            payload["outstanding_keys"] = [merged[k] for k in sorted(merged)]
            payload["updated_at"] = stamp
            self._write_payload(path, payload)
            return len(payload["outstanding_keys"])

    def clear_outstanding_keys(
        self, dataset: str, pairs: Iterable[tuple[str, date]] | None = None
    ) -> int:
        """Drop filled keys. ``pairs=None`` clears the ledger. Returns what is left."""
        path = self._path(dataset)
        with self._dataset_lock(dataset):
            payload = self._read_payload(path)
            if pairs is None:
                payload.pop("outstanding_keys", None)
                remaining = 0
            else:
                done = {
                    (symbol, day.isoformat() if hasattr(day, "isoformat") else str(day))
                    for symbol, day in pairs
                }
                rows = payload.get("outstanding_keys")
                kept = [
                    row
                    for row in (rows if isinstance(rows, list) else [])
                    if (row.get("symbol"), row.get("trade_date")) not in done
                ]
                if kept:
                    payload["outstanding_keys"] = kept
                else:
                    payload.pop("outstanding_keys", None)
                remaining = len(kept)
            payload["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._write_payload(path, payload)
            return remaining

    def get_negative_evidence(
        self,
        dataset: str,
        *,
        identity: dict | None = None,
        now: datetime | None = None,
    ) -> list[dict]:
        """Return live, identity-matching negative evidence for *dataset*.

        Negative evidence is deliberately kept in the dataset state file
        rather than in an untracked process cache.  Each record is a small
        symbol/window claim (for example, ``Sina returned an empty response``)
        with an expiry and the instrument/status revision it was based on.
        Expired records and records based on a newer catalog revision are
        removed while reading, so a newly-listed or otherwise changed
        instrument is never hidden by an old absence claim.

        ``identity=None`` is useful to inspect the raw live cache.  Callers
        that use the evidence for routing should always provide the current
        identity so catalog changes invalidate old claims.
        """
        clock = _utc_now(now)
        path = self._path(dataset)
        with self._dataset_lock(dataset):
            payload = self._read_payload(path)
            raw = payload.get("negative_evidence", [])
            if not isinstance(raw, list):
                raw = []
            retained: list[dict] = []
            changed = not isinstance(payload.get("negative_evidence"), list)
            for item in raw:
                if not isinstance(item, dict):
                    changed = True
                    continue
                expires_at = _parse_utc(item.get("expires_at"))
                if expires_at is None or expires_at <= clock:
                    changed = True
                    continue
                if identity is not None and item.get("identity") != identity:
                    changed = True
                    continue
                retained.append(dict(item))
            if changed or len(retained) != len(raw):
                if retained:
                    payload["negative_evidence"] = retained
                else:
                    payload.pop("negative_evidence", None)
                payload["updated_at"] = clock.isoformat()
                self._write_payload(path, payload)
            return retained

    def record_negative_evidence(
        self,
        dataset: str,
        entries: Iterable[dict],
        *,
        ttl_days: int,
        identity: dict | None = None,
        now: datetime | None = None,
    ) -> None:
        """Persist bounded negative observations for a dataset.

        Records are merged by symbol, requested window, reason and source.
        A zero/negative TTL disables persistence, which is useful for tests or
        deployments that prefer every missing key to be retried immediately.
        The method accepts plain dictionaries to keep the state payload
        forward-compatible with newer evidence fields.
        """
        if ttl_days <= 0:
            return
        clock = _utc_now(now)
        expires = clock + timedelta(days=ttl_days)
        normalized: list[dict] = []
        for raw in entries:
            if not isinstance(raw, dict):
                continue
            symbol = str(raw.get("symbol", "")).strip().upper()
            start = str(raw.get("window_start", raw.get("start", ""))).strip()
            end = str(raw.get("window_end", raw.get("end", ""))).strip()
            if not symbol or not start or not end:
                continue
            item = dict(raw)
            item["symbol"] = symbol
            item["window_start"] = start
            item["window_end"] = end
            item["expires_at"] = expires.isoformat()
            if identity is not None:
                item["identity"] = dict(identity)
            elif "identity" in item and not isinstance(item["identity"], dict):
                item.pop("identity", None)
            normalized.append(item)
        if not normalized:
            return

        path = self._path(dataset)
        with self._dataset_lock(dataset):
            payload = self._read_payload(path)
            raw_existing = payload.get("negative_evidence", [])
            existing = [item for item in raw_existing if isinstance(item, dict)]
            # Purge stale records even when no caller happened to read the
            # cache first. This bounds state-file growth on long-lived lakes.
            live: list[dict] = []
            for item in existing:
                expires_at = _parse_utc(item.get("expires_at"))
                if expires_at is None or expires_at <= clock:
                    continue
                if identity is not None and item.get("identity") != identity:
                    # A catalog revision invalidates the old claim. Drop it
                    # at write time as well as read time so state does not
                    # accumulate one dead copy per instruments revision.
                    continue
                live.append(dict(item))
            merged: dict[tuple[str, str, str, str, str], dict] = {}
            for item in live + normalized:
                key = (
                    str(item.get("symbol", "")),
                    str(item.get("window_start", "")),
                    str(item.get("window_end", "")),
                    str(item.get("reason", "")),
                    str(item.get("source", "")),
                )
                merged[key] = item
            values = sorted(
                merged.values(),
                key=lambda item: (
                    str(item.get("symbol", "")),
                    str(item.get("window_start", "")),
                    str(item.get("window_end", "")),
                    str(item.get("reason", "")),
                    str(item.get("source", "")),
                ),
            )
            payload["negative_evidence"] = values
            payload["updated_at"] = clock.isoformat()
            self._write_payload(path, payload)

    def clear_negative_evidence(self, dataset: str) -> None:
        """Remove all negative evidence for *dataset* while preserving state."""
        path = self._path(dataset)
        with self._dataset_lock(dataset):
            payload = self._read_payload(path)
            if "negative_evidence" not in payload:
                return
            payload.pop("negative_evidence", None)
            payload["updated_at"] = _utc_now().isoformat()
            self._write_payload(path, payload)


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
