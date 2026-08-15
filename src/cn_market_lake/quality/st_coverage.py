"""Versioned completion evidence for historical ST backfills.

Rows in ``trading_status`` answer a market question (ST, normal, suspended).
They cannot, by themselves, prove that every symbol in a requested scope was
checked.  This module keeps that operational proof separate from the facts:
an exact, versioned checkpoint while work is in progress and an immutable
coverage receipt once the whole scope is resolved.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.domain.symbols import is_all_a_symbol, parse_symbol

ST_EVIDENCE_VERSION = 2
ST_COVERAGE_CLAIM = "historical_st_evidence"


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.stem}-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _canonical_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def symbol_scope_hash(symbols: list[str]) -> str:
    return _canonical_hash(sorted(set(symbols)))


def current_st_universe(config: Config) -> list[str]:
    """All-A instruments whose price history makes ST evidence relevant.

    This is deliberately disk-only.  A coverage check must never make a
    provider call merely to decide whether an existing receipt is trustworthy.
    """
    path = config.curated_root / "instruments" / "part-merged.parquet"
    if not path.exists():
        return []
    frame = pl.read_parquet(path, columns=["symbol"])
    symbols: list[str] = []
    for raw in frame["symbol"].to_list():
        try:
            parsed = parse_symbol(str(raw))
        except ValueError:
            continue
        if is_all_a_symbol(parsed.code, parsed.exchange):
            symbols.append(str(raw))
    bars_root = config.curated_root / "daily_bars"
    bar_files = list(bars_root.rglob("*.parquet")) if bars_root.exists() else []
    bars = (
        set(
            pl.scan_parquet([str(path) for path in bar_files])
            .select("symbol")
            .unique()
            .collect()["symbol"]
            .to_list()
        )
        if bar_files
        else set()
    )
    if bars:
        symbols = [symbol for symbol in symbols if symbol in bars]
    return sorted(set(symbols))


def build_st_scope(
    symbols: list[str],
    start: date,
    end: date,
    *,
    universe: str,
) -> dict[str, Any]:
    if start > end:
        raise ValueError(f"ST evidence window is inverted: {start} > {end}")
    resolved = sorted(set(symbols))
    identity = {
        "evidence_version": ST_EVIDENCE_VERSION,
        "source": "baostock",
        "universe": universe,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "symbols_sha256": symbol_scope_hash(resolved),
    }
    return {
        **identity,
        "scope_id": _canonical_hash(identity),
        "expected_symbols": resolved,
        "expected_symbols_count": len(resolved),
    }


def st_checkpoint_path(config: Config, scope_id: str) -> Path:
    return (
        config.meta_root
        / "state"
        / ST_COVERAGE_CLAIM
        / f"v{ST_EVIDENCE_VERSION}"
        / f"{scope_id}.json"
    )


def load_st_checkpoint(config: Config, scope: dict[str, Any]) -> dict[str, Any]:
    """Load only an exact v2 scope; legacy sparse-ST markers are invalid.

    Version 1 marked never-ST symbols complete without persisting their normal
    rows. Reusing it would claim evidence that does not exist, so migration is
    intentionally a clean resweep rather than a metadata rewrite.
    """
    path = st_checkpoint_path(config, str(scope["scope_id"]))
    if not path.exists():
        return {
            "schema_version": 1,
            "claim": ST_COVERAGE_CLAIM,
            "scope": scope,
            "status": "pending",
            "completed_symbols": [],
            "unresolved_symbols": [],
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("scope") != scope or payload.get("claim") != ST_COVERAGE_CLAIM:
        raise RuntimeError(f"ST checkpoint identity mismatch: {path}")
    return payload


def write_st_checkpoint(config: Config, payload: dict[str, Any]) -> Path:
    payload = dict(payload)
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    path = st_checkpoint_path(config, str(payload["scope"]["scope_id"]))
    _atomic_json(path, payload)
    return path


def _st_row_counts(
    config: Config,
    scope: dict[str, Any],
    symbols: set[str],
    *,
    staging_run_id: str | None = None,
) -> dict[str, int]:
    """Count persisted Baostock facts in curated plus one resumable run."""
    if not symbols:
        return {}
    files = list((config.curated_root / "trading_status").rglob("*.parquet"))
    if staging_run_id:
        from cn_market_lake.storage import StagingWriter

        files.extend(
            StagingWriter(config.staging_root).list_run_files("trading_status", staging_run_id)
        )
    if not files:
        return {}
    frame = pl.scan_parquet([str(path) for path in files], missing_columns="insert")
    schema = frame.collect_schema().names()
    frame = frame.filter(
        pl.col("symbol").is_in(sorted(symbols)),
        pl.col("trade_date").is_between(
            date.fromisoformat(scope["start"]),
            date.fromisoformat(scope["end"]),
        ),
    )
    if "source" in schema:
        frame = frame.filter(pl.col("source") == "baostock")
    counts = frame.group_by("symbol").len().collect()
    return {row["symbol"]: int(row["len"]) for row in counts.iter_rows(named=True)}


def reusable_st_checkpoint_symbols(
    config: Config,
    checkpoint: dict[str, Any],
    run_id: str,
) -> set[str]:
    """Symbols whose checkpoint facts still exist after a crash/restart.

    Zero-row source responses are durable operational evidence on their own.
    Positive row counts must still be visible either in curated storage or in
    staging for the same run being resumed.
    """
    completed = set(checkpoint.get("completed_symbols", []))
    expected_counts = checkpoint.get("evidence_rows_by_symbol") or {}
    persisted = _st_row_counts(
        config,
        checkpoint["scope"],
        completed,
        staging_run_id=run_id,
    )
    return {
        symbol
        for symbol in completed
        if symbol in expected_counts
        and (
            int(expected_counts[symbol]) == 0
            or persisted.get(symbol, 0) >= int(expected_counts[symbol])
        )
    }


def _receipt_path(config: Config, scope_id: str) -> Path:
    return config.meta_root / "quality" / "coverage" / ST_COVERAGE_CLAIM / f"{scope_id}.json"


def publish_st_coverage_receipt(config: Config, checkpoint: dict[str, Any]) -> Path:
    scope = checkpoint["scope"]
    expected = set(scope["expected_symbols"])
    completed = set(checkpoint.get("completed_symbols", []))
    unresolved = set(checkpoint.get("unresolved_symbols", []))
    if completed != expected or unresolved:
        raise ValueError("cannot publish incomplete ST coverage evidence")
    expected_counts = checkpoint.get("evidence_rows_by_symbol") or {}
    if set(expected_counts) != expected:
        raise ValueError("cannot publish ST coverage without per-symbol row evidence")
    persisted = _st_row_counts(config, scope, expected)
    missing = [
        symbol
        for symbol in expected
        if int(expected_counts[symbol]) > 0
        and persisted.get(symbol, 0) < int(expected_counts[symbol])
    ]
    if missing:
        raise ValueError(
            f"cannot publish ST coverage before {len(missing)} symbol(s) reach curated storage"
        )
    receipt = {
        "schema_version": 1,
        "claim": ST_COVERAGE_CLAIM,
        "status": "complete",
        "scope": {key: value for key, value in scope.items() if key != "expected_symbols"},
        "completed_symbols": sorted(completed),
        "completed_symbols_count": len(completed),
        "completed_symbols_sha256": symbol_scope_hash(sorted(completed)),
        "evidence_rows": sum(int(value) for value in expected_counts.values()),
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    path = _receipt_path(config, str(scope["scope_id"]))
    _atomic_json(path, receipt)
    return path


def publish_st_receipts_for_compacted_run(config: Config, run_id: str) -> list[Path]:
    """Publish completed scopes only after this run's staging was compacted."""
    root = config.meta_root / "state" / ST_COVERAGE_CLAIM / f"v{ST_EVIDENCE_VERSION}"
    if not root.exists():
        return []
    published: list[Path] = []
    for path in root.glob("*.json"):
        try:
            checkpoint = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if checkpoint.get("status") != "complete":
            continue
        if checkpoint.get("completion_run_id") != run_id:
            continue
        published.append(publish_st_coverage_receipt(config, checkpoint))
    return published


def _receipts(config: Config) -> list[dict[str, Any]]:
    root = config.meta_root / "quality" / "coverage" / ST_COVERAGE_CLAIM
    if not root.exists():
        return []
    out: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if payload.get("claim") == ST_COVERAGE_CLAIM:
            out.append(payload)
    return out


def st_evidence_coverage_report(
    config: Config,
    start: date | None = None,
    end: date | None = None,
) -> dict[str, Any]:
    """Return whether a complete, current all-A receipt covers the window."""
    symbols = current_st_universe(config)
    candidates: list[dict[str, Any]] = []
    for receipt in _receipts(config):
        scope = receipt.get("scope") or {}
        if receipt.get("status") != "complete":
            continue
        if scope.get("evidence_version") != ST_EVIDENCE_VERSION:
            continue
        if scope.get("source") != "baostock" or scope.get("universe") != "all_a":
            continue
        covered_symbols = set(receipt.get("completed_symbols", []))
        if not symbols or not set(symbols) <= covered_symbols:
            continue
        scope_start = date.fromisoformat(scope["start"])
        scope_end = date.fromisoformat(scope["end"])
        if start is not None and scope_start > start:
            continue
        if end is not None and scope_end < end:
            continue
        candidates.append(receipt)

    best = min(
        candidates,
        key=lambda item: (
            date.fromisoformat(item["scope"]["start"]),
            -date.fromisoformat(item["scope"]["end"]).toordinal(),
        ),
        default=None,
    )
    if best is None:
        return {
            "verified": False,
            "claim": ST_COVERAGE_CLAIM,
            "evidence_version": ST_EVIDENCE_VERSION,
            "requested_start": start.isoformat() if start else None,
            "requested_end": end.isoformat() if end else None,
            "current_symbols": len(symbols),
            "reason": "no_current_universe" if not symbols else "no_matching_complete_receipt",
        }
    scope = best["scope"]
    return {
        "verified": True,
        "claim": ST_COVERAGE_CLAIM,
        "evidence_version": ST_EVIDENCE_VERSION,
        "requested_start": start.isoformat() if start else None,
        "requested_end": end.isoformat() if end else None,
        "coverage_start": scope["start"],
        "coverage_end": scope["end"],
        "current_symbols": len(symbols),
        "scope_id": scope["scope_id"],
    }
