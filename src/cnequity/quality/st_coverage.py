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
import logging
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from cnequity.config import Config
from cnequity.domain.symbols import is_all_a_symbol, is_cdr_symbol, parse_symbol
from cnequity.query.canonical import dedupe_by_primary_key, dedupe_lazy_by_primary_key
from cnequity.query.parquet_scan import (
    collect_parquet_root,
    scan_parquet_files,
    scan_parquet_root,
)

ST_EVIDENCE_VERSION = 2
ST_COVERAGE_CLAIM = "historical_st_evidence"
# Baostock's k-data ``isST`` history does not contain North Exchange (BJ)
# securities. Keep this explicit instead of retrying 580 symbols forever and
# then presenting a partial receipt as if it covered the full all-A universe.
ST_EVIDENCE_UNSUPPORTED_EXCHANGES = frozenset({"BJ"})
ST_EVIDENCE_COMPATIBLE_UNIVERSES = frozenset({"all_a", "all_a_sz", "all_a_sh_sz"})
_RECEIPT_INTEGRITY_CACHE_LIMIT = 64
_receipt_integrity_cache: dict[tuple[str, str, str, str], bool] = {}

logger = logging.getLogger(__name__)


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


def _trading_status_fingerprint(config: Config) -> str:
    """Fingerprint Parquet layout metadata for receipt-cache invalidation."""
    root = config.curated_root / "trading_status"
    if not root.exists():
        return "missing"
    entries: list[tuple[str, int, int]] = []
    for path in sorted(root.rglob("*.parquet")):
        try:
            stat = path.stat()
        except OSError:
            continue
        entries.append(
            (
                path.relative_to(root).as_posix(),
                int(stat.st_size),
                int(stat.st_mtime_ns),
            )
        )
    return _canonical_hash(entries)


def current_st_universe(
    config: Config,
    *,
    start: date | None = None,
    end: date | None = None,
    symbols: list[str] | None = None,
    universe: str = "all_a",
) -> list[str]:
    """All-A instruments whose price history makes ST evidence relevant.

    This is deliberately disk-only.  A coverage check must never make a
    provider call merely to decide whether an existing receipt is trustworthy.
    """
    if universe not in ST_EVIDENCE_COMPATIBLE_UNIVERSES:
        raise ValueError(
            f"unsupported ST evidence universe: {universe!r} "
            f"(supported: {', '.join(sorted(ST_EVIDENCE_COMPATIBLE_UNIVERSES))})"
        )
    requested_symbols = set(symbols) if symbols is not None else None
    root = config.curated_root / "instruments"
    if not root.exists():
        return []
    try:
        frame = dedupe_by_primary_key(
            collect_parquet_root(root, hive=False),
            "instruments",
        )
    except (FileNotFoundError, OSError, pl.exceptions.PolarsError, ValueError) as exc:
        # A partial universe is unsafe here: it can make an old receipt appear
        # to cover every currently visible name. Keep the claim unverified
        # until the structural audit repairs the unreadable input.
        logger.warning("ST coverage: instruments are not readable: %s", exc)
        return []
    if "symbol" not in frame.columns:
        return []
    resolved_symbols: list[str] = []
    for raw in frame["symbol"].to_list():
        try:
            parsed = parse_symbol(str(raw))
        except ValueError:
            continue
        # ``is_all_a_symbol`` is intentionally a broad code-space predicate
        # used by source discovery and therefore still admits SH 689xxx CDRs.
        # The research ``all_a`` universe excludes CDRs; ST evidence must use
        # the same universe or it creates an unnecessary scope member that can
        # never contribute to stock-universe queries.
        if is_all_a_symbol(parsed.code, parsed.exchange) and not is_cdr_symbol(
            parsed.code, parsed.exchange
        ):
            resolved_symbols.append(str(raw))
    if requested_symbols is not None:
        resolved_symbols = [symbol for symbol in resolved_symbols if symbol in requested_symbols]
    if universe in {"all_a_sh_sz", "all_a_sz"}:
        resolved_symbols = [
            symbol for symbol in resolved_symbols if parse_symbol(symbol).exchange in {"SH", "SZ"}
        ]

    bars_root = config.curated_root / "daily_bars"
    if bars_root.exists() and any(bars_root.rglob("*.parquet")):
        try:
            # Let the partition-aware scanner prune the requested bar window
            # and build one schema-aware lazy plan. The previous per-file
            # concat constructed thousands of independent scans for a
            # multi-decade daily lake before collecting only unique symbols.
            bar_scan = scan_parquet_root(
                bars_root,
                partition_col="trade_date",
                start=start,
                end=end,
                traded_only=True,
            )
            bars = set(
                bar_scan.select("symbol")
                .unique()
                .collect(engine="streaming")
                .get_column("symbol")
                .to_list()
            )
        except (FileNotFoundError, OSError, pl.exceptions.PolarsError, ValueError) as exc:
            logger.warning("ST coverage: daily_bars scan is not readable: %s", exc)
            return []
        resolved_symbols = [symbol for symbol in resolved_symbols if symbol in bars]
    return sorted(set(resolved_symbols))


def _tushare_st_enabled(config: Config | None) -> bool:
    return bool(
        config is not None and config.sources.get("tushare", False) and config.tushare_token
    )


def _tushare_floor_symbols(
    config: Config | None,
    symbols: list[str],
    start: date | None,
    end: date | None,
) -> set[str]:
    if not _tushare_st_enabled(config) or start is None or end is None:
        return set()
    from cnequity.adapters.tushare.st_history import TUSHARE_ST_HISTORY_FLOOR

    if start >= TUSHARE_ST_HISTORY_FLOOR:
        return set()
    first_dates = _first_traded_dates(config, set(symbols), start, end)
    return {
        symbol
        for symbol, first_date in first_dates.items()
        if first_date < TUSHARE_ST_HISTORY_FLOOR
    }


def st_evidence_unsupported_symbols(
    symbols: list[str],
    *,
    config: Config | None = None,
    start: date | None = None,
    end: date | None = None,
) -> list[str]:
    """Symbols the configured historical ST source cannot query.

    This is intentionally source-specific.  The symbols remain valid all-A
    securities and must not be deleted from instruments or daily bars; they
    are reported as an explicit research limitation until an independent BJ
    historical ST source is configured.  Tushare, when explicitly enabled with
    a token, covers BJ from its documented 2016 floor (``bak_basic`` name
    evidence for 2016 and ``stock_st`` from 2017); older persisted BJ bars
    remain unsupported instead of being silently certified.
    """
    unsupported: list[str] = []
    tushare_enabled = _tushare_st_enabled(config)
    floor_symbols = _tushare_floor_symbols(config, symbols, start, end)
    for symbol in symbols:
        try:
            parsed = parse_symbol(symbol)
            if parsed.exchange in ST_EVIDENCE_UNSUPPORTED_EXCHANGES and (
                not tushare_enabled or symbol in floor_symbols
            ):
                unsupported.append(symbol)
        except ValueError:
            continue
    return sorted(set(unsupported))


def st_evidence_supported_symbols(
    symbols: list[str],
    *,
    config: Config | None = None,
    start: date | None = None,
    end: date | None = None,
) -> list[str]:
    """Symbols that Baostock can serve for the historical ST sweep."""
    unsupported = set(st_evidence_unsupported_symbols(symbols, config=config, start=start, end=end))
    return sorted(set(symbols) - unsupported)


def st_evidence_source_symbols(
    symbols: list[str],
    source: str,
    *,
    config: Config | None = None,
    start: date | None = None,
    end: date | None = None,
) -> list[str]:
    """Return the portion of a scope owned by one historical ST source."""
    if source == "baostock":
        return sorted(
            symbol
            for symbol in symbols
            if parse_symbol(symbol).exchange not in ST_EVIDENCE_UNSUPPORTED_EXCHANGES
        )
    if source == "tushare":
        if not _tushare_st_enabled(config):
            return []
        unsupported = set(
            st_evidence_unsupported_symbols(symbols, config=config, start=start, end=end)
        )
        return sorted(
            symbol
            for symbol in symbols
            if parse_symbol(symbol).exchange in ST_EVIDENCE_UNSUPPORTED_EXCHANGES
            and symbol not in unsupported
        )
    raise ValueError(f"unknown historical ST evidence source: {source}")


def build_st_scope(
    symbols: list[str],
    start: date,
    end: date,
    *,
    universe: str,
    source: str = "baostock",
) -> dict[str, Any]:
    if start > end:
        raise ValueError(f"ST evidence window is inverted: {start} > {end}")
    resolved = sorted(set(symbols))
    identity = {
        "evidence_version": ST_EVIDENCE_VERSION,
        "source": source,
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
        # The all-A universe can grow after a delisted/instrument recovery.
        # In that case the exact scope hash changes even though the requested
        # date window and evidence contract are unchanged. Reuse the most
        # advanced compatible checkpoint as a seed; reusable_st_checkpoint_symbols
        # will still verify that its positive facts survived compaction before
        # the caller skips any symbol.
        root = path.parent
        current_symbols = set(scope.get("expected_symbols", []))
        candidates: list[tuple[int, float, dict[str, Any]]] = []
        for candidate_path in root.glob("*.json"):
            if candidate_path == path:
                continue
            try:
                candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            previous_scope = candidate.get("scope") or {}
            previous_symbols = set(previous_scope.get("expected_symbols", []))
            if not previous_symbols or not previous_symbols <= current_symbols:
                continue
            if any(
                previous_scope.get(key) != scope.get(key)
                for key in ("evidence_version", "source", "start", "end")
            ):
                continue
            completed = set(candidate.get("completed_symbols", [])) & current_symbols
            candidates.append((len(completed), candidate_path.stat().st_mtime, candidate))
        if candidates:
            _, _, previous = max(candidates, key=lambda item: (item[0], item[1]))
            completed = sorted(set(previous.get("completed_symbols", [])) & current_symbols)
            evidence_rows = {
                symbol: int(count)
                for symbol, count in (previous.get("evidence_rows_by_symbol") or {}).items()
                if symbol in completed
            }
            unresolved = sorted(set(previous.get("unresolved_symbols", [])) & current_symbols)
            logger.info(
                "ST checkpoint scope grew from %d to %d symbols; inheriting %d completed symbols",
                len(previous.get("scope", {}).get("expected_symbols", [])),
                len(current_symbols),
                len(completed),
            )
            return {
                "schema_version": 1,
                "claim": ST_COVERAGE_CLAIM,
                "scope": scope,
                "status": "incomplete" if unresolved else "pending",
                "completed_symbols": completed,
                "evidence_rows_by_symbol": evidence_rows,
                "unresolved_symbols": unresolved,
                "inherited_from_scope_id": previous.get("scope", {}).get("scope_id"),
            }
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
        from cnequity.storage import StagingWriter

        files.extend(
            StagingWriter(config.staging_root).list_run_files("trading_status", staging_run_id)
        )
    if not files:
        return {}
    frame = scan_parquet_files(files, missing_columns="insert", extra_columns="ignore")
    schema = frame.collect_schema().names()
    frame = frame.filter(
        pl.col("symbol").is_in(sorted(symbols)),
        pl.col("trade_date").is_between(
            date.fromisoformat(scope["start"]),
            date.fromisoformat(scope["end"]),
        ),
    )
    if "source" in schema:
        source = str(scope.get("source") or "baostock")
        frame = frame.filter(pl.col("source") == source)
    # Count only distinct facts from the evidence owner. Filter the source
    # first: a same-PK daily snapshot must not erase the historical source row
    # before canonicalization.
    frame = dedupe_lazy_by_primary_key(frame, "trading_status")
    counts = frame.group_by("symbol").len().collect(engine="streaming")
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
        "evidence_rows_by_symbol": {
            symbol: int(expected_counts[symbol]) for symbol in sorted(expected)
        },
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
    # A newer scoped sweep may overlap an older, wider receipt.  Compose them
    # only after both source facts are in curated storage so a short tail sweep
    # can safely extend a deep-history receipt without re-querying every name
    # from the beginning of time.
    for path in list(published):
        try:
            receipt = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        composed = compose_st_coverage_receipt(config, receipt)
        if composed is not None and composed not in published:
            published.append(composed)
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
        if isinstance(payload, dict) and payload.get("claim") == ST_COVERAGE_CLAIM:
            out.append(payload)
    return out


def _pending_checkpoint_progress(
    config: Config,
    start: date | None,
    end: date | None,
    supported_symbols: list[str],
) -> dict[str, Any]:
    """Expose an exact in-progress sweep without treating it as evidence.

    A checkpoint is operational state, not a coverage receipt.  Still, hiding
    it behind the generic ``no_matching_complete_receipt`` finding makes a
    multi-hour backfill look identical to a job that was never started.  Only
    report a checkpoint when its supported-symbol scope matches the current
    universe and its end date reaches the requested end; this keeps progress
    useful without making a partial or stale sweep look research-ready.
    """
    if start is None or end is None or not supported_symbols:
        return {}
    root = config.meta_root / "state" / ST_COVERAGE_CLAIM / f"v{ST_EVIDENCE_VERSION}"
    if not root.exists():
        return {}
    expected = set(supported_symbols)
    candidates: list[tuple[int, float, dict[str, Any]]] = []
    for path in root.glob("*.json"):
        try:
            checkpoint = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if checkpoint.get("claim") != ST_COVERAGE_CLAIM:
            continue
        if checkpoint.get("status") not in {"pending", "incomplete"}:
            continue
        scope = checkpoint.get("scope") or {}
        if scope.get("source") != "baostock":
            continue
        if scope.get("end") != end.isoformat():
            continue
        checkpoint_symbols = set(scope.get("expected_symbols") or [])
        if checkpoint_symbols != expected:
            continue
        completed = set(checkpoint.get("completed_symbols") or []) & expected
        candidates.append((len(completed), path.stat().st_mtime, checkpoint))
    if not candidates:
        return {}
    _, _, checkpoint = max(candidates, key=lambda item: (item[0], item[1]))
    scope = checkpoint["scope"]
    completed = set(checkpoint.get("completed_symbols") or []) & expected
    unresolved = set(checkpoint.get("unresolved_symbols") or []) & expected
    return {
        "checkpoint_status": checkpoint.get("status"),
        "checkpoint_scope_start": scope.get("start"),
        "checkpoint_scope_end": scope.get("end"),
        "checkpoint_completed_symbols": len(completed),
        "checkpoint_expected_symbols": len(expected),
        "checkpoint_unresolved_symbols": len(unresolved),
    }


def _valid_receipt_scope(receipt: dict[str, Any]) -> tuple[dict[str, Any], date, date] | None:
    """Return a receipt scope only when its integrity fields are consistent."""
    scope = receipt.get("scope")
    completed = receipt.get("completed_symbols")
    if not isinstance(scope, dict) or not isinstance(completed, list):
        return None
    if receipt.get("schema_version") != 1 or receipt.get("status") != "complete":
        return None
    if not all(isinstance(symbol, str) for symbol in completed):
        return None
    # The scope hash intentionally ignores ordering. Check uniqueness
    # separately so a duplicated symbol cannot preserve the same hash while
    # inflating the receipt count and weakening the scope integrity check.
    if len(set(completed)) != len(completed):
        return None
    if receipt.get("completed_symbols_count") != len(completed):
        return None
    evidence_rows = receipt.get("evidence_rows")
    if not isinstance(evidence_rows, int) or isinstance(evidence_rows, bool) or evidence_rows < 0:
        return None
    row_counts = receipt.get("evidence_rows_by_symbol")
    if row_counts is not None:
        if not isinstance(row_counts, dict) or set(row_counts) != set(completed):
            return None
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in row_counts.values()
        ):
            return None
        if sum(row_counts.values()) != evidence_rows:
            return None
    if scope.get("expected_symbols_count") != len(completed):
        return None
    if receipt.get("completed_symbols_sha256") != symbol_scope_hash(completed):
        return None
    if scope.get("symbols_sha256") != symbol_scope_hash(completed):
        return None
    scope_id = scope.get("scope_id")
    identity = {
        key: value
        for key, value in scope.items()
        if key not in {"scope_id", "expected_symbols_count"}
    }
    if not isinstance(scope_id, str) or scope_id != _canonical_hash(identity):
        return None
    try:
        scope_start = date.fromisoformat(str(scope["start"]))
        scope_end = date.fromisoformat(str(scope["end"]))
    except (KeyError, TypeError, ValueError):
        return None
    if scope_start > scope_end:
        return None
    return scope, scope_start, scope_end


def _receipt_rows_intact(
    config: Config,
    receipt: dict[str, Any],
    scope: dict[str, Any],
    symbols: set[str],
) -> bool:
    """Check that persisted source facts still back a valid receipt.

    Receipts are immutable metadata, but curated Parquet can be compacted,
    repaired, or damaged after publication. New receipts carry per-symbol
    counts; legacy receipts carry only an aggregate, which is still checked so
    deletion of material evidence cannot silently leave research certified.
    """
    status_fingerprint = _trading_status_fingerprint(config)
    receipt_fingerprint = _canonical_hash(
        {
            "completed_symbols_sha256": receipt.get("completed_symbols_sha256"),
            "evidence_rows": receipt.get("evidence_rows"),
            "evidence_rows_by_symbol": receipt.get("evidence_rows_by_symbol"),
        }
    )
    cache_key = (
        str(config.data_root),
        str(scope.get("scope_id")),
        symbol_scope_hash(sorted(symbols)),
        f"{status_fingerprint}:{receipt_fingerprint}",
    )
    cached = _receipt_integrity_cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        expected_by_symbol = receipt.get("evidence_rows_by_symbol")
        # An old aggregate receipt describes every completed symbol. A
        # requested subset cannot be compared against that whole-scope total;
        # verify the entire legacy receipt, without inventing per-name counts.
        verification_symbols = (
            symbols if isinstance(expected_by_symbol, dict) else set(receipt["completed_symbols"])
        )
        persisted = _st_row_counts(config, scope, verification_symbols)
        if isinstance(expected_by_symbol, dict):
            intact = all(
                persisted.get(symbol, 0) >= int(expected_by_symbol[symbol]) for symbol in symbols
            )
        else:
            intact = sum(persisted.values()) >= int(receipt["evidence_rows"])
    except (KeyError, OSError, ValueError, pl.exceptions.PolarsError) as exc:
        logger.warning(
            "ST coverage: could not revalidate receipt %s: %s",
            scope.get("scope_id"),
            exc,
        )
        intact = False
    if len(_receipt_integrity_cache) >= _RECEIPT_INTEGRITY_CACHE_LIMIT:
        _receipt_integrity_cache.pop(next(iter(_receipt_integrity_cache)))
    _receipt_integrity_cache[cache_key] = intact
    return intact


def _first_traded_dates(
    config: Config, symbols: set[str], start: date, end: date
) -> dict[str, date]:
    """Return first persisted traded bar for each symbol in a set."""
    if not symbols:
        return {}
    root = config.curated_root / "daily_bars"
    if not root.exists():
        return {}
    try:
        frame = (
            scan_parquet_root(
                root,
                partition_col="trade_date",
                start=start,
                end=end,
                traded_only=True,
            )
            .filter(pl.col("symbol").is_in(sorted(symbols)))
            .select("symbol", "trade_date")
            .group_by("symbol")
            .agg(pl.col("trade_date").min().alias("first_trade_date"))
            .collect(engine="streaming")
        )
    except (FileNotFoundError, OSError, pl.exceptions.PolarsError, ValueError) as exc:
        logger.warning("ST coverage: cannot inspect symbol inception bars: %s", exc)
        return {}
    return {
        row["symbol"]: row["first_trade_date"]
        for row in frame.iter_rows(named=True)
        if row["first_trade_date"] is not None
    }


def compose_st_coverage_receipt(
    config: Config,
    extension_receipt: dict[str, Any],
) -> Path | None:
    """Compose an overlapping deep-history receipt with a newer tail receipt.

    The extension must cover every symbol in the base receipt and overlap its
    end date. Symbols added since the base receipt are accepted only when
    their first persisted traded bar is on or after the extension start; this
    proves that the shorter extension window does not omit a pre-listing
    interval. The base receipt's row total is rechecked before composition.
    """
    parsed_extension = _valid_receipt_scope(extension_receipt)
    if parsed_extension is None:
        return None
    extension_scope, extension_start, extension_end = parsed_extension
    extension_source = extension_scope.get("source")
    if extension_source not in {"baostock", "tushare"}:
        return None
    extension_symbols = set(extension_receipt["completed_symbols"])

    candidates: list[tuple[date, date, dict[str, Any], dict[str, Any]]] = []
    for candidate in _receipts(config):
        parsed_base = _valid_receipt_scope(candidate)
        if parsed_base is None:
            continue
        base_scope, base_start, base_end = parsed_base
        if base_scope.get("scope_id") == extension_scope.get("scope_id"):
            continue
        if base_scope.get("source") != extension_source:
            continue
        base_symbols = set(candidate["completed_symbols"])
        if not base_symbols <= extension_symbols:
            continue
        # The two receipts must overlap; otherwise a calendar interval would
        # be left unproven between the deep receipt and the tail sweep.
        if base_start >= extension_start or base_end < extension_start:
            continue
        candidates.append((base_start, base_end, candidate, base_scope))
    if not candidates:
        return None

    _, _, base_receipt, base_scope = min(
        candidates, key=lambda item: (item[0], -item[1].toordinal())
    )
    base_symbols = set(base_receipt["completed_symbols"])

    # The old receipt schema predates per-symbol counts. Its aggregate count
    # is still a useful tamper check and is sufficient because the receipt is
    # immutable evidence for the zero-row symbols as well.
    base_counts = _st_row_counts(
        config,
        base_scope,
        base_symbols,
    )
    if sum(base_counts.values()) != int(base_receipt.get("evidence_rows", -1)):
        logger.warning(
            "ST coverage: refusing receipt composition; base row total changed (%d != %s)",
            sum(base_counts.values()),
            base_receipt.get("evidence_rows"),
        )
        return None

    added_symbols = extension_symbols - base_symbols
    first_dates = _first_traded_dates(
        config,
        added_symbols,
        base_start,
        extension_end,
    )
    if any(
        first_dates.get(symbol) is None or first_dates[symbol] < extension_start
        for symbol in added_symbols
    ):
        logger.info(
            "ST coverage: cannot compose %s; added symbols lack post-extension inception evidence",
            extension_scope.get("scope_id"),
        )
        return None

    merged_scope = build_st_scope(
        sorted(extension_symbols),
        base_start,
        extension_end,
        universe=str(extension_scope.get("universe", "all_a")),
        source=str(extension_source),
    )
    merged_counts = _st_row_counts(config, merged_scope, extension_symbols)
    receipt = {
        "schema_version": 1,
        "claim": ST_COVERAGE_CLAIM,
        "status": "complete",
        "scope": {key: value for key, value in merged_scope.items() if key != "expected_symbols"},
        "completed_symbols": sorted(extension_symbols),
        "completed_symbols_count": len(extension_symbols),
        "completed_symbols_sha256": symbol_scope_hash(sorted(extension_symbols)),
        "evidence_rows": sum(merged_counts.values()),
        "evidence_rows_by_symbol": dict(sorted(merged_counts.items())),
        "composition": {
            "type": "overlapping_receipts",
            "base_scope_id": base_scope["scope_id"],
            "extension_scope_id": extension_scope["scope_id"],
            "added_symbols_since_base": sorted(added_symbols),
        },
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    path = _receipt_path(config, str(merged_scope["scope_id"]))
    _atomic_json(path, receipt)
    return path


def st_evidence_coverage_report(
    config: Config,
    start: date | None = None,
    end: date | None = None,
    *,
    symbols: list[str] | None = None,
    universe: str = "all_a",
) -> dict[str, Any]:
    """Return whether a complete, current all-A receipt covers the window.

    ``symbols`` narrows the evidence scope for a strict query that explicitly
    selected a subset of all-A names. The default remains the complete current
    all-A scope used by the lake-level research gate.
    """
    symbols = current_st_universe(
        config,
        start=start,
        end=end,
        symbols=symbols,
        universe=universe,
    )
    unsupported_symbols = st_evidence_unsupported_symbols(
        symbols,
        config=config,
        start=start,
        end=end,
    )
    supported_symbols = sorted(set(symbols) - set(unsupported_symbols))
    source_groups = {
        source: st_evidence_source_symbols(
            symbols,
            source,
            config=config,
            start=start,
            end=end,
        )
        for source in ("baostock", "tushare")
    }

    best_by_source: dict[str, dict[str, Any]] = {}
    receipt_integrity_failures: set[str] = set()
    for source, source_symbols in source_groups.items():
        if not source_symbols:
            continue
        candidates: list[dict[str, Any]] = []
        for receipt in _receipts(config):
            parsed = _valid_receipt_scope(receipt)
            if parsed is None:
                continue
            scope, scope_start, scope_end = parsed
            if scope.get("evidence_version") != ST_EVIDENCE_VERSION:
                continue
            if (
                scope.get("source") != source
                or scope.get("universe") not in ST_EVIDENCE_COMPATIBLE_UNIVERSES
            ):
                continue
            covered_symbols = set(receipt.get("completed_symbols", []))
            if not set(source_symbols) <= covered_symbols:
                continue
            if start is not None and scope_start > start:
                continue
            if end is not None and scope_end < end:
                continue
            candidates.append(receipt)
        best = None
        for candidate in sorted(
            candidates,
            key=lambda item: (
                date.fromisoformat(item["scope"]["start"]),
                -date.fromisoformat(item["scope"]["end"]).toordinal(),
            ),
        ):
            if _receipt_rows_intact(config, candidate, candidate["scope"], set(source_symbols)):
                best = candidate
                break
            receipt_integrity_failures.add(source)
        if best is not None:
            best_by_source[source] = best

    required_sources = {source for source, group in source_groups.items() if group}
    missing_sources = required_sources - set(best_by_source)
    receipts_complete = not missing_sources and bool(required_sources)
    if not receipts_complete:
        report = {
            "verified": False,
            "claim": ST_COVERAGE_CLAIM,
            "evidence_version": ST_EVIDENCE_VERSION,
            "requested_start": start.isoformat() if start else None,
            "requested_end": end.isoformat() if end else None,
            "requested_universe": universe,
            "current_symbols": len(symbols),
            "supported_symbols": len(supported_symbols),
            "unsupported_symbols": len(unsupported_symbols),
            "unsupported_exchange_counts": {
                exchange: sum(
                    1 for symbol in unsupported_symbols if parse_symbol(symbol).exchange == exchange
                )
                for exchange in sorted(ST_EVIDENCE_UNSUPPORTED_EXCHANGES)
                if any(parse_symbol(symbol).exchange == exchange for symbol in unsupported_symbols)
            },
            # Unsupported exchanges are a source-capability blocker even when
            # there is no compatible receipt at all. Preserve that fact in the
            # reason so strict readers and the historical validity contract do
            # not reduce BJ to a generic missing-cache message.
            "reason": (
                "unsupported_exchange_symbols"
                if unsupported_symbols
                else (
                    "receipt_data_drift"
                    if receipt_integrity_failures
                    else ("no_current_universe" if not symbols else "no_matching_complete_receipt")
                )
            ),
            "source_coverage": {
                source: {
                    "required_symbols": len(source_groups[source]),
                    "verified": source in best_by_source,
                    "scope_id": (
                        best_by_source[source].get("scope", {}).get("scope_id")
                        if source in best_by_source
                        else None
                    ),
                    "reason": (
                        "receipt_data_drift" if source in receipt_integrity_failures else None
                    ),
                }
                for source in source_groups
                if source_groups[source]
            },
        }
        report.update(_pending_checkpoint_progress(config, start, end, source_groups["baostock"]))
        return report
    unsupported = bool(unsupported_symbols)
    scopes = {source: receipt["scope"] for source, receipt in best_by_source.items()}
    coverage_start = min(scope["start"] for scope in scopes.values())
    coverage_end = max(scope["end"] for scope in scopes.values())
    scope_id = (
        next(iter(scopes.values()))["scope_id"]
        if len(scopes) == 1
        else _canonical_hash({source: scope["scope_id"] for source, scope in scopes.items()})
    )
    return {
        "verified": not unsupported and receipts_complete,
        "claim": ST_COVERAGE_CLAIM,
        "evidence_version": ST_EVIDENCE_VERSION,
        "requested_start": start.isoformat() if start else None,
        "requested_end": end.isoformat() if end else None,
        "requested_universe": universe,
        "coverage_start": coverage_start,
        "coverage_end": coverage_end,
        "current_symbols": len(symbols),
        "supported_symbols": len(supported_symbols),
        "unsupported_symbols": len(unsupported_symbols),
        "unsupported_exchange_counts": {
            exchange: sum(
                1 for symbol in unsupported_symbols if parse_symbol(symbol).exchange == exchange
            )
            for exchange in sorted(ST_EVIDENCE_UNSUPPORTED_EXCHANGES)
            if any(parse_symbol(symbol).exchange == exchange for symbol in unsupported_symbols)
        },
        "supported_coverage_verified": receipts_complete,
        "reason": "unsupported_exchange_symbols" if unsupported else None,
        "scope_id": scope_id,
        "source_coverage": {
            source: {
                "required_symbols": len(source_groups[source]),
                "verified": True,
                "scope_id": receipt["scope"]["scope_id"],
                "coverage_start": receipt["scope"]["start"],
                "coverage_end": receipt["scope"]["end"],
            }
            for source, receipt in best_by_source.items()
        },
    }
