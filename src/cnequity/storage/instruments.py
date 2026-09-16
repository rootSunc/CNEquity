"""Merge-style compact for instruments (preserve delisted symbols)."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date
from pathlib import Path

import polars as pl

from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.schemas import INSTRUMENTS_SCHEMA, validate_dataframe
from cnequity.domain.symbols import is_subscription_placeholder
from cnequity.storage.atomic import write_parquet_atomic
from cnequity.storage.parquet import StagingWriter

# Refuse delist inference when too many symbols vanish from a snapshot — usually
# a partial TDX fetch, not a mass delisting event.
ABSENT_DELIST_THRESHOLD = 0.05
# A live-universe snapshot is a point-in-time observation. Require repeated
# absences before assigning a sticky delist_date; one partial fetch must not
# permanently remove a still-trading name from all_a.
ABSENT_DELIST_CONFIRMATIONS = 2


def _symbols_without_bars(curated_root: Path, symbols: list[str]) -> set[str]:
    """Of *symbols*, those the lake holds no daily bar for.

    Scoped to the handful of codes absent from one snapshot, so this is a
    targeted read rather than a scan of the bars dataset.
    """
    if not symbols:
        return set()
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = curated_root / "daily_bars"
    if not dataset_has_parquet(root):
        # No bars at all (a fresh lake, or a fixture): absence proves nothing
        # either way, so fall back to the caller's existing streak logic.
        return set()
    seen = (
        scan_parquet_root(root, partition_col="trade_date", symbols=sorted(symbols))
        .select("symbol")
        .unique()
        .collect()
        .get_column("symbol")
        .to_list()
    )
    return set(symbols) - set(seen)


def _absence_state_path(curated_root: Path) -> Path:
    return curated_root.parent / "meta" / "instruments_absence_streak.json"


def _load_absence_state(path: Path) -> dict[str, dict[str, object]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        str(symbol): value
        for symbol, value in payload.items()
        if isinstance(value, dict) and isinstance(value.get("count"), int)
    }


def _save_absence_state(path: Path, state: dict[str, dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(state, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _strip_subscription_placeholders(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty() or "name" not in df.columns:
        return df
    keep = [not is_subscription_placeholder(name) for name in df["name"].to_list()]
    return df.filter(pl.Series(keep))


def _business_digest(df: pl.DataFrame) -> str:
    """Hash instrument business content while ignoring fetch provenance churn."""
    volatile = {"source", "data_version", "fetched_at", "run_id", "capture_id"}
    columns = [column for column in df.columns if column not in volatile]
    if not columns:
        return hashlib.sha256(b"[]").hexdigest()
    canonical = df.select(columns).sort(columns)
    encoded = json.dumps(
        canonical.to_dicts(), sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def compact_instruments(
    staging_root: Path,
    curated_root: Path,
    run_id: str,
    trade_date: date,
    changed_files: list[Path] | None = None,
    base_root: Path | None = None,
) -> tuple[int, list[dict]]:
    """Merge staging instruments into curated, retaining symbols missing from TDX."""
    staging = StagingWriter(staging_root)
    files = staging.list_run_files("instruments", run_id)
    if not files:
        return 0, []

    incoming = pl.concat(
        [validate_dataframe(pl.read_parquet(f), "instruments") for f in files],
        how="diagonal_relaxed",
    )
    incoming = _strip_subscription_placeholders(incoming)
    incoming = dedupe_by_primary_key(incoming, "instruments")

    out_path = curated_root / "instruments" / "part-merged.parquet"
    read_dir = Path(base_root) if base_root is not None else out_path.parent
    curated_files = sorted(read_dir.rglob("*.parquet")) if read_dir.exists() else []
    # ``base_root`` is an immutable revision generation used as the merge
    # input.  Its canonical parquet path is necessarily different from the
    # mutable output path, but that does not make the generation a stale
    # fragment.  Treat only files discovered directly in the mutable curated
    # directory as fragments; otherwise a provenance-only refresh rewrites the
    # mutable parquet on every compact.
    had_fragments = base_root is None and any(path != out_path for path in curated_files)
    had_duplicate_rows = False
    had_removed_rows = False
    if curated_files:
        existing = pl.concat(
            [validate_dataframe(pl.read_parquet(path), "instruments") for path in curated_files],
            how="diagonal_relaxed",
        )
        raw_existing_height = existing.height
        existing = _strip_subscription_placeholders(existing)
        had_removed_rows = existing.height != raw_existing_height
        had_duplicate_rows = existing.height != existing.select("symbol").n_unique()
        existing = dedupe_by_primary_key(existing, "instruments")
    else:
        existing = pl.DataFrame(schema=INSTRUMENTS_SCHEMA)

    incoming_symbols = incoming["symbol"].to_list()
    findings: list[dict] = []
    absence_path = _absence_state_path(curated_root)
    absence_state = _load_absence_state(absence_path)
    if not existing.is_empty():
        preserved = existing.filter(~pl.col("symbol").is_in(incoming_symbols))
        # Symbols with a known delist date before this snapshot are expected
        # to be absent.  Counting them in the circuit breaker made a mature
        # catalog look like a partial fetch on every run, eventually masking
        # genuine omissions from the still-live universe.
        expected_live = existing.filter(
            pl.col("delist_date").is_null() | (pl.col("delist_date") >= trade_date)
        )
        absent_live = expected_live.filter(~pl.col("symbol").is_in(incoming_symbols))
        absent_count = absent_live.height
        expected_live_count = expected_live.height
        absent_ratio = absent_count / expected_live_count if expected_live_count else 0.0
        if absent_count and absent_ratio > ABSENT_DELIST_THRESHOLD:
            findings.append(
                {
                    "dataset": "instruments",
                    "severity": "error",
                    "check": "instruments_delist_suppressed",
                    "message": (
                        f"Refused to infer delist_date: {absent_count}/{expected_live_count} symbols "
                        f"({absent_ratio:.1%}) absent from snapshot (>{ABSENT_DELIST_THRESHOLD:.0%} "
                        "threshold); likely partial fetch"
                    ),
                    "absent_count": absent_count,
                    "existing_count": expected_live_count,
                    "absent_ratio": absent_ratio,
                }
            )
        else:
            inferred: dict[str, date] = {}
            pending = 0
            never_traded = _symbols_without_bars(
                curated_root,
                [
                    row["symbol"]
                    for row in absent_live.select("symbol", "delist_date").iter_rows(named=True)
                    if row["delist_date"] is None
                ],
            )
            skipped_never_traded = 0
            for row in absent_live.select("symbol", "delist_date").iter_rows(named=True):
                symbol = row["symbol"]
                if row["delist_date"] is not None:
                    absence_state.pop(symbol, None)
                    continue
                if symbol in never_traded:
                    # A security that has never printed a bar cannot have stopped
                    # trading — it has not started. TDX lists a new code briefly
                    # before its first session and then drops it, which reads as
                    # a run of absences and used to earn a delist_date days
                    # before the listing: 36 rows were stamped that way on
                    # 2026-09-10 alone, and 'C'/'N' name prefixes marked them all
                    # as new issues. Keep the streak so a genuine retirement is
                    # still inferred once bars exist.
                    skipped_never_traded += 1
                    continue
                prior = absence_state.get(symbol, {})
                count = int(prior.get("count", 0)) + 1
                absence_state[symbol] = {
                    "count": count,
                    "last_missing": trade_date.isoformat(),
                }
                if count >= ABSENT_DELIST_CONFIRMATIONS:
                    inferred[symbol] = trade_date
                    absence_state.pop(symbol, None)
                else:
                    pending += 1
            if inferred:
                delist_expr = pl.col("delist_date")
                for symbol, inferred_date in inferred.items():
                    delist_expr = (
                        pl.when(pl.col("symbol") == symbol)
                        .then(pl.lit(inferred_date))
                        .otherwise(delist_expr)
                    )
                preserved = preserved.with_columns(delist_expr.alias("delist_date"))
            if skipped_never_traded:
                findings.append(
                    {
                        "dataset": "instruments",
                        "severity": "info",
                        "check": "instruments_delist_skipped_never_traded",
                        "message": (
                            f"{skipped_never_traded} absent symbol(s) have no bar in the lake, "
                            "so absence is a pending listing rather than a delisting"
                        ),
                        "symbols": skipped_never_traded,
                    }
                )
            if pending:
                findings.append(
                    {
                        "dataset": "instruments",
                        "severity": "warning",
                        "check": "instruments_delist_pending",
                        "message": (
                            f"{pending}/{absent_count} absent symbol(s) need another "
                            f"consecutive snapshot before delist inference "
                            f"({ABSENT_DELIST_CONFIRMATIONS} confirmations required)"
                        ),
                        "pending_count": pending,
                        "absent_count": absent_count,
                        "confirmations_required": ABSENT_DELIST_CONFIRMATIONS,
                    }
                )
        prior_dates = existing.select(
            [
                "symbol",
                pl.col("list_date").alias("_prior_list_date"),
                pl.col("delist_date").alias("_prior_delist_date"),
            ]
        )
        incoming = incoming.join(prior_dates, on="symbol", how="left")
        # Both dates are sticky: a live snapshot carries neither (TDX has no such
        # field), so coalescing is what keeps a delist_date — inferred from an
        # earlier absence or fetched from baostock — from being erased the next
        # day. Never resurrect a name a prior run buried.
        incoming = incoming.with_columns(
            pl.coalesce(pl.col("list_date"), pl.col("_prior_list_date")).alias("list_date"),
            pl.coalesce(pl.col("delist_date"), pl.col("_prior_delist_date")).alias("delist_date"),
        ).drop("_prior_list_date", "_prior_delist_date")
        for symbol in incoming_symbols:
            absence_state.pop(symbol, None)
    else:
        preserved = pl.DataFrame(schema=INSTRUMENTS_SCHEMA)

    merged = pl.concat([incoming, preserved], how="diagonal_relaxed")
    merged = dedupe_by_primary_key(merged, "instruments")
    # A delist_date that predates the listing is a self-contradiction, and the
    # listing date is the half backed by evidence: delist_date here is inferred
    # from a run of absences, and TDX lists a new code, drops it, then lists it
    # for real. That sequence earned a delisting 13 days before the security
    # started trading — rows that go on to reach `delisting_events` and the
    # survivorship check as a delisting that never happened.
    #
    # Applied to the merged frame, not just the incoming snapshot: the same
    # codes are routinely dropped from the live list again, and a contradiction
    # does not stop being one because the vendor is no longer listing the name.
    #
    # Deliberately narrow: only an *inverted* pair is cleared. A genuinely
    # retired name has list_date < delist_date and is never touched, so "never
    # resurrect a name a prior run buried" still holds.
    merged = merged.with_columns(
        pl.when(
            pl.col("list_date").is_not_null()
            & pl.col("delist_date").is_not_null()
            & (pl.col("delist_date") < pl.col("list_date"))
        )
        .then(None)
        .otherwise(pl.col("delist_date"))
        .alias("delist_date")
    )

    before_business_digest = _business_digest(existing) if curated_files else None
    after_business_digest = _business_digest(merged)
    business_changed = before_business_digest != after_business_digest
    # A same-business-content refresh (for example only a new fetched_at or
    # source label) is a true no-op.  Avoid rewriting the canonical file so
    # downstream revision logic and file mtimes remain quiet.  Fragments are
    # still consolidated when present, but that cleanup alone is not a new
    # business revision.
    if (
        business_changed
        or had_fragments
        or had_duplicate_rows
        or had_removed_rows
        or not out_path.is_file()
    ):
        write_parquet_atomic(out_path, merged, compression="zstd")
        # Instruments is merge-style, so there is no partition writer to clean
        # up stale fragments. Keep one canonical file; readers and whole-lake
        # audits must not see an old ``part-*.parquet`` beside it.
        for stale in out_path.parent.rglob("*.parquet"):
            if stale != out_path:
                stale.unlink()
    if changed_files is not None and business_changed:
        changed_files.append(out_path)
    _save_absence_state(absence_path, absence_state)
    return merged.height, findings
