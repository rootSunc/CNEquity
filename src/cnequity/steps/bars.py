"""L1 bar steps: daily_bars, index_bars."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Iterable
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

from cnequity.adapters.tdx_protocol.client import (
    INDEX_SYMBOLS,
    fetch_index_bars,
    normalize_with_source,
)
from cnequity.config import Config
from cnequity.domain.frames import with_columns_unless_blank
from cnequity.domain.market_time import A_SHARE_FINAL_AT, shanghai_now
from cnequity.domain.rate_limit import (
    SINA_FETCH_ATTEMPTS,
    SINA_RATE_LIMIT_CIRCUIT_SECONDS,
    SINA_RATE_LIMIT_COOLDOWN_SECONDS,
    SINA_RATE_LIMIT_STATUS_CODES,
    SINA_RETRY_STATUS_CODES,
    source_request,
)
from cnequity.domain.symbols import (
    filter_ingest_universe,
    in_ingest_universe,
    is_tdx_servable,
    parse_symbol,
    split_by_quote_source,
)
from cnequity.orchestrator.registry import register_step
from cnequity.orchestrator.worker_pool import fetch_daily_bars_parallel
from cnequity.quality.audit import persist_step_findings
from cnequity.query.canonical import dedupe_by_primary_key
from cnequity.steps.common import (
    BACKFILL_START,
    DailyBarOwnership,
    classify_daily_bar_ownership,
    incremental_window,
    instrument_metadata,
    is_trading_day,
    last_session_on_or_before,
    list_trading_dates,
    load_bar_universe,
    load_curated_instruments,
    load_curated_trading_status,
    load_negative_evidence,
    load_symbols,
    negative_evidence_covers,
    record_negative_evidence,
)
from cnequity.storage.state import StateStore

logger = logging.getLogger(__name__)

# The closing auction ends at 15:00. Leave a small settlement buffer before
# trusting TDX's current daily bar; the default core schedule starts at 16:00.
_DAILY_BAR_FINAL_AT = A_SHARE_FINAL_AT
# Sina's anti-abuse policy is shared with the other two sweeps that hit it;
# see cnequity/domain/rate_limit.py.
_SINA_RETRY_STATUS_CODES = SINA_RETRY_STATUS_CODES
_SINA_RATE_LIMIT_STATUS_CODES = SINA_RATE_LIMIT_STATUS_CODES
_SINA_FETCH_ATTEMPTS = SINA_FETCH_ATTEMPTS
_SINA_RATE_LIMIT_COOLDOWN_SECONDS = SINA_RATE_LIMIT_COOLDOWN_SECONDS
_SINA_RATE_LIMIT_CIRCUIT_SECONDS = SINA_RATE_LIMIT_CIRCUIT_SECONDS
_EXCHANGE_BULK_GAPFILL_MAX_SESSIONS = 20


def _reject_unfinished_daily_bar_window(
    config: Config,
    end: date,
    *,
    now: datetime | None = None,
) -> None:
    """Reject a window whose newest daily bar is still forming in Shanghai.

    TDX ``start=0`` includes the current daily K. Once trading begins that row
    has plausible OHLC and non-zero volume, so content checks cannot distinguish
    it from a settled bar. Refuse the fetch before any symbol batch starts: a
    market-wide sweep that crosses 15:00 would otherwise mix partial and final
    bars in one curated partition.

    Historical windows and non-trading days are unaffected. ``now`` is
    injectable so the timezone boundary is deterministic in tests.
    """
    local_now = shanghai_now(now)
    today = local_now.date()
    if end < today or local_now.time() >= _DAILY_BAR_FINAL_AT:
        return
    if not is_trading_day(config, today):
        return
    raise RuntimeError(
        f"daily_bars {end}: the current A-share session is not final until "
        f"{_DAILY_BAR_FINAL_AT.strftime('%H:%M')} Asia/Shanghai "
        f"(now {local_now.strftime('%H:%M:%S')}); refusing to stage an "
        "in-progress daily bar. Re-run after the cutoff."
    )


def _last_final_session(now: datetime | None = None) -> date:
    """The newest date whose daily bar has settled.

    The mirror of :func:`_reject_unfinished_daily_bar_window`: what that refuses
    to fetch, this is what a history sweep should ask for instead.

    Deliberately clock-only. Asking the calendar whether today trades would
    make a window depend on a `trading_calendar` the lake may not have yet —
    `cne init` builds it one phase before the first backfill — and it buys
    nothing: stepping back a day on a Sunday lands on Saturday, and a window
    ending on a day with no session loses none of the sessions before it.
    """
    local_now = shanghai_now(now)
    if local_now.time() >= _DAILY_BAR_FINAL_AT:
        return local_now.date()
    return local_now.date() - timedelta(days=1)


def _backfill_window(config: Config, trade_date: date) -> tuple[date, date]:
    """``--start/--end`` window for a backfill, defaulting to the full history.

    Repairing a single bad session must not mean re-fetching a decade for every
    symbol. A capture that fires before the close writes a truncated bar — right
    open, wrong close, partial volume — and the repair is one day wide.

    An unspecified end means "as much history as there is", which is the last
    *settled trading session* — not today, whose bar is still forming until
    15:05, and not a Saturday/holiday for which no bar can exist. It
    defaulted to today, so `cne init` run during a session failed the whole
    phase on a window nobody asked for: 37 minutes of reference and corporate
    actions, then `phase2c_daily_bars_backfill` refused in 2.8ms and phases 3
    and 4 never ran. An explicit trading-day `--end` is still honored exactly,
    so repairing today's truncated bar before the close fails loudly. An
    explicit non-trading end is normalized to the preceding session instead
    of manufacturing a permanently missing Saturday/holiday key.

    `trade_date` bounds it from the other side. Taking the settled session alone
    ignored the date the caller named: replaying an old one — `cne run daily
    --trade-date 2025-01-10` — asked for everything up to *today* instead, which
    is not the window anybody requested and not a run anybody could reproduce.
    The smaller of the two is the only answer that satisfies both: today's run
    stops before the forming bar, and a replay stops where it was told.
    """
    settled = _last_final_session()
    requested_end = getattr(config, "_backfill_end", None) or min(trade_date, settled)
    end = last_session_on_or_before(config, requested_end)
    start = getattr(config, "_backfill_start", None) or BACKFILL_START
    return start, end


def _instrument_spans(
    config: Config,
) -> dict[str, tuple[date | None, date | None, str | None]]:
    return {
        row["symbol"]: (row["list_date"], row["delist_date"], row.get("asset_type"))
        for row in instrument_metadata(config).iter_rows(named=True)
    }


def _classify_daily_scope(
    config: Config,
    symbols: list[str],
    start: date,
    end: date,
    *,
    bar_universe: set[str] | None = None,
) -> DailyBarOwnership:
    """Classify one daily-bar request from disk-backed evidence.

    This helper is deliberately disk-only. A missing status file or malformed
    evidence leaves a symbol ``unknown``; the caller then fetches it and the
    final validator decides whether the run may be published.
    """
    metadata = instrument_metadata(config)
    spans = {
        row["symbol"]: (row["list_date"], row["delist_date"], row.get("asset_type"))
        for row in metadata.iter_rows(named=True)
    }
    sessions = list_trading_dates(config, start, end)
    status = load_curated_trading_status(
        config,
        start=start,
        end=end,
        symbols=symbols,
    )
    evidence = load_negative_evidence(config, "daily_bars", metadata=metadata)
    return classify_daily_bar_ownership(
        symbols,
        spans,
        start,
        end,
        bar_universe=bar_universe,
        trading_status=status,
        trading_sessions=sessions,
        negative_evidence=evidence,
    )


def _placeholder_bar_universe(
    config: Config,
    spans: dict[str, tuple[date | None, date | None, str | None]],
) -> set[str] | None:
    """Return traded bars only when an undated symbol needs reconciliation.

    Scanning every daily_bars file is unnecessary for normal runs. An empty
    traded universe is also not evidence that every undated symbol is a
    placeholder, so leave the classifier conservative in a brand-new lake.

    A run that names its symbols gets no placeholder universe at all. The
    classification is a cost control for the full-market sweep — it keeps
    pre-listing codes out of the per-symbol fallback — and it reads an absent
    list_date as "not listed yet". An operator asking for a symbol by name has
    already made that call, so honouring the guess instead fetched nothing,
    wrote nothing and still reported success: `cne backfill daily_bars
    --symbols 000001.SZ` returned 0 rows against a source that had them.
    """
    if getattr(config, "_backfill_symbols", None):
        return None
    if not any(list_date is None for list_date, _delist_date, _asset in spans.values()):
        return None
    universe = load_bar_universe(config)
    return universe or None


def _ownership_context(
    config: Config,
    ownership: DailyBarOwnership,
    start: date,
    end: date,
) -> tuple[dict, bool]:
    from cnequity.steps.delisted import delisted_recovery_covers

    delegated_complete = delisted_recovery_covers(config, start, end, ownership.delegated_delisted)
    findings = [
        {
            "dataset": "daily_bars",
            "severity": "info" if delegated_complete else "warning",
            "check": "daily_bars_source_ownership",
            "message": (
                f"generic={len(ownership.generic)}, "
                f"delegated_delisted={len(ownership.delegated_delisted)}, "
                f"expected_no_data={len(ownership.expected_no_data)}, "
                f"placeholder={len(ownership.placeholder)}, "
                f"negative_cached={len(ownership.negative_cached)}, "
                f"unknown={len(ownership.unknown)}, "
                f"delegated_complete={delegated_complete}"
            ),
            "start": start.isoformat(),
            "end": end.isoformat(),
        }
    ]
    if ownership.placeholder:
        preview = ", ".join(sorted(ownership.placeholder)[:8])
        suffix = "..." if len(ownership.placeholder) > 8 else ""
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_placeholder_skipped",
                "message": (
                    f"{len(ownership.placeholder)} undated placeholder(s) skipped "
                    "(no list_date and no traded bar anywhere in the lake; not "
                    f"verified no-data): {preview}{suffix}"
                ),
                "symbols": sorted(ownership.placeholder),
            }
        )
    if ownership.unknown:
        preview = ", ".join(sorted(ownership.unknown)[:8])
        suffix = "..." if len(ownership.unknown) > 8 else ""
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_unknown_ownership",
                "message": (
                    f"{len(ownership.unknown)} symbol(s) lack sufficient instrument/status "
                    f"evidence and remain strict fetch obligations: {preview}{suffix}"
                ),
                "symbols": sorted(ownership.unknown),
            }
        )
    return {
        "daily_bars_ownership": {
            "generic": len(ownership.generic),
            "delegated_delisted": len(ownership.delegated_delisted),
            "expected_no_data": len(ownership.expected_no_data),
            "placeholder": len(ownership.placeholder),
            "negative_cached": len(ownership.negative_cached),
            "unknown": len(ownership.unknown),
            "delegated_complete": delegated_complete,
        },
        "audit_findings": findings,
    }, delegated_complete


def _record_delegated_ownership_batch(
    config: Config,
    run_id: str,
    symbols: list[str],
    start: date,
    end: date,
    *,
    batch_id: str | None = None,
) -> bool:
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.steps.delisted import delisted_recovery_covers

    if not symbols:
        return True
    complete = delisted_recovery_covers(config, start, end, symbols)
    manifest = Manifest(config.manifest_path)
    if batch_id is None:
        identity = json.dumps(
            {"symbols": sorted(symbols), "start": start.isoformat(), "end": end.isoformat()},
            sort_keys=True,
            separators=(",", ":"),
        )
        batch_id = f"ownership-{hashlib.sha256(identity.encode()).hexdigest()[:16]}"
    existing = manifest.get_batch(run_id, batch_id)
    if existing is None or existing["status"] != "success":
        # Re-open the same deterministic ownership batch on retry. Without
        # this, finish_batch() only updates rows still marked ``running`` and
        # a prior warning can never become successful after recovery completes.
        manifest.start_batch(
            run_id,
            batch_id,
            task_id="daily_bars_ownership",
            dataset="daily_bars",
            symbols=sorted(symbols),
            window_start=start.isoformat(),
            window_end=end.isoformat(),
            blocks_compaction=True,
        )
    manifest.finish_batch(
        run_id,
        batch_id,
        "success" if complete else "warning",
        error_message=(
            "delegated delisted recovery receipt verified"
            if complete
            else "delegated delisted symbols lack a complete recovery receipt"
        ),
    )
    return complete


def _reuse_successful_daily_bars(
    config: Config,
    run_id: str,
    symbols: list[str],
    start: date,
    end: date,
) -> set[str]:
    """Seed this run from verified staging batches of an interrupted run.

    A failed catchup should not make the next run re-fetch every symbol whose
    earlier batch already finished successfully. Only manifest-successful
    batches with the exact same window are eligible; failed/running staging is
    never reused. A symbol is removed from the new fetch scope only when all
    trading sessions in the window are present.
    """
    import polars as pl

    from cnequity.orchestrator.manifest import Manifest
    from cnequity.storage import StagingWriter

    sessions = list_trading_dates(config, start, end)
    if not symbols or not sessions:
        return set()
    batches = Manifest(config.manifest_path).get_successful_batches(
        "daily_bars",
        start.isoformat(),
        end.isoformat(),
        exclude_run_id=run_id,
    )
    if not batches:
        return set()

    files = []
    for batch in batches:
        path = (
            config.staging_root
            / "daily_bars"
            / f"run_id={batch['run_id']}"
            / (f"part-{batch['batch_id']}.parquet")
        )
        if path.exists():
            files.append(path)
    if not files:
        return set()

    frames = [pl.read_parquet(path) for path in files]
    reused = pl.concat(frames, how="diagonal_relaxed").filter(
        pl.col("symbol").is_in(symbols) & pl.col("trade_date").is_in(sessions)
    )
    if reused.is_empty():
        return set()
    reused = dedupe_by_primary_key(reused, "daily_bars")
    reused_symbols = set(
        reused.group_by("symbol")
        .len()
        .filter(pl.col("len") == len(sessions))
        .get_column("symbol")
        .to_list()
    )
    if not reused_symbols:
        return set()

    reused = reused.filter(pl.col("symbol").is_in(sorted(reused_symbols)))
    StagingWriter(config.staging_root).write_batch(
        "daily_bars",
        run_id,
        f"reused-successful-{start.isoformat()}-{end.isoformat()}",
        reused,
    )
    logger.info(
        "daily_bars: reused %d symbol(s) from %d prior successful batch(es); "
        "fetching the remaining scope",
        len(reused_symbols),
        len(files),
    )
    return reused_symbols


def _merge_ownership_result(
    out: dict,
    config: Config,
    ownership: DailyBarOwnership,
    start: date,
    end: date,
) -> dict:
    updates, delegated_complete = _ownership_context(config, ownership, start, end)
    context = out.setdefault("context_updates", {})
    context.setdefault("audit_findings", []).extend(updates["audit_findings"])
    context["daily_bars_ownership"] = updates["daily_bars_ownership"]
    if ownership.delegated_delisted and not delegated_complete:
        out["status"] = "warning"
        out["delegated_symbols"] = len(ownership.delegated_delisted)
    return out


def _resolve_daily_bar_scope(config: Config, symbols: list[str]) -> list[str]:
    """Validate an explicit daily-bar repair scope against instruments."""
    requested = list(
        dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip())
    )
    if not requested:
        raise RuntimeError("daily_bars backfill symbols must not be empty")
    known = set(load_symbols(config))
    unknown = sorted(set(requested) - known)
    if unknown:
        preview = ", ".join(unknown[:8])
        suffix = "..." if len(unknown) > 8 else ""
        raise RuntimeError(
            f"daily_bars backfill symbols are not present in instruments: {preview}{suffix}"
        )
    return requested


# TDX counts Beijing volume in lots; anything further apart is a different row.
_BJ_AMOUNT_REPAIR_LOT = 100
_BJ_AMOUNT_REPAIR_BATCH = 40


def repair_bse_tip_amounts_from_curated(
    config: Config,
    trade_date: date,
    run_id: str,
    symbols: list[str],
) -> dict:
    """Supplement an existing BJ session without re-fetching Sina history.

    This targeted repair uses the curated OHLCV as the cross-check input and
    queries BSE once for its current snapshot. Only rows that receive a
    non-null BSE amount are staged; no price, volume, or historical row is
    invented.
    """
    from cnequity.query.parquet_scan import collect_parquet_root
    from cnequity.steps.http_common import write_fetched

    target = _resolve_daily_bar_scope(config, symbols)
    current = collect_parquet_root(
        config.curated_root / "daily_bars",
        partition_col="trade_date",
        start=trade_date,
        end=trade_date,
        symbols=target,
    )
    current = dedupe_by_primary_key(current, "daily_bars")
    if current.is_empty():
        raise RuntimeError(f"daily_bars {trade_date}: no curated rows found for the repair scope")

    observed = set(current.get_column("symbol").to_list())
    missing = sorted(set(target) - observed)
    candidate = current.filter(pl.col("amount").is_null())
    updated, findings = _supplement_bse_tip_amounts(
        config,
        candidate,
        trade_date=trade_date,
        symbols=target,
    )
    if "source" not in updated.columns:
        updated = with_columns_unless_blank(updated, pl.lit("sina").alias("source"))
    changed = updated.filter(pl.col("amount").is_not_null() & (pl.col("source") == "bse"))
    if not changed.is_empty():
        out = write_fetched(
            config,
            run_id,
            "daily_bars",
            changed,
            source="bse",
            batch_id="bse-tip-repair-0000",
        )
    else:
        out = {"rows_read": 0, "rows_written": 0}

    if missing:
        preview = ", ".join(missing[:8])
        suffix = "..." if len(missing) > 8 else ""
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_bse_tip_repair_missing_rows",
                "message": (
                    f"{len(missing)} requested BJ symbol(s) have no curated row on "
                    f"{trade_date}: {preview}{suffix}"
                ),
                "source": "bse",
                "missing_symbols": len(missing),
            }
        )
    result = {
        "rows_read": current.height,
        "rows_written": int(out.get("rows_written", 0)),
    }
    if findings:
        result["context_updates"] = {"audit_findings": findings}
        if any(
            f.get("severity") == "warning" or f.get("check") == "daily_bars_bse_amount_unavailable"
            for f in findings
        ):
            result["status"] = "warning"
    return result


def _supplement_bj_amounts_from_tdx(
    config: Config,
    merged: pl.DataFrame,
    *,
    start: date,
    end: date,
) -> tuple[pl.DataFrame, list[dict]]:
    """Fill Beijing turnover TDX serves and Sina never published.

    Sina is the historical fallback for Beijing bars and exposes no turnover at
    all: every one of the 505,518 Sina rows in the lake carries a null amount.
    TDX serves the board under market id 2 and does publish it.

    The gate is what makes this a supplement rather than a re-fetch. TDX must
    agree on open/high/low/close to the last digit — measured across 2,048 rows
    against Sina and 3,086 against BSE/Sina/THS, with no exception — and its
    volume must sit within one lot of the stored figure, which is the only way
    the two ever differ (TDX counts in lots; Sina did too through 2025 and has
    been exact since 2026). Anything outside that is a different row, and the
    stored one is left alone as an audit finding.

    Only `amount` is written. Keeping the stored volume costs nothing and saves
    the 7,270 rows where Sina is the finer of the two.
    """
    from cnequity.adapters.tdx_protocol.client import fetch_daily_bars

    symbols = sorted(set(merged.get_column("symbol").to_list()))
    if not symbols:
        return merged, []
    frames: list[pl.DataFrame] = []
    failed: list[str] = []
    for index in range(0, len(symbols), _BJ_AMOUNT_REPAIR_BATCH):
        chunk = symbols[index : index + _BJ_AMOUNT_REPAIR_BATCH]
        try:
            frame = fetch_daily_bars(chunk, start, end, config=config)
        except Exception as exc:  # noqa: BLE001 — the stored rows stay as they are
            logger.warning(
                "TDX Beijing amount supplement failed for %d symbol(s): %s", len(chunk), exc
            )
            failed.extend(chunk)
            continue
        if not frame.is_empty():
            frames.append(
                frame.select(
                    "symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"
                )
            )
    findings: list[dict] = []
    if failed:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_tdx_amount_unavailable",
                "message": f"TDX served no Beijing history for {len(failed)} symbol(s) over {start}..{end}",
                "source": "tdx_protocol",
                "source_limited": True,
            }
        )
    if not frames:
        return merged, findings

    tdx = pl.concat(frames, how="vertical").rename(
        {
            "open": "_tdx_open",
            "high": "_tdx_high",
            "low": "_tdx_low",
            "close": "_tdx_close",
            "volume": "_tdx_volume",
            "amount": "_tdx_amount",
        }
    )
    if "source" not in merged.columns:
        merged = with_columns_unless_blank(merged, pl.lit("sina").alias("source"))
    joined = merged.join(tdx, on=["symbol", "trade_date"], how="left")
    prices_match = pl.all_horizontal(
        pl.col(left) == pl.col(right)
        for left, right in (
            ("open", "_tdx_open"),
            ("high", "_tdx_high"),
            ("low", "_tdx_low"),
            ("close", "_tdx_close"),
        )
    )
    within_a_lot = (pl.col("volume") - pl.col("_tdx_volume")).abs() < _BJ_AMOUNT_REPAIR_LOT
    supplement = (
        pl.col("_tdx_amount").is_not_null()
        & pl.col("amount").is_null()
        & prices_match
        & within_a_lot
    )
    rejected = joined.filter(pl.col("_tdx_amount").is_not_null() & ~(prices_match & within_a_lot))
    # Every candidate has to land somewhere. A key TDX simply did not serve is
    # neither supplied nor rejected, and counting it nowhere is how a repair
    # reports success over rows it never touched — the Beijing board's retired
    # 8xxxxx/430xxx codes are ~218,000 such rows.
    unserved = joined.filter(pl.col("_tdx_amount").is_null())
    supplemented = joined.filter(supplement)
    updated = joined.with_columns(
        pl.when(supplement).then(pl.col("_tdx_amount")).otherwise(pl.col("amount")).alias("amount"),
        pl.when(supplement)
        .then(pl.lit("tdx_protocol"))
        .otherwise(pl.col("source"))
        .alias("source"),
    ).drop("_tdx_open", "_tdx_high", "_tdx_low", "_tdx_close", "_tdx_volume", "_tdx_amount")
    if supplemented.height:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_tdx_amount_supplement",
                "message": (
                    f"TDX supplied amount for {supplemented.height} Beijing row(s) over "
                    f"{start}..{end} after exact OHLC matching"
                ),
                "source": "tdx_protocol",
                "rows_supplemented": supplemented.height,
                "symbols_supplemented": supplemented.get_column("symbol").n_unique(),
            }
        )
    if unserved.height:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_tdx_amount_unserved",
                "message": (
                    f"TDX served no row for {unserved.height} Beijing key(s) over {start}..{end} "
                    f"across {unserved.get_column('symbol').n_unique()} symbol(s); "
                    "those rows keep their null amount"
                ),
                "source": "tdx_protocol",
                "source_limited": True,
                "rows_unserved": unserved.height,
            }
        )
    if rejected.height:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_tdx_amount_mismatch",
                "message": (
                    f"TDX disagreed on price or volume for {rejected.height} Beijing row(s) over "
                    f"{start}..{end}; those rows keep their null amount"
                ),
                "source": "tdx_protocol",
                "rows_rejected": rejected.height,
            }
        )
    return updated, findings


def repair_bj_amounts_from_tdx(
    config: Config,
    start: date,
    end: date,
    run_id: str,
    symbols: list[str] | None,
) -> dict:
    """Supplement stored Beijing rows with the turnover Sina never carried.

    Walks the window a year at a time: the whole board is half a million rows,
    and one staging write of that is neither necessary nor kind to memory.
    """
    from cnequity.query.parquet_scan import collect_parquet_root
    from cnequity.steps.http_common import write_fetched

    target = sorted(
        s
        for s in (
            _resolve_daily_bar_scope(config, symbols)
            if symbols
            else filter_ingest_universe(load_symbols(config), config.ingest_universe)
        )
        if s.endswith(".BJ")
    )
    if not target:
        raise RuntimeError("BJ amount repair needs at least one Beijing symbol in scope")

    rows_read = rows_written = 0
    findings: list[dict] = []
    for index, (lo, hi) in enumerate(_yearly_slices(start, end)):
        current = collect_parquet_root(
            config.curated_root / "daily_bars",
            partition_col="trade_date",
            start=lo,
            end=hi,
            symbols=target,
        )
        if current.is_empty():
            continue
        candidate = dedupe_by_primary_key(current, "daily_bars").filter(pl.col("amount").is_null())
        if candidate.is_empty():
            continue
        rows_read += candidate.height
        updated, slice_findings = _supplement_bj_amounts_from_tdx(
            config, candidate, start=lo, end=hi
        )
        findings.extend(slice_findings)
        changed = updated.filter(
            pl.col("amount").is_not_null() & (pl.col("source") == "tdx_protocol")
        )
        if changed.is_empty():
            continue
        out = write_fetched(
            config,
            run_id,
            "daily_bars",
            changed,
            source="tdx_protocol",
            batch_id=f"bj-amount-repair-{index:04d}",
        )
        rows_written += int(out.get("rows_written", 0))
        logger.info(
            "BJ amount repair %s..%s: %d/%d row(s) supplied",
            lo,
            hi,
            changed.height,
            candidate.height,
        )

    result: dict = {"rows_read": rows_read, "rows_written": rows_written}
    if findings:
        result["context_updates"] = {"audit_findings": findings}
        if any(f.get("severity") == "warning" for f in findings):
            result["status"] = "warning"
    return result


def _yearly_slices(start: date, end: date) -> list[tuple[date, date]]:
    slices: list[tuple[date, date]] = []
    year = start.year
    while year <= end.year:
        slices.append((max(start, date(year, 1, 1)), min(end, date(year, 12, 31))))
        year += 1
    return slices


@register_step(
    "daily_bars",
    group="core",
    depends_on=["instruments", "corporate_actions"],
    requires_workers=True,
)
def step_daily_bars(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    batch_specs = context.get("_retry_batch_specs")
    if batch_specs:
        # Retry windows are encoded on each BatchSpec; tip/multi-day gap-fill
        # still applies using the outer trade_date / per-spec window.
        windows = {(s, e) for _, _, s, e in batch_specs}
        start = min(s for s, _ in windows)
        end = max(e for _, e in windows)
        _reject_unfinished_daily_bar_window(config, end)
        spans = _instrument_spans(config)
        bar_universe = _placeholder_bar_universe(config, spans)
        metadata = instrument_metadata(config)
        evidence = load_negative_evidence(config, "daily_bars", metadata=metadata)
        remaining: list[tuple[str, list[str], date, date]] = []
        fallback_specs: list[tuple[str, list[str], date, date]] = []
        ownership = DailyBarOwnership()
        for batch_id, symbols, spec_start, spec_end in batch_specs:
            symbols = filter_ingest_universe(symbols, config.ingest_universe)
            if not symbols:
                # Every symbol in this batch has left the ingest scope. Retrying
                # it would re-fetch codes this lake no longer ingests, and
                # leaving it failed would block compaction forever.
                from cnequity.orchestrator.manifest import Manifest

                Manifest(config.manifest_path).supersede_batches(
                    run_id,
                    [batch_id],
                    superseded_by="ingest-universe-excluded",
                )
                continue
            status = load_curated_trading_status(
                config,
                start=spec_start,
                end=spec_end,
                symbols=symbols,
            )
            sessions = list_trading_dates(config, spec_start, spec_end)
            routed = classify_daily_bar_ownership(
                symbols,
                spans,
                spec_start,
                spec_end,
                bar_universe=bar_universe,
                trading_status=status,
                trading_sessions=sessions,
                negative_evidence=evidence,
            )
            ownership.generic.extend(routed.generic)
            ownership.delegated_delisted.extend(routed.delegated_delisted)
            ownership.expected_no_data.extend(routed.expected_no_data)
            ownership.placeholder.extend(routed.placeholder)
            ownership.unknown.extend(routed.unknown)
            ownership.negative_cached.extend(routed.negative_cached)
            ownership.no_data_reasons.update(routed.no_data_reasons)
            fetch_scope = list(dict.fromkeys(routed.generic + routed.unknown))
            tdx_symbols, fallback_symbols = split_by_quote_source(fetch_scope)
            if tdx_symbols:
                remaining.append((batch_id, tdx_symbols, spec_start, spec_end))
            if fallback_symbols:
                fallback_specs.append((batch_id, fallback_symbols, spec_start, spec_end))
            if routed.delegated_delisted:
                delegated_id = f"{batch_id}-delegated" if routed.generic else batch_id
                _record_delegated_ownership_batch(
                    config,
                    run_id,
                    routed.delegated_delisted,
                    spec_start,
                    spec_end,
                    batch_id=delegated_id,
                )
            elif not routed.generic and not routed.unknown and not routed.placeholder:
                # The original failed batch now has only proven no-data symbols.
                from cnequity.orchestrator.manifest import Manifest

                Manifest(config.manifest_path).supersede_batches(
                    run_id,
                    [batch_id],
                    superseded_by="ownership-expected-no-data",
                )
            elif not routed.generic and routed.placeholder:
                # Keep the audit distinction above, but do not leave the old
                # worker failure blocking compaction forever.
                from cnequity.orchestrator.manifest import Manifest

                Manifest(config.manifest_path).supersede_batches(
                    run_id,
                    [batch_id],
                    superseded_by="ownership-placeholder",
                )
        result = (
            fetch_daily_bars_parallel(
                config,
                [],
                start,
                end,
                run_id,
                "daily_bars",
                batch_specs=remaining,
            )
            if remaining
            else {"rows_read": 0, "rows_written": 0, "failed_symbols": []}
        )
        sina_result = None
        if fallback_specs:
            sina_result = {
                "rows_read": 0,
                "rows_written": 0,
                "failed_symbols": 0,
                "failed_symbol_names": [],
                "empty_symbol_names": [],
            }
            for batch_id, fallback_symbols, spec_start, spec_end in fallback_specs:
                # No BSE tip on a chunked backfill, so TDX takes the whole spec
                # window and Sina only chases what it could not answer.
                bj_history = _fetch_bj_history_via_tdx(
                    config,
                    fallback_symbols,
                    spec_start,
                    spec_end,
                    run_id,
                    reserve_tip=False,
                )
                sina_symbols = [
                    symbol for symbol in fallback_symbols if symbol not in bj_history["covered"]
                ]
                sina_result["rows_read"] += bj_history["rows_read"]
                sina_result["rows_written"] += bj_history["rows_written"]
                if not sina_symbols:
                    continue
                fallback = fetch_bars_via_sina(
                    config,
                    sina_symbols,
                    spec_start,
                    spec_end,
                    run_id,
                    batch_prefix=f"{batch_id}-sina",
                )
                sina_result["rows_read"] += int(fallback.get("rows_read", 0))
                sina_result["rows_written"] += int(fallback.get("rows_written", 0))
                sina_result["failed_symbols"] += int(fallback.get("failed_symbols", 0))
                sina_result["failed_symbol_names"].extend(fallback.get("failed_symbol_names") or [])
                sina_result["empty_symbol_names"].extend(fallback.get("empty_symbol_names") or [])
                fallback_findings = (fallback.get("context_updates") or {}).get(
                    "audit_findings"
                ) or []
                if fallback_findings:
                    sina_result.setdefault("context_updates", {}).setdefault(
                        "audit_findings", []
                    ).extend(fallback_findings)
        out = _finish_daily_bars(
            config,
            trade_date,
            run_id,
            start=start,
            end=end,
            expected_tdx_symbols=list(
                dict.fromkeys(symbol for _, symbols, _, _ in remaining for symbol in symbols)
            ),
            expected_fallback_symbols=list(
                dict.fromkeys(symbol for _, symbols, _, _ in fallback_specs for symbol in symbols)
            ),
            tdx_result=result,
            sina_result=sina_result,
            expected_no_data_symbols=sorted(
                set(ownership.expected_no_data) - set(ownership.negative_cached)
            ),
        )
        return _merge_ownership_result(out, config, ownership, start, end)

    if getattr(config, "_backfill", False):
        start, end = _backfill_window(config, trade_date)
    else:
        start = incremental_window(config, "daily_bars", trade_date)
        end = trade_date
    _reject_unfinished_daily_bar_window(config, end)

    if getattr(config, "_bj_amount_repair", False):
        return repair_bj_amounts_from_tdx(
            config, start, end, run_id, getattr(config, "_backfill_symbols", None)
        )

    if getattr(config, "_bse_tip_repair", False):
        if start != end:
            raise RuntimeError("BSE tip repair requires a one-session daily_bars window")
        explicit_scope = getattr(config, "_backfill_symbols", None)
        if explicit_scope is None:
            raise RuntimeError("BSE tip repair requires an explicit symbol scope")
        return repair_bse_tip_amounts_from_curated(config, end, run_id, explicit_scope)

    explicit_scope = (
        getattr(config, "_backfill_symbols", None) if getattr(config, "_backfill", False) else None
    )
    symbols = (
        # An explicit repair scope is the operator's own request and is never
        # narrowed; the implicit full-market scope is.
        _resolve_daily_bar_scope(config, explicit_scope)
        if explicit_scope is not None
        else filter_ingest_universe(load_symbols(config), config.ingest_universe)
    )
    rebackfill = context.get("symbols_to_rebackfill") or []
    if rebackfill:
        symbols = list(dict.fromkeys(rebackfill + symbols))

    spans = _instrument_spans(config)
    metadata = instrument_metadata(config)
    ownership = classify_daily_bar_ownership(
        symbols,
        spans,
        start,
        end,
        bar_universe=_placeholder_bar_universe(config, spans),
        trading_status=load_curated_trading_status(
            config,
            start=start,
            end=end,
            symbols=symbols,
        ),
        trading_sessions=list_trading_dates(config, start, end),
        negative_evidence=load_negative_evidence(config, "daily_bars", metadata=metadata),
    )
    _record_delegated_ownership_batch(
        config,
        run_id,
        ownership.delegated_delisted,
        start,
        end,
    )

    # `split_by_quote_source` keeps Beijing out of the main sweep because the
    # rest of the pipeline reads `is_tdx_servable` as "Baostock serves this".
    # The protocol itself does carry Beijing daily bars under market id 2, so
    # the fallback leg below reaches for TDX before Sina — see
    # `_fetch_bj_history_via_tdx`. Tip gaps after TDX are a second routing case
    # (ADR-0005): EastMoney clist.
    fetch_scope = list(dict.fromkeys(ownership.generic + ownership.unknown))
    tdx_symbols, fallback_symbols = split_by_quote_source(fetch_scope)
    reused_symbols = _reuse_successful_daily_bars(config, run_id, fetch_scope, start, end)
    fetch_tdx_symbols = [symbol for symbol in tdx_symbols if symbol not in reused_symbols]
    fetch_fallback_symbols = [symbol for symbol in fallback_symbols if symbol not in reused_symbols]
    # Each exchange publishes its whole board for the session it is currently
    # serving. On a tip-only window that answers for almost every SH/SZ symbol
    # in two requests, leaving the per-symbol sweep only the remainder; on the
    # deep window it is a cheap head start that the sweep then reconciles.
    exchange_tip = _fetch_tip_via_exchange(config, fetch_tdx_symbols, end, run_id)
    if start >= end and exchange_tip["covered"]:
        fetch_tdx_symbols = [s for s in fetch_tdx_symbols if s not in exchange_tip["covered"]]
    result = fetch_daily_bars_parallel(
        config,
        fetch_tdx_symbols,
        start,
        end,
        run_id,
        "daily_bars",
    )
    if exchange_tip["rows_written"]:
        result["rows_read"] = int(result.get("rows_read", 0)) + exchange_tip["rows_read"]
        result["rows_written"] = int(result.get("rows_written", 0)) + exchange_tip["rows_written"]
    sina_result = None
    fallback_start = start
    if fetch_fallback_symbols:
        # The Beijing board's tip comes from the exchange's own paginated
        # snapshot; Sina stays the per-symbol backstop for whatever that misses
        # and for the (short) history window behind it.
        bse = _fetch_bj_tip_via_bse(config, fetch_fallback_symbols, end, run_id)
        fallback_start = _bj_history_start(config, start, end)
        # The history behind the tip comes from TDX in one call per symbol;
        # Sina is left the tip BSE missed and the symbols TDX did not answer.
        bj_history = _fetch_bj_history_via_tdx(
            config, fetch_fallback_symbols, fallback_start, end, run_id, reserve_tip=True
        )
        if fallback_start >= end:
            # Tip-only window: skip every symbol BSE already covered.
            sina_scope = [s for s in fetch_fallback_symbols if s not in bse["covered"]]
        else:
            sina_scope = [
                symbol
                for symbol in fetch_fallback_symbols
                if symbol not in bse["covered"] or symbol not in bj_history["covered"]
            ]
        if sina_scope:
            sina_result = fetch_bars_via_sina(
                config, sina_scope, fallback_start, end, run_id, batch_prefix="sina"
            )
        if bse["rows_written"] or bj_history["rows_written"]:
            sina_result = sina_result or {
                "rows_read": 0,
                "rows_written": 0,
                "failed_symbols": 0,
                "failed_symbol_names": [],
                "empty_symbol_names": [],
            }
            sina_result["rows_read"] += bse["rows_read"] + bj_history["rows_read"]
            sina_result["rows_written"] += bse["rows_written"] + bj_history["rows_written"]
    out = _finish_daily_bars(
        config,
        trade_date,
        run_id,
        start=start,
        fallback_start=fallback_start,
        end=end,
        expected_tdx_symbols=tdx_symbols,
        expected_fallback_symbols=fallback_symbols,
        tdx_result=result,
        sina_result=sina_result,
        expected_no_data_symbols=sorted(
            set(ownership.expected_no_data) - set(ownership.negative_cached)
        ),
    )
    out.setdefault("metrics", {})["cache_hits"] = int(
        out.get("metrics", {}).get("cache_hits", 0) or 0
    ) + len(reused_symbols)
    return _merge_ownership_result(out, config, ownership, start, end)


def _owed_keys_for_symbols(
    config: Config, symbols: Iterable[str], start: date, end: date
) -> set[tuple[str, date]]:
    """The (symbol, session) keys a whole-symbol gap actually owes.

    Clipped to each symbol's own listing window. Owing the full sweep window
    for a symbol listed halfway through it would record sessions that never
    existed, and a debt nothing can ever pay off is worse than no ledger: it
    would sit at the same weight as a real one and drown it.
    """
    spans = _instrument_spans(config)
    sessions = list_trading_dates(config, start, end)
    owed: set[tuple[str, date]] = set()
    for symbol in symbols:
        listed, delisted, _asset = spans.get(symbol, (None, None, None))
        first = max(start, listed) if listed else start
        last = min(end, delisted) if delisted else end
        owed |= {(symbol, day) for day in sessions if first <= day <= last}
    return owed


def _in_bar_universe(symbol: str, universe: str) -> bool:
    try:
        info = parse_symbol(symbol)
    except Exception:  # noqa: BLE001 — an unparseable symbol is simply not ours
        return False
    return in_ingest_universe(info.code, info.exchange, universe)


_LATE_ADMISSION_LOOKBACK_DAYS = 365


def _record_late_admissions(config: Config, run_id: str, symbols: Iterable[str], end: date) -> int:
    """Sessions a security traded before the universe knew about it.

    A security listed after the last instrument refresh is in no sweep until
    that refresh picks it up, and the watermark then moves past the sessions it
    missed, so no incremental run asks for them again. The interior-gap ledger
    does not catch it either: those keys were never expected.

    Measured on 2026-09-17: thirteen BSE securities listed between 07-22 and
    09-04 all took their first bar on 09-07, short 366 sessions between them,
    every one of which TDX still served. Recording them makes `cne backfill
    daily_bars --outstanding` the one way to work this off too.

    Bounded to a year of listings so the scan stays proportional to what can
    plausibly still be owed.
    """
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(root):
        return 0
    horizon = end - timedelta(days=_LATE_ADMISSION_LOOKBACK_DAYS)
    spans = _instrument_spans(config)
    universe = getattr(config, "ingest_universe", "all_a")
    recent = {
        symbol: listed
        for symbol in symbols
        # `daily_bars` does not carry the ETF and LOF quote codes that
        # `instruments` also lists. Owing their sessions would put debts on the
        # ledger that no repair can ever pay off, at the same weight as the
        # real ones — the caller's universe is not trusted to have filtered.
        if _in_bar_universe(symbol, universe)
        and (listed := spans.get(symbol, (None, None, None))[0]) is not None
        and horizon <= listed < end
    }
    if not recent:
        return 0
    first_bar = (
        scan_parquet_root(root, partition_col="trade_date", start=horizon, end=end)
        .filter(pl.col("symbol").is_in(list(recent)))
        .group_by("symbol")
        .agg(pl.col("trade_date").min().alias("first_bar"))
        .collect()
    )
    seen = dict(zip(first_bar["symbol"], first_bar["first_bar"], strict=True))
    owed: set[tuple[str, date]] = set()
    for symbol, listed in recent.items():
        # No bar at all still owes from the listing: the security may have been
        # admitted today and simply not swept yet.
        cutoff = seen.get(symbol, end + timedelta(days=1))
        if cutoff <= listed:
            continue
        owed |= _owed_keys_for_symbols(config, [symbol], listed, cutoff - timedelta(days=1))
    if not owed:
        return 0
    recorded = StateStore(config.meta_root).record_outstanding_keys(
        "daily_bars", owed, run_id=run_id, reason="late_admission"
    )
    logger.warning(
        "%d securit(y/ies) listed before the universe carried them owe %d session(s) "
        "(`cne backfill daily_bars --outstanding`)",
        len({symbol for symbol, _ in owed}),
        recorded,
    )
    return recorded


def _unresolved_budget(config: Config, expected: int, *, tip: bool = False) -> int:
    """How many unresolved keys a sweep may carry without failing.

    A fraction alone misbehaves on a small universe — one symbol out of five is
    20% — so this floors at zero and rounds down: a five-symbol demo tolerates
    nothing, and the whole market tolerates a few dozen.

    ``tip`` reads the daily job's own, tighter knob. The same fraction means
    something else over one session: 1% of a three-year backfill is scatter,
    1% of today is 55 symbols absent from the freshest bar anyone trades on.
    """
    knob = "daily_bars_tip_unresolved_tolerance" if tip else "daily_bars_unresolved_tolerance"
    fraction = float(getattr(config, knob, 0.0) or 0.0)
    if fraction <= 0 or expected <= 0:
        return 0
    return int(expected * fraction)


def _unresolved_key_remedy(
    config: Config,
    run_id: str,
    unknown: set[str] | list[str],
    start: date,
    end: date,
) -> str:
    """What to do about keys no source would resolve.

    The terminal error is the only thing most operators read, and these three
    gates gave a count and nothing to act on: not which keys, not which vendor
    was down, not which command resumes the run. All of it is known here — the
    findings file already holds the per-key reasons — so say it once, in the
    message that actually reaches the terminal.
    """
    findings_path = config.meta_root / "quality" / "findings" / f"{run_id}.json"
    keys = sorted(unknown)
    sample = ",".join(keys[:3]) + (",..." if len(keys) > 3 else "")
    return (
        f"\n  Full list and per-key reasons: {findings_path}"
        "\n  Which vendor is down: cne sources probe"
        f"\n  Resume this run once it is back: cne run retry --run-id {run_id}"
        f"\n  Or repair just these keys: cne backfill daily_bars --symbols {sample} "
        f"--start {start} --end {end}"
    )


def _certify_missing_daily_symbols(
    config: Config,
    symbols: set[str],
    start: date,
    end: date,
    *,
    explicit_no_data: set[str] | None = None,
    source_empty: set[str] | None = None,
) -> tuple[set[str], set[str], DailyBarOwnership]:
    """Split missing keys into evidenced no-data and strict unknown keys.

    ``explicit_no_data`` comes from the pre-fetch ownership classifier or from
    the gap-fill chain after at least two independent per-symbol sources both
    returned a successful empty response. ``source_empty`` therefore contains
    only already-arbitrated empties, never a raw single-vendor absence.
    Transport failures, rate limits, publisher-file omissions, and partial
    status snapshots stay unknown. There is no market-size based allowance.
    """
    requested = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
    if not requested:
        return set(), set(), DailyBarOwnership()
    metadata = instrument_metadata(config)
    spans = {
        row["symbol"]: (row["list_date"], row["delist_date"], row.get("asset_type"))
        for row in metadata.iter_rows(named=True)
    }
    status = load_curated_trading_status(
        config,
        start=start,
        end=end,
        symbols=sorted(requested),
    )
    sessions = list_trading_dates(config, start, end)
    evidence = load_negative_evidence(config, "daily_bars", metadata=metadata)
    ownership = classify_daily_bar_ownership(
        sorted(requested),
        spans,
        start,
        end,
        trading_status=status,
        trading_sessions=sessions,
        negative_evidence=evidence,
    )
    certified = {
        str(symbol).strip().upper() for symbol in (explicit_no_data or ()) if str(symbol).strip()
    }
    certified.update(
        str(symbol).strip().upper() for symbol in (source_empty or ()) if str(symbol).strip()
    )
    certified.update(ownership.expected_no_data)
    certified.intersection_update(requested)
    unknown = requested - certified
    return certified, unknown, ownership


def _record_daily_negative_observations(
    config: Config,
    symbols: set[str],
    start: date,
    end: date,
    *,
    reason: str,
    source: str,
) -> None:
    """Write source-empty observations after the final missing-key split."""
    if symbols:
        record_negative_evidence(
            config,
            "daily_bars",
            symbols,
            start,
            end,
            reason=reason,
            source=source,
        )


def _record_certified_daily_no_data(
    config: Config,
    certified: set[str],
    source_empty: set[str],
    ownership: DailyBarOwnership,
    start: date,
    end: date,
) -> None:
    """Persist fresh, symbol-scoped no-data proofs without extending cache TTL.

    ``ownership.negative_cached`` contains claims merely reused from the
    persistent cache.  Re-saving those claims on every run would turn a TTL
    into a permanent suppression.  Listing/status proofs and fresh upstream
    empty responses are new observations and may refresh their own bounded
    evidence records.
    """
    fresh = set(certified) - set(ownership.negative_cached)
    empty = fresh & set(source_empty)
    if empty:
        _record_daily_negative_observations(
            config,
            empty,
            start,
            end,
            reason="source_empty",
            source="fallback",
        )
    verified = fresh - empty
    if verified:
        _record_daily_negative_observations(
            config,
            verified,
            start,
            end,
            reason="verified_no_data",
            source="instruments_or_trading_status",
        )


def _fetch_tip_via_exchange(
    config: Config, symbols: list[str], trade_date: date, run_id: str
) -> dict:
    """Stage the SSE auction snapshot; leave SZSE report totals to audit.

    SZSE's historical stock report includes turnover outside the intraday
    auction series. Its OHLC can arbitrate prices, but its volume/amount must
    not replace auction-based daily bars from TDX/Sina/THS. Keeping those SZ
    symbols uncovered lets the regular quote path fetch compatible quantities.
    """
    from cnequity.adapters.exchange.daily_quotes import fetch_exchange_daily_quotes
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.storage import StagingWriter

    empty = {"rows_read": 0, "rows_written": 0, "covered": set(), "source_outcomes": {}}
    if not config.sources.get("exchange", True) or not symbols:
        return empty
    wanted = set(symbols)
    try:
        result = fetch_exchange_daily_quotes(trade_date, config=config)
    except Exception as exc:  # noqa: BLE001 — the per-symbol path still owns these
        logger.warning(
            "exchange tip snapshot unavailable (%s: %s); SH/SZ tip falls back to TDX",
            type(exc).__name__,
            exc,
        )
        return {**empty, "source_outcomes": {"exchange": {"status": "failed", "requests": 2}}}

    # `covered` is the point of ExchangeQuotesResult: one exchange answering is
    # not the market answering, and a caller that ignores it reports a
    # SZSE-only snapshot as if it had covered SH too.
    if result.is_empty:
        if result.failures:
            logger.warning(
                "exchange tip snapshot returned nothing (%s); SH/SZ tip falls back to TDX",
                "; ".join(f"{k}: {v}" for k, v in sorted(result.failures.items())),
            )
        return {**empty, "source_outcomes": {"exchange": {"status": "empty", "requests": 2}}}
    if result.failures:
        logger.info(
            "exchange tip snapshot: %s did not answer; those symbols stay with TDX",
            ", ".join(sorted(result.failures)),
        )
    frame = result.quotes.filter(
        pl.col("symbol").is_in(sorted(wanted)) & pl.col("symbol").str.ends_with(".SH")
    )
    if frame.is_empty():
        return {**empty, "source_outcomes": {"exchange": {"status": "empty", "requests": 2}}}

    staged = with_provenance(frame, source="exchange", data_version=data_version_for("daily_bars"))
    StagingWriter(config.staging_root).write_batch(
        "daily_bars", run_id, "exchange-tip-0000", staged
    )
    covered = set(staged["symbol"].to_list())
    logger.info(
        "exchange tip snapshot staged %d SH/SZ bar(s) for %s from %s",
        staged.height,
        trade_date,
        ", ".join(sorted(result.covered)),
    )
    return {
        "rows_read": staged.height,
        "rows_written": staged.height,
        "covered": covered,
        "source_outcomes": {"exchange": {"status": "success", "requests": 2}},
    }


def _bj_history_covered(
    config: Config, run_id: str, symbols: list[str], start: date, end: date
) -> set[str]:
    """Which of *symbols* this run has whole staged evidence for over the window."""
    import polars as pl

    from cnequity.storage import StagingWriter

    files = StagingWriter(config.staging_root).list_run_files("daily_bars", run_id)
    if not files:
        return set()
    staged = set(
        pl.scan_parquet([str(f) for f in files])
        .filter(
            (pl.col("trade_date") >= start)
            & (pl.col("trade_date") <= end)
            & pl.col("symbol").is_in(symbols)
        )
        .select("symbol")
        .unique()
        .collect()["symbol"]
        .to_list()
    )
    return staged - _staged_daily_bar_partial_symbols(config, run_id, list(symbols), start, end)


def _fetch_bj_history_via_tdx(
    config: Config,
    symbols: list[str],
    start: date,
    end: date,
    run_id: str,
    *,
    reserve_tip: bool,
) -> dict:
    """Beijing daily history from TDX, which serves it under market id 2.

    Sina answers one symbol for one session per request — the whole board over
    five sessions is ~2,900 of them, which is what earns HTTP 456 and a
    vendor-wide cooldown. TDX answers a symbol's whole range in one call, so
    the same work is one request per symbol.

    Measured against what the lake already held: open/high/low/close agree to
    the last digit — 3,086 rows over 344 securities against BSE/Sina/THS, and
    2,048 rows over 120 securities against Sina alone, with no exception in
    either. Two differences, both in TDX's favour on balance:

    - ``amount``: Sina publishes none for Beijing, so all 505,518 Sina rows in
      the lake carry a null turnover. TDX fills it, agreeing with BSE to
      0.00003%.
    - ``volume``: TDX reports it in lots. So did Sina for 2019 through 2025 —
      every one of those rows is a multiple of 100 — so the history gains
      nothing coarser than it already had. Only from 2026 is Sina finer (86%
      lots), and there TDX lands a median 50 shares away, one row in 2,048
      further than 100.

    ``reserve_tip`` keeps the current session on the BSE snapshot regardless,
    which publishes exact shares and its own turnover. TDX fills only the
    history behind it, where the alternative was Sina's cost or nothing at all.
    """
    sessions = list_trading_dates(config, start, end)
    if reserve_tip:
        sessions = [day for day in sessions if day < end]
    if not symbols or not sessions:
        return {"rows_read": 0, "rows_written": 0, "covered": set(), "requested": False}
    lo, hi = sessions[0], sessions[-1]
    result = fetch_daily_bars_parallel(config, list(symbols), lo, hi, run_id, "daily_bars")
    failed = set(result.get("failed_symbols") or [])
    covered = _bj_history_covered(config, run_id, list(symbols), lo, hi) - failed
    logger.info(
        "Beijing history via TDX: %d/%d symbol(s) answered over %s..%s (%d rows)",
        len(covered),
        len(symbols),
        lo,
        hi,
        int(result.get("rows_written", 0)),
    )
    return {
        "rows_read": int(result.get("rows_read", 0)),
        "rows_written": int(result.get("rows_written", 0)),
        "covered": covered,
        "requested": True,
    }


# Above this the scope is no longer a repair and the board-wide guard applies.
_BJ_SCOPED_WINDOW_MAX_SYMBOLS = 60


def _bj_history_start(config: Config, start: date, end: date) -> date:
    """Window start for the per-symbol Beijing backstop.

    The dataset's 5-session reconciliation lookback exists for vendors that
    revise settled rows. Running it through Sina costs one request per symbol
    per session — ~2,900 a day for the Beijing board — and that is what earns
    HTTP 456 and a vendor-wide cooldown that then strands unrelated symbols.
    The tip comes from the BSE board snapshot instead, so this only decides how
    far the per-symbol backstop reaches behind it.
    """
    sessions = [day for day in list_trading_dates(config, start, end) if day <= end]
    if not sessions:
        return end
    # A scoped repair is not the daily sweep. The lookback guards the whole
    # board — 580 symbols over five sessions — but truncating an explicit
    # `--symbols` window to its last session is what left fourteen securities
    # owing 225 sessions that the repair kept declining to fetch. The scope
    # itself bounds the cost, so honour the window the caller asked for.
    scoped = list(getattr(config, "_backfill_symbols", None) or [])
    if scoped and len(scoped) <= _BJ_SCOPED_WINDOW_MAX_SYMBOLS:
        return sessions[0]
    lookback = max(int(getattr(config, "bj_history_lookback_days", 1) or 1), 1)
    return sessions[max(0, len(sessions) - lookback)]


def _fetch_bj_tip_via_bse(
    config: Config, symbols: list[str], trade_date: date, run_id: str
) -> dict:
    """Stage the whole Beijing board's tip from the exchange's own snapshot.

    One paginated sweep (~30 requests) replaces one request per symbol (~580),
    and it comes from the exchange rather than a third party. BSE publishes a
    *current* snapshot: `fetch_daily_quotes` returns nothing when its session
    is not `trade_date`, so this can never stamp today's prices onto an older
    session. Anything it does not return stays with the Sina backstop.
    """
    from cnequity.adapters.bse.daily_quotes import fetch_daily_quotes
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.storage import StagingWriter

    if not config.sources.get("bse", True) or not symbols:
        return {"rows_read": 0, "rows_written": 0, "covered": set()}
    try:
        quotes = fetch_daily_quotes(trade_date, symbols=set(symbols), config=config)
    except Exception as exc:  # noqa: BLE001 — Sina still backs this leg
        logger.warning(
            "BSE tip snapshot unavailable (%s: %s); Beijing tip falls back to Sina",
            type(exc).__name__,
            exc,
        )
        return {
            "rows_read": 0,
            "rows_written": 0,
            "covered": set(),
            "source_outcomes": {"bse": {"status": "failed", "requests": 1}},
        }
    if quotes is None or quotes.is_empty():
        return {
            "rows_read": 0,
            "rows_written": 0,
            "covered": set(),
            "source_outcomes": {"bse": {"status": "empty", "requests": 1}},
        }
    staged = with_provenance(quotes, source="bse", data_version=data_version_for("daily_bars"))
    StagingWriter(config.staging_root).write_batch("daily_bars", run_id, "bse-tip-0000", staged)
    covered = set(staged["symbol"].to_list())
    logger.info("BSE tip snapshot staged %d Beijing bar(s) for %s", staged.height, trade_date)
    return {
        "rows_read": staged.height,
        "rows_written": staged.height,
        "covered": covered,
        "source_outcomes": {"bse": {"status": "success", "requests": 1}},
    }


def _finish_daily_bars(
    config: Config,
    trade_date: date,
    run_id: str,
    *,
    start: date,
    end: date,
    expected_tdx_symbols: list[str],
    expected_fallback_symbols: list[str] | None = None,
    # The Beijing leg reconciles a shorter window than the TDX leg (see
    # `_bj_history_start`), so the coverage gate must judge it on the window it
    # was actually asked to fetch. Defaults to `start`, which is the old
    # single-window behaviour.
    fallback_start: date | None = None,
    tdx_result: dict,
    sina_result: dict | None,
    expected_no_data_symbols: list[str] | None = None,
) -> dict:
    """Apply gap-fill and validate the latest fetched session.

    ``trade_date`` is the job's as-of date, while ``end`` is the session
    actually fetched. They differ for a historical ``cne backfill`` run (for
    example a weekend repair), so all staging and pre-open checks must use
    ``end``. Normal daily runs happen to have the same two dates.
    """
    rows_read = int(tdx_result.get("rows_read", 0))
    rows_written = int(tdx_result.get("rows_written", 0))
    findings: list[dict] = []
    failed_symbols = list(tdx_result.get("failed_symbols") or [])
    fallback_failed_symbols: set[str] = set()
    explicit_no_data = {
        str(symbol).strip().upper()
        for symbol in (expected_no_data_symbols or [])
        if str(symbol).strip()
    }
    source_empty_symbols: set[str] = set()
    source_attempts: list[dict] = []

    def capture_source_outcomes(payload: dict | None, stage: str) -> None:
        for source, outcome in ((payload or {}).get("source_outcomes") or {}).items():
            source_attempts.append({"stage": stage, "source": source, **dict(outcome)})

    # Ownership was evaluated before this finalization call.  Persist those
    # fresh listing/status proofs even when every requested symbol was routed
    # out of the fetch sets and therefore there is no later missing-key pass.
    if explicit_no_data:
        _record_daily_negative_observations(
            config,
            explicit_no_data,
            start,
            end,
            reason="verified_no_data",
            source="instruments_or_trading_status",
        )

    if sina_result:
        rows_read += int(sina_result.get("rows_read", 0))
        rows_written += int(sina_result.get("rows_written", 0))
        fallback_failed_symbols = set(sina_result.get("failed_symbol_names") or [])
        sina_findings = (sina_result.get("context_updates") or {}).get("audit_findings") or []
        findings.extend(sina_findings)
        capture_source_outcomes(sina_result, "initial_fallback")

    tip = start == end
    historical_tip = tip and end != trade_date
    fallback_window_start = fallback_start or start
    fallback_set = set(expected_fallback_symbols or [])

    def _leg_window(symbols) -> tuple[date, date]:
        """The window a gap-fill owes for *symbols*.

        Narrowing the initial fetch for the Beijing leg without narrowing its
        gap-fill only moves the cost: the leg is asked for one session, and
        then the recovery chain chases five of them one symbol at a time
        through THS at 1 req/s — 644 silent seconds of a 772-second step.
        Sessions this run never requested are not this run's gap.
        """
        if fallback_window_start != start and symbols and set(symbols) <= fallback_set:
            return fallback_window_start, end
        return start, end

    if tip:
        expected_symbols = set(expected_tdx_symbols) | set(expected_fallback_symbols or [])
        if not historical_tip:
            staged_before_gapfill = _staged_daily_bar_symbols(config, run_id, end)
            had_tip_gap = bool(expected_symbols - staged_before_gapfill)
            gap = _gapfill_tip_via_clist(config, end, run_id, expected_symbols=expected_tdx_symbols)
            rows_read += int(gap.get("rows_read", 0))
            rows_written += int(gap.get("rows_written", 0))
            findings.extend(gap.get("audit_findings") or [])
            capture_source_outcomes(gap, "tip_clist_gapfill")
            # A clean primary day still needs an independent peer capture. The
            # gap-fill path already captures one when it had to query the
            # clist; avoid issuing that expensive full-market request twice.
            if not had_tip_gap and expected_symbols:
                from cnequity.quality.failover import (
                    failover_spec,
                    snapshot_daily_bars_clist,
                )

                failover = failover_spec(config, "daily_bars")
                if failover is not None and failover.snapshot_cadence == "daily":
                    try:
                        backup_snapshot = snapshot_daily_bars_clist(
                            config,
                            trade_date=end,
                            run_id=run_id,
                            batch_id="em-clist-independent-snapshot",
                            symbols=sorted(expected_symbols),
                        )
                        if backup_snapshot is None or backup_snapshot.is_empty():
                            findings.append(
                                {
                                    "dataset": "daily_bars",
                                    "severity": "warning",
                                    "check": "backup_snapshot_unavailable",
                                    "message": (
                                        "daily_bars independent backup snapshot returned no rows; "
                                        "primary data was not marked erroneous"
                                    ),
                                    "peer_unavailable": True,
                                    "retryable": True,
                                }
                            )
                    except Exception as exc:  # noqa: BLE001 — peer is best-effort
                        logger.warning(
                            "daily_bars: independent backup snapshot unavailable (%s: %s); "
                            "primary result remains valid and will be retried",
                            type(exc).__name__,
                            exc,
                        )
                        findings.append(
                            {
                                "dataset": "daily_bars",
                                "severity": "warning",
                                "check": "backup_snapshot_unavailable",
                                "message": (
                                    "daily_bars independent backup snapshot was unavailable; "
                                    "primary data was not marked erroneous"
                                ),
                                "peer_unavailable": True,
                                "retryable": True,
                            }
                        )

        missing_staged = sorted(expected_symbols - _staged_daily_bar_symbols(config, run_id, end))
        if missing_staged:
            # clist is a live snapshot: it can supplement today's close but
            # must never be re-stamped onto an older retry date. Per-symbol
            # kline is also the bounded second chance for today's clist misses.
            kline = _gapfill_multiday_via_kline(
                config,
                run_id,
                symbols=missing_staged,
                start=end,
                end=end,
                require_complete=False,
            )
            rows_read += int(kline.get("rows_read", 0))
            rows_written += int(kline.get("rows_written", 0))
            findings.extend(kline.get("audit_findings") or [])
            capture_source_outcomes(kline, "tip_kline_gapfill")
            explicit_no_data.update(kline.get("expected_no_data_symbols") or [])
            source_empty_symbols.update(kline.get("expected_no_data_symbols") or [])
    elif failed_symbols or expected_tdx_symbols or expected_fallback_symbols:
        all_expected_symbols = list(
            dict.fromkeys((expected_tdx_symbols or []) + (expected_fallback_symbols or []))
        )
        partial_symbols = _staged_daily_bar_partial_symbols(
            config, run_id, all_expected_symbols, start, end
        )
        failed_set = set(failed_symbols) | fallback_failed_symbols
        if failed_set:
            gap_start, gap_end = _leg_window(failed_set)
            gap = _gapfill_multiday_via_kline(
                config,
                run_id,
                symbols=sorted(failed_set),
                start=gap_start,
                end=gap_end,
            )
            rows_read += int(gap.get("rows_read", 0))
            rows_written += int(gap.get("rows_written", 0))
            findings.extend(gap.get("audit_findings") or [])
            capture_source_outcomes(gap, "failed_batch_gapfill")
            explicit_no_data.update(gap.get("expected_no_data_symbols") or [])
            source_empty_symbols.update(gap.get("expected_no_data_symbols") or [])
            # A source can complete the failed symbol set in two valid ways:
            # it may stage replacement rows, or it may prove that every
            # unresolved symbol has no bars in this window (for example a
            # suspended/new ETF).  The latter deliberately has
            # ``filled=False`` but must not keep the whole market snapshot in
            # a failed state.  Do not clear errors for an unattempted fallback
            # with neither rows nor explicit expected-no-data evidence.
            if gap.get("complete", False) and (
                gap.get("filled") or gap.get("expected_no_data_symbols")
            ):
                _resolve_recovered_daily_batches(config, run_id, resolved_symbols=failed_set)

        partial_only = sorted(partial_symbols - failed_set)
        if partial_only:
            gap_start, gap_end = _leg_window(partial_only)
            gap = _gapfill_multiday_via_kline(
                config,
                run_id,
                symbols=partial_only,
                start=gap_start,
                end=gap_end,
                require_complete=False,
            )
            rows_read += int(gap.get("rows_read", 0))
            rows_written += int(gap.get("rows_written", 0))
            findings.extend(gap.get("audit_findings") or [])
            capture_source_outcomes(gap, "partial_key_gapfill")
            explicit_no_data.update(gap.get("expected_no_data_symbols") or [])
            source_empty_symbols.update(gap.get("expected_no_data_symbols") or [])

    _reject_preopen_placeholder(config, run_id, end)

    # Symbols proven to have no data in this window, collected by the
    # certification below so the interior-session gate does not re-report them.
    certified_no_data: set[str] = set()
    unresolved_tolerated: set[str] = set()

    if tip:
        staged = _staged_daily_bar_symbols(config, run_id, end)
        expected_symbols = set(expected_tdx_symbols) | set(expected_fallback_symbols or [])
        missing_staged = expected_symbols - staged
        if missing_staged:
            certified, unknown, ownership = _certify_missing_daily_symbols(
                config,
                set(missing_staged),
                end,
                end,
                explicit_no_data=explicit_no_data,
                source_empty=source_empty_symbols,
            )
            if certified:
                certified_no_data.update(certified)
                _record_certified_daily_no_data(
                    config,
                    certified,
                    source_empty_symbols,
                    ownership,
                    end,
                    end,
                )
                findings.append(
                    {
                        "dataset": "daily_bars",
                        "severity": "info",
                        "check": "daily_bars_expected_no_data",
                        "message": (
                            f"daily_bars {end}: {len(certified)} missing tip key(s) "
                            "were excluded by explicit listing/status/negative evidence"
                        ),
                        "symbols": sorted(certified),
                        "reasons": {
                            symbol: ownership.no_data_reasons.get(symbol, "source_empty")
                            for symbol in sorted(certified)
                        },
                    }
                )
            if unknown:
                preview = ", ".join(sorted(unknown)[:8])
                suffix = "..." if len(unknown) > 8 else ""
                budget = _unresolved_budget(config, len(expected_symbols), tip=True)
                # Nothing staged is not a residue, it is an outage: the whole
                # session failed and there is nothing to publish anyway.
                tolerated = bool(staged) and len(unknown) <= budget
                remedy = _unresolved_key_remedy(config, run_id, unknown, end, end)
                headline = (
                    f"daily_bars {end}: {len(unknown)} expected tip key(s) remain "
                    f"unknown after failover ({preview}{suffix})"
                )
                findings.append(
                    {
                        "dataset": "daily_bars",
                        "severity": "warning" if tolerated else "error",
                        "check": "daily_bars_unknown_missing_symbols",
                        "message": (
                            f"{headline}; "
                            + (
                                f"within the {budget}-key tip tolerance, so the run continues"
                                if tolerated
                                else "refusing to checkpoint"
                            )
                        ),
                        "missing_keys": len(unknown),
                        "symbols": sorted(unknown),
                        "tolerated": tolerated,
                        "tolerance_keys": budget,
                    }
                )
                if not tolerated:
                    persist_step_findings(config, run_id, end, findings)
                    if not staged:
                        raise RuntimeError(
                            f"daily_bars {end}: primary/fallback and EastMoney clist/kline "
                            f"gap-fill produced no staged tip rows for {len(unknown)} "
                            f"unknown key(s) ({preview}{suffix})." + remedy
                        )
                    raise RuntimeError(
                        f"{headline}; refusing to checkpoint a partial market snapshot." + remedy
                    )
                owed = StateStore(config.meta_root).record_outstanding_keys(
                    "daily_bars",
                    {(symbol, end) for symbol in unknown},
                    run_id=run_id,
                    reason="unresolved_tip",
                )
                unresolved_tolerated.update(unknown)
                logger.warning(
                    "%s; within the %d-key tip tolerance, so the run continues "
                    "(%d key(s) now owed — `cne backfill daily_bars --outstanding`).%s",
                    headline,
                    budget,
                    owed,
                    remedy,
                )
        if expected_symbols:
            _resolve_recovered_daily_batches(
                config,
                run_id,
                resolved_symbols=expected_symbols,
            )
    elif expected_tdx_symbols or expected_fallback_symbols:
        # A vendor may report a nominally successful response while omitting
        # one symbol×session key.  Validate the staged key set independently
        # of its transport-level error bit; otherwise a partial success could
        # bypass the strict unknown classification below.
        all_expected_symbols = list(
            dict.fromkeys((expected_tdx_symbols or []) + (expected_fallback_symbols or []))
        )
        staged = _staged_daily_bar_symbols(config, run_id, end)
        missing_staged = set(all_expected_symbols) - staged
        if missing_staged:
            certified, unknown, ownership = _certify_missing_daily_symbols(
                config,
                missing_staged,
                start,
                end,
                explicit_no_data=explicit_no_data,
                source_empty=source_empty_symbols,
            )
            if certified:
                certified_no_data.update(certified)
                _record_certified_daily_no_data(
                    config,
                    certified,
                    source_empty_symbols,
                    ownership,
                    start,
                    end,
                )
                findings.append(
                    {
                        "dataset": "daily_bars",
                        "severity": "info",
                        "check": "daily_bars_expected_no_data",
                        "message": (
                            f"daily_bars {start}..{end}: {len(certified)} missing key(s) "
                            "were excluded by explicit listing/status/negative evidence"
                        ),
                        "symbols": sorted(certified),
                        "reasons": {
                            symbol: ownership.no_data_reasons.get(symbol, "source_empty")
                            for symbol in sorted(certified)
                        },
                    }
                )
            if unknown:
                preview = ", ".join(sorted(unknown)[:8])
                suffix = "..." if len(unknown) > 8 else ""
                # A handful of symbols lost to a vendor's transient outage is
                # not a partial market. Refusing the checkpoint over 14 of
                # 5,500 discarded two hours of `cne init` and left phases 3 and
                # 4 unrun; the hole is smaller than the cost of throwing the
                # sweep away, and it stays visible in the finding, in the
                # remedy, and in `cne verify` until it is filled.
                budget = _unresolved_budget(config, len(all_expected_symbols))
                tolerated = len(unknown) <= budget
                remedy = _unresolved_key_remedy(config, run_id, unknown, start, end)
                headline = (
                    f"daily_bars {start}..{end}: {len(unknown)} expected key(s) remain "
                    f"unknown after failover ({preview}{suffix})"
                )
                findings.append(
                    {
                        "dataset": "daily_bars",
                        "severity": "warning" if tolerated else "error",
                        "check": "daily_bars_unknown_missing_symbols",
                        "message": (
                            f"{headline}; "
                            + (
                                f"within the {budget}-key tolerance, so the run continues"
                                if tolerated
                                else "refusing to checkpoint"
                            )
                        ),
                        "missing_keys": len(unknown),
                        "symbols": sorted(unknown),
                        "tolerated": tolerated,
                        "tolerance_keys": budget,
                    }
                )
                if not tolerated:
                    persist_step_findings(config, run_id, end, findings)
                    raise RuntimeError(
                        f"{headline}; refusing to checkpoint a partial market snapshot." + remedy
                    )
                # Clipped to each symbol's own listing window. Recording the
                # whole sweep window for a symbol listed halfway through it
                # would owe sessions that never existed, and a debt nothing can
                # ever pay off is worse than no ledger at all.
                owed_pairs = _owed_keys_for_symbols(config, unknown, start, end)
                owed = StateStore(config.meta_root).record_outstanding_keys(
                    "daily_bars", owed_pairs, run_id=run_id, reason="unresolved_symbol"
                )
                unresolved_tolerated.update(unknown)
                logger.warning(
                    "%s; continuing (%d key(s) now owed — "
                    "`cne backfill daily_bars --outstanding`).%s",
                    headline,
                    owed,
                    remedy,
                )

    # A source can return at least one row for every symbol while silently
    # omitting an interior session.  The symbol-level certification above
    # cannot see that case, so validate the full ``symbol×session`` key set
    # before allowing any batch to remain successful.  Mark the owning worker
    # batches stale so the normal retry path will fetch the exact window again;
    # otherwise a raised step would leave successful receipts that
    # ``retry_failed_only`` is allowed to skip.
    #
    # This runs *after* certification on purpose.  It used to run before, which
    # meant a window with any unfillable key raised here and the certification
    # never executed — and since certification is the only writer of daily-bar
    # negative evidence, that cache stayed permanently empty and every proven
    # dead symbol was re-fetched from every vendor on every run.
    if not tip and (expected_tdx_symbols or expected_fallback_symbols):
        # Each leg is judged on the window it was asked for. Holding the
        # Beijing symbols to the TDX window would report every session behind
        # their own lookback as an interior gap.
        legs = (
            ([s for s in (expected_tdx_symbols or []) if s not in certified_no_data], start),
            (
                [s for s in (expected_fallback_symbols or []) if s not in certified_no_data],
                fallback_start or start,
            ),
        )
        missing_pairs: set[tuple[str, date]] = set()
        for leg_symbols, leg_start in legs:
            if leg_symbols and leg_start < end:
                missing_pairs |= _staged_daily_bar_missing_keys(
                    config, run_id, list(dict.fromkeys(leg_symbols)), leg_start, end
                )
        if missing_pairs:
            missing_symbols = {symbol for symbol, _day in missing_pairs}
            # Judged against the keys this sweep actually asked for, not the
            # symbol count: an interior gap is a symbol×session hole, and 5,037
            # of them across a three-year window is 0.12% — the size that used
            # to take the whole of `cne init` down with it.
            expected_keys = 0
            for leg_symbols, leg_start in legs:
                if leg_symbols and leg_start < end:
                    sessions = list_trading_dates(config, leg_start, end)
                    expected_keys += len(set(leg_symbols)) * max(len(sessions), 1)
            budget = _unresolved_budget(config, expected_keys)
            tolerated = len(missing_pairs) <= budget
            remedy = _unresolved_key_remedy(config, run_id, missing_symbols, start, end)
            headline = (
                f"daily_bars {start}..{end}: {len(missing_pairs)} interior "
                f"symbol×session key(s) remain absent across {len(missing_symbols)} symbol(s)"
            )
            finding = {
                "dataset": "daily_bars",
                "severity": "warning" if tolerated else "error",
                "check": "daily_bars_interior_gap",
                "message": (
                    f"{headline}; "
                    + (
                        f"within the {budget}-key tolerance, so the run continues"
                        if tolerated
                        else "refusing to checkpoint"
                    )
                    + remedy
                ),
                "missing_keys": len(missing_pairs),
                "missing_symbols": sorted(missing_symbols),
                "tolerated": tolerated,
                "tolerance_keys": budget,
                "expected_keys": expected_keys,
                "sample_keys": [
                    {"symbol": symbol, "trade_date": day.isoformat()}
                    for symbol, day in sorted(missing_pairs)[:8]
                ],
            }
            findings.append(finding)
            if not tolerated:
                _mark_unresolved_daily_bar_batches(
                    config,
                    run_id,
                    missing_pairs,
                )
                persist_step_findings(config, run_id, end, findings)
                raise RuntimeError(finding["message"])
            # Checkpointing past a hole means no incremental run will ever ask
            # for these sessions again — the watermark has moved over them. The
            # ledger is the only thing that remembers, and `cne backfill
            # daily_bars --outstanding` is what works it off.
            outstanding = StateStore(config.meta_root).record_outstanding_keys(
                "daily_bars", missing_pairs, run_id=run_id, reason="interior_gap"
            )
            unresolved_tolerated.update(missing_symbols)
            logger.warning(
                "%s; within the %d-key tolerance, so the run continues "
                "(%d key(s) now owed — `cne backfill daily_bars --outstanding`).%s",
                headline,
                budget,
                outstanding,
                remedy,
            )

    # Not `all_expected_symbols`: that name is bound only in the two elif
    # branches above, and the tip path reaches here without it.
    _record_late_admissions(
        config,
        run_id,
        dict.fromkeys((expected_tdx_symbols or []) + (expected_fallback_symbols or [])),
        end,
    )

    result: dict = {"rows_read": rows_read, "rows_written": rows_written}
    metrics = dict(tdx_result.get("metrics") or {})
    # The fallback scope is known even when its upstream call returns no
    # rows. Recording requested fallback work is more useful than inferring
    # it from output rows (which would hide a failed fallback).
    fallback_scope = len(expected_fallback_symbols or [])
    if fallback_scope:
        metrics["fallback_requests"] = (
            int(metrics.get("fallback_requests", 0) or 0) + fallback_scope
        )
    metrics["rows_read"] = rows_read
    metrics["rows_written"] = rows_written
    taxonomy = {
        "rate_limited": 0,
        "source_empty": 0,
        "proxy_failed": 0,
        "direct_failed": 0,
        "circuit_open": 0,
        "transport_error": 0,
        "http_error": 0,
    }
    for attempt in source_attempts:
        reasons = attempt.get("failure_reasons") or {}
        if isinstance(reasons, dict):
            taxonomy["rate_limited"] += int(reasons.get("rate_limited", 0) or 0)
            taxonomy["circuit_open"] += int(reasons.get("circuit_open", 0) or 0)
            taxonomy["transport_error"] += sum(
                value == "transport_error" for value in reasons.values()
            )
            taxonomy["http_error"] += int(reasons.get("http_error", 0) or 0)
        taxonomy["source_empty"] += int(attempt.get("empty_symbols", 0) or 0)
        taxonomy["proxy_failed"] += int(attempt.get("proxy_failed", 0) or 0)
        taxonomy["direct_failed"] += int(attempt.get("direct_failed", 0) or 0)
    metrics["source_failures"] = taxonomy
    result["metrics"] = metrics
    if source_attempts:
        result["source_outcomes"] = source_attempts
    if findings:
        result["context_updates"] = {"audit_findings": findings}
    if unresolved_tolerated:
        # Tolerated, not invisible: the caller reports a warning rather than a
        # clean success, and the keys travel with it.
        result["status"] = "warning"
        result["unresolved_symbols"] = sorted(unresolved_tolerated)
        # The work itself is finished — the rows are staged and the shortfall is
        # in the outstanding ledger — so the batch must settle. Leaving it
        # `warning` had compact skip the whole dataset for one batch: a measured
        # init staged 3,894,608 rows and published none of them.
        result["batch_settled"] = True
    return result


def _resolve_recovered_daily_batches(
    config: Config, run_id: str, *, resolved_symbols: set[str]
) -> None:
    """Unblock only worker attempts whose failed symbols were verified downstream."""
    from cnequity.orchestrator.manifest import Manifest

    manifest = Manifest(config.manifest_path)
    for batch in manifest.get_failed_batches(run_id):
        if batch["dataset"] != "daily_bars" or batch["task_id"] != "daily_bars":
            continue
        batch_symbols = set(json.loads(batch["symbols_json"] or "[]"))
        if not batch_symbols or not batch_symbols.issubset(resolved_symbols):
            continue
        manifest.resolve_failed_batch(
            run_id,
            batch["batch_id"],
            error_message=(
                "resolved by exchange/Sina/EastMoney/THS gap-fill or verified expected no-data"
            ),
        )


def _mark_unresolved_daily_bar_batches(
    config: Config,
    run_id: str,
    missing_keys: set[tuple[str, date]],
) -> None:
    """Schedule single-session child batches for exact missing keys.

    The original worker batch and its staged rows stay immutable.  Grouping by
    date gives the existing worker API an exact request window, while chunking
    by ``batch_size`` prevents a market-wide incident from creating one
    unbounded recovery request.
    """
    if not missing_keys:
        return
    from cnequity.orchestrator.manifest import Manifest

    manifest = Manifest(config.manifest_path)
    by_date: dict[date, list[str]] = {}
    for symbol, session in sorted(missing_keys):
        by_date.setdefault(session, []).append(symbol)
    chunk_size = max(1, int(config.batch_size))
    child_ids: list[str] = []
    for session, day_symbols in sorted(by_date.items()):
        for offset in range(0, len(day_symbols), chunk_size):
            chunk = day_symbols[offset : offset + chunk_size]
            scope_digest = hashlib.sha1(  # noqa: S324 — stable id, not security
                "\n".join(chunk).encode("utf-8")
            ).hexdigest()[:10]
            batch_id = (
                f"daily-gap-{session.isoformat()}-{scope_digest}-batch-{offset // chunk_size:04d}"
            )
            child_ids.append(batch_id)
            manifest.start_batch(
                run_id,
                batch_id,
                task_id="daily_bars",
                dataset="daily_bars",
                symbols=chunk,
                window_start=session.isoformat(),
                window_end=session.isoformat(),
            )
            manifest.mark_batch_stale(
                run_id,
                batch_id,
                "daily_bars exact symbol×session gap requires retry",
            )

    scheduled_symbols = {symbol for symbol, _session in missing_keys}
    replaceable: list[str] = []
    for batch in manifest.get_failed_batches(run_id):
        if batch["dataset"] != "daily_bars" or batch["task_id"] != "daily_bars":
            continue
        batch_symbols = set(json.loads(batch["symbols_json"] or "[]"))
        if batch_symbols and batch_symbols.issubset(scheduled_symbols):
            replaceable.append(batch["batch_id"])
    if replaceable:
        manifest.supersede_batches(
            run_id,
            replaceable,
            superseded_by="exact daily-gap child batches",
            replacement_pending=True,
        )


def _staged_daily_bar_symbols(config: Config, run_id: str, trade_date: date | None) -> set[str]:
    import polars as pl

    from cnequity.storage import StagingWriter

    files = StagingWriter(config.staging_root).list_run_files("daily_bars", run_id)
    if not files:
        return set()
    lf = pl.scan_parquet([str(f) for f in files]).select("symbol", "trade_date")
    if trade_date is not None:
        lf = lf.filter(pl.col("trade_date") == trade_date)
    return set(lf.select("symbol").unique().collect()["symbol"].to_list())


def _staged_daily_bar_partial_symbols(
    config: Config,
    run_id: str,
    symbols: list[str],
    start: date,
    end: date,
) -> set[str]:
    """Find symbols with a missing expected session in a staged window.

    A symbol can legitimately have no rows before listing, after delisting, or
    during a suspension. Instrument metadata narrows the expected range for
    the first two cases; when metadata is unavailable, use the requested range
    so a vendor response cannot hide a missing leading or trailing session.
    """
    if start >= end or not symbols:
        return set()
    return {
        symbol
        for symbol, _day in _staged_daily_bar_missing_keys(config, run_id, symbols, start, end)
    }


def _staged_daily_bar_missing_keys(
    config: Config,
    run_id: str,
    symbols: list[str],
    start: date,
    end: date,
) -> set[tuple[str, date]]:
    """Return missing session keys for symbols that have partial evidence."""
    if start >= end or not symbols:
        return set()
    import polars as pl

    from cnequity.storage import StagingWriter

    files = StagingWriter(config.staging_root).list_run_files("daily_bars", run_id)
    if not files:
        return set()
    staged = (
        pl.scan_parquet([str(f) for f in files])
        .filter(
            (pl.col("trade_date") >= start)
            & (pl.col("trade_date") <= end)
            & pl.col("symbol").is_in(symbols)
        )
        .select("symbol", "trade_date")
        .unique()
        .collect()
    )
    if staged.is_empty():
        return set()

    sessions = list_trading_dates(config, start, end)
    observed_symbols = set(staged["symbol"].to_list())
    missing: set[tuple[str, date]] = set()
    metadata = _instrument_spans(config)
    status_by_symbol: dict[str, dict[date, bool | None]] = {}
    status = load_curated_trading_status(
        config,
        start=start,
        end=end,
        symbols=sorted(observed_symbols),
    )
    if status is not None and not status.is_empty():
        required_status = {"symbol", "trade_date", "is_trading"}
        if required_status.issubset(status.columns):
            for status_row in status.select(*sorted(required_status)).iter_rows(named=True):
                status_by_symbol.setdefault(str(status_row["symbol"]), {})[
                    status_row["trade_date"]
                ] = status_row["is_trading"]
    # Suspensions this run learned from the vendor are staged but not yet
    # curated, so they are not in `status_by_symbol` and would otherwise be
    # condemned here as interior gaps.
    learned = getattr(config, "_learned_suspensions", None)
    learned_suspensions: set[tuple[str, date]] = (
        learned["keys"] if isinstance(learned, dict) and learned.get("keys") else set()
    )
    observed = staged.group_by("symbol").agg(pl.col("trade_date").unique().alias("dates"))
    # Live negative evidence is the same class of independently inspectable
    # fact as an explicit non-trading status row: a key covered by a bounded,
    # identity-matched source-empty proof is certified absent, not missing.
    # Without this, bootstrap init can never pass the gate — the records are
    # written after the gate's split, so an uncertified deadlock would repeat
    # on every resume.
    evidence_by_symbol: dict[str, list[dict]] = {}
    for item in load_negative_evidence(config, "daily_bars"):
        evidence_by_symbol.setdefault(
            str(item.get("symbol", "")).strip().upper(), []
        ).append(item)
    for row in observed.iter_rows(named=True):
        span = metadata.get(row["symbol"], (None, None, None))
        list_date, delist_date = span[:2]
        expected_start = max(start, list_date) if list_date is not None else start
        expected_end = min(end, delist_date) if delist_date is not None else end
        expected = {session for session in sessions if expected_start <= session <= expected_end}
        missing.update(
            (row["symbol"], day)
            for day in expected - set(row["dates"])
            if status_by_symbol.get(row["symbol"], {}).get(day) is not False
            and (row["symbol"], day) not in learned_suspensions
            and not any(
                negative_evidence_covers(item, row["symbol"], day, day)
                for item in evidence_by_symbol.get(row["symbol"], ())
            )
        )
    # A symbol with no rows at all is handled by the explicit no-data/unknown
    # classifier.  This helper is specifically the interior partial-evidence
    # gate and must not turn a whole-symbol empty response into a duplicate
    # error path.
    return {key for key in missing if key[0] in observed_symbols}


def _gapfill_tip_via_clist(
    config: Config,
    trade_date: date,
    run_id: str,
    *,
    expected_symbols: list[str],
) -> dict:
    """Route missing tip keys through one EastMoney clist snapshot (ADR-0005)."""
    import polars as pl

    from cnequity.adapters.eastmoney.bars import fetch_daily_bars_clist
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.quality.failover import failover_spec, snapshot_daily_bars_clist
    from cnequity.storage import StagingWriter

    if not expected_symbols:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "filled": False,
            "source_outcomes": {"eastmoney": {"status": "not_needed", "requests": 0}},
        }
    staged = _staged_daily_bar_symbols(config, run_id, trade_date)
    missing = [s for s in expected_symbols if s not in staged]
    if not missing:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "filled": False,
            "source_outcomes": {"eastmoney": {"status": "not_needed", "requests": 0}},
        }

    spec = failover_spec(config, "daily_bars")
    if spec is None or not config.sources.get(spec.backup, True):
        return {
            "rows_read": 0,
            "rows_written": 0,
            "filled": False,
            "source_outcomes": {"eastmoney": {"status": "disabled", "requests": 0}},
            "audit_findings": [
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_clist_gapfill",
                    "message": (
                        f"{len(missing)} tip key(s) missing after TDX but eastmoney "
                        "backup is disabled; curated tip stays sparse"
                    ),
                }
            ],
        }

    # One full clist pull, then keep only missing keys so compact cannot
    # overwrite successful TDX rows for the same PK (keep=last by fetched_at).
    full = fetch_daily_bars_clist(trade_date, config=config)
    if full.is_empty():
        return {
            "rows_read": 0,
            "rows_written": 0,
            "filled": False,
            "source_outcomes": {
                "eastmoney": {"status": "empty", "requests": 1, "empty_symbols": len(missing)}
            },
            "audit_findings": [
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_clist_gapfill",
                    "message": (
                        f"{len(missing)} tip key(s) missing after TDX; "
                        "EastMoney clist returned no rows"
                    ),
                }
            ],
        }

    snapshot_daily_bars_clist(
        config,
        trade_date=trade_date,
        run_id=run_id,
        batch_id="em-clist-snapshot",
        df=full,
    )
    missing_set = set(missing)
    gap_df = full.filter(pl.col("symbol").is_in(list(missing_set)))
    if gap_df.is_empty():
        return {
            "rows_read": full.height,
            "rows_written": 0,
            "filled": False,
            "source_outcomes": {
                "eastmoney": {
                    "status": "source_missing_keys",
                    "requests": 1,
                    "empty_symbols": len(missing),
                }
            },
            "audit_findings": [
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_clist_gapfill",
                    "message": (
                        f"clist had {full.height} rows but none of the "
                        f"{len(missing)} missing tip key(s)"
                    ),
                }
            ],
        }

    gap_df = with_provenance(
        gap_df, source=spec.backup, data_version=data_version_for("daily_bars")
    )
    batch_id = "em-clist-gapfill"
    filled_syms = sorted(set(gap_df["symbol"].to_list()))
    manifest = Manifest(config.manifest_path)
    manifest.start_batch(
        run_id,
        batch_id,
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=filled_syms,
        window_start=trade_date.isoformat(),
        window_end=trade_date.isoformat(),
    )
    StagingWriter(config.staging_root).write_batch("daily_bars", run_id, batch_id, gap_df)
    manifest.finish_batch(
        run_id,
        batch_id,
        "success",
        rows_read=gap_df.height,
        rows_written=gap_df.height,
    )
    logger.warning(
        "daily_bars tip gap-fill: staged %s EastMoney clist row(s) for %s missing key(s)",
        gap_df.height,
        len(missing),
    )
    return {
        "rows_read": gap_df.height,
        "rows_written": gap_df.height,
        "filled": True,
        "complete": len(filled_syms) == len(missing),
        "source_outcomes": {
            "eastmoney": {
                "status": "success" if len(filled_syms) == len(missing) else "partial",
                "requests": 1,
                "rows_written": gap_df.height,
                "empty_symbols": len(missing) - len(filled_syms),
            }
        },
        "audit_findings": [
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_clist_gapfill",
                "message": (
                    f"routed {gap_df.height} tip key(s) through EastMoney clist "
                    f"after TDX left {len(missing)} missing (ADR-0005 routing)"
                ),
                "missing_requested": len(missing),
                "rows_written": gap_df.height,
                "complete": len(filled_syms) == len(missing),
            }
        ],
    }


def _stage_daily_gap_batch(
    config: Config,
    run_id: str,
    *,
    batch_id: str,
    source: str,
    frame: pl.DataFrame,
    symbols: list[str],
    start: date,
    end: date,
) -> int:
    """Merge a gap-fill attempt into its stable staging object."""
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.storage import StagingWriter

    if frame.is_empty():
        return 0
    current_rows = frame.height
    staged = with_provenance(frame, source=source, data_version=data_version_for("daily_bars"))
    writer = StagingWriter(config.staging_root)
    path = config.staging_root / "daily_bars" / f"run_id={run_id}" / f"part-{batch_id}.parquet"
    if path.exists():
        staged = dedupe_by_primary_key(
            pl.concat([pl.read_parquet(path), staged], how="diagonal_relaxed"),
            "daily_bars",
        )
    manifest = Manifest(config.manifest_path)
    manifest.start_batch(
        run_id,
        batch_id,
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=sorted(set(symbols)),
        window_start=start.isoformat(),
        window_end=end.isoformat(),
    )
    writer.write_batch("daily_bars", run_id, batch_id, staged)
    manifest.finish_batch(
        run_id,
        batch_id,
        "success",
        rows_read=current_rows,
        rows_written=current_rows,
    )
    return current_rows


def _gapfill_complete_symbols_via_exchange(
    config: Config,
    run_id: str,
    *,
    symbols: list[str],
    start: date,
    end: date,
) -> dict:
    """Stage compatible SSE quotes when they complete a whole window.

    SZSE report turnover includes trades outside the auction series and is
    reserved for authority checks. It cannot fill auction-based daily bars.
    SSE offers only a current-session snapshot; absent quotes are not proof
    of a halt, and remain owned by the vendor fallback paths.
    """
    import polars as pl

    from cnequity.adapters.exchange.daily_quotes import fetch_sse_daily_quotes
    from cnequity.storage import StagingWriter

    requested = set(dict.fromkeys(symbols))
    sessions = list_trading_dates(config, start, end)
    if not requested or not sessions or not config.sources.get("exchange", False):
        return {
            "rows_read": 0,
            "rows_written": 0,
            "complete_symbols": [],
            "source_outcomes": {"exchange": {"status": "disabled", "requests": 0}},
        }
    if len(sessions) > _EXCHANGE_BULK_GAPFILL_MAX_SESSIONS:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "complete_symbols": [],
            "source_outcomes": {
                "exchange": {
                    "status": "skipped_long_window",
                    "requests": 0,
                    "sessions": len(sessions),
                    "max_sessions": _EXCHANGE_BULK_GAPFILL_MAX_SESSIONS,
                }
            },
        }

    frames: list[pl.DataFrame] = []
    requests = 0
    empty_responses = 0
    sh_symbols = {symbol for symbol in requested if symbol.upper().endswith(".SH")}
    if sh_symbols and len(sessions) == 1:
        requests += 1
        frame = fetch_sse_daily_quotes(sessions[0], config=config)
        if frame.is_empty():
            empty_responses += 1
        else:
            frames.append(frame.filter(pl.col("symbol").is_in(sorted(sh_symbols))))

    fetched = (
        pl.concat([frame for frame in frames if not frame.is_empty()], how="vertical_relaxed")
        if any(not frame.is_empty() for frame in frames)
        else pl.DataFrame()
    )
    writer = StagingWriter(config.staging_root)
    files = writer.list_run_files("daily_bars", run_id)
    existing = (
        pl.scan_parquet([str(path) for path in files])
        .filter(
            (pl.col("trade_date") >= start)
            & (pl.col("trade_date") <= end)
            & pl.col("symbol").is_in(sorted(requested))
        )
        .select("symbol", "trade_date")
        .unique()
        .collect()
        if files
        else pl.DataFrame(schema={"symbol": pl.Utf8, "trade_date": pl.Date})
    )
    existing_keys = set(
        zip(existing["symbol"].to_list(), existing["trade_date"].to_list(), strict=True)
    )
    fetched_keys = (
        set(zip(fetched["symbol"].to_list(), fetched["trade_date"].to_list(), strict=True))
        if not fetched.is_empty()
        else set()
    )
    required_by_symbol = {
        symbol: {(symbol, session) for session in sessions} for symbol in requested
    }
    complete_symbols = {
        symbol
        for symbol, required in required_by_symbol.items()
        if required.issubset(existing_keys | fetched_keys)
    }
    if fetched.is_empty() or not complete_symbols:
        return {
            "rows_read": fetched.height,
            "rows_written": 0,
            "complete_symbols": sorted(complete_symbols),
            "source_outcomes": {
                "exchange": {
                    "status": "empty" if not fetched.height else "partial",
                    "requests": requests,
                    "empty_responses": empty_responses,
                    "rows": fetched.height,
                }
            },
        }

    gap = fetched.filter(pl.col("symbol").is_in(sorted(complete_symbols))).join(
        existing,
        on=["symbol", "trade_date"],
        how="anti",
    )
    if gap.is_empty():
        rows_written = 0
    else:
        rows_written = _stage_daily_gap_batch(
            config,
            run_id,
            batch_id="exchange-gapfill",
            source="exchange",
            frame=gap,
            symbols=sorted(complete_symbols),
            start=start,
            end=end,
        )

    return {
        "rows_read": fetched.height,
        "rows_written": rows_written,
        "complete_symbols": sorted(complete_symbols),
        "audit_findings": [
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_exchange_gapfill",
                "message": (
                    f"official exchange quotes completed {len(complete_symbols)} symbol(s) "
                    f"over {start}..{end} with {requests} bulk request(s)"
                ),
                "symbols": sorted(complete_symbols),
                "rows_written": rows_written,
                "requests": requests,
            }
        ],
        "source_outcomes": {
            "exchange": {
                "status": "success",
                "requests": requests,
                "empty_responses": empty_responses,
                "rows": fetched.height,
                "completed_symbols": len(complete_symbols),
            }
        },
    }


def _gapfill_missing_keys_via_ths(
    config: Config,
    run_id: str,
    *,
    missing_keys: set[tuple[str, date]],
    start: date,
    end: date,
) -> dict:
    """Use THS only for exact keys still absent after cheaper batch routes."""
    from cnequity.adapters.ths.stock_bars import fetch_stock_bars

    ths_enabled = config.sources.get("ths", False)
    if not missing_keys or not ths_enabled:
        # See the baostock link: "disabled" is a claim about configuration, and
        # an earlier link having resolved everything is not one.
        return {
            "rows_read": 0,
            "rows_written": 0,
            "empty_symbols": [],
            "failed_symbols": {},
            "source_outcomes": {
                "ths": {
                    "status": "disabled" if not ths_enabled else "not_needed",
                    "requests": 0,
                }
            },
        }

    by_symbol: dict[str, set[date]] = {}
    for symbol, session in missing_keys:
        by_symbol.setdefault(symbol, set()).add(session)
    rows: list[dict] = []
    empty: list[str] = []
    failed: dict[str, str] = {}
    for symbol, required_dates in sorted(by_symbol.items()):
        try:
            fetched = fetch_stock_bars(
                symbol,
                min(required_dates),
                max(required_dates),
                config=config,
            )
        except Exception as exc:  # noqa: BLE001 — final fallback is isolated per symbol
            failed[symbol] = type(exc).__name__
            logger.warning("THS final daily-bar fallback failed for %s: %s", symbol, exc)
            continue
        selected = [row for row in fetched if (symbol, row.get("trade_date")) in missing_keys]
        if not selected:
            empty.append(symbol)
            continue
        rows.extend(selected)

    frame = pl.DataFrame(rows) if rows else pl.DataFrame()
    written = _stage_daily_gap_batch(
        config,
        run_id,
        batch_id="ths-kline-gapfill",
        source="ths",
        frame=frame,
        symbols=sorted(by_symbol),
        start=start,
        end=end,
    )
    status = "success" if written else ("failed" if failed else "empty")
    findings: list[dict] = []
    if written or failed or empty:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning" if failed or empty else "info",
                "check": "daily_bars_ths_gapfill",
                "message": (
                    f"THS final fallback wrote {written} row(s) for {len(by_symbol)} "
                    f"remaining symbol(s); empty={len(empty)}, failed={len(failed)}"
                ),
                "rows_written": written,
                "empty_symbols": sorted(empty),
                "failed_symbols": sorted(failed),
            }
        )
    return {
        "rows_read": len(rows),
        "rows_written": written,
        "empty_symbols": sorted(empty),
        "failed_symbols": failed,
        "audit_findings": findings,
        "source_outcomes": {
            "ths": {
                "status": status,
                "requests": len(by_symbol),
                "rows": len(rows),
                "empty_symbols": len(empty),
                "failed_symbols": len(failed),
            }
        },
    }


# Bounds the slowest link in the chain. Baostock paces at one request per
# second, so an unbounded residue (a dead primary, not a few stragglers) would
# add over an hour to a run that is already failing.
_BAOSTOCK_GAPFILL_MAX_SYMBOLS = 300


def _gapfill_missing_keys_via_baostock(
    config: Config,
    run_id: str,
    *,
    missing_keys: set[tuple[str, date]],
    start: date,
    end: date,
) -> dict:
    """The last independent per-symbol vendor, reached when the others cannot be.

    `daily_bars` has declared baostock supplementary since the spec was
    written, but nothing in the live chain ever called it: the code path
    existed only for delisted recovery. That left the chain with exactly two
    per-symbol vendors — EastMoney and THS — and EastMoney's history host is
    the one that is unreachable from whole classes of egress (measured: every
    `push2his` host dropped the connection from this vantage while `push2`
    still served clist). When it is down, one vendor remains, and one vendor
    can never certify a no-data key, because certification requires two
    independent empties. That is how a run whose missing names had genuinely
    not traded still failed with "expected key(s) remain unknown".

    Baostock answers the same question from a different failure domain, and
    answers it correctly for this purpose: a suspended session comes back as no
    row rather than as an error, which is the explicit empty the arbitration
    needs. Measured against the session that failed: it served both live
    symbols exactly (matching Sina to the cent) and returned empty, not failed,
    for all of the suspended ones.

    Last on purpose — it logs in per call and walks one symbol at a time, so it
    is the most expensive link and only ever sees the keys nothing else could
    resolve.
    """
    # Named for the recovery path it was written for; it is an ordinary
    # per-symbol bar fetch and carries the same fields the THS link stages.
    from cnequity.adapters.baostock.delisted_bars import fetch_delisted_bars as fetch_baostock_bars

    # Default off like the THS link: a chain that reaches a network vendor
    # nobody enabled is a chain that reaches out from a unit test. Every
    # shipped config sets `[sources.baostock] enabled = true`.
    enabled = config.sources.get("baostock", False)
    if not missing_keys or not enabled:
        # "disabled" only when it really is. An earlier link resolving
        # everything is the chain working, and reporting that as a config
        # problem sends the operator to edit a setting that is already right.
        return {
            "rows_read": 0,
            "rows_written": 0,
            "empty_symbols": [],
            "failed_symbols": {},
            "source_outcomes": {
                "baostock": {
                    "status": "disabled" if not enabled else "not_needed",
                    "requests": 0,
                }
            },
        }

    by_symbol: dict[str, set[date]] = {}
    for symbol, session in missing_keys:
        # Baostock covers Shanghai and Shenzhen only. A Beijing code comes back
        # as a retried failure rather than an answer (measured: three live BJ
        # names, three failures, one login cycle each), so sending them costs
        # the sweep and evidences nothing.
        if not is_tdx_servable(symbol):
            continue
        by_symbol.setdefault(symbol, set()).add(session)
    if not by_symbol:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "empty_symbols": [],
            "failed_symbols": {},
            "source_outcomes": {"baostock": {"status": "skipped", "requests": 0}},
        }
    requested = _research_first(by_symbol)

    if len(requested) > _BAOSTOCK_GAPFILL_MAX_SYMBOLS:
        # This link is for residue. A queue this long means the primary path
        # is broken, and walking it at one request per second would spend more
        # than an hour proving that slowly.
        message = (
            f"baostock final fallback skipped: {len(requested)} symbol(s) exceeds the "
            f"{_BAOSTOCK_GAPFILL_MAX_SYMBOLS}-symbol residue bound for a 1 req/s link; "
            "the primary route is what needs repairing"
        )
        logger.warning("%s", message)
        return {
            "rows_read": 0,
            "rows_written": 0,
            "empty_symbols": [],
            "failed_symbols": {},
            "audit_findings": [
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_baostock_gapfill",
                    "message": message,
                    "requested_symbols": len(requested),
                }
            ],
            "source_outcomes": {
                "baostock": {"status": "skipped", "requests": 0, "pending": len(requested)}
            },
        }

    try:
        fetched, failed_symbols = fetch_baostock_bars(requested, start, end, config=config)
    except Exception as exc:  # noqa: BLE001 — a dead final link must not raise
        logger.warning("baostock final daily-bar fallback failed: %s", exc)
        return {
            "rows_read": 0,
            "rows_written": 0,
            "empty_symbols": [],
            "failed_symbols": {symbol: type(exc).__name__ for symbol in requested},
            "source_outcomes": {
                "baostock": {
                    "status": "failed",
                    "requests": len(requested),
                    "failed_symbols": len(requested),
                }
            },
        }

    rows = [row for row in fetched if (row.get("symbol"), row.get("trade_date")) in missing_keys]
    returned = {row.get("symbol") for row in rows}
    failed = {symbol: "fetch_failed" for symbol in failed_symbols}
    # Empty means answered-and-had-nothing. A symbol the vendor never got to
    # answer for is not evidence of anything.
    empty = sorted(
        symbol for symbol in requested if symbol not in returned and symbol not in failed
    )

    frame = pl.DataFrame(rows) if rows else pl.DataFrame()
    written = _stage_daily_gap_batch(
        config,
        run_id,
        batch_id="baostock-kline-gapfill",
        source="baostock",
        frame=frame,
        symbols=requested,
        start=start,
        end=end,
    )
    status = "success" if written else ("failed" if failed else "empty")
    findings: list[dict] = []
    if written or failed or empty:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning" if failed else "info",
                "check": "daily_bars_baostock_gapfill",
                "message": (
                    f"baostock final fallback wrote {written} row(s) for {len(requested)} "
                    f"remaining symbol(s); empty={len(empty)}, failed={len(failed)}"
                ),
                "rows_written": written,
                "empty_symbols": empty,
                "failed_symbols": sorted(failed),
            }
        )
    return {
        "rows_read": len(rows),
        "rows_written": written,
        "empty_symbols": empty,
        "failed_symbols": failed,
        "audit_findings": findings,
        "source_outcomes": {
            "baostock": {
                "status": status,
                "requests": len(requested),
                "rows": len(rows),
                "empty_symbols": len(empty),
                "failed_symbols": len(failed),
            }
        },
    }


def _research_first(symbols: Iterable[str]) -> list[str]:
    """Order a fallback queue so research-universe symbols are attempted first.

    The per-symbol vendors in the fallback chain open a circuit after a few
    consecutive transport failures and abandon everything still queued behind
    it. Under a scope that deliberately keeps ETF/LOF quote codes
    (``[universe].ingest = "all_instruments"``) an alphabetical queue spends
    that whole budget on 15xxxx/16xxxx codes before it ever reaches 6xxxxx —
    which is how a few unservable fund codes left real A shares unresolved.

    Still fully deterministic: sorted within each class, never shuffled.
    """
    return sorted(symbols, key=lambda symbol: (0 if _is_research_symbol(symbol) else 1, symbol))


def _is_research_symbol(symbol: str) -> bool:
    try:
        info = parse_symbol(symbol)
    except ValueError:
        return False
    return in_ingest_universe(info.code, info.exchange, "all_a")


def _expected_session_keys(
    config: Config,
    symbols: list[str],
    sessions: list[date],
    start: date,
    end: date,
) -> set[tuple[str, date]]:
    """Keys a symbol is actually expected to have over *sessions*.

    Every trading day in the window is the wrong answer, and it was the one
    this chain used: it counted days before a symbol listed, days after it
    delisted, and days the lake already knows it was suspended. A backfill of a
    name with any suspension in its window could therefore never report itself
    complete, which left the failed primary batch unresolved, which left
    `compact` skipping the dataset — rows fetched, staged, and never published.

    The interior-gap gate downstream has always judged by listing span and
    `trading_status`. This is the same rule, applied where completeness is
    first decided, so the two cannot disagree about what "missing" means.
    """
    spans = _instrument_spans(config)
    status = load_curated_trading_status(config, start=start, end=end, symbols=sorted(symbols))
    halted: dict[str, set[date]] = {}
    if status is not None and not status.is_empty():
        needed = {"symbol", "trade_date", "is_trading"}
        if needed.issubset(status.columns):
            for row in status.select(*sorted(needed)).iter_rows(named=True):
                if row["is_trading"] is False:
                    halted.setdefault(str(row["symbol"]), set()).add(row["trade_date"])

    keys: set[tuple[str, date]] = set()
    for symbol in symbols:
        list_date, delist_date = spans.get(symbol, (None, None, None))[:2]
        first = max(start, list_date) if list_date is not None else start
        last = min(end, delist_date) if delist_date is not None else end
        closed = halted.get(symbol, set())
        keys.update(
            (symbol, session)
            for session in sessions
            if first <= session <= last and session not in closed
        )
    return keys


def _learn_suspensions_from_baostock(
    config: Config,
    run_id: str,
    missing: set[tuple[str, date]],
    start: date,
    end: date,
) -> tuple[set[tuple[str, date]], dict]:
    """Ask the vendor which of these absences were suspensions, and keep the answer.

    A pre-2016 window has no `trading_status` in the lake to excuse an interior
    gap with, so a backfill of a name that was ever suspended could not report
    itself complete however many bars it actually fetched — and the operator
    had to know to run `cne backfill trading_status` first, for the same
    symbols and window, before the bars would publish. Two commands, in an
    order nothing announced.

    Baostock publishes the fact per symbol-day (`tradestatus`), and is already
    this dataset's declared backfill source, so the step asks for it itself.
    The rows are staged as well as used, so the next run does not ask again.

    Returns the (symbol, session) pairs it confirmed suspended, and an outcome
    record for the receipt.
    """
    from cnequity.adapters.baostock.st_history import fetch_st_history
    from cnequity.steps.http_common import write_fetched

    if not missing or not config.sources.get("baostock", False):
        return set(), {"status": "disabled" if missing else "not_needed", "requests": 0}
    # Baostock serves Shanghai and Shenzhen only; a Beijing code costs a
    # retried failure and answers nothing.
    symbols = sorted({symbol for symbol, _day in missing if is_tdx_servable(symbol)})
    if not symbols:
        return set(), {"status": "skipped", "requests": 0, "reason": "no SH/SZ key"}
    if len(symbols) > _BAOSTOCK_GAPFILL_MAX_SYMBOLS:
        return set(), {
            "status": "skipped",
            "requests": 0,
            "pending": len(symbols),
            "reason": "residue exceeds the 1 req/s bound",
        }

    try:
        frame, failed = fetch_st_history(symbols, start, end, config=config)
    except Exception as exc:  # noqa: BLE001 — evidence we could not get is not a new failure
        logger.warning("suspension evidence unavailable for %d symbol(s): %s", len(symbols), exc)
        return set(), {"status": "failed", "requests": len(symbols), "error": type(exc).__name__}
    if frame.is_empty():
        return set(), {"status": "empty", "requests": len(symbols), "failed_symbols": len(failed)}

    halted = frame.filter(~pl.col("is_trading"))
    confirmed = {
        (row["symbol"], row["trade_date"])
        for row in halted.select("symbol", "trade_date").iter_rows(named=True)
    } & missing
    written = write_fetched(
        config,
        run_id,
        "trading_status",
        frame,
        source="baostock",
        batch_id=f"suspension-evidence-{start.isoformat()}-{end.isoformat()}",
    )
    logger.info(
        "learned %d suspended session(s) for %d symbol(s) from baostock; "
        "%s trading_status row(s) staged",
        len(confirmed),
        len(symbols),
        written.get("rows_written", 0),
    )
    # The rows are staged, not curated, until this run's compact — and the
    # interior-gap gate below reads curated. Carry the answer on the config for
    # the rest of this run so the gate does not re-condemn what we just learned.
    learned = getattr(config, "_learned_suspensions", None)
    if not isinstance(learned, dict) or learned.get("run_id") != run_id:
        learned = {"run_id": run_id, "keys": set()}
        config._learned_suspensions = learned
    learned["keys"].update(confirmed)
    return confirmed, {
        "status": "success",
        "requests": len(symbols),
        "suspended_keys": len(confirmed),
        "rows_written": int(written.get("rows_written", 0)),
        "failed_symbols": len(failed),
    }


def _gapfill_multiday_via_kline(
    config: Config,
    run_id: str,
    *,
    symbols: list[str],
    start: date,
    end: date,
    require_complete: bool = True,
) -> dict:
    """Fill exact missing keys through the bounded multi-source route chain."""
    import polars as pl

    from cnequity.adapters.eastmoney.bars import fetch_daily_bars as fetch_em_kline
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.quality.failover import failover_spec, write_backup_snapshot
    from cnequity.storage import StagingWriter

    if not symbols:
        return {"rows_read": 0, "rows_written": 0, "filled": False}

    requested = list(dict.fromkeys(symbols))
    sessions = list_trading_dates(config, start, end)
    expected_keys = _expected_session_keys(config, requested, sessions, start, end)
    writer = StagingWriter(config.staging_root)

    def staged_keys() -> set[tuple[str, date]]:
        return staged_key_volume_sets()[0]

    def staged_key_volume_sets() -> tuple[set[tuple[str, date]], set[tuple[str, date]]]:
        """(all staged keys, keys with positive volume).

        A zero-volume pre-open placeholder is explicitly *not* evidence that a
        symbol ever traded (see ``load_bar_universe``); certification must not
        treat a dead fund's own tip placeholder as proof of life.
        """
        files = writer.list_run_files("daily_bars", run_id)
        if not files:
            return set(), set()
        keys = (
            pl.scan_parquet([str(path) for path in files])
            .filter(
                (pl.col("trade_date") >= start)
                & (pl.col("trade_date") <= end)
                & pl.col("symbol").is_in(requested)
            )
            .select("symbol", "trade_date", "volume")
            .unique()
            .collect()
        )
        all_keys = set(
            zip(keys["symbol"].to_list(), keys["trade_date"].to_list(), strict=True)
        )
        positive = keys.filter(pl.col("volume") > 0)
        real_keys = set(
            zip(positive["symbol"].to_list(), positive["trade_date"].to_list(), strict=True)
        )
        return all_keys, real_keys

    def missing_keys() -> set[tuple[str, date]]:
        return expected_keys - staged_keys()

    rows_read = 0
    rows_written = 0
    findings: list[dict] = []
    source_outcomes: dict[str, dict] = {}
    empty_evidence: dict[str, set[str]] = {}

    # Fastest broad recovery first: one official SZSE request per session (and
    # a same-session SSE snapshot) can complete hundreds of symbols at once.
    exchange = _gapfill_complete_symbols_via_exchange(
        config,
        run_id,
        symbols=requested,
        start=start,
        end=end,
    )
    rows_read += int(exchange.get("rows_read", 0))
    rows_written += int(exchange.get("rows_written", 0))
    findings.extend(exchange.get("audit_findings") or [])
    source_outcomes.update(exchange.get("source_outcomes") or {})
    exchange_complete = set(exchange.get("complete_symbols") or [])
    pending_symbols = _research_first(
        {symbol for symbol, _day in missing_keys()} - exchange_complete
    )
    if not pending_symbols:
        return {
            "rows_read": rows_read,
            "rows_written": rows_written,
            "filled": bool(rows_written),
            "complete": True,
            "audit_findings": findings,
            "source_outcomes": source_outcomes,
        }

    # Sina is a bounded recent-tail request now. It remains skipped for the
    # partial-only path because the bulk/primary staging may already contain
    # valid keys and EastMoney/THS below can filter exact missing pairs.
    if require_complete and config.sources.get("sina", True):
        sina = fetch_bars_via_sina(
            config,
            pending_symbols,
            start,
            end,
            run_id,
            batch_prefix="sina-kline-gapfill",
        )
        rows_read += int(sina.get("rows_read", 0))
        rows_written += int(sina.get("rows_written", 0))
        findings.extend((sina.get("context_updates") or {}).get("audit_findings") or [])
        source_outcomes.update(sina.get("source_outcomes") or {})
        for symbol in sina.get("empty_symbol_names") or []:
            empty_evidence.setdefault(symbol, set()).add("sina")
        if sina.get("empty_symbol_names"):
            findings.append(
                {
                    "dataset": "daily_bars",
                    "severity": "info",
                    "check": "daily_bars_sina_source_empty",
                    "message": (
                        f"Sina returned an explicit empty payload for "
                        f"{len(sina.get('empty_symbol_names') or [])} symbol(s); "
                        "a second independent empty source is required before no-data certification"
                    ),
                    "symbols": sorted(sina.get("empty_symbol_names") or []),
                }
            )

    pending = missing_keys()
    pending_symbols = _research_first({symbol for symbol, _day in pending})
    spec = failover_spec(config, "daily_bars")
    if pending_symbols and spec is not None and config.sources.get(spec.backup, True):
        diagnostics: dict = {}
        df = fetch_em_kline(
            pending_symbols,
            start,
            end,
            config=config,
            timeout_sec=8.0,
            diagnostics=diagnostics,
        )
        rows_read += df.height
        for symbol in diagnostics.get("empty_symbols") or []:
            empty_evidence.setdefault(symbol, set()).add("eastmoney")
        failures = diagnostics.get("failed_symbols") or {}
        if df.is_empty():
            status = "failed" if failures else "empty"
            written = 0
        else:
            wanted = pl.DataFrame(
                {
                    "symbol": [symbol for symbol, _day in sorted(pending)],
                    "trade_date": [day for _symbol, day in sorted(pending)],
                },
                schema={"symbol": pl.Utf8, "trade_date": pl.Date},
            )
            gap_df = df.join(wanted, on=["symbol", "trade_date"], how="inner")
            snapshot = with_provenance(
                df,
                source=spec.backup,
                data_version=data_version_for("daily_bars"),
            )
            write_backup_snapshot(
                config,
                "daily_bars",
                snapshot,
                run_id=run_id,
                batch_id="em-kline-snapshot",
                source=spec.backup,
                trade_date=end,
            )
            written = _stage_daily_gap_batch(
                config,
                run_id,
                batch_id="em-kline-gapfill",
                source=spec.backup,
                frame=gap_df,
                symbols=pending_symbols,
                start=start,
                end=end,
            )
            rows_written += written
            status = "success" if written else "partial"
        source_outcomes["eastmoney"] = {
            "status": status,
            "requests": len(pending_symbols),
            "rows": df.height,
            "rows_written": written,
            "empty_symbols": len(diagnostics.get("empty_symbols") or []),
            "failed_symbols": len(failures),
            "failure_reasons": dict(sorted(failures.items())),
            "proxy_failed": sum(
                bool(route.get("proxy_failed"))
                for route in (diagnostics.get("route_outcomes") or {}).values()
            ),
            "direct_failed": sum(
                bool(route.get("direct_failed"))
                for route in (diagnostics.get("route_outcomes") or {}).values()
            ),
            "direct_succeeded": sum(
                bool(route.get("direct_succeeded"))
                for route in (diagnostics.get("route_outcomes") or {}).values()
            ),
        }
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning" if status != "success" else "info",
                "check": "daily_bars_eastmoney_gapfill",
                "message": (
                    f"EastMoney historical fallback wrote {written} row(s) for "
                    f"{len(pending_symbols)} remaining symbol(s); status={status}"
                ),
                **source_outcomes["eastmoney"],
            }
        )
    elif pending_symbols:
        source_outcomes["eastmoney"] = {"status": "disabled", "requests": 0}

    pending = missing_keys()
    ths = _gapfill_missing_keys_via_ths(
        config,
        run_id,
        missing_keys=pending,
        start=start,
        end=end,
    )
    rows_read += int(ths.get("rows_read", 0))
    rows_written += int(ths.get("rows_written", 0))
    findings.extend(ths.get("audit_findings") or [])
    source_outcomes.update(ths.get("source_outcomes") or {})
    for symbol in ths.get("empty_symbols") or []:
        empty_evidence.setdefault(symbol, set()).add("ths")

    # Everything above can be unavailable at once — EastMoney's history host is
    # unreachable from some egress, THS can be disabled, and the exchange
    # publishes per session rather than per symbol. Reaching a fourth vendor in
    # its own failure domain is what keeps the run from ending on "unknown"
    # when the answer was obtainable; it costs nothing when the chain already
    # resolved everything, because it is only handed what is still missing.
    pending = missing_keys()
    baostock = _gapfill_missing_keys_via_baostock(
        config,
        run_id,
        missing_keys=pending,
        start=start,
        end=end,
    )
    rows_read += int(baostock.get("rows_read", 0))
    rows_written += int(baostock.get("rows_written", 0))
    findings.extend(baostock.get("audit_findings") or [])
    source_outcomes.update(baostock.get("source_outcomes") or {})
    for symbol in baostock.get("empty_symbols") or []:
        empty_evidence.setdefault(symbol, set()).add("baostock")

    remaining = missing_keys()
    suspended: set[tuple[str, date]] = set()
    if remaining:
        # Nothing above could fill these. Before calling them unresolved, find
        # out whether they are absences the market itself explains.
        suspended, suspension_outcome = _learn_suspensions_from_baostock(
            config, run_id, remaining, start, end
        )
        source_outcomes["baostock_suspensions"] = suspension_outcome
        if suspended:
            expected_keys -= suspended
            findings.append(
                {
                    "dataset": "daily_bars",
                    "severity": "info",
                    "check": "daily_bars_suspension_evidence",
                    "message": (
                        f"{len(suspended)} absent key(s) confirmed as suspensions by baostock "
                        "and recorded in trading_status"
                    ),
                    "suspended_keys": len(suspended),
                }
            )
    remaining = missing_keys()
    observed, real_keys = staged_key_volume_sets()
    # Two independent per-symbol sources agreed the window is empty, and the
    # symbol carries no positive-volume staged row in it (placeholders are not
    # trade evidence). That certifies the whole symbol as source-empty; a
    # staged placeholder on some days no longer disqualifies the claim.
    expected_no_data = {
        symbol
        for symbol, sources in empty_evidence.items()
        if len(sources) >= 2
        and not any((symbol, session) in real_keys for session in sessions)
    }
    # A vendor saying "suspended on every session you asked about" is not the
    # same claim as two vendors independently returning nothing: the first is a
    # positive statement about the market, the second only says nobody had it.
    # Names that spend a whole window halted for a restructuring are exactly
    # the case the two-empty rule cannot reach — measured on the 2005-2015
    # repair, 19 of them were suspended for every session of 2010 and the run
    # still refused, because no vendor can return rows that never existed.
    halted_symbols = {symbol for symbol, _day in suspended}
    positively_halted = {
        symbol
        for symbol in halted_symbols
        if not any(key[0] == symbol for key in observed)
        and not any(key[0] == symbol for key in expected_keys)
    }
    # Kept apart from `expected_no_data` for the report below: both mean "no
    # rows are owed here", but only one of them was certified by two sources
    # agreeing, and `empty_evidence` holds an entry for that one alone.
    multi_source_no_data = set(expected_no_data)
    if positively_halted:
        expected_no_data |= positively_halted
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_window_fully_suspended",
                "message": (
                    f"{len(positively_halted)} symbol(s) were suspended for every session in "
                    f"{start}..{end} — certified from the vendor's own trading status, not "
                    "from an absence of rows"
                ),
                "symbols": sorted(positively_halted),
            }
        )
    # Symbols with real staged rows (partial histories — e.g. funds whose
    # source retention starts mid-window) can never satisfy the symbol-level
    # rule above. Certify their missing keys per segment through the same
    # two-source agreement instead of leaving them unresolved forever.
    real_symbols = {symbol for symbol, _day in real_keys}
    segment_missing = {
        key for key in remaining if key[0] in real_symbols and key[0] not in expected_no_data
    }
    segment_certified, seg_rows_read, seg_rows_written = _certify_missing_segments(
        config,
        run_id,
        segment_missing,
        real_keys=real_keys,
        sessions=sessions,
        spec=spec,
        findings=findings,
        missing_keys_fn=missing_keys,
    )
    rows_read += seg_rows_read
    rows_written += seg_rows_written
    remaining = missing_keys() - segment_certified
    unresolved = {key for key in remaining if key[0] not in expected_no_data}
    if multi_source_no_data:
        # Only the symbols this rule actually certified. A halted name reaches
        # `expected_no_data` through the finding above, on the vendor's trading
        # status rather than on two sources returning nothing — it has no entry
        # in `empty_evidence`, so listing it here both raised `KeyError` and
        # claimed evidence that was never collected.
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_multi_source_no_data",
                "message": (
                    f"certified {len(multi_source_no_data)} symbol(s) as source-empty only after "
                    "two independent per-symbol sources agreed"
                ),
                "symbols": sorted(multi_source_no_data),
                "evidence": {
                    symbol: sorted(empty_evidence[symbol])
                    for symbol in sorted(multi_source_no_data)
                },
            }
        )
    if unresolved:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_gapfill_incomplete",
                "message": (
                    f"multi-source gap-fill left {len(unresolved)} symbol×session key(s) "
                    f"unresolved over {start}..{end}"
                ),
                "missing_keys": len(unresolved),
                "sample_keys": [
                    {"symbol": symbol, "trade_date": day.isoformat()}
                    for symbol, day in sorted(unresolved)[:8]
                ],
            }
        )
    return {
        "rows_read": rows_read,
        "rows_written": rows_written,
        "filled": bool(rows_written) or bool(expected_no_data) or bool(segment_certified),
        "complete": not unresolved,
        "expected_no_data_symbols": sorted(expected_no_data),
        "expected_no_data_keys": sorted(segment_certified),
        "audit_findings": findings,
        "source_outcomes": source_outcomes,
        "missing_keys": len(unresolved),
    }


# A bar captured before the session opens is the previous close stamped on every
# field: open==high==low==close and zero volume. A handful of these on any day
# are genuine suspensions, but a whole universe of them means the fetch ran too
# early — 2026-07-22 arrived that way from a pre-open run. Below this share it is
# suspensions; at or above it, it is a mis-timed capture.
_PLACEHOLDER_SHARE_LIMIT = 0.5


def _certify_missing_segments(
    config: Config,
    run_id: str,
    missing: set[tuple[str, date]],
    *,
    real_keys: set[tuple[str, date]],
    sessions: list[date],
    spec,
    findings: list[dict],
    missing_keys_fn,
) -> tuple[set[tuple[str, date]], int, int]:
    """Certify missing keys of partially-staged symbols per missing segment.

    A symbol with real staged rows can never qualify for symbol-level
    source-empty certification, yet part of its history may be genuinely
    unobtainable — a fund whose source retention starts mid-window is the
    common case. For each run of missing sessions bounded by the symbol's
    real staged rows, ask the enabled per-symbol sources (EastMoney kline,
    Sina, THS) to serve the span. Rows any source returns for missing keys
    are staged first; only when two independent sources agree the segment is
    empty are its keys certified and persisted as bounded negative evidence,
    so the interior-gap gate and future runs can skip them.
    """
    import polars as pl

    from cnequity.adapters.eastmoney.bars import fetch_daily_bars as fetch_em_kline
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.quality.failover import write_backup_snapshot
    from cnequity.storage import StagingWriter

    if not missing or not sessions:
        return set(), 0, 0
    em_enabled = spec is not None and config.sources.get(spec.backup, True)
    sina_enabled = config.sources.get("sina", True)
    ths_enabled = config.sources.get("ths", True)
    # A config that never declared per-symbol sources has no certain voters
    # (unconfigured keys default to enabled at read time, but nothing was
    # actually set up to serve them) — and tests build exactly such configs.
    # Probing would only make live network calls that the run never asked for.
    if not config.sources or sum((em_enabled, sina_enabled, ths_enabled)) < 2:
        # Two-source agreement needs two voters; with fewer enabled sources a
        # probe could never certify, so it would only burn requests.
        return set(), 0, 0

    real_dates: dict[str, set[date]] = {}
    for symbol, day in real_keys:
        real_dates.setdefault(symbol, set()).add(day)
    missing_by_symbol: dict[str, list[date]] = {}
    for symbol, day in missing:
        missing_by_symbol.setdefault(symbol, []).append(day)

    rows_read = 0
    rows_written = 0
    certified: set[tuple[str, date]] = set()
    segments_log: list[dict] = []

    for symbol in sorted(missing_by_symbol):
        sym_days = sorted(missing_by_symbol[symbol])
        sym_real = real_dates.get(symbol, set())
        # Contiguous missing runs: a run breaks where a real staged row sits.
        groups: list[list[date]] = [[sym_days[0]]]
        for prev, day in zip(sym_days, sym_days[1:], strict=False):
            if any(prev < gap_day < day for gap_day in sym_real):
                groups.append([day])
            else:
                groups[-1].append(day)

        for seg_days in groups:
            seg_start, seg_end = seg_days[0], seg_days[-1]
            wanted = {
                (symbol, day) for day in sessions if seg_start <= day <= seg_end
            } & missing
            if not wanted:
                continue
            empty_votes: set[str] = set()
            diagnostics: dict = {}
            if em_enabled:
                df = fetch_em_kline(
                    [symbol],
                    seg_start,
                    seg_end,
                    config=config,
                    timeout_sec=8.0,
                    diagnostics=diagnostics,
                )
                rows_read += df.height
                if symbol in (diagnostics.get("empty_symbols") or []):
                    empty_votes.add("eastmoney")
                if not df.is_empty():
                    gap_df = df.join(
                        pl.DataFrame(
                            {
                                "symbol": [s for s, _day in sorted(wanted)],
                                "trade_date": [day for _s, day in sorted(wanted)],
                            },
                            schema={"symbol": pl.Utf8, "trade_date": pl.Date},
                        ),
                        on=["symbol", "trade_date"],
                        how="inner",
                    )
                    snapshot = with_provenance(
                        df,
                        source=spec.backup,
                        data_version=data_version_for("daily_bars"),
                    )
                    write_backup_snapshot(
                        config,
                        "daily_bars",
                        snapshot,
                        run_id=run_id,
                        batch_id="em-kline-segment-gapfill",
                        source=spec.backup,
                        trade_date=seg_end,
                    )
                    rows_written += _stage_daily_gap_batch(
                        config,
                        run_id,
                        batch_id="em-kline-segment-gapfill",
                        source=spec.backup,
                        frame=gap_df,
                        symbols=[symbol],
                        start=seg_start,
                        end=seg_end,
                    )
            if sina_enabled:
                sina_res = fetch_bars_via_sina(
                    config,
                    [symbol],
                    seg_start,
                    seg_end,
                    run_id,
                    batch_prefix="sina-kline-segment-gapfill",
                )
                rows_read += int(sina_res.get("rows_read", 0))
                rows_written += int(sina_res.get("rows_written", 0))
                findings.extend((sina_res.get("context_updates") or {}).get("audit_findings") or [])
                if symbol in (sina_res.get("empty_symbol_names") or []):
                    empty_votes.add("sina")
            if len(empty_votes) < 2 and ths_enabled:
                ths_res = _gapfill_missing_keys_via_ths(
                    config,
                    run_id,
                    missing_keys=wanted,
                    start=seg_start,
                    end=seg_end,
                )
                rows_read += int(ths_res.get("rows_read", 0))
                rows_written += int(ths_res.get("rows_written", 0))
                findings.extend(ths_res.get("audit_findings") or [])
                if symbol in (ths_res.get("empty_symbols") or []):
                    empty_votes.add("ths")
            still_missing = wanted & missing_keys_fn()
            if len(empty_votes) >= 2 and still_missing:
                certified |= still_missing
                record_negative_evidence(
                    config,
                    "daily_bars",
                    {symbol},
                    seg_start,
                    seg_end,
                    reason="source_empty",
                    source="multi_source_gapfill",
                )
                segments_log.append(
                    {
                        "symbol": symbol,
                        "segment": [seg_start.isoformat(), seg_end.isoformat()],
                        "keys": len(still_missing),
                        "sources": sorted(empty_votes),
                    }
                )

    if certified:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_segment_no_data",
                "message": (
                    f"certified {len(certified)} missing key(s) across "
                    f"{len(segments_log)} segment(s) after two independent "
                    "sources returned empty for each segment"
                ),
                "segments": segments_log[:20],
            }
        )
    return certified, rows_read, rows_written


def _reject_preopen_placeholder(config: Config, run_id: str, trade_date: date) -> None:
    """Fail the step if the freshest staged day is mostly pre-open placeholders.

    Checked against staging, before compact promotes anything, so a mis-timed
    run stays quarantined in staging instead of overwriting a good curated
    partition. `by_date` semantics mean the fix is simply to re-run after the
    close, which a failed step invites rather than hides.
    """
    import polars as pl

    from cnequity.storage import StagingWriter

    files = StagingWriter(config.staging_root).list_run_files("daily_bars", run_id)
    if not files:
        return
    df = (
        pl.scan_parquet([str(f) for f in files])
        .filter(pl.col("trade_date") == trade_date)
        .select("open", "high", "low", "close", "volume")
        .collect()
    )
    if df.is_empty():
        return
    placeholder = df.filter(
        (pl.col("open") == pl.col("close"))
        & (pl.col("high") == pl.col("low"))
        & (pl.col("open") == pl.col("high"))
        & (pl.col("volume") == 0)
    ).height
    share = placeholder / df.height
    if share >= _PLACEHOLDER_SHARE_LIMIT:
        raise RuntimeError(
            f"daily_bars {trade_date}: {placeholder}/{df.height} rows "
            f"({share:.0%}) are pre-open placeholders (OHLC flat, zero volume) — "
            "the capture ran before the close. Re-run after the session closes."
        )


def _supplement_bse_tip_amounts(
    config: Config,
    merged: pl.DataFrame,
    *,
    trade_date: date,
    symbols: list[str],
) -> tuple[pl.DataFrame, list[dict]]:
    """Fill only BJ tip turnover that passes an official OHLCV cross-check.

    Sina is the historical fallback for Beijing bars but does not expose
    turnover. BSE's quotation endpoint is a current snapshot, so it is not a
    history source: a row is eligible only when its session is the requested
    date and every OHLCV field agrees exactly with the Sina row already staged.
    A mismatch keeps the Sina row unchanged and becomes an audit finding.
    """
    bse_symbols = sorted({symbol for symbol in symbols if symbol.endswith(".BJ")})
    if not bse_symbols or not config.sources.get("bse", False):
        return merged, []

    from cnequity.adapters.bse.daily_quotes import fetch_daily_quotes

    try:
        bse = fetch_daily_quotes(trade_date, symbols=bse_symbols, config=config)
    except Exception as exc:  # noqa: BLE001 — Sina remains the usable fallback
        logger.warning("BSE tip turnover supplement failed for %s: %s", trade_date, exc)
        return merged, [
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_bse_amount_unavailable",
                "message": f"BSE tip quote unavailable for {trade_date}: {exc}",
                "source": "bse",
                "source_limited": True,
            }
        ]

    if bse.is_empty():
        return merged, [
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_bse_amount_unavailable",
                "message": (
                    f"BSE returned no dated tip quote for {trade_date}; Sina amount stays null"
                ),
                "source": "bse",
                "source_limited": True,
            }
        ]

    if "source" not in merged.columns:
        merged = with_columns_unless_blank(merged, pl.lit("sina").alias("source"))
    bse = bse.select(
        "symbol",
        "trade_date",
        pl.col("open").alias("_bse_open"),
        pl.col("high").alias("_bse_high"),
        pl.col("low").alias("_bse_low"),
        pl.col("close").alias("_bse_close"),
        pl.col("volume").alias("_bse_volume"),
        pl.col("amount").alias("_bse_amount"),
    )
    joined = merged.join(bse, on=["symbol", "trade_date"], how="left")
    bse_present = pl.col("_bse_amount").is_not_null()
    exact_match = pl.all_horizontal(
        pl.col(left) == pl.col(right)
        for left, right in (
            ("open", "_bse_open"),
            ("high", "_bse_high"),
            ("low", "_bse_low"),
            ("close", "_bse_close"),
            ("volume", "_bse_volume"),
        )
    )
    amount_missing = pl.col("amount").is_null()
    supplement = bse_present & exact_match & amount_missing
    mismatch = bse_present & ~exact_match
    supplemented = joined.filter(supplement)
    mismatched = joined.filter(mismatch)
    updated = joined.with_columns(
        pl.when(supplement).then(pl.col("_bse_amount")).otherwise(pl.col("amount")).alias("amount"),
        pl.when(supplement).then(pl.lit("bse")).otherwise(pl.col("source")).alias("source"),
    ).drop(
        "_bse_open",
        "_bse_high",
        "_bse_low",
        "_bse_close",
        "_bse_volume",
        "_bse_amount",
    )
    findings: list[dict] = []
    if supplemented.height:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_bse_amount_supplement",
                "message": (
                    f"BSE official tip quote supplied amount for {supplemented.height} row(s) "
                    f"on {trade_date} after exact Sina OHLCV matching"
                ),
                "source": "bse",
                "rows_supplemented": supplemented.height,
                "symbols_supplemented": supplemented.get_column("symbol").n_unique(),
            }
        )
    if mismatched.height:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_bse_quote_mismatch",
                "message": (
                    f"BSE tip quote disagreed with Sina OHLCV for {mismatched.height} row(s) "
                    f"on {trade_date}; Sina rows were retained"
                ),
                "source": "bse",
                "rows_mismatched": mismatched.height,
            }
        )
    return updated, findings


def fetch_bars_via_sina(
    config: Config,
    symbols: list[str],
    start: date,
    end: date,
    run_id: str,
    *,
    batch_prefix: str = "sina",
    fetch=None,
) -> dict:
    """Stage daily bars for symbols the primary protocol cannot serve.

    Failures are collected rather than raised: one unreachable symbol must not
    cost the whole run its Beijing coverage. They surface as an audit finding so
    a persistent gap is visible instead of silently shrinking the universe.
    """
    from concurrent.futures import ThreadPoolExecutor

    import httpx
    import polars as pl

    from cnequity.adapters.sina.bars import fetch_daily_bars_sina
    from cnequity.steps.http_common import write_fetched

    default_fetch = fetch is None
    use_parallel = default_fetch
    requested_symbols = list(dict.fromkeys(symbols))
    fetch = fetch or (
        lambda symbol, client: fetch_daily_bars_sina(
            symbol, start=start, end=end, client=client, config=config
        )
    )
    frames: list[pl.DataFrame] = []
    failed: list[str] = []
    empty: list[str] = []
    covered_dates: dict[str, set[date]] = {}
    audit_findings: list[dict] = []
    failure_reasons: dict[str, int] = {}
    rows = 0
    # A daily run can enter this helper for BJ, TDX failures and interior gaps.
    # Share the circuit across those passes; resetting it per call repeatedly
    # re-triggers the same vendor ban and its 30/120-second cooldowns.
    circuit_state = getattr(config, "_sina_bar_circuit", None)
    if circuit_state is None or circuit_state[0] != run_id:
        # Config is passed to process workers elsewhere. Store only simple
        # values here; a threading.Event would make that config unpicklable.
        circuit_state = (run_id, False)
        config._sina_bar_circuit = circuit_state

    # The BSE quotation API is an official current-session snapshot, not a
    # history source.  Use it first whenever this window contains exactly one
    # live session.  The normal incremental Monday window is often
    # ``Friday+1 .. Monday`` because the watermark is calendar-based; without
    # this check BJ symbols unnecessarily enter the much larger Sina sweep.
    # ``fetch`` is injectable for tests, so skip this live path when a fake
    # fetcher is supplied.
    bse_symbols = [symbol for symbol in requested_symbols if symbol.upper().endswith(".BJ")]
    bse_attempted = False
    if use_parallel and bse_symbols and config.sources.get("bse", False):
        sessions = list_trading_dates(config, start, end)
        if len(sessions) == 1 and not getattr(config, "_backfill", False):
            bse_attempted = True
            try:
                from cnequity.adapters.bse.daily_quotes import fetch_daily_quotes

                bse = fetch_daily_quotes(sessions[0], symbols=bse_symbols, config=config)
            except Exception as exc:  # noqa: BLE001 — Sina remains the fallback
                logger.warning("BSE tip bars failed for %s: %s", sessions[0], exc)
                audit_findings.append(
                    {
                        "dataset": "daily_bars",
                        "severity": "info",
                        "check": "daily_bars_bse_tip_unavailable",
                        "message": f"BSE tip quote unavailable for {sessions[0]}: {exc}",
                        "source": "bse",
                        "source_limited": True,
                    }
                )
            else:
                if not bse.is_empty():
                    out = write_fetched(
                        config,
                        run_id,
                        "daily_bars",
                        bse,
                        source="bse",
                        batch_id=f"{batch_prefix}-bse-0000",
                    )
                    rows += int(out.get("rows_written", 0))
                    for symbol in bse.get_column("symbol").unique().to_list():
                        covered_dates[symbol] = {sessions[0]}
                    covered = set(bse.get_column("symbol").unique().to_list())
                    requested_symbols = [
                        symbol for symbol in requested_symbols if symbol not in covered
                    ]
                    audit_findings.append(
                        {
                            "dataset": "daily_bars",
                            "severity": "info",
                            "check": "daily_bars_bse_tip",
                            "message": (
                                f"routed {len(covered)} current BJ bar(s) through the official BSE "
                                f"snapshot for {sessions[0]}"
                            ),
                            "source": "bse",
                            "rows_written": int(out.get("rows_written", 0)),
                            "symbols": len(covered),
                        }
                    )

    def fetch_one(symbol: str, client: httpx.Client) -> tuple[str, pl.DataFrame | None, str | None]:
        rate_limit_failures = 0
        for attempt in range(_SINA_FETCH_ATTEMPTS):
            if config._sina_bar_circuit == (run_id, True):
                return symbol, None, "circuit_open"
            # Gapfill is a best-effort repair path. Keep one unresponsive
            # symbol from holding the whole daily run for the full timeout.
            try:
                from cnequity.domain.rate_limit import source_request

                if default_fetch:
                    # The adapter owns the exact HTTP boundary. Keeping
                    # the lease there matters if it adds a probe or retry
                    # request in the future; an outer lease would turn
                    # several wire calls into one slot/QPS event.
                    bars = fetch(symbol, client)
                else:
                    # Injected integrations historically receive only
                    # ``(symbol, client)``; guard their opaque operation at
                    # this boundary so custom network fetchers remain
                    # source-limited too.
                    with source_request(config, "sina_bars"):
                        bars = fetch(symbol, client)
            except Exception as exc:  # noqa: BLE001 — keep the rest of the board
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                retryable = status_code in _SINA_RETRY_STATUS_CODES
                if not retryable or attempt + 1 >= _SINA_FETCH_ATTEMPTS:
                    logger.warning("sina bars failed for %s: %s", symbol, exc)
                    if status_code in _SINA_RATE_LIMIT_STATUS_CODES:
                        reason = "rate_limited"
                    elif status_code is not None:
                        reason = "http_error"
                    elif isinstance(exc, httpx.TransportError):
                        reason = "transport_error"
                    else:
                        reason = "parse_or_adapter_error"
                    return symbol, None, reason
                if status_code in _SINA_RATE_LIMIT_STATUS_CODES:
                    rate_limit_failures += 1
                    if rate_limit_failures >= 2:
                        config.defer_source("sina_bars", _SINA_RATE_LIMIT_CIRCUIT_SECONDS)
                        config._sina_bar_circuit = (run_id, True)
                        logger.warning(
                            "sina bars repeated HTTP %s; opening run-local circuit and "
                            "cooling all Sina lanes for %.0fs",
                            status_code,
                            _SINA_RATE_LIMIT_CIRCUIT_SECONDS,
                        )
                        return symbol, None, "rate_limited"
                    delay = _SINA_RATE_LIMIT_COOLDOWN_SECONDS
                    config.defer_source("sina_bars", delay)
                    logger.warning(
                        "sina bars HTTP %s for %s; vendor-wide cooldown %.0fs before one retry",
                        status_code,
                        symbol,
                        delay,
                    )
                    continue
                delay = max(float(getattr(config, "retry_backoff_seconds", 5)), 1.0) * (attempt + 1)
                logger.warning(
                    "sina bars transient HTTP %s for %s; retrying in %.1fs (%d/%d)",
                    status_code,
                    symbol,
                    delay,
                    attempt + 1,
                    _SINA_FETCH_ATTEMPTS - 1,
                )
                time.sleep(delay)
                continue
            if bars.is_empty():
                return symbol, None, "source_empty"
            return symbol, bars, None
        return symbol, None, "failed"

    with httpx.Client(timeout=8.0) as client:
        if use_parallel and requested_symbols:
            workers = min(
                config.source_concurrency_for("sina_bars", default=1),
                len(requested_symbols),
            )
            with ThreadPoolExecutor(max_workers=workers) as pool:
                results = list(
                    pool.map(lambda symbol: fetch_one(symbol, client), requested_symbols)
                )
        else:
            results = [fetch_one(symbol, client) for symbol in requested_symbols]
    for symbol, bars, failure_kind in results:
        if failure_kind is not None:
            failed.append(symbol)
            failure_reasons[failure_kind] = failure_reasons.get(failure_kind, 0) + 1
            if failure_kind == "source_empty":
                empty.append(symbol)
            continue
        assert bars is not None
        covered_dates[symbol] = set(bars["trade_date"].to_list())
        frames.append(bars)

    expected_dates = set(list_trading_dates(config, start, end))
    for symbol in requested_symbols:
        if symbol in failed:
            continue
        if expected_dates - covered_dates.get(symbol, set()):
            failed.append(symbol)

    supplement_findings: list[dict] = []
    if frames:
        merged = pl.concat(frames, how="diagonal_relaxed")
        # Every fallback is append-only with respect to keys already staged by
        # TDX, exchange quotes, or an earlier route. This is the write-side
        # enforcement of source priority; compact ordering never has to guess.
        from cnequity.storage import StagingWriter

        files = StagingWriter(config.staging_root).list_run_files("daily_bars", run_id)
        if files:
            existing = (
                pl.scan_parquet([str(path) for path in files])
                .select("symbol", "trade_date")
                .unique()
                .collect()
            )
            merged = merged.join(existing, on=["symbol", "trade_date"], how="anti")
        if start == end and not bse_attempted:
            merged, supplement_findings = _supplement_bse_tip_amounts(
                config, merged, trade_date=start, symbols=requested_symbols
            )
        if not merged.is_empty():
            out = write_fetched(
                config,
                run_id,
                "daily_bars",
                merged,
                source="sina",
                batch_id=f"{batch_prefix}-0000",
            )
            rows += int(out.get("rows_written", 0))

    result: dict = {"rows_read": rows, "rows_written": rows}
    audit_findings.extend(supplement_findings)
    if failed:
        result["failed_symbols"] = len(failed)
        # Keep the names as well as the count. The daily step can then route
        # failed fallback symbols through the same historical gap-fill as TDX
        # failures; a count alone cannot identify which keys need recovery.
        result["failed_symbol_names"] = list(dict.fromkeys(failed))
        result["empty_symbol_names"] = list(dict.fromkeys(empty))
        audit_findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "fallback_source_incomplete",
                "message": (
                    f"{len(failed)}/{len(requested_symbols)} symbols without a TDX route "
                    f"failed to fetch from the fallback vendor "
                    f"(e.g. {', '.join(failed[:5])})"
                ),
                "empty_symbols": len(empty),
            }
        )
    if audit_findings:
        result["context_updates"] = {"audit_findings": audit_findings}
    result["source_outcomes"] = {
        "sina": {
            "status": "success" if not failed else ("partial" if rows else "failed"),
            "requests": len(requested_symbols),
            "rows_written": rows,
            "failed_symbols": len(failed),
            "empty_symbols": len(empty),
            "failure_reasons": dict(sorted(failure_reasons.items())),
        }
    }
    return result


def _validate_index_bar_coverage(
    config: Config,
    df,
    start: date,
    end: date,
) -> None:
    """Reject an index window with an interior symbol×session hole."""
    if df.is_empty():
        raise RuntimeError(f"index_bars: no rows returned for {start}..{end}")
    expected_symbols = {f"{code}.{exchange}" for code, exchange in INDEX_SYMBOLS}
    observed_symbols = set(df["symbol"].unique().to_list())
    missing_symbols = sorted(expected_symbols - observed_symbols)
    if missing_symbols:
        raise RuntimeError("index_bars: missing complete series for " + ", ".join(missing_symbols))

    sessions = list_trading_dates(config, start, end)
    if not sessions:
        return
    observed = df.select("symbol", "trade_date").unique()
    missing: list[tuple[str, date]] = []
    for symbol in sorted(expected_symbols):
        have = set(observed.filter(observed["symbol"] == symbol)["trade_date"].to_list())
        missing.extend((symbol, session) for session in sessions if session not in have)
    if missing:
        sample = ", ".join(f"{symbol}@{session.isoformat()}" for symbol, session in missing[:8])
        raise RuntimeError(
            f"index_bars: {len(missing)} symbol×trading-session key(s) missing "
            f"in {start}..{end} (e.g. {sample})"
        )


@register_step("index_bars", group="core", depends_on=["instruments"])
def step_index_bars(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        start, end = _backfill_window(config, trade_date)
    else:
        start = incremental_window(config, "index_bars", trade_date)
        end = trade_date
    # Index daily bars have the same current-session semantics as stock daily
    # bars. Without this guard a pre-close run can stage a plausible but
    # incomplete index bar and advance the index coverage watermark.
    _reject_unfinished_daily_bar_window(config, end)
    rl = config.tdx_rate_limit_spec()
    df = fetch_index_bars(
        start,
        end,
        rate_limit=rl,
        allow_mock=config.tdx_allow_mock,
        backfill=getattr(config, "_backfill", False),
        config=config,
    )
    df = normalize_with_source(df, "tdx_protocol")
    _validate_index_bar_coverage(config, df, start, end)
    from cnequity.steps.common import write_simple

    return write_simple(config, run_id, "index_bars", df)


# The primary vendor serves 2016 onward; 同花顺 keeps per-year files back to each
# listing. Deep history is a separate step, not a wider window on the daily one:
# it uses a different source, runs for hours, and must never be on the daily path.
HISTORY_BACKFILL_START = date(2001, 1, 1)


def _validate_planned_stock_bars(
    rows: list[dict], symbol: str, start: date, end: date
) -> list[dict]:
    """Normalize and validate one THS response before adding it to a batch."""
    normalized: list[dict] = []
    for row in rows:
        if not isinstance(row, dict) or row.get("symbol") != symbol:
            raise RuntimeError(f"THS history returned a row for an unexpected symbol: {symbol}")
        raw_date = row.get("trade_date")
        if isinstance(raw_date, datetime):
            trade_date = raw_date.date()
        elif isinstance(raw_date, date):
            trade_date = raw_date
        elif isinstance(raw_date, str):
            try:
                trade_date = date.fromisoformat(raw_date)
            except ValueError as exc:
                raise RuntimeError(
                    f"THS history returned an invalid trade_date for {symbol}"
                ) from exc
        else:
            raise RuntimeError(f"THS history returned an invalid trade_date for {symbol}")
        if not start <= trade_date <= end:
            raise RuntimeError(
                f"THS history returned {symbol} row outside requested window "
                f"{start.isoformat()}..{end.isoformat()}: {trade_date.isoformat()}"
            )
        normalized_row = dict(row)
        normalized_row["trade_date"] = trade_date
        normalized.append(normalized_row)
    return normalized


@register_step(
    "daily_bars_history",
    group="backfill",
    depends_on=["instruments"],
)
def step_daily_bars_history(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """Backfill pre-2016 unadjusted daily bars from 同花顺.

    Writes into ``daily_bars`` like the daily step, so `compact` and every reader
    treat the older rows identically. Only raw prices are fetched — hfq stays
    derived from the Sina factors already in use, which reach back to listing, so
    one adjustment convention spans the whole series (verified continuous across
    the 2015→2016 seam at 0.0bps).
    """
    import polars as pl

    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.storage import StagingWriter

    start = getattr(config, "_backfill_start", None) or HISTORY_BACKFILL_START
    # Same normalisation as `_backfill_window`: one meaning for `_backfill_end`
    # across every reader, or a Saturday means "Friday" in one step and
    # "a session with no rows" in the next.
    end = last_session_on_or_before(
        config, getattr(config, "_backfill_end", None) or date(2015, 12, 31)
    )
    plan = _history_plan(config, start, end)
    resume = set(context.get("_history_done") or [])
    if resume:
        plan = [p for p in plan if p[0] not in resume]

    requests = sum((end.year - s.year + 1) for _, s in plan)
    logger.info(
        "daily_bars_history: %d symbols, %s..%s, ~%d year-requests "
        "(ETF/LOF included; 北交所 remains outside this SH/SZ history source)",
        len(plan),
        start,
        end,
        requests,
    )
    writer = StagingWriter(config.staging_root)
    written = 0
    batch_no = 0

    def _flush(rows: list[dict], done: list[str]) -> None:
        nonlocal written, batch_no
        if not rows:
            return
        batch_no += 1
        df = with_provenance(
            pl.DataFrame(rows), source="ths", data_version=data_version_for("daily_bars")
        )
        writer.write_batch("daily_bars", run_id, f"history-{batch_no:04d}", df)
        written += df.height
        logger.info(
            "daily_bars_history: batch %d — %d rows, %d symbols", batch_no, df.height, len(done)
        )

    failed = sweep_stock_bars_planned(plan, end, config=config, on_batch=_flush)
    return {
        "dataset": "daily_bars",
        "rows_read": written,
        "rows_written": written,
        "symbols": len(plan),
        "failed_symbols": len(failed),
        "note": f"{start}..{end} via 同花顺 (raw only; hfq derives from Sina factors)",
    }


def _history_plan(config: Config, start: date, end: date) -> list[tuple[str, date]]:
    """``[(symbol, fetch_start)]`` for the symbols worth fetching.

    Two filters and a per-symbol window, which together cut the sweep by ~78%:

    * Stocks and ETFs/LOFs. Both carry Sina hfq factors and an enriched
      ``list_date``, so deeper raw bars can be served as hfq with one
      adjustment convention. 北交所 is excluded because this THS history
      route is limited to SH/SZ.
      An ETF with no ``list_date`` is an unlisted placeholder (or an enrichment
      gap) with no verifiable history, so it is skipped rather than planned and
      failed.
    * Nothing listed after the window. A 2016 IPO has no pre-2016 history, and
      asking for it is ~2600 symbols' worth of empty year files.
    * The rest start at their listing year rather than at ``start``.
    """
    # The factor pipeline has no Beijing coverage.  Filtering only 92xxxx
    # misses legacy BSE/NEEQ codes (43/83/87xxxx), which are also represented as
    # ``.BJ`` in the instrument lake and must not enter this THS-only history
    # path.
    symbols = [s for s in load_symbols(config) if not s.upper().endswith(".BJ")]
    inst = load_curated_instruments(config)
    if inst is None:
        # No instruments to plan against: fall back to the full window rather
        # than silently fetching nothing.
        return [(s, start) for s in symbols]
    inst = inst.select("symbol", "list_date", "asset_type")
    meta = {r["symbol"]: r for r in inst.to_dicts()}

    plan: list[tuple[str, date]] = []
    for sym in symbols:
        row = meta.get(sym)
        if row is None:
            continue
        asset_type = row.get("asset_type")
        if asset_type not in ("stock", "etf"):
            continue
        listed = row.get("list_date")
        if asset_type == "etf" and listed is None:
            continue
        if listed is not None:
            if listed > end:
                continue
            if listed > start:
                plan.append((sym, date(listed.year, 1, 1)))
                continue
        plan.append((sym, start))
    return plan


def sweep_stock_bars_planned(
    plan: list[tuple[str, date]],
    end: date,
    *,
    config: Config,
    on_batch,
    batch_size: int = 50,
) -> list[str]:
    """Sweep a per-symbol plan, batching writes. Returns failed symbols."""
    from cnequity.adapters.ths.stock_bars import fetch_stock_bars

    rows: list[dict] = []
    batch: list[str] = []
    failed: list[str] = []
    streak = 0
    for i, (symbol, sym_start) in enumerate(plan, start=1):
        try:
            got = fetch_stock_bars(symbol, sym_start, end, config=config)
            if not got:
                # A missing pre-IPO year is normal inside fetch_stock_bars, but
                # an entire planned window with no usable rows is not a success:
                # this symbol was eligible for the history sweep and would
                # otherwise disappear from the quality signal and retry scope.
                raise RuntimeError(
                    f"THS history returned no usable bars for {symbol} in "
                    f"{sym_start.isoformat()}..{end.isoformat()}"
                )
            rows.extend(_validate_planned_stock_bars(got, symbol, sym_start, end))
            batch.append(symbol)
            streak = 0
        except Exception as exc:  # noqa: BLE001 — recorded, sweep continues
            logger.warning("THS history failed for %s: %s", symbol, exc)
            failed.append(symbol)
            streak += 1
            if streak >= 10:
                logger.error("THS: %d consecutive failures at %s — aborting", streak, symbol)
                break
        if i % batch_size == 0 or i == len(plan):
            on_batch(rows, batch)
            rows, batch = [], []
    if batch:
        on_batch(rows, batch)
    return failed


# Rosters are sampled rather than walked day by day: a stock that traded at all
# appears on some quarter-end, and 40 roster queries beat 2,500.
_ROSTER_SAMPLE_MONTHS = (3, 6, 9, 12)


def _delisted_universe(config: Config, start: date, end: date) -> list[str]:
    """Symbols that traded in the window but hold no bars in the lake.

    Compares baostock's historical rosters against what daily_bars actually
    carries. Anything present then and absent now is a name the current-roster
    snapshot lost — the survivorship gap, 16.8% of the cross-section on
    2016-06-30 and still 6.0% on 2020-06-30.
    """
    from cnequity.adapters.baostock._session import _login, import_baostock
    from cnequity.adapters.baostock.delisted_bars import roster_on
    from cnequity.query.parquet_scan import scan_parquet_root

    bars_root = config.curated_root / "daily_bars"
    bars = scan_parquet_root(bars_root, partition_col="trade_date", hive=False, traded_only=True)
    have = set(bars.select("symbol").unique().collect()["symbol"].to_list())

    bs = import_baostock()
    # Keep compatibility with test/integration doubles that expose the
    # historical one-argument _login hook; real login traffic is still held
    # under the source lease when this path uses the built-in helper.
    with source_request(config, "baostock"):
        _login(bs)
    missing: set[str] = set()
    try:
        for year in range(start.year, end.year + 1):
            for month in _ROSTER_SAMPLE_MONTHS:
                day = date(year, month, 28)
                if not (start <= day <= end):
                    continue
                roster = roster_on(day, bs=bs, login=False, config=config)
                if not roster:
                    continue
                gap = roster - have
                if gap:
                    logger.info(
                        "roster %s: %d stocks, %d absent from daily_bars",
                        day,
                        len(roster),
                        len(gap),
                    )
                missing |= gap
    finally:
        with source_request(config, "baostock"):
            bs.logout()
    return sorted(missing)


@register_step(
    "daily_bars_delisted",
    group="backfill",
    depends_on=["instruments"],
)
def step_daily_bars_delisted(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """Recover bars for stocks that delisted inside the window.

    The live vendors serve only what currently trades, so this is the one path
    that can close the survivorship gap; baostock keeps each delisted name
    through its final session. Rows land in ``daily_bars`` like any other, and
    hfq keeps deriving from the Sina factors, which still cover these symbols.
    """
    import polars as pl

    from cnequity.adapters.baostock.delisted_bars import fetch_delisted_bars
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.storage import StagingWriter

    start = getattr(config, "_backfill_start", None) or date(2016, 1, 1)
    end = last_session_on_or_before(config, getattr(config, "_backfill_end", None) or trade_date)
    symbols = context.get("_delisted_symbols") or _delisted_universe(config, start, end)
    if not symbols:
        return {"rows_read": 0, "rows_written": 0, "note": "no survivorship gap found"}

    logger.info("daily_bars_delisted: %d recovered symbols, %s..%s", len(symbols), start, end)
    rows, failed = fetch_delisted_bars(symbols, start, end, config=config)
    written = 0
    if rows:
        df = with_provenance(
            pl.DataFrame(rows), source="baostock", data_version=data_version_for("daily_bars")
        )
        StagingWriter(config.staging_root).write_batch("daily_bars", run_id, "delisted-0000", df)
        written = df.height
    return {
        "dataset": "daily_bars",
        "rows_read": written,
        "rows_written": written,
        "symbols": len(symbols),
        "failed_symbols": len(failed),
        "note": f"survivorship repair {start}..{end} via baostock",
    }


def _withhold_unbacked_disputes(
    frame: pl.DataFrame,
    joined: pl.DataFrame,
    adjudicator: pl.DataFrame,
    totals: dict,
) -> pl.DataFrame:
    """Drop the disputed rows an independent source does not back.

    A row whose close already matches is switched for its provenance alone. A
    row whose close differs is only switched when the third source agrees with
    the peer; otherwise the existing value stays, because importing a known
    regression to gain provenance on one row is a bad trade.
    """
    tolerance = 5e-5
    disputed = joined.filter(
        (pl.col("close_peer") - pl.col("close")).abs() > pl.col("close").abs() * tolerance
    ).select("symbol", "trade_date", "close", "close_peer")
    if disputed.is_empty():
        return frame

    judged = disputed.join(
        adjudicator.select("symbol", "trade_date", pl.col("close").alias("_third")),
        on=["symbol", "trade_date"],
        how="left",
    )
    backs_peer = judged.filter(
        pl.col("_third").is_not_null()
        & (
            (pl.col("_third") - pl.col("close_peer")).abs()
            <= pl.col("close_peer").abs() * tolerance
        )
    )
    withheld = judged.join(backs_peer, on=["symbol", "trade_date"], how="anti").select(
        "symbol", "trade_date"
    )
    totals["disputes_backed"] = totals.get("disputes_backed", 0) + backs_peer.height
    totals["disputes_withheld"] = totals.get("disputes_withheld", 0) + withheld.height
    if withheld.is_empty():
        return frame
    return frame.join(withheld, on=["symbol", "trade_date"], how="anti")


def _append_diff(path: Path, frame: pl.DataFrame) -> None:
    """Accumulate the diff across chunks, so a long sweep survives inspection.

    Writes through ``write_parquet_atomic``: this was the one parquet writer
    in the tree that truncated its destination inode in place, which a
    reader holding the previous footer sees as a corrupt file, and which
    would silently damage any generation that hardlinks this path.
    """
    from cnequity.storage.atomic import write_parquet_atomic

    if frame.is_empty():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        frame = pl.concat([pl.read_parquet(path), frame], how="vertical")
    write_parquet_atomic(path, frame)


def repair_deep_history_ths_official(
    config: Config,
    run_id: str,
    *,
    start: date,
    end: date,
    symbols: list[str] | None = None,
    chunk_size: int = 100,
    workers: int = 4,
    dry_run: bool = True,
    diff_out: Path | None = None,
    adjudicator: pl.DataFrame | None = None,
) -> dict:
    """Re-source the 2005-2015 block from the licensed peer instead of the scraper.

    This is **switching**, not routing ([ADR-0005](../../docs/adr/0005-source-routing-vs-switching.md)):
    4,403,582 rows already have a canonical owner, so nothing here happens on a
    schedule and ``dry_run`` defaults to true. A caller has to ask twice.

    The case for asking is provenance rather than accuracy. ``daily_bars`` splits
    cleanly: ``ths`` — an unauthenticated scrape of 10jqka's public pages, which
    ``sources/SOURCES.yml`` records as an unregistered client — owns
    2001-01-02..2015-12-31 alone, while ``tdx_protocol`` owns 2016 onward with a
    configured backup. Re-sourcing the part the official API reaches puts 82.3%
    of that block on a registered footing. The 949,815 rows before 2005 are
    outside the service's floor and keep their existing source.

    Accuracy barely moves either way. Measured 2026-09-12 over 100 securities and
    177,914 comparable rows, the two disagree on 117 (0.066%), clustered on a
    handful of dates — 23 of them on 2015-05-08 alone — and usually by a single
    tick. Neither side can be shown right from inside the lake: both closes sit
    within their own high/low range. What settles the choice is that both series
    are 同花顺's, and only one of them is the licensed reading.

    ``dry_run`` reports the diff without writing, so the size and shape of the
    change is known before it is made. ``diff_out`` writes the disputed rows
    themselves, which is what lets an independent third source settle whether
    the change is an improvement rather than just a different opinion.

    ``adjudicator`` is that third source: ``(symbol, trade_date, close)`` from a
    vendor sharing no lineage with either candidate — baostock, say. Both
    candidates here are 同花顺's, one scraped from its public pages and one from
    its licensed API, so they cannot arbitrate each other.

    With an adjudicator, a **disputed** row is only switched when the third
    source backs the peer. Measured 2026-09-12 over 551 adjudicated disputes the
    peer was right 373 times and the incumbent 178, with no dispute where all
    three differed — so each has a right answer, and taking the peer blindly
    would import 178 known regressions. A dispute the adjudicator has no opinion
    on keeps its existing value and source: 1,418 rows against 4,396,510 is a
    rounding error for the provenance this is for, and a guaranteed
    non-regression is worth more than those rows.

    Undisputed rows switch regardless — no value to protect, only provenance to
    improve.
    """
    from cnequity.adapters.ths_official import SOURCE as THS_SOURCE
    from cnequity.adapters.ths_official import client_from_config
    from cnequity.adapters.ths_official.bars import HISTORY_FLOOR, fetch_daily_bars
    from cnequity.query.canonical import dedupe_lazy_by_primary_key
    from cnequity.query.parquet_scan import scan_parquet_root
    from cnequity.steps.http_common import write_fetched

    if start < HISTORY_FLOOR:
        start = HISTORY_FLOOR
    client = client_from_config(config)
    if client is None:
        return {"status": "skipped", "reason": "no api key", "rows_written": 0}

    root = config.curated_root / "daily_bars"
    existing = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(root, partition_col="trade_date", start=start, end=end),
            "daily_bars",
        )
        .filter(pl.col("source") == "ths")
        .select("symbol", "trade_date", "open", "high", "low", "close", "volume", "amount")
        .collect()
    )
    if existing.is_empty():
        client.close()
        return {
            "status": "skipped",
            "reason": "no scraper-sourced rows in window",
            "rows_written": 0,
        }
    if symbols is None:
        symbols = sorted(existing.get_column("symbol").unique().to_list())

    totals = {
        "rows_written": 0,
        "compared": 0,
        "changed": 0,
        "only_curated": 0,
        "only_peer": 0,
        "unanswered": 0,
    }
    counters: dict[str, int] = {}
    unanswered_symbols: set[str] = set()
    try:
        for offset in range(0, len(symbols), chunk_size):
            chunk = symbols[offset : offset + chunk_size]
            frame, chunk_counters = fetch_daily_bars(
                chunk, start, end, client=client, workers=workers
            )
            unanswered = set(chunk_counters.pop("unanswered_symbols", ()) or ())
            for key, value in chunk_counters.items():
                counters[key] = counters.get(key, 0) + value
            if unanswered:
                totals["unanswered"] += len(unanswered)
                unanswered_symbols.update(unanswered)
            if frame.is_empty() and not unanswered:
                continue
            # A symbol the vendor never answered for looks exactly like one it
            # answered "nothing" for once the frame is built, so its curated
            # rows would be counted as evidence the peer lacks them. They are
            # evidence of a failed request and nothing else.
            before = existing.filter(
                pl.col("symbol").is_in(chunk) & ~pl.col("symbol").is_in(list(unanswered))
            )
            if before.is_empty() and frame.is_empty():
                continue
            joined = before.join(frame, on=["symbol", "trade_date"], how="inner", suffix="_peer")
            changed = joined.filter(
                (pl.col("close_peer") - pl.col("close")).abs() > pl.col("close").abs() * 5e-5
            ).height
            totals["compared"] += joined.height
            totals["changed"] += changed
            totals["only_curated"] += before.join(
                frame, on=["symbol", "trade_date"], how="anti"
            ).height
            peer_only = frame.join(before, on=["symbol", "trade_date"], how="anti")
            totals["only_peer"] += peer_only.height
            if diff_out is not None:
                disputed = joined.filter(
                    (pl.col("close_peer") - pl.col("close")).abs() > pl.col("close").abs() * 5e-5
                ).select(
                    "symbol",
                    "trade_date",
                    pl.col("close").alias("curated_close"),
                    pl.col("close_peer").alias("peer_close"),
                    pl.lit("changed").alias("kind"),
                )
                added = peer_only.select(
                    "symbol",
                    "trade_date",
                    pl.lit(None, dtype=pl.Float64).alias("curated_close"),
                    pl.col("close").alias("peer_close"),
                    pl.lit("peer_only").alias("kind"),
                )
                missing = before.join(frame, on=["symbol", "trade_date"], how="anti").select(
                    "symbol",
                    "trade_date",
                    pl.col("close").alias("curated_close"),
                    pl.lit(None, dtype=pl.Float64).alias("peer_close"),
                    pl.lit("curated_only").alias("kind"),
                )
                _append_diff(diff_out, pl.concat([disputed, added, missing], how="vertical"))
            if dry_run:
                continue
            staged = frame
            if adjudicator is not None:
                staged = _withhold_unbacked_disputes(frame, joined, adjudicator, totals)
            if staged.is_empty():
                continue
            written = write_fetched(
                config,
                run_id,
                "daily_bars",
                staged,
                source=THS_SOURCE,
                batch_id=f"ths-deep-{offset // chunk_size:04d}",
            )
            totals["rows_written"] += written.get("rows_written", frame.height)
    finally:
        client.close()

    totals.update(counters)
    totals["symbols"] = len(symbols)
    # Named so a reader can re-run exactly the scope that went unanswered
    # rather than the whole window.
    totals["unanswered_symbols"] = sorted(unanswered_symbols)
    totals["status"] = "dry_run" if dry_run else "applied"
    return totals


def snapshot_daily_bars_ths_official(
    config: Config,
    run_id: str,
    *,
    start: date,
    end: date,
    symbols: list[str] | None = None,
    sample: int = 400,
    workers: int = 4,
) -> dict:
    """Capture a third opinion on recent bars, for arbitration only.

    2016 onward is already tdx against eastmoney. Two vendors can disagree but
    cannot say which is wrong, and the revision gate has to decide something —
    so a binary comparison either blocks on noise or waves through a real break.

    Verification class: this writes to ``meta/source_snapshots`` and never to
    curated, so it is safe whenever a key is present and changes nothing about
    what the lake holds. Sampled rather than exhaustive, because the point is to
    arbitrate the days the two incumbents already disagree on, not to mirror the
    market.
    """
    from cnequity.adapters.ths_official import SOURCE as THS_SOURCE
    from cnequity.adapters.ths_official import client_from_config
    from cnequity.adapters.ths_official.bars import fetch_daily_bars
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
    from cnequity.storage.source_snapshots import SnapshotStore

    if not getattr(config, "ths_official_verify_enabled", True):
        return {"rows_written": 0, "status": "skipped", "reason": "verify off"}
    client = client_from_config(config)
    if client is None:
        return {"rows_written": 0, "status": "skipped", "reason": "no api key"}

    if symbols is None:
        root = config.curated_root / "daily_bars"
        if not dataset_has_parquet(root):
            client.close()
            return {"rows_written": 0, "status": "skipped", "reason": "no daily_bars"}
        # Stocks only. `/api/a-share/prices/historical` refuses an ETF outright
        # (`code=1002`), and ranking `daily_bars` by turnover puts ETFs at the
        # top — an unfiltered sample of 200 lost 42 to that before this guard.
        tradable = None
        inst_root = config.curated_root / "instruments"
        if dataset_has_parquet(inst_root):
            tradable = (
                scan_parquet_root(inst_root)
                .filter((pl.col("asset_type") == "stock") & pl.col("delist_date").is_null())
                .select("symbol")
                .collect()
                .get_column("symbol")
                .to_list()
            )
        # The most traded names: a thin stock's disagreement is usually an empty
        # auction rather than a data defect, and says little about either vendor.
        liquid = scan_parquet_root(root, partition_col="trade_date", start=start, end=end)
        if tradable:
            liquid = liquid.filter(pl.col("symbol").is_in(tradable))
        symbols = (
            liquid.group_by("symbol")
            .agg(pl.col("amount").median().alias("_amount"))
            .sort("_amount", descending=True)
            .limit(sample)
            .collect()
            .get_column("symbol")
            .to_list()
        )

    try:
        frame, counters = fetch_daily_bars(symbols, start, end, client=client, workers=workers)
    finally:
        client.close()
    if frame.is_empty():
        return {"rows_written": 0, "status": "warning", "reason": "peer returned nothing"}

    SnapshotStore(config.meta_root).write(
        "daily_bars",
        with_provenance(frame, source=THS_SOURCE, data_version=data_version_for("daily_bars")),
        source=THS_SOURCE,
        data_version=data_version_for("daily_bars"),
        run_id=run_id,
    )
    return {"rows_written": frame.height, "symbols": len(symbols), **counters}
