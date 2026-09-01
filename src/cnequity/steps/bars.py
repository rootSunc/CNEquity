"""L1 bar steps: daily_bars, index_bars."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import date, datetime

import polars as pl

from cnequity.adapters.tdx_protocol.client import (
    INDEX_SYMBOLS,
    fetch_index_bars,
    normalize_with_source,
)
from cnequity.config import Config
from cnequity.domain.frames import with_columns_unless_blank
from cnequity.domain.market_time import A_SHARE_FINAL_AT
from cnequity.domain.rate_limit import source_request
from cnequity.domain.symbols import split_by_quote_source
from cnequity.orchestrator.registry import register_step
from cnequity.orchestrator.worker_pool import fetch_daily_bars_parallel
from cnequity.query.canonical import dedupe_by_primary_key
from cnequity.steps.common import (
    BACKFILL_START,
    DailyBarOwnership,
    classify_daily_bar_ownership,
    incremental_window,
    instrument_metadata,
    list_trading_dates,
    load_bar_universe,
    load_curated_instruments,
    load_curated_trading_status,
    load_negative_evidence,
    load_symbols,
    record_negative_evidence,
    reject_unfinished_eod_window,
)

logger = logging.getLogger(__name__)

# The closing auction ends at 15:00. Leave a small settlement buffer before
# trusting TDX's current daily bar; the default core schedule starts at 16:00.
_DAILY_BAR_FINAL_AT = A_SHARE_FINAL_AT
_SINA_RETRY_STATUS_CODES = frozenset({429, 456, 500, 502, 503, 504})
_SINA_FETCH_ATTEMPTS = 3


def _reject_unfinished_daily_bar_window(
    config: Config,
    end: date,
    *,
    now: datetime | None = None,
) -> None:
    """Reject a window whose newest daily bar is still forming in Shanghai.

    Thin wrapper over :func:`cnequity.steps.common.reject_unfinished_eod_window`
    preserving the daily_bars message label. TDX ``start=0`` includes the
    current daily K: once trading begins that row has plausible OHLC and
    non-zero volume, so content checks cannot distinguish it from a settled
    bar. Refuse the fetch before any symbol batch starts so a market-wide
    sweep that crosses 15:05 never mixes partial and final bars in one
    curated partition.
    """
    reject_unfinished_eod_window(config, end, what="daily_bars", now=now)


def _backfill_window(config: Config, trade_date: date) -> tuple[date, date]:
    """``--start/--end`` window for a backfill, defaulting to the full history.

    Repairing a single bad session must not mean re-fetching a decade for every
    symbol. A capture that fires before the close writes a truncated bar — right
    open, wrong close, partial volume — and the repair is one day wide.
    """
    end = getattr(config, "_backfill_end", None) or trade_date
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


def _etf_placeholder_bar_universe(
    config: Config,
    spans: dict[str, tuple[date | None, date | None, str | None]],
) -> set[str] | None:
    """Return traded bars only when an undated ETF needs reconciliation.

    Scanning every daily_bars file is unnecessary for normal runs. An empty
    traded universe is also not evidence that every undated ETF is a
    placeholder, so leave the classifier conservative in a brand-new lake.
    """
    if not any(
        asset_type == "etf" and list_date is None
        for list_date, _delist_date, asset_type in spans.values()
    ):
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
                "check": "daily_bars_etf_placeholder_skipped",
                "message": (
                    f"{len(ownership.placeholder)} undated ETF/LOF placeholder(s) "
                    "skipped (no list_date and no traded bar; not verified "
                    f"no-data): {preview}{suffix}"
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
        bar_universe = _etf_placeholder_bar_universe(config, spans)
        metadata = instrument_metadata(config)
        evidence = load_negative_evidence(config, "daily_bars", metadata=metadata)
        remaining: list[tuple[str, list[str], date, date]] = []
        fallback_specs: list[tuple[str, list[str], date, date]] = []
        ownership = DailyBarOwnership()
        for batch_id, symbols, spec_start, spec_end in batch_specs:
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
                    superseded_by="ownership-etf-placeholder",
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
                fallback = fetch_bars_via_sina(
                    config,
                    fallback_symbols,
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
        _resolve_daily_bar_scope(config, explicit_scope)
        if explicit_scope is not None
        else load_symbols(config)
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
        bar_universe=_etf_placeholder_bar_universe(config, spans),
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

    # TDX has no Beijing exchange route at all — the protocol rejects the market id —
    # so BJ symbols must come from the fallback vendor or they silently never
    # arrive, which is exactly how the lake ended up with zero BJ coverage.
    # Tip gaps after TDX are a second routing case (ADR-0005): EastMoney clist.
    fetch_scope = list(dict.fromkeys(ownership.generic + ownership.unknown))
    tdx_symbols, fallback_symbols = split_by_quote_source(fetch_scope)
    reused_symbols = _reuse_successful_daily_bars(config, run_id, fetch_scope, start, end)
    fetch_tdx_symbols = [symbol for symbol in tdx_symbols if symbol not in reused_symbols]
    fetch_fallback_symbols = [symbol for symbol in fallback_symbols if symbol not in reused_symbols]
    result = fetch_daily_bars_parallel(
        config,
        fetch_tdx_symbols,
        start,
        end,
        run_id,
        "daily_bars",
    )
    sina_result = None
    if fetch_fallback_symbols:
        sina_result = fetch_bars_via_sina(
            config, fetch_fallback_symbols, start, end, run_id, batch_prefix="sina"
        )
    out = _finish_daily_bars(
        config,
        trade_date,
        run_id,
        start=start,
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

    ``explicit_no_data`` comes from the pre-fetch ownership classifier.  The
    fallback's symbol-specific empty response is also accepted as a bounded
    negative observation for this exact request and persisted by the caller.
    Everything else — including a transport failure or a partial status
    snapshot — stays unknown.  There is intentionally no market-size based
    allowance here.
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


def _finish_daily_bars(
    config: Config,
    trade_date: date,
    run_id: str,
    *,
    start: date,
    end: date,
    expected_tdx_symbols: list[str],
    expected_fallback_symbols: list[str] | None = None,
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
        source_empty_symbols.update(sina_result.get("empty_symbol_names") or [])
        sina_findings = (sina_result.get("context_updates") or {}).get("audit_findings") or []
        findings.extend(sina_findings)

    tip = start == end
    historical_tip = tip and end != trade_date
    if tip:
        expected_symbols = set(expected_tdx_symbols) | set(expected_fallback_symbols or [])
        if not historical_tip:
            staged_before_gapfill = _staged_daily_bar_symbols(config, run_id, end)
            had_tip_gap = bool(expected_symbols - staged_before_gapfill)
            gap = _gapfill_tip_via_clist(config, end, run_id, expected_symbols=expected_tdx_symbols)
            rows_read += int(gap.get("rows_read", 0))
            rows_written += int(gap.get("rows_written", 0))
            findings.extend(gap.get("audit_findings") or [])
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
            gap = _gapfill_multiday_via_kline(
                config,
                run_id,
                symbols=sorted(failed_set),
                start=start,
                end=end,
            )
            rows_read += int(gap.get("rows_read", 0))
            rows_written += int(gap.get("rows_written", 0))
            findings.extend(gap.get("audit_findings") or [])
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
            gap = _gapfill_multiday_via_kline(
                config,
                run_id,
                symbols=partial_only,
                start=start,
                end=end,
                require_complete=False,
            )
            rows_read += int(gap.get("rows_read", 0))
            rows_written += int(gap.get("rows_written", 0))
            findings.extend(gap.get("audit_findings") or [])
            explicit_no_data.update(gap.get("expected_no_data_symbols") or [])
            source_empty_symbols.update(gap.get("expected_no_data_symbols") or [])

    # A source can return at least one row for every symbol while silently
    # omitting an interior session.  The symbol-level missing check below
    # cannot see that case, so validate the full ``symbol×session`` key set
    # before allowing any batch to remain successful.  Mark the owning worker
    # batches stale so the normal retry path will fetch the exact window again;
    # otherwise a raised step would leave successful receipts that
    # ``retry_failed_only`` is allowed to skip.
    if not tip and (expected_tdx_symbols or expected_fallback_symbols):
        all_expected_symbols = list(
            dict.fromkeys((expected_tdx_symbols or []) + (expected_fallback_symbols or []))
        )
        missing_pairs = _staged_daily_bar_missing_keys(
            config, run_id, all_expected_symbols, start, end
        )
        if missing_pairs:
            missing_symbols = {symbol for symbol, _day in missing_pairs}
            finding = {
                "dataset": "daily_bars",
                "severity": "error",
                "check": "daily_bars_interior_gap",
                "message": (
                    f"daily_bars {start}..{end}: {len(missing_pairs)} interior "
                    "symbol×session key(s) remain absent; refusing to checkpoint"
                ),
                "missing_keys": len(missing_pairs),
                "missing_symbols": sorted(missing_symbols),
                "sample_keys": [
                    {"symbol": symbol, "trade_date": day.isoformat()}
                    for symbol, day in sorted(missing_pairs)[:8]
                ],
            }
            findings.append(finding)
            _mark_unresolved_daily_bar_batches(
                config,
                run_id,
                missing_symbols,
                start=start,
                end=end,
            )
            raise RuntimeError(finding["message"])

    _reject_preopen_placeholder(config, run_id, end)

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
                findings.append(
                    {
                        "dataset": "daily_bars",
                        "severity": "error",
                        "check": "daily_bars_unknown_missing_symbols",
                        "message": (
                            f"daily_bars {end}: {len(unknown)} expected tip key(s) "
                            "remain unknown after primary/fallback and gap-fill; "
                            f"refusing to checkpoint: {preview}{suffix}"
                        ),
                        "missing_keys": len(unknown),
                        "symbols": sorted(unknown),
                    }
                )
                if not staged:
                    raise RuntimeError(
                        f"daily_bars {end}: primary/fallback and EastMoney clist/kline "
                        f"gap-fill produced no staged tip rows for {len(unknown)} "
                        "unknown key(s)"
                    )
                raise RuntimeError(
                    f"daily_bars {end}: {len(unknown)} expected tip key(s) remain "
                    "unknown after failover; refusing to checkpoint a partial "
                    "market snapshot"
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
                findings.append(
                    {
                        "dataset": "daily_bars",
                        "severity": "error",
                        "check": "daily_bars_unknown_missing_symbols",
                        "message": (
                            f"daily_bars {start}..{end}: {len(unknown)} expected key(s) "
                            "remain unknown after failover; refusing to checkpoint: "
                            f"{preview}{suffix}"
                        ),
                        "missing_keys": len(unknown),
                        "symbols": sorted(unknown),
                    }
                )
                raise RuntimeError(
                    f"daily_bars {start}..{end}: {len(unknown)} expected key(s) remain "
                    "unknown after failover; refusing "
                    "to checkpoint a partial market snapshot"
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
    result["metrics"] = metrics
    if findings:
        result["context_updates"] = {"audit_findings": findings}
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
            error_message="resolved by Sina/EastMoney gap-fill or verified expected no-data",
        )


def _mark_unresolved_daily_bar_batches(
    config: Config,
    run_id: str,
    symbols: set[str],
    *,
    start: date,
    end: date,
) -> None:
    """Keep a partial multi-day window retryable after final validation fails."""
    if not symbols:
        return
    from cnequity.orchestrator.manifest import Manifest

    manifest = Manifest(config.manifest_path)
    for batch in manifest.get_batches_for_run(run_id):
        if batch["dataset"] != "daily_bars" or batch["task_id"] != "daily_bars":
            continue
        try:
            batch_symbols = set(json.loads(batch["symbols_json"] or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            batch_symbols = set()
        if not batch_symbols.intersection(symbols):
            continue
        batch_start = batch["window_start"]
        batch_end = batch["window_end"]
        if batch_start and batch_end:
            try:
                if date.fromisoformat(batch_end) < start or date.fromisoformat(batch_start) > end:
                    continue
            except ValueError:
                # An invalid window is already a manifest contract problem;
                # keep it blocking and retryable rather than silently skip it.
                pass
        if batch["status"] in {"success", "running"}:
            manifest.mark_batch_stale(
                run_id,
                batch["batch_id"],
                "daily_bars interior symbol×session gap requires retry",
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
    observed = staged.group_by("symbol").agg(pl.col("trade_date").unique().alias("dates"))
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
        return {"rows_read": 0, "rows_written": 0, "filled": False}
    staged = _staged_daily_bar_symbols(config, run_id, trade_date)
    missing = [s for s in expected_symbols if s not in staged]
    if not missing:
        return {"rows_read": 0, "rows_written": 0, "filled": False}

    spec = failover_spec(config, "daily_bars")
    if spec is None or not config.sources.get(spec.backup, True):
        return {
            "rows_read": 0,
            "rows_written": 0,
            "filled": False,
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


def _gapfill_multiday_via_kline(
    config: Config,
    run_id: str,
    *,
    symbols: list[str],
    start: date,
    end: date,
    require_complete: bool = True,
) -> dict:
    """Stage a secondary kline source for failed or partially covered symbols.

    Sina is tried first for a complete failed TDX batch because it is reachable
    from the overseas deployment.  EastMoney remains a second fallback for any
    Sina misses.  The path is only entered for symbols that already failed TDX
    and never changes an existing TDX row.
    """
    import polars as pl

    from cnequity.adapters.eastmoney.bars import fetch_daily_bars as fetch_em_kline
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.quality.failover import failover_spec, write_backup_snapshot
    from cnequity.storage import StagingWriter

    if not symbols:
        return {"rows_read": 0, "rows_written": 0, "filled": False}

    sina_rows = 0
    sina_failed: set[str] = set()
    sina_empty: set[str] = set()
    sina_findings: list[dict] = []
    # A complete failed batch has no TDX rows to protect.  Do not use this
    # shortcut for partial-only repair calls, where the staging area may
    # already contain valid TDX rows for the same symbol.
    if require_complete and config.sources.get("sina", True):
        sina = fetch_bars_via_sina(
            config,
            symbols,
            start,
            end,
            run_id,
            batch_prefix="sina-kline-gapfill",
        )
        sina_rows = int(sina.get("rows_written", 0))
        sina_failed = set(sina.get("failed_symbol_names") or [])
        sina_empty = set(sina.get("empty_symbol_names") or [])
        sina_findings.extend((sina.get("context_updates") or {}).get("audit_findings") or [])
        if sina_empty:
            sina_findings.append(
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_sina_expected_no_data",
                    "message": (
                        f"Sina returned no bars for {len(sina_empty)} symbol(s) over "
                        f"{start}..{end}; treated as expected no-data after the "
                        "primary TDX batch failed"
                    ),
                    "symbols": sorted(sina_empty),
                }
            )
        if sina_rows:
            sina_findings.append(
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_sina_gapfill",
                    "message": (
                        f"routed {sina_rows} row(s) through Sina for "
                        f"{len(symbols)} failed TDX symbol(s)"
                    ),
                    "symbols": len(symbols),
                    "unresolved_symbols": len(sina_failed),
                    "complete": not sina_failed,
                }
            )
        if not sina_failed:
            return {
                "rows_read": sina_rows,
                "rows_written": sina_rows,
                "filled": bool(sina_rows),
                "complete": True,
                "audit_findings": sina_findings,
            }
        # An empty, non-error response is an explicit no-data signal from
        # Sina.  Do not spend another long request on those symbols; this is
        # common for newly listed or non-price ETF instruments that TDX also
        # cannot serve.
        unresolved = sorted(sina_failed - sina_empty)
        if not unresolved:
            return {
                "rows_read": sina_rows,
                "rows_written": sina_rows,
                "filled": bool(sina_rows) or bool(sina_empty),
                "complete": True,
                "expected_no_data_symbols": sorted(sina_empty),
                "audit_findings": sina_findings,
            }
        # Only ask EastMoney about what Sina could not supply.  This keeps the
        # request bounded and prevents a successful Sina row from being
        # overwritten by a later backup source.
        symbols = unresolved

    spec = failover_spec(config, "daily_bars")
    if spec is None or not config.sources.get(spec.backup, True):
        return {
            "rows_read": sina_rows,
            "rows_written": sina_rows,
            "filled": bool(sina_rows) or bool(sina_empty),
            "complete": not (sina_failed - sina_empty),
            "expected_no_data_symbols": sorted(sina_empty),
            "audit_findings": sina_findings,
        }

    # EastMoney is a secondary path and is intermittently returning 502s from
    # the overseas proxy. Bound this repair request more tightly than the
    # normal vendor timeout so one failed batch cannot stall daily for minutes
    # after Sina has already been tried.
    df = fetch_em_kline(symbols, start, end, config=config, timeout_sec=8.0)
    if df.is_empty():
        return {
            "rows_read": sina_rows,
            "rows_written": sina_rows,
            "filled": bool(sina_rows) or bool(sina_empty),
            "complete": not (sina_failed - sina_empty),
            "expected_no_data_symbols": sorted(sina_empty),
            "audit_findings": [
                *sina_findings,
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_kline_gapfill",
                    "message": (
                        f"TDX/Sina coverage was incomplete for {len(symbols)} "
                        f"symbol(s) over {start}..{end}; EastMoney kline returned no rows"
                    ),
                },
            ],
        }

    expected_dates = list_trading_dates(config, start, end)
    expected_symbols = set(symbols) - sina_empty
    expected_keys = {(symbol, day) for symbol in expected_symbols for day in expected_dates}
    actual_keys = set(zip(df["symbol"].to_list(), df["trade_date"].to_list(), strict=True))

    # Drop symbol×date pairs already staged so we never overwrite TDX rows.
    files = StagingWriter(config.staging_root).list_run_files("daily_bars", run_id)
    if files:
        existing = (
            pl.scan_parquet([str(f) for f in files])
            .select("symbol", "trade_date")
            .unique()
            .collect()
        )
        gap_df = df.join(existing, on=["symbol", "trade_date"], how="anti")
    else:
        gap_df = df

    existing_keys = (
        set(zip(existing["symbol"].to_list(), existing["trade_date"].to_list(), strict=True))
        if files
        else set()
    )
    missing_keys = expected_keys - (actual_keys | existing_keys)

    if gap_df.is_empty():
        audit_findings = []
        if missing_keys:
            audit_findings.append(
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_kline_gapfill",
                    "message": (
                        f"EastMoney kline added no new rows for {len(symbols)} symbol(s) over "
                        f"{start}..{end}; {len(missing_keys)} key(s) remain absent "
                        "(may be suspended)"
                    ),
                    "missing_keys": len(missing_keys),
                    "complete": False,
                }
            )
        return {
            "rows_read": sina_rows + df.height,
            "rows_written": sina_rows,
            "filled": True,
            "complete": not missing_keys,
            "audit_findings": [*sina_findings, *audit_findings],
        }

    gap_df = with_provenance(
        gap_df, source=spec.backup, data_version=data_version_for("daily_bars")
    )
    write_backup_snapshot(
        config,
        "daily_bars",
        gap_df,
        run_id=run_id,
        batch_id="em-kline-snapshot",
        source=spec.backup,
        trade_date=end,
    )
    batch_id = "em-kline-gapfill"
    manifest = Manifest(config.manifest_path)
    manifest.start_batch(
        run_id,
        batch_id,
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=sorted(set(gap_df["symbol"].to_list())),
        window_start=start.isoformat(),
        window_end=end.isoformat(),
    )
    StagingWriter(config.staging_root).write_batch("daily_bars", run_id, batch_id, gap_df)
    manifest.finish_batch(
        run_id,
        batch_id,
        "success",
        rows_read=gap_df.height,
        rows_written=gap_df.height,
    )
    result = {
        "rows_read": sina_rows + gap_df.height,
        "rows_written": sina_rows + gap_df.height,
        "filled": True,
        "complete": not missing_keys,
        "audit_findings": [
            *sina_findings,
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_kline_gapfill",
                "message": (
                    f"routed {gap_df.height} row(s) through EastMoney kline for "
                    f"{len(symbols)} partially covered symbol(s) ({start}..{end})"
                ),
                "missing_keys": len(missing_keys),
                "complete": not missing_keys,
            },
        ],
    }
    if not require_complete and missing_keys:
        result["audit_findings"][0]["message"] += "; unresolved keys may be suspended"
    return result


# A bar captured before the session opens is the previous close stamped on every
# field: open==high==low==close and zero volume. A handful of these on any day
# are genuine suspensions, but a whole universe of them means the fetch ran too
# early — 2026-07-22 arrived that way from a pre-open run. Below this share it is
# suspensions; at or above it, it is a mis-timed capture.
_PLACEHOLDER_SHARE_LIMIT = 0.5


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
    rows = 0

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

    def fetch_one(symbol: str) -> tuple[str, pl.DataFrame | None, str | None]:
        for attempt in range(_SINA_FETCH_ATTEMPTS):
            # Gapfill is a best-effort repair path. Keep one unresponsive
            # symbol from holding the whole daily run for the full timeout.
            try:
                with httpx.Client(timeout=8.0) as client:
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
                    return symbol, None, "failed"
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
                return symbol, None, "empty"
            return symbol, bars, None
        return symbol, None, "failed"

    if use_parallel and requested_symbols:
        with ThreadPoolExecutor(max_workers=min(6, len(requested_symbols))) as pool:
            results = list(pool.map(fetch_one, requested_symbols))
    else:
        results = [fetch_one(symbol) for symbol in requested_symbols]
    for symbol, bars, failure_kind in results:
        if failure_kind == "failed":
            failed.append(symbol)
            continue
        if failure_kind == "empty":
            empty.append(symbol)
            failed.append(symbol)
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
        if start == end and not bse_attempted:
            merged, supplement_findings = _supplement_bse_tip_amounts(
                config, merged, trade_date=start, symbols=requested_symbols
            )
        out = write_fetched(
            config, run_id, "daily_bars", merged, source="sina", batch_id=f"{batch_prefix}-0000"
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
    df = normalize_with_source(df)
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
    end = getattr(config, "_backfill_end", None) or date(2015, 12, 31)
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
    end = getattr(config, "_backfill_end", None) or trade_date
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
