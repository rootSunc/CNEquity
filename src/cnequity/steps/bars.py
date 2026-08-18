"""L1 bar steps: daily_bars, index_bars."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, datetime

import polars as pl

from cnequity.adapters.tdx_protocol.client import (
    INDEX_SYMBOLS,
    fetch_index_bars,
    normalize_with_source,
)
from cnequity.config import Config
from cnequity.domain.market_time import A_SHARE_FINAL_AT, shanghai_now
from cnequity.domain.symbols import split_by_quote_source
from cnequity.orchestrator.registry import register_step
from cnequity.orchestrator.worker_pool import fetch_daily_bars_parallel
from cnequity.steps.common import (
    BACKFILL_START,
    DailyBarOwnership,
    classify_daily_bar_ownership,
    incremental_window,
    instrument_metadata,
    is_trading_day,
    list_trading_dates,
    load_bar_universe,
    load_curated_instruments,
    load_symbols,
)

logger = logging.getLogger(__name__)

# The closing auction ends at 15:00. Leave a small settlement buffer before
# trusting TDX's current daily bar; the default core schedule starts at 16:00.
_DAILY_BAR_FINAL_AT = A_SHARE_FINAL_AT


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


def _backfill_window(config: Config, trade_date: date) -> tuple[date, date]:
    """``--start/--end`` window for a backfill, defaulting to the full history.

    Repairing a single bad session must not mean re-fetching a decade for every
    symbol. A capture that fires before the close writes a truncated bar — right
    open, wrong close, partial volume — and the repair is one day wide.
    """
    end = getattr(config, "_backfill_end", None) or trade_date
    start = getattr(config, "_backfill_start", None) or BACKFILL_START
    return start, end


def _instrument_spans(config: Config) -> dict[str, tuple[date | None, date | None, str | None]]:
    return {
        row["symbol"]: (row["list_date"], row["delist_date"], row.get("asset_type"))
        for row in instrument_metadata(config).iter_rows(named=True)
    }


def _placeholder_bar_universe(
    config: Config,
    spans: dict[str, tuple[date | None, date | None, str | None]],
) -> set[str] | None:
    """Traded-bar universe only when an unlisted instrument placeholder is in scope.

    Scanning every daily_bars file is expensive; skip it unless there is at
    least one instrument with no listing date that
    ``classify_daily_bar_ownership`` may need to prove has never traded.
    """
    if not any(list_date is None for list_date, _, _ in spans.values()):
        return None
    return load_bar_universe(config)


def _ownership_context(
    config: Config,
    ownership: DailyBarOwnership,
    start: date,
    end: date,
) -> tuple[dict, bool]:
    from cnequity.steps.delisted import delisted_recovery_covers

    delegated_complete = delisted_recovery_covers(config, start, end, ownership.delegated_delisted)
    findings: list[dict] = []
    finding = {
        "dataset": "daily_bars",
        "severity": "info" if delegated_complete else "warning",
        "check": "daily_bars_source_ownership",
        "message": (
            f"generic={len(ownership.generic)}, "
            f"delegated_delisted={len(ownership.delegated_delisted)}, "
            f"expected_no_data={len(ownership.expected_no_data)}, "
            f"placeholder={len(ownership.placeholder)}, "
            f"delegated_complete={delegated_complete}"
        ),
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
    findings.append(finding)
    if ownership.placeholder:
        preview = ", ".join(sorted(ownership.placeholder)[:8])
        suffix = "..." if len(ownership.placeholder) > 8 else ""
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_unlisted_placeholder_skipped",
                "message": (
                    f"{len(ownership.placeholder)} unlisted instrument placeholder(s) "
                    "skipped (no list_date and no traded bar; not verified "
                    f"no-data): {preview}{suffix}"
                ),
                "symbols": sorted(ownership.placeholder),
            }
        )
    return {
        "daily_bars_ownership": {
            "generic": len(ownership.generic),
            "delegated_delisted": len(ownership.delegated_delisted),
            "expected_no_data": len(ownership.expected_no_data),
            "placeholder": len(ownership.placeholder),
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
        remaining: list[tuple[str, list[str], date, date]] = []
        ownership = DailyBarOwnership()
        for batch_id, symbols, spec_start, spec_end in batch_specs:
            routed = classify_daily_bar_ownership(
                symbols,
                spans,
                spec_start,
                spec_end,
                bar_universe=bar_universe,
            )
            ownership.generic.extend(routed.generic)
            ownership.delegated_delisted.extend(routed.delegated_delisted)
            ownership.expected_no_data.extend(routed.expected_no_data)
            ownership.placeholder.extend(routed.placeholder)
            if routed.generic:
                remaining.append((batch_id, routed.generic, spec_start, spec_end))
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
            elif not routed.generic and not routed.placeholder:
                # The original failed batch now has only proven no-data symbols.
                _resolve_failed_daily_bar_batches(
                    config,
                    run_id,
                    [batch_id],
                    superseded_by="ownership-expected-no-data",
                )
            elif not routed.generic and routed.placeholder:
                # Unlisted placeholders are skipped but not verified no-data;
                # resolving the failed batch keeps compact usable while the
                # ownership audit finding still records that it was not proven
                # empty.
                _resolve_failed_daily_bar_batches(
                    config,
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
        out = _finish_daily_bars(
            config,
            trade_date,
            run_id,
            start=start,
            end=end,
            expected_tdx_symbols=list(dict.fromkeys(ownership.generic)),
            tdx_result=result,
            sina_result=None,
        )
        return _merge_ownership_result(out, config, ownership, start, end)

    if getattr(config, "_backfill", False):
        start, end = _backfill_window(config, trade_date)
    else:
        start = incremental_window(config, "daily_bars", trade_date)
        end = trade_date
    _reject_unfinished_daily_bar_window(config, end)

    symbols = load_symbols(config)
    rebackfill = context.get("symbols_to_rebackfill") or []
    if rebackfill:
        symbols = list(dict.fromkeys(rebackfill + symbols))

    spans = _instrument_spans(config)
    ownership = classify_daily_bar_ownership(
        symbols,
        spans,
        start,
        end,
        bar_universe=_placeholder_bar_universe(config, spans),
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
    tdx_symbols, fallback_symbols = split_by_quote_source(ownership.generic)
    result = fetch_daily_bars_parallel(
        config,
        tdx_symbols,
        start,
        end,
        run_id,
        "daily_bars",
    )
    sina_result = None
    if fallback_symbols:
        sina_result = fetch_bars_via_sina(
            config, fallback_symbols, start, end, run_id, batch_prefix="sina"
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
    )
    return _merge_ownership_result(out, config, ownership, start, end)


def _resolve_failed_daily_bar_batches(
    config: Config,
    run_id: str,
    batch_ids: list[str],
    *,
    superseded_by: str,
) -> None:
    """Close failed worker batches after a verified retry/gap-fill path.

    A worker batch is marked ``failed`` as soon as TDX misses a symbol, before
    the step-level EastMoney gap-fill has a chance to stage the missing keys.
    Leaving that terminal failure behind would block compaction even after the
    rows were recovered. ``superseded`` keeps the failure in the ledger for
    audit while excluding it from compact/retry eligibility.
    """
    if not batch_ids:
        return
    from cnequity.orchestrator.manifest import Manifest

    Manifest(config.manifest_path).supersede_batches(
        run_id,
        batch_ids,
        superseded_by=superseded_by,
    )
    logger.info(
        "daily_bars: resolved %d failed batch(es) via %s",
        len(batch_ids),
        superseded_by,
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
) -> dict:
    """Apply tip clist/kline or multi-day kline gap-fill, then pre-open rejection."""
    rows_read = int(tdx_result.get("rows_read", 0))
    rows_written = int(tdx_result.get("rows_written", 0))
    findings: list[dict] = []
    had_error = bool(tdx_result.get("had_error"))
    failed_symbols = list(tdx_result.get("failed_symbols") or [])
    failed_batch_ids = list(tdx_result.get("failed_batch_ids") or [])
    fallback_failed_symbols: set[str] = set()

    if sina_result:
        rows_read += int(sina_result.get("rows_read", 0))
        rows_written += int(sina_result.get("rows_written", 0))
        if int(sina_result.get("failed_symbols", 0)):
            had_error = True
        fallback_failed_symbols = set(sina_result.get("failed_symbol_names") or [])
        sina_findings = (sina_result.get("context_updates") or {}).get("audit_findings") or []
        findings.extend(sina_findings)

    tip = start == end
    bar_date = end
    stale_retry = tip and bar_date != trade_date
    if tip:
        expected_symbols = set(expected_tdx_symbols) | set(expected_fallback_symbols or [])
        if stale_retry:
            # clist is a live snapshot, so it cannot be re-stamped onto a past
            # session. A retry of an older tip must use per-symbol kline for
            # the actual bar date instead.
            missing_staged = sorted(
                expected_symbols - _staged_daily_bar_symbols(config, run_id, bar_date)
            )
            if missing_staged:
                kline = _gapfill_multiday_via_kline(
                    config,
                    run_id,
                    symbols=missing_staged,
                    start=bar_date,
                    end=bar_date,
                    require_complete=False,
                )
                rows_read += int(kline.get("rows_read", 0))
                rows_written += int(kline.get("rows_written", 0))
                findings.extend(kline.get("audit_findings") or [])
        else:
            gap = _gapfill_tip_via_clist(
                config, trade_date, run_id, expected_symbols=expected_tdx_symbols
            )
            rows_read += int(gap.get("rows_read", 0))
            rows_written += int(gap.get("rows_written", 0))
            findings.extend(gap.get("audit_findings") or [])
            staged = _staged_daily_bar_symbols(config, run_id, bar_date)
            missing_staged = [s for s in expected_symbols if s not in staged]
            if missing_staged:
                # clist is a live snapshot and can omit a listed ETF/LOF that
                # TDX also misses. Per-symbol kline is slow but is the existing
                # multi-day recovery path, so reuse it for a small tip leftover
                # instead of leaving the original failed worker batch to block
                # compact forever.
                kline = _gapfill_multiday_via_kline(
                    config,
                    run_id,
                    symbols=sorted(missing_staged),
                    start=bar_date,
                    end=bar_date,
                    require_complete=False,
                )
                rows_read += int(kline.get("rows_read", 0))
                rows_written += int(kline.get("rows_written", 0))
                findings.extend(kline.get("audit_findings") or [])
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
            if gap.get("filled") and gap.get("complete", False):
                had_error = False

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

    if failed_batch_ids and not had_error:
        _resolve_failed_daily_bar_batches(
            config,
            run_id,
            failed_batch_ids,
            superseded_by="multiday-kline-gapfill",
        )

    _reject_preopen_placeholder(config, run_id, bar_date)

    if tip:
        staged = _staged_daily_bar_symbols(config, run_id, bar_date)
        expected_symbols = set(expected_tdx_symbols) | set(expected_fallback_symbols or [])
        missing_staged = sorted(expected_symbols - staged)
        if expected_symbols and not staged:
            raise RuntimeError(
                f"daily_bars {bar_date}: TDX failed and EastMoney clist/kline gap-fill "
                "produced no staged tip rows"
            )
        if missing_staged:
            # A handful of keys can legitimately stay missing (suspension,
            # trading halt) even after TDX, clist, and kline gap-fill all had a
            # shot at them. Failing the whole market-wide tip over that would
            # make every run fail on any given day some symbol is halted;
            # surface it as a finding instead.
            preview = ", ".join(sorted(missing_staged)[:8])
            suffix = "..." if len(missing_staged) > 8 else ""
            findings.append(
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_tip_missing_symbols",
                    "message": (
                        f"daily_bars {bar_date}: {len(missing_staged)} expected tip "
                        "key(s) remain missing after TDX and EastMoney clist/kline gap-fill "
                        f"(may be suspended): {preview}{suffix}"
                    ),
                    "missing_keys": len(missing_staged),
                }
            )
        if failed_batch_ids:
            _resolve_failed_daily_bar_batches(
                config,
                run_id,
                failed_batch_ids,
                superseded_by="tip-gapfill",
            )
        # A tip is usable once at least one expected key is staged; a handful
        # of legitimately-missing symbols must not fail the whole run — see
        # the daily_bars_tip_missing_symbols finding above.
        had_error = False
    elif had_error:
        raise RuntimeError("daily_bars: one or more symbol batches failed")

    result: dict = {"rows_read": rows_read, "rows_written": rows_written}
    if findings:
        result["context_updates"] = {"audit_findings": findings}
    return result


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
    """Find symbols with an interior session gap in a staged multi-day window.

    A symbol can legitimately have no rows before listing, after delisting, or
    during a suspension. Comparing only the interval between its first and
    last staged bars avoids those edge gaps while still catching a partial
    vendor response inside the symbol's observed history.
    """
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
    partial: set[str] = set()
    spans = staged.group_by("symbol").agg(
        pl.col("trade_date").min().alias("first_date"),
        pl.col("trade_date").max().alias("last_date"),
        pl.col("trade_date").n_unique().alias("observed_days"),
    )
    for row in spans.iter_rows(named=True):
        expected = [
            session for session in sessions if row["first_date"] <= session <= row["last_date"]
        ]
        if row["observed_days"] < len(expected):
            partial.add(row["symbol"])
    return partial


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
    """Stage EastMoney kline for failed or partially covered symbols."""
    import polars as pl

    from cnequity.adapters.eastmoney.bars import fetch_daily_bars as fetch_em_kline
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.quality.failover import failover_spec, write_backup_snapshot
    from cnequity.storage import StagingWriter

    spec = failover_spec(config, "daily_bars")
    if spec is None or not config.sources.get(spec.backup, True) or not symbols:
        return {"rows_read": 0, "rows_written": 0, "filled": False}

    df = fetch_em_kline(symbols, start, end, config=config)
    if df.is_empty():
        return {
            "rows_read": 0,
            "rows_written": 0,
            "filled": False,
            "complete": False,
            "audit_findings": [
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_kline_gapfill",
                    "message": (
                        f"TDX coverage was incomplete for {len(symbols)} symbol(s) over "
                        f"{start}..{end}; EastMoney kline returned no rows"
                    ),
                }
            ],
        }

    expected_dates = list_trading_dates(config, start, end)
    expected_keys = {(symbol, day) for symbol in symbols for day in expected_dates}
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
            "rows_read": df.height,
            "rows_written": 0,
            "filled": True,
            "complete": not missing_keys,
            "audit_findings": audit_findings,
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
        "rows_read": gap_df.height,
        "rows_written": gap_df.height,
        "filled": True,
        "complete": not missing_keys,
        "audit_findings": [
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
            }
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
    import httpx
    import polars as pl

    from cnequity.adapters.sina.bars import fetch_daily_bars_sina
    from cnequity.steps.http_common import write_fetched

    fetch = fetch or (
        lambda symbol, client: fetch_daily_bars_sina(symbol, start=start, end=end, client=client)
    )
    frames: list[pl.DataFrame] = []
    failed: list[str] = []
    covered_dates: dict[str, set[date]] = {}
    with httpx.Client(timeout=30.0) as client:
        for symbol in symbols:
            config.rate_limit("sina")
            try:
                bars = fetch(symbol, client)
            except Exception as exc:  # noqa: BLE001 — keep the rest of the board
                logger.warning("sina bars failed for %s: %s", symbol, exc)
                failed.append(symbol)
                continue
            if bars.is_empty():
                failed.append(symbol)
                continue
            covered_dates[symbol] = set(bars["trade_date"].to_list())
            frames.append(bars)

    expected_dates = set(list_trading_dates(config, start, end))
    for symbol in symbols:
        if symbol in failed:
            continue
        if expected_dates - covered_dates.get(symbol, set()):
            failed.append(symbol)

    rows = 0
    if frames:
        merged = pl.concat(frames, how="diagonal_relaxed")
        out = write_fetched(
            config, run_id, "daily_bars", merged, source="sina", batch_id=f"{batch_prefix}-0000"
        )
        rows = int(out.get("rows_written", 0))

    result: dict = {"rows_read": rows, "rows_written": rows}
    if failed:
        result["failed_symbols"] = len(failed)
        # Keep the names as well as the count. The daily step can then route
        # failed fallback symbols through the same historical gap-fill as TDX
        # failures; a count alone cannot identify which keys need recovery.
        result["failed_symbol_names"] = list(dict.fromkeys(failed))
        result["context_updates"] = {
            "audit_findings": [
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "fallback_source_incomplete",
                    "message": (
                        f"{len(failed)}/{len(symbols)} symbols without a TDX route "
                        f"failed to fetch from the fallback vendor "
                        f"(e.g. {', '.join(failed[:5])})"
                    ),
                }
            ]
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
        "(ETF factors unverified and 北交所 has no Sina coverage — both excluded)",
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

    * Stocks only. ETF/LOF factors are not yet reliable (Sina varies the factor
      field per fund and omits some ETFs), so deeper raw bars could not be
      served as hfq with one verified convention; fetching them would spend
      hours on data the research path must refuse anyway. 北交所 is excluded
      because Sina has no factor coverage for it.
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
        if row is None or row.get("asset_type") != "stock":
            continue
        listed = row.get("list_date")
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
    _login(bs)
    missing: set[str] = set()
    try:
        for year in range(start.year, end.year + 1):
            for month in _ROSTER_SAMPLE_MONTHS:
                day = date(year, month, 28)
                if not (start <= day <= end):
                    continue
                roster = roster_on(day, bs=bs, login=False)
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
