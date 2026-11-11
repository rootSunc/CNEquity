"""The ``daily_bars`` step: fetch, stage and hand the run to ``finish``."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.symbols import (
    filter_ingest_universe,
    split_by_quote_source,
)
from cnequity.orchestrator.registry import register_step
from cnequity.orchestrator.worker_pool import fetch_daily_bars_parallel
from cnequity.quality.audit import persist_step_findings
from cnequity.run_scope import backfill_of, run_flags_of
from cnequity.steps.bars import (
    admission,
    amounts,
    bar_scope,
    finish,
    sina_bars,
    tip_fetch,
    turnover,
)
from cnequity.steps.common import (
    DailyBarOwnership,
    classify_daily_bar_ownership,
    incremental_window,
    instrument_metadata,
    list_trading_dates,
    load_curated_trading_status,
    load_negative_evidence,
    load_symbols,
    mark_reconciled,
    reconciliation_depth,
)
from cnequity.storage.fund_no_print import approved_no_print_keys


@register_step(
    "daily_bars",
    group="core",
    depends_on=["instruments", "corporate_actions"],
    requires_workers=True,
)
def step_daily_bars(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    issues: list[dict] = []
    token = bar_scope._FUND_SCOPE_ISSUES.set(issues)
    try:
        result = _step_daily_bars(config, trade_date, run_id, context)
    finally:
        bar_scope._FUND_SCOPE_ISSUES.reset(token)
    if issues and isinstance(result, dict):
        updates = result.setdefault("context_updates", {})
        updates.setdefault("audit_findings", []).extend(issues)
        persist_step_findings(config, run_id, trade_date, issues)
        if result.get("status", "success") == "success":
            result["status"] = "warning"
    return result


def _step_daily_bars(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    batch_specs = context.get("_retry_batch_specs")
    if batch_specs:
        # Retry windows are encoded on each BatchSpec; tip/multi-day gap-fill
        # still applies using the outer trade_date / per-spec window.
        windows = {(s, e) for _, _, s, e in batch_specs}
        start = min(s for s, _ in windows)
        end = max(e for _, e in windows)
        bar_scope._reject_unfinished_daily_bar_window(config, end)
        spans = bar_scope._instrument_spans(config)
        bar_universe = bar_scope._placeholder_bar_universe(config, spans)
        metadata = instrument_metadata(config)
        evidence = load_negative_evidence(config, "daily_bars", metadata=metadata)
        remaining: list[tuple[str, list[str], date, date]] = []
        fallback_specs: list[tuple[str, list[str], date, date]] = []
        ownership = DailyBarOwnership()
        etf_retry_scopes: dict[date, list[str] | None] = {}
        explicit_retry = bool(
            backfill_of(config).active and backfill_of(config).symbol_list() is not None
        )
        for batch_id, symbols, spec_start, spec_end in batch_specs:
            if explicit_retry:
                symbols = bar_scope._resolve_daily_bar_scope(config, symbols)
            else:
                if spec_end not in etf_retry_scopes:
                    etf_retry_scopes[spec_end] = bar_scope._daily_fund_bar_symbols(
                        config, spec_end, retry=True
                    )
                eligible_etfs = etf_retry_scopes[spec_end]
                # An unavailable fund scope stops new admissions, but it must
                # not erase fund work already planned in this failed run.
                symbols = [
                    symbol
                    for symbol in symbols
                    if admission.in_ingest_universe_symbol(symbol, config)
                    or eligible_etfs is None
                    or symbol in eligible_etfs
                ]
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
                no_print_keys=approved_no_print_keys(config, start=spec_start, end=spec_end),
            )
            ownership.generic.extend(routed.generic)
            ownership.delegated_delisted.extend(routed.delegated_delisted)
            ownership.expected_no_data.extend(routed.expected_no_data)
            ownership.placeholder.extend(routed.placeholder)
            ownership.probe.extend(routed.probe)
            ownership.unknown.extend(routed.unknown)
            ownership.negative_cached.extend(routed.negative_cached)
            ownership.no_data_reasons.update(routed.no_data_reasons)
            fetch_scope = list(dict.fromkeys(routed.generic + routed.unknown + routed.probe))
            tdx_symbols, fallback_symbols = split_by_quote_source(fetch_scope)
            if tdx_symbols:
                remaining.append((batch_id, tdx_symbols, spec_start, spec_end))
            if fallback_symbols:
                fallback_specs.append((batch_id, fallback_symbols, spec_start, spec_end))
            if routed.delegated_delisted:
                delegated_id = f"{batch_id}-delegated" if routed.generic else batch_id
                bar_scope._record_delegated_ownership_batch(
                    config,
                    run_id,
                    routed.delegated_delisted,
                    spec_start,
                    spec_end,
                    batch_id=delegated_id,
                )
            elif (
                not routed.generic
                and not routed.unknown
                and not routed.probe
                and not routed.placeholder
            ):
                # The original failed batch now has only proven no-data symbols.
                from cnequity.orchestrator.manifest import Manifest

                Manifest(config.manifest_path).supersede_batches(
                    run_id,
                    [batch_id],
                    superseded_by="ownership-expected-no-data",
                )
            elif not routed.generic and not routed.probe and routed.placeholder:
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
                bj_history = tip_fetch._fetch_bj_history_via_tdx(
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
                fallback = sina_bars.fetch_bars_via_sina(
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
        out = finish._finish_daily_bars(
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
            probe_symbols=ownership.probe,
        )
        return bar_scope._merge_ownership_result(out, config, ownership, start, end, run_id)

    depth = None
    if backfill_of(config).active:
        start, end = bar_scope._backfill_window(config, trade_date)
    else:
        start = incremental_window(config, "daily_bars", trade_date)
        depth = reconciliation_depth(config, "daily_bars", trade_date)
        end = trade_date
    bar_scope._reject_unfinished_daily_bar_window(config, end)

    if run_flags_of(config).tdx_amount_repair:
        return amounts.repair_sina_history_amounts_from_tdx(
            config, start, end, run_id, backfill_of(config).symbol_list()
        )

    if run_flags_of(config).turnover_repair:
        return turnover.repair_daily_bar_turnover(
            config, start, end, run_id, backfill_of(config).symbol_list()
        )

    if run_flags_of(config).bse_tip_repair:
        if start != end:
            raise RuntimeError("BSE tip repair requires a one-session daily_bars window")
        explicit_scope = backfill_of(config).symbol_list()
        if explicit_scope is None:
            raise RuntimeError("BSE tip repair requires an explicit symbol scope")
        return amounts.repair_bse_tip_amounts_from_curated(config, end, run_id, explicit_scope)

    explicit_scope = backfill_of(config).symbol_list() if backfill_of(config).active else None
    catalog_etfs = (
        set(bar_scope._daily_fund_bar_symbols(config, end) or ())
        if explicit_scope is None
        else set()
    )
    known_symbols = load_symbols(config)
    symbols = (
        # An explicit repair scope is the operator's own request and is never
        # narrowed; the implicit full-market scope is.
        bar_scope._resolve_daily_bar_scope(config, explicit_scope)
        if explicit_scope is not None
        else list(
            dict.fromkeys(
                [
                    *filter_ingest_universe(known_symbols, config.ingest_universe),
                    *(symbol for symbol in known_symbols if symbol in catalog_etfs),
                ]
            )
        )
    )
    rebackfill = context.get("symbols_to_rebackfill") or []
    if rebackfill:
        symbols = list(dict.fromkeys(rebackfill + symbols))

    spans = bar_scope._instrument_spans(config)
    if explicit_scope is None:
        new_instruments = context.get("new_instruments") or []
        if config.ingest_listed_funds and new_instruments:
            from cnequity.storage.instrument_catalog import listed_fund_symbols

            catalog_etfs.update(listed_fund_symbols(pl.DataFrame(new_instruments), end))
        # A security listing today is in this run's instruments output but not
        # yet in curated, which is where the universe is read from; without
        # this its first session is never fetched (920201.BJ, 2026-09-24).
        discovered = [
            row
            for row in new_instruments
            if row.get("symbol")
            and (
                admission.in_ingest_universe_symbol(row["symbol"], config)
                or row["symbol"] in catalog_etfs
            )
        ]
        for row in discovered:
            spans.setdefault(
                row["symbol"], (row.get("list_date"), row.get("delist_date"), row.get("asset_type"))
            )
        symbols = list(dict.fromkeys([*symbols, *(row["symbol"] for row in discovered)]))
    metadata = instrument_metadata(config)
    ownership = classify_daily_bar_ownership(
        symbols,
        spans,
        start,
        end,
        bar_universe=bar_scope._placeholder_bar_universe(config, spans),
        trading_status=load_curated_trading_status(
            config,
            start=start,
            end=end,
            symbols=symbols,
        ),
        trading_sessions=list_trading_dates(config, start, end),
        negative_evidence=load_negative_evidence(config, "daily_bars", metadata=metadata),
        no_print_keys=approved_no_print_keys(config, start=start, end=end),
    )
    bar_scope._record_delegated_ownership_batch(
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
    fetch_scope = list(dict.fromkeys(ownership.generic + ownership.unknown + ownership.probe))
    tdx_symbols, fallback_symbols = split_by_quote_source(fetch_scope)
    reused_symbols = bar_scope._reuse_successful_daily_bars(config, run_id, fetch_scope, start, end)
    fetch_tdx_symbols = [symbol for symbol in tdx_symbols if symbol not in reused_symbols]
    fetch_fallback_symbols = [symbol for symbol in fallback_symbols if symbol not in reused_symbols]
    # Each exchange publishes its whole board for the session it is currently
    # serving. On a tip-only window that answers for almost every SH/SZ symbol
    # in two requests, leaving the per-symbol sweep only the remainder; on the
    # deep window it is a cheap head start that the sweep then reconciles.
    # A scoped historical repair would download two whole-board publisher
    # snapshots just to save one tip row per symbol. Small explicit tip repairs
    # have the same poor tradeoff; their TDX pages are already bounded.
    scoped_history = explicit_scope is not None and start < end
    exchange_tip = (
        {"rows_read": 0, "rows_written": 0, "covered": set()}
        if explicit_scope is not None and (scoped_history or len(fetch_tdx_symbols) <= 4)
        else tip_fetch._fetch_tip_via_exchange(config, fetch_tdx_symbols, end, run_id)
    )
    if start >= end and exchange_tip["covered"]:
        fetch_tdx_symbols = [s for s in fetch_tdx_symbols if s not in exchange_tip["covered"]]
    # SZSE's report is audit-only (see above), so SZ — and any SH the SSE board
    # missed — still fell to the per-symbol sweep: ~2,900 requests for one
    # session. TDX batch quotes answer it 80 symbols per request.
    quote_tip = (
        tip_fetch._fetch_tip_via_tdx_quotes(config, fetch_tdx_symbols, end, run_id)
        if start >= end
        else {"rows_read": 0, "rows_written": 0, "covered": set()}
    )
    if quote_tip["covered"]:
        fetch_tdx_symbols = [s for s in fetch_tdx_symbols if s not in quote_tip["covered"]]
    result = fetch_daily_bars_parallel(
        config,
        fetch_tdx_symbols,
        start,
        end,
        run_id,
        "daily_bars",
    )
    for tip in (exchange_tip, quote_tip):
        if tip["rows_written"]:
            result["rows_read"] = int(result.get("rows_read", 0)) + tip["rows_read"]
            result["rows_written"] = int(result.get("rows_written", 0)) + tip["rows_written"]
    sina_result = None
    fallback_start = start
    if fetch_fallback_symbols:
        # The Beijing board's tip comes from the exchange's own paginated
        # snapshot; Sina stays the per-symbol backstop for whatever that misses
        # and for the (short) history window behind it.
        # The BSE tip endpoint pages through the whole board (roughly 30
        # requests). A one-name backfill should not incur that sweep.
        bse = (
            {"rows_read": 0, "rows_written": 0, "covered": set()}
            if explicit_scope is not None and (scoped_history or len(fetch_fallback_symbols) <= 4)
            else tip_fetch._fetch_bj_tip_via_bse(config, fetch_fallback_symbols, end, run_id)
        )
        fallback_start = tip_fetch._bj_history_start(config, start, end)
        # The history behind the tip comes from TDX in one call per symbol;
        # Sina is left the tip BSE missed and the symbols TDX did not answer.
        bj_history = tip_fetch._fetch_bj_history_via_tdx(
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
            sina_result = sina_bars.fetch_bars_via_sina(
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
    out = finish._finish_daily_bars(
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
        probe_symbols=ownership.probe,
    )
    out.setdefault("metrics", {})["cache_hits"] = int(
        out.get("metrics", {}).get("cache_hits", 0) or 0
    ) + len(reused_symbols)
    merged = bar_scope._merge_ownership_result(out, config, ownership, start, end, run_id)
    if depth is not None and explicit_scope is None and merged.get("status") != "failed":
        # The window was walked. Keys it could not settle are already owed in
        # the outstanding-key ledger and retried on their own; holding the
        # weekly marker back for them only made every day a deep day.
        mark_reconciled(config, "daily_bars", trade_date, depth)
    return merged
