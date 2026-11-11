"""Finish a ``daily_bars`` run: gap-fill, certify, quarantine and publish."""

from __future__ import annotations

import hashlib
import json
from datetime import date

from cnequity.config import Config
from cnequity.orchestrator.outcomes import SourceUnavailableError
from cnequity.quality.audit import persist_step_findings
from cnequity.steps.bars import admission, gapfill, gapfill_chain, staging
from cnequity.steps.bars._base import logger
from cnequity.steps.common import (
    list_trading_dates,
)
from cnequity.storage.state import StateStore


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
    probe_symbols: list[str] | None = None,
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
        admission._record_daily_negative_observations(
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
            staged_before_gapfill = staging._staged_daily_bar_symbols(config, run_id, end)
            had_tip_gap = bool(expected_symbols - staged_before_gapfill)
            gap = gapfill._gapfill_tip_via_clist(
                config, end, run_id, expected_symbols=expected_tdx_symbols
            )
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

        missing_staged = sorted(
            expected_symbols - staging._staged_daily_bar_symbols(config, run_id, end)
        )
        if missing_staged:
            # clist is a live snapshot: it can supplement today's close but
            # must never be re-stamped onto an older retry date. Per-symbol
            # kline is also the bounded second chance for today's clist misses.
            kline = gapfill_chain._gapfill_multiday_via_kline(
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
        partial_symbols = staging._staged_daily_bar_partial_symbols(
            config, run_id, all_expected_symbols, start, end
        )
        failed_set = set(failed_symbols) | fallback_failed_symbols
        if failed_set:
            gap_start, gap_end = _leg_window(failed_set)
            gap = gapfill_chain._gapfill_multiday_via_kline(
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
            gap = gapfill_chain._gapfill_multiday_via_kline(
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

    gapfill_chain._reject_preopen_placeholder(config, run_id, end)

    # An undated, never-traded equity was asked for only to learn whether it
    # has listed. A clean empty answer settles that it has not: it joins the
    # proven no-data set instead of the unresolved keys, which is what once
    # held a whole market snapshot hostage (2026-09-15). A transport failure
    # settles nothing and stays with the strict unknowns.
    probe = {str(symbol).strip().upper() for symbol in (probe_symbols or ()) if str(symbol).strip()}
    probe_settled: set[str] = set()
    if probe:
        placeholders = gapfill_chain._quarantine_probe_placeholders(
            config, run_id, probe, start, end
        )
        if placeholders:
            findings.append(
                {
                    "dataset": "daily_bars",
                    "severity": "info",
                    "check": "daily_bars_probe_placeholder",
                    "message": (
                        f"{len(placeholders)} undated code(s) returned only zero-volume "
                        "rows; quarantined as pre-listing placeholders"
                    ),
                    "symbols": sorted(placeholders),
                }
            )
        staged_probe = staging._staged_daily_bar_symbols(config, run_id, end) & probe
        unanswered = set(failed_symbols) | fallback_failed_symbols
        not_listed = probe - staged_probe - unanswered
        if not_listed:
            probe_settled = not_listed
            explicit_no_data.update(not_listed)
            admission._record_daily_negative_observations(
                config,
                not_listed,
                start,
                end,
                reason="not_yet_listed",
                source="probe",
            )
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_undated_probe",
                "message": (
                    f"{len(probe)} undated equity code(s) probed: "
                    f"{len(staged_probe)} listed (bars staged), "
                    f"{len(not_listed)} not yet listed, "
                    f"{len(probe & unanswered)} unanswered"
                ),
                "listed": sorted(staged_probe),
                "not_listed": sorted(not_listed),
            }
        )

    # Symbols proven to have no data in this window, collected by the
    # certification below so the interior-session gate does not re-report them.
    # A settled probe is one already: owing its window would book sessions
    # from before a listing that has not happened.
    certified_no_data: set[str] = set(probe_settled)
    unresolved_tolerated: set[str] = set()

    if tip:
        staged = staging._staged_daily_bar_symbols(config, run_id, end)
        expected_symbols = set(expected_tdx_symbols) | set(expected_fallback_symbols or [])
        missing_staged = expected_symbols - staged
        if missing_staged:
            certified, unknown, ownership = admission._certify_missing_daily_symbols(
                config,
                set(missing_staged),
                end,
                end,
                explicit_no_data=explicit_no_data,
                source_empty=source_empty_symbols,
            )
            if certified:
                certified_no_data.update(certified)
                admission._record_certified_daily_no_data(
                    config,
                    certified - probe_settled,
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
                budget = admission._unresolved_budget(config, len(expected_symbols), tip=True)
                # Nothing staged is not a residue, it is an outage: the whole
                # session failed and there is nothing to publish anyway.
                tolerated = bool(staged & expected_symbols)
                remedy = admission._unresolved_key_remedy(config, run_id, unknown, end, end)
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
                                "valid rows will be published; missing keys remain recorded"
                                if tolerated
                                else "no usable requested rows"
                            )
                        ),
                        "missing_keys": len(unknown),
                        "symbols": sorted(unknown),
                        "tolerated": tolerated,
                        "tolerance_keys": budget,
                    }
                )
                if not tolerated:
                    StateStore(config.meta_root).record_outstanding_keys(
                        "daily_bars",
                        {(symbol, end) for symbol in unknown},
                        run_id=run_id,
                        reason="source_unavailable",
                    )
                    persist_step_findings(config, run_id, end, findings)
                    raise SourceUnavailableError(
                        f"daily_bars {end}: no usable rows after permitted failover." + remedy
                    )
                owed = StateStore(config.meta_root).record_outstanding_keys(
                    "daily_bars",
                    {(symbol, end) for symbol in unknown},
                    run_id=run_id,
                    reason="unresolved_tip",
                )
                unresolved_tolerated.update(unknown)
                logger.warning(
                    "%s; coverage threshold %d keys, publishing valid rows "
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
                resolved_symbols=expected_symbols - unresolved_tolerated,
            )
    elif expected_tdx_symbols or expected_fallback_symbols:
        # A vendor may report a nominally successful response while omitting
        # one symbol×session key.  Validate the staged key set independently
        # of its transport-level error bit; otherwise a partial success could
        # bypass the strict unknown classification below.
        all_expected_symbols = list(
            dict.fromkeys((expected_tdx_symbols or []) + (expected_fallback_symbols or []))
        )
        staged = staging._staged_daily_bar_symbols(config, run_id, end)
        missing_staged = set(all_expected_symbols) - staged
        if missing_staged:
            certified, unknown, ownership = admission._certify_missing_daily_symbols(
                config,
                missing_staged,
                start,
                end,
                explicit_no_data=explicit_no_data,
                source_empty=source_empty_symbols,
            )
            if certified:
                certified_no_data.update(certified)
                admission._record_certified_daily_no_data(
                    config,
                    certified - probe_settled,
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
                budget = admission._unresolved_budget(config, len(all_expected_symbols))
                tolerated = bool(
                    set(all_expected_symbols)
                    & staging._staged_daily_bar_symbols(config, run_id, None, start=start, end=end)
                )
                remedy = admission._unresolved_key_remedy(config, run_id, unknown, start, end)
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
                                "valid rows will be published; missing keys remain recorded"
                                if tolerated
                                else "no usable requested rows"
                            )
                        ),
                        "missing_keys": len(unknown),
                        "symbols": sorted(unknown),
                        "tolerated": tolerated,
                        "tolerance_keys": budget,
                    }
                )
                if not tolerated:
                    StateStore(config.meta_root).record_outstanding_keys(
                        "daily_bars",
                        admission._owed_keys_for_symbols(config, unknown, start, end),
                        run_id=run_id,
                        reason="source_unavailable",
                    )
                    persist_step_findings(config, run_id, end, findings)
                    raise SourceUnavailableError(
                        f"{headline}; no usable requested result." + remedy
                    )
                # Clipped to each symbol's own listing window. Recording the
                # whole sweep window for a symbol listed halfway through it
                # would owe sessions that never existed, and a debt nothing can
                # ever pay off is worse than no ledger at all.
                owed_pairs = admission._owed_keys_for_symbols(config, unknown, start, end)
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
                missing_pairs |= staging._staged_daily_bar_missing_keys(
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
            budget = admission._unresolved_budget(config, expected_keys)
            tolerated = True
            remedy = admission._unresolved_key_remedy(config, run_id, missing_symbols, start, end)
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
                        "valid rows will be published; missing keys remain recorded"
                        if tolerated
                        else "no usable requested rows"
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
            # Checkpointing past a hole means no incremental run will ever ask
            # for these sessions again — the watermark has moved over them. The
            # ledger is the only thing that remembers, and `cne backfill
            # daily_bars --outstanding` is what works it off.
            outstanding = StateStore(config.meta_root).record_outstanding_keys(
                "daily_bars", missing_pairs, run_id=run_id, reason="interior_gap"
            )
            _mark_unresolved_daily_bar_batches(config, run_id, missing_pairs)
            unresolved_tolerated.update(missing_symbols)
            logger.warning(
                "%s; coverage threshold %d keys, publishing valid rows "
                "(%d key(s) now owed — `cne backfill daily_bars --outstanding`).%s",
                headline,
                budget,
                outstanding,
                remedy,
            )

    # Not `all_expected_symbols`: that name is bound only in the two elif
    # branches above, and the tip path reaches here without it.
    admission._record_late_admissions(
        config,
        run_id,
        dict.fromkeys((expected_tdx_symbols or []) + (expected_fallback_symbols or [])),
        end,
    )

    result: dict = {"rows_read": rows_read, "rows_written": rows_written}
    result["usable_result"] = bool(
        certified_no_data
        or set((expected_tdx_symbols or []) + (expected_fallback_symbols or []))
        & staging._staged_daily_bar_symbols(config, run_id, None, start=start, end=end)
    )
    if certified_no_data:
        result["expected_no_data"] = len(certified_no_data)
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
        persist_step_findings(config, run_id, end, findings)
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
        result["coverage_status"] = "partial"
        result["reason_code"] = "source_scope_incomplete"
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
            manifest.finish_batch(
                run_id,
                batch_id,
                "failed",
                error_message="daily_bars exact symbol×session gap requires retry",
                execution_status="completed",
                reason_code="source_payload_invalid",
            )

    scheduled_symbols = {symbol for symbol, _session in missing_keys}
    replaceable: list[str] = []
    for batch in manifest.get_failed_batches(run_id):
        if batch["batch_id"] in child_ids:
            continue
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
