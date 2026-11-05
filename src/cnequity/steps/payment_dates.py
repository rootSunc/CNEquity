"""Scoped, source-backed cash payment date enrichment through normal publication."""

from __future__ import annotations

import json
from datetime import date

import polars as pl

from cnequity.adapters.baostock.corporate_actions import fetch_corporate_actions_baostock
from cnequity.domain.action_evidence import clear_invalid_payment_evidence, valid_payment_expr
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.domain.symbols import is_etf_symbol, parse_symbol
from cnequity.orchestrator.source_gaps import record_source_gap
from cnequity.query.reader import load
from cnequity.run_scope import backfill_of
from cnequity.steps.common import write_simple
from cnequity.steps.http_common import verify_raw_archive, write_fetched


def unique_payment_issues(issues: list[dict]) -> list[dict]:
    """Report each distinct event/reason once, preserving source order."""
    seen: set[tuple[str, str, str]] = set()
    result = []
    for issue in issues:
        key = (issue["symbol"], issue["ex_date"], issue["reason"])
        if key not in seen:
            seen.add(key)
            result.append(issue)
    return result


def match_payment_dates(existing: pl.DataFrame, fetched: pl.DataFrame):
    """Match event keys and amounts within TDX Float32 wire precision.

    Preserve the exact stored amount; never replace it with the peer rounding.
    """
    lookup = {(r["symbol"], r["ex_date"], r["action_type"]): r for r in fetched.to_dicts()}
    accepted, unresolved = [], []
    for old in existing.to_dicts():
        key = (old["symbol"], old["ex_date"], old["action_type"])
        new = lookup.get(key)
        reason = None
        if new is None:
            reason = "source_event_missing"
        elif new.get("payment_date") is None:
            reason = "source_payment_date_missing"
        elif new["payment_date"] < old["ex_date"]:
            reason = "payment_before_ex_date"
        elif any(
            abs(float(old.get(c) or 0) - float(new.get(c) or 0))
            > max(1e-8, abs(float(old.get(c) or 0)) * 1e-7)
            for c in ("cash_dividend", "bonus_ratio", "transfer_ratio")
        ):
            reason = "economic_amount_conflict"
        elif old.get("payment_date") not in (None, new["payment_date"]):
            reason = "existing_payment_date_conflict"
        if reason:
            unresolved.append({"symbol": key[0], "ex_date": str(key[1]), "reason": reason})
        elif old.get("payment_date") is None:
            # Keep exact old economics (not rounded replacements), stamp the
            # corroborating source and retain all optional corporate fields.
            accepted.append(
                {
                    **old,
                    "payment_date": new["payment_date"],
                    "payment_source": new["payment_source"],
                    "source": "baostock",
                }
            )
    return pl.DataFrame(accepted, schema_overrides={"payment_date": pl.Date}), unresolved


def repair_payment_dates(config, trade_date: date, run_id: str, context: dict):
    symbols = context.get("_retry_symbols") or backfill_of(config).symbol_list()
    start, end = backfill_of(config).start, backfill_of(config).end
    if not symbols or start is None or end is None or start > end:
        raise ValueError("payment repair requires explicit symbols and valid start/end")
    # Reviewed stock terms are issuer evidence too; applying them here lets a
    # new lake reproduce every reviewed correction through one entry point.
    from cnequity.adapters.eastmoney.bse_stock_terms import repair_reviewed_bj_stock_terms

    stock_terms = repair_reviewed_bj_stock_terms(config, run_id, symbols, start, end)
    all_actions = load("corporate_actions", data_root=config.data_root, start=start, end=end)
    existing = all_actions.filter(
        pl.col("symbol").is_in(symbols)
        & (pl.col("action_type") == "cash_dividend")
        & (pl.col("cash_dividend") > 0)
    )
    invalid = existing.head(0)
    if "payment_date" in existing.columns:
        # A date before the ex-date is a source typo, i.e. no evidence: look
        # for the real date like any other gap, and clear it if none is found.
        invalid = existing.filter(pl.col("payment_date").is_not_null() & ~valid_payment_expr())
        existing = clear_invalid_payment_evidence(existing).filter(pl.col("payment_date").is_null())
    if existing.is_empty():
        return stock_terms
    fund_symbols = {
        symbol
        for symbol in existing["symbol"].unique().to_list()
        if is_etf_symbol(parse_symbol(symbol).code, parse_symbol(symbol).exchange)
    }
    fund_pending = existing.filter(pl.col("symbol").is_in(fund_symbols))
    pending = existing.filter(~pl.col("symbol").is_in(fund_symbols))
    metrics = {
        "network_requests": 0,
        "replayed_requests": 0,
        "cninfo_network_responses": 0,
    }
    from cnequity.adapters.cninfo.fund_payment_notices import repair_fund_payment_notices
    from cnequity.adapters.exchange.fund_payment_notices import repair_sse_fund_payment_notices

    sse_fund_keys, sse_fund_diagnostics = repair_sse_fund_payment_notices(
        config, run_id, fund_pending, metrics=metrics
    )
    sse_correction_blocked = [
        (issue["symbol"], date.fromisoformat(issue["ex_date"]))
        for issue in sse_fund_diagnostics
        if issue["reason"] == "sse_fund_correction_chain_requires_review"
    ]
    if sse_fund_keys or sse_correction_blocked:
        fund_pending = fund_pending.filter(
            ~pl.struct("symbol", "ex_date").is_in(
                [
                    {"symbol": symbol, "ex_date": day}
                    for symbol, day in [*sse_fund_keys, *sse_correction_blocked]
                ]
            )
        )
    cninfo_fund_keys, cninfo_fund_diagnostics = repair_fund_payment_notices(
        config, run_id, fund_pending, metrics=metrics
    )
    fund_keys = [*sse_fund_keys, *cninfo_fund_keys]
    repaired_fund_keys = {(symbol, str(day)) for symbol, day in fund_keys}
    diagnostics = [
        issue
        for issue in [*sse_fund_diagnostics, *cninfo_fund_diagnostics]
        if (issue["symbol"], issue["ex_date"]) not in repaired_fund_keys
    ]
    if not config.sources.get("cninfo", False):
        diagnostics.extend(
            {
                "symbol": row["symbol"],
                "ex_date": str(row["ex_date"]),
                "reason": "fund_payment_source_required",
            }
            for row in fund_pending.to_dicts()
        )

    def remove_keys(frame: pl.DataFrame, keys: list[tuple[str, date]]) -> pl.DataFrame:
        if not keys:
            return frame
        return frame.filter(
            ~pl.struct("symbol", "ex_date").is_in(
                [{"symbol": symbol, "ex_date": day} for symbol, day in keys]
            )
        )

    # Exact primary-source notices are both more authoritative and cheaper
    # than retrying a vendor that already omitted or conflicted on the event.
    # Reviewed exceptional/correction chains run first; the generic CNINFO
    # parser then accepts only PDFs whose code, ex-date, pretax amount and
    # payment date all agree with the stored event.
    from cnequity.adapters.cninfo.payment_notices import repair_cninfo_payment_notices
    from cnequity.adapters.eastmoney.bse_payment_notices import repair_bj_payment_notices
    from cnequity.adapters.eastmoney.payment_notices import repair_reviewed_notices

    reviewed_keys = repair_reviewed_notices(config, run_id, pending)
    pending = remove_keys(pending, reviewed_keys)
    bj_keys, bj_diagnostics = repair_bj_payment_notices(
        config, run_id, pending, all_actions=all_actions, metrics=metrics
    )
    diagnostics.extend(bj_diagnostics)
    bj_economic_blockers = [
        (issue["symbol"], date.fromisoformat(issue["ex_date"]))
        for issue in bj_diagnostics
        if issue["reason"] == "bj_stock_terms_require_reconciliation"
    ]
    pending = remove_keys(pending, [*bj_keys, *bj_economic_blockers])
    # CNINFO's A-share directory does not identify historical BJ/NEEQ codes.
    # Keep those rows for the remaining-source report, but do not spend a
    # mainland CNINFO request on a different market's issuer directory.
    from cnequity.domain.market_profile import serves_expr

    cninfo_pending = pending.filter(serves_expr("cninfo_issuer_directory"))
    cninfo_keys, cninfo_diagnostics = repair_cninfo_payment_notices(
        config, run_id, cninfo_pending, metrics=metrics
    )
    diagnostics.extend(cninfo_diagnostics)
    pending = remove_keys(pending, cninfo_keys)

    windows = {
        r["symbol"]: (r["start"], r["end"])
        for r in pending.group_by("symbol")
        .agg(pl.col("ex_date").min().alias("start"), pl.col("ex_date").max().alias("end"))
        .to_dicts()
    }
    scope = f"payment-dates:{start}:{end}"
    years = {
        symbol: set(pending.filter(pl.col("symbol") == symbol)["ex_date"].dt.year().to_list())
        for symbol in windows
    }
    issuer_only = bool(getattr(config, "_corporate_actions_issuer_notice_only", False))
    if windows and not issuer_only and not config.sources.get("baostock", False):
        raise ValueError("unresolved payment events require the baostock source")
    if windows and not issuer_only:
        fetched, failed = fetch_corporate_actions_baostock(
            sorted(windows),
            start,
            end,
            config=config,
            run_id=run_id,
            symbol_windows=windows,
            request_scope=scope,
            repair_years=years,
            diagnostics=diagnostics,
            metrics=metrics,
        )
        enriched, unresolved = match_payment_dates(pending, fetched)
    else:
        fetched, failed = pending.head(0), []
        enriched = pending.head(0)
        unresolved = (
            [
                {
                    "symbol": row["symbol"],
                    "ex_date": str(row["ex_date"]),
                    "reason": "cninfo_notice_unresolved",
                }
                for row in pending.to_dicts()
            ]
            if issuer_only
            else []
        )
    issues = {(r["symbol"], r["ex_date"]): r for r in diagnostics}
    unresolved = [issues.get((r["symbol"], r["ex_date"]), r) for r in unresolved]
    repaired = [
        *reviewed_keys,
        *fund_keys,
        *bj_keys,
        *cninfo_keys,
        *(
            zip(enriched["symbol"].to_list(), enriched["ex_date"].to_list(), strict=True)
            if not enriched.is_empty()
            else ()
        ),
    ]
    invalid = remove_keys(invalid, repaired)
    cleared = clear_invalid_payment_evidence(invalid)
    report = config.meta_root / "payment_date_repairs" / f"{run_id}.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "symbols": sorted(existing["symbol"].unique().to_list()),
                "start": str(start),
                "end": str(end),
                "requested_events": existing.height,
                "matched_events": enriched.height
                + len(reviewed_keys)
                + len(fund_keys)
                + len(bj_keys)
                + len(cninfo_keys),
                "failed_symbols": failed,
                "event_conflicts": diagnostics,
                "cleared_invalid_payment_dates": [
                    {
                        "symbol": row["symbol"],
                        "ex_date": str(row["ex_date"]),
                        "payment_date": str(row["payment_date"]),
                        "payment_source": row["payment_source"],
                    }
                    for row in invalid.to_dicts()
                ],
                "acquisition": metrics,
                "unresolved": unique_payment_issues(
                    [*unresolved, *[r for r in diagnostics if r.get("ex_date")]]
                ),
                "reviewed_notice_events": len(reviewed_keys),
                "fund_notice_events": len(fund_keys),
                "sse_fund_notice_events": len(sse_fund_keys),
                "cninfo_fund_notice_events": len(cninfo_fund_keys),
                "bj_issuer_notice_events": len(bj_keys),
                "cninfo_notice_events": len(cninfo_keys),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    evidence = (
        verify_raw_archive(
            config, "corporate_actions", run_id, source="baostock", request_scope=scope
        )
        if config.should_archive_raw("corporate_actions") and not enriched.is_empty()
        else None
    )
    if failed:
        # Avoid falsely acknowledging complete publication when a source query failed.
        record_source_gap(
            "corporate_actions",
            f"payment date fetch unavailable for {len(failed)} symbols; see {report}",
        )
    result = {
        "rows_read": fetched.height
        + len(reviewed_keys)
        + len(fund_keys)
        + len(bj_keys)
        + len(cninfo_keys)
        + stock_terms["rows_read"],
        "rows_written": len(reviewed_keys)
        + len(fund_keys)
        + len(bj_keys)
        + len(cninfo_keys)
        + stock_terms["rows_written"],
    }
    if not cleared.is_empty():
        # Not a source observation, so no wire evidence is claimed: the stored
        # row is restated without the impossible date, keeping its own source.
        for source, rows in cleared.group_by("source"):
            staged = write_simple(
                config,
                run_id,
                "corporate_actions",
                with_provenance(
                    rows.drop("fetched_at", strict=False),
                    source=str(source[0]),
                    data_version=data_version_for("corporate_actions"),
                ),
                batch_id=f"{context.get('_batch_id') or 'payment-dates'}-clear-{source[0]}",
            )
            result["rows_written"] += int(staged.get("rows_written", 0))
    if not enriched.is_empty():
        staged = write_fetched(
            config,
            run_id,
            "corporate_actions",
            enriched,
            source="baostock",
            batch_id=context.get("_batch_id") or "payment-dates",
            raw_archive_evidence=evidence,
        )
        result["rows_read"] += int(staged.get("rows_read", 0))
        result["rows_written"] += int(staged.get("rows_written", 0))
    result["payment_date_report"] = str(report)
    return result
