from __future__ import annotations

import json
import logging
from datetime import date, timedelta

import polars as pl

from cnequity.adapters.calendar.exchange_calendar import (
    CALENDAR_FORWARD_COVERAGE_WARN_DAYS,
    calendar_forward_coverage_days,
    calendar_seed_end,
)
from cnequity.adapters.calendar.holidays_cn import CLOSED_DATES
from cnequity.config import Config
from cnequity.domain.datasets import PARTITION_COLS, curated_dataset_names, is_dataset_enabled
from cnequity.domain.market_time import is_session_final
from cnequity.quality.authority_checks import run_authority_checks
from cnequity.quality.bar_finality import daily_bar_finality_findings
from cnequity.quality.cross_checks import (
    ADJ_RECON_LOOKBACK_DAYS,
    adj_factor_arbitration_findings,
    adj_factor_coverage_findings,
    adj_factor_reconciliation_findings,
    balance_sheet_identity_findings,
    corporate_action_classification_findings,
    daily_bars_arbitration_findings,
    daily_bars_calendar_findings,
    daily_bars_close_crosscheck_findings,
    financial_statement_peer_findings,
    instrument_listing_order_findings,
    st_label_crosscheck_findings,
    trading_calendar_horizon_findings,
    undeclared_source_findings,
    universe_survivorship_findings,
    unrecorded_ex_event_findings,
    untraded_instrument_findings,
    valuation_bars_coverage_findings,
)
from cnequity.quality.dataset_checks import (
    audit_curated_dataset,
    check_mixed_partition_granularity,
    check_partition_fragmentation,
)
from cnequity.quality.derived_checks import industry_index_findings, market_breadth_findings
from cnequity.quality.intraday_checks import minute_bars_findings
from cnequity.quality.macro_checks import macro_staleness_findings
from cnequity.quality.pit_checks import pit_announce_date_findings
from cnequity.quality.run_health import degraded_job_findings
from cnequity.quality.source_diff import run_source_diffs
from cnequity.quality.st_coverage import st_evidence_coverage_report
from cnequity.quality.tick_checks import trade_ticks_findings
from cnequity.quality.unit_checks import (
    daily_bars_amount_completeness_findings,
    daily_bars_implied_price_findings,
    daily_bars_volume_unit_findings,
    valuation_ratio_unit_findings,
)
from cnequity.query.canonical import dedupe_lazy_by_primary_key
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.query.universe import (
    coverage_end_date,
    coverage_start_date,
    st_coverage_start,
    trading_status_coverage_start,
)
from cnequity.storage.atomic import write_json_atomic

logger = logging.getLogger(__name__)

# Sample missing/orphan dates surfaced in a coverage finding.
_INDEX_COVERAGE_SAMPLE = 8

# THS and TDX were both checked for these 399001.SZ dates; neither source
# publishes a bar. Keep the dates explicit so a newly missing session cannot
# be mistaken for the documented source limitation. The finding stays visible
# as ``info`` when every missing date is in this allow-list.
_KNOWN_INDEX_SOURCE_GAPS: dict[str, frozenset[date]] = {
    "399001.SZ": frozenset(
        date.fromisoformat(value)
        for value in (
            "1991-09-30",
            "1991-11-11",
            "1992-02-03",
            "1992-02-07",
            "1993-01-20",
            "1993-01-21",
            "1993-01-22",
            "1993-01-27",
            "1993-01-28",
            "1993-01-29",
            "1993-09-17",
            "1995-02-06",
            "1995-02-07",
            "1995-02-08",
            "1995-02-09",
            "1995-02-10",
            "1995-08-04",
            "1995-09-01",
        )
    ),
}


def _index_bars_coverage_findings(config: Config, trade_date: date) -> list[dict]:
    """index_bars vs trading_calendar within each symbol's covered span."""
    findings: list[dict] = []
    cal_root = config.curated_root / "trading_calendar"
    ib_root = config.curated_root / "index_bars"
    if not dataset_has_parquet(cal_root) or not dataset_has_parquet(ib_root):
        return findings

    cal = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(cal_root, partition_col="trade_date", end=trade_date),
            "trading_calendar",
        )
        .filter(
            pl.col("is_trading")
            & (pl.col("trade_date").dt.weekday() <= 5)
            & ~pl.col("trade_date").dt.strftime("%Y-%m-%d").is_in(CLOSED_DATES)
        )
        .select("trade_date")
        .unique()
        .collect(engine="streaming")
    )
    trading_days = set(cal["trade_date"].to_list())
    if not trading_days:
        return findings

    ib = (
        scan_parquet_root(ib_root, partition_col="trade_date", end=trade_date)
        .select("symbol", "trade_date")
        .unique()
        .collect(engine="streaming")
    )
    if ib.is_empty():
        return findings

    for sym in sorted(ib["symbol"].unique().to_list()):
        days = sorted(ib.filter(pl.col("symbol") == sym)["trade_date"].to_list())
        first, last = days[0], days[-1]
        have = set(days)
        expected = {d for d in trading_days if first <= d <= last}
        missing = sorted(expected - have)
        orphan = sorted(d for d in days if d not in trading_days)
        if not missing and not orphan:
            continue
        known_source_gaps = _KNOWN_INDEX_SOURCE_GAPS.get(sym, frozenset())
        known_missing = sorted(set(missing) & known_source_gaps)
        unexpected_missing = sorted(set(missing) - known_source_gaps)
        parts = []
        if unexpected_missing:
            parts.append(f"{len(unexpected_missing)} calendar trading day(s) with no bar")
        if known_missing:
            parts.append(f"{len(known_missing)} known source-limited trading day(s) with no bar")
        if orphan:
            parts.append(f"{len(orphan)} bar(s) on non-trading days")
        source_limited = bool(known_missing) and not unexpected_missing and not orphan
        findings.append(
            {
                "dataset": "index_bars",
                "symbol": sym,
                "severity": "info" if source_limited else "warning",
                "check": "index_bars_calendar_coverage",
                "message": (
                    f"{sym}: " + "; ".join(parts) + f" over {first.isoformat()}..{last.isoformat()}"
                ),
                "covered_days": len(have),
                "expected_days": len(expected),
                "missing_count": len(missing),
                "known_source_gap_count": len(known_missing),
                "unexpected_missing_count": len(unexpected_missing),
                "source_limited": source_limited,
                "orphan_count": len(orphan),
                "missing_sample": [d.isoformat() for d in missing[:_INDEX_COVERAGE_SAMPLE]],
                "orphan_sample": [d.isoformat() for d in orphan[:_INDEX_COVERAGE_SAMPLE]],
            }
        )
    return findings


def _unregistered_curated_dirs(config: Config) -> list[dict]:
    """Directories under ``curated/`` that no dataset in the registry owns.

    Manual surgery leaves things like ``corporate_actions.bak.20260709T122646Z``
    sitting next to the real dataset. Every engine path is keyed by dataset name
    so nothing reads them — which is exactly the problem: they are invisible to
    audit, they double the layer's apparent size, and a downstream consumer
    scanning ``curated/**/*.parquet`` rather than one dataset at a time silently
    reads a stale copy alongside the live one. Backups belong in ``backups/``.
    """
    root = config.curated_root
    if not root.exists():
        return []
    known = curated_dataset_names()
    stray = sorted(d.name for d in root.iterdir() if d.is_dir() and d.name not in known)
    if not stray:
        return []
    return [
        {
            "dataset": "curated",
            "severity": "warning",
            "check": "unregistered_curated_dir",
            "message": (
                f"{len(stray)} directory(ies) under curated/ belong to no registered "
                f"dataset ({', '.join(stray[:5])}"
                + (f", +{len(stray) - 5} more" if len(stray) > 5 else "")
                + ") — leftover backups or renamed datasets; move them out of the "
                "curated layer so whole-layer scans cannot pick them up"
            ),
            "stray_count": len(stray),
            "stray_dirs": stray[:20],
        }
    ]


def _stale_dataset_names(config: Config, trade_date: date) -> set[str]:
    """Datasets already behind, judged exactly as `cne status --datasets` judges.

    Best effort: a lake too young or too damaged to answer must not stop the
    audit, so an unreadable inventory means "nothing known to be stale" rather
    than an exception.
    """
    from cnequity.domain.datasets import DATASETS, is_dataset_enabled, is_stale
    from cnequity.query.reader import list_datasets

    try:
        rows = list_datasets(config=config).iter_rows(named=True)
    except Exception:  # noqa: BLE001 — freshness is an input here, not the subject
        return set()
    anchor = _last_trading_day(config, trade_date)
    out: set[str] = set()
    for row in rows:
        name = row["dataset"]
        if name not in DATASETS or not row.get("has_data") or not row.get("watermarked"):
            continue
        if not is_dataset_enabled(name, config):
            continue
        mark = row.get("watermark") or row.get("coverage_end")
        if mark is not None and is_stale(name, mark, anchor):
            out.add(name)
    return out


def _collect_lake_findings(
    config: Config,
    trade_date: date,
    context: dict | None = None,
    *,
    full: bool = False,
) -> list[dict]:
    """All quality findings for the current curated lake (run-independent).

    ``full`` is reserved for the explicit lake-health path. It makes the
    per-dataset structural/schema checks cover every historical Parquet file;
    ordinary run findings continue to inspect only the active partition.
    """
    findings: list[dict] = []
    context = context or {}

    for skip in context.get("compact_skipped_datasets") or []:
        incomplete = skip.get(
            "incomplete_batches",
            skip.get("failed_batches", 0),
        )
        findings.append(
            {
                "dataset": skip["dataset"],
                "severity": "warning",
                "check": "compact_skipped",
                "message": (
                    f"{incomplete} incomplete batch(es) in run; "
                    "staging not merged and watermark not advanced"
                ),
                "incomplete_batches": incomplete,
            }
        )

    for extra in context.get("audit_findings") or []:
        findings.append(extra)

    # Structural checks must run before any cross-dataset scan. A corrupt
    # Parquet file otherwise fails during the first lazy collect and prevents
    # the audit from identifying the file that caused the failure. Once a
    # dataset has an unreadable file, skip scans that could consume it and say
    # so explicitly in the report.
    invalid_datasets: set[str] = set()
    stale_datasets = _stale_dataset_names(config, trade_date)
    # A demo lake is a handful of symbols and two or three datasets by
    # construction. Auditing it against the whole registry produced 32 errors
    # for a lake that had done exactly what it promised, so a new user's first
    # health check read as a broken install. Judge what this lake holds.
    demo_lake = getattr(config, "lake_profile", None) in {"demo", "sample"}
    for ds, pcol in PARTITION_COLS.items():
        root = config.curated_root / ds
        if demo_lake and not root.exists():
            continue
        dataset_findings = audit_curated_dataset(
            ds, pcol, root, trade_date, full=full, stale=ds in stale_datasets
        )
        findings.extend(dataset_findings)
        if any(
            f.get("check") == "schema_contract" and f.get("unreadable_files", 0) > 0
            for f in dataset_findings
        ):
            invalid_datasets.add(ds)
        elif full:
            # Layout checks, not data checks: both walk the dataset's entire
            # directory tree, and neither can change between two daily runs
            # unless someone deliberately repartitions. Running them on every
            # trading day made the daily health gate open all 17k curated
            # Parquet files — the single largest cost in the pipeline — to
            # re-confirm a layout nobody touched. They belong to the weekly
            # whole-lake sweep, which is also when a repartition would be
            # reviewed.
            mixed = check_mixed_partition_granularity(ds, pcol, root)
            if mixed is not None:
                findings.append(mixed)
            fragmented = check_partition_fragmentation(ds, pcol, root)
            if fragmented is not None:
                findings.append(fragmented)

    findings.extend(_unregistered_curated_dirs(config))
    # Derived checks are independent of the curated cross-dataset scans. Keep
    # them visible even when a corrupt curated file forces those broader scans
    # to stop early.
    findings.extend(market_breadth_findings(config, trade_date, full=full))
    findings.extend(industry_index_findings(config, trade_date, full=full))
    if full:
        findings.extend(pit_announce_date_findings(config))
    if invalid_datasets:
        names = ", ".join(sorted(invalid_datasets))
        findings.append(
            {
                "dataset": "curated",
                "severity": "warning",
                "check": "quality_checks_skipped",
                "message": (
                    "Cross-dataset quality scans were skipped because these curated "
                    f"datasets contain unreadable Parquet: {names}. "
                    "Repair or remove the reported files, then rerun the audit."
                ),
                "datasets": sorted(invalid_datasets),
            }
        )
        return findings

    seed_end = calendar_seed_end()
    forward_days = calendar_forward_coverage_days(trade_date)
    if forward_days < CALENDAR_FORWARD_COVERAGE_WARN_DAYS:
        findings.append(
            {
                "dataset": "trading_calendar",
                "severity": "warning",
                "check": "calendar_forward_coverage",
                "message": (
                    f"holiday seed hardcoded through {seed_end.isoformat()}; "
                    f"only {forward_days} day(s) forward from {trade_date.isoformat()}; "
                    "extend holidays_cn.py before calendar goes stale"
                ),
                "seed_end": seed_end.isoformat(),
                "forward_days": forward_days,
                "warn_threshold_days": CALENDAR_FORWARD_COVERAGE_WARN_DAYS,
            }
        )

    ts_start = trading_status_coverage_start(config)
    if ts_start is not None:
        bars_start = coverage_start_date(config, "daily_bars")
        bars_end = coverage_end_date(config, "daily_bars") or _last_trading_day(config, trade_date)
        observed_st_start = st_coverage_start(config)
        # The audit may run on a weekend or holiday. Evidence must cover the
        # actual bar window, not the wall-clock date when the audit runs;
        # otherwise a receipt through Friday remains falsely incomplete on
        # Saturday even though no Saturday bar can exist.
        evidence = st_evidence_coverage_report(config, bars_start, bars_end)
        if evidence["verified"]:
            message = (
                "trading_status has complete versioned ST/normal evidence for "
                f"{evidence['coverage_start']}..{evidence['coverage_end']}"
            )
        else:
            message = (
                "trading_status rows exist, but no complete current-scope ST evidence "
                f"receipt covers the bar window ({evidence['reason']})"
            )
            checkpoint_completed = evidence.get("checkpoint_completed_symbols")
            checkpoint_expected = evidence.get("checkpoint_expected_symbols")
            if checkpoint_completed is not None and checkpoint_expected is not None:
                message += (
                    f"; ST evidence backfill checkpoint is in progress "
                    f"({checkpoint_completed}/{checkpoint_expected} symbols)"
                )
            unsupported_symbols = int(evidence.get("unsupported_symbols", 0))
            if unsupported_symbols:
                exchange_counts = evidence.get("unsupported_exchange_counts") or {}
                exchange_detail = ", ".join(
                    f"{exchange}={count}" for exchange, count in sorted(exchange_counts.items())
                )
                message += (
                    f"; historical ST source does not cover {unsupported_symbols} "
                    f"current symbol(s) ({exchange_detail or 'exchange unknown'})"
                )
        findings.append(
            {
                "dataset": "trading_status",
                "severity": "info" if evidence["verified"] else "warning",
                "check": "trading_status_coverage_start",
                "message": message,
                "coverage_start": ts_start.isoformat(),
                "st_coverage_start": observed_st_start.isoformat() if observed_st_start else None,
                "st_evidence_coverage_start": evidence.get("coverage_start"),
                "st_evidence_coverage_end": evidence.get("coverage_end"),
                "st_evidence_verified": evidence["verified"],
                "st_evidence_receipt_reason": evidence.get("reason"),
                "st_evidence_checkpoint_status": evidence.get("checkpoint_status"),
                "st_evidence_checkpoint_scope_start": evidence.get("checkpoint_scope_start"),
                "st_evidence_checkpoint_scope_end": evidence.get("checkpoint_scope_end"),
                "st_evidence_checkpoint_completed_symbols": evidence.get(
                    "checkpoint_completed_symbols"
                ),
                "st_evidence_checkpoint_expected_symbols": evidence.get(
                    "checkpoint_expected_symbols"
                ),
                "st_evidence_checkpoint_unresolved_symbols": evidence.get(
                    "checkpoint_unresolved_symbols"
                ),
                "st_evidence_supported_symbols": evidence.get("supported_symbols"),
                "st_evidence_unsupported_symbols": evidence.get("unsupported_symbols", 0),
                "st_evidence_unsupported_exchange_counts": evidence.get(
                    "unsupported_exchange_counts", {}
                ),
                "source_limited": bool(evidence.get("unsupported_symbols")),
                "daily_bars_start": bars_start.isoformat() if bars_start else None,
                "daily_bars_end": bars_end.isoformat() if bars_end else None,
            }
        )

    findings.extend(_index_bars_coverage_findings(config, trade_date))
    findings.extend(daily_bars_calendar_findings(config, trade_date))
    findings.extend(daily_bar_finality_findings(config, trade_date))
    findings.extend(trading_calendar_horizon_findings(config, trade_date))
    findings.extend(daily_bars_volume_unit_findings(config, trade_date))
    findings.extend(daily_bars_amount_completeness_findings(config, trade_date))
    findings.extend(_optional_intraday_findings(config, trade_date))
    # Reaches an external vendor for ~12 quotes; gated on [sources.sina] so a
    # lake without it (and every unit test) stays offline.
    findings.extend(
        daily_bars_close_crosscheck_findings(config, _last_trading_day(config, trade_date))
    )
    findings.extend(valuation_bars_coverage_findings(config, trade_date))
    findings.extend(
        adj_factor_reconciliation_findings(
            config,
            trade_date,
            lookback_days=None if full else ADJ_RECON_LOOKBACK_DAYS,
        )
    )
    findings.extend(adj_factor_coverage_findings(config, trade_date))
    findings.extend(universe_survivorship_findings(config, trade_date))
    findings.extend(instrument_listing_order_findings(config))
    findings.extend(untraded_instrument_findings(config, trade_date))
    findings.extend(undeclared_source_findings(config))
    findings.extend(daily_bars_implied_price_findings(config, trade_date))
    findings.extend(valuation_ratio_unit_findings(config, trade_date))
    findings.extend(balance_sheet_identity_findings(config))
    findings.extend(degraded_job_findings(config))
    findings.extend(adj_factor_arbitration_findings(config))
    # After the price-based check, so the loud days it already named are not
    # reported a second time by the factor-based one.
    findings.extend(
        unrecorded_ex_event_findings(
            config,
            trade_date,
            exclude={
                (finding.get("symbol"), finding.get("trade_date"))
                for finding in findings
                if finding.get("check") == "missing_corporate_action"
            },
        )
    )
    findings.extend(daily_bars_arbitration_findings(config))
    findings.extend(financial_statement_peer_findings(config))
    findings.extend(
        corporate_action_classification_findings(
            config, trade_date - timedelta(days=ADJ_RECON_LOOKBACK_DAYS), trade_date
        )
    )
    # Both sides already in curated — costs no requests (issue #10).
    findings.extend(st_label_crosscheck_findings(config, trade_date))
    findings.extend(macro_staleness_findings(config, trade_date))
    # Reaches the statistics bureau and the exchanges; gated on [sources.nbs]
    # and [sources.exchange] so an offline lake (and every unit test) stays off
    # the network.
    findings.extend(run_authority_checks(config, trade_date))
    return findings


def _optional_intraday_findings(config: Config, trade_date: date) -> list[dict]:
    """Run quality checks only for optional intraday captures in use.

    Historical Parquet can outlive the configuration that created it. That is
    useful for manual inspection, but a disabled optional capture must not
    produce freshness or comparability findings in the routine lake audit.
    The lower-level check functions remain callable directly for that manual
    inspection path.
    """
    findings: list[dict] = []
    if any(is_dataset_enabled(dataset, config) for dataset in ("minute_bars", "minute_bars_5m")):
        findings.extend(minute_bars_findings(config, trade_date))
    if is_dataset_enabled("trade_ticks", config):
        findings.extend(trade_ticks_findings(config, trade_date))
    return findings


def persist_step_findings(
    config: Config, run_id: str, trade_date: date, findings: list[dict]
) -> None:
    """Write a failing step's own findings where the audit file would be.

    A step that raises never reaches ``run_audit``, so everything it learned
    died with the exception message. ``daily_bars`` was the expensive case: its
    interior-gap finding carries the missing symbols and sample keys, but only
    the one-line message survived the raise, leaving the operator to reverse
    engineer the cause out of a log file.

    Best effort by construction — losing the diagnostic must never replace the
    real failure with an I/O error.
    """
    if not findings:
        return
    try:
        out_dir = config.meta_root / "quality" / "findings"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{run_id}.json"
        existing: list[dict] = []
        if out_path.exists():
            with open(out_path, encoding="utf-8") as handle:
                existing = json.load(handle).get("findings", [])
        write_json_atomic(
            out_path,
            {
                "run_id": run_id,
                "trade_date": trade_date.isoformat(),
                "findings": existing + findings,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    except Exception:  # noqa: BLE001 — diagnostics must not mask the failure
        logger.warning("could not persist step findings for run %s", run_id, exc_info=True)


def run_audit(config: Config, run_id: str, trade_date: date, context: dict | None = None) -> int:
    findings = _collect_lake_findings(config, trade_date, context)
    # Keep the dedicated source-diff artifact, but also include its findings in
    # the per-run audit file. Otherwise callers reading one run's findings (and
    # not the second directory) can mistake "audit completed" for "sources
    # agreed".
    findings.extend(run_source_diffs(config, run_id, trade_date))

    out_dir = config.meta_root / "quality" / "findings"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{run_id}.json"
    write_json_atomic(
        out_path,
        {"run_id": run_id, "trade_date": trade_date.isoformat(), "findings": findings},
        ensure_ascii=False,
        indent=2,
        default=str,
    )

    by_severity: dict[str, int] = {}
    for item in findings:
        key = str(item.get("severity", "info"))
        by_severity[key] = by_severity.get(key, 0) + 1
    if context is not None:
        # The step needs severities, not just a count, to decide whether this
        # run should have been allowed to publish. The caller already owns
        # this dict; returning a richer type would break every other caller.
        context["audit_by_severity"] = by_severity
        # The CLI gates on this and has to be able to say *what* failed; a bare
        # count sends the operator back to the findings file.
        context["audit_error_findings"] = [
            item for item in findings if str(item.get("severity")) == "error"
        ]

    return len(findings)


def _all_a_st_evidence_summary(findings: list[dict]) -> dict | None:
    """Extract the all-A ST evidence baseline from the lake audit findings.

    ``lake_health`` may validate a narrower research universe, but the
    operational trading-status audit intentionally remains all-A. Persisting
    this baseline beside the selected research contract prevents a scoped
    READY result from hiding an unresolved BJ source limitation.
    """
    finding = next(
        (item for item in findings if item.get("check") == "trading_status_coverage_start"),
        None,
    )
    if finding is None:
        return None
    return {
        "verified": bool(finding.get("st_evidence_verified")),
        "coverage_start": finding.get("st_evidence_coverage_start"),
        "coverage_end": finding.get("st_evidence_coverage_end"),
        "supported_symbols": finding.get("st_evidence_supported_symbols"),
        "unsupported_symbols": finding.get("st_evidence_unsupported_symbols", 0),
        "unsupported_exchange_counts": finding.get("st_evidence_unsupported_exchange_counts", {}),
        "reason": finding.get("st_evidence_receipt_reason"),
    }


def lake_health(
    config: Config,
    trade_date: date,
    *,
    research_start: date | None = None,
    research_end: date | None = None,
    research_universe: str = "all_a",
) -> dict:
    """Lake health: findings + freshness → ``meta/quality/health-latest.json``."""
    from cnequity.domain.datasets import DATASETS, is_dataset_enabled, is_stale
    from cnequity.quality.historical_validity import historical_universe_validity
    from cnequity.query.reader import list_datasets

    anchor = _last_trading_day(config, trade_date)
    # Health can be requested on a weekend/holiday. All data observations
    # (including source snapshots) must use the last actual session; the raw
    # calendar date is retained below for reporting only.
    findings = _collect_lake_findings(config, anchor, None, full=True)
    # source_diff is local-only: it compares curated rows with the latest
    # already-captured backup snapshot. Running it here makes an explicit
    # health check authoritative even when no ingestion run happened today.
    findings.extend(run_source_diffs(config, f"health-{anchor.isoformat()}", anchor))
    by_severity: dict[str, int] = {}
    for f in findings:
        sev = f.get("severity", "info")
        by_severity[sev] = by_severity.get(sev, 0) + 1

    catalog = list_datasets(config=config)
    stale: list[str] = []
    empty: list[str] = []
    expected_empty: list[str] = []
    for row in catalog.iter_rows(named=True):
        if not row["has_data"]:
            spec = DATASETS.get(row["dataset"])
            if spec is not None and spec.empty_severity == "info":
                expected_empty.append(row["dataset"])
            else:
                empty.append(row["dataset"])
            continue
        if not row["watermarked"] or not is_dataset_enabled(row["dataset"], config):
            continue
        mark = row["watermark"] or row["coverage_end"]
        if is_stale(row["dataset"], mark, anchor):
            stale.append(row["dataset"])

    historical_validity = historical_universe_validity(
        config,
        start=research_start,
        end=research_end,
        universe=research_universe,
    )
    all_a_st_evidence = _all_a_st_evidence_summary(findings)
    health = {
        "trade_date": trade_date.isoformat(),
        "last_trading_day": anchor.isoformat(),
        "findings_by_severity": by_severity,
        "error_findings": [f for f in findings if f.get("severity") == "error"],
        "warning_findings": [f for f in findings if f.get("severity") == "warning"],
        # Keep informational evidence in the persisted full-health snapshot
        # too.  It is intentionally separate from warnings so known source
        # limitations do not make an operationally healthy lake look broken,
        # but dropping the details would make the distinction undiscoverable.
        "info_findings": [f for f in findings if f.get("severity") == "info"],
        "stale_datasets": sorted(stale),
        "empty_datasets": sorted(empty),
        "expected_empty_datasets": sorted(expected_empty),
        # Research readiness is intentionally independent of operational
        # health. A fresh lake can still be unsafe for a long backtest, while a
        # stale optional dataset need not invalidate a closed historical study.
        "historical_universe_validity": historical_validity,
        "historical_universe": historical_validity.get("universe", research_universe),
        "historical_all_a_st_evidence": all_a_st_evidence,
        "healthy": by_severity.get("error", 0) == 0 and not stale,
    }

    out_dir = config.meta_root / "quality"
    out_dir.mkdir(parents=True, exist_ok=True)
    write_json_atomic(
        out_dir / "health-latest.json",
        health,
        ensure_ascii=False,
        indent=2,
        default=str,
    )
    write_json_atomic(
        out_dir / "historical-validity-latest.json",
        historical_validity,
        ensure_ascii=False,
        indent=2,
        default=str,
    )
    return health


def _last_trading_day(config: Config, trade_date: date) -> date:
    from datetime import timedelta

    from cnequity.steps.common import is_trading_day

    d = trade_date if is_session_final(trade_date) else trade_date - timedelta(days=1)
    for _ in range(15):
        if is_trading_day(config, d):
            return d
        d -= timedelta(days=1)
    return trade_date
