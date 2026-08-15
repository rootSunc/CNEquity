from __future__ import annotations

import json
from datetime import date

import polars as pl

from cn_market_lake.adapters.calendar.exchange_calendar import (
    CALENDAR_FORWARD_COVERAGE_WARN_DAYS,
    calendar_forward_coverage_days,
    calendar_seed_end,
)
from cn_market_lake.config import Config
from cn_market_lake.domain.datasets import PARTITION_COLS, curated_dataset_names
from cn_market_lake.quality.authority_checks import run_authority_checks
from cn_market_lake.quality.cross_checks import (
    adj_factor_coverage_findings,
    adj_factor_reconciliation_findings,
    daily_bars_calendar_findings,
    daily_bars_close_crosscheck_findings,
    st_label_crosscheck_findings,
    trading_calendar_horizon_findings,
    universe_survivorship_findings,
    valuation_bars_coverage_findings,
)
from cn_market_lake.quality.dataset_checks import (
    audit_curated_dataset,
    check_mixed_partition_granularity,
    check_partition_fragmentation,
)
from cn_market_lake.quality.intraday_checks import minute_bars_findings
from cn_market_lake.quality.macro_checks import macro_staleness_findings
from cn_market_lake.quality.source_diff import run_source_diffs
from cn_market_lake.quality.st_coverage import st_evidence_coverage_report
from cn_market_lake.quality.tick_checks import trade_ticks_findings
from cn_market_lake.quality.unit_checks import daily_bars_volume_unit_findings
from cn_market_lake.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cn_market_lake.query.universe import (
    coverage_start_date,
    st_coverage_start,
    trading_status_coverage_start,
)

# Sample missing/orphan dates surfaced in a coverage finding.
_INDEX_COVERAGE_SAMPLE = 8


def _index_bars_coverage_findings(config: Config, trade_date: date) -> list[dict]:
    """index_bars vs trading_calendar within each symbol's covered span."""
    findings: list[dict] = []
    cal_root = config.curated_root / "trading_calendar"
    ib_root = config.curated_root / "index_bars"
    if not dataset_has_parquet(cal_root) or not dataset_has_parquet(ib_root):
        return findings

    cal = (
        scan_parquet_root(cal_root, partition_col="trade_date", end=trade_date)
        .filter(pl.col("is_trading"))
        .select("trade_date")
        .unique()
        .collect()
    )
    trading_days = set(cal["trade_date"].to_list())
    if not trading_days:
        return findings

    ib = (
        scan_parquet_root(ib_root, partition_col="trade_date", end=trade_date)
        .select("symbol", "trade_date")
        .unique()
        .collect()
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
        parts = []
        if missing:
            parts.append(f"{len(missing)} calendar trading day(s) with no bar")
        if orphan:
            parts.append(f"{len(orphan)} bar(s) on non-trading days")
        findings.append(
            {
                "dataset": "index_bars",
                "symbol": sym,
                "severity": "warning",
                "check": "index_bars_calendar_coverage",
                "message": (
                    f"{sym}: " + "; ".join(parts) + f" over {first.isoformat()}..{last.isoformat()}"
                ),
                "covered_days": len(have),
                "expected_days": len(expected),
                "missing_count": len(missing),
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


def _collect_lake_findings(
    config: Config, trade_date: date, context: dict | None = None
) -> list[dict]:
    """All quality findings for the current curated lake (run-independent)."""
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
        observed_st_start = st_coverage_start(config)
        evidence = st_evidence_coverage_report(config, bars_start, trade_date)
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
                "daily_bars_start": bars_start.isoformat() if bars_start else None,
            }
        )

    findings.extend(_index_bars_coverage_findings(config, trade_date))
    findings.extend(daily_bars_calendar_findings(config, trade_date))
    findings.extend(trading_calendar_horizon_findings(config, trade_date))
    findings.extend(daily_bars_volume_unit_findings(config, trade_date))
    # No-ops on a lake that never enabled intraday capture.
    findings.extend(minute_bars_findings(config, trade_date))
    findings.extend(trade_ticks_findings(config, trade_date))
    # Reaches an external vendor for ~12 quotes; gated on [sources.sina] so a
    # lake without it (and every unit test) stays offline.
    findings.extend(
        daily_bars_close_crosscheck_findings(config, _last_trading_day(config, trade_date))
    )
    findings.extend(valuation_bars_coverage_findings(config, trade_date))
    findings.extend(adj_factor_reconciliation_findings(config, trade_date))
    findings.extend(adj_factor_coverage_findings(config, trade_date))
    findings.extend(universe_survivorship_findings(config, trade_date))
    # Both sides already in curated — costs no requests (issue #10).
    findings.extend(st_label_crosscheck_findings(config, trade_date))
    findings.extend(macro_staleness_findings(config, trade_date))
    # Reaches the statistics bureau and the exchanges; gated on [sources.nbs]
    # and [sources.exchange] so an offline lake (and every unit test) stays off
    # the network.
    findings.extend(run_authority_checks(config, trade_date))
    findings.extend(_unregistered_curated_dirs(config))

    for ds, pcol in PARTITION_COLS.items():
        root = config.curated_root / ds
        findings.extend(audit_curated_dataset(ds, pcol, root, trade_date))
        mixed = check_mixed_partition_granularity(ds, pcol, root)
        if mixed is not None:
            findings.append(mixed)
        fragmented = check_partition_fragmentation(ds, pcol, root)
        if fragmented is not None:
            findings.append(fragmented)
    return findings


def run_audit(config: Config, run_id: str, trade_date: date, context: dict | None = None) -> int:
    findings = _collect_lake_findings(config, trade_date, context)

    out_dir = config.meta_root / "quality" / "findings"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{run_id}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(
            {"run_id": run_id, "trade_date": trade_date.isoformat(), "findings": findings},
            f,
            ensure_ascii=False,
            indent=2,
            default=str,
        )

    diffs = run_source_diffs(config, run_id, trade_date)
    return len(findings) + len(diffs)


def lake_health(
    config: Config,
    trade_date: date,
    *,
    research_start: date | None = None,
    research_end: date | None = None,
) -> dict:
    """Lake health: findings + freshness → ``meta/quality/health-latest.json``."""
    from cn_market_lake.domain.datasets import is_stale
    from cn_market_lake.quality.historical_validity import historical_universe_validity
    from cn_market_lake.query.reader import list_datasets

    findings = _collect_lake_findings(config, trade_date, None)
    by_severity: dict[str, int] = {}
    for f in findings:
        sev = f.get("severity", "info")
        by_severity[sev] = by_severity.get(sev, 0) + 1

    anchor = _last_trading_day(config, trade_date)
    catalog = list_datasets(config=config)
    stale: list[str] = []
    empty: list[str] = []
    for row in catalog.iter_rows(named=True):
        if not row["has_data"]:
            empty.append(row["dataset"])
            continue
        if not row["watermarked"]:
            continue
        mark = row["watermark"] or row["coverage_end"]
        if is_stale(row["dataset"], mark, anchor):
            stale.append(row["dataset"])

    historical_validity = historical_universe_validity(
        config,
        start=research_start,
        end=research_end,
    )
    health = {
        "trade_date": trade_date.isoformat(),
        "last_trading_day": anchor.isoformat(),
        "findings_by_severity": by_severity,
        "error_findings": [f for f in findings if f.get("severity") == "error"],
        "warning_findings": [f for f in findings if f.get("severity") == "warning"],
        "stale_datasets": sorted(stale),
        "empty_datasets": sorted(empty),
        # Research readiness is intentionally independent of operational
        # health. A fresh lake can still be unsafe for a long backtest, while a
        # stale optional dataset need not invalidate a closed historical study.
        "historical_universe_validity": historical_validity,
        "healthy": by_severity.get("error", 0) == 0 and not stale,
    }

    out_dir = config.meta_root / "quality"
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "health-latest.json", "w", encoding="utf-8") as f:
        json.dump(health, f, ensure_ascii=False, indent=2, default=str)
    with open(out_dir / "historical-validity-latest.json", "w", encoding="utf-8") as f:
        json.dump(historical_validity, f, ensure_ascii=False, indent=2, default=str)
    return health


def _last_trading_day(config: Config, trade_date: date) -> date:
    from datetime import timedelta

    from cn_market_lake.steps.common import is_trading_day

    d = trade_date
    for _ in range(15):
        if is_trading_day(config, d):
            return d
        d -= timedelta(days=1)
    return trade_date
