"""Machine-readable validity contract for historical research universes."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.universe_profiles import (
    UniverseProfile,
    UniverseProfileError,
    resolve_universe_profile,
)
from cnequity.quality.delisted_coverage import delisted_coverage_report
from cnequity.quality.st_coverage import st_evidence_coverage_report
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.query.universe import (
    SUPPORTED_UNIVERSES,
    coverage_end_date,
    coverage_start_date,
    st_coverage_start,
)


def _daily_bar_missing_sessions(
    config: Config,
    start: date,
    end: date,
    *,
    symbols: list[str] | None = None,
) -> list[date]:
    """Return whole-market trading sessions absent from daily_bars.

    Individual symbols can legitimately be suspended, so this is deliberately
    a dataset-level check: a session is missing only when no daily bar landed
    for any symbol.  The boundary check above cannot see this interior hole,
    yet a backtest spanning it would silently bridge the missing session.
    """
    if start > end or not dataset_has_parquet(config.curated_root / "daily_bars"):
        return []
    if symbols is not None and not symbols:
        return []

    from cnequity.query.calendar import list_trading_dates

    expected = list_trading_dates(config, start, end)
    if not expected:
        return []
    bars = scan_parquet_root(
        config.curated_root / "daily_bars",
        partition_col="trade_date",
        start=start,
        end=end,
        symbols=symbols,
        traded_only=True,
    )
    actual = bars.select("trade_date").unique().collect(engine="streaming")
    present = set(actual["trade_date"].drop_nulls().to_list())
    return [session for session in expected if session not in present]


def _bars_end(config: Config) -> date | None:
    return coverage_end_date(config, "daily_bars")


def _scoped_bar_bounds(config: Config, universe: str) -> tuple[date | None, date | None]:
    """Return daily-bar bounds for the requested research universe."""
    if universe == "all_a":
        return coverage_start_date(config, "daily_bars"), _bars_end(config)

    from cnequity.quality.st_coverage import current_st_universe

    symbols = current_st_universe(config, universe=universe)
    if not symbols or not dataset_has_parquet(config.curated_root / "daily_bars"):
        return None, None
    bounds = (
        scan_parquet_root(
            config.curated_root / "daily_bars",
            partition_col="trade_date",
            symbols=symbols,
            traded_only=True,
        )
        .select(
            pl.col("trade_date").min().alias("start"),
            pl.col("trade_date").max().alias("end"),
        )
        .collect(engine="streaming")
        .row(0)
    )
    return bounds[0], bounds[1]


def historical_universe_validity(
    config: Config,
    start: date | None = None,
    end: date | None = None,
    *,
    sample: int = 15,
    universe: str = "all_a",
    profile: str | UniverseProfile | None = None,
) -> dict:
    """Return a strict, read-only historical-universe validity manifest.

    The contract covers the price-window boundary, historical ST filtering and
    catalogued delistings. Adjustment-factor exactness, PIT fundamentals and
    strategy-specific feature coverage remain downstream responsibilities.
    """
    resolved_profile = None
    if profile is not None:
        try:
            resolved_profile = resolve_universe_profile(profile)
        except UniverseProfileError as exc:
            raise ValueError(str(exc)) from exc
        if universe != "all_a" and universe != resolved_profile.legacy_universe:
            raise ValueError(
                f"universe={universe!r} conflicts with profile={resolved_profile.name!r}"
            )
        universe = resolved_profile.legacy_universe
    if universe not in SUPPORTED_UNIVERSES:
        raise ValueError(
            f"unsupported historical universe: {universe!r} "
            f"(supported: {', '.join(sorted(SUPPORTED_UNIVERSES))})"
        )
    bars_read_error: str | None = None
    try:
        observed_start, observed_end = _scoped_bar_bounds(config, universe)
    except (OSError, pl.exceptions.PolarsError, ValueError) as exc:
        # The structural lake audit reports the exact bad file. Keep this
        # machine-readable research gate usable as a standalone API too:
        # unreadable bars mean the historical claim is not ready, not that the
        # health command itself should crash.
        observed_start = observed_end = None
        bars_read_error = str(exc)
    requested_start = start or observed_start
    requested_end = end or observed_end

    blockers: list[dict] = []
    if bars_read_error:
        blockers.append(
            {
                "check": "daily_bars_readable",
                "code": "daily_bars_unreadable",
                "message": f"daily_bars could not be read: {bars_read_error}",
                "remediation": (
                    "Repair or remove the unreadable Parquet file reported by the lake "
                    "quality audit, then rerun this validity check."
                ),
            }
        )
    window_valid = (
        requested_start is not None
        and requested_end is not None
        and requested_start <= requested_end
        and observed_start is not None
        and observed_end is not None
        and observed_start <= requested_start
        and observed_end >= requested_end
    )
    if requested_start is None or requested_end is None:
        blockers.append(
            {
                "check": "daily_bars_window",
                "code": "daily_bars_window_unknown",
                "message": "daily_bars has no observable research window",
                "remediation": "Backfill and compact daily_bars before validating research history.",
            }
        )
    elif requested_start > requested_end:
        blockers.append(
            {
                "check": "daily_bars_window",
                "code": "invalid_requested_window",
                "message": "requested start is after requested end",
                "remediation": "Choose an inclusive window with start on or before end.",
            }
        )
    elif not window_valid:
        blockers.append(
            {
                "check": "daily_bars_window",
                "code": "daily_bars_window_incomplete",
                "message": (
                    f"requested {requested_start.isoformat()}..{requested_end.isoformat()} "
                    f"is not contained by daily_bars "
                    f"{observed_start.isoformat() if observed_start else 'unknown'}.."
                    f"{observed_end.isoformat() if observed_end else 'unknown'}"
                ),
                "remediation": "Backfill and compact daily_bars for the full requested window.",
            }
        )

    observed_positive_st_start = st_coverage_start(config)
    # What the lake can back, independent of what was asked for. A gate that
    # only ever answers "not the whole history" tells a new install nothing it
    # can act on; this says where research can start today.
    from cnequity.quality.st_coverage import st_evidence_supported_window

    supported = st_evidence_supported_window(config, universe=universe)
    st_evidence = st_evidence_coverage_report(
        config,
        requested_start,
        requested_end,
        universe=universe,
    )
    st_valid = bool(st_evidence["verified"])
    if not st_valid:
        unsupported = int(st_evidence.get("unsupported_symbols", 0) or 0)
        if st_evidence.get("reason") == "unsupported_exchange_symbols":
            message = (
                "historical ST evidence covers the supported exchanges, but "
                f"{unsupported} {universe} symbol(s) belong to an exchange not served by "
                "the configured historical source"
            )
            tushare_ready = bool(config.sources.get("tushare", False) and config.tushare_token)
            if tushare_ready:
                remediation = (
                    "Tushare is enabled and covers BJ from 2016; symbols with bars "
                    "before 2016 still require a deeper historical ST source, or "
                    "BJ must be excluded from the research universe. Do not treat "
                    "unresolved symbols as normal."
                )
            else:
                remediation = (
                    "Move the research window inside the supported window above: the "
                    "exchange board answers BJ every session and that evidence grows "
                    "daily, at no cost. For history before it, enable [sources.tushare] "
                    "with TUSHARE_TOKEN (covers BJ from 2016; symbols with bars before "
                    "2016 still require a deeper source), or exclude BJ from the "
                    "research universe. Do not treat unresolved symbols as normal."
                )
        else:
            message = (
                "historical ST evidence has no complete, current scope receipt "
                f"for the requested window ({st_evidence['reason']})"
            )
            remediation = (
                "Run a full `cne backfill trading_status` for this window and current "
                "all-A symbol scope; resolve every failed symbol."
            )
        blockers.append(
            {
                "check": "historical_st_labels",
                "code": "historical_st_labels_incomplete",
                "message": message,
                "remediation": remediation,
                "unsupported_symbols": unsupported,
            }
        )

    daily_bar_missing: list[date] = []
    if (
        not bars_read_error
        and window_valid
        and requested_start is not None
        and requested_end is not None
    ):
        from cnequity.quality.st_coverage import current_st_universe

        scoped_symbols = (
            None
            if universe == "all_a"
            else current_st_universe(
                config,
                start=requested_start,
                end=requested_end,
                universe=universe,
            )
        )
        daily_bar_missing = _daily_bar_missing_sessions(
            config,
            requested_start,
            requested_end,
            symbols=scoped_symbols,
        )
        if daily_bar_missing:
            blockers.append(
                {
                    "check": "daily_bars_interior_coverage",
                    "code": "daily_bars_interior_gap",
                    "message": (
                        f"daily_bars has no rows for {len(daily_bar_missing)} trading "
                        f"session(s) inside the requested window (e.g. "
                        f"{daily_bar_missing[0].isoformat()})"
                    ),
                    "missing_sessions": len(daily_bar_missing),
                    "sample_sessions": [d.isoformat() for d in daily_bar_missing[:15]],
                    "remediation": (
                        "Backfill and compact daily_bars for the missing sessions before "
                        "using this research window."
                    ),
                }
            )

    survivorship: dict | None = None
    survivorship_valid = False
    if (
        not bars_read_error
        and requested_start is not None
        and requested_end is not None
        and requested_start <= requested_end
    ):
        try:
            survivorship = delisted_coverage_report(
                config,
                requested_start,
                requested_end,
                sample=sample,
                universe=universe,
            )
        except (OSError, pl.exceptions.PolarsError, ValueError) as exc:
            survivorship = {
                "verified": False,
                "counts": {
                    "pending_probe": 0,
                    "missing_bars": 0,
                    "unknown_overlap": 0,
                    "terminal_mismatch": 0,
                    "missing_instrument": 0,
                    "invalid_delist_date": 0,
                },
                "error": str(exc),
            }
            blockers.append(
                {
                    "check": "delisted_universe",
                    "code": "delisted_universe_unreadable",
                    "message": f"delisted coverage could not be read: {exc}",
                    "remediation": (
                        "Repair the unreadable published Parquet file, then rerun delisted "
                        "coverage and this validity check."
                    ),
                }
            )
        survivorship_valid = bool(survivorship["verified"])
        if not survivorship_valid and "error" not in survivorship:
            counts = survivorship["counts"]
            blockers.append(
                {
                    "check": "delisted_universe",
                    "code": "delisted_universe_unverified",
                    "message": (
                        "delisted coverage is unverified: "
                        f"{counts.get('pending_probe', 0)} pending probes, "
                        f"{counts.get('recent_quarantined', 0)} recent names without current evidence, "
                        f"{counts.get('formal_unresolved', 0)} formal names without overlap evidence, "
                        f"{counts.get('missing_bars', 0)} missing bars, "
                        f"{counts.get('unknown_overlap', 0)} unknown overlaps, "
                        f"{counts.get('terminal_mismatch', 0)} terminal mismatches, "
                        f"{counts.get('missing_instrument', 0)} missing instruments, "
                        f"{counts.get('invalid_delist_date', 0)} invalid delist dates"
                    ),
                    "remediation": (
                        "`cne delisted status` shows the current counts; from a source checkout, run "
                        "`scripts/delisted_ops.py coverage` for samples, then complete discovery and "
                        "repair the reported catalogue, bars, or instruments gaps."
                    ),
                }
            )

    universe_ready = window_valid and not daily_bar_missing and st_valid and survivorship_valid
    result = {
        "schema_version": 1,
        "claim": f"historical_{universe}_universe_validity",
        "universe": universe,
        "window": {
            "start": requested_start.isoformat() if requested_start else None,
            "end": requested_end.isoformat() if requested_end else None,
        },
        "universe_ready": universe_ready,
        "supported_window": supported.get("window"),
        "checks": {
            "daily_bars_window": {
                "passed": window_valid,
                "observed_start": observed_start.isoformat() if observed_start else None,
                "observed_end": observed_end.isoformat() if observed_end else None,
            },
            "daily_bars_interior_coverage": {
                "passed": not daily_bar_missing,
                "missing_sessions": len(daily_bar_missing),
                "sample_sessions": [d.isoformat() for d in daily_bar_missing[:15]],
            },
            "historical_st_labels": {
                "passed": st_valid,
                "supported_window": supported.get("window"),
                "supported_window_by_source": supported.get("by_source"),
                "supported_window_blocked_by": supported.get("missing_source"),
                "supported_window_missing_symbols": supported.get("missing_symbols"),
                "supported_window_current_symbols": supported.get("current_symbols"),
                "coverage_start": st_evidence.get("coverage_start"),
                "coverage_end": st_evidence.get("coverage_end"),
                "observed_positive_st_start": (
                    observed_positive_st_start.isoformat() if observed_positive_st_start else None
                ),
                "evidence": st_evidence,
            },
            "delisted_universe": {
                "passed": survivorship_valid,
                "report": survivorship,
            },
        },
        "blockers": blockers,
        "limitations": [
            "Does not verify adjustment-factor exactness or strategy feature coverage.",
            "Does not verify point-in-time semantics of fundamentals used by a strategy.",
        ],
    }
    if resolved_profile is not None:
        # Keep the legacy fields above for old consumers, while binding a
        # profile-aware caller to the exact version and scope rules it asked
        # for.  The profile's strict evidence requirements are represented by
        # the existing historical ST/delisting checks in this report.
        result["profile"] = resolved_profile.to_dict()
        result["profile_name"] = resolved_profile.name
        result["profile_version"] = resolved_profile.version
        result["scope_hash"] = resolved_profile.scope_hash
        result["claim"] = f"historical_{resolved_profile.name}_universe_validity"
    return result
