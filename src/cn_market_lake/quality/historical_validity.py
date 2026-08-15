"""Machine-readable validity contract for historical all-A universes."""

from __future__ import annotations

from datetime import date

from cn_market_lake.config import Config
from cn_market_lake.quality.st_coverage import st_evidence_coverage_report
from cn_market_lake.query.parquet_scan import list_partitions
from cn_market_lake.query.universe import coverage_start_date, st_coverage_start
from cn_market_lake.steps.delisted import delisted_coverage_report


def _bars_end(config: Config) -> date | None:
    parts = list_partitions(config.curated_root / "daily_bars", "trade_date")
    return parts[-1].end if parts else None


def historical_universe_validity(
    config: Config,
    start: date | None = None,
    end: date | None = None,
    *,
    sample: int = 15,
) -> dict:
    """Return a strict, read-only all-A universe validity manifest.

    The contract covers the price-window boundary, historical ST filtering and
    catalogued delistings. Adjustment-factor exactness, PIT fundamentals and
    strategy-specific feature coverage remain downstream responsibilities.
    """
    observed_start = coverage_start_date(config, "daily_bars")
    observed_end = _bars_end(config)
    requested_start = start or observed_start
    requested_end = end or observed_end

    blockers: list[dict] = []
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
    st_evidence = st_evidence_coverage_report(config, requested_start, requested_end)
    st_valid = bool(st_evidence["verified"])
    if not st_valid:
        blockers.append(
            {
                "check": "historical_st_labels",
                "code": "historical_st_labels_incomplete",
                "message": "historical ST evidence has no complete, current scope receipt "
                f"for the requested window ({st_evidence['reason']})",
                "remediation": (
                    "Run a full `cml backfill trading_status` for this window and current "
                    "all-A symbol scope; resolve every failed symbol."
                ),
            }
        )

    survivorship: dict | None = None
    survivorship_valid = False
    if (
        requested_start is not None
        and requested_end is not None
        and requested_start <= requested_end
    ):
        survivorship = delisted_coverage_report(
            config, requested_start, requested_end, sample=sample
        )
        survivorship_valid = bool(survivorship["verified"])
        if not survivorship_valid:
            counts = survivorship["counts"]
            blockers.append(
                {
                    "check": "delisted_universe",
                    "code": "delisted_universe_unverified",
                    "message": (
                        "delisted coverage is unverified: "
                        f"{counts['pending_probe']} pending probes, "
                        f"{counts['missing_bars']} missing bars, "
                        f"{counts['unknown_overlap']} unknown overlaps, "
                        f"{counts['terminal_mismatch']} terminal mismatches, "
                        f"{counts['missing_instrument']} missing instruments, "
                        f"{counts['invalid_delist_date']} invalid delist dates"
                    ),
                    "remediation": (
                        "Run `cml delisted coverage` for samples, then complete discovery and "
                        "repair the reported catalogue, bars, or instruments gaps."
                    ),
                }
            )

    universe_ready = window_valid and st_valid and survivorship_valid
    return {
        "schema_version": 1,
        "claim": "historical_all_a_universe_validity",
        "window": {
            "start": requested_start.isoformat() if requested_start else None,
            "end": requested_end.isoformat() if requested_end else None,
        },
        "universe_ready": universe_ready,
        "checks": {
            "daily_bars_window": {
                "passed": window_valid,
                "observed_start": observed_start.isoformat() if observed_start else None,
                "observed_end": observed_end.isoformat() if observed_end else None,
            },
            "historical_st_labels": {
                "passed": st_valid,
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
