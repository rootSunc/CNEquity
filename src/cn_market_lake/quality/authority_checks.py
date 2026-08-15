"""Compare curated against the bodies that actually publish the numbers.

The other checks in this package all reason about the lake's internal
consistency: does a value look stale, did it change, do two feeds we already
hold agree. None of them can see the failure where a vendor publishes on time,
in the right shape, with a wrong number — which is precisely the failure
issue #3 turned up (``m2_yoy`` carried M0 month-over-month growth for its whole
history, on schedule, in the right column type).

Catching that needs an outside reading, so these two reach the publisher:

* ``macro_pmi_vs_nbs`` — 制造业 PMI against the NBS release
* ``st_labels_vs_exchange`` — ST designations against the SSE / SZSE listings

Both cost network requests, so both are gated on their ``[sources.*]`` flag —
defaulting **off** when the section is absent — and degrade to silence when the
source is unreachable, following ``daily_bars_close_crosscheck_findings``.

**Why M2 is not here.** The PBOC publishes 货币供应量 as levels only, and revised
the M1 caliber from 2025-01. Deriving a year-on-year figure from levels across a
caliber change would manufacture a number the publisher deliberately computes on
a comparable basis, so a derived comparison would report drift that is an
artefact of our own arithmetic. EastMoney's M2 was verified against the PBOC
release by hand (2026-06: 同比 8%, 余额 356.71万亿, and M1/M0 likewise); a
resident check waits for the PBOC to publish the rate itself.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.query.parquet_scan import dataset_has_parquet, scan_parquet_root

logger = logging.getLogger(__name__)

_SAMPLE = 8

# PMI is published to one decimal, so any real disagreement is at least 0.1.
# The tolerance only absorbs float representation.
PMI_TOLERANCE = 0.05

# Naming and board membership move on the same day but are captured by different
# steps, so a couple of names either side of that boundary is routine.
ST_MAX_DISAGREEMENT = 3


@dataclass(frozen=True)
class AuthorityCheckOutcome:
    """Result state for a publisher comparison, including non-findings."""

    status: str
    findings: list[dict]


def _curated_value(config: Config, indicator_id: str, obs_date: date) -> float | None:
    root = config.curated_root / "macro_indicators"
    if not dataset_has_parquet(root):
        return None
    out = (
        scan_parquet_root(root, partition_col="obs_date", start=obs_date, end=obs_date)
        .filter(pl.col("indicator_id") == indicator_id)
        .select("value")
        .collect()
    )
    return None if out.is_empty() else float(out.get_column("value")[-1])


def _macro_pmi_vs_nbs_outcome(config: Config, trade_date: date) -> AuthorityCheckOutcome:
    """Newest curated ``pmi_manufacturing`` against the NBS release for that month."""
    # Default off, like the sina close cross-check: absent config means an
    # offline lake, and `cml audit` must not silently start making requests.
    if not config.sources.get("nbs", False):
        return AuthorityCheckOutcome("skipped_disabled", [])
    from cn_market_lake.adapters.nbs.pmi_release import fetch_latest_pmi

    published = fetch_latest_pmi(config=config)
    if published is None:
        return AuthorityCheckOutcome("unavailable", [])

    obs_date = published["obs_date"]
    if obs_date > trade_date:
        # The bureau has published a month the run has not reached yet.
        return AuthorityCheckOutcome("skipped_not_due", [])

    ours = _curated_value(config, "pmi_manufacturing", obs_date)
    if ours is None:
        # Missing coverage is `macro_indicator_stale`'s job, not a disagreement.
        return AuthorityCheckOutcome("skipped_no_curated", [])
    if abs(ours - published["value"]) <= PMI_TOLERANCE:
        return AuthorityCheckOutcome("agreed", [])

    return AuthorityCheckOutcome(
        "disagreed",
        [
            {
                "dataset": "macro_indicators",
                "severity": "error",
                "check": "macro_pmi_vs_nbs",
                "message": (
                    f"pmi_manufacturing for {obs_date.isoformat()} is {ours} in curated but "
                    f"{published['value']} in the 国家统计局 release — the vendor has drifted "
                    "from the publisher"
                ),
                "indicator_id": "pmi_manufacturing",
                "obs_date": obs_date.isoformat(),
                "curated_value": ours,
                "published_value": published["value"],
                "source_url": published["url"],
            }
        ],
    )


def macro_pmi_vs_nbs(config: Config, trade_date: date) -> list[dict]:
    """Return only disagreement findings for backwards-compatible callers."""
    return _macro_pmi_vs_nbs_outcome(config, trade_date).findings


def _curated_status(config: Config, trade_date: date) -> tuple[set[str], set[str]] | None:
    """``(every symbol covered that day, the ST-labeled subset)``."""
    root = config.curated_root / "trading_status"
    if not dataset_has_parquet(root):
        return None
    out = (
        scan_parquet_root(root, partition_col="trade_date", end=trade_date)
        .filter(pl.col("trade_date") == pl.lit(trade_date))
        .select("symbol", "status")
        .unique()
        .collect()
    )
    if out.is_empty():
        return None
    covered = set(out.get_column("symbol").to_list())
    labeled = set(out.filter(pl.col("status") == "st").get_column("symbol").to_list())
    return covered, labeled


def _st_labels_vs_exchange_outcome(config: Config, trade_date: date) -> AuthorityCheckOutcome:
    """Curated ST labels against the 简称 the exchanges publish.

    Compared over the **shared universe** only. The exchanges carry a company
    until formal delisting while a quote feed drops it once it stops trading —
    measured 2026-08-01, SSE still listed two ST names that both EastMoney and
    TDX had dropped. Judging over either side's full set would report that
    permanently.
    """
    if not config.sources.get("exchange", False):
        return AuthorityCheckOutcome("skipped_disabled", [])
    status = _curated_status(config, trade_date)
    if status is None:
        return AuthorityCheckOutcome("skipped_no_curated", [])
    covered, labeled = status

    from cn_market_lake.adapters.exchange.st_lists import fetch_exchange_names, is_st_name

    names = fetch_exchange_names(config=config)
    if not names:
        return AuthorityCheckOutcome("unavailable", [])

    # Both directions are judged only on symbols both sides carry. Restricting
    # one side is not enough: SSE designates two names as ST that no quote feed
    # still lists, which would otherwise register as a permanent shortfall and
    # burn most of the tolerance before any real disagreement appeared.
    shared = set(names) & covered
    by_exchange = {sym for sym in shared if is_st_name(names[sym])}
    labeled_shared = labeled & shared

    missing = sorted(by_exchange - labeled_shared)
    extra = sorted(labeled_shared - by_exchange)
    total = len(missing) + len(extra)
    if total <= ST_MAX_DISAGREEMENT:
        return AuthorityCheckOutcome("agreed", [])

    return AuthorityCheckOutcome(
        "disagreed",
        [
            {
                "dataset": "trading_status",
                "severity": "error",
                "check": "st_labels_vs_exchange",
                "message": (
                    f"ST labels disagree with the exchange listings on {trade_date.isoformat()}: "
                    f"{len(missing)} designated ST by the exchange but not labeled, "
                    f"{len(extra)} labeled but not designated"
                ),
                "trade_date": trade_date.isoformat(),
                "shared_universe": len(shared),
                "designated_not_labeled": len(missing),
                "labeled_not_designated": len(extra),
                "symbols": (missing + extra)[:_SAMPLE],
            }
        ],
    )


def st_labels_vs_exchange(config: Config, trade_date: date) -> list[dict]:
    """Return only disagreement findings for backwards-compatible callers."""
    return _st_labels_vs_exchange_outcome(config, trade_date).findings


def run_authority_checks(config: Config, trade_date: date) -> list[dict]:
    """Run every publisher comparison and persist the result.

    Findings go back to the caller for the ordinary audit stream; the same
    comparison is also written to ``meta/quality/source_diffs/`` so that a clean
    run leaves evidence it happened. The persisted status distinguishes an
    effective comparison from a disabled, unavailable, or not-yet-covered check.
    """
    findings: list[dict] = []
    checks = {
        "macro_pmi_vs_nbs": _macro_pmi_vs_nbs_outcome,
        "st_labels_vs_exchange": _st_labels_vs_exchange_outcome,
    }
    ran: dict[str, str] = {}
    for name, fn in checks.items():
        try:
            result = fn(config, trade_date)
        except Exception as exc:
            # A publisher's site being down must not fail a data run.
            logger.warning("authority check %s failed: %s", name, exc)
            ran[name] = f"error: {exc}"
            continue
        ran[name] = result.status
        findings.extend(result.findings)

    out_dir = config.meta_root / "quality" / "source_diffs"
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "trade_date": trade_date.isoformat(),
            "kind": "authority_crosscheck",
            "checks": ran,
            "findings": findings,
        }
        path = out_dir / f"authority-{trade_date.isoformat()}.json"
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
    except OSError as exc:
        logger.warning("could not persist authority cross-check: %s", exc)
    return findings
