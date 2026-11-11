"""Compare curated against the bodies that actually publish the numbers.

The other checks in this package all reason about the lake's internal
consistency: does a value look stale, did it change, do two feeds we already
hold agree. None of them can see the failure where a vendor publishes on time,
in the right shape, with a wrong number — which is precisely the failure
issue #3 turned up (``m2_yoy`` carried M0 month-over-month growth for its whole
history, on schedule, in the right column type).

Catching that needs an outside reading, so these three reach the publisher:

* ``macro_pmi_vs_nbs`` — 制造业 PMI against the NBS release
* ``st_labels_vs_exchange`` — ST designations against the SSE / SZSE listings
* ``daily_bars_vs_exchange`` — curated OHLC and turnover against the closes the
  SSE and SZSE publish themselves. Everything else that arbitrates prices does
  it by comparing two vendors against each other, which cannot distinguish
  "both right" from "both wrong the same way".

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

import logging
from dataclasses import dataclass
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_lazy_by_primary_key
from cnequity.domain.trading_status import STATUS_DELISTED, normalize_legacy
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.atomic import write_json_atomic

logger = logging.getLogger(__name__)

_SAMPLE = 8

# PMI is published to one decimal, so any real disagreement is at least 0.1.
# The tolerance only absorbs float representation.
PMI_TOLERANCE = 0.05

# Naming and board membership move on the same day but are captured by different
# steps, so a couple of names either side of that boundary is routine.
ST_MAX_DISAGREEMENT = 3

# Which `daily_bars` fields are judged on which terms. Measured against the
# publishers on 2026-08-28: prices matched to 0 bps across 5,212 symbols, while
# turnover carried a one-directional definitional gap on 305 SZ names. The two
# groups therefore get different tolerances and different reporting.
_BAR_PRICE_FIELDS = ("open", "high", "low", "close")
_BAR_TURNOVER_FIELDS = ("volume", "amount")


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
        dedupe_lazy_by_primary_key(
            scan_parquet_root(root, partition_col="obs_date", start=obs_date, end=obs_date),
            "macro_indicators",
        )
        .filter(pl.col("indicator_id") == indicator_id)
        .select("value")
        .collect()
    )
    return None if out.is_empty() else float(out.get_column("value")[-1])


def _macro_pmi_vs_nbs_outcome(config: Config, trade_date: date) -> AuthorityCheckOutcome:
    """Newest curated ``pmi_manufacturing`` against the NBS release for that month."""
    # Default off, like the sina close cross-check: absent config means an
    # offline lake, and `cne audit` must not silently start making requests.
    if not config.sources.get("nbs", False):
        return AuthorityCheckOutcome("skipped_disabled", [])
    from cnequity.adapters.nbs.pmi_release import fetch_latest_pmi

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
                    f"{published['value']} in the 国家统计局 release — the vendor has drifted "  # i18n-ignore: Chinese term in an English message
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
        dedupe_lazy_by_primary_key(
            scan_parquet_root(
                root,
                partition_col="trade_date",
                start=trade_date,
                end=trade_date,
            ),
            "trading_status",
        )
        .filter(pl.col("trade_date") == pl.lit(trade_date))
        .collect()
    )
    if out.is_empty():
        return None
    # A partition may predate the status/risk_warning split.
    out = normalize_legacy(out).select("symbol", "status", "risk_warning").unique()
    if out.is_empty():
        return None
    # A delisted security is compared with the exchange only while the exchange
    # still lists it, which it does until formal deregistration. Comparing a
    # name the lake knows is gone against a listing that has not caught up
    # produces a disagreement about nothing.
    out = out.filter(pl.col("status") != STATUS_DELISTED)
    if out.is_empty():
        return None
    covered = set(out.get_column("symbol").to_list())
    labeled = set(out.filter(pl.col("risk_warning")).get_column("symbol").to_list())
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

    from cnequity.adapters.exchange.st_lists import (
        fetch_exchange_names_with_status,
        is_st_name,
    )

    exchange_result = fetch_exchange_names_with_status(config=config)
    if exchange_result.failures:
        # A comparison over one exchange cannot establish that the other
        # exchange agrees. Persist the status, but do not turn a publisher
        # outage into a data disagreement finding.
        if not exchange_result.names:
            return AuthorityCheckOutcome("unavailable", [])
        return AuthorityCheckOutcome("skipped_partial", [])
    names = exchange_result.names
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


def _curated_daily_bars(config: Config, trade_date: date) -> pl.DataFrame:
    """Curated bars for one session, one row per symbol."""
    root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(root):
        return pl.DataFrame()
    out = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(
                root,
                partition_col="trade_date",
                start=trade_date,
                end=trade_date,
            ),
            "daily_bars",
        )
        .filter(pl.col("trade_date") == pl.lit(trade_date))
        .select("symbol", "trade_date", *_BAR_PRICE_FIELDS, *_BAR_TURNOVER_FIELDS)
        .collect()
    )
    return out


def _worst_drift(
    joined: pl.DataFrame, fields: tuple[str, ...], tolerance_bps: float
) -> pl.DataFrame:
    """Rows where any of *fields* exceeds *tolerance_bps*, worst first.

    Both sides are required to be positive: a zero on either side is a
    suspended or untraded security, where a relative difference has no meaning.
    """
    drift = pl.max_horizontal(
        [
            pl.when((pl.col(f) > 0) & (pl.col(f"{f}_official") > 0))
            .then((pl.col(f) - pl.col(f"{f}_official")).abs() / pl.col(f"{f}_official") * 10_000.0)
            .otherwise(0.0)
            for f in fields
        ]
    )
    return (
        joined.with_columns(drift.alias("_bps"))
        .filter(pl.col("_bps") > tolerance_bps)
        .sort("_bps", descending=True)
    )


def _daily_bars_vs_exchange_outcome(config: Config, trade_date: date) -> AuthorityCheckOutcome:
    """Curated `daily_bars` against the closes the exchanges themselves publish.

    This is the only check in the lake that compares a price against its
    publisher. `daily_bars` is otherwise arbitrated by TDX against EastMoney —
    two redistributors, whose agreement establishes that they do not differ,
    not that either is right.

    Prices and turnover are judged on different terms, because measurement
    against the publisher says they behave differently (2026-08-28, 5,212
    shared symbols):

    * **OHLC matched exactly** — every field, every symbol, 0 bps. So a price
      tolerance can be tight, and a breach is a real finding.
    * **Turnover did not, in one direction only.** 305 SZ symbols carried less
      curated volume than the exchange published, never more, across every SZ
      board. The exchange's daily total folds in trading a continuous-auction
      bar excludes, so this is a definitional gap and not a vendor error;
      reported per symbol it would be 300 findings a day forever. It is
      therefore summarised once, and only when it widens past the configured
      share of the compared universe.

    Comparison runs over the **shared** universe. Neither exchange publishes
    Beijing here, and curated additionally carries symbols the equity feeds drop,
    so judging over either side's full set would report a permanent shortfall.
    """
    if not config.sources.get("exchange", False):
        return AuthorityCheckOutcome("skipped_disabled", [])
    curated = _curated_daily_bars(config, trade_date)
    if curated.is_empty():
        return AuthorityCheckOutcome("skipped_no_curated", [])

    from cnequity.adapters.exchange.daily_quotes import fetch_exchange_daily_quotes

    official = fetch_exchange_daily_quotes(trade_date, config=config)
    if official.is_empty:
        return AuthorityCheckOutcome("unavailable", [])

    joined = curated.join(
        official.quotes, on=["symbol", "trade_date"], how="inner", suffix="_official"
    )
    if joined.is_empty():
        return AuthorityCheckOutcome("no_shared_universe", [])

    covered = "+".join(sorted(official.covered))
    findings: list[dict] = []

    # An exchange that published a *traded* close for a symbol curated has no
    # bar for is a hole in the lake, and only the publisher can prove it is one.
    # Zero-volume official rows are excluded: SZSE lists a suspended security
    # with its previous close repeated across OHLC, while a quote feed emits no
    # bar at all. Measured 2026-08-27, that alone accounted for every symbol on
    # this side (000016.SZ, 002274.SZ), so counting them would report the
    # convention as a defect every day.
    missing = official.quotes.filter(pl.col("volume") > 0).join(
        curated, on=["symbol", "trade_date"], how="anti"
    )
    if not missing.is_empty():
        symbols = sorted(missing.get_column("symbol").to_list())
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "error",
                "check": "daily_bars_missing_vs_exchange",
                "message": (
                    f"{len(symbols)} symbol(s) have an official {covered} close on "
                    f"{trade_date.isoformat()} but no curated bar"
                ),
                "trade_date": trade_date.isoformat(),
                "exchanges": covered,
                "missing_symbols": len(symbols),
                "symbols": symbols[:_SAMPLE],
            }
        )

    price_drift = _worst_drift(joined, _BAR_PRICE_FIELDS, config.exchange_audit_price_tolerance_bps)
    if not price_drift.is_empty():
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "error",
                "check": "daily_bars_vs_exchange",
                "message": (
                    f"{price_drift.height} of {joined.height} shared symbol(s) disagree with "
                    f"the official {covered} close on {trade_date.isoformat()} by more than "
                    f"{config.exchange_audit_price_tolerance_bps:.0f} bps "
                    f"(worst {price_drift.get_column('_bps').max():.0f} bps)"
                ),
                "trade_date": trade_date.isoformat(),
                "exchanges": covered,
                "compared_symbols": joined.height,
                "disagreeing_symbols": price_drift.height,
                "worst_bps": round(float(price_drift.get_column("_bps").max()), 2),
                "tolerance_bps": config.exchange_audit_price_tolerance_bps,
                "symbols": price_drift.get_column("symbol").to_list()[:_SAMPLE],
            }
        )

    turnover_drift = _worst_drift(
        joined, _BAR_TURNOVER_FIELDS, config.exchange_audit_turnover_tolerance_bps
    )
    divergent_fraction = turnover_drift.height / joined.height
    if divergent_fraction > config.exchange_audit_turnover_max_fraction:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_turnover_vs_exchange",
                "message": (
                    f"{turnover_drift.height} of {joined.height} shared symbol(s) "
                    f"({divergent_fraction:.1%}) diverge from official {covered} turnover by "
                    f"more than {config.exchange_audit_turnover_tolerance_bps:.0f} bps on "
                    f"{trade_date.isoformat()}; a continuous-auction bar legitimately excludes "
                    "trading the exchange total folds in, so treat a widening share as the "
                    "signal, not the level"
                ),
                "trade_date": trade_date.isoformat(),
                "exchanges": covered,
                "compared_symbols": joined.height,
                "divergent_symbols": turnover_drift.height,
                "divergent_fraction": round(divergent_fraction, 4),
                "tolerance_bps": config.exchange_audit_turnover_tolerance_bps,
                "symbols": turnover_drift.get_column("symbol").to_list()[:_SAMPLE],
            }
        )

    if findings:
        return AuthorityCheckOutcome("disagreed", findings)
    return AuthorityCheckOutcome("agreed_partial" if official.missing else "agreed", [])


def daily_bars_vs_exchange(config: Config, trade_date: date) -> list[dict]:
    """Return only disagreement findings for backwards-compatible callers."""
    return _daily_bars_vs_exchange_outcome(config, trade_date).findings


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
        "daily_bars_vs_exchange": _daily_bars_vs_exchange_outcome,
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
        write_json_atomic(path, payload, ensure_ascii=False, indent=2, default=str)
    except OSError as exc:
        logger.warning("could not persist authority cross-check: %s", exc)
    return findings
