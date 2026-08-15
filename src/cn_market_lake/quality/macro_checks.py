"""macro_indicators freshness and revision checks (issue #10).

Two failure modes that curated data alone does not surface.

**Staleness.** Every monthly series is fetched in full on every run and deduped
on ``(indicator_id, obs_date)``, so a feed that quietly stops publishing looks
exactly like a feed that is up to date — the old rows are still there and no
step fails. Only the distance between the newest observation and the run date
gives it away. This is not hypothetical: 社融 used to be read from MOFCOM, whose
copy ran two release cycles behind the PBOC original and still carried a
superseded 2026-04 value. Nothing in the lake showed it until the lag was
measured — which is why the series is now read from the PBOC directly and why
this check exists.

**Revision.** Macro series get restated. Compact keeps the newest ``fetched_at``
per key, so a restatement silently replaces the earlier value with no record
that it ever differed. That overwrite is *wanted* — it is what let the bad
``m2_yoy`` history heal itself without a migration script (issue #3) — so the
answer is not to block it but to leave a trace. This module diffs incoming rows
against curated before the write and reports what changed; curated still ends up
holding the latest published value.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.query.parquet_scan import dataset_has_parquet, scan_parquet_root

_SAMPLE = 8

# Publication lag past which a monthly series is called stale, per indicator.
# These are the *observed* lags plus roughly one release cycle of headroom, not
# aspirations: PMI lands on the last day of its own month, while M2 and 社融 are
# both PBOC releases landing mid-following-month.
MONTHLY_STALE_DAYS: dict[str, int] = {
    "pmi_manufacturing": 45,
    "m2_yoy": 75,
    "social_financing": 75,
    "lpr_1y": 45,
}

# A restatement past this is worth a look rather than routine revision noise.
REVISION_MATERIAL_RELATIVE = 0.05


def _latest_obs(config: Config, trade_date: date) -> dict[str, date]:
    root = config.curated_root / "macro_indicators"
    if not dataset_has_parquet(root):
        return {}
    out = (
        scan_parquet_root(root, partition_col="obs_date", end=trade_date)
        .group_by("indicator_id")
        .agg(pl.col("obs_date").max().alias("latest"))
        .collect()
    )
    return {r["indicator_id"]: r["latest"] for r in out.iter_rows(named=True)}


def macro_staleness_findings(config: Config, trade_date: date) -> list[dict]:
    """Flag monthly indicators whose newest observation is too far behind."""
    findings: list[dict] = []
    for indicator_id, latest in sorted(_latest_obs(config, trade_date).items()):
        limit = MONTHLY_STALE_DAYS.get(indicator_id)
        if limit is None:
            # Daily series (bond yield, SHIBOR) are covered by the ordinary
            # freshness/watermark checks; only opt-in monthly ones land here.
            continue
        lag = (trade_date - latest).days
        if lag <= limit:
            continue
        findings.append(
            {
                "dataset": "macro_indicators",
                "severity": "warning",
                "check": "macro_indicator_stale",
                "message": (
                    f"{indicator_id} newest observation is {latest.isoformat()}, "
                    f"{lag} days before {trade_date.isoformat()} (limit {limit}) — "
                    "the publisher stopped, or the upstream shape changed and rows "
                    "are being dropped"
                ),
                "indicator_id": indicator_id,
                "latest_obs_date": latest.isoformat(),
                "lag_days": lag,
                "limit_days": limit,
            }
        )
    return findings


def macro_revision_findings(
    config: Config,
    incoming: pl.DataFrame,
    trade_date: date,
) -> list[dict]:
    """Values that changed for an ``(indicator_id, obs_date)`` already in curated.

    Called before the write, since compact keeps only the newest row per key and
    the previous value is unrecoverable afterwards. Returns findings only —
    the write proceeds either way.
    """
    if incoming.is_empty() or "indicator_id" not in incoming.columns:
        return []
    root = config.curated_root / "macro_indicators"
    if not dataset_has_parquet(root):
        return []

    existing = (
        scan_parquet_root(root, partition_col="obs_date", end=trade_date)
        .select("indicator_id", "obs_date", "value")
        .collect()
    )
    if existing.is_empty():
        return []

    # Latest stored value per key — curated may still hold pre-compact duplicates.
    existing = existing.unique(subset=["indicator_id", "obs_date"], keep="last")
    joined = incoming.select("indicator_id", "obs_date", "value").join(
        existing,
        on=["indicator_id", "obs_date"],
        how="inner",
        suffix="_old",
    )
    if joined.is_empty():
        return []

    revised = [
        row
        for row in joined.iter_rows(named=True)
        if row["value"] is not None
        and row["value_old"] is not None
        and row["value"] != row["value_old"]
    ]
    if not revised:
        return []

    def _relative(row: dict) -> float:
        old = row["value_old"]
        return abs(row["value"] - old) / abs(old) if old else float("inf")

    material = [r for r in revised if _relative(r) > REVISION_MATERIAL_RELATIVE]
    sample = "; ".join(
        f"{r['indicator_id']}@{r['obs_date']} {r['value_old']} → {r['value']}"
        for r in sorted(revised, key=_relative, reverse=True)[:_SAMPLE]
    )
    return [
        {
            "dataset": "macro_indicators",
            "severity": "warning" if material else "info",
            "check": "macro_value_revised",
            "message": (
                f"{len(revised)} published value(s) changed since the last fetch "
                f"({len(material)} by more than "
                f"{REVISION_MATERIAL_RELATIVE:.0%}): {sample}. "
                "curated keeps the newest value; this is the only record that the "
                "earlier one existed"
            ),
            "trade_date": trade_date.isoformat(),
            "revised": len(revised),
            "material": len(material),
            "indicators": sorted({r["indicator_id"] for r in revised}),
        }
    ]
