"""Fill missing status keys from actual prints, without claiming historical ST.

ST does not apply to funds, so a fund's derived row says ``risk_warning=False``
rather than unknown; a stock's stays null until first-hand evidence exists.
"""

from __future__ import annotations

from contextlib import nullcontext
from datetime import date, datetime, timezone
from uuid import uuid4

import polars as pl

from cnequity.config import Config
from cnequity.domain.schemas import with_provenance
from cnequity.domain.trading_status import DERIVED_TRADED_SOURCE, STATUS_NORMAL
from cnequity.quality.trading_status_coverage import KEY, traded_status_evidence
from cnequity.storage.lake_write import planned_on
from cnequity.storage.read_context import ReadContext
from cnequity.storage.repairs.publish import publish_staged
from cnequity.storage.revisions import RevisionStore


def repair_status_traded(config: Config, *, start: date, end: date, apply: bool = False) -> dict:
    """Preview without writes, or publish additive rows under the mutation lock.

    Apply recomputes the candidates against the then-current inputs. Existing
    status keys, including contradictions, are never replaced. Old revisions
    stay readable. An ingestion still writing staging cannot change these
    committed inputs while this function holds the publication lock.
    """
    if end < start:
        raise ValueError("end must not be before start")
    with planned_on(config) if apply else nullcontext():
        store = RevisionStore(
            config.meta_root, config.curated_root, config.derived_root, create=apply
        )
        if apply:
            for dataset in ("daily_bars", "instruments", "trading_status"):
                store.ensure_current(dataset)
        context = ReadContext.capture(config, ("daily_bars", "instruments", "trading_status"), {})
        evidence = traded_status_evidence(config, start=start, end=end, read_context=context)
        missing = evidence.filter(pl.col("exclusion").is_null() & ~pl.col("has_status"))
        pairs = missing.select(*KEY, "asset_class").sort(KEY).collect()
        # Fund rows this repair wrote before funds were marked not-ST.
        funds = (
            evidence.filter(
                pl.col("exclusion").is_null()
                & pl.col("derived_traded")
                & ~pl.col("risk_warning_known")
                & (pl.col("asset_class") == "etf")
            )
            .select(*KEY, "asset_class")
            .sort(KEY)
            .collect()
        )
        excluded = evidence.filter(pl.col("exclusion").is_not_null()).group_by("exclusion")
        exclusions = dict(excluded.len().sort("exclusion").collect().iter_rows())
        report = {
            "applied": False,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "rows": pairs.height,
            "symbols": pairs["symbol"].n_unique(),
            "source": DERIVED_TRADED_SOURCE,
            "risk_warning": {"etf": False, "other": None},
            "fund_rows_marked_not_st": funds.height,
            "by_class": dict(pairs.group_by("asset_class").len().sort("asset_class").iter_rows()),
            "by_date": {
                str(day): count
                for day, count in pairs.group_by("trade_date").len().sort("trade_date").iter_rows()
            },
            "exclusions": exclusions,
            "sample": pairs.head(20).with_columns(pl.col("trade_date").cast(pl.String)).to_dicts(),
            "input_roots": {name: str(root) for name, root in context.roots.items()},
        }
        if not apply or (pairs.is_empty() and funds.is_empty()):
            return report
        rows = with_provenance(
            pl.concat([pairs, funds]).select(
                *KEY,
                pl.lit(True).alias("is_trading"),
                pl.lit(STATUS_NORMAL).alias("status"),
                pl.when(pl.col("asset_class") == "etf")
                .then(pl.lit(False))
                .otherwise(pl.lit(None, dtype=pl.Boolean))
                .alias("risk_warning"),
            ),
            source=DERIVED_TRADED_SOURCE,
            data_version="v1",
        )
        run_id = f"status-traded-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
        published = publish_staged(
            config,
            store,
            "trading_status",
            rows,
            run_id=run_id,
            base_root=context.roots["trading_status"],
            metadata={
                "reason": "status_traded",
                "rows_added": pairs.height,
                "fund_rows_marked_not_st": funds.height,
                "input_roots": report["input_roots"],
            },
        )
        if published is None:
            raise RuntimeError("status_traded produced no revision")
        report.update(
            applied=True,
            revision=published["revision"],
            publication_audit=published["publication_audit"],
        )
        return report
