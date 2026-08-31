"""L6 macro + L8 risk batch steps."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.cninfo.regulatory import (
    fetch_regulatory_events,
    fetch_regulatory_events_range,
)
from cnequity.adapters.eastmoney.share_unlock import fetch_share_unlock_schedule
from cnequity.adapters.macro.indicators import fetch_macro_indicators
from cnequity.config import Config
from cnequity.derive.market_breadth import MARKET_BREADTH_METRICS, compute_market_breadth
from cnequity.orchestrator.registry import register_step
from cnequity.quality.macro_checks import macro_revision_findings
from cnequity.steps.common import BACKFILL_START
from cnequity.steps.http_common import run_incremental_fetched, verify_raw_archive

_REQUIRED_DAILY_MACRO_INDICATORS = frozenset({"cnbond_yield_10y", "shibor_3m"})
_MARKET_BREADTH_METRICS = frozenset(MARKET_BREADTH_METRICS)


def _existing_market_breadth_dates(config: Config, days: list[date]) -> set[date]:
    """Only skip dates with a complete seven-metric breadth observation."""
    if not days:
        return set()
    from cnequity.query.parquet_scan import collect_parquet_root

    root = config.curated_root / "market_breadth"
    if not root.exists():
        return set()
    try:
        frame = collect_parquet_root(
            root,
            partition_col="trade_date",
            start=min(days),
            end=max(days),
        )
    except (FileNotFoundError, OSError, pl.exceptions.PolarsError, ValueError):
        # A damaged existing partition is not complete evidence; let the
        # backfill retry it and let compact/audit surface the damaged file.
        return set()
    required = {"trade_date", "metric_id", "value"}
    if not required.issubset(frame.columns):
        return set()
    valid_metric = (
        pl.col("metric_id").is_in(sorted(_MARKET_BREADTH_METRICS)) & pl.col("value").is_not_null()
    )
    complete = (
        frame.group_by("trade_date")
        .agg(
            pl.col("metric_id").filter(valid_metric).n_unique().alias("metric_count"),
            pl.len().alias("row_count"),
        )
        .filter(
            (pl.col("metric_count") == len(_MARKET_BREADTH_METRICS))
            & (pl.col("row_count") == len(_MARKET_BREADTH_METRICS))
        )
    )
    if complete.is_empty():
        return set()
    wide = (
        frame.filter(valid_metric)
        .group_by("trade_date")
        .agg(
            [
                pl.col("value").filter(pl.col("metric_id") == metric).first().alias(metric)
                for metric in MARKET_BREADTH_METRICS
            ]
        )
    )
    valid = wide.filter(
        (pl.col("total_count") > 0)
        & (pl.col("advance_count") >= 0)
        & (pl.col("decline_count") >= 0)
        & (pl.col("flat_count") >= 0)
        & (pl.col("limit_up_count") >= 0)
        & (pl.col("limit_down_count") >= 0)
        & (pl.col("advance_ratio") >= 0)
        & (pl.col("advance_ratio") <= 1)
        & (
            pl.col("advance_count") + pl.col("decline_count") + pl.col("flat_count")
            == pl.col("total_count")
        )
        & (pl.col("limit_up_count") <= pl.col("advance_count"))
        & (pl.col("limit_down_count") <= pl.col("decline_count"))
        & (
            (pl.col("advance_ratio") - pl.col("advance_count") / pl.col("total_count")).abs()
            <= 1e-6
        )
    )
    return set(complete.join(valid.select("trade_date"), on="trade_date")["trade_date"])


def _missing_daily_macro_indicators(df, trade_date: date) -> list[str]:
    """Return daily rate series absent for one requested session.

    ``fetch_macro_indicators`` also returns the whole monthly history, so a
    non-empty frame is not evidence that each daily feed answered. Keep this
    check at the step boundary where the requested date is known.
    """
    if df.is_empty() or not {"indicator_id", "obs_date"}.issubset(df.columns):
        return sorted(_REQUIRED_DAILY_MACRO_INDICATORS)
    observed = {
        indicator
        for indicator, obs_date in df.select(["indicator_id", "obs_date"]).iter_rows()
        if obs_date == trade_date
    }
    return sorted(_REQUIRED_DAILY_MACRO_INDICATORS - observed)


def _validate_market_breadth_snapshot(df: pl.DataFrame) -> pl.DataFrame:
    """Reject partial or duplicated derived metric sets before staging."""
    if df.is_empty():
        return df
    required = {"trade_date", "metric_id", "value"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(
            "market_breadth: derived snapshot is missing required column(s): " + ", ".join(missing)
        )
    if df["trade_date"].n_unique() != 1:
        raise RuntimeError("market_breadth: derived snapshot must contain exactly one trade_date")
    valid = df.filter(
        pl.col("metric_id").is_in(sorted(_MARKET_BREADTH_METRICS)) & pl.col("value").is_not_null()
    )
    if df.height != len(_MARKET_BREADTH_METRICS) or (
        valid.height != len(_MARKET_BREADTH_METRICS)
        or valid.get_column("metric_id").n_unique() != len(_MARKET_BREADTH_METRICS)
    ):
        raise RuntimeError(
            "market_breadth: incomplete derived snapshot; expected exactly "
            f"{len(_MARKET_BREADTH_METRICS)} unique non-null metrics, got {df.height} row(s)"
        )
    values = {
        row["metric_id"]: row["value"]
        for row in valid.select("metric_id", "value").iter_rows(named=True)
    }
    total = values["total_count"]
    counts = [values[name] for name in ("advance_count", "decline_count", "flat_count")]
    if (
        total is None
        or total <= 0
        or any(
            value < 0 for value in [*counts, values["limit_up_count"], values["limit_down_count"]]
        )
        or not 0 <= values["advance_ratio"] <= 1
        or sum(counts) != total
        or values["limit_up_count"] > values["advance_count"]
        or values["limit_down_count"] > values["decline_count"]
        or abs(values["advance_ratio"] - values["advance_count"] / total) > 1e-6
    ):
        raise RuntimeError("market_breadth: derived snapshot violates count/ratio invariants")
    return df


@register_step("macro_indicators", group="macro_risk")
def step_macro_indicators(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("macro_indicators: eastmoney source disabled in config")
    # Revisions have to be detected here, between fetch and write: compact keeps
    # only the newest row per (indicator_id, obs_date), so once the write lands
    # the previous published value is gone. The overwrite itself is deliberate —
    # it is what lets a corrected history heal on the next run without a
    # migration (issue #3) — so this records the change rather than blocking it.
    revisions: list[dict] = []
    daily_gaps: dict[date, list[str]] = {}

    def _fetch(day: date):
        df = fetch_macro_indicators(day, config=config)
        missing = _missing_daily_macro_indicators(df, day)
        if missing:
            daily_gaps[day] = missing
        revisions.extend(macro_revision_findings(config, df, day))
        return df

    result = run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "macro_indicators",
        _fetch,
        # The adapter stamps `source` per row (EastMoney and the PBOC both feed
        # this dataset), and with_provenance keeps a pre-set column. This value
        # only applies to the empty-frame case.
        source="eastmoney",
        allow_empty=False,
    )
    if revisions:
        updates = result.setdefault("context_updates", {})
        updates["audit_findings"] = [*(updates.get("audit_findings") or []), *revisions]
    if daily_gaps:
        samples = [
            f"{day.isoformat()}: {', '.join(indicators)}"
            for day, indicators in sorted(daily_gaps.items())[:5]
        ]
        updates = result.setdefault("context_updates", {})
        updates["audit_findings"] = [
            *(updates.get("audit_findings") or []),
            {
                "dataset": "macro_indicators",
                "severity": "warning",
                "check": "daily_series_gap",
                "message": (
                    f"{len(daily_gaps)} requested session(s) are missing daily macro "
                    f"series ({'; '.join(samples)})"
                ),
                "missing_dates": {
                    day.isoformat(): indicators for day, indicators in sorted(daily_gaps.items())
                },
            },
        ]
        result["status"] = "warning"
    return result


@register_step("market_breadth", group="macro_risk", depends_on=["daily_bars"])
def step_market_breadth(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        from cnequity.steps.common import walk_day_backfill

        # Pure local computation from daily_bars — no network, no rate limit,
        # so the floor is daily_bars' own start rather than a probed vendor date.
        return walk_day_backfill(
            config,
            trade_date,
            run_id,
            "market_breadth",
            lambda d: _validate_market_breadth_snapshot(compute_market_breadth(config, d)),
            source="derived",
            floor=date(2001, 1, 1),
            existing_dates_fn=lambda days: _existing_market_breadth_dates(config, days),
        )
    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "market_breadth",
        lambda d: _validate_market_breadth_snapshot(compute_market_breadth(config, d)),
        source="derived",
        allow_empty=True,
    )


@register_step("share_unlock_schedule", group="macro_risk", depends_on=["instruments"])
def step_share_unlock_schedule(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("share_unlock_schedule: eastmoney source disabled in config")
    if getattr(config, "_backfill", False):
        return _backfill_share_unlock_schedule(config, trade_date, run_id)
    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "share_unlock_schedule",
        lambda d: fetch_share_unlock_schedule(d, config=config),
        source="eastmoney",
        allow_empty=True,
    )


_UNLOCK_HORIZON_DAYS = 180
# Under the horizon so consecutive windows overlap — a 180-day stride would
# leave a one-day crack an unlock could fall through if a period boundary
# landed exactly wrong; 150 leaves 30 days of slack on both sides.
_UNLOCK_STRIDE_DAYS = 150

# Each stride re-walks a 63-page market-wide report from page 1. Measured:
# three backfill runs each failed once, on three different pages (8, 27, 28) —
# a transient-load pattern, not one broken page — and the default 3/5s budget
# (sized for the single-page daily call) wasn't enough. 5/15s mirrors the same
# fix already proven on the F10 shareholder sweeps for the same failure shape.
_UNLOCK_SWEEP_RETRIES = 5
_UNLOCK_SWEEP_BACKOFF_SECONDS = 15.0


def _backfill_share_unlock_schedule(config: Config, trade_date: date, run_id: str) -> dict:
    """Walk in ~150-day strides, not daily.

    Each call returns every unlock in the next 180 days from *its* date — PK is
    (symbol, unlock_date), no snapshot/as-of column at all, so it is not a
    per-day PIT series to replay. A daily walk would re-fetch the same event
    up to ~180 times before it aged out of the window; striding under the
    horizon covers the same ground once, with 30 days of overlap as a margin
    against an unlock landing exactly on a stride boundary.

    Flushes after every stride rather than once at the end — measured in
    production: one stride's page 28 hit an unretried EastMoney timeout 38
    minutes into a ~40-stride sweep, and because nothing had been written yet,
    all 38 minutes of prior strides were lost with it.

    Walks newest-to-oldest, not oldest-to-newest. RPT_LIFT_STAGE is sorted
    FREE_DATE descending and each stride pages until it reaches rows past its
    own window — so a stride targeting 2026 reads the ~7 pages the module
    docstring describes, but a stride targeting 2010 has to page through
    nearly the whole 63-page report to get there. The old oldest-first order
    ran the most expensive, most failure-prone strides first: four
    consecutive attempts each died on an early stride, on four different
    pages (8, 27, 28, 33), losing the cheap, high-value recent strides to a
    failure in the expensive tail every time. Newest-first means a failure
    anywhere still leaves the most-likely-to-be-queried years landed.
    """
    from datetime import timedelta

    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.storage import StagingWriter

    start = getattr(config, "_backfill_start", None) or BACKFILL_START
    end = getattr(config, "_backfill_end", None) or trade_date
    cursors = []
    cursor = start
    while cursor <= end:
        cursors.append(cursor)
        cursor += timedelta(days=_UNLOCK_STRIDE_DAYS)
    cursors.reverse()

    writer = StagingWriter(config.staging_root)
    rows_written = 0
    n_parts = 0
    seen_unlocks: set[tuple[object, object]] = set()
    for cursor in cursors:
        horizon_days = min(_UNLOCK_HORIZON_DAYS, (end - cursor).days)
        df = fetch_share_unlock_schedule(
            cursor,
            horizon_days=horizon_days,
            config=config,
            max_retries=_UNLOCK_SWEEP_RETRIES,
            retry_backoff_seconds=_UNLOCK_SWEEP_BACKOFF_SECONDS,
        )
        if not df.is_empty():
            # Adjacent strides intentionally overlap by 30 days. Keep the
            # first (newest-stride) copy so the overlap does not create
            # duplicate staged rows or make compaction do avoidable work.
            keep_indices: list[int] = []
            for index, (symbol, unlock_date) in enumerate(
                df.select(["symbol", "unlock_date"]).iter_rows()
            ):
                key = (symbol, unlock_date)
                if key in seen_unlocks:
                    continue
                seen_unlocks.add(key)
                keep_indices.append(index)
            if not keep_indices:
                continue
            df = df[keep_indices]
            part = with_provenance(
                df, source="eastmoney", data_version=data_version_for("share_unlock_schedule")
            )
            writer.write_batch("share_unlock_schedule", run_id, f"bf-{n_parts:04d}", part)
            n_parts += 1
            rows_written += part.height
    return {"rows_read": rows_written, "rows_written": rows_written}


@register_step("regulatory_events", group="macro_risk", depends_on=["instruments"])
def step_regulatory_events(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not config.sources.get("cninfo", True):
        raise RuntimeError("regulatory_events: cninfo source disabled in config")
    if getattr(config, "_backfill", False):
        from cnequity.steps.events import _cninfo_range_backfill

        return _cninfo_range_backfill(
            config,
            trade_date,
            run_id,
            "regulatory_events",
            fetch_regulatory_events_range,
            date_col="event_date",
            floor=date(2010, 1, 1),
            batch_id=context.get("_batch_id"),
        )
    from cnequity.steps.events import _fetch_cninfo_single, _record_cninfo_metrics

    metrics: dict = {"run_id": run_id}
    result = run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "regulatory_events",
        lambda d: _fetch_cninfo_single(
            fetch_regulatory_events,
            d,
            config,
            metrics,
            dataset="regulatory_events",
        ),
        source="cninfo",
        allow_empty=True,
        date_col="event_date",
        raw_archive_evidence_factory=lambda: verify_raw_archive(
            config,
            "regulatory_events",
            run_id,
            source="cninfo",
            request_scope=(f"range:regulatory:{trade_date.isoformat()}:{trade_date.isoformat()}"),
        ),
    )
    if len(metrics) > 1:
        _record_cninfo_metrics(
            config,
            run_id,
            "regulatory_events",
            metrics,
            batch_id=context.get("_batch_id"),
        )
    result["metrics"] = metrics
    return result
