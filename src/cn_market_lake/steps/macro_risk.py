"""L6 macro + L8 risk batch steps."""

from __future__ import annotations

from datetime import date

from cn_market_lake.adapters.cninfo.regulatory import fetch_regulatory_events
from cn_market_lake.adapters.eastmoney.share_unlock import fetch_share_unlock_schedule
from cn_market_lake.adapters.macro.indicators import fetch_macro_indicators
from cn_market_lake.config import Config
from cn_market_lake.derive.market_breadth import compute_market_breadth
from cn_market_lake.orchestrator.registry import register_step
from cn_market_lake.quality.macro_checks import macro_revision_findings
from cn_market_lake.steps.common import BACKFILL_START
from cn_market_lake.steps.http_common import run_incremental_fetched


@register_step("macro_indicators", group="macro_risk")
def step_macro_indicators(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    # Revisions have to be detected here, between fetch and write: compact keeps
    # only the newest row per (indicator_id, obs_date), so once the write lands
    # the previous published value is gone. The overwrite itself is deliberate —
    # it is what lets a corrected history heal on the next run without a
    # migration (issue #3) — so this records the change rather than blocking it.
    revisions: list[dict] = []

    def _fetch(day: date):
        df = fetch_macro_indicators(day, config=config)
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
        allow_empty=True,
    )
    if revisions:
        updates = result.setdefault("context_updates", {})
        updates["audit_findings"] = [*(updates.get("audit_findings") or []), *revisions]
    return result


@register_step("market_breadth", group="macro_risk", depends_on=["daily_bars"])
def step_market_breadth(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        from cn_market_lake.steps.common import walk_day_backfill

        # Pure local computation from daily_bars — no network, no rate limit,
        # so the floor is daily_bars' own start rather than a probed vendor date.
        return walk_day_backfill(
            config,
            trade_date,
            run_id,
            "market_breadth",
            lambda d: compute_market_breadth(config, d),
            source="derived",
            floor=date(2001, 1, 1),
        )
    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "market_breadth",
        lambda d: compute_market_breadth(config, d),
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
        fetch_share_unlock_schedule,
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

    from cn_market_lake.domain.schemas import data_version_for, with_provenance
    from cn_market_lake.storage import StagingWriter

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
    for cursor in cursors:
        config.rate_limit("eastmoney")
        df = fetch_share_unlock_schedule(
            cursor,
            horizon_days=_UNLOCK_HORIZON_DAYS,
            max_retries=_UNLOCK_SWEEP_RETRIES,
            retry_backoff_seconds=_UNLOCK_SWEEP_BACKOFF_SECONDS,
        )
        if not df.is_empty():
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
        from cn_market_lake.steps.common import walk_day_backfill

        return walk_day_backfill(
            config,
            trade_date,
            run_id,
            "regulatory_events",
            lambda d: fetch_regulatory_events(d, config=config),
            source="cninfo",
            date_col="event_date",
            floor=date(2010, 1, 1),
        )
    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "regulatory_events",
        lambda d: fetch_regulatory_events(d, config=config),
        source="cninfo",
        allow_empty=True,
    )
