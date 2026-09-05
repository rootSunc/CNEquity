"""News wire + economic calendar archive steps (daily batch)."""

from __future__ import annotations

from datetime import date, timedelta

from cnequity.adapters.eastmoney.economic_calendar import fetch_economic_calendar
from cnequity.adapters.eastmoney.news_wire import fetch_flash_news_wire
from cnequity.config import Config
from cnequity.orchestrator.registry import register_step
from cnequity.steps.common import SnapshotBackfillError
from cnequity.steps.http_common import (
    call_with_run_id,
    empty_ok,
    run_incremental_fetched,
    verify_raw_archive,
    write_fetched,
)


@register_step("flash_news_wire", group="news_wire")
def step_flash_news_wire(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("flash_news_wire: eastmoney source disabled in config")
    # Fail-loud on empty: an empty success left the dataset unregistered in curated
    # and permanently failed lake_health (exists error) while the step looked green.

    def _fetch(d: date):
        return call_with_run_id(
            fetch_flash_news_wire,
            d,
            pipeline_config=config,
            dataset="flash_news_wire",
            run_id=run_id,
            config=config,
        )

    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "flash_news_wire",
        _fetch,
        source="eastmoney",
        allow_empty=False,
        date_col="publish_date",
        raw_archive_evidence_factory=lambda: verify_raw_archive(
            config,
            "flash_news_wire",
            run_id,
            source="eastmoney",
            request_scope=f"snapshot:{trade_date.isoformat()}",
        ),
    )


@register_step("economic_calendar", group="macro_risk")
def step_economic_calendar(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("economic_calendar: eastmoney source disabled in config")
    # This is a rolling live window, not historical by-date data. It cannot
    # be routed through the daily helper because its event_date intentionally
    # contains future dates; reject backfill explicitly before fetching.
    if getattr(config, "_backfill", False):
        raise SnapshotBackfillError(
            "economic_calendar: backfill not supported — fetch semantics are snapshot "
            "(rolling live window; historical values unavailable)"
        )
    df = call_with_run_id(
        fetch_economic_calendar,
        trade_date,
        pipeline_config=config,
        dataset="economic_calendar",
        run_id=run_id,
        config=config,
    )
    empty_ok(df, "economic_calendar", trade_date)
    window_start = trade_date - timedelta(days=2)
    window_end = trade_date + timedelta(days=14)
    return write_fetched(
        config,
        run_id,
        "economic_calendar",
        df,
        source="eastmoney",
        raw_archive_evidence=(
            verify_raw_archive(
                config,
                "economic_calendar",
                run_id,
                source="eastmoney",
                request_scope=f"rolling:{window_start.isoformat()}:{window_end.isoformat()}",
            )
            if config.should_archive_raw("economic_calendar")
            else None
        ),
        snapshot_date=trade_date,
    )
