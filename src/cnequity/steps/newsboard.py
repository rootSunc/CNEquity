"""News wire + economic calendar, events and holidays archive steps (daily batch)."""

from __future__ import annotations

from datetime import date, timedelta

from cnequity.adapters.jin10.economic_calendar import SOURCE as CALENDAR_SOURCE
from cnequity.adapters.jin10.economic_calendar import (
    fetch_economic_calendar,
    fetch_economic_calendar_window,
)
from cnequity.adapters.jin10.events import fetch_economic_events, fetch_market_holidays
from cnequity.config import Config
from cnequity.orchestrator.outcomes import SourceUnavailableError
from cnequity.orchestrator.registry import register_step
from cnequity.steps.common import SnapshotBackfillError
from cnequity.steps.http_common import (
    call_with_run_id,
    empty_ok,
    verify_raw_archive,
    write_fetched,
)


@register_step("flash_news_wire", group="research")
def step_flash_news_wire(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    # Same EastMoney fast-news list as news_headlines: one shared incremental
    # fetch per run (steps/news_feed.py). Fails on empty only while the lake
    # holds no flash news at all — an empty 15-minute window is normal.
    from cnequity.steps.news_feed import stage_news

    return stage_news(config, trade_date, run_id, "flash_news_wire")


@register_step("economic_calendar", group="macro_risk")
def step_economic_calendar(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not config.sources.get(CALENDAR_SOURCE, True):
        raise SourceUnavailableError("economic_calendar: jin10 source disabled in config")
    # The daily run is a rolling live window with future dates; a backfill
    # walks past days of the same endpoint instead.
    if getattr(config, "_backfill", False):
        return _backfill_economic_calendar(config, trade_date, run_id)
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
    df = _with_withdrawn(config, "economic_calendar", df, window_start, window_end)
    return write_fetched(
        config,
        run_id,
        "economic_calendar",
        df,
        source=CALENDAR_SOURCE,
        raw_archive_evidence=(
            verify_raw_archive(
                config,
                "economic_calendar",
                run_id,
                source=CALENDAR_SOURCE,
                request_scope=f"rolling:{window_start.isoformat()}:{window_end.isoformat()}",
            )
            if config.should_archive_raw("economic_calendar")
            else None
        ),
        snapshot_date=trade_date,
    )


def _with_withdrawn(config: Config, dataset: str, df, start: date, end: date):
    """Mark the window's stored events this capture no longer lists.

    Jin10 reissues a rescheduled release under a new data_id and drops the old
    one (Japan's October manufacturing PMI final: jin10:1178833 on 11-05, then
    jin10:1189668 on 11-02). The old row stays, flagged, so a reader can drop
    it without losing what was announced.
    """
    import polars as pl

    df = df.with_columns(pl.lit(False).alias("withdrawn"))
    root = config.curated_root / dataset
    files = list(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        return df
    scan = pl.scan_parquet(files, missing_columns="insert")
    names = scan.collect_schema().names()
    # Columns no stored file has yet (rows from before they existed).
    scan = scan.with_columns(
        pl.lit(None, dtype=df.schema[c]).alias(c) for c in df.columns if c not in names
    )
    stored = (
        scan.filter(pl.col("event_date").is_between(start, end))
        .filter(~pl.col("withdrawn").fill_null(False))
        .filter(~pl.col("event_id").is_in(df["event_id"].implode()))
        .select(df.columns[:-1])
        .collect()
    )
    if stored.is_empty():
        return df
    gone = stored.with_columns(pl.lit(True).alias("withdrawn"))
    return pl.concat([df, gone.select(df.columns)], how="vertical_relaxed")


CALENDAR_HISTORY_START = date(2008, 1, 1)


def _calendar_days_present(config: Config) -> set[date]:
    import polars as pl

    root = config.curated_root / "economic_calendar"
    files = list(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        return set()
    # A day written before indicator_code existed is not "present": the
    # backfill refetches it so every row names its series.
    days = (
        pl.scan_parquet(files, missing_columns="insert")
        .group_by("event_date")
        .agg(pl.col("indicator_code").null_count().alias("uncoded"))
        .filter(pl.col("uncoded") == 0)
        .collect()
    )
    return set(days["event_date"])


def _backfill_economic_calendar(config: Config, trade_date: date, run_id: str) -> dict:
    """Walk [start, end] month by month, fetching the days curated lacks.

    One request per day under the jin10 limiter. Each month is staged as its
    own part, so a killed walk resumes after the last compacted month. A day
    with no events is never "present", so a rerun asks for it again (cheap).
    """
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.storage import StagingWriter

    start = max(
        getattr(config, "_backfill_start", None) or CALENDAR_HISTORY_START, CALENDAR_HISTORY_START
    )
    end = min(getattr(config, "_backfill_end", None) or trade_date, trade_date)
    present = _calendar_days_present(config)
    writer = StagingWriter(config.staging_root)
    rows = months = skipped = 0
    month_start = start.replace(day=1)
    while month_start <= end:
        next_month = (month_start + timedelta(days=32)).replace(day=1)
        day, last = max(month_start, start), min(next_month - timedelta(days=1), end)
        missing = []
        while day <= last:
            if day not in present:
                missing.append(day)
            day += timedelta(days=1)
        if not missing:
            skipped += 1
        else:
            df = fetch_economic_calendar_window(
                missing[0], missing[-1], config=config, run_id=run_id
            )
            if not df.is_empty():
                part = with_provenance(
                    df, source=CALENDAR_SOURCE, data_version=data_version_for("economic_calendar")
                )
                writer.write_batch("economic_calendar", run_id, f"bf-{month_start:%Y%m}", part)
                rows += part.height
            months += 1
        month_start = next_month
    return {
        "rows_read": rows,
        "rows_written": rows,
        "months_fetched": months,
        "months_skipped": skipped,
    }


def _jin10_snapshot(
    config: Config, trade_date: date, run_id: str, dataset: str, fetch, scope: str
) -> dict:
    if not config.sources.get(CALENDAR_SOURCE, True):
        raise SourceUnavailableError(f"{dataset}: jin10 source disabled in config")
    # Rolling live windows like the calendar: future dates are the point, so
    # there is no by-date history to backfill through this step.
    if getattr(config, "_backfill", False):
        raise SnapshotBackfillError(
            f"{dataset}: backfill not supported — fetch semantics are snapshot "
            "(rolling live window)"
        )
    df = call_with_run_id(
        fetch,
        trade_date,
        pipeline_config=config,
        dataset=dataset,
        run_id=run_id,
        config=config,
    )
    empty_ok(df, dataset, trade_date)
    if dataset == "economic_events":
        df = _with_withdrawn(
            config, dataset, df, trade_date - timedelta(days=2), trade_date + timedelta(days=14)
        )
    return write_fetched(
        config,
        run_id,
        dataset,
        df,
        source=CALENDAR_SOURCE,
        raw_archive_evidence=(
            verify_raw_archive(config, dataset, run_id, source=CALENDAR_SOURCE, request_scope=scope)
            if config.should_archive_raw(dataset)
            else None
        ),
        snapshot_date=trade_date,
    )


@register_step("economic_events", group="macro_risk")
def step_economic_events(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    start = trade_date - timedelta(days=2)
    end = trade_date + timedelta(days=14)
    return _jin10_snapshot(
        config, trade_date, run_id, "economic_events", fetch_economic_events,
        f"rolling:{start.isoformat()}:{end.isoformat()}",
    )  # fmt: skip


@register_step("market_holidays", group="macro_risk")
def step_market_holidays(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    return _jin10_snapshot(
        config, trade_date, run_id, "market_holidays", fetch_market_holidays,
        f"from:{trade_date.isoformat()}",
    )  # fmt: skip
