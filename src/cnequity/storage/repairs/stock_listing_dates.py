"""Repair unknown stock listing dates using replayable official IPO events."""

import json
from contextlib import nullcontext
from datetime import date, datetime, timezone
from uuid import uuid4

import httpx
import polars as pl

from cnequity.adapters.cninfo.listings import IPO_CALENDAR_URL, parse_listing_calendar
from cnequity.adapters.eastmoney.instruments import _fill_list_dates
from cnequity.domain.http_policy import record_http_response
from cnequity.domain.rate_limit import source_request
from cnequity.file_lock import lake_mutation_lock
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.raw_archive import RawPayloadArchive
from cnequity.storage.read_context import ReadContext
from cnequity.storage.repairs.publish import publish_staged
from cnequity.storage.revisions import RevisionStore


def _unknown_traded(context):
    catalog = scan_parquet_root(context.roots["instruments"], committed=False).collect()
    missing = catalog.filter(
        pl.col("asset_type").is_in(["stock", "cdr"]) & pl.col("list_date").is_null()
    )
    if not dataset_has_parquet(context.roots["daily_bars"], committed=False):
        return missing, {}
    traded = (
        scan_parquet_root(context.roots["daily_bars"], committed=False)
        .filter(pl.col("symbol").is_in(missing["symbol"].to_list()) & (pl.col("volume") > 0))
        .group_by("symbol")
        .agg(pl.col("trade_date").min())
        .collect()
    )
    return missing, dict(traded.iter_rows())


def repair_stock_listing_dates(
    config, *, apply=False, refresh_sources=False, max_sessions=32, event_dates=()
):
    if max_sessions < 1:
        raise ValueError("max_sessions must be positive")
    requested = set(event_dates)
    if any(not isinstance(day, date) or isinstance(day, datetime) for day in requested):
        raise ValueError("event_dates must contain calendar dates")
    if len(requested) > max_sessions:
        raise ValueError("explicit event dates exceed max_sessions")
    if refresh_sources and not config.sources.get("cninfo", False):
        raise ValueError("cninfo source disabled")
    context = ReadContext.capture(config, ("instruments", "daily_bars"), {})
    if not dataset_has_parquet(context.roots["instruments"], committed=False):
        return {"applied": False, "rows": 0, "failures": []}
    _, first = _unknown_traded(context)
    days = (
        sorted(requested) + sorted(set(first.values()) - requested)[: max_sessions - len(requested)]
    )
    archive = RawPayloadArchive(config.meta_root, enabled=True, datasets=["instruments"])
    cache = config.meta_root / "source_cache" / "stock_listing_dates"
    run_id = "stock-listings-" + uuid4().hex
    dates, failures, conflicts = {}, [], set()
    with httpx.Client(timeout=45, follow_redirects=True) as client:
        for day in days:
            params = {"queryDate": str(day)}
            path = cache / f"{day}.json"
            try:
                if refresh_sources:
                    with source_request(config, "cninfo"):
                        response = client.get(IPO_CALENDAR_URL, params=params)
                        record_http_response(config, "cninfo", response)
                    response.raise_for_status()
                    confirmed = parse_listing_calendar(response.content, day)
                    record = archive.archive(
                        "instruments",
                        response.content,
                        source="cninfo",
                        run_id=run_id,
                        url=IPO_CALENDAR_URL,
                        request_params=params,
                        payload_format="json",
                    ).to_dict()
                    write_json_atomic(path, record)
                else:
                    record = json.loads(path.read_text())
                    if (
                        record.get("url") != IPO_CALENDAR_URL
                        or record.get("request_params") != params
                        or record.get("source") != "cninfo"
                        or record.get("dataset") != "instruments"
                    ):
                        raise ValueError("cached IPO calendar identity differs")
                    confirmed = parse_listing_calendar(archive.read(record), day)
                conflicts.update(s for s, d in confirmed.items() if s in dates and dates[s] != d)
                dates.update(confirmed)
            except (OSError, ValueError, RuntimeError, httpx.HTTPError) as exc:
                failures.append({"date": str(day), "error": str(exc)})
    # Re-read inside the lock: do not overwrite dates or contradict new bars
    # from a concurrent writer. The first bar only selects the request day;
    # the independently dated 'listed' event supplies the fact.
    with lake_mutation_lock(config.meta_root) if apply else nullcontext():
        context = ReadContext.capture(config, ("instruments", "daily_bars"), {})
        missing, first = _unknown_traded(context)
        missing_symbols = set(missing["symbol"].to_list())
        fills = {
            symbol: day
            for symbol, day in dates.items()
            if symbol in missing_symbols
            and symbol not in conflicts
            and (
                first.get(symbol) == day
                or (day in requested and (symbol not in first or first[symbol] >= day))
            )
        }
        report = {
            "applied": False,
            "rows": len(fills),
            "sessions": len(days),
            "remaining_unknown_stocks": missing.height - len(fills),
            "listing_dates": {s: str(d) for s, d in sorted(fills.items())},
            "failures": failures,
            "conflicting_symbols": sorted(conflicts),
        }
        rows = _fill_list_dates(missing.filter(pl.col("symbol").is_in(fills)), fills)
        report["cleared_invalid_delist_dates"] = rows.filter(
            pl.col("delist_date") < pl.col("list_date")
        ).height
        if not apply or not fills:
            return report
        # A confirmed new listing cannot have ended before it began. Remove
        # that stale end date in the same retained revision as the new fact.
        rows = rows.with_columns(
            pl.when(pl.col("delist_date") < pl.col("list_date"))
            .then(pl.lit(None, dtype=pl.Date))
            .otherwise(pl.col("delist_date"))
            .alias("delist_date"),
            pl.lit(datetime.now(timezone.utc)).alias("fetched_at"),
        )
        store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
        store.ensure_current("instruments")
        published = publish_staged(
            config,
            store,
            "instruments",
            rows,
            run_id=run_id,
            base_root=context.roots["instruments"],
            metadata={
                "reason": "confirmed_ipo_listing_dates",
                **report,
            },
        )
        if published is None:
            raise RuntimeError("stock listing repair produced no revision")
        report.update(applied=True, revision=published["revision"])
        return report
