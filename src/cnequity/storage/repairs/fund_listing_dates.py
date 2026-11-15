"""Fill missing ETF/LOF listing dates from complete official directories."""

from contextlib import nullcontext
from datetime import datetime, timezone
from uuid import uuid4

import polars as pl

from cnequity.adapters.eastmoney.instruments import _fill_list_dates
from cnequity.adapters.exchange.fund_listings import fetch_exchange_fund_listings
from cnequity.config import Config
from cnequity.domain.market_time import shanghai_today
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.lake_write import planned_on
from cnequity.storage.raw_archive import RawPayloadArchive
from cnequity.storage.read_context import ReadContext
from cnequity.storage.repairs.publish import publish_staged
from cnequity.storage.revisions import RevisionStore


def repair_fund_listing_dates(config: Config, *, apply: bool = False) -> dict:
    if not config.sources.get("exchange", True):
        raise RuntimeError("exchange source disabled")
    # Fetch before taking the publication lock; re-read the live generation
    # below, so another writer's dates cannot be overwritten by this repair.
    dates, wire = fetch_exchange_fund_listings(config, exchanges={"SH", "SZ"})
    with planned_on(config) if apply else nullcontext():
        store = RevisionStore(
            config.meta_root, config.curated_root, config.derived_root, create=apply
        )
        context = ReadContext.capture(config, ("instruments",), {})
        root = context.roots["instruments"]
        report = {"applied": False, "rows": 0, "official_dates": len(dates)}
        if not dataset_has_parquet(root, committed=False):
            return report
        before = scan_parquet_root(root, committed=False).collect()
        missing = before.filter(
            pl.col("asset_type").is_in(["etf", "lof"]) & pl.col("list_date").is_null()
        )
        wanted = set(missing["symbol"].to_list())
        fills = {s: d for s, d in dates.items() if s in wanted and d <= shanghai_today()}
        report.update(
            rows=len(fills),
            remaining_unknown=missing.height - len(fills),
            listing_dates={s: str(d) for s, d in sorted(fills.items())},
        )
        if not apply or not fills:
            return report
        run_id = f"fund-listings-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
        archive = RawPayloadArchive(
            config.meta_root,
            enabled=True,
            datasets=["instruments"],
            compression=config.raw_archive_compression,
            max_payload_bytes=config.raw_archive_max_payload_bytes,
        )
        for url, payload, params, kind in wire:
            archive.archive(
                "instruments",
                payload,
                source="exchange",
                run_id=run_id,
                request_params=params,
                url=url,
                payload_format=kind,
            )
        changed_rows = _fill_list_dates(missing.filter(pl.col("symbol").is_in(fills)), fills)
        changed_rows = changed_rows.with_columns(
            pl.lit(datetime.now(timezone.utc)).alias("fetched_at")
        )
        published = publish_staged(
            config,
            store,
            "instruments",
            changed_rows,
            run_id=run_id,
            base_root=root,
            metadata={
                "reason": "official_fund_listing_dates",
                **report,
            },
        )
        if published is None:
            raise RuntimeError("fund listing repair produced no revision")
        report.update(
            applied=True,
            revision=published["revision"],
            publication_audit=published["publication_audit"],
        )
        return report
