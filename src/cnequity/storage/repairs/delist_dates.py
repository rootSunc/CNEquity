"""Hold ``instruments.delist_date`` to the last listed session.

``delist_date`` is the last session a security was listed, inclusive: universe
filters keep it through that day. baostock's ``outDate`` is the removal date
(摘牌日), the first day it is no longer listed; no traded bar ever falls on it.
Rows that stored ``outDate`` directly ran one session late. The removal date
itself stays in the delisted identity evidence.
"""

from contextlib import nullcontext
from datetime import date, datetime, timezone
from uuid import uuid4

import polars as pl

from cnequity.file_lock import lake_mutation_lock
from cnequity.query.calendar import last_listed_sessions
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.read_context import ReadContext
from cnequity.storage.repairs.publish import publish_staged
from cnequity.storage.revisions import RevisionStore


def _last_traded(context, symbols: list[str]) -> dict[str, date]:
    # baostock also writes a zero-volume row dated on the removal day itself;
    # only a positive-volume bar proves the security was still listed.
    if not symbols or not dataset_has_parquet(context.roots["daily_bars"], committed=False):
        return {}
    frame = (
        scan_parquet_root(context.roots["daily_bars"], committed=False)
        .filter(pl.col("symbol").is_in(symbols) & (pl.col("volume") > 0))
        .group_by("symbol")
        .agg(pl.col("trade_date").max())
        .collect()
    )
    return dict(frame.iter_rows())


def repair_delist_dates(config, *, apply: bool = False, sample: int = 15) -> dict:
    """Move delist dates that equal the formal removal date back one session.

    Only rows whose ``delist_date`` equals baostock's removal date change, and
    never to a day before the security's last traded bar. Dry-run unless ``apply``.
    """
    from cnequity.quality.delisted_coverage import known_delisted_instruments

    removals = known_delisted_instruments(config, date.today())
    with lake_mutation_lock(config.meta_root) if apply else nullcontext():
        context = ReadContext.capture(config, ("instruments", "daily_bars"), {})
        if not dataset_has_parquet(context.roots["instruments"], committed=False):
            return {"applied": False, "rows": 0}
        catalog = scan_parquet_root(context.roots["instruments"], committed=False).collect()
        late = catalog.filter(
            pl.col("delist_date").is_not_null()
            & (
                pl.col("delist_date")
                == pl.col("symbol").replace_strict(removals, default=None, return_dtype=pl.Date)
            )
        )
        targets = last_listed_sessions(
            config, {symbol: removals[symbol] for symbol in late["symbol"].to_list()}
        )
        last_traded = _last_traded(context, sorted(targets))
        fixes = {
            symbol: day
            for symbol, day in targets.items()
            if symbol not in last_traded or last_traded[symbol] <= day
        }
        report = {
            "applied": False,
            "rows": len(fixes),
            "late_rows": late.height,
            "outside_calendar": late.height - len(targets),
            "before_last_traded": len(targets) - len(fixes),
            "samples": [
                {
                    "symbol": symbol,
                    "removal_date": removals[symbol].isoformat(),
                    "delist_date": day.isoformat(),
                }
                for symbol, day in sorted(fixes.items())[:sample]
            ],
        }
        if not apply or not fixes:
            return report
        run_id = "delist-dates-" + uuid4().hex
        rows = late.filter(pl.col("symbol").is_in(list(fixes))).with_columns(
            pl.col("symbol").replace_strict(fixes, return_dtype=pl.Date).alias("delist_date"),
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
                "reason": "delist_date_is_last_listed_session",
                **report,
            },
        )
        if published is None:
            raise RuntimeError("delist date repair produced no revision")
        report.update(applied=True, revision=published["revision"])
        return report
