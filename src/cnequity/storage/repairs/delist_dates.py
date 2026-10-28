"""Hold ``instruments.delist_date`` to the last listed session.

``delist_date`` is the last session a security was listed, inclusive: universe
filters keep it through that day. baostock's ``outDate`` is the removal date
(摘牌日), the first day it is no longer listed; no traded bar ever falls on it.
Rows that stored ``outDate`` directly ran one session late. The removal date
itself stays in the delisted identity evidence.
"""

from bisect import bisect_left
from contextlib import nullcontext
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import polars as pl

from cnequity.domain.contracts import contract_fingerprint, dataset_contract
from cnequity.file_lock import lake_mutation_lock
from cnequity.quality.publication import check_repair_publication
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.parquet import StagingWriter, compact_dataset
from cnequity.storage.read_context import ReadContext
from cnequity.storage.revisions import RevisionStore

# A removal is announced within weeks of the last listed session; a calendar
# gap wider than this means the calendar does not reach that far back.
_MAX_LOOKBACK_DAYS = 60


def last_listed_sessions(config, removals: dict[str, date]) -> dict[str, date]:
    """``symbol -> last trading session before its removal date``.

    Symbols whose removal predates the trading calendar are left out rather
    than given a calendar-day guess.
    """
    if not removals:
        return {}
    from cnequity.query.calendar import list_trading_dates

    start = min(removals.values()) - timedelta(days=_MAX_LOOKBACK_DAYS)
    sessions = list_trading_dates(config, start, max(removals.values()), include_staging=True)
    out = {}
    for symbol, removal in removals.items():
        index = bisect_left(sessions, removal) - 1
        if index >= 0 and (removal - sessions[index]).days <= _MAX_LOOKBACK_DAYS:
            out[symbol] = sessions[index]
    return out


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
        store.materialize_current("instruments")
        changed = []
        with TemporaryDirectory(prefix="cne-delist-dates-") as directory:
            staging = Path(directory)
            StagingWriter(staging).write_batch("instruments", run_id, "repair-0", rows)
            compact_dataset(
                staging,
                config.curated_root,
                "instruments",
                run_id,
                changed_files=changed,
                base_root=context.roots["instruments"],
            )
        publication = check_repair_publication(config, "instruments", run_id, changed)
        contract = dataset_contract("instruments")
        revision = store.commit(
            "instruments",
            run_id=run_id,
            changed_files=changed,
            schema_version=int(contract["schema_version"]),
            contract_fingerprint=contract_fingerprint(contract),
            metadata={
                "reason": "delist_date_is_last_listed_session",
                **report,
                "publication_audit": publication["report_path"],
            },
        )
        if revision is None:
            raise RuntimeError("delist date repair produced no revision")
        report.update(applied=True, revision=revision.revision)
        return report
