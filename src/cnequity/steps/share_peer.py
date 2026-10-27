"""TDX share-change events as the ``share_structure`` peer and backup.

The weekly corporate-actions sweep already asks TDX for every symbol's xdxr
record, and the same response carries each share-count change with the total
and 流通A counts after it. Reading those costs no request. Measured on 300
symbols since 2024: on the 1,435 dates both sources record, total shares agree
to 0.01% on 1,430; `float_shares` agrees on 92%, the rest being A+H companies,
whose EastMoney figure counts the H shares TDX leaves out.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

import polars as pl

from cnequity.config import Config
from cnequity.domain.schemas import data_version_for, with_provenance

logger = logging.getLogger(__name__)

SOURCE = "tdx_protocol"
#: Share changes dated this far back are compared and, if missing, backed up.
WINDOW_DAYS = 90
#: EastMoney counts as stale once its newest row was fetched this long ago.
STALE_DAYS = 4
_TOTAL_TOLERANCE = 1e-4
_FLOAT_TOLERANCE = 1e-3


def _stored(config: Config, since: date) -> pl.DataFrame:
    """The lake's latest-disclosed row per (symbol, change_date) since *since*."""
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.curated_root / "share_structure"
    if not dataset_has_parquet(root):
        return pl.DataFrame()
    frame = (
        scan_parquet_root(root, partition_col="change_date", start=since)
        .filter(pl.col("change_date") >= since)
        .collect()
    )
    if frame.is_empty():
        return frame
    if "float_a_shares" not in frame.columns:
        frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("float_a_shares"))
    return frame.sort("announce_date", nulls_last=False).unique(
        subset=["symbol", "change_date"], keep="last"
    )


def _eastmoney_fresh(config: Config, now: datetime) -> bool:
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.curated_root / "share_structure"
    if not dataset_has_parquet(root):
        return False
    newest = (
        scan_parquet_root(root, partition_col="change_date")
        .filter(pl.col("source").str.starts_with("eastmoney"))
        .select(pl.col("fetched_at").max())
        .collect()
        .item()
    )
    if newest is None:
        return False
    if newest.tzinfo is None:
        newest = newest.replace(tzinfo=timezone.utc)
    return now - newest < timedelta(days=STALE_DAYS)


def _rel(a: str, b: str) -> pl.Expr:
    return (pl.col(a) - pl.col(b)).abs() / pl.col(b).abs().clip(lower_bound=1.0)


def compare(events: pl.DataFrame, stored: pl.DataFrame) -> tuple[dict, pl.DataFrame, set[str]]:
    """``(finding, TDX-only events, A-share-only symbols)`` for one sweep."""
    keys = ["symbol", "change_date"]
    if stored.is_empty():
        joined = events.with_columns(
            *(
                pl.lit(None, dtype=pl.Float64).alias(c)
                for c in ("total_shares_lake", "float_shares", "float_a_shares_lake")
            )
        )
    else:
        joined = events.join(
            stored.select(
                *keys,
                pl.col("total_shares").alias("total_shares_lake"),
                "float_shares",
                pl.col("float_a_shares").alias("float_a_shares_lake"),
            ),
            on=keys,
            how="left",
        )
    both = joined.filter(pl.col("total_shares_lake").is_not_null())
    tdx_only = joined.filter(pl.col("total_shares_lake").is_null()).select(events.columns)
    total_differ = both.filter(_rel("total_shares", "total_shares_lake") > _TOTAL_TOLERANCE)
    float_a_differ = both.filter(
        pl.col("float_a_shares_lake").is_not_null()
        & (_rel("float_a_shares", "float_a_shares_lake") > _FLOAT_TOLERANCE)
    )
    # Where EastMoney's float equals TDX's A-share float, the company has no
    # other listed class, and TDX's figure can stand in for `float_shares`.
    a_only = set(both.filter(_rel("float_a_shares", "float_shares") <= _FLOAT_TOLERANCE)["symbol"])
    finding = {
        "dataset": "share_structure",
        "severity": "warning" if total_differ.height else "info",
        "check": "share_structure_peer",
        "message": (
            f"share_structure vs TDX: {both.height} change(s) on both, "
            f"{total_differ.height} with a different total, "
            f"{float_a_differ.height} with a different A-share float, "
            f"{tdx_only.height} only on TDX"
        ),
        "compared": both.height,
        "total_differ": sorted(
            f"{r['symbol']}@{r['change_date']}" for r in total_differ.head(20).iter_rows(named=True)
        ),
        "float_a_differ": sorted(
            f"{r['symbol']}@{r['change_date']}"
            for r in float_a_differ.head(20).iter_rows(named=True)
        ),
        "tdx_only": tdx_only.height,
    }
    return finding, tdx_only, a_only


def backup_rows(events: pl.DataFrame, a_only: set[str], observed: date) -> pl.DataFrame:
    """TDX events in the ``share_structure`` shape, disclosed as of *observed*.

    TDX gives no disclosure date, so the sweep that saw the change is the
    earliest honest one. ``float_shares`` is filled only where it is known to
    equal the A-share float; restricted and free-float counts stay unknown.
    """
    return events.select(
        "symbol",
        "change_date",
        "total_shares",
        pl.when(pl.col("symbol").is_in(sorted(a_only)))
        .then(pl.col("float_a_shares"))
        .otherwise(None)
        .alias("float_shares"),
        pl.lit(None, dtype=pl.Float64).alias("restricted_shares"),
        "float_a_shares",
        pl.lit(None, dtype=pl.Float64).alias("free_float_shares"),
        pl.lit(None, dtype=pl.Utf8).alias("change_reason"),
        pl.lit(observed).alias("announce_date"),
    )


def share_structure_from_sweep(
    config: Config, run_id: str, trade_date: date, events: list[dict]
) -> list[dict]:
    """Snapshot, compare and (when EastMoney is stale) back up the sweep's share events."""
    from cnequity.adapters.tdx_protocol.client import quotes_client_factory
    from cnequity.adapters.tdx_protocol.corporate_actions import fetch_share_events_tdx
    from cnequity.steps.http_common import verify_raw_archive, write_fetched
    from cnequity.storage.source_snapshots import SnapshotStore

    since = trade_date - timedelta(days=WINDOW_DAYS)
    frame = (
        pl.DataFrame(
            events,
            schema={
                "symbol": pl.Utf8,
                "change_date": pl.Date,
                "total_shares": pl.Float64,
                "float_a_shares": pl.Float64,
            },
        )
        .filter(pl.col("change_date").is_between(since, trade_date))
        .unique(subset=["symbol", "change_date"], keep="last")
    )
    if frame.is_empty():
        return []
    version = data_version_for("share_structure")
    SnapshotStore(config.meta_root).write(
        "share_structure",
        with_provenance(backup_rows(frame, set(), trade_date), source=SOURCE, data_version=version),
        source=SOURCE,
        data_version=version,
        run_id=run_id,
        trade_date=trade_date,
    )
    finding, tdx_only, a_only = compare(frame, _stored(config, since))
    findings = [finding]
    if tdx_only.is_empty() or _eastmoney_fresh(config, datetime.now(timezone.utc)):
        return findings

    # Exact wire evidence must be filed under `share_structure`, so the few
    # symbols being written are asked again under that dataset's capture.
    scope = f"share_backup:{trade_date.isoformat()}"
    refetched = fetch_share_events_tdx(
        sorted(set(tdx_only["symbol"])),
        client_factory=quotes_client_factory(config),
        config=config,
        run_id=run_id,
        request_scope=scope,
        rate_limit=config.tdx_rate_limit_spec(),
    ).join(tdx_only.select("symbol", "change_date"), on=["symbol", "change_date"], how="semi")
    if refetched.is_empty():
        return findings
    rows = with_provenance(
        backup_rows(refetched, a_only, trade_date), source=SOURCE, data_version=version
    )
    result = write_fetched(
        config,
        run_id,
        "share_structure",
        rows,
        source=SOURCE,
        batch_id="tdx-backup",
        raw_archive_evidence=(
            verify_raw_archive(
                config, "share_structure", run_id, source=SOURCE, request_scope=scope
            )
            if config.should_archive_raw("share_structure")
            else None
        ),
    )
    findings.append(
        {
            "dataset": "share_structure",
            "severity": "warning",
            "check": "share_structure_backup",
            "message": (
                f"EastMoney share_structure is stale; wrote {rows.height} TDX share change(s) "
                f"dated {trade_date} as their disclosure"
            ),
            "rows_written": int(result.get("rows_written", rows.height)),
        }
    )
    return findings
