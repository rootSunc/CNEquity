"""The 同花顺 official adjustment-factor dump, shaped as corporate actions.

``adj_factors`` is the lake's only dataset with no peer at all: 19,088,826 rows,
100% from sina, no ``backup_source``, no ``backfill_source``, and no entry in
``[[failover.datasets]]``. Nothing can currently arbitrate it.

That matters because the lake already disagrees with itself. Measured
2026-09-09 over the full population: of 60,598 comparable corporate-action
dates, 6,551 (10.8%) see no move in the sina hfq factor, and of 55,575 factor
jumps, 1,528 (2.7%) have no recorded action — 8,079 (symbol, date) pairs where
two internal series contradict each other and no third party can say which is
right. The gap widens each year (155 in 2023, 380 in 2025, 526 in 2026).

**One download, not 5,500 requests.** ``/api/dump/market-dumps/adjustment-
factors/download-url`` returns a presigned link to the whole event history:
57,139 rows over 5,420 securities, 1991-02-26 to 2026-09-16, measured
2026-09-09. Cash dividends agreed with the lake on 52,509 of 52,583 comparable
events (99.86%).

**Two caliber limits, both measured, neither worked around:**

*No 转增.* ``per_share_bonus`` is 送股 alone — of 34 comparable events carrying
a transfer, 33 matched 送股 and not 送+转. ``transfer_ratio`` is therefore left
null rather than folded into ``bonus_ratio``, and a reader deriving dilution
from these rows alone would under-adjust a 转增. This is evidence for
arbitration, not a replacement series.

*No delisted securities.* The upstream refuses them outright
(``code=1002 Unknown thscode``), so the 594 delisted names the lake carries are
absent from the dump and stay unarbitrated.
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import polars as pl

from cnequity.domain.http_policy import record_http_response
from cnequity.domain.market_time import SHANGHAI_ZONE
from cnequity.domain.rate_limit import source_request
from cnequity.storage.atomic import replace_with_retry

if TYPE_CHECKING:
    from cnequity.adapters.ths_official.client import ThsOfficialClient

logger = logging.getLogger(__name__)

__all__ = ["DUMP_KIND", "download_adjustment_factor_dump", "parse_adjustment_factor_dump"]

CST = timezone(timedelta(hours=8))
SOURCE = "ths_official"
DUMP_KIND = "adjustment-factors"

_OUTPUT_SCHEMA = {
    "symbol": pl.Utf8,
    "ex_date": pl.Date,
    "action_type": pl.Utf8,
    "cash_dividend": pl.Float64,
    "bonus_ratio": pl.Float64,
    "transfer_ratio": pl.Float64,
    "allotment_ratio": pl.Float64,
    "allotment_price": pl.Float64,
}


class ThsOfficialDumpError(RuntimeError):
    """The dump could not be fetched or does not look like the documented shape."""


def download_adjustment_factor_dump(
    client: ThsOfficialClient, destination: Path, *, timeout: float = 300.0
) -> Path:
    """Fetch the dump to *destination*.

    The presigned link lives about 300 seconds, so it is requested immediately
    before the transfer and never stored. A link fetched earlier and replayed
    answers 403, which is what a cached envelope produces.
    """
    url, ttl = client.download_url(DUMP_KIND)
    logger.info("ths_official %s dump: link valid for %ss", DUMP_KIND, ttl)
    destination.parent.mkdir(parents=True, exist_ok=True)
    config = getattr(client, "_config", None)
    temporary = None
    try:
        with source_request(config, "ths_official"):
            with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as response:
                record_http_response(config, "ths_official", response)
                if response.status_code != 200:
                    raise ThsOfficialDumpError(
                        f"{DUMP_KIND} dump download failed: HTTP {response.status_code}"
                    )
                with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as handle:
                    temporary = Path(handle.name)
                    for chunk in response.iter_bytes():
                        handle.write(chunk)
                    handle.flush()
                    os.fsync(handle.fileno())
        replace_with_retry(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination


def parse_adjustment_factor_dump(path: Path) -> pl.DataFrame:
    """Expand the dump into one row per (symbol, ex_date, action_type).

    ``corporate_actions`` keys on the action type, so one dated event carrying
    both a dividend and a bonus becomes two rows, exactly as the lake's other
    corporate-action adapters emit them.
    """
    raw = pl.read_parquet(path)
    required = {"thscode", "ex_date_ms", "dividend_per_share", "per_share_bonus"}
    missing = required - set(raw.columns)
    if missing:
        raise ThsOfficialDumpError(f"{DUMP_KIND} dump is missing columns: {sorted(missing)}")

    for optional in ("allotment_ratio", "allotment_price"):
        if optional not in raw.columns:
            raw = raw.with_columns(pl.lit(None, dtype=pl.Float64).alias(optional))

    frame = raw.select(
        pl.col("thscode").cast(pl.Utf8).alias("symbol"),
        pl.from_epoch(pl.col("ex_date_ms"), time_unit="ms")
        .dt.convert_time_zone(SHANGHAI_ZONE)
        .dt.date()
        .alias("ex_date"),
        pl.col("dividend_per_share").cast(pl.Float64).fill_null(0.0).alias("_cash"),
        pl.col("per_share_bonus").cast(pl.Float64).fill_null(0.0).alias("_bonus"),
        pl.col("allotment_ratio").cast(pl.Float64).alias("_allot_ratio"),
        pl.col("allotment_price").cast(pl.Float64).alias("_allot_price"),
    )

    def _rows(mask: pl.Expr, action_type: str, **columns: pl.Expr) -> pl.DataFrame:
        selected = frame.filter(mask)
        if selected.is_empty():
            return pl.DataFrame(schema=_OUTPUT_SCHEMA)
        base = {
            "symbol": pl.col("symbol"),
            "ex_date": pl.col("ex_date"),
            "action_type": pl.lit(action_type),
            "cash_dividend": pl.lit(0.0),
            "bonus_ratio": pl.lit(0.0),
            # Never inferred: the upstream has no 转增 field at all, and folding
            # it into bonus_ratio would manufacture a caliber it does not report.
            "transfer_ratio": pl.lit(None, dtype=pl.Float64),
            "allotment_ratio": pl.lit(None, dtype=pl.Float64),
            "allotment_price": pl.lit(None, dtype=pl.Float64),
        }
        base.update(columns)
        return selected.select(**base).cast(_OUTPUT_SCHEMA)  # type: ignore[arg-type]

    parts = [
        _rows(pl.col("_cash") > 0, "cash_dividend", cash_dividend=pl.col("_cash")),
        _rows(pl.col("_bonus") > 0, "bonus", bonus_ratio=pl.col("_bonus")),
        _rows(
            pl.col("_allot_ratio").fill_null(0.0) > 0,
            "allotment",
            allotment_ratio=pl.col("_allot_ratio"),
            allotment_price=pl.col("_allot_price"),
        ),
    ]
    populated = [part for part in parts if not part.is_empty()]
    if not populated:
        # Every event was zero-amount. Legitimate for a narrow slice of the dump,
        # so return the empty shape rather than failing the caller.
        return pl.DataFrame(schema=_OUTPUT_SCHEMA)
    out = pl.concat(populated, how="vertical")
    return out.unique(subset=["symbol", "ex_date", "action_type"], keep="last").sort(
        ["symbol", "ex_date", "action_type"]
    )


def fetch_corporate_actions_dump(
    client: ThsOfficialClient, *, cache_dir: Path, as_of: date | None = None
) -> tuple[pl.DataFrame, Path]:
    """Download and parse in one step, keeping the parquet for offline re-reads."""
    stamp = (as_of or datetime.now(CST).date()).isoformat()
    target = cache_dir / f"ths_adjustment_factors_{stamp}.parquet"
    if not target.exists():
        download_adjustment_factor_dump(client, target)
    return parse_adjustment_factor_dump(target), target
