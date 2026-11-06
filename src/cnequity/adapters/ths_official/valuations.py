"""Valuation snapshots from the 同花顺 official API.

The endpoint has no history at all — it answers for today and nothing else, and
the contract says so plainly. That reads like a weakness and mostly is, but it
is also the one thing the lake's own valuation history cannot offer.

``valuation_metrics`` holds 16,457,034 rows from baostock covering 2001 onward.
Those are **restated**: a ratio computed now, from the share count and earnings
as they are understood now, and stamped with an old date. That is the right
series for a like-for-like comparison across time and the wrong one for asking
what a screen would have seen on the day. The dataset says as much —
``pit=none``.

A snapshot taken today and kept is the other thing. Nothing reconstructs it
later, which is exactly why it is worth accumulating: run this daily and the
store grows a licensed record of what the market actually showed, one session at
a time. It cannot backfill a single day before the first run.

Cheap enough to make that routine: the endpoint takes 100 securities per
request, so the whole market costs about 55 of them.

Three of the lake's five columns map. ``total_mv`` and ``float_mv`` have no
counterpart here, and the upstream's ``pe_mrq`` and ``pcf_ttm`` have none in the
lake — neither is invented in either direction.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    from cnequity.adapters.ths_official.client import ThsOfficialClient

logger = logging.getLogger(__name__)

__all__ = ["BATCH_SIZE", "VALUATION_FIELDS", "fetch_valuation_snapshot"]

SOURCE = "ths_official"

# The server caps a request at 100 raw tokens and callers cannot raise it.
BATCH_SIZE = 100

# lake column -> upstream field. `pb` is the MRQ reading because that is the
# only book-value basis the upstream publishes.
VALUATION_FIELDS: dict[str, str] = {
    "pe_ttm": "pe_ttm",
    "pb": "pb_mrq",
    "ps_ttm": "ps_ttm",
}

_OUTPUT_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "pe_ttm": pl.Float64,
    "pb": pl.Float64,
    "ps_ttm": pl.Float64,
    "total_mv": pl.Float64,
    "float_mv": pl.Float64,
}


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def fetch_valuation_snapshot(
    symbols: list[str],
    *,
    client: ThsOfficialClient,
    as_of: date,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Today's valuation ratios for *symbols*, stamped ``as_of``.

    ``as_of`` is supplied rather than derived because the caller knows which
    session these numbers belong to; the response itself carries only a
    readiness timestamp, which is not the same thing near a session boundary.

    ``total_mv`` and ``float_mv`` come back null. They are part of the lake's
    shape and the upstream does not publish them, so they stay empty rather than
    being computed from a share count this response does not carry.
    """
    rows: list[dict] = []
    counters = {"requests": 0, "returned": 0, "failed": 0, "split_retries": 0}
    rejected: list[str] = []

    def _query(batch: list[str]) -> list[dict]:
        """Return the batch's items, halving around a code the upstream rejects.

        One unknown code fails the whole request — ``code=3001 Target not found
        in A-share code table`` — and a market-wide sweep will always contain a
        few, so a plain loop loses 100 good securities to one bad one. Measured
        before this: four of five batches came back empty for that reason.
        Halving isolates the offender in about seven extra requests instead.
        """
        if not batch:
            return []
        counters["requests"] += 1
        try:
            data = client.get("/api/a-share/valuations/snapshot", thscodes=",".join(batch))
        except Exception as exc:  # noqa: BLE001 — one batch must not end the sweep
            if len(batch) == 1:
                counters["failed"] += 1
                rejected.append(batch[0])
                return []
            counters["split_retries"] += 1
            logger.debug("ths_official valuations: halving %d symbol(s) after %s", len(batch), exc)
            middle = len(batch) // 2
            return _query(batch[:middle]) + _query(batch[middle:])
        return (data or {}).get("item") or []

    for offset in range(0, len(symbols), BATCH_SIZE):
        items = _query(symbols[offset : offset + BATCH_SIZE])
        counters["returned"] += len(items)
        for item in items:
            symbol = item.get("thscode")
            if not symbol:
                continue
            values = {
                column: _finite(item.get(field)) for column, field in VALUATION_FIELDS.items()
            }
            if all(value is None for value in values.values()):
                continue
            rows.append(
                {
                    "symbol": str(symbol),
                    "trade_date": as_of,
                    **values,
                    "total_mv": None,
                    "float_mv": None,
                }
            )

    if rejected:
        logger.info(
            "ths_official valuations: %d code(s) the upstream does not carry (e.g. %s)",
            len(rejected),
            rejected[:5],
        )
    if not rows:
        return pl.DataFrame(schema=_OUTPUT_SCHEMA), counters
    from cnequity.domain.value_rules import apply_value_rules

    frame = apply_value_rules(pl.DataFrame(rows, schema=_OUTPUT_SCHEMA), "valuation_metrics")
    return frame.unique(subset=["symbol", "trade_date"], keep="last").sort("symbol"), counters
