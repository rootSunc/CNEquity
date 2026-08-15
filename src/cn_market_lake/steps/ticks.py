"""Transaction-record capture (分笔), in its own step group.

Not in ``steps/intraday.py`` and not in the ``intraday`` group, even though
both are opt-in intraday capture off the same vendor. Two reasons, and the
second is the load-bearing one:

* the fetch shape is inverted. Minute bars page backwards from today's tip, so
  a run asks for a *window* and the adapter walks to it. Transaction records
  are requested one session at a time, so this step iterates trading days and
  the window never reaches the wire.
* a user who enabled minute bars did not thereby enable this. Full-market 1m is
  ~30MB a session; full-market ticks are ~60MB and twenty minutes of wire time,
  on a dataset most lakes will never want. Sharing a group would make one
  opt-in silently imply the other.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import polars as pl

from cn_market_lake.adapters.tdx_protocol.client import (
    fetch_trade_ticks_batch,
    normalize_with_source,
)
from cn_market_lake.config import Config
from cn_market_lake.domain.datasets import get_dataset
from cn_market_lake.orchestrator.registry import register_step
from cn_market_lake.steps.common import incremental_window
from cn_market_lake.storage import StagingWriter

logger = logging.getLogger(__name__)

DATASET = "trade_ticks"

# Symbols per staged batch. Smaller than the minute bars' 200 because a
# symbol-day here is a whole session of rows rather than a slice of one: 50
# symbols over a 5-day chunk is ~675k rows in a staging file, and compact reads
# every file of a run into one frame.
_BATCH_SYMBOLS = 50


class TradeTicksScopeError(RuntimeError):
    """Raised when the configured scope cannot be resolved to symbols."""


def resolve_scope(config: Config) -> list[str]:
    """Symbols the capture covers, per ``[trade_ticks].scope``.

    ``watchlist`` — exactly ``[trade_ticks].symbols``.
    ``index:<symbol>`` — that index's latest constituents.

    There is no ``all``: the config rejects it (see ``validate_config``), and
    whatever a scope resolves to is checked against ``max_symbols`` here,
    before a single request goes out.
    """
    scope = (config.trade_ticks_scope or "").strip()
    if scope == "watchlist":
        symbols = [s.strip() for s in config.trade_ticks_symbols if s.strip()]
        if not symbols:
            raise TradeTicksScopeError(
                "[trade_ticks].scope = 'watchlist' but [trade_ticks].symbols is empty"
            )
    elif scope.startswith("index:"):
        from cn_market_lake.steps.intraday import _index_members

        symbols = _index_members(config, scope.split(":", 1)[1].strip())
    else:
        raise TradeTicksScopeError(
            f"unknown [trade_ticks].scope {scope!r} (expected 'watchlist' or 'index:<symbol>')"
        )

    # TDX has no Beijing route for transaction records — the adapter raises on
    # those rather than returning empty, so dropping them here keeps a scope
    # that happens to include one from reading as a wall of failures.
    symbols = [s for s in symbols if not s.endswith(".BJ")]

    limit = config.trade_ticks_max_symbols
    if len(symbols) > limit:
        raise TradeTicksScopeError(
            f"[trade_ticks].scope {scope!r} resolves to {len(symbols)} symbols, over the "
            f"max_symbols ceiling of {limit}. At ~1.85 requests and ~4.25 bytes/row per "
            f"symbol-session that is roughly {len(symbols) * 1.85:.0f} requests and "
            f"{len(symbols) * 2700 * 4.25 / 1e6:.1f}MB per session. Raise "
            "[trade_ticks].max_symbols if that is what you want."
        )
    return symbols


def _sessions(config: Config, trade_date: date) -> list[date]:
    """Trading days to capture, clamped to the source's floor.

    Clamping rather than failing: a first run legitimately asks for more than
    the source has, and the honest answer is "here is everything that exists",
    logged so it is not mistaken for complete history.
    """
    if getattr(config, "_backfill", False):
        end = getattr(config, "_backfill_end", None) or trade_date
        start = getattr(config, "_backfill_start", None) or (end - timedelta(days=30))
    else:
        start = incremental_window(config, DATASET, trade_date)
        end = trade_date

    floor = get_dataset(DATASET).earliest_available(trade_date)
    if floor is not None and start < floor:
        logger.warning(
            "%s: requested start %s is before the source floor %s; clamping",
            DATASET,
            start,
            floor,
        )
        start = floor
    end = min(end, trade_date)
    if start > end:
        return []

    from cn_market_lake.steps.common import _load_trading_calendar_df

    calendar = _load_trading_calendar_df(config, start=start, end=end)
    if calendar is not None and not calendar.is_empty() and "is_trading" in calendar.columns:
        return sorted(calendar.filter(pl.col("is_trading"))["trade_date"].to_list())
    # No calendar yet (a lake seeded out of order). Weekdays over-count by the
    # public holidays, and each extra day costs one request that comes back
    # empty — wasteful, not wrong.
    logger.warning("%s: no trading calendar available; falling back to weekdays", DATASET)
    span = (end - start).days
    days = [start + timedelta(days=offset) for offset in range(span + 1)]
    return [d for d in days if d.weekday() < 5]


def capture_trade_ticks(config: Config, trade_date: date, run_id: str) -> dict:
    """Capture transaction records for the configured scope and window."""
    if not config.trade_ticks_enabled:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "note": "trade_ticks capture disabled ([trade_ticks].enabled = false)",
        }

    symbols = resolve_scope(config)
    sessions = _sessions(config, trade_date)
    if not sessions:
        return {"rows_read": 0, "rows_written": 0, "note": "no trading sessions in window"}

    logger.info(
        "%s: %d symbol(s) over %d session(s), %s..%s",
        DATASET,
        len(symbols),
        len(sessions),
        sessions[0],
        sessions[-1],
    )

    writer = StagingWriter(config.staging_root)
    rate_limit = config.tdx_rate_limit_spec()
    written = 0
    failed: list[str] = []
    with_rows: set[str] = set()

    for index in range(0, len(symbols), _BATCH_SYMBOLS):
        chunk = symbols[index : index + _BATCH_SYMBOLS]
        try:
            df, chunk_failed = fetch_trade_ticks_batch(
                chunk,
                sessions,
                rate_limit=rate_limit,
                config=config,
                workers=config.trade_ticks_fetch_workers,
            )
        except Exception as exc:  # noqa: BLE001 — recorded, sweep continues
            # A batch failing outright (a connect timeout after many
            # reconnects) costs this batch, not the step. None of these symbols
            # got a chance to fail individually, so all of them count.
            logger.warning(
                "%s: batch of %d symbol(s) failed outright (%s..%s): %s",
                DATASET,
                len(chunk),
                chunk[0],
                chunk[-1],
                exc,
            )
            failed.extend(f"{sym}@batch" for sym in chunk)
            continue
        failed.extend(chunk_failed)
        if df.is_empty():
            continue
        with_rows.update(df["symbol"].unique().to_list())
        df = normalize_with_source(df, dataset=DATASET)
        writer.write_batch(DATASET, run_id, f"ticks-{index // _BATCH_SYMBOLS:04d}", df)
        written += df.height
        logger.info(
            "%s: %d/%d symbols, %d rows staged",
            DATASET,
            min(index + _BATCH_SYMBOLS, len(symbols)),
            len(symbols),
            written,
        )

    result: dict = {
        "rows_read": written,
        "rows_written": written,
        "symbols": len(symbols),
        "symbols_with_rows": len(with_rows),
        "sessions": len(sessions),
        "failed_symbol_days": len(failed),
        "note": f"{sessions[0]}..{sessions[-1]} scope={config.trade_ticks_scope}",
    }
    if failed:
        result["context_updates"] = {
            "audit_findings": [
                {
                    "dataset": DATASET,
                    "severity": "warning",
                    "check": "trade_ticks_symbol_day_fetch",
                    "message": (
                        f"{len(failed)} symbol-day(s) of "
                        f"{len(symbols) * len(sessions)} returned no transaction records "
                        f"(e.g. {', '.join(failed[:5])})"
                    ),
                }
            ]
        }
    if written == 0 and symbols:
        raise RuntimeError(
            f"{DATASET}: no rows for any of {len(symbols)} symbol(s) over "
            f"{len(sessions)} session(s) — check TDX reachability and that the "
            "window is inside the source's history floor"
        )
    return result


@register_step(DATASET, group="ticks", depends_on=["instruments"])
def step_trade_ticks(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """Capture transaction records for the configured scope (opt-in)."""
    return capture_trade_ticks(config, trade_date, run_id)
