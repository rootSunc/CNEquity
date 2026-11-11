"""Repair stored ``turnover_pct`` against an authority or a peer source."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.symbols import (
    is_baostock_stock,
)
from cnequity.run_scope import run_flags_of
from cnequity.steps.bars import bar_scope
from cnequity.steps.bars._base import _yearly_slices, logger

# Baostock and the stored row must describe the same session: unadjusted
# prices to within half a cent.
_TURNOVER_REPAIR_PRICE_TOLERANCE = 0.006
# Symbols per baostock call and per staged batch of the turnover repair.
_TURNOVER_REPAIR_BATCH = 100


def _turnover_replacements(
    stored: pl.DataFrame, fresh: pl.DataFrame
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Baostock rows that may replace stored rows with unusable turnover.

    A replacement is the whole baostock row, never a spliced field, and only
    when its prices match the stored row and its own turnover is usable.
    """
    from cnequity.domain.units import turnover_defect_expr

    prices = ("open", "high", "low", "close")
    candidates = fresh.filter(turnover_defect_expr().is_null() & (pl.col("volume") > 0))
    joined = stored.select("symbol", "trade_date", *prices).join(
        candidates.select("symbol", "trade_date", *[pl.col(c).alias(f"_new_{c}") for c in prices]),
        on=["symbol", "trade_date"],
        how="left",
    )
    served = pl.col("_new_close").is_not_null()
    same = pl.all_horizontal(
        (pl.col(c) - pl.col(f"_new_{c}")).abs() <= _TURNOVER_REPAIR_PRICE_TOLERANCE for c in prices
    )
    accepted = joined.filter(served & same).select("symbol", "trade_date")
    counts = {
        "rows_checked": stored.height,
        "rows_unserved": joined.filter(~served).height,
        "rows_disagreeing": joined.filter(served & ~same).height,
        "rows_replaced": accepted.height,
    }
    return fresh.join(accepted, on=["symbol", "trade_date"], how="semi"), counts


def _authority_turnover_replacements(stored, fresh, quotes):
    """Confirm raw prices independently; keep the entire BaoStock bar.

    SZSE report turnover includes a broader transaction scope, so only its
    exact dated OHLC is admissible here, never its quantities or amounts.
    """
    if stored.is_empty() or quotes.is_empty():
        return fresh.head(0)
    prices = ("open", "high", "low", "close")
    candidates = fresh.filter(
        pl.col("volume").is_finite()
        & (pl.col("volume") > 0)
        & pl.col("amount").is_finite()
        & (pl.col("amount") > 0)
        & (pl.col("amount") / pl.col("volume")).is_between(
            pl.col("low") * 0.98, pl.col("high") * 1.02
        )
    ).join(stored.select("symbol", "trade_date"), on=["symbol", "trade_date"], how="semi")
    confirmed = (
        candidates.join(
            quotes.select(
                "symbol", "trade_date", *[pl.col(c).alias(f"_authority_{c}") for c in prices]
            ),
            on=["symbol", "trade_date"],
        )
        .filter(
            pl.all_horizontal(
                pl.col(c).is_finite()
                & (pl.col(c) > 0)
                & (
                    (pl.col(c) - pl.col(f"_authority_{c}")).abs()
                    <= _TURNOVER_REPAIR_PRICE_TOLERANCE
                )
                for c in prices
            )
        )
        .select("symbol", "trade_date")
    )
    return candidates.join(confirmed, on=["symbol", "trade_date"], how="semi")


def _peer_turnover_replacements(stored, fresh, peer):
    """Select whole bars using a second price reading for conflicts.

    EastMoney can truncate stock volume to lots and round amounts. Agreement
    tolerances never replace BaoStock's published quantity with an estimate.
    """
    fields = ("open", "high", "low", "close", "volume", "amount")

    def usable(frame):
        return frame.filter(
            pl.all_horizontal(pl.col(c).is_finite() & (pl.col(c) > 0) for c in fields)
            & (pl.col("low") <= pl.min_horizontal("open", "close"))
            & (pl.col("high") >= pl.max_horizontal("open", "close"))
            & (pl.col("amount") / pl.col("volume")).is_between(
                pl.col("low") * 0.98, pl.col("high") * 1.02
            )
        )

    empty = fresh.head(0)
    if stored.is_empty() or peer.is_empty():
        return empty, empty
    peer = usable(peer)
    fresh = usable(fresh)
    prices = fields[:4]
    keys = ["symbol", "trade_date"]
    readings = stored.select(*keys, *prices).join(
        peer.select(*keys, *[pl.col(c).alias(f"_peer_{c}") for c in fields]), on=keys
    )
    unchanged = readings.filter(
        pl.all_horizontal(
            (pl.col(c) - pl.col(f"_peer_{c}")).abs() <= _TURNOVER_REPAIR_PRICE_TOLERANCE
            for c in prices
        )
    ).select(keys)
    corroborated = (
        fresh.join(readings.select(*keys, *[f"_peer_{c}" for c in fields]), on=keys)
        .filter(
            pl.all_horizontal(
                (pl.col(c) - pl.col(f"_peer_{c}")).abs() <= _TURNOVER_REPAIR_PRICE_TOLERANCE
                for c in prices
            )
            & (pl.col("volume") - pl.col("_peer_volume")).is_between(0, 99)
            & (
                (pl.col("amount") - pl.col("_peer_amount")).abs()
                <= pl.max_horizontal(pl.lit(1.0), pl.col("amount").abs() * 2e-7)
            )
        )
        .select(keys)
    )
    bao = fresh.join(corroborated, on=keys, how="semi")
    em = peer.join(unchanged, on=keys, how="semi").join(corroborated, on=keys, how="anti")
    return bao, em


def repair_daily_bar_turnover(
    config: Config,
    start: date,
    end: date,
    run_id: str,
    symbols: list[str] | None,
) -> dict:
    """Replace stored stock bars whose turnover is missing, zero or mis-unit.

    同花顺's pre-2004 year files store 0 for turnover they lack, some of them
    count volume in 手, and Sina never publishes turnover. Baostock serves the
    same unadjusted sessions with both fields in the lake's units; its row
    replaces the stored one only when their prices agree. Rows it cannot
    serve or that disagree keep their stored value and are counted.
    """
    from cnequity.adapters.baostock.delisted_bars import fetch_delisted_bars
    from cnequity.domain.http_policy import SourceCoolingDown
    from cnequity.domain.units import turnover_defect_expr
    from cnequity.query.parquet_scan import collect_parquet_root
    from cnequity.steps.http_common import write_fetched

    scope = bar_scope._resolve_daily_bar_scope(config, symbols) if symbols else None
    totals: dict[str, int] = dict.fromkeys(
        ("rows_checked", "rows_unserved", "rows_disagreeing", "rows_replaced"), 0
    )
    peer_review = run_flags_of(config).turnover_peer_review
    if peer_review and not (
        config.sources.get("eastmoney", True)
        or config.sources.get("exchange", True)
        or config.tdx_enabled
    ):
        raise ValueError(
            "turnover peer review requires an enabled exchange, TDX or EastMoney source"
        )
    failed: set[str] = set()
    rows_written = 0
    # Find every defect first, then ask baostock once per stock for the years
    # it needs: its pacing rests after every 20 symbols of a call, so walking
    # year by year would pay that rest again for each year of each stock.
    parts: list[pl.DataFrame] = []
    for lo, hi in _yearly_slices(start, end):
        current = collect_parquet_root(
            config.curated_root / "daily_bars",
            partition_col="trade_date",
            start=lo,
            end=hi,
            symbols=scope,
        )
        if current.is_empty():
            continue
        defective = (
            dedupe_by_primary_key(current, "daily_bars")
            .with_columns(turnover_defect_expr().alias("_defect"))
            .filter(pl.col("_defect").is_not_null())
        )
        defective = defective.filter(
            pl.col("symbol").map_elements(is_baostock_stock, return_dtype=pl.Boolean)
        )
        if not defective.is_empty():
            parts.append(defective)
    stored = pl.concat(parts, how="diagonal_relaxed") if parts else pl.DataFrame()
    by_defect: dict[str, int] = (
        dict(stored.group_by("_defect").len().iter_rows()) if not stored.is_empty() else {}
    )
    spans = (
        stored.group_by("symbol")
        .agg(
            pl.col("trade_date").min().dt.year().alias("first"),
            pl.col("trade_date").max().dt.year().alias("last"),
        )
        .sort("first", "last", "symbol")
        if not stored.is_empty()
        else pl.DataFrame()
    )
    groups: dict[tuple[int, int], list[str]] = {}
    for row in spans.iter_rows(named=True):
        groups.setdefault((row["first"], row["last"]), []).append(row["symbol"])
    batch = 0
    stopped: str | None = None
    authority_failures = []
    rows_authority_confirmed = 0
    rows_tdx_confirmed = 0
    for (first, last), names in groups.items():
        lo, hi = max(start, date(first, 1, 1)), min(end, date(last, 12, 31))
        for offset in range(0, len(names), _TURNOVER_REPAIR_BATCH):
            chunk = names[offset : offset + _TURNOVER_REPAIR_BATCH]
            try:
                rows, chunk_failed = fetch_delisted_bars(chunk, lo, hi, config=config)
            except SourceCoolingDown as exc:
                # Login refused before this chunk. Rows already staged stay;
                # a rerun only asks for symbols that are still defective.
                stopped = str(exc)
                logger.warning(
                    "turnover repair stopped: %s; a rerun continues from rows still defective",
                    exc,
                )
                break
            except Exception as exc:  # noqa: BLE001 — one dead session is one batch
                # A login that times out loses this batch, not the batches
                # already staged: they still publish, and a rerun only asks
                # for rows that are still defective.
                logger.warning("turnover repair: %d symbol(s) not served: %s", len(chunk), exc)
                failed.update(chunk)
                continue
            failed.update(chunk_failed)
            fresh = pl.DataFrame(
                rows,
                schema={
                    "symbol": pl.Utf8,
                    "trade_date": pl.Date,
                    **dict.fromkeys(("open", "high", "low", "close"), pl.Float64),
                    "volume": pl.Int64,
                    "amount": pl.Float64,
                },
            )
            target = stored.filter(pl.col("symbol").is_in(chunk)).drop("_defect")
            replacement, counts = _turnover_replacements(target, fresh)
            peer_replacement = fresh.head(0)
            tdx_replacement = fresh.head(0)
            if peer_review and counts["rows_replaced"] < target.height:
                from cnequity.adapters.eastmoney.bars import fetch_daily_bars
                from cnequity.adapters.exchange.turnover_prices import (
                    fetch_exchange_turnover_prices,
                )

                unresolved = target.join(
                    replacement.select("symbol", "trade_date"),
                    on=["symbol", "trade_date"],
                    how="anti",
                )
                quotes, failures = fetch_exchange_turnover_prices(
                    unresolved, config=config, run_id=run_id
                )
                authority_failures.extend(failures)
                authoritative = _authority_turnover_replacements(unresolved, fresh, quotes)
                rows_authority_confirmed += authoritative.height
                replacement = pl.concat([replacement, authoritative], how="diagonal_relaxed")
                unresolved = unresolved.join(
                    authoritative.select("symbol", "trade_date"),
                    on=["symbol", "trade_date"],
                    how="anti",
                )
                if not unresolved.is_empty() and config.tdx_enabled:
                    from cnequity.adapters.tdx_protocol.client import fetch_daily_bars as fetch_tdx
                    from cnequity.storage.raw_archive import RawPayloadArchive

                    tdx_names = unresolved["symbol"].unique().sort().to_list()
                    try:
                        tdx_peer = fetch_tdx(tdx_names, lo, hi, config=config, backfill=True)
                    except Exception as exc:  # noqa: BLE001 — peer failure retains confirmed bars
                        authority_failures.append(
                            {"source": "tdx_protocol", "scope": tdx_names, "error": str(exc)}
                        )
                        tdx_peer = pl.DataFrame()
                    if not tdx_peer.is_empty():
                        # Native decoder output is not exact HTTP wire bytes.
                        # Record its normalized format and request scope honestly.
                        RawPayloadArchive(
                            config.meta_root, enabled=True, datasets=["daily_bars"]
                        ).archive(
                            "daily_bars",
                            tdx_peer.write_json().encode(),
                            source="tdx_protocol",
                            run_id=run_id,
                            request_params={
                                "symbols": tdx_names,
                                "start": str(lo),
                                "end": str(hi),
                                "backfill": True,
                            },
                            payload_format="normalized_json",
                        )
                    extra, tdx_replacement = _peer_turnover_replacements(
                        unresolved, fresh, tdx_peer
                    )
                    rows_tdx_confirmed += extra.height + tdx_replacement.height
                    replacement = pl.concat([replacement, extra], how="diagonal_relaxed")
                    accepted_tdx = pl.concat(
                        [
                            extra.select("symbol", "trade_date"),
                            tdx_replacement.select("symbol", "trade_date"),
                        ]
                    )
                    unresolved = unresolved.join(
                        accepted_tdx, on=["symbol", "trade_date"], how="anti"
                    )
                peer = (
                    fetch_daily_bars(
                        unresolved["symbol"].unique().sort().to_list(), lo, hi, config=config
                    )
                    if not unresolved.is_empty() and config.sources.get("eastmoney", True)
                    else fresh.head(0)
                )
                extra, peer_replacement = _peer_turnover_replacements(unresolved, fresh, peer)
                replacement = pl.concat([replacement, extra], how="diagonal_relaxed")
                accepted = pl.concat(
                    [
                        replacement.select("symbol", "trade_date"),
                        peer_replacement.select("symbol", "trade_date"),
                        tdx_replacement.select("symbol", "trade_date"),
                    ]
                )
                remaining = target.join(accepted, on=["symbol", "trade_date"], how="anti")
                _, counts = _turnover_replacements(remaining, fresh)
                counts.update(rows_checked=target.height, rows_replaced=accepted.height)
            for key, value in counts.items():
                totals[key] += value
            batch += 1
            if not tdx_replacement.is_empty():
                out = write_fetched(
                    config,
                    run_id,
                    "daily_bars",
                    tdx_replacement,
                    source="tdx_protocol",
                    batch_id=f"turnover-tdx-{batch:04d}",
                )
                rows_written += int(out.get("rows_written", 0))
            if not peer_replacement.is_empty():
                out = write_fetched(
                    config,
                    run_id,
                    "daily_bars",
                    peer_replacement,
                    source="eastmoney",
                    batch_id=f"turnover-peer-{batch:04d}",
                )
                rows_written += int(out.get("rows_written", 0))
            if replacement.is_empty():
                continue
            out = write_fetched(
                config,
                run_id,
                "daily_bars",
                replacement,
                source="baostock",
                batch_id=f"turnover-repair-{batch:04d}",
            )
            rows_written += int(out.get("rows_written", 0))
            logger.info(
                "turnover repair %s..%s, %d symbol(s): %d of %d row(s) replaced",
                lo,
                hi,
                len(chunk),
                counts["rows_replaced"],
                counts["rows_checked"],
            )
        if stopped:
            break

    findings: list[dict] = [
        {
            "dataset": "daily_bars",
            "severity": "info",
            "check": "daily_bars_turnover_repair",
            "message": (
                f"turnover repair over {start}..{end}: replaced {totals['rows_replaced']} of "
                f"{totals['rows_checked']} stock row(s) with unusable turnover"
            ),
            "source": "baostock",
            "peer_review": peer_review,
            "rows_authority_confirmed": rows_authority_confirmed,
            "rows_tdx_confirmed": rows_tdx_confirmed,
            "authority_failures": authority_failures,
            "defects": by_defect,
            **totals,
        }
    ]
    if totals["rows_disagreeing"] or totals["rows_unserved"] or failed:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_turnover_repair_skipped",
                "message": (
                    f"turnover repair left {totals['rows_disagreeing']} row(s) whose prices "
                    f"disagree with baostock, {totals['rows_unserved']} row(s) without a usable "
                    f"replacement, and {len(failed)} failed symbol request(s)"
                ),
                "source": "baostock",
                "rows_disagreeing": totals["rows_disagreeing"],
                "rows_unserved": totals["rows_unserved"],
                "failed_symbols": sorted(failed)[:50],
            }
        )
    if stopped:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_turnover_repair_stopped",
                "message": (
                    "turnover repair stopped at a BaoStock access limit; "
                    "staged replacements still publish, and the next run continues "
                    "from rows that are still defective"
                ),
                "source": "baostock",
                "error": stopped,
            }
        )
    result: dict = {
        "rows_read": totals["rows_checked"],
        "rows_written": rows_written,
        "context_updates": {"audit_findings": findings},
    }
    if len(findings) > 1:
        result["status"] = "warning"
    return result
