"""Fill missing traded amounts: BSE tip bars and Sina history, from TDX."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.frames import with_columns_unless_blank
from cnequity.steps.bars import bar_scope
from cnequity.steps.bars._base import _yearly_slices, logger

# TDX counts Beijing volume in lots; anything further apart is a different row.
_BJ_AMOUNT_REPAIR_LOT = 100
_BJ_AMOUNT_REPAIR_BATCH = 40


def repair_bse_tip_amounts_from_curated(
    config: Config,
    trade_date: date,
    run_id: str,
    symbols: list[str],
) -> dict:
    """Supplement an existing BJ session without re-fetching Sina history.

    This targeted repair uses the curated OHLCV as the cross-check input and
    queries BSE once for its current snapshot. Only rows that receive a
    non-null BSE amount are staged; no price, volume, or historical row is
    invented.
    """
    from cnequity.query.parquet_scan import collect_parquet_root
    from cnequity.steps.http_common import write_fetched

    target = bar_scope._resolve_daily_bar_scope(config, symbols)
    current = collect_parquet_root(
        config.curated_root / "daily_bars",
        partition_col="trade_date",
        start=trade_date,
        end=trade_date,
        symbols=target,
    )
    current = dedupe_by_primary_key(current, "daily_bars")
    if current.is_empty():
        raise RuntimeError(f"daily_bars {trade_date}: no curated rows found for the repair scope")

    observed = set(current.get_column("symbol").to_list())
    missing = sorted(set(target) - observed)
    candidate = current.filter(pl.col("amount").is_null())
    updated, findings = _supplement_bse_tip_amounts(
        config,
        candidate,
        trade_date=trade_date,
        symbols=target,
    )
    if "source" not in updated.columns:
        updated = with_columns_unless_blank(updated, pl.lit("sina").alias("source"))
    changed = updated.filter(pl.col("amount").is_not_null() & (pl.col("source") == "bse"))
    if not changed.is_empty():
        out = write_fetched(
            config,
            run_id,
            "daily_bars",
            changed,
            source="bse",
            batch_id="bse-tip-repair-0000",
        )
    else:
        out = {"rows_read": 0, "rows_written": 0}

    if missing:
        preview = ", ".join(missing[:8])
        suffix = "..." if len(missing) > 8 else ""
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_bse_tip_repair_missing_rows",
                "message": (
                    f"{len(missing)} requested BJ symbol(s) have no curated row on "
                    f"{trade_date}: {preview}{suffix}"
                ),
                "source": "bse",
                "missing_symbols": len(missing),
            }
        )
    result = {
        "rows_read": current.height,
        "rows_written": int(out.get("rows_written", 0)),
    }
    if findings:
        result["context_updates"] = {"audit_findings": findings}
        if any(
            f.get("severity") == "warning" or f.get("check") == "daily_bars_bse_amount_unavailable"
            for f in findings
        ):
            result["status"] = "warning"
    return result


def _supplement_bj_amounts_from_tdx(
    config: Config,
    merged: pl.DataFrame,
    *,
    start: date,
    end: date,
    subject: str = "Beijing",
) -> tuple[pl.DataFrame, list[dict]]:
    """Fill Beijing turnover TDX serves and Sina never published.

    Sina is the historical fallback for Beijing bars and exposes no turnover at
    all: every one of the 505,518 Sina rows in the lake carries a null amount.
    TDX serves the board under market id 2 and does publish it.

    The gate is what makes this a supplement rather than a re-fetch. TDX must
    agree on open/high/low/close to the last digit — measured across 2,048 rows
    against Sina and 3,086 against BSE/Sina/THS, with no exception — and its
    volume must sit within one lot of the stored figure, which is the only way
    the two ever differ (TDX counts in lots; Sina did too through 2025 and has
    been exact since 2026). Anything outside that is a different row, and the
    stored one is left alone as an audit finding.

    Only `amount` is written. Keeping the stored volume costs nothing and saves
    the 7,270 rows where Sina is the finer of the two.
    """
    from cnequity.adapters.tdx_protocol.client import fetch_daily_bars

    symbols = sorted(set(merged.get_column("symbol").to_list()))
    if not symbols:
        return merged, []
    frames: list[pl.DataFrame] = []
    failed: list[str] = []
    for index in range(0, len(symbols), _BJ_AMOUNT_REPAIR_BATCH):
        chunk = symbols[index : index + _BJ_AMOUNT_REPAIR_BATCH]
        try:
            frame = fetch_daily_bars(chunk, start, end, config=config)
        except Exception as exc:  # noqa: BLE001 — the stored rows stay as they are
            logger.warning(
                "TDX Beijing amount supplement failed for %d symbol(s): %s", len(chunk), exc
            )
            failed.extend(chunk)
            continue
        if not frame.is_empty():
            frames.append(
                frame.select(
                    "symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"
                )
            )
    findings: list[dict] = []
    if failed:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_tdx_amount_unavailable",
                "message": (
                    f"TDX served no {subject} history for {len(failed)} symbol(s) over {start}..{end}"
                ),
                "source": "tdx_protocol",
                "source_limited": True,
            }
        )
    if not frames:
        return merged, findings

    tdx = pl.concat(frames, how="vertical").rename(
        {
            "open": "_tdx_open",
            "high": "_tdx_high",
            "low": "_tdx_low",
            "close": "_tdx_close",
            "volume": "_tdx_volume",
            "amount": "_tdx_amount",
        }
    )
    if "source" not in merged.columns:
        merged = with_columns_unless_blank(merged, pl.lit("sina").alias("source"))
    joined = merged.join(tdx, on=["symbol", "trade_date"], how="left")
    prices_match = pl.all_horizontal(
        pl.col(left) == pl.col(right)
        for left, right in (
            ("open", "_tdx_open"),
            ("high", "_tdx_high"),
            ("low", "_tdx_low"),
            ("close", "_tdx_close"),
        )
    )
    within_a_lot = (pl.col("volume") - pl.col("_tdx_volume")).abs() < _BJ_AMOUNT_REPAIR_LOT
    supplement = (
        pl.col("_tdx_amount").is_not_null()
        & pl.col("amount").is_null()
        & prices_match
        & within_a_lot
    )
    rejected = joined.filter(pl.col("_tdx_amount").is_not_null() & ~(prices_match & within_a_lot))
    # Every candidate has to land somewhere. A key TDX simply did not serve is
    # neither supplied nor rejected, and counting it nowhere is how a repair
    # reports success over rows it never touched — the Beijing board's retired
    # 8xxxxx/430xxx codes are ~218,000 such rows.
    unserved = joined.filter(pl.col("_tdx_amount").is_null())
    supplemented = joined.filter(supplement)
    updated = joined.with_columns(
        pl.when(supplement).then(pl.col("_tdx_amount")).otherwise(pl.col("amount")).alias("amount"),
        pl.when(supplement)
        .then(pl.lit("tdx_protocol"))
        .otherwise(pl.col("source"))
        .alias("source"),
    ).drop("_tdx_open", "_tdx_high", "_tdx_low", "_tdx_close", "_tdx_volume", "_tdx_amount")
    if supplemented.height:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_tdx_amount_supplement",
                "message": (
                    f"TDX supplied amount for {supplemented.height} {subject} row(s) over "
                    f"{start}..{end} after exact OHLC matching"
                ),
                "source": "tdx_protocol",
                "rows_supplemented": supplemented.height,
                "symbols_supplemented": supplemented.get_column("symbol").n_unique(),
            }
        )
    if unserved.height:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_tdx_amount_unserved",
                "message": (
                    f"TDX served no row for {unserved.height} {subject} key(s) over {start}..{end} "
                    f"across {unserved.get_column('symbol').n_unique()} symbol(s); "
                    "those rows keep their null amount"
                ),
                "source": "tdx_protocol",
                "source_limited": True,
                "rows_unserved": unserved.height,
            }
        )
    if rejected.height:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_tdx_amount_mismatch",
                "message": (
                    f"TDX disagreed on price or volume for {rejected.height} {subject} row(s) over "
                    f"{start}..{end}; those rows keep their null amount"
                ),
                "source": "tdx_protocol",
                "rows_rejected": rejected.height,
            }
        )
    return updated, findings


def _sina_history_amount_rows(frame: pl.DataFrame) -> pl.DataFrame:
    """Sina daily bars on SH/SZ/BJ whose turnover was never published.

    Delisted names land here because TDX serves nothing once a code has left
    the market, and the daily gap-fill then keeps Sina's row. Asking TDX again
    is still the right pass: a code it can still see gets an amount, and a
    code it cannot stays on the stored row.
    """
    if frame.is_empty() or "source" not in frame.columns:
        return frame.clear()
    return frame.filter(
        pl.col("amount").is_null()
        & (pl.col("source") == "sina")
        & pl.col("symbol").str.slice(-3).is_in([".SH", ".SZ", ".BJ"])
    )


def _repair_null_amounts_from_tdx(
    config: Config,
    start: date,
    end: date,
    run_id: str,
    *,
    symbols: list[str] | None,
    select,
    batch_prefix: str,
    log_name: str,
    subject: str,
) -> dict:
    """Walk *start*..*end* a year at a time and stage rows *select* keeps."""
    from cnequity.query.parquet_scan import collect_parquet_root
    from cnequity.steps.http_common import write_fetched

    rows_read = rows_written = 0
    findings: list[dict] = []
    for index, (lo, hi) in enumerate(_yearly_slices(start, end)):
        current = collect_parquet_root(
            config.curated_root / "daily_bars",
            partition_col="trade_date",
            start=lo,
            end=hi,
            symbols=symbols,
        )
        if current.is_empty():
            continue
        candidate = select(dedupe_by_primary_key(current, "daily_bars"))
        if candidate.is_empty():
            continue
        rows_read += candidate.height
        updated, slice_findings = _supplement_bj_amounts_from_tdx(
            config, candidate, start=lo, end=hi, subject=subject
        )
        findings.extend(slice_findings)
        changed = updated.filter(
            pl.col("amount").is_not_null() & (pl.col("source") == "tdx_protocol")
        )
        if changed.is_empty():
            continue
        out = write_fetched(
            config,
            run_id,
            "daily_bars",
            changed,
            source="tdx_protocol",
            batch_id=f"{batch_prefix}-{index:04d}",
        )
        rows_written += int(out.get("rows_written", 0))
        logger.info(
            "%s %s..%s: %d/%d row(s) supplied",
            log_name,
            lo,
            hi,
            changed.height,
            candidate.height,
        )

    result: dict = {"rows_read": rows_read, "rows_written": rows_written}
    if findings:
        result["context_updates"] = {"audit_findings": findings}
        if any(f.get("severity") == "warning" for f in findings):
            result["status"] = "warning"
    return result


def repair_sina_history_amounts_from_tdx(
    config: Config,
    start: date,
    end: date,
    run_id: str,
    symbols: list[str] | None,
) -> dict:
    """Fill turnover on Sina-sourced SH/SZ/BJ history from TDX, when TDX has it.

    The stored price and volume stay. A delisted code TDX no longer serves is
    counted and left on its Sina row; that absence is not a failed bar.
    """
    scope = bar_scope._resolve_daily_bar_scope(config, symbols) if symbols else None
    return _repair_null_amounts_from_tdx(
        config,
        start,
        end,
        run_id,
        symbols=scope,
        select=_sina_history_amount_rows,
        batch_prefix="tdx-amount-repair",
        log_name="TDX amount repair",
        subject="Sina history",
    )


def _supplement_bse_tip_amounts(
    config: Config,
    merged: pl.DataFrame,
    *,
    trade_date: date,
    symbols: list[str],
) -> tuple[pl.DataFrame, list[dict]]:
    """Fill only BJ tip turnover that passes an official OHLCV cross-check.

    Sina is the historical fallback for Beijing bars but does not expose
    turnover. BSE's quotation endpoint is a current snapshot, so it is not a
    history source: a row is eligible only when its session is the requested
    date and every OHLCV field agrees exactly with the Sina row already staged.
    A mismatch keeps the Sina row unchanged and becomes an audit finding.
    """
    from cnequity.domain.market_profile import served

    bse_symbols = sorted(set(served("bse_boards", symbols)))
    if not bse_symbols or not config.sources.get("bse", False):
        return merged, []

    from cnequity.adapters.bse.daily_quotes import fetch_daily_quotes

    try:
        bse = fetch_daily_quotes(trade_date, symbols=bse_symbols, config=config)
    except Exception as exc:  # noqa: BLE001 — Sina remains the usable fallback
        logger.warning("BSE tip turnover supplement failed for %s: %s", trade_date, exc)
        return merged, [
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_bse_amount_unavailable",
                "message": f"BSE tip quote unavailable for {trade_date}: {exc}",
                "source": "bse",
                "source_limited": True,
            }
        ]

    if bse.is_empty():
        return merged, [
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_bse_amount_unavailable",
                "message": (
                    f"BSE returned no dated tip quote for {trade_date}; Sina amount stays null"
                ),
                "source": "bse",
                "source_limited": True,
            }
        ]

    if "source" not in merged.columns:
        merged = with_columns_unless_blank(merged, pl.lit("sina").alias("source"))
    bse = bse.select(
        "symbol",
        "trade_date",
        pl.col("open").alias("_bse_open"),
        pl.col("high").alias("_bse_high"),
        pl.col("low").alias("_bse_low"),
        pl.col("close").alias("_bse_close"),
        pl.col("volume").alias("_bse_volume"),
        pl.col("amount").alias("_bse_amount"),
    )
    joined = merged.join(bse, on=["symbol", "trade_date"], how="left")
    bse_present = pl.col("_bse_amount").is_not_null()
    exact_match = pl.all_horizontal(
        pl.col(left) == pl.col(right)
        for left, right in (
            ("open", "_bse_open"),
            ("high", "_bse_high"),
            ("low", "_bse_low"),
            ("close", "_bse_close"),
            ("volume", "_bse_volume"),
        )
    )
    amount_missing = pl.col("amount").is_null()
    supplement = bse_present & exact_match & amount_missing
    mismatch = bse_present & ~exact_match
    supplemented = joined.filter(supplement)
    mismatched = joined.filter(mismatch)
    updated = joined.with_columns(
        pl.when(supplement).then(pl.col("_bse_amount")).otherwise(pl.col("amount")).alias("amount"),
        pl.when(supplement).then(pl.lit("bse")).otherwise(pl.col("source")).alias("source"),
    ).drop(
        "_bse_open",
        "_bse_high",
        "_bse_low",
        "_bse_close",
        "_bse_volume",
        "_bse_amount",
    )
    findings: list[dict] = []
    if supplemented.height:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_bse_amount_supplement",
                "message": (
                    f"BSE official tip quote supplied amount for {supplemented.height} row(s) "
                    f"on {trade_date} after exact Sina OHLCV matching"
                ),
                "source": "bse",
                "rows_supplemented": supplemented.height,
                "symbols_supplemented": supplemented.get_column("symbol").n_unique(),
            }
        )
    if mismatched.height:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_bse_quote_mismatch",
                "message": (
                    f"BSE tip quote disagreed with Sina OHLCV for {mismatched.height} row(s) "
                    f"on {trade_date}; Sina rows were retained"
                ),
                "source": "bse",
                "rows_mismatched": mismatched.height,
            }
        )
    return updated, findings
