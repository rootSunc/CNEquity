"""TDX xdxr (除权除息) → corporate_actions schema."""

from __future__ import annotations

import logging
import math
from collections.abc import Callable
from datetime import date

import polars as pl

from cnequity.adapters.tdx_protocol.session import close_quotes_client
from cnequity.domain.rate_limit import RateLimitSpec, source_request_slot_spec, wait_spec
from cnequity.progress import sweep_progress
from cnequity.storage.raw_archive import RawArchiveError, RawPayloadArchive, begin_capture

logger = logging.getLogger(__name__)


def _configured_archive(
    config,
    dataset: str,
    *,
    run_id: str | None = None,
    request_scope: str | None = None,
) -> RawPayloadArchive | None:
    """Build a strict archive for an explicitly governed source adapter."""
    if config is None or not hasattr(config, "meta_root"):
        return None
    should_archive = getattr(config, "should_archive_raw", None)
    if callable(should_archive) and not should_archive(dataset):
        return None
    if not bool(getattr(config, "raw_archive_enabled", True)):
        return None
    scope = str(request_scope or f"dataset:{dataset}")
    nonce = begin_capture(config, dataset, run_id, source="tdx_protocol", request_scope=scope)
    return RawPayloadArchive(
        config.meta_root,
        enabled=True,
        datasets=[dataset],
        compression=getattr(config, "raw_archive_compression", "gzip"),
        max_payload_bytes=getattr(config, "raw_archive_max_payload_bytes", None),
        capture_owner=config,
        capture_run_id=run_id,
        capture_source="tdx_protocol",
        capture_scope=scope,
        capture_nonce=nonce,
    )


_ACTION_TYPES = {
    "cash_dividend": "cash_dividend",
    "bonus": "bonus",
    "transfer": "transfer",
    "allotment": "allotment",
}


def _num(value: object) -> float:
    try:
        parsed = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    return parsed if math.isfinite(parsed) else 0.0


def _event_num(value: object) -> float:
    """Parse a core action amount without turning malformed data into zero."""
    if value is None or value == "" or value == "-":
        return 0.0
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"invalid corporate-action amount: {value!r}") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"invalid corporate-action amount: {value!r}")
    return parsed


def _rows_from_xdxr(symbol: str, pdf: pl.DataFrame) -> list[dict]:
    rows: list[dict] = []
    for record in pdf.iter_rows(named=True):
        year = record.get("year")
        month = record.get("month")
        day = record.get("day")
        if not all(v is not None for v in (year, month, day)):
            continue
        try:
            ex_date = date(int(year), int(month), int(day))
            category = int(record.get("category") or 0)
        except (TypeError, ValueError, OverflowError):
            continue
        if category != 1:
            continue

        try:
            # These fields decide whether an event row exists. A malformed
            # value must not quietly become 0 and erase a real action.
            fenhong = _event_num(record.get("fenhong"))
            songzhuangu = _event_num(record.get("songzhuangu"))
            peigu = _event_num(record.get("peigu"))
        except ValueError as exc:
            logger.warning("TDX xdxr: skipping row with invalid event amount: %s", exc)
            continue
        # The allotment price is optional in the upstream payload; retain the
        # action with a null price when it is absent or malformed.
        peigujia = _num(record.get("peigujia"))

        if fenhong > 0:
            rows.append(
                {
                    "symbol": symbol,
                    "ex_date": ex_date,
                    "action_type": _ACTION_TYPES["cash_dividend"],
                    "cash_dividend": fenhong / 10.0,
                    "bonus_ratio": 0.0,
                    "transfer_ratio": 0.0,
                    "allotment_ratio": None,
                    "allotment_price": None,
                }
            )
        if songzhuangu > 0:
            rows.append(
                {
                    "symbol": symbol,
                    "ex_date": ex_date,
                    "action_type": _ACTION_TYPES["bonus"],
                    "cash_dividend": 0.0,
                    # per-share contract: TDX songzhuangu is 每10股 (combined
                    # 送+转); divide by 10. All total goes to bonus_ratio —
                    # xdxr does not split 送 vs 转, but total mult is exact.
                    "bonus_ratio": songzhuangu / 10.0,
                    "transfer_ratio": 0.0,
                    "allotment_ratio": None,
                    "allotment_price": None,
                }
            )
        if peigu > 0:
            rows.append(
                {
                    "symbol": symbol,
                    "ex_date": ex_date,
                    "action_type": _ACTION_TYPES["allotment"],
                    "cash_dividend": 0.0,
                    "bonus_ratio": 0.0,
                    "transfer_ratio": 0.0,
                    # per-share contract: peigu is 每10股, divide by 10.
                    # peigujia is already a per-share price — leave as-is.
                    "allotment_ratio": peigu / 10.0,
                    "allotment_price": peigujia if peigujia > 0 else None,
                }
            )
    return rows


def fetch_xdxr_for_symbol(
    client,
    symbol: str,
    *,
    rate_limit: RateLimitSpec | None = None,
    on_date: date | None = None,
    strict: bool = False,
    archive: RawPayloadArchive | None = None,
    archive_dataset: str = "corporate_actions",
    archive_run_id: str | None = None,
    request_scope: str | None = None,
) -> pl.DataFrame:
    wait_spec(rate_limit)
    code, _, exch = symbol.partition(".")
    # ``quotes.xdxr()`` falls back to ``market_for_stock()`` when market is
    # omitted, and that heuristic only distinguishes SH/SZ — it has no notion
    # of 北交所 at all, so every BJ symbol silently queried market=0 (深圳) and
    # got back an empty (not erroring) result. Confirmed live: market=0 returns
    # 0 events for every BJ code sampled; market=2 (北京) returns real ones for
    # the same codes (920002.BJ: 15 events, 920014.BJ: 34, ...). This mirrors
    # the resolution `fetch_bars_paginated` already does correctly for daily
    # bars — the fix here is applying that same pattern to xdxr.
    market = 1 if exch == "SH" else (0 if exch == "SZ" else 2)
    try:
        with source_request_slot_spec(rate_limit):
            raw = client.xdxr(symbol=code, market=market)
    except Exception as exc:
        logger.debug("TDX xdxr failed for %s: %s", symbol, exc)
        if archive is not None and archive.enabled:
            raise RawArchiveError(
                f"TDX corporate_actions {symbol}: exact wire response unavailable"
            ) from exc
        if strict:
            raise RuntimeError(f"TDX xdxr failed for {symbol}") from exc
        return pl.DataFrame()

    if archive is not None and archive.enabled:
        wire = getattr(client, "last_response_wire", None)
        if isinstance(wire, bytearray):
            wire = bytes(wire)
        elif isinstance(wire, memoryview):
            wire = wire.tobytes()
        if not isinstance(wire, bytes):
            raise RawArchiveError(
                f"TDX corporate_actions {symbol}: response has no exact wire bytes"
            )
        archive.archive(
            archive_dataset,
            wire,
            source="tdx_protocol",
            request_params={"symbol": code, "market": market},
            run_id=archive_run_id,
            payload_format="bytes",
            http_metadata={"wire_exact": True, "protocol": "tdx", "compressed_frame": True},
            observation_id=(
                f"{archive_run_id or 'anonymous'}:xdxr:{symbol}:"
                f"scope={request_scope or 'scope-unknown'}"
            ),
            request_scope=request_scope,
        )

    if raw is None or len(raw) == 0:
        return pl.DataFrame()

    pdf = pl.from_pandas(raw) if hasattr(raw, "columns") else pl.DataFrame(raw)
    rows = _rows_from_xdxr(symbol, pdf)
    if on_date is not None:
        rows = [r for r in rows if r["ex_date"] == on_date]
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "ex_date", "action_type"], keep="last")


def fetch_corporate_actions_tdx(
    symbols: list[str],
    *,
    trade_date: date | None = None,
    backfill: bool = False,
    client_factory,
    rate_limit: RateLimitSpec | None = None,
    strict: bool = False,
    on_progress: Callable[[int, int], None] | None = None,
    config=None,
    run_id: str | None = None,
    archive: RawPayloadArchive | None = None,
    request_scope: str | None = None,
) -> pl.DataFrame:
    if archive is None:
        archive = _configured_archive(
            config,
            "corporate_actions",
            run_id=run_id,
            request_scope=request_scope,
        )
    if archive is not None and archive.enabled and not run_id:
        raise RawArchiveError("TDX corporate_actions archive requires a non-empty run_id")
    client = None
    frames: list[pl.DataFrame] = []
    on_date = None if backfill else trade_date
    total = len(symbols)
    report = sweep_progress(logger, "corporate_actions TDX xdxr", total)
    try:
        # ``client_factory`` returns a client owned by this invocation.  The
        # socket is touched by one thread only, so no global session lock is
        # needed; holding one would block unrelated daily/minute lanes.
        client = client_factory()
        for index, sym in enumerate(symbols, start=1):
            df = fetch_xdxr_for_symbol(
                client,
                sym,
                rate_limit=rate_limit,
                on_date=on_date,
                strict=strict,
                archive=archive,
                archive_run_id=run_id,
                request_scope=request_scope,
            )
            if df.height:
                frames.append(df)
            if on_progress is not None:
                on_progress(index, total)
            report(index)
    finally:
        close_quotes_client(client)

    if not frames:
        return pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "ex_date": pl.Date,
                "action_type": pl.Utf8,
                "cash_dividend": pl.Float64,
                "bonus_ratio": pl.Float64,
                "transfer_ratio": pl.Float64,
                "allotment_ratio": pl.Float64,
                "allotment_price": pl.Float64,
            }
        )

    out = pl.concat(frames, how="diagonal_relaxed")
    return out.unique(subset=["symbol", "ex_date", "action_type"], keep="last")
