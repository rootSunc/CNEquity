"""EastMoney corporate actions (backup for TDX xdxr)."""

from __future__ import annotations

import logging
import math
from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.datacenter import (
    EastMoneyDatacenterError,
    fetch_datacenter,
)
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient, rate_limit_if_unconfigured
from cnequity.config import Config
from cnequity.domain.symbols import format_symbol, is_all_a_symbol
from cnequity.storage.raw_archive import RawArchiveError, RawPayloadArchive, begin_capture

logger = logging.getLogger(__name__)

_REPORT = "RPT_SHAREBONUS_DET"
# EastMoney renamed these columns (EX_DIV_DATE→EX_DIVIDEND_DATE,
# CASH_BTAX_RMB→PRETAX_BONUS_RMB, TRANSFER_RATIO→IT_RATIO) ~2026; the old names
# now 404 the whole report (code=9501). EM quotes amounts/ratios per-10-shares,
# pretax; _parse_row divides by 10 to honor the per-share CORPORATE_ACTIONS
# unit contract (schemas.py). Do NOT stage the raw per-10 values.
_EX_DATE_COL = "EX_DIVIDEND_DATE"
# Oldest ex-date a backup snapshot walks back to when the caller names no
# start.  EastMoney's corporate-action report is documented in this project
# as covering 2015-09-29 onward; keeping the implicit floor at 2016 silently
# dropped the first available quarter from the optional audit artifact.
# The report itself reaches back to 1991, but only through the daily equality
# filter. Paging the backfill that far costs a full-history walk, so the sweep
# stops here; `--eastmoney-date-repair` is how a named older date is reached.
EASTMONEY_BACKFILL_FLOOR = date(2015, 9, 29)
_BACKFILL_FLOOR = EASTMONEY_BACKFILL_FLOOR
_COLUMNS = (
    "SECURITY_CODE,SECUCODE,EX_DIVIDEND_DATE,EQUITY_RECORD_DATE,PRETAX_BONUS_RMB,"
    "BONUS_RATIO,IT_RATIO,BONUS_IT_RATIO,IMPL_PLAN_PROFILE,ASSIGN_PROGRESS"
)


def _num(value: object) -> float:
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0
    return parsed if math.isfinite(parsed) else 0.0


def _strict_num(value: object, *, field: str) -> float | None:
    """Parse an optional numeric report field without hiding malformed data."""
    if value is None or (isinstance(value, str) and value.strip() in {"", "-"}):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"EastMoney corporate_actions invalid {field}: {value!r}") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"EastMoney corporate_actions invalid {field}: {value!r}")
    return parsed


def _map_action_type(row: dict) -> str | None:
    impl = str(row.get("IMPL_PLAN_PROFILE") or "").lower()
    if "配" in impl:
        return "allotment"
    if "转" in impl:
        return "transfer"
    if "送" in impl:
        return "bonus"
    if "派" in impl or "息" in impl or "现金" in impl:
        return "cash_dividend"
    if _num(row.get("PRETAX_BONUS_RMB")) > 0:
        return "cash_dividend"
    if _num(row.get("IT_RATIO")) > 0:
        return "transfer"
    if _num(row.get("BONUS_RATIO")) > 0:
        return "bonus"
    return None


def _ex_date(row: dict) -> date | None:
    """Ex-date of one report row, or None when the row carries neither field."""
    raw = row.get(_EX_DATE_COL) or row.get("EQUITY_RECORD_DATE")
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def _parse_row(row: dict) -> dict | None:
    """Parse the first standard event in one EastMoney row.

    Kept as a small compatibility helper for callers/tests that expect one
    dict.  Fetch paths use ``_parse_rows`` so a combined cash + stock plan does
    not lose any component.
    """
    rows = _parse_rows(row)
    return rows[0] if rows else None


def _parse_rows(row: dict) -> list[dict]:
    """Expand one EastMoney plan row into one row per standard action type."""
    ex_date = _ex_date(row)
    if ex_date is None:
        return []

    secucode = str(row.get("SECUCODE") or "")
    code_part, _, suffix = secucode.partition(".")
    code = str(row.get("SECURITY_CODE") or code_part or "").zfill(6)
    if suffix in ("SH", "SZ", "BJ"):
        exchange = suffix
    elif code.startswith(("60", "68")):
        exchange = "SH"
    elif code.startswith(("43", "83", "87", "88", "92")):
        exchange = "BJ"
    else:
        exchange = "SZ"
    if not code.isdigit() or not is_all_a_symbol(code, exchange):
        return []
    symbol = format_symbol(code, exchange)

    try:
        # EM values are per-10-shares; divide by 10 for the per-share contract.
        cash_raw = _strict_num(row.get("PRETAX_BONUS_RMB"), field="PRETAX_BONUS_RMB")
        bonus_raw = _strict_num(row.get("BONUS_RATIO"), field="BONUS_RATIO")
        transfer_raw = _strict_num(row.get("IT_RATIO"), field="IT_RATIO")
    except ValueError:
        # A malformed numeric field must not become 0 and erase an event. Let
        # the fetch step fail closed so the caller retries/surfaces the source
        # drift instead of writing an incomplete corporate-action row.
        raise
    cash = (cash_raw or 0.0) / 10.0
    bonus = (bonus_raw or 0.0) / 10.0
    transfer = (transfer_raw or 0.0) / 10.0
    impl = str(row.get("IMPL_PLAN_PROFILE") or "").lower()
    combined_resolved = False
    combined_raw: float | None = None
    if bonus == 0.0 and transfer == 0.0:
        # Older/alternate report shapes expose only the combined 送转 field.
        # Preserve the total multiplier; when the plan text identifies a pure
        # 转增 plan, keep its semantic type as transfer.
        combined_raw = _strict_num(row.get("BONUS_IT_RATIO"), field="BONUS_IT_RATIO")
        combined = (combined_raw or 0.0) / 10.0
        if combined > 0:
            combined_resolved = True
            if "转" in impl and "送" not in impl:
                transfer = combined
            else:
                bonus = combined

    rows: list[dict] = []

    def add(action_type: str, *, cash_dividend=0.0, bonus_ratio=0.0, transfer_ratio=0.0):
        rows.append(
            {
                "symbol": symbol,
                "ex_date": ex_date,
                "action_type": action_type,
                "cash_dividend": cash_dividend,
                "bonus_ratio": bonus_ratio,
                "transfer_ratio": transfer_ratio,
                # RPT_SHAREBONUS_DET carries no allotment (配股) price/ratio
                # columns; allotment detail is a separate report, out of scope
                # for daily ex-date.
                "allotment_ratio": None,
                "allotment_price": None,
            }
        )

    # Once the combined 送转 field resolved bonus vs. transfer above, the
    # plan text has already done its job picking a side; falling back to it
    # again here would let the *other* type's shared "转"/"送" mention add a
    # second, phantom zero-ratio row for whichever type lost that split.
    cash_text = any(token in impl for token in ("派", "息", "现金"))
    if cash > 0:
        add("cash_dividend", cash_dividend=cash)
    elif cash_text:
        if cash_raw is None:
            raise ValueError("EastMoney corporate_actions cash-dividend plan has no numeric amount")
    if bonus > 0:
        add("bonus", bonus_ratio=bonus)
    elif not combined_resolved and "送" in impl:
        if bonus_raw is None and combined_raw is None:
            raise ValueError("EastMoney corporate_actions bonus plan has no numeric ratio")
    if transfer > 0:
        add("transfer", transfer_ratio=transfer)
    elif not combined_resolved and "转" in impl:
        if transfer_raw is None and combined_raw is None:
            raise ValueError("EastMoney corporate_actions transfer plan has no numeric ratio")
    return rows


def fetch_corporate_actions_eastmoney(
    trade_date: date,
    *,
    backfill: bool = False,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
    run_id: str | None = None,
    request_scope: str | None = None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)

    # No range predicate on the backfill path. EastMoney's datacenter rejects
    # range comparisons on date columns — "参数预处理错误: org.antlr.v4.runtime.
    # InputMismatchException (code=9501)" — the same change that broke
    # share_unlock_schedule and northbound_flows. Here it took out
    # `cne backfill corporate_actions` entirely. Equality still parses, so the
    # daily path keeps its exact-date filter; the backfill pages the report
    # newest-first (it is already sorted that way) and stops at the first page
    # that ends before the floor.
    stop_after = None
    if backfill:
        date_filter = ""
        floor = getattr(config, "_backfill_start", None) or _BACKFILL_FLOOR
        ceiling = getattr(config, "_backfill_end", None) or trade_date

        def stop_after(batch: list[dict]) -> bool:
            for item in reversed(batch):
                parsed = _ex_date(item)
                if parsed is not None:
                    return parsed < floor
            return False
    else:
        ds = trade_date.isoformat()
        date_filter = f"({_EX_DATE_COL}='{ds}')"

    retries = config.max_retries if config is not None else 3
    backoff = float(config.retry_backoff_seconds if config is not None else 5)

    try:
        try:
            rate_limit_if_unconfigured(client, config)
            archive = None
            if config is not None and hasattr(config, "meta_root"):
                archive_dataset = "corporate_actions"
                should_archive = getattr(config, "should_archive_raw", None)
                if should_archive is None or should_archive(archive_dataset):
                    capture_scope = request_scope or (
                        f"{'backfill' if backfill else 'daily'}:{trade_date.isoformat()}"
                    )
                    nonce = begin_capture(
                        config,
                        archive_dataset,
                        run_id,
                        source="eastmoney",
                        request_scope=capture_scope,
                    )
                    archive = RawPayloadArchive(
                        config.meta_root,
                        enabled=getattr(config, "raw_archive_enabled", False),
                        datasets=[archive_dataset],
                        compression=getattr(config, "raw_archive_compression", "gzip"),
                        max_payload_bytes=getattr(config, "raw_archive_max_payload_bytes", None),
                        capture_owner=config,
                        capture_run_id=run_id,
                        capture_source="eastmoney",
                        capture_scope=capture_scope,
                        capture_nonce=nonce,
                    )
            if archive is not None and archive.enabled and not run_id:
                raise RawArchiveError(
                    "EastMoney corporate_actions archive requires a non-empty run_id"
                )
            datacenter_kwargs = {
                "filter_expr": date_filter,
                "sort_columns": _EX_DATE_COL,
                "sort_types": "-1",
                "max_retries": retries,
                "retry_backoff_seconds": backoff,
                "stop_after": stop_after,
            }
            if archive is not None:
                datacenter_kwargs.update(
                    {
                        "archive": archive,
                        "archive_dataset": "corporate_actions",
                        "archive_run_id": run_id,
                    }
                )
            raw = fetch_datacenter(client, _REPORT, _COLUMNS, **datacenter_kwargs)
        except EastMoneyDatacenterError:
            raise
        except Exception as exc:
            raise EastMoneyDatacenterError(
                f"EastMoney corporate_actions failed for filter {date_filter!r}"
            ) from exc

        rows = []
        outside_daily = 0
        for item in raw:
            parsed_rows = _parse_rows(item)
            if not parsed_rows:
                continue
            for parsed in parsed_rows:
                if not backfill and parsed["ex_date"] != trade_date:
                    outside_daily += 1
                    continue
                if backfill and not (floor <= parsed["ex_date"] <= ceiling):
                    continue
                rows.append(parsed)

        if not backfill and outside_daily and not rows:
            raise EastMoneyDatacenterError(
                "EastMoney corporate_actions response contains no "
                f"EX_DIVIDEND_DATE row for {trade_date.isoformat()}"
            )
    finally:
        if owns:
            client.close()

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "ex_date", "action_type"], keep="last")
