"""Settle owed ETF sessions before listing or after delisting (official calendars)."""

from __future__ import annotations

import hashlib
import json
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.steps.bars._base import logger
from cnequity.storage.state import StateStore


def _settle_official_etf_prelisting_debts(config: Config, on: date, run_id: str) -> dict | None:
    """Retire only owed ETF sessions predating an exchange-proven listing.

    The debt ledger originally treats an undated ETF as live for every session
    in a repair window. A later official directory can establish that these
    sessions never existed. Save the pinned source and exact keys before
    removing them; source-empty or unverified price gaps remain owed.
    """
    if not (config.ingest_eligible_etfs or config.ingest_listed_funds):
        return None
    from cnequity.query import load_with_receipt
    from cnequity.query.receipt import ReadReceiptError
    from cnequity.storage.atomic import write_json_atomic

    store = StateStore(config.meta_root)
    owed = store.get_outstanding_keys("daily_bars")
    if not owed:
        return None
    try:
        read = load_with_receipt("etf_profiles", config=config, end=on, require_replayable=True)
    except (ReadReceiptError, FileNotFoundError, ValueError):
        return None
    frame = read.frame
    if not {"symbol", "as_of_date", "list_date"} <= set(frame.columns):
        return None
    listings = (
        frame.filter(
            (pl.col("as_of_date") <= on)
            & pl.col("list_date").is_not_null()
            & (pl.col("list_date") <= on)
        )
        .sort("as_of_date")
        .unique(subset=["symbol"], keep="last")
    )
    list_dates = dict(listings.select("symbol", "list_date").iter_rows())
    resolved = sorted(
        (row["symbol"], date.fromisoformat(row["trade_date"]))
        for row in owed
        if row.get("symbol") in list_dates
        and row.get("trade_date")
        and row["trade_date"] < list_dates[row["symbol"]].isoformat()
    )
    if not resolved:
        return None
    receipt = {
        "run_id": run_id,
        "dataset": "daily_bars",
        "reason": "official_etf_list_date_proves_prelisting_session",
        "profile_receipt_id": read.receipt["receipt_id"],
        "profile_revision": read.receipt["dependencies"]["etf_profiles"]["revision_id"],
        "as_of": on.isoformat(),
        "status": "prepared",
        "resolved_keys": [
            {
                "symbol": symbol,
                "trade_date": day.isoformat(),
                "official_list_date": list_dates[symbol].isoformat(),
            }
            for symbol, day in resolved
        ],
    }
    receipt_digest = hashlib.sha256(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    path = config.meta_root / "repairs" / "etf_prelisting_debts" / f"{run_id}-{receipt_digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, receipt, ensure_ascii=False, indent=2)
    remaining = store.clear_outstanding_keys("daily_bars", resolved)
    receipt["status"] = "settled"
    receipt["remaining_outstanding"] = remaining
    write_json_atomic(path, receipt, ensure_ascii=False, indent=2)
    logger.info(
        "Retired %d ETF prelisting debt(s) using exchange directory receipt %s; %d remain",
        len(resolved),
        read.receipt["receipt_id"],
        remaining,
    )
    return {
        "dataset": "daily_bars",
        "severity": "info",
        "check": "official_etf_prelisting_debts_settled",
        "message": f"{len(resolved)} ETF prelisting keys settled from official directory dates",
        "keys_settled": len(resolved),
        "profile_receipt_id": read.receipt["receipt_id"],
        "repair_receipt": str(path.relative_to(config.meta_root)),
    }


def _settle_official_etf_delisted_debts(config: Config, on: date, run_id: str) -> dict | None:
    """Retire owed ETF sessions after a delisting two sources agree on.

    A fund being wound up stops quoting, so its post-delisting tip keys can
    never be filled. A key is retired only when curated instruments carry a
    delist date before the session and the latest official exchange directory
    no longer lists a fund an earlier snapshot did. A fund the directory has
    never covered (LOFs) has only the instruments delisting, which already
    takes repeated snapshot absences and leaves the fetch scope on its own.
    The pinned directory receipt and exact keys are saved first.
    """
    if not (config.ingest_eligible_etfs or config.ingest_listed_funds):
        return None
    from cnequity.query import load_with_receipt
    from cnequity.query.receipt import ReadReceiptError
    from cnequity.storage.atomic import write_json_atomic
    from cnequity.storage.instrument_catalog import load_curated_instruments

    store = StateStore(config.meta_root)
    owed = store.get_outstanding_keys("daily_bars")
    if not owed:
        return None
    instruments = load_curated_instruments(config)
    if instruments is None or not {"symbol", "asset_type", "delist_date"} <= set(
        instruments.columns
    ):
        return None
    delisted = dict(
        instruments.filter(
            pl.col("asset_type").is_in(["etf", "lof"]) & pl.col("delist_date").is_not_null()
        )
        .select("symbol", "delist_date")
        .iter_rows()
    )
    candidates = [
        row
        for row in owed
        if row.get("symbol") in delisted
        and row.get("trade_date")
        and row["trade_date"] > delisted[row["symbol"]].isoformat()
    ]
    if not candidates:
        return None
    try:
        read = load_with_receipt("etf_profiles", config=config, end=on, require_replayable=True)
    except (ReadReceiptError, FileNotFoundError, ValueError):
        return None
    frame = read.frame
    if not {"symbol", "as_of_date"} <= set(frame.columns):
        return None
    frame = frame.filter(pl.col("as_of_date") <= on)
    if frame.is_empty():
        return None
    latest = frame["as_of_date"].max()
    listed_now = set(frame.filter(pl.col("as_of_date") == latest)["symbol"].to_list())
    listed_before = set(frame.filter(pl.col("as_of_date") < latest)["symbol"].to_list())
    listed_ever = listed_before | listed_now
    resolved = sorted(
        (row["symbol"], date.fromisoformat(row["trade_date"]))
        for row in candidates
        if row["symbol"] not in listed_now
        and (row["symbol"] in listed_before or row["symbol"] not in listed_ever)
    )
    if not resolved:
        return None
    receipt = {
        "run_id": run_id,
        "dataset": "daily_bars",
        "reason": "instruments_delisting_confirmed_by_directory_where_covered",
        "profile_receipt_id": read.receipt["receipt_id"],
        "profile_revision": read.receipt["dependencies"]["etf_profiles"]["revision_id"],
        "directory_as_of": latest.isoformat(),
        "as_of": on.isoformat(),
        "status": "prepared",
        "resolved_keys": [
            {
                "symbol": symbol,
                "trade_date": day.isoformat(),
                "instrument_delist_date": delisted[symbol].isoformat(),
            }
            for symbol, day in resolved
        ],
    }
    receipt_digest = hashlib.sha256(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    path = config.meta_root / "repairs" / "etf_delisted_debts" / f"{run_id}-{receipt_digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, receipt, ensure_ascii=False, indent=2)
    remaining = store.clear_outstanding_keys("daily_bars", resolved)
    receipt["status"] = "settled"
    receipt["remaining_outstanding"] = remaining
    write_json_atomic(path, receipt, ensure_ascii=False, indent=2)
    logger.info(
        "Retired %d delisted ETF debt(s) using exchange directory receipt %s; %d remain",
        len(resolved),
        read.receipt["receipt_id"],
        remaining,
    )
    return {
        "dataset": "daily_bars",
        "severity": "info",
        "check": "official_etf_delisted_debts_settled",
        "message": f"{len(resolved)} delisted ETF keys settled from directory absence",
        "keys_settled": len(resolved),
        "profile_receipt_id": read.receipt["receipt_id"],
        "repair_receipt": str(path.relative_to(config.meta_root)),
    }
