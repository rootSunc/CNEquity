#!/usr/bin/env python
"""One-off probe for issue #8 (trade_ticks). Measures, writes nothing to the lake.

The dataset's shape hangs on numbers nobody has: how deep TDX serves 分笔, how
many records a session really holds, what ``direction`` means, whether a
re-fetch of a settled session is byte-identical (which is what decides whether
a positional ``tick_seq`` can be a primary key). Everything here is read-only
and prints; nothing is staged, curated or cached.

    .venv/bin/python scripts/probe_trade_ticks.py --json-out /tmp/ticks.json

Not part of the package. Delete it once the findings are in the issue.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from cn_market_lake.adapters.tdx_protocol._wire import MAX_TICK_PAGE
from cn_market_lake.adapters.tdx_protocol.client import _quotes_client
from cn_market_lake.config import load_config
from cn_market_lake.domain.rate_limit import wait_spec
from cn_market_lake.query.parquet_scan import parquet_glob

# A liquidity spread plus the awkward cases: a fund (whose price coefficient is
# 0.001, not the stocks' 0.01), and a Beijing name (which has no TDX route at
# all on the bar path — the tick path should say so rather than look empty).
SYMBOLS = [
    "600519.SH",  # SH main board, heavily traded
    "000001.SZ",  # SZ main board, heavily traded
    "300750.SZ",  # ChiNext
    "688981.SH",  # STAR
    "603005.SH",  # mid cap
    "600107.SH",  # least-traded ordinary stock on the reference date
    "510300.SH",  # ETF — price scale check
    "920003.BJ",  # Beijing — expected to have no route
]

REFERENCE_DATE = date(2026, 7, 31)


def market_of(symbol: str) -> int:
    return 1 if symbol.endswith(".SH") else 0


def fetch_session(client, symbol: str, on_date: date, spec) -> tuple[list[dict], int, float]:
    """Every record of one session, oldest-first. Returns (rows, pages, seconds).

    ``start`` counts back from the session's *last* record, so pages arrive
    newest-block-first and are prepended. The walk stops on a short page, which
    is what marks the far edge — an empty page only happens when the whole
    session is a multiple of the page size.
    """
    code = symbol.split(".")[0]
    market = market_of(symbol)
    rows: list[dict] = []
    pages = 0
    started = time.time()
    start = 0
    while True:
        wait_spec(spec)
        page = client.ticks_history(code, on_date, market=market, start=start, offset=MAX_TICK_PAGE)
        pages += 1
        if not page:
            break
        rows = list(page) + rows
        if len(page) < MAX_TICK_PAGE:
            break
        start += MAX_TICK_PAGE
    return rows, pages, time.time() - started


def summarise(rows: list[dict]) -> dict:
    if not rows:
        return {"records": 0}
    times = [r["time"] for r in rows]
    return {
        "records": len(rows),
        "first_time": times[0],
        "last_time": times[-1],
        "distinct_minutes": len(set(times)),
        "max_records_per_minute": Counter(times).most_common(1)[0][1],
        "sum_vol": sum(r["vol"] for r in rows),
        "direction_counts": dict(sorted(Counter(r["direction"] for r in rows).items())),
        "price_raw_min": min(r["price_raw"] for r in rows),
        "price_raw_max": max(r["price_raw"] for r in rows),
    }


def daily_reference(config, on_date: date) -> dict[str, dict]:
    """`daily_bars` for the reference session, for the unit reconciliation."""
    root = config.curated_root / "daily_bars"
    frame = (
        pl.scan_parquet(parquet_glob(root))
        .filter(pl.col("trade_date") == pl.lit(on_date))
        .select("symbol", "volume", "amount", "close", "data_version")
        .collect()
    )
    return {r["symbol"]: r for r in frame.iter_rows(named=True)}


def trading_days(config, start: date, end: date) -> list[date]:
    root = config.curated_root / "trading_calendar"
    frame = (
        pl.scan_parquet(parquet_glob(root))
        .filter(
            (pl.col("trade_date") >= pl.lit(start))
            & (pl.col("trade_date") <= pl.lit(end))
            & pl.col("is_trading")
        )
        .select("trade_date")
        .collect()
    )
    return sorted(frame["trade_date"].to_list())


def probe_depth(client, symbol: str, days: list[date], spec) -> dict:
    """Earliest session still served, by bisection over the trading calendar.

    One tiny request per candidate — enough to answer "is there anything here",
    which is the only question the search asks.
    """
    code = symbol.split(".")[0]
    market = market_of(symbol)
    requests = 0

    def served(day: date) -> bool:
        nonlocal requests
        wait_spec(spec)
        requests += 1
        try:
            return bool(client.ticks_history(code, day, market=market, start=0, offset=10))
        except Exception:
            return False

    if not served(days[-1]):
        return {"earliest": None, "requests": requests, "note": "no data even on the newest day"}

    low, high = 0, len(days) - 1  # days[high] served, days[low] unknown
    if served(days[low]):
        return {
            "earliest": days[low].isoformat(),
            "trading_days_back": len(days),
            "requests": requests,
            "note": "served at the oldest day probed — the real edge is older",
        }
    while high - low > 1:
        mid = (low + high) // 2
        if served(days[mid]):
            high = mid
        else:
            low = mid
    return {
        "earliest": days[high].isoformat(),
        "trading_days_back": len(days) - high,
        "requests": requests,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="configs/cn-market-lake.toml")
    parser.add_argument("--date", default=REFERENCE_DATE.isoformat())
    parser.add_argument("--json-out", default=None)
    parser.add_argument("--skip-depth", action="store_true")
    args = parser.parse_args()

    reference = date.fromisoformat(args.date)
    config = load_config(args.config)
    spec = config.tdx_rate_limit_spec()
    client = _quotes_client(config)
    daily = daily_reference(config, reference)
    out: dict = {"server": list(client.server), "reference_date": reference.isoformat()}

    print(f"server {client.server}  reference session {reference}\n")

    # --- M1-2/3/4/5: one full session per symbol -------------------------
    sessions: dict[str, dict] = {}
    for symbol in SYMBOLS:
        try:
            rows, pages, seconds = fetch_session(client, symbol, reference, spec)
        except Exception as exc:  # noqa: BLE001 - a probe reports, it does not raise
            sessions[symbol] = {"error": f"{type(exc).__name__}: {exc}"}
            print(f"{symbol}: FAILED {type(exc).__name__}: {exc}")
            continue
        entry = summarise(rows)
        entry["pages"] = pages
        entry["seconds"] = round(seconds, 2)

        row = daily.get(symbol)
        if row and rows and row["volume"]:
            # Ticks report lots; the lake stores shares. If this ratio lands on
            # 1.0 the source is lots, if it lands on 0.01 it is already shares.
            entry["vol_lots_ratio"] = round(entry["sum_vol"] * 100 / row["volume"], 6)
            if row["amount"]:
                turnover = sum(r["price_raw"] / 100 * r["vol"] * 100 for r in rows)
                entry["amount_ratio"] = round(turnover / row["amount"], 6)
            entry["daily_close"] = row["close"]
        # After-hours records: same price as the close, own direction code.
        entry["after_1500"] = sum(1 for r in rows if (r["hour"], r["minute"]) > (15, 0))
        sessions[symbol] = entry
        print(
            f"{symbol}: {entry['records']:>5} rec / {pages} page(s) in {entry['seconds']}s  "
            f"{entry.get('first_time')}–{entry.get('last_time')}  "
            f"dirs={entry.get('direction_counts')}  "
            f"vol_ratio={entry.get('vol_lots_ratio')}  amt_ratio={entry.get('amount_ratio')}"
        )
    out["sessions"] = sessions

    # --- M1-6: is a settled session reproducible? ------------------------
    print("\nreproducibility (same settled session, fetched twice)")
    repeat: dict[str, dict] = {}
    for symbol in ("600519.SH", "000001.SZ"):
        first = sessions.get(symbol, {})
        if not first.get("records"):
            continue
        rows, _, _ = fetch_session(client, symbol, reference, spec)
        again = summarise(rows)
        identical = all(again.get(k) == first.get(k) for k in ("records", "sum_vol", "first_time"))
        repeat[symbol] = {
            "records_first": first["records"],
            "records_second": again["records"],
            "sum_vol_first": first["sum_vol"],
            "sum_vol_second": again["sum_vol"],
            "identical_summary": identical,
        }
        print(
            f"  {symbol}: {first['records']} vs {again['records']} records, identical={identical}"
        )
    out["reproducibility"] = repeat

    # --- M1-7: same-session command, and what `trade_count` adds ---------
    print("\nsame-session command (0x0fc5) against the historical one (0x0fb5)")
    same: dict[str, dict] = {}
    for symbol in ("600519.SH", "000001.SZ"):
        wait_spec(spec)
        try:
            rows = client.ticks(
                symbol.split(".")[0], market=market_of(symbol), start=0, offset=MAX_TICK_PAGE
            )
        except Exception as exc:  # noqa: BLE001
            same[symbol] = {"error": f"{type(exc).__name__}: {exc}"}
            continue
        entry = summarise(rows)
        entry["has_trade_count"] = bool(rows) and "trade_count" in rows[0]
        if rows:
            counts = [r["trade_count"] for r in rows]
            entry["trade_count_max"] = max(counts)
            entry["trade_count_mean"] = round(statistics.fmean(counts), 3)
            entry["records_with_multiple_trades"] = sum(1 for c in counts if c > 1)
        same[symbol] = entry
        print(
            f"  {symbol}: {entry['records']} rec {entry.get('first_time')}–"
            f"{entry.get('last_time')}  trade_count mean={entry.get('trade_count_mean')} "
            f"max={entry.get('trade_count_max')}"
        )
    out["same_session"] = same

    # --- non-trading day: the contract for an empty answer ---------------
    wait_spec(spec)
    closed = client.ticks_history("600519", reference + timedelta(days=2), market=1, offset=10)
    out["non_trading_day"] = {"records": len(closed or [])}
    print(f"\nnon-trading day ({reference + timedelta(days=2)}): {len(closed or [])} record(s)")

    # --- M1-1: how deep does the source go? ------------------------------
    if not args.skip_depth:
        print("\nhistory depth (bisection over the trading calendar)")
        days = trading_days(config, reference - timedelta(days=1500), reference)
        depth: dict[str, dict] = {}
        for symbol in ("600519.SH", "000001.SZ", "300750.SZ", "600107.SH", "510300.SH"):
            found = probe_depth(client, symbol, days, spec)
            depth[symbol] = found
            print(
                f"  {symbol}: earliest={found.get('earliest')} "
                f"({found.get('trading_days_back')} trading days) "
                f"in {found['requests']} request(s) {found.get('note', '')}"
            )
        out["history_depth"] = depth
        out["calendar_days_probed"] = len(days)

    client.close()

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
        print(f"\nwrote {args.json_out}")


if __name__ == "__main__":
    main()
