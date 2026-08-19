"""Trade-date diff between the EastMoney and baostock trading_status feeds.

Compare both vendors' outputs for one trade date after the 2026-08 adapter
fix: ST board vs baostock name ST, and EastMoney suspension vs baostock
`tradeStatus=0`. Prints a report and exits:

- 0  when both dimensions agree over the SH/SZ universe
- 1  when there are symmetric differences
- 2  on a tooling/network error (EastMoney push2 may be IP-throttled; retry)

Usage:
    python scripts/trading_status_backup_diff.py --date 2026-08-18
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

import baostock as bs

from cnequity.adapters.baostock.trading_status import (
    _symbol_from_baostock,
    fetch_trading_status_baostock,
)
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.adapters.eastmoney.trading_status import (
    _fetch_st_symbols,
    _fetch_suspended_symbols,
)

logger = logging.getLogger("trading_status_backup_diff")


def _baostock_sets(trade_date: date, symbols_sh_sz: list[str]) -> tuple[set[str], set[str]]:
    """(suspended_symbols, st_symbols) from one query_all_stock snapshot."""
    df = fetch_trading_status_baostock(symbols_sh_sz, trade_date)
    suspended: set[str] = set()
    st: set[str] = set()
    for row in df.iter_rows(named=True):
        if row["status"] == "suspended":
            suspended.add(row["symbol"])
        elif row["status"] == "st":
            st.add(row["symbol"])
    return suspended, st


def _ea_sets(client: EastMoneyClient, trade_date: date) -> tuple[set[str], set[str]]:
    return _fetch_suspended_symbols(client, trade_date), _fetch_st_symbols(client)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    # argparse hides the epilog when __doc__ has Usage; keep it short here.
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")
    parser.add_argument("--symbols", default=None, help="path to a symbol list, one per line")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)
    trade_date = date.fromisoformat(args.date) if args.date else date.today()

    all_a_sh_sz: list[str] = []
    if args.symbols:
        all_a_sh_sz = [
            line.strip() for line in open(args.symbols, encoding="utf-8") if line.strip()
        ]
    else:
        # Derive the SH/SZ universe from baostock itself (single request).
        bs.login()
        try:
            rs = bs.query_all_stock(day=trade_date.isoformat())
            while rs.next():
                rec = dict(zip(rs.fields, rs.get_row_data(), strict=False))
                sym = _symbol_from_baostock(str(rec.get("code", "")))
                if sym:
                    all_a_sh_sz.append(sym)
        finally:
            bs.logout()

    bs_susp, bs_st = _baostock_sets(trade_date, all_a_sh_sz)

    client = EastMoneyClient(min_interval=2.5, config=None)
    try:
        try:
            em_susp, em_st = _ea_sets(client, trade_date)
        except Exception as exc:  # noqa: BLE001 — push2/datacenter may be throttled
            print(
                f"EastMoney side unavailable for {trade_date}: {exc}\n"
                "(this is usually the push2 IP bucket — wait and retry, or use a proxy)"
            )
            return 2
    finally:
        client.close()

    common = set(all_a_sh_sz)
    st_report = {
        "em_only": sorted((em_st - bs_st) & common),
        "bs_only": sorted(bs_st - em_st),
    }
    susp_report = {
        "em_only": sorted((em_susp - bs_susp) & common),
        "bs_only": sorted(bs_susp - em_susp),
    }

    print(f"trading_status diff for {trade_date} (SH/SZ universe size {len(common)})")
    print(
        f"  ST:         em={len(em_st)} bs={len(bs_st)} "
        f"em_only={st_report['em_only']} bs_only={st_report['bs_only']}"
    )
    print(
        f"  suspension: em={len(em_susp)} bs={len(bs_susp)} "
        f"em_only={susp_report['em_only']} bs_only={susp_report['bs_only']}"
    )
    st_ok = not st_report["em_only"] and not st_report["bs_only"]
    susp_ok = not susp_report["em_only"] and not susp_report["bs_only"]
    print("ST agreement:", "OK" if st_ok else "MISMATCH")
    print("Suspension agreement:", "OK" if susp_ok else "MISMATCH")
    return 0 if (st_ok and susp_ok) else 1


if __name__ == "__main__":
    raise SystemExit(main())
