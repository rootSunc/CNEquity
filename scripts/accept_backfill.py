#!/usr/bin/env python3
"""Post-init / post-backfill acceptance checks (see docs/operations/runbook.md)."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl

from cn_market_lake.config import load_config
from cn_market_lake.query.reader import load

DEFAULT_CONFIG = ROOT / "configs/cn-market-lake.toml"
CORE_DATASETS = (
    "instruments",
    "daily_bars",
    "index_bars",
    "corporate_actions",
    "trading_status",
    "trading_calendar",
)


def _cfg(path: Path):
    return load_config(path)


def curated_row_counts(cfg) -> dict[str, int]:
    out: dict[str, int] = {}
    for ds in CORE_DATASETS:
        root = cfg.curated_root / ds
        if not root.exists():
            out[ds] = 0
            continue
        files = list(root.glob("**/*.parquet"))
        if not files:
            out[ds] = 0
            continue
        out[ds] = sum(pl.read_parquet(f).height for f in files)
    derived = cfg.derived_root / "adj_factors"
    if derived.exists():
        files = list(derived.glob("**/*.parquet"))
        out["adj_factors"] = sum(pl.read_parquet(f).height for f in files) if files else 0
    else:
        out["adj_factors"] = 0
    return out


def cmd_snapshot(cfg, out_path: Path) -> int:
    payload = {
        "captured_at": date.today().isoformat(),
        "data_root": str(cfg.data_root),
        "row_counts": curated_row_counts(cfg),
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote snapshot → {out_path}")
    for k, v in payload["row_counts"].items():
        print(f"  {k}: {v:,}")
    return 0


def yearly_daily_bars(cfg) -> pl.DataFrame:
    glob = str(cfg.curated_root / "daily_bars" / "**" / "*.parquet")
    return (
        pl.scan_parquet(glob)
        .group_by(pl.col("trade_date").dt.year().alias("year"))
        .agg(
            pl.len().alias("rows"),
            pl.col("symbol").n_unique().alias("symbols"),
            pl.col("trade_date").n_unique().alias("trade_days"),
        )
        .sort("year")
        .collect()
    )


def spot_check_symbol(
    cfg, symbol: str, sample_dates: list[str] | None, *, fallback_end: str | None = None
) -> pl.DataFrame:
    glob = str(cfg.curated_root / "daily_bars" / "**" / "*.parquet")
    if sample_dates:
        dates = [date.fromisoformat(d) for d in sample_dates]
    elif fallback_end:
        dates = [date.fromisoformat(fallback_end)]
    else:
        dates = []
    raw = (
        pl.scan_parquet(glob)
        .filter(pl.col("symbol") == symbol, pl.col("trade_date").is_in(dates))
        .select(["symbol", "trade_date", "open", "high", "low", "close", "volume", "source"])
        .sort("trade_date")
        .collect()
    )
    if raw.is_empty():
        return raw

    adjust = "hfq"
    factor_files = list((cfg.derived_root / "adj_factors").glob("**/*.parquet"))
    if factor_files:
        factors = pl.concat([pl.read_parquet(f) for f in factor_files])
        typed = factors.filter(pl.col("adjust_type") == adjust)
        if typed.is_empty():
            adjust = "qfq"
            typed = factors.filter(pl.col("adjust_type") == "qfq")
        if not typed.is_empty():
            adj = raw.join(
                typed.select(["symbol", "trade_date", "factor"]),
                on=["symbol", "trade_date"],
                how="left",
            ).with_columns(
                (pl.col("close") * pl.col("factor").fill_null(1.0)).alias("adj_close"),
                pl.lit(adjust).alias("adjust_type"),
            )
            return adj.drop("factor")

    return raw.with_columns(pl.col("close").alias("adj_close"))


def load_universe_smoke(cfg, start: str, end: str) -> tuple[int, int, list[str]]:
    raw = load("daily_bars", start=start, end=end, config=cfg)
    filtered = load("daily_bars", start=start, end=end, universe="all_a", config=cfg)
    removed = sorted(
        set(raw.get_column("symbol").to_list()) - set(filtered.get_column("symbol").to_list())
    )
    return raw.height, filtered.height, removed


def cmd_check(cfg, compare_path: Path | None, start: str, end: str, symbol: str) -> int:
    errors: list[str] = []

    counts = curated_row_counts(cfg)
    print("=== Curated row counts ===")
    for k, v in counts.items():
        print(f"  {k}: {v:,}")

    if compare_path is not None:
        baseline = json.loads(compare_path.read_text(encoding="utf-8"))["row_counts"]
        print(f"\n=== Idempotency vs {compare_path} ===")
        for ds in sorted(set(baseline) | set(counts)):
            before, after = baseline.get(ds, 0), counts.get(ds, 0)
            ok = before == after
            mark = "OK" if ok else "MISMATCH"
            print(f"  {ds}: {before:,} → {after:,}  [{mark}]")
            if not ok:
                errors.append(f"idempotency: {ds} {before} != {after}")

    print("\n=== daily_bars by year (watch for cliffs) ===")
    yearly = yearly_daily_bars(cfg)
    if yearly.is_empty():
        errors.append("daily_bars: no data")
        print("  (empty)")
    else:
        print(yearly)

    if not yearly.is_empty() and yearly.height >= 2:
        med = yearly["rows"].median()
        for row in yearly.iter_rows(named=True):
            if med and row["rows"] < med * 0.7:
                errors.append(
                    f"year {row['year']}: {row['rows']} rows "
                    f"(<{med * 0.7:.0f}, possible pagination/rate-limit gap)"
                )

    print(f"\n=== Spot check {symbol} (compare close/adj_close vs 行情软件) ===")
    samples = spot_check_symbol(
        cfg,
        symbol,
        None,
        fallback_end=end,
    )
    if samples.is_empty():
        errors.append(f"spot check: no bars for {symbol} in window ending {end}")
        print("  (no rows — pick dates inside your backfill window)")
    else:
        print(samples)

    print(f"\n=== load() universe smoke ({start} .. {end}) ===")
    try:
        raw_n, filt_n, removed = load_universe_smoke(cfg, start, end)
        print(f"  raw rows: {raw_n:,}  universe=all_a: {filt_n:,}  removed: {raw_n - filt_n:,}")
        if removed:
            print(f"  symbols removed by universe (sample): {removed[:10]}")
        if raw_n == 0:
            errors.append("load smoke: no rows in window")
        elif raw_n == filt_n and counts.get("trading_status", 0) > 0:
            print(
                "  WARN: universe filter removed nothing — verify trading_status has ST/suspended rows"
            )
    except Exception as exc:
        errors.append(f"load smoke failed: {exc}")
        print(f"  FAIL: {exc}")

    print("\n=== load() hfq smoke ===")
    try:
        hfq = load(
            "daily_bars",
            start=start,
            end=end,
            adjust="hfq",
            universe="all_a",
            symbols=[symbol],
            config=cfg,
        )
        if hfq.is_empty():
            errors.append(
                'load(..., adjust="hfq"): empty — add "hfq" to [adj_factors].adjust_types and re-run derive'
            )
            print("  empty (derive may only have qfq; see docs/operations/runbook.md)")
        elif (
            "adj_close" in hfq.columns
            and "close" in hfq.columns
            and (hfq["adj_close"] == hfq["close"]).all()
        ):
            print(
                "  WARN: adj_close == close (adj_factors likely missing; run cml derive adj_factors)"
            )
            print(
                hfq.select(
                    [c for c in ("symbol", "trade_date", "close", "adj_close") if c in hfq.columns]
                ).head(5)
            )
        else:
            cols = [c for c in ("symbol", "trade_date", "close", "adj_close") if c in hfq.columns]
            print(hfq.select(cols).head(5))
    except Exception as exc:
        errors.append(f"hfq load failed: {exc}")
        print(f"  FAIL: {exc}")

    if errors:
        print("\n=== FAILED ===")
        for e in errors:
            print(f"  - {e}")
        return 1

    print("\n=== PASSED (automated checks) ===")
    print("Manual: confirm spot-check prices against 行情软件 before production cutover.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Post-backfill acceptance checks")
    sub = parser.add_subparsers(dest="cmd", required=True)

    snap = sub.add_parser("snapshot", help="Save curated row counts before idempotency re-run")
    snap.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    snap.add_argument("--out", type=Path, required=True)

    chk = sub.add_parser("check", help="Run acceptance checks")
    chk.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    chk.add_argument(
        "--compare",
        type=Path,
        help="Snapshot JSON from `snapshot` — assert row counts unchanged after re-run",
    )
    chk.add_argument("--start", default="2024-01-01", help="Window for load() smoke")
    chk.add_argument("--end", default="2024-12-31")
    chk.add_argument("--symbol", default="600519.SH")

    args = parser.parse_args()
    cfg = _cfg(args.config)

    if args.cmd == "snapshot":
        return cmd_snapshot(cfg, args.out)
    return cmd_check(cfg, args.compare, args.start, args.end, args.symbol)


if __name__ == "__main__":
    raise SystemExit(main())
