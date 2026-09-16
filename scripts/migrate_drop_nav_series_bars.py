#!/usr/bin/env python3
"""Remove `daily_bars` rows that are a fund's NAV series, not a quoted price.

TDX answers for fund codes the same way it answers for shares, and an over-broad
prefix once swept them into the universe: the rows carry a `close` with zero
volume and zero turnover on every session, because a LOF is subscribed and
redeemed at net asset value away from the exchange. Every liquidity screen,
turnover aggregate and tradable-universe filter then treats them as market data.

`[universe].ingest = "all_a"` stops new ones arriving. This removes the ones
already stored, using the audit's own criterion rather than a looser one: a
symbol whose maximum volume *and* maximum turnover are both zero across at
least 20 sessions in the trailing year. On the reference lake that is exactly
the 60 symbols `untraded_instruments` reports, and none of them is an A share —
a real security that is merely halted still carries prints either side of the
halt, which is what makes the test structural rather than statistical.

Only the zero-volume rows of those symbols are removed. If one of them ever
genuinely traded, that session is a real observation and is kept.

Usage::

    scripts/migrate_drop_nav_series_bars.py --config configs/cnequity.toml
    scripts/migrate_drop_nav_series_bars.py --config configs/cnequity.toml --apply

Dry-run is the default; ``--apply`` is required to edit curated files.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl

from cnequity.config import load_config
from cnequity.domain.symbols import is_all_a_symbol, parse_symbol
from cnequity.query.canonical import dedupe_lazy_by_primary_key
from cnequity.query.parquet_scan import scan_parquet_root
from cnequity.storage.atomic import write_parquet_atomic

DEFAULT_CONFIG = ROOT / "configs/cnequity.toml"
MIN_SESSIONS = 20


def publish_revision(cfg, dataset: str, changed: list[Path], *, reason: str) -> None:
    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.storage.revisions import RevisionStore

    if not changed:
        return
    contract = dataset_contract(dataset)
    revision = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root).commit(
        dataset,
        run_id=f"migration-{reason}",
        changed_files=changed,
        schema_version=int(contract["schema_version"]),
        contract_fingerprint=contract_fingerprint(contract),
        metadata={"migration": reason, "files": len(changed)},
    )
    print(
        "Revision unchanged — nothing to publish."
        if revision is None
        else f"Published revision {revision.revision}: {revision.revision_id}"
    )


def nav_series_symbols(cfg, trade_date: date) -> list[str]:
    """Symbols the audit's own `untraded_instruments` criterion identifies."""
    root = cfg.curated_root / "daily_bars"
    bars = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(
                root,
                partition_col="trade_date",
                start=trade_date - timedelta(days=365),
                end=trade_date,
            ),
            "daily_bars",
        )
        .select("symbol", "volume", "amount")
        .collect()
    )
    if bars.is_empty():
        return []
    per_symbol = bars.group_by("symbol").agg(
        pl.len().alias("rows"),
        pl.col("volume").fill_null(0).max().alias("_v"),
        pl.col("amount").fill_null(0).max().alias("_a"),
    )
    hit = per_symbol.filter(
        (pl.col("_v") <= 0) & (pl.col("_a") <= 0) & (pl.col("rows") >= MIN_SESSIONS)
    )
    return sorted(hit.get_column("symbol").to_list())


def run(cfg, quarantine_root: Path, *, trade_date: date, apply: bool) -> int:
    symbols = nav_series_symbols(cfg, trade_date)
    if not symbols:
        print("daily_bars: no NAV-only symbols under the audit criterion")
        return 0

    shares = [s for s in symbols if _is_share(s)]
    if shares:
        # A share reaching this filter would mean the criterion is wrong, not
        # that the share is a fund. Refuse rather than delete equity history.
        print(f"refusing: {len(shares)} A share(s) matched the NAV filter: {shares[:8]}")
        return 1

    root = cfg.curated_root / "daily_bars"
    files = sorted(root.glob("**/*.parquet"))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    quarantine = quarantine_root / f"daily_bars_nav_series_{stamp}"
    doomed = (
        pl.col("symbol").is_in(symbols)
        & (pl.col("volume").fill_null(0) <= 0)
        & (pl.col("amount").fill_null(0) <= 0)
    )
    changed = removed = 0
    rewritten: list[Path] = []

    for path in files:
        frame = pl.read_parquet(path)
        if "symbol" not in frame.columns or "volume" not in frame.columns:
            continue
        hits = frame.filter(doomed)
        if hits.is_empty():
            continue
        changed += 1
        removed += hits.height
        if apply:
            target = quarantine / path.relative_to(cfg.curated_root)
            target.parent.mkdir(parents=True, exist_ok=True)
            write_parquet_atomic(target, hits, compression="zstd")
            kept = frame.filter(~doomed)
            if kept.is_empty():
                path.unlink()
            else:
                write_parquet_atomic(path, kept, compression="zstd")
                rewritten.append(path)

    verb = "Removed" if apply else "Would remove"
    print(
        f"{verb} {removed:,} NAV row(s) across {len(symbols)} fund code(s) "
        f"in {changed}/{len(files)} daily_bars file(s)."
    )
    if apply and removed:
        print(f"Quarantined copy: {quarantine}")
        publish_revision(cfg, "daily_bars", rewritten, reason="drop-nav-series")
    if not apply:
        print("Dry run — nothing was written. Re-run with --apply to commit.")
    return 0


def _is_share(symbol: str) -> bool:
    try:
        info = parse_symbol(symbol)
    except ValueError:
        return False
    return is_all_a_symbol(info.code, info.exchange)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    cfg = load_config(args.config)
    td = date.fromisoformat(args.trade_date) if args.trade_date else date.today()
    return run(cfg, cfg.data_root / "_quarantine", trade_date=td, apply=args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
