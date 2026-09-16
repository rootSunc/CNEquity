#!/usr/bin/env python3
"""Relabel `trading_status` rows that name TDX as the source of EastMoney data.

`fetch_trading_status` lives in `adapters/tdx_protocol/client.py`, but its body
only forwards to `fetch_trading_status_eastmoney` — TDX serves no ST or halt
feed at all. Until the step began naming its source explicitly, those rows went
through `normalize_with_source()`, whose default is ``source="tdx_protocol"``,
and were stored claiming a provenance they never had.

On the reference lake that is 58,672 rows over 3,196 symbols, 2026-07-06 to
2026-08-14 — the day the step started stamping `eastmoney` itself. Every row
after that date already carries the right label, so this is a bounded, closed
defect rather than an ongoing one.

Row-level provenance is the lake's headline guarantee: `source`,
`data_version` and `fetched_at` are supposed to say where a row came from. A
row that names the wrong vendor is worse than one that names none, because
`cne sources policy` reads it for licensing terms, PIT precedence ranks
exchange history above a current-state board, and the failure-domain report
counts it toward the wrong blast radius.

Only the label changes. Values, dates and `fetched_at` are untouched: the
observation was always EastMoney's, and it is the claim about who made it that
was wrong. The originals are quarantined first, so the change is reversible.

Usage::

    scripts/migrate_relabel_trading_status_provenance.py --config configs/cnequity.toml
    scripts/migrate_relabel_trading_status_provenance.py --config configs/cnequity.toml --apply

Dry-run is the default; ``--apply`` is required to edit curated files.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl

from cnequity.config import load_config
from cnequity.storage.atomic import write_parquet_atomic

DEFAULT_CONFIG = ROOT / "configs/cnequity.toml"
WRONG = "tdx_protocol"
RIGHT = "eastmoney"


def publish_revision(cfg, dataset: str, changed: list[Path], *, reason: str) -> None:
    """Publish the rewritten curated files as a new immutable generation.

    `curated/<dataset>` is the mutable working copy; readers resolve through
    `meta/revisions/.../current.json`. Editing the files alone changes nothing a
    query can see, and is undone by the next compact.
    """
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


def run(cfg, quarantine_root: Path, *, apply: bool) -> int:
    root = cfg.curated_root / "trading_status"
    files = sorted(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        print(f"trading_status: no parquet under {root}")
        return 0

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    quarantine = quarantine_root / f"trading_status_provenance_{stamp}"
    wrong = pl.col("source") == WRONG
    changed = relabelled = scanned = 0
    rewritten: list[Path] = []
    span: list = []

    for path in files:
        frame = pl.read_parquet(path)
        scanned += frame.height
        if "source" not in frame.columns:
            continue
        hits = frame.filter(wrong)
        if hits.is_empty():
            continue
        changed += 1
        relabelled += hits.height
        if "trade_date" in hits.columns:
            span += [hits["trade_date"].min(), hits["trade_date"].max()]
        if apply:
            target = quarantine / path.relative_to(cfg.curated_root)
            target.parent.mkdir(parents=True, exist_ok=True)
            write_parquet_atomic(target, hits, compression="zstd")
            write_parquet_atomic(
                path,
                frame.with_columns(
                    pl.when(wrong).then(pl.lit(RIGHT)).otherwise(pl.col("source")).alias("source")
                ),
                compression="zstd",
            )
            rewritten.append(path)

    verb = "Relabelled" if apply else "Would relabel"
    window = f"{min(span)} .. {max(span)}" if span else "n/a"
    print(
        f"{verb} {relabelled:,} row(s) {WRONG!r} -> {RIGHT!r} across {changed}/{len(files)} "
        f"trading_status file(s); window {window}; {scanned:,} row(s) scanned."
    )
    if apply and relabelled:
        print(f"Quarantined originals: {quarantine}")
        publish_revision(cfg, "trading_status", rewritten, reason="relabel-provenance")
    if not apply:
        print("Dry run — nothing was written. Re-run with --apply to commit.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    cfg = load_config(args.config)
    return run(cfg, cfg.data_root / "_quarantine", apply=args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
