#!/usr/bin/env python3
"""Null `valuation_metrics.ps_ttm` rows that hold an amount instead of a ratio.

The EastMoney adapter read clist field ``f45`` as 市销率 TTM. It is not: it is a
figure in yuan. Measured over the whole dataset that put a median of 2.05e7
into `ps_ttm` for every EastMoney row, against 3.2 from baostock for the same
column, and made 96.8% of 154,705 values exceed 1000.

The adapter now reads ``f130``, which the feed's own numbers identify as the
ratio — total_mv / f132 (营业总收入 TTM) equals f130 exactly, 9.184 for 600519
and 1.729 for 000001 — and which matches the 9.20 a licensed third source
reports for 600519.

The stored values cannot be repaired in place: recovering a ratio needs the
revenue figure, and the lake kept the wrong amount rather than the revenue. So
they are set to null, which is what "we do not know this" means here; the other
columns in those rows (pe_ttm, pb, total_mv, float_mv) were always correct and
are left untouched. Re-fetch the window with `cne backfill valuation_metrics`
to fill the ratio back in.

Only rows whose ps_ttm is actually implausible are touched, so running this
after the adapter fix cannot damage correct new rows.

Usage::

    scripts/migrate_null_bad_eastmoney_ps_ttm.py --config configs/cnequity.toml
    scripts/migrate_null_bad_eastmoney_ps_ttm.py --config configs/cnequity.toml --apply

Dry-run is the default; ``--apply`` is required to edit curated files.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl

from cnequity.config import load_config
from cnequity.quality.unit_checks import RATIO_PLAUSIBLE_MAX
from cnequity.storage.atomic import write_parquet_atomic

DEFAULT_CONFIG = ROOT / "configs/cnequity.toml"


def publish_revision(cfg, dataset: str, changed: list[Path], *, reason: str) -> None:
    """Publish the rewritten curated files as a new immutable generation.

    `curated/<dataset>` is the mutable working copy; readers resolve through
    `meta/revisions/.../current.json`. Editing the files alone therefore changes
    nothing a query can see — and is silently undone by the next compact. The
    revision store's own contract is "compaction writes curated, then this
    snapshots the complete result", so a migration has to do both halves too.
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
    if revision is None:
        print("Revision unchanged — nothing to publish.")
    else:
        print(f"Published revision {revision.revision}: {revision.revision_id}")


def run(cfg, *, source: str, apply: bool) -> int:
    curated_root = cfg.curated_root
    root = curated_root / "valuation_metrics"
    files = sorted(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        print(f"valuation_metrics: no parquet under {root}")
        return 0

    implausible = (
        (pl.col("source") == source)
        & pl.col("ps_ttm").is_not_null()
        & (pl.col("ps_ttm").abs() > RATIO_PLAUSIBLE_MAX)
    )
    changed = nulled = scanned = 0
    rewritten: list[Path] = []
    for path in files:
        frame = pl.read_parquet(path)
        scanned += frame.height
        if not {"source", "ps_ttm"} <= set(frame.columns):
            continue
        hits = int(frame.filter(implausible).height)
        if not hits:
            continue
        changed += 1
        nulled += hits
        if apply:
            write_parquet_atomic(
                path,
                frame.with_columns(
                    pl.when(implausible).then(None).otherwise(pl.col("ps_ttm")).alias("ps_ttm")
                ),
                compression="zstd",
            )
            rewritten.append(path)

    verb = "Nulled" if apply else "Would null"
    print(
        f"{verb} ps_ttm on {nulled:,} {source} row(s) holding an amount "
        f"(>{RATIO_PLAUSIBLE_MAX:g}) across {changed}/{len(files)} file(s); "
        f"{scanned:,} row(s) scanned."
    )
    if apply and nulled:
        publish_revision(cfg, "valuation_metrics", rewritten, reason="null-bad-ps-ttm")
        print("Re-fetch the window with `cne backfill valuation_metrics` to restore the ratio.")
    if not apply:
        print("Dry run — nothing was written. Re-run with --apply to commit.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--source", default="eastmoney")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    cfg = load_config(args.config)
    return run(cfg, source=args.source, apply=args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
