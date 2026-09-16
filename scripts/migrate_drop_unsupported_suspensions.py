#!/usr/bin/env python3
"""Remove `trading_status` suspensions that no evidence ever supported.

A removed code path wrote rows with ``source="multi_source_bar_absence"``:
``is_trading=False, status="suspended"`` asserted purely because no configured
source returned a bar. That is not what the lake means by evidence. The current
derived label, ``derived_bar_gap``, reconstructs a halt only for a listed symbol
missing a session **its own bar history spans** — a gap inside a series the lake
can see. "Nobody answered" is an unknown, and the classifier says so:

    An absent status row, malformed instrument metadata, or an incomplete
    source response remains ``unknown`` and must be retried.
        — cnequity/steps/common.py, classify_daily_bar_ownership

On the reference lake this labelled 1,188 rows across 175 symbols, 171 of them
ETF/LOF quote codes no vendor serves (now outside `[universe].ingest`), and 4
real A shares whose fetch had simply failed that day — so it recorded a halt for
securities that traded normally.

The label no longer exists anywhere in the code, which is why the audit reports
it as a source whose terms are undetermined (`unregistered_source`). Registering
a policy entry for it would silence the finding while keeping the false
suspensions; this removes the rows instead. Removed rows are written to
``_quarantine/`` first, never deleted outright.

Reversible by construction: the quarantine copy holds exactly what was dropped.

Usage::

    scripts/migrate_drop_unsupported_suspensions.py --config configs/cnequity.toml
    scripts/migrate_drop_unsupported_suspensions.py --config configs/cnequity.toml --apply

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
UNSUPPORTED_SOURCE = "multi_source_bar_absence"


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


def run(cfg, quarantine_root: Path, *, source: str, apply: bool) -> int:
    curated_root = cfg.curated_root
    root = curated_root / "trading_status"
    files = sorted(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        print(f"trading_status: no parquet under {root}")
        return 0

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    quarantine = quarantine_root / f"trading_status_{source}_{stamp}"
    changed = dropped = scanned = 0
    symbols: set[str] = set()
    rewritten: list[Path] = []

    for path in files:
        frame = pl.read_parquet(path)
        scanned += frame.height
        if "source" not in frame.columns:
            continue
        doomed = frame.filter(pl.col("source") == source)
        if doomed.is_empty():
            continue
        changed += 1
        dropped += doomed.height
        if "symbol" in doomed.columns:
            symbols.update(doomed.get_column("symbol").to_list())
        if apply:
            # Quarantine before removing: an operator must be able to read back
            # exactly what left curated, including the rows' own provenance.
            target = quarantine / path.relative_to(curated_root)
            target.parent.mkdir(parents=True, exist_ok=True)
            write_parquet_atomic(target, doomed, compression="zstd")
            kept = frame.filter(pl.col("source") != source)
            if kept.is_empty():
                path.unlink()
            else:
                write_parquet_atomic(path, kept, compression="zstd")
                rewritten.append(path)

    verb = "Removed" if apply else "Would remove"
    print(
        f"{verb} {dropped:,} row(s) with source={source!r} across {changed}/{len(files)} "
        f"trading_status file(s) ({len(symbols)} symbol(s); {scanned:,} row(s) scanned)."
    )
    if apply and dropped:
        print(f"Quarantined copy: {quarantine}")
        publish_revision(cfg, "trading_status", rewritten, reason="drop-unsupported-suspensions")
    if not apply:
        print("Dry run — nothing was written. Re-run with --apply to commit.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--source", default=UNSUPPORTED_SOURCE)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    cfg = load_config(args.config)
    return run(
        cfg,
        cfg.data_root / "_quarantine",
        source=args.source,
        apply=args.apply,
    )


if __name__ == "__main__":
    raise SystemExit(main())
