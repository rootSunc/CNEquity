"""Collapse the Beijing board's pre-rename codes onto the codes it uses now.

On 2025-09-30 the BSE renumbered 248 securities from their NEEQ codes
(430xxx/83xxxx/87xxxx) to 920xxx. The vendors then served each security's whole
history under its new code, and the lake kept the old series as well — so
2016..2025 is stored twice. Measured: 215,433 daily_bars rows where all five
OHLCV fields are byte-identical between the two codes, with no exception and no
legacy row lacking a twin. Anything counting Beijing securities counted 248 of
them twice.

The rule is the same for every time series: a row whose current-code twin
exists is redundant and goes; a row without one is that security's data under a
name it no longer uses, so it is re-stamped rather than dropped. That second
case is not hypothetical — `corporate_actions` holds 165 transfer records from
2016 that exist only under the old codes.

`instruments` is a registry, not a series: the legacy entry is removed and the
rename is recorded as `prev_symbol` on the current one, which is what that
column is for and what nothing had populated.

The three legacy codes BSE's table does not map (832317, 833874, 833994 — last
traded 2021) never migrated, and are left alone.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.domain.schemas import PRIMARY_KEYS
from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.atomic import write_parquet_atomic

logger = logging.getLogger(__name__)

# Every dataset that carries a symbol and held a legacy Beijing code when this
# was written. A dataset absent here is simply not rewritten.
_SERIES_DATASETS = ("daily_bars", "trading_status", "corporate_actions", "adj_factors")
# A rename is not a delisting, so nothing about it belongs in these. `probe`
# cannot tell the two apart — the old code simply stops answering — so all 248
# were filed as delistings on 2025-09-30 and became 248 of the 343 rows in
# `delisting_events`. `steps.delisted.renamed_symbols` stops new ones; this
# removes the ones already recorded.
_DELISTING_DATASET = "delisting_events"
_DELISTED_STATE = ("delisted_catalog.json", "delisted_ingested.json")
# `adj_factors` is derived, and carried 207,463 legacy rows — every one with a
# current-code twin — because it was derived from the duplicated bars.
_DERIVED = frozenset({"adj_factors"})


def _layer_root(config: Config, dataset: str) -> Path:
    return (config.derived_root if dataset in _DERIVED else config.curated_root) / dataset


def _mapping() -> dict[str, str]:
    from cnequity.adapters.eastmoney.corporate_actions_migration import _code_mapping

    return {f"{old}.BJ": f"{new}.BJ" for old, new in _code_mapping().items()}


def migrate_bse_legacy_codes(config: Config, *, apply: bool = False) -> dict:
    """Report the collapse, and perform it when *apply*.

    Returns per-dataset counts. Reading is cheap; the write is a
    read-modify-write over curated partitions and takes the mutation lock so it
    cannot race compact or repartition.
    """
    if not apply:
        return _plan(config, _mapping())
    mapping = _mapping()
    with lake_mutation_lock(config.meta_root, blocking=True):
        report, touched = _apply(config, mapping)
    # Readers resolve a published revision, not `curated/<dataset>`, so a
    # rewrite nothing publishes is a rewrite nobody sees: the first run of this
    # reported 224,788 rows removed while every query still returned them.
    # `commit` takes the mutation lock itself, so it runs outside ours.
    report["revisions"] = _publish(config, touched)
    return report


def _unpublished_files(config: Config, dataset: str, store) -> list[Path]:
    """Curated partitions written since the live generation was published.

    Needed because the first run of this rewrote curated and published
    nothing, so the work is on disk and invisible. A re-run finds nothing left
    to change and would otherwise publish nothing either. Modification time is
    enough here: the generation is a complete snapshot regardless, so this set
    only decides what the receipt names as changed.
    """
    pointer = store.current_pointer(dataset)
    if not pointer:
        return []
    receipt = config.meta_root / str(pointer.get("receipt") or "")
    if not receipt.is_file():
        return []
    published_at = receipt.stat().st_mtime
    return [
        path
        for path in _partition_files(_layer_root(config, dataset))
        if path.stat().st_mtime > published_at
    ]


def _publish(config: Config, touched: dict[str, list[Path]]) -> dict:
    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.storage.revisions import RevisionStore

    run_id = Manifest(config.manifest_path).start_run("maintenance:bse_code_migration")
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    published: dict = {}
    for dataset in list(touched) or list(_SERIES_DATASETS) + ["instruments"]:
        files = touched.get(dataset) or _unpublished_files(config, dataset, store)
        if not files:
            continue
        contract = dataset_contract(dataset)
        revision = store.commit(
            dataset,
            run_id=run_id,
            changed_files=files,
            schema_version=int(contract["schema_version"]),
            contract_fingerprint=contract_fingerprint(contract),
            metadata={
                "layer": "derived" if dataset in _DERIVED else "curated",
                "reason": "bse_code_migration",
            },
        )
        published[dataset] = None if revision is None else revision.revision
        logger.info("%s: published revision %s", dataset, published[dataset])
    return published


def _partition_files(root: Path) -> list[Path]:
    return sorted(root.glob("**/*.parquet")) if root.exists() else []


def _twinned_keys(files: list[Path], mapping: dict[str, str], keys: list[str]) -> set[tuple]:
    """Keys already present under a current code, as (current_symbol, *rest)."""
    current = set(mapping.values())
    if not files:
        return set()
    rows = (
        # `corporate_actions` partitions disagree on `split_factor`, so a plain
        # scan over all of them raises before it can read the keys.
        pl.scan_parquet([str(f) for f in files], extra_columns="ignore", missing_columns="insert")
        .filter(pl.col("symbol").is_in(list(current)))
        .select(keys)
        .unique()
        .collect()
    )
    return set(rows.iter_rows())


def _split(df: pl.DataFrame, mapping: dict[str, str], keys: list[str], twins: set[tuple]):
    """(rows to drop, rows to re-stamp, rows to leave) for one partition."""
    legacy = df.filter(pl.col("symbol").is_in(list(mapping)))
    if legacy.is_empty():
        return legacy, legacy, df
    rest = [k for k in keys if k != "symbol"]
    renamed = legacy.with_columns(pl.col("symbol").replace_strict(mapping).alias("_current"))
    has_twin = [
        tuple([row["_current"]] + [row[k] for k in rest]) in twins
        for row in renamed.iter_rows(named=True)
    ]
    flagged = renamed.with_columns(pl.Series("_twin", has_twin))
    drop = flagged.filter(pl.col("_twin"))
    restamp = flagged.filter(~pl.col("_twin")).with_columns(pl.col("_current").alias("symbol"))
    keep = df.filter(~pl.col("symbol").is_in(list(mapping)))
    return drop.drop("_current", "_twin"), restamp.drop("_current", "_twin"), keep


def _plan(config: Config, mapping: dict[str, str]) -> dict:
    report: dict = {"applied": False, "datasets": {}}
    for dataset in _SERIES_DATASETS:
        files = _partition_files(_layer_root(config, dataset))
        keys = PRIMARY_KEYS.get(dataset) or ["symbol"]
        twins = _twinned_keys(files, mapping, keys)
        dropped = restamped = 0
        for path in files:
            df = pl.read_parquet(path)
            if "symbol" not in df.columns:
                continue
            drop, restamp, _ = _split(df, mapping, keys, twins)
            dropped += drop.height
            restamped += restamp.height
        report["datasets"][dataset] = {"rows_dropped": dropped, "rows_restamped": restamped}

    files = _partition_files(config.curated_root / "instruments")
    legacy_entries = 0
    for path in files:
        df = pl.read_parquet(path)
        if "symbol" in df.columns:
            legacy_entries += df.filter(pl.col("symbol").is_in(list(mapping))).height
    report["datasets"]["instruments"] = {
        "entries_dropped": legacy_entries,
        "prev_symbol_recorded": legacy_entries,
    }
    phantom = 0
    for path in _partition_files(config.derived_root / _DELISTING_DATASET):
        df = pl.read_parquet(path)
        if "symbol" in df.columns:
            phantom += df.filter(pl.col("symbol").is_in(list(mapping))).height
    report["datasets"][_DELISTING_DATASET] = {"rows_dropped": phantom}
    return report


def _apply(config: Config, mapping: dict[str, str]) -> tuple[dict, dict[str, list[Path]]]:
    report: dict = {"applied": True, "datasets": {}}
    touched: dict[str, list[Path]] = {}
    for dataset in _SERIES_DATASETS:
        root = _layer_root(config, dataset)
        files = _partition_files(root)
        keys = PRIMARY_KEYS.get(dataset) or ["symbol"]
        twins = _twinned_keys(files, mapping, keys)
        dropped = restamped = partitions = 0
        changed: list[Path] = []
        for path in files:
            df = pl.read_parquet(path)
            if "symbol" not in df.columns:
                continue
            drop, restamp, keep = _split(df, mapping, keys, twins)
            if drop.is_empty() and restamp.is_empty():
                continue
            out = pl.concat([keep, restamp], how="vertical_relaxed") if restamp.height else keep
            dropped += drop.height
            restamped += restamp.height
            partitions += 1
            if out.is_empty():
                path.unlink(missing_ok=True)
                _maybe_rmdir(path.parent)
            else:
                write_parquet_atomic(path, out, compression="zstd")
                changed.append(path)
        touched[dataset] = changed
        logger.info(
            "%s: dropped %d redundant legacy BJ row(s), re-stamped %d across %d partition(s)",
            dataset,
            dropped,
            restamped,
            partitions,
        )
        report["datasets"][dataset] = {
            "rows_dropped": dropped,
            "rows_restamped": restamped,
            "partitions_rewritten": partitions,
        }

    ins_report, ins_changed = _apply_instruments(config, mapping)
    report["datasets"]["instruments"] = ins_report
    touched["instruments"] = ins_changed

    de_report, de_changed = _drop_phantom_delistings(config, mapping)
    report["datasets"][_DELISTING_DATASET] = de_report
    touched[_DELISTING_DATASET] = de_changed
    report["state"] = _clear_delisted_state(config, mapping)
    return report, touched


def _drop_phantom_delistings(config: Config, mapping: dict[str, str]) -> tuple[dict, list[Path]]:
    """Remove the renames that were recorded as the end of a listing."""
    root = config.derived_root / _DELISTING_DATASET
    changed: list[Path] = []
    dropped = 0
    for path in _partition_files(root):
        df = pl.read_parquet(path)
        if "symbol" not in df.columns:
            continue
        hit = df.filter(pl.col("symbol").is_in(list(mapping)))
        if hit.is_empty():
            continue
        keep = df.filter(~pl.col("symbol").is_in(list(mapping)))
        dropped += hit.height
        if keep.is_empty():
            path.unlink(missing_ok=True)
            _maybe_rmdir(path.parent)
        else:
            write_parquet_atomic(path, keep, compression="zstd")
            changed.append(path)
    logger.info("%s: dropped %d rename(s) recorded as delistings", _DELISTING_DATASET, dropped)
    return {"rows_dropped": dropped}, changed


def _clear_delisted_state(config: Config, mapping: dict[str, str]) -> dict:
    """Take the renamed codes out of the catalogue and the ingest ledger.

    Leaving them there would keep the dedicated delisted fetch chasing history
    for codes that no longer exist, and would put the rows straight back the
    next time `delisting_events` is written.
    """
    import json

    removed: dict[str, int] = {}
    for name in _DELISTED_STATE:
        path = config.meta_root / "state" / name
        if not path.is_file():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        count = 0
        delisted = payload.get("delisted")
        if isinstance(delisted, dict):
            for symbol in list(delisted):
                if symbol in mapping:
                    del delisted[symbol]
                    count += 1
        done = payload.get("completed")
        if isinstance(done, list):
            kept = [s for s in done if s not in mapping]
            count += len(done) - len(kept)
            payload["completed"] = kept
        if count:
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        removed[name] = count
    logger.info("delisted state: removed %s renamed entry(ies)", removed)
    return removed


def _apply_instruments(config: Config, mapping: dict[str, str]) -> tuple[dict, list[Path]]:
    root = config.curated_root / "instruments"
    files = _partition_files(root)
    changed: list[Path] = []
    dropped = recorded = 0
    reverse = {new: old for old, new in mapping.items()}
    for path in files:
        df = pl.read_parquet(path)
        if "symbol" not in df.columns:
            continue
        legacy = df.filter(pl.col("symbol").is_in(list(mapping)))
        if legacy.is_empty() and not df.filter(pl.col("symbol").is_in(list(reverse))).height:
            continue
        out = df.filter(~pl.col("symbol").is_in(list(mapping)))
        if "prev_symbol" in out.columns:
            # Count only what this call sets. Other exchanges' instruments
            # already carry a `prev_symbol`, and counting those would report a
            # rename the migration never wrote.
            to_set = pl.col("symbol").is_in(list(reverse)) & pl.col("prev_symbol").is_null()
            recorded += out.filter(to_set).height
            out = out.with_columns(
                pl.when(to_set)
                .then(pl.col("symbol").replace_strict(reverse, default=None))
                .otherwise(pl.col("prev_symbol"))
                .alias("prev_symbol")
            )
        dropped += legacy.height
        if out.is_empty():
            path.unlink(missing_ok=True)
            _maybe_rmdir(path.parent)
        else:
            write_parquet_atomic(path, out, compression="zstd")
            changed.append(path)
    logger.info(
        "instruments: removed %d legacy BJ entry(ies), recorded %d prev_symbol", dropped, recorded
    )
    return {"entries_dropped": dropped, "prev_symbol_recorded": recorded}, changed


def _maybe_rmdir(path: Path) -> None:
    try:
        if path.is_dir() and not any(path.iterdir()):
            path.rmdir()
    except OSError:
        pass
