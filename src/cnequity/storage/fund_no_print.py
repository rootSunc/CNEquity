"""Replay approved publisher evidence, independently of suspension status."""

import json
import logging
from datetime import date

import polars as pl

from cnequity.adapters.exchange.fund_quotes import report_no_print_symbols
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.instrument_catalog import load_curated_instruments
from cnequity.storage.raw_archive import RawArchiveError, RawPayloadArchive

logger = logging.getLogger(__name__)


def approved_no_print_keys(config, *, start=None, end=None, read_context=None) -> set:
    """Verify archive bytes, listing bounds and absence of actual traded bars."""
    directory = config.meta_root / "repairs" / "fund_no_print"
    if not directory.is_dir():
        return set()
    archive = RawPayloadArchive(config.meta_root, enabled=True)
    cache_path = config.meta_root / "stats" / "partition_cache" / "fund_no_print_served.json"
    try:
        cache = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        cache = {}
    fresh_cache: dict = {}
    keys = set()
    for path in sorted(directory.glob("*.json")):
        try:
            day = date.fromisoformat(path.stem[:10])
            if (start and day < start) or (end and day > end):
                continue
            proof = json.loads(path.read_text())
            record = proof["archive"]
            if (
                proof.get("schema_version") != 1
                or proof.get("trade_date") != str(day)
                or not isinstance(proof.get("symbols"), list)
            ):
                raise ValueError("invalid fund no-print proof")
            # Every compact decompressed, hashed and parsed all of these
            # publisher reports again (~2.5 s for 232). The answer moves only
            # if the proof or its archived bytes do; a payload outside the
            # lake's meta tree is always re-read.
            stamp = _proof_stamp(config.meta_root, path, record)
            entry = cache.get(path.name)
            if stamp is not None and entry is not None and entry.get("stamp") == stamp:
                served = set(entry["served"])
            else:
                served = set(report_no_print_symbols(archive.read(record), record, day))
            if stamp is not None:
                fresh_cache[path.name] = {"stamp": stamp, "served": sorted(served)}
            keys.update((symbol, day) for symbol in proof["symbols"] if symbol in served)
        except (OSError, ValueError, KeyError, TypeError, RawArchiveError) as exc:
            logger.warning("Ignoring unusable fund no-print proof %s: %s", path.name, exc)
    if fresh_cache != cache:
        from cnequity.storage.atomic import write_json_atomic

        try:
            write_json_atomic(cache_path, fresh_cache)
        except OSError as exc:
            logger.debug("fund no-print cache not written: %s", exc)
    if not keys:
        return set()
    from cnequity.storage.read_context import read_root

    instruments = load_curated_instruments(config, read_context)
    if instruments is None or not {"asset_type", "list_date", "delist_date"} <= set(
        instruments.columns
    ):
        return set()
    wanted = pl.DataFrame(
        sorted(keys), schema={"symbol": pl.String, "trade_date": pl.Date}, orient="row"
    )
    valid = (
        wanted.join(
            instruments.select("symbol", "asset_type", "list_date", "delist_date"), on="symbol"
        )
        .filter(
            pl.col("asset_type").is_in(["etf", "lof"])
            & pl.col("list_date").is_not_null()
            & (pl.col("trade_date") >= pl.col("list_date"))
            & (pl.col("delist_date").is_null() | (pl.col("trade_date") < pl.col("delist_date")))
        )
        .select("symbol", "trade_date")
    )
    root = read_root(config, "daily_bars", read_context)
    if dataset_has_parquet(root, committed=False) and not valid.is_empty():
        actual = scan_parquet_root(
            root,
            partition_col="trade_date",
            start=valid["trade_date"].min(),
            end=valid["trade_date"].max(),
            symbols=valid["symbol"].unique().to_list(),
            committed=False,
        )
        columns = actual.collect_schema().names()
        printed = pl.col("volume") > 0 if "volume" in columns else pl.lit(True)
        if "amount" in columns:
            printed = printed | (pl.col("amount") > 0)
        actual = actual.filter(printed).select("symbol", "trade_date").unique().collect()
        valid = valid.join(actual, on=["symbol", "trade_date"], how="anti")
    return set(valid.iter_rows())


def _proof_stamp(meta_root, proof_path, record) -> list | None:
    """Identity of a proof and the archived bytes it names, or None if unknowable."""
    from pathlib import Path

    if not isinstance(record, dict) or not record.get("payload_path"):
        return None
    payload = (Path(meta_root) / str(record["payload_path"])).resolve()
    try:
        payload.relative_to(Path(meta_root).resolve())
        proof_info, payload_info = proof_path.stat(), payload.stat()
    except (OSError, ValueError):
        return None
    return [
        proof_info.st_size,
        proof_info.st_mtime_ns,
        str(record.get("payload_sha256", "")),
        payload_info.st_size,
        payload_info.st_mtime_ns,
    ]


def reconcile_no_print_coverage(config) -> dict:
    """Retire valid proofs; restore obligations when retained evidence is lost."""
    from cnequity.storage.coverage import resolved_outstanding_keys
    from cnequity.storage.read_context import ReadContext
    from cnequity.storage.state import StateStore

    state = StateStore(config.meta_root)
    payload = state.get_payload("daily_bars")
    previous = payload.get("verified_no_print_keys", [])
    if not previous and not (config.meta_root / "repairs" / "fund_no_print").is_dir():
        return {"proved_keys": 0, "invalidated_keys": 0}
    context = ReadContext.capture(config, ("instruments", "daily_bars"), {})
    valid = approved_no_print_keys(config, read_context=context)
    invalid = {(r["symbol"], r["trade_date"]) for r in previous} - {(s, str(d)) for s, d in valid}
    if invalid:
        rows = [{"symbol": s, "trade_date": d} for s, d in sorted(invalid)]
        present = resolved_outstanding_keys(
            "daily_bars", context.roots["daily_bars"], rows, committed=False
        )
        state.record_outstanding_keys(
            "daily_bars",
            [(s, date.fromisoformat(d)) for s, d in invalid - present],
            run_id="fund-no-print-recheck",
            reason="no_print_evidence_unavailable",
        )
    state.clear_outstanding_keys("daily_bars", [(s, str(d)) for s, d in valid])
    with state.transaction("daily_bars") as current:
        current["verified_no_print_keys"] = [
            {"symbol": s, "trade_date": str(d)} for s, d in sorted(valid)
        ]
    return {"proved_keys": len(valid), "invalidated_keys": len(invalid)}
