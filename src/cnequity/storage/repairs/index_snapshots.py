"""Replay dated official index books and replace whole index/date snapshots."""

import json
from contextlib import nullcontext
from datetime import datetime
from uuid import uuid4

import httpx
import polars as pl

from cnequity.adapters.exchange.index_snapshots import (
    INDEX_COUNTS,
    parse_index_snapshot,
    snapshot_requests,
)
from cnequity.domain.datasets import granularity_for_dataset
from cnequity.domain.http_policy import record_http_response
from cnequity.domain.market_time import last_closed_session, shanghai_now
from cnequity.domain.partitions import partition_value
from cnequity.domain.rate_limit import source_request
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.raw_archive import RawPayloadArchive
from cnequity.storage.repairs.publish import PartitionRewrite
from cnequity.storage.revisions import RevisionStore


def repair_index_snapshots(config, *, indices=None, apply=False, refresh_sources=False):
    indices = sorted(set(INDEX_COUNTS if indices is None else indices))
    unknown = set(indices) - set(INDEX_COUNTS)
    if unknown:
        raise ValueError(f"unsupported indices: {sorted(unknown)}")
    dataset = "index_constituents"
    run_id = "index-snapshots-" + uuid4().hex
    archive = RawPayloadArchive(config.meta_root, enabled=True, datasets=[dataset])
    cache = config.meta_root / "source_cache" / "index_snapshots"
    frames, failures = [], []
    latest = last_closed_session() or last_closed_session(shanghai_now().replace(hour=8, minute=0))
    if latest is None:
        raise ValueError("no known closed session for official index snapshots")
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        for symbol in indices:
            for url, params, weights, lane in snapshot_requests(symbol):
                path = cache / f"{symbol}-{'weights' if weights else 'members'}.json"
                try:
                    if refresh_sources:
                        if not config.sources.get(lane, True):
                            raise ValueError(f"{lane} source disabled")
                        with source_request(config, lane):
                            response = client.get(url, params=params)
                            record_http_response(config, lane, response)
                        response.raise_for_status()
                        frame = parse_index_snapshot(response.content, symbol, weights=weights)
                        record = archive.archive(
                            dataset,
                            response.content,
                            source=lane,
                            run_id=run_id,
                            url=url,
                            request_params=params,
                            payload_format="xls",
                        ).to_dict()
                        write_json_atomic(path, record)
                    else:
                        record = json.loads(path.read_text())
                        if (
                            record.get("url") != url
                            or record.get("request_params") != params
                            or record.get("source") != lane
                            or record.get("dataset") != dataset
                        ):
                            raise ValueError("cached official index identity differs")
                        frame = parse_index_snapshot(archive.read(record), symbol, weights=weights)
                    if frame["as_of_date"][0] > latest:
                        raise ValueError("official workbook date is after the last closed session")
                    frame = with_provenance(
                        frame,
                        source="exchange" if lane == "exchange" else "cni_weights",
                        data_version=data_version_for(dataset),
                    ).with_columns(
                        pl.lit(datetime.fromisoformat(record["captured_at"])).alias("fetched_at")
                    )
                    frames.append(frame)
                except (OSError, ValueError, RuntimeError, httpx.HTTPError) as exc:
                    failures.append({"index_symbol": symbol, "weights": weights, "error": str(exc)})
    if not frames:
        return {"applied": False, "rows": 0, "snapshots": [], "failures": failures}
    # If both books happen to have the same date, the complete weight book wins.
    combined = pl.concat(frames)
    weighted = combined.filter(pl.col("weight").is_not_null())
    weighted_pairs = weighted.select("index_symbol", "as_of_date").unique()
    # Prefer an entire dated weight book, not the union of differing cohorts.
    rows = pl.concat(
        [
            weighted,
            combined.filter(pl.col("weight").is_null()).join(
                weighted_pairs, on=["index_symbol", "as_of_date"], how="anti"
            ),
        ]
    )
    pairs = rows.select("index_symbol", "as_of_date").unique()
    report = {
        "applied": False,
        "rows": rows.height,
        "weighted_rows": rows["weight"].drop_nulls().len(),
        "snapshots": pairs.sort("index_symbol", "as_of_date")
        .with_columns(pl.col("as_of_date").cast(pl.String))
        .to_dicts(),
        "failures": failures,
    }
    if not apply:
        return report
    with lake_mutation_lock(config.meta_root) if apply else nullcontext():
        store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
        store.ensure_current(dataset)
        with PartitionRewrite(config, store, dataset, run_id=run_id, apply=True) as rewrite:
            partitioned = rows.with_columns(
                pl.col("as_of_date")
                .map_elements(
                    lambda day: partition_value(day, granularity_for_dataset(dataset)),
                    return_dtype=pl.String,
                )
                .alias("_partition")
            )
            for value in sorted(partitioned["_partition"].unique()):
                additions = partitioned.filter(pl.col("_partition") == value).drop("_partition")
                selected_pairs = additions.select("index_symbol", "as_of_date").unique()
                stored = rewrite.rows(f"as_of_date={value}")
                before = stored if stored is not None else rows.clear()
                retained = []
                for book in additions.partition_by(
                    ["index_symbol", "as_of_date"], maintain_order=True
                ):
                    index, day = book.select("index_symbol", "as_of_date").row(0)
                    existing = before.filter(
                        (pl.col("index_symbol") == index) & (pl.col("as_of_date") == day)
                    )
                    if (
                        book["weight"].null_count() == book.height
                        and not existing.is_empty()
                        and existing["weight"].null_count() == 0
                        and existing["source"].is_in(["exchange", "cni_weights"]).all()
                    ):
                        same_members = set(book["symbol"]) == set(existing["symbol"])
                        report.setdefault("snapshots_retained_weights", []).append(
                            {"index_symbol": index, "as_of_date": str(day)}
                        )
                        if not same_members:
                            failures.append(
                                {
                                    "index_symbol": index,
                                    "error": "membership-only cohort conflicts with retained dated weight book",
                                }
                            )
                        continue
                    retained.append(book)
                if not retained:
                    continue
                additions = pl.concat(retained)
                selected_pairs = additions.select("index_symbol", "as_of_date").unique()
                # Replace the whole official cohort. Per-member upserts retain
                # removed members from an inaccurate earlier snapshot.
                old_book = before.join(
                    selected_pairs, on=["index_symbol", "as_of_date"], how="semi"
                )
                fields = [
                    "index_symbol",
                    "symbol",
                    "as_of_date",
                    "weight",
                    "source",
                    "data_version",
                ]
                if (
                    old_book.select(fields)
                    .sort(fields)
                    .equals(additions.select(fields).sort(fields))
                ):
                    continue
                kept = before.join(selected_pairs, on=["index_symbol", "as_of_date"], how="anti")
                merged = pl.concat([kept, additions], how="diagonal_relaxed")
                rewrite.replace(f"as_of_date={value}", merged)
            if not rewrite.rewritten:
                report["note"] = "official snapshots already present"
                return report
            published = rewrite.publish({"reason": "official_dated_index_snapshots", **report})
        if published is None:
            raise RuntimeError("official index repair produced no revision")
        report.update(applied=True, revision=published["revision"])
        return report
