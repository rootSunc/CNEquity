"""Replay the bounded, reviewed official source catalogs and replace whole history books."""

import hashlib
import json
from bisect import bisect_right
from contextlib import nullcontext
from datetime import date, datetime
from importlib import resources
from io import BytesIO
from uuid import uuid4

import httpx
import polars as pl

from cnequity._lazy import lazy_module
from cnequity.adapters.cni.index_announcements import (
    parse_announcement,
    parse_change_workbook,
    parse_named_merger,
    parse_reviewed_legacy_table,
    reconstruct_membership,
)
from cnequity.adapters.exchange.index_notices import (
    parse_star50_adjustment,
    parse_star50_attachment,
    parse_star50_effective_date,
    parse_star50_launch_date,
    parse_star50_no_change_date,
    parse_star50_opening,
)
from cnequity.domain.contracts import contract_fingerprint, dataset_contract
from cnequity.domain.datasets import granularity_for_dataset
from cnequity.domain.http_policy import record_http_response
from cnequity.domain.partitions import partition_value
from cnequity.domain.rate_limit import source_request
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.file_lock import lake_mutation_lock
from cnequity.quality.publication import check_repair_publication
from cnequity.storage.parquet import CuratedWriter
from cnequity.storage.raw_archive import RawPayloadArchive
from cnequity.storage.revisions import RevisionStore

pd = lazy_module("pandas")

DATASET = "index_constituents"


def _catalog(index_symbol="399001.SZ"):
    supported = {"399001.SZ": ("cni", 500), "000688.SH": ("exchange", 50)}
    if index_symbol not in supported:
        raise ValueError("unsupported reviewed index")
    package, size = supported[index_symbol]
    catalog = json.loads(
        resources.files(f"cnequity.adapters.{package}")
        .joinpath(f"seeds/{index_symbol[:6]}-history.json")
        .read_text()
    )
    if (
        catalog.get("schema_version") != 1
        or catalog.get("index_symbol") != index_symbol
        or catalog.get("member_count") != size
    ):
        raise ValueError("unsupported reviewed source catalog")
    return catalog


def _matches(record, spec):
    return (
        record.get("dataset") == DATASET
        and record.get("source") == spec["source"]
        and record.get("url") == spec["url"]
        and record.get("request_params") == spec["request_params"]
        and record.get("payload_sha256") == spec["sha256"]
    )


def _payload(config, archive, client, spec, *, refresh, run_id):
    directory = config.meta_root / "raw" / DATASET / f"source={spec['source']}"
    for path in sorted(directory.glob(f"captured_date=*/{spec['sha256']}*.json"), reverse=True):
        try:
            record = json.loads(path.read_text())
            if _matches(record, spec):
                payload = archive.read(record)
                datetime.fromisoformat(record["captured_at"])
                return payload, record
        except (OSError, ValueError, KeyError, TypeError, RuntimeError):
            continue
    if refresh:
        if not config.sources.get(spec["source"], True):
            raise ValueError(f"{spec['source']} source disabled")
        with source_request(config, spec["source"]):
            response = client.get(spec["url"], params=spec["request_params"])
            record_http_response(config, spec["source"], response)
        response.raise_for_status()
        payload = response.content
        # Archive changed responses too; they are evidence of a source change,
        # never a license to apply a previously reviewed date to different bytes.
        record = archive.archive(
            DATASET,
            payload,
            source=spec["source"],
            run_id=run_id,
            url=spec["url"],
            request_params=spec["request_params"],
            payload_format=spec["format"],
        ).to_dict()
        if hashlib.sha256(payload).hexdigest() != spec["sha256"]:
            raise ValueError("official attachment differs from the reviewed source catalog")
        return payload, record
    raise ValueError("reviewed official attachment is not archived; use --refresh-sources")


def _replay(config, *, refresh_sources, run_id, index_symbol):
    catalog = _catalog(index_symbol)
    symbol = catalog["index_symbol"]
    opening_day, closing_day = map(
        date.fromisoformat, (catalog["opening_date"], catalog["closing_date"])
    )
    archive = RawPayloadArchive(config.meta_root, enabled=True, datasets=[DATASET])
    receipts = []
    seen = {}
    with httpx.Client(timeout=30, follow_redirects=True) as client:

        def read(spec):
            identity = json.dumps(spec, sort_keys=True)
            if identity not in seen:
                payload, record = _payload(
                    config, archive, client, spec, refresh=refresh_sources, run_id=run_id
                )
                seen[identity] = payload
                receipts.append(record)
            return seen[identity]

        if symbol == "000688.SH":
            opening_members = parse_star50_opening(read(catalog["opening"]))
            if parse_star50_launch_date(read(catalog["opening_notice"])) != opening_day:
                raise ValueError("opening notice live launch differs from the reviewed catalog")
            changes = []
            for item in catalog["events"]:
                payload = read(item["payload"])
                day = date.fromisoformat(item["effective_date"])
                if item["parser"] == "sse_announcement":
                    change = parse_star50_adjustment(payload)
                elif item["parser"] == "sse_attachment":
                    authority_day = parse_star50_effective_date(read(item["date_authority"]))
                    if authority_day != day:
                        raise ValueError(
                            "announcement effective date differs from the reviewed catalog"
                        )
                    change = parse_star50_attachment(
                        payload, kind=item["payload"]["format"], effective_date=authority_day
                    )
                else:
                    raise ValueError("unsupported reviewed SSE catalog parser")
                if change.effective_date != day:
                    raise ValueError(
                        "announcement effective date differs from the reviewed catalog"
                    )
                changes.append(change)
            unchanged = set()
            for item in catalog["verified_no_change"]:
                day = parse_star50_no_change_date(read(item["payload"]))
                if (
                    day != date.fromisoformat(item["effective_date"])
                    or not opening_day < day <= closing_day
                    or day in unchanged
                    or day in {change.effective_date for change in changes}
                ):
                    raise ValueError("no-change announcement contradicts the reviewed catalog")
                unchanged.add(day)
        else:
            opening = pd.read_excel(BytesIO(read(catalog["opening"])), dtype=str)
            # The CNI workbook names its columns in Chinese.
            code = "证券代码"  # i18n-ignore
            declaration = f"公告时间：2015-3-20 正式实施时间：{opening_day.year}-{opening_day.month}-{opening_day.day}"  # i18n-ignore
            if code not in opening.columns or declaration not in opening.columns:
                raise ValueError("opening workbook does not identify its declared effective date")
            members = opening[code].tolist()
            if len(members) != len(set(members)):
                raise ValueError("opening workbook duplicates members")
            opening_members = frozenset(member + ".SZ" for member in members)
            changes = []
            for item in catalog["events"]:
                payload = read(item["payload"])
                day = date.fromisoformat(item["effective_date"])
                parser = item["parser"]
                if parser == "workbook":
                    # The separately reviewed, digest-bound notice proves the date
                    # (including old first-trading-day rules). The XLS has no date.
                    read(item["date_authority"])
                    change = parse_change_workbook(payload, index_symbol=symbol, effective_date=day)
                elif parser == "reviewed_legacy":
                    change = parse_reviewed_legacy_table(
                        payload,
                        index_symbol=symbol,
                        effective_date=day,
                        reviewed_sha256=item["payload"]["sha256"],
                    )
                elif parser == "merger":
                    change = parse_named_merger(payload, index_symbol=symbol)
                elif parser == "announcement":
                    authority = (
                        read(item["delisting_authority"]) if "delisting_authority" in item else None
                    )
                    change = parse_announcement(
                        payload,
                        kind=item["payload"]["format"],
                        index_symbol=symbol,
                        delisting_authority=authority,
                    )
                else:
                    raise ValueError("unsupported reviewed catalog parser")
                if change.effective_date != day:
                    raise ValueError(
                        "announcement effective date differs from the reviewed catalog"
                    )
                changes.append(change)
    reference = catalog["closing_reference"]
    # A portable, fixed-date publisher reference is bundled because CNI's
    # download-history endpoint rotates its workbook. This is a disclosed
    # reference book, not a fresh download or an invented archive receipt.
    closing_members = reference["members"]
    if len(closing_members) != len(set(closing_members)):
        raise ValueError("closing reference duplicates members")
    frame = reconstruct_membership(
        index_symbol=symbol,
        opening_date=opening_day,
        opening_members=opening_members,
        closing_date=closing_day,
        closing_members=frozenset(closing_members),
        changes=changes,
    )
    fetched = max(datetime.fromisoformat(record["captured_at"]) for record in receipts)
    frame = with_provenance(
        frame,
        source="exchange" if symbol == "000688.SH" else "cni",
        data_version=data_version_for(DATASET),
    ).with_columns(pl.lit(fetched).alias("fetched_at"))
    return frame, catalog, receipts


def _replacement(before, rows, *, symbol, start, end):
    selected = before.filter(
        (pl.col("index_symbol") == symbol) & pl.col("as_of_date").is_between(start, end)
    )
    retained = []
    books = {
        book["as_of_date"][0]: frozenset(book["symbol"]) for book in rows.partition_by("as_of_date")
    }
    days = sorted(books)
    for book in selected.partition_by("as_of_date"):
        if (
            not book["source"].is_in(["exchange", "cni_weights"]).all()
            or book["weight"].null_count() == book.height
        ):
            continue
        day = book["as_of_date"][0]
        expected = books[days[bisect_right(days, day) - 1]]
        if (
            book.height != len(expected)
            or frozenset(book["symbol"]) != expected
            or book["symbol"].n_unique() != book.height
            or book["weight"].null_count()
            or not book["weight"].is_finite().all()
            or not book["weight"].is_between(0, 1).all()
            or not 0.99 <= book["weight"].sum() <= 1.01
        ):
            raise ValueError(
                "historical membership conflicts with a retained official dated weight book"
            )
        retained.append(book)
    weighted = pl.concat(retained, how="diagonal_relaxed") if retained else before.clear()
    new = rows.join(
        weighted.select("index_symbol", "as_of_date").unique(),
        on=["index_symbol", "as_of_date"],
        how="anti",
    )
    kept = before.filter(
        ~((pl.col("index_symbol") == symbol) & pl.col("as_of_date").is_between(start, end))
    )
    return (
        pl.concat([kept, weighted, new], how="diagonal_relaxed"),
        selected.height,
        weighted.height,
    )


def repair_index_history(config, *, index_symbol="399001.SZ", apply=False, refresh_sources=False):
    run_id = "index-history-" + uuid4().hex
    try:
        rows, catalog, receipts = _replay(
            config, refresh_sources=refresh_sources, run_id=run_id, index_symbol=index_symbol
        )
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, httpx.HTTPError) as exc:
        return {"applied": False, "rows": 0, "failures": [{"error": str(exc)}]}
    start, end = map(date.fromisoformat, (catalog["opening_date"], catalog["closing_date"]))
    report = {
        "applied": False,
        "rows": rows.height,
        "snapshots": rows["as_of_date"].n_unique(),
        "index_symbol": catalog["index_symbol"],
        "opening_date": str(start),
        "closing_date": str(end),
        "catalog_version": catalog["catalog_version"],
        "historical_weighted_rows": 0,
        "closing_reference": {
            k: v for k, v in catalog["closing_reference"].items() if k != "members"
        },
        "source_receipts": receipts,
        "failures": [],
    }
    with lake_mutation_lock(config.meta_root) if apply else nullcontext():
        store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
        if apply:
            store.ensure_current(DATASET)
        base = store.current_root(DATASET)
        files = list(base.rglob("*.parquet")) if base is not None and base.exists() else []
        before = (
            pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")
            if files
            else rows.clear()
        )
        try:
            merged, removed, weighted = _replacement(
                before, rows, symbol=catalog["index_symbol"], start=start, end=end
            )
        except ValueError as exc:
            report["failures"].append({"error": str(exc)})
            return report
        report.update(replaced_range_rows=removed, retained_weighted_rows=weighted)
        fields = ["index_symbol", "symbol", "as_of_date", "weight", "source", "data_version"]
        if before.select(fields).sort(fields).equals(merged.select(fields).sort(fields)):
            report["note"] = "reviewed history already present"
            return report
        if not apply:
            return report
        store.materialize_current(DATASET)
        writer = CuratedWriter(config.curated_root)
        granularity = granularity_for_dataset(DATASET)
        staged = merged.with_columns(
            pl.col("as_of_date")
            .map_elements(lambda day: partition_value(day, granularity), return_dtype=pl.String)
            .alias("_partition")
        )
        physical = {}
        for path in files:
            directory = path.relative_to(base).parts[0]
            if not directory.startswith("as_of_date="):
                raise ValueError("index history requires the registered partition layout")
            value = directory.removeprefix("as_of_date=")
            physical.setdefault(value, []).append(pl.read_parquet(path))
        # Compare physical contents, not just their logical date slice. An old
        # file may contain rows belonging to another month; skipping its write
        # would leave removed members behind and duplicate retained rows.
        affected = sorted(set(physical) | set(staged["_partition"]))
        changed = []
        for value in affected:
            partition = staged.filter(pl.col("_partition") == value).drop("_partition")
            old = (
                pl.concat(physical[value], how="diagonal_relaxed")
                if value in physical
                else before.clear()
            )
            if old.select(fields).sort(fields).equals(partition.select(fields).sort(fields)):
                continue
            changed.append(
                writer.write_partition(
                    DATASET, "as_of_date", value, partition, "part-merged.parquet"
                )
            )
        publication = check_repair_publication(config, DATASET, run_id, changed)
        contract = dataset_contract(DATASET)
        revision = store.commit(
            DATASET,
            run_id=run_id,
            changed_files=changed,
            schema_version=int(contract["schema_version"]),
            contract_fingerprint=contract_fingerprint(contract),
            metadata={
                "reason": "reviewed_official_index_history",
                **report,
                "publication_audit": publication["report_path"],
            },
        )
        if revision is None:
            raise RuntimeError("index history repair produced no revision")
        report.update(applied=True, revision=revision.revision)
        return report
