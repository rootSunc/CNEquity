"""Auditable view of reviewed, holder-specific issuer cash rights.

The issuer notices remain immutable raw evidence. This view refuses to publish
rights when a later corporate-action revision no longer agrees with them.
It does not infer a record date or turn source collection time into disclosure.
"""

from __future__ import annotations

import hashlib
import io
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl

from cnequity._lazy import lazy_attribute
from cnequity.adapters.cninfo.reviewed_holder_notices import _a_share_record_date
from cnequity.domain.cash_entitlements import HOLDER_CLASSES
from cnequity.quality.decision_gaps import (
    _code_sha256,
    check_research_window,
    save_immutable,
)
from cnequity.query.reader import ReaderError, load
from cnequity.storage.raw_archive import RawPayloadArchive
from cnequity.storage.revisions import committed_revision

PdfReader = lazy_attribute("pypdf", "PdfReader")


def _legacy_record_date(meta_root: Path, entry: dict) -> date:
    """Migrate v1 holder evidence from its verified original PDF, never by T-1."""
    digest = entry["source_sha256"]
    paths = sorted(
        (meta_root / "raw" / "corporate_actions" / "source=cninfo").glob(
            f"captured_date=*/{digest}.*.json"
        )
    )
    archive = RawPayloadArchive(meta_root)
    found: set[date] = set()
    for path in paths:
        raw = archive.read(archive.record(path.resolve()))
        if not raw.startswith(b"%PDF"):
            continue
        if hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError(f"holder PDF hash disagrees with evidence: {path}")
        text = "\n".join(page.extract_text() or "" for page in PdfReader(io.BytesIO(raw)).pages)
        parsed = _a_share_record_date(
            text, date.fromisoformat(entry["ex_date"]), date.fromisoformat(entry["payment_date"])
        )
        if parsed is not None:
            found.add(parsed)
    if len(found) != 1:
        raise ValueError(
            f"holder record date is not uniquely verified: {entry['symbol']} {entry['ex_date']}"
        )
    return next(iter(found))


def reviewed_rights_inventory(
    *,
    data_root: Path,
    start: date,
    end: date,
    revision_id: str | None = None,
    holdout_start: date | None = None,
) -> dict:
    """Build a revision-bound view of exact reviewed notices in a safe window."""
    # Guard before any lake or metadata read.
    check_research_window(start, end, holdout_start=holdout_start)
    root = Path(data_root)
    revision = committed_revision(
        root / "curated" / "corporate_actions",
        dataset="corporate_actions",
        meta_root=root / "meta",
    )
    if revision is None:
        raise ReaderError(
            "corporate_actions has no committed revision; build it with `cne backfill corporate_actions`"
        )
    pinned_id = revision_id or revision[1]
    actions = load("corporate_actions", data_root=root, start=start, end=end, revision=pinned_id)
    by_key = {
        (row["symbol"], row["ex_date"]): row
        for row in actions.filter(pl.col("action_type") == "cash_dividend").iter_rows(named=True)
    }
    rights: list[dict] = []
    seen: set[tuple[str, date, str]] = set()
    evidence_dir = root / "meta" / "decision_cash_entitlements"
    for path in sorted(evidence_dir.glob("*.json")):
        raw = path.read_bytes()
        if path.stem != hashlib.sha256(raw).hexdigest():
            raise ValueError(f"cash-right evidence content hash mismatch: {path}")
        payload = json.loads(raw)
        if payload.get("schema") != "cnequity.holder_cash_evidence.v1":
            raise ValueError(f"unknown cash-right evidence schema: {path}")
        for entry in payload["entries"]:
            ex_date = date.fromisoformat(entry["ex_date"])
            if not start <= ex_date <= end:
                continue
            symbol = entry["symbol"]
            if symbol != payload["symbol"]:
                raise ValueError(f"cash-right evidence symbol mismatch: {path}")
            action = by_key.get((symbol, ex_date))
            if action is None:
                raise ValueError(f"cash-right evidence has no dividend action: {symbol} {ex_date}")
            if action["payment_date"] != date.fromisoformat(entry["payment_date"]):
                raise ValueError(
                    f"cash-right payment date disagrees with revision: {symbol} {ex_date}"
                )
            issuer_source = str(action.get("payment_source") or "")
            if (
                entry["source_sha256"] not in issuer_source
                or f"issuer_notice:{entry['source_document_id']}:" not in issuer_source
            ):
                raise ValueError(f"cash-right source disagrees with revision: {symbol} {ex_date}")
            published = entry.get("source_published_at")
            if published is not None and datetime.fromisoformat(published).tzinfo is None:
                raise ValueError(f"cash-right publication time lacks timezone: {path}")
            if entry["evidence_status"] != "verified_repaired" or not published:
                raise ValueError(f"cash-right evidence is not publication-verified: {path}")
            record_date = _legacy_record_date(root / "meta", entry)
            if entry.get("record_date") and record_date != date.fromisoformat(entry["record_date"]):
                raise ValueError(f"cash-right record date disagrees with original PDF: {path}")
            if record_date >= ex_date:
                raise ValueError(f"cash-right record date must precede ex-date: {path}")
            classes = entry["holder_classes"]
            if "tradable_a" not in classes or any(key not in HOLDER_CLASSES for key in classes):
                raise ValueError(f"cash-right holder classes invalid: {path}")
            if abs(
                Decimal(str(action["cash_dividend"])) - Decimal(classes["tradable_a"])
            ) > Decimal("0.00000001"):
                raise ValueError(f"tradable A cash disagrees with revision: {symbol} {ex_date}")
            for holder_class, amount_text in sorted(classes.items()):
                amount = Decimal(amount_text)
                if amount < 0:
                    raise ValueError(f"negative cash right: {symbol} {ex_date}")
                key = (symbol, ex_date, holder_class)
                if key in seen:
                    raise ValueError(f"duplicate cash right: {key}")
                seen.add(key)
                rights.append(
                    {
                        "symbol": symbol,
                        "ex_date": ex_date.isoformat(),
                        "record_date": record_date.isoformat(),
                        "payment_date": entry["payment_date"],
                        "holder_class": holder_class,
                        "pretax_cash_per_share": str(amount),
                        "source_document_id": entry["source_document_id"],
                        "source_sha256": entry["source_sha256"],
                        "source_published_at": published,
                        "evidence_status": "verified_repaired",
                        "evidence_file_sha256": path.stem,
                    }
                )
    reviewed_actions = {
        (row["symbol"], row["ex_date"])
        for row in by_key.values()
        if "holder_class_cash_verified" in str(row.get("payment_source") or "")
    }
    missing_evidence = sorted(
        reviewed_actions
        - {
            (symbol, ex_date)
            for symbol, ex_date, holder_class in seen
            if holder_class == "tradable_a"
        }
    )
    if missing_evidence:
        raise ValueError(f"reviewed corporate action lacks holder evidence: {missing_evidence}")
    rights.sort(key=lambda row: (row["symbol"], row["ex_date"], row["holder_class"]))
    return {
        "schema": "cnequity.reviewed_cash_rights.v2",
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "corporate_actions_revision": revision[0] if pinned_id == revision[1] else None,
        "corporate_actions_revision_id": pinned_id,
        "code_sha256": _code_sha256(),
        "count": len(rights),
        "event_count": len({(row["symbol"], row["ex_date"]) for row in rights}),
        "record_date_status": "verified_from_original_notice",
        "rights": rights,
    }


def save_reviewed_rights(payload: dict, output_dir: Path) -> Path:
    """Save a content-addressed view, retaining all earlier revisions."""
    return save_immutable(payload, output_dir, prefix="cash-rights")
