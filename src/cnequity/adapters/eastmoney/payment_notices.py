"""Reviewed issuer notices for exact events omitted by structured dividend feeds.

Each extraction is pinned to the complete PDF bytes, not inferred from ex-date.
The notice is fetched and checked again before normal staging/publication.
"""

from __future__ import annotations

import hashlib
from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.storage.raw_archive import (
    RawPayloadArchive,
    StageWrite,
    begin_capture,
    verify_raw_archive,
)

# Issuer notice 2024-045, pages 1 and 2: A-share cash 0.03 CNY,
# ex-date/payment 2024-09-11. B shares pay 2024-09-27 and are NOT this event.
# The 0.028 virtual ex-price deduction on page 2 is NOT the cash entitlement.
NOTICES = {
    # BSE issuer notice 2022-060, pages 1-2.  The issuer states the gross
    # 10-share cash amount, 2022-05-17 record date, and that China Clearing's
    # Beijing branch credits cash on the 2022-05-18 ex-date.  The PDF is an
    # exact issuer-document mirror; the BSE listing points to the same notice.
    ("833874.BJ", date(2022, 5, 18)): {
        "url": "https://pdf.dfcfw.com/pdf/H2_AN202205101564715860_1.pdf",
        "official_url": ("https://www.bse.cn/disclosure/2022/2022-05-10/1652167994_202674.pdf"),
        "sha256": "ff302c430bf4d3e0f7ba7648ce35a97330484fae0600f873ddd0ea55830cec19",
        "notice_id": "833874:2022-060",
        "symbol": "833874.BJ",
        "ex_date": date(2022, 5, 18),
        "share_class": "A",
        "cash_dividend": 0.5,
        "payment_date": date(2022, 5, 18),
        "page": "1-2",
    },
    ("002358.SZ", date(2019, 7, 5)): {
        "documents": [
            {
                "url": "https://static.cninfo.com.cn/finalpage/2019-06-28/1206402614.PDF",
                "sha256": "ce7391d8069de2964b2a8116d9571da8e34663d2e413a2c3982c8dfa63093e19",
                "notice_id": "002358:2019-042",
                "page": "1-2",
                "role": "original",
            },
            {
                "url": "https://static.cninfo.com.cn/finalpage/2019-06-29/1206408622.PDF",
                "sha256": "87f5c87f5226fae8508a6133de6b54241d19797ab16c49fcaba2c5dd39175875",
                "notice_id": "002358:2019-043",
                "page": 1,
                "role": "correction",
            },
        ],
        "notice_id": "002358:2019-042+2019-043",
        "symbol": "002358.SZ",
        "ex_date": date(2019, 7, 5),
        "share_class": "A",
        "cash_dividend": 0.1,
        "payment_date": date(2019, 7, 5),
        "page": "1-2+1",
        "source": "cninfo",
    },
    ("002352.SZ", date(2024, 11, 7)): {
        "url": "https://static.cninfo.com.cn/finalpage/2024-10-31/1221573964.PDF",
        "sha256": "338b6f3fe077d74966638cf0caf937e0d5e53aa86d06ed518b23e15c94cdc871",
        "notice_id": "002352:2024-098",
        "symbol": "002352.SZ",
        "ex_date": date(2024, 11, 7),
        "share_class": "A",
        "cash_dividend": 1.4,
        "payment_date": date(2024, 11, 7),
        "page": 3,
        "source": "cninfo",
    },
    ("600094.SH", date(2024, 9, 11)): {
        "url": "https://pdf.dfcfw.com/pdf/H2_AN202409031639713952_1.pdf",
        "sha256": "d14cd9c1d2aa74df4119399564e3eb85a3fddfd6c8a84f90cc005722fc6a8a37",
        "notice_id": "600094:2024-045",
        "symbol": "600094.SH",
        "ex_date": date(2024, 9, 11),
        "share_class": "A",
        "cash_dividend": 0.03,
        "payment_date": date(2024, 9, 11),
        "page": 1,
    },
}


def _notice_documents(notice: dict) -> list[dict]:
    if "documents" in notice:
        return list(notice["documents"])
    return [notice]


def verified_notice_row(old: dict, notice: dict, raw: bytes | list[bytes]) -> dict:
    documents = _notice_documents(notice)
    payloads = [raw] if isinstance(raw, bytes) else list(raw)
    if len(payloads) != len(documents) or any(
        hashlib.sha256(payload).hexdigest() != document["sha256"] or not payload.startswith(b"%PDF")
        for payload, document in zip(payloads, documents, strict=True)
    ):
        raise ValueError("issuer payment notice bytes changed; requires new review")
    if (old["symbol"], old["ex_date"]) != (notice["symbol"], notice["ex_date"]):
        raise ValueError("issuer payment notice event identity mismatch")
    if any(old.get(c) for c in ("bonus_ratio", "transfer_ratio", "allotment_ratio")):
        raise ValueError("cash-only notice cannot confirm stock distribution terms")
    if old["action_type"] != "cash_dividend" or old.get("payment_date") is not None:
        raise ValueError("notice repair only fills an unknown cash payment date")
    if notice["payment_date"] < old["ex_date"]:
        # China Clearing credits A-share cash on the ex-date; an earlier date
        # (usually the record date or last year's template) is a notice typo.
        raise ValueError("issuer payment notice pays before the ex-date")
    if notice["share_class"] != "A" or not (
        old["symbol"].startswith(("60", "00", "30", "68")) or old["symbol"].endswith(".BJ")
    ):
        raise ValueError("issuer payment notice share class mismatch")
    if abs(old["cash_dividend"] - notice["cash_dividend"]) > max(
        1e-8, abs(old["cash_dividend"]) * 1e-7
    ):
        raise ValueError("issuer payment notice cash amount conflicts with stored event")
    return {
        **old,
        "payment_date": notice["payment_date"],
        "payment_source": f"issuer_notice:{notice['notice_id']}:page{notice['page']}:A",
        "source": notice.get("source", "eastmoney"),
    }


def repair_reviewed_notices(
    config, run_id: str, existing: pl.DataFrame, *, stage: StageWrite
) -> list[tuple[str, date]]:
    matches = [row for row in existing.to_dicts() if (row["symbol"], row["ex_date"]) in NOTICES]
    if not matches or not config.sources.get("eastmoney", False):
        return []
    client = EastMoneyClient(config=config)
    done = []
    try:
        for old in matches:
            key = (old["symbol"], old["ex_date"])
            notice = NOTICES[key]
            documents = _notice_documents(notice)
            payloads = []
            for document in documents:
                response = client.get(document["url"])
                response.raise_for_status()
                payloads.append(response.content)
            row = verified_notice_row(old, notice, payloads)
            source = notice.get("source", "eastmoney")
            batch_id = f"payment-notice-{old['symbol']}-{old['ex_date']}"
            if len(documents) == 1:
                document = documents[0]
                stage(
                    config,
                    run_id,
                    "corporate_actions",
                    pl.DataFrame([row]),
                    source=source,
                    batch_id=batch_id,
                    raw_payload=payloads[0],
                    url=document["url"],
                    request_params={
                        "notice_id": document["notice_id"],
                        "pdf_sha256": document["sha256"],
                        "reviewed_page": document["page"],
                        "share_class": "A",
                        **(
                            {"official_url": notice["official_url"]}
                            if notice.get("official_url")
                            else {}
                        ),
                    },
                )
            else:
                scope = f"reviewed-payment-notice:{old['symbol']}:{old['ex_date']}"
                nonce = begin_capture(
                    config,
                    "corporate_actions",
                    run_id,
                    source=source,
                    request_scope=scope,
                )
                archive = RawPayloadArchive(
                    config.meta_root,
                    enabled=True,
                    datasets=["corporate_actions"],
                    compression=config.raw_archive_compression,
                    max_payload_bytes=config.raw_archive_max_payload_bytes,
                    capture_owner=config,
                    capture_run_id=run_id,
                    capture_source=source,
                    capture_scope=scope,
                    capture_nonce=nonce,
                )
                records = []
                for document, payload in zip(documents, payloads, strict=True):
                    records.append(
                        archive.archive(
                            "corporate_actions",
                            payload,
                            source=source,
                            request_params={
                                "notice_id": document["notice_id"],
                                "pdf_sha256": document["sha256"],
                                "reviewed_page": document["page"],
                                "role": document["role"],
                                "share_class": "A",
                            },
                            run_id=run_id,
                            url=document["url"],
                            payload_format="bytes",
                            http_metadata={"wire_exact": True},
                            observation_id=f"{document['notice_id']}:{document['role']}",
                            request_scope=scope,
                        )
                    )
                evidence = verify_raw_archive(
                    config,
                    "corporate_actions",
                    run_id,
                    source=source,
                    request_scope=scope,
                    records=records,
                )
                stage(
                    config,
                    run_id,
                    "corporate_actions",
                    pl.DataFrame([row]),
                    source=source,
                    batch_id=batch_id,
                    raw_archive_evidence=evidence,
                    request_params={
                        "notice_chain": [document["notice_id"] for document in documents],
                        "share_class": "A",
                    },
                )
            done.append(key)
    finally:
        client.close()
    return done
