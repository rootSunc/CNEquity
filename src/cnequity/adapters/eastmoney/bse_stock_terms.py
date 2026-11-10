"""Reviewed corrections for BJ stock distributions duplicated across feeds.

TDX reports a combined 送转 quantity as bonus while THS reports the same
capital-reserve transfer separately.  These issuer PDFs explicitly state
transfer only.  Corrections retain the old revision and stage a zero bonus
row under the existing event key; the already-correct transfer row is kept.
"""

from __future__ import annotations

import hashlib
import io
import re
from datetime import date

import polars as pl
from pypdf import PdfReader

from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.domain.action_evidence import REVIEWED_BJ_CORRECTION_CHAIN, REVIEWED_BJ_TRANSFERS
from cnequity.query.reader import load
from cnequity.storage.raw_archive import (
    RawPayloadArchive,
    StageWrite,
    begin_capture,
    verify_raw_archive,
)

# The reviewed events live in the domain registry, which the canonical merge
# also reads so a later vendor sweep cannot undo them.  Each PDF must still
# match its pinned hash and pass the semantic check below on every run.
REVIEWED = REVIEWED_BJ_TRANSFERS
CORRECTED_CHAIN = REVIEWED_BJ_CORRECTION_CHAIN


def _pdf_text(payload: bytes, digest: str) -> str:
    if not payload.startswith(b"%PDF") or hashlib.sha256(payload).hexdigest() != digest:
        raise ValueError("issuer stock-terms PDF changed")
    return re.sub(
        r"\s+",
        "",
        "".join(page.extract_text() or "" for page in PdfReader(io.BytesIO(payload)).pages),
    )


def verified_bonus_correction(rows: list[dict], payload: bytes, expected: tuple) -> dict:
    """Return only the spurious bonus row, neutralised with source evidence."""
    _art_code, digest, transfer = expected
    text = _pdf_text(payload, digest)
    codes = set(re.findall(r"证券代码[：:]?(\d{6})", text))
    if len(codes) != 1 or not ("北京证券交易所" in text or "全国中小企业股份转让系统" in text):
        raise ValueError("issuer stock-terms identity is not unique")
    if not any(row["symbol"].startswith(next(iter(codes))) for row in rows):
        # Reviewed old/new-code aliases are not trading-code conversions.
        from cnequity.adapters.eastmoney.bse_payment_notices import _matches_reviewed_issuer_code

        if not all(
            _matches_reviewed_issuer_code(row["symbol"][:6], next(iter(codes))) for row in rows
        ):
            raise ValueError("issuer stock-terms code mismatch")
    transfers = {
        float(x) / 10
        for x in re.findall(r"每10股转增([0-9]+(?:\.[0-9]+)?)股", text)
        if float(x) > 0
    }
    bonuses = {
        float(x) / 10 for x in re.findall(r"每10股送(?:红)?股?([0-9]+(?:\.[0-9]+)?)股", text)
    }
    if transfers != {transfer} or any(x > 0 for x in bonuses):
        raise ValueError("issuer stock-terms economic mismatch")
    by_type = {row["action_type"]: row for row in rows}
    if set(by_type) != {"bonus", "transfer", "cash_dividend"}:
        raise ValueError("stock-terms action set changed")
    bonus, transferred = by_type["bonus"], by_type["transfer"]
    if (
        abs(float(bonus["bonus_ratio"]) - transfer) > 1e-8
        or abs(float(transferred["transfer_ratio"]) - transfer) > 1e-8
    ):
        raise ValueError("stored stock-terms amount changed")
    if bonus.get("source") != "tdx_protocol":
        raise ValueError("spurious bonus source changed")
    return {**bonus, "bonus_ratio": 0.0, "source": "eastmoney"}


def verified_correction_chain(rows: list[dict], correction: bytes, final: bytes) -> list[dict]:
    """Reclassify 920799's transfer and fill cash only from its final notice."""
    changed = _pdf_text(correction, CORRECTED_CHAIN["correction"][1])
    published = _pdf_text(final, CORRECTED_CHAIN["final"][1])
    if "证券代码：830799" not in changed or "除权除息" not in changed or "更正" not in changed:
        raise ValueError("issuer correction chain mismatch")
    if (
        "证券代码：830799" not in published
        or "每10股转增5股" not in published
        or "每10股派人民币现金2.5元" not in published
    ):
        raise ValueError("issuer final economic terms mismatch")
    if (
        "权益登记日为：2022年5月31日" not in published
        or "除权除息日为：2022年6月1日" not in published
        or "现金红利将于2022年6月1日" not in published
    ):
        raise ValueError("issuer final dates mismatch")
    by_type = {row["action_type"]: row for row in rows}
    if set(by_type) != {"bonus", "cash_dividend"}:
        raise ValueError("issuer correction current action set changed")
    bonus, cash = by_type["bonus"], by_type["cash_dividend"]
    if (
        abs(float(bonus["bonus_ratio"]) - 0.5) > 1e-8
        or abs(float(cash["cash_dividend"]) - 0.25) > 1e-8
        or cash.get("payment_date") is not None
    ):
        raise ValueError("issuer correction current economics changed")
    return [
        {**bonus, "bonus_ratio": 0.0, "source": "eastmoney"},
        {
            **bonus,
            "action_type": "transfer",
            "bonus_ratio": 0.0,
            "transfer_ratio": 0.5,
            "source": "eastmoney",
        },
        {
            **cash,
            "payment_date": date(2022, 6, 1),
            "payment_source": f"issuer_notice:{CORRECTED_CHAIN['final'][0]}:A",
            "source": "eastmoney",
        },
    ]


def repair_reviewed_bj_stock_terms(
    config, run_id: str, symbols: list[str], start: date, end: date, *, stage: StageWrite
) -> dict:
    keys = [(symbol, day) for symbol, day in REVIEWED if symbol in symbols and start <= day <= end]
    correction_key = (CORRECTED_CHAIN["symbol"], CORRECTED_CHAIN["ex_date"])
    correct_chain = correction_key[0] in symbols and start <= correction_key[1] <= end
    if not keys and not correct_chain:
        return {"rows_read": 0, "rows_written": 0}
    frame = load("corporate_actions", data_root=config.data_root, start=start, end=end)
    client = EastMoneyClient(config=config)
    written = 0
    try:
        for symbol, day in keys:
            rows = frame.filter(
                (pl.col("symbol") == symbol) & (pl.col("ex_date") == day)
            ).to_dicts()
            if not rows or not any(
                r["action_type"] == "bonus" and r["bonus_ratio"] > 0 for r in rows
            ):
                continue
            expected = REVIEWED[(symbol, day)]
            url = f"https://pdf.dfcfw.com/pdf/H2_{expected[0]}_1.pdf"
            response = client.get(url)
            response.raise_for_status()
            corrected = verified_bonus_correction(rows, response.content, expected)
            stage(
                config,
                run_id,
                "corporate_actions",
                pl.DataFrame([corrected]),
                source="eastmoney",
                batch_id=f"bj-stock-terms-{symbol}-{day}",
                raw_payload=response.content,
                url=url,
                request_params={
                    "issuer_notice_id": expected[0],
                    "pdf_sha256": expected[1],
                    "reviewed_transfer_ratio": expected[2],
                },
            )
            written += 1
        if correct_chain:
            symbol, day = correction_key
            rows = frame.filter(
                (pl.col("symbol") == symbol) & (pl.col("ex_date") == day)
            ).to_dicts()
            if rows and any(r["action_type"] == "bonus" and r["bonus_ratio"] > 0 for r in rows):
                scope = f"bj-reviewed-stock-terms:{symbol}:{day}"
                nonce = begin_capture(
                    config, "corporate_actions", run_id, source="eastmoney", request_scope=scope
                )
                archive = RawPayloadArchive(
                    config.meta_root,
                    enabled=True,
                    datasets=["corporate_actions"],
                    compression=config.raw_archive_compression,
                    max_payload_bytes=config.raw_archive_max_payload_bytes,
                    capture_owner=config,
                    capture_run_id=run_id,
                    capture_source="eastmoney",
                    capture_scope=scope,
                    capture_nonce=nonce,
                )
                payloads, records = [], []
                for role in ("correction", "final"):
                    art_code, digest = CORRECTED_CHAIN[role]
                    url = f"https://pdf.dfcfw.com/pdf/H2_{art_code}_1.pdf"
                    response = client.get(url)
                    response.raise_for_status()
                    payloads.append(response.content)
                    records.append(
                        archive.archive(
                            "corporate_actions",
                            response.content,
                            source="eastmoney",
                            request_params={
                                "art_code": art_code,
                                "pdf_sha256": digest,
                                "role": role,
                            },
                            run_id=run_id,
                            url=url,
                            payload_format="bytes",
                            http_metadata={"wire_exact": True},
                            observation_id=f"{scope}:{role}",
                            request_scope=scope,
                        )
                    )
                corrected = verified_correction_chain(rows, *payloads)
                receipt = verify_raw_archive(
                    config,
                    "corporate_actions",
                    run_id,
                    source="eastmoney",
                    request_scope=scope,
                    records=records,
                )
                stage(
                    config,
                    run_id,
                    "corporate_actions",
                    pl.DataFrame(corrected),
                    source="eastmoney",
                    batch_id=f"bj-stock-terms-{symbol}-{day}",
                    raw_archive_evidence=receipt,
                    request_params={
                        "issuer_notice_chain": [
                            CORRECTED_CHAIN[role][0] for role in ("correction", "final")
                        ]
                    },
                )
                written += len(corrected)
    finally:
        client.close()
    return {"rows_read": len(keys) + int(correct_chain), "rows_written": written}
