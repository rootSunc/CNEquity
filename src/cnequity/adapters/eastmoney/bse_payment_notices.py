"""Conservative BJ/NEEQ issuer-notice cash-payment repair.

EastMoney distributes the issuer's original PDF.  Its listing timestamp is
not treated as the notice's first public availability time.  Only the PDF's
explicit code, record/ex/payment dates and gross cash entitlement can repair
an existing event; corrections, ambiguity and unavailable pages stay blocked.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
from datetime import date, timedelta

import polars as pl
from pypdf import PdfReader

from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.domain.bse_codes import current_to_legacy
from cnequity.steps.http_common import verify_raw_archive, write_fetched
from cnequity.storage.raw_archive import RawPayloadArchive, begin_capture

_QUERY = "https://np-anotice-stock.eastmoney.com/api/security/ann"
_PDF = "https://pdf.dfcfw.com/pdf/H2_{art_code}_1.pdf"
_TITLE = ("权益分派实施公告", "权益分配实施公告", "利润分配实施公告")
_DATE = r"(20\d{2})年(\d{1,2})月(\d{1,2})日"
# A current-code listing may distribute an original PDF printed under the
# issuer's old code.  These are reviewed issuer correspondences, not dated
# trading-symbol conversions.  Never infer an effective date from the BSE
# table's "listing date" (for former Select-tier issuers it is their tier date).
# https://www.bse.cn/service/code_mapping.html
_REVIEWED_OLD_CODES = current_to_legacy()


def _matches_reviewed_issuer_code(current_code: str, pdf_code: str) -> bool:
    return pdf_code == current_code or _REVIEWED_OLD_CODES.get(current_code) == pdf_code


def _notice_query_codes(current_code: str, year: int) -> tuple[str, ...]:
    """Search a reviewed old issuer code for pre-BSE documents as well.

    This is only a search alias.  The PDF must independently match the issuer
    code and event economics; it establishes no trading-code conversion date.
    """
    old_code = _REVIEWED_OLD_CODES.get(current_code)
    if old_code and year < 2022:
        return current_code, old_code
    return (current_code,)


def _printed_date(text: str, names: tuple[str, ...]) -> date | None:
    found: set[date] = set()
    for name in names:
        for match in re.finditer(name + r"[：:]?" + _DATE, text):
            try:
                found.add(date(*(int(part) for part in match.groups())))
            except ValueError:
                return None
    return next(iter(found)) if len(found) == 1 else None


def parse_bj_payment_pdf(payload: bytes) -> dict | None:
    if not payload.startswith(b"%PDF"):
        return None
    text = "".join(page.extract_text() or "" for page in PdfReader(io.BytesIO(payload)).pages)
    return _parse_bj_payment_text(text)


def _parse_bj_payment_text(text: str) -> dict | None:
    compact = re.sub(r"\s+", "", text).replace("（", "(").replace("）", ")")
    if not any(word in compact for word in ("北京证券交易所", "全国中小企业股份转让系统")):
        return None
    codes = set(re.findall(r"证券代码[：:]?(\d{6})", compact))
    cash_values = {
        float(value) / 10
        for value in re.findall(
            r"(?:向全体股东)?每10股派(?:发)?([0-9]+(?:\.[0-9]+)?)元人民币现金",
            compact,
        )
    }
    # The gross figure must be explicit and identifiable as pretax cash.
    if "扣税说明" not in compact and "含税" not in compact:
        return None
    record_date = _printed_date(compact, ("权益登记日为", "股权登记日为"))
    ex_date = _printed_date(compact, ("除权除息日为", "除息日为"))
    payment_date = _printed_date(
        compact,
        ("代派的现金红利将于", "现金红利发放日为", "红利发放日为"),
    )
    if (
        len(codes) != 1
        or len(cash_values) != 1
        or record_date is None
        or ex_date is None
        or payment_date is None
        or not record_date < ex_date <= payment_date
    ):
        return None
    return {
        "code": next(iter(codes)),
        "record_date": record_date,
        "ex_date": ex_date,
        "payment_date": payment_date,
        "cash_dividend": next(iter(cash_values)),
    }


def _query_year(
    client: EastMoneyClient, code: str, year: int, archive, run_id: str, scope: str, metrics: dict
):
    params = {
        "sr": "-1",
        "page_size": "100",
        "page_index": "1",
        "ann_type": "A",
        "client_source": "web",
        "stock_list": code,
        "begin_time": str(date(year, 1, 1) - timedelta(days=75)),
        "end_time": str(date(year, 12, 31)),
    }
    records = []
    items = []
    page = 1
    while True:
        params["page_index"] = str(page)
        response = client.get(_QUERY, params=params)
        response.raise_for_status()
        metrics["bj_issuer_network_responses"] = metrics.get("bj_issuer_network_responses", 0) + 1
        metrics["network_requests"] = metrics.get("network_requests", 0) + 1
        parsed = json.loads(response.content)
        data = parsed.get("data") or {}
        rows = data.get("list") or []
        records.append(
            archive.archive(
                "corporate_actions",
                response.content,
                source="eastmoney",
                request_params=dict(params),
                run_id=run_id,
                url=str(response.request.url),
                payload_format="json",
                http_metadata={"wire_exact": True},
                observation_id=f"{scope}:query:{code}:{page}",
                request_scope=scope,
            )
        )
        items.extend(rows)
        total = int(data.get("total_hits") or 0)
        if page * 100 >= total:
            return items, records
        if page >= 10:
            raise ValueError(f"BJ issuer notice query exceeds 10 pages: {code}/{year}")
        page += 1


def repair_bj_payment_notices(
    config,
    run_id: str,
    existing: pl.DataFrame,
    *,
    all_actions: pl.DataFrame | None = None,
    metrics: dict | None = None,
):
    """Stage only unique four-field matches, preserving every unresolved row."""
    if existing.is_empty() or not config.sources.get("eastmoney", False):
        return [], []
    pending = existing.filter(pl.col("symbol").str.ends_with(".BJ"))
    if pending.is_empty():
        return [], []
    done = []
    diagnostics = []
    # The cash-only repair path cannot validate a same-day stock distribution.
    # In particular, some vendor rows contain both bonus and transfer entries
    # while the issuer notice specifies transfer only.  Require a separate
    # economic reconciliation before accepting a payment repair on that date.
    if all_actions is not None and not all_actions.is_empty():
        stock_keys = {
            (row["symbol"], row["ex_date"])
            for row in all_actions.to_dicts()
            if row["action_type"] != "cash_dividend"
            and row["action_type"] in {"bonus", "transfer", "split"}
        }
        blocked = [
            row for row in pending.to_dicts() if (row["symbol"], row["ex_date"]) in stock_keys
        ]
        diagnostics.extend(
            {
                "symbol": row["symbol"],
                "ex_date": str(row["ex_date"]),
                "reason": "bj_stock_terms_require_reconciliation",
            }
            for row in blocked
        )
        if blocked:
            pending = pending.filter(
                ~pl.struct("symbol", "ex_date").is_in(
                    [{"symbol": row["symbol"], "ex_date": row["ex_date"]} for row in blocked]
                )
            )
    if pending.is_empty():
        return done, diagnostics
    if metrics is None:
        metrics = {}
    client = EastMoneyClient(config=config)
    try:
        for symbol, group in pending.group_by("symbol", maintain_order=True):
            symbol = symbol[0] if isinstance(symbol, tuple) else symbol
            code = symbol.split(".", 1)[0]
            for year, year_group in group.group_by(
                pl.col("ex_date").dt.year(), maintain_order=True
            ):
                year = int(year[0] if isinstance(year, tuple) else year)
                scope = f"bj-issuer-payment:{symbol}:{year}"
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
                items, records = [], []
                query_codes = _notice_query_codes(code, year)
                for query_code in query_codes:
                    query_items, query_records = _query_year(
                        client, query_code, year, archive, run_id, scope, metrics
                    )
                    items.extend(query_items)
                    records.extend(query_records)
                candidates = [
                    item
                    for item in items
                    if any(title in str(item.get("title") or "") for title in _TITLE)
                    and not any(word in str(item.get("title") or "") for word in ("更正", "补充"))
                    and any(
                        str(c.get("stock_code")) in query_codes for c in item.get("codes") or []
                    )
                ]
                corrections = [
                    item
                    for item in items
                    if any(word in str(item.get("title") or "") for word in ("更正", "补充"))
                    and any(title in str(item.get("title") or "") for title in _TITLE)
                    and any(
                        str(c.get("stock_code")) in query_codes for c in item.get("codes") or []
                    )
                ]
                facts_by_id = {}
                for item in candidates:
                    art_code = str(item.get("art_code") or "")
                    if not re.fullmatch(r"AN\d{18}", art_code) or art_code in facts_by_id:
                        continue
                    url = _PDF.format(art_code=art_code)
                    response = client.get(url)
                    response.raise_for_status()
                    metrics["bj_issuer_network_responses"] = (
                        metrics.get("bj_issuer_network_responses", 0) + 1
                    )
                    metrics["network_requests"] = metrics.get("network_requests", 0) + 1
                    payload = response.content
                    records.append(
                        archive.archive(
                            "corporate_actions",
                            payload,
                            source="eastmoney",
                            request_params={
                                "art_code": art_code,
                                "pdf_sha256": hashlib.sha256(payload).hexdigest(),
                            },
                            run_id=run_id,
                            url=url,
                            payload_format="bytes",
                            http_metadata={"wire_exact": True},
                            observation_id=f"{scope}:pdf:{art_code}",
                            request_scope=scope,
                        )
                    )
                    facts_by_id[art_code] = parse_bj_payment_pdf(payload)
                rows = []
                for old in year_group.to_dicts():
                    if any(
                        old.get(field)
                        for field in ("bonus_ratio", "transfer_ratio", "allotment_ratio")
                    ):
                        diagnostics.append(
                            {
                                "symbol": symbol,
                                "ex_date": str(old["ex_date"]),
                                "reason": "bj_stock_terms_unverified",
                            }
                        )
                        continue
                    if any(
                        str(item.get("notice_date") or "")[:10] <= str(old["ex_date"])
                        and str(item.get("notice_date") or "")[:10]
                        >= str(old["ex_date"] - timedelta(days=75))
                        for item in corrections
                    ):
                        diagnostics.append(
                            {
                                "symbol": symbol,
                                "ex_date": str(old["ex_date"]),
                                "reason": "bj_correction_chain_requires_review",
                            }
                        )
                        continue
                    matches = [
                        (art_code, facts)
                        for art_code, facts in facts_by_id.items()
                        if facts is not None
                        and _matches_reviewed_issuer_code(code, facts["code"])
                        and facts["ex_date"] == old["ex_date"]
                        and abs(float(facts["cash_dividend"]) - float(old["cash_dividend"]))
                        <= max(1e-8, abs(float(old["cash_dividend"])) * 1e-7)
                    ]
                    if len(matches) != 1:
                        diagnostics.append(
                            {
                                "symbol": symbol,
                                "ex_date": str(old["ex_date"]),
                                "reason": "bj_notice_missing_or_ambiguous",
                            }
                        )
                        continue
                    art_code, facts = matches[0]
                    if (
                        old.get("payment_date") is not None
                        or old.get("action_type") != "cash_dividend"
                    ):
                        continue
                    rows.append(
                        {
                            **old,
                            "payment_date": facts["payment_date"],
                            "payment_source": f"issuer_notice:{art_code}:A",
                            "source": "eastmoney",
                        }
                    )
                    done.append((symbol, old["ex_date"]))
                if rows:
                    receipt = verify_raw_archive(
                        config,
                        "corporate_actions",
                        run_id,
                        source="eastmoney",
                        request_scope=scope,
                        records=records,
                    )
                    write_fetched(
                        config,
                        run_id,
                        "corporate_actions",
                        pl.DataFrame(rows),
                        source="eastmoney",
                        batch_id=f"bj-payment-{symbol}-{year}",
                        raw_archive_evidence=receipt,
                        request_params={
                            "issuer_notice_ids": sorted(facts_by_id),
                            "first_publication_time_verified": False,
                        },
                    )
    finally:
        client.close()
    return done, diagnostics
