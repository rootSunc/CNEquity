"""Source-backed cash dates for exchange-traded funds in CNINFO notices.

Fund distributions have their own announcement directory and payment schedule.
Stock corporate-action endpoints and the A-share ex-date rule cannot establish
when a fund's cash becomes available.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
from datetime import date, timedelta

import httpx
import polars as pl

from cnequity._lazy import lazy_attribute
from cnequity.adapters.cninfo.announcements import post_with_retry
from cnequity.adapters.cninfo.payment_notices import _get, _source_date
from cnequity.storage.raw_archive import (
    RawPayloadArchive,
    StageWrite,
    begin_capture,
    verify_raw_archive,
)

PdfReader = lazy_attribute("pypdf", "PdfReader")

_DIRECTORY = "https://www.cninfo.com.cn/new/data/fund_stock.json"
_QUERY = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
_PDF_ROOT = "https://static.cninfo.com.cn/"
_DATE = r"(20\d{2})年(\d{1,2})月(\d{1,2})日"
_TITLES = ("分红公告", "收益分配公告", "利润分配公告")


def _unique_date(text: str, label: str) -> date | None:
    values = set()
    for match in re.finditer(re.escape(label) + r"[：:]?" + _DATE, text):
        try:
            values.add(date(*(int(part) for part in match.groups())))
        except ValueError:
            return None
    return next(iter(values)) if len(values) == 1 else None


def _exchange_date(text: str, label: str, listed_class: str | None = None) -> date | None:
    """A listed fund uses the explicitly labelled on-exchange schedule."""
    match = re.search(
        re.escape(label) + r"[：:]?" + _DATE + r"\(场内\)" + _DATE + r"\(场外\)", text
    )
    if match is not None:
        try:
            return date(*(int(part) for part in match.groups()[:3]))
        except ValueError:
            return None
    if listed_class:
        match = re.search(
            re.escape(label) + r"[：:]?" + _DATE + r"\(" + re.escape(listed_class) + r"\)", text
        )
        if match is not None:
            try:
                return date(*(int(part) for part in match.groups()))
            except ValueError:
                return None
        if re.search(re.escape(label) + r"[：:]?" + _DATE + r"\([^)]+\)", text):
            return None
    return _unique_date(text, label)


def _class_cash(text: str, code: str) -> float | None:
    """Pair a listed class code with its own amount in the issuer's table."""
    flat = " ".join(text.replace("（", "(").replace("）", ")").split())
    codes = re.search(r"下属(?:分级|各类)基金的交易代码\s+((?:\d{6}\s+){1,3}\d{6})", flat)
    amounts = re.search(
        r"本次下属(?:分级|各类)基金分红方案\s*\(单位[：:]?\s*(?:人民币)?元\s*/10\s*份\s*基金\s*份额\)"
        r"\s*((?:(?:\d+(?:\.\d+)?|-)\s+){1,3}(?:\d+(?:\.\d+)?|-))",
        flat,
    )
    if codes is None or amounts is None:
        return None
    class_codes, class_amounts = codes.group(1).split(), amounts.group(1).split()
    if (
        len(class_codes) != len(class_amounts)
        or len(class_codes) < 2
        or len(set(class_codes)) != len(class_codes)
        or code not in class_codes
    ):
        return None
    selected = class_amounts[class_codes.index(code)]
    return float(selected) / 10 if selected != "-" else None


def parse_fund_payment_text(text: str) -> dict | None:
    """Accept an explicit listed-class gross amount and settlement schedule."""
    compact = re.sub(r"\s+", "", text).replace("（", "(").replace("）", ")")
    codes = set(re.findall(r"基金主代码[：:]?(\d{6})", compact))
    if len(codes) != 1:
        return None
    code = next(iter(codes))
    listed_match = re.search(r"场内简称\s+([^\s]+)", text)
    listed_class = listed_match.group(1) if listed_match else None
    if (
        "下属分级基金" in compact
        or "下属各类基金" in compact
        or re.search(r"(?:A类|C类|E类|A份额|C份额|E份额)", compact)
    ):
        cash = _class_cash(text, code)
    else:
        amounts = {
            float(value) / 10
            for value in re.findall(
                r"本次(?:基金)?分红方案\(单位[：:]?(?:人民币)?元/10份基金份额\)[：:]?([0-9]+(?:\.[0-9]+)?)",
                compact,
            )
        }
        cash = next(iter(amounts)) if len(amounts) == 1 else None
    if cash is None:
        return None
    record_date = _unique_date(compact, "权益登记日")
    ex_date = _exchange_date(compact, "除息日", listed_class)
    payment_date = _exchange_date(compact, "现金红利发放日", listed_class)
    if (
        record_date is None
        or ex_date is None
        or payment_date is None
        or not record_date < ex_date <= payment_date
    ):
        return None
    return {
        "code": code,
        "cash_dividend": cash,
        "record_date": record_date,
        "ex_date": ex_date,
        "payment_date": payment_date,
    }


def parse_fund_payment_pdf(payload: bytes) -> dict | None:
    if not payload.startswith(b"%PDF"):
        return None
    text = "".join(page.extract_text() or "" for page in PdfReader(io.BytesIO(payload)).pages)
    return parse_fund_payment_text(text)


def _query_notices(client, config, code: str, org_id: str, year: int, on_response):
    rows = []
    page = 1
    while True:
        body = {
            "pageNum": page,
            "pageSize": 30,
            "column": "fund",
            "tabName": "fulltext",
            "plate": "",
            "stock": f"{code},{org_id}",
            "searchkey": "分红",
            "secid": "",
            "category": "",
            "trade": "",
            "seDate": f"{max(year - 1, 2016)}-11-01~{year}-12-31"
            if year > 2016
            else "2016-01-01~2016-12-31",
        }
        parsed = post_with_retry(
            client,
            _QUERY,
            data=body,
            config=config,
            on_response=lambda response, data, attempt, _body=body: on_response(
                response, data, attempt, _body
            ),
        )
        rows.extend(parsed.get("announcements") or [])
        total = int(parsed.get("totalpages") or parsed.get("totalPages") or 1)
        if page >= total:
            return rows
        if page >= 20:
            raise ValueError(f"CNINFO fund notice query exceeded 20 pages for {code}/{year}")
        page += 1


def repair_fund_payment_notices(
    config, run_id: str, existing: pl.DataFrame, *, stage: StageWrite, metrics=None
):
    """Archive original responses and stage only unique exact issuer facts."""
    if existing.is_empty() or not config.sources.get("cninfo", False):
        return [], []
    client = httpx.Client(timeout=30.0, headers={"User-Agent": "Mozilla/5.0"})
    done, diagnostics = [], []
    try:
        directory_response = _get(client, _DIRECTORY, config=config)
        if metrics is not None:
            metrics["network_requests"] = metrics.get("network_requests", 0) + 1
            metrics["cninfo_fund_network_responses"] = (
                metrics.get("cninfo_fund_network_responses", 0) + 1
            )
        directory = {
            str(row.get("code") or ""): str(row.get("orgId") or "")
            for row in json.loads(directory_response.content).get("stockList") or []
        }
        for symbol, group in existing.group_by("symbol", maintain_order=True):
            symbol = symbol[0] if isinstance(symbol, tuple) else symbol
            code = symbol.split(".", 1)[0]
            org_id = directory.get(code)
            if not org_id:
                diagnostics.extend(
                    {
                        "symbol": symbol,
                        "ex_date": str(row["ex_date"]),
                        "reason": "fund_directory_identity_missing",
                    }
                    for row in group.to_dicts()
                )
                continue
            for year, year_group in group.group_by(
                pl.col("ex_date").dt.year(), maintain_order=True
            ):
                year = int(year[0] if isinstance(year, tuple) else year)
                scope = f"cninfo-fund-payment:{symbol}:{year}"
                nonce = begin_capture(
                    config, "corporate_actions", run_id, source="cninfo", request_scope=scope
                )
                archive = RawPayloadArchive(
                    config.meta_root,
                    enabled=True,
                    datasets=["corporate_actions"],
                    compression=config.raw_archive_compression,
                    max_payload_bytes=config.raw_archive_max_payload_bytes,
                    capture_owner=config,
                    capture_run_id=run_id,
                    capture_source="cninfo",
                    capture_scope=scope,
                    capture_nonce=nonce,
                )
                records = []

                def archive_query(
                    response,
                    parsed,
                    attempt,
                    body,
                    *,
                    _records=records,
                    _archive=archive,
                    _scope=scope,
                ):
                    if metrics is not None:
                        metrics["network_requests"] = metrics.get("network_requests", 0) + 1
                        metrics["cninfo_fund_network_responses"] = (
                            metrics.get("cninfo_fund_network_responses", 0) + 1
                        )
                    _records.append(
                        _archive.archive(
                            "corporate_actions",
                            response.content,
                            source="cninfo",
                            request_params=body,
                            run_id=run_id,
                            url=_QUERY,
                            payload_format="json" if parsed is not None else "bytes",
                            http_metadata={"wire_exact": True, "attempt": attempt},
                            observation_id=f"{run_id}:{_scope}:query:{body['pageNum']}:{attempt}",
                            request_scope=_scope,
                        )
                    )

                announcements = _query_notices(client, config, code, org_id, year, archive_query)
                candidates = {
                    str(item.get("announcementId")): item
                    for item in announcements
                    if str(item.get("secCode") or "") == code
                    and any(title in str(item.get("announcementTitle") or "") for title in _TITLES)
                    and not any(
                        word in str(item.get("announcementTitle") or "")
                        for word in ("更正", "补充")
                    )
                    and item.get("announcementId")
                }
                rows = []
                facts_by_id = {}
                for old in year_group.to_dicts():
                    accepted = []
                    for notice_id, item in candidates.items():
                        published = _source_date(item)
                        if (
                            published is None
                            or not old["ex_date"] - timedelta(days=75)
                            <= published
                            <= old["ex_date"]
                        ):
                            continue
                        path = str(item.get("adjunctUrl") or "").lstrip("/")
                        if not path:
                            continue
                        if notice_id not in facts_by_id:
                            url = _PDF_ROOT + path
                            response = _get(client, url, config=config)
                            if metrics is not None:
                                metrics["network_requests"] = metrics.get("network_requests", 0) + 1
                                metrics["cninfo_fund_network_responses"] = (
                                    metrics.get("cninfo_fund_network_responses", 0) + 1
                                )
                            payload = response.content
                            digest = hashlib.sha256(payload).hexdigest()
                            records.append(
                                archive.archive(
                                    "corporate_actions",
                                    payload,
                                    source="cninfo",
                                    request_params={
                                        "announcement_id": notice_id,
                                        "pdf_sha256": digest,
                                    },
                                    run_id=run_id,
                                    url=url,
                                    payload_format="bytes",
                                    http_metadata={"wire_exact": True},
                                    observation_id=f"{run_id}:{scope}:notice:{notice_id}",
                                    request_scope=scope,
                                )
                            )
                            try:
                                facts_by_id[notice_id] = (parse_fund_payment_pdf(payload), digest)
                            except Exception:
                                facts_by_id[notice_id] = (None, digest)
                        facts, digest = facts_by_id[notice_id]
                        if facts is not None and (
                            facts["code"] == code
                            and facts["ex_date"] == old["ex_date"]
                            and abs(float(facts["cash_dividend"]) - float(old["cash_dividend"]))
                            <= max(1e-8, abs(float(old["cash_dividend"])) * 1e-7)
                        ):
                            accepted.append((notice_id, facts, digest))
                    if len(accepted) != 1:
                        diagnostics.append(
                            {
                                "symbol": symbol,
                                "ex_date": str(old["ex_date"]),
                                "reason": "fund_notice_missing_or_ambiguous",
                                "candidate_count": len(accepted),
                            }
                        )
                        continue
                    notice_id, facts, digest = accepted[0]
                    rows.append(
                        {
                            **old,
                            "payment_date": facts["payment_date"],
                            "payment_source": f"fund_notice:{notice_id}:sha256:{digest}",
                            "source": "cninfo",
                        }
                    )
                    done.append((symbol, old["ex_date"]))
                if rows:
                    evidence = verify_raw_archive(
                        config,
                        "corporate_actions",
                        run_id,
                        source="cninfo",
                        request_scope=scope,
                        records=records,
                    )
                    stage(
                        config,
                        run_id,
                        "corporate_actions",
                        pl.DataFrame(rows),
                        source="cninfo",
                        batch_id=f"fund-payment-{symbol}-{year}",
                        raw_archive_evidence=evidence,
                        request_params={
                            "symbol": symbol,
                            "year": year,
                            "validation": "fund_code+ex_date+gross_cash+payment_date",
                        },
                    )
    finally:
        client.close()
    return done, diagnostics
