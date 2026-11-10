"""Exact cash-payment dates from CNINFO issuer implementation notices.

This is a conservative repair source.  A row is accepted only when one exact
issuer PDF independently states the security code, ex-date, pretax cash amount
and payment date and all four agree with the existing corporate-action event.
Unparseable scans, corrections and conflicting notices remain unresolved.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx
import polars as pl
from pypdf import PdfReader

from cnequity.adapters.cninfo.announcements import post_with_retry
from cnequity.adapters.cninfo.reviewed_cash_corrections import reviewed_cash_correction
from cnequity.adapters.cninfo.reviewed_holder_notices import reviewed_holder_notice
from cnequity.domain.http_policy import record_http_response
from cnequity.domain.market_time import SHANGHAI_TZ
from cnequity.domain.rate_limit import source_request
from cnequity.storage.raw_archive import (
    RawPayloadArchive,
    StageWrite,
    begin_capture,
    verify_raw_archive,
)

_DIRECTORY_URL = "https://www.cninfo.com.cn/new/data/szse_stock.json"
_QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
_PDF_ROOT = "https://static.cninfo.com.cn/"
_NOTICE_TITLES = (
    "权益分派实施公告",
    "权益分配实施公告",
    "分红派息实施公告",
    "末期股息及公司特别股息公告",
    "利润分配实施公告",
    "利润分配方案实施公告",
    "利润分配及资本公积金转增股本实施公告",
    "利润分派实施公告",
    "红利分派实施公告",
    "特别分红实施公告",
    "特别股息分派实施公告",
    "特别派息实施公告",
)
_NOTICE_SEARCH_KEYS = (
    "权益分派",
    "权益分配",
    "利润分配实施公告",
    "利润分配方案实施公告",
    "利润分配及资本公积金转增股本实施公告",
    "利润分派实施公告",
    "红利分派实施公告",
    "分红派息实施公告",
    "末期股息",
    "特别派息实施公告",
    "特别股息分派实施公告",
)
_CORRECTION_MARKERS = ("更正", "补充")
_DATE = r"(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日"


def _compact_text(text: str) -> str:
    compact = re.sub(r"\s+", "", text).replace("（", "(").replace("）", ")")
    # Some issuer PDFs put a repeated page title or statutory disclaimer in
    # the middle of a date at a page boundary. Remove only those fixed
    # boilerplate strings; never invent a missing date component.
    compact = re.sub(r"20\d{2}年年度权益分派实施公告-\d+-", "", compact)
    compact = compact.replace(
        "本公司及董事会全体成员保证信息披露内容的真实、准确和完整，"
        "没有虚假记载、误导性陈述或重大遗漏。",
        "",
    )
    return compact.replace("人民币现金人民币现金", "人民币现金")


def _date_match(patterns: tuple[str, ...], text: str) -> date | None:
    for prefix in patterns:
        match = re.search(prefix + r"[：:]?" + _DATE, text)
        if match:
            try:
                return date(*(int(value) for value in match.groups()[-3:]))
            except ValueError:
                return None
    return None


def _pretax_cash_per_share(text: str) -> float | None:
    special = re.search(
        r"派发20\d{2}年度末期股息现金每股人民币([0-9]+(?:\.[0-9]+)?)元\(含税\)"
        r".{0,60}?派发特别股息现金每股人民币([0-9]+(?:\.[0-9]+)?)元\(含税\)"
        r".{0,60}?两项合计派发股息现金每股人民币([0-9]+(?:\.[0-9]+)?)元\(含税\)",
        text,
    )
    if special is not None:
        annual, extra, total = (float(value) for value in special.groups())
        return total if abs(annual + extra - total) <= 1e-8 else None
    header = text.split("相关日期", 1)[0]
    if (
        "每股分配比例" in header
        and "特别" in header
        and any(marker in header for marker in ("合计", "共计"))
    ):
        # Annual plus special dividends are one cash entitlement on the
        # shared ex-date. The headline must print exactly two components and
        # their total; a conflicting or incomplete breakdown remains blocked.
        amounts = [float(value) for value in re.findall(r"([0-9]+(?:\.[0-9]+)?)元(?:/股)?", header)]
        if len(amounts) == 3:
            total = max(amounts)
            if abs(sum(amounts) - 2 * total) <= 1e-8:
                return total
        return None
    # One implementation can combine an annual and an interim dividend into
    # a single ex-date. Require the printed total to equal both components;
    # the ordinary parser correctly rejects their differing per-share amounts.
    combined = re.search(
        r"A股每股现金红利([0-9]+(?:\.[0-9]+)?)元[，,]为.{0,100}?"
        r"合并后的分配比例[，,]其中(.{0,100}?)相关日期",
        text,
    )
    if combined is not None:
        parts = [
            float(value)
            for value in re.findall(r"每股现金红利([0-9]+(?:\.[0-9]+)?)元", combined.group(2))
        ]
        total = float(combined.group(1))
        if len(parts) == 2 and abs(sum(parts) - total) <= 1e-8:
            return total
        return None
    # A proposal earlier in the PDF can differ from the implemented amount
    # after a share-count change.  An explicitly labelled implementation
    # section is the controlling statement for the payment event.
    implementation = re.search(r"二、(?:本次实施的)?(?:权益分派|利润分配)方案(.+?)(?:三、|$)", text)
    if implementation is not None:
        scoped = _cash_amount_in_text(implementation.group(1))
        if scoped is not None:
            return scoped
    # When a buyback or cancelled incentive shares alter the distribution
    # base, the issuer explicitly holds the total dividend fixed and prints a
    # final per-share amount after the original proposal.  Only that final
    # section describes the holder's entitlement.
    if "现金分红总额" in text and "固定不变" in text:
        final_section = text.split("固定不变", 1)[1]
        scoped = _cash_amount_in_text(final_section)
        if scoped is not None:
            return scoped
    return _cash_amount_in_text(text)


def _cash_amount_in_text(text: str) -> float | None:
    patterns = (
        r"每10股派(?:发)?(?:现金红利)?([0-9]+(?:\.[0-9]+)?)元(?:人民币)?现金\(含税",
        r"每10股(?:派发)?现金红利([0-9]+(?:\.[0-9]+)?)元\(含税",
        r"每10股派([0-9]+(?:\.[0-9]+)?)元\(含税",
        r"每10股(?:派发|分配|派送|派)(?:现金(?:红利|股利|分红)?)?(?:人民币)?"
        r"([0-9]+(?:\.[0-9]+)?)元(?:人民币)?(?:现金(?:红利)?)?\(含税",
        r"每10股派发现金分红人民币([0-9]+(?:\.[0-9]+)?)元\(含税",
        r"每10股送红股[0-9]+(?:\.[0-9]+)?股[，,]?派([0-9]+(?:\.[0-9]+)?)"
        r"元人民币现金\(含税",
    )
    values = []
    for pattern in patterns:
        values.extend(float(value) / 10.0 for value in re.findall(pattern, text))
    if not values:
        per_share = re.findall(
            r"(?:A股每股现金股利|A股每股现金红利|每股派发现金红利|每股派发现金股利)"
            r"(?:人民币)?([0-9]+(?:\.[0-9]+)?)元\(含税",
            text,
        )
        values = [float(value) for value in per_share]
    if not values:
        return None
    first = values[0]
    if any(abs(value - first) > max(1e-8, abs(first) * 1e-7) for value in values):
        return None
    return first


def _sh_a_share_table_dates(text: str) -> tuple[date, date] | None:
    """Read only the A-share row of an issuer's explicit SH payment table."""

    rows = []
    table = re.compile(
        r"股权登记日最后交易日除权\(息\)日(?:.{0,40}?)现金红利发放日"
        r"(?:Ａ|A)股(.{0,85})"
    )
    for match in table.finditer(text):
        segment = re.split(r"差异化|[一二三四五六]、|(?:H|Ｈ)股", match.group(1), maxsplit=1)[0]
        # PDF extraction may concatenate adjacent table cells, for example
        # ``2024/9/52024/9/5``.  Calendar-bounded day/month patterns keep
        # the first date from swallowing the next year as day 52.
        dates = re.findall(
            r"20\d{2}/(?:1[0-2]|0?[1-9])/(?:3[01]|[12]\d|0?[1-9])"
            r"(?=20\d{2}/|[^0-9]|$)",
            segment,
        )
        if len(dates) < 3:
            continue
        try:
            parsed = [date(*(int(part) for part in value.split("/"))) for value in dates]
        except ValueError:
            continue
        rows.append((parsed[-2], parsed[-1]))
    return rows[0] if rows and all(row == rows[0] for row in rows) else None


def _sh_single_class_table_dates(text: str) -> tuple[date, date] | None:
    """Parse an unlabelled SH table only when no B/H class can be mistaken for A."""

    if "B股" in text or "H股" in text or "Ｂ股" in text or "Ｈ股" in text:
        return None
    rows = []
    for match in re.finditer(r"股权登记日除权\(息\)日现金红利发放日(.{0,45})", text):
        dates = re.findall(
            r"20\d{2}/(?:1[0-2]|0?[1-9])/(?:3[01]|[12]\d|0?[1-9])"
            r"(?=20\d{2}/|[^0-9]|$)",
            match.group(1),
        )
        if len(dates) != 3:
            continue
        try:
            record, ex, payment = (
                date(*(int(part) for part in value.split("/"))) for value in dates
            )
        except ValueError:
            continue
        if record <= ex and payment >= record:
            rows.append((ex, payment))
    return rows[0] if rows and all(row == rows[0] for row in rows) else None


def parse_payment_notice_text(text: str) -> dict[str, Any] | None:
    """Extract independently checkable A-share payment facts from one PDF."""

    compact = _compact_text(text)
    symbol_match = re.search(r"(?:股票|证券)代码[：:]?(\d{6})", compact)
    ex_date = _date_match(
        (r"除权除息日为", r"除权除息日", r"除权日\(除息日\)", r"除息日为", r"除息日"),
        compact,
    )
    payment_date = _date_match(
        (
            r"(?:A股股东)?现金红利将于",
            r"A股现金红利发放日",
            r"现金红利发放日",
            r"红利发放日",
            r"派发现金红利日",
            r"(?:A股股东)?现金股利将于",
            r"(?:A股股东)?现金分红将于",
        ),
        compact,
    )
    table_dates = _sh_a_share_table_dates(compact)
    if table_dates is not None:
        # A generic payment-date phrase later in a dual-listed issuer PDF may
        # refer to H shares.  The labelled A-share row is authoritative here.
        ex_date, payment_date = table_dates
    elif (single_class_dates := _sh_single_class_table_dates(compact)) is not None:
        ex_date, payment_date = single_class_dates
    # Shenzhen A/B issuers can put both security classes on the same notice.
    # Only the explicitly labelled A-share entitlement and payment apply to
    # this A-share event; a later B-share date must never be used as fallback.
    sz_a_ex = re.search(r"A股股权登记日为" + _DATE + r"[,，；;]?除息日为" + _DATE, compact)
    sz_a_pay = re.search(r"代派的A股股东股息将于" + _DATE, compact)
    if sz_a_ex is not None and sz_a_pay is not None:
        try:
            ex_date = date(*(int(part) for part in sz_a_ex.groups()[-3:]))
            payment_date = date(*(int(part) for part in sz_a_pay.groups()))
        except ValueError:
            return None
    elif "A股股权登记日" in compact and "B股" in compact and table_dates is None:
        # An incomplete A-share section must not fall through to a B-share
        # ex-date or cash-payment sentence elsewhere in the same document.
        return None
    cash = _pretax_cash_per_share(compact)
    if symbol_match is None or ex_date is None or payment_date is None or cash is None:
        return None
    if payment_date < ex_date:
        # China Clearing credits A-share cash on the ex-date.  An earlier date
        # is a drafting error, typically last year's template left unedited
        # ("登记日 2016年7月5日 … 红利将于 2015年7月6日"); it proves nothing.
        return None
    return {
        "code": symbol_match.group(1),
        "ex_date": ex_date,
        "payment_date": payment_date,
        "cash_dividend": cash,
    }


def _strip_repeated_pdf_header(text: str, page_number: int | None = None) -> str:
    lines = text.splitlines()
    # Some exchange PDFs print the physical page number as the first line.
    # If a cash amount continues onto the next page, whitespace compaction
    # would otherwise turn ``每股派发现金红利`` + ``2`` + ``19.106`` into
    # a false 219.106. Remove only a number equal to the known PDF page.
    if page_number is not None and len(lines) >= 3 and lines[0].strip() == str(page_number):
        if not lines[1].strip():
            lines = lines[2:]
    if len(lines) >= 3 and "年度权益分派实施公告" in lines[0] and lines[1].strip().isdigit():
        return "\n".join(lines[2:])
    return "\n".join(lines)


def _pdf_text(payload: bytes) -> tuple[str, tuple[int, ...]]:
    if not payload.startswith(b"%PDF"):
        raise ValueError("CNINFO issuer notice is not a PDF")
    pages = []
    relevant = []
    for number, page in enumerate(PdfReader(io.BytesIO(payload)).pages, start=1):
        text = _strip_repeated_pdf_header(page.extract_text() or "", number)
        pages.append(text)
        compact = _compact_text(text)
        if any(marker in compact for marker in ("除权除息日", "现金红利", "红利发放日")):
            relevant.append(number)
    return "\n".join(pages), tuple(relevant)


def _source_time(item: dict) -> datetime | None:
    raw = item.get("announcementTime")
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return datetime.fromtimestamp(float(raw) / 1000, tz=timezone.utc)
    return None


def _source_date(item: dict) -> date | None:
    timestamp = _source_time(item)
    return timestamp.astimezone(SHANGHAI_TZ).date() if timestamp is not None else None


def _get(client: httpx.Client, url: str, *, config) -> httpx.Response:
    last: Exception | None = None
    for attempt in range(3):
        try:
            with source_request(config, "cninfo"):
                response = client.get(url)
                record_http_response(config, "cninfo", response)
                response.raise_for_status()
                return response
        except Exception as exc:  # noqa: BLE001 - bounded transport retry
            last = exc
            if attempt < 2:
                time.sleep(2.0 * (attempt + 1))
    raise last  # type: ignore[misc]


def _stock_directory(client: httpx.Client, *, config) -> tuple[dict[str, str], bytes]:
    response = _get(client, _DIRECTORY_URL, config=config)
    payload = response.content
    parsed = json.loads(payload)
    rows = parsed.get("stockList") or []
    mapping = {
        str(row.get("code") or "").zfill(6): str(row.get("orgId") or "")
        for row in rows
        if str(row.get("code") or "").isdigit() and str(row.get("orgId") or "")
    }
    return mapping, payload


def _query_symbol(
    client: httpx.Client,
    code: str,
    org_id: str,
    *,
    config,
    on_response,
    search_key: str = "权益分派",
) -> list[dict]:
    rows: list[dict] = []
    page = 1
    while True:
        body = {
            "pageNum": page,
            "pageSize": 30,
            "column": "szse",
            "tabName": "fulltext",
            "plate": "",
            "stock": f"{code},{org_id}",
            "searchkey": search_key,
            "secid": "",
            "category": "",
            "trade": "",
            "seDate": "2016-01-01~2024-12-31",
        }
        parsed = post_with_retry(
            client,
            _QUERY_URL,
            data=body,
            config=config,
            on_response=lambda response, data, attempt, _body=body: on_response(
                response, data, attempt, _body
            ),
        )
        page_rows = list(parsed.get("announcements") or [])
        rows.extend(page_rows)
        total_pages = int(parsed.get("totalpages") or parsed.get("totalPages") or 1)
        if page >= total_pages:
            break
        if page >= 20:
            raise ValueError(f"CNINFO payment notice query exceeded 20 pages for {code}")
        page += 1
    return rows


def _matches(old: dict, facts: dict) -> bool:
    code = str(old["symbol"]).split(".", 1)[0]
    return (
        facts["code"] == code
        and facts["ex_date"] == old["ex_date"]
        and abs(float(old["cash_dividend"]) - float(facts["cash_dividend"]))
        <= max(1e-8, abs(float(old["cash_dividend"])) * 1e-7)
    )


def _repurchase_price_adjustment(text: str) -> float | None:
    """Read a buyback-adjusted ex-price deduction, never the holder's cash.

    Some vendors put this diluted deduction in ``cash_dividend``.  Correcting
    it is safe only when the issuer explicitly distinguishes repurchased
    shares, states they do not participate, and prints the ex-price formula.
    """

    compact = _compact_text(text)
    if "回购" not in compact or "不参与" not in compact:
        return None
    values = [
        float(value)
        for value in re.findall(
            r"除权除息价格=股权登记日收盘价-([0-9]+(?:\.[0-9]+)?)(?:元/股)?",
            compact,
        )
    ]
    if not values or any(abs(value - values[0]) > 1e-8 for value in values):
        return None
    return values[0]


def _repurchase_cash_correction(old: dict, facts: dict, text: str) -> bool:
    """Allow an issuer-proven gross cash correction from an ex-price amount."""

    adjustment = _repurchase_price_adjustment(text)
    old_cash = float(old["cash_dividend"])
    return bool(
        adjustment is not None
        and facts["code"] == str(old["symbol"]).split(".", 1)[0]
        and facts["ex_date"] == old["ex_date"]
        and float(facts["cash_dividend"]) > adjustment
        and abs(old_cash - adjustment) <= max(1e-8, abs(adjustment) * 1e-7)
    )


def _issuer_precision_cash_correction(old: dict, facts: dict, text: str) -> bool:
    """Replace a rounded vendor value with an explicit issuer gross amount."""

    compact = _compact_text(text)
    gross = float(facts["cash_dividend"])
    old_cash = float(old["cash_dividend"])
    return bool(
        "回购" in compact
        and "不参与" in compact
        and re.search(r"二、(?:本次实施的)?(?:权益分派|利润分配)方案", compact)
        and facts["code"] == str(old["symbol"]).split(".", 1)[0]
        and facts["ex_date"] == old["ex_date"]
        and 1e-8 < abs(old_cash - gross) <= 5e-7
        and abs(old_cash - round(gross, 4)) <= 1e-8
    )


def repair_cninfo_payment_notices(
    config,
    run_id: str,
    existing: pl.DataFrame,
    *,
    stage: StageWrite,
    metrics: dict[str, int] | None = None,
) -> tuple[list[tuple[str, date]], list[dict]]:
    """Fetch, validate, archive and stage exact issuer-notice repairs."""

    if existing.is_empty() or not config.sources.get("cninfo", False):
        return [], []
    client = httpx.Client(timeout=30.0, headers={"User-Agent": "Mozilla/5.0"})
    done: list[tuple[str, date]] = []
    diagnostics: list[dict] = []
    try:
        # The directory only routes a request to CNINFO's issuer id.  The
        # returned per-symbol query repeats both secCode and orgId and is the
        # evidence that matters, so do not bind the shared directory bytes to
        # every symbol capture (content-addressed archive metadata correctly
        # rejects one payload claimed under many request scopes).
        directory, _directory_payload = _stock_directory(client, config=config)
        if metrics is not None:
            metrics["network_requests"] = metrics.get("network_requests", 0) + 1
            metrics["cninfo_network_responses"] = metrics.get("cninfo_network_responses", 0) + 1
        for symbol, group in existing.group_by("symbol", maintain_order=True):
            symbol = symbol[0] if isinstance(symbol, tuple) else symbol
            code = str(symbol).split(".", 1)[0]
            org_id = directory.get(code)
            if not org_id:
                diagnostics.append({"symbol": symbol, "reason": "cninfo_org_id_missing"})
                continue
            scope = f"cninfo-payment-notices:{symbol}:2016-2024"
            nonce = begin_capture(
                config,
                "corporate_actions",
                run_id,
                source="cninfo",
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
                    metrics["cninfo_network_responses"] = (
                        metrics.get("cninfo_network_responses", 0) + 1
                    )
                _records.append(
                    _archive.archive(
                        "corporate_actions",
                        response.content,
                        source="cninfo",
                        request_params=body,
                        run_id=run_id,
                        url=_QUERY_URL,
                        payload_format="json" if parsed is not None else "bytes",
                        http_metadata={"wire_exact": True, "attempt": attempt},
                        observation_id=(
                            f"{run_id}:{_scope}:query-{body['searchkey']}:"
                            f"page-{body['pageNum']}-attempt-{attempt}"
                        ),
                        request_scope=_scope,
                    )
                )

            announcements_by_id: dict[str, dict] = {}
            for search_key in _NOTICE_SEARCH_KEYS:
                for item in _query_symbol(
                    client,
                    code,
                    org_id,
                    config=config,
                    on_response=archive_query,
                    search_key=search_key,
                ):
                    announcement_id = str(item.get("announcementId") or "")
                    if announcement_id:
                        announcements_by_id.setdefault(announcement_id, item)
            announcements = list(announcements_by_id.values())
            candidates = [
                item
                for item in announcements
                if any(
                    marker in str(item.get("announcementTitle") or "") for marker in _NOTICE_TITLES
                )
                and not any(
                    marker in str(item.get("announcementTitle") or "")
                    for marker in _CORRECTION_MARKERS
                )
                and str(item.get("secCode") or "").zfill(6) == code
            ]
            repaired_rows = []
            holder_evidence = []
            pdf_cache: dict[
                str, tuple[bytes, str, str | None, int | None, dict | None, str | None]
            ] = {}
            archived_notices: set[tuple[str, str]] = set()
            for old in group.to_dicts():
                possible = [
                    item
                    for item in candidates
                    if (day := _source_date(item)) is not None
                    and old["ex_date"] - timedelta(days=75) <= day <= old["ex_date"]
                ]
                accepted = []
                for item in possible:
                    path = str(item.get("adjunctUrl") or "").lstrip("/")
                    if not path:
                        continue
                    url = _PDF_ROOT + path
                    if url not in pdf_cache:
                        response = _get(client, url, config=config)
                        if metrics is not None:
                            metrics["network_requests"] = metrics.get("network_requests", 0) + 1
                            metrics["cninfo_network_responses"] = (
                                metrics.get("cninfo_network_responses", 0) + 1
                            )
                        payload = response.content
                        digest = hashlib.sha256(payload).hexdigest()
                        try:
                            text, pages = _pdf_text(payload)
                            facts = parse_payment_notice_text(text)
                            parse_error = None
                        except Exception as exc:  # noqa: BLE001 - bytes archived below
                            text, pages, facts = None, None, None
                            parse_error = type(exc).__name__
                        pdf_cache[url] = (payload, digest, text, pages, facts, parse_error)
                    payload, digest, text, pages, facts, parse_error = pdf_cache[url]
                    announcement_id = str(item.get("announcementId") or "")
                    evidence_key = (url, announcement_id)
                    if evidence_key not in archived_notices:
                        records.append(
                            archive.archive(
                                "corporate_actions",
                                payload,
                                source="cninfo",
                                request_params={
                                    "announcement_id": announcement_id,
                                    "pdf_sha256": digest,
                                },
                                run_id=run_id,
                                url=url,
                                payload_format="bytes",
                                http_metadata={"wire_exact": True},
                                observation_id=f"{run_id}:{scope}:notice:{announcement_id}",
                                request_scope=scope,
                            )
                        )
                        archived_notices.add(evidence_key)
                    if parse_error is not None:
                        diagnostics.append(
                            {
                                "symbol": symbol,
                                "ex_date": str(old["ex_date"]),
                                "announcement_id": str(item.get("announcementId") or ""),
                                "reason": f"pdf_parse_failed:{parse_error}",
                            }
                        )
                        continue
                    holder = reviewed_holder_notice(
                        old, text, str(item.get("announcementId") or ""), digest
                    )
                    if holder is not None:
                        accepted.append((item, holder, digest, pages, "holder_class_cash_verified"))
                        continue
                    if facts is not None:
                        exact = _matches(old, facts)
                        correction = None
                        if not exact and _repurchase_cash_correction(old, facts, text):
                            correction = "buyback_ex_price_cash_corrected"
                        elif not exact and _issuer_precision_cash_correction(old, facts, text):
                            correction = "issuer_precision_cash_corrected"
                        elif not exact and reviewed_cash_correction(
                            old, facts, str(item.get("announcementId") or ""), digest
                        ):
                            correction = "issuer_reviewed_cash_corrected"
                        if exact or correction is not None:
                            accepted.append((item, facts, digest, pages, correction))
                if len(accepted) != 1:
                    diagnostics.append(
                        {
                            "symbol": symbol,
                            "ex_date": str(old["ex_date"]),
                            "reason": "no_unique_exact_issuer_notice",
                            "candidate_count": len(accepted),
                        }
                    )
                    continue
                item, facts, digest, pages, correction = accepted[0]
                announcement_id = str(item.get("announcementId") or "")
                if correction == "holder_class_cash_verified":
                    source_time = _source_time(item)
                    holder_evidence.append(
                        {
                            "symbol": symbol,
                            "ex_date": old["ex_date"].isoformat(),
                            "record_date": facts["record_date"].isoformat(),
                            "payment_date": facts["payment_date"].isoformat(),
                            "holder_classes": facts["holder_classes"],
                            "source_document_id": announcement_id,
                            "source_sha256": digest,
                            "source_published_at": source_time.isoformat()
                            if source_time is not None
                            else None,
                            "evidence_status": "verified_repaired",
                        }
                    )
                repaired_rows.append(
                    {
                        **old,
                        "cash_dividend": facts["cash_dividend"]
                        if correction
                        else old["cash_dividend"],
                        "payment_date": facts["payment_date"],
                        "payment_source": (
                            f"issuer_notice:{announcement_id}:pages"
                            f"{','.join(str(page) for page in pages)}:A:sha256:{digest}"
                            + (f":{correction}" if correction else "")
                        ),
                        "source": "cninfo",
                    }
                )
                done.append((symbol, old["ex_date"]))
            if repaired_rows:
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
                    pl.DataFrame(repaired_rows),
                    source="cninfo",
                    batch_id=f"cninfo-payment-notices-{symbol}",
                    raw_archive_evidence=evidence,
                    request_params={
                        "symbol": symbol,
                        "validation": "symbol+ex_date+pretax_cash+payment_date",
                    },
                )
                if holder_evidence:
                    from cnequity.adapters.cninfo.reviewed_holder_notices import (
                        save_holder_evidence,
                    )

                    save_holder_evidence(config.meta_root, run_id, symbol, holder_evidence)
    finally:
        client.close()
    return done, diagnostics
