"""Official current ETF directories from the Shanghai and Shenzhen exchanges.

SSE publishes an explicit asset-market subclass. SZSE ETF membership, stock
fund category and a separately archived, code-matched official index
methodology are all required before a Shenzhen row becomes eligible.
"""

from __future__ import annotations

import io
import json
import logging
import re
import time
import warnings
from datetime import date
from hashlib import sha256
from typing import Any

import httpx
import pandas as pd
import polars as pl
from pypdf import PdfReader

from cnequity.config import Config
from cnequity.domain.http_policy import record_http_response
from cnequity.domain.rate_limit import source_request
from cnequity.domain.schemas import frame_from_rows

logger = logging.getLogger(__name__)

SSE_URL = "https://query.sse.com.cn/commonSoaQuery.do"
SZSE_URL = "https://www.szse.cn/api/report/ShowReport"
SSE_REFERER = "https://www.sse.com.cn/assortment/fund/etf/list/"
SZSE_REFERER = "https://fund.szse.cn/marketdata/etf/"
SZSE_FUNDS_REFERER = "https://fund.szse.cn/marketdata/fundslist/"
# A fund being wound up can briefly appear with blank fields; more than this
# many such rows means the directory format changed, not a single delisting.
SSE_MAX_BLANK_ROWS = 3
SSE_DOMESTIC_EQUITY_SUBCLASSES = frozenset({"01", "03", "09", "31"})
SSE_EXCLUDED_SUBCLASSES = frozenset({"02", "06", "08", "32", "33", "37"})
SZSE_NON_STOCK_CATEGORIES = frozenset({"债券基金", "货币市场基金", "其它基金", "混合基金", "ABS"})
# Each entry is an individually verified methodology, not an index-code range.
# The publisher's PDF is fetched and archived on every observation. A changed
# document must pass the exact code and A-share universe checks before use.
CNI_DOMESTIC_INDEX_METHODOLOGIES = {
    "399006": "https://www.cnindex.com.cn/docs/gz_399006_e.pdf",
    "399330": "https://www.cnindex.com.cn/docs/gz_399330_e.pdf",
    "399673": "https://www.cnindex.com.cn/docs/gz_399673_e.pdf",
}


def _get_official_response(
    config: Config,
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, str] | None = None,
    referer: str | None = None,
) -> httpx.Response:
    """Retry a brief transport interruption without bypassing source pacing."""
    for attempt in range(3):
        try:
            with source_request(config, "exchange"):
                response = client.get(
                    url,
                    params=params,
                    headers={"Referer": referer} if referer else None,
                )
                record_http_response(config, "exchange", response)
            response.raise_for_status()
            return response
        except httpx.TransportError:
            if attempt == 2:
                raise
            time.sleep(attempt + 1)
    raise AssertionError("unreachable")


def parse_cni_domestic_index_methodology(
    payload: bytes, index_code: str, url: str, *, index_evidence: dict | None = None
) -> dict:
    """Accept only a code-matched official methodology with an A-share universe."""
    if not payload.startswith(b"%PDF-") or index_code not in CNI_DOMESTIC_INDEX_METHODOLOGIES:
        raise ValueError("unrecognized official index methodology")
    if url != CNI_DOMESTIC_INDEX_METHODOLOGIES[index_code]:
        raise ValueError("index methodology URL does not match its code")
    reader = PdfReader(io.BytesIO(payload))
    content = " ".join(page.extract_text() or "" for page in reader.pages)
    normalized = " ".join(content.split())
    if not re.search(rf"Index Code:\s*{re.escape(index_code)}(?!\d)", normalized):
        raise ValueError(f"index methodology does not identify {index_code}")
    direct_scope = {
        "399006": r"All A shares listed on the ChiNext Market of Shenzhen Stock Exchange",
        "399330": r"All A shares listed on Shenzhen Stock Exchange",
    }
    parent = None
    if index_code in direct_scope:
        if not re.search(direct_scope[index_code], normalized, flags=re.IGNORECASE):
            raise ValueError("index methodology does not establish domestic A-share scope")
    elif index_code == "399673":
        if not re.search(
            r"Index Universe\s+Constituents of the ChiNext Index", normalized, flags=re.IGNORECASE
        ):
            raise ValueError("index methodology does not identify ChiNext Index constituents")
        parent = (index_evidence or {}).get("399006")
        if (
            not parent
            or parent.get("index_code") != "399006"
            or parent.get("asset_scope") != "domestic_a_shares"
            or parent.get("source_url") != CNI_DOMESTIC_INDEX_METHODOLOGIES["399006"]
            or not re.fullmatch(r"[0-9a-f]{64}", parent.get("payload_sha256", ""))
        ):
            raise ValueError("ChiNext 50 requires the archived 399006 A-share methodology")
    else:
        raise ValueError(f"index methodology has no reviewed scope rule: {index_code}")
    evidence = {
        "index_code": index_code,
        "asset_scope": "domestic_a_shares",
        "source_url": url,
        "payload_sha256": sha256(payload).hexdigest(),
    }
    if parent:
        evidence["parent_index_code"] = "399006"
        evidence["parent_payload_sha256"] = parent["payload_sha256"]
    return evidence


def _code(value: object) -> str:
    code = str(value or "").strip()
    if not re.fullmatch(r"[0-9]{6}", code):
        raise ValueError(f"exchange ETF directory contains invalid code {value!r}")
    return code


def _text(value: object) -> str | None:
    text = str(value or "").strip()
    return text if text and text != "-" else None


def _tracked_index(value: object) -> tuple[str | None, str | None]:
    """Split the SZSE workbook's optional ``code name`` display field."""
    raw = _text(value)
    if raw is None:
        return None, None
    match = re.fullmatch(r"([A-Za-z0-9.]+)(?:\s+(.+))?", raw)
    if match is None:
        raise ValueError(f"SZSE ETF directory contains invalid tracked index {raw!r}")
    return match.group(1), match.group(2)


def _workbook_records(payload: bytes) -> list[dict[str, Any]]:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Workbook contains no default style")
        frame = pd.read_excel(io.BytesIO(payload), dtype=str)
    return frame.where(pd.notna(frame), None).to_dict("records")


def parse_sse_profiles(payload: bytes, observed: date) -> list[dict[str, Any]]:
    response = json.loads(payload)
    if response.get("actionErrors") or response.get("fieldErrors"):
        raise ValueError("SSE rejected the ETF directory query")
    rows = response.get("result")
    page = response.get("pageHelp") or {}
    if not isinstance(rows, list) or not rows or int(page.get("total") or 0) != len(rows):
        raise ValueError("SSE ETF directory is empty or incomplete")
    out = []
    blank = []
    for item in rows:
        code = _code(item.get("fundCode"))
        subclass = str(item.get("subClass") or "").strip()
        name = _text(item.get("secNameFull"))
        if not name or str(item.get("fundType") or "") != "00":
            blank.append(code)
            continue
        listed = _text(item.get("listingDate"))
        list_date = (
            date.fromisoformat(f"{listed[:4]}-{listed[4:6]}-{listed[6:8]}") if listed else None
        )
        status = (
            "eligible"
            if subclass in SSE_DOMESTIC_EQUITY_SUBCLASSES
            else "excluded"
            if subclass in SSE_EXCLUDED_SUBCLASSES
            else "unverified"
        )
        index_code = _text(item.get("INDEX_CODE"))
        index_name = _text(item.get("INDEX_NAME"))
        if status == "eligible" and (not index_code or not index_name):
            status = "unverified"
        out.append(
            {
                "symbol": f"{code}.SH",
                "as_of_date": observed,
                "exchange": "SH",
                "name": name,
                "list_date": list_date,
                "tracking_index_code": index_code,
                "tracking_index_name": index_name,
                "fund_category": "ETF",
                "investment_category": None,
                "eligibility_status": status,
                "classification_basis": (
                    f"sse_official_subclass:{subclass or 'missing'}"
                    + (
                        ":index_missing"
                        if status == "unverified" and subclass in SSE_DOMESTIC_EQUITY_SUBCLASSES
                        else ""
                    )
                ),
                "source_url": SSE_REFERER,
                "source": "exchange",
            }
        )
    if len(blank) > SSE_MAX_BLANK_ROWS:
        raise ValueError(f"SSE ETF directory has {len(blank)} rows without name or ETF fund type")
    if blank:
        logger.warning("SSE ETF directory: skipped row(s) without name or ETF fund type: %s", blank)
    return out


def parse_szse_fund_classes(payload: bytes) -> dict[str, dict[str, Any]]:
    """Read the separate official all-funds directory before classifying ETFs."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Workbook contains no default style")
        frame = pd.read_excel(io.BytesIO(payload), dtype=str)
    required = {"基金代码", "基金类别", "投资类别", "上市日期"}
    if frame.empty or not required <= set(frame.columns):
        raise ValueError("SZSE all-funds workbook is empty or missing classification columns")
    out = {}
    for item in frame.where(pd.notna(frame), None).to_dict("records"):
        if _text(item.get("基金类别")) != "ETF":
            continue
        code = _code(item.get("基金代码"))
        if code in out:
            raise ValueError(f"SZSE all-funds workbook duplicates ETF {code}")
        out[code] = {
            "fund_category": "ETF",
            "investment_category": _text(item.get("投资类别")),
            "list_date": date.fromisoformat(item["上市日期"])
            if _text(item.get("上市日期"))
            else None,
        }
    if not out:
        raise ValueError("SZSE all-funds workbook has no ETFs")
    return out


def parse_szse_profiles(
    payload: bytes,
    observed: date,
    *,
    fund_classes: dict[str, dict[str, Any]] | None = None,
    index_evidence: dict[str, dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Workbook contains no default style")
        frame = pd.read_excel(io.BytesIO(payload), dtype=str)
    required = {"证券代码", "证券简称", "拟合指数"}
    if frame.empty or not required <= set(frame.columns):
        raise ValueError("SZSE ETF workbook is empty or missing identity/index columns")
    codes = {_code(value) for value in frame["证券代码"]}
    if fund_classes is not None and codes != fund_classes.keys():
        raise ValueError("SZSE ETF and all-funds directories disagree on ETF membership")
    out = []
    for item in frame.where(pd.notna(frame), None).to_dict("records"):
        code = _code(item.get("证券代码"))
        name = _text(item.get("证券简称"))
        if not name:
            raise ValueError(f"SZSE ETF {code} has no name")
        classification = fund_classes[code] if fund_classes is not None else {}
        category = classification.get("investment_category")
        status = "excluded" if category in SZSE_NON_STOCK_CATEGORIES else "unverified"
        index_code, index_name = _tracked_index(item.get("拟合指数"))
        evidence = (index_evidence or {}).get(index_code or "")
        if (
            status == "unverified"
            and category == "股票基金"
            and evidence is not None
            and evidence.get("index_code") == index_code
            and evidence.get("asset_scope") == "domestic_a_shares"
            and evidence.get("source_url") == CNI_DOMESTIC_INDEX_METHODOLOGIES.get(index_code)
            and re.fullmatch(r"[0-9a-f]{64}", evidence.get("payload_sha256", ""))
            and (
                index_code != "399673"
                or (
                    evidence.get("parent_index_code") == "399006"
                    and evidence.get("parent_payload_sha256")
                    == (index_evidence or {}).get("399006", {}).get("payload_sha256")
                )
            )
        ):
            status = "eligible"
        out.append(
            {
                "symbol": f"{code}.SZ",
                "as_of_date": observed,
                "exchange": "SZ",
                "name": name,
                "list_date": classification.get("list_date"),
                "tracking_index_code": index_code,
                "tracking_index_name": index_name,
                "fund_category": classification.get("fund_category", "ETF"),
                "investment_category": category,
                "eligibility_status": status,
                "classification_basis": (
                    f"szse_official_investment_category:{category}:non_stock"
                    if status == "excluded"
                    else (
                        f"szse_official_index_methodology:{index_code}:"
                        f"sha256={evidence['payload_sha256']}:url={evidence['source_url']}"
                        + (
                            f":parent=399006:parent_sha256={evidence['parent_payload_sha256']}"
                            if index_code == "399673"
                            else ""
                        )
                    )
                    if status == "eligible"
                    else "szse_official_etf_listing:index_asset_class_unverified"
                ),
                "source_url": SZSE_FUNDS_REFERER if fund_classes is not None else SZSE_REFERER,
                "source": "exchange",
            }
        )
    return out


def fetch_exchange_etf_profiles(
    config: Config, observed: date, *, client: httpx.Client | None = None
) -> tuple[pl.DataFrame, list[tuple[str, bytes, dict[str, str], str]]]:
    """Fetch the three directories and matched methodologies for one atomic archive."""
    own_client = client is None
    client = client or httpx.Client(timeout=30.0, headers={"User-Agent": "Mozilla/5.0"})
    sse_params = {
        "isPagination": "true",
        "pageHelp.pageSize": "2000",
        "pageHelp.pageNo": "1",
        "sqlId": "FUND_LIST",
        "fundType": "00",
        "subClass": "",
    }
    szse_params = {"SHOWTYPE": "xlsx", "CATALOGID": "fund_etf", "TABKEY": "tab1"}
    try:
        wire = []
        for url, params, referer, kind in (
            (SSE_URL, sse_params, SSE_REFERER, "json"),
            (SZSE_URL, szse_params, SZSE_REFERER, "xlsx"),
            (
                SZSE_URL,
                {"SHOWTYPE": "xlsx", "CATALOGID": "1000_lf", "TABKEY": "tab1"},
                SZSE_FUNDS_REFERER,
                "xlsx",
            ),
        ):
            response = _get_official_response(config, client, url, params=params, referer=referer)
            if not response.content:
                raise ValueError(f"empty exchange ETF directory: {url}")
            wire.append((str(response.request.url), response.content, params, kind))
        classes = parse_szse_fund_classes(wire[2][1])
        tracked_codes = {
            _tracked_index(item.get("拟合指数"))[0] for item in _workbook_records(wire[1][1])
        }
        if "399673" in tracked_codes:
            tracked_codes.add("399006")  # required parent scope, even without a 399006 ETF
        index_evidence = {}
        for index_code, url in CNI_DOMESTIC_INDEX_METHODOLOGIES.items():
            if index_code not in tracked_codes:
                continue
            response = _get_official_response(config, client, url)
            evidence = parse_cni_domestic_index_methodology(
                response.content, index_code, url, index_evidence=index_evidence
            )
            index_evidence[index_code] = evidence
            wire.append(
                (str(response.request.url), response.content, {"index_code": index_code}, "pdf")
            )
        rows = parse_sse_profiles(wire[0][1], observed) + parse_szse_profiles(
            wire[1][1], observed, fund_classes=classes, index_evidence=index_evidence
        )
        frame = frame_from_rows(rows, "etf_profiles")
        if frame["symbol"].n_unique() != frame.height:
            raise ValueError("exchange ETF directories contain duplicate symbols")
        return frame, wire
    finally:
        if own_client:
            client.close()
