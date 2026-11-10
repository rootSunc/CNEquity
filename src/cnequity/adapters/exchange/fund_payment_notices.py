"""Official Shanghai exchange fund notices for exact cash-payment repair."""

from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from urllib.parse import urljoin

import httpx
import polars as pl

from cnequity.adapters.cninfo.fund_payment_notices import parse_fund_payment_pdf
from cnequity.domain.http_policy import record_http_response
from cnequity.domain.rate_limit import source_request
from cnequity.storage.raw_archive import (
    RawPayloadArchive,
    StageWrite,
    begin_capture,
    verify_raw_archive,
)

_QUERY = "https://query.sse.com.cn/commonQuery.do"
_ROOT = "https://www.sse.com.cn"
_SQL_ID = "COMMON_PL_JJXX_JJGG_NEW_L"


def _get(client: httpx.Client, url: str, *, config, params: dict | None = None):
    with source_request(config, "exchange"):
        response = client.get(url, params=params, headers={"Referer": _ROOT + "/"})
        record_http_response(config, "exchange", response)
    response.raise_for_status()
    return response


def _query_symbol(client, config, code: str, year: int, archive, run_id: str, scope: str, metrics):
    seen = {}
    records = []
    start = f"{year - 1}-11-01" if year > 2016 else "2016-01-01"
    for keyword in ("分红", "收益分配", "利润分配"):
        page = 1
        while True:
            params = {
                "isPagination": "true",
                "pageHelp.pageSize": "25",
                "pageHelp.pageNo": str(page),
                "pageHelp.beginPage": str(page),
                "pageHelp.cacheSize": "1",
                "pageHelp.endPage": str(page),
                "type": "inParams",
                "sqlId": _SQL_ID,
                "TITLE": keyword,
                "SECURITY_CODE": code,
                "START_DATE": start,
                "END_DATE": f"{year}-12-31",
                "DATE_DESC": "1",
            }
            response = _get(client, _QUERY, config=config, params=params)
            metrics["network_requests"] = metrics.get("network_requests", 0) + 1
            metrics["sse_fund_network_responses"] = metrics.get("sse_fund_network_responses", 0) + 1
            records.append(
                archive.archive(
                    "corporate_actions",
                    response.content,
                    source="exchange",
                    request_params=params,
                    run_id=run_id,
                    url=str(response.request.url),
                    payload_format="json",
                    http_metadata={"wire_exact": True},
                    observation_id=f"{run_id}:{scope}:query:{keyword}:{page}",
                    request_scope=scope,
                )
            )
            parsed = json.loads(response.content)
            if parsed.get("actionErrors") or parsed.get("fieldErrors"):
                raise ValueError(
                    f"SSE fund query rejected {code}/{year}: {parsed.get('actionErrors')}"
                )
            for item in parsed.get("result") or []:
                if str(item.get("SECURITY_CODE") or "") == code:
                    seen.setdefault(str(item.get("URL") or ""), item)
            page_help = parsed.get("pageHelp") or {}
            pages = int(page_help.get("pageCount") or 1)
            if page >= pages:
                break
            if page >= 20:
                raise ValueError(f"SSE fund query exceeded 20 pages for {code}/{year}")
            page += 1
    return list(seen.values()), records


def repair_sse_fund_payment_notices(
    config, run_id: str, existing: pl.DataFrame, *, stage: StageWrite, metrics=None
):
    """Stage only unique code/date/amount matches from official PDFs."""
    if existing.is_empty() or not config.sources.get("exchange", False):
        return [], []
    pending = existing.filter(pl.col("symbol").str.ends_with(".SH"))
    if pending.is_empty():
        return [], []
    if metrics is None:
        metrics = {}
    client = httpx.Client(timeout=30.0, headers={"User-Agent": "Mozilla/5.0"})
    done, diagnostics = [], []
    try:
        for symbol, group in pending.group_by("symbol", maintain_order=True):
            symbol = symbol[0] if isinstance(symbol, tuple) else symbol
            code = symbol.split(".", 1)[0]
            for year, year_group in group.group_by(
                pl.col("ex_date").dt.year(), maintain_order=True
            ):
                year = int(year[0] if isinstance(year, tuple) else year)
                scope = f"sse-fund-payment:{symbol}:{year}"
                nonce = begin_capture(
                    config, "corporate_actions", run_id, source="exchange", request_scope=scope
                )
                archive = RawPayloadArchive(
                    config.meta_root,
                    enabled=True,
                    datasets=["corporate_actions"],
                    compression=config.raw_archive_compression,
                    max_payload_bytes=config.raw_archive_max_payload_bytes,
                    capture_owner=config,
                    capture_run_id=run_id,
                    capture_source="exchange",
                    capture_scope=scope,
                    capture_nonce=nonce,
                )
                notices, records = _query_symbol(
                    client, config, code, year, archive, run_id, scope, metrics
                )
                facts_by_url = {}
                rows = []
                corrections = [
                    item
                    for item in notices
                    if any(word in str(item.get("TITLE") or "") for word in ("更正", "补充"))
                ]
                for old in year_group.to_dicts():
                    accepted = []
                    for item in notices:
                        title = str(item.get("TITLE") or "")
                        if not any(
                            word in title for word in ("分红", "收益分配", "利润分配")
                        ) or any(word in title for word in ("更正", "补充")):
                            continue
                        try:
                            published = date.fromisoformat(str(item.get("SSEDATE") or "")[:10])
                        except ValueError:
                            continue
                        if not old["ex_date"] - timedelta(days=75) <= published <= old["ex_date"]:
                            continue
                        path = str(item.get("URL") or "")
                        if not path.startswith(
                            "/disclosure/fund/announcement/"
                        ) or not path.lower().endswith(".pdf"):
                            continue
                        if path not in facts_by_url:
                            url = urljoin(_ROOT, path)
                            response = _get(client, url, config=config)
                            metrics["network_requests"] = metrics.get("network_requests", 0) + 1
                            metrics["sse_fund_network_responses"] = (
                                metrics.get("sse_fund_network_responses", 0) + 1
                            )
                            payload = response.content
                            digest = hashlib.sha256(payload).hexdigest()
                            records.append(
                                archive.archive(
                                    "corporate_actions",
                                    payload,
                                    source="exchange",
                                    request_params={"code": code, "pdf_sha256": digest},
                                    run_id=run_id,
                                    url=url,
                                    payload_format="bytes",
                                    http_metadata={"wire_exact": True},
                                    observation_id=f"{run_id}:{scope}:pdf:{path}",
                                    request_scope=scope,
                                )
                            )
                            try:
                                facts_by_url[path] = (parse_fund_payment_pdf(payload), digest)
                            except Exception:
                                facts_by_url[path] = (None, digest)
                        facts, digest = facts_by_url[path]
                        if facts is not None and (
                            facts["code"] == code
                            and facts["ex_date"] == old["ex_date"]
                            and abs(float(facts["cash_dividend"]) - float(old["cash_dividend"]))
                            <= max(1e-8, abs(float(old["cash_dividend"])) * 1e-7)
                        ):
                            accepted.append((path, facts, digest))
                    if any(
                        old["ex_date"] - timedelta(days=75)
                        <= date.fromisoformat(str(item.get("SSEDATE") or "")[:10])
                        <= old["ex_date"]
                        for item in corrections
                        if str(item.get("SSEDATE") or "")[:10].count("-") == 2
                    ):
                        reason = "sse_fund_correction_chain_requires_review"
                    elif len(accepted) != 1:
                        reason = "sse_fund_notice_missing_or_ambiguous"
                    else:
                        reason = None
                    if reason:
                        diagnostics.append(
                            {
                                "symbol": symbol,
                                "ex_date": str(old["ex_date"]),
                                "reason": reason,
                                "candidate_count": len(accepted),
                            }
                        )
                        continue
                    path, facts, digest = accepted[0]
                    rows.append(
                        {
                            **old,
                            "payment_date": facts["payment_date"],
                            "payment_source": f"fund_notice:sse:{path}:sha256:{digest}",
                            "source": "exchange",
                        }
                    )
                    done.append((symbol, old["ex_date"]))
                if rows:
                    evidence = verify_raw_archive(
                        config,
                        "corporate_actions",
                        run_id,
                        source="exchange",
                        request_scope=scope,
                        records=records,
                    )
                    stage(
                        config,
                        run_id,
                        "corporate_actions",
                        pl.DataFrame(rows),
                        source="exchange",
                        batch_id=f"sse-fund-payment-{symbol}-{year}",
                        raw_archive_evidence=evidence,
                        request_params={
                            "symbol": symbol,
                            "year": year,
                            "validation": "official_sse_pdf+fund_code+ex_date+gross_cash+payment_date",
                        },
                    )
    finally:
        client.close()
    return done, diagnostics
