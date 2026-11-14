"""Append source-original annual figures with honest current observation times."""

import json
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import polars as pl

from cnequity.adapters.cninfo.announcements import (
    _announcement_batch,
    _pagination_total_pages,
    _pagination_total_records,
    post_with_retry,
)
from cnequity.adapters.cninfo.financial_originals import (
    original_report_notice,
    parse_annual_pdf,
    parse_report_pdf,
    report_query,
)
from cnequity.adapters.cninfo.payment_notices import _get, _stock_directory
from cnequity.domain.market_time import SHANGHAI_TZ
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.domain.symbols import is_all_a_symbol
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.lake_write import planned_on
from cnequity.storage.raw_archive import RawPayloadArchive
from cnequity.storage.read_context import ReadContext
from cnequity.storage.repairs.publish import publish_staged
from cnequity.storage.revisions import RevisionStore

QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
PDF_ROOT = "https://static.cninfo.com.cn/"
DATASET = "financial_statement_items"
_FACT_FIELDS = [
    "symbol",
    "report_period",
    "statement_type",
    "item_code",
    "announce_date",
    "source",
    "item_value",
    "source_published_at",
    "available_at",
]


def _novel_originals(rows, root):
    if rows.is_empty() or not dataset_has_parquet(root, committed=False):
        return rows
    existing = scan_parquet_root(root, committed=False)
    if not set(_FACT_FIELDS + ["observed_at"]) <= set(existing.collect_schema().names()):
        return rows
    known = (
        existing.filter(
            (pl.col("source") == "cninfo_original")
            & pl.col("symbol").is_in(rows["symbol"].unique().to_list())
            & pl.col("report_period").is_in(rows["report_period"].unique().to_list())
            & pl.col("observed_at").is_not_null()
        )
        .select(_FACT_FIELDS)
        .unique()
        .collect()
    )
    # A parser upgrade may add new cells, but re-reading an identical fact
    # must not advance its original observation time or replace its receipt.
    return rows.join(known, on=_FACT_FIELDS, how="anti")


def _query_page_count(response, page):
    total = _pagination_total_records(response, column="szse", page=page)
    if total is not None:
        # The publisher's totalpages counts full pages, excluding a final
        # partial page. Record totals establish the actual number of reads.
        return max(1, (total + 29) // 30), total
    return max(1, _pagination_total_pages(response, column="szse", page=page) or 1), None


def _replay(archive, cached, symbol, year, quarter=4):
    records = []
    if (
        not isinstance(cached, dict)
        or not isinstance(cached.get("queries"), list)
        or not cached["queries"]
        or not all(isinstance(p, dict) for p in cached["queries"])
    ):
        raise ValueError("periodic report query archive is missing or malformed")
    pages = cached["queries"]
    search, window = report_query(year, quarter)
    total_records = None
    for number, receipt in enumerate(pages, start=1):
        params = receipt.get("request_params", {})
        if (
            receipt.get("url") != QUERY_URL
            or receipt.get("source") != "cninfo"
            or receipt.get("dataset") != DATASET
            or params.get("pageNum") != number
            or params.get("searchkey") not in (search, "")
            or params.get("category", "") != ""
            or params.get("seDate") != window
            or not str(params.get("stock", "")).startswith(symbol.split(".")[0] + ",")
        ):
            raise ValueError("periodic report query archive identity differs")
        response = json.loads(archive.read(receipt))
        total_pages, declared = _query_page_count(response, number)
        if params.get("searchkey") == "" and declared is None:
            raise ValueError("unfiltered issuer query lacks a record count")
        if total_pages != len(pages):
            raise ValueError("periodic report query pagination is incomplete")
        if number == 1:
            total_records = declared
        elif declared != total_records:
            raise ValueError("issuer query record count changed between pages")
        records.extend(_announcement_batch(response, column="szse", page=number))
    if any(row.get("secCode") != symbol.split(".")[0] for row in records):
        raise ValueError("issuer query includes a different security")
    identities = {
        (
            row.get("secCode"),
            row.get("announcementId") or row.get("adjunctUrl"),
            row.get("announcementTime"),
        )
        for row in records
    }
    if total_records is not None and (
        len(records) != total_records or len(identities) != total_records
    ):
        raise ValueError("issuer query is incomplete or has duplicated pages")
    notice = original_report_notice(records, symbol.split(".")[0], year, quarter=quarter)
    report = cached.get("report")
    if not isinstance(report, dict):
        raise ValueError("original report PDF is not archived")
    if (
        report.get("url") != PDF_ROOT + notice["adjunctUrl"]
        or report.get("source") != "cninfo"
        or report.get("dataset") != DATASET
    ):
        raise ValueError("periodic report PDF archive identity differs")
    return notice, archive.read(report), datetime.fromisoformat(report["captured_at"])


def repair_financial_originals(
    config,
    *,
    symbols=None,
    all_stocks=False,
    after=None,
    start_year=2016,
    end_year=2024,
    max_reports=16,
    quarter=4,
    reparse=False,
    refresh_sources=False,
    apply=False,
):
    if bool(symbols) == bool(all_stocks):
        raise ValueError("provide either stock symbols or all_stocks")
    if not 2001 <= start_year <= end_year <= datetime.now().year or max_reports < 1:
        raise ValueError("provide a valid annual-report range/batch limit")
    report_query(start_year, quarter)
    context = ReadContext.capture(
        config, (DATASET, "instruments") if all_stocks else (DATASET,), {}
    )
    if all_stocks:
        catalog = context.roots["instruments"]
        if not dataset_has_parquet(catalog, committed=False):
            raise ValueError("all_stocks requires an instrument catalogue")
        symbols = (
            scan_parquet_root(catalog, committed=False)
            .filter(pl.col("asset_type").is_in(["stock", "cdr"]))
            .select("symbol")
            .unique()
            .collect()["symbol"]
            .to_list()
        )
    for symbol in symbols:
        parts = symbol.split(".")
        if len(parts) != 2 or not is_all_a_symbol(*parts):
            raise ValueError(f"not a supported stock symbol: {symbol}")
    if refresh_sources and not config.sources.get("cninfo", False):
        raise ValueError("cninfo source disabled")
    root = context.roots[DATASET]
    done = set()
    if not reparse and dataset_has_parquet(root, committed=False):
        done = set(
            scan_parquet_root(root, committed=False)
            .filter((pl.col("source") == "cninfo_original") & pl.col("symbol").is_in(symbols))
            .select("symbol", "report_period")
            .unique()
            .collect()
            .iter_rows()
        )
    wanted, selected = _report_batch(
        symbols, start_year, end_year, done, after, max_reports, quarter
    )
    run_id = "financial-originals-" + uuid4().hex
    archive = RawPayloadArchive(config.meta_root, enabled=True, datasets=[DATASET])
    cache = config.meta_root / "source_cache" / "financial_originals"
    frames, failures = [], []
    with httpx.Client(timeout=60, headers={"User-Agent": "Mozilla/5.0"}) as client:
        directory = (
            _stock_directory(client, config=config)[0] if refresh_sources and selected else {}
        )
        for symbol, year in selected:
            period = str(year) if quarter == 4 else f"{year}Q{quarter}"
            path = cache / f"{symbol}-{period}.json"
            search, window = report_query(year, quarter)
            try:
                if refresh_sources:
                    code = symbol.split(".")[0]
                    if code not in directory:
                        raise ValueError("CNINFO issuer routing id is unavailable")
                    queries, records = [], []
                    for page in range(1, 101):
                        body = {
                            "pageNum": page,
                            "pageSize": 30,
                            "column": "szse",
                            "tabName": "fulltext",
                            "plate": "",
                            "stock": f"{code},{directory[code]}",
                            # Keyword matching can silently omit the original
                            # title. Read the issuer's complete date window.
                            "searchkey": "",
                            "secid": "",
                            "category": "",
                            "trade": "",
                            "seDate": window,
                        }

                        def capture(response, _data, _attempt, body=body, queries=queries):
                            queries.append(
                                archive.archive(
                                    DATASET,
                                    response.content,
                                    source="cninfo",
                                    run_id=run_id,
                                    url=QUERY_URL,
                                    request_params=body,
                                    payload_format="json",
                                ).to_dict()
                            )

                        response = post_with_retry(
                            client, QUERY_URL, data=body, config=config, on_response=capture
                        )
                        # Keep only the successful response for this page.
                        queries = [r for r in queries if r["request_params"]["pageNum"] < page] + [
                            queries[-1]
                        ]
                        records.extend(_announcement_batch(response, column="szse", page=page))
                        total_pages, total = _query_page_count(response, page)
                        if total is None:
                            raise ValueError("unfiltered issuer query lacks a record count")
                        if total_pages > 100:
                            raise ValueError(
                                "issuer query exceeds 100 pages; original-disclosure coverage is unresolved"
                            )
                        if page >= total_pages:
                            break
                    else:
                        raise ValueError("periodic report query exceeded 100 pages")
                    # Retain complete queries even when no eligible original
                    # report exists, or the PDF download fails. Offline replay
                    # must report that specific unresolved evidence boundary.
                    write_json_atomic(path, {"queries": queries})
                    notice = original_report_notice(records, code, year, quarter=quarter)
                    url = PDF_ROOT + notice["adjunctUrl"]
                    pdf = _get(client, url, config=config).content
                    receipt = archive.archive(
                        DATASET,
                        pdf,
                        source="cninfo",
                        run_id=run_id,
                        url=url,
                        request_params={},
                        payload_format="pdf",
                    ).to_dict()
                    write_json_atomic(path, {"queries": queries, "report": receipt})
                notice, pdf, captured_at = _replay(
                    archive, json.loads(path.read_text()), symbol, year, quarter
                )
                parsed = (
                    parse_annual_pdf(pdf, symbol, year)
                    if quarter == 4
                    else parse_report_pdf(pdf, symbol, year, quarter=quarter)
                )
                published = datetime.fromtimestamp(
                    notice["announcementTime"] / 1000, tz=timezone.utc
                )
                day = published.astimezone(SHANGHAI_TZ).date()
                rows = pl.DataFrame([{**r, "announce_date": day} for r in parsed])
                rows = with_provenance(
                    rows, source="cninfo_original", data_version=data_version_for(DATASET)
                )
                rows = rows.with_columns(
                    pl.lit(captured_at).alias("fetched_at"),
                    pl.lit(captured_at).alias("observed_at"),
                    pl.lit(published).alias("source_published_at"),
                    pl.lit(published).alias("available_at"),
                )
                frames.append(rows)
            except (OSError, ValueError, RuntimeError, httpx.HTTPError) as exc:
                failures.append({"symbol": symbol, "year": year, "error": str(exc)})
    parsed_rows = pl.concat(frames) if frames else None
    incoming = _novel_originals(parsed_rows, root) if parsed_rows is not None else None
    report = {
        "applied": False,
        "rows": incoming.height if incoming is not None else 0,
        "rows_parsed": sum(f.height for f in frames),
        "reports_parsed": len(frames),
        "reports_selected": len(selected),
        "reports_pending_batch_limit": len(wanted) - len(selected),
        "next_cursor": (
            f"{selected[-1][1]}:{selected[-1][0]}"
            if selected and len(wanted) > len(selected)
            else None
        ),
        "quarter": f"Q{quarter}",
        "failures": failures,
        "scope": "supported current-period consolidated items only; missing fields, ambiguous layouts and unselected reports remain unresolved",
    }
    if not apply or incoming is None or incoming.is_empty():
        return report
    with planned_on(config):
        store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
        store.ensure_current(DATASET)
        context = ReadContext.capture(config, (DATASET,), {})
        incoming = _novel_originals(parsed_rows, context.roots[DATASET])
        report["rows"] = incoming.height
        if incoming.is_empty():
            report["note"] = "original facts already present; observation times preserved"
            return report
        published = publish_staged(
            config,
            store,
            DATASET,
            incoming,
            run_id=run_id,
            base_root=context.roots[DATASET],
            metadata={"reason": "issuer_original_reports", **report},
            batch="original-0",
            partition_col="report_period",
        )
        if published is None:
            report["note"] = "original report figures already present"
            return report
        report.update(applied=True, revision=published["revision"])
    return report


def _report_batch(symbols, start_year, end_year, done, after, max_reports, quarter=4):
    cursor = None
    if after:
        try:
            year, symbol = after.split(":")
            cursor = (int(year), symbol)
        except (ValueError, TypeError) as exc:
            raise ValueError("after must be YEAR:SYMBOL") from exc
        if not start_year <= cursor[0] <= end_year or cursor[1] not in symbols:
            raise ValueError("after cursor is outside the selected annual-report universe")
    wanted = [
        (s, y)
        for y in range(start_year, end_year + 1)
        for s in sorted(set(symbols))
        if (s, f"{y}Q{quarter}") not in done and (cursor is None or (y, s) > cursor)
    ]
    return wanted, wanted[:max_reports]
