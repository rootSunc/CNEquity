"""Official ETF catalogues must not turn a code prefix into eligibility."""

import io
import json
from contextlib import nullcontext
from datetime import date

import httpx
import pandas as pd
import pytest

from cnequity.adapters.exchange.etf_profiles import (
    CNI_DOMESTIC_INDEX_METHODOLOGIES,
    _get_official_response,
    parse_cni_domestic_index_methodology,
    parse_sse_profiles,
    parse_szse_fund_classes,
    parse_szse_profiles,
)
from cnequity.config import Config
from cnequity.domain.schemas import frame_from_rows
from cnequity.steps.etf_profiles import step_etf_profiles


def test_official_directory_retries_transport_failure_under_source_guard(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    attempts = []
    guarded = []
    recorded = []

    def handle(request):
        attempts.append(request)
        if len(attempts) == 1:
            raise httpx.ConnectError("connection reset", request=request)
        return httpx.Response(200, content=b"directory", request=request)

    def guard(_cfg, source):
        guarded.append(source)
        return nullcontext()

    monkeypatch.setattr("cnequity.adapters.exchange.etf_profiles.source_request", guard)
    monkeypatch.setattr(
        "cnequity.adapters.exchange.etf_profiles.record_http_response",
        lambda _cfg, source, response: recorded.append((source, response.status_code)),
    )
    monkeypatch.setattr("cnequity.adapters.exchange.etf_profiles.time.sleep", lambda _: None)
    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        response = _get_official_response(cfg, client, "https://exchange.test/directory")
    assert response.content == b"directory"
    assert len(attempts) == 2
    assert guarded == ["exchange", "exchange"]
    assert recorded == [("exchange", 200)]


def test_official_directory_does_not_retry_http_rejection(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    attempts = []

    def handle(request):
        attempts.append(request)
        return httpx.Response(403, request=request)

    monkeypatch.setattr(
        "cnequity.adapters.exchange.etf_profiles.source_request", lambda *_: nullcontext()
    )
    monkeypatch.setattr(
        "cnequity.adapters.exchange.etf_profiles.record_http_response", lambda *_: None
    )
    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            _get_official_response(cfg, client, "https://exchange.test/directory")
    assert len(attempts) == 1


def _sse_payload(rows, total=None):
    return json.dumps(
        {
            "actionErrors": [],
            "fieldErrors": {},
            "pageHelp": {"total": len(rows) if total is None else total},
            "result": rows,
        }
    ).encode()


def _szse_workbook(rows):
    stream = io.BytesIO()
    pd.DataFrame(rows).to_excel(stream, index=False)
    return stream.getvalue()


def test_sse_subclass_controls_eligibility_not_etf_name():
    today = date(2026, 9, 27)
    rows = parse_sse_profiles(
        _sse_payload(
            [
                {
                    "fundCode": "510050",
                    "fundType": "00",
                    "subClass": "01",
                    "secNameFull": "上证50ETF",
                    "listingDate": "20050223",
                    "INDEX_CODE": "000016",
                    "INDEX_NAME": "上证50指数",
                },
                {
                    "fundCode": "513000",
                    "fundType": "00",
                    "subClass": "33",
                    "secNameFull": "境外股票ETF",
                    "INDEX_CODE": "HSTECH",
                },
                {
                    "fundCode": "501000",
                    "fundType": "00",
                    "subClass": "05",
                    "secNameFull": "名称含ETF但类别未证实",
                },
            ]
        ),
        today,
    )
    assert [item["eligibility_status"] for item in rows] == ["eligible", "excluded", "unverified"]
    assert rows[0]["tracking_index_code"] == "000016"
    assert rows[0]["list_date"] == date(2005, 2, 23)
    assert all(item["as_of_date"] == today for item in rows)


def test_szse_listing_is_not_domestic_equity_proof():
    rows = parse_szse_profiles(
        _szse_workbook(
            [
                {"证券代码": "159915", "证券简称": "创业板ETF", "拟合指数": "399006 创业板指"},
                {"证券代码": "159920", "证券简称": "恒生ETF", "拟合指数": "HSI"},
            ]
        ),
        date(2026, 9, 27),
    )
    assert {item["symbol"]: item["tracking_index_code"] for item in rows} == {
        "159915.SZ": "399006",
        "159920.SZ": "HSI",
    }
    assert rows[0]["tracking_index_name"] == "创业板指"
    assert {item["eligibility_status"] for item in rows} == {"unverified"}


def test_incomplete_sse_directory_is_rejected():
    with pytest.raises(ValueError, match="incomplete"):
        parse_sse_profiles(_sse_payload([{"fundCode": "510050"}], total=100), date(2026, 9, 27))


def test_sse_blank_row_of_a_fund_being_wound_up_is_skipped(caplog):
    good = {"fundCode": "510050", "fundType": "00", "subClass": "01", "secNameFull": "上证50ETF"}
    rows = parse_sse_profiles(
        _sse_payload([good, {"fundCode": "512390", "fundType": "00", "secNameFull": ""}]),
        date(2026, 10, 27),
    )
    assert [item["symbol"] for item in rows] == ["510050.SH"]
    assert "512390" in caplog.text


def test_sse_directory_with_many_blank_rows_is_rejected():
    blank = [{"fundCode": f"51{i:04d}", "fundType": "00"} for i in range(4)]
    with pytest.raises(ValueError, match="4 rows without name"):
        parse_sse_profiles(_sse_payload(blank), date(2026, 10, 27))


def test_szse_classification_excludes_nonstock_but_does_not_guess_geography():
    directory = _szse_workbook(
        [
            {"证券代码": "159915", "证券简称": "创业板ETF", "拟合指数": "399006"},
            {"证券代码": "159920", "证券简称": "恒生ETF", "拟合指数": "HSI"},
            {"证券代码": "159400", "证券简称": "债券ETF", "拟合指数": "BOND"},
        ]
    )
    classes = parse_szse_fund_classes(
        _szse_workbook(
            [
                {
                    "基金代码": "159915",
                    "基金类别": "ETF",
                    "投资类别": "股票基金",
                    "上市日期": "2011-12-09",
                },
                {
                    "基金代码": "159920",
                    "基金类别": "ETF",
                    "投资类别": "股票基金",
                    "上市日期": "2012-10-22",
                },
                {
                    "基金代码": "159400",
                    "基金类别": "ETF",
                    "投资类别": "债券基金",
                    "上市日期": "2025-01-01",
                },
            ]
        )
    )
    rows = parse_szse_profiles(directory, date(2026, 9, 27), fund_classes=classes)
    by_code = {row["symbol"]: row for row in rows}
    assert by_code["159915.SZ"]["eligibility_status"] == "unverified"
    assert by_code["159920.SZ"]["eligibility_status"] == "unverified"
    assert by_code["159400.SZ"]["eligibility_status"] == "excluded"
    assert by_code["159915.SZ"]["list_date"] == date(2011, 12, 9)
    with pytest.raises(ValueError, match="disagree"):
        parse_szse_profiles(
            directory, date(2026, 9, 27), fund_classes={"159915": classes["159915"]}
        )


def test_index_methodology_is_code_matched_and_only_promotes_domestic_stock(tmp_path, monkeypatch):
    class Page:
        def extract_text(self):
            return (
                "Index Code: 399006 3. Index Universe All A shares listed on the "
                "ChiNext Market of Shenzhen Stock Exchange"
            )

    class Reader:
        def __init__(self, _stream):
            self.pages = [Page()]

    monkeypatch.setattr("cnequity.adapters.exchange.etf_profiles.PdfReader", Reader)
    url = CNI_DOMESTIC_INDEX_METHODOLOGIES["399006"]
    evidence = parse_cni_domestic_index_methodology(b"%PDF-example", "399006", url)
    assert evidence["asset_scope"] == "domestic_a_shares"
    with pytest.raises(ValueError, match="URL"):
        parse_cni_domestic_index_methodology(b"%PDF-example", "399006", "https://wrong.test")
    with pytest.raises(ValueError, match="unrecognized"):
        parse_cni_domestic_index_methodology(b"%PDF-example", "HSI", url)

    directory = _szse_workbook(
        [
            {"证券代码": "159915", "证券简称": "A", "拟合指数": "399006 创业板指"},
            {"证券代码": "159920", "证券简称": "B", "拟合指数": "HSI"},
            {"证券代码": "159400", "证券简称": "C", "拟合指数": "399006"},
            {"证券代码": "159401", "证券简称": "D", "拟合指数": "399300"},
        ]
    )
    classes = {
        "159915": {"fund_category": "ETF", "investment_category": "股票基金"},
        "159920": {"fund_category": "ETF", "investment_category": "股票基金"},
        "159400": {"fund_category": "ETF", "investment_category": "债券基金"},
        "159401": {"fund_category": "ETF", "investment_category": "股票基金"},
    }
    rows = parse_szse_profiles(
        directory,
        date(2026, 9, 27),
        fund_classes=classes,
        index_evidence={"399006": evidence},
    )
    statuses = {row["symbol"]: row["eligibility_status"] for row in rows}
    assert statuses == {
        "159915.SZ": "eligible",
        "159920.SZ": "unverified",
        "159400.SZ": "excluded",
        "159401.SZ": "unverified",
    }
    eligible = rows[0]
    assert eligible["classification_basis"].endswith(
        f"sha256={evidence['payload_sha256']}:url={url}"
    )


def test_new_index_evidence_requires_direct_a_shares_or_archived_parent(monkeypatch):
    class Page:
        def __init__(self, content):
            self.content = content

        def extract_text(self):
            return self.content

    class Reader:
        def __init__(self, stream):
            code = stream.getvalue().decode().split("-")[-1]
            scope = {
                "399006": "All A shares listed on the ChiNext Market of Shenzhen Stock Exchange",
                "399330": "All A shares listed on Shenzhen Stock Exchange",
                "399673": "Index Universe Constituents of the ChiNext Index",
            }[code]
            self.pages = [Page(f"Index Code: {code} 3. Index Universe {scope}")]

    monkeypatch.setattr("cnequity.adapters.exchange.etf_profiles.PdfReader", Reader)

    def parse(code, **kwargs):
        return parse_cni_domestic_index_methodology(
            f"%PDF-{code}".encode(), code, CNI_DOMESTIC_INDEX_METHODOLOGIES[code], **kwargs
        )

    parent = parse("399006")
    direct = parse("399330")
    with pytest.raises(ValueError, match="requires the archived 399006"):
        parse("399673")
    with pytest.raises(ValueError, match="requires the archived 399006"):
        parse(
            "399673",
            index_evidence={"399006": {**parent, "source_url": "https://example.com/other.pdf"}},
        )
    child = parse("399673", index_evidence={"399006": parent})
    assert child["parent_payload_sha256"] == parent["payload_sha256"]

    directory = _szse_workbook(
        [
            {"证券代码": "159901", "证券简称": "深100", "拟合指数": "399330 深证100"},
            {"证券代码": "159902", "证券简称": "创50", "拟合指数": "399673 创业板50"},
            {"证券代码": "159903", "证券简称": "跨境", "拟合指数": "HSI"},
            {"证券代码": "159904", "证券简称": "债基", "拟合指数": "399330 深证100"},
        ]
    )
    classes = {
        code: {"fund_category": "ETF", "investment_category": category}
        for code, category in (
            ("159901", "股票基金"),
            ("159902", "股票基金"),
            ("159903", "股票基金"),
            ("159904", "债券基金"),
        )
    }
    evidence = {"399006": parent, "399330": direct, "399673": child}
    rows = parse_szse_profiles(
        directory, date(2026, 9, 27), fund_classes=classes, index_evidence=evidence
    )
    assert [row["eligibility_status"] for row in rows] == [
        "eligible",
        "eligible",
        "unverified",
        "excluded",
    ]
    assert "parent_sha256=" in rows[1]["classification_basis"]
    rows_without_parent = parse_szse_profiles(
        directory,
        date(2026, 9, 27),
        fund_classes=classes,
        index_evidence={"399330": direct, "399673": child},
    )
    assert rows_without_parent[1]["eligibility_status"] == "unverified"


def test_step_archives_all_three_wire_sources_before_staging(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    observed = date(2026, 9, 27)
    sse = _sse_payload(
        [{"fundCode": "510050", "fundType": "00", "subClass": "01", "secNameFull": "上证50ETF"}]
    )
    szse = _szse_workbook([{"证券代码": "159915", "证券简称": "创业板ETF", "拟合指数": "399006"}])
    funds = _szse_workbook(
        [
            {
                "基金代码": "159915",
                "基金类别": "ETF",
                "投资类别": "股票基金",
                "上市日期": "2011-12-09",
            }
        ]
    )

    def fake_fetch(_config, day):
        assert day == observed
        rows = parse_sse_profiles(sse, day) + parse_szse_profiles(
            szse, day, fund_classes=parse_szse_fund_classes(funds)
        )
        return frame_from_rows(rows, "etf_profiles"), [
            ("https://query.sse.com.cn/commonSoaQuery.do", sse, {"sqlId": "FUND_LIST"}, "json"),
            ("https://www.szse.cn/api/report/ShowReport", szse, {"CATALOGID": "fund_etf"}, "xlsx"),
            ("https://www.szse.cn/api/report/ShowReport", funds, {"CATALOGID": "1000_lf"}, "xlsx"),
        ]

    monkeypatch.setattr("cnequity.steps.etf_profiles.shanghai_today", lambda: observed)
    monkeypatch.setattr("cnequity.steps.etf_profiles.fetch_exchange_etf_profiles", fake_fetch)
    result = step_etf_profiles(cfg, observed, "etf-test-run", {})

    assert result["rows_written"] == 2
    assert len(list((cfg.meta_root / "raw" / "etf_profiles").rglob("*.json"))) >= 3
    assert list(cfg.staging_root.rglob("*.parquet"))


def test_step_refuses_to_publish_without_raw_archive(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake", raw_archive_enabled=False)
    monkeypatch.setattr(
        "cnequity.steps.etf_profiles.fetch_exchange_etf_profiles",
        lambda *_args: pytest.fail("source should not be fetched without archive"),
    )
    with pytest.raises(RuntimeError, match="requires raw archive"):
        step_etf_profiles(cfg, date(2026, 9, 27), "etf-test-run", {})
    assert not list(cfg.staging_root.rglob("*.parquet"))
