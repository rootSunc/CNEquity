import io
import json
from datetime import date, datetime, timezone

import polars as pl
import pytest
from pypdf import PdfWriter
from tests.catalog_seed import commit_tree, publish_folders

from cnequity.adapters.cninfo.financial_originals import (
    original_annual_notice,
    original_report_notice,
    parse_annual_text,
    parse_report_text,
)
from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.pit import classify_pit_rows
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.raw_archive import RawPayloadArchive
from cnequity.storage.repairs.financial_originals import (
    DATASET,
    PDF_ROOT,
    QUERY_URL,
    _replay,
    _report_batch,
    repair_financial_originals,
)
from cnequity.storage.revisions import RevisionStore

TEXT = """公司代码：600519 公司简称：sample
2016年年度报告
合并资产负债表
2016年12月31日
单位：元 币种：人民币
项目 附注 期末余额 期初余额
资产总计 100.00 90.00
负债合计 40.00 30.00
所有者权益合计 60.00 60.00
应收账款                             1.00
母公司资产负债表
资产总计 999.00 888.00
合并利润表
2016年1-12月
单位：元 币种：人民币
项目 附注 本期发生额 上期发生额
一、营业总收入 1 20.00 19.00
归属于母公司所有者的净利润 2 10.00 9.00
母公司利润表
合并现金流量表
2016年1-12月
单位：元 币种：人民币
项目 附注 本期发生额 上期发生额
经营活动产生的现金流量净额 3 15.00 12.00
母公司现金流量表
"""
STAMP = int(datetime(2017, 4, 14, 16, tzinfo=timezone.utc).timestamp() * 1000)
NOTICE = {
    "secCode": "600519",
    "announcementTitle": "2016年年度报告",
    "announcementTime": STAMP,
    "adjunctUrl": "finalpage/2017-04-15/1200000000.PDF",
}


def test_only_current_consolidated_values_and_explicit_units():
    rows = parse_annual_text(TEXT, "600519.SH", 2016)
    values = {r["item_code"]: r["item_value"] for r in rows}
    assert values["total_assets"] == 100
    assert values["net_profit"] == 10
    assert (
        parse_annual_text(TEXT.replace("2016年年度报告", "2016年度报告"), "600519.SH", 2016) == rows
    )
    assert "accounts_receivable" not in values  # current is blank, comparative is 1
    large = parse_annual_text(TEXT.replace("单位：元", "单位：万元"), "600519.SH", 2016)
    assert large[0]["item_value"] == 1000000
    # Swapped headers cannot lend the prior-year cells current-year meaning.
    with pytest.raises(ValueError):
        parse_annual_text(
            TEXT.replace("期末余额 期初余额", "期初余额 期末余额").replace(
                "本期发生额 上期发生额", "上期发生额 本期发生额"
            ),
            "600519.SH",
            2016,
        )


def test_pdf_padding_does_not_hide_headers_or_change_cells():
    padded = "\n".join(" " * 10000 + line.replace(" ", " " * 1000) for line in TEXT.splitlines())
    assert parse_annual_text(padded, "600519.SH", 2016) == parse_annual_text(
        TEXT, "600519.SH", 2016
    )


def test_primary_book_currency_declaration_is_not_functional_currency():
    text = TEXT.replace("单位：元 币种：人民币", "单位：元").replace(
        "合并资产负债表", "财务附注中报表的单位为：人民币元\n合并资产负债表", 1
    )
    assert parse_annual_text(text, "600519.SH", 2016) == parse_annual_text(TEXT, "600519.SH", 2016)
    with pytest.raises(ValueError):
        parse_annual_text(text.replace("人民币元", "美元"), "600519.SH", 2016)


def test_glossary_currency_definition_applies_only_to_the_named_unit():
    text = TEXT.replace("单位：元 币种：人民币", "单位：元")
    glossary = "释义\n释义项 指 释义内容\n元 指 人民币元\n第二节 公司简介\n"
    assert parse_annual_text(glossary + text, "600519.SH", 2016) == parse_annual_text(
        TEXT, "600519.SH", 2016
    )
    with pytest.raises(ValueError):
        parse_annual_text(glossary + text.replace("单位：元", "单位：万元"), "600519.SH", 2016)
    with pytest.raises(ValueError):
        parse_annual_text(
            glossary + text.replace("单位：元", "单位：元\n币种：美元"), "600519.SH", 2016
        )


@pytest.mark.parametrize(
    "glossary",
    [
        "元 指 人民币元\n",  # a currency mention outside the glossary is insufficient
        "释义\n元 指 美元\n第二节 公司简介\n",
        "释义\n元 指 人民币元\n元 指 美元\n第二节 公司简介\n",
        "释义\n第二节 公司简介\n元 指 人民币元\n",
        "释义\n第二节公司简介\n元 指 人民币元\n",
    ],
)
def test_ambiguous_or_out_of_glossary_currency_is_not_assumed(glossary):
    with pytest.raises(ValueError):
        parse_annual_text(
            glossary + TEXT.replace("单位：元 币种：人民币", "单位：元"), "600519.SH", 2016
        )


def test_invalid_pdf_layout_does_not_abort_the_market_batch(monkeypatch):
    from cnequity.adapters.cninfo import financial_originals as module

    class Page:
        def get(self, key):
            return True

        def extract_text(self, **kwargs):
            raise ZeroDivisionError("zero-width PDF font")

    class Reader:
        pages = [Page()]

    monkeypatch.setattr(module, "PdfReader", lambda *a: Reader())
    with pytest.raises(ValueError, match="cannot be read"):
        module.parse_annual_pdf(b"%PDF-fixture", "600519.SH", 2016)


def test_missing_crypto_dependency_is_an_isolated_report_failure(monkeypatch):
    from pypdf.errors import DependencyError

    from cnequity.adapters.cninfo import financial_originals as module

    def reader(*args):
        raise DependencyError("AES requires cryptography")

    monkeypatch.setattr(module, "PdfReader", reader)
    with pytest.raises(ValueError, match="crypto dependency"):
        module.parse_annual_pdf(b"%PDF-fixture", "600519.SH", 2016)


def test_public_aes_pdf_with_empty_password_reaches_identity_validation():
    from cnequity.adapters.cninfo.financial_originals import parse_annual_pdf

    writer = PdfWriter()
    writer.add_blank_page(width=100, height=100)
    writer.encrypt("", algorithm="AES-256")
    buffer = io.BytesIO()
    writer.write(buffer)
    # A readable blank report is rejected for absent issuer evidence, rather
    # than dependency or decryption failure; no password guessing is involved.
    with pytest.raises(ValueError, match="requested issuer"):
        parse_annual_pdf(buffer.getvalue(), "600519.SH", 2016)


@pytest.mark.parametrize("cached", [{}, {"queries": []}, {"queries": [None]}])
def test_missing_query_archive_is_an_isolated_evidence_failure(cached):
    with pytest.raises(ValueError, match="query archive"):
        _replay(None, cached, "600519.SH", 2016)


@pytest.mark.parametrize(
    "text",
    [
        TEXT.replace("600519", "600000"),
        TEXT.replace("2016", "2015"),
        TEXT.replace("负债合计 40.00", "负债合计 50.00"),
        TEXT.replace("资产总计 100.00 90.00", "资产总计 100.00 90.00\n资产总计 100.00 90.00"),
    ],
)
def test_ambiguous_identity_and_accounts_fail_closed(text):
    with pytest.raises(ValueError):
        parse_annual_text(text, "600519.SH", 2016)


def test_original_notice_excludes_comparisons_corrections_and_summaries():
    rows = [
        NOTICE,
        {**NOTICE, "announcementTitle": "2016年年度报告摘要"},
        {**NOTICE, "announcementTitle": "2016年年度报告（修订版）"},
        {**NOTICE, "secCode": "600000"},
    ]
    assert original_annual_notice(rows, "600519", 2016) == NOTICE
    short_title = {**NOTICE, "announcementTitle": "2016年度报告"}
    assert original_annual_notice([short_title], "600519", 2016) == short_title
    with pytest.raises(ValueError):
        original_annual_notice(rows[1:], "600519", 2016)
    with pytest.raises(ValueError):
        original_annual_notice(
            [NOTICE, {**NOTICE, "adjunctUrl": "finalpage/2017-04-15/1200000001.PDF"}],
            "600519",
            2016,
        )


def test_report_batches_advance_past_failures_without_forgetting_retry_candidates():
    symbols = ["600519.SH", "000001.SZ"]
    done = {("600519.SH", "2016Q4")}
    wanted, first = _report_batch(symbols, 2016, 2017, done, None, 1)
    assert len(wanted) == 3 and first == [("000001.SZ", 2016)]
    # An unsuccessful first report stays eligible for an explicit retry, but
    # does not starve every later issuer when the returned cursor is used.
    _, later = _report_batch(symbols, 2016, 2017, done, "2016:000001.SZ", 1)
    assert later == [("000001.SZ", 2017)]
    assert _report_batch(symbols, 2016, 2017, done, None, 1)[1] == first
    with pytest.raises(ValueError):
        _report_batch(symbols, 2016, 2017, done, "2015:600519.SH", 1)


def test_numbered_headings_rmb_units_and_parenthesized_negatives():
    text = TEXT.replace("单位：元 币种：人民币", "单位：人民币元")
    for number, title in enumerate(
        (
            "合并资产负债表",
            "母公司资产负债表",
            "合并利润表",
            "母公司利润表",
            "合并现金流量表",
            "母公司现金流量表",
        ),
        1,
    ):
        text = text.replace("\n" + title + "\n", f"\n{number}、{title}\n")
    text = text.replace("10.00 9.00", "（10.00） (9.00)")
    values = {
        row["item_code"]: row["item_value"] for row in parse_annual_text(text, "600519.SH", 2016)
    }
    assert values["net_profit"] == -10
    assert values["total_assets"] == 100


def test_explicit_thousand_units_and_note_columns():
    text = TEXT.replace("单位：元", "单位：千元").replace("公司代码：600519", "股票代码 600519")
    text = text.replace("100.00 90.00", "100,001 90,000")
    text = text.replace("40.00 30.00", "40,000 30,000")
    text = text.replace("60.00 60.00", "60,000 60,000")
    text = text.replace("一、营业总收入 1", "一、营业总收入 七（一）")
    rows = {r["item_code"]: r["item_value"] for r in parse_annual_text(text, "600519.SH", 2016)}
    assert rows["total_assets"] == 100001000
    assert rows["revenue"] == 20000
    signed = text.replace("15.00 12.00", "(15,000) (12,000)")
    signed_rows = {
        r["item_code"]: r["item_value"] for r in parse_annual_text(signed, "600519.SH", 2016)
    }
    assert signed_rows["net_cash_operate"] == -15000000


def test_combined_group_and_parent_books_require_role_and_date_order():
    text = """股票代码 000333
2016年年度报告
合并及公司资产负债表
2016年12月31日
(除特别注明外，金额单位为人民币千元)
资产 附注 2016年 2015年 2016年 2015年
合并 合并 公司 公司
资产总计 100,000 90,000 80,000 70,000
负债合计 40,000 30,000 30,000 20,000
股东权益合计 60,000 60,000 50,000 50,000
主管会计工作的负责人：sample 会计机构负责人：sample
2016 年度合并及公司利润表
(除特别注明外，金额单位为人民币千元)
项目 附注 2016年度 2015年度 2016年度 2015年度
合并 合并 公司 公司
一、营业总收入 20,000 19,000 15,000 14,000
减：营业成本 (10,000) (9,000) (9,000) (8,000)
归属于母公司股东的净利润 10,000 9,000 5,000 4,000
主管会计工作的负责人：sample 会计机构负责人：sample
2016 年度合并及公司现金流量表
(除特别注明外，金额单位为人民币千元)
项目 附注 2016年度 2015年度 2016年度 2015年度
合并 合并 公司 公司
经营活动产生的现金流量净额 四(48)(c) (15,000) (12,000) (9,000) (8,000)
主管会计工作的负责人：sample 会计机构负责人：sample
"""
    rows = {r["item_code"]: r["item_value"] for r in parse_annual_text(text, "000333.SZ", 2016)}
    assert rows["total_assets"] == 100000000
    assert rows["net_profit"] == 10000000
    assert rows["net_cash_operate"] == -15000000
    assert "operating_cost" not in rows  # signed deductions stay unresolved
    with pytest.raises(ValueError):
        parse_annual_text(
            text.replace("合并 合并 公司 公司", "公司 公司 合并 合并"), "000333.SZ", 2016
        )
    with pytest.raises(ValueError):
        parse_annual_text(
            text.replace("2016年 2015年 2016年 2015年", "2015年 2016年 2015年 2016年").replace(
                "2016年度 2015年度 2016年度 2015年度", "2015年度 2016年度 2015年度 2016年度"
            ),
            "000333.SZ",
            2016,
        )


def test_split_balance_three_dates_and_explicit_report_currency_policy():
    text = TEXT.replace("单位：元 币种：人民币", "单位：元")
    text = text.replace("合并资产负债表", "合并资产负债表(资产)")
    text = text.replace("母公司资产负债表", "母公司资产负债表(资产)")
    text = text.replace(
        "项目 附注 期末余额 期初余额", "资产 附注 2016年12月31日 2016年1月1日 2015年12月31日"
    )
    for current, prior in (("100.00", "90.00"), ("40.00", "30.00"), ("60.00", "60.00")):
        text = text.replace(f"{current} {prior}", f"{current} {prior} {prior}")
    text += "\n本公司以人民币为记账本位币。\n"
    # Functional currency alone does not establish presentation currency.
    with pytest.raises(ValueError):
        parse_annual_text(text, "600519.SH", 2016)
    text += "本公司按照以下规定，将以外币表示的财务报表折算为人民币金额表示的财务报表。\n"
    rows = {r["item_code"]: r["item_value"] for r in parse_annual_text(text, "600519.SH", 2016)}
    assert rows["total_assets"] == 100 and rows["total_liabilities"] == 40
    assert rows["net_profit"] == 10


def test_date_above_heading_standalone_currency_and_annual_column_aliases():
    text = TEXT.replace("单位：元 币种：人民币", "人民币元")
    text = text.replace("合并资产负债表\n2016年12月31日", "2016年12月31日\n合并资产负债表")
    text = text.replace("期末余额 期初余额", "年末余额 年初余额")
    text = text.replace("本期发生额 上期发生额", "本年金额 上年金额")
    rows = {r["item_code"]: r["item_value"] for r in parse_annual_text(text, "600519.SH", 2016)}
    assert rows["total_assets"] == 100
    assert rows["net_profit"] == 10


def third_quarter_text():
    text = TEXT.replace("2016年年度报告", "2016年第三季度报告")
    text = text.replace("2016年12月31日", "2016年9月30日").replace("2016年1-12月", "2016年1—9月")
    text = text.replace(
        "本期发生额 上期发生额",
        "本期金额 上期金额 年初至报告期期末金额 上年年初至报告期期末金额",
        1,
    )
    text = text.replace("20.00 19.00", "5.00 4.00 20.00 19.00")
    text = text.replace("10.00 9.00", "3.00 2.00 10.00 9.00")
    return text.replace("本期发生额 上期发生额", "年初至报告期期末金额 上年年初至报告期期末金额")


def test_q3_selects_cumulative_columns_and_rejects_wrong_report_period():
    text = third_quarter_text()
    rows = parse_report_text(text, "600519.SH", 2016, quarter=3)
    values = {r["item_code"]: r["item_value"] for r in rows}
    assert values["revenue"] == 20 and values["net_profit"] == 10
    assert values["net_cash_operate"] == 15
    assert {r["report_period"] for r in rows} == {"2016Q3"}
    with pytest.raises(ValueError):
        parse_report_text(text, "600519.SH", 2016, quarter=1)
    # Two nonblank quarter cells with blank cumulative cells stay missing.
    sparse = parse_report_text(
        text.replace("5.00 4.00 20.00 19.00", "5.00 4.00"), "600519.SH", 2016, quarter=3
    )
    assert "revenue" not in {r["item_code"] for r in sparse}
    swapped = text.replace(
        "本期金额 上期金额 年初至报告期期末金额 上年年初至报告期期末金额",
        "年初至报告期期末金额 上年年初至报告期期末金额 本期金额 上期金额",
    )
    assert not any(
        r["statement_type"] == "income"
        for r in parse_report_text(swapped, "600519.SH", 2016, quarter=3)
    )
    dated = text.replace(
        "本期金额 上期金额 年初至报告期期末金额 上年年初至报告期期末金额",
        "2016年第三季度 2015年第三季度 2016年前三季度 2015年前三季度",
    )
    items = {
        r["item_code"]: r["item_value"]
        for r in parse_report_text(dated, "600519.SH", 2016, quarter=3)
    }
    assert items["revenue"] == 20


def test_quarterly_notice_never_selects_another_quarter_or_an_early_publication():
    q1 = {
        **NOTICE,
        "announcementTitle": "2016年第一季度报告",
        "announcementTime": int(datetime(2016, 4, 20, 16, tzinfo=timezone.utc).timestamp() * 1000),
    }
    q3 = {
        **q1,
        "announcementTitle": "2016年第三季度报告",
        "announcementTime": int(datetime(2016, 10, 28, 16, tzinfo=timezone.utc).timestamp() * 1000),
    }
    assert original_report_notice([q3, q1], "600519", 2016, quarter=1) == q1
    assert original_report_notice([q1, q3], "600519", 2016, quarter=3) == q3
    body = {
        **q1,
        "announcementTitle": "2016年第一季度报告正文",
        "adjunctUrl": "finalpage/2016-04-21/1200000001.PDF",
    }
    assert original_report_notice([body], "600519", 2016, quarter=1) == body
    assert original_report_notice([body, q1], "600519", 2016, quarter=1) == q1
    early_body = {**body, "announcementTime": body["announcementTime"] - 86400000}
    assert original_report_notice([q1, early_body], "600519", 2016, quarter=1) == early_body
    with pytest.raises(ValueError):
        original_report_notice(
            [{**q3, "announcementTime": q1["announcementTime"]}], "600519", 2016, quarter=3
        )


def test_quarterly_reports_without_parent_tables_require_explicit_boundaries():
    text = third_quarter_text()
    for title in ("母公司资产负债表", "母公司利润表", "母公司现金流量表"):
        text = text.replace(
            title, "公司负责人：sample 主管会计工作负责人：sample 会计机构负责人：sample"
        )
    text = text.replace("归属于母公司所有者的净利润", "1.归属于母公司股东的净利润（净")
    rows = {
        row["item_code"]: row["item_value"]
        for row in parse_report_text(text, "600519.SH", 2016, quarter=3)
    }
    assert rows["total_assets"] == 100 and rows["net_profit"] == 10
    assert rows["net_cash_operate"] == 15
    before, after = text.rsplit(
        "公司负责人：sample 主管会计工作负责人：sample 会计机构负责人：sample", 1
    )
    missing = before + after
    # The final cash-flow table without a closing boundary stays unresolved.
    assert not any(
        row["statement_type"] == "cashflow"
        for row in parse_report_text(missing, "600519.SH", 2016, quarter=3)
    )


def test_quarterly_archive_publication_is_separate_from_annual_batches(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    notice = {
        **NOTICE,
        "announcementTitle": "2016年第三季度报告",
        "announcementTime": int(datetime(2016, 10, 28, 16, tzinfo=timezone.utc).timestamp() * 1000),
        "adjunctUrl": "finalpage/2016-10-29/1200000000.PDF",
    }
    archive = RawPayloadArchive(cfg.meta_root, enabled=True, datasets=[DATASET])
    query = archive.archive(
        DATASET,
        json.dumps({"totalpages": 1, "announcements": [NOTICE, notice]}).encode(),
        source="cninfo",
        run_id="fixture",
        url=QUERY_URL,
        request_params={
            "pageNum": 1,
            "stock": "600519,org",
            "searchkey": "2016年第三季度报告",
            "seDate": "2016-01-01~2017-12-31",
        },
        payload_format="json",
    )
    pdf = archive.archive(
        DATASET,
        b"%PDF-fixture",
        source="cninfo",
        run_id="fixture",
        url=PDF_ROOT + notice["adjunctUrl"],
        request_params={},
        payload_format="pdf",
    )
    write_json_atomic(
        cfg.meta_root / "source_cache" / "financial_originals" / "600519.SH-2016Q3.json",
        {"queries": [query.to_dict()], "report": pdf.to_dict()},
    )
    monkeypatch.setattr(
        "cnequity.storage.repairs.financial_originals.parse_report_pdf",
        lambda payload, symbol, year, quarter: parse_report_text(
            third_quarter_text(), symbol, year, quarter=quarter
        ),
    )
    result = repair_financial_originals(
        cfg, symbols=["600519.SH"], start_year=2016, end_year=2016, quarter=3, apply=True
    )
    assert result["applied"] and result["rows"] == 6
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    rows = _rows(store.current_root(DATASET))
    assert rows["report_period"].unique().to_list() == ["2016Q3"]
    assert rows["announce_date"].unique().to_list() == [date(2016, 10, 29)]
    assert rows.filter(pl.col("item_code") == "revenue")["item_value"][0] == 20
    assert (
        repair_financial_originals(
            cfg, symbols=["600519.SH"], start_year=2016, end_year=2016, quarter=3
        )["reports_selected"]
        == 0
    )
    annual = repair_financial_originals(cfg, symbols=["600519.SH"], start_year=2016, end_year=2016)
    assert annual["reports_selected"] == 1 and annual["failures"]


def test_issuer_archive_reads_partial_last_page_and_rejects_false_coverage(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    archive = RawPayloadArchive(cfg.meta_root, enabled=True, datasets=[DATASET])
    other = [
        {
            **NOTICE,
            "announcementId": str(number),
            "announcementTitle": f"other {number}",
            "adjunctUrl": f"finalpage/2017-04-15/120000{number:04}.PDF",
        }
        for number in range(30)
    ]

    def page(number, rows):
        return archive.archive(
            DATASET,
            json.dumps({"totalpages": 1, "totalRecordNum": 31, "announcements": rows}).encode(),
            source="cninfo",
            run_id="fixture",
            url=QUERY_URL,
            request_params={
                "pageNum": number,
                "stock": "600519,org",
                "searchkey": "",
                "category": "",
                "seDate": "2017-01-01~2017-12-31",
            },
            payload_format="json",
        ).to_dict()

    pdf = archive.archive(
        DATASET,
        b"%PDF-fixture",
        source="cninfo",
        run_id="fixture",
        url=PDF_ROOT + NOTICE["adjunctUrl"],
        request_params={},
        payload_format="pdf",
    ).to_dict()
    cached = {"queries": [page(1, other), page(2, [NOTICE])], "report": pdf}
    assert _replay(archive, cached, "600519.SH", 2016)[0] == NOTICE
    with pytest.raises(ValueError, match="incomplete"):
        _replay(archive, {**cached, "queries": cached["queries"][:1]}, "600519.SH", 2016)
    with pytest.raises(ValueError, match="duplicated"):
        _replay(
            archive,
            {**cached, "queries": [cached["queries"][0], page(2, [other[0]])]},
            "600519.SH",
            2016,
        )


def _rows(root):
    """The rows of a published root, whether a directory or a catalog revision."""
    from cnequity.query.parquet_scan import scan_parquet_root

    return scan_parquet_root(root, committed=False).collect()


def test_append_original_with_current_observation_and_archive_replay(tmp_path, monkeypatch, settle):
    cfg = Config(data_root=tmp_path / "lake")
    root = cfg.curated_root / DATASET / "report_period=2016Q4"
    root.mkdir(parents=True)
    path = root / "part-0.parquet"
    old = pl.DataFrame(
        [
            dict(
                symbol="600519.SH",
                report_period="2016Q4",
                statement_type="balance",
                item_code="total_assets",
                item_value=110.0,
                announce_date=date(2017, 4, 15),
                source="eastmoney_backfill",
                data_version="v1",
                fetched_at=datetime(2026, 10, 12, tzinfo=timezone.utc),
            )
        ],
        schema=DATASET_SCHEMAS[DATASET],
    )
    old.write_parquet(path)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    commit_tree(
        store,
        DATASET,
        run_id="seed",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="fixture",
    )
    previous = store.current_root(DATASET)
    archive = RawPayloadArchive(cfg.meta_root, enabled=True, datasets=[DATASET])
    query = archive.archive(
        DATASET,
        json.dumps({"totalpages": 1, "announcements": [NOTICE]}).encode(),
        source="cninfo",
        run_id="proof",
        url=QUERY_URL,
        request_params={
            "pageNum": 1,
            "stock": "600519,org",
            "searchkey": "2016年年度报告",
            "seDate": "2017-01-01~2017-12-31",
        },
        payload_format="json",
    )
    report = archive.archive(
        DATASET,
        b"%PDF-fixture",
        source="cninfo",
        run_id="proof",
        url=PDF_ROOT + NOTICE["adjunctUrl"],
        request_params={},
        payload_format="pdf",
    )
    write_json_atomic(
        cfg.meta_root / "source_cache" / "financial_originals" / "600519.SH-2016.json",
        {"queries": [query.to_dict()], "report": report.to_dict()},
    )
    monkeypatch.setattr(
        "cnequity.storage.repairs.financial_originals.parse_annual_pdf",
        lambda *a: parse_annual_text(TEXT, "600519.SH", 2016),
    )
    settle(cfg)
    preview = repair_financial_originals(cfg, symbols=["600519.SH"], start_year=2016, end_year=2016)
    assert preview["rows"] == 6 and not preview["applied"]
    result = repair_financial_originals(
        cfg, symbols=["600519.SH"], start_year=2016, end_year=2016, apply=True
    )
    assert result["applied"] and result["revision"] == 2
    current = _rows(store.current_root(DATASET))
    original = current.filter(pl.col("source") == "cninfo_original")
    assert original["observed_at"].min().year >= 2026
    assert original["announce_date"].unique().to_list() == [date(2017, 4, 15)]
    assert classify_pit_rows(
        original, DATASET, as_of=date(2017, 4, 16), pit_mode="strict"
    ).is_empty()
    best = classify_pit_rows(original, DATASET, as_of=date(2017, 4, 16), pit_mode="best_effort")
    assert best.height == 6 and not best["pit_is_exact"].any()
    assert _rows(previous)["item_value"].to_list() == [110]
    replay = repair_financial_originals(
        cfg, symbols=["600519.SH"], start_year=2016, end_year=2016, reparse=True, apply=True
    )
    assert not replay["applied"] and replay["rows"] == 0 and replay["reports_parsed"] == 1
    after = _rows(store.current_root(DATASET))
    assert after.equals(current)
    # Later reconstructed fetches cannot replace this original-date fact.
    later = old.with_columns(pl.lit(datetime(2027, 1, 1, tzinfo=timezone.utc)).alias("fetched_at"))
    combined = pl.concat([original, later], how="diagonal_relaxed")
    picked = dedupe_by_primary_key(combined, DATASET)
    assert picked.filter(pl.col("item_code") == "total_assets")["item_value"][0] == 100
    assert (
        repair_financial_originals(cfg, symbols=["600519.SH"], start_year=2016, end_year=2016)[
            "reports_selected"
        ]
        == 0
    )
    # Whole-catalogue batches include stocks, excluding listed funds.
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame(
        [
            dict(symbol="600519.SH", code="600519", exchange="SH", asset_type="stock"),
            dict(symbol="510300.SH", code="510300", exchange="SH", asset_type="fund"),
        ],
        schema=DATASET_SCHEMAS["instruments"],
    ).write_parquet(instruments / "part-0.parquet")
    publish_folders(cfg, "instruments")
    assert (
        repair_financial_originals(cfg, all_stocks=True, start_year=2016, end_year=2016)[
            "reports_selected"
        ]
        == 0
    )
    with pytest.raises(ValueError, match="either"):
        repair_financial_originals(cfg, symbols=["600519.SH"], all_stocks=True)


def test_later_membership_only_does_not_erase_exact_date_weights():
    from cnequity.domain.schemas import INDEX_CONSTITUENTS_SCHEMA

    frame = pl.DataFrame(
        [
            dict(
                index_symbol="000688.SH",
                symbol="688001.SH",
                as_of_date=date(2026, 9, 30),
                weight=0.02,
                source="exchange",
                data_version="v1",
                fetched_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
            ),
            dict(
                index_symbol="000688.SH",
                symbol="688001.SH",
                as_of_date=date(2026, 9, 30),
                weight=None,
                source="eastmoney",
                data_version="v1",
                fetched_at=datetime(2026, 10, 2, tzinfo=timezone.utc),
            ),
        ],
        schema=INDEX_CONSTITUENTS_SCHEMA,
    )
    assert dedupe_by_primary_key(frame, "index_constituents")["weight"][0] == 0.02


def test_company_parent_title_and_explicit_rmb_amount_declaration():
    text = TEXT.replace("\n母公司", "\n公司").replace(
        "单位：元 币种：人民币", "（除特别注明外，金额单位均为人民币元）"
    )
    rows = parse_annual_text(text, "600519.SH", 2016)
    assert {r["item_code"]: r["item_value"] for r in rows} == {
        "total_assets": 100,
        "total_liabilities": 40,
        "total_equity": 60,
        "revenue": 20,
        "net_profit": 10,
        "net_cash_operate": 15,
    }
    assert all(r["item_value"] != 999 for r in rows)


@pytest.mark.parametrize(
    "declaration",
    [
        "（金额单位均为美元元）",
        "（本位币为人民币元）",
        "（人民币元仅适用于公司报表）",
    ],
)
def test_amount_declaration_requires_explicit_rmb_table_units(declaration):
    text = TEXT.replace("\n母公司", "\n公司").replace("单位：元 币种：人民币", declaration)
    with pytest.raises(ValueError):
        parse_annual_text(text, "600519.SH", 2016)


def test_bare_standalone_book_is_a_boundary_and_never_a_source_of_consolidated_cells():
    text = TEXT.replace("\n母公司", "\n").replace("单位：元 币种：人民币", "金额单位均为人民币元")
    assert parse_annual_text(text, "600519.SH", 2016) == parse_annual_text(TEXT, "600519.SH", 2016)


def test_dated_rmb_unit_line_and_asset_or_note_column_headers():
    text = TEXT.replace("2016年12月31日\n单位：元 币种：人民币", "2016年12月31日 人民币元")
    text = text.replace("2016年1-12月\n单位：元 币种：人民币", "2016年度 人民币元")
    text = text.replace("项目 附注 期末余额 期初余额", "资产 附注五 2016年12月31日 2015年12月31日")
    text = text.replace("项目 附注 本期发生额 上期发生额", "附注五 2016年 2015年")
    assert parse_annual_text(text, "600519.SH", 2016) == parse_annual_text(TEXT, "600519.SH", 2016)
    with pytest.raises(ValueError):
        parse_annual_text(text.replace("人民币元", "美元元"), "600519.SH", 2016)
    with pytest.raises(ValueError):
        parse_annual_text(
            text.replace("2016年12月31日 2015年12月31日", "2015年12月31日 2016年12月31日").replace(
                "2016年 2015年", "2015年 2016年"
            ),
            "600519.SH",
            2016,
        )


@pytest.mark.parametrize("negative", ["( 15.00 )", "（ 15.00 ）"])
def test_padding_inside_accounting_parentheses_keeps_current_negative_sign(negative):
    text = TEXT.replace("3 15.00 12.00", "3 " + negative + " 12.00")
    rows = parse_annual_text(text, "600519.SH", 2016)
    assert next(r["item_value"] for r in rows if r["item_code"] == "net_cash_operate") == -15


@pytest.mark.parametrize("negative", ["( 15.00", "15.00 )"])
def test_incomplete_negative_cell_is_unresolved_instead_of_positive(negative):
    rows = parse_annual_text(
        TEXT.replace("3 15.00 12.00", "3 " + negative + " 12.00"), "600519.SH", 2016
    )
    assert all(r["item_code"] != "net_cash_operate" for r in rows)


def test_inline_table_units_keep_currency_and_parent_boundaries():
    text = TEXT
    for title in ("资产负债表", "利润表", "现金流量表"):
        text = text.replace("合并" + title + "\n", "合并" + title + "  单位：元 币种：人民币\n")
        text = text.replace("母公司" + title + "\n", "母公司" + title + "  单位：元\n")
    text = text.replace("\n单位：元 币种：人民币\n", "\n")
    assert parse_annual_text(text, "600519.SH", 2016) == parse_annual_text(TEXT, "600519.SH", 2016)
    for currency in ("美元", "人民币美元"):
        foreign = text.replace(
            "合并资产负债表  单位：元 币种：人民币",
            "合并资产负债表  单位：元 币种：" + currency,
        )
        assert "total_assets" not in {
            r["item_code"] for r in parse_annual_text(foreign, "600519.SH", 2016)
        }


def test_inline_plain_units_require_the_primary_rmb_declaration():
    text = TEXT.replace("单位：元 币种：人民币\n", "")
    for title in ("资产负债表", "利润表", "现金流量表"):
        text = text.replace("合并" + title + "\n", "合并" + title + "  单位：元\n")
        text = text.replace("母公司" + title + "\n", "母公司" + title + "  单位：元\n")
    with pytest.raises(ValueError):
        parse_annual_text(text, "600519.SH", 2016)
    text = text.replace("合并资产负债表", "财务附注中报表的单位为：人民币元\n合并资产负债表", 1)
    assert parse_annual_text(text, "600519.SH", 2016) == parse_annual_text(TEXT, "600519.SH", 2016)


@pytest.mark.parametrize("suffix", ["附注1", "单位：股", "单位：万元 另见附注", "其它说明"])
def test_inline_titles_never_accept_note_references_or_unknown_units(suffix):
    text = TEXT
    for title in ("资产负债表", "利润表", "现金流量表"):
        text = text.replace("合并" + title + "\n", "合并" + title + "  " + suffix + "\n")
    with pytest.raises(ValueError):
        parse_annual_text(text, "600519.SH", 2016)


@pytest.mark.parametrize("copies", [2, 4])
def test_overprinted_statement_titles_keep_consolidated_scope(copies):
    text = TEXT
    for title in (
        "合并资产负债表",
        "母公司资产负债表",
        "合并利润表",
        "母公司利润表",
        "合并现金流量表",
        "母公司现金流量表",
    ):
        text = text.replace(title + "\n", "1、、、、" + (title * copies) + "\n")
    text = text.replace("单位：元 币种：人民币", "单位：元")
    text = text.replace(
        "1、、、、合并资产负债表", "财务附注中报表的单位为：人民币元\n1、、、、合并资产负债表", 1
    )
    assert parse_annual_text(text, "600519.SH", 2016) == parse_annual_text(TEXT, "600519.SH", 2016)


@pytest.mark.parametrize(
    "heading",
    [
        "合并资产负债表母公司资产负债表",
        "合并资产负债表" * 5,
        "说明合并资产负债表合并资产负债表",
    ],
)
def test_overprinted_title_recovery_rejects_mixed_or_unbounded_text(heading):
    text = TEXT.replace("合并资产负债表\n", heading + "\n")
    rows = parse_annual_text(text, "600519.SH", 2016)
    assert "total_assets" not in {row["item_code"] for row in rows}


@pytest.mark.parametrize("unit,scale", [("元", 1), ("万元", 10000)])
@pytest.mark.parametrize("parentheses", [("（", "）"), ("(", ")")])
def test_paired_glossary_units_bind_each_exact_rmb_definition(unit, scale, parentheses):
    left, right = parentheses
    glossary = f"释义\n元{left}万元{right} 指 人民币元{left}人民币万元{right}\n第二节 公司简介\n"
    text = TEXT.replace("单位：元 币种：人民币", f"单位：{unit}")
    rows = parse_annual_text(glossary + text, "600519.SH", 2016)
    assert {r["item_code"]: r["item_value"] for r in rows}["total_assets"] == 100 * scale
    assert "accounts_receivable" not in {r["item_code"] for r in rows}


@pytest.mark.parametrize(
    "definition",
    [
        "元（万元） 指 美元（人民币万元）",
        "元（万元） 指 人民币万元（人民币元）",
        "元（万元） 指 人民币元（人民币万元）\n元 指 美元",
        "元（元） 指 人民币元（美元）",
    ],
)
def test_paired_glossary_definitions_never_override_conflicting_currency(definition):
    text = TEXT.replace("单位：元 币种：人民币", "单位：元")
    with pytest.raises(ValueError):
        parse_annual_text("释义\n" + definition + "\n第二节 公司简介\n" + text, "600519.SH", 2016)


@pytest.mark.parametrize("unit,scale", [("元", 1), ("万元", 10000)])
@pytest.mark.parametrize(
    "meaning", ["人民币元、万元", "人民币元、人民币万元", "人民币元和人民币万元"]
)
def test_enumerated_glossary_units_bind_exact_ordered_pair(unit, scale, meaning):
    glossary = f"释义\n元、万元 指 {meaning}\n第二节 公司简介\n"
    text = TEXT.replace("单位：元 币种：人民币", f"单位：{unit}")
    rows = parse_annual_text(glossary + text, "600519.SH", 2016)
    assert {r["item_code"]: r["item_value"] for r in rows}["total_assets"] == 100 * scale


@pytest.mark.parametrize(
    "definition",
    [
        "元、万元 指 美元元、万元",
        "元、万元 指 人民币万元、元",
        "元、万元 指 人民币元、美元万元",
        "元、元 指 人民币元、元",
        "元、万元 指 人民币万元和人民币元",
        "元、万元 指 美元元和人民币万元",
        "元、万元 指 人民币元、万元\n元 指 美元",
        "元 指 人民币元\n元、万元 指 美元元、万元",
    ],
)
def test_enumerated_glossary_rejects_ambiguous_or_conflicting_meanings(definition):
    text = TEXT.replace("单位：元 币种：人民币", "单位：元")
    with pytest.raises(ValueError):
        parse_annual_text("释义\n" + definition + "\n第二节 公司简介\n" + text, "600519.SH", 2016)


def test_a_label_wrapped_inside_its_bracket_takes_its_tail_from_the_next_line():
    """600301.SH 2022: 所有者权益（或股东权 / 益）合计, amounts on the first line."""
    wrapped = TEXT.replace(
        "所有者权益合计 60.00 60.00\n",
        "所有者权益（或股东权 60.00 60.00\n益）合计\n",
    )
    values = {
        r["item_code"]: r["item_value"]
        for r in parse_annual_text(wrapped, "600519.SH", 2016)
        if r["statement_type"] == "balance"
    }
    assert values["total_equity"] == 60
    # A wrap whose next line carries amounts is a different row, not a tail.
    other = TEXT.replace(
        "所有者权益合计 60.00 60.00\n", "所有者权益（或股东权 60.00 60.00\n存货 5.00 4.00\n"
    )
    assert "total_equity" not in {
        r["item_code"] for r in parse_annual_text(other, "600519.SH", 2016)
    }
