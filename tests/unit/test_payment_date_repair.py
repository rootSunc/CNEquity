import json
from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.adapters.baostock.corporate_actions import _FIELDS, _action_rows
from cnequity.adapters.cninfo.payment_notices import (
    _issuer_precision_cash_correction,
    _repurchase_cash_correction,
    _strip_repeated_pdf_header,
    parse_payment_notice_text,
)
from cnequity.domain.canonical import dedupe_by_primary_key, dedupe_lazy_by_primary_key
from cnequity.run_scope import backfill_of, set_backfill
from cnequity.steps.payment_dates import match_payment_dates, unique_payment_issues


def test_payment_report_keeps_one_copy_of_each_distinct_failure():
    missing = {"symbol": "920926.BJ", "ex_date": "2024-05-20", "reason": "source_missing"}
    correction = {**missing, "reason": "correction_requires_review"}
    assert unique_payment_issues([missing, correction, missing]) == [missing, correction]


def event(**kwargs):
    return dict(
        symbol="600000.SH",
        ex_date=date(2023, 7, 21),
        action_type="cash_dividend",
        cash_dividend=0.32,
        bonus_ratio=0.0,
        transfer_ratio=0.0,
        **kwargs,
    )


def test_payment_uses_explicit_pay_field_never_ex_date():
    row = [""] * len(_FIELDS)
    for key, value in {
        "code": "sh.600000",
        "dividOperateDate": "2023-07-21",
        "dividPayDate": "2023-07-25",
        "dividCashPsBeforeTax": ".32",
    }.items():
        row[_FIELDS.index(key)] = value
    positions = {k: i for i, k in enumerate(_FIELDS)}
    parsed = _action_rows("600000.SH", row, positions, date(2023, 1, 1), date(2023, 12, 31))
    assert parsed[0]["payment_date"] == date(2023, 7, 25)
    row[_FIELDS.index("dividPayDate")] = ""
    assert (
        _action_rows("600000.SH", row, positions, date(2023, 1, 1), date(2023, 12, 31))[0][
            "payment_date"
        ]
        is None
    )


def test_issuer_notice_accepts_explicit_sz_payment_and_pretax_amount():
    text = (
        "证券代码：002643 2017年度分红派息实施公告。"
        "向全体股东每10股派1.530000元人民币现金（含税；扣税后每10股派1.377元）。"
        "股权登记日为2018年6月4日，除息日为2018年6月5日。"
        "委托中国结算深圳分公司代派的现金红利将于2018年6月5日划入资金账户。"
    )
    assert parse_payment_notice_text(text) == {
        "code": "002643",
        "ex_date": date(2018, 6, 5),
        "payment_date": date(2018, 6, 5),
        "cash_dividend": 0.153,
    }


def test_issuer_notice_sz_a_b_share_uses_only_a_share_dates():
    text = (
        "证券代码：000550；200550。每10股派现金红利23.17元（含税）。"
        "A股股权登记日为2018年3月26日，除息日为2018年3月27日。"
        "B股最后交易日为2018年3月26日，除息日为2018年3月27日，"
        "B股股权登记日为2018年3月29日。"
        "代派的A股股东股息将于2018年3月27日划入资金账户。"
        "代派的B股股东股息将于2018年3月29日划入资金账户。"
    )
    assert parse_payment_notice_text(text) == {
        "code": "000550",
        "ex_date": date(2018, 3, 27),
        "payment_date": date(2018, 3, 27),
        "cash_dividend": 2.317,
    }


def test_issuer_notice_does_not_substitute_b_share_payment_for_incomplete_a_share():
    text = (
        "证券代码：000550。每10股派现金红利23.17元（含税）。"
        "A股股权登记日为2018年3月26日，除息日为2018年3月27日。"
        "B股现金红利发放日2018年3月29日。"
    )
    assert parse_payment_notice_text(text) is None


def test_issuer_notice_removes_page_boilerplate_splitting_explicit_date():
    text = (
        "证券代码：002631。每10股派0.250000元人民币现金（含税）。"
        "除权除息日为2016年6"
        "2015年年度权益分派实施公告-2-"
        "月21日。现金红利将于2016年6月21日划入资金账户。"
    )
    assert parse_payment_notice_text(text) == {
        "code": "002631",
        "ex_date": date(2016, 6, 21),
        "payment_date": date(2016, 6, 21),
        "cash_dividend": 0.025,
    }
    disclaimer = (
        "本公司及董事会全体成员保证信息披露内容的真实、准确和完整，"
        "没有虚假记载、误导性陈述或重大遗漏。"
    )
    assert parse_payment_notice_text(
        text.replace("2015年年度权益分派实施公告-2-", disclaimer)
    ) == parse_payment_notice_text(text)


def test_issuer_notice_reads_cash_after_bonus_shares_without_inventing_date():
    text = (
        "证券代码：300110。每10股送红股1.000000股，派0.150000元人民币现金（含税）。"
        "除权除息日为2017年5月31日。现金红利将于2017年5月31日到账。"
    )
    assert parse_payment_notice_text(text) == {
        "code": "300110",
        "ex_date": date(2017, 5, 31),
        "payment_date": date(2017, 5, 31),
        "cash_dividend": 0.015,
    }
    assert parse_payment_notice_text(text.replace("除权除息日为2017年5月31日。", "")) is None


def test_issuer_notice_payment_before_ex_date_is_a_template_typo():
    text = (
        "证券代码：002627。每10股派1.500000元人民币现金（含税）。"
        "股权登记日为：2016年7月5日，除权除息日为：2016年7月6日。"
        "代派的现金红利将于2015年7月6日通过股东托管证券公司直接划入其资金账户。"
    )
    assert parse_payment_notice_text(text) is None
    assert parse_payment_notice_text(text.replace("2015年7月6日", "2016年7月6日"))[
        "payment_date"
    ] == date(2016, 7, 6)


def test_issuer_notice_uses_issuer_final_amount_after_fixed_total_adjustment():
    text = (
        "证券代码：300018。一、原方案每10股派发现金红利1.00元（含税）。"
        "回购注销后按现金分红总额固定不变的原则，"
        "最终以现有股本向全体股东每10股派1.005486元人民币现金（含税）。"
        "除权除息日为2018年6月20日。"
        "代派的A股股东现金红利将于2018年6月20日划入资金账户。"
    )
    assert parse_payment_notice_text(text) == {
        "code": "300018",
        "ex_date": date(2018, 6, 20),
        "payment_date": date(2018, 6, 20),
        "cash_dividend": pytest.approx(0.1005486),
    }


def test_issuer_notice_strips_repeated_page_header_before_payment_date():
    first = (
        "证券代码：002636。每10股派0.850000元人民币现金（含税）。"
        "除权除息日为2022年8月9日。代派的A股股东现金红利将于"
    )
    second = _strip_repeated_pdf_header(
        "金安国纪科技股份有限公司 2021年年度权益分派实施公告\n"
        "2\n2022年8月9日通过证券公司划入资金账户。"
    )
    assert parse_payment_notice_text(first + second) == {
        "code": "002636",
        "ex_date": date(2022, 8, 9),
        "payment_date": date(2022, 8, 9),
        "cash_dividend": pytest.approx(0.085),
    }


def test_issuer_notice_strips_only_matching_physical_page_number_from_split_cash():
    from cnequity.adapters.cninfo.payment_notices import _strip_repeated_pdf_header

    first = (
        "证券代码：600519。A股每股现金红利19.106元（含税）。"
        "股份类别股权登记日最后交易日除权（息）日现金红利发放日"
        "Ａ股2023/12/19－2023/12/202023/12/20。"
        "本次分红以总股本为基数，每股派发现金红利"
    )
    second = _strip_repeated_pdf_header(" 2 \n\n19.106元（含税）。", 2)
    assert parse_payment_notice_text(first + second) == {
        "code": "600519",
        "ex_date": date(2023, 12, 20),
        "payment_date": date(2023, 12, 20),
        "cash_dividend": pytest.approx(19.106),
    }
    assert _strip_repeated_pdf_header(" 3 \n\n19.106元（含税）。", 2).startswith(" 3")


def test_sh_table_handles_concatenated_one_digit_days():
    text = (
        "证券代码：600803。A股每股现金红利0.91元（含税）。"
        "股份类别股权登记日最后交易日除权（息）日现金红利发放日"
        "Ａ股2024/7/31－2024/8/12024/8/1。"
    )
    assert parse_payment_notice_text(text) == {
        "code": "600803",
        "ex_date": date(2024, 8, 1),
        "payment_date": date(2024, 8, 1),
        "cash_dividend": pytest.approx(0.91),
    }


def test_combined_dividend_requires_components_to_equal_printed_total():
    text = (
        "证券代码：601966。A股每股现金红利0.377元，"
        "为2023年度和2024年一季度合并后的分配比例，"
        "其中2023年度每股现金红利0.286元、2024年一季度每股现金红利0.091元。"
        "相关日期股份类别股权登记日最后交易日除权（息）日现金红利发放日"
        "Ａ股2024/6/13－2024/6/142024/6/14。"
    )
    assert parse_payment_notice_text(text) == {
        "code": "601966",
        "ex_date": date(2024, 6, 14),
        "payment_date": date(2024, 6, 14),
        "cash_dividend": pytest.approx(0.377),
    }
    assert parse_payment_notice_text(text.replace("0.091元", "0.092元")) is None


@pytest.mark.parametrize(
    ("headline", "total"),
    [
        (
            "每股分配比例2022年度每股派发现金红利人民币1.30元（含税），"
            "每股再派发特别红利现金人民币0.50元（含税），"
            "共计A股每股现金红利人民币1.80元（含税）。",
            1.8,
        ),
        (
            "每股分配比例每股合计派发现金红利人民币1.20元（含税），"
            "其中年度分红人民币1.00元/股（含税）以及30周年特别分红"
            "人民币0.20元/股（含税）。",
            1.2,
        ),
    ],
)
def test_special_dividend_total_requires_two_consistent_components(headline, total):
    text = (
        "证券代码：601318。" + headline + "相关日期股份类别股权登记日最后交易日"
        "除权（息）日现金红利发放日Ａ股2018/6/6－2018/6/72018/6/7。"
    )
    assert parse_payment_notice_text(text)["cash_dividend"] == pytest.approx(total)
    altered = text.replace("1.30元", "1.31元") if total == 1.8 else text.replace("0.20元", "0.21元")
    assert parse_payment_notice_text(altered) is None


def test_annual_plus_special_dividend_uses_tax_inclusive_issuer_total():
    text = (
        "证券代码：601088。"
        "二、分配方案：派发2016年度末期股息现金每股人民币0.460元（含税）；"
        "派发特别股息现金每股人民币2.510元（含税）。"
        "两项合计派发股息现金每股人民币2.970元（含税）。"
        "股份类别股权登记日最后交易日除权（息）日现金红利发放日"
        "Ａ股2017/7/7－2017/7/102017/7/10。"
    )
    assert parse_payment_notice_text(text) == {
        "code": "601088",
        "ex_date": date(2017, 7, 10),
        "payment_date": date(2017, 7, 10),
        "cash_dividend": pytest.approx(2.97),
    }
    assert parse_payment_notice_text(text.replace("2.510元", "2.520元")) is None


def test_issuer_notice_sh_a_share_table_does_not_use_h_share_dates():
    text = (
        "股票代码：600188。A股每股现金股利1.00元（含税）。"
        "相关日期 股份类别 股权登记日 最后交易日 除权（息）日 现金红利发放日 "
        "Ａ股 2021/7/22 － 2021/7/23 2021/7/23。"
        "H股现金红利发放日2021/8/8。"
    )
    assert parse_payment_notice_text(text) == {
        "code": "600188",
        "ex_date": date(2021, 7, 23),
        "payment_date": date(2021, 7, 23),
        "cash_dividend": 1.0,
    }


def test_issuer_notice_reads_cash_dividend_label_and_expanded_a_share_table():
    text = (
        "股票代码：600188。A股每股现金红利4.30元（含税）。"
        "股份类别 股权登记日 最后交易日 除权（息）日 "
        "新增无限售条件流通股份上市日 现金红利发放日 "
        "Ａ股 2023/7/14 － 2023/7/17 2023/7/17 2023/7/17。"
        "H股现金红利发放日2023/8/8。"
    )
    assert parse_payment_notice_text(text) == {
        "code": "600188",
        "ex_date": date(2023, 7, 17),
        "payment_date": date(2023, 7, 17),
        "cash_dividend": 4.3,
    }


def test_issuer_notice_reads_concatenated_a_share_date_cells():
    text = (
        "证券代码：603285。A股每股现金红利1.00元（含税）。"
        "股份类别股权登记日最后交易日除权（息）日现金红利发放日"
        "Ａ股2024/9/4－2024/9/52024/9/5。"
    )
    assert parse_payment_notice_text(text) == {
        "code": "603285",
        "ex_date": date(2024, 9, 5),
        "payment_date": date(2024, 9, 5),
        "cash_dividend": 1.0,
    }


def test_issuer_notice_reads_cash_distribution_wording():
    text = (
        "证券代码：300750。每10股派发现金分红人民币50.28元（含税）。"
        "除权除息日为2024年4月30日。A股股东现金分红将于2024年4月30日到账。"
    )
    facts = parse_payment_notice_text(text)
    assert facts is not None
    assert facts["code"] == "300750"
    assert facts["ex_date"] == facts["payment_date"] == date(2024, 4, 30)
    assert facts["cash_dividend"] == pytest.approx(5.028)


def test_issuer_notice_uses_final_implementation_after_share_count_change():
    text = (
        "证券代码：002621。一、股东大会审议通过利润分配方案。"
        "拟每10股派发现金红利0.20元（含税）。"
        "二、本次实施的利润分配方案：每10股派0.200017元（含税）。"
        "三、股权登记日与除权除息日：除权除息日为2019年6月14日。"
        "现金红利将于2019年6月14日划入资金账户。"
    )
    facts = parse_payment_notice_text(text)
    assert facts is not None
    assert facts["cash_dividend"] == pytest.approx(0.0200017)
    assert facts["payment_date"] == date(2019, 6, 14)


def test_issuer_notice_precision_correction_requires_buyback_and_matching_identity():
    facts = {
        "code": "300114",
        "ex_date": date(2021, 6, 4),
        "payment_date": date(2021, 6, 4),
        "cash_dividend": 0.0504002,
    }
    old = {"symbol": "300114.SZ", "ex_date": date(2021, 6, 4), "cash_dividend": 0.0504}
    notice = (
        "回购股份不参与利润分配。二、本次实施的利润分配方案：每10股派0.504002元人民币现金（含税）。"
    )
    assert _issuer_precision_cash_correction(old, facts, notice)
    assert not _issuer_precision_cash_correction(old, facts, notice.replace("不参与", "参与"))
    assert not _issuer_precision_cash_correction(old, {**facts, "code": "300115"}, notice)
    assert not _issuer_precision_cash_correction(old, {**facts, "cash_dividend": 0.0505}, notice)


def test_issuer_notice_rejects_conflicting_pretax_amounts():
    text = (
        "证券代码：002643。每10股派1.53元人民币现金（含税）。"
        "每10股分配现金股利2.00元（含税）。"
        "除息日为2018年6月5日，现金红利将于2018年6月5日到账。"
    )
    assert parse_payment_notice_text(text) is None


def test_issuer_notice_corrects_only_explicit_buyback_ex_price_amount():
    facts = {
        "code": "002206",
        "ex_date": date(2020, 7, 3),
        "payment_date": date(2020, 7, 3),
        "cash_dividend": 0.1,
    }
    old = {"symbol": "002206.SZ", "ex_date": date(2020, 7, 3), "cash_dividend": 0.094724}
    notice = (
        "公司回购股份不参与分红。每10股派1.00元人民币现金（含税）。"
        "除权除息价格=股权登记日收盘价-0.094724。"
    )
    assert _repurchase_cash_correction(old, facts, notice)
    assert not _repurchase_cash_correction(old, facts, notice.replace("不参与", "参与"))
    assert not _repurchase_cash_correction(old, facts, notice.replace("0.094724", "0.094725"))
    assert not _repurchase_cash_correction(old, {**facts, "code": "002207"}, notice)
    assert not _repurchase_cash_correction(old, {**facts, "ex_date": date(2020, 7, 4)}, notice)


def test_match_refuses_changed_cash_and_conflicting_dates():
    old = pl.DataFrame([event()])
    row = event(payment_date=date(2023, 7, 25), payment_source="baostock:dividPayDate")
    ok, bad = match_payment_dates(old, pl.DataFrame([row]))
    assert ok.height == 1 and not bad
    changed = {**row, "cash_dividend": 0.31}
    ok, bad = match_payment_dates(old, pl.DataFrame([changed]))
    assert ok.is_empty() and bad[0]["reason"] == "economic_amount_conflict"
    old = pl.DataFrame([event(payment_date=date(2023, 7, 24))])
    ok, bad = match_payment_dates(old, pl.DataFrame([row]))
    assert ok.is_empty() and bad[0]["reason"] == "existing_payment_date_conflict"


def test_dateless_refresh_cannot_displace_a_row_with_payment_evidence():
    old = event(
        payment_date=date(2023, 7, 25),
        payment_source="baostock:dividPayDate",
        source="baostock",
        fetched_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    fresh = {
        **old,
        "payment_date": None,
        "payment_source": None,
        "source": "tdx_protocol",
        "fetched_at": datetime(2026, 2, 1, tzinfo=timezone.utc),
    }
    # A restated amount without evidence does not win on recency either: the
    # stored amount already agreed with the vendor that reported the date.
    for cash in (0.32, 0.33):
        df = pl.DataFrame([old, {**fresh, "cash_dividend": cash}])
        for out in [
            dedupe_by_primary_key(df, "corporate_actions"),
            dedupe_lazy_by_primary_key(df.lazy(), "corporate_actions").collect(),
        ]:
            assert out["payment_date"].item() == date(2023, 7, 25)
            assert out["cash_dividend"].item() == 0.32
            assert out["source"].item() == "baostock"
    # Without evidence on either side, the newer fetch still wins.
    bare = {**old, "payment_date": None, "payment_source": None}
    out = dedupe_by_primary_key(
        pl.DataFrame([bare, {**fresh, "cash_dividend": 0.33}]), "corporate_actions"
    )
    assert (out["cash_dividend"].item(), out["source"].item()) == (0.33, "tdx_protocol")


def test_repair_step_stages_only_matched_cash_and_reports_missing(tmp_path, monkeypatch):
    from cnequity.config import Config
    from cnequity.steps import payment_dates

    cfg = Config(data_root=tmp_path / "lake", sources={"baostock": True}, raw_archive_enabled=False)
    set_backfill(cfg, symbols=["600000.SH"], start=date(2023, 1, 1), end=date(2023, 12, 31))
    old = event(
        source="tdx_protocol",
        data_version="v1",
        fetched_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        allotment_ratio=None,
        allotment_price=None,
        split_factor=1.0,
    )
    old["cash_dividend"] = 0.3200000047683716
    monkeypatch.setattr(payment_dates, "load", lambda *a, **kw: pl.DataFrame([old]))
    fetched = event(payment_date=date(2023, 7, 25), payment_source="baostock:dividPayDate")
    monkeypatch.setattr(
        payment_dates,
        "fetch_corporate_actions_baostock",
        lambda *a, **kw: (pl.DataFrame([fetched]), []),
    )
    result = payment_dates.repair_payment_dates(cfg, date(2024, 1, 1), "test-run", {})
    assert result["rows_written"] == 1
    rows = pl.read_parquet(next(cfg.staging_root.rglob("*.parquet")))
    assert rows["cash_dividend"].item() == old["cash_dividend"]
    assert rows["payment_date"].item() == date(2023, 7, 25)
    assert rows["source"].item() == "baostock"


def test_repair_treats_a_payment_before_ex_date_as_unknown(tmp_path, monkeypatch):
    from cnequity.config import Config
    from cnequity.steps import payment_dates

    cfg = Config(data_root=tmp_path / "lake", sources={"baostock": True}, raw_archive_enabled=False)
    set_backfill(cfg, symbols=["600000.SH"], start=date(2023, 1, 1), end=date(2023, 12, 31))
    typo = event(
        payment_date=date(2022, 7, 21),
        payment_source="issuer_notice:template-typo:A",
        source="cninfo",
        data_version="v1",
        fetched_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        allotment_ratio=None,
        allotment_price=None,
        split_factor=1.0,
    )
    monkeypatch.setattr(payment_dates, "load", lambda *a, **kw: pl.DataFrame([typo]))
    monkeypatch.setattr(
        payment_dates,
        "fetch_corporate_actions_baostock",
        lambda *a, **kw: (pl.DataFrame([event()]).head(0), []),
    )
    result = payment_dates.repair_payment_dates(cfg, date(2024, 1, 1), "clear-run", {})
    assert result["rows_written"] == 1
    rows = pl.read_parquet(next(cfg.staging_root.rglob("*.parquet")))
    assert rows.select("payment_date", "payment_source", "source").row(0) == (None, None, "cninfo")
    report = (cfg.meta_root / "payment_date_repairs" / "clear-run.json").read_text()
    assert "template-typo" in report

    # When a source does report the real date, it replaces the typo instead.
    fetched = event(payment_date=date(2023, 7, 21), payment_source="baostock:dividPayDate")
    monkeypatch.setattr(
        payment_dates,
        "fetch_corporate_actions_baostock",
        lambda *a, **kw: (pl.DataFrame([fetched]), []),
    )
    cfg2 = Config(
        data_root=tmp_path / "lake2", sources={"baostock": True}, raw_archive_enabled=False
    )
    set_backfill(
        cfg2,
        symbols=backfill_of(cfg).symbol_list(),
        start=backfill_of(cfg).start,
        end=backfill_of(cfg).end,
    )
    assert (
        payment_dates.repair_payment_dates(cfg2, date(2024, 1, 1), "fix-run", {})["rows_written"]
        == 1
    )
    rows = pl.read_parquet(next(cfg2.staging_root.rglob("*.parquet")))
    assert rows["payment_date"].item() == date(2023, 7, 21)


def test_source_plan_retains_more_precision_than_rounded_cash_field():
    row = [""] * len(_FIELDS)
    values = {
        "code": "sz.000034",
        "dividOperateDate": "2022-05-05",
        "dividPayDate": "2022-05-05",
        "dividCashPsBeforeTax": "0.191842",
        "dividCashStock": "10派1.918424元（含税，扣税后1.726582或1.918424元）",
    }
    for key, value in values.items():
        row[_FIELDS.index(key)] = value
    parsed = _action_rows(
        "000034.SZ",
        row,
        dict(zip(_FIELDS, range(len(_FIELDS)), strict=True)),
        date(2022, 1, 1),
        date(2022, 12, 31),
    )
    assert parsed[0]["cash_dividend"] == 1.918424 / 10


def test_reviewed_notice_is_bound_to_pdf_cash_and_a_share_identity():
    import hashlib

    import pytest

    from cnequity.adapters.eastmoney.payment_notices import NOTICES, verified_notice_row

    raw = b"%PDF-test fixture"
    notice = {
        **NOTICES[("600094.SH", date(2024, 9, 11))],
        "sha256": hashlib.sha256(raw).hexdigest(),
    }
    old = {
        **event(),
        "symbol": "600094.SH",
        "ex_date": date(2024, 9, 11),
        "cash_dividend": 0.030000001192092896,
    }
    assert verified_notice_row(old, notice, raw)["payment_date"] == date(2024, 9, 11)
    for changed, payload in [
        ({**old, "symbol": "900940.SH"}, raw),
        ({**old, "cash_dividend": 0.028}, raw),
        (old, raw + b"changed"),
    ]:
        with pytest.raises(ValueError):
            verified_notice_row(changed, notice, payload)


def test_reviewed_bse_notice_accepts_only_exact_old_code_event():
    import hashlib

    import pytest

    from cnequity.adapters.eastmoney.payment_notices import NOTICES, verified_notice_row

    raw = b"%PDF-bse-issuer-notice"
    notice = {
        **NOTICES[("833874.BJ", date(2022, 5, 18))],
        "sha256": hashlib.sha256(raw).hexdigest(),
    }
    old = {
        **event(),
        "symbol": "833874.BJ",
        "ex_date": date(2022, 5, 18),
        "cash_dividend": 0.5,
    }
    repaired = verified_notice_row(old, notice, raw)
    assert repaired["payment_date"] == date(2022, 5, 18)
    assert repaired["payment_source"] == "issuer_notice:833874:2022-060:page1-2:A"
    for changed in (
        {**old, "symbol": "301192.SZ"},
        {**old, "cash_dividend": 0.05},
    ):
        with pytest.raises(ValueError):
            verified_notice_row(changed, notice, raw)


def test_reviewed_notice_requires_every_document_in_correction_chain():
    import hashlib

    import pytest

    from cnequity.adapters.eastmoney.payment_notices import NOTICES, verified_notice_row

    originals = [b"%PDF-original", b"%PDF-correction"]
    notice = {
        **NOTICES[("002358.SZ", date(2019, 7, 5))],
        "documents": [
            {**document, "sha256": hashlib.sha256(payload).hexdigest()}
            for document, payload in zip(
                NOTICES[("002358.SZ", date(2019, 7, 5))]["documents"],
                originals,
                strict=True,
            )
        ],
    }
    old = {
        **event(),
        "symbol": "002358.SZ",
        "ex_date": date(2019, 7, 5),
        "cash_dividend": 0.1,
    }
    repaired = verified_notice_row(old, notice, originals)
    assert repaired["payment_date"] == date(2019, 7, 5)
    assert "2019-042+2019-043" in repaired["payment_source"]
    with pytest.raises(ValueError, match="bytes changed"):
        verified_notice_row(old, notice, originals[:1])
    with pytest.raises(ValueError, match="bytes changed"):
        verified_notice_row(old, notice, [originals[0], originals[1] + b"changed"])


def test_reviewed_notice_rejects_a_payment_before_the_ex_date():
    import hashlib

    from cnequity.adapters.eastmoney.payment_notices import NOTICES, verified_notice_row

    raw = b"%PDF-record-date-payment"
    notice = {
        **NOTICES[("833874.BJ", date(2022, 5, 18))],
        "sha256": hashlib.sha256(raw).hexdigest(),
        "payment_date": date(2022, 5, 17),
    }
    old = {**event(), "symbol": "833874.BJ", "ex_date": date(2022, 5, 18), "cash_dividend": 0.5}
    with pytest.raises(ValueError, match="before the ex-date"):
        verified_notice_row(old, notice, raw)


def test_incomplete_duplicate_is_unknown_but_explicit_conflict_still_fails():
    import pytest

    positions = {k: i for i, k in enumerate(_FIELDS)}
    row = [""] * len(_FIELDS)
    for key, value in {
        "code": "sz.000560",
        "dividOperateDate": "2021-06-25",
        "dividPayDate": "2021-06-25",
        "dividCashStock": "10派0.13元（含税，扣税后0.117或0.13元）",
    }.items():
        row[positions[key]] = value
    args = ("000560.SZ", row, positions, date(2021, 1, 1), date(2021, 12, 31))
    assert _action_rows(*args) == []
    row[positions["dividCashPsBeforeTax"]] = "0.013"
    assert _action_rows(*args)[0]["cash_dividend"] == pytest.approx(0.013)
    for value in ["0", "0.02"]:
        row[positions["dividCashPsBeforeTax"]] = value
        with pytest.raises(ValueError, match="conflicts"):
            _action_rows(*args)


def test_combined_stock_and_cash_plan_restores_precision_without_tolerance_change():
    import pytest

    for plan in ["10转3.999615派2.439765元", "10送0转0派2.439765元", "10转0送0派2.439765元"]:
        values = {
            "code": "sz.002368",
            "dividOperateDate": "2020-07-09",
            "dividPayDate": "2020-07-09",
            "dividCashPsBeforeTax": "0.243976",
            "dividCashStock": plan,
        }
        rows = _action_rows(
            "002368.SZ",
            [values.get(k, "") for k in _FIELDS],
            {k: i for i, k in enumerate(_FIELDS)},
            date(2020, 1, 1),
            date(2020, 12, 31),
        )
        assert rows[0]["cash_dividend"] == pytest.approx(0.2439765)


def test_fund_gap_retained_without_stock_endpoint_or_archive_claim(tmp_path, monkeypatch):
    import json

    from cnequity.config import Config
    from cnequity.steps import payment_dates

    cfg = Config(data_root=tmp_path / "lake", sources={"baostock": True}, raw_archive_enabled=True)
    set_backfill(cfg, symbols=["161010.SZ"], start=date(2023, 1, 1), end=date(2023, 12, 31))
    old = event()
    old["symbol"] = "161010.SZ"
    monkeypatch.setattr(payment_dates, "load", lambda *a, **kw: pl.DataFrame([old]))
    result = payment_dates.repair_payment_dates(cfg, date(2024, 1, 1), "fund-test", {})
    report = json.loads((cfg.meta_root / "payment_date_repairs/fund-test.json").read_text())
    assert result["rows_written"] == 0
    assert report["unresolved"][0]["reason"] == "fund_payment_source_required"
    assert report["acquisition"]["network_requests"] == 0


def test_sse_fund_correction_does_not_fall_through_to_cninfo(tmp_path, monkeypatch):
    from cnequity.adapters.cninfo import fund_payment_notices
    from cnequity.adapters.exchange import fund_payment_notices as sse_fund_payment_notices
    from cnequity.config import Config
    from cnequity.steps import payment_dates

    cfg = Config(
        data_root=tmp_path / "lake",
        sources={"exchange": True, "cninfo": True},
        raw_archive_enabled=False,
    )
    set_backfill(cfg, symbols=["510720.SH"], start=date(2024, 1, 1), end=date(2024, 12, 31))
    old = event()
    old.update(symbol="510720.SH", ex_date=date(2024, 6, 13), payment_date=None)
    monkeypatch.setattr(payment_dates, "load", lambda *a, **kw: pl.DataFrame([old]))
    monkeypatch.setattr(
        sse_fund_payment_notices,
        "repair_sse_fund_payment_notices",
        lambda *a, **kw: (
            [],
            [
                {
                    "symbol": "510720.SH",
                    "ex_date": "2024-06-13",
                    "reason": "sse_fund_correction_chain_requires_review",
                }
            ],
        ),
    )

    def no_cninfo_fallback(config, run_id, pending, **kwargs):
        assert pending.is_empty()
        return [], []

    monkeypatch.setattr(fund_payment_notices, "repair_fund_payment_notices", no_cninfo_fallback)
    payment_dates.repair_payment_dates(cfg, date(2024, 12, 31), "correction-test", {})
    report = json.loads((cfg.meta_root / "payment_date_repairs/correction-test.json").read_text())
    assert report["matched_events"] == 0
    assert report["unresolved"][0]["reason"] == "sse_fund_correction_chain_requires_review"


def test_cninfo_notice_parser_requires_code_ex_date_pretax_cash_and_payment_date():
    from cnequity.adapters.cninfo.payment_notices import parse_payment_notice_text

    text = """
    股票代码：000538 股票简称：云南白药
    2024年特别分红权益分派实施公告
    向全体股东每 10 股派 12.130000 元人民币现金（含税；扣税后另计）。
    股权登记日为：2024 年 11 月 22 日；除权除息日为：2024 年 11 月 25 日。
    本公司此次委托中国结算深圳分公司代派的 A 股股东现金红利将于
    2024 年 11 月 25 日通过股东托管证券公司直接划入其资金账户。
    """
    assert parse_payment_notice_text(text) == {
        "code": "000538",
        "ex_date": date(2024, 11, 25),
        "payment_date": date(2024, 11, 25),
        "cash_dividend": 1.213,
    }


def test_cninfo_notice_parser_does_not_promote_incomplete_text():
    from cnequity.adapters.cninfo.payment_notices import parse_payment_notice_text

    assert (
        parse_payment_notice_text(
            "股票代码：000538 每10股派12.13元（含税）除权除息日为2024年11月25日"
        )
        is None
    )
