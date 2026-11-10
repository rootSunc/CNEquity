"""Fund cash dates require fund-class evidence, not a stock endpoint."""

import json
from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from cnequity.adapters.cninfo import fund_payment_notices
from cnequity.adapters.cninfo.fund_payment_notices import parse_fund_payment_text
from cnequity.adapters.exchange import fund_payment_notices as sse_fund_payment_notices

NOTICE = """
博时中证红利低波动100交易型开放式指数证券投资基金分红公告
基金主代码 159307
本次分红方案（单位：元/10 份基金份额）0.1400
权益登记日 2024年9月18日
除息日 2024年9月19日
现金红利发放日 2024年9月23日
"""


def test_unique_fund_notice_extracts_cash_availability_after_ex_date():
    assert parse_fund_payment_text(NOTICE) == {
        "code": "159307",
        "cash_dividend": 0.014000000000000002,
        "record_date": date(2024, 9, 18),
        "ex_date": date(2024, 9, 19),
        "payment_date": date(2024, 9, 23),
    }
    assert parse_fund_payment_text(
        NOTICE.replace("本次分红方案", "本次基金分红方案")
    ) == parse_fund_payment_text(NOTICE)
    venue = NOTICE.replace("本次分红方案（单位：元", "本次分红方案（单位：人民币元")
    venue = venue.replace(
        "除息日 2024年9月19日", "除息日 2024年9月19日（场内）2024年9月18日（场外）"
    )
    assert parse_fund_payment_text(venue) == parse_fund_payment_text(NOTICE)


def test_fund_notice_rejects_ambiguous_class_amount_or_date():
    assert parse_fund_payment_text(NOTICE + "基金主代码 159308") is None
    assert parse_fund_payment_text(NOTICE + "本次分红方案（单位：元/10份基金份额）0.1500") is None
    assert parse_fund_payment_text(NOTICE + "A类基金份额") is None
    assert parse_fund_payment_text(NOTICE.replace("2024年9月23日", "2024年9月18日")) is None
    assert parse_fund_payment_text(NOTICE.replace("现金红利发放日", "红利再投资日")) is None


def test_multi_class_notice_uses_the_listed_code_column_only():
    multi = NOTICE.replace(
        "本次分红方案（单位：元/10 份基金份额）0.1400",
        """
下属分级基金的基金简称 南方金利定开债券 A 南方金利定开债券 C
下属分级基金的交易代码 160128 160129
本次下属分级基金分红方案（单位：元/10 份
基金份额）0.0600 0.0500
""",
    ).replace("基金主代码 159307", "基金主代码 160128")
    assert parse_fund_payment_text(multi)["cash_dividend"] == 0.006
    assert (
        parse_fund_payment_text(multi.replace("160128 160129", "160129 160128"))["cash_dividend"]
        == 0.005
    )
    assert parse_fund_payment_text(multi.replace("0.0600 0.0500", "0.0600")) is None


def test_listed_money_fund_uses_its_class_and_on_exchange_payment_date():
    notice = """
银华交易型货币市场基金 A 类和 B 类基金份额分红公告
基金主代码 511880
下属各类基金的交易代码 511880 003816 015557
本次下属各类基金分红方案（单位：元 /10 份基金份额） 15.521 17.949 -
权益登记日 2024年12月30日
除息日 2024年12月31日（场内）2024年12月30日（场外）
现金红利发放日 2025年1月6日（场内）2024年12月31日（场外）
"""
    assert parse_fund_payment_text(notice) == {
        "code": "511880",
        "cash_dividend": 1.5521,
        "record_date": date(2024, 12, 30),
        "ex_date": date(2024, 12, 31),
        "payment_date": date(2025, 1, 6),
    }
    assert parse_fund_payment_text(notice.replace("511880 003816 015557", "003816 511880 015557"))[
        "cash_dividend"
    ] == pytest.approx(1.7949)
    assert parse_fund_payment_text(notice.replace("15.521 17.949 -", "- 17.949 15.521")) is None


def test_listed_money_fund_uses_named_class_date_when_venue_label_is_absent():
    notice = """
银华交易型货币市场基金 A 类和 B 类基金份额分红公告
场内简称 银华日利
基金主代码 511880
下属各类基金的交易代码 511880 003816
本次下属各类基金分红方案（单位：元/10 份基金
份额） 17.17 19.63
权益登记日 2022年12月29日
除息日 2022年12月30日（银华日利）2022年12月29日（银华日利B）
现金红利发放日 2023年1月5日（银华日利）2022年12月30日（银华日利B）
"""
    assert parse_fund_payment_text(notice) == {
        "code": "511880",
        "cash_dividend": pytest.approx(1.717),
        "record_date": date(2022, 12, 29),
        "ex_date": date(2022, 12, 30),
        "payment_date": date(2023, 1, 5),
    }
    assert parse_fund_payment_text(notice.replace("场内简称 银华日利", "场内简称 别的基金")) is None


def test_missing_fund_directory_identity_is_event_scoped(monkeypatch):
    class Client:
        def __init__(self, **kwargs):
            pass

        def close(self):
            pass

    monkeypatch.setattr(fund_payment_notices.httpx, "Client", Client)
    monkeypatch.setattr(
        fund_payment_notices,
        "_get",
        lambda *args, **kwargs: SimpleNamespace(content=b'{"stockList":[]}'),
    )
    frame = pl.DataFrame({"symbol": ["510720.SH"], "ex_date": [date(2024, 5, 10)]})
    config = SimpleNamespace(sources={"cninfo": True})

    def never(*args, **kwargs):
        raise AssertionError("nothing is staged without a directory identity")

    assert fund_payment_notices.repair_fund_payment_notices(config, "run", frame, stage=never) == (
        [],
        [
            {
                "symbol": "510720.SH",
                "ex_date": "2024-05-10",
                "reason": "fund_directory_identity_missing",
            }
        ],
    )


def test_sse_query_is_code_and_historical_year_scoped(monkeypatch):
    calls = []

    class Client:
        pass

    class Archive:
        def archive(self, *args, **kwargs):
            return SimpleNamespace()

    def fake_get(client, url, *, config, params):
        calls.append(params)
        payload = {
            "result": [
                {"SECURITY_CODE": "510720", "URL": "/disclosure/fund/announcement/a.pdf"},
                {"SECURITY_CODE": "510721", "URL": "/disclosure/fund/announcement/b.pdf"},
            ],
            "pageHelp": {"pageCount": 1},
        }
        return SimpleNamespace(
            content=json.dumps(payload).encode(),
            request=SimpleNamespace(url="https://query.sse.com.cn/commonQuery.do"),
        )

    monkeypatch.setattr(sse_fund_payment_notices, "_get", fake_get)
    notices, _ = sse_fund_payment_notices._query_symbol(
        Client(), None, "510720", 2024, Archive(), "run", "scope", {}
    )
    assert len(notices) == 1
    assert all(call["SECURITY_CODE"] == "510720" for call in calls)
    assert all(call["END_DATE"] == "2024-12-31" for call in calls)
    assert {call["TITLE"] for call in calls} == {"分红", "收益分配", "利润分配"}
