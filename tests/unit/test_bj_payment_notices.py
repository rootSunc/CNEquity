"""Strict issuer-notice extraction for BJ/NEEQ payment repair."""

from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from cnequity.adapters.eastmoney.bse_payment_notices import (
    _matches_reviewed_issuer_code,
    _notice_query_codes,
    _parse_bj_payment_text,
    repair_bj_payment_notices,
)
from cnequity.domain.bse_codes import current_to_legacy


def test_reviewed_old_code_mapping_does_not_guess_other_issuers():
    assert len(current_to_legacy()) == 248
    assert _matches_reviewed_issuer_code("920454", "833454")
    assert _matches_reviewed_issuer_code("920926", "870726")
    assert _matches_reviewed_issuer_code("920252", "873152")
    assert _matches_reviewed_issuer_code("920726", "831726")
    assert _matches_reviewed_issuer_code("920455", "833455")
    assert _matches_reviewed_issuer_code("920454", "920454")
    assert not _matches_reviewed_issuer_code("920454", "833455")
    assert not _matches_reviewed_issuer_code("920999", "833455")


def test_pre_bse_search_uses_only_reviewed_old_issuer_code():
    assert _notice_query_codes("920014", 2019) == ("920014", "834014")
    assert _notice_query_codes("920014", 2022) == ("920014",)
    assert _notice_query_codes("920999", 2019) == ("920999",)


def test_bj_cash_repair_blocks_same_day_stock_terms_before_network():
    cash = {"symbol": "920799.BJ", "ex_date": date(2022, 6, 1), "action_type": "cash_dividend"}
    stock = {**cash, "action_type": "bonus"}
    matched, issues = repair_bj_payment_notices(
        SimpleNamespace(sources={"eastmoney": True}),
        "test-run",
        pl.DataFrame([cash]),
        stage=lambda *a, **k: pytest.fail("blocked before staging anything"),
        all_actions=pl.DataFrame([cash, stock]),
    )
    assert matched == []
    assert issues == [
        {
            "symbol": "920799.BJ",
            "ex_date": "2022-06-01",
            "reason": "bj_stock_terms_require_reconciliation",
        }
    ]


_NOTICE = """
证券代码：833874 证券简称：泰祥股份 公告编号：2022-060
十堰市泰祥实业股份有限公司 2021年年度权益分派实施公告
以公司现有总股本为基数，向全体股东每10股派5.000000元人民币现金。
2、扣税说明
本次权益分派权益登记日为：2022年5月17日
除权除息日为：2022年5月18日
截止2022年5月17日下午北京证券交易所收市后登记在册的股东享有权益。
本公司此次委托中国结算北京分公司代派的现金红利将于2022年5月18日
通过股东托管证券公司直接划入其资金账户。
"""


def test_bj_notice_requires_exact_gross_cash_and_all_three_dates():
    assert _parse_bj_payment_text(_NOTICE) == {
        "code": "833874",
        "record_date": date(2022, 5, 17),
        "ex_date": date(2022, 5, 18),
        "payment_date": date(2022, 5, 18),
        "cash_dividend": 0.5,
    }
    assert _parse_bj_payment_text(_NOTICE.replace("扣税说明", "其他说明")) is None
    assert _parse_bj_payment_text(_NOTICE.replace("现金红利将于", "现金红利预计于")) is None
    assert (
        _parse_bj_payment_text(_NOTICE.replace("2022年5月17日\n除权", "2022年5月19日\n除权"))
        is None
    )
    assert _parse_bj_payment_text(_NOTICE + "向全体股东每10股派6元人民币现金") is None


def test_bj_notice_rejects_other_market_and_ambiguous_security_code():
    assert _parse_bj_payment_text(_NOTICE.replace("北京证券交易所", "上海证券交易所")) is None
    assert _parse_bj_payment_text(_NOTICE + "证券代码：301192") is None
