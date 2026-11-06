"""Parsing for the two publisher adapters behind the authority checks (#10)."""

from __future__ import annotations

from datetime import date

import pytest

from cnequity.adapters.exchange import st_lists
from cnequity.adapters.nbs import pmi_release

# --- NBS release -------------------------------------------------------------

# The sentence the NBS has phrased the same way for years, with the tag soup
# that surrounds it on the real page.
_RELEASE = """
<div><p>　7 月份，制造业采购经理指数（ <b>PMI</b> ）为 49.2% ，比上月下降 1.1 个百分点。</p>
<p>二、中国非制造业采购经理指数运行情况</p>
<p>7 月份，非制造业商务活动指数为 49.0% ，比上月下降 1.2 个百分点。</p></div>
"""


def test_pmi_is_parsed_through_the_markup():
    """Tags split the sentence, so whitespace has to be removed, not collapsed."""
    assert pmi_release.parse_pmi(_RELEASE) == 49.2


def test_the_services_index_is_not_mistaken_for_manufacturing():
    """`非制造业采购经理指数` contains the manufacturing phrase as a substring."""
    services_only = "<p>7月份，非制造业采购经理指数为49.0%。</p>"
    assert pmi_release.parse_pmi(services_only) is None


def test_a_reworded_release_parses_to_none_rather_than_a_guess():
    assert pmi_release.parse_pmi("<p>本月经济运行总体平稳。</p>") is None


def test_out_of_range_pmi_is_rejected():
    assert pmi_release.parse_pmi("<p>制造业采购经理指数（PMI）为101.0%</p>") is None


_INDEX = """
<a href="./202607/t20260731_1964253.html">2026年7月中国采购经理指数运行情况</a>
<a href="./202606/t20260630_1963000.html">2026年6月中国采购经理指数运行情况</a>
<a href="./202607/t20260715_1964000.html">2026年上半年国民经济运行情况</a>
"""


def test_latest_release_picks_the_newest_month():
    found = pmi_release.find_latest_release(_INDEX)
    assert found is not None
    obs, url = found
    assert obs == date(2026, 7, 31)
    assert url.endswith("202607/t20260731_1964253.html")
    assert url.startswith("https://")


def test_non_pmi_releases_are_ignored():
    only_gdp = '<a href="./202607/t20260715_1964000.html">2026年上半年国民经济运行情况</a>'
    assert pmi_release.find_latest_release(only_gdp) is None


# --- exchange names ----------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("ST海王", True),
        ("*ST美丽", True),
        ("*ST康佳A", True),
        ("ST 思科瑞", True),  # vendor padding
        ("*ST联翔\x00", True),  # TDX NUL padding
        ("公司ST", False),  # ST is a designation prefix, not a substring
        ("CST科技", False),
        ("平安银行", False),
        ("", False),
        (None, False),
    ],
)
def test_st_designation_is_read_from_the_short_name(name, expected):
    assert st_lists.is_st_name(name) is expected


def test_sse_list_is_parsed_from_the_tab_separated_download(monkeypatch):
    body = (
        "公司代码 \t公司简称 \t代码\t简称\t上市日期\t\n"
        "600000\t  浦发银行\t  600000\t  浦发银行\t  1999-11-10\t\n"
        "600053\t  *ST九鼎\t  600053\t  *ST九鼎\t  1996-10-25\t\n"
        "6000001\t  格式异常\t  6000001\t  格式异常\t  2020-01-01\t\n"
    ).encode("gbk")

    class _Resp:
        content = body

        def raise_for_status(self):
            return None

    monkeypatch.setattr(
        st_lists, "_client", lambda: type("C", (), {"get": lambda *a, **k: _Resp()})
    )
    names = st_lists.fetch_sse_names()
    assert names == {"600000.SH": "浦发银行", "600053.SH": "*ST九鼎"}
    assert {s for s, n in names.items() if st_lists.is_st_name(n)} == {"600053.SH"}


def test_sse_failure_degrades_to_empty(monkeypatch):
    def _boom(*_a, **_k):
        raise RuntimeError("connection reset")

    monkeypatch.setattr(st_lists, "_client", lambda: type("C", (), {"get": _boom}))
    assert st_lists.fetch_sse_names() == {}
    assert st_lists.fetch_szse_names() == {}


def test_one_exchange_down_does_not_discard_the_other(monkeypatch):
    monkeypatch.setattr(st_lists, "fetch_sse_names", lambda **_kw: {"600053.SH": "*ST九鼎"})
    monkeypatch.setattr(st_lists, "fetch_szse_names", lambda **_kw: {})
    assert st_lists.fetch_exchange_names() == {"600053.SH": "*ST九鼎"}


def test_status_fetch_reports_a_partial_exchange_snapshot(monkeypatch):
    monkeypatch.setattr(
        st_lists,
        "fetch_sse_names",
        lambda **_kw: {"600053.SH": "*ST九鼎"},
    )
    monkeypatch.setattr(st_lists, "fetch_szse_names", lambda **_kw: {})
    result = st_lists.fetch_exchange_names_with_status()
    assert result.names == {"600053.SH": "*ST九鼎"}
    assert result.failures == {"szse": "no usable rows"}


# --- Exchange daily quotes ---------------------------------------------------

from cnequity.adapters.exchange import daily_quotes  # noqa: E402

TRADE_DATE = date(2026, 8, 28)


class _Resp:
    def __init__(self, payload=None, content=b""):
        self._payload = payload
        self.content = content

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _sse(monkeypatch, payload):
    monkeypatch.setattr(
        daily_quotes,
        "_client",
        lambda: type("C", (), {"get": staticmethod(lambda *a, **k: _Resp(payload))}),
    )


def _sse_payload(rows, *, day=20260828, time=162906):
    return {"date": day, "time": time, "list": rows}


# code, name, open, high, low, last, volume, amount, prev_close — shared status/quote request.
_SSE_ROWS = [
    ["600000", "浦发银行", 9.01, 9.04, 8.95, 9.00, 58786810, 528817735, 8.98],
    ["688267", "中触媒", 18.23, 19.56, 18.23, 19.26, 2852200, 54524888, 18.40],
    ["900902", "市北B股", 0.5, 0.5, 0.5, 0.5, 1000, 500, 0.5],
]


def test_sse_quotes_parse_and_drop_b_shares(monkeypatch):
    _sse(monkeypatch, _sse_payload(_SSE_ROWS))
    out = daily_quotes.fetch_sse_daily_quotes(TRADE_DATE)
    assert out.get_column("symbol").to_list() == ["600000.SH", "688267.SH"]
    row = out.filter(out["symbol"] == "600000.SH").to_dicts()[0]
    assert (row["open"], row["high"], row["low"], row["close"]) == (9.01, 9.04, 8.95, 9.00)
    assert row["pre_close"] == 8.98
    # The endpoint already states shares and yuan; nothing is rescaled.
    assert row["volume"] == 58786810
    assert row["amount"] == 528817735


def test_sse_snapshot_for_another_session_is_not_relabelled(monkeypatch):
    """The endpoint serves one session. Asking for another must yield nothing."""
    _sse(monkeypatch, _sse_payload(_SSE_ROWS))
    assert daily_quotes.fetch_sse_daily_quotes(date(2026, 8, 27)).is_empty()


def test_sse_mid_session_snapshot_is_not_a_close(monkeypatch):
    """`last` is the running price before 15:00 and would fake drift every run."""
    _sse(monkeypatch, _sse_payload(_SSE_ROWS, time=133000))
    assert daily_quotes.fetch_sse_daily_quotes(TRADE_DATE).is_empty()


def test_sse_unreadable_date_is_silent(monkeypatch):
    _sse(monkeypatch, _sse_payload(_SSE_ROWS, day="not-a-date"))
    assert daily_quotes.fetch_sse_daily_quotes(TRADE_DATE).is_empty()


def _szse_workbook(rows):
    import io

    import pandas as pd

    buf = io.BytesIO()
    pd.DataFrame(rows).to_excel(buf, index=False)
    return buf.getvalue()


_SZSE_ROWS = [
    {
        "交易日期": "2026-08-28",
        "证券代码": "000001",
        "证券简称": "平安银行",
        "开盘": "11.54",
        "最高": "11.68",
        "最低": "11.48",
        "今收": "11.65",
        "前收": "11.60",
        "成交量(万股)": "8,385.19",
        "成交金额(万元)": "97,367.83",
    },
    {
        "交易日期": "2026-08-28",
        "证券代码": "200011",
        "证券简称": "深物业B",
        "开盘": "5.0",
        "最高": "5.0",
        "最低": "5.0",
        "今收": "5.0",
        "成交量(万股)": "1.00",
        "成交金额(万元)": "5.00",
    },
]


def test_szse_quotes_are_rescaled_from_wan_units(monkeypatch):
    content = _szse_workbook(_SZSE_ROWS)
    monkeypatch.setattr(
        daily_quotes,
        "_client",
        lambda: type("C", (), {"get": staticmethod(lambda *a, **k: _Resp(content=content))}),
    )
    out = daily_quotes.fetch_szse_daily_quotes(TRADE_DATE)
    # 200xxx is a B share and is not part of all-A.
    assert out.get_column("symbol").to_list() == ["000001.SZ"]
    row = out.to_dicts()[0]
    # 8,385.19 万股 -> shares; 97,367.83 万元 -> yuan. Separators and all.
    assert row["volume"] == pytest.approx(83_851_900.0)
    assert row["amount"] == pytest.approx(973_678_300.0)
    assert row["pre_close"] == 11.60


def test_szse_missing_columns_are_reported_not_guessed(monkeypatch):
    content = _szse_workbook([{"交易日期": "2026-08-28", "证券代码": "000001"}])
    monkeypatch.setattr(
        daily_quotes,
        "_client",
        lambda: type("C", (), {"get": staticmethod(lambda *a, **k: _Resp(content=content))}),
    )
    assert daily_quotes.fetch_szse_daily_quotes(TRADE_DATE).is_empty()


def test_combined_result_names_the_exchange_that_answered(monkeypatch):
    monkeypatch.setattr(
        daily_quotes, "fetch_sse_daily_quotes", lambda *a, **k: daily_quotes._EMPTY_QUOTES.clone()
    )
    monkeypatch.setattr(
        daily_quotes,
        "fetch_szse_daily_quotes",
        lambda *a, **k: daily_quotes._finish(
            [
                {
                    "symbol": "000001.SZ",
                    "trade_date": TRADE_DATE,
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": 1.0,
                    "amount": 1.0,
                }
            ]
        ),
    )
    result = daily_quotes.fetch_exchange_daily_quotes(TRADE_DATE)
    # A SZSE-only comparison must never be readable as covering the market.
    assert result.covered == frozenset({"szse"})
    assert "sse" in result.failures
    assert result.quotes.height == 1


def test_complete_quotes_cover_every_exchange_the_adapter_asks(monkeypatch):
    row = {
        "symbol": "600000.SH",
        "trade_date": TRADE_DATE,
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
        "volume": 1.0,
        "amount": 1.0,
    }
    for exchange in daily_quotes.EXCHANGES:
        monkeypatch.setattr(
            daily_quotes,
            f"fetch_{exchange}_daily_quotes",
            lambda *a, **k: daily_quotes._finish([row]),
        )
    result = daily_quotes.fetch_exchange_daily_quotes(TRADE_DATE)
    assert result.covered == frozenset(daily_quotes.EXCHANGES)
    assert result.missing == frozenset()
