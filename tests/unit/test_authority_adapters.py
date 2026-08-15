"""Parsing for the two publisher adapters behind the authority checks (#10)."""

from __future__ import annotations

from datetime import date

import pytest

from cn_market_lake.adapters.exchange import st_lists
from cn_market_lake.adapters.nbs import pmi_release

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
