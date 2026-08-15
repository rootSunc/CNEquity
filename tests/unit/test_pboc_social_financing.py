"""PBOC 社会融资规模增量 adapter (issue #10).

Covers the two defects that live parsing turned up: a second, percentage-unit
table stacked in the same sheet, and overlapping year workbooks carrying
different vintages of the same month.
"""

from __future__ import annotations

import io
from datetime import date

import pandas as pd
import pytest

from cn_market_lake.adapters.pboc import _tables
from cn_market_lake.adapters.pboc import social_financing as sf

# Layout of a real workbook: bilingual title, an explicit unit line, a header
# block, then one row per month. Months the PBOC has not published yet are
# present but blank, and the sheet ends in prose notes.
_HEADER = [
    ["社会融资规模增量统计表", None],
    ["Aggregate Financing to the Real Economy (Flow)", None],
    ["单位：亿元人民币", None],
    ["Unit: 100 million Yuan", None],
    ["月份\nMonth", "社会融资规模增量"],
]
_NOTES = [["注： 1.社会融资规模增量是指……", None]]


def _workbook(rows: list[list], *, extra: list[list] | None = None) -> bytes:
    buf = io.BytesIO()
    pd.DataFrame(_HEADER + rows + _NOTES + (extra or [])).to_excel(buf, header=False, index=False)
    return buf.getvalue()


def test_months_parse_to_month_end():
    content = _workbook([["2026.01", 72185], ["2026.02", 23837]])
    assert _tables.parse_month_column(content, 1) == [
        {"obs_date": date(2026, 1, 31), "value": 72185.0},
        {"obs_date": date(2026, 2, 28), "value": 23837.0},
    ]


def test_october_is_not_read_as_january():
    """`.xlsx` stores the month as a float, so 2026.10 arrives as 2026.1."""
    content = _workbook([[2026.01, 72185], [2026.10, 8680]])
    assert _tables.parse_month_column(content, 1) == [
        {"obs_date": date(2026, 1, 31), "value": 72185.0},
        {"obs_date": date(2026, 10, 31), "value": 8680.0},
    ]


def test_unpublished_months_are_skipped_not_zeroed():
    content = _workbook([["2026.06", 33645], ["2026.07", None], ["2026.08", None]])
    assert _tables.parse_month_column(content, 1) == [
        {"obs_date": date(2026, 6, 30), "value": 33645.0}
    ]


def test_a_percentage_table_in_the_same_sheet_is_excluded():
    """The 2019 workbook stacks 表2 …增量占比数据 (单位：%) under the 亿元 table.

    Both have a month column, so reading every month-shaped row pulled a column
    of literal 100s into the series. Collection follows the `单位` declaration.
    """
    content = _workbook(
        [["2017.01", 37720]],
        extra=[
            ["表2：2017年以来各月完善后的社会融资规模增量占比数据", None],
            ["单位：%", None],
            ["月份\nMonth", "社会融资规模当月增量"],
            ["2017.01", 100],
            ["2017.02", 100],
        ],
    )
    parsed = _tables.parse_month_column(content, 1)
    assert parsed == [{"obs_date": date(2017, 1, 31), "value": 37720.0}]
    assert all(row["value"] != 100.0 for row in parsed)


def test_rows_before_any_unit_declaration_are_ignored():
    buf = io.BytesIO()
    pd.DataFrame([["2026.01", 999]]).to_excel(buf, header=False, index=False)
    assert _tables.parse_month_column(buf.getvalue(), 1) == []


# --- year discovery and vintage precedence -----------------------------------


_INDEX = """
<a href='/diaochatongjisi/116219/116319/2026ntjsj/index.html'>2026年统计数据</a>
<a href="/diaochatongjisi/116219/116319/3959050/index.html">2020年统计数据</a>
"""


def test_year_sections_accept_either_quote_style():
    """The site mixes single- and double-quoted attributes on one page."""
    assert _tables.year_sections(_INDEX) == {
        2026: f"{_tables.BASE}/diaochatongjisi/116219/116319/2026ntjsj/index.html",
        2020: f"{_tables.BASE}/diaochatongjisi/116219/116319/3959050/index.html",
    }


def _patch_pipeline(monkeypatch, per_year: dict[int, list[list]]):
    monkeypatch.setattr(
        _tables,
        "year_sections",
        lambda *a, **k: {y: f"{_tables.BASE}/{y}/index.html" for y in per_year},
    )
    monkeypatch.setattr(
        _tables, "workbook_url", lambda section, **kw: section.replace("index.html", "wb")
    )

    class _Resp:
        def __init__(self, content):
            self.content = content

    def _fake_get(url, **kwargs):
        year = int(url.rstrip("/wb").rsplit("/", 1)[-1])
        return _Resp(_workbook(per_year[year]))

    monkeypatch.setattr(_tables, "get_bytes", lambda url: _fake_get(url).content)


def test_a_later_workbook_supersedes_an_earlier_vintage(monkeypatch):
    """Workbooks overlap and the newer publication wins.

    The 2019 file restates 2017 under the 完善后 caliber (2017-01 = 37720) while
    the 2017 file still carries the original 36970.49. Descending year order is
    load-bearing, not incidental.
    """
    _patch_pipeline(
        monkeypatch,
        {
            2017: [["2017.01", 36970.493675329046]],
            2019: [["2017.01", 37720], ["2019.01", 46791]],
        },
    )
    rows = sf.fetch_social_financing(start_year=2015)
    by_month = {r["obs_date"]: r["value"] for r in rows}
    assert by_month[date(2017, 1, 31)] == 37720.0
    assert len(rows) == len(by_month), "each month must appear once"


def test_one_unreachable_year_does_not_lose_the_rest(monkeypatch):
    _patch_pipeline(monkeypatch, {2025: [["2025.01", 1.0]], 2026: [["2026.01", 2.0]]})

    def _flaky(section, **kw):
        if "2025" in section:
            raise RuntimeError("timeout")
        return section.replace("index.html", "wb")

    monkeypatch.setattr(_tables, "workbook_url", _flaky)
    rows = sf.fetch_social_financing(start_year=2015)
    assert [r["obs_date"] for r in rows] == [date(2026, 1, 31)]


def test_unreachable_index_degrades_to_empty(monkeypatch):
    def _boom(*_a, **_k):
        raise RuntimeError("network down")

    monkeypatch.setattr(_tables, "year_sections", _boom)
    assert sf.fetch_social_financing() == []


@pytest.mark.parametrize("start_year", [2015, 2026])
def test_start_year_bounds_the_sweep(monkeypatch, start_year):
    _patch_pipeline(monkeypatch, {2015: [["2015.01", 1.0]], 2026: [["2026.01", 2.0]]})
    years = {r["obs_date"].year for r in sf.fetch_social_financing(start_year=start_year)}
    assert min(years) >= start_year
