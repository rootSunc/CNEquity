"""Isolated issuer books and after-close membership dates."""

from datetime import date
from io import BytesIO

import pandas as pd
import pytest

from cnequity.adapters.exchange.index_notices import (
    parse_star50_adjustment,
    parse_star50_effective_date,
    parse_star50_launch_date,
    parse_star50_opening,
)


def html(text):
    return ("<html><script>于1999年1月1日</script><p>" + text + "</p></html>").encode()


def opening(*, duplicate=False, count=50, index="000688", wrong_code=False):
    codes = [f"{688001 + i:06d}" for i in range(count)]
    if duplicate:
        codes[-1] = codes[0]
    if wrong_code:
        codes[-1] = "600001"
    rows = {
        "指数代码\nIndex Code": [index] * count,
        "指数名称\nIndex Name": ["科创50"] * count,
        "证券代码\nSecurities Code": codes,
    }
    out = BytesIO()
    pd.DataFrame(rows).to_excel(out, index=False)
    return out.getvalue()


def test_original_opening_is_a_whole_50_member_book():
    book = parse_star50_opening(opening())
    assert len(book) == 50 and "688001.SH" in book


@pytest.mark.parametrize(
    "kwargs", [{"duplicate": True}, {"count": 49}, {"index": "000300"}, {"wrong_code": True}]
)
def test_no_guessed_or_incomplete_opening(kwargs):
    with pytest.raises(ValueError):
        parse_star50_opening(opening(**kwargs))


def test_live_launch_does_not_use_backtested_base_date():
    text = "指数基日为2019年12月31日。将于2020年7月22日收盘后发布上证科创板50成份指数历史行情，7月23日正式发布实时行情。"
    assert parse_star50_launch_date(html(text)) == date(2020, 7, 23)
    with pytest.raises(ValueError):
        parse_star50_launch_date(html(text.replace("7月23日", "7月22日")))
    with pytest.raises(ValueError):
        parse_star50_launch_date(html("指数基日为2019年12月31日。"))


@pytest.mark.parametrize(
    "declared,expected",
    [
        ("于2020年9月14日", date(2020, 9, 14)),
        ("于2021年3月15日起", date(2021, 3, 15)),
        ("自2022年3月11日收市后", date(2022, 3, 14)),
        ("于2022年6月10日收市后", date(2022, 6, 13)),
        ("于2022年9月9日收盘后", date(2022, 9, 13)),
        ("于2024年9月13日收市后", date(2024, 9, 18)),
    ],
)
def test_after_close_dates_advance_over_weekends_and_exchange_holidays(declared, expected):
    assert (
        parse_star50_effective_date(html("决定调整科创50指数样本，" + declared + "生效。"))
        == expected
    )


@pytest.mark.parametrize(
    "text",
    [
        "决定调整科创50指数样本，日期另行通知。",
        "于2020年9月14日生效，于2020年9月15日生效。",
        "于2022年6月11日收市后生效。",
        "于2015年6月12日收市后生效。",
        "于2028年6月9日收市后生效。",
    ],
)
def test_unknown_or_inconsistent_date_is_not_inferred(text):
    with pytest.raises(ValueError):
        parse_star50_effective_date(html(text))


TABLE = """决定于2020年9月14日调整科创50指数样本。
科创50指数样本调整名单：
调出名单 调入名单 证券代码 证券简称 证券代码 证券简称
688011 新光光电 688100 威胜信息
688068 热景生物 688158 优刻得-W
科创50指数备选名单：
排序 证券代码 证券简称 1 688037 芯源微
上证180指数样本调整名单：
调出名单 调入名单 证券代码 证券名称 证券代码 证券名称
600001 旧 600002 新
"""


def test_direction_scope_and_reserve_members_are_not_confused():
    change = parse_star50_adjustment(html(TABLE))
    assert change.effective_date == date(2020, 9, 14)
    assert change.index_symbol == "000688.SH"
    assert change.additions == {"688100.SH", "688158.SH"}
    assert change.removals == {"688011.SH", "688068.SH"}
    assert "688037.SH" not in change.additions


@pytest.mark.parametrize(
    "text",
    [
        TABLE.replace("调出名单 调入名单", "调入名单 调出名单"),
        TABLE.replace("688068", "688011"),
        TABLE.replace("688100", "688011"),
        TABLE.replace("688100", "600001"),
        TABLE.replace("688158", "无代码"),
        "决定于2020年9月14日调整科创50指数样本。附件：科创50指数样本调整名单",
    ],
)
def test_unreadable_attachment_or_invalid_table_requires_evidence(text):
    with pytest.raises(ValueError):
        parse_star50_adjustment(html(text))


def word_xml(text):
    from xml.sax.saxutils import escape
    from zipfile import ZipFile

    output = BytesIO()
    ns = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
    with ZipFile(output, "w") as archive:
        archive.writestr(
            "word/document.xml",
            f'<w:document xmlns:w="{ns}"><w:body><w:p><w:r><w:t>{escape(text)}</w:t></w:r></w:p></w:body></w:document>',
        )
    return output.getvalue()


def test_word_xml_wps_attachment_uses_declared_table_not_reserve_list():
    from cnequity.adapters.exchange.index_notices import parse_star50_attachment

    change = parse_star50_attachment(word_xml(TABLE), kind="wps", effective_date=date(2020, 9, 14))
    assert change.additions == {"688100.SH", "688158.SH"}
    assert change.removals == {"688011.SH", "688068.SH"}
    with pytest.raises(ValueError, match="supported Word"):
        parse_star50_attachment(
            b"legacy opaque WPS binary", kind="wps", effective_date=date(2020, 9, 14)
        )


def test_workbook_scopes_target_index_and_ignores_reserve_sheet():
    from cnequity.adapters.exchange.index_notices import parse_star50_attachment

    incoming = pd.DataFrame(
        {
            "指数代码": ["000688", "000682"],
            "指数简称": ["科创50", "科创信息"],
            "证券代码": ["688100", "688101"],
        }
    )
    outgoing = pd.DataFrame(
        {
            "指数代码": ["000688", "000682"],
            "指数简称": ["科创50", "科创信息"],
            "证券代码": ["688011", "688012"],
        }
    )
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        incoming.to_excel(writer, index=False, sheet_name="调入")
        outgoing.to_excel(writer, index=False, sheet_name="调出")
        pd.DataFrame({"证券代码": ["688037"]}).to_excel(writer, index=False, sheet_name="备选名单")
    change = parse_star50_attachment(
        output.getvalue(), kind="xlsx", effective_date=date(2020, 9, 14)
    )
    assert change.additions == {"688100.SH"} and change.removals == {"688011.SH"}
    with pytest.raises(ValueError, match="incoming/outgoing"):
        parse_star50_attachment(opening(), kind="xlsx", effective_date=date(2020, 9, 14))


def test_pdf_attachment_with_repeated_caption_has_only_one_real_table(monkeypatch):
    from types import SimpleNamespace

    from cnequity.adapters.exchange import index_notices

    text = "附件：科创50指数样本调整名单 " + TABLE
    monkeypatch.setattr(
        index_notices,
        "PdfReader",
        lambda _: SimpleNamespace(pages=[SimpleNamespace(extract_text=lambda: text)]),
    )
    change = index_notices.parse_star50_attachment(
        b"isolated PDF", kind="pdf", effective_date=date(2020, 9, 14)
    )
    assert change.additions == {"688100.SH", "688158.SH"}


def test_no_change_requires_explicit_publisher_statement():
    from cnequity.adapters.exchange.index_notices import parse_star50_no_change_date

    assert parse_star50_no_change_date(
        html("于2024年12月13日收市后生效，科创50指数样本无变动。")
    ) == date(2024, 12, 16)
    with pytest.raises(ValueError, match="explicitly"):
        parse_star50_no_change_date(html("于2024年12月13日收市后生效，附件中未列出科创50指数。"))


def csi300_html(*, count=300, duplicate=False, code=None, day="2005年7月1日", columns="代码 名称"):
    codes = [f"{i + 1:06d}" for i in range(100)] + [f"{600001 + i:06d}" for i in range(count - 100)]
    if duplicate:
        codes[-1] = codes[0]
    if code is not None:
        codes[-1] = code
    rows = "".join(f"<tr><td>{c}</td><td>名称</td></tr>" for c in codes)
    return (
        f"<html><h1>2015-09-11 归档日期 999999</h1><table><tr><td>沪深 300 指数样本股名单（{day}起生效）</td></tr><tr><td>{columns}</td></tr>{rows}</table><script>600999</script></html>"
    ).encode()


def test_csi300_original_table_uses_body_date_and_both_markets():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_opening

    effective, members = parse_csi300_opening(csi300_html())
    assert effective == date(2005, 7, 1) and len(members) == 300
    assert "000001.SZ" in members and "600001.SH" in members and "999999.SZ" not in members


@pytest.mark.parametrize(
    "kwargs",
    [
        {"count": 299},
        {"count": 301},
        {"duplicate": True},
        {"code": "900001"},
        {"day": "2015-09-11"},
        {"columns": "名称"},
    ],
)
def test_csi300_incomplete_ambiguous_or_wrong_identity_is_rejected(kwargs):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_opening

    with pytest.raises(ValueError):
        parse_csi300_opening(csi300_html(**kwargs))


def test_csi300_different_tables_cannot_be_merged_or_chosen():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_opening

    with pytest.raises(ValueError, match="unique"):
        parse_csi300_opening(csi300_html() + csi300_html(day="2006年1月1日"))
    with pytest.raises(ValueError, match="unique"):
        parse_csi300_opening(b"<h1>CSI300 2005-07-01</h1>")


def csi300_changes(
    *,
    column="证券代码",
    name_column="指数简称",
    missing=False,
    index_name="沪深300",
    index_code="000300",
    duplicate=False,
    code="600001",
    overlap=False,
    unequal=False,
    ambiguous=False,
):
    buffer = BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        for name, member in [("调入", code), ("调出", code if overlap else "000001")]:
            if missing and name == "调出":
                continue
            rows = [
                dict(指数代码=index_code, 指数简称=index_name, **{column: member}),
                dict(指数代码="000016", 指数简称="上证50", **{column: "600999"}),
            ]
            if duplicate or (unequal and name == "调入"):
                rows.insert(0, {**rows[0], column: member if duplicate else "600002"})
            sheet = pd.DataFrame(rows).rename(columns={"指数简称": name_column})
            if ambiguous:
                sheet["股票代码" if column == "证券代码" else "证券代码"] = sheet[column]
            sheet.to_excel(writer, sheet_name=name, index=False)
        pd.DataFrame({"指数代码": ["000300"], "证券代码": ["600888"]}).to_excel(
            writer, sheet_name="备选名单", index=False
        )
    return buffer.getvalue()


@pytest.mark.parametrize("column", ["证券代码", "股票代码"])
@pytest.mark.parametrize("name_column", ["指数简称", "指数名称"])
def test_csi300_change_sheets_keep_direction_both_markets_and_exclude_reserve(column, name_column):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_change_workbook

    event = parse_csi300_change_workbook(
        csi300_changes(column=column, name_column=name_column), effective_date=date(2015, 6, 15)
    )
    assert event.index_symbol == "000300.SH" and event.effective_date == date(2015, 6, 15)
    assert event.additions == {"600001.SH"} and event.removals == {"000001.SZ"}


@pytest.mark.parametrize(
    "kwargs",
    [
        {"missing": True},
        {"index_code": "000016"},
        {"index_name": "沪深300行业"},
        {"duplicate": True},
        {"code": "900001"},
        {"code": "1"},
        {"overlap": True},
        {"unequal": True},
        {"ambiguous": True},
    ],
)
def test_csi300_changes_reject_incomplete_identity_ambiguity_and_contradictions(kwargs):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_change_workbook

    with pytest.raises(ValueError):
        parse_csi300_change_workbook(csi300_changes(**kwargs), effective_date=date(2015, 6, 15))


CONDITIONAL = """关于对沪深300指数样本股进行临时调整的公告 时间：2006-04-14
鉴于齐鲁石化（600002）、石油大明（000406）、扬子石化（000866）和中原油气（000956）将终止上市，
自股票终止上市生效日起，将其从指数中调出，同时按照终止上市的时间先后顺序依次调入G燃气（000793）、航天信息（600271）、G张裕（000869）、上电股份（600627）。"""
AUTHORITY = """中国石化齐鲁石油化工股份有限公司 中国石化扬子石油化工股份有限公司
中国石化中原油气高新股份有限公司 中国石化胜利油田大明（集团）股份有限公司
深圳证券交易所批准了扬子石化、中原油气和石油大明流通股股票自2006年4月21日起终止上市交易。
上海证券交易所批准齐鲁石化流通股股票自2006年4月24日起终止上市交易。"""


def conditional_changes(monkeypatch, *, notice=CONDITIONAL, authority=AUTHORITY):
    from types import SimpleNamespace

    from cnequity.adapters.exchange import csi300_notices

    monkeypatch.setattr(
        csi300_notices,
        "PdfReader",
        lambda stream: SimpleNamespace(pages=[SimpleNamespace(extract_text=lambda: authority)]),
    )
    return csi300_notices.parse_csi300_conditional_delistings(
        html(notice), authority_pdf=b"isolated issuer original"
    )


def test_conditional_dates_group_ties_and_preserve_original_replacement_order(monkeypatch):
    events = conditional_changes(monkeypatch)
    assert [event.effective_date for event in events] == [date(2006, 4, 21), date(2006, 4, 24)]
    assert events[0].removals == {"000406.SZ", "000866.SZ", "000956.SZ"}
    assert events[0].additions == {"000793.SZ", "600271.SH", "000869.SZ"}
    assert events[1].removals == {"600002.SH"} and events[1].additions == {"600627.SH"}
    assert all(event.index_symbol == "000300.SH" for event in events)


def test_conditional_all_same_day_produces_one_whole_cohort(monkeypatch):
    events = conditional_changes(monkeypatch, authority=AUTHORITY.replace("4月24日", "4月21日"))
    assert len(events) == 1 and len(events[0].removals) == len(events[0].additions) == 4


@pytest.mark.parametrize(
    "authority",
    [
        AUTHORITY.replace("4月21日", "4月14日"),
        AUTHORITY.replace("2006年", "2007年"),
        AUTHORITY.replace("上海证券交易所批准齐鲁石化", "拟申请齐鲁石化"),
        AUTHORITY.replace("中国石化齐鲁石油化工股份有限公司", "其他公司"),
        AUTHORITY + AUTHORITY,
    ],
)
def test_conditional_authority_rejects_wrong_dates_identity_and_ambiguity(monkeypatch, authority):
    with pytest.raises(ValueError):
        conditional_changes(monkeypatch, authority=authority)


@pytest.mark.parametrize(
    "notice",
    [
        CONDITIONAL.replace("600002", "600001"),
        CONDITIONAL.replace("2006-04-14", "2006-04-21"),
        CONDITIONAL.replace("000793", "000406"),
        CONDITIONAL.replace("先后顺序依次", ""),
        CONDITIONAL.replace("600627", "000793"),
    ],
)
def test_conditional_notice_requires_identity_order_and_distinct_replacements(monkeypatch, notice):
    with pytest.raises(ValueError):
        conditional_changes(monkeypatch, notice=notice)


def csi300_table(*, incoming_first=False, reserve=True, other=True):
    directions = "调入样本 调出样本" if incoming_first else "调出名单 调入名单"
    codes = "600001 新 000001 旧" if incoming_first else "000001 旧 600001 新"
    scope = f"沪深300指数样本股调整名单： {directions} 证券代码 证券简称 证券代码 证券简称 {codes}"
    if other:
        scope += " 中证100指数样本股调整名单： 调出名单 调入名单 股票代码 股票名称 股票代码 股票名称 600888 旧 600889 新"
    if reserve:
        scope += " 沪深300指数备选名单：排序 证券代码 名称 1 600999 备选"
    return html(scope)


@pytest.mark.parametrize("incoming_first", [True, False])
def test_csi300_table_uses_declared_direction_and_excludes_other_indices_reserve(incoming_first):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_change_table

    additions, removals = parse_csi300_change_table(csi300_table(incoming_first=incoming_first))
    assert additions == {"600001.SH"} and removals == {"000001.SZ"}


def test_csi300_old_anonymous_table_requires_single_index_single_table():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_change_table

    body = "<h1>关于调整沪深300指数样本股的公告</h1><table><tr><td>调入样本 调出样本 证券代码 证券简称 证券代码 证券简称 600001 新 000001 旧</td></tr></table>"
    assert parse_csi300_change_table(body.encode()) == ({"600001.SH"}, {"000001.SZ"})
    with pytest.raises(ValueError):
        parse_csi300_change_table((body + body).encode())
    with pytest.raises(ValueError):
        parse_csi300_change_table(body.replace("沪深300", "中证100").encode())


@pytest.mark.parametrize(
    "old,new",
    [
        ("调出名单 调入名单", "调出名单 调出名单"),
        ("000001 旧 600001 新", "000001 旧"),
        ("600001 新", "900001 新"),
        ("600001 新", "000001 新"),
        ("000001 旧 600001 新", "000001 旧 600001 新 000001 旧 600002 新"),
        ("沪深300指数样本股调整名单", "沪深300行业指数样本股调整名单"),
    ],
)
def test_csi300_table_rejects_direction_identity_and_incomplete_changes(old, new):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_change_table

    with pytest.raises(ValueError):
        parse_csi300_change_table(csi300_table().replace(old.encode(), new.encode()))


def test_csi300_pdf_table_keeps_target_directions_across_other_index_pages(monkeypatch):
    from types import SimpleNamespace

    from cnequity.adapters.exchange import csi300_notices

    pages = [
        "附件：部分指数样本调整名单 沪深300指数样本调整名单： 调出名单 调入名单 证券代码 证券名称 证券代码 证券名称 000001 旧 600001 新 中证500指数样本调整名单：",
        "调出名单 调入名单 证券代码 证券名称 证券代码 证券名称 600999 其他 600888 其他 沪深300指数备选名单： 600777 备选",
    ]
    monkeypatch.setattr(
        csi300_notices,
        "PdfReader",
        lambda stream: SimpleNamespace(
            pages=[SimpleNamespace(extract_text=lambda text=text: text) for text in pages]
        ),
    )
    assert csi300_notices.parse_csi300_change_pdf(b"isolated source") == (
        {"600001.SH"},
        {"000001.SZ"},
    )


def test_csi300_image_only_pdf_cannot_be_treated_as_no_change(monkeypatch):
    from types import SimpleNamespace

    from cnequity.adapters.exchange import csi300_notices

    monkeypatch.setattr(
        csi300_notices,
        "PdfReader",
        lambda stream: SimpleNamespace(pages=[SimpleNamespace(extract_text=lambda: None)]),
    )
    with pytest.raises(ValueError):
        csi300_notices.parse_csi300_change_pdf(b"isolated scanned source")


def inline_book(
    *,
    wide=False,
    reverse=False,
    duplicate=False,
    code="000300",
    name="沪深300",
    placeholder="-",
    symbol="600001",
    ambiguous=False,
    unequal=False,
    anonymous=False,
):
    rows = [
        ["指数代码", "指数简称", "调出", None, "调入", None],
        [None, None, "证券代码", "证券简称", "证券代码", "证券简称"],
        ["000016", "上证50", "600888", "其他", "600889", "其他"],
        [code, name, "000001", "旧", symbol, "新"],
    ]
    if wide:
        rows = [
            ["指数代码", "指数简称", "调出", None, None, None, "调入", None, None, None],
            [
                None,
                None,
                "证券代码",
                "证券简称",
                "证券代码",
                "证券简称",
                "证券代码",
                "证券简称",
                "证券代码",
                "证券简称",
            ],
            [code, name, "000001", "旧", placeholder, placeholder, symbol, "新", "-", "-"],
        ]
    if reverse:
        for row in rows:
            middle = 2 + (len(row) - 2) // 2
            row[2:] = row[middle:] + row[2:middle]
    if anonymous:
        rows.append([None, None, "000002", "旧", "600002", "新"])
    if duplicate:
        rows.append(list(rows[-1]))
    if unequal:
        rows.append([code, name, "000002", "旧"] + [None] * (len(rows[0]) - 4))
    buffer = BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        pd.DataFrame(rows).to_excel(writer, sheet_name="调整名单", header=False, index=False)
        if ambiguous:
            pd.DataFrame(rows).to_excel(writer, sheet_name="第二名单", header=False, index=False)
    return buffer.getvalue()


@pytest.mark.parametrize("wide,reverse", [(False, False), (True, False), (True, True)])
def test_inline_directions_and_paired_placeholders_are_source_declared(wide, reverse):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_inline_workbook

    assert parse_csi300_inline_workbook(inline_book(wide=wide, reverse=reverse)) == (
        {"600001.SH"},
        {"000001.SZ"},
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"duplicate": True},
        {"code": "000016"},
        {"name": "沪深300行业"},
        {"symbol": "000001"},
        {"symbol": "900001"},
        {"wide": True, "placeholder": "0"},
        {"ambiguous": True},
        {"unequal": True},
        {"anonymous": True},
    ],
)
def test_inline_rejects_wrong_index_ambiguity_duplicates_and_unequal_changes(kwargs):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_inline_workbook

    with pytest.raises(ValueError):
        parse_csi300_inline_workbook(inline_book(**kwargs))


def test_stock_termination_date_is_distinct_from_notice_and_decision_date():
    from cnequity.adapters.exchange.csi300_notices import parse_stock_delisting_authority

    text = "关于海通证券股份有限公司股票终止上市的公告 2025-02-26 证券代码：600837，证券简称：海通证券。决定日期为2025年2月25日。本所决定自2025年3月4日起终止其股票在本所上市交易。"
    assert parse_stock_delisting_authority(html(text), code="600837") == date(2025, 3, 4)


@pytest.mark.parametrize(
    "effect",
    [
        "本所决定自2008年3月18日起依法终止你公司股票在本所上市交易。",
        "本公司股票将于2008年3月18日起终止在上海证券交易所上市交易。",
    ],
)
def test_legacy_issuer_termination_uses_explicit_legal_effect(effect):
    from cnequity.adapters.exchange.csi300_notices import parse_stock_delisting_authority

    text = (
        "证券代码：600786 股票终止上市的公告。2008年3月13日接到2008年3月12日签发的通知。"
        + effect
        + "余股收购期间为2008年3月21日至2008年5月20日。公告日期2008年3月14日。"
    )
    assert parse_stock_delisting_authority(html(text), code="600786") == date(2008, 3, 18)


def test_legacy_issuer_termination_rejects_conflicting_legal_effects():
    from cnequity.adapters.exchange.csi300_notices import parse_stock_delisting_authority

    text = (
        "证券代码：600786 股票终止上市。"
        "本所决定自2008年3月18日起依法终止你公司股票在本所上市交易。"
        "本公司股票将于2008年3月24日起终止在上海证券交易所上市交易。"
    )
    with pytest.raises(ValueError, match="unique explicit"):
        parse_stock_delisting_authority(html(text), code="600786")


def test_explicit_issuer_termination_field_is_distinct_from_receipt_and_signature():
    from cnequity.adapters.exchange.csi300_notices import parse_stock_delisting_authority

    text = (
        "关于股票终止上市的公告。2011年8月18日收到交易所通知。"
        "股票代码：600631；终止上市日期：2011年8月23日。二〇一一年八月十八日。"
    )
    assert parse_stock_delisting_authority(html(text), code="600631") == date(2011, 8, 23)


@pytest.mark.parametrize(
    "field",
    [
        "预计终止上市日期为2011年8月23日。",
        "预计终止上市日期：2011年8月23日。",
        "终止上市日期：2011年8月23日或另行通知。",
        "终止上市日期：2011年8月23日。本所决定自2011年8月24日起终止上市。",
    ],
)
def test_termination_field_rejects_tentative_unclosed_or_conflicting_effect(field):
    from cnequity.adapters.exchange.csi300_notices import parse_stock_delisting_authority

    with pytest.raises(ValueError):
        parse_stock_delisting_authority(
            html("股票代码：600631 股票终止上市的公告。" + field), code="600631"
        )


@pytest.mark.parametrize(
    "text",
    [
        "证券代码：600838 股票终止上市 本所决定自2025年3月4日起终止其股票在本所上市交易。",
        "证券代码：6008370 股票终止上市 本所决定自2025年3月4日起终止其股票在本所上市交易。",
        "证券代码：600837 股票终止上市 决定日期为2025年2月25日。",
        "证券代码：600837 股票终止上市 公司可能于2025年3月4日终止上市。",
        "证券代码：600837 股票终止上市 本所决定自2025年3月4日起终止其股票在本所上市交易。公司股票将于2025年3月5日终止上市。",
    ],
)
def test_stock_termination_authority_requires_identity_explicit_effect_and_unique_day(text):
    from cnequity.adapters.exchange.csi300_notices import parse_stock_delisting_authority

    with pytest.raises(ValueError):
        parse_stock_delisting_authority(html(text), code="600837")


def test_issuer_pdf_can_prove_explicit_termination_without_day_suffix_qi(monkeypatch):
    from types import SimpleNamespace

    from cnequity.adapters.exchange import csi300_notices

    text = "证券代码：600837 关于公司A股股票终止上市的公告 决定日期为2025年2月25日。公司A股股票将于2025年3月4日终止上市。"
    monkeypatch.setattr(
        csi300_notices,
        "PdfReader",
        lambda stream: SimpleNamespace(pages=[SimpleNamespace(extract_text=lambda: text)]),
    )
    assert csi300_notices.parse_stock_delisting_authority(
        b"isolated issuer report", code="600837", kind="pdf"
    ) == date(2025, 3, 4)


@pytest.mark.parametrize(
    "year,rule", [(2010, "2010年1月"), (2011, "2011年"), (2012, "2012年1月"), (2013, "2013年1月")]
)
def test_january_first_session_binds_independent_exchange_closure(year, rule):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_first_session_date

    notice = html(f"决定于{rule}第一个交易日调整沪深300、中证100指数样本。")
    authority = html(
        f"关于上海证券交易所{year}年全年休市安排的通知 元旦：1月1日至1月3日休市，1月4日起照常开市。春节：另行安排。"
    )
    assert parse_csi300_first_session_date(notice, holiday_notice=authority) == date(year, 1, 4)


@pytest.mark.parametrize(
    "notice,authority",
    [
        (
            "决定于1月第一个交易日调整沪深300。",
            "关于2013年全年休市安排的通知 元旦：1月1日至1月3日休市，1月4日起照常开市。春节",
        ),
        (
            "决定于2013年1月第一个交易日调整沪深300。",
            "关于2012年全年休市安排的通知 元旦：1月1日至1月3日休市，1月4日起照常开市。春节",
        ),
        (
            "决定于2013年1月第一个交易日调整沪深300。",
            "关于2013年全年休市安排的通知 元旦：1月2日至1月3日休市，1月4日起照常开市。春节",
        ),
        (
            "决定于2013年1月第一个交易日调整沪深300。",
            "关于2013年全年休市安排的通知 元旦：1月1日至1月2日休市，1月4日起照常开市。春节",
        ),
        (
            "决定于2013年1月第一个交易日调整沪深300。",
            "关于2013年全年休市安排的通知 元旦：1月1日至1月4日休市，1月5日起照常开市。春节",
        ),
        (
            "决定于2013年1月第一个交易日调整沪深300。",
            "关于2013年全年休市安排的通知 元旦：1月1日至1月3日休市，1月4日起照常开市。元旦：1月1日至1月6日休市，1月7日起照常开市。春节",
        ),
        (
            "决定于2013年1月第一个交易日调整沪深300。自2012年第一个交易日调整沪深300。",
            "关于2013年全年休市安排的通知 元旦：1月1日至1月3日休市，1月4日起照常开市。春节",
        ),
    ],
)
def test_first_session_does_not_guess_year_missing_closure_gaps_or_conflicting_authority(
    notice, authority
):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_first_session_date

    with pytest.raises(ValueError):
        parse_csi300_first_session_date(html(notice), holiday_notice=html(authority))


@pytest.mark.parametrize(
    "columns,valid",
    [
        (("股票代码", "股票名称"), True),
        (("股票代码", "证券名称"), False),
        (("证券代码", "股票名称"), False),
        (("基金代码", "基金名称"), False),
    ],
)
def test_inline_old_stock_headers_require_a_declared_code_and_name_pair(columns, valid):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_inline_workbook

    frame = pd.read_excel(BytesIO(inline_book()), header=None, dtype=str)
    frame.iloc[1, 2:4] = columns
    frame.iloc[1, 4:6] = columns
    buffer = BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        frame.to_excel(writer, sheet_name="Sheet1", index=False, header=False)
        pd.DataFrame().to_excel(writer, sheet_name="Sheet2", index=False, header=False)
    if valid:
        assert parse_csi300_inline_workbook(buffer.getvalue()) == ({"600001.SH"}, {"000001.SZ"})
    else:
        with pytest.raises(ValueError, match="security columns"):
            parse_csi300_inline_workbook(buffer.getvalue())


def mixed_table(*, direction="调入", other_direction="调出", span=2, continuation=True):
    return (
        '<table><tr><td rowspan="2">指数名称</td>'
        f'<td colspan="2">{direction}</td><td colspan="2">{other_direction}</td>'
        "<tr><td>股票代码</td><td>股票名称</td><td>股票代码</td><td>股票名称</td>"
        f'<tr><td rowspan="{span}">沪深300指数</td><td>601600</td><td>中国铝业</td><td>600205</td><td>旧甲</td>'
        + (
            "<tr><td>600501</td><td>航天晨光</td><td>600296</td><td>旧乙</td>"
            if continuation
            else ""
        )
        + "<tr><td>中证100指数</td><td>600001</td><td>其它</td><td>000001</td><td>其它旧</td></table>"
    ).encode()


def test_mixed_index_table_carries_only_explicit_rowspan_and_excludes_other_indices():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_mixed_change_table

    assert parse_csi300_mixed_change_table(mixed_table()) == (
        {"601600.SH", "600501.SH"},
        {"600205.SH", "600296.SH"},
    )


@pytest.mark.parametrize("direction,other", [("调出", "调入"), ("删除", "纳入")])
def test_mixed_table_source_direction_controls_sets(direction, other):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_mixed_change_table

    assert parse_csi300_mixed_change_table(
        mixed_table(direction=direction, other_direction=other)
    ) == ({"600205.SH", "600296.SH"}, {"601600.SH", "600501.SH"})


@pytest.mark.parametrize(
    "payload",
    [
        mixed_table(span=1),
        mixed_table(span=3),
        mixed_table(continuation=False),
        mixed_table(direction="调出"),
        mixed_table().replace(b"600501", b"601600"),
        mixed_table().replace(b"600296", b"601600"),
        mixed_table().replace(b"600296", b"900296"),
        mixed_table().replace(b'rowspan="2"', b'rowspan="0"'),
        mixed_table() + mixed_table(),
        mixed_table().replace("沪深300指数".encode(), "沪深300行业".encode()),
        mixed_table().replace("股票代码".encode(), "股票名称".encode()),
    ],
)
def test_mixed_table_rejects_anonymous_continuations_wrong_spans_identity_and_ambiguity(payload):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_mixed_change_table

    with pytest.raises(ValueError):
        parse_csi300_mixed_change_table(payload)


def test_month_day_binds_original_signed_year_instead_of_url_or_catalogue_date():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_signed_effective_date

    assert parse_csi300_signed_effective_date(
        html("决定自1月23日起，对沪深300指数进行调整。中证指数有限公司2007年1月10日")
    ) == date(2007, 1, 23)


@pytest.mark.parametrize(
    "text",
    [
        "决定自1月23日起，对沪深300指数进行调整。",
        "决定自1月23日起，对沪深300指数进行调整。中证指数有限公司2007年12月10日",
        "决定自1月23日起和自1月24日起，对沪深300指数进行调整。中证指数有限公司2007年1月10日",
        "决定自1月23日起，对沪深300指数进行调整。中证指数有限公司2007年1月10日 中证指数有限公司2008年1月10日",
        "决定自1月23日起，对中证100指数进行调整。中证指数有限公司2007年1月10日",
    ],
)
def test_signed_dates_require_original_identity_unique_signature_and_no_cross_year_guess(text):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_signed_effective_date

    with pytest.raises(ValueError):
        parse_csi300_signed_effective_date(html(text))


def test_mixed_table_does_not_silently_drop_blank_index_security_rows():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_mixed_change_table

    payload = mixed_table().replace("<td>中证100指数</td>".encode(), b"<td></td>")
    with pytest.raises(ValueError, match="anonymous"):
        parse_csi300_mixed_change_table(payload)


def test_signed_month_day_cannot_override_a_conflicting_full_adjustment_date():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_signed_effective_date

    with pytest.raises(ValueError, match="override"):
        parse_csi300_signed_effective_date(
            html(
                "自2007年1月24日调整，自1月23日起，对沪深300指数进行调整。中证指数有限公司2007年1月10日"
            )
        )


def test_january_first_session_can_bridge_an_explicit_exchange_weekend_closure():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_first_session_date

    notice = html("决定于2009年1月第一个交易日调整沪深300指数。")
    authority = html(
        "关于2009年全年休市安排的通知 元旦：1月1日（星期四）—1月3日（星期六）为节假日休市，1月5日(星期一)起照常开市。1月4日（星期日）为周末休市。春节：另行安排。"
    )
    assert parse_csi300_first_session_date(notice, holiday_notice=authority) == date(2009, 1, 5)


@pytest.mark.parametrize(
    "weekend",
    [
        "",
        "1月4日（星期日）为周末调休。",
        "1月4日（星期六）为周末休市。",
        "1月3日（星期六）为周末休市。",
    ],
)
def test_january_gap_requires_the_exact_day_and_consistent_explicit_weekend_closure(weekend):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_first_session_date

    notice = html("决定于2009年1月第一个交易日调整沪深300指数。")
    authority = html(
        "关于2009年全年休市安排的通知 元旦：1月1日至1月3日休市，1月5日起照常开市。"
        + weekend
        + "春节：另行安排。"
    )
    with pytest.raises(ValueError):
        parse_csi300_first_session_date(notice, holiday_notice=authority)


@pytest.mark.parametrize(
    "signature", ["二OO六年六月十二日", "二〇〇六年六月十二日", "二零零六年六月十二日"]
)
def test_signed_effective_date_reads_original_chinese_signature_without_catalogue_year(signature):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_signed_effective_date

    assert parse_csi300_signed_effective_date(
        html("决定于7月3日调整沪深300指数。中证指数有限公司" + signature)
    ) == date(2006, 7, 3)


@pytest.mark.parametrize(
    "signature", ["二OO六年一一月十二日", "二OO六年六月三十二日", "二OO六年十三月十二日"]
)
def test_chinese_signature_rejects_noncanonical_or_impossible_date(signature):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_signed_effective_date

    with pytest.raises(ValueError):
        parse_csi300_signed_effective_date(
            html("决定于7月3日调整沪深300指数。中证指数有限公司" + signature)
        )


def old_single_notice(*, count=1, second=False, other=False):
    table = (
        '<table><tr><td colspan="2">调出样本</td><td colspan="2">调入样本</td></tr>'
        "<tr><td>股票代码</td><td>股票名称</td><td>证券代码</td><td>证券简称</td></tr>"
        "<tr><td>600001</td><td>旧甲</td><td>000001</td><td>新乙</td></tr></table>"
    )
    return (
        "<table><tr><td>装饰</td></tr></table>"
        f"<p>沪深300指数将于2005年7月1日调整样本股。本次共调整{count}只样本股，调整名单见下表。</p>"
        + table
        + (table if second else "")
        + ("中证100指数也调整。" if other else "")
    ).encode()


def test_old_single_index_rule_binds_one_directed_table_among_layout_tables():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_change_table

    assert parse_csi300_change_table(old_single_notice()) == ({"000001.SZ"}, {"600001.SH"})


@pytest.mark.parametrize("kwargs", [{"count": 2}, {"second": True}, {"other": True}])
def test_old_single_index_table_requires_declared_count_unique_table_and_scope(kwargs):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_change_table

    with pytest.raises(ValueError):
        parse_csi300_change_table(old_single_notice(**kwargs))


FACTBOOK_TERMINATIONS = """2009年暂停上市的公司
股票代码 股票简称 暂停上市公告刊登日 暂停上市实施起始日
600001 其它 2009-1-1 2009-1-2
2009年终止上市的公司
股票代码 股票简称 终止上市公告刊登日 终止上市实施起始日
600357 承德钒钛 2009-12-26 2009-12-29
600001 邯郸钢铁 2009-12-26 2009-12-29
2009年恢复上市的公司
股票代码 股票简称 恢复上市公告刊登日 上市日期
600001 其它 2009-1-3 2009-1-4
"""


def test_factbook_termination_uses_requested_year_scope_and_implementation_column():
    from cnequity.adapters.exchange.index_notices import (
        parse_csi300_mixed_change_table,
        parse_delisting_factbook_text,
    )

    for code in ("600001", "600357"):
        assert parse_delisting_factbook_text(FACTBOOK_TERMINATIONS, code=code, year=2009) == date(
            2009, 12, 29
        )
    assert parse_csi300_mixed_change_table(
        mixed_table().replace("指数名称".encode(), "指数简称".encode())
    ) == ({"601600.SH", "600501.SH"}, {"600205.SH", "600296.SH"})


@pytest.mark.parametrize(
    "text,code,year",
    [
        (FACTBOOK_TERMINATIONS, "600002", 2009),
        (FACTBOOK_TERMINATIONS, "600001", 2008),
        (FACTBOOK_TERMINATIONS * 2, "600001", 2009),
        (FACTBOOK_TERMINATIONS.replace("终止上市实施起始日", "决定日期"), "600001", 2009),
        (
            FACTBOOK_TERMINATIONS.replace(
                "600001 邯郸钢铁 2009-12-26 2009-12-29", "600001 邯郸钢铁 2009-12-29 2009-12-26"
            ),
            "600001",
            2009,
        ),
        (
            FACTBOOK_TERMINATIONS.replace(
                "600001 邯郸钢铁 2009-12-26 2009-12-29", "600001 邯郸钢铁 2009-12-26"
            ),
            "600001",
            2009,
        ),
        (
            FACTBOOK_TERMINATIONS.replace(
                "600001 邯郸钢铁 2009-12-26 2009-12-29", "600001 邯郸钢铁 2009-12-26 2010-1-4"
            ),
            "600001",
            2009,
        ),
    ],
)
def test_factbook_dates_reject_wrong_security_year_scope_columns_and_chronology(text, code, year):
    from cnequity.adapters.exchange.csi300_notices import parse_delisting_factbook_text

    with pytest.raises(ValueError):
        parse_delisting_factbook_text(text, code=code, year=year)


@pytest.mark.parametrize(
    "outgoing,incoming",
    [("剔除样本名单", "新进样本名单"), ("调出名单", "新进样本名单"), ("剔除样本名单", "调入名单")],
)
def test_old_directed_sample_synonyms_preserve_direction_and_exclude_reserves(outgoing, incoming):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_change_table

    payload = csi300_table().replace(
        "调出名单 调入名单".encode(), (outgoing + " " + incoming).encode()
    )
    assert parse_csi300_change_table(payload) == ({"600001.SH"}, {"000001.SZ"})


@pytest.mark.parametrize("labels", ["调出名单 剔除样本名单", "新进样本名单 调入名单"])
def test_synonymous_same_direction_columns_remain_invalid(labels):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_change_table

    with pytest.raises(ValueError):
        parse_csi300_change_table(
            csi300_table().replace("调出名单 调入名单".encode(), labels.encode())
        )


def test_headed_regular_table_cannot_omit_a_declared_change_pair():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_change_table

    assert parse_csi300_change_table("其中沪深300指数更换1只股票。".encode() + csi300_table()) == (
        {"600001.SH"},
        {"000001.SZ"},
    )
    with pytest.raises(ValueError, match="declared change count"):
        parse_csi300_change_table("其中沪深300指数更换2只股票。".encode() + csi300_table())


def name_first_mixed_table():
    import re

    text = mixed_table().decode().replace("股票代码</td><td>股票名称", "股票名称</td><td>股票代码")
    return re.sub(r"<td>(\d{6})</td><td>([^<]+)</td>", r"<td>\2</td><td>\1</td>", text).encode()


def test_mixed_table_uses_declared_name_before_code_columns():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_mixed_change_table

    assert parse_csi300_mixed_change_table(name_first_mixed_table()) == (
        {"601600.SH", "600501.SH"},
        {"600205.SH", "600296.SH"},
    )


def test_mixed_table_never_repairs_values_that_contradict_declared_column_order():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_mixed_change_table

    bad = name_first_mixed_table().replace(
        "<td>旧甲</td><td>600205</td>".encode(),
        "<td>600205</td><td>旧甲</td>".encode(),
    )
    with pytest.raises(ValueError, match="security identity"):
        parse_csi300_mixed_change_table(bad)


def directory_response(row=None, **changes):
    import json

    stock = {
        "COMPANY_CODE": "600472",
        "A_STOCK_CODE": "600472",
        "STOCK_TYPE": "1",
        "STATE_CODE": "3",
        "FULL_NAME": "sample issuer",
        "COMPANY_ABBR": "sample",
        "LIST_DATE": "20050509",
        "DELIST_DATE": "20071226",
    }
    stock.update(row or {})
    payload = {
        "sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
        "type": "inParams",
        "actionErrors": [],
        "fieldErrors": {},
        "result": [stock],
        "pageHelp": {"total": 1, "pageCount": 1, "pageNo": 1, "data": [stock]},
    }
    payload.update(changes)
    return json.dumps(payload).encode()


def test_official_termination_directory_reads_explicit_date_and_identity():
    from cnequity.adapters.exchange.csi300_notices import parse_sse_delisting_directory

    assert parse_sse_delisting_directory(directory_response(), code="600472") == date(2007, 12, 26)
    with pytest.raises(ValueError):
        parse_sse_delisting_directory(directory_response(), code="000001")


@pytest.mark.parametrize(
    "row",
    [
        {"COMPANY_CODE": "600001"},
        {"A_STOCK_CODE": "600001"},
        {"STOCK_TYPE": "2"},
        {"STATE_CODE": "5"},
        {"FULL_NAME": ""},
        {"DELIST_DATE": "2007-12-26"},
        {"DELIST_DATE": "20070230"},
        {"DELIST_DATE": "20050101"},
        {"LIST_DATE": None},
    ],
)
def test_official_termination_directory_rejects_wrong_identity_status_or_dates(row):
    from cnequity.adapters.exchange.csi300_notices import parse_sse_delisting_directory

    with pytest.raises(ValueError):
        parse_sse_delisting_directory(directory_response(row), code="600472")


@pytest.mark.parametrize(
    "changes",
    [
        {"sqlId": "other query"},
        {"actionErrors": ["query failed"]},
        {"fieldErrors": {"STOCK_CODE": "invalid"}},
        {"result": []},
        {"pageHelp": {"total": 2, "pageCount": 1, "pageNo": 1}},
        {"pageHelp": {"total": 1, "pageCount": 1, "pageNo": 2}},
        {"pageHelp": {"total": 1, "pageCount": 1, "pageNo": 1, "data": []}},
    ],
)
def test_official_termination_directory_requires_unique_complete_query(changes):
    from cnequity.adapters.exchange.csi300_notices import parse_sse_delisting_directory

    with pytest.raises(ValueError):
        parse_sse_delisting_directory(directory_response(**changes), code="600472")


def test_official_directory_never_selects_the_first_of_multiple_stock_rows():
    import json

    from cnequity.adapters.exchange.csi300_notices import parse_sse_delisting_directory

    payload = json.loads(directory_response())
    payload["result"].append(
        {**payload["result"][0], "COMPANY_CODE": "600786", "A_STOCK_CODE": "600786"}
    )
    payload["pageHelp"]["total"] = 2
    with pytest.raises(ValueError):
        parse_sse_delisting_directory(json.dumps(payload).encode(), code="600472")


def january_calendar_wire(*, year=2007, change=None):
    import json
    from datetime import date, timedelta

    rows = [
        [(date(year, 1, 1) + timedelta(days=i)).isoformat(), "1" if i == 3 else "0"]
        for i in range(31)
    ]
    if change is not None:
        change(rows)
    parts = [
        "frame",
        "header",
        "length",
        "success",
        "query_trade_dates",
        "id",
        "1",
        "2000",
        json.dumps({"record": rows}),
        f"{year}-01-01",
        f"{year}-01-31",
        "calendar_date, is_trading_day",
        "footer",
    ]
    return "\x01".join(parts).encode()


def first_session_calendar_notice():
    return "沪深300指数和中证100指数将于2007年1月第一个交易日调整样本股".encode()


def test_first_session_can_use_complete_explicit_vendor_calendar():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_first_session_calendar_date

    assert parse_csi300_first_session_calendar_date(
        first_session_calendar_notice(), calendar_wire=january_calendar_wire()
    ) == date(2007, 1, 4)


@pytest.mark.parametrize(
    "change",
    [
        lambda rows: rows.pop(0),
        lambda rows: rows.append(rows[-1]),
        lambda rows: rows.__setitem__(1, rows[0]),
        lambda rows: rows.reverse(),
        lambda rows: rows[0].__setitem__(1, True),
        lambda rows: rows[0].__setitem__(1, "unknown"),
        lambda rows: [row.__setitem__(1, "0") for row in rows],
    ],
)
def test_first_session_vendor_calendar_rejects_gaps_duplicates_and_unknown_days(change):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_first_session_calendar_date

    with pytest.raises(ValueError):
        parse_csi300_first_session_calendar_date(
            first_session_calendar_notice(), calendar_wire=january_calendar_wire(change=change)
        )


@pytest.mark.parametrize(
    "old,new",
    [
        ("success", "failed"),
        ("query_trade_dates", "query_history_k_data_plus"),
        ("\x011\x012000\x01", "\x012\x012000\x01"),
        ("calendar_date, is_trading_day", "calendar_date, volume"),
        ("2007-01-31", "2007-01-30"),
    ],
)
def test_first_session_vendor_calendar_requires_exact_query_identity(old, new):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_first_session_calendar_date

    with pytest.raises(ValueError):
        parse_csi300_first_session_calendar_date(
            first_session_calendar_notice(),
            calendar_wire=january_calendar_wire().replace(old.encode(), new.encode()),
        )


def test_first_session_vendor_calendar_cannot_supply_a_missing_or_different_rule():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_first_session_calendar_date

    for notice in [
        b"no explicit index rule",
        first_session_calendar_notice().replace(b"2007", b"2008"),
    ]:
        with pytest.raises(ValueError):
            parse_csi300_first_session_calendar_date(notice, calendar_wire=january_calendar_wire())


def test_first_session_vendor_calendar_does_not_override_an_explicit_adjustment_date():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_first_session_calendar_date

    notice = first_session_calendar_notice() + "，自2007年1月5日起调整。".encode()
    with pytest.raises(ValueError, match="cannot override"):
        parse_csi300_first_session_calendar_date(notice, calendar_wire=january_calendar_wire())


@pytest.mark.parametrize("year,month", [(2008, 1), (2008, 7), (2009, 7)])
def test_first_session_calendar_accepts_original_index_after_date_rule(year, month):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_first_session_calendar_date

    notice = f"中证指数有限公司决定于{year}年{month}月第一个交易日调整沪深300指数、中证100指数样本股。".encode()
    wire = january_calendar_wire(year=year).replace(
        f"{year}-01-".encode(), f"{year}-{month:02d}-".encode()
    )
    assert parse_csi300_first_session_calendar_date(notice, calendar_wire=wire) == date(
        year, month, 4
    )


@pytest.mark.parametrize(
    "notice",
    [
        "中证指数有限公司决定于2008年2月第一个交易日调整沪深300指数",
        "中证指数有限公司决定于2008年7月第一个交易日调整中证100指数",
        "中证指数有限公司决定于2008年1月第一个交易日调整沪深300指数，同时决定于2008年7月第一个交易日调整沪深300指数",
    ],
)
def test_first_session_calendar_rejects_other_month_index_or_multiple_periods(notice):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_first_session_calendar_date

    with pytest.raises(ValueError):
        parse_csi300_first_session_calendar_date(
            notice.encode(), calendar_wire=january_calendar_wire(year=2008)
        )


def test_first_session_calendar_cannot_use_january_rows_for_july_rule():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_first_session_calendar_date

    notice = "中证指数有限公司决定于2008年7月第一个交易日调整沪深300指数".encode()
    with pytest.raises(ValueError):
        parse_csi300_first_session_calendar_date(
            notice, calendar_wire=january_calendar_wire(year=2008)
        )


@pytest.mark.parametrize("reverse_direction", [False, True])
def test_explicit_mixed_pair_reader_preserves_groups_when_subheader_order_disagrees(
    reverse_direction,
):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_mixed_pair_change_table

    payload = name_first_mixed_table().replace(
        "<td>旧甲</td><td>600205</td>".encode(), "<td>600205</td><td>旧甲</td>".encode()
    )
    if reverse_direction:
        payload = payload.replace("调入".encode(), "删除".encode()).replace(
            "调出".encode(), "纳入".encode()
        )
    additions, removals = parse_csi300_mixed_pair_change_table(payload)
    assert (additions, removals) == (
        ({"600205.SH", "600296.SH"}, {"601600.SH", "600501.SH"})
        if reverse_direction
        else ({"601600.SH", "600501.SH"}, {"600205.SH", "600296.SH"})
    )


@pytest.mark.parametrize(
    "replacement",
    [
        "<td>600205</td><td>600206</td>",
        "<td>旧甲</td><td>旧乙</td>",
        "<td>600205</td><td>-</td>",
        "<td>600205</td><td>123</td>",
        "<td>900205</td><td>旧甲</td>",
    ],
)
def test_explicit_mixed_pair_reader_requires_one_code_and_textual_name(replacement):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_mixed_pair_change_table

    payload = mixed_table().replace("<td>600205</td><td>旧甲</td>".encode(), replacement.encode())
    with pytest.raises(ValueError):
        parse_csi300_mixed_pair_change_table(payload)


@pytest.mark.parametrize(
    "payload",
    [
        mixed_table(direction="调整"),
        mixed_table(span=3),
        mixed_table().replace("沪深300指数".encode(), b""),
        mixed_table().replace("股票名称".encode(), "未知列".encode()),
    ],
)
def test_explicit_mixed_pair_reader_never_infers_direction_index_spans_or_headers(payload):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_mixed_pair_change_table

    with pytest.raises(ValueError):
        parse_csi300_mixed_pair_change_table(payload)


def coded_change_table(removal="601299", addition="300003"):
    return (
        '<table><tr><td rowspan="2">指数代码</td><td rowspan="2">指数简称</td>'
        '<td colspan="2">调出</td><td colspan="2">调入</td></tr>'
        "<tr><td>股票代码</td><td>股票名称</td><td>股票代码</td><td>股票名称</td></tr>"
        f"<tr><td>000300</td><td>沪深300</td><td>{removal}</td><td>旧甲</td>"
        f"<td>{addition}</td><td>新甲</td></tr>"
        "<tr><td>000904</td><td>中证200</td><td>600886</td><td>其它旧</td>"
        "<td>000001</td><td>其它新</td></tr></table>"
    ).encode()


def test_coded_tables_select_original_trigger_without_combining_distinct_events():
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_coded_change_table

    payload = coded_change_table() + coded_change_table("600832", "000738")
    assert parse_csi300_coded_change_table(payload, removed_code="601299") == (
        {"300003.SZ"},
        {"601299.SH"},
    )
    assert parse_csi300_coded_change_table(payload, removed_code="600832") == (
        {"000738.SZ"},
        {"600832.SH"},
    )


@pytest.mark.parametrize(
    "payload,code",
    [
        (coded_change_table(), "600832"),
        (coded_change_table(), "601299.SH"),
        (coded_change_table() * 2, "601299"),
        (coded_change_table().replace(b"000300", b"000016"), "601299"),
        (coded_change_table().replace("沪深300".encode(), "沪深300行业".encode()), "601299"),
        (coded_change_table().replace("调入".encode(), "调整".encode()), "601299"),
        (coded_change_table().replace(b'<td rowspan="2">', b'<td rowspan="3">'), "601299"),
        (
            coded_change_table().replace(b"<td>601299</td>", b'<td colspan="2">601299</td>'),
            "601299",
        ),
        (coded_change_table(addition="900003"), "601299"),
    ],
)
def test_coded_table_rejects_missing_ambiguous_trigger_identity_direction_or_spans(payload, code):
    from cnequity.adapters.exchange.csi300_notices import parse_csi300_coded_change_table

    with pytest.raises(ValueError):
        parse_csi300_coded_change_table(payload, removed_code=code)


@pytest.mark.parametrize("scope", ["公司", "本公司A股"])
def test_issuer_exchange_delisting_arrangement_separates_effect_from_decision(scope):
    from cnequity.adapters.exchange.csi300_notices import parse_stock_delisting_authority

    text = (
        "证券代码：601299 股票终止上市公告。终止上市决定日期：2015年5月15日。"
        f"上海证券交易所将在2015年5月20日对{scope}股票予以摘牌，{scope}股票终止上市。"
    )
    assert parse_stock_delisting_authority(html(text), code="601299") == date(2015, 5, 20)


@pytest.mark.parametrize(
    "text",
    [
        "上海证券交易所可能在2015年5月20日对公司股票予以摘牌，公司股票终止上市。",
        "摘牌日期：2015年5月20日。终止上市决定日期：2015年5月15日。",
        "上海证券交易所将在2015年5月20日对公司股票予以摘牌，公司股票终止上市。"
        "本所决定自2015年5月21日起终止上市。",
    ],
)
def test_delisting_arrangement_rejects_tentative_unbound_or_conflicting_dates(text):
    from cnequity.adapters.exchange.csi300_notices import parse_stock_delisting_authority

    with pytest.raises(ValueError):
        parse_stock_delisting_authority(
            html("证券代码：601299 股票终止上市公告。" + text), code="601299"
        )
