"""Conservative extraction of current-period consolidated issuer report figures."""

import io
import re
from calendar import monthrange
from datetime import date, datetime

from cnequity._lazy import lazy_attribute, lazy_module
from cnequity.domain.market_time import SHANGHAI_TZ

PdfReader = lazy_attribute("pypdf", "PdfReader")
_pypdf_errors = lazy_module("pypdf.errors")

_SECTIONS = {
    "balance": (
        "合并资产负债表",
        "母公司资产负债表",
        {
            "资产总计": "total_assets",
            "负债合计": "total_liabilities",
            "所有者权益合计": "total_equity",
            "股东权益合计": "total_equity",
            "货币资金": "monetary_funds",
            "应收账款": "accounts_receivable",
            "存货": "inventory",
            "固定资产": "fixed_assets",
        },
    ),
    "income": (
        "合并利润表",
        "母公司利润表",
        {
            "营业总收入": "revenue",
            "营业成本": "operating_cost",
            "营业利润": "operating_profit",
            "利润总额": "total_profit",
            "所得税费用": "income_tax",
            "归属于母公司所有者的净利润": "net_profit",
            "归属于母公司股东的净利润": "net_profit",
            "销售费用": "sale_expense",
            "管理费用": "manage_expense",
            "财务费用": "finance_expense",
        },
    ),
    "cashflow": (
        "合并现金流量表",
        "母公司现金流量表",
        {
            "经营活动产生的现金流量净额": "net_cash_operate",
            "投资活动产生的现金流量净额": "net_cash_invest",
            "筹资活动产生的现金流量净额": "net_cash_finance",
            "期末现金及现金等价物余额": "end_cash",
        },
    ),
}
_AMOUNT = re.compile(
    r"(?<![\d.])(?:\([ \t]*\d[\d,]*\.\d{2}[ \t]*\)|"
    r"（[ \t]*\d[\d,]*\.\d{2}[ \t]*）|[-−]?\d[\d,]*\.\d{2})(?!\d)"
)
_INTEGER_AMOUNT = re.compile(
    r"(?<![\d.,])(?:[-−]?\d{1,3}(?:,\d{3})+|\(\d{1,3}(?:,\d{3})+\)|（\d{1,3}(?:,\d{3})+）)(?![\d.,])"
)
REPORT_TITLES = {1: "第一季度报告", 2: "半年度报告", 3: "第三季度报告", 4: "年度报告"}


def report_query(year, quarter=4):
    if quarter not in REPORT_TITLES:
        raise ValueError("report quarter must be 1..4")
    begin = year + 1 if quarter == 4 else year
    return f"{year}年{REPORT_TITLES[quarter]}", f"{begin}-01-01~{year + 1}-12-31"


def _heading(title):
    inline_unit = (
        r"(?P<inline_unit>单位[:：][ \t]*(?:人民币)?(?:万元|千元|元)"
        r"(?:[ \t]+币种[:：][ \t]*[\u4e00-\u9fff]{2,10})?)"
    )
    return (
        rf"(?m)^[ \t]*(?:[0-9一二三四五六七八九十]+[、.．][ \t]*)?{title}"
        rf"(?:[ \t]+{inline_unit})?[ \t]*$"
    )


def _merged_statement(text, statement, year):
    title = {"balance": "资产负债表", "income": "利润表", "cashflow": "现金流量表"}[statement]
    prefix = rf"(?:{year}\s*年度\s*)?合并及公司"
    heading = re.search(_heading(prefix + title), text)
    if heading is None:
        return None, None
    remaining = text[heading.end() :]
    boundary = re.search(r"(?m)^.*(?:主管会计工作(?:的)?负责人|会计机构负责人)[:：].*$", remaining)
    return heading, boundary


def _glossary_rmb_units(text):
    """Accept exact currency definitions in the issuer's opening glossary."""
    opening = text[:15000]
    heading = re.search(r"(?m)^释义$", opening)
    if heading is None:
        return set()
    glossary = opening[heading.end() :]
    boundary = re.search(r"(?m)^第[一二三四五六七八九十0-9]+节", glossary)
    if boundary is not None:
        glossary = glossary[: boundary.start()]
    definitions = {}
    for line in glossary.splitlines():
        match = re.fullmatch(r"(元|千元|万元)[ \t]+指[ \t]+(.+)", line.strip())
        if match:
            unit, meaning = match.groups()
            definitions.setdefault(unit, set()).add(meaning.strip())
            continue
        enumerated = re.fullmatch(r"(元|千元|万元)、(元|千元|万元)[ \t]+指[ \t]+(.+)", line.strip())
        if enumerated:
            unit, other_unit, meaning = enumerated.groups()
            exact = unit != other_unit and meaning.strip() in (
                f"人民币{unit}、{other_unit}",
                f"人民币{unit}、人民币{other_unit}",
                f"人民币{unit}和人民币{other_unit}",
            )
            # The currency prefix may govern this exact ordered pair only.
            # Preserve malformed or conflicting definitions as conflicts.
            for named in (unit, other_unit):
                definitions.setdefault(named, set()).add(
                    f"人民币{named}" if exact else "ambiguous enumerated definition"
                )
            continue
        paired = re.fullmatch(
            r"(元|千元|万元)\((元|千元|万元)\)[ \t]+指[ \t]+([^()]+)\(([^()]+)\)",
            line.strip().replace("（", "(").replace("）", ")"),
        )
        if paired:
            unit, nested_unit, meaning, nested_meaning = paired.groups()
            # Bind each unit to its own definition; reversed, foreign or
            # contradictory meanings cannot lend another unit RMB identity.
            definitions.setdefault(unit, set()).add(meaning.strip())
            definitions.setdefault(nested_unit, set()).add(nested_meaning.strip())
    return {unit for unit, meanings in definitions.items() if meanings == {f"人民币{unit}"}}


def _statement_heading_text(line):
    # Some PDFs draw bold titles repeatedly at the same position. Collapse
    # only complete, identical statement titles, never data or mixed scopes.
    repeated = re.fullmatch(
        r"(?:(?P<number>[0-9一二三四五六七八九十]+)[、.．]{1,4}[ \t]*)?"
        r"(?P<title>(?:合并|母公司|公司)?(?:资产负债表|利润表|现金流量表))"
        r"(?:[ \t]*(?P=title)){1,3}",
        line,
    )
    if repeated is None:
        return line
    prefix = f"{repeated['number']}、" if repeated["number"] else ""
    return prefix + repeated["title"]


def parse_annual_text(text: str, symbol: str, year: int) -> list[dict]:
    return parse_report_text(text, symbol, year, quarter=4)


def parse_report_text(text: str, symbol: str, year: int, *, quarter=4) -> list[dict]:
    """Require an issuer identity, current-before-comparative headers and exact units.

    A one-value line may be a blank current cell followed by last year's value;
    it is deliberately skipped. Scans, reordered/ambiguous tables and unsupported
    currencies/layouts are unresolved, never filled with zeros or comparisons.
    """
    code = symbol.split(".")[0]
    report_query(year, quarter)
    # PDF layout extraction can emit thousands of padding spaces per line.
    # They carry no cell identity for this parser, but can exhaust the header
    # window and cause multiline whitespace backtracking over entire reports.
    text = "\n".join(
        _statement_heading_text(re.sub(r"[ \t]{2,}", "  ", line.strip()))
        for line in text.splitlines()
    )
    glossary_rmb_units = _glossary_rmb_units(text)
    compact = re.sub(r"\s+", "", text[:15000])
    code_label = r"(?:公\s*司\s*代\s*码|证\s*券\s*代\s*码|股\s*票\s*代\s*码)"
    if not re.search(rf"{code_label}\s*[:：]?\s*{code}(?!\d)", text[:15000]):
        raise ValueError("periodic report does not identify the requested issuer")
    if not re.search(rf"{year}(?:年)?{REPORT_TITLES[quarter]}", compact):
        raise ValueError("report does not identify the requested year and quarter")
    report_compact = re.sub(r"\s+", "", text)
    rmb_presentation = (
        "本公司以人民币为记账本位币" in report_compact
        and re.search(
            r"本公司(?:按照以下规定[，,])?将以外币表示的财务报表折算为人民币金额表示的财务报表",
            report_compact,
        )
        is not None
    )
    # A common exchange template declares currency once immediately before
    # the primary consolidated books, then prints only 单位：元 per table.
    # Bind that declaration to the primary heading, not a functional currency
    # note or an unrelated subsidiary/foreign-currency table elsewhere.
    rmb_presentation = (
        rmb_presentation
        or re.search(
            r"财务附注中报表的单位为[:：]人民币元(?:[0-9一二三四五六七八九十]+[、.．])?合并资产负债表",
            report_compact,
        )
        is not None
    )
    rows = []
    for statement, (title, end_title, labels) in _SECTIONS.items():
        # Anchored headings avoid a table of contents or note reference.
        balance_parts = (
            r"(?:[（(](?:资产|负债及(?:所有者|股东)权益)[）)])?" if statement == "balance" else ""
        )
        heading = re.search(_heading(title + balance_parts), text)
        merged = False
        if heading is None and quarter == 4:
            heading, end = _merged_statement(text, statement, year)
            merged = heading is not None
        if heading is None:
            continue
        if not merged:
            # Bare standalone books also end the consolidated scope. They
            # never provide facts merely because a parent prefix is absent.
            parent_title = r"(?:母公司|公司)?" + end_title.removeprefix("母公司")
            end = re.search(_heading(parent_title + balance_parts), text[heading.end() :])
        if end is None and quarter < 4:
            # Newer quarterly reports omit parent-only tables. Require an
            # explicit next consolidated statement or accounting sign-off;
            # do not extend a section arbitrarily through notes or summaries.
            later_titles = {
                "balance": ["合并利润表", "合并现金流量表"],
                "income": ["合并现金流量表"],
                "cashflow": [],
            }[statement]
            boundaries = [
                found
                for later in later_titles
                if (found := re.search(_heading(later), text[heading.end() :])) is not None
            ]
            signature = re.search(
                r"(?m)^.*(?:主管会计工作负责人|会计机构负责人)[:：].*$",
                text[heading.end() :],
            )
            if signature is not None:
                boundaries.append(signature)
            end = min(boundaries, key=lambda match: match.start()) if boundaries else None
        if end is None:
            continue
        section = text[heading.end() : heading.end() + end.start()]
        # A right-aligned unit can share the title's physical PDF line. Keep
        # that declaration in this table's header after consuming the title.
        if inline_unit := heading.groupdict().get("inline_unit"):
            currency = re.search(r"币种[:：][ \t]*([\u4e00-\u9fff]+)", inline_unit)
            if currency is not None and currency.group(1) != "人民币":
                continue
            section = inline_unit + "\n" + section
        header = re.sub(r"\s+", "", section[:600])
        if re.search(r"币种[:：](?!人民币)", header):
            continue
        dated_context = re.sub(r"\s+", "", text[max(0, heading.start() - 150) : heading.end()])
        if str(year) not in header and str(year) not in dated_context:
            continue
        if quarter < 4:
            month = quarter * 3
            if statement == "balance":
                day = monthrange(year, month)[1]
                period = rf"{year}(?:年0?{month}月{day}日|-{month:02d}-{day})"
            else:
                # Q2/Q3 income and cash flow are year-to-date, not just the
                # latest quarter. Require the report's explicit Jan-to-end
                # interval before interpreting a two-column table.
                period = rf"{year}年(?:1[—–\-至～~]0?{month}月|1月1日[—–\-至～~]0?{month}月{monthrange(year, month)[1]}日)"
            if not re.search(period, header + dated_context):
                continue
        unit_match = re.search(
            r"单位[:：](?:(元|千元|万元)币种[:：]人民币|人民币(元|千元|万元))", header
        )
        if unit_match is None and merged:
            unit_match = re.search(r"金额单位为人民币(元|千元|万元)", header)
        if unit_match is None:
            unit_match = re.search(r"金额单位(?:均)?为人民币(万元|千元|元)", header)
        if unit_match is None and rmb_presentation:
            unit_match = re.search(r"单位[:：](元|千元|万元)", header)
        if (
            unit_match is None
            and glossary_rmb_units
            and re.search(r"币种[:：](?!人民币)", header) is None
        ):
            declared = re.search(r"(?m)^[ \t]*单位[:：][ \t]*(万元|千元|元)[ \t]*$", section[:600])
            if declared is not None and declared.group(1) in glossary_rmb_units:
                unit_match = declared
        standalone = re.search(r"(?m)^\s*人民币(元|千元|万元)\s*$", section[:600])
        if standalone is None and quarter == 4:
            standalone = re.search(
                rf"(?m)^[ \t]*{year}[ \t]*(?:年度|年[ \t]*12[ \t]*月[ \t]*31[ \t]*日)"
                r"[ \t]+人民币(万元|千元|元)[ \t]*$",
                section[:600],
            )
        if unit_match is None and standalone is None:
            continue
        order = (
            r"(?:期末|年末)余额.*(?:期初|年初)余额"
            if statement == "balance"
            else r"(?:本期|本年)(?:发生额|金额).*(?:上期|上年)(?:发生额|金额)"
        )
        dated_header = any(
            re.search(
                rf"^(?:项目|资产|负债(?:和|及)(?:股东|所有者)权益|附注[一二三四五六七八九十0-9]*)"
                rf".*{year}(?:年|年度).*{year - 1}(?:年|年度)",
                re.sub(r"\s+", "", line),
            )
            for line in section[:600].splitlines()
        )
        three_balance_columns = statement == "balance" and any(
            re.search(
                rf"{year}年12月31日.*{year}年0?1月0?1日.*{year - 1}年12月31日",
                re.sub(r"\s+", "", line),
            )
            for line in section[:600].splitlines()
        )
        if three_balance_columns:
            dated_header = True
        if merged:
            roles = any(
                re.sub(r"\s+", "", line) == "合并合并公司公司"
                for line in section[:600].splitlines()
            )
            dates = any(
                re.search(
                    rf"{year}年.*{year - 1}年.*{year}年.*{year - 1}年", re.sub(r"\s+", "", line)
                )
                for line in section[:600].splitlines()
            )
            if not roles or not dates:
                continue
            dated_header = True
        cumulative = quarter < 4 and re.search(r"年初至报告期.*上年年初至报告期", header)
        four_columns = (
            quarter == 3
            and statement == "income"
            and any(
                re.search(
                    r"本期(?:金额|发生额).*上期(?:金额|发生额).*年初至报告期.*上年年初至报告",
                    re.sub(r"\s+", "", line),
                )
                or re.search(
                    rf"{year}年第三季度.*{year - 1}年第三季度.*{year}年前三季度.*{year - 1}年前三季度",
                    re.sub(r"\s+", "", line),
                )
                for line in section[:600].splitlines()
            )
        )
        if (
            quarter == 3
            and statement == "income"
            and re.search(r"本期(?:金额|发生额)|本报告期", header)
            and re.search(r"年初[至到]报告期", header)
            and not four_columns
        ):
            # A four-column layout whose order is unclear cannot degrade to
            # a two-cell parse: those cells may be quarter-only figures.
            continue
        if (
            quarter == 3
            and statement == "income"
            and re.search(rf"{year}年第三季度", header)
            and not four_columns
        ):
            continue
        if not re.search(order, header) and not dated_header and not cumulative:
            continue
        unit = next(g for g in unit_match.groups() if g) if unit_match else standalone.group(1)
        scale = {"元": 1, "千元": 1000, "万元": 10000}[unit]
        found = {}
        lines = section.splitlines()
        for index, line in enumerate(lines):
            values = list(_AMOUNT.finditer(line))
            if not values and scale > 1:
                # Explicit scaled units can publish rounded integer cells.
                # Requiring grouping commas avoids mistaking numeric note
                # references for amounts; small ungrouped cells stay unknown.
                values = list(_INTEGER_AMOUNT.finditer(line))
            expected_cells = 4 if four_columns or merged else 3 if three_balance_columns else 2
            if len(values) != expected_cells:
                continue
            if any(
                not match.group().startswith(("(", "（"))
                and (
                    line[: match.start()].rstrip().endswith(("(", "（"))
                    or line[match.end() :].lstrip().startswith((")", "）"))
                )
                for match in values
            ):
                # An incomplete accounting-negative cell cannot become positive.
                continue
            label = re.sub(r"\s+", "", line[: values[0].start()])
            # A long label wraps inside its bracket — 所有者权益（或股东权 / 益）合计
            # — and the amounts sit on the first line; take the tail from the next.
            if label.count("（") + label.count("(") > label.count("）") + label.count(")"):
                tail = re.sub(r"\s+", "", lines[index + 1]) if index + 1 < len(lines) else ""
                if tail and not _AMOUNT.search(tail):
                    label += tail
            # Remove only row numbering and the optional numeric note column.
            label = re.sub(r"^(?:[一二三四五六七八九十]+、|[0-9]+[.．、])", "", label)
            label = re.sub(r"^(?:其中|加|减)[:：]", "", label)
            label = re.sub(
                r"[一二三四五六七八九十]+[（(]\d+[）)](?:[（(][a-zA-Z0-9]+[）)])+$", "", label
            )
            label = re.sub(
                r"[一二三四五六七八九十]*[（(][一二三四五六七八九十0-9]+[）)]\d*$", "", label
            )
            label = re.sub(r"[一二三四五六七八九十]+[、.]\d+$", "", label)
            label = re.sub(r"[0-9]+(?:\([0-9]+\))?$", "", label)
            label = re.sub(r"[（(].*[）)]", "", label)
            label = re.sub(r"[（(].*$", "", label)
            item = labels.get(label)
            if item is None:
                continue
            if (
                merged
                and statement == "income"
                and item
                in {
                    "operating_cost",
                    "income_tax",
                    "sale_expense",
                    "manage_expense",
                    "finance_expense",
                }
            ):
                # Signed presentation of deductions differs from canonical
                # expense conventions. These cells remain unresolved.
                continue
            token = values[2 if four_columns else 0].group().replace(",", "").replace("−", "-")
            value = float(token.strip("()（）")) * scale
            if token.startswith(("(", "（")):
                value = -value
            if item in found:
                raise ValueError(f"consolidated table duplicates {item}")
            found[item] = value
        if statement == "balance" and all(
            k in found for k in ("total_assets", "total_liabilities", "total_equity")
        ):
            error = abs(found["total_assets"] - found["total_liabilities"] - found["total_equity"])
            if error > max(scale, abs(found["total_assets"]) * 1e-8):
                raise ValueError("consolidated balance does not reconcile")
        rows.extend(
            {
                "symbol": symbol,
                "report_period": f"{year}Q{quarter}",
                "statement_type": statement,
                "item_code": item,
                "item_value": value,
            }
            for item, value in found.items()
        )
    if not rows:
        raise ValueError("periodic report has no unambiguous supported consolidated figures")
    return rows


def parse_annual_pdf(payload: bytes, symbol: str, year: int) -> list[dict]:
    return parse_report_pdf(payload, symbol, year, quarter=4)


def parse_report_pdf(payload: bytes, symbol: str, year: int, *, quarter=4) -> list[dict]:
    if not payload.startswith(b"%PDF"):
        raise ValueError("issuer periodic report is not a PDF")
    try:
        text = "\n".join(
            page.extract_text(extraction_mode="layout") or ""
            for page in PdfReader(io.BytesIO(payload)).pages
            if page.get("/Contents") is not None
        )
    except _pypdf_errors.DependencyError as exc:
        raise ValueError("issuer PDF crypto dependency unavailable; install pypdf[crypto]") from exc
    except (_pypdf_errors.PdfReadError, KeyError, ZeroDivisionError) as exc:
        raise ValueError("issuer periodic PDF cannot be read") from exc
    return parse_report_text(text, symbol, year, quarter=quarter)


def original_annual_notice(records: list[dict], code: str, year: int) -> dict:
    return original_report_notice(records, code, year, quarter=4)


def original_report_notice(records: list[dict], code: str, year: int, *, quarter=4) -> dict:
    report_query(year, quarter)
    expected = rf"{year}(?:年)?{REPORT_TITLES[quarter]}"
    edition = r"(?:全文|正文|[（(]全文[）)])?" if quarter in (1, 3) else r"(?:全文|[（(]全文[）)])?"
    candidates = []
    for row in records:
        title = str(row.get("announcementTitle", ""))
        title = re.sub(r"<[^>]+>", "", title).replace(" ", "")
        # Issuer-name prefixes are allowed; announcement identity and the
        # report itself must both repeat the code. Restated editions are not
        # original disclosures.
        if (
            not re.fullmatch(rf"[^0-9]*{expected}{edition}", title)
            or any(
                word in title
                for word in ("更正", "修订", "补充", "更新", "取消", "英文", "摘要", "关于")
            )
            or row.get("secCode") != code
        ):
            continue
        path = row.get("adjunctUrl", "")
        if not re.fullmatch(r"finalpage/\d{4}-\d{2}-\d{2}/[0-9]+\.[Pp][Dd][Ff]", path):
            continue
        stamp = row.get("announcementTime")
        if not isinstance(stamp, (int, float)) or isinstance(stamp, bool):
            continue
        timestamp = datetime.fromtimestamp(stamp / 1000, tz=SHANGHAI_TZ)
        earliest_day = date(year, quarter * 3, monthrange(year, quarter * 3)[1])
        if timestamp.date() <= earliest_day or timestamp.year > year + 1:
            continue
        candidates.append(row)
    if not candidates:
        raise ValueError("no original issuer periodic report in the complete query")
    earliest = min(row["announcementTime"] for row in candidates)
    first = [r for r in candidates if r["announcementTime"] == earliest]
    full = [row for row in first if "正文" not in str(row.get("announcementTitle", ""))]
    first = full or first
    if len({r["adjunctUrl"] for r in first}) != 1:
        raise ValueError("multiple original report PDFs on the earliest announcement date")
    return first[0]
