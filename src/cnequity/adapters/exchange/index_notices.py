"""Original CSI 300 / STAR 50 books and SSE effective-date adjustment tables."""

import json
import re
from datetime import date, timedelta
from html import escape
from html.parser import HTMLParser
from io import BytesIO
from xml.etree import ElementTree
from zipfile import BadZipFile, ZipFile

import pandas as pd
import polars as pl
from pypdf import PdfReader

from cnequity.adapters.calendar.exchange_calendar import load_seed_calendar
from cnequity.adapters.cni.index_announcements import MembershipChange
from cnequity.domain.symbols import infer_exchange_from_code, is_all_a_symbol


class _Text(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts: list[str] = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style"}:
            self.hidden += 1

    def handle_endtag(self, tag):
        if tag in {"script", "style"}:
            self.hidden -= 1

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def _text(payload):
    parser = _Text()
    parser.feed(payload.decode("utf-8"))
    return re.sub(r"\s+", " ", " ".join(parser.parts))


def _symbols(codes):
    if not codes or len(codes) != len(set(codes)):
        raise ValueError("STAR 50 book has empty or duplicate members")
    if any(
        not isinstance(code, str)
        or not code.startswith(("688", "689"))
        or not is_all_a_symbol(code, "SH")
        for code in codes
    ):
        raise ValueError("STAR 50 book has an unsupported member identity")
    return frozenset(code + ".SH" for code in codes)


def parse_star50_opening(payload: bytes) -> frozenset[str]:
    """Read the publisher's complete opening book, without inventing weights."""
    sheet = pd.read_excel(BytesIO(payload), dtype=str)
    code_col, index_col, name_col = (
        "证券代码\nSecurities Code",
        "指数代码\nIndex Code",
        "指数名称\nIndex Name",
    )
    if not {code_col, index_col, name_col}.issubset(sheet.columns):
        raise ValueError("STAR 50 opening workbook has different columns")
    if (
        len(sheet) != 50
        or not sheet[index_col].eq("000688").all()
        or not sheet[name_col].eq("科创50").all()
    ):
        raise ValueError("STAR 50 opening workbook has different index identity or size")
    return _symbols(sheet[code_col].tolist())


def parse_star50_launch_date(payload: bytes) -> date:
    """Use the declared live launch, never the index's retrospective base date."""
    text = re.sub(r"\s+", "", _text(payload))
    matches = re.findall(
        r"将于(\d{4})年(\d{1,2})月(\d{1,2})日收盘后发布上证科创板50成份指数历史行情[，,](\d{1,2})月(\d{1,2})日正式发布实时行情",
        text,
    )
    if len(matches) != 1:
        raise ValueError("STAR 50 notice has no unique explicit live launch date")
    year, history_month, history_day, month, day = map(int, matches[0])
    historical = date(year, history_month, history_day)
    live = date(year, month, day)
    if live <= historical:
        raise ValueError("STAR 50 live launch precedes retrospective history publication")
    return live


def parse_star50_effective_date(payload: bytes) -> date:
    """Read explicit dates; an after-close change begins on the next trading session."""
    text = _text(payload)
    compact = re.sub(r"\s+", "", text)
    matches = re.findall(r"[于自](\d{4})年(\d{1,2})月(\d{1,2})日(?:起)?(收(?:盘|市)后)?", compact)
    dates = {(date(*map(int, row[:3])), bool(row[3])) for row in matches}
    if len(dates) != 1:
        raise ValueError("SSE notice has no unique explicit adjustment date")
    effective, after_close = dates.pop()
    if after_close:
        calendar = load_seed_calendar()
        if effective < calendar["trade_date"].min():
            raise ValueError("after-close adjustment date is outside the supported calendar")
        session = calendar.filter(pl.col("trade_date") == effective)
        if session.is_empty() or not session["is_trading"][0]:
            raise ValueError("after-close adjustment does not identify a known closing session")
        candidates = calendar.filter(pl.col("is_trading") & (pl.col("trade_date") > effective))
        if candidates.is_empty():
            raise ValueError("after-close adjustment needs a known following session")
        effective = candidates["trade_date"].min()
    return effective


def parse_csi300_first_session_calendar_date(payload: bytes, *, calendar_wire: bytes) -> date:
    """Bind an original January/July rule to a complete public BaoStock calendar.

    Callers retain both source receipts. This explicitly uses vendor calendar
    evidence; it does not describe that evidence as an exchange holiday notice.
    """
    text = re.sub(r"\s+", "", _text(payload))
    if re.search(r"[于自]\d{4}年\d{1,2}月\d{1,2}日", text):
        raise ValueError("calendar cannot override an explicit full adjustment date")
    periods = set(
        re.findall(
            r"沪深300指数(?:和中证100指数)?将于(\d{4})年([17])月第一个交易日调整样本股",
            text,
        )
    )
    periods.update(re.findall(r"决定于(\d{4})年([17])月第一个交易日调整沪深300指数", text))
    if len(periods) != 1:
        raise ValueError("CSI notice has no unique explicit January/July first-session rule")
    year, month = map(int, periods.pop())
    prefix = f"{year}-{month:02d}"
    parts = calendar_wire.decode("utf-8").split("\x01")
    if (
        len(parts) != 13
        or parts[3:5] != ["success", "query_trade_dates"]
        or parts[6:8] != ["1", "2000"]
        or parts[9:11] != [f"{prefix}-01", f"{prefix}-31"]
        or [field.strip() for field in parts[11].split(",")] != ["calendar_date", "is_trading_day"]
    ):
        raise ValueError("calendar wire has a different query, page, year or schema")
    data = json.loads(parts[8])
    rows = data.get("record") if isinstance(data, dict) else None
    expected = [(date(year, month, 1) + timedelta(days=i)).isoformat() for i in range(31)]
    if (
        not isinstance(rows, list)
        or len(rows) != 31
        or any(
            not isinstance(row, list) or len(row) != 2 or row[0] != day or row[1] not in ("0", "1")
            for row, day in zip(rows, expected, strict=True)
        )
    ):
        raise ValueError("calendar must explicitly classify every requested month day in order")
    sessions = [date.fromisoformat(row[0]) for row in rows if row[1] == "1"]
    if not sessions:
        raise ValueError("calendar has no requested-month trading session")
    return sessions[0]


def parse_csi300_first_session_date(payload: bytes, *, holiday_notice: bytes) -> date:
    """Bind an explicit January first-session rule to a separate exchange notice."""
    text = re.sub(r"\s+", "", _text(payload))
    years = set(re.findall(r"[于自](\d{4})年(?:1月)?第一个交易日调整沪深300", text))
    if len(years) != 1:
        raise ValueError("CSI notice has no unique explicit January first-session rule")
    year = int(years.pop())
    authority = re.sub(r"\s+", "", _text(holiday_notice))
    declared = set(re.findall(r"关于(?:上海证券交易所)?(\d{4})年全年休市安排的通知", authority))
    if declared != {str(year)}:
        raise ValueError("exchange holiday authority has different or ambiguous year")
    sections = re.findall(r"元旦[：:](.*?)(?=春节)", authority)
    days = set()
    for section in sections:
        matches = re.findall(
            r"1月1日(?:[（(]星期.[）)])?(?:至|[—－-])1月(\d{1,2})日"
            r"(?:[（(]星期.[）)])?(?:为节假日)?休市[，,]1月(\d{1,2})日"
            r"(?:[（(]星期.[）)])?起照常开市",
            section,
        )
        for closed, reopened in matches:
            closed_date = date(year, 1, int(closed))
            reopened_date = date(year, 1, int(reopened))
            weekends = set()
            for day, weekday in re.findall(
                r"1月(\d{1,2})日[（(]星期([六日天])[）)]为周末休市", section
            ):
                weekend = date(year, 1, int(day))
                if weekend.weekday() != {"六": 5, "日": 6, "天": 6}[weekday]:
                    raise ValueError(
                        "exchange January weekend declaration has inconsistent weekday"
                    )
                weekends.add(weekend)
            gap = (reopened_date - closed_date).days
            if (
                not 1 <= gap <= 3
                or reopened_date.weekday() >= 5
                or any(
                    closed_date + timedelta(days=offset) not in weekends for offset in range(1, gap)
                )
            ):
                raise ValueError("exchange January closure does not prove the first session")
            days.add(reopened_date)
    if len(days) != 1:
        raise ValueError(
            "exchange authority has no unique continuous January closure and reopening"
        )
    return days.pop()


def parse_star50_adjustment(payload: bytes) -> MembershipChange:
    """Read an explicitly headed sample table; exclude its reserve-stock table."""
    effective = parse_star50_effective_date(payload)
    text = _text(payload)
    return _parse_table(text, effective)


def _parse_table(text: str, effective: date) -> MembershipChange:
    heading = r"(?:上\s*证\s*)?科\s*创\s*(?:板\s*)?50\s*(?:成\s*份\s*)?指\s*数\s*样\s*本\s*(?:股\s*)?调\s*整\s*名\s*单"
    scopes = []
    for title in re.finditer(heading, text):
        scope = text[title.end() :]
        end = re.search(
            r"(?:上\s*证|中\s*证|科\s*创)[^：:。]{0,60}指\s*数\s*(?:样\s*本|备\s*选)[^：:。]{0,20}名\s*单",
            scope,
        )
        if end:
            scope = scope[: end.start()]
        columns = re.sub(r"\s+", "", scope).lstrip("：:")
        if re.match(r"调出名单调入名单证券代码证券(?:简称|名称)证券代码证券(?:简称|名称)", columns):
            scopes.append(scope)
    if len(scopes) != 1:
        raise ValueError("SSE notice has no unique table with explicit STAR 50 directions")
    scope = scopes[0]
    codes = re.findall(r"(?<!\d)\d{6}(?!\d)", scope)
    if not codes or len(codes) % 2:
        raise ValueError("SSE adjustment table has empty or unpaired changes")
    removals, additions = _symbols(codes[::2]), _symbols(codes[1::2])
    if additions & removals:
        raise ValueError("SSE adjustment adds and removes the same member")
    return MembershipChange("000688.SH", effective, additions, removals)


def parse_star50_attachment(payload: bytes, *, kind: str, effective_date: date) -> MembershipChange:
    """Read an original attachment with its separately verified notice date."""
    if kind == "xlsx":
        sheets = pd.read_excel(BytesIO(payload), sheet_name=None, dtype=str)
        if not {"调入", "调出"}.issubset(sheets):
            raise ValueError("SSE workbook has no declared incoming/outgoing sheets")
        pairs = []
        for name in ("调入", "调出"):
            sheet = sheets[name]
            if not {"指数代码", "指数简称", "证券代码"}.issubset(sheet.columns):
                raise ValueError("SSE workbook has different index/member columns")
            selected = sheet[sheet["指数代码"] == "000688"]
            if not selected["指数简称"].eq("科创50").all():
                raise ValueError("SSE workbook identifies a different target index")
            pairs.append(_symbols(selected["证券代码"].tolist()))
        additions, removals = pairs
        if additions & removals or len(additions) != len(removals):
            raise ValueError("SSE workbook has overlapping or unequal changes")
        return MembershipChange("000688.SH", effective_date, additions, removals)
    if kind == "pdf":
        text = " ".join(page.extract_text() or "" for page in PdfReader(BytesIO(payload)).pages)
    elif kind in {"wps", "docx"}:
        try:
            with ZipFile(BytesIO(payload)) as archive:
                info = archive.getinfo("word/document.xml")
                if info.file_size > 2_000_000:
                    raise ValueError("SSE document table exceeds the supported XML size")
                document = ElementTree.fromstring(archive.read(info))
        except (BadZipFile, KeyError, ElementTree.ParseError) as exc:
            raise ValueError("SSE WPS attachment is not a supported Word XML document") from exc
        namespace = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
        if document.tag != namespace + "document":
            raise ValueError("SSE document has a different Word XML identity")
        text = " ".join(
            "".join(run.text or "" for run in paragraph.iter(namespace + "t"))
            for paragraph in document.iter(namespace + "p")
        )
    else:
        raise ValueError("SSE attachment format is unsupported")
    return _parse_table(re.sub(r"\s+", " ", text), effective_date)


def parse_star50_no_change_date(payload: bytes) -> date:
    """A missing table proves nothing; require an explicit no-change declaration."""
    compact = re.sub(r"\s+", "", _text(payload))
    if "科创50指数样本无变动" not in compact:
        raise ValueError("SSE notice does not explicitly declare unchanged STAR 50 samples")
    return parse_star50_effective_date(payload)


class _Tables(_Text):
    """Keep table text separate from page navigation, titles and URL dates."""

    def __init__(self):
        super().__init__()
        self.stack: list[list[str]] = []
        self.tables: list[str] = []

    def handle_starttag(self, tag, attrs):
        super().handle_starttag(tag, attrs)
        if tag == "table":
            self.stack.append([])

    def handle_endtag(self, tag):
        if tag == "table" and self.stack:
            self.tables.append(" ".join(self.stack.pop()))
        super().handle_endtag(tag)

    def handle_data(self, data):
        if not self.hidden:
            for parts in self.stack:
                parts.append(data)


def parse_csi300_opening(payload: bytes) -> tuple[date, frozenset[str]]:
    """Read an explicitly dated complete CSI 300 HTML book, without weights.

    This validates one original book, not the completeness of later changes.
    A page's archive URL or publication date never supplies its effective date.
    """
    parser = _Tables()
    parser.feed(payload.decode("utf-8"))
    candidates = []
    heading = r"沪深\s*300\s*指数样本股名单\s*[（(]\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日起生效\s*[）)]"
    for table in parser.tables:
        matches = list(re.finditer(heading, table))
        if not matches:
            continue
        if len(matches) != 1:
            raise ValueError("CSI 300 table has ambiguous effective dates")
        match = matches[0]
        body = table[match.end() :]
        if not re.match(r"\s*代码\s*名称", body):
            raise ValueError("CSI 300 table has no explicit security code/name columns")
        codes = re.findall(r"(?<!\d)\d{6}(?!\d)", body)
        if len(codes) != 300 or len(set(codes)) != 300:
            raise ValueError("CSI 300 opening is not a unique complete 300-member book")
        members = []
        for code in codes:
            market = infer_exchange_from_code(code)
            if not is_all_a_symbol(code, market):
                raise ValueError("CSI 300 opening has an unsupported member identity")
            members.append(code + "." + market)
        candidates.append((date(*map(int, match.groups())), frozenset(members)))
    if len(candidates) != 1:
        raise ValueError("CSI 300 page has no unique explicitly dated complete table")
    return candidates[0]


def parse_csi300_change_workbook(payload: bytes, *, effective_date: date) -> MembershipChange:
    """Read directed original CSI 300 sheets with a separately verified date.

    Both modern security-code and old stock-code columns are supported. The
    caller must bind the effective date to its original announcement; workbook
    names and other indices' dates do not prove a CSI 300 effective date.
    """
    sheets = pd.read_excel(BytesIO(payload), sheet_name=None, dtype=str)
    if not {"调入", "调出"}.issubset(sheets):
        raise ValueError("CSI 300 workbook has no explicit incoming/outgoing sheets")
    pairs = []
    for name in ("调入", "调出"):
        sheet = sheets[name]
        code_columns = {"证券代码", "股票代码"} & set(sheet.columns)
        name_columns = {"指数简称", "指数名称"} & set(sheet.columns)
        if "指数代码" not in sheet.columns or len(name_columns) != 1 or len(code_columns) != 1:
            raise ValueError("CSI 300 workbook has ambiguous index/member columns")
        selected = sheet[sheet["指数代码"] == "000300"]
        if selected.empty or not selected[name_columns.pop()].eq("沪深300").all():
            raise ValueError("CSI 300 workbook has no matching target index identity")
        codes = selected[code_columns.pop()].tolist()
        if len(codes) != len(set(codes)):
            raise ValueError("CSI 300 workbook duplicates changes")
        members = []
        for code in codes:
            if not isinstance(code, str) or not re.fullmatch(r"\d{6}", code):
                raise ValueError("CSI 300 workbook has an unsupported security code")
            market = infer_exchange_from_code(code)
            if not is_all_a_symbol(code, market):
                raise ValueError("CSI 300 workbook has an unsupported member identity")
            members.append(code + "." + market)
        pairs.append(frozenset(members))
    additions, removals = pairs
    if additions & removals or len(additions) != len(removals):
        raise ValueError("CSI 300 workbook has overlapping or unequal changes")
    return MembershipChange("000300.SH", effective_date, additions, removals)


def parse_csi300_conditional_delistings(
    payload: bytes, *, authority_pdf: bytes
) -> list[MembershipChange]:
    """Resolve the four-company 2006 notice using the issuer's original report.

    The caller must archive and verify both original sources. Same-day removals
    form one cohort; their individual replacement pairing is not inferred.
    """
    notice = re.sub(r"\s+", "", _text(payload))
    identities = {
        "齐鲁石化": "600002",
        "石油大明": "000406",
        "扬子石化": "000866",
        "中原油气": "000956",
    }
    if (
        "关于对沪深300指数样本股进行临时调整的公告" not in notice
        or "时间：2006-04-14" not in notice
        or any(f"{name}（{code}）" not in notice for name, code in identities.items())
    ):
        raise ValueError("conditional notice has different index/company/date identities")
    clauses = re.findall(
        r"自股票终止上市生效日起，将其从指数中调出，同时按照终止上市的时间先后顺序依次调入([^。]+)。",
        notice,
    )
    if len(clauses) != 1:
        raise ValueError("conditional notice has no unique ordered replacement rule")
    additions = re.findall(r"（(\d{6})）", clauses[0])
    if len(additions) != 4 or len(set(additions)) != 4:
        raise ValueError("conditional notice has incomplete or duplicate replacements")

    def symbol(code):
        market = infer_exchange_from_code(code)
        if not is_all_a_symbol(code, market):
            raise ValueError("conditional notice has an unsupported security identity")
        return code + "." + market

    incoming = [symbol(code) for code in additions]
    if set(incoming) & {symbol(code) for code in identities.values()}:
        raise ValueError("conditional notice replaces a removed member with itself")
    authority = re.sub(
        r"\s+",
        "",
        " ".join(page.extract_text() or "" for page in PdfReader(BytesIO(authority_pdf)).pages),
    )
    companies = (
        "中国石化齐鲁石油化工股份有限公司",
        "中国石化扬子石油化工股份有限公司",
        "中国石化中原油气高新股份有限公司",
        "中国石化胜利油田大明（集团）股份有限公司",
    )
    if any(company not in authority for company in companies):
        raise ValueError("delisting authority does not identify all four legal companies")
    szse_dates = re.findall(
        r"深圳证券交易所批准了扬子石化、中原油气和石油大明流通股股票自(\d{4})年(\d{1,2})月(\d{1,2})日起终止上市交易",
        authority,
    )
    sse_dates = re.findall(
        r"上海证券交易所批准齐鲁石化流通股股票自(\d{4})年(\d{1,2})月(\d{1,2})日起终止上市交易",
        authority,
    )
    if len(szse_dates) != 1 or len(sse_dates) != 1:
        raise ValueError("delisting authority has missing or ambiguous exchange-approved dates")
    szse_day, sse_day = (date(*map(int, rows[0])) for rows in (szse_dates, sse_dates))
    if any(not date(2006, 4, 14) < day <= date(2006, 6, 30) for day in (szse_day, sse_day)):
        raise ValueError("delisting dates lie outside this conditional announcement window")
    cohorts: dict[date, set[str]] = {}
    for day, codes in (
        (szse_day, ["000406", "000866", "000956"]),
        (sse_day, ["600002"]),
    ):
        cohorts.setdefault(day, set()).update(symbol(code) for code in codes)
    changes = []
    offset = 0
    for day, removals in sorted(cohorts.items()):
        replacement = frozenset(incoming[offset : offset + len(removals)])
        changes.append(MembershipChange("000300.SH", day, replacement, frozenset(removals)))
        offset += len(removals)
    return changes


def parse_csi300_change_table(payload: bytes) -> tuple[frozenset[str], frozenset[str]]:
    """Read additions/removals from a directed original CSI 300 HTML table.

    This returns no effective date. The caller must independently verify it and
    the completeness of the announcement chain before constructing history.
    """
    text = _text(payload)
    declared_count = None
    heading = r"沪\s*深\s*300\s*指\s*数\s*样\s*本\s*(?:股\s*)?调\s*整\s*名\s*单"
    scopes = []
    for title in re.finditer(heading, text):
        scope = text[title.end() :]
        end = re.search(
            r"(?:沪\s*深|中\s*证|上\s*证)[^。]{0,40}指\s*数\s*(?:样\s*本|备\s*选)[^。]{0,20}名\s*单",
            scope,
        )
        if end:
            scope = scope[: end.start()]
        scopes.append(scope)
    compact_notice = re.sub(r"\s+", "", text)
    announced_counts = re.findall(r"其中沪深300指数更换([1-9]\d*)只股票", compact_notice)
    if len(announced_counts) > 1:
        raise ValueError("CSI 300 notice has ambiguous declared change counts")
    if announced_counts:
        declared_count = int(announced_counts[0])
    declared = re.findall(
        r"沪深300指数将于\d{4}年\d{1,2}月\d{1,2}日调整样本股。"
        r"本次共调整([1-9]\d*)只样本股，调整名单见下表。",
        compact_notice,
    )
    if (
        not scopes
        and len(declared) == 1
        and not re.search(
            r"(?:中证|上证)\d+指数|沪深300(?:行业|价值|成长|主题)指数", compact_notice
        )
    ):
        tables = _Tables()
        tables.feed(payload.decode("utf-8"))
        # Decorative layout tables do not identify securities. The explicit
        # single-index rule and count bind the unique directed table instead.
        scopes = [
            table
            for table in tables.tables
            if re.match(
                r"调[入出]样本调[入出]样本(?:股票|证券)代码",
                re.sub(r"\s+", "", table),
            )
        ]
        declared_count = int(declared[0])
    if not scopes and "关于调整沪深300指数样本股的公告" in compact_notice:
        tables = _Tables()
        tables.feed(payload.decode("utf-8"))
        # Old single-index notices put the index identity outside their sole
        # table. Multiple anonymous tables cannot safely be assigned by order.
        if len(tables.tables) == 1:
            scopes = tables.tables
    if len(scopes) != 1:
        raise ValueError("CSI 300 notice has no unique target adjustment table")
    scope = scopes[0]
    compact = re.sub(r"\s+", "", scope).lstrip("：:")
    direction = r"(调[入出](?:名单|样本)|剔除样本名单|新进样本名单)"
    columns = re.match(
        direction + direction + r"(?:证券|股票)代码(?:证券|股票)(?:简称|名称)"
        r"(?:证券|股票)代码(?:证券|股票)(?:简称|名称)",
        compact,
    )
    incoming_labels = {"调入名单", "调入样本", "新进样本名单"}
    if not columns or (columns[1] in incoming_labels) == (columns[2] in incoming_labels):
        raise ValueError("CSI 300 table has no explicit opposite incoming/outgoing columns")
    codes = re.findall(r"(?<!\d)\d{6}(?!\d)", scope)
    if not codes or len(codes) % 2:
        raise ValueError("CSI 300 table has empty or unpaired changes")
    if declared_count is not None and len(codes) != 2 * declared_count:
        raise ValueError("CSI 300 directed table differs from the declared change count")
    pairs = []
    for side in (codes[::2], codes[1::2]):
        if len(side) != len(set(side)):
            raise ValueError("CSI 300 table duplicates a directed member")
        members = []
        for code in side:
            market = infer_exchange_from_code(code)
            if not is_all_a_symbol(code, market):
                raise ValueError("CSI 300 table has an unsupported member identity")
            members.append(code + "." + market)
        pairs.append(frozenset(members))
    additions, removals = pairs if columns[1] in incoming_labels else reversed(pairs)
    if additions & removals:
        raise ValueError("CSI 300 table adds and removes the same member")
    return additions, removals


def parse_csi300_change_pdf(payload: bytes) -> tuple[frozenset[str], frozenset[str]]:
    """Read directed original PDF tables; effective dates require separate proof."""
    text = " ".join(page.extract_text() or "" for page in PdfReader(BytesIO(payload)).pages)
    return parse_csi300_change_table(("<p>" + escape(text) + "</p>").encode())


def parse_csi300_inline_workbook(payload: bytes) -> tuple[frozenset[str], frozenset[str]]:
    """Read two-level directed sheets without filling identities or blank symbols.

    This returns no effective date. Each target row must explicitly identify
    CSI 300; anonymous rows are not carried forward from a preceding index.
    """
    candidates = []
    for sheet in pd.read_excel(BytesIO(payload), sheet_name=None, header=None, dtype=str).values():
        frame = sheet.fillna("")
        if frame.shape[0] < 3 or frame.shape[1] < 6:
            continue
        if frame.iloc[0, :2].tolist() != ["指数代码", "指数简称"]:
            continue
        selected = frame.iloc[2:]
        selected = selected[selected.iloc[:, 0] == "000300"]
        if selected.empty:
            continue
        if any(
            not row[0] and any(re.fullmatch(r"\d{6}", cell) for cell in row[2:])
            for row in frame.iloc[2:].itertuples(index=False, name=None)
        ):
            raise ValueError("inline CSI 300 sheet has anonymous security rows")
        labels = [(i, frame.iloc[0, i]) for i in range(2, frame.shape[1]) if frame.iloc[0, i]]
        if (
            len(labels) != 2
            or labels[0][0] != 2
            or {label for _, label in labels} != {"调入", "调出"}
            or not selected.iloc[:, 1].eq("沪深300").all()
        ):
            raise ValueError("inline CSI 300 sheet has ambiguous direction or index identity")
        sides = {}
        for position, (start, direction) in enumerate(labels):
            end = labels[position + 1][0] if position + 1 < len(labels) else frame.shape[1]
            if (end - start) % 2:
                raise ValueError("inline CSI 300 sheet has unpaired code/name columns")
            members = []
            for col in range(start, end, 2):
                if tuple(frame.iloc[1, col : col + 2]) not in {
                    ("证券代码", "证券简称"),
                    ("证券代码", "证券名称"),
                    ("股票代码", "股票名称"),
                }:
                    raise ValueError("inline CSI 300 sheet has different security columns")
                for code, name in selected.iloc[:, col : col + 2].itertuples(
                    index=False, name=None
                ):
                    if (code, name) in [("", ""), ("-", "-")]:
                        continue
                    if not re.fullmatch(r"\d{6}", code) or not name or name == "-":
                        raise ValueError("inline CSI 300 sheet has an incomplete security identity")
                    market = infer_exchange_from_code(code)
                    if not is_all_a_symbol(code, market):
                        raise ValueError("inline CSI 300 sheet has an unsupported member identity")
                    members.append(code + "." + market)
            if not members or len(members) != len(set(members)):
                raise ValueError("inline CSI 300 sheet has empty or duplicate changes")
            sides[direction] = frozenset(members)
        additions, removals = sides["调入"], sides["调出"]
        if additions & removals or len(additions) != len(removals):
            raise ValueError("inline CSI 300 sheet has overlapping or unequal changes")
        candidates.append((additions, removals))
    if len(candidates) != 1:
        raise ValueError("workbook has no unique explicit inline CSI 300 change sheet")
    return candidates[0]


def parse_sse_delisting_directory(payload: bytes, *, code: str) -> date:
    """Read one identified stock's explicit date from the SSE termination directory.

    This is a directory fact, not a rule for choosing between conflicting
    historical authorities or an index announcement's conditional triggers.
    """
    if not re.fullmatch(r"6\d{5}", code):
        raise ValueError("unsupported SSE directory security code")
    data = json.loads(payload)
    if (
        not isinstance(data, dict)
        or data.get("sqlId") != "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L"
        or data.get("type") != "inParams"
        or data.get("actionErrors") != []
        or data.get("fieldErrors") != {}
    ):
        raise ValueError("SSE directory response has a different scope or query errors")
    rows, page = data.get("result"), data.get("pageHelp")
    if (
        not isinstance(rows, list)
        or len(rows) != 1
        or not isinstance(rows[0], dict)
        or not isinstance(page, dict)
        or any(
            type(page.get(key)) is not int or page[key] != 1
            for key in ("total", "pageCount", "pageNo")
        )
        or page.get("data") != rows
    ):
        raise ValueError("SSE directory is not a complete unique stock response")
    row = rows[0]
    if (
        row.get("COMPANY_CODE") != code
        or row.get("A_STOCK_CODE") != code
        or row.get("STOCK_TYPE") != "1"
        or row.get("STATE_CODE") != "3"
        or any(
            not isinstance(row.get(key), str) or not row[key].strip()
            for key in ("FULL_NAME", "COMPANY_ABBR")
        )
    ):
        raise ValueError("SSE directory has a different stock identity or termination status")
    days = []
    for key in ("LIST_DATE", "DELIST_DATE"):
        value = row.get(key)
        if not isinstance(value, str) or not re.fullmatch(r"\d{8}", value):
            raise ValueError("SSE directory has an invalid explicit date")
        days.append(date(int(value[:4]), int(value[4:6]), int(value[6:])))
    if days[1] <= days[0]:
        raise ValueError("SSE directory termination precedes its listing")
    return days[1]


def parse_stock_delisting_authority(payload: bytes, *, code: str, kind: str = "html") -> date:
    """Read an identified stock's explicit termination day, not its decision day."""
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("unsupported delisting authority security code")
    if kind == "html":
        text = _text(payload)
    elif kind == "pdf":
        text = " ".join(page.extract_text() or "" for page in PdfReader(BytesIO(payload)).pages)
    else:
        raise ValueError("unsupported delisting authority format")
    compact = re.sub(r"\s+", "", text)
    if (
        not re.search(r"(?:证券|股票)代码[：:]" + code + r"(?!\d)", compact)
        or "股票终止上市" not in compact
    ):
        raise ValueError("delisting authority has a different stock identity or scope")
    matches = re.findall(
        r"(?:本所决定自|(?:公司)?(?:A股)?股票将[于自])(\d{4})年(\d{1,2})月(\d{1,2})日(?:起)?"
        r"(?:终止其股票在本所上市交易|依法终止你公司股票在本所上市交易|"
        r"终止在上海证券交易所上市交易|终止上市)",
        compact,
    )
    matches += re.findall(
        r"(?:^|[。；;])终止上市日期[：:](\d{4})年(\d{1,2})月(\d{1,2})日(?:[。；;]|$)", compact
    )
    matches += re.findall(
        r"上海证券交易所将在(\d{4})年(\d{1,2})月(\d{1,2})日"
        r"对(?:本)?公司(?:A股)?股票予以摘牌[，,](?:本)?公司(?:A股)?股票终止上市[。；;]",
        compact,
    )
    days = {date(*map(int, match)) for match in matches}
    if len(days) != 1:
        raise ValueError("delisting authority has no unique explicit stock termination day")
    return days.pop()


def parse_delisting_factbook_text(text: str, *, code: str, year: int) -> date:
    """Read an exchange yearbook's termination column, never its notice column."""
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("unsupported factbook security code")
    heading = rf"(?m)^[ \t]*{year}年终止上市的公司[ \t]*$"
    sections = []
    for match in re.finditer(heading, text):
        scope = text[match.end() :]
        end = re.search(r"(?m)^[ \t]*\d{4}年[^\n]*公司[ \t]*$", scope)
        sections.append(scope[: end.start()] if end else scope)
    if len(sections) != 1:
        raise ValueError("factbook has no unique requested-year termination table")
    lines = [line.strip() for line in sections[0].splitlines() if line.strip()]
    header = "股票代码股票简称终止上市公告刊登日终止上市实施起始日"
    if not lines or re.sub(r"\s+", "", lines[0]) != header:
        raise ValueError("factbook does not declare distinct notice and termination columns")
    matches = []
    for line in lines[1:]:
        if not re.match(rf"{code}(?!\d)", line):
            continue
        row = re.fullmatch(
            rf"{code}\s+\S.+?\s+(\d{{4}}-\d{{1,2}}-\d{{1,2}})"
            r"\s+(\d{4}-\d{1,2}-\d{1,2})",
            line,
        )
        if row is None:
            raise ValueError("factbook termination row has ambiguous date cells")
        announced, terminated = (date(*map(int, value.split("-"))) for value in row.groups())
        if announced.year != year or terminated.year != year or terminated < announced:
            raise ValueError("factbook termination row has inconsistent year or chronology")
        matches.append(terminated)
    if len(matches) != 1:
        raise ValueError("factbook has no unique requested security termination row")
    return matches[0]


class _CellTables(_Text):
    """Preserve declared cells and spans, including old implicitly closed rows."""

    def __init__(self):
        super().__init__()
        self.stack = []
        self.tables = []

    def _cell(self):
        state = self.stack[-1]
        if state["cell"] is not None:
            parts, rowspan, colspan = state["cell"]
            state["row"].append((re.sub(r"\s+", "", "".join(parts)), rowspan, colspan))
            state["cell"] = None

    def _row(self):
        self._cell()
        state = self.stack[-1]
        if state["row"]:
            state["rows"].append(state["row"])
            state["row"] = []

    def handle_starttag(self, tag, attrs):
        super().handle_starttag(tag, attrs)
        if tag == "table":
            self.stack.append({"rows": [], "row": [], "cell": None})
        elif self.stack and tag == "tr":
            self._row()
        elif self.stack and tag in {"td", "th"}:
            self._cell()
            attrs = dict(attrs)
            spans = [int(attrs.get(key, "1")) for key in ("rowspan", "colspan")]
            if any(not 1 <= span <= 1000 for span in spans):
                raise ValueError("HTML table has unsupported cell spans")
            self.stack[-1]["cell"] = ([], *spans)

    def handle_endtag(self, tag):
        if self.stack:
            if tag == "table":
                self._row()
                self.tables.append(self.stack.pop()["rows"])
            elif tag == "tr":
                self._row()
            elif tag in {"td", "th"}:
                self._cell()
        super().handle_endtag(tag)

    def handle_data(self, data):
        if not self.hidden and self.stack and self.stack[-1]["cell"] is not None:
            self.stack[-1]["cell"][0].append(data)


def parse_csi300_mixed_change_table(payload: bytes) -> tuple[frozenset[str], frozenset[str]]:
    """Read mixed-index HTML rows; only an explicit rowspan carries an identity."""
    return _parse_csi300_mixed_change_table(payload, unordered_pairs=False)


def parse_csi300_mixed_pair_change_table(payload: bytes) -> tuple[frozenset[str], frozenset[str]]:
    """Read explicit directed two-cell pairs without assuming code/name order.

    This is an opt-in reader for legacy subheaders that disagree with cells.
    Each direction still requires exactly one literal six-digit stock code
    and one textual name; neither index nor direction can be inferred.
    """
    return _parse_csi300_mixed_change_table(payload, unordered_pairs=True)


def _parse_csi300_mixed_change_table(payload: bytes, *, unordered_pairs: bool):
    parser = _CellTables()
    parser.feed(payload.decode("utf-8"))
    candidates = []
    directions = {"调入": "in", "调出": "out", "纳入": "in", "删除": "out"}
    for rows in parser.tables:
        if len(rows) < 3 or rows[0][0][0] not in {"指数名称", "指数简称"}:
            continue
        header, columns = rows[:2]
        security_pairs = {
            ("股票代码", "股票名称"),
            ("证券代码", "证券名称"),
            ("证券代码", "证券简称"),
            ("股票名称", "股票代码"),
            ("证券名称", "证券代码"),
            ("证券简称", "证券代码"),
        }
        if (
            len(header) != 3
            or header[0][1:] != (2, 1)
            or any(c[1:] != (1, 2) for c in header[1:])
            or {directions.get(c[0]) for c in header[1:]} != {"in", "out"}
            or len(columns) != 4
            or any(c[1:] != (1, 1) for c in columns)
            or any(
                tuple(c[0] for c in columns[start : start + 2]) not in security_pairs
                for start in (0, 2)
            )
        ):
            raise ValueError("mixed CSI table has ambiguous index or directed security columns")
        sides = {"in": [], "out": []}
        identity, remaining = None, 0
        for row in rows[2:]:
            if remaining:
                if len(row) != 4:
                    raise ValueError("mixed CSI table contradicts its declared index rowspan")
                remaining -= 1
                cells = row
            else:
                if len(row) != 5 or row[0][2] != 1:
                    raise ValueError("mixed CSI table has an anonymous or malformed index row")
                identity, remaining = row[0][0], row[0][1] - 1
                cells = row[1:]
                if not identity and any(re.fullmatch(r"\d{6}", c[0]) for c in cells):
                    raise ValueError("mixed CSI table has an anonymous security row")
            if identity not in {"沪深300", "沪深300指数"}:
                continue
            if any(c[1:] != (1, 1) for c in cells):
                raise ValueError("mixed CSI table merges security cells")
            for position, label in enumerate(header[1:]):
                values = [c[0] for c in cells[2 * position : 2 * position + 2]]
                code, name = (
                    values if columns[2 * position][0].endswith("代码") else reversed(values)
                )
                if unordered_pairs:
                    codes = [value for value in values if re.fullmatch(r"\d{6}", value)]
                    names = [
                        value for value in values if re.search(r"[A-Za-z\u4e00-\u9fff]", value)
                    ]
                    if len(codes) != 1 or len(names) != 1:
                        raise ValueError("mixed CSI pair has no unique literal code and name")
                    code, name = codes[0], names[0]
                market = infer_exchange_from_code(code)
                if (
                    not re.fullmatch(r"\d{6}", code)
                    or not name
                    or name == "-"
                    or not is_all_a_symbol(code, market)
                ):
                    raise ValueError("mixed CSI table has an unsupported security identity")
                sides[directions[label[0]]].append(code + "." + market)
        if remaining:
            raise ValueError("mixed CSI table has an unfinished index rowspan")
        if not sides["in"]:
            continue
        if any(len(codes) != len(set(codes)) for codes in sides.values()):
            raise ValueError("mixed CSI table duplicates directed members")
        additions, removals = frozenset(sides["in"]), frozenset(sides["out"])
        if len(additions) != len(removals) or additions & removals:
            raise ValueError("mixed CSI table has overlapping or unequal changes")
        candidates.append((additions, removals))
    if len(candidates) != 1:
        raise ValueError("HTML has no unique declared mixed CSI 300 table")
    return candidates[0]


def parse_csi300_coded_change_table(payload: bytes, *, removed_code: str):
    """Select an explicit coded table by its removal, without combining triggers."""
    if not re.fullmatch(r"\d{6}", removed_code):
        raise ValueError("coded CSI table requires an explicit six-digit removal code")
    parser = _CellTables()
    parser.feed(payload.decode("utf-8"))
    candidates = []
    for rows in parser.tables:
        if not rows or not rows[0] or rows[0][0][0] != "指数代码":
            continue
        header = rows[0]
        if (
            len(rows) < 3
            or len(header) != 4
            or header[:2] != [("指数代码", 2, 1), ("指数简称", 2, 1)]
            or any(len(row) != 6 or any(c[1:] != (1, 1) for c in row) for row in rows[2:])
        ):
            raise ValueError("coded CSI table has malformed index identity columns")
        for row in rows[2:]:
            code, name = row[0][0], row[1][0]
            if (code == "000300") != (name in {"沪深300", "沪深300指数"}):
                raise ValueError("coded CSI table index code and name disagree")
        # Preserve original directions, security order and spans for the strict reader.
        normalized = [header[1:], rows[1], *[row[1:] for row in rows[2:]]]
        table = (
            "<table>"
            + "".join(
                "<tr>"
                + "".join(
                    f'<td rowspan="{rs}" colspan="{cs}">{escape(value)}</td>'
                    for value, rs, cs in row
                )
                + "</tr>"
                for row in normalized
            )
            + "</table>"
        )
        additions, removals = parse_csi300_mixed_change_table(table.encode())
        if any(symbol.split(".")[0] == removed_code for symbol in removals):
            candidates.append((additions, removals))
    if len(candidates) != 1:
        raise ValueError("HTML has no unique coded CSI table for the requested removal")
    return candidates[0]


def parse_csi300_signed_effective_date(payload: bytes) -> date:
    """Resolve a month/day only within an explicitly signed original notice year."""
    text = re.sub(r"\s+", "", _text(payload))
    if re.search(r"[于自]\d{4}年\d{1,2}月\d{1,2}日", text):
        raise ValueError("CSI signed month/day cannot override an explicit full adjustment date")
    digits = str.maketrans("〇零○OＯ一二三四五六七八九", "00000123456789")

    def number(value):
        if value.isdecimal():
            return int(value)
        if not re.fullmatch(r"(?:[一二三]?十[一二三四五六七八九]?|[一二三四五六七八九])", value):
            raise ValueError("CSI signed date has an unsupported Chinese day or month")
        if "十" in value:
            tens, ones = value.split("十")
            return 10 * (int(tens.translate(digits)) if tens else 1) + (
                int(ones.translate(digits)) if ones else 0
            )
        return int(value.translate(digits))

    signatures = {
        date(int(year.translate(digits)), number(month), number(day))
        for year, month, day in re.findall(
            r"中证指数有限公司([0-9〇零○OＯ一二三四五六七八九]{4})年"
            r"([0-9一二三四五六七八九十]{1,3})月([0-9一二三四五六七八九十]{1,3})日",
            text,
        )
    }
    matches = set(re.findall(r"(?:自(\d{1,2})月(\d{1,2})日起|于(\d{1,2})月(\d{1,2})日调整)", text))
    matches = {tuple(filter(None, match)) for match in matches}
    if len(signatures) != 1 or len(matches) != 1 or "沪深300" not in text:
        raise ValueError("CSI original has no unique signed year and month/day adjustment")
    signed = signatures.pop()
    effective = date(signed.year, *map(int, matches.pop()))
    if effective < signed:
        raise ValueError("CSI signed month/day would require guessing a different year")
    return effective
