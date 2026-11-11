"""Original CSI 300 / STAR 50 books and SSE effective-date adjustment tables."""

import re
from datetime import date
from html.parser import HTMLParser
from io import BytesIO
from xml.etree import ElementTree
from zipfile import BadZipFile, ZipFile

import polars as pl

from cnequity._lazy import lazy_attribute, lazy_module
from cnequity.adapters.calendar.exchange_calendar import load_seed_calendar
from cnequity.adapters.cni.index_announcements import MembershipChange
from cnequity.domain.symbols import is_all_a_symbol

pd = lazy_module("pandas")
PdfReader = lazy_attribute("pypdf", "PdfReader")


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


_MOVED_TO_CSI300 = frozenset(
    {
        "parse_csi300_change_pdf",
        "parse_csi300_change_table",
        "parse_csi300_change_workbook",
        "parse_csi300_coded_change_table",
        "parse_csi300_conditional_delistings",
        "parse_csi300_first_session_calendar_date",
        "parse_csi300_first_session_date",
        "parse_csi300_inline_workbook",
        "parse_csi300_mixed_change_table",
        "parse_csi300_mixed_pair_change_table",
        "parse_csi300_opening",
        "parse_csi300_signed_effective_date",
        "parse_delisting_factbook_text",
        "parse_sse_delisting_directory",
        "parse_stock_delisting_authority",
    }
)


def __getattr__(name: str):
    # The CSI 300 and delisting parsers moved to ``csi300_notices``; research
    # scripts that import them from here keep working.
    if name in _MOVED_TO_CSI300:
        from cnequity.adapters.exchange import csi300_notices

        return getattr(csi300_notices, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
