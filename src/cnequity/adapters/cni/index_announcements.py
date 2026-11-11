"""Strict CNI announcement changes and anchor-checked membership reconstruction.

These parsers do not discover a complete announcement catalog. A matching end
book alone cannot prove that intermediate announcements were not omitted.
"""

import re
from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
from io import BytesIO

import polars as pl

from cnequity._lazy import lazy_attribute
from cnequity.domain.symbols import is_all_a_symbol

PdfReader = lazy_attribute("pypdf", "PdfReader")

_NAMES = {"399001.SZ": ("深证成份指数", "深证成指"), "399006.SZ": ("创业板指数", "创业板指")}
_COUNTS = {"399001.SZ": 500, "399006.SZ": 100, "000688.SH": 50}
_CODE = re.compile(r"(?<!\d)\d{6}(?!\d)")


@dataclass(frozen=True)
class MembershipChange:
    index_symbol: str
    effective_date: date
    additions: frozenset[str]
    removals: frozenset[str]


class _HtmlText(HTMLParser):
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


def _text(payload: bytes, kind: str) -> str:
    if kind == "html":
        parser = _HtmlText()
        parser.feed(payload.decode("utf-8"))
        text = " ".join(parser.parts)
    elif kind == "pdf":
        text = "\n".join(page.extract_text() or "" for page in PdfReader(BytesIO(payload)).pages)
    else:
        raise ValueError("announcement must be HTML or PDF")
    # Keep spaces between numbers. Compacting a page number and the first code
    # on its page would silently drop or change the next member.
    return re.sub(r"\s+", " ", text)


def _heading(value: str) -> str:
    return r"\s*".join(map(re.escape, value))


def _symbols(codes: list[str]) -> frozenset[str]:
    if not codes or len(codes) != len(set(codes)):
        raise ValueError("empty or duplicate announcement changes")
    if any(not is_all_a_symbol(code, "SZ") for code in codes):
        raise ValueError("announcement contains a non-A-share Shenzhen member")
    return frozenset(code + ".SZ" for code in codes)


def parse_delisting_date(payload: bytes, *, code: str) -> date:
    """Resolve a conditional CNI event only from an identified issuer PDF."""
    text = re.sub(r"\s+", "", _text(payload, "pdf"))
    if not re.fullmatch(r"\d{6}", code) or f"证券代码：{code}" not in text:
        raise ValueError("delisting authority has a different issuer identity")
    matches = re.findall(r"(?:将于|将自)(\d{4})年(\d{1,2})月(\d{1,2})日起终止上市", text)
    days = {date(*map(int, match)) for match in matches}
    if len(days) != 1:
        raise ValueError("delisting authority has no unique explicit effective date")
    return days.pop()


def parse_announcement(
    payload: bytes,
    *,
    kind: str,
    index_symbol: str,
    delisting_authority: bytes | None = None,
) -> MembershipChange:
    """Read explicit change tables; never infer dates from URLs or filenames."""
    if index_symbol not in _NAMES:
        raise ValueError("unsupported CNI index")
    text = _text(payload, kind)
    compact = re.sub(r"\s+", "", text)
    dates = {
        date(*map(int, match))
        for match in re.findall(r"决定于(\d{4})年(\d{1,2})月(\d{1,2})日", compact)
    }
    if len(dates) == 1:
        effective = next(iter(dates))
    elif not dates and "决定于中航机电退市日起" in compact and delisting_authority is not None:
        effective = parse_delisting_date(delisting_authority, code="002013")
    else:
        raise ValueError("announcement has no unique supported effective date")
    long_name, short_name = _NAMES[index_symbol]
    title = re.search(_heading(long_name + "样本股调整名单"), text)
    if title:
        scope = text[title.end() :]
        end = re.search(_heading("指数样本股调整名单"), scope)
        if end:
            # The next heading starts with its index name; it carries no codes.
            scope = scope[: end.start()]
        headings = re.sub(r"\s+", "", scope)
        if not headings.startswith("调入名单调出名单证券代码证券简称证券代码证券简称"):
            raise ValueError("announcement table directions or columns differ")
        codes = _CODE.findall(scope)
        if not codes or len(codes) % 2:
            raise ValueError("announcement has an empty or unpaired change table")
        incoming, outgoing = codes[::2], codes[1::2]
    else:
        if "调入股票调出股票证券代码证券简称证券代码证券简称" not in compact:
            raise ValueError("temporary announcement table directions differ")
        matches = list(
            re.finditer(r"(?<!\d)" + index_symbol[:6] + r"(?!\d)\s*" + _heading(short_name), text)
        )
        if not matches:
            raise ValueError("announcement does not contain this index")
        incoming, outgoing = [], []
        for row in matches:
            scope = text[row.end() :]
            end = re.search(r"(?<!\d)(?:399\d{3}|CN\d{4})(?!\d)", scope)
            scope = scope[: end.start()] if end else scope
            codes = _CODE.findall(scope)
            if len(codes) != 2:
                raise ValueError("temporary announcement row needs exactly one explicit pair")
            incoming.append(codes[0])
            outgoing.append(codes[1])
    additions, removals = _symbols(incoming), _symbols(outgoing)
    if additions & removals:
        raise ValueError("announcement adds and removes the same member")
    if not dates and "决定于中航机电退市日起" in compact and removals != {"002013.SZ"}:
        raise ValueError("conditional delisting date does not identify the removed member")
    return MembershipChange(index_symbol, effective, additions, removals)


def reconstruct_membership(
    *,
    index_symbol: str,
    opening_date: date,
    opening_members: frozenset[str],
    closing_date: date,
    closing_members: frozenset[str],
    changes: list[MembershipChange],
) -> pl.DataFrame:
    """Produce whole event-date books only when both anchors and all steps agree.

    The caller must additionally verify catalog completeness and bind each
    anchor/change to its archived public source before publication.
    """
    if index_symbol not in _COUNTS or opening_date >= closing_date:
        raise ValueError("unsupported index or invalid anchor window")
    size = _COUNTS[index_symbol]

    def valid_member(symbol):
        if index_symbol == "000688.SH":
            return bool(re.fullmatch(r"68[89]\d{3}\.SH", symbol)) and is_all_a_symbol(
                symbol[:6], "SH"
            )
        return bool(re.fullmatch(r"\d{6}\.SZ", symbol)) and is_all_a_symbol(symbol[:6], "SZ")

    for book in (opening_members, closing_members):
        if len(book) != size or any(not valid_member(symbol) for symbol in book):
            raise ValueError("anchor membership identity or size differs")
    ordered = sorted(changes, key=lambda change: change.effective_date)
    if len({change.effective_date for change in ordered}) != len(ordered):
        raise ValueError("duplicate event dates need explicit consolidation")
    book = opening_members
    rows = [(index_symbol, member, opening_date, None) for member in sorted(book)]
    for change in ordered:
        if (
            change.index_symbol != index_symbol
            or not opening_date < change.effective_date <= closing_date
        ):
            raise ValueError("change identity or date lies outside the anchor window")
        if (
            not change.additions
            or not change.removals
            or change.removals - book
            or change.additions & book
            or change.additions & change.removals
            or len(change.additions) != len(change.removals)
            or any(not valid_member(symbol) for symbol in change.additions | change.removals)
        ):
            raise ValueError("change contradicts the preceding complete book")
        book = (book - change.removals) | change.additions
        if len(book) != size:
            raise ValueError("change breaks the expected index size")
        rows.extend((index_symbol, member, change.effective_date, None) for member in sorted(book))
    if book != closing_members:
        raise ValueError("reconstructed membership differs from the independent closing book")
    return pl.DataFrame(
        rows,
        schema={
            "index_symbol": pl.String,
            "symbol": pl.String,
            "as_of_date": pl.Date,
            "weight": pl.Float64,
        },
        orient="row",
    )


def parse_change_workbook(
    payload: bytes, *, index_symbol: str, effective_date: date
) -> MembershipChange:
    """Read an old +/- book; the calling catalog must prove its effective date.

    CNI's old workbooks usually do not carry the effective date in their cells.
    Never derive it from a filename or from when the response was captured.
    """
    import pandas as pd  # noqa: PLC0415

    if index_symbol not in _NAMES:
        raise ValueError("unsupported CNI index")
    sheet = pd.read_excel(BytesIO(payload), dtype=str)
    required = {"指数代码", "指数简称", "证券代码", "证券简称", "调整类型"}
    if not required.issubset(sheet.columns):
        raise ValueError("CNI change workbook has different columns")
    rows = sheet[sheet["指数代码"] == index_symbol[:6]]
    if rows.empty or not rows["调整类型"].isin(["+", "-"]).all():
        raise ValueError("CNI change workbook has no supported change types for the index")
    if not rows["指数简称"].eq(_NAMES[index_symbol][1]).all():
        raise ValueError("CNI change workbook has a different index identity")
    additions = _symbols(rows.loc[rows["调整类型"] == "+", "证券代码"].tolist())
    removals = _symbols(rows.loc[rows["调整类型"] == "-", "证券代码"].tolist())
    if additions & removals or len(additions) != len(removals):
        raise ValueError("CNI change workbook has overlapping or unequal changes")
    return MembershipChange(index_symbol, effective_date, additions, removals)


def parse_reviewed_legacy_table(
    payload: bytes,
    *,
    index_symbol: str,
    effective_date: date,
    reviewed_sha256: str,
) -> MembershipChange:
    """Replay a reviewed original with a broken Chinese PDF text layer.

    Date and left/right headings must be independently reviewed in the original
    page and bound to its exact digest by the calling source catalog. This
    function cannot discover dates or prove catalog completeness.
    """
    import hashlib  # noqa: PLC0415

    if index_symbol not in _NAMES:
        raise ValueError("unsupported CNI index")
    if (
        not re.fullmatch(r"[0-9a-f]{64}", reviewed_sha256)
        or hashlib.sha256(payload).hexdigest() != reviewed_sha256
    ):
        raise ValueError("legacy PDF differs from the reviewed original")
    text = _text(payload, "pdf")
    # Keep all numeric delimiters: damaged fonts can leave only the index and
    # member codes. No text-layer guess of effective date or direction is made.
    markers = list(re.finditer(r"(?<!\d)(?:399\d{3}|CN\d{4})(?!\d)", text))
    selected = [i for i, marker in enumerate(markers) if marker.group() == index_symbol[:6]]
    if not selected:
        raise ValueError("legacy PDF has no index row")
    incoming, outgoing = [], []
    for offset in selected:
        start = markers[offset].end()
        end = markers[offset + 1].start() if offset + 1 < len(markers) else len(text)
        codes = _CODE.findall(text[start:end])
        if len(codes) != 2:
            raise ValueError("legacy PDF row has no unique explicit incoming/outgoing pair")
        incoming.append(codes[0])
        outgoing.append(codes[1])
    additions, removals = _symbols(incoming), _symbols(outgoing)
    if additions & removals:
        raise ValueError("legacy PDF adds and removes the same member")
    return MembershipChange(index_symbol, effective_date, additions, removals)


def parse_named_merger(payload: bytes, *, index_symbol: str) -> MembershipChange:
    """Read named merger replacements only when the notice identifies both codes."""
    if index_symbol not in _NAMES:
        raise ValueError("unsupported CNI index")
    text = re.sub(r"\s+", "", _text(payload, "pdf"))
    if _NAMES[index_symbol][0] not in text:
        raise ValueError("merger notice does not identify this index")
    identity = re.search(
        r"鉴于(?P<out>[\u4e00-\u9fff]+)[(（](?P<outcode>\d{6})[)）]"
        r"即将被(?P<in>[\u4e00-\u9fff]+)[(（](?P<incode>\d{6})[)）]吸收合并而退市",
        text,
    )
    if identity is None:
        raise ValueError("merger notice does not identify incoming/outgoing codes")
    pattern = (
        r"于(\d{4})年(\d{1,2})月(\d{1,2})日调入"
        + re.escape(identity["in"])
        + r"[，,]调出"
        + re.escape(identity["out"])
        + r"[。.]"
    )
    dates = {date(*map(int, row)) for row in re.findall(pattern, text)}
    if len(dates) != 1:
        raise ValueError("merger notice has no unique dated replacement direction")
    additions, removals = _symbols([identity["incode"]]), _symbols([identity["outcode"]])
    if additions & removals:
        raise ValueError("merger notice adds and removes the same member")
    return MembershipChange(index_symbol, dates.pop(), additions, removals)
