"""Whole-market quarterly statements from 通达信's ``tdxfin`` packages.

通达信 publishes one zip per report period at ``down.tdx.com.cn:8001/tdxfin``,
indexed with an MD5 in ``gpcw.txt``, from 1989 through the quarter now being
disclosed. Each holds one ``.dat`` file: a header, an 11-byte directory entry
per security, and 584 little-endian float32 fields per security. No account
or token is needed, and one request covers the whole market, where the
EastMoney datacenter needs a page per report family.

Measured 2026-10-26 against the lake's EastMoney rows for 2016Q3, 2024Q4 and
2025Q2 (every listed security, 0.1% tolerance), the mapped fields agreed on
93-99.9% of values, and field 314 matched the first-disclosure date on 96-98%
of securities.

**Values are current, dates are first-disclosure.** 通达信 rewrites old
packages (the 2025Q2 zip was regenerated on 2026-10-26) and a restated filing
replaces the original figure. Of 161 net-profit disagreements in 2024Q4, the
同花顺 peer sided with EastMoney 11 times in 12, and those securities carried
a restatement notice five times as often as the rest (17% vs 3.3%). So a row is
labelled ``tdx_fin`` only when its disclosure is recent at capture; anything
older is ``tdx_fin_backfill``, which strict PIT excludes.

**Coverage is narrower than the lake's.** Field 74 is 营业收入, not the
营业总收入 the lake's ``revenue`` holds, so ``revenue`` is not mapped; neither
are the ``indicator`` ratios, which agreed less often. A zero is how the
format spells "absent", so zeros are dropped rather than stored.
"""

from __future__ import annotations

import hashlib
import io
import struct
import zipfile
from datetime import date, timedelta

import httpx
import polars as pl

from cnequity.domain.http_policy import record_http_response
from cnequity.domain.rate_limit import source_request
from cnequity.domain.symbols import format_symbol, infer_exchange_from_code, is_all_a_symbol

SOURCE = "tdx_fin"
BACKFILL_SOURCE = "tdx_fin_backfill"
BASE_URL = "http://down.tdx.com.cn:8001/tdxfin/"
INDEX_NAME = "gpcw.txt"

#: A disclosure at most this old when captured is labelled ``tdx_fin``. The
#: fundamentals group may run weekly; two weeks absorbs one missed run.
FRESH_DAYS = 14

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; cnequity/0.1)"}
_HEADER = struct.Struct("<hIH3L")
_ENTRY = struct.Struct("<6scL")
_ANNOUNCE_FIELD = 314

#: lake item_code -> (statement_type, 1-based package field). Each agreed with
#: the lake on at least 93% of securities in every period measured.
FIELD_MAP: dict[str, tuple[str, int]] = {
    "monetary_funds": ("balance", 8),
    "accounts_receivable": ("balance", 11),
    "inventory": ("balance", 17),
    "fixed_assets": ("balance", 27),
    "total_assets": ("balance", 40),
    "total_liabilities": ("balance", 63),
    "total_equity": ("balance", 72),
    "operating_cost": ("income", 75),
    "sale_expense": ("income", 77),
    "manage_expense": ("income", 78),
    "finance_expense": ("income", 80),
    "operating_profit": ("income", 86),
    "total_profit": ("income", 92),
    "income_tax": ("income", 93),
    "net_profit": ("income", 96),
    "net_profit_deducted": ("income", 206),
    "net_cash_operate": ("cashflow", 107),
    "capex": ("cashflow", 114),
    "net_cash_invest": ("cashflow", 119),
    "net_cash_finance": ("cashflow", 128),
}

ROW_SCHEMA = {
    "symbol": pl.Utf8,
    "report_period": pl.Utf8,
    "statement_type": pl.Utf8,
    "item_code": pl.Utf8,
    "item_value": pl.Float64,
    "announce_date": pl.Date,
    "source": pl.Utf8,
}


class TdxFinPayloadError(RuntimeError):
    """A package or its index does not have the shape this parser expects."""


def period_end(period: str) -> date:
    """``2025Q2`` -> 2025-06-30."""
    year, quarter = int(period[:4]), int(period[5])
    month = quarter * 3
    return date(year, month, 30 if month in (6, 9) else 31)


def package_name(period: str) -> str:
    return f"gpcw{period_end(period):%Y%m%d}.zip"


def latest_periods(day: date, count: int = 2) -> list[str]:
    """The *count* report periods ending on or before *day*, newest first."""
    year, quarter = day.year, (day.month - 1) // 3 + 1
    if day < period_end(f"{year}Q{quarter}"):
        quarter -= 1
    out = []
    while len(out) < count:
        if quarter == 0:
            year, quarter = year - 1, 4
        out.append(f"{year}Q{quarter}")
        quarter -= 1
    return out


def _get(client: httpx.Client | None, config, name: str) -> httpx.Response:
    session = client if client is not None else httpx.Client(timeout=120.0)
    try:
        with source_request(config, SOURCE):
            response = session.get(BASE_URL + name, headers=_HEADERS)
            record_http_response(config, SOURCE, response)
        response.raise_for_status()
        return response
    finally:
        if client is None:
            session.close()


def fetch_index(*, client: httpx.Client | None = None, config=None) -> dict[str, str]:
    """``{zip name: md5}`` for every package 通达信 currently serves."""
    text = _get(client, config, INDEX_NAME).text
    index = {}
    for line in text.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) >= 2 and parts[0].startswith("gpcw") and parts[0].endswith(".zip"):
            index[parts[0]] = parts[1].lower()
    if not index:
        raise TdxFinPayloadError("tdxfin index lists no packages")
    return index


def fetch_package(
    period: str,
    *,
    index: dict[str, str],
    client: httpx.Client | None = None,
    config=None,
    archive=None,
    run_id: str | None = None,
    request_scope: str | None = None,
    archive_dataset: str = "financial_statement_items",
) -> bytes | None:
    """The zip for *period*, checked against the index MD5; None if not served."""
    name = package_name(period)
    expected = index.get(name)
    if expected is None:
        return None
    response = _get(client, config, name)
    content = response.content
    if hashlib.md5(content).hexdigest() != expected:
        raise TdxFinPayloadError(f"{name}: MD5 does not match the tdxfin index")
    if archive is not None:
        archive.archive(
            archive_dataset,
            content,
            source=SOURCE,
            run_id=run_id,
            url=str(response.request.url),
            response_status=response.status_code,
            payload_format="bytes",
            http_metadata={"wire_exact": True, "protocol": "http"},
            observation_id=f"{run_id or 'anonymous'}:{name}:{expected}:scope={request_scope}",
            request_scope=request_scope,
        )
    return content


def parse_package(content: bytes) -> tuple[date, dict[str, tuple[float, ...]]]:
    """Report date and ``{six-digit code: fields}`` from one package zip."""
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            members = [name for name in zf.namelist() if name.lower().endswith(".dat")]
            if len(members) != 1:
                raise TdxFinPayloadError(f"expected one .dat member, found {len(members)}")
            data = zf.read(members[0])
    except zipfile.BadZipFile as exc:
        raise TdxFinPayloadError(f"not a zip: {exc}") from exc
    if len(data) < _HEADER.size:
        raise TdxFinPayloadError("package shorter than its header")
    _, report_date, count, _, report_size, _ = _HEADER.unpack_from(data)
    field_count = report_size // 4
    if count and field_count < _ANNOUNCE_FIELD:
        raise TdxFinPayloadError(f"{field_count} fields per security, need {_ANNOUNCE_FIELD}")
    try:
        reported = date(report_date // 10000, report_date // 100 % 100, report_date % 100)
    except ValueError as exc:
        raise TdxFinPayloadError(f"bad report date {report_date}") from exc
    record = struct.Struct(f"<{field_count}f")
    out: dict[str, tuple[float, ...]] = {}
    for i in range(count):
        code, _market, offset = _ENTRY.unpack_from(data, _HEADER.size + i * _ENTRY.size)
        if offset + record.size > len(data):
            raise TdxFinPayloadError(f"record {i} runs past the end of the package")
        out[code.decode("ascii", "replace")] = record.unpack_from(data, offset)
    return reported, out


def _announce_date(value: float) -> date | None:
    """Field 314: ``YYMMDD`` (or ``YYYYMMDD``) as a float; 0 when unknown."""
    raw = int(value) if value and value == value else 0
    if raw <= 0:
        return None
    if raw < 1_000_000:
        yy = raw // 10000
        raw += (1900 if yy >= 80 else 2000) * 10000
    try:
        return date(raw // 10000, raw // 100 % 100, raw % 100)
    except ValueError:
        return None


def package_rows(
    period: str,
    content: bytes,
    *,
    observed_on: date,
    fresh_days: int = FRESH_DAYS,
) -> pl.DataFrame:
    """Long ``financial_statement_items`` rows for every A-share in one package.

    A security without a usable disclosure date is skipped rather than given an
    invented one, as is a date before the period closes or after *observed_on*.
    """
    reported, records = parse_package(content)
    closes = period_end(period)
    if reported != closes:
        raise TdxFinPayloadError(f"package for {period} reports {reported}")
    fresh_from = observed_on - timedelta(days=fresh_days)
    rows: list[dict] = []
    for code, fields in records.items():
        exchange = infer_exchange_from_code(code)
        if not is_all_a_symbol(code, exchange):
            continue
        announced = _announce_date(fields[_ANNOUNCE_FIELD - 1])
        if announced is None or announced <= closes or announced > observed_on:
            continue
        source = SOURCE if announced >= fresh_from else BACKFILL_SOURCE
        symbol = format_symbol(code, exchange)
        for item_code, (statement_type, column) in FIELD_MAP.items():
            value = float(fields[column - 1])
            if value == 0.0 or value != value:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "report_period": period,
                    "statement_type": statement_type,
                    "item_code": item_code,
                    "item_value": value,
                    "announce_date": announced,
                    "source": source,
                }
            )
    return pl.DataFrame(rows, schema=ROW_SCHEMA)
