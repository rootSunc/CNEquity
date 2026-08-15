"""Official ST / *ST designations, read from the exchanges themselves.

Both exchanges publish their full listed-security table with the 简称 they
assign, and the ST / *ST prefix is part of that name. One request each, so this
is cheap enough to run as a check rather than a backfill.

Two things measured on 2026-08-01 shape the implementation.

SSE's ``stockType=1`` (主板) and ``stockType=8`` (科创板) downloads **exclude**
risk-warning board names — five ST symbols were missing from both, which would
have produced five phantom disagreements every run. ``stockType=10`` is the
complete A-share table and contains them.

Neither exchange's universe matches a quote vendor's exactly: SSE still listed
two ST names (600355, 603388) that TDX and EastMoney had already dropped, because
an exchange carries a company until formal delisting while a quote feed drops it
when it stops trading. Comparisons must therefore run over the shared universe,
never over one side's full set.
"""

from __future__ import annotations

import io
import logging
import warnings

from cn_market_lake.domain.symbols import format_symbol, is_all_a_symbol

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 60.0
# Must match the `[sources.<name>]` section that gates this adapter:
# `SourceRateLimiters.wait` silently no-ops on an unregistered name, so a
# mismatch here would disable `min_interval_seconds` without any error.
_SOURCE = "exchange"

# stockType=10 is the whole A-share table including the risk-warning board.
SSE_URL = (
    "http://query.sse.com.cn/security/stock/downloadStockListFile.do"
    "?csrcCode=&stockCode=&areaName=&stockType=10"
)
_SSE_HEADERS = {"Referer": "https://www.sse.com.cn/"}

# One-shot xlsx export; the JSON form of the same report paginates at 20 rows
# across ~145 pages.
SZSE_URL = (
    "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1110&TABKEY=tab1&random=0.1"
)
_SZSE_HEADERS = {"Referer": "https://www.szse.cn/"}
_SZSE_CODE_COL = "A股代码"
_SZSE_NAME_COL = "A股简称"


def _client():
    from curl_cffi import requests as cr

    return cr


def is_st_name(name: str | None) -> bool:
    """True when an exchange short name carries the ST / *ST designation.

    Vendor feeds pad names with spaces and NUL bytes, so normalise before
    testing rather than comparing raw.
    """
    if not name:
        return False
    return "ST" in name.replace(" ", "").replace("\x00", "").upper()


def fetch_sse_names(*, config=None) -> dict[str, str]:
    """``{symbol: 简称}`` for every SSE A-share. Empty when unreachable."""
    if config is not None:
        config.rate_limit(_SOURCE)
    try:
        resp = _client().get(
            SSE_URL, headers=_SSE_HEADERS, impersonate="chrome", timeout=_TIMEOUT_SECONDS
        )
        resp.raise_for_status()
        text = resp.content.decode("gbk", "ignore")
    except Exception as exc:
        logger.warning("SSE stock list unavailable: %s", exc)
        return {}

    names: dict[str, str] = {}
    for line in text.splitlines()[1:]:
        fields = [part.strip() for part in line.split("\t") if part.strip()]
        if len(fields) < 2 or not fields[0].isdigit():
            continue
        code = fields[0].zfill(6)
        if is_all_a_symbol(code, "SH"):
            names[format_symbol(code, "SH")] = fields[1]
    if not names:
        logger.warning("SSE stock list returned no usable rows; format may have changed")
    return names


def fetch_szse_names(*, config=None) -> dict[str, str]:
    """``{symbol: 简称}`` for every SZSE A-share. Empty when unreachable."""
    if config is not None:
        config.rate_limit(_SOURCE)
    try:
        import pandas as pd

        resp = _client().get(
            SZSE_URL, headers=_SZSE_HEADERS, impersonate="chrome", timeout=_TIMEOUT_SECONDS
        )
        resp.raise_for_status()
        with warnings.catch_warnings():
            # The export ships without a default style; openpyxl warns and then
            # applies its own, which is fine and not worth surfacing every run.
            warnings.filterwarnings("ignore", message="Workbook contains no default style")
            pdf = pd.read_excel(io.BytesIO(resp.content), dtype={_SZSE_CODE_COL: str})
    except Exception as exc:
        logger.warning("SZSE stock list unavailable: %s", exc)
        return {}

    if _SZSE_CODE_COL not in pdf.columns or _SZSE_NAME_COL not in pdf.columns:
        logger.warning("SZSE stock list missing %s/%s columns", _SZSE_CODE_COL, _SZSE_NAME_COL)
        return {}

    names: dict[str, str] = {}
    for code, name in zip(pdf[_SZSE_CODE_COL], pdf[_SZSE_NAME_COL], strict=False):
        if code is None or name is None:
            continue
        code = str(code).strip().zfill(6)
        if code.isdigit() and is_all_a_symbol(code, "SZ"):
            names[format_symbol(code, "SZ")] = str(name).strip()
    if not names:
        logger.warning("SZSE stock list returned no usable rows; format may have changed")
    return names


def fetch_exchange_names(*, config=None) -> dict[str, str]:
    """Combined SSE + SZSE ``{symbol: 简称}``.

    A partial result is still useful — the caller compares over the shared
    universe — so one exchange being down does not discard the other. Beijing
    (BJ) names are not covered by either download; symbols outside the returned
    universe are simply not compared.
    """
    names = fetch_sse_names(config=config)
    names.update(fetch_szse_names(config=config))
    return names
