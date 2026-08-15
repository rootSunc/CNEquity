"""Shenwan (申万) industry classification history — C2 backfill source.

Source: SwClass2021 ``StockClassifyUse_stock.xls`` (interval rows with 计入日期).
Daily EastMoney industry snapshots stay ``classification_system=eastmoney``;
historical rows use ``classification_system=sw``.
"""

from __future__ import annotations

import functools
import io
import logging
import ssl
from datetime import date
from pathlib import Path

import httpx
import polars as pl

from cn_market_lake.domain.symbols import format_symbol, is_all_a_symbol

logger = logging.getLogger(__name__)

__all__ = [
    "SW_INDUSTRY_XLS_URL",
    "exchange_from_code",
    "fetch_sw_industry_intervals",
    "expand_sw_industry_as_of",
    "sw_ssl_context",
]

SW_INDUSTRY_XLS_URL = (
    "https://www.swsresearch.com/swindex/pdf/SwClass2021/StockClassifyUse_stock.xls"
)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; cn-market-lake/0.1)"}

# swsresearch.com serves its leaf certificate and nothing else — measured over
# six handshakes, the chain is one cert deep every time. Browsers and macOS
# curl paper over that by fetching the issuer named in the leaf's Authority
# Information Access extension; Python does not, so certifi cannot build a path
# to a root it does trust and every request fails with
# "unable to get local issuer certificate". Measured: httpx 0/5, curl_cffi 0/6.
#
# The missing link is a public DigiCert intermediate (GeoTrust G2 TLS CN
# RSA4096 SHA256 2022 CA1, valid to 2032-12-14) whose own issuer — DigiCert
# Global Root G2 — is already in certifi. Shipping it restores the path without
# weakening anything: the root must still be trusted, the chain must still
# verify, and the hostname must still match. `verify=False` would have been the
# one-line alternative and would have turned a server misconfiguration into a
# silently unauthenticated download of the classification history.
#
# When swsresearch rotates CAs this file goes stale and the same error returns.
# Refresh it from the leaf's AIA URL:
#   openssl s_client -connect www.swsresearch.com:443 | openssl x509 -noout -text \
#     | grep -A2 "Authority Information Access"
_SW_INTERMEDIATE_PEM = Path(__file__).with_name("geotrust_g2_tls_cn_rsa4096.pem")


@functools.lru_cache(maxsize=1)
def sw_ssl_context() -> ssl.SSLContext:
    """Default trust store plus the intermediate swsresearch.com omits."""
    ctx = httpx.create_ssl_context()
    try:
        ctx.load_verify_locations(cafile=str(_SW_INTERMEDIATE_PEM))
    except (OSError, ssl.SSLError) as exc:  # pragma: no cover — packaging slip
        logger.warning("Shenwan intermediate cert unusable (%s); TLS may fail", exc)
    return ctx


def sw_client(timeout: float = 120.0) -> httpx.Client:
    return httpx.Client(timeout=timeout, follow_redirects=True, verify=sw_ssl_context())


def exchange_from_code(code: str) -> str:
    if code.startswith(("60", "68")):
        return "SH"
    if code.startswith(("43", "83", "87", "88", "92")):
        return "BJ"
    return "SZ"


def _code_to_symbol(code: str) -> str | None:
    code = str(code).zfill(6)
    exchange = exchange_from_code(code)
    if not is_all_a_symbol(code, exchange):
        return None
    return format_symbol(code, exchange)


def fetch_sw_industry_intervals(*, client: httpx.Client | None = None) -> pl.DataFrame:
    """Download Shenwan stock→industry interval history (one row per classification spell)."""
    try:
        import pandas as pd  # noqa: PLC0415 — optional; Excel parse only
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "pandas is required to parse Shenwan industry XLS; "
            "reinstall with `pip install --force-reinstall cn-market-lake` "
            "(or `pip install -e .` from a source checkout)."
        ) from exc

    owns = client is None
    if client is None:
        client = sw_client()
    try:
        resp = client.get(SW_INDUSTRY_XLS_URL, headers=_HEADERS)
        resp.raise_for_status()
    finally:
        if owns:
            client.close()

    pdf = pd.read_excel(
        io.BytesIO(resp.content),
        dtype={"股票代码": str, "行业代码": str},
    )
    if pdf.empty:
        raise RuntimeError("Shenwan industry XLS returned no rows")

    pdf = pdf.rename(
        columns={
            "股票代码": "code",
            "计入日期": "start_date",
            "行业代码": "industry_code",
            "更新日期": "update_time",
        }
    )
    pdf["start_date"] = pd.to_datetime(pdf["start_date"], errors="coerce").dt.date
    pdf = pdf.dropna(subset=["code", "start_date", "industry_code"])
    rows: list[dict] = []
    for rec in pdf.to_dict(orient="records"):
        sym = _code_to_symbol(str(rec["code"]))
        if not sym:
            continue
        rows.append(
            {
                "symbol": sym,
                "start_date": rec["start_date"],
                "industry_code": str(rec["industry_code"]).strip(),
            }
        )
    if not rows:
        raise RuntimeError("Shenwan industry XLS produced no all_a symbols")
    return pl.DataFrame(rows).sort(["symbol", "start_date"])


def expand_sw_industry_as_of(
    intervals: pl.DataFrame,
    as_of_dates: list[date],
) -> pl.DataFrame:
    """Point-in-time expand: for each as_of, latest start_date <= as_of per symbol."""
    if not as_of_dates:
        return pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "classification_system": pl.Utf8,
                "industry_code": pl.Utf8,
                "industry_name": pl.Utf8,
                "as_of_date": pl.Date,
            }
        )
    frames: list[pl.DataFrame] = []
    for as_of in as_of_dates:
        snap = (
            intervals.filter(pl.col("start_date") <= as_of)
            .sort(["symbol", "start_date"])
            .unique(subset=["symbol"], keep="last")
            .select(
                pl.col("symbol"),
                pl.lit("sw").alias("classification_system"),
                pl.col("industry_code"),
                # Official name map is a separate SW publish; code is stable for joins.
                pl.col("industry_code").alias("industry_name"),
                pl.lit(as_of).alias("as_of_date"),
            )
        )
        if not snap.is_empty():
            frames.append(snap)
    if not frames:
        return pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "classification_system": pl.Utf8,
                "industry_code": pl.Utf8,
                "industry_name": pl.Utf8,
                "as_of_date": pl.Date,
            }
        )
    return pl.concat(frames)
