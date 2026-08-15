"""CNI (国证指数) historical index membership — C2 backfill for SZ indices.

CSI (中证) 000300/000905/… have no free dated membership API in-repo; EM
``TRADE_DATE`` is entry-date on the *current* book, not a full as-of snapshot.
CNI publishes adjustment spreadsheets with [开始日期, 结束日期] spells that
reconstruct membership for 399001/399006 (and peers) from late 2021.
"""

from __future__ import annotations

import io
import logging
from datetime import date

import httpx
import polars as pl

from cn_market_lake.adapters.sw.industry_history import exchange_from_code
from cn_market_lake.domain.symbols import format_symbol, is_all_a_symbol

logger = logging.getLogger(__name__)

__all__ = [
    "CNI_ADJUST_URL",
    "CNI_BACKFILL_INDICES",
    "fetch_cni_index_adjustments",
    "expand_cni_constituents_as_of",
]

CNI_ADJUST_URL = "https://www.cnindex.com.cn/sample-detail/download-adjustment"

# Index codes CNI serves adjustment history for (empty Excel for CSI codes).
CNI_BACKFILL_INDICES: tuple[str, ...] = ("399001.SZ", "399006.SZ")

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; cn-market-lake/0.1)"}


def _index_code(index_symbol: str) -> str:
    return index_symbol.split(".", 1)[0].zfill(6)


def _member_symbol(code: str) -> str | None:
    code = str(code).zfill(6)
    exchange = exchange_from_code(code)
    if not is_all_a_symbol(code, exchange):
        return None
    return format_symbol(code, exchange)


def fetch_cni_index_adjustments(
    index_symbol: str,
    *,
    client: httpx.Client | None = None,
) -> pl.DataFrame:
    """Download CNI adjustment history for one index; empty if unsupported."""
    try:
        import pandas as pd  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "pandas is required to parse CNI adjustment XLSX; "
            "reinstall with `pip install --force-reinstall cn-market-lake` "
            "(or `pip install -e .` from a source checkout)."
        ) from exc

    owns = client is None
    if client is None:
        client = httpx.Client(timeout=120.0, follow_redirects=True)
    try:
        resp = client.get(
            CNI_ADJUST_URL,
            params={"indexcode": _index_code(index_symbol)},
            headers=_HEADERS,
        )
        resp.raise_for_status()
        if not resp.content or len(resp.content) < 100:
            return pl.DataFrame(
                schema={
                    "index_symbol": pl.Utf8,
                    "symbol": pl.Utf8,
                    "start_date": pl.Date,
                    "end_date": pl.Date,
                    "adjust_type": pl.Utf8,
                }
            )
        try:
            pdf = pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")
        except Exception as exc:  # noqa: BLE001 — empty/corrupt payload
            logger.warning("CNI adjust parse failed for %s: %s", index_symbol, exc)
            return pl.DataFrame(
                schema={
                    "index_symbol": pl.Utf8,
                    "symbol": pl.Utf8,
                    "start_date": pl.Date,
                    "end_date": pl.Date,
                    "adjust_type": pl.Utf8,
                }
            )
    finally:
        if owns:
            client.close()

    if pdf.empty:
        return pl.DataFrame(
            schema={
                "index_symbol": pl.Utf8,
                "symbol": pl.Utf8,
                "start_date": pl.Date,
                "end_date": pl.Date,
                "adjust_type": pl.Utf8,
            }
        )

    rename = {
        "开始日期": "start_date",
        "结束日期": "end_date",
        "样本代码": "code",
        "调整类型": "adjust_type",
    }
    pdf = pdf.rename(columns={k: v for k, v in rename.items() if k in pdf.columns})
    for col in ("start_date", "end_date"):
        pdf[col] = pd.to_datetime(pdf[col], errors="coerce").dt.date
    rows: list[dict] = []
    for rec in pdf.to_dict(orient="records"):
        adj = str(rec.get("adjust_type") or "").strip()
        # OLD / + are in-book; '-' removals and 备选 are not active members.
        if adj not in {"OLD", "+"}:
            continue
        sym = _member_symbol(str(rec.get("code") or ""))
        start = rec.get("start_date")
        end = rec.get("end_date")
        if not sym or start is None or end is None:
            continue
        rows.append(
            {
                "index_symbol": index_symbol,
                "symbol": sym,
                "start_date": start,
                "end_date": end,
                "adjust_type": adj,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "index_symbol": pl.Utf8,
                "symbol": pl.Utf8,
                "start_date": pl.Date,
                "end_date": pl.Date,
                "adjust_type": pl.Utf8,
            }
        )
    return pl.DataFrame(rows)


def expand_cni_constituents_as_of(
    adjustments: pl.DataFrame,
    as_of_dates: list[date],
) -> pl.DataFrame:
    """Members where start_date <= as_of < end_date for each as_of."""
    schema = {
        "index_symbol": pl.Utf8,
        "symbol": pl.Utf8,
        "as_of_date": pl.Date,
        "weight": pl.Float64,
    }
    if adjustments.is_empty() or not as_of_dates:
        return pl.DataFrame(schema=schema)
    frames: list[pl.DataFrame] = []
    for as_of in as_of_dates:
        snap = (
            adjustments.filter((pl.col("start_date") <= as_of) & (pl.col("end_date") > as_of))
            .select(
                pl.col("index_symbol"),
                pl.col("symbol"),
                pl.lit(as_of).alias("as_of_date"),
                pl.lit(0.0).alias("weight"),
            )
            .unique(subset=["index_symbol", "symbol", "as_of_date"], keep="last")
        )
        if not snap.is_empty():
            frames.append(snap)
    if not frames:
        return pl.DataFrame(schema=schema)
    return pl.concat(frames)
