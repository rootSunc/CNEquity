"""Sell-side broker forecasts (Tushare ``report_rc``), only with a Tushare token.

One row per research report × forecast fiscal year: the broker, analysts,
report date and that report's revenue / profit / EPS / P/E / ROE estimates,
rating and target-price band. Unlike ``analyst_consensus`` (a live
EastMoney snapshot with no dates) every row is dated by the day the report
was published, so a consensus at any past date can be rebuilt from the
reports known by then.

Requests go through the shared client to ``[sources.tushare].api_url``. On
2026-11-04, 93–95% of ``report_rc`` EPS values equalled EastMoney's own
report list exactly; the rest differ by EastMoney's rounding.

A report has no identity field. ``forecast_id`` hashes every column the
source returns, as raw JSON values, so an exact repeat collapses and two
genuinely different rows — same broker, same day, different numbers — both
survive.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, datetime, timezone

import polars as pl

from cnequity.adapters.tushare.client import SOURCE, TushareClient
from cnequity.config import Config
from cnequity.domain.schemas import BROKER_FORECASTS_SCHEMA

__all__ = ["BROKER_FORECASTS_FLOOR", "SOURCE", "fetch_broker_forecasts"]

logger = logging.getLogger(__name__)

DATA_VERSION = "v1"
# Tushare documents report_rc from 2010; full days were served from 2016.
BROKER_FORECASTS_FLOOR = date(2016, 1, 1)
# report_rc returns at most 3,000 rows per call; page well under that.
_PAGE_SIZE = 2000
_MAX_PAGES = 50
_FIELDS = (
    "ts_code,name,report_date,report_title,report_type,classify,org_name,author_name,"
    "quarter,op_rt,op_pr,tp,np,eps,pe,rd,roe,ev_ebitda,rating,max_price,min_price"
)
_NUMERIC = (
    "op_rt",
    "op_pr",
    "tp",
    "np",
    "eps",
    "pe",
    "rd",
    "roe",
    "ev_ebitda",
    "max_price",
    "min_price",
)
_TEXT = (
    "name",
    "report_title",
    "report_type",
    "classify",
    "org_name",
    "author_name",
    "quarter",
    "rating",
)


def _forecast_id(record: dict) -> str:
    canonical = json.dumps(
        [record.get(k) for k in _FIELDS.split(",")],
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def _fiscal_year(quarter: object) -> int | None:
    text = str(quarter or "")
    return int(text[:4]) if len(text) >= 4 and text[:4].isdigit() else None


def _to_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None  # NaN → None


def normalize(fields: list[str], items: list[list], *, fetched_at: datetime) -> pl.DataFrame:
    """Source rows → BROKER_FORECASTS_SCHEMA; rows without code or date are dropped."""
    out = []
    for item in items:
        record = dict(zip(fields, item, strict=False))
        code = str(record.get("ts_code") or "").strip().upper()
        raw_day = str(record.get("report_date") or "").strip()
        if not code or len(raw_day) != 8 or not raw_day.isdigit():
            continue
        row = {
            "forecast_id": _forecast_id(record),
            "symbol": code,
            "report_date": date(int(raw_day[:4]), int(raw_day[4:6]), int(raw_day[6:])),
            "fiscal_year": _fiscal_year(record.get("quarter")),
            **{
                k: (str(record[k]).strip() or None) if record.get(k) is not None else None
                for k in _TEXT
            },
            **{k: _to_float(record.get(k)) for k in _NUMERIC},
            "source": SOURCE,
            "data_version": DATA_VERSION,
            "fetched_at": fetched_at,
        }
        out.append(row)
    frame = (
        pl.DataFrame(out, schema=BROKER_FORECASTS_SCHEMA, orient="row")
        if out
        else pl.DataFrame(schema=BROKER_FORECASTS_SCHEMA)
    )
    return frame.unique(subset=["forecast_id"], keep="first", maintain_order=True)


def fetch_broker_forecasts(
    start: date,
    end: date | None = None,
    *,
    config: Config,
    client: TushareClient | None = None,
) -> pl.DataFrame:
    """Every report published on ``start``..``end`` (inclusive), paged.

    Raises ``TushareNotConfigured`` before any request without a token.
    """
    end = end or start
    own = client is None
    client = client or TushareClient(config)
    fetched_at = datetime.now(timezone.utc)
    try:
        fields, items = client.all_rows(
            "report_rc",
            {"start_date": start.strftime("%Y%m%d"), "end_date": end.strftime("%Y%m%d")},
            _FIELDS,
            page_size=_PAGE_SIZE,
            max_pages=_MAX_PAGES,
        )
    finally:
        if own:
            client.close()
    return normalize(fields, items, fetched_at=fetched_at)
