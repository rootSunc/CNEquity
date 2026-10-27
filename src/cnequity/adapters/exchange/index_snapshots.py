"""Dated publisher membership/weight workbooks; weights are ratios, not percent."""

import io
import math
from datetime import date

import pandas as pd
import polars as pl

from cnequity.domain.symbols import is_all_a_symbol

# None: membership varies (the SSE Composite holds every listed SH share, B
# shares included), so only a floor is checked.
INDEX_COUNTS = {
    "000001.SH": None,
    "000300.SH": 300,
    "000688.SH": 50,
    "399001.SZ": 500,
    "399006.SZ": 100,
}
_MIN_VARIABLE_MEMBERS = 1000
CNI_WEIGHT_URL = "https://www.cnindex.com.cn/sample-detail/download-history"
CSI_ROOT = "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile"


def snapshot_requests(index_symbol: str):
    if index_symbol not in INDEX_COUNTS:
        raise ValueError(f"unsupported official index snapshot: {index_symbol}")
    code = index_symbol.split(".")[0]
    if index_symbol.endswith(".SZ"):
        return [(CNI_WEIGHT_URL, {"indexcode": code}, True, "cni")]
    return [
        (f"{CSI_ROOT}/{kind}/{code}{kind}.xls", {}, kind == "closeweight", "exchange")
        for kind in ("cons", "closeweight")
    ]


def parse_index_snapshot(payload: bytes, index_symbol: str, *, weights: bool) -> pl.DataFrame:
    if index_symbol not in INDEX_COUNTS:
        raise ValueError("unsupported official index snapshot")
    sheet = pd.read_excel(io.BytesIO(payload))
    if index_symbol.endswith(".SZ"):
        columns = ("日期", "样本代码", None, None, "权重（%）")
    else:
        columns = (
            "日期Date",
            "成份券代码Constituent Code",
            "交易所Exchange",
            "指数代码 Index Code",
            "权重(%)weight",
        )
    day_col, code_col, exchange_col, index_col, weight_col = columns
    required = {c for c in columns[:-1] if c}
    if weights:
        required.add(weight_col)
    expected = INDEX_COUNTS[index_symbol]
    complete = len(sheet) == expected if expected else len(sheet) >= _MIN_VARIABLE_MEMBERS
    if not required <= set(sheet.columns) or not complete:
        raise ValueError("official index workbook has missing columns or incomplete membership")
    rows = []
    for record in sheet.to_dict(orient="records"):
        raw_day = str(record[day_col])
        day = date.fromisoformat(
            raw_day if "-" in raw_day else f"{raw_day[:4]}-{raw_day[4:6]}-{raw_day[6:8]}"
        )
        code = str(record[code_col]).zfill(6)
        if index_col and str(record[index_col]).zfill(6) != index_symbol.split(".")[0]:
            raise ValueError("official workbook identifies a different index")
        exchange = (
            {"上海证券交易所": "SH", "深圳证券交易所": "SZ"}.get(record.get(exchange_col))
            if exchange_col
            else "SZ"
        )
        b_share = index_symbol == "000001.SH" and exchange == "SH" and code.startswith("900")
        if exchange is None or not (is_all_a_symbol(code, exchange) or b_share):
            raise ValueError("official workbook has an unsupported constituent")
        weight = float(record[weight_col]) / 100 if weights else None
        if weights and (not math.isfinite(weight) or not 0 <= weight <= 1):
            raise ValueError("official workbook weight is outside 0..100 percent")
        rows.append(
            {
                "index_symbol": index_symbol,
                "symbol": f"{code}.{exchange}",
                "as_of_date": day,
                "weight": weight,
            }
        )
    frame = pl.DataFrame(
        rows,
        schema={
            "index_symbol": pl.Utf8,
            "symbol": pl.Utf8,
            "as_of_date": pl.Date,
            "weight": pl.Float64,
        },
    )
    if frame["as_of_date"].n_unique() != 1 or frame["symbol"].n_unique() != frame.height:
        raise ValueError("official workbook mixes dates or duplicates constituents")
    if weights and not 0.99 <= frame["weight"].sum() <= 1.01:
        raise ValueError("official workbook weights do not sum to 100 percent")
    return frame


def fetch_official_books(
    index_symbol: str,
    trade_date: date,
    previous_session: date | None,
    *,
    client,
    config=None,
) -> tuple[pl.DataFrame, list[pl.DataFrame]]:
    """The publisher's membership for *trade_date*, and its dated weight books.

    Membership is CSI's current constituent file for CSI indices and CNI's
    adjustment spells expanded to *trade_date* for CNI ones. A CSI file is
    accepted when dated *trade_date* or *previous_session* (it may not have
    been refreshed by the time the daily run reads it) and is recorded on
    *trade_date*. Weight books keep their own month-end date, and only one
    dated on or before *trade_date* is returned. Rows carry their ``source``:
    ``exchange`` for CSI files, ``cni`` / ``cni_weights`` for CNI.
    """
    from cnequity.domain.http_policy import record_http_response
    from cnequity.domain.rate_limit import source_request

    def get(url, params, lane):
        with source_request(config, lane):
            response = client.get(url, params=params)
            record_http_response(config, lane, response)
        response.raise_for_status()
        return response.content

    weights: list[pl.DataFrame] = []
    if index_symbol.endswith(".SZ"):
        from cnequity.adapters.cni.index_constituents_history import (
            expand_cni_constituents_as_of,
            fetch_cni_index_adjustments,
        )

        adjustments = fetch_cni_index_adjustments(index_symbol, client=client, config=config)
        members = expand_cni_constituents_as_of(adjustments, [trade_date]).select(
            "index_symbol", "symbol", "as_of_date", pl.lit(None, dtype=pl.Float64).alias("weight")
        )
        expected = INDEX_COUNTS[index_symbol]
        if members.height != expected:
            raise ValueError(
                f"{index_symbol}: CNI adjustments give {members.height} members, expected {expected}"
            )
        members = members.with_columns(pl.lit("cni").alias("source"))
        for url, params, _weights, lane in snapshot_requests(index_symbol):
            book = parse_index_snapshot(get(url, params, lane), index_symbol, weights=True)
            if book["as_of_date"][0] <= trade_date:
                weights.append(book.with_columns(pl.lit("cni_weights").alias("source")))
        return members, weights

    members = None
    for url, params, is_weights, lane in snapshot_requests(index_symbol):
        book = parse_index_snapshot(get(url, params, lane), index_symbol, weights=is_weights)
        day = book["as_of_date"][0]
        book = book.with_columns(pl.lit("exchange").alias("source"))
        if is_weights:
            if day <= trade_date:
                weights.append(book)
            continue
        if day != trade_date and day != previous_session:
            raise ValueError(
                f"{index_symbol}: CSI constituent file is dated {day}, not {trade_date}"
            )
        members = book.with_columns(pl.lit(trade_date).alias("as_of_date"))
    if members is None:
        raise ValueError(f"{index_symbol}: CSI served no constituent file")
    return members, weights
