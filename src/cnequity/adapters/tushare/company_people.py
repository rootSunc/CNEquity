"""Executives, their pay and holdings, and investor Q&A, from Tushare.

* ``executives`` (``stk_managers``): every board member, supervisor, executive
  and committee member a company has announced, with term start and end.
  The source has no natural key; ``record_id`` is a hash of every field.
* ``executive_compensation`` (``stk_rewards``): pay (yuan) and shares held
  per person, title and report period, as each annual report announced it.
  Many rows have no title, so ``record_id`` hashes stock, period, name,
  title and announcement date.
* ``investor_qa`` (``irm_qa_sh`` from 2023-04, ``irm_qa_sz`` from 2022):
  questions investors asked on the exchanges' platforms (上证e互动, 互动易)
  and the company's answers. The same answer is often served on several
  days, so ``qa_id`` hashes the stock, time, question and answer, and the
  earliest day wins.

The per-person APIs ignore dates, so a fetch reads one company's history.
"""

from __future__ import annotations

import hashlib
from datetime import date

import polars as pl

from cnequity.adapters.tushare.client import SOURCE, TushareClient
from cnequity.domain.bse_codes import legacy_symbol_map

__all__ = [
    "INVESTOR_QA_FLOOR",
    "SOURCE",
    "fetch_executive_compensation",
    "fetch_executives",
    "fetch_investor_qa",
    "normalize_executive_compensation",
    "normalize_executives",
    "normalize_investor_qa",
]

INVESTOR_QA_FLOOR = date(2022, 1, 1)

EXECUTIVES_SCHEMA = {
    "record_id": pl.Utf8,
    "symbol": pl.Utf8,
    "ann_date": pl.Date,
    "name": pl.Utf8,
    "gender": pl.Utf8,
    "role_group": pl.Utf8,
    "title": pl.Utf8,
    "education": pl.Utf8,
    "nationality": pl.Utf8,
    "birth_year": pl.Utf8,
    "start_date": pl.Date,
    "end_date": pl.Date,
}
EXECUTIVE_COMPENSATION_SCHEMA = {
    "record_id": pl.Utf8,
    "symbol": pl.Utf8,
    "period_end": pl.Date,
    "name": pl.Utf8,
    "title": pl.Utf8,
    "ann_date": pl.Date,
    "pay": pl.Float64,
    "shares_held": pl.Float64,
}
INVESTOR_QA_SCHEMA = {
    "qa_id": pl.Utf8,
    "symbol": pl.Utf8,
    "exchange": pl.Utf8,
    "trade_date": pl.Date,
    "published_at": pl.Utf8,
    "question": pl.Utf8,
    "answer": pl.Utf8,
    "industry": pl.Utf8,
}

_PAGE_SIZE = {"stk_managers": 4000, "stk_rewards": 3000, "irm_qa_sh": 1000, "irm_qa_sz": 1000}


def _day(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False)


def _text(column: str) -> pl.Expr:
    value = pl.col(column).cast(pl.Utf8).str.strip_chars()
    return pl.when(value == "").then(None).otherwise(value).alias(column)


def _symbol() -> pl.Expr:
    legacy_bj = legacy_symbol_map()
    return _text("ts_code").replace(legacy_bj).alias("symbol")


def _frame(names: list[str], items: list[list]) -> pl.DataFrame:
    return pl.DataFrame(items, schema=names, orient="row", infer_schema_length=None)


def _hash_rows(frame: pl.DataFrame, fields: list[str]) -> pl.Series:
    return pl.Series(
        [
            hashlib.sha1(
                "\x1f".join("" if v is None else str(v) for v in row).encode()
            ).hexdigest()[:20]
            for row in frame.select(fields).iter_rows()
        ],
        dtype=pl.Utf8,
    )


def normalize_executives(names: list[str], items: list[list]) -> pl.DataFrame:
    """``stk_managers`` rows as ``executives`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=EXECUTIVES_SCHEMA)
    frame = (
        _frame(names, items)
        .select(
            _symbol(),
            _day("ann_date").alias("ann_date"),
            _text("name"),
            _text("gender"),
            _text("lev").alias("role_group"),
            _text("title"),
            _text("edu").alias("education"),
            _text("national").alias("nationality"),
            _text("birthday").alias("birth_year"),
            _day("begin_date").alias("start_date"),
            _day("end_date").alias("end_date"),
        )
        .filter(pl.col("symbol").is_not_null() & pl.col("name").is_not_null())
    )
    fields = [c for c in EXECUTIVES_SCHEMA if c != "record_id"]
    frame = frame.unique(fields, keep="first", maintain_order=True)
    return (
        frame.with_columns(_hash_rows(frame, fields).alias("record_id"))
        .select(list(EXECUTIVES_SCHEMA))
        .cast(EXECUTIVES_SCHEMA)
    )


def normalize_executive_compensation(names: list[str], items: list[list]) -> pl.DataFrame:
    """``stk_rewards`` rows as ``executive_compensation`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=EXECUTIVE_COMPENSATION_SCHEMA)
    identity = ["symbol", "period_end", "name", "title", "ann_date"]
    frame = (
        _frame(names, items)
        .select(
            _symbol(),
            _day("end_date").alias("period_end"),
            _text("name"),
            _text("title"),
            _day("ann_date").alias("ann_date"),
            pl.col("reward").cast(pl.Float64, strict=False).alias("pay"),
            pl.col("hold_vol").cast(pl.Float64, strict=False).alias("shares_held"),
        )
        .filter(
            pl.col("symbol").is_not_null()
            & pl.col("period_end").is_not_null()
            & pl.col("name").is_not_null()
        )
        .unique(identity, keep="last", maintain_order=True)
    )
    return (
        frame.with_columns(_hash_rows(frame, identity).alias("record_id"))
        .select(list(EXECUTIVE_COMPENSATION_SCHEMA))
        .cast(EXECUTIVE_COMPENSATION_SCHEMA)
    )


def normalize_investor_qa(exchange: str, names: list[str], items: list[list]) -> pl.DataFrame:
    """``irm_qa_sh`` (*exchange* ``SH``) or ``irm_qa_sz`` (``SZ``) rows."""
    if not items:
        return pl.DataFrame(schema=INVESTOR_QA_SCHEMA)
    frame = _frame(names, items)
    if "industry" not in frame.columns:
        frame = frame.with_columns(pl.lit(None, dtype=pl.Utf8).alias("industry"))
    frame = (
        frame.select(
            _symbol(),
            pl.lit(exchange).alias("exchange"),
            _day("trade_date").alias("trade_date"),
            _text("pub_time").alias("published_at"),
            _text("q").alias("question"),
            _text("a").alias("answer"),
            _text("industry"),
        )
        .filter(pl.col("symbol").is_not_null() & pl.col("trade_date").is_not_null())
        .sort("trade_date", maintain_order=True)
    )
    identity = ["symbol", "published_at", "question", "answer"]
    frame = frame.unique(identity, keep="first", maintain_order=True)
    return (
        frame.with_columns(_hash_rows(frame, identity).alias("qa_id"))
        .select(list(INVESTOR_QA_SCHEMA))
        .cast(INVESTOR_QA_SCHEMA)
    )


def _run(api: str, params: dict, normalize, config, client: TushareClient | None):
    if client is not None:
        return normalize(*client.all_rows(api, params, "", page_size=_PAGE_SIZE[api]))
    with TushareClient(config) as own:
        return normalize(*own.all_rows(api, params, "", page_size=_PAGE_SIZE[api]))


def fetch_executives(symbol: str, *, config, client: TushareClient | None = None):
    """One company's announced people. Raises without a token."""
    return _run("stk_managers", {"ts_code": symbol}, normalize_executives, config, client)


def fetch_executive_compensation(symbol: str, *, config, client: TushareClient | None = None):
    """One company's pay and holdings history. Raises without a token."""
    return _run(
        "stk_rewards", {"ts_code": symbol}, normalize_executive_compensation, config, client
    )


def fetch_investor_qa(day: date, *, config, client: TushareClient | None = None):
    """Both exchanges' answers published on one day. Raises without a token."""
    params = {"trade_date": day.strftime("%Y%m%d")}

    def run(c: TushareClient) -> pl.DataFrame:
        frames = [
            normalize_investor_qa(exchange, *c.all_rows(api, params, "", page_size=_PAGE_SIZE[api]))
            for exchange, api in (("SH", "irm_qa_sh"), ("SZ", "irm_qa_sz"))
        ]
        return pl.concat(frames)

    if client is not None:
        return run(client)
    with TushareClient(config) as own:
        return run(own)
