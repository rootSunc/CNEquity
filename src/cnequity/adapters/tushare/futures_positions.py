"""Daily futures member position rankings from Tushare (``fut_holding``).

Exchanges publish, per contract and session, the top members by volume, long
open interest and short open interest. Tushare joins the three lists into one
row per member; a member outside one list has nulls there.

Besides contracts, the source carries product-level rows, stored with
``level = "product"`` and ``symbol`` = ``<product>.<exchange>``:

* SHFE and INE category totals (members ``期货公司`` and ``非期货公司`` and
  their variants), sent under the bare product code;
* product rankings: SHFE's 2015-03..2016-12 ``<product>ACTV`` rows (the sum
  of the product's active contracts) and CZCE's ``<中文名><product>`` rows
  (a day that also sends the bare code keeps the named copy).

Contract codes are stored in the lake's form (``AG2606.SHF``); the few
three-digit CZCE codes are widened to four digits from the trade date.
Member names have their full-width brackets made half-width. Volumes and
open interest are in contracts.
"""

from __future__ import annotations

import re
from datetime import date

import polars as pl

from cnequity.adapters.tushare.client import SOURCE, TushareClient
from cnequity.domain.futures_products import PRODUCTS

__all__ = [
    "FUTURES_MEMBER_POSITIONS_FLOOR",
    "SOURCE",
    "contract_symbol",
    "fetch_futures_member_positions",
    "normalize_futures_member_positions",
]

FUTURES_MEMBER_POSITIONS_FLOOR = date(2010, 1, 4)

_PAGE_SIZE = 5000
_EXCHANGE = {spec.product.upper(): spec.exchange for spec in PRODUCTS}
_ALIASES = {"PTA": "TA"}
_CODE = re.compile(r"^(?:[^A-Z]*?)([A-Z]+?)(ACTV)?(\d{3,4})?$")

FUTURES_MEMBER_POSITIONS_SCHEMA = {
    "trade_date": pl.Date,
    "exchange": pl.Utf8,
    "product": pl.Utf8,
    "symbol": pl.Utf8,
    "level": pl.Utf8,
    "member": pl.Utf8,
    "volume": pl.Int64,
    "volume_change": pl.Int64,
    "long_open_interest": pl.Int64,
    "long_change": pl.Int64,
    "short_open_interest": pl.Int64,
    "short_change": pl.Int64,
}


def contract_symbol(code: str, trade_date: date) -> tuple[str, str, str, str] | None:
    """``(exchange, product, symbol, level)`` for one source code, or ``None``.

    >>> contract_symbol("ag2606", date(2026, 1, 5))
    ('SHF', 'AG', 'AG2606.SHF', 'contract')
    >>> contract_symbol("棉花CF", date(2024, 6, 5))
    ('CZC', 'CF', 'CF.CZC', 'product')
    """
    match = _CODE.match(code.strip().upper())
    if match is None:
        return None
    product, _active, digits = match.groups()
    product = _ALIASES.get(product, product)
    exchange = _EXCHANGE.get(product)
    if exchange is None:
        return None
    if digits is None:
        return exchange, product, f"{product}.{exchange}", "product"
    if len(digits) == 3:
        year = trade_date.year // 10 * 10 + int(digits[0])
        if year < trade_date.year:
            year += 10
        digits = f"{year % 100:02d}{digits[1:]}"
    return exchange, product, f"{product}{digits}.{exchange}", "contract"


def normalize_futures_member_positions(
    names: list[str], items: list[list], trade_date: date
) -> pl.DataFrame:
    """One session's ``fut_holding`` rows as ``futures_member_positions`` columns."""
    if not items:
        return pl.DataFrame(schema=FUTURES_MEMBER_POSITIONS_SCHEMA)
    frame = pl.DataFrame(items, schema=names, orient="row", infer_schema_length=None)
    codes = frame["symbol"].cast(pl.Utf8).unique().drop_nulls().to_list()
    mapped = {code: contract_symbol(code, trade_date) for code in codes}
    lookup = pl.DataFrame(
        [(code, *m) for code, m in mapped.items() if m is not None],
        schema=["symbol", "exchange", "product", "lake_symbol", "level"],
        orient="row",
    )

    def _int(column: str) -> pl.Expr:
        return pl.col(column).cast(pl.Float64, strict=False).round(0).cast(pl.Int64)

    member = pl.col("broker").cast(pl.Utf8).str.strip_chars()
    return (
        frame.with_columns(pl.col("symbol").cast(pl.Utf8))
        .join(lookup, on="symbol", how="inner")
        .with_columns(pl.col("symbol").alias("source_code"))
        .select(
            pl.lit(trade_date).alias("trade_date"),
            "exchange",
            "product",
            pl.col("lake_symbol").alias("symbol"),
            "level",
            member.str.replace_all("（", "(").str.replace_all("）", ")").alias("member"),
            _int("vol").alias("volume"),
            _int("vol_chg").alias("volume_change"),
            _int("long_hld").alias("long_open_interest"),
            _int("long_chg").alias("long_change"),
            _int("short_hld").alias("short_open_interest"),
            _int("short_chg").alias("short_change"),
            # CZCE product rankings sometimes arrive twice, as ``AP`` and ``苹果AP``.
            pl.col("source_code").str.contains(r"[^A-Za-z0-9 ]").alias("_named"),
        )
        .filter(pl.col("member").is_not_null() & (pl.col("member") != ""))
        .sort("_named", maintain_order=True)
        .unique(["trade_date", "symbol", "member"], keep="last", maintain_order=True)
        .select(list(FUTURES_MEMBER_POSITIONS_SCHEMA))
        .cast(FUTURES_MEMBER_POSITIONS_SCHEMA)
    )


def fetch_futures_member_positions(
    day: date, *, config, client: TushareClient | None = None
) -> pl.DataFrame:
    """Every exchange's rankings for one session. Raises without a token."""
    params = {"trade_date": day.strftime("%Y%m%d")}

    def run(c: TushareClient) -> pl.DataFrame:
        names, items = c.all_rows("fut_holding", params, "", page_size=_PAGE_SIZE)
        return normalize_futures_member_positions(names, items, day)

    if client is not None:
        return run(client)
    with TushareClient(config) as own:
        return run(own)
