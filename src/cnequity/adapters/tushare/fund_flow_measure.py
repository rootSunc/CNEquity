"""Tushare's own per-stock money flow (``moneyflow``), from 2010.

Unlike ``moneyflow_dc`` (EastMoney's measure, which goes into ``fund_flow``),
this is Tushare's computation from tick-level trades: buy and sell volume and
amount for small, medium, large and extra-large orders, plus the net. The
measure differs from EastMoney's, so it lives in ``fund_flow_tushare`` and is
never mixed into ``fund_flow``.

Volumes arrive in lots and amounts in 万元; they are stored as shares and CNY.
Beijing-exchange legacy codes are stored under the current 920xxx code.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.corporate_actions_migration import _code_mapping
from cnequity.adapters.tushare.client import SOURCE, TushareClient

__all__ = ["FUND_FLOW_TUSHARE_FLOOR", "SOURCE", "fetch_fund_flow_measure", "normalize"]

#: First session ``moneyflow`` serves.
FUND_FLOW_TUSHARE_FLOOR = date(2010, 1, 4)

_SIZES = {"sm": "small", "md": "medium", "lg": "large", "elg": "xlarge"}
_PAGE_SIZE = 3000
_COLUMNS: dict[str, tuple[str, float]] = {}
for _src, _dst in _SIZES.items():
    for _side in ("buy", "sell"):
        _COLUMNS[f"{_side}_{_src}_vol"] = (f"{_side}_{_dst}_volume", 100.0)
        _COLUMNS[f"{_side}_{_src}_amount"] = (f"{_side}_{_dst}_amount", 10_000.0)
_COLUMNS["net_mf_vol"] = ("net_volume", 100.0)
_COLUMNS["net_mf_amount"] = ("net_amount", 10_000.0)
_FIELDS = ",".join(["ts_code", "trade_date", *_COLUMNS])
_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    **{dst: pl.Float64 for dst, _ in _COLUMNS.values()},
}


def normalize(names: list[str], items: list[list]) -> pl.DataFrame:
    """Source rows as ``fund_flow_tushare`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=_SCHEMA)
    legacy_bj = {f"{old}.BJ": f"{new}.BJ" for old, new in _code_mapping().items()}
    frame = pl.DataFrame(items, schema=names, orient="row")
    return (
        frame.select(
            pl.col("ts_code").cast(pl.Utf8).str.strip_chars().replace(legacy_bj).alias("symbol"),
            pl.col("trade_date").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False),
            *[
                (pl.col(src).cast(pl.Float64, strict=False) * scale).alias(dst)
                for src, (dst, scale) in _COLUMNS.items()
            ],
        )
        .filter(pl.col("symbol").is_not_null() & pl.col("trade_date").is_not_null())
        .unique(["symbol", "trade_date"], keep="last", maintain_order=True)
        .cast(_SCHEMA)
    )


def fetch_fund_flow_measure(
    trade_date: date, *, config, client: TushareClient | None = None
) -> pl.DataFrame:
    """One session. Raises ``TushareNotConfigured`` without a token."""
    params = {"trade_date": trade_date.strftime("%Y%m%d")}
    if client is not None:
        return normalize(*client.all_rows("moneyflow", params, _FIELDS, page_size=_PAGE_SIZE))
    with TushareClient(config) as own:
        return normalize(*own.all_rows("moneyflow", params, _FIELDS, page_size=_PAGE_SIZE))
