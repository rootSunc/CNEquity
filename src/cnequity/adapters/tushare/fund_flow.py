"""Per-stock money flow from Tushare ``moneyflow_dc`` (EastMoney's measure).

Tushare republishes EastMoney's own per-stock flow, so its rows can sit in
``fund_flow`` next to the lake's EastMoney rows: on 2026-09-18, all five nets
agreed on every one of 5,553 shared stocks. It serves every session from
2023-09-11, which EastMoney itself no longer does, and the medium and small
nets that the 千股千评 fallback leaves null.

Beijing-exchange stocks are listed under their pre-2024 codes (43/83/87xxx)
on older sessions; they are stored under the current 920xxx code, as the rest
of the lake does.

Values arrive in 万元 and are stored in CNY like the rest of the dataset. A
code with every net at zero did not trade (on 2026-11-03, 391 such rows, most
long delisted), so it is dropped rather than recorded as a flat day.
Tushare's ``moneyflow`` is a different, Tushare-computed measure and does not
belong here.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.tushare.client import SOURCE, TushareClient
from cnequity.domain.bse_codes import legacy_symbol_map

__all__ = ["MONEYFLOW_DC_FLOOR", "SOURCE", "fetch_fund_flow", "normalize"]

#: First session ``moneyflow_dc`` serves (probed 2026-11-04: 2023-08-31 is empty).
MONEYFLOW_DC_FLOOR = date(2023, 9, 11)

_FIELDS = "ts_code,trade_date,net_amount,buy_elg_amount,buy_lg_amount,buy_md_amount,buy_sm_amount"
_PAGE_SIZE = 3000
_TEN_THOUSAND = 10_000.0
_NETS = {
    "net_amount": "main_net_inflow",
    "buy_elg_amount": "super_large_net_inflow",
    "buy_lg_amount": "large_net_inflow",
    "buy_md_amount": "medium_net_inflow",
    "buy_sm_amount": "small_net_inflow",
}
_SCHEMA = {"symbol": pl.Utf8, "trade_date": pl.Date, **dict.fromkeys(_NETS.values(), pl.Float64)}


def normalize(names: list[str], items: list[list]) -> pl.DataFrame:
    """Source rows as ``fund_flow`` columns in CNY, minus provenance."""
    if not items:
        return pl.DataFrame(schema=_SCHEMA)
    frame = pl.DataFrame(items, schema=names, orient="row")
    nets = [
        (pl.col(src).cast(pl.Float64, strict=False) * _TEN_THOUSAND).alias(dst)
        for src, dst in _NETS.items()
    ]
    return (
        frame.select(
            pl.col("ts_code")
            .cast(pl.Utf8)
            .str.strip_chars()
            .replace(legacy_symbol_map())
            .alias("symbol"),
            pl.col("trade_date").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False),
            *nets,
        )
        .filter(pl.col("symbol").is_not_null() & pl.col("trade_date").is_not_null())
        .filter(~pl.all_horizontal(pl.col(list(_NETS.values())).fill_null(0.0) == 0.0))
        .unique(["symbol", "trade_date"], keep="last", maintain_order=True)
        .cast(_SCHEMA)
    )


def fetch_fund_flow(
    trade_date: date, *, config, client: TushareClient | None = None
) -> pl.DataFrame:
    """One session's flows. Raises ``TushareNotConfigured`` without a token."""
    params = {"trade_date": trade_date.strftime("%Y%m%d")}
    if client is not None:
        return normalize(*client.all_rows("moneyflow_dc", params, _FIELDS, page_size=_PAGE_SIZE))
    with TushareClient(config) as own:
        return normalize(*own.all_rows("moneyflow_dc", params, _FIELDS, page_size=_PAGE_SIZE))
