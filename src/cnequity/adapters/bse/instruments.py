"""The live Beijing listing, enumerated from the exchange's own board.

TDX serves Shanghai and Shenzhen only, so nothing in the daily instruments path
can see Beijing. The gap was filled by `_merge_untdxable_instruments`, which
replays the *last* code-space sweep (`scripts/delisted_ops.py discover`) — an
occasional operator action, not a daily one. So a Beijing name listed since that
sweep never entered the catalogue at all, and a symbol that is not in the
catalogue gets no bars, no status, no anything.

Measured on 2026-09-15: the board published 343 securities and the lake held
327, missing 16 that were trading that day with real volume (森合高科, 杰理科技,
华汇智能 and 13 others) and had no row anywhere in the lake.

The board is the authority for this exchange, it is one paginated read for the
whole market, and the quote and trading-status paths already walk it — so this
costs no additional requests beyond the ones Beijing already serves us, and it
carries the 证券简称 that the TDX-shaped rows never had.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.bse.trading_status import board_names


def fetch_bse_instruments(trade_date: date, *, client=None, config=None) -> pl.DataFrame:
    """Every security the Beijing board published for *trade_date*.

    Listing dates are absent from the board (``hqssrq`` comes back null), so
    they stay null and the compact's sticky coalesce keeps whatever an earlier
    source established.
    """
    listed, _complete = board_names(trade_date, client=client, config=config)
    symbols = sorted(listed)
    return pl.DataFrame(
        {
            "symbol": symbols,
            "name": [listed[s] or None for s in symbols],
            "exchange": ["BJ"] * len(symbols),
            "asset_type": ["stock"] * len(symbols),
            "list_date": pl.Series([None] * len(symbols), dtype=pl.Date),
            "delist_date": pl.Series([None] * len(symbols), dtype=pl.Date),
            "prev_symbol": pl.Series([None] * len(symbols), dtype=pl.Utf8),
        },
        schema_overrides={"name": pl.Utf8},
    )
