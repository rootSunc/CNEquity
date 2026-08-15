"""The lake's traded-quantity unit, and the per-vendor conversions into it.

A-share vendors disagree about what a "volume" is. Some report 股 (shares),
some report 手 (lots, 100 shares). Nothing in a bar payload says which, so a
column that mixes both is silently wrong by exactly 100× — large enough to
destroy any turnover or liquidity factor, small enough that OHLC checks and
row counts never notice.

**The lake stores 股.** That is what ``docs/datasets/schema.md`` has always
promised, and it is the only choice that makes ``amount ≈ close × volume``
hold, which in turn is what lets :mod:`cn_market_lake.quality.unit_checks`
detect a regression from the data alone.

Every adapter converts at its own boundary, before the row is handed on, so a
frame that has left an adapter is always in 股.

Measured units per vendor (ratio = ``amount / close / volume`` over the whole
curated lake, which is ~1 when volume is 股 and ~100 when it is 手):

===============  =========  =====================================
vendor           native     evidence
===============  =========  =====================================
tdx_protocol     手         median 100.000 over 12,182,204 rows
ths              股         median 0.999 over 5,303,037 rows
baostock         股         median 1.000 over 374,888 rows
sina             股         vendor docs; ``amount`` is not served,
                            so the ratio cannot be measured
eastmoney        手         inferred — see the caveat below
===============  =========  =====================================

The EastMoney reading is **not independently verified**: ``push2his`` is
unreachable from the network this was measured on, and the only EastMoney rows
in the lake are all-zero suspension placeholders. It is taken from the same
endpoint and field index that ``commodity_bars`` already documents as 东财口径
手 (``docs/datasets/schema.md``). If it is wrong, ``daily_bars_volume_unit``
fires the first time a real EastMoney row is curated, which is the point of
that check.

TDX is per-frequency, not per-vendor: daily K (``frequency=9``) is 手, while
1-minute bars off the same wire parser are 股 (verified: 600519 1m bar
vol=59,700 against amount=88,977,784 at ~1490 → 59,716 shares). A future
``minute_bars`` dataset must not reuse the daily conversion, and any
minute-to-daily volume reconciliation has to compare 股 to 股.
"""

from __future__ import annotations

__all__ = ["SHARES_PER_LOT", "lots_to_shares"]

SHARES_PER_LOT = 100


def lots_to_shares(volume_lots: float | int | None) -> int:
    """Convert a vendor's 手 quantity to the lake's 股.

    ``None`` becomes 0 to match the suspension convention (``volume=0``,
    ``amount=0``) rather than introducing a null into an ``Int64`` column.
    """
    if volume_lots is None:
        return 0
    return int(volume_lots) * SHARES_PER_LOT
