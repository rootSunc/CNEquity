"""Shenwan research adapters (industry classification history)."""

from cn_market_lake.adapters.sw.industry_history import (
    expand_sw_industry_as_of,
    fetch_sw_industry_intervals,
)

__all__ = ["expand_sw_industry_as_of", "fetch_sw_industry_intervals"]
