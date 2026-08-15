from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

PROVENANCE = ["source", "data_version", "fetched_at"]

FETCHED_AT_DTYPE = pl.Datetime(time_unit="us", time_zone="UTC")

# Rows carrying this source value are synthetic and must never be trusted
# downstream; audit raises an error finding whenever they reach curated.
MOCK_SOURCE = "mock"

DEFAULT_DATA_VERSION = "v1"

# Datasets whose stored values changed *meaning* — not shape. Adding a column
# is a schema change and leaves `data_version` alone; reinterpreting a value
# already written does not, because old and new rows are then not comparable
# and a reader has no other way to tell them apart.
#
# daily_bars v2: `volume` is 股 for every source. v1 rows are 手 from
# tdx_protocol and sina, 股 from ths and baostock — see
# `cn_market_lake.domain.units` and docs/datasets/schema.md.
DATASET_DATA_VERSION = {
    "daily_bars": "v2",
}


def data_version_for(dataset: str) -> str:
    return DATASET_DATA_VERSION.get(dataset, DEFAULT_DATA_VERSION)


DAILY_BARS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "amount": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

# Intraday bars. One registered dataset holds exactly one frequency: TDX serves
# 95 trading days of 1m but 491 of 5m and coarser, and a dataset carries a
# single watermark and a single coverage_start — mixing the two horizons under
# one name would make both of them lie. `frequency` is still in the schema and
# in the PK so a second frequency can be added without a breaking change.
#
# bar_time is the bar's CLOSING minute, which is how TDX labels them: a session
# runs 09:31…11:30 and 13:01…15:00, 240 bars, no lunch bars, and the 15:00 bar
# carries the closing auction. It is a naive Asia/Shanghai wall clock, matching
# the convention that only fetched_at is stored tz-aware.
#
# A-shares have no overnight session, so trade_date == bar_time.date() always;
# it is stored anyway because it is the partition column.
MINUTE_BARS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "bar_time": pl.Datetime(time_unit="us"),
    "frequency": pl.Utf8,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    # 股, like every other dataset (cn_market_lake.domain.units). TDX reports
    # intraday bars in 股 natively — unlike its daily K, which is 手 — so this
    # path must NOT reuse the daily lots_to_shares conversion.
    "volume": pl.Int64,
    "amount": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

# Transaction records (分笔). Not tick data — A-share Level-1 is a 3-second
# snapshot, so one row aggregates however many real trades landed in one frame
# (6–33 on average, measured). See adapters/tdx_protocol/trade_ticks.py.
TRADE_TICKS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    # Position in the session, ascending in time, dense from 0. This is the
    # identity of a row: the wire timestamp has no seconds, so up to twenty
    # records share a `trade_time` and it cannot key anything. Safe as a key
    # because a settled session is frozen — refetched twice, 4,308 rows came
    # back identical field for field.
    "tick_seq": pl.Int32,
    # Minute precision. The seconds are always :00 — they are not truncated
    # from a finer timestamp, the protocol never carried them.
    "trade_time": pl.Datetime(time_unit="us"),
    "price": pl.Float64,
    # 股, like every other dataset (cn_market_lake.domain.units). The wire reports
    # 手; the adapter multiplies by 100, which reconciliation against
    # daily_bars confirms rather than assumes.
    "volume": pl.Int64,
    # buy / sell / neutral / after_hours. TDX's own tick-rule inference, not an
    # exchange field. `after_hours` is the 15:05–15:30 fixed-price session and
    # is *not* in the exchange's daily volume — exclude it before reconciling.
    # No `amount`: the source does not carry one, and a stored price × volume
    # would look like a fact while being an approximation (one representative
    # price stands for every trade folded into the frame).
    "direction": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

# Domestic commodity futures main-continuous daily bars (东财主连).
# symbol = {ROOT}0.{EXCH} e.g. AU0.SHF / I0.DCE — not A-share .SH/.SZ.
COMMODITY_BARS_SCHEMA = {
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "exchange": pl.Utf8,
    "trade_date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "amount": pl.Float64,
    "open_interest": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

INSTRUMENTS_SCHEMA = {
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "exchange": pl.Utf8,
    "asset_type": pl.Utf8,
    "list_date": pl.Date,
    "delist_date": pl.Date,
    "prev_symbol": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

TRADING_CALENDAR_SCHEMA = {
    "trade_date": pl.Date,
    "is_trading": pl.Boolean,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

TRADING_STATUS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "is_trading": pl.Boolean,
    "status": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

# UNIT CONTRACT (per-share): every ratio/amount below is per ONE held share,
# NOT the "每10股" convention Chinese sources quote raw. Adapters divide raw
# per-10-share source values by 10 before staging. So "10派8.5元" → 0.85,
# "10送8股" → 0.8, "10转4股" → 0.4, "10配3股" → 0.3. Downstream real-share
# accounting is uniform: shares_after = shares * (1 + bonus_ratio +
# transfer_ratio); cash = shares * cash_dividend. No /10 magic numbers.
# allotment_price stays a per-share price (yuan paid per allotted share).
CORPORATE_ACTIONS_SCHEMA = {
    "symbol": pl.Utf8,
    "ex_date": pl.Date,
    "action_type": pl.Utf8,
    "cash_dividend": pl.Float64,  # per share (yuan), pretax
    "bonus_ratio": pl.Float64,  # per share (送股: new shares per held share)
    "transfer_ratio": pl.Float64,  # per share (转股: new shares per held share)
    "allotment_ratio": pl.Float64,  # per share (配股: offered shares per held share)
    "allotment_price": pl.Float64,  # per allotted share (yuan), NOT a ratio
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

ADJ_FACTORS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "adjust_type": pl.Utf8,
    "factor": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

FINANCIAL_STATEMENT_ITEMS_SCHEMA = {
    "symbol": pl.Utf8,
    "report_period": pl.Utf8,
    "statement_type": pl.Utf8,
    "item_code": pl.Utf8,
    "item_value": pl.Float64,
    "announce_date": pl.Date,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

# 股本结构. Keyed by the date the structure *changed*, not a report period: a
# company can restructure twice in one quarter and both rows matter, which is
# why `change_reason` rides along rather than being dropped as prose.
SHARE_STRUCTURE_SCHEMA = {
    "symbol": pl.Utf8,
    "change_date": pl.Date,
    "total_shares": pl.Float64,
    "float_shares": pl.Float64,
    "restricted_shares": pl.Float64,
    # 自由流通股 — float minus strategic/locked holdings. The denominator an
    # index uses for free-float weighting; not the same as `float_shares`.
    "free_float_shares": pl.Float64,
    "change_reason": pl.Utf8,
    "announce_date": pl.Date,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

# 股东户数. The input to 筹码集中度 factors: a falling holder count against a
# flat share count means concentration.
SHAREHOLDER_COUNTS_SCHEMA = {
    "symbol": pl.Utf8,
    # The date the count is as of, NOT a report period. Companies disclose
    # 股东户数 at 旬末/月末 as well as quarter-ends — 2025-07-10 carries 894 rows
    # and 2025-07-31 another 1,162 — and those interim counts are the timely
    # half of the signal. A quarter label would collapse them onto each other.
    "count_date": pl.Date,
    "holder_count": pl.Float64,
    "holder_count_change_pct": pl.Float64,
    "avg_float_shares": pl.Float64,
    "avg_holding_value": pl.Float64,
    "announce_date": pl.Date,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

# 前十大股东 / 前十大流通股东 — one table, scope discriminator. Both are the same
# shape from the same disclosure: a ranked repeating group of ten. That is what
# the long-format statement table genuinely cannot express, and why this is its
# own dataset rather than more `item_code` rows.
#
# `holding_pct` means different things per scope, deliberately: for `total` it
# is a share of total shares, for `float` a share of the float. They are not
# comparable across scopes, and averaging them would invent a number neither
# source published.
TOP_HOLDERS_SCHEMA = {
    "symbol": pl.Utf8,
    # The list's as-of date. Mostly quarter-ends, but not only: 2025 Q3 has
    # 10,749 total-scope rows dated to something else (prospectuses, 权益变动).
    "record_date": pl.Date,
    "holder_scope": pl.Utf8,
    "holder_rank": pl.Int32,
    "holder_name": pl.Utf8,
    "holding_shares": pl.Float64,
    "holding_pct": pl.Float64,
    "is_institution": pl.Boolean,
    "holder_type": pl.Utf8,
    "announce_date": pl.Date,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

FUND_FLOW_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "main_net_inflow": pl.Float64,
    "super_large_net_inflow": pl.Float64,
    "large_net_inflow": pl.Float64,
    "medium_net_inflow": pl.Float64,
    "small_net_inflow": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

MARGIN_TRADING_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "margin_balance": pl.Float64,
    "margin_buy": pl.Float64,
    "short_balance": pl.Float64,
    "short_sell_volume": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

NORTHBOUND_HOLDINGS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "channel": pl.Utf8,
    "holding_shares": pl.Float64,
    "holding_mv": pl.Float64,
    "holding_ratio": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

NORTHBOUND_FLOWS_SCHEMA = {
    "trade_date": pl.Date,
    "channel": pl.Utf8,
    "net_buy": pl.Float64,
    "buy_amount": pl.Float64,
    "sell_amount": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

VALUATION_METRICS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "pe_ttm": pl.Float64,
    "pb": pl.Float64,
    "ps_ttm": pl.Float64,
    "total_mv": pl.Float64,
    "float_mv": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

SECTOR_MEMBERS_SCHEMA = {
    "symbol": pl.Utf8,
    "sector_code": pl.Utf8,
    "sector_name": pl.Utf8,
    "as_of_date": pl.Date,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

ANNOUNCEMENT_INDEX_SCHEMA = {
    "announcement_id": pl.Utf8,
    "symbol": pl.Utf8,
    "title": pl.Utf8,
    "announce_date": pl.Date,
    "category": pl.Utf8,
    "url": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

# Scheduled disclosure dates (预约披露) for periodic reports. Current-state:
# a revision overwrites scheduled_date in place; first_scheduled_date keeps the
# original appointment and actual_date stays null until the report is published.
EARNINGS_DISCLOSURE_SCHEDULE_SCHEMA = {
    "symbol": pl.Utf8,
    "report_period": pl.Utf8,
    "scheduled_date": pl.Date,
    "first_scheduled_date": pl.Date,
    "actual_date": pl.Date,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

DRAGON_TIGER_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "reason": pl.Utf8,
    "buy_amount": pl.Float64,
    "sell_amount": pl.Float64,
    "net_amount": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

BLOCK_TRADES_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "price": pl.Float64,
    "volume": pl.Float64,
    "amount": pl.Float64,
    "premium_ratio": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

INDEX_CONSTITUENTS_SCHEMA = {
    "index_symbol": pl.Utf8,
    "symbol": pl.Utf8,
    "as_of_date": pl.Date,
    "weight": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

INDUSTRY_MEMBERS_SCHEMA = {
    "symbol": pl.Utf8,
    "classification_system": pl.Utf8,
    "industry_code": pl.Utf8,
    "industry_name": pl.Utf8,
    "as_of_date": pl.Date,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

MACRO_INDICATORS_SCHEMA = {
    "indicator_id": pl.Utf8,
    "obs_date": pl.Date,
    "value": pl.Float64,
    "frequency": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

MARKET_BREADTH_SCHEMA = {
    "trade_date": pl.Date,
    "metric_id": pl.Utf8,
    "value": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

SHARE_UNLOCK_SCHEDULE_SCHEMA = {
    "symbol": pl.Utf8,
    "unlock_date": pl.Date,
    "unlock_shares": pl.Float64,
    "unlock_ratio": pl.Float64,
    "unlock_type": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

REGULATORY_EVENTS_SCHEMA = {
    "event_id": pl.Utf8,
    "symbol": pl.Utf8,
    "event_date": pl.Date,
    "event_type": pl.Utf8,
    "title": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

INSTITUTIONAL_HOLDINGS_SCHEMA = {
    "symbol": pl.Utf8,
    "holder_type": pl.Utf8,
    "report_period": pl.Utf8,
    "holding_shares": pl.Float64,
    "holding_ratio": pl.Float64,
    "holding_mv": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

ANALYST_CONSENSUS_SCHEMA = {
    "symbol": pl.Utf8,
    "forecast_date": pl.Date,
    "forecast_year": pl.Int64,
    "eps_forecast": pl.Float64,
    "pe_forecast": pl.Float64,
    "target_price": pl.Float64,
    "rating": pl.Utf8,
    "analyst_count": pl.Int64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

SENTIMENT_SCORES_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "score_channel": pl.Utf8,
    "sentiment_score": pl.Float64,
    "headline_count": pl.Int64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

HOT_RANK_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "rank": pl.Int64,
    "rank_change": pl.Int64,
    "hist_rank": pl.Int64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

SECTOR_BARS_SCHEMA = {
    "sector_code": pl.Utf8,
    "sector_name": pl.Utf8,
    "board_type": pl.Utf8,
    "trade_date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "amount": pl.Float64,
    "change_pct": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

SECTOR_FUND_FLOW_SCHEMA = {
    "sector_code": pl.Utf8,
    "sector_name": pl.Utf8,
    "board_type": pl.Utf8,
    "trade_date": pl.Date,
    "main_net_inflow": pl.Float64,
    "change_pct": pl.Float64,
    "turnover_pct": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

NEWS_HEADLINES_SCHEMA = {
    "news_id": pl.Utf8,
    "publish_date": pl.Date,
    "publish_time": pl.Utf8,
    "title": pl.Utf8,
    "summary": pl.Utf8,
    "related_symbols": pl.Utf8,
    "channel": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

FLASH_NEWS_WIRE_SCHEMA = {
    "wire_id": pl.Utf8,
    "wire_source": pl.Utf8,
    "item_hash": pl.Utf8,
    "publish_date": pl.Date,
    "publish_time": pl.Utf8,
    "title": pl.Utf8,
    "summary": pl.Utf8,
    "related_symbols": pl.Utf8,
    "importance": pl.Int8,
    "channel": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

ECONOMIC_CALENDAR_SCHEMA = {
    "event_id": pl.Utf8,
    "event_date": pl.Date,
    "event_time": pl.Utf8,
    "country": pl.Utf8,
    "indicator": pl.Utf8,
    "importance": pl.Int8,
    "forecast": pl.Float64,
    "previous": pl.Float64,
    "actual": pl.Float64,
    "unit": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

# One row per recovered delisting, describing how its price series *ends*.
#
# Whether a series runs through the 退市整理期 decides whether a backtest can
# realise the final loss or marks the position at its last pre-suspension price.
# Measured on this lake, that period is worth -27% to -92%, so the distinction is
# not cosmetic — and it cannot be assumed, because trading-rule delistings
# (面值/市值) legitimately have no consolidation period while a truncated vendor
# series looks identical. Recording the shape lets research separate them
# instead of silently treating both as complete.
INDUSTRY_INDEX_SCHEMA = {
    "trade_date": pl.Date,
    "industry_code": pl.Utf8,
    # L1 / L2 / L3 — the 申万 code is prefix-hierarchical, so one membership
    # series yields all three depths.
    "level": pl.Utf8,
    # equal | amount. Both are stored because free-float cap, the 申万
    # convention, is only ~69% populated in valuation_metrics.
    "weighting": pl.Utf8,
    "ret": pl.Float64,
    # Members known that day, members that actually had a priced bar, and the
    # difference — names without an adjustment factor (北交所) cannot enter the
    # index, and which industries that distorts has to stay visible.
    "n_members": pl.Int64,
    "n_priced": pl.Int64,
    "n_excluded": pl.Int64,
    "amount": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

DELISTING_EVENTS_SCHEMA = {
    "symbol": pl.Utf8,
    "first_trade_date": pl.Date,
    "last_trade_date": pl.Date,
    # consolidation | abrupt_decline | abrupt_stable | insufficient
    "ending_pattern": pl.Utf8,
    "final_close": pl.Float64,
    "halt_gap_days": pl.Int64,
    "worst_final_return": pl.Float64,
    "final_window_return": pl.Float64,
    "bars": pl.Int64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

DATASET_SCHEMAS = {
    "instruments": INSTRUMENTS_SCHEMA,
    "trading_calendar": TRADING_CALENDAR_SCHEMA,
    "trading_status": TRADING_STATUS_SCHEMA,
    "daily_bars": DAILY_BARS_SCHEMA,
    "index_bars": {**DAILY_BARS_SCHEMA, "frequency": pl.Utf8},
    "minute_bars": MINUTE_BARS_SCHEMA,
    "minute_bars_5m": MINUTE_BARS_SCHEMA,
    "trade_ticks": TRADE_TICKS_SCHEMA,
    "commodity_bars": COMMODITY_BARS_SCHEMA,
    "corporate_actions": CORPORATE_ACTIONS_SCHEMA,
    "adj_factors": ADJ_FACTORS_SCHEMA,
    "financial_statement_items": FINANCIAL_STATEMENT_ITEMS_SCHEMA,
    "share_structure": SHARE_STRUCTURE_SCHEMA,
    "shareholder_counts": SHAREHOLDER_COUNTS_SCHEMA,
    "top_holders": TOP_HOLDERS_SCHEMA,
    "fund_flow": FUND_FLOW_SCHEMA,
    "margin_trading": MARGIN_TRADING_SCHEMA,
    "northbound_holdings": NORTHBOUND_HOLDINGS_SCHEMA,
    "northbound_flows": NORTHBOUND_FLOWS_SCHEMA,
    "valuation_metrics": VALUATION_METRICS_SCHEMA,
    "sector_members": SECTOR_MEMBERS_SCHEMA,
    "industry_index": INDUSTRY_INDEX_SCHEMA,
    "announcement_index": ANNOUNCEMENT_INDEX_SCHEMA,
    "earnings_disclosure_schedule": EARNINGS_DISCLOSURE_SCHEDULE_SCHEMA,
    "dragon_tiger": DRAGON_TIGER_SCHEMA,
    "block_trades": BLOCK_TRADES_SCHEMA,
    "index_constituents": INDEX_CONSTITUENTS_SCHEMA,
    "industry_members": INDUSTRY_MEMBERS_SCHEMA,
    "macro_indicators": MACRO_INDICATORS_SCHEMA,
    "market_breadth": MARKET_BREADTH_SCHEMA,
    "share_unlock_schedule": SHARE_UNLOCK_SCHEDULE_SCHEMA,
    "regulatory_events": REGULATORY_EVENTS_SCHEMA,
    "institutional_holdings": INSTITUTIONAL_HOLDINGS_SCHEMA,
    "analyst_consensus": ANALYST_CONSENSUS_SCHEMA,
    "sentiment_scores": SENTIMENT_SCORES_SCHEMA,
    "hot_rank": HOT_RANK_SCHEMA,
    "sector_bars": SECTOR_BARS_SCHEMA,
    "sector_fund_flow": SECTOR_FUND_FLOW_SCHEMA,
    "news_headlines": NEWS_HEADLINES_SCHEMA,
    "flash_news_wire": FLASH_NEWS_WIRE_SCHEMA,
    "economic_calendar": ECONOMIC_CALENDAR_SCHEMA,
    "delisting_events": DELISTING_EVENTS_SCHEMA,
}

PRIMARY_KEYS = {
    "instruments": ["symbol"],
    "trading_calendar": ["trade_date"],
    "trading_status": ["symbol", "trade_date"],
    "daily_bars": ["symbol", "trade_date"],
    "index_bars": ["symbol", "trade_date", "frequency"],
    "minute_bars": ["symbol", "trade_date", "bar_time", "frequency"],
    "minute_bars_5m": ["symbol", "trade_date", "bar_time", "frequency"],
    # Not trade_time: it has no seconds, so a busy minute holds twenty records
    # sharing one. tick_seq is the only thing that separates them.
    "trade_ticks": ["symbol", "trade_date", "tick_seq"],
    "commodity_bars": ["symbol", "trade_date"],
    "corporate_actions": ["symbol", "ex_date", "action_type"],
    "adj_factors": ["symbol", "trade_date", "adjust_type"],
    # announce_date is part of the key, not an attribute of it: a restatement
    # republishes the same (period, item) with a new value on a new date, and
    # keying without the date lets the newer row destroy the original. That
    # silently rewrites history — a query as of a date before the restatement
    # would find the item missing entirely — and makes revision-based signals
    # impossible. With the date in the key, vintages accumulate and the reader
    # picks the latest one known as of the query date.
    "financial_statement_items": [
        "symbol",
        "report_period",
        "statement_type",
        "item_code",
        "announce_date",
    ],
    # announce_date is in the key for the same reason it is in FSI's: a
    # restatement republishes the same period, and overwriting in place would
    # make a query as of a date before the restatement find the old figure
    # gone rather than find the figure that was known then.
    "share_structure": ["symbol", "change_date", "announce_date"],
    "shareholder_counts": ["symbol", "count_date", "announce_date"],
    # holder_name is in the key because holder_rank is NOT unique: holders tied
    # on share count share a rank. 600010.SH 2025-06-30 rank 9 is both 博时基金
    # and 易方达基金 at 167,831,580 shares each. Keying without the name drops
    # one of them — 1,730 rows market-wide in that one period, and concentrated
    # in the parallel-vehicle holders (中证金融 et al.) worth noticing.
    "top_holders": [
        "symbol",
        "record_date",
        "holder_scope",
        "holder_rank",
        "holder_name",
        "announce_date",
    ],
    "fund_flow": ["symbol", "trade_date"],
    "margin_trading": ["symbol", "trade_date"],
    "northbound_holdings": ["symbol", "trade_date", "channel"],
    "northbound_flows": ["trade_date", "channel"],
    "valuation_metrics": ["symbol", "trade_date"],
    "sector_members": ["symbol", "sector_code", "as_of_date"],
    "industry_index": ["trade_date", "industry_code", "level", "weighting"],
    "announcement_index": ["announcement_id"],
    "earnings_disclosure_schedule": ["symbol", "report_period"],
    "dragon_tiger": ["symbol", "trade_date", "reason"],
    "block_trades": ["symbol", "trade_date", "price", "volume"],
    "index_constituents": ["index_symbol", "symbol", "as_of_date"],
    "industry_members": ["symbol", "classification_system", "as_of_date"],
    "macro_indicators": ["indicator_id", "obs_date"],
    "market_breadth": ["trade_date", "metric_id"],
    "share_unlock_schedule": ["symbol", "unlock_date"],
    "regulatory_events": ["event_id"],
    "institutional_holdings": ["symbol", "holder_type", "report_period"],
    "analyst_consensus": ["symbol", "forecast_date"],
    "sentiment_scores": ["symbol", "trade_date", "score_channel"],
    "hot_rank": ["symbol", "trade_date"],
    "sector_bars": ["sector_code", "trade_date"],
    "sector_fund_flow": ["sector_code", "trade_date"],
    "news_headlines": ["news_id"],
    "flash_news_wire": ["wire_id", "wire_source"],
    "economic_calendar": ["event_id"],
    "delisting_events": ["symbol"],
}


class SchemaValidationError(ValueError):
    """Raised when a DataFrame does not match the dataset contract."""


def validate_dataframe(df: pl.DataFrame, dataset: str) -> pl.DataFrame:
    """Cast and validate *df* against the curated schema for *dataset*."""
    schema = DATASET_SCHEMAS.get(dataset)
    if schema is None:
        return df

    if df.is_empty():
        return pl.DataFrame(schema=schema)

    missing = [col for col in schema if col not in df.columns]
    if missing:
        raise SchemaValidationError(f"dataset '{dataset}': missing columns {missing}")

    casts = []
    for col, dtype in schema.items():
        if isinstance(dtype, pl.Datetime) and df.schema[col] == pl.Utf8:
            casts.append(
                pl.col(col)
                .str.to_datetime(time_unit=dtype.time_unit, time_zone=dtype.time_zone, strict=False)
                .alias(col)
            )
        elif dtype == pl.Date and df.schema[col] == pl.Utf8:
            casts.append(pl.col(col).str.to_date(strict=False).alias(col))
        else:
            casts.append(pl.col(col).cast(dtype, strict=False))
    return df.with_columns(casts).select(list(schema.keys()))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def with_provenance(df: pl.DataFrame, source: str, data_version: str) -> pl.DataFrame:
    # An adapter may pre-set `source` (e.g. MOCK_SOURCE) to flag row origin;
    # that marker must survive normalization.
    cols = [
        pl.lit(data_version).alias("data_version"),
        pl.lit(datetime.now(timezone.utc)).cast(FETCHED_AT_DTYPE).alias("fetched_at"),
    ]
    if "source" not in df.columns:
        cols.append(pl.lit(source).alias("source"))
    return df.with_columns(cols)
