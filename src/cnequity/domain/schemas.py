from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from cnequity.domain.datasets import DATASETS, PIT_DATASET_NAMES
from cnequity.domain.pit import (
    PIT_STORAGE_COLUMNS,
    PIT_STORAGE_DTYPES,
)

PROVENANCE = ["source", "data_version", "fetched_at"]

FETCHED_AT_DTYPE = pl.Datetime(time_unit="us", time_zone="UTC")

# Rows carrying this source value are synthetic and must never be trusted
# downstream; audit raises an error finding whenever they reach curated.
MOCK_SOURCE = "mock"

DEFAULT_DATA_VERSION = "v1"

#: Datasets past ``v1``: derived from ``DatasetSpec.data_version``, which says
#: when a version is bumped.
DATASET_DATA_VERSION = {
    name: spec.data_version
    for name, spec in DATASETS.items()
    if spec.data_version != DEFAULT_DATA_VERSION
}


def data_version_for(dataset: str) -> str:
    return DATASET_DATA_VERSION.get(dataset, DEFAULT_DATA_VERSION)


# Columns a writer may leave out; validation adds them as null. Older rows and
# sources that do not publish the value read the same way.
#: Action types that carry the exchange's ex-rights reference price.
REFERENCE_PRICE_ACTIONS = frozenset({"reorg_transfer", "exchange_reference"})

OPTIONAL_COLUMNS = frozenset(
    {
        "pre_close",
        "l1_code",
        "l1_name",
        "l2_code",
        "l2_name",
        "l3_code",
        "l3_name",
        "float_a_shares",
        # economic_calendar rows written before 2026-10-30 carry no Jin10 id.
        "indicator_code",
        # Jin10 calendar and events rows written before 2026-10-31.
        "withdrawn",
    }
)

DAILY_BARS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    # The exchange's previous close for the session, when a source publishes
    # it (exchange and BSE boards, TDX quotes). On an ex-date it is the
    # ex-rights reference price, so the day's factor step is close_prev /
    # pre_close. Null where the source does not carry it, e.g. K-line history.
    "pre_close": pl.Float64,
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
    # 股, like every other dataset (cnequity.domain.units). TDX reports
    # intraday bars in 股 natively — unlike its daily K, which is 手 — so this
    # path must NOT reuse the daily lots_to_shares conversion.
    "volume": pl.Int64,
    "amount": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

# Stored 15m/30m/60m resamples (derive/minute_resample.py). `resampled_from`
# is "1m" or "5m": which input built the bar, because the two disagree on OHLC
# when a vendor 5m bar folds in an untraded minute's carried quote.
_PROVENANCE = ("source", "data_version", "fetched_at")
RESAMPLED_MINUTE_BARS_SCHEMA = {
    **{k: v for k, v in MINUTE_BARS_SCHEMA.items() if k not in _PROVENANCE},
    "resampled_from": pl.Utf8,
    **{k: MINUTE_BARS_SCHEMA[k] for k in _PROVENANCE},
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
    # 股, like every other dataset (cnequity.domain.units). The wire reports
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

# Futures and options (ADR-0013). Contracts live in their own tables rather
# than in `instruments`: CZCE's one-digit year codes repeat every decade, and
# every equity path assumes a SH/SZ/BJ symbol. Symbols are the canonical form
# in `domain/derivatives.py` (CU2511.SHF, TA2601.CZC, IO2512C4000.CFE);
# `exchange_code` keeps the exchange's own spelling.
#
# Bars are single-sided (the four commodity exchanges counted both sides until
# 2019-12-31; the adapters halve those), turnover is 元, and implied volatility
# is a fraction. A contract with no trades on a session has a settlement but no
# prices, so its OHLC is null.
FUTURES_CONTRACTS_SCHEMA = {
    "symbol": pl.Utf8,
    "exchange": pl.Utf8,
    "exchange_code": pl.Utf8,
    "product": pl.Utf8,
    "product_name": pl.Utf8,
    "delivery_month": pl.Date,
    "list_date": pl.Date,
    "last_trade_date": pl.Date,
    "multiplier": pl.Float64,
    "tick_size": pl.Float64,
    "quote_unit": pl.Utf8,
    # exchange / observed / observed_truncated: where list_date and
    # last_trade_date came from (see derive/derivative_contracts.py).
    "dates_basis": pl.Utf8,
    "first_seen_date": pl.Date,
    "last_seen_date": pl.Date,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

OPTION_CONTRACTS_SCHEMA = {
    "symbol": pl.Utf8,
    "exchange": pl.Utf8,
    "exchange_code": pl.Utf8,
    "product": pl.Utf8,
    "product_name": pl.Utf8,
    # A futures contract symbol, or for CFFEX index options the index itself.
    "underlying_symbol": pl.Utf8,
    "underlying_kind": pl.Utf8,
    "option_type": pl.Utf8,
    "strike": pl.Float64,
    "exercise_style": pl.Utf8,
    "expiry_month": pl.Date,
    "list_date": pl.Date,
    "expiry_date": pl.Date,
    "multiplier": pl.Float64,
    "tick_size": pl.Float64,
    "dates_basis": pl.Utf8,
    "first_seen_date": pl.Date,
    "last_seen_date": pl.Date,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

FUTURES_BARS_SCHEMA = {
    "symbol": pl.Utf8,
    "exchange": pl.Utf8,
    "exchange_code": pl.Utf8,
    "product": pl.Utf8,
    "trade_date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "settle": pl.Float64,
    "pre_settle": pl.Float64,
    "volume": pl.Int64,
    "amount": pl.Float64,
    "open_interest": pl.Int64,
    "oi_change": pl.Int64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

OPTION_BARS_SCHEMA = {
    "symbol": pl.Utf8,
    "exchange": pl.Utf8,
    "exchange_code": pl.Utf8,
    "product": pl.Utf8,
    "underlying_symbol": pl.Utf8,
    "option_type": pl.Utf8,
    "strike": pl.Float64,
    "trade_date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "settle": pl.Float64,
    "pre_settle": pl.Float64,
    "volume": pl.Int64,
    "amount": pl.Float64,
    "open_interest": pl.Int64,
    "oi_change": pl.Int64,
    "exercise_volume": pl.Int64,
    # As the exchange published them; `option_greeks` holds the lake's own.
    "delta": pl.Float64,
    "implied_vol": pl.Float64,
    # SHFE and INE publish one volatility per expiry series, not per strike.
    "series_implied_vol": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

# Main and second-month continuous series, derived from futures_bars (see
# derive/futures_continuous.py). One row per product, series and session.
FUTURES_CONTINUOUS_SCHEMA = {
    "symbol": pl.Utf8,
    "series": pl.Utf8,
    "trade_date": pl.Date,
    "contract_symbol": pl.Utf8,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "settle": pl.Float64,
    "volume": pl.Int64,
    "open_interest": pl.Int64,
    "rolled": pl.Boolean,
    "adj_ratio": pl.Float64,
    "adj_diff": pl.Float64,
    "roll_yield": pl.Float64,
    "rule": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

# The lake's own IV and Greeks per option settlement (derive/option_greeks.py).
# Greeks are per unit move: vega per 1.00 of volatility, theta per year, rho
# per 1.00 of rate. `status` says why a row has no IV (expiry_day,
# below_intrinsic, no_underlying, no_expiry, no_price, above_bound).
OPTION_GREEKS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "underlying_symbol": pl.Utf8,
    "underlying_price": pl.Float64,
    "forward_source": pl.Utf8,
    "time_to_expiry": pl.Float64,
    "rate": pl.Float64,
    "rate_source": pl.Utf8,
    "model": pl.Utf8,
    "iv": pl.Float64,
    "delta": pl.Float64,
    "gamma": pl.Float64,
    "vega": pl.Float64,
    "theta": pl.Float64,
    "rho": pl.Float64,
    "status": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

# One-minute futures bars for a chosen set of contracts (adapters/sina/
# futures_minute.py). bar_time is the closing minute, Beijing wall clock; a
# night-session bar belongs to the next trading day, so here — unlike the
# equity minute bars — trade_date is not bar_time.date().
FUTURES_MINUTE_BARS_SCHEMA = {
    "symbol": pl.Utf8,
    "exchange": pl.Utf8,
    "trade_date": pl.Date,
    "bar_time": pl.Datetime(time_unit="us"),
    "frequency": pl.Utf8,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "open_interest": pl.Int64,
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

ETF_PROFILES_SCHEMA = {
    "symbol": pl.Utf8,
    "as_of_date": pl.Date,
    "exchange": pl.Utf8,
    "name": pl.Utf8,
    "list_date": pl.Date,
    "tracking_index_code": pl.Utf8,
    "tracking_index_name": pl.Utf8,
    "fund_category": pl.Utf8,
    "investment_category": pl.Utf8,
    "eligibility_status": pl.Utf8,
    "classification_basis": pl.Utf8,
    "source_url": pl.Utf8,
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

# `status` and `risk_warning` are two orthogonal facts and must stay two
# columns: a security can be halted *and* under risk warning at once, and the
# single-column encoding resolved that by dropping the ST label. See
# `domain/trading_status.py` for the whole story and the legacy read path.
TRADING_STATUS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "is_trading": pl.Boolean,
    # normal | suspended | delisted
    "status": pl.Utf8,
    # ST / *ST designation. Nullable: a derived bar-gap suspension row has no
    # evidence either way, and saying `False` there would invent a fact.
    "risk_warning": pl.Boolean,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

# UNIT CONTRACT (per-share): every ratio/amount below is per ONE held share,
# NOT the "每10股" convention Chinese sources quote raw. Adapters divide raw
# per-10-share source values by 10 before staging. So "10派8.5元" → 0.85,
# "10送8股" → 0.8, "10转4股" → 0.4, "10配3股" → 0.3. Downstream real-share
# accounting is uniform: shares_after = shares * (1 + bonus_ratio +
# transfer_ratio) * split_factor; cash = shares * cash_dividend. No /10 magic numbers.
# allotment_price stays a per-share price (yuan paid per allotted share).
CORPORATE_ACTIONS_SCHEMA = {
    "payment_date": pl.Date,  # source-reported cash payment, never inferred from ex-date
    "payment_source": pl.Utf8,
    "symbol": pl.Utf8,
    "ex_date": pl.Date,
    "action_type": pl.Utf8,
    "cash_dividend": pl.Float64,  # per share (yuan), pretax
    "bonus_ratio": pl.Float64,  # per share (送股: new shares per held share)
    "transfer_ratio": pl.Float64,  # per share (转股: new shares per held share)
    "split_factor": pl.Float64,  # unit_split: units after / before; 1 is neutral
    "allotment_ratio": pl.Float64,  # per share (配股: offered shares per held share)
    "allotment_price": pl.Float64,  # per allotted share (yuan), NOT a ratio
    # reorg_transfer / exchange_reference only: the ex-rights reference price
    # (yuan per share after the event). On reorg_transfer the issuer publishes
    # it for a restructuring conversion, whose price effect follows the
    # court-approved formula, not 1 + transfer_ratio. An exchange_reference row
    # carries no terms: it records the exchange's published previous close for
    # an ex-date whose recorded terms disagree with it, and that price sets the
    # factor step.
    "reference_price": pl.Float64,
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
    # 流通A股 — listed tradable A shares only. `float_shares` also counts an
    # A+H company's H shares (600188.SH: 7.66 bn against 5.96 bn A), so this is
    # the count an A-share float market cap or turnover needs. Null before v2.
    "float_a_shares": pl.Float64,
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
    "pe_dynamic": pl.Float64,
    "total_mv_basis": pl.Utf8,
    "float_mv_basis": pl.Utf8,
    "shares_as_of": pl.Date,
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

# Tushare report_rc: one row per report × forecast fiscal year.
# Money columns are 万元 as the source reports them; eps/max/min price CNY/share.
BROKER_FORECASTS_SCHEMA = {
    "forecast_id": pl.Utf8,
    "symbol": pl.Utf8,
    "report_date": pl.Date,
    "fiscal_year": pl.Int16,
    "name": pl.Utf8,
    "report_title": pl.Utf8,
    "report_type": pl.Utf8,
    "classify": pl.Utf8,
    "org_name": pl.Utf8,
    "author_name": pl.Utf8,
    "quarter": pl.Utf8,
    "rating": pl.Utf8,
    "op_rt": pl.Float64,
    "op_pr": pl.Float64,
    "tp": pl.Float64,
    "np": pl.Float64,
    "eps": pl.Float64,
    "pe": pl.Float64,
    "rd": pl.Float64,
    "roe": pl.Float64,
    "ev_ebitda": pl.Float64,
    "max_price": pl.Float64,
    "min_price": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

FUND_FLOW_TUSHARE_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "buy_small_volume": pl.Float64,
    "buy_small_amount": pl.Float64,
    "sell_small_volume": pl.Float64,
    "sell_small_amount": pl.Float64,
    "buy_medium_volume": pl.Float64,
    "buy_medium_amount": pl.Float64,
    "sell_medium_volume": pl.Float64,
    "sell_medium_amount": pl.Float64,
    "buy_large_volume": pl.Float64,
    "buy_large_amount": pl.Float64,
    "sell_large_volume": pl.Float64,
    "sell_large_amount": pl.Float64,
    "buy_xlarge_volume": pl.Float64,
    "buy_xlarge_amount": pl.Float64,
    "sell_xlarge_volume": pl.Float64,
    "sell_xlarge_amount": pl.Float64,
    "net_volume": pl.Float64,
    "net_amount": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

LIMIT_PRICES_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "up_limit": pl.Float64,
    "down_limit": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

CONVERTIBLE_BONDS_SCHEMA = {
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "full_name": pl.Utf8,
    "bond_type": pl.Utf8,
    "stock_symbol": pl.Utf8,
    "exchange": pl.Utf8,
    "par_value": pl.Float64,
    "issue_price": pl.Float64,
    "issue_size": pl.Float64,
    "remaining_size": pl.Float64,
    "term_years": pl.Float64,
    "coupon_rate": pl.Float64,
    "coupon_schedule": pl.Utf8,
    "value_date": pl.Date,
    "maturity_date": pl.Date,
    "list_date": pl.Date,
    "delist_date": pl.Date,
    "conversion_start_date": pl.Date,
    "conversion_end_date": pl.Date,
    "conversion_stop_date": pl.Date,
    "first_conversion_price": pl.Float64,
    "conversion_price": pl.Float64,
    "issue_announce_date": pl.Date,
    "issue_method": pl.Utf8,
    "subscription_date": pl.Date,
    "holder_allotment_record_date": pl.Date,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

CONVERTIBLE_BOND_BARS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "pre_close": pl.Float64,
    "volume": pl.Int64,
    "amount": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

HOLDER_TRADES_SCHEMA = {
    "event_id": pl.Utf8,
    "symbol": pl.Utf8,
    "ann_date": pl.Date,
    "holder_name": pl.Utf8,
    "holder_type": pl.Utf8,
    "direction": pl.Utf8,
    "change_shares": pl.Float64,
    "change_ratio": pl.Float64,
    "after_shares": pl.Float64,
    "after_ratio": pl.Float64,
    "avg_price": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

SHARE_REPURCHASES_SCHEMA = {
    "event_id": pl.Utf8,
    "symbol": pl.Utf8,
    "ann_date": pl.Date,
    "progress_date": pl.Date,
    "stage": pl.Utf8,
    "expiry_date": pl.Date,
    "shares": pl.Float64,
    "amount": pl.Float64,
    "price_high": pl.Float64,
    "price_low": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

PLEDGE_STATS_SCHEMA = {
    "symbol": pl.Utf8,
    "end_date": pl.Date,
    "pledge_count": pl.Int64,
    "unrestricted_pledged_shares": pl.Float64,
    "restricted_pledged_shares": pl.Float64,
    "total_shares": pl.Float64,
    "pledge_ratio": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

CHIP_DISTRIBUTION_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "history_low": pl.Float64,
    "history_high": pl.Float64,
    "cost_5pct": pl.Float64,
    "cost_15pct": pl.Float64,
    "cost_50pct": pl.Float64,
    "cost_85pct": pl.Float64,
    "cost_95pct": pl.Float64,
    "cost_mean": pl.Float64,
    "winner_rate": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

NORTHBOUND_TOP10_SCHEMA = {
    "trade_date": pl.Date,
    "channel": pl.Utf8,
    "symbol": pl.Utf8,
    "rank": pl.Int64,
    "close": pl.Float64,
    "change": pl.Float64,
    "amount": pl.Float64,
    "net_amount": pl.Float64,
    "buy_amount": pl.Float64,
    "sell_amount": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

LIMIT_EVENTS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "limit_type": pl.Utf8,
    "close": pl.Float64,
    "pct_change": pl.Float64,
    "amount": pl.Float64,
    "amount_at_limit": pl.Float64,
    "seal_amount": pl.Float64,
    "float_market_cap": pl.Float64,
    "total_market_cap": pl.Float64,
    "turnover_rate": pl.Float64,
    "first_limit_time": pl.Utf8,
    "last_limit_time": pl.Utf8,
    "open_count": pl.Int64,
    "consecutive_limits": pl.Int64,
    "recent_limit_ups": pl.Int64,
    "recent_window_days": pl.Int64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

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
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

EARNINGS_FORECASTS_SCHEMA = {
    "symbol": pl.Utf8,
    "ann_date": pl.Date,
    "period_end": pl.Date,
    "forecast_type": pl.Utf8,
    "change_min_pct": pl.Float64,
    "change_max_pct": pl.Float64,
    "net_profit_min": pl.Float64,
    "net_profit_max": pl.Float64,
    "prior_net_profit": pl.Float64,
    "first_ann_date": pl.Date,
    "summary": pl.Utf8,
    "change_reason": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

BUSINESS_SEGMENTS_SCHEMA = {
    "symbol": pl.Utf8,
    "period_end": pl.Date,
    "segment_type": pl.Utf8,
    "segment": pl.Utf8,
    "revenue": pl.Float64,
    "cost": pl.Float64,
    "gross_profit": pl.Float64,
    "currency": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

INVESTOR_VISITS_SCHEMA = {
    "event_id": pl.Utf8,
    "symbol": pl.Utf8,
    "visit_date": pl.Date,
    "visitor_org": pl.Utf8,
    "org_type": pl.Utf8,
    "visitors": pl.Utf8,
    "place": pl.Utf8,
    "method": pl.Utf8,
    "company_hosts": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

HOT_MONEY_TRADERS_SCHEMA = {
    "trader": pl.Utf8,
    "description": pl.Utf8,
    "seats": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

HOT_MONEY_TRADES_SCHEMA = {
    "trade_date": pl.Date,
    "symbol": pl.Utf8,
    "trader": pl.Utf8,
    "seats": pl.Utf8,
    "buy_amount": pl.Float64,
    "sell_amount": pl.Float64,
    "net_amount": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

CONVERTIBLE_BOND_CALLS_SCHEMA = {
    "symbol": pl.Utf8,
    "ann_date": pl.Date,
    "call_type": pl.Utf8,
    "stage": pl.Utf8,
    "call_date": pl.Date,
    "call_price": pl.Float64,
    "call_price_after_tax": pl.Float64,
    "call_volume": pl.Float64,
    "call_amount": pl.Float64,
    "record_date": pl.Date,
    "payment_date": pl.Date,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

CONVERTIBLE_BOND_CONVERSIONS_SCHEMA = {
    "symbol": pl.Utf8,
    "end_date": pl.Date,
    "publish_date": pl.Date,
    "issue_size": pl.Float64,
    "first_conversion_price": pl.Float64,
    "conversion_price": pl.Float64,
    "converted_amount": pl.Float64,
    "converted_shares": pl.Float64,
    "converted_pct": pl.Float64,
    "total_converted_amount": pl.Float64,
    "total_converted_shares": pl.Float64,
    "total_converted_pct": pl.Float64,
    "remaining_size": pl.Float64,
    "total_shares": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

CONVERTIBLE_BOND_COUPONS_SCHEMA = {
    "symbol": pl.Utf8,
    "start_date": pl.Date,
    "end_date": pl.Date,
    "coupon_rate": pl.Float64,
    "payments_per_year": pl.Int64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

POPULARITY_RANKS_SCHEMA = {
    "trade_date": pl.Date,
    "provider": pl.Utf8,
    "list": pl.Utf8,
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "rank": pl.Int64,
    "pct_change": pl.Float64,
    "price": pl.Float64,
    "heat": pl.Float64,
    "concepts": pl.Utf8,
    "reason": pl.Utf8,
    "rank_time": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

LIMIT_LADDER_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "consecutive_limits": pl.Int64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

STRONGEST_SECTORS_SCHEMA = {
    "trade_date": pl.Date,
    "sector_code": pl.Utf8,
    "sector_name": pl.Utf8,
    "rank": pl.Int64,
    "days_listed": pl.Int64,
    "limit_stat": pl.Utf8,
    "consecutive_limit_stocks": pl.Int64,
    "limit_up_stocks": pl.Int64,
    "pct_change": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

KPL_LIMIT_UPS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "status": pl.Utf8,
    "first_limit_time": pl.Utf8,
    "last_limit_time": pl.Utf8,
    "reason": pl.Utf8,
    "theme": pl.Utf8,
    "amount": pl.Float64,
    "turnover_rate": pl.Float64,
    "free_float_cap": pl.Float64,
    "main_net_amount": pl.Float64,
    "limit_order_amount": pl.Float64,
    "max_limit_order_amount": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

FUND_FLOW_THS_TUSHARE_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "close": pl.Float64,
    "change_pct": pl.Float64,
    "net_amount": pl.Float64,
    "net_amount_5d": pl.Float64,
    "large_net_amount": pl.Float64,
    "large_net_rate": pl.Float64,
    "medium_net_amount": pl.Float64,
    "medium_net_rate": pl.Float64,
    "small_net_amount": pl.Float64,
    "small_net_rate": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

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
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
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
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
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
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

NAME_HISTORY_SCHEMA = {
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "start_date": pl.Date,
    "end_date": pl.Date,
    "announce_date": pl.Date,
    "change_reason": pl.Utf8,
    "risk_warning": pl.Boolean,
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
    # v2: the level-3 leaf. v1 EastMoney rows hold whichever level the source
    # page order left last; read ``l1_*``..``l3_*`` for a fixed level.
    "industry_code": pl.Utf8,
    "industry_name": pl.Utf8,
    "l1_code": pl.Utf8,
    "l1_name": pl.Utf8,
    "l2_code": pl.Utf8,
    "l2_name": pl.Utf8,
    "l3_code": pl.Utf8,
    "l3_name": pl.Utf8,
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

# 同花顺 money flow (adapters/ths/fund_flow.py): a different measure from
# EastMoney's — total inflow / outflow / net, no main-force split — so its own
# tables. Fetched only on days push2 cannot deliver `fund_flow`.
FUND_FLOW_THS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "inflow": pl.Float64,
    "outflow": pl.Float64,
    "net_inflow": pl.Float64,
    "amount": pl.Float64,
    "change_pct": pl.Float64,
    "turnover_pct": pl.Float64,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

SECTOR_FUND_FLOW_THS_SCHEMA = {
    "sector_code": pl.Utf8,
    "sector_name": pl.Utf8,
    "board_type": pl.Utf8,
    "trade_date": pl.Date,
    "sector_index": pl.Float64,
    "change_pct": pl.Float64,
    "inflow": pl.Float64,
    "outflow": pl.Float64,
    "net_inflow": pl.Float64,
    "company_count": pl.Int64,
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
    "indicator_code": pl.Utf8,
    "event_date": pl.Date,
    "event_time": pl.Utf8,
    "country": pl.Utf8,
    "indicator": pl.Utf8,
    "importance": pl.Int8,
    "forecast": pl.Float64,
    "previous": pl.Float64,
    "actual": pl.Float64,
    "unit": pl.Utf8,
    # True once a later capture of the event's window no longer lists it:
    # Jin10 reissues a rescheduled release under a new data_id.
    "withdrawn": pl.Boolean,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

ECONOMIC_EVENTS_SCHEMA = {
    "event_id": pl.Utf8,
    "event_date": pl.Date,
    "event_time": pl.Utf8,
    "time_status": pl.Utf8,
    "country": pl.Utf8,
    "importance": pl.Int8,
    "people": pl.Utf8,
    "content": pl.Utf8,
    "withdrawn": pl.Boolean,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "fetched_at": FETCHED_AT_DTYPE,
}

MARKET_HOLIDAYS_SCHEMA = {
    "holiday_id": pl.Utf8,
    "holiday_date": pl.Date,
    "country": pl.Utf8,
    "exchange": pl.Utf8,
    "name": pl.Utf8,
    "note": pl.Utf8,
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
    "etf_profiles": ETF_PROFILES_SCHEMA,
    "trading_calendar": TRADING_CALENDAR_SCHEMA,
    "trading_status": TRADING_STATUS_SCHEMA,
    "daily_bars": DAILY_BARS_SCHEMA,
    "index_bars": {**DAILY_BARS_SCHEMA, "frequency": pl.Utf8},
    "minute_bars": MINUTE_BARS_SCHEMA,
    "minute_bars_5m": MINUTE_BARS_SCHEMA,
    "minute_bars_15m": RESAMPLED_MINUTE_BARS_SCHEMA,
    "minute_bars_30m": RESAMPLED_MINUTE_BARS_SCHEMA,
    "minute_bars_60m": RESAMPLED_MINUTE_BARS_SCHEMA,
    "trade_ticks": TRADE_TICKS_SCHEMA,
    "commodity_bars": COMMODITY_BARS_SCHEMA,
    "futures_contracts": FUTURES_CONTRACTS_SCHEMA,
    "option_contracts": OPTION_CONTRACTS_SCHEMA,
    "futures_bars": FUTURES_BARS_SCHEMA,
    "option_bars": OPTION_BARS_SCHEMA,
    "futures_continuous": FUTURES_CONTINUOUS_SCHEMA,
    "option_greeks": OPTION_GREEKS_SCHEMA,
    "futures_minute_bars": FUTURES_MINUTE_BARS_SCHEMA,
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
    "broker_forecasts": BROKER_FORECASTS_SCHEMA,
    "name_history": NAME_HISTORY_SCHEMA,
    "limit_prices": LIMIT_PRICES_SCHEMA,
    "fund_flow_tushare": FUND_FLOW_TUSHARE_SCHEMA,
    "convertible_bonds": CONVERTIBLE_BONDS_SCHEMA,
    "convertible_bond_bars": CONVERTIBLE_BOND_BARS_SCHEMA,
    "holder_trades": HOLDER_TRADES_SCHEMA,
    "share_repurchases": SHARE_REPURCHASES_SCHEMA,
    "pledge_stats": PLEDGE_STATS_SCHEMA,
    "chip_distribution": CHIP_DISTRIBUTION_SCHEMA,
    "northbound_top10": NORTHBOUND_TOP10_SCHEMA,
    "limit_events": LIMIT_EVENTS_SCHEMA,
    "futures_member_positions": FUTURES_MEMBER_POSITIONS_SCHEMA,
    "earnings_forecasts": EARNINGS_FORECASTS_SCHEMA,
    "business_segments": BUSINESS_SEGMENTS_SCHEMA,
    "investor_visits": INVESTOR_VISITS_SCHEMA,
    "hot_money_traders": HOT_MONEY_TRADERS_SCHEMA,
    "hot_money_trades": HOT_MONEY_TRADES_SCHEMA,
    "convertible_bond_calls": CONVERTIBLE_BOND_CALLS_SCHEMA,
    "convertible_bond_conversions": CONVERTIBLE_BOND_CONVERSIONS_SCHEMA,
    "convertible_bond_coupons": CONVERTIBLE_BOND_COUPONS_SCHEMA,
    "popularity_ranks": POPULARITY_RANKS_SCHEMA,
    "limit_ladder": LIMIT_LADDER_SCHEMA,
    "strongest_sectors": STRONGEST_SECTORS_SCHEMA,
    "kpl_limit_ups": KPL_LIMIT_UPS_SCHEMA,
    "fund_flow_ths_tushare": FUND_FLOW_THS_TUSHARE_SCHEMA,
    "executives": EXECUTIVES_SCHEMA,
    "executive_compensation": EXECUTIVE_COMPENSATION_SCHEMA,
    "investor_qa": INVESTOR_QA_SCHEMA,
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
    "fund_flow_ths": FUND_FLOW_THS_SCHEMA,
    "sector_fund_flow_ths": SECTOR_FUND_FLOW_THS_SCHEMA,
    "news_headlines": NEWS_HEADLINES_SCHEMA,
    "flash_news_wire": FLASH_NEWS_WIRE_SCHEMA,
    "economic_calendar": ECONOMIC_CALENDAR_SCHEMA,
    "economic_events": ECONOMIC_EVENTS_SCHEMA,
    "market_holidays": MARKET_HOLIDAYS_SCHEMA,
    "delisting_events": DELISTING_EVENTS_SCHEMA,
}

PRIMARY_KEYS = {
    "instruments": ["symbol"],
    "etf_profiles": ["symbol", "as_of_date"],
    "trading_calendar": ["trade_date"],
    "trading_status": ["symbol", "trade_date"],
    "daily_bars": ["symbol", "trade_date"],
    "index_bars": ["symbol", "trade_date", "frequency"],
    "minute_bars": ["symbol", "trade_date", "bar_time", "frequency"],
    "minute_bars_5m": ["symbol", "trade_date", "bar_time", "frequency"],
    "minute_bars_15m": ["symbol", "trade_date", "bar_time", "frequency"],
    "minute_bars_30m": ["symbol", "trade_date", "bar_time", "frequency"],
    "minute_bars_60m": ["symbol", "trade_date", "bar_time", "frequency"],
    # Not trade_time: it has no seconds, so a busy minute holds twenty records
    # sharing one. tick_seq is the only thing that separates them.
    "trade_ticks": ["symbol", "trade_date", "tick_seq"],
    "commodity_bars": ["symbol", "trade_date"],
    "futures_contracts": ["symbol"],
    "option_contracts": ["symbol"],
    "futures_bars": ["symbol", "trade_date"],
    "option_bars": ["symbol", "trade_date"],
    "futures_continuous": ["symbol", "series", "trade_date"],
    "option_greeks": ["symbol", "trade_date"],
    "futures_minute_bars": ["symbol", "bar_time"],
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
    "broker_forecasts": ["forecast_id"],
    "name_history": ["symbol", "start_date", "name"],
    "limit_prices": ["symbol", "trade_date"],
    "fund_flow_tushare": ["symbol", "trade_date"],
    "convertible_bonds": ["symbol"],
    "convertible_bond_bars": ["symbol", "trade_date"],
    "holder_trades": ["event_id"],
    "share_repurchases": ["event_id"],
    "pledge_stats": ["symbol", "end_date"],
    "chip_distribution": ["symbol", "trade_date"],
    "northbound_top10": ["trade_date", "channel", "symbol"],
    "limit_events": ["symbol", "trade_date"],
    "futures_member_positions": ["trade_date", "symbol", "member"],
    "earnings_forecasts": ["symbol", "ann_date", "period_end"],
    "business_segments": ["symbol", "period_end", "segment_type", "segment"],
    "investor_visits": ["event_id"],
    "hot_money_traders": ["trader"],
    "hot_money_trades": ["trade_date", "symbol", "trader", "seats"],
    "convertible_bond_calls": ["symbol", "ann_date", "call_type", "stage"],
    "convertible_bond_conversions": ["symbol", "end_date"],
    "convertible_bond_coupons": ["symbol", "start_date"],
    "popularity_ranks": ["trade_date", "provider", "list", "symbol"],
    "limit_ladder": ["symbol", "trade_date"],
    "strongest_sectors": ["trade_date", "sector_code"],
    "kpl_limit_ups": ["symbol", "trade_date"],
    "fund_flow_ths_tushare": ["symbol", "trade_date"],
    "executives": ["record_id"],
    "executive_compensation": ["record_id"],
    "investor_qa": ["qa_id"],
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
    "fund_flow_ths": ["symbol", "trade_date"],
    "sector_fund_flow_ths": ["board_type", "sector_code", "trade_date"],
    "news_headlines": ["news_id"],
    "flash_news_wire": ["wire_id", "wire_source"],
    "economic_calendar": ["event_id"],
    "economic_events": ["event_id"],
    "market_holidays": ["holiday_id"],
    "delisting_events": ["symbol"],
}


class SchemaValidationError(ValueError):
    """Raised when a DataFrame does not match the dataset contract."""


def required_columns_for_dataset(dataset: str, schema: dict[str, pl.DataType]) -> list[str]:
    """Return columns that must be present and non-null for a stored row.

    Most fundamental fields are legitimately nullable (for example PE for a
    loss-making company, or Sina's unavailable turnover). Primary keys and
    provenance are never nullable. Beyond those, the registry's
    ``value_rules.non_null`` names the fields whose null means the source row
    was malformed: a market bar's price and volume, a calendar's ``is_trading``
    (Polars casts an invalid boolean to null, and an unknown session read as
    non-trading would silently thin the calendar).
    """
    from cnequity.domain.value_rules import rules_for

    required = list(PRIMARY_KEYS.get(dataset, []))
    required.extend(col for col in PROVENANCE if col in schema and col not in required)
    required.extend(col for col in rules_for(dataset).non_null if col in schema)
    return list(dict.fromkeys(required))


def market_bar_violations(dataset: str, columns: list[str]) -> pl.Expr | None:
    """Impossible equity/commodity market rows, shared by writers and audits.

    Historical Parquet can omit nullable columns, so only refer to columns
    actually present in the frame being checked. Applies to the datasets whose
    registry ``value_rules`` mark them ``market_bar``; a null price (a session
    without a trade) is not a violation.
    """
    from cnequity.domain.value_rules import rules_for

    if not rules_for(dataset).market_bar:
        return None

    checks: list[pl.Expr] = []
    price_cols = [col for col in ("open", "high", "low", "close", "price") if col in columns]
    checks.extend(pl.col(col) <= 0 for col in price_cols)
    if "volume" in columns:
        checks.append(pl.col("volume") < 0)
    if "amount" in columns:
        checks.append(pl.col("amount").is_not_null() & (pl.col("amount") < 0))
    if "pre_close" in columns:
        checks.append(pl.col("pre_close").is_not_null() & (pl.col("pre_close") <= 0))

    if all(col in columns for col in ("open", "high", "low", "close")):
        # Intraday sources emit zero-volume carried-forward placeholders for
        # suspended names. Their OHLC fields can legitimately straddle the
        # stale close, so only enforce the candle envelope on rows that carry
        # an actual print. Positive-price and non-negative-volume checks above
        # still apply to every row.
        ohlc_row = (
            pl.col("volume").is_null() | (pl.col("volume") > 0)
            if "volume" in columns
            else pl.lit(True)
        )
        checks.extend(
            [
                ohlc_row & (pl.col("high") < pl.col("open")),
                ohlc_row & (pl.col("high") < pl.col("close")),
                ohlc_row & (pl.col("low") > pl.col("open")),
                ohlc_row & (pl.col("low") > pl.col("close")),
                ohlc_row & (pl.col("low") > pl.col("high")),
            ]
        )

    return pl.any_horizontal(checks) if checks else None


def _validate_bar_semantics(df: pl.DataFrame, dataset: str) -> None:
    """Reject impossible numeric market rows before they reach Parquet."""
    predicate = market_bar_violations(dataset, df.columns)
    if predicate is None or df.is_empty():
        return
    bad = df.filter(predicate).height
    if bad:
        raise SchemaValidationError(
            f"dataset '{dataset}': {bad} row(s) violate numeric market-data invariants"
        )


def derivative_bar_violations(dataset: str) -> pl.Expr | None:
    """Rows of a futures/option bar frame that cannot be true, as one predicate.

    Written from what the exchanges actually publish, sampled once a quarter
    across every exchange's history (2026-09-25), not from how a bar "should"
    look. Volume can be positive with only a close printed — delivery-month
    sessions, exchange-for-physical and exercise-related trades leave no
    open/high/low — and an expiring option can carry no settlement at all, or
    settle at exactly zero when it expires out of the money. What stays
    impossible: a non-positive trade price or futures settlement, a negative
    option settlement, a missing or negative count, and a full candle whose
    high or low does not bound its open and close. Every source prints 0 for no
    volume or no position, so a missing count is a malformed cell, not a zero.

    Applies to the datasets whose ``value_rules`` say ``bar_kind="derivative"``.
    A check names only columns the dataset stores: ``futures_continuous`` has
    no ``pre_settle`` or ``amount``.
    """
    from cnequity.domain.value_rules import rules_for

    if rules_for(dataset).bar_kind != "derivative":
        return None
    stored = set(DATASET_SCHEMAS.get(dataset, ()))
    ohlc = ("open", "high", "low", "close")
    every = pl.all_horizontal([pl.col(c).is_not_null() for c in ohlc])
    if dataset == "futures_minute_bars":
        return pl.any_horizontal(
            [
                *[pl.col(c).is_not_null() & (pl.col(c) <= 0) for c in ohlc],
                pl.col("volume").is_null() | (pl.col("volume") < 0),
                pl.col("open_interest").is_null() | (pl.col("open_interest") < 0),
                every & (pl.col("high") < pl.max_horizontal("open", "close")),
                every & (pl.col("low") > pl.min_horizontal("open", "close")),
            ]
        )
    settle_floor = pl.col("settle") < 0 if dataset == "option_bars" else pl.col("settle") <= 0
    checks = [
        pl.col("settle").is_not_null() & settle_floor,
        *[pl.col(c).is_not_null() & (pl.col(c) <= 0) for c in ohlc],
        pl.col("volume").is_null() | (pl.col("volume") < 0),
        pl.col("open_interest").is_null() | (pl.col("open_interest") < 0),
        every & (pl.col("high") < pl.max_horizontal("open", "close")),
        every & (pl.col("low") > pl.min_horizontal("open", "close")),
    ]
    if "pre_settle" in stored:
        checks.append(pl.col("pre_settle").is_not_null() & (pl.col("pre_settle") <= 0))
    if "amount" in stored:
        checks.append(pl.col("amount").is_not_null() & (pl.col("amount") < 0))
    if dataset == "option_bars":
        checks.extend(
            [
                pl.col("strike") <= 0,
                ~pl.col("option_type").is_in(["C", "P"]),
                # SHFE prints deep in-the-money puts at -1.000001.
                pl.col("delta").is_not_null() & (pl.col("delta").abs() > 1.0001),
                pl.col("exercise_volume").is_not_null() & (pl.col("exercise_volume") < 0),
                pl.col("implied_vol").is_not_null() & (pl.col("implied_vol") < 0),
                pl.col("series_implied_vol").is_not_null() & (pl.col("series_implied_vol") < 0),
            ]
        )
    return pl.any_horizontal(checks)


def _validate_derivative_bar_semantics(df: pl.DataFrame, dataset: str) -> None:
    """Refuse a futures/option frame holding a row :func:`derivative_bar_violations` names.

    Kept apart from the equity bar checks, which require a positive price on
    every row: an option strike that did not trade still has a settlement, and
    that settlement is the row. The step quarantines offending rows before
    they get here, so this only fires on a writer that skipped that.
    """
    predicate = derivative_bar_violations(dataset)
    if predicate is None or df.is_empty():
        return
    bad = df.filter(predicate).height
    if bad:
        raise SchemaValidationError(
            f"dataset '{dataset}': {bad} row(s) violate futures/option bar invariants"
        )


def _validate_finite_values(df: pl.DataFrame, dataset: str) -> None:
    """Reject NaN/Inf in any optional numeric field as well as required ones."""
    float_columns = [col for col, dtype in df.schema.items() if dtype in (pl.Float32, pl.Float64)]
    if not float_columns or df.is_empty():
        return
    bad_counts = df.select(
        [
            (pl.col(col).is_not_null() & ~pl.col(col).is_finite()).sum().alias(col)
            for col in float_columns
        ]
    ).row(0, named=True)
    invalid = {col: int(count) for col, count in bad_counts.items() if count}
    if invalid:
        detail = ", ".join(f"{col}={count}" for col, count in invalid.items())
        raise SchemaValidationError(
            f"dataset '{dataset}': non-finite numeric values are not allowed: {detail}"
        )


def validate_dataframe(
    df: pl.DataFrame,
    dataset: str,
    *,
    allow_missing_optional: bool = False,
) -> pl.DataFrame:
    """Cast and validate *df* against the curated schema for *dataset*.

    Writers use the strict default: every registered column must be present.
    Historical audits may set ``allow_missing_optional`` because old Parquet
    files can legitimately predate a nullable column that was added to the
    schema. Primary keys, provenance, and core bar fields remain mandatory in
    that mode; a file that lacks one of those is still invalid.
    """
    schema = DATASET_SCHEMAS.get(dataset)
    if schema is None:
        return df

    # The bitemporal columns are deliberately absent from DATASET_SCHEMAS (see
    # domain/pit.py: optional until every writer can fill them), but the
    # projection below selects exactly the registered columns — which silently
    # dropped them on the way to disk, so a PIT dataset could never actually
    # store one. Carry them through when the caller already has them, without
    # making them part of the registered shape.
    pit_passthrough: tuple[str, ...] = ()
    if dataset in PIT_DATASET_NAMES:
        pit_passthrough = tuple(
            col for col in PIT_STORAGE_COLUMNS if col in df.columns and col not in schema
        )

    if df.is_empty():
        empty_schema = dict(schema)
        empty_schema.update({col: PIT_STORAGE_DTYPES[col] for col in pit_passthrough})
        return pl.DataFrame(schema=empty_schema)

    if dataset == "trading_status":
        # Rows written before ST moved out of `status` into its own column are
        # upgraded here rather than at each of the half-dozen read paths
        # (reader, compact, repartition, derive, audit). Idempotent, so it also
        # no-ops on an already migrated lake; delete this once
        # the old status/risk_warning split migration has been run
        # everywhere. See domain/trading_status.py.
        from cnequity.domain.trading_status import normalize_legacy

        df = normalize_legacy(df)

    schema = DATASET_SCHEMAS.get(dataset, {})
    absent = [c for c in OPTIONAL_COLUMNS if c in schema and c not in df.columns]
    if absent:
        df = df.with_columns(pl.lit(None, dtype=schema[c]).alias(c) for c in absent)

    if dataset == "corporate_actions":
        for name, dtype in (
            ("payment_date", pl.Date),
            ("payment_source", pl.Utf8),
            ("reference_price", pl.Float64),
        ):
            if name not in df.columns:
                df = df.with_columns(pl.lit(None, dtype=dtype).alias(name))
        if "action_type" in df.columns:
            is_reorg = pl.col("action_type").is_in(list(REFERENCE_PRICE_ACTIONS))
            reference = pl.col("reference_price").cast(pl.Float64, strict=False)
            bad_reference = (is_reorg & (reference.is_null() | ~(reference > 0))) | (
                ~is_reorg & reference.is_not_null()
            )
            if df.filter(bad_reference).height:
                raise SchemaValidationError(
                    "reference_price is required on reorg_transfer and exchange_reference rows "
                    "and only allowed there"
                )
        # Old rows predate explicit fund unit splits and retain neutral units.
        # A new split event must supply its ratio; never guess it from a bonus.
        is_split = (
            pl.col("action_type") == "unit_split" if "action_type" in df.columns else pl.lit(False)
        )
        if "split_factor" not in df.columns:
            if df.filter(is_split).height:
                raise SchemaValidationError("unit_split requires an explicit split_factor")
            df = df.with_columns(pl.lit(1.0).alias("split_factor"))
        factor = pl.col("split_factor").cast(pl.Float64, strict=False)
        invalid = (
            (pl.col("split_factor").is_not_null() & factor.is_null())
            | (factor.is_not_null() & (~factor.is_finite() | (factor <= 0)))
            | (is_split & (factor.is_null() | (factor == 1)))
            | (~is_split & factor.is_not_null() & (factor != 1))
        )
        if df.filter(invalid).height:
            raise SchemaValidationError("invalid unit split factor or action_type")
        stock_terms = [
            column
            for column in (
                "cash_dividend",
                "bonus_ratio",
                "transfer_ratio",
                "allotment_ratio",
                "allotment_price",
            )
            if column in df.columns
        ]
        if (
            stock_terms
            and df.filter(
                is_split
                & pl.any_horizontal(
                    [
                        pl.col(c).cast(pl.Float64, strict=False).fill_null(0) != 0
                        for c in stock_terms
                    ]
                )
            ).height
        ):
            raise SchemaValidationError("unit_split cannot carry stock distribution terms")
        df = df.with_columns(factor.fill_null(1.0).alias("split_factor"))

    if dataset == "valuation_metrics":
        from cnequity.domain.valuation import MV_BASES

        # Columns added after the original valuation contract. A legacy row's
        # basis stays unknown (null); it is never inferred here.
        df = df.with_columns(
            [
                pl.lit(None, dtype=schema[col]).alias(col)
                for col in ("pe_dynamic", "total_mv_basis", "float_mv_basis", "shares_as_of")
                if col not in df.columns
            ]
        )
        for col in ("total_mv_basis", "float_mv_basis"):
            unknown = set(df.get_column(col).drop_nulls().cast(pl.Utf8).unique()) - MV_BASES
            if unknown:
                raise SchemaValidationError(
                    f"dataset 'valuation_metrics': unknown {col} {sorted(unknown)}"
                )

    missing = [col for col in schema if col not in df.columns]
    if allow_missing_optional:
        required = required_columns_for_dataset(dataset, schema)
        missing_required = [col for col in required if col not in df.columns]
        if missing_required:
            raise SchemaValidationError(
                f"dataset '{dataset}': missing required columns {missing_required}"
            )
        columns = [col for col in schema if col in df.columns]
    else:
        columns = list(schema)
    if missing and not allow_missing_optional:
        raise SchemaValidationError(f"dataset '{dataset}': missing columns {missing}")

    casts = []
    for col in columns:
        dtype = schema[col]
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
    for col in pit_passthrough:
        casts.append(pl.col(col).cast(PIT_STORAGE_DTYPES[col], strict=False))
    try:
        normalized = df.with_columns(casts).select([*columns, *pit_passthrough])
        # Vendor placeholders become nulls here, once, for every writer and
        # reader: see domain/value_rules.py.
        from cnequity.domain.value_rules import apply_value_rules

        normalized = apply_value_rules(normalized, dataset)
    except pl.exceptions.PolarsError as exc:
        # ``strict=False`` turns many bad scalar casts into nulls, but Polars
        # still raises for some Utf8 -> Boolean conversions. Keep all schema
        # failures on the domain error boundary so writers and audits report a
        # consistent contract violation instead of leaking an engine-specific
        # exception.
        raise SchemaValidationError(
            f"dataset '{dataset}': values cannot be cast to the registered schema: {exc}"
        ) from exc

    required = [col for col in required_columns_for_dataset(dataset, schema) if col in columns]
    if required:
        null_counts = normalized.select(
            [pl.col(col).null_count().alias(col) for col in required]
        ).row(0, named=True)
        missing_values = {col: int(count) for col, count in null_counts.items() if count}
        if missing_values:
            detail = ", ".join(f"{col}={count}" for col, count in missing_values.items())
            raise SchemaValidationError(
                f"dataset '{dataset}': required columns contain null or unparseable values: {detail}"
            )

        string_required = [col for col in required if normalized.schema[col] == pl.Utf8]
        if string_required:
            blank_counts = normalized.select(
                [pl.col(col).str.strip_chars().eq("").sum().alias(col) for col in string_required]
            ).row(0, named=True)
            blank_values = {col: int(count) for col, count in blank_counts.items() if count}
            if blank_values:
                detail = ", ".join(f"{col}={count}" for col, count in blank_values.items())
                raise SchemaValidationError(
                    f"dataset '{dataset}': required string columns cannot be blank: {detail}"
                )

    _validate_finite_values(normalized, dataset)
    _validate_bar_semantics(normalized, dataset)
    _validate_derivative_bar_semantics(normalized, dataset)
    return normalized


def sanitize_dataset_rows(df: pl.DataFrame, dataset: str) -> pl.DataFrame:
    """Drop known transport artefacts before rows are persisted or merged."""
    if (
        dataset in {"daily_bars", "index_bars"}
        and "trade_date" in df.columns
        and df.schema["trade_date"] == pl.Date
    ):
        from cnequity.domain.holidays_cn import CLOSED_DATES

        # SSE/SZSE sessions are Monday-Friday. Historical source payloads also
        # contain a small set of known holiday rows; retaining them contaminates
        # the derived calendar and creates false coverage gaps.
        return df.filter(
            (pl.col("trade_date").dt.weekday() <= 5)
            & ~pl.col("trade_date").dt.strftime("%Y-%m-%d").is_in(CLOSED_DATES)
        )
    if (
        dataset == "trading_calendar"
        and "trade_date" in df.columns
        and "is_trading" in df.columns
        and df.schema["trade_date"] == pl.Date
    ):
        from cnequity.domain.holidays_cn import CLOSED_DATES

        return df.with_columns(
            (
                pl.col("is_trading")
                & (pl.col("trade_date").dt.weekday() <= 5)
                & ~pl.col("trade_date").dt.strftime("%Y-%m-%d").is_in(CLOSED_DATES)
            ).alias("is_trading")
        )
    return df


def with_provenance(
    df: pl.DataFrame,
    source: str,
    data_version: str,
    *,
    preserve_fetched_at: bool = False,
) -> pl.DataFrame:
    """Stamp source / data_version / fetched_at onto every row of *df*.

    ``preserve_fetched_at`` is for adapters that carry a verified source
    capture time through a response-cache hit; ordinary callers keep the
    existing current-time stamp.

    A frame with **no columns at all** is returned untouched. Polars broadcasts
    a literal against a zero-column frame to length one, so stamping a bare
    ``pl.DataFrame()`` — the codebase's ordinary "nothing to report" value —
    would manufacture a row carrying provenance and no business fields. That
    row has no primary key, so a strict validate rejects it, but a diagonal
    concat with a real day's rows first fills its keys with nulls and launders
    it into the lake. A zero-*row* frame that has a schema is unaffected and
    still gets its columns.
    """
    from cnequity.domain.frames import is_blank

    if is_blank(df):
        return df

    # An adapter may pre-set `source` (e.g. MOCK_SOURCE) to flag row origin;
    # that marker must survive normalization.
    fetched_at = (
        pl.col("fetched_at").cast(FETCHED_AT_DTYPE)
        if preserve_fetched_at and "fetched_at" in df.columns
        else pl.lit(datetime.now(timezone.utc)).cast(FETCHED_AT_DTYPE)
    )
    cols = [pl.lit(data_version).alias("data_version"), fetched_at.alias("fetched_at")]
    if "source" not in df.columns:
        cols.append(pl.lit(source).alias("source"))
    return df.with_columns(cols)


def frame_from_rows(rows: list[dict], dataset: str) -> pl.DataFrame:
    """Build a frame from row dicts using the dataset's declared dtypes.

    `pl.DataFrame(rows)` reads the first hundred rows to pick a dtype. A column
    that is null across all of them becomes `Null`, and the first real value
    after that raises `could not append value ... to the builder`. Measured:
    `news_headlines` and `flash_news_wire` failed 22 times between 2026-09-12
    and 2026-09-16 on sessions whose first hundred flashes named no security,
    so `related_symbols` was typed away before the hundred-and-first arrived.

    Only the columns the rows actually carry are pinned, which leaves an
    adapter free to build an intermediate frame the stored schema does not
    describe.
    """
    if not rows:
        return pl.DataFrame()
    schema = DATASET_SCHEMAS.get(dataset)
    if not schema:
        return pl.DataFrame(rows, infer_schema_length=None)
    present = {key for row in rows for key in row}
    overrides = {col: dtype for col, dtype in schema.items() if col in present}
    return pl.DataFrame(rows, schema_overrides=overrides, infer_schema_length=None)
