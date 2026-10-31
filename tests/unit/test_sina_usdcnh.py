"""Sina fx_susdcnh day kline: the close is the last price, and the window is applied here."""

from datetime import date

import pytest

from cnequity.adapters.sina.usdcnh import parse_usdcnh_body

# Field order measured 2026-10-31 against the EastMoney OHLC of the same day:
# date, open, low, high, close. 6.65280 is the close; 6.65780 is the high.
_BODY = (
    'var _fx_susdcnh="2014-11-07,6.13750,6.13030,6.13770,6.13410,'
    "|2026-10-29,6.66170,6.65490,6.66300,6.65580,"
    "|2026-10-30,6.65600,6.64870,6.65780,6.65280,"
    '|2026-10-31,9.00000,8.00000,100.00000,6.64000,";'
)


def test_close_is_the_last_price_and_bars_outside_the_window_are_dropped():
    rows = parse_usdcnh_body(_BODY, start=date(2026, 10, 29), end=date(2026, 10, 30))
    assert [(row["obs_date"], row["source"]) for row in rows] == [
        (date(2026, 10, 29), "sina"),
        (date(2026, 10, 30), "sina"),
    ]
    assert rows[0]["value"] == pytest.approx(6.65580)
    assert rows[1]["value"] == pytest.approx(6.65280)


def test_a_close_outside_the_sane_range_is_dropped():
    body = 'var _fx="2026-10-30,6.65,6.64,6.66,100.0,|2026-10-29,6.66,6.65,6.67,6.6558,";'
    rows = parse_usdcnh_body(body, start=date(2026, 10, 1), end=date(2026, 10, 31))
    assert [row["obs_date"] for row in rows] == [date(2026, 10, 29)]


def test_a_body_without_a_dated_bar_is_refused():
    with pytest.raises(ValueError, match="没有行情"):
        parse_usdcnh_body("", start=date(2026, 10, 30), end=date(2026, 10, 30))
    with pytest.raises(ValueError, match="没有日期"):
        parse_usdcnh_body('var _fx="not-a-bar";', start=date(2026, 10, 30), end=date(2026, 10, 30))
