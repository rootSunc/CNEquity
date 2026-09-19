"""A bar whose turnover, share count and range disagree with each other.

`daily_bars_volume_unit_findings` takes a median per source — the right shape
for a source that rescaled every row, the wrong one for a source that got a few
rows wrong, because the median stays at 1.0 and those rows never surface.
160806.SZ on 2026-09-11 is the shape of it: amount 2,825.6 agreeing to the cent
with the minute stream, a range of 2.016..2.032, and a volume of 154,400 — an
implied 0.0183 per share.
"""

from datetime import date, datetime, timezone

import polars as pl

from cnequity.config import Config
from cnequity.quality.unit_checks import daily_bars_implied_price_findings
from cnequity.storage.layout import init_data_layout

DAY = date(2026, 9, 11)
FETCHED = datetime(2026, 9, 19, tzinfo=timezone.utc)


def _bar(symbol: str, *, volume: int, amount: float, low: float, high: float) -> dict:
    return {
        "symbol": symbol,
        "trade_date": DAY,
        "open": low,
        "high": high,
        "low": low,
        "close": high,
        "volume": volume,
        "amount": amount,
        "source": "tdx_protocol",
        "data_version": "v1",
        "fetched_at": FETCHED,
    }


def _lake(tmp_path, bars: list[dict], classes: dict[str, str] | None = None) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    part = cfg.curated_root / "daily_bars" / f"trade_date={DAY.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(bars).write_parquet(part / "part-0.parquet")
    if classes:
        root = cfg.curated_root / "instruments"
        root.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            [
                {
                    "symbol": symbol,
                    "name": symbol[:6],
                    "exchange": symbol.split(".")[1],
                    "asset_type": asset_type,
                    "list_date": date(2020, 1, 2),
                    "delist_date": None,
                    "prev_symbol": None,
                    "source": "tdx_protocol",
                    "data_version": "v1",
                    "fetched_at": FETCHED,
                }
                for symbol, asset_type in classes.items()
            ]
        ).write_parquet(root / "part-merged.parquet")
    return cfg


def test_a_bar_that_agrees_with_itself_says_nothing(tmp_path):
    cfg = _lake(
        tmp_path,
        [_bar("600519.SH", volume=1000, amount=2020.0, low=2.016, high=2.032)],
        {"600519.SH": "stock"},
    )

    assert daily_bars_implied_price_findings(cfg, DAY) == []


def test_a_volume_that_cannot_produce_that_turnover_is_reported(tmp_path):
    """The real 160806.SZ row: turnover matches the minute stream, volume does not."""
    cfg = _lake(
        tmp_path,
        [_bar("160806.SZ", volume=154400, amount=2825.6, low=2.016, high=2.032)],
        {"160806.SZ": "etf"},
    )

    (finding,) = daily_bars_implied_price_findings(cfg, DAY)

    assert finding["check"] == "daily_bars_implied_price"
    assert finding["severity"] == "warning"
    assert finding["asset_type"] == "etf"
    assert finding["rows"] == 1
    assert finding["sample"][0]["implied_price"] == 0.0183


def test_each_asset_class_is_counted_on_its_own(tmp_path):
    """Hundreds of fund days must not bury a handful of stock ones."""
    cfg = _lake(
        tmp_path,
        [
            _bar("160806.SZ", volume=154400, amount=2825.6, low=2.016, high=2.032),
            _bar("161729.SZ", volume=100000, amount=1000.0, low=1.0, high=1.02),
            _bar("920491.BJ", volume=1552253, amount=31_057_000.0, low=20.85, high=21.64),
        ],
        {"160806.SZ": "etf", "161729.SZ": "etf", "920491.BJ": "stock"},
    )

    findings = {f["asset_type"]: f["rows"] for f in daily_bars_implied_price_findings(cfg, DAY)}

    assert findings == {"etf": 2, "stock": 1}


def test_rounding_in_the_published_figures_is_not_a_break(tmp_path):
    """Half a percent outside the range is the source rounding, not a defect."""
    cfg = _lake(
        tmp_path,
        [_bar("600519.SH", volume=1000, amount=2005.0, low=2.016, high=2.032)],
        {"600519.SH": "stock"},
    )

    assert daily_bars_implied_price_findings(cfg, DAY) == []


def test_a_lake_with_no_catalogue_still_reports_the_break(tmp_path):
    cfg = _lake(tmp_path, [_bar("160806.SZ", volume=154400, amount=2825.6, low=2.016, high=2.032)])

    (finding,) = daily_bars_implied_price_findings(cfg, DAY)

    assert finding["asset_type"] == "unknown"
    assert finding["rows"] == 1


def test_an_untraded_day_is_not_a_break(tmp_path):
    """Zero volume and zero turnover is a halt, and says nothing about units."""
    cfg = _lake(
        tmp_path,
        [_bar("600519.SH", volume=0, amount=0.0, low=2.016, high=2.032)],
        {"600519.SH": "stock"},
    )

    assert daily_bars_implied_price_findings(cfg, DAY) == []
