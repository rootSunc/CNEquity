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
from tests.catalog_seed import publish_folders

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
    publish_folders(cfg, "daily_bars", *(["instruments"] if classes else []))
    return cfg


def test_a_bar_that_agrees_with_itself_says_nothing(tmp_path):
    cfg = _lake(
        tmp_path,
        [_bar("600519.SH", volume=1000, amount=2020.0, low=2.016, high=2.032)],
        {"600519.SH": "stock"},
    )

    assert daily_bars_implied_price_findings(cfg, DAY) == []


def test_a_share_count_rounded_to_whole_lots_says_nothing(tmp_path):
    """TDX history counts lots and keeps turnover exact: 2,155 shares at 1.305
    arrive as 2,100 shares and 2,812.3 yuan, an implied 1.339 over a 1.305 high."""
    cfg = _lake(
        tmp_path,
        [_bar("164508.SZ", volume=2100, amount=2812.3, low=1.305, high=1.305)],
        {"164508.SZ": "etf"},
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


def _blocks(cfg: Config, rows: list[tuple[str, float, float]]) -> None:
    part = cfg.curated_root / "block_trades" / f"trade_date={DAY.strftime('%Y-%m')}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "symbol": symbol,
                "trade_date": DAY,
                "price": amount / volume,
                "volume": volume,
                "amount": amount,
                "premium_ratio": None,
                "source": "eastmoney",
                "data_version": "v1",
                "fetched_at": FETCHED,
            }
            for symbol, volume, amount in rows
        ],
        schema_overrides={"premium_ratio": pl.Float64},
    ).write_parquet(part / "part-merged.parquet")
    publish_folders(cfg, "block_trades")


def test_a_beijing_day_is_judged_net_of_its_block_trades(tmp_path):
    """920491.BJ on 2026-09-16: 307,000 shares crossed as a block at 14.77, and
    Beijing bars count them. Net of the block the day averages 21.30."""
    cfg = _lake(
        tmp_path,
        [_bar("920491.BJ", volume=1552253, amount=31_057_000.0, low=20.85, high=21.64)],
        {"920491.BJ": "stock"},
    )
    _blocks(cfg, [("920491.BJ", 30.6995, 453.431615)])

    assert daily_bars_implied_price_findings(cfg, DAY) == []


def test_a_block_trade_does_not_excuse_a_shanghai_row(tmp_path):
    """Shanghai and Shenzhen bars leave block trades out; netting would hide a
    real break there."""
    cfg = _lake(
        tmp_path,
        [_bar("600491.SH", volume=1552253, amount=31_057_000.0, low=20.85, high=21.64)],
        {"600491.SH": "stock"},
    )
    _blocks(cfg, [("600491.SH", 30.6995, 453.431615)])

    (finding,) = daily_bars_implied_price_findings(cfg, DAY)
    assert finding["rows"] == 1


def test_a_beijing_day_past_block_trade_coverage_is_awaiting_not_broken(tmp_path):
    from datetime import timedelta

    cfg = _lake(
        tmp_path,
        [_bar("920701.BJ", volume=1552253, amount=31_057_000.0, low=20.85, high=21.64)],
        {"920701.BJ": "stock"},
    )
    part = (
        cfg.curated_root
        / "block_trades"
        / f"trade_date={(DAY - timedelta(days=40)).strftime('%Y-%m')}"
    )
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["920001.BJ"],
            "trade_date": [DAY - timedelta(days=40)],
            "volume": [1.0],
            "amount": [1.0],
            "source": ["exchange"],
            "data_version": ["v1"],
            "fetched_at": [datetime(2026, 1, 1, tzinfo=timezone.utc)],
        }
    ).write_parquet(part / "part.parquet")
    publish_folders(cfg, "block_trades")
    (finding,) = daily_bars_implied_price_findings(cfg, DAY)
    assert finding["check"] == "daily_bars_implied_price_awaiting_blocks"
    assert finding["severity"] == "info"
