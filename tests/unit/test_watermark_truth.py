"""The watermark answers "through what date do we have data", not "when did we run".

It is computed from the published rows whenever it is read, so there is no
stored claim that could run ahead of the data or need pulling back.
"""

from datetime import date

import polars as pl

import cnequity.steps  # noqa: F401 — register steps
from cnequity.config import Config
from cnequity.quality.verify import max_published_date, watermark_date

_VAL_SCHEMA_ROWS = ("pe_ttm", "pb", "ps_ttm", "total_mv", "float_mv")


def _write_valuation(cfg: Config, trade_date: date, symbols: int = 3) -> None:
    part = cfg.curated_root / "valuation_metrics" / f"trade_date={trade_date.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    frame = {
        "symbol": [f"60000{i}.SH" for i in range(symbols)],
        "trade_date": [trade_date] * symbols,
        **{col: [1.0] * symbols for col in _VAL_SCHEMA_ROWS},
        "source": ["baostock"] * symbols,
        "data_version": ["v1"] * symbols,
    }
    pl.DataFrame(frame).write_parquet(part / "part-merged.parquet")


def test_snapshot_watermark_follows_the_data_not_the_run_date(tmp_path):
    """A backfill writing an older day must not stamp the watermark with today."""
    cfg = Config(data_root=tmp_path / "data")
    _write_valuation(cfg, date(2026, 7, 20))

    # valuation_metrics is a snapshot dataset; the run happens on the 21st.
    assert watermark_date(cfg, "valuation_metrics", "trade_date") == date(2026, 7, 20)


def test_watermark_advances_when_the_day_actually_landed(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_valuation(cfg, date(2026, 7, 20))
    _write_valuation(cfg, date(2026, 7, 21))

    assert watermark_date(cfg, "valuation_metrics", "trade_date") == date(2026, 7, 21)


def test_valuation_watermark_does_not_cross_a_sparse_tip(tmp_path):
    """A low-coverage valuation tip must not become an incremental boundary."""
    cfg = Config(data_root=tmp_path / "data")
    calendar = cfg.curated_root / "trading_calendar" / "trade_date=2026"
    calendar.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "trade_date": [date(2026, 7, 20)],
            "is_trading": [True],
        }
    ).write_parquet(calendar / "part-0.parquet")

    bars = cfg.curated_root / "daily_bars" / "trade_date=2026-07-20"
    bars.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600000.SH", "600001.SH", "600002.SH"],
            "trade_date": [date(2026, 7, 20)] * 3,
        }
    ).write_parquet(bars / "part-0.parquet")
    _write_valuation(cfg, date(2026, 7, 20), symbols=1)

    assert watermark_date(cfg, "valuation_metrics", "trade_date") is None


def test_an_outage_accumulates_staleness_instead_of_being_masked(tmp_path):
    """The point of the fix: lag has to grow while a source stays dark.

    Stamping the watermark with the run date pinned the lag at zero forever, so
    lake_health called the dataset fresh on day one of an outage and on day ten
    alike. Anchored to the data, each day the source misses adds a day of lag
    until it crosses the staleness tolerance and gets reported.
    """
    from cnequity.quality.freshness import is_stale

    cfg = Config(data_root=tmp_path / "data")
    last_good = date(2026, 7, 20)
    _write_valuation(cfg, last_good)

    lags = []
    for day in (21, 22, 23, 25):
        anchor = date(2026, 7, day)
        # Source is dark: nothing new lands, so nothing compacts.
        mark = watermark_date(cfg, "valuation_metrics", "trade_date")
        lags.append((anchor - mark).days)

    assert lags == [1, 2, 3, 5], "lag must grow with the outage"
    assert is_stale("valuation_metrics", last_good, date(2026, 7, 21)) is False
    assert is_stale("valuation_metrics", last_good, date(2026, 7, 23)) is True


def test_dense_watermark_stops_before_an_interior_trading_day_gap(tmp_path):
    """A later row must not make an earlier missing session unreachable."""
    cfg = Config(data_root=tmp_path / "data")
    calendar = cfg.curated_root / "trading_calendar" / "trade_date=2026"
    calendar.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "trade_date": [date(2026, 7, 20), date(2026, 7, 21), date(2026, 7, 22)],
            "is_trading": [True, True, True],
        }
    ).write_parquet(calendar / "part-0.parquet")

    for day in (date(2026, 7, 20), date(2026, 7, 22)):
        part = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [day],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.5],
                "volume": [1000],
                "amount": [10_500.0],
                "source": ["tdx_protocol"],
                "data_version": ["v1"],
                "fetched_at": [f"{day.isoformat()}T00:00:00+00:00"],
            }
        ).write_parquet(part / "part-merged.parquet")

    assert watermark_date(cfg, "daily_bars", "trade_date") == date(2026, 7, 20)


def test_max_partition_date_keeps_root_legacy_files_in_mixed_layout(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = cfg.curated_root / "valuation_metrics"
    part = root / "trade_date=2026-07-20"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"trade_date": [date(2026, 7, 20)]}).write_parquet(part / "part-0.parquet")
    pl.DataFrame({"trade_date": [date(2026, 7, 21)]}).write_parquet(root / "part-legacy.parquet")

    assert max_published_date(cfg, "valuation_metrics", "trade_date") == date(2026, 7, 21)


def test_daily_bars_dense_prefix_starts_at_the_backfill_floor(tmp_path, monkeypatch):
    """A lone recovered 1990s delisting must not pin the live watermark."""
    from cnequity.steps.common import BACKFILL_START

    seen = {}

    def fake(config, spec, *, start=None):
        seen[spec.name] = start
        return date(2026, 10, 28)

    monkeypatch.setattr("cnequity.quality.verify.last_contiguous_dense_date", fake)
    cfg = Config(data_root=tmp_path / "data")

    assert watermark_date(cfg, "daily_bars", "trade_date") == date(2026, 10, 28)
    assert seen == {"daily_bars": BACKFILL_START}
