from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.quality.bar_finality import daily_bar_finality_findings


def _write(cfg, observations):
    root = cfg.curated_root / "daily_bars"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(observations).write_parquet(root / "part-0.parquet")


def _bar(hour, *, day=6, volume=100, symbol="000004.SZ"):
    return {
        "symbol": symbol,
        "trade_date": date(2026, 7, 6),
        "volume": volume,
        "fetched_at": datetime(2026, 7, day, hour, tzinfo=timezone.utc),
    }


def test_complete_date_key_can_still_hold_intraday_values(tmp_path):
    cfg = Config(data_root=tmp_path)
    _write(cfg, [_bar(2)])  # 10:00 CST, matching the real stale observation.
    findings = daily_bar_finality_findings(cfg, date(2026, 7, 6))
    assert len(findings) == 1
    assert findings[0]["rows"] == 1
    assert findings[0]["sample"][0]["symbol"] == "000004.SZ"
    assert daily_bar_finality_findings(cfg, date(2026, 7, 5)) == []


@pytest.mark.parametrize("row", [_bar(7), _bar(2, day=7), _bar(2, volume=0)])
def test_closed_historical_and_zero_volume_rows_are_not_intraday_trades(tmp_path, row):
    cfg = Config(data_root=tmp_path)
    _write(cfg, [row])
    assert daily_bar_finality_findings(cfg, date(2026, 7, 6)) == []


def test_latest_final_observation_supersedes_early_duplicate(tmp_path):
    cfg = Config(data_root=tmp_path)
    _write(cfg, [_bar(2), _bar(8)])
    assert daily_bar_finality_findings(cfg, date(2026, 7, 6)) == []
