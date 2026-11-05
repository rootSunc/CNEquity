"""A repair may not delete an ex-date that falls inside a trading halt.

The gate decides "the market was open that day" from daily_bars. It used to
ask whether a ``trade_date=<day>`` directory existed, which only worked while
daily_bars was day-partitioned; month partitions would have made every day
look closed and let every such removal through.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.quality.publication import _halt_dated_removals

OPEN, HALTED_DAY, SATURDAY = date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 8)


def _write_bars(root: Path, granularity: str) -> None:
    rows = pl.DataFrame(
        {
            # 600001 trades both days; 600002 is halted on HALTED_DAY.
            "symbol": ["600001.SH", "600002.SH", "600001.SH"],
            "trade_date": [OPEN, OPEN, HALTED_DAY],
            "close": [10.0, 20.0, 10.5],
            "volume": [100, 200, 100],
        }
    )
    for value, group in rows.group_by(
        pl.col("trade_date").dt.strftime("%Y-%m-%d" if granularity == "day" else "%Y-%m")
    ):
        out = root / f"trade_date={value[0]}"
        out.mkdir(parents=True, exist_ok=True)
        group.write_parquet(out / "part-0.parquet")


def _actions(*rows: tuple[str, date]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "ex_date": [r[1] for r in rows],
            "action_type": ["dividend"] * len(rows),
        },
        schema={"symbol": pl.Utf8, "ex_date": pl.Date, "action_type": pl.Utf8},
    )


@pytest.mark.parametrize("granularity", ["day", "month"])
def test_removing_a_halt_dated_action_is_caught_in_any_layout(tmp_path, granularity):
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(cfg.curated_root / "daily_bars", granularity)
    before = _actions(("600002.SH", HALTED_DAY), ("600001.SH", SATURDAY))
    after = _actions()
    findings = _halt_dated_removals(cfg, before, after)
    assert len(findings) == 1
    assert findings[0]["violations"] == {f"600002.SH|{HALTED_DAY}|dividend": 1}


@pytest.mark.parametrize("granularity", ["day", "month"])
def test_a_removal_on_a_closed_market_day_is_allowed(tmp_path, granularity):
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(cfg.curated_root / "daily_bars", granularity)
    assert _halt_dated_removals(cfg, _actions(("600001.SH", SATURDAY)), _actions()) == []


def test_a_lake_without_bars_has_no_open_days_to_protect(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    assert _halt_dated_removals(cfg, _actions(("600002.SH", HALTED_DAY)), _actions()) == []
