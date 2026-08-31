from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.derive.trading_status_history import derive_suspension_history
from cnequity.storage.parquet import compact_dataset


def _derive_and_publish(cfg, *, run_id: str = "run-derive", **window) -> int:
    """Stage the derive and compact it, the way the daily run does.

    The staged rows are worthless until compact merges and publishes them, so
    every assertion below is made against the compacted partition rather than
    against a directory the derive wrote behind compact's back.
    """
    rows = derive_suspension_history(cfg, run_id, **window)
    compact_dataset(
        cfg.staging_root,
        cfg.curated_root,
        "trading_status",
        run_id,
        partition_col="trade_date",
    )
    return rows


def test_derive_suspension_tie_break_is_not_file_order_dependent(tmp_path):
    """Same-rank status rows choose the newest observation deterministically."""
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    day = date(2024, 6, 27)
    days = [date(2024, 6, 26), day, date(2024, 6, 28)]
    for current in days:
        _write(
            root,
            "trading_calendar",
            "trade_date",
            current.isoformat(),
            pl.DataFrame(
                {
                    "trade_date": [current],
                    "is_trading": [True],
                    "source": ["seed"],
                    "data_version": ["v1"],
                    "fetched_at": ["2024-06-27T00:00:00+00:00"],
                }
            ),
        )
        rows = [
            {
                "symbol": "600519.SH",
                "trade_date": current,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 100,
                "amount": 100.0,
                "source": "tdx_protocol",
                "data_version": "v1",
                "fetched_at": "2024-06-27T00:00:00+00:00",
            }
        ]
        if current != day:
            rows.append({**rows[0], "symbol": "000001.SZ"})
        _write(root, "daily_bars", "trade_date", current.isoformat(), pl.DataFrame(rows))
    (root / "curated" / "instruments").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "list_date": [date(2010, 1, 1), date(2010, 1, 1)],
            "delist_date": [None, None],
        }
    ).write_parquet(root / "curated" / "instruments" / "part-merged.parquet")

    # Both rows are final same-session evidence.  The newer one must win even
    # when it is written first in the existing partition.
    _write(
        root,
        "trading_status",
        "trade_date",
        "2024-06",
        pl.DataFrame(
            {
                "symbol": ["600519.SH", "600519.SH"],
                "trade_date": [day, day],
                "is_trading": [True, False],
                "status": ["N", "suspended"],
                "source": ["eastmoney", "eastmoney"],
                "data_version": ["v1", "v1"],
                "fetched_at": [
                    "2024-06-27T07:30:00+00:00",
                    "2024-06-27T08:30:00+00:00",
                ],
            }
        ),
    )

    assert _derive_and_publish(cfg) == 1
    out = pl.read_parquet(
        root / "curated" / "trading_status" / "trade_date=2024-06" / "part-merged.parquet"
    )
    assert out.filter(pl.col("symbol") == "600519.SH")["status"].to_list() == ["suspended"]


def test_derive_suspension_uses_canonical_calendar_rows(tmp_path):
    """A superseded trading-calendar row must not create a false suspension."""
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    day_before = date(2024, 6, 26)
    gap_day = date(2024, 6, 27)
    day_after = date(2024, 6, 28)

    for current in (day_before, day_after):
        _write(
            root,
            "trading_calendar",
            "trade_date",
            current.isoformat(),
            pl.DataFrame(
                {
                    "trade_date": [current],
                    "is_trading": [True],
                    "source": ["seed"],
                    "data_version": ["v1"],
                    "fetched_at": ["2024-06-28T00:00:00+00:00"],
                }
            ),
        )
    calendar_dir = root / "curated" / "trading_calendar" / f"trade_date={gap_day.isoformat()}"
    calendar_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "trade_date": [gap_day],
            "is_trading": [True],
            "source": ["seed"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-27T07:00:00+00:00"],
        }
    ).write_parquet(calendar_dir / "part-old.parquet")
    pl.DataFrame(
        {
            "trade_date": [gap_day],
            "is_trading": [False],
            "source": ["exchange"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-27T08:00:00+00:00"],
        }
    ).write_parquet(calendar_dir / "part-new.parquet")

    for current in (day_before, day_after):
        _write(
            root,
            "daily_bars",
            "trade_date",
            current.isoformat(),
            pl.DataFrame(
                {
                    "symbol": ["600519.SH"],
                    "trade_date": [current],
                    "open": [1.0],
                    "high": [1.0],
                    "low": [1.0],
                    "close": [1.0],
                    "volume": [100],
                    "amount": [100.0],
                    "source": ["tdx_protocol"],
                    "data_version": ["v1"],
                    "fetched_at": ["2024-06-28T00:00:00+00:00"],
                }
            ),
        )
    instruments = root / "curated" / "instruments"
    instruments.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "list_date": [date(2010, 1, 1)],
            "delist_date": [None],
        }
    ).write_parquet(instruments / "part-merged.parquet")

    assert _derive_and_publish(cfg) == 0


def _write(root, dataset, partition_col, val, df):
    d = root / "curated" / dataset / f"{partition_col}={val}"
    d.mkdir(parents=True, exist_ok=True)
    df.write_parquet(d / "part-merged.parquet")


def test_derive_suspension_from_bar_gaps(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    # calendar: 3 trading days
    days = [date(2024, 6, 26), date(2024, 6, 27), date(2024, 6, 28)]
    for d in days:
        _write(
            root,
            "trading_calendar",
            "trade_date",
            d.isoformat(),
            pl.DataFrame(
                {
                    "trade_date": [d],
                    "is_trading": [True],
                    "source": ["seed"],
                    "data_version": ["v1"],
                    "fetched_at": ["2024-06-28T00:00:00+00:00"],
                }
            ),
        )

    # bars: 600519 trades all 3 days; 000001 missing the middle day (suspended)
    def bar(sym, d):
        return {
            "symbol": sym,
            "trade_date": d,
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1,
            "amount": 1.0,
            "source": "tdx_protocol",
            "data_version": "v1",
            "fetched_at": "2024-06-28T00:00:00+00:00",
        }

    for d in days:
        rows = [bar("600519.SH", d)]
        if d != date(2024, 6, 27):
            rows.append(bar("000001.SZ", d))
        _write(root, "daily_bars", "trade_date", d.isoformat(), pl.DataFrame(rows))
    # instruments: both listed before window, not delisted
    (root / "curated" / "instruments").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "name": ["A"],
            "exchange": ["SH"],
            "asset_type": ["stock"],
            "list_date": [date(2010, 1, 1)],
            "delist_date": [None],
            "prev_symbol": [None],
            "source": ["tdx"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    ).write_parquet(root / "curated" / "instruments" / "part-merged.parquet")
    nested_instruments = root / "curated" / "instruments" / ".old-fragments"
    nested_instruments.mkdir()
    pl.DataFrame(
        {
            "symbol": ["000001.SZ"],
            "name": ["B"],
            "exchange": ["SZ"],
            "asset_type": ["stock"],
            "list_date": [date(2010, 1, 1)],
            "delist_date": [None],
            "prev_symbol": [None],
            "source": ["tdx"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    ).write_parquet(nested_instruments / "part-old.parquet")

    existing = root / "curated" / "trading_status" / "trade_date=2024-06" / "fragments"
    existing.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 26)],
            "is_trading": [True],
            "status": ["N"],
            "source": ["eastmoney"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    ).write_parquet(existing / "part-old.parquet")

    n = _derive_and_publish(cfg)
    assert n == 1  # only 000001 on 2024-06-27

    # trading_status is month-partitioned (DatasetSpec); never write day dirs.
    month_dir = root / "curated" / "trading_status" / "trade_date=2024-06"
    assert month_dir.is_dir()
    assert not (root / "curated" / "trading_status" / "trade_date=2024-06-27").exists()

    ts = pl.read_parquet(month_dir / "part-merged.parquet")
    assert (
        ts.filter(
            (pl.col("symbol") == "600519.SH") & (pl.col("trade_date") == date(2024, 6, 26))
        ).height
        == 1
    )
    assert [path.name for path in month_dir.rglob("*.parquet")] == ["part-merged.parquet"]
    susp = ts.filter(pl.col("status") == "suspended")
    assert susp.height == 1
    assert susp["symbol"][0] == "000001.SZ"
    assert susp["trade_date"][0] == date(2024, 6, 27)
    assert susp["is_trading"][0] is False
    assert susp["source"][0] == "derived_bar_gap"


def test_derive_suspension_treats_zero_volume_placeholder_as_missing(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    days = [date(2024, 6, 27), date(2024, 6, 28)]
    for d in days:
        _write(
            root,
            "trading_calendar",
            "trade_date",
            d.isoformat(),
            pl.DataFrame(
                {
                    "trade_date": [d],
                    "is_trading": [True],
                    "source": ["seed"],
                    "data_version": ["v1"],
                    "fetched_at": ["2024-06-28T00:00:00+00:00"],
                }
            ),
        )

    def bar(day: date, volume: int) -> dict:
        return {
            "symbol": "000001.SZ",
            "trade_date": day,
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "volume": volume,
            "amount": float(volume) * 10.0,
            "source": "tdx_protocol",
            "data_version": "v1",
            "fetched_at": "2024-06-28T00:00:00+00:00",
        }

    _write(
        root,
        "daily_bars",
        "trade_date",
        "2024-06-27",
        pl.DataFrame([bar(days[0], 0)]),
    )
    _write(
        root,
        "daily_bars",
        "trade_date",
        "2024-06-28",
        pl.DataFrame([bar(days[1], 100)]),
    )
    (root / "curated" / "instruments").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["000001.SZ"],
            "name": ["B"],
            "exchange": ["SZ"],
            "asset_type": ["stock"],
            "list_date": [date(2010, 1, 1)],
            "delist_date": [None],
            "prev_symbol": [None],
            "source": ["tdx"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    ).write_parquet(root / "curated" / "instruments" / "part-merged.parquet")

    assert _derive_and_publish(cfg) == 1
    status = pl.read_parquet(
        root / "curated" / "trading_status" / "trade_date=2024-06" / "part-merged.parquet"
    )
    assert status.filter(pl.col("status") == "suspended")["trade_date"].to_list() == [
        date(2024, 6, 27)
    ]


def test_derive_suspension_ignores_placeholder_only_symbol(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    days = [date(2024, 6, 27), date(2024, 6, 28)]
    for d in days:
        _write(
            root,
            "trading_calendar",
            "trade_date",
            d.isoformat(),
            pl.DataFrame(
                {
                    "trade_date": [d],
                    "is_trading": [True],
                    "source": ["seed"],
                    "data_version": ["v1"],
                    "fetched_at": ["2024-06-28T00:00:00+00:00"],
                }
            ),
        )

    rows = []
    for symbol, volume in (("600519.SH", 100), ("000001.SZ", 0)):
        for d in days:
            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": d,
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": volume,
                    "amount": float(volume),
                    "source": "tdx_protocol",
                    "data_version": "v1",
                    "fetched_at": "2024-06-28T00:00:00+00:00",
                }
            )
    _write(
        root,
        "daily_bars",
        "trade_date",
        "2024-06",
        pl.DataFrame(rows),
    )

    (root / "curated" / "instruments").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "list_date": [date(2010, 1, 1), date(2010, 1, 1)],
            "delist_date": [None, None],
        }
    ).write_parquet(root / "curated" / "instruments" / "part-merged.parquet")

    assert _derive_and_publish(cfg) == 0


def test_derive_suspension_empty_lake(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    assert _derive_and_publish(cfg) == 0


def test_derive_suspension_respects_end_window(tmp_path):
    """Gaps outside [--start,--end] must not be written."""
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    days = [
        date(2015, 12, 30),
        date(2015, 12, 31),
        date(2016, 1, 4),
        date(2016, 1, 5),
    ]

    def bar(sym, d):
        return {
            "symbol": sym,
            "trade_date": d,
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1,
            "amount": 1.0,
            "source": "tdx_protocol",
            "data_version": "v1",
            "fetched_at": "2016-01-05T00:00:00+00:00",
        }

    for d in days:
        _write(
            root,
            "trading_calendar",
            "trade_date",
            d.isoformat(),
            pl.DataFrame(
                {
                    "trade_date": [d],
                    "is_trading": [True],
                    "source": ["seed"],
                    "data_version": ["v1"],
                    "fetched_at": ["2016-01-05T00:00:00+00:00"],
                }
            ),
        )
        # 000001 missing 2015-12-31 and 2016-01-04
        rows = [bar("600519.SH", d)]
        if d not in (date(2015, 12, 31), date(2016, 1, 4)):
            rows.append(bar("000001.SZ", d))
        _write(root, "daily_bars", "trade_date", d.isoformat(), pl.DataFrame(rows))

    (root / "curated" / "instruments").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "name": ["A", "B"],
            "exchange": ["SH", "SZ"],
            "asset_type": ["stock", "stock"],
            "list_date": [date(2010, 1, 1), date(2010, 1, 1)],
            "delist_date": [None, None],
            "prev_symbol": [None, None],
            "source": ["tdx", "tdx"],
            "data_version": ["v1", "v1"],
            "fetched_at": ["2016-01-05T00:00:00+00:00"] * 2,
        }
    ).write_parquet(root / "curated" / "instruments" / "part-merged.parquet")

    n = _derive_and_publish(cfg, start=date(2015, 1, 1), end=date(2015, 12, 31))
    assert n == 1
    ts = pl.read_parquet(
        root / "curated" / "trading_status" / "trade_date=2015-12" / "part-merged.parquet"
    )
    assert ts.filter(pl.col("status") == "suspended")["trade_date"].to_list() == [
        date(2015, 12, 31)
    ]
    assert not (root / "curated" / "trading_status" / "trade_date=2016-01").exists()


def test_the_derive_batch_does_not_overwrite_the_vendor_batch(tmp_path):
    """Both writers stage `trading_status` under one run_id.

    They are separate batches on purpose: a shared batch id would make one
    staging file replace the other, and the run would publish whichever step
    happened to write last.
    """
    from cnequity.domain.schemas import with_provenance
    from cnequity.steps.common import write_simple
    from cnequity.storage.parquet import StagingWriter

    root = tmp_path / "data"
    cfg = Config(data_root=root)
    _seed_single_gap_lake(root)

    run_id = "run-shared"
    vendor = with_provenance(
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 27)],
                "is_trading": [True],
                "status": ["normal"],
                "risk_warning": [None],
            }
        ),
        source="eastmoney",
        data_version="v1",
    )
    write_simple(cfg, run_id, "trading_status", vendor)
    assert derive_suspension_history(cfg, run_id, batch_id="derive-0") == 1

    staged = StagingWriter(cfg.staging_root).list_run_files("trading_status", run_id)
    assert len(staged) == 2

    compact_dataset(
        cfg.staging_root, cfg.curated_root, "trading_status", run_id, partition_col="trade_date"
    )
    published = pl.read_parquet(
        root / "curated" / "trading_status" / "trade_date=2024-06" / "part-merged.parquet"
    )
    assert set(published["source"].to_list()) == {"eastmoney", "derived_bar_gap"}


def test_the_shipped_daily_job_derives_between_bars_and_compact():
    """Ordering is the whole point: staged before compact, derived after bars."""
    from pathlib import Path

    from cnequity.config import load_config

    root = Path(__file__).resolve().parents[2]
    cfg = load_config(root / "configs" / "cnequity.example.toml")
    order = [step for wave in cfg.daily_waves for step in wave.steps]
    assert order.index("daily_bars") < order.index("trading_status_derive")
    assert order.index("trading_status_derive") < order.index("compact")


def _seed_single_gap_lake(root):
    """A two-symbol lake where 000001.SZ has one interior missing session."""
    days = [date(2024, 6, 26), date(2024, 6, 27), date(2024, 6, 28)]

    def bar(sym, day):
        return {
            "symbol": sym,
            "trade_date": day,
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1,
            "amount": 1.0,
            "source": "tdx_protocol",
            "data_version": "v1",
            "fetched_at": "2024-06-28T00:00:00+00:00",
        }

    for day in days:
        _write(
            root,
            "trading_calendar",
            "trade_date",
            day.isoformat(),
            pl.DataFrame(
                {
                    "trade_date": [day],
                    "is_trading": [True],
                    "source": ["seed"],
                    "data_version": ["v1"],
                    "fetched_at": ["2024-06-28T00:00:00+00:00"],
                }
            ),
        )
        rows = [bar("600519.SH", day)]
        if day != date(2024, 6, 27):
            rows.append(bar("000001.SZ", day))
        _write(root, "daily_bars", "trade_date", day.isoformat(), pl.DataFrame(rows))

    (root / "curated" / "instruments").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "list_date": [date(2010, 1, 1), date(2010, 1, 1)],
            "delist_date": [None, None],
        }
    ).write_parquet(root / "curated" / "instruments" / "part-merged.parquet")


def test_init_derives_the_whole_history_after_the_bars_are_committed():
    """A tail window can never reach 2016, so init runs the derive unbounded.

    Ordering matters twice here: the derive reads *committed* bars, so it has
    to follow phase4's compact, and its own rows are only published by the
    compact that runs after it in the same phase.
    """
    from cnequity.orchestrator.deps import step_execution_levels
    from cnequity.orchestrator.init_phases import (
        DEFAULT_INIT_PHASES,
        INIT_PHASE_STEPS,
        phase_backfill,
    )

    phases = DEFAULT_INIT_PHASES
    assert phases.index("phase4_finalize") < phases.index("phase5_derive_and_publish")
    steps = INIT_PHASE_STEPS["phase5_derive_and_publish"]
    assert steps == ["trading_status_derive", "compact"]
    assert step_execution_levels(steps) == [["trading_status_derive"], ["compact"]]
    assert phase_backfill("phase5_derive_and_publish") is True


def test_backfill_mode_drops_the_daily_tail_window(tmp_path, monkeypatch):
    from cnequity.steps.reference import step_trading_status_derive

    seen: dict = {}

    def fake_derive(cfg, run_id, *, start=None, end=None, batch_id="derive-0"):
        seen.update(start=start, end=end)
        return 0

    monkeypatch.setattr(
        "cnequity.derive.trading_status_history.derive_suspension_history", fake_derive
    )
    cfg = Config(data_root=tmp_path / "data")
    trade_date = date(2026, 5, 20)

    step_trading_status_derive(cfg, trade_date, "run-daily", {})
    assert seen == {"start": date(2026, 2, 19), "end": trade_date}

    cfg._backfill = True
    step_trading_status_derive(cfg, trade_date, "run-init", {})
    assert seen == {"start": None, "end": None}
