from datetime import date, datetime, timezone

import polars as pl
import pytest

import cnequity.steps  # noqa: F401 — register steps used by step_compact
from cnequity.config import Config
from cnequity.derive.trading_status_history import derive_suspension_history
from cnequity.domain.trading_status import DERIVED_BAR_GAP_SOURCE, status_evidence_rank
from cnequity.query.reader import load
from cnequity.steps.finalize import step_compact


def test_status_evidence_precedence_is_pit_aware():
    trade_day = date(2024, 6, 28)
    assert status_evidence_rank({"source": "baostock", "trade_date": trade_day}) == 0
    assert (
        status_evidence_rank(
            {
                "source": "eastmoney",
                "trade_date": trade_day,
                "fetched_at": datetime(2024, 6, 28, 7, 30, tzinfo=timezone.utc),
            }
        )
        == 0
    )
    assert (
        status_evidence_rank(
            {
                "source": "eastmoney",
                "trade_date": trade_day,
                "fetched_at": datetime(2024, 7, 1, 8, 0, tzinfo=timezone.utc),
            }
        )
        == 2
    )
    assert status_evidence_rank({"source": "derived_bar_gap", "trade_date": trade_day}) == 1


def _derive_and_compact(cfg: Config, run_id: str, trade_date: date, **kwargs) -> int:
    """Stage derived rows and publish them through a real compact step."""
    n = derive_suspension_history(cfg, run_id=run_id, **kwargs)
    if n:
        step_compact(cfg, trade_date, run_id, {})
    return n


def test_derive_stages_rows_with_status_schema(tmp_path):
    """3.1 — derived rows land in staging with the trading_status schema."""
    root = tmp_path / "data"
    cfg = Config(data_root=root)
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
    for d in days:
        rows = [_bar("600519.SH", d)]
        if d != date(2024, 6, 27):
            rows.append(_bar("000001.SZ", d))
        _write(root, "daily_bars", "trade_date", d.isoformat(), pl.DataFrame(rows))
    _write_instruments(root, ["600519.SH", "000001.SZ"])

    n = derive_suspension_history(cfg, run_id="run-a")
    assert n == 1

    staged = list((cfg.staging_root / "trading_status" / "run_id=run-a").rglob("*.parquet"))
    assert len(staged) == 1
    frame = pl.read_parquet(staged[0])
    assert frame.schema.names() == [
        "symbol",
        "trade_date",
        "is_trading",
        "status",
        "risk_warning",
        "source",
        "data_version",
        "fetched_at",
    ]
    assert frame["source"].to_list() == [DERIVED_BAR_GAP_SOURCE]
    assert frame["status"].to_list() == ["suspended"]
    assert not (cfg.curated_root / "trading_status").exists() or not any(
        (cfg.curated_root / "trading_status").rglob("*.parquet")
    )


def test_derive_publish_is_visible_to_committed_readers(tmp_path):
    """3.2 — after derive + compact, load_curated_trading_status sees is_trading=false."""
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    days = [date(2024, 6, 26), date(2024, 6, 27), date(2024, 6, 28)]
    for d in days:
        _write(
            root,
            "trading_calendar",
            "trade_date",
            d.isoformat(),
            _calendar_row(d),
        )
    for d in days:
        rows = [_bar("600519.SH", d)]
        if d != date(2024, 6, 27):
            rows.append(_bar("000001.SZ", d))
        _write(root, "daily_bars", "trade_date", d.isoformat(), pl.DataFrame(rows))
    _write_instruments(root, ["600519.SH", "000001.SZ"])
    # Existing authored snapshot to prove compact preserves it alongside derive.
    _write(
        root,
        "trading_status",
        "trade_date",
        "2024-06",
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 26)],
                "is_trading": [True],
                "status": ["normal"],
                "risk_warning": [False],
                "source": ["eastmoney"],
                "data_version": ["v1"],
                "fetched_at": ["2024-06-28T00:00:00+00:00"],
            }
        ),
    )

    assert _derive_and_compact(cfg, "run-b", days[-1]) == 1

    committed = load(
        "trading_status",
        data_root=cfg.data_root,
        start=days[0],
        end=days[-1],
    )
    susp = committed.filter(pl.col("status") == "suspended")
    assert susp.height == 1
    row = susp.to_dicts()[0]
    assert row["symbol"] == "000001.SZ"
    assert row["trade_date"] == date(2024, 6, 27)
    assert row["is_trading"] is False
    assert row["source"] == DERIVED_BAR_GAP_SOURCE
    assert committed.filter(pl.col("symbol") == "600519.SH").height == 1


def test_repeat_compact_keeps_derived_suspension_rows(tmp_path):
    """3.3 — a second, later compact does not rebuild away the derived row."""
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    days = [date(2024, 6, 26), date(2024, 6, 27), date(2024, 6, 28)]
    for d in days:
        _write(root, "trading_calendar", "trade_date", d.isoformat(), _calendar_row(d))
    for d in days:
        rows = [_bar("600519.SH", d)]
        if d != date(2024, 6, 27):
            rows.append(_bar("000001.SZ", d))
        _write(root, "daily_bars", "trade_date", d.isoformat(), pl.DataFrame(rows))
    _write_instruments(root, ["600519.SH", "000001.SZ"])

    assert _derive_and_compact(cfg, "run-c", days[-1]) == 1
    step_compact(cfg, days[-1], "run-c", {})  # second compact over the same run

    committed = load("trading_status", data_root=cfg.data_root, start=days[0], end=days[-1])
    susp = committed.filter(pl.col("source") == DERIVED_BAR_GAP_SOURCE)
    assert susp.height == 1
    assert susp["symbol"].to_list() == ["000001.SZ"]
    assert susp["trade_date"].to_list() == [date(2024, 6, 27)]


def test_derive_suspension_from_bar_gaps(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    days = [date(2024, 6, 26), date(2024, 6, 27), date(2024, 6, 28)]
    for d in days:
        _write(root, "trading_calendar", "trade_date", d.isoformat(), _calendar_row(d))
    for d in days:
        rows = [_bar("600519.SH", d)]
        if d != date(2024, 6, 27):
            rows.append(_bar("000001.SZ", d))
        _write(root, "daily_bars", "trade_date", d.isoformat(), pl.DataFrame(rows))
    _write_instruments(root, ["600519.SH", "000001.SZ"])
    _write(
        root,
        "trading_status",
        "trade_date",
        "2024-06",
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 26)],
                "is_trading": [True],
                "status": ["normal"],
                "risk_warning": [False],
                "source": ["eastmoney"],
                "data_version": ["v1"],
                "fetched_at": ["2024-06-28T00:00:00+00:00"],
            }
        ),
    )

    assert _derive_and_compact(cfg, "run-d", days[-1]) == 1

    # trading_status stays month-partitioned in curated; staged derive is flat.
    ts = pl.read_parquet(
        root / "curated" / "trading_status" / "trade_date=2024-06" / "part-merged.parquet"
    )
    assert (
        ts.filter(
            (pl.col("symbol") == "600519.SH") & (pl.col("trade_date") == date(2024, 6, 26))
        ).height
        == 1
    )
    assert [path.name for path in (root / "curated" / "trading_status").rglob("*.parquet")] == [
        "part-merged.parquet"
    ]
    susp = ts.filter(pl.col("status") == "suspended")
    assert susp.height == 1
    assert susp["symbol"][0] == "000001.SZ"
    assert susp["trade_date"][0] == date(2024, 6, 27)
    assert susp["is_trading"][0] is False
    assert susp["source"][0] == DERIVED_BAR_GAP_SOURCE


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
    (root / "curated" / "instruments").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "list_date": [date(2010, 1, 1)],
            "delist_date": [None],
        }
    ).write_parquet(root / "curated" / "instruments" / "part-merged.parquet")

    assert derive_suspension_history(cfg, run_id="run-e") == 0


def _write_instruments(root, symbols):
    (root / "curated" / "instruments").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": symbols,
            "name": ["A", "B"] if len(symbols) == 2 else ["A"],
            "exchange": [s.split(".")[1] for s in symbols],
            "asset_type": ["stock"] * len(symbols),
            "list_date": [date(2010, 1, 1)] * len(symbols),
            "delist_date": [None] * len(symbols),
            "prev_symbol": [None] * len(symbols),
            "source": ["tdx"] * len(symbols),
            "data_version": ["v1"] * len(symbols),
            "fetched_at": ["2024-06-28T00:00:00+00:00"] * len(symbols),
        }
    ).write_parquet(root / "curated" / "instruments" / "part-merged.parquet")


def test_derive_suspension_treats_zero_volume_placeholder_as_missing(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    days = [date(2024, 6, 27), date(2024, 6, 28)]
    for d in days:
        _write(root, "trading_calendar", "trade_date", d.isoformat(), _calendar_row(d))
    _write(
        root,
        "daily_bars",
        "trade_date",
        "2024-06-27",
        pl.DataFrame([_bar("000001.SZ", days[0], volume=0)]),
    )
    _write(
        root,
        "daily_bars",
        "trade_date",
        "2024-06-28",
        pl.DataFrame([_bar("000001.SZ", days[1], volume=100)]),
    )
    _write_instruments(root, ["000001.SZ"])

    assert _derive_and_compact(cfg, "run-f", days[-1]) == 1
    status = load("trading_status", data_root=cfg.data_root, start=days[0], end=days[-1])
    assert status.filter(pl.col("status") == "suspended")["trade_date"].to_list() == [
        date(2024, 6, 27)
    ]


def test_derive_suspension_ignores_placeholder_only_symbol(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    days = [date(2024, 6, 27), date(2024, 6, 28)]
    for d in days:
        _write(root, "trading_calendar", "trade_date", d.isoformat(), _calendar_row(d))
    rows = []
    for symbol, volume in (("600519.SH", 100), ("000001.SZ", 0)):
        for d in days:
            rows.append(_bar(symbol, d, volume=volume))
    _write(root, "daily_bars", "trade_date", "2024-06", pl.DataFrame(rows))
    _write_instruments(root, ["600519.SH", "000001.SZ"])

    assert derive_suspension_history(cfg, run_id="run-g") == 0


def test_derive_suspension_empty_lake(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    assert derive_suspension_history(cfg, run_id="run-h") == 0


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
    for d in days:
        _write(
            root,
            "trading_calendar",
            "trade_date",
            d.isoformat(),
            _calendar_row(d, fetched="2016-01-05T00:00:00+00:00"),
        )
        rows = [_bar("600519.SH", d)]
        if d not in (date(2015, 12, 31), date(2016, 1, 4)):
            rows.append(_bar("000001.SZ", d))
        _write(root, "daily_bars", "trade_date", d.isoformat(), pl.DataFrame(rows))
    _write_instruments(root, ["600519.SH", "000001.SZ"])

    n = _derive_and_compact(
        cfg,
        "run-i",
        days[-1],
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
    )
    assert n == 1
    ts = pl.read_parquet(
        root / "curated" / "trading_status" / "trade_date=2015-12" / "part-merged.parquet"
    )
    assert ts.filter(pl.col("status") == "suspended")["trade_date"].to_list() == [
        date(2015, 12, 31)
    ]
    assert not (root / "curated" / "trading_status" / "trade_date=2016-01").exists()


def test_derive_rejects_unfinished_today_session(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    today = date(2026, 9, 3)  # a Thursday; marked trading below for determinism
    _write(
        root,
        "trading_calendar",
        "trade_date",
        today.isoformat(),
        pl.DataFrame(
            {
                "trade_date": [today],
                "is_trading": [True],
                "source": ["seed"],
                "data_version": ["v1"],
            }
        ),
    )
    now = datetime(2026, 9, 3, 6, 0, tzinfo=timezone.utc)  # 14:00 Shanghai

    with pytest.raises(RuntimeError, match="not final until 15:05 Asia/Shanghai"):
        derive_suspension_history(cfg, end=today, run_id="run-j", now=now)


def test_derive_allows_finalized_session_and_historical_end(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    today = date(2026, 9, 3)
    _write(
        root,
        "trading_calendar",
        "trade_date",
        today.isoformat(),
        pl.DataFrame(
            {
                "trade_date": [today],
                "is_trading": [True],
                "source": ["seed"],
                "data_version": ["v1"],
            }
        ),
    )
    # 07:05 UTC -> 15:05 Shanghai, past the settlement buffer.
    now = datetime(2026, 9, 3, 7, 5, tzinfo=timezone.utc)
    assert derive_suspension_history(cfg, end=today, run_id="run-k", now=now) == 0
    # Historical end never trips the guard, day or night.
    assert (
        derive_suspension_history(
            cfg,
            end=date(2024, 6, 28),
            run_id="run-l",
            now=datetime(2026, 9, 3, 6, 0, tzinfo=timezone.utc),
        )
        == 0
    )


def _calendar_row(d: date, *, fetched: str = "2024-06-28T00:00:00+00:00") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": [d],
            "is_trading": [True],
            "source": ["seed"],
            "data_version": ["v1"],
            "fetched_at": [fetched],
        }
    )


def _bar(symbol: str, d: date, *, volume: int = 1) -> dict:
    return {
        "symbol": symbol,
        "trade_date": d,
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
        "volume": volume,
        "amount": 1.0,
        "source": "tdx_protocol",
        "data_version": "v1",
        "fetched_at": "2024-06-28T00:00:00+00:00",
    }


def _write(root, dataset, partition_col, val, df):
    d = root / "curated" / dataset / f"{partition_col}={val}"
    d.mkdir(parents=True, exist_ok=True)
    df.write_parquet(d / "part-merged.parquet")
