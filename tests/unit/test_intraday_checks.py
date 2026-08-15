from datetime import date, datetime, timedelta

import polars as pl
import pytest

from cn_market_lake.adapters.tdx_protocol.minute_bars import in_session
from cn_market_lake.config import Config
from cn_market_lake.domain.schemas import with_provenance
from cn_market_lake.quality.intraday_checks import (
    RECONCILE_MIN_SYMBOL_DAYS,
    dataset_findings,
    minute_bars_findings,
    session_coverage_findings,
)

TRADE_DATE = date(2026, 7, 31)


@pytest.fixture
def cfg(tmp_path):
    config = Config(data_root=tmp_path / "lake")
    config.curated_root.mkdir(parents=True, exist_ok=True)
    return config


def _session_stamps(day: date, count: int) -> list[datetime]:
    """The first *count* closing-minute labels of *day*'s session."""
    out: list[datetime] = []
    stamp = datetime(day.year, day.month, day.day, 9, 31)
    while len(out) < count:
        if in_session(stamp):
            out.append(stamp)
        stamp += timedelta(minutes=1)
    return out


def _write_minute_bars(cfg: Config, rows: list[dict]):
    df = with_provenance(pl.DataFrame(rows), source="tdx_protocol", data_version="v1")
    for day, part in df.partition_by("trade_date", as_dict=True).items():
        value = (day[0] if isinstance(day, tuple) else day).isoformat()
        out = cfg.curated_root / "minute_bars" / f"trade_date={value}"
        out.mkdir(parents=True, exist_ok=True)
        part.write_parquet(out / "part-merged.parquet")


def _minute_rows(symbols: list[str], days: list[date], bars: int, volume: int = 100):
    rows = []
    for day in days:
        for sym in symbols:
            for stamp in _session_stamps(day, bars):
                rows.append(
                    {
                        "symbol": sym,
                        "trade_date": day,
                        "bar_time": stamp,
                        "frequency": "1m",
                        "open": 10.0,
                        "high": 10.0,
                        "low": 10.0,
                        "close": 10.0,
                        "volume": volume,
                        "amount": volume * 10.0,
                    }
                )
    return rows


def _write_daily_bars(cfg: Config, rows: list[dict], data_version: str = "v2"):
    df = with_provenance(pl.DataFrame(rows), source="tdx_protocol", data_version=data_version)
    df = df.with_columns(pl.lit(data_version).alias("data_version"))
    for day, part in df.partition_by("trade_date", as_dict=True).items():
        value = (day[0] if isinstance(day, tuple) else day).isoformat()
        out = cfg.curated_root / "daily_bars" / f"trade_date={value}"
        out.mkdir(parents=True, exist_ok=True)
        part.write_parquet(out / "part-merged.parquet")


def test_no_findings_when_the_dataset_is_unused(cfg):
    assert minute_bars_findings(cfg, TRADE_DATE) == []


def test_full_sessions_produce_no_shape_findings(cfg):
    _write_minute_bars(cfg, _minute_rows(["600519.SH"], [TRADE_DATE], 240))
    checks = {f["check"] for f in minute_bars_findings(cfg, TRADE_DATE)}
    assert "minute_bars_off_session" not in checks
    assert "minute_bars_session_coverage" not in checks
    assert "minute_bars_trade_date_mismatch" not in checks


def test_off_session_bar_is_an_error(cfg):
    rows = _minute_rows(["600519.SH"], [TRADE_DATE], 240)
    rows.append({**rows[0], "bar_time": datetime(2026, 7, 31, 12, 15)})
    _write_minute_bars(cfg, rows)

    findings = [
        f for f in minute_bars_findings(cfg, TRADE_DATE) if f["check"] == "minute_bars_off_session"
    ]
    assert len(findings) == 1
    assert findings[0]["severity"] == "error"
    assert findings[0]["rows"] == 1


def test_trade_date_disagreeing_with_bar_time_is_an_error(cfg):
    rows = _minute_rows(["600519.SH"], [TRADE_DATE], 240)
    # A-shares have no overnight session, so this can only be a partitioning bug.
    rows[0] = {**rows[0], "bar_time": datetime(2026, 7, 30, 9, 31)}
    _write_minute_bars(cfg, rows)

    findings = [
        f
        for f in minute_bars_findings(cfg, TRADE_DATE)
        if f["check"] == "minute_bars_trade_date_mismatch"
    ]
    assert len(findings) == 1
    assert findings[0]["severity"] == "error"


def test_widespread_short_sessions_warn(cfg):
    # Every symbol truncated at half a session — a pipeline problem, not halts.
    _write_minute_bars(cfg, _minute_rows(["600519.SH", "000001.SZ"], [TRADE_DATE], 120))
    findings = [
        f
        for f in minute_bars_findings(cfg, TRADE_DATE)
        if f["check"] == "minute_bars_session_coverage"
    ]
    assert len(findings) == 1
    assert findings[0]["severity"] == "warning"
    assert findings[0]["short_symbol_days"] == 2


def test_one_halted_symbol_does_not_warn(cfg):
    # 1 of 10 symbol-days short is the shape a genuine intraday halt makes.
    symbols = [f"60000{i}.SH" for i in range(9)]
    rows = _minute_rows(symbols, [TRADE_DATE], 240)
    rows += _minute_rows(["600519.SH"], [TRADE_DATE], 60)
    _write_minute_bars(cfg, rows)

    checks = {f["check"] for f in minute_bars_findings(cfg, TRADE_DATE)}
    assert "minute_bars_session_coverage" not in checks


def test_reconciliation_passes_when_minute_volume_matches_the_day(cfg):
    symbols = [f"60000{i}.SH" for i in range(RECONCILE_MIN_SYMBOL_DAYS + 5)]
    _write_minute_bars(cfg, _minute_rows(symbols, [TRADE_DATE], 240, volume=100))
    _write_daily_bars(
        cfg,
        [
            {
                "symbol": s,
                "trade_date": TRADE_DATE,
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
                "volume": 240 * 100,
                "amount": 240 * 1000.0,
            }
            for s in symbols
        ],
    )
    checks = {f["check"] for f in minute_bars_findings(cfg, TRADE_DATE)}
    assert "minute_bars_daily_reconciliation" not in checks


def test_reconciliation_flags_a_volume_mismatch(cfg):
    symbols = [f"60000{i}.SH" for i in range(RECONCILE_MIN_SYMBOL_DAYS + 5)]
    _write_minute_bars(cfg, _minute_rows(symbols, [TRADE_DATE], 240, volume=100))
    _write_daily_bars(
        cfg,
        [
            {
                "symbol": s,
                "trade_date": TRADE_DATE,
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
                "volume": 240 * 50,  # half of what the minutes add up to
                "amount": 240 * 500.0,
            }
            for s in symbols
        ],
    )
    findings = [
        f
        for f in minute_bars_findings(cfg, TRADE_DATE)
        if f["check"] == "minute_bars_daily_reconciliation"
    ]
    # Both metrics are halved in this fixture, so both must report.
    assert {f["metric"] for f in findings} == {"volume", "amount"}
    assert all(f["severity"] == "warning" for f in findings)
    assert all(f["median_ratio"] == pytest.approx(2.0) for f in findings)


def test_reconciliation_ignores_pre_v2_daily_rows(cfg):
    # v1 daily volume is 手 for tdx_protocol; comparing it against minute 股
    # would report a 100x break that is really an un-migrated partition.
    symbols = [f"60000{i}.SH" for i in range(RECONCILE_MIN_SYMBOL_DAYS + 5)]
    _write_minute_bars(cfg, _minute_rows(symbols, [TRADE_DATE], 240, volume=100))
    _write_daily_bars(
        cfg,
        [
            {
                "symbol": s,
                "trade_date": TRADE_DATE,
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
                "volume": 240,  # 手
                "amount": 240 * 1000.0,
            }
            for s in symbols
        ],
        data_version="v1",
    )
    findings = [
        f
        for f in minute_bars_findings(cfg, TRADE_DATE)
        if f["check"] == "minute_bars_daily_reconciliation"
    ]
    # Reported as info (nothing comparable), never as a 100x break.
    assert [f["severity"] for f in findings] == ["info"]


def test_reconciliation_catches_an_amount_only_break(cfg):
    """Turnover is the metric a unit error cannot explain away.

    ``volume`` has a unit history (股 vs 手), so a break there is ambiguous.
    ``amount`` is yuan from every source, so a break there means the wrong
    bars — and a volume-only reconciliation would never see it.
    """
    symbols = [f"60000{i}.SH" for i in range(RECONCILE_MIN_SYMBOL_DAYS + 5)]
    _write_minute_bars(cfg, _minute_rows(symbols, [TRADE_DATE], 240, volume=100))
    _write_daily_bars(
        cfg,
        [
            {
                "symbol": s,
                "trade_date": TRADE_DATE,
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
                "volume": 240 * 100,  # agrees
                "amount": 240 * 250.0,  # does not: minutes sum to 4x this
            }
            for s in symbols
        ],
    )
    findings = [
        f
        for f in minute_bars_findings(cfg, TRADE_DATE)
        if f["check"] == "minute_bars_daily_reconciliation"
    ]
    assert [f["metric"] for f in findings] == ["amount"]
    assert findings[0]["median_ratio"] == pytest.approx(4.0)


def test_session_coverage_on_an_empty_lazyframe_is_a_no_op(cfg):
    schema = {
        "symbol": pl.Utf8,
        "trade_date": pl.Date,
        "bar_time": pl.Datetime("us"),
        "frequency": pl.Utf8,
    }
    empty = pl.DataFrame(schema=schema).lazy()
    assert session_coverage_findings(empty, "minute_bars", TRADE_DATE, TRADE_DATE) == []


def test_session_coverage_skips_an_unrecognized_frequency(cfg):
    # Data carrying a frequency the registry does not know (corrupt row, or a
    # dataset serving a frequency this version predates) must not crash the
    # check — it has no session size to compare against, so it is skipped.
    rows = _minute_rows(["600519.SH"], [TRADE_DATE], 10)
    for row in rows:
        row["frequency"] = "3m"
    _write_minute_bars(cfg, rows)
    lf = pl.scan_parquet(str(cfg.curated_root / "minute_bars" / "**" / "*.parquet"))
    assert session_coverage_findings(lf, "minute_bars", TRADE_DATE, TRADE_DATE) == []


def test_reconciliation_skips_amount_when_daily_amount_is_zero(cfg):
    # amount_ratio is null wherever daily_amount is 0 (division would be
    # meaningless); an all-null column must be skipped, not reported as 0/0.
    symbols = [f"60000{i}.SH" for i in range(RECONCILE_MIN_SYMBOL_DAYS + 5)]
    _write_minute_bars(cfg, _minute_rows(symbols, [TRADE_DATE], 240, volume=100))
    _write_daily_bars(
        cfg,
        [
            {
                "symbol": s,
                "trade_date": TRADE_DATE,
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
                "volume": 240 * 100,  # agrees with the minutes
                "amount": 0.0,  # unusable — must not produce a ratio at all
            }
            for s in symbols
        ],
    )
    findings = [
        f
        for f in minute_bars_findings(cfg, TRADE_DATE)
        if f["check"] == "minute_bars_daily_reconciliation"
    ]
    assert findings == []


def test_dataset_findings_ignores_a_dataset_missing_required_columns(cfg):
    # A schema this check cannot reason about (e.g. a partial write, or a
    # future column set this version predates) must be skipped, not crash.
    out = cfg.curated_root / "minute_bars" / f"trade_date={TRADE_DATE.isoformat()}"
    out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [TRADE_DATE]}).write_parquet(
        out / "part-merged.parquet"
    )
    assert dataset_findings(cfg, "minute_bars", TRADE_DATE) == []


def test_dataset_findings_ignores_data_outside_the_lookback_window(cfg):
    # Real rows exist, just not inside the window this audit run looks at —
    # partition pruning returns a typed-but-empty frame, not None.
    old_day = TRADE_DATE - timedelta(days=30)
    _write_minute_bars(cfg, _minute_rows(["600519.SH"], [old_day], 240))
    assert dataset_findings(cfg, "minute_bars", TRADE_DATE, lookback_days=7) == []
