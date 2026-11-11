from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.steps import bars
from cnequity.steps.common import classify_daily_bar_ownership

DAYS = [date(2026, 9, 14), date(2026, 9, 15)]
SYMBOL = "600001.SH"


@pytest.mark.parametrize(
    ("source", "observed", "proved"),
    [
        (None, None, False),
        ("derived_bar_gap", None, False),
        ("derived_delisted", None, False),
        ("derived_traded", None, False),
        ("eastmoney_cached", None, False),
        ("eastmoney", datetime(2026, 9, 16, 8, tzinfo=timezone.utc), False),
        ("eastmoney", datetime(2026, 9, 14, 7, tzinfo=timezone.utc), True),
        ("baostock", None, True),
    ],
)
def test_planner_and_interior_gate_require_same_independent_halt(
    tmp_path, monkeypatch, source, observed, proved
):
    cfg = Config(data_root=tmp_path / "lake")
    status = pl.DataFrame(
        {
            "symbol": [SYMBOL],
            "trade_date": DAYS[:1],
            "is_trading": [False],
            "source": [source],
            "fetched_at": [observed],
        },
        schema_overrides={"source": pl.Utf8, "fetched_at": pl.Datetime("us", "UTC")},
    )
    result = classify_daily_bar_ownership(
        [SYMBOL],
        {SYMBOL: (DAYS[0], None, "stock")},
        DAYS[0],
        DAYS[0],
        trading_sessions=DAYS[:1],
        trading_status=status,
    )
    assert bool(result.expected_no_data) is proved

    # A fetched second session does not excuse the missing first session.
    path = cfg.staging_root / "daily_bars" / "run_id=run" / "batch.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": [SYMBOL], "trade_date": DAYS[1:]}).write_parquet(path)
    monkeypatch.setattr(bars.admission, "list_trading_dates", lambda *a: DAYS)
    monkeypatch.setattr(bars.bar_scope, "list_trading_dates", lambda *a: DAYS)
    monkeypatch.setattr(bars.daily, "list_trading_dates", lambda *a: DAYS)
    monkeypatch.setattr(bars.finish, "list_trading_dates", lambda *a: DAYS)
    monkeypatch.setattr(bars.gapfill, "list_trading_dates", lambda *a: DAYS)
    monkeypatch.setattr(bars.gapfill_chain, "list_trading_dates", lambda *a: DAYS)
    monkeypatch.setattr(bars.index_history, "list_trading_dates", lambda *a: DAYS)
    monkeypatch.setattr(bars.sina_bars, "list_trading_dates", lambda *a: DAYS)
    monkeypatch.setattr(bars.staging, "list_trading_dates", lambda *a: DAYS)
    monkeypatch.setattr(bars.tip_fetch, "list_trading_dates", lambda *a: DAYS)
    monkeypatch.setattr(
        bars.bar_scope, "_instrument_spans", lambda *a: {SYMBOL: (DAYS[0], None, "stock")}
    )
    monkeypatch.setattr(bars.admission, "load_curated_trading_status", lambda *a, **k: status)
    monkeypatch.setattr(bars.daily, "load_curated_trading_status", lambda *a, **k: status)
    monkeypatch.setattr(bars.gapfill, "load_curated_trading_status", lambda *a, **k: status)
    monkeypatch.setattr(bars.staging, "load_curated_trading_status", lambda *a, **k: status)
    missing = bars.staging._staged_daily_bar_missing_keys(cfg, "run", [SYMBOL], *DAYS)
    assert missing == (set() if proved else {(SYMBOL, DAYS[0])})


@pytest.mark.parametrize("reverse", [False, True])
def test_conflicting_trusted_rows_never_silence_a_fetch(reverse):
    status = pl.DataFrame(
        {
            "symbol": [SYMBOL] * 2,
            "trade_date": DAYS[:1] * 2,
            "is_trading": [True, False],
            "source": ["baostock", "exchange"],
        }
    )
    result = classify_daily_bar_ownership(
        [SYMBOL],
        {SYMBOL: (DAYS[0], None, "stock")},
        DAYS[0],
        DAYS[0],
        trading_sessions=DAYS[:1],
        trading_status=status.reverse() if reverse else status,
    )
    assert result.generic == [SYMBOL]
    assert result.expected_no_data == []


@pytest.mark.parametrize("source", ["eastmoney_cached", "derived_bar_gap", "derived_delisted"])
def test_missing_observation_column_does_not_upgrade_derived_or_carried_evidence(source):
    from cnequity.domain.trading_status import independent_suspension_keys

    status = pl.DataFrame(
        {"symbol": [SYMBOL], "trade_date": DAYS[:1], "is_trading": [False], "source": [source]}
    )
    assert independent_suspension_keys(status).is_empty()
