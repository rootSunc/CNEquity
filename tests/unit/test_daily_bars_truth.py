from datetime import date, datetime, timedelta, timezone

import polars as pl
from tests.coverage_fakes import covered

from cnequity.config import Config, load_config, validate_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.steps.common import classify_daily_bar_ownership, incremental_trade_dates


def test_daily_bar_ownership_keeps_missing_catalog_or_partial_status_unknown():
    sessions = [date(2024, 6, 27), date(2024, 6, 28)]
    status = pl.DataFrame(
        {
            "symbol": ["600001.SH"],
            "trade_date": [sessions[0]],
            "is_trading": [False],
        }
    )

    result = classify_daily_bar_ownership(
        ["600001.SH", "600002.SH"],
        {"600001.SH": (date(2000, 1, 1), None, "stock")},
        sessions[0],
        sessions[-1],
        trading_status=status,
        trading_sessions=sessions,
    )

    # One status row does not prove a two-session absence, and no instrument
    # row means we cannot even establish the security's expected span.
    assert result.unknown == ["600001.SH", "600002.SH"]
    assert result.expected_no_data == []


def test_daily_bar_ownership_accepts_all_false_status_as_expected_no_data():
    sessions = [date(2024, 6, 27), date(2024, 6, 28)]
    status = pl.DataFrame(
        {
            "symbol": ["600001.SH", "600001.SH"],
            "trade_date": sessions,
            "is_trading": [False, False],
            "source": ["baostock", "baostock"],
        }
    )

    result = classify_daily_bar_ownership(
        ["600001.SH"],
        {"600001.SH": (date(2000, 1, 1), None, "stock")},
        sessions[0],
        sessions[-1],
        trading_status=status,
        trading_sessions=sessions,
    )

    assert result.expected_no_data == ["600001.SH"]
    assert result.no_data_reasons == {"600001.SH": "trading_status_non_trading"}


def test_positive_status_wins_over_old_negative_evidence():
    session = date(2024, 6, 28)
    status = pl.DataFrame(
        {
            "symbol": ["600001.SH"],
            "trade_date": [session],
            "is_trading": [True],
        }
    )
    result = classify_daily_bar_ownership(
        ["600001.SH"],
        {"600001.SH": (date(2000, 1, 1), None, "stock")},
        session,
        session,
        trading_status=status,
        trading_sessions=[session],
        negative_evidence=[
            {
                "symbol": "600001.SH",
                "window_start": session.isoformat(),
                "window_end": session.isoformat(),
            }
        ],
    )

    assert result.generic == ["600001.SH"]
    assert result.expected_no_data == []


def _calendar_lake(tmp_path, monkeypatch, *, watermark: date, **config_kwargs) -> Config:
    cfg = Config(data_root=tmp_path / "data", **config_kwargs)
    rows = []
    current = date(2024, 5, 20)
    while current <= date(2024, 6, 28):
        rows.append({"trade_date": current, "is_trading": current.weekday() < 5})
        current += timedelta(days=1)
    calendar = cfg.curated_root / "trading_calendar"
    calendar.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(calendar / "part-merged.parquet")
    covered(monkeypatch, "daily_bars", watermark)
    return cfg


def _swept_deep(cfg: Config, day: date) -> None:
    from cnequity.quality.planned_coverage import recheck_finding, record_answers
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core.catalog import Catalog

    if not (lake_root(cfg) / "catalog.sqlite").exists():
        Catalog(lake_root(cfg), create=True).close()
    asked = datetime.combine(day, datetime.min.time(), timezone.utc)
    record_answers(cfg, "deep", [recheck_finding("daily_bars", [day], "deep", asked)])


def test_daily_bars_reconciles_five_sessions_on_its_deep_day(tmp_path, monkeypatch):
    """The reconciliation window that catches vendor revisions.

    TDX bills per symbol, not per session — one request returns up to 800 bars
    — so this window costs about the same whether it spans one session or five.
    It is therefore priced as its own job and runs on its own day;
    `deep_reconciliation_dow` names it (default Saturday).
    """
    cfg = _calendar_lake(tmp_path, monkeypatch, watermark=date(2024, 6, 25))
    saturday = date(2024, 6, 29)
    assert saturday.isoweekday() == cfg.deep_reconciliation_dow

    assert incremental_trade_dates(cfg, "daily_bars", saturday) == [
        date(2024, 6, 19),
        date(2024, 6, 20),
        date(2024, 6, 21),
        date(2024, 6, 24),
        date(2024, 6, 25),
        date(2024, 6, 26),
        date(2024, 6, 27),
        date(2024, 6, 28),
    ]


def test_an_ordinary_day_still_covers_everything_since_the_watermark(tmp_path, monkeypatch):
    """Tiering narrows the *reconciliation* tail, never the catch-up gap.

    A missed session is missing data, not a revision, so it must be fetched on
    the next run whatever day that is.
    """
    cfg = _calendar_lake(tmp_path, monkeypatch, watermark=date(2024, 6, 25))
    friday = date(2024, 6, 28)
    assert friday.isoweekday() != cfg.deep_reconciliation_dow
    _swept_deep(cfg, date(2024, 6, 24))

    # Every missed session; not the settled watermark session itself, whose
    # revisions are the weekly deep sweep's to catch.
    assert incremental_trade_dates(cfg, "daily_bars", friday) == [
        date(2024, 6, 26),
        date(2024, 6, 27),
        date(2024, 6, 28),
    ]


def test_a_caught_up_lake_fetches_only_the_tip(tmp_path, monkeypatch):
    """Which is what the whole-board exchange snapshot answers in two requests."""
    cfg = _calendar_lake(tmp_path, monkeypatch, watermark=date(2024, 6, 28))
    _swept_deep(cfg, date(2024, 6, 24))

    assert incremental_trade_dates(cfg, "daily_bars", date(2024, 6, 28)) == [date(2024, 6, 28)]


def test_disabling_tiering_restores_the_daily_five_session_window(tmp_path, monkeypatch):
    cfg = _calendar_lake(
        tmp_path, monkeypatch, watermark=date(2024, 6, 25), deep_reconciliation_dow=0
    )

    assert incremental_trade_dates(cfg, "daily_bars", date(2024, 6, 28))[0] == date(2024, 6, 19)


def test_incremental_negative_evidence_ttl_is_configurable(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "data")}"\n'
        "[orchestrator]\nworkers = 1\n"
        '[[job.daily.waves]]\nname = "w"\nsteps = ["instruments"]\n'
        "[incremental]\nnegative_evidence_ttl_days = 11\n",
        encoding="utf-8",
    )
    cfg = load_config(path)

    assert cfg.negative_evidence_ttl_days == 11
    assert validate_config(cfg) == []


def test_source_empty_answers_land_on_the_ledger_and_survive_a_status_publish(
    tmp_path, monkeypatch
):
    """2026-10-23: compact republished trading_status seconds after daily_bars
    certified 000016.SZ and 001381.SZ source-empty, and `cne check` then found
    no evidence for either."""
    from cnequity.quality.planned_coverage import answered_cells
    from cnequity.steps import common
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core.catalog import Catalog

    cfg = Config(data_root=tmp_path)
    Catalog(lake_root(cfg), create=True).close()
    day = date(2026, 10, 23)
    monkeypatch.setattr(common, "list_trading_dates", lambda _c, lo, hi: [day])
    for reason, symbol in (
        ("source_empty", "000016.SZ"),
        ("verified_no_data", "000017.SZ"),
        ("not_yet_listed", "001381.SZ"),
    ):
        common.record_negative_evidence(
            cfg, "daily_bars", [symbol], day, day, reason=reason, source="probe"
        )

    expected = {(day.isoformat(), "000016.SZ"), (day.isoformat(), "001381.SZ")}
    assert answered_cells(cfg, "daily_bars") == expected
    status = cfg.curated_root / "trading_status" / "trade_date=2026-10"
    status.mkdir(parents=True)
    pl.DataFrame(
        {"symbol": ["000016.SZ"], "trade_date": [day], "is_trading": [True]}
    ).write_parquet(status / "part-merged.parquet")
    assert answered_cells(cfg, "daily_bars") == expected
