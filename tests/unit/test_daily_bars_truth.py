from datetime import date, datetime, timedelta, timezone

import polars as pl

from cnequity.config import Config, load_config, validate_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.steps.common import classify_daily_bar_ownership, incremental_trade_dates
from cnequity.storage.state import StateStore


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


def test_negative_evidence_is_ttl_bounded_and_catalog_revision_invalidates(tmp_path):
    store = StateStore(tmp_path / "meta")
    now = datetime(2024, 6, 28, tzinfo=timezone.utc)
    identity = {"instruments_revision": 3, "instruments_fingerprint": "catalog-a"}
    store.record_negative_evidence(
        "daily_bars",
        [
            {
                "symbol": "600001.SH",
                "window_start": "2024-06-28",
                "window_end": "2024-06-28",
                "reason": "source_empty",
                "source": "sina",
            }
        ],
        ttl_days=2,
        identity=identity,
        now=now,
    )

    assert (
        store.get_negative_evidence("daily_bars", identity=identity, now=now + timedelta(days=1))[
            0
        ]["symbol"]
        == "600001.SH"
    )
    # A catalog revision is an immediate invalidation, even before TTL.
    assert (
        store.get_negative_evidence(
            "daily_bars",
            identity={"instruments_revision": 4, "instruments_fingerprint": "catalog-b"},
            now=now + timedelta(days=1),
        )
        == []
    )
    assert (
        store.get_negative_evidence("daily_bars", identity=identity, now=now + timedelta(days=3))
        == []
    )


def _calendar_lake(tmp_path, *, watermark: date, **config_kwargs) -> Config:
    cfg = Config(data_root=tmp_path / "data", **config_kwargs)
    rows = []
    current = date(2024, 5, 20)
    while current <= date(2024, 6, 28):
        rows.append({"trade_date": current, "is_trading": current.weekday() < 5})
        current += timedelta(days=1)
    calendar = cfg.curated_root / "trading_calendar"
    calendar.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(calendar / "part-merged.parquet")
    StateStore(cfg.meta_root).set_date("daily_bars", watermark)
    return cfg


def test_daily_bars_reconciles_five_sessions_on_its_deep_day(tmp_path):
    """The reconciliation window that catches vendor revisions.

    TDX bills per symbol, not per session — one request returns up to 800 bars
    — so this window costs the same ~5,559 requests whether it spans one
    session or five. It is therefore priced as its own job and runs on its own
    day; `deep_reconciliation_dow` names it (default Saturday).
    """
    cfg = _calendar_lake(tmp_path, watermark=date(2024, 6, 25))
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


def test_an_ordinary_day_still_covers_everything_since_the_watermark(tmp_path):
    """Tiering narrows the *reconciliation* tail, never the catch-up gap.

    A missed session is missing data, not a revision, so it must be fetched on
    the next run whatever day that is.
    """
    cfg = _calendar_lake(tmp_path, watermark=date(2024, 6, 25))
    friday = date(2024, 6, 28)
    assert friday.isoweekday() != cfg.deep_reconciliation_dow

    assert incremental_trade_dates(cfg, "daily_bars", friday) == [
        date(2024, 6, 25),
        date(2024, 6, 26),
        date(2024, 6, 27),
        date(2024, 6, 28),
    ]


def test_a_caught_up_lake_fetches_only_the_tip(tmp_path):
    """Which is what the whole-board exchange snapshot answers in two requests."""
    cfg = _calendar_lake(tmp_path, watermark=date(2024, 6, 28))

    assert incremental_trade_dates(cfg, "daily_bars", date(2024, 6, 28)) == [date(2024, 6, 28)]


def test_disabling_tiering_restores_the_daily_five_session_window(tmp_path):
    cfg = _calendar_lake(tmp_path, watermark=date(2024, 6, 25), deep_reconciliation_dow=0)

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


def test_streamed_identity_matches_legacy_json_bytes():
    import hashlib
    import json

    from cnequity.steps.common import _row_fingerprint

    rows = [
        {"symbol": "测试.BJ", "day": date(2026, 9, 15), "flag": None},
        {"symbol": 'a\\"', "day": datetime(2026, 9, 16, tzinfo=timezone.utc), "flag": False},
    ]
    normalized = [
        {k: v.isoformat() if isinstance(v, date) else v for k, v in row.items()} for row in rows
    ]
    expected = hashlib.sha256(
        json.dumps(normalized, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
    assert _row_fingerprint(iter(rows)) == expected
    assert _row_fingerprint(iter(())) == hashlib.sha256(b"[]").hexdigest()


def test_empty_negative_cache_skips_full_status_identity(tmp_path, monkeypatch):
    from cnequity.steps.common import load_negative_evidence

    cfg = Config(data_root=tmp_path)

    def unexpected(*args):
        raise AssertionError("empty cache must not scan the lake")

    monkeypatch.setattr("cnequity.steps.common._instrument_identity", unexpected)
    assert load_negative_evidence(cfg, "daily_bars") == []


def test_live_negative_cache_still_validates_identity(tmp_path, monkeypatch):
    from cnequity.steps.common import load_negative_evidence

    cfg = Config(data_root=tmp_path)
    now = datetime(2026, 9, 16, tzinfo=timezone.utc)
    store = StateStore(cfg.meta_root)
    store.record_negative_evidence(
        "daily_bars",
        [{"symbol": "920001.BJ", "window_start": "2026-09-15", "window_end": "2026-09-15"}],
        ttl_days=2,
        identity={"instruments_revision": 1},
        now=now,
    )
    monkeypatch.setattr(
        "cnequity.steps.common._instrument_identity", lambda *args: {"instruments_revision": 2}
    )
    assert load_negative_evidence(cfg, "daily_bars", now=now) == []


def test_direct_status_edit_invalidates_identity_without_revision_change(tmp_path, monkeypatch):
    from cnequity.steps.common import _instrument_identity

    cfg = Config(data_root=tmp_path)
    metadata = pl.DataFrame(
        {"symbol": ["920001.BJ"], "list_date": [date(2026, 1, 1)], "asset_type": ["stock"]}
    )
    status = pl.DataFrame(
        {"symbol": ["920001.BJ"], "trade_date": [date(2026, 9, 15)], "is_trading": [False]}
    )
    monkeypatch.setattr("cnequity.steps.common.load_curated_trading_status", lambda cfg: status)
    before = _instrument_identity(cfg, metadata)
    status = status.with_columns(pl.lit(True).alias("is_trading"))
    after = _instrument_identity(cfg, metadata)
    assert before["trading_status_revision"] == after["trading_status_revision"]
    assert before["trading_status_fingerprint"] != after["trading_status_fingerprint"]
