from datetime import date

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import Config, WaveConfig
from cn_market_lake.orchestrator.engine import JobEngine
from cn_market_lake.storage.layout import init_data_layout


def test_reused_run_id_metadata_merge_new_values_win(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    engine = JobEngine(cfg)
    monkeypatch.setattr(engine, "_run_wave", lambda *args, **kwargs: ([], 0, 0, False, False))

    run_id = engine.manifest.start_run("init", {"phases": ["phase1", "phase2c"]})
    engine.run_job(
        "init",
        date(2024, 6, 28),
        steps=["trading_calendar"],
        backfill=False,
        run_id=run_id,
    )
    assert engine.manifest.get_run_metadata(run_id)["backfill"] is False

    engine.run_job(
        "init",
        date(2024, 6, 28),
        steps=["daily_bars"],
        backfill=True,
        run_id=run_id,
    )
    meta = engine.manifest.get_run_metadata(run_id)
    assert meta["backfill"] is True
    assert meta["phases"] == ["phase1", "phase2c"]
    assert meta["trade_date"] == "2024-06-28"


def test_explicit_waves_not_overridden_by_steps(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    engine = JobEngine(cfg)
    captured: list[bool] = []

    def _capture_wave(wave, *args, **kwargs):
        captured.append(wave.parallel)
        return ([], 0, 0, False, False)

    monkeypatch.setattr(engine, "_run_wave", _capture_wave)
    waves = [WaveConfig(name="group:core", parallel=False, steps=["instruments"])]
    engine.run_job(
        "daily:core",
        date(2024, 6, 28),
        steps=["instruments"],
        waves=waves,
    )
    assert captured == [False]
