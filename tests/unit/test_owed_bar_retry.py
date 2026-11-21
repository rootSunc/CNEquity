"""The daily run retries recently owed daily bars: the old ledger's and the planner's."""

from __future__ import annotations

from datetime import date

from cnequity.cli import backfill_cmds, run_cmds
from cnequity.config import Config
from cnequity.quality import planned_coverage
from cnequity.run_scope import backfill_of, set_backfill
from cnequity.storage.state import StateStore

DAY = date(2026, 11, 19)
RECENT, OLD = date(2026, 11, 13), date(2026, 9, 1)  # inside and outside the 45 days


def _lake(tmp_path) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    StateStore(cfg.meta_root).record_outstanding_keys(
        "daily_bars",
        [("000001.SZ", RECENT), ("000002.SZ", OLD)],
        run_id="r0",
        reason="interior_gap",
    )
    return cfg


def test_the_planner_s_gaps_join_the_ledger_and_the_window_is_put_back(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)
    monkeypatch.setattr(
        planned_coverage,
        "gap_keys",
        lambda c, ds, day: [("600000.SH", RECENT), ("000001.SZ", RECENT)],
    )
    seen = {}

    def repair(c, dataset, workers, *, since=None):
        seen["since"] = since
        seen["owed"] = {
            (row["symbol"], row["trade_date"])
            for row in StateStore(c.meta_root).get_outstanding_keys(dataset)
        }
        set_backfill(c, active=True, symbols=["600000.SH"], start=RECENT, end=RECENT)
        return {"status": "success"}

    monkeypatch.setattr(backfill_cmds, "_repair_outstanding", repair)
    before = backfill_of(cfg)
    out = backfill_cmds.retry_recent_owed(cfg, "daily_bars", DAY)
    assert seen["since"] == date(2026, 10, 5)
    assert ("600000.SH", "2026-11-13") in seen["owed"] and len(seen["owed"]) == 3
    assert (out["planner_keys"], out["planner_added"]) == (2, 1)
    assert backfill_of(cfg) == before and not backfill_of(cfg).active


def test_a_planner_that_cannot_plan_leaves_the_ledger_deciding(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)

    def broken(c, ds, day):
        raise OSError("catalog busy")

    monkeypatch.setattr(planned_coverage, "gap_keys", broken)
    monkeypatch.setattr(
        backfill_cmds, "_repair_outstanding", lambda c, d, w, *, since=None: {"status": "success"}
    )
    assert backfill_cmds.retry_recent_owed(cfg, "daily_bars", DAY)["planner_keys"] == 0


def test_only_the_recent_keys_are_repaired_and_older_debt_stays_owed(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)
    asked = []
    monkeypatch.setattr("cnequity.steps.bars.bar_scope._last_final_session", lambda *a, **k: DAY)
    monkeypatch.setattr(backfill_cmds, "_bar_fingerprint", lambda *a, **k: None)

    def once(c, dataset):
        window = backfill_of(c)
        asked.append((tuple(window.symbols or ()), window.start, window.end))
        return {"status": "success"}

    monkeypatch.setattr(backfill_cmds, "_backfill_once", once)
    backfill_cmds._repair_outstanding(cfg, "daily_bars", 1, since=date(2026, 10, 5))
    assert asked == [(("000001.SZ",), RECENT, RECENT)]
    owed = {row["symbol"] for row in StateStore(cfg.meta_root).get_outstanding_keys("daily_bars")}
    assert owed == {"000001.SZ", "000002.SZ"}  # nothing landed, and the old key was not tried


def test_a_failed_retry_never_fails_the_day(tmp_path, monkeypatch):
    def broken(c, dataset, day):
        raise RuntimeError("tdx down")

    monkeypatch.setattr(backfill_cmds, "retry_recent_owed", broken)
    out = run_cmds._retry_owed_bars(Config(data_root=tmp_path / "data"), DAY)
    assert out["status"] == "failed" and "tdx down" in out["error"]


def _day_with(tmp_path, monkeypatch, planned):
    from types import SimpleNamespace

    cfg = SimpleNamespace(
        schedule_groups={"core": SimpleNamespace(steps=["daily_bars"], cadence="daily")},
        planned_datasets=frozenset(planned),
        events_groups={},
    )
    calls = []
    monkeypatch.setattr(run_cmds, "_group_is_runnable", lambda c, g: True)
    monkeypatch.setattr("cnequity.orchestrator.cadence.due_steps", lambda g, d: g.steps)
    monkeypatch.setattr(run_cmds, "_settled_session", lambda c, td: DAY)
    monkeypatch.setattr(run_cmds, "attach_revision_retention", lambda c, p: None)
    monkeypatch.setattr(run_cmds, "emit_json", lambda payload, **k: calls.append(payload))
    monkeypatch.setattr(
        run_cmds, "_retry_owed_bars", lambda c, day: {"status": "success", "day": day}
    )
    engine = SimpleNamespace(run_job=lambda *a, **k: {"status": "success"})
    run_cmds._run_all_groups(cfg, engine, None, backfill=False, repairs=[])
    return calls[-1]


def test_the_daily_run_retries_owed_bars_only_when_daily_bars_is_planned(tmp_path, monkeypatch):
    assert _day_with(tmp_path, monkeypatch, {"daily_bars"})["owed_bars"]["day"] == DAY
    assert "owed_bars" not in _day_with(tmp_path, monkeypatch, {"margin_trading"})


def _bars_alone(tmp_path) -> Config:
    from dataclasses import replace

    return replace(_lake(tmp_path), planner_only=frozenset({"daily_bars"}))


def test_planner_only_bars_owe_no_ledger_and_cache_no_absence(tmp_path):
    from cnequity.steps.common import load_negative_evidence, owe_keys, record_negative_evidence

    cfg = _bars_alone(tmp_path)
    before = StateStore(cfg.meta_root).get_outstanding_keys("daily_bars")
    assert owe_keys(cfg, "daily_bars", {("600000.SH", DAY)}, run_id="r", reason="x") == 1
    assert StateStore(cfg.meta_root).get_outstanding_keys("daily_bars") == before
    record_negative_evidence(
        cfg, "daily_bars", ["000003.SZ"], RECENT, DAY, reason="verified_no_data", source="t"
    )
    assert StateStore(cfg.meta_root).get_negative_evidence("daily_bars") == []
    assert load_negative_evidence(cfg, "daily_bars") == []


def test_planner_only_bars_retry_the_planner_s_keys_and_plan_again(tmp_path, monkeypatch):
    cfg = _bars_alone(tmp_path)
    plans = iter([[("600000.SH", RECENT), ("600001.SH", RECENT)], [("600001.SH", RECENT)]])
    monkeypatch.setattr(planned_coverage, "gap_keys", lambda c, ds, day: next(plans))
    seen = {}

    def repair(c, dataset, workers, *, since=None, owed_rows=None):
        seen["rows"] = owed_rows
        return {"status": "warning"}

    monkeypatch.setattr(backfill_cmds, "_repair_outstanding", repair)
    before = StateStore(cfg.meta_root).get_outstanding_keys("daily_bars")
    out = backfill_cmds.retry_recent_owed(cfg, "daily_bars", DAY)
    assert [row["symbol"] for row in seen["rows"]] == ["600000.SH", "600001.SH"]
    assert out["outstanding"] == {"before": 2, "filled": 1, "still_owed": 1}
    assert out["status"] == "degraded"
    # The ledger was neither read for keys nor written.
    assert StateStore(cfg.meta_root).get_outstanding_keys("daily_bars") == before
