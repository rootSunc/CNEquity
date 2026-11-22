"""The daily run retries the daily bars the coverage planner owes from the last 45 days."""

from __future__ import annotations

from datetime import date

from cnequity.cli import backfill_cmds, run_cmds
from cnequity.config import Config
from cnequity.quality import planned_coverage
from cnequity.run_scope import backfill_of, set_backfill

DAY = date(2026, 11, 19)
RECENT, OLD = date(2026, 11, 13), date(2026, 9, 1)  # inside and outside the 45 days


def _lake(tmp_path) -> Config:
    return Config(data_root=tmp_path / "data")


def test_the_retry_repairs_the_planner_s_keys_and_puts_the_window_back(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)
    plans = iter([[("600000.SH", RECENT), ("600001.SH", RECENT)], [("600001.SH", RECENT)]])
    monkeypatch.setattr(planned_coverage, "gap_keys", lambda c, ds, day: next(plans))
    seen = {}

    def repair(c, dataset, workers, *, since=None, owed_rows=None):
        seen.update(since=since, rows=owed_rows)
        set_backfill(c, active=True, symbols=["600000.SH"], start=RECENT, end=RECENT)
        return {"status": "warning"}

    monkeypatch.setattr(backfill_cmds, "_repair_outstanding", repair)
    before = backfill_of(cfg)
    out = backfill_cmds.retry_recent_owed(cfg, "daily_bars", DAY)
    assert seen["since"] == date(2026, 10, 5)
    assert [row["symbol"] for row in seen["rows"]] == ["600000.SH", "600001.SH"]
    assert out["outstanding"] == {"before": 2, "filled": 1, "still_owed": 1}
    assert out["status"] == "degraded"
    assert backfill_of(cfg) == before and not backfill_of(cfg).active
    # Nothing is written to meta/state.
    assert not (cfg.meta_root / "state" / "daily_bars.json").exists()


def test_only_the_recent_keys_are_repaired_and_older_debt_stays_owed(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)
    asked = []
    monkeypatch.setattr("cnequity.steps.bars.bar_scope._last_final_session", lambda *a, **k: DAY)
    monkeypatch.setattr(backfill_cmds, "_bar_fingerprint", lambda *a, **k: None)
    owed = [("000001.SZ", RECENT), ("000002.SZ", OLD)]
    monkeypatch.setattr(
        planned_coverage,
        "owed_cells",
        lambda c, ds, start, end: [(s, d) for s, d in owed if start <= d <= end],
    )

    def once(c, dataset):
        window = backfill_of(c)
        asked.append((tuple(window.symbols or ()), window.start, window.end))
        return {"status": "success"}

    monkeypatch.setattr(backfill_cmds, "_backfill_once", once)
    out = backfill_cmds._repair_outstanding(cfg, "daily_bars", 1, since=date(2026, 10, 5))
    assert asked == [(("000001.SZ",), RECENT, RECENT)]
    # Nothing landed (the planner still owes it), and the old key was not tried.
    assert out["outstanding"] == {"before": 1, "filled": 0, "still_owed": 1}


def test_a_failed_retry_never_fails_the_day(tmp_path, monkeypatch):
    def broken(c, dataset, day):
        raise RuntimeError("tdx down")

    monkeypatch.setattr(backfill_cmds, "retry_recent_owed", broken)
    out = run_cmds._retry_owed_bars(Config(data_root=tmp_path / "data"), DAY)
    assert out["status"] == "failed" and "tdx down" in out["error"]


def test_the_daily_run_retries_owed_bars(tmp_path, monkeypatch):
    from types import SimpleNamespace

    cfg = SimpleNamespace(
        schedule_groups={"core": SimpleNamespace(steps=["daily_bars"], cadence="daily")},
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
    assert calls[-1]["owed_bars"]["day"] == DAY


def test_owed_keys_are_counted_not_recorded_and_no_absence_is_cached(tmp_path):
    from cnequity.steps.common import load_negative_evidence, owe_keys, record_negative_evidence

    cfg = _lake(tmp_path)
    assert owe_keys(cfg, "daily_bars", {("600000.SH", DAY)}, run_id="r", reason="x") == 1
    record_negative_evidence(
        cfg, "daily_bars", ["000003.SZ"], RECENT, DAY, reason="verified_no_data", source="t"
    )
    assert load_negative_evidence(cfg, "daily_bars") == []
    assert not (cfg.meta_root / "state" / "daily_bars.json").exists()


def test_the_repair_derives_factors_once_after_every_monthly_pass(tmp_path, monkeypatch):
    """2026-11-23: twelve 1991-94 months each paid a ~4-minute factor sync.

    The passes now only note what changed; factors are realigned once for the
    union of symbols, and suspensions once per window, overlapping months
    merged. A pass that failed but published bars still gets its lineage.
    """
    from cnequity.derive import lineage

    cfg = _lake(tmp_path)
    monkeypatch.setattr("cnequity.steps.bars.bar_scope._last_final_session", lambda *a, **k: DAY)
    owed = [
        ("000004.SZ", date(1991, 10, 9)),
        ("000003.SZ", date(1991, 11, 7)),
        ("600652.SH", date(1994, 4, 4)),
    ]
    monkeypatch.setattr(
        planned_coverage, "owed_cells", lambda c, ds, start, end: owed if start is None else []
    )
    monkeypatch.setattr(backfill_cmds, "_bar_fingerprint", lambda *a, **k: object())
    statuses = iter(["success", "failed", "success"])
    monkeypatch.setattr(
        backfill_cmds, "_backfill_once", lambda c, ds: {"status": next(statuses), "rows_written": 1}
    )
    scopes = iter(
        [
            (["000004.SZ"], date(1991, 10, 1), date(1991, 10, 1)),
            (["000003.SZ"], date(1991, 11, 1), date(1991, 11, 1)),
            (["600652.SH"], date(1994, 4, 1), date(1994, 4, 1)),
        ]
    )
    monkeypatch.setattr(lineage, "changed_bar_scope", lambda before, after: next(scopes))
    synced, derived = [], []

    def sync(c, affected, **kwargs):
        synced.append(sorted(affected))
        return {"synchronized": True}

    def derive(c, start, end):
        derived.append((start, end))
        return {"status": "success"}

    monkeypatch.setattr(backfill_cmds, "_sync_adj_factors", sync)
    monkeypatch.setattr(backfill_cmds, "_derive_suspensions", derive)
    out = backfill_cmds._repair_outstanding(cfg, "daily_bars", 1)

    assert synced == [["000003.SZ", "000004.SZ", "600652.SH"]]
    assert derived == [
        (date(1991, 7, 3), date(1991, 11, 7)),  # 1991-10 and 1991-11 overlap: one window
        (date(1994, 1, 1), date(1994, 4, 4)),
    ]
    assert out["passes"] == 3
    assert out["followups"][0]["months"] == ["1991-10", "1991-11", "1994-04"]


def test_suspension_windows_merge_only_when_they_overlap():
    spans = [
        (date(2020, 3, 1), date(2020, 3, 1), date(2020, 3, 31)),
        (date(2020, 1, 1), date(2020, 1, 1), date(2020, 1, 31)),
        (date(2024, 6, 1), date(2024, 7, 1), date(2024, 7, 15)),
    ]
    assert backfill_cmds._suspension_windows(spans, 90) == [
        (date(2019, 10, 3), date(2020, 3, 31)),
        (date(2024, 3, 3), date(2024, 7, 15)),
    ]
