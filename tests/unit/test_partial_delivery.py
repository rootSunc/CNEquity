"""Source outages deliver validated facts and recovery guidance, even at zero rows."""

import json
import shlex
from dataclasses import replace
from datetime import date

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.backfill_cmds import _backfill_chunked, _backfill_once, _run_delisted_profile
from cnequity.cli.main import cli
from cnequity.config import Config, ScheduleGroup
from cnequity.domain.datasets import DATASETS
from cnequity.domain.schemas import with_provenance
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.outcomes import SourceUnavailableError
from cnequity.orchestrator.registry import StepEntry
from cnequity.run_scope import backfill_of, set_backfill
from cnequity.steps.common import walk_day_backfill
from cnequity.storage.parquet import StagingWriter
from cnequity.storage.revisions import RevisionStore
from cnequity.storage.state import StateStore

DAY = date(2024, 6, 28)


def _entries(monkeypatch, overrides):
    from cnequity.orchestrator import engine

    original = engine.get_step
    monkeypatch.setattr(engine, "get_step", lambda name: overrides.get(name) or original(name))


def _outage(*args, **kwargs):
    raise SourceUnavailableError("all permitted providers unavailable")


def _breadth(day):
    return pl.DataFrame({"trade_date": [day], "metric_id": ["advance_ratio"], "value": [0.5]})


def test_date_walk_publishes_before_an_outage_and_clears_the_gap_after_repair(
    tmp_path, monkeypatch
):
    cfg = Config(data_root=tmp_path / "data")
    start, end = date(2024, 6, 27), DAY
    set_backfill(cfg, start=start, end=end)
    calls = []
    recovering = False

    def fetch(day):
        calls.append(day)
        if day == end and not recovering:
            raise SourceUnavailableError("provider unavailable for the second session")
        return _breadth(day)

    def step(config, trade_date, run_id, context):
        return walk_day_backfill(
            config, trade_date, run_id, "market_breadth", fetch, source="derived"
        )

    _entries(monkeypatch, {"market_breadth": StepEntry(fn=step, group="core")})
    result = _backfill_once(cfg, "market_breadth")
    assert result["execution_status"] == "completed"
    assert result["publication_status"] == "partial"
    assert result["usable_result"] is True
    assert result["fallback"][0]["retry_command"]
    assert StateStore(cfg.meta_root).get_missing_dates("market_breadth") == {end}
    root = RevisionStore(cfg.meta_root, cfg.curated_root).current_root("market_breadth")
    assert pl.read_parquet(list(root.rglob("*.parquet"))).get_column("trade_date").to_list() == [
        start
    ]

    recovering = True
    again = _backfill_once(cfg, "market_breadth")
    assert calls == [start, end, end]
    assert again["execution_status"] == "completed"
    assert again["status"] == "success"
    assert StateStore(cfg.meta_root).get_missing_dates("market_breadth") == set()
    root = RevisionStore(cfg.meta_root, cfg.curated_root).current_root("market_breadth")
    assert set(pl.read_parquet(list(root.rglob("*.parquet")))["trade_date"]) == {start, end}


def test_empty_source_backfill_cli_finishes_with_truthful_fallback(tmp_path, monkeypatch):
    cfg_path = tmp_path / "config with spaces.toml"
    cfg_path.write_text(f'[data]\nroot = "{(tmp_path / "data").as_posix()}"\n')
    _entries(monkeypatch, {"market_breadth": StepEntry(fn=_outage, group="core")})
    result = CliRunner().invoke(
        cli,
        [
            "backfill",
            "market_breadth",
            "--start",
            DAY.isoformat(),
            "--end",
            DAY.isoformat(),
            "--config",
            str(cfg_path),
        ],
    )
    # Every source failed and nothing was written: the payload stays truthful,
    # and the backfill exits 1 because it filled none of the range it was asked for.
    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "degraded"
    assert payload["execution_status"] == "completed"
    assert payload["coverage_status"] == "partial"
    assert payload["usable_result"] is False
    assert "fetched nothing usable" in result.stderr
    assert "✓" not in result.stderr
    assert payload["publication_status"] == "none"
    assert payload["results"][0]["status"] == "warning"
    fallback = payload["fallback"][0]
    assert fallback["available_data"] is False
    assert fallback["scope"]["start"] == DAY.isoformat()
    assert shlex.split(fallback["retry_command"])[-2:] == ["--config", str(cfg_path)]


def test_source_outage_keeps_existing_revision_available(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    engine = JobEngine(cfg)
    seed = engine.manifest.start_run("seed")
    StagingWriter(cfg.staging_root).write_batch(
        "market_breadth",
        seed,
        "seed",
        with_provenance(_breadth(DAY), source="derived", data_version="v1"),
    )
    engine.run_step("compact", DAY, seed)
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    before = store.current_pointer("market_breadth")
    _entries(monkeypatch, {"market_breadth": StepEntry(fn=_outage, group="core")})

    result = _backfill_once(cfg, "market_breadth")

    assert result["status"] == "degraded"
    assert result["fallback"][0]["available_data"] is True
    assert store.current_pointer("market_breadth") == before


def test_valid_days_publish_when_remaining_days_are_unavailable(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2024, 6, 24), end=DAY)

    def fetch(day):
        return _breadth(day) if day == backfill_of(cfg).start else _outage()

    def step(config, day, run_id, context):
        return walk_day_backfill(config, day, run_id, "market_breadth", fetch, source="derived")

    _entries(monkeypatch, {"market_breadth": StepEntry(fn=step, group="core")})
    result = _backfill_once(cfg, "market_breadth")

    assert result["status"] == "degraded"
    assert result["rows_written"] == 1
    assert result["publication_status"] == "partial"
    root = RevisionStore(cfg.meta_root, cfg.curated_root).current_root("market_breadth")
    assert pl.read_parquet(list(root.rglob("*.parquet"))).height == 1
    gaps = StateStore(cfg.meta_root).get_payload("market_breadth")["missing_ranges"]
    assert [row["start"] for row in gaps] == [f"2024-06-{day}" for day in (25, 26, 27, 28)]


def test_all_unavailable_chunks_finish_and_keep_each_recovery_scope(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    _entries(monkeypatch, {"market_breadth": StepEntry(fn=_outage, group="core")})
    result = _backfill_chunked(cfg, "market_breadth", date(2024, 6, 24), DAY, 2)
    assert result["status"] == "degraded"
    assert result["execution_status"] == "completed"
    assert result["usable_result"] is False
    assert len(result["slices"]) == len(result["failed_scopes"]) == len(result["fallback"]) == 3
    assert [option["scope"]["start"] for option in result["fallback"]] == [
        "2024-06-24",
        "2024-06-26",
        "2024-06-28",
    ]


def test_empty_delisted_recovery_finishes_with_recovery_command(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        "cnequity.steps.delisted.backfill_delisted_bars",
        lambda *a, **k: {
            "status": "warning",
            "rows_written": 0,
            "failed": 1,
            "coverage_status": "partial",
            "reason_code": "source_unavailable",
        },
    )
    result = _run_delisted_profile(cfg, DAY)
    assert result["status"] in {"warning", "degraded"}
    assert result["execution_status"] == "completed"
    assert "--profile delisted --start 2024-06-28" in result["fallback"][0]["retry_command"]
    assert result["fallback"][0]["scope"]["start"] == DAY.isoformat()


@pytest.mark.parametrize(
    "source,switches,tdx_enabled,enabled",
    [
        ("eastmoney_dc", {"eastmoney": False}, True, False),
        ("eastmoney_dc", {"eastmoney_dc": False}, True, False),
        ("ths_fund_flow", {"ths": False}, True, False),
        ("tdx_protocol", {}, False, False),
        ("futures_exchange_shfe", {"futures_exchange": False}, True, False),
        ("eastmoney_dc", {}, True, True),
    ],
)
def test_fallback_reports_effective_source_switches(
    tmp_path, monkeypatch, source, switches, tdx_enabled, enabled
):
    cfg = Config(data_root=tmp_path / "data", sources=switches, tdx_enabled=tdx_enabled)
    monkeypatch.setitem(
        DATASETS,
        "market_breadth",
        replace(DATASETS["market_breadth"], primary_source=source),
    )
    _entries(monkeypatch, {"market_breadth": StepEntry(fn=_outage, group="core")})
    result = _backfill_once(cfg, "market_breadth")
    primary = result["fallback"][0]["registered_sources"][0]
    assert primary == {"source": source, "role": "primary", "enabled": enabled}


def test_delisted_child_fallback_resumes_the_parent_scope(tmp_path):
    from cnequity.orchestrator.recovery import fallback_options

    cfg = Config(data_root=tmp_path / "data")
    engine = JobEngine(cfg)
    parent = engine.manifest.start_run("init", {"history_start": DAY.isoformat()})
    child = engine.manifest.start_run(
        "delisted_backfill",
        {"parent_init_run_id": parent, "start": DAY.isoformat(), "end": DAY.isoformat()},
    )
    engine._record_step_result(
        name="daily_bars",
        entry=StepEntry(fn=_outage, group="core"),
        run_id=child,
        status="warning",
        out={"rows_written": 0, "reason_code": "source_unavailable"},
    )
    fallback = fallback_options(cfg, engine.manifest, child)[0]
    assert shlex.split(fallback["retry_command"]) == ["cne", "init", "--resume", "--run-id", parent]
    assert fallback["scope"] == {"start": DAY.isoformat(), "end": DAY.isoformat()}


def test_snapshot_history_finishes_with_configured_collection_command(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        schedule_groups={"capital": ScheduleGroup(at="16:00", steps=["fund_flow"])},
    )
    _entries(
        monkeypatch,
        {
            "fund_flow": StepEntry(
                fn=lambda *a: pytest.fail("snapshot replay attempted"), group="core"
            )
        },
    )
    result = _backfill_once(cfg, "fund_flow")
    assert result["status"] == "degraded"
    assert result["execution_status"] == "completed"
    assert result["fallback"][0]["collection_commands"] == ["cne run daily --group capital"]
    assert result["fallback"][0]["retry_command"] is None
    assert not list(cfg.data_root.rglob("*.parquet"))


def test_source_history_floor_delivers_available_scope(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2024, 6, 24), end=DAY)
    monkeypatch.setitem(
        DATASETS,
        "market_breadth",
        replace(DATASETS["market_breadth"], history_floor_date=date(2024, 6, 26)),
    )
    requested = []

    def step(config, day, run_id, context):
        requested.append((backfill_of(config).start, backfill_of(config).end))
        return walk_day_backfill(config, day, run_id, "market_breadth", _breadth, source="derived")

    _entries(monkeypatch, {"market_breadth": StepEntry(fn=step, group="core")})
    result = _backfill_once(cfg, "market_breadth")
    assert result["status"] == "degraded"
    assert result["publication_status"] == "partial"
    assert result["rows_written"] == 3
    assert requested == [(date(2024, 6, 26), DAY)]
    assert backfill_of(cfg).start == date(2024, 6, 24)
    assert result["results"][0]["requested_scope"]["start"] == "2024-06-24"
    assert result["results"][0]["effective_scope"]["start"] == "2024-06-26"
    assert result["fallback"][0]["scope"]["start"] == "2024-06-24"


def test_request_before_history_floor_finishes_without_source_requests(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2024, 6, 24), end=date(2024, 6, 25))
    monkeypatch.setitem(
        DATASETS,
        "market_breadth",
        replace(DATASETS["market_breadth"], history_floor_date=date(2024, 6, 26)),
    )
    _entries(
        monkeypatch,
        {"market_breadth": StepEntry(fn=lambda *a: pytest.fail("unservable fetch"), group="core")},
    )
    result = _backfill_once(cfg, "market_breadth")
    assert result["status"] == "degraded"
    assert result["execution_status"] == "completed"
    assert result["fallback"][0]["reason_codes"] == ["capability_limit"]
    assert result["fallback"][0]["earliest_available"] == "2024-06-26"
    assert not list(cfg.data_root.rglob("*.parquet"))


def test_partial_index_bars_publish_without_advancing_complete_coverage(tmp_path, monkeypatch):
    from cnequity.adapters.tdx_protocol.client import INDEX_SYMBOLS

    cfg = Config(data_root=tmp_path / "data", publication_gate="block")
    set_backfill(cfg, start=DAY, end=DAY)
    frame = pl.DataFrame(
        {
            "symbol": ["000852.SH"],
            "trade_date": [DAY],
            "open": [10.0],
            "high": [10.0],
            "low": [10.0],
            "close": [10.0],
            "volume": [100],
            "amount": [1000.0],
            "frequency": ["1d"],
        }
    )
    monkeypatch.setattr("cnequity.steps.bars.index_history.fetch_index_bars", lambda *a, **k: frame)
    result = _backfill_once(cfg, "index_bars")
    assert result["status"] == "degraded"
    assert result["publication_status"] == "partial"
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    assert pl.read_parquet(list(store.current_root("index_bars").rglob("*.parquet"))).height == 1
    state = StateStore(cfg.meta_root).get_payload("index_bars")
    assert len(state["outstanding_keys"]) == len(INDEX_SYMBOLS) - 1
    assert state["coverage_status"] == "incomplete"
    assert not state.get("complete_through") or state["complete_through"] < DAY.isoformat()


def test_partial_scope_rows_publish_but_do_not_clear_date_debt(tmp_path, monkeypatch):
    from cnequity.orchestrator.source_gaps import record_source_gap
    from cnequity.steps.common import write_simple

    cfg = Config(data_root=tmp_path / "data")

    def step(config, day, run_id, context):
        frame = with_provenance(_breadth(DAY), source="derived", data_version="v1")
        record_source_gap(
            "market_breadth", "source returned only part of the requested metrics", frame=frame
        )
        result = write_simple(config, run_id, "market_breadth", frame)
        StateStore(config.meta_root).mark_staged_request_days("market_breadth", run_id, [DAY])
        return result

    _entries(monkeypatch, {"market_breadth": StepEntry(fn=step, group="core")})
    result = _backfill_once(cfg, "market_breadth")
    assert result["status"] == "degraded"
    assert result["publication_status"] == "partial"
    assert result["rows_written"] == 1
    assert result["fallback"][0]["available_data"] is True
    payload = StateStore(cfg.meta_root).get_payload("market_breadth")
    assert payload["missing_ranges"][0]["start"] == DAY.isoformat()
    assert payload["coverage_status"] == "incomplete"


def test_partial_scope_does_not_bypass_schema_validation(tmp_path, monkeypatch):
    from cnequity.orchestrator.source_gaps import record_source_gap
    from cnequity.steps.common import write_simple

    cfg = Config(data_root=tmp_path / "data")

    def step(config, day, run_id, context):
        record_source_gap("market_breadth", "source returned only a subset", dates=[DAY])
        return write_simple(config, run_id, "market_breadth", _breadth(DAY))  # missing provenance

    _entries(monkeypatch, {"market_breadth": StepEntry(fn=step, group="core")})
    result = _backfill_once(cfg, "market_breadth")
    assert result["status"] == "failed"
    assert result["publication_status"] == "none"
    assert not list(cfg.staging_root.rglob("*.parquet"))


@pytest.mark.parametrize("status", ["failed", "blocked", "invalid"])
def test_partial_scope_cannot_hide_a_returned_failure(tmp_path, monkeypatch, status):
    from cnequity.orchestrator.source_gaps import record_source_gap

    cfg = Config(data_root=tmp_path / "data")

    def step(config, day, run_id, context):
        record_source_gap("market_breadth", "source returned only a subset", dates=[DAY])
        return {"status": status, "reason_code": "invariant_violation"}

    _entries(monkeypatch, {"market_breadth": StepEntry(fn=step, group="core")})
    result = _backfill_once(cfg, "market_breadth")
    assert result["status"] == "failed"
    assert result["execution_status"] == "failed"
    assert result["fallback"] == []


def test_partial_init_recovery_cannot_hide_a_daily_bar_failure(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("init", {})

    def recovery(parent_run, day, context):
        context["init_delisted_recovery"] = {"coverage_status": "partial"}

    _entries(
        monkeypatch,
        {
            "daily_bars": StepEntry(
                fn=lambda *a: {"status": "failed", "reason_code": "invariant_violation"},
                group="core",
            )
        },
    )
    monkeypatch.setattr(engine, "_recover_init_delisted_bars", recovery)
    result = engine.run_step("daily_bars", DAY, run_id)
    assert result["status"] == "failed"
    assert result["execution_status"] == "failed"
    assert result["reason_code"] == "invariant_violation"


def test_delisted_profile_preserves_a_returned_execution_failure(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        "cnequity.steps.delisted.backfill_delisted_bars",
        lambda *a, **k: {"status": "failed", "reason_code": "invariant_violation"},
    )
    result = _run_delisted_profile(cfg, DAY)
    assert result["status"] == "failed"
    assert result["execution_status"] == "failed"
    assert result["fallback"] == []


@pytest.mark.parametrize("error", [RuntimeError("bug in computation"), OSError("disk full")])
def test_program_and_storage_errors_still_fail(tmp_path, monkeypatch, error):
    cfg = Config(data_root=tmp_path / "data")

    def broken(*args):
        raise error

    _entries(monkeypatch, {"market_breadth": StepEntry(fn=broken, group="core")})
    result = _backfill_once(cfg, "market_breadth")
    assert result["status"] == "failed"
    assert result["execution_status"] == "failed"
    assert result["fallback"] == []


@pytest.mark.parametrize("dataset", ["fund_flow", "market_breadth"])
def test_fallback_guidance_is_english_unless_chinese_is_chosen(tmp_path, monkeypatch, dataset):
    """The action and history note were Chinese literals in an English-source CLI."""
    import re

    cfg = Config(data_root=tmp_path / "data")
    _entries(monkeypatch, {dataset: StepEntry(fn=_outage, group="core")})
    result = _backfill_once(cfg, dataset)
    option = result["fallback"][0]

    assert not re.search(r"[一-鿿]", option["action"] + option["history_note"])


@pytest.mark.parametrize(("usable", "code"), [(False, 1), (True, 0)])
def test_outstanding_repair_fails_only_when_it_filled_nothing(tmp_path, monkeypatch, usable, code):
    cfg_path = tmp_path / "cnequity.toml"
    cfg_path.write_text(f'[data]\nroot = "{(tmp_path / "data").as_posix()}"\n')
    monkeypatch.setattr(
        "cnequity.cli.backfill_cmds._repair_outstanding",
        lambda cfg, dataset, workers: {
            "dataset": dataset,
            "status": "warning",
            "usable_result": usable,
            "outstanding": {"filled": int(usable), "still_owed": 3},
        },
    )
    result = CliRunner().invoke(
        cli, ["backfill", "daily_bars", "--outstanding", "--config", str(cfg_path)]
    )
    assert result.exit_code == code, result.output
    assert ("fetched nothing usable" in result.stderr) is not usable
