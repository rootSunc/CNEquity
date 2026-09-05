import sys
from pathlib import Path

import pytest

import cnequity.steps  # noqa: F401 — register steps
from cnequity.config import (
    Config,
    FailoverDatasetSpec,
    ScheduleGroup,
    WaveConfig,
    load_config,
    validate_config,
)
from cnequity.config.bootstrap import path_for_toml


def test_validate_config_rejects_unknown_group_step(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        daily_waves=[WaveConfig(name="core", parallel=True, steps=["instruments"])],
        schedule_groups={
            "capital": ScheduleGroup(at="16:30", steps=["not_a_dataset_step"]),
        },
    )
    errors = validate_config(cfg)
    assert any("unknown step 'not_a_dataset_step'" in err for err in errors)


def test_validate_config_rejects_invalid_tdx_servers(tmp_path):
    cfg_path = tmp_path / "test.toml"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[[job.daily.waves]]
name = "core"
parallel = true
steps = ["instruments"]

[tdx_protocol]
servers = "not-a-server"
"""
    )
    cfg = load_config(cfg_path)
    errors = validate_config(cfg)
    assert any("servers must be" in e for e in errors)


def test_validate_config_rejects_invalid_failover_snapshot_cadence(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        failover_datasets=[
            FailoverDatasetSpec(
                name="daily_bars",
                primary="tdx_protocol",
                backup="eastmoney",
                snapshot_cadence="hourly",
            )
        ],
    )
    errors = validate_config(cfg)
    assert any("snapshot_cadence" in e for e in errors)


def test_validate_config_rejects_unknown_failover_dataset_name(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        failover_datasets=[
            FailoverDatasetSpec(
                name="not_a_dataset",
                primary="tdx_protocol",
                backup="eastmoney",
            )
        ],
    )
    errors = validate_config(cfg)
    assert any("'not_a_dataset' is not a registered dataset" in e for e in errors)


def test_validate_config_rejects_unknown_failover_source(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        failover_datasets=[
            FailoverDatasetSpec(
                name="daily_bars",
                primary="tdx_protocol",
                backup="not_a_source",
            )
        ],
    )
    errors = validate_config(cfg)
    assert any("unknown backup source 'not_a_source'" in e for e in errors)
    assert not any("unknown primary source" in e for e in errors)


def test_validate_config_rejects_duplicate_failover_entries(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        failover_datasets=[
            FailoverDatasetSpec(name="daily_bars", primary="tdx_protocol", backup="eastmoney"),
            FailoverDatasetSpec(name="daily_bars", primary="eastmoney", backup="tdx_protocol"),
        ],
    )
    errors = validate_config(cfg)
    assert any("duplicate entry for 'daily_bars'" in e for e in errors)


def test_validate_config_accepts_registered_waves(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        workers=1,
        daily_waves=[
            WaveConfig(
                name="reference",
                parallel=True,
                steps=["instruments", "trading_calendar"],
            )
        ],
    )
    assert validate_config(cfg) == []


def test_example_config_validates(monkeypatch):
    # Example keeps workers=8 for Linux hosts; pin platform so the assertion
    # is stable on Darwin CI/dev machines.
    monkeypatch.setattr(sys, "platform", "linux")
    root = Path(__file__).resolve().parents[2]
    cfg = load_config(root / "configs" / "cnequity.example.toml")
    assert validate_config(cfg) == []
    # Free-source anti-blacklist defaults (time may be slow; bans are worse).
    assert cfg.source_intervals["baostock"] == 1.0
    assert cfg.baostock_batch_size == 20
    assert cfg.baostock_batch_rest_seconds == 120.0
    assert cfg.source_intervals["tushare"] == 1.0
    assert cfg.failover_backfill_snapshots is False
    # EastMoney pacing is a floor, not a fixed value. The example targets a
    # mainland route, where 0.5s sweeps ~991 sector boards in ~10min; overseas
    # users raise it in their own config. What the floor guards is shipping a
    # config with no pacing at all, which is how a source starts 429ing.
    assert cfg.source_intervals["eastmoney"] >= 0.5
    # PBOC feeds social_financing; an index fetch plus a workbook per year.
    assert cfg.source_intervals["pboc"] >= 1.0
    # AkShare is retired — shipping a [sources.akshare] section would advertise
    # a source no adapter reads (issue #3).
    assert "akshare" not in cfg.sources
    assert cfg.tdx_min_interval_ms == 100
    assert cfg.tdx_lock_timeout_sec == 15.0
    # Every schedule group we ship must be defined and pass validation. This
    # replaced four per-group tests that each re-asserted validate_config == []
    # without pinning the platform, so all four failed on macOS.
    assert set(cfg.schedule_groups) == {
        "core",
        "capital",
        "signals",
        "fundamentals",
        "macro_risk",
        "research",
        # Defined but deliberately unscheduled — `cne run daily --group
        # intraday` is the only way in, and [minute_bars].enabled gates it.
        "intraday",
        # Ticks get their own group rather than a fourth step in `intraday`,
        # so enabling minute bars cannot drag transaction records along.
        "ticks",
    }
    assert set(cfg.event_groups) == {"corporate_events", "news_wire"}
    assert cfg.event_groups["corporate_events"].steps == [
        "announcement_index",
        "regulatory_events",
        "compact",
    ]
    assert cfg.event_groups["news_wire"].steps == [
        "flash_news_wire",
        "news_headlines",
        "compact",
    ]
    assert cfg.minute_bars_enabled is False


def test_load_config_parses_optional_backfill_snapshots(tmp_path):
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "data")}"\n'
        "[failover]\nbackfill_snapshots = true\n"
    )

    cfg = load_config(cfg_path)

    assert cfg.failover_backfill_snapshots is True
    assert cfg.trade_ticks_enabled is False


def test_example_config_ships_a_tick_scope_that_cannot_sweep_the_market(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    root = Path(__file__).resolve().parents[2]
    cfg = load_config(root / "configs" / "cnequity.example.toml")
    assert cfg.trade_ticks_scope == "watchlist"
    assert cfg.trade_ticks_max_symbols == 200


def test_validate_config_rejects_scope_all_for_ticks(tmp_path):
    # 'all' is refused at validation rather than at run time: finding out that
    # a full-market tick sweep is ~9,600 requests twenty minutes in is too late.
    path = tmp_path / "c.toml"
    path.write_text(
        '[data]\nroot = "/tmp/lake"\n\n'
        '[[job.daily.waves]]\nname = "w"\nsteps = ["instruments"]\n\n'
        '[trade_ticks]\nscope = "all"\n',
        encoding="utf-8",
    )
    errors = validate_config(load_config(path))
    assert any("scope = 'all' is not supported" in e for e in errors)


def test_validate_config_rejects_an_empty_tick_watchlist(tmp_path):
    path = tmp_path / "c.toml"
    path.write_text(
        '[data]\nroot = "/tmp/lake"\n\n'
        '[[job.daily.waves]]\nname = "w"\nsteps = ["instruments"]\n\n'
        '[trade_ticks]\nenabled = true\nscope = "watchlist"\nsymbols = []\n',
        encoding="utf-8",
    )
    errors = validate_config(load_config(path))
    assert any("symbols is empty" in e for e in errors)


def test_validate_config_rejects_multiprocess_on_darwin(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "platform", "darwin")
    cfg = Config(
        data_root=tmp_path / "data",
        workers=2,
        daily_waves=[WaveConfig(name="core", parallel=True, steps=["instruments"])],
    )
    errors = validate_config(cfg)
    assert any("workers must be 1 on macOS" in e for e in errors)


def test_validate_config_allows_multiprocess_on_linux(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    cfg = Config(
        data_root=tmp_path / "data",
        workers=8,
        daily_waves=[WaveConfig(name="core", parallel=True, steps=["instruments"])],
    )
    assert validate_config(cfg) == []


def test_validate_config_allows_multiprocess_on_windows(tmp_path, monkeypatch):
    # Windows uses spawn, not fork — so the macOS hard reject does not apply.
    # `cne config init` still defaults workers=1; users may raise it later.
    monkeypatch.setattr(sys, "platform", "win32")
    cfg = Config(
        data_root=tmp_path / "data",
        workers=4,
        daily_waves=[WaveConfig(name="core", parallel=True, steps=["instruments"])],
    )
    assert validate_config(cfg) == []


def test_load_config_parses_independent_tdx_http_and_group_budgets(tmp_path, monkeypatch):
    """The macOS global process guard must not collapse source-local pools."""
    monkeypatch.setattr(sys, "platform", "darwin")
    path = tmp_path / "split.toml"
    path.write_text(
        f'''
[data]
root = "{path_for_toml(tmp_path / "data")}"

[orchestrator]
workers = 1
tdx_daily_workers = 3
tdx_daily_backend = "auto"
adj_factor_workers = 5
derive_workers = 6
http_workers = {{ sina = 2 }}

[sources.sina]
max_concurrency = 4

[[job.daily.waves]]
name = "w"
steps = ["instruments"]

[job.daily.groups.core]
at = "16:00"
steps = ["instruments"]
''',
        encoding="utf-8",
    )
    cfg = load_config(path)

    assert cfg.workers == 1
    assert cfg.tdx_daily_worker_count() == 3
    assert cfg.tdx_daily_executor() == "thread"
    assert cfg.adj_factor_worker_count() == 5
    # Source-local configuration has precedence over the legacy alias map.
    assert cfg.source_concurrency_for("sina", 6) == 4
    assert cfg.schedule_groups["core"].parallel is True
    assert validate_config(cfg) == []


def test_validate_config_rejects_invalid_specialized_worker_budget(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        tdx_daily_workers=0,
        adj_factor_workers=0,
        derive_workers=0,
        source_concurrency={"sina": 0},
        daily_waves=[WaveConfig(name="w", parallel=True, steps=["instruments"])],
    )
    errors = validate_config(cfg)
    assert any("tdx_daily_workers" in error for error in errors)
    assert any("adj_factor_workers" in error for error in errors)
    assert any("derive_workers" in error for error in errors)
    assert any("source concurrency" in error for error in errors)


def test_validate_config_rejects_malformed_source_concurrency_without_fallback(tmp_path):
    path = tmp_path / "invalid-source-limit.toml"
    path.write_text(
        f'''
[data]
root = "{path_for_toml(tmp_path / "data")}"

[orchestrator]
source_concurrency = {{ sina = "not-an-integer" }}

[sources.sina]
max_concurrency = 0

[[job.daily.waves]]
name = "w"
steps = ["instruments"]
''',
        encoding="utf-8",
    )
    cfg = load_config(path)
    errors = validate_config(cfg)

    assert any("source_concurrency['sina']" in error for error in errors)
    # The source-local zero is retained in the canonical map and must not be
    # silently replaced by the global worker budget.
    assert cfg.source_concurrency["sina"] == 0
    with pytest.raises(ValueError, match="positive integer"):
        cfg.source_concurrency_for("sina")


def test_validate_config_rejects_non_mapping_source_concurrency(tmp_path):
    path = tmp_path / "invalid-source-map.toml"
    path.write_text(
        f'''
[data]
root = "{path_for_toml(tmp_path / "data")}"

[orchestrator]
source_concurrency = "not-a-map"

[[job.daily.waves]]
name = "w"
steps = ["instruments"]
''',
        encoding="utf-8",
    )

    errors = validate_config(load_config(path))

    assert any("source concurrency" in error and "not-a-map" in error for error in errors)


def test_programmatic_relative_data_root_matches_load_config_resolution(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    expected = (tmp_path / "relative-lake").resolve()

    cfg = Config(data_root="relative-lake")

    assert cfg.data_root == expected
    assert cfg.data_root.is_absolute()


def test_raw_archive_policy_defaults_to_critical_datasets_and_honors_allowlist(tmp_path):
    cfg = Config(data_root=tmp_path / "data")

    assert cfg.should_archive_raw("daily_bars") is False
    assert cfg.should_archive_raw("announcement_index") is True
    assert cfg.should_archive_raw("financial_statement_items") is True
    assert cfg.should_archive_raw("corporate_actions") is True

    cfg.raw_archive_datasets = ["daily_bars"]
    assert cfg.should_archive_raw("daily_bars") is True
    assert cfg.should_archive_raw("financial_statement_items") is False
    cfg.raw_archive_enabled = False
    assert cfg.should_archive_raw("daily_bars") is False


def test_unknown_intraday_frequency_is_rejected(tmp_path):
    """A frequency with no dataset has nowhere to put rows and no horizon to
    declare, so it must fail validation rather than no-op at run time."""
    cfg = Config(
        data_root=tmp_path / "data",
        workers=1,
        daily_waves=[WaveConfig(name="w", parallel=False, steps=["instruments"])],
        minute_bars_frequencies=["1m", "3m"],
    )
    errors = validate_config(cfg)
    assert any("'3m' has no registered dataset" in e for e in errors)
    assert not any("'1m'" in e for e in errors)


def test_enabled_intraday_capture_needs_at_least_one_frequency(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        workers=1,
        daily_waves=[WaveConfig(name="w", parallel=False, steps=["instruments"])],
        minute_bars_enabled=True,
        minute_bars_frequencies=[],
    )
    assert any("frequencies is empty" in e for e in validate_config(cfg))


def test_fetch_workers_below_one_is_rejected(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        workers=1,
        daily_waves=[WaveConfig(name="w", parallel=False, steps=["instruments"])],
        minute_bars_fetch_workers=0,
    )
    assert any("fetch_workers must be >= 1" in e for e in validate_config(cfg))


def test_rate_limited_sources_are_all_declared_in_the_example_config():
    """Every `config.rate_limit("x")` must have a matching `[sources.x]`.

    `SourceRateLimiters.wait` no-ops on a name it has no limiter for, so a
    typo — or gating on `[sources.exchange]` while throttling on `"sse"` —
    disables `min_interval_seconds` silently instead of failing.
    """
    import re

    root = Path(__file__).resolve().parents[2]
    cfg = load_config(root / "configs" / "cnequity.example.toml")
    declared = set(cfg.sources) | {"tdx_protocol"}

    literal = re.compile(r"""rate_limit\(\s*["'](\w+)["']\s*\)""")
    used: dict[str, str] = {}
    for path in (root / "src" / "cnequity").rglob("*.py"):
        for name in literal.findall(path.read_text(encoding="utf-8")):
            used.setdefault(name, path.relative_to(root).as_posix())

    missing = {name: where for name, where in used.items() if name not in declared}
    assert not missing, f"rate_limit() names with no [sources.*] section: {missing}"
