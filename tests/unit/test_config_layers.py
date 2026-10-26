"""`extends = "builtin"`: a config that holds only what differs from the defaults."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - Python 3.10
    import tomli as tomllib

from cnequity.cli.main import cli
from cnequity.config import load_config
from cnequity.config.bootstrap import example_toml_text, path_for_toml, write_user_config
from cnequity.config.drift import config_drift
from cnequity.config.layers import ConfigLayerError, builtin_defaults, effective_raw, merge
from cnequity.config.minimize import plan_minimize
from cnequity.config.toml_out import dumps
from cnequity.domain.datasets import (
    best_effort_dataset_names,
    is_dataset_enabled,
    removed_from_schedule,
)


def _layered(tmp_path: Path, body: str = "") -> Path:
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f'extends = "builtin"\n[data]\nroot = "{path_for_toml(tmp_path / "lake")}"\n{body}',
        encoding="utf-8",
    )
    return path


def _example_groups() -> dict:
    return tomllib.loads(example_toml_text())["job"]["daily"]["groups"]


def test_merge_recurses_into_tables_and_replaces_everything_else():
    base = {"a": {"x": 1, "y": [1, 2]}, "b": [{"n": 1}], "c": 1}
    over = {"a": {"y": [3]}, "b": [{"n": 2}]}
    assert merge(base, over) == {"a": {"x": 1, "y": [3]}, "b": [{"n": 2}], "c": 1}
    assert base["a"]["y"] == [1, 2]


def test_defaults_never_supply_the_data_root():
    assert "root" not in builtin_defaults().get("data", {})


def test_a_layered_config_runs_the_default_schedule(tmp_path):
    cfg = load_config(_layered(tmp_path))

    expected = _example_groups()
    assert set(cfg.schedule_groups) == set(expected)
    assert set(cfg.schedule_groups["core"].steps) == set(expected["core"]["steps"])
    assert cfg.data_root == tmp_path / "lake"


def test_group_edits_swap_a_step_and_keep_the_rest(tmp_path):
    path = _layered(
        tmp_path,
        '[job.daily.groups.capital]\nsteps_remove = ["fund_flow"]\nsteps_add = ["fund_flow_ths"]\n',
    )

    steps = load_config(path).schedule_groups["capital"].steps

    default = _example_groups()["capital"]["steps"]
    # Best-effort steps the config did not name stay off by default.
    off = best_effort_dataset_names() - {"fund_flow_ths"}
    assert set(steps) == (set(default) - {"fund_flow"} - off) | {"fund_flow_ths"}


def test_best_effort_datasets_are_off_unless_named(tmp_path):
    cfg = load_config(_layered(tmp_path, ""))
    scheduled = {step for group in cfg.schedule_groups.values() for step in group.steps}
    scheduled |= {step for group in cfg.events_groups.values() for step in group.steps}
    assert not scheduled & best_effort_dataset_names()
    assert cfg.best_effort_enabled == frozenset()
    assert not is_dataset_enabled("hot_rank", cfg)
    assert removed_from_schedule(cfg, "hot_rank")

    named = load_config(
        _layered(tmp_path, '[datasets]\nbest_effort = ["hot_rank", "news_headlines"]\n')
    )
    assert named.best_effort_enabled == {"hot_rank", "news_headlines"}
    assert "hot_rank" in named.schedule_groups["research"].steps
    assert "news_headlines" in named.events_groups["news_wire"].steps
    assert "sector_bars" not in named.schedule_groups["research"].steps

    everything = load_config(_layered(tmp_path, '[datasets]\nbest_effort = ["all"]\n'))
    assert everything.best_effort_enabled == best_effort_dataset_names()


def test_best_effort_rejects_a_dataset_it_does_not_cover(tmp_path):
    with pytest.raises(ConfigLayerError, match="not best-effort"):
        load_config(_layered(tmp_path, '[datasets]\nbest_effort = ["daily_bars"]\n'))


def test_a_group_can_be_disabled(tmp_path):
    path = _layered(tmp_path, "[job.daily.groups.research]\nenabled = false\n")

    assert "research" not in load_config(path).schedule_groups
    # An opt-out is not drift.
    drift = config_drift(path)
    assert drift.clean, drift


def test_removed_steps_are_not_reported_as_drift(tmp_path):
    path = _layered(tmp_path, '[job.daily.groups.capital]\nsteps_remove = ["fund_flow"]\n')

    assert config_drift(path).clean


def test_a_full_steps_override_that_shadows_a_default_step_is_drift(tmp_path):
    path = _layered(tmp_path, '[job.daily.groups.signals]\nsteps = ["dragon_tiger"]\n')

    drift = config_drift(path)

    assert "block_trades" in drift.group_steps["job.daily.groups.signals"]


def test_layering_keys_are_validated(tmp_path):
    bad = tmp_path / "bad.toml"
    bad.write_text('extends = "default"\n', encoding="utf-8")
    with pytest.raises(ConfigLayerError, match="extends"):
        load_config(bad)
    with pytest.raises(ConfigLayerError, match="steps_add"):
        effective_raw({"job": {"daily": {"groups": {"core": {"steps_add": "x"}}}}})


def test_bad_extends_is_reported_by_name_on_the_cli(tmp_path):
    bad = tmp_path / "bad.toml"
    bad.write_text('extends = "default"\n', encoding="utf-8")

    result = CliRunner().invoke(cli, ["config", "validate", "--config", str(bad)])

    assert result.exit_code != 0
    assert 'extends must be "builtin"' in result.output


def test_create_writes_a_minimal_layered_config(tmp_path):
    out = write_user_config(tmp_path / "c.toml", data_root=str(tmp_path / "lake"))

    raw = tomllib.loads(out.read_text(encoding="utf-8"))

    assert raw == {"extends": "builtin", "data": {"root": (tmp_path / "lake").resolve().as_posix()}}


def test_toml_writer_round_trips_the_example():
    data = tomllib.loads(example_toml_text())
    assert tomllib.loads(dumps(data)) == data
    tricky = {"a": 'q"\\\n\x01', "b": {"c d": [1.5, True]}}
    assert tomllib.loads(dumps(tricky)) == tricky


def _full_config(tmp_path: Path) -> Path:
    text = example_toml_text().replace(
        'root = "../data/cnequity"', f'root = "{path_for_toml(tmp_path / "lake")}"'
    )
    # As `config create` used to write it on macOS, where validation insists on 1.
    text = re.sub(r"(?m)^workers = \d+", "workers = 1", text)
    text = text.replace('"fund_flow",', '"fund_flow_ths",', 1)
    text = text.replace("[job.daily.groups.research]", "[job.daily.groups.research_old]", 1)
    path = tmp_path / "full.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_minimize_keeps_only_differences_and_the_same_schedule(tmp_path):
    path = _full_config(tmp_path)
    before = load_config(path)

    plan = plan_minimize(path)

    assert plan.step_edits["job.daily.groups.capital"] == (["fund_flow_ths"], ["fund_flow"])
    assert "job.daily.groups.research" in plan.disabled_groups
    candidate = tmp_path / "min.toml"
    candidate.write_text(plan.text, encoding="utf-8")
    after = load_config(candidate)
    assert after.data_root == before.data_root
    assert set(after.schedule_groups) == set(before.schedule_groups)
    for name, group in before.schedule_groups.items():
        assert set(after.schedule_groups[name].steps) == set(group.steps), name
    assert plan.kept_keys < plan.total_keys


def test_minimize_reports_settings_the_defaults_now_supply(tmp_path):
    path = tmp_path / "old.toml"
    path.write_text(f'[data]\nroot = "{path_for_toml(tmp_path / "lake")}"\n', encoding="utf-8")

    plan = plan_minimize(path)

    names = {name for name, _, _ in plan.behaviour_changes}
    assert any(name.startswith("source_concurrency[") for name in names), names


def test_minimize_cli_writes_a_backup_and_dry_run_writes_nothing(tmp_path):
    path = _full_config(tmp_path)
    original = path.read_text(encoding="utf-8")

    dry = CliRunner().invoke(
        cli, ["config", "upgrade", "--minimize", "--dry-run", "--config", str(path)]
    )
    assert dry.exit_code == 0, dry.output
    assert path.read_text(encoding="utf-8") == original

    result = CliRunner().invoke(cli, ["config", "upgrade", "--minimize", "--config", str(path)])
    assert result.exit_code == 0, result.output
    assert tomllib.loads(path.read_text(encoding="utf-8"))["extends"] == "builtin"
    assert [p.read_text(encoding="utf-8") for p in tmp_path.glob("full.toml.bak-*")] == [original]
    again = CliRunner().invoke(cli, ["config", "upgrade", "--minimize", "--config", str(path)])
    assert "already extends" in again.output


def test_show_marks_file_settings_and_masks_secrets(tmp_path):
    path = _layered(
        tmp_path,
        '[sources.tushare]\ntoken = "secret-value"\n'
        '[sources.eastmoney]\nproxy = "http://user:pw@127.0.0.1:1"\n',
    )

    result = CliRunner().invoke(cli, ["config", "show", "--config", str(path)])

    assert result.exit_code == 0, result.output
    assert "secret-value" not in result.output
    assert "user:pw" not in result.output
    assert 'token = "***"  # set in this file' in result.output
    tomllib.loads(result.output)  # the comments aside, it is a valid config


def test_show_defaults_prints_the_documented_template():
    result = CliRunner().invoke(cli, ["config", "show", "--defaults"])

    assert result.exit_code == 0, result.output
    assert "[job.daily.groups.core]" in result.output


def test_explicit_opt_outs_are_recorded_on_the_config(tmp_path):
    path = _layered(
        tmp_path,
        '[job.daily.groups.capital]\nsteps_remove = ["fund_flow"]\nsteps_add = ["fund_flow_ths"]\n'
        "[job.daily.groups.research]\nenabled = false\n",
    )
    cfg = load_config(path)
    assert "fund_flow" in cfg.opted_out_steps
    assert set(_example_groups()["research"]["steps"]) <= cfg.opted_out_steps
    assert "fund_flow_ths" not in cfg.opted_out_steps
