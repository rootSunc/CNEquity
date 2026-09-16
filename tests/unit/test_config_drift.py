"""`cne config diff` — what a config written once has fallen behind.

The user config is gitignored and written a single time by `cne config create`.
A release that adds a step to a schedule group is therefore invisible: the
feature ships, the config never schedules it, and `cne config validate` still
answers `Configuration OK`.
"""

from __future__ import annotations

from cnequity.config.bootstrap import example_toml_text
from cnequity.config.drift import config_drift, render_drift


def _write(tmp_path, text: str):
    path = tmp_path / "cnequity.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_the_packaged_example_has_no_drift_against_itself(tmp_path):
    drift = config_drift(_write(tmp_path, example_toml_text()))

    assert drift.clean
    assert drift.unscheduled_steps == []


def test_a_step_missing_from_every_group_is_reported(tmp_path):
    """The case that actually loses data: installed, configured nowhere, never runs."""
    text = example_toml_text().replace(
        'steps = ["corporate_actions", "daily_bars", "trading_status_derive"]',
        'steps = ["corporate_actions", "daily_bars"]',
    )
    drift = config_drift(_write(tmp_path, text))

    assert "trading_status_derive" in drift.unscheduled_steps
    assert not drift.clean


def test_missing_sections_and_keys_are_separated(tmp_path):
    text = example_toml_text().replace('audit_gate = "shadow"', "")
    drift = config_drift(_write(tmp_path, text))

    assert "quality.audit_gate" in drift.missing_keys
    assert "quality" not in drift.missing_sections


def test_a_whole_missing_section_is_reported_once_not_per_key(tmp_path):
    lines = example_toml_text().splitlines()
    start = lines.index("[incremental]")
    end = next(i for i in range(start + 1, len(lines)) if lines[i].startswith("["))
    drift = config_drift(_write(tmp_path, "\n".join(lines[:start] + lines[end:])))

    assert "incremental" in drift.missing_sections
    assert not [key for key in drift.missing_keys if key.startswith("incremental.")]


def test_local_values_are_not_drift(tmp_path):
    """data.root and the platform worker count are the operator's, always."""
    text = (
        example_toml_text()
        .replace('root = "./data/cnequity"', 'root = "/srv/lake"')
        .replace("workers = 1", "workers = 8")
    )
    drift = config_drift(_write(tmp_path, text))

    assert drift.clean


def test_extra_local_settings_are_not_reported(tmp_path):
    """A config may carry more than the example; only what it lacks matters."""
    drift = config_drift(_write(tmp_path, example_toml_text() + "\n[my_own]\nthing = 1\n"))

    assert drift.clean


def test_render_names_the_unscheduled_steps_first(tmp_path):
    text = example_toml_text().replace(
        'steps = ["corporate_actions", "daily_bars", "trading_status_derive"]',
        'steps = ["corporate_actions", "daily_bars"]',
    )
    path = _write(tmp_path, text)

    lines = render_drift(config_drift(path), path)

    assert "trading_status_derive" in lines[1]
    assert "未被调度" in lines[0]


def test_render_says_so_when_there_is_nothing_to_report(tmp_path):
    path = _write(tmp_path, example_toml_text())

    assert len(render_drift(config_drift(path), path)) == 1
