"""`cne config upgrade` — schedule drift fixed by the command that reports it."""

from __future__ import annotations

import re
import sys

import pytest
from click.testing import CliRunner

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - Python 3.10
    import tomli as tomllib

from cnequity.cli.main import cli
from cnequity.config.bootstrap import example_toml_text
from cnequity.config.drift import config_drift
from cnequity.config.upgrade import _append_to_steps, plan_upgrade


def _old_config(tmp_path, text: str | None = None):
    """The example as an older release shipped it: core without `ths_official_snapshot`."""
    text = text if text is not None else example_toml_text()
    text = text.replace(
        '  "compact", "derive_adj_factors", "derive_industry_index",\n  "ths_official_snapshot",\n]',
        '  "compact", "derive_adj_factors", "derive_industry_index",\n]',
    )
    # `cne config create` writes workers = 1 on macOS; the raw example does not.
    text = re.sub(r"(?m)^workers = \d+", "workers = 1", text)
    path = tmp_path / "cnequity.toml"
    path.write_text(text, encoding="utf-8")
    return path


def _core_steps(path) -> list[str]:
    return tomllib.loads(path.read_text(encoding="utf-8"))["job"]["daily"]["groups"]["core"][
        "steps"
    ]


def test_upgrade_restores_the_group_steps_and_keeps_a_backup(tmp_path):
    path = _old_config(tmp_path)
    before = path.read_text(encoding="utf-8")

    result = CliRunner().invoke(cli, ["config", "upgrade", "--config", str(path)])

    assert result.exit_code == 0, result.output
    assert "ths_official_snapshot" in _core_steps(path)
    assert config_drift(path).group_steps == {}
    backups = list(tmp_path.glob("cnequity.toml.bak-*"))
    assert len(backups) == 1 and backups[0].read_text(encoding="utf-8") == before


def test_dry_run_writes_nothing(tmp_path):
    path = _old_config(tmp_path)
    before = path.read_text(encoding="utf-8")

    result = CliRunner().invoke(cli, ["config", "upgrade", "--dry-run", "--config", str(path)])

    assert result.exit_code == 0, result.output
    assert "ths_official_snapshot" in result.output
    assert path.read_text(encoding="utf-8") == before
    assert not list(tmp_path.glob("*.bak-*"))


@pytest.mark.usefixtures("chinese_output")
def test_an_up_to_date_config_is_left_alone(tmp_path):
    path = tmp_path / "cnequity.toml"
    path.write_text(example_toml_text(), encoding="utf-8")

    result = CliRunner().invoke(cli, ["config", "upgrade", "--config", str(path)])

    assert result.exit_code == 0, result.output
    assert "无需升级" in result.output
    assert not list(tmp_path.glob("*.bak-*"))


def test_a_missing_group_is_copied_with_its_comments(tmp_path):
    text = example_toml_text()
    start = text.index("# Futures and options.")
    end = text.index("# Not scheduled by anything")
    path = _old_config(tmp_path, text[:start] + text[end:])

    plan = plan_upgrade(path)

    assert plan.added_groups == ["job.daily.groups.derivatives"]
    assert "# Futures and options." in plan.text
    groups = tomllib.loads(plan.text)["job"]["daily"]["groups"]
    assert list(groups)[-1] == "derivatives", "appended after the existing groups"


def test_operator_settings_survive(tmp_path):
    text = example_toml_text().replace('at = "17:00"', 'at = "17:05"  # my slot')
    path = _old_config(tmp_path, text)

    plan = plan_upgrade(path)

    assert 'at = "17:05"  # my slot' in plan.text
    assert tomllib.loads(plan.text)["job"]["daily"]["groups"]["capital"]["at"] == "17:05"


@pytest.mark.usefixtures("chinese_output")
def test_a_layout_the_edit_cannot_read_is_left_for_a_manual_edit(tmp_path):
    """An inline core table is not edited; the run says which steps need a hand."""
    path = tmp_path / "cnequity.toml"
    path.write_text(
        'job.daily.groups.core = { at = "16:00", steps = ["daily_bars", "compact"] }\n',
        encoding="utf-8",
    )

    result = CliRunner().invoke(cli, ["config", "upgrade", "--config", str(path)])

    assert result.exit_code == 1
    assert "手动" in result.output and "ths_official_snapshot" in result.output
    assert _core_steps(path) == ["daily_bars", "compact"]


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ('steps = ["a", "b"]\n', ["a", "b", "x", "y"]),
        ('steps = ["a", "b",]\n', ["a", "b", "x", "y"]),
        ("steps = []\n", ["x", "y"]),
        ('steps = [\n  "a",\n  "b"\n]\n', ["a", "b", "x", "y"]),
        ('steps = [\n  "a",  # keep\n  "b",  # keep too\n]\n', ["a", "b", "x", "y"]),
        ('steps = ["a",\n  "b"]  # trailing\n', ["a", "b", "x", "y"]),
    ],
)
def test_append_handles_array_layouts(body, expected):
    text = (
        f'[job.daily.groups.core]\nat = "16:00"\n{body}\n[job.daily.groups.next]\nsteps = ["z"]\n'
    )

    edited = _append_to_steps(text, "job.daily.groups.core", ["x", "y"])

    parsed = tomllib.loads(edited)
    assert parsed["job"]["daily"]["groups"]["core"]["steps"] == expected
    assert parsed["job"]["daily"]["groups"]["next"]["steps"] == ["z"]
    assert "# keep" in edited or "# keep" not in body


def test_explicit_dry_run_is_only_for_upgrade(tmp_path):
    path = tmp_path / "cnequity.toml"
    path.write_text(example_toml_text(), encoding="utf-8")
    result = CliRunner().invoke(cli, ["config", "validate", "--dry-run", "--config", str(path)])
    assert result.exit_code != 0


def _with_old_names(tmp_path):
    """Old spellings in place of the current ones, each still taking effect."""
    text = example_toml_text()
    text = re.sub(r"(?m)^tdx_daily_backend = .*$", 'tdx_daily_executor = "thread"  # pinned', text)
    text = re.sub(r"(?m)^derive_workers = \d+\n", "", text)
    text = text.replace("[sources.cninfo]\n", "[sources.cninfo]\nconcurrency = 2\nworker_cap\n", 1)
    # After _old_config, whose macOS rewrite would turn these into workers = 1.
    path = _old_config(tmp_path, text + "\n[derive]\nworker_cap\n")
    text = path.read_text(encoding="utf-8").replace("worker_cap\n", "workers = 5\n", 1)
    path.write_text(text.replace("worker_cap\n", "workers = 3\n", 1), encoding="utf-8")
    return path


def test_old_key_names_are_rewritten_without_changing_the_settings(tmp_path):
    from cnequity.config import load_config

    path = _with_old_names(tmp_path)
    before = load_config(path)

    plan = plan_upgrade(path)
    written = {(i.old_table, i.old_key, i.shadowed) for i in plan.renamed_keys}
    assert written == {
        ("orchestrator", "tdx_daily_executor", False),
        ("derive", "workers", False),
        ("sources.cninfo", "concurrency", False),
        ("sources.cninfo", "workers", True),
    }
    result = CliRunner().invoke(cli, ["config", "upgrade", "--config", str(path)])

    assert result.exit_code == 0, result.output
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    assert raw["orchestrator"]["tdx_daily_backend"] == "thread"
    assert raw["orchestrator"]["derive_workers"] == 3
    assert raw["sources"]["cninfo"]["max_concurrency"] == 2
    assert "tdx_daily_executor" not in raw["orchestrator"]
    assert "workers" not in raw["derive"]
    assert {"concurrency", "workers"}.isdisjoint(raw["sources"]["cninfo"])
    assert "# pinned" in path.read_text(encoding="utf-8")
    after = load_config(path)
    assert (
        (after.tdx_daily_backend, after.derive_workers)
        == (
            before.tdx_daily_backend,
            before.derive_workers,
        )
        == ("thread", 3)
    )
    assert after.source_concurrency["cninfo"] == before.source_concurrency["cninfo"] == 2
    assert plan_upgrade(path).renamed_keys == []


def test_an_old_key_the_new_name_overrides_is_only_deleted(tmp_path):
    text = example_toml_text().replace(
        "[orchestrator]\n", '[orchestrator]\ntdx_daily_executor = "process"\n', 1
    )
    path = _old_config(tmp_path, text)

    plan = plan_upgrade(path)

    [item] = plan.renamed_keys
    assert (item.old_key, item.shadowed) == ("tdx_daily_executor", True)
    raw = tomllib.loads(plan.text)
    assert "tdx_daily_executor" not in raw["orchestrator"]
    assert raw["orchestrator"]["tdx_daily_backend"] == "auto"


def test_loading_an_old_key_name_warns_once(tmp_path, caplog):
    from cnequity.config import load_config, loader

    path = _with_old_names(tmp_path)
    loader._WARNED_RENAMES.clear()
    with caplog.at_level("WARNING", logger="cnequity.config.loader"):
        load_config(path)
        load_config(path)
    warned = [r.getMessage() for r in caplog.records if "old name" in r.getMessage()]
    assert len(warned) == 4
    assert any("[derive] workers" in m and "derive_workers" in m for m in warned)
