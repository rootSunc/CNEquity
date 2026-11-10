"""The job runner and scheduler live in `cnequity.ops`, outside the web package."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src" / "cnequity"


def test_old_module_names_are_the_same_modules():
    import cnequity.ops.runner as runner
    import cnequity.ops.scheduler as scheduler
    import cnequity.serve.ops.runner as old_runner
    from cnequity.serve.ops import scheduler as old_scheduler

    assert old_runner is runner and old_scheduler is scheduler


def test_an_installed_timer_keeps_its_command(tmp_path):
    """`cne schedule status` compares the installed command exactly."""
    from cnequity.ops.scheduler_backend import SchedulerBackend

    backend = SchedulerBackend(tmp_path / "meta", tmp_path / "cne.toml")
    assert backend.argv[3:5] == ["-m", "cnequity.serve.ops.scheduled"]
    result = subprocess.run(
        [sys.executable, "-m", "cnequity.serve.ops.scheduled", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_the_cli_reaches_the_web_package_only_to_start_it():
    importers = sorted(
        path.name
        for path in (SRC / "cli").glob("*.py")
        if "cnequity.serve" in path.read_text(encoding="utf-8")
    )
    assert importers == ["consume_cmds.py"], importers
