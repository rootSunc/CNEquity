"""Read-only ``cne backfill --plan`` for the preview step.

Run out of process so a plan cannot change the serve process's config or
logging. Sixty seconds is a bound, not a promise that every plan is fast.
"""

from __future__ import annotations

import subprocess
import sys

from cnequity.i18n import _
from cnequity.ops.catalog import OpsError
from cnequity.ops.environment import command_environment

PLAN_TIMEOUT_SECONDS = 60


def describe_backfill(argv: list[str]) -> str:
    """Return the plan text, or raise ``OpsError`` when the plan itself fails."""
    try:
        completed = subprocess.run(
            [sys.executable, "-m", "cnequity", *argv, "--plan"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=command_environment(),
            timeout=PLAN_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise OpsError(
            _("The backfill preview took over 60 seconds, so it did not start.")
        ) from exc
    except OSError as exc:
        raise OpsError(_("Could not start the backfill preview ({exc}).").format(exc=exc)) from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise OpsError(detail or _("The backfill preview failed, so it did not start."))
    return completed.stdout.strip()
