#!/usr/bin/env bash
# Preserve host groups, vantage, cadence and config overrides on reinstall.
# CNE_GROUPS / CNE_SOURCE_VANTAGE explicitly override those saved choices.
# --check reports drift; --dry-run DIR renders without changing launchd.
# --daily-only updates the daily agent without adding/changing other jobs.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ "$(uname)" != "Darwin" ]]; then
  echo "install_scheduler: launchd is macOS-only; use daily_pipeline.sh/stale_pipeline.sh with cron." >&2
  exit 1
fi
if [[ ! -x "$REPO_ROOT/.venv/bin/cne" ]]; then
  echo "install_scheduler: create the project venv first." >&2
  exit 1
fi
exec "${CNE_PYTHON:-$REPO_ROOT/.venv/bin/python}" "$REPO_ROOT/scripts/scheduler_config.py" "$@"
