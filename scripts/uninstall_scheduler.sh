#!/usr/bin/env bash
# B1 — Remove the launchd agent installed by install_scheduler.sh.
set -euo pipefail

LABEL="com.cnmarketlake.daily"
DEST="$HOME/Library/LaunchAgents/$LABEL.plist"

if [[ "$(uname)" != "Darwin" ]]; then
  echo "uninstall_scheduler: launchd is macOS-only; nothing to do." >&2
  exit 0
fi

if [[ -f "$DEST" ]]; then
  launchctl unload "$DEST" 2>/dev/null || true
  rm -f "$DEST"
  echo "uninstall_scheduler: removed $LABEL ($DEST)"
else
  echo "uninstall_scheduler: no plist at $DEST — nothing to do."
fi
