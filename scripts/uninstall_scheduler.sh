#!/usr/bin/env bash
# B1 — Remove the launchd agents installed by install_scheduler.sh.
set -euo pipefail

LABEL="com.cnequity.daily"
STALE_LABEL="com.cnequity.stale"
EVENTS_LABEL="com.cnequity.events"
DEST_DIR="$HOME/Library/LaunchAgents"
DEST="$DEST_DIR/$LABEL.plist"
STALE_DEST="$DEST_DIR/$STALE_LABEL.plist"
EVENTS_DEST="$DEST_DIR/$EVENTS_LABEL.plist"
LAUNCHCTL="${CNE_LAUNCHCTL:-launchctl}"

if [[ "$(uname)" != "Darwin" ]]; then
  echo "uninstall_scheduler: launchd is macOS-only; nothing to do." >&2
  exit 0
fi

removed=0
for pair in "$LABEL:$DEST" "$STALE_LABEL:$STALE_DEST" "$EVENTS_LABEL:$EVENTS_DEST"; do
  label="${pair%%:*}"
  plist="${pair#*:}"
  if [[ -f "$plist" ]]; then
    "$LAUNCHCTL" unload "$plist" 2>/dev/null || true
    rm -f "$plist"
    echo "uninstall_scheduler: removed $label ($plist)"
    removed=1
  fi
done

if [[ "$removed" -eq 0 ]]; then
  echo "uninstall_scheduler: no scheduler plists at $DEST_DIR — nothing to do."
fi
