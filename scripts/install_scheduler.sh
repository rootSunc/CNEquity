#!/usr/bin/env bash
# B1 — Install (or reinstall) the launchd agents for daily and stale-only runs.
# Generates plists from the templates with this repo's absolute path, drops
# them in ~/Library/LaunchAgents, and loads them. Idempotent: re-run to update.
#
# Usage: scripts/install_scheduler.sh
# Uninstall: scripts/uninstall_scheduler.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="com.cnequity.daily"
STALE_LABEL="com.cnequity.stale"
EVENTS_LABEL="com.cnequity.events"
TEMPLATE="$REPO_ROOT/scripts/launchd/$LABEL.plist.template"
STALE_TEMPLATE="$REPO_ROOT/scripts/launchd/$STALE_LABEL.plist.template"
EVENTS_TEMPLATE="$REPO_ROOT/scripts/launchd/$EVENTS_LABEL.plist.template"
DEST_DIR="$HOME/Library/LaunchAgents"
DEST="$DEST_DIR/$LABEL.plist"
STALE_DEST="$DEST_DIR/$STALE_LABEL.plist"
EVENTS_DEST="$DEST_DIR/$EVENTS_LABEL.plist"
SOURCE_VANTAGE="${CNE_SOURCE_VANTAGE:-local}"
LAUNCHCTL="${CNE_LAUNCHCTL:-launchctl}"

if [[ "$(uname)" != "Darwin" ]]; then
  echo "install_scheduler: launchd is macOS-only. On Linux use cron:" >&2
  echo "  15 11 * * *  $REPO_ROOT/scripts/daily_pipeline.sh" >&2
  echo "  5 20 * * *   $REPO_ROOT/scripts/stale_pipeline.sh" >&2
  echo "  0 14 * * *   $REPO_ROOT/scripts/events_pipeline.sh" >&2
  exit 1
fi
if [[ ! -x "$REPO_ROOT/.venv/bin/cne" ]]; then
  echo "install_scheduler: $REPO_ROOT/.venv/bin/cne not found — create the venv first." >&2
  exit 1
fi
if [[ ! "$SOURCE_VANTAGE" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "install_scheduler: CNE_SOURCE_VANTAGE must match [A-Za-z0-9._-]+" >&2
  exit 1
fi

mkdir -p "$DEST_DIR" "$REPO_ROOT/data/cnequity/logs"

# Values are inserted into XML text nodes, so escape XML first and then the
# `#`-delimited sed replacement syntax. A checkout such as ``CN&Equity`` must
# remain both a valid path after plist decoding and a well-formed plist.
xml_escape() {
  printf '%s' "$1" | sed \
    -e 's/&/\&amp;/g' \
    -e 's/</\&lt;/g' \
    -e 's/>/\&gt;/g' \
    -e 's/"/\&quot;/g' \
    -e "s/'/\\\&apos;/g"
}
REPO_ROOT_XML="$(xml_escape "$REPO_ROOT")"
SOURCE_VANTAGE_XML="$(xml_escape "$SOURCE_VANTAGE")"
REPO_ROOT_SED="$(printf '%s' "$REPO_ROOT_XML" | sed 's/[\\&#]/\\&/g')"
SOURCE_VANTAGE_SED="$(printf '%s' "$SOURCE_VANTAGE_XML" | sed 's/[\\&#]/\\&/g')"
render_template() {
  local template="$1" dest="$2" tmp
  tmp="$(mktemp "${dest}.XXXXXX")"
  if ! sed \
    -e "s#__REPO_ROOT__#$REPO_ROOT_SED#g" \
    -e "s#__SOURCE_VANTAGE__#$SOURCE_VANTAGE_SED#g" \
    "$template" >"$tmp"; then
    rm -f "$tmp"
    return 1
  fi
  if ! mv -f "$tmp" "$dest"; then
    rm -f "$tmp"
    return 1
  fi
}

render_template "$TEMPLATE" "$DEST"
render_template "$STALE_TEMPLATE" "$STALE_DEST"
render_template "$EVENTS_TEMPLATE" "$EVENTS_DEST"

# Reload if already present.  The daily and stale agents share the shell-level
# scheduler lock, so a stale pass never overlaps the multi-group run even when
# an operator starts it manually during the daily window.  The events agent
# deliberately holds its own: those feeds publish around the clock and must not
# be skipped because the evening batch is still going.
"$LAUNCHCTL" unload "$DEST" 2>/dev/null || true
"$LAUNCHCTL" unload "$STALE_DEST" 2>/dev/null || true
"$LAUNCHCTL" unload "$EVENTS_DEST" 2>/dev/null || true
"$LAUNCHCTL" load "$DEST"
"$LAUNCHCTL" load "$STALE_DEST"
"$LAUNCHCTL" load "$EVENTS_DEST"

echo "install_scheduler: loaded $LABEL, $STALE_LABEL and $EVENTS_LABEL"
echo "  daily plist:  $DEST"
echo "  stale plist:  $STALE_DEST"
echo "  events plist: $EVENTS_DEST"
echo "  daily schedule: daily 11:15 host-local time"
echo "  stale schedule: daily 20:05 host-local time"
echo "  events schedule: every calendar day 14:00 host-local time"
echo "  source vantage: $SOURCE_VANTAGE"
echo "  logs:     $REPO_ROOT/data/cnequity/logs/"
echo "  verify:   launchctl list | grep cnequity"
echo "  test now: launchctl start $LABEL"
echo "  test stale: launchctl start $STALE_LABEL"
echo "  test events: launchctl start $EVENTS_LABEL"
