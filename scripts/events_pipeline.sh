#!/usr/bin/env bash
# B1 — Continuous event streams (disclosures, regulatory events, news).
#
# A separate entry point from daily_pipeline.sh on purpose. These feeds publish
# on weekends and holidays, so `cne run events` is not gated on the trading
# calendar, and it takes its own `events_ingestion` lock rather than the one
# every daily group shares. This wrapper therefore also takes its own shell
# lock: an event sweep must not be skipped just because the evening batch is
# still running. The two jobs are validated to ingest disjoint datasets
# (`validate_config`), so overlapping them is safe.
#
# The groups run in the order they appear in [job.events.groups], which is how
# `regulatory_events` gets to read the announcements the group before it just
# published.
#
# Usage: scripts/events_pipeline.sh [YYYY-MM-DD]
# Env: CNE_CONFIG, CNE_LOG_DIR, CNE_BIN, CNE_TRADE_DATE,
#      CNE_EVENTS_GROUP (one group; default: every configured group),
#      CNE_SCHEDULER_LOCK_DIR (or CNE_LOCK_DIR for compatibility).
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CNE="${CNE_BIN:-$REPO_ROOT/.venv/bin/cne}"
CONFIG="${CNE_CONFIG:-$REPO_ROOT/configs/cnequity.toml}"
LOG_DIR="${CNE_LOG_DIR:-$REPO_ROOT/data/cnequity/logs}"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/events-$(date +%Y%m%d).log"
TRADE_DATE="${1:-${CNE_TRADE_DATE:-}}"
GROUP="${CNE_EVENTS_GROUP:-}"

# Bash 3.2 raises an unbound-variable error for an empty `${arr[@]}` under
# `set -u`; use the guarded expansion used by daily_pipeline.sh.
ARGS=()
if [[ -n "$TRADE_DATE" ]]; then
  ARGS+=(--trade-date "$TRADE_DATE")
fi
if [[ -n "$GROUP" ]]; then
  ARGS+=(--group "$GROUP")
fi

. "$REPO_ROOT/scripts/scheduler_lock.sh"

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG"; }

lock_rc=0
scheduler_lock_acquire "$REPO_ROOT" events || lock_rc=$?
if [[ "$lock_rc" -eq 1 ]]; then
  # A previous sweep is still running. Skipping costs nothing: every group
  # re-reads its own window on the next tick.
  log "another events sweep is active — skipping"
  exit 0
elif [[ "$lock_rc" -ne 0 ]]; then
  log "unable to acquire scheduler lock (rc=$lock_rc)"
  exit 1
fi
scheduler_lock_install_traps

log "==== events pipeline start $(date '+%Y-%m-%d %H:%M:%S') date=${TRADE_DATE:-today} group=${GROUP:-all} ===="
if "$CNE" run events --config "$CONFIG" ${ARGS[@]+"${ARGS[@]}"} >>"$LOG" 2>&1; then
  log "==== events pipeline DONE ok ===="
  exit 0
else
  status=$?
  log "events sweep FAILED (exit $status; see $LOG)"
  log "==== events pipeline DONE failed ===="
  exit "$status"
fi
