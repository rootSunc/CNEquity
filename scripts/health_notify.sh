#!/usr/bin/env bash
# B2 — Freshness SLO check with a local notification on failure.
# Runs the whole-lake health snapshot and per-dataset freshness gate; if either
# reports a problem (non-zero exit), pops a macOS notification and exits 1 so a
# scheduler treats the day as failed. Safe to run standalone or from the daily
# pipeline.
#
# The whole-lake audit (`cne audit --full`) reads every historical Parquet file.
# On a 25 GB lake that is I/O bound for hours — measured at 1h39m wall for 7m of
# CPU — and it ran here on every single trading day, which made the health check,
# not ingestion, the wall-clock cost of the daily pipeline. The per-run audit
# inspects only the active partitions, so the daily gate uses that plus the
# freshness check, and the whole-lake sweep moves to once a week.
#
# Usage: scripts/health_notify.sh
# Env: CNE_BIN (cne path), CNE_CONFIG (config path), CNE_LOG_DIR (log destination),
#      CNE_NOTIFY=0 to suppress the desktop notification,
#      CNE_FULL_AUDIT_DOW=6 (default, Saturday; 1-7 = Mon-Sun) — day the
#        whole-lake audit runs. 0 disables it, "always" restores the old
#        every-day behaviour.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Same override as daily_pipeline.sh/stale_pipeline.sh: this script is one of
# the steps that pipeline runs, so a CNE_BIN it honours and this one ignores
# meant the health gate silently probed a different (or absent) binary.
CNE="${CNE_BIN:-$REPO_ROOT/.venv/bin/cne}"
CONFIG="${CNE_CONFIG:-$REPO_ROOT/configs/cnequity.toml}"
LOG_DIR="${CNE_LOG_DIR:-$REPO_ROOT/data/cnequity/logs}"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/health-$(date +%Y%m%d).log"

notify() {
  # $1 = title, $2 = message. macOS only; no-op elsewhere or when suppressed.
  [[ "${CNE_NOTIFY:-1}" == "0" ]] && return 0
  command -v osascript >/dev/null 2>&1 || return 0
  local msg="${2//\"/\'}"
  osascript -e "display notification \"${msg}\" with title \"${1}\"" >/dev/null 2>&1 || true
}

{
  echo "==== health check $(date '+%Y-%m-%d %H:%M:%S') ===="
} >>"$LOG"

problems=()

FULL_AUDIT_DOW="${CNE_FULL_AUDIT_DOW:-6}"
today_dow="$(date +%u)"
if [[ "$FULL_AUDIT_DOW" == "always" || "$today_dow" == "$FULL_AUDIT_DOW" ]]; then
  audit_mode="full"
else
  audit_mode="per-run"
fi
echo "audit mode: $audit_mode (CNE_FULL_AUDIT_DOW=$FULL_AUDIT_DOW, today=$today_dow)" >>"$LOG"

if [[ "$audit_mode" == "full" ]]; then
  # 1a. Whole-lake health snapshot. Also refreshes meta/quality/health-latest.json,
  # which is what `cne serve` reads for its health card.
  if ! health_out="$("$CNE" audit --full --quality-only --config "$CONFIG" 2>&1)"; then
    problems+=("lake health UNHEALTHY")
  fi
else
  # 1b. Per-run audit: active partitions only, and it gates on its own error
  # findings exactly as --full gates on UNHEALTHY.
  if ! health_out="$("$CNE" audit --config "$CONFIG" 2>&1)"; then
    problems+=("error finding(s) in the latest run")
  fi
fi
echo "$health_out" >>"$LOG"

# 2. Per-dataset freshness gate, scoped to the groups this host actually runs.
# Unscoped, it failed every single day on a core-only host: twenty-odd datasets
# belong to groups no job here fetches, so they are permanently stale by
# construction — 21 to 25 of them on 2026-09-12/13/14. A notification that
# fires daily is one an operator stops reading, and three genuinely UNHEALTHY
# days went by inside that noise. CNE_GROUPS is the same variable the pipeline
# and the launchd plist use, so the gate and the scheduler cannot drift apart.
gate_groups=()
if [[ -n "${CNE_GROUPS:-}" ]]; then
  gate_groups=(--groups "$CNE_GROUPS")
fi
status_out="freshness gate disabled (no scheduled gate group)"
if [[ "${CNE_FRESHNESS_CHECK:-1}" != "0" ]] && ! status_out="$("$CNE" status --datasets "${gate_groups[@]+"${gate_groups[@]}"}" \
  --config "$CONFIG" 2>&1)"; then
  problems+=("dataset(s) STALE")
fi
echo "$status_out" >>"$LOG"

if [[ ${#problems[@]} -gt 0 ]]; then
  summary="$(IFS='; '; echo "${problems[*]}")"
  # Pull the concise UNHEALTHY/STALE lines for the notification body.
  detail="$(printf '%s\n%s\n' "$health_out" "$status_out" \
    | grep -iE 'UNHEALTHY|STALE|\[error\]' | head -4 | tr '\n' ' ')"
  echo "RESULT: FAIL — $summary" >>"$LOG"
  # Title by what actually failed. "数据异常" over a pure freshness miss is the
  # kind of wrong that costs the alert its meaning: nothing was anomalous, a
  # scheduled group had simply not run.
  case "$summary" in
    *health*|*error*) title="cnequity 数据异常" ;;
    *) title="cnequity 数据滞后" ;;
  esac
  notify "$title" "${summary}. ${detail} 见 $LOG"
  echo "health_notify: FAIL — $summary (log: $LOG)" >&2
  exit 1
fi

echo "RESULT: OK" >>"$LOG"
echo "health_notify: OK (log: $LOG)"
