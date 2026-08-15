#!/usr/bin/env bash
# B2 — Freshness SLO check with a local notification on failure.
# Runs the whole-lake health snapshot and per-dataset freshness gate; if either
# reports a problem (non-zero exit), pops a macOS notification and exits 1 so a
# scheduler treats the day as failed. Safe to run standalone or from the daily
# pipeline.
#
# Usage: scripts/health_notify.sh
# Env: CML_CONFIG (config path), CML_LOG_DIR (log destination),
#      CML_NOTIFY=0 to suppress the desktop notification.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CML="$REPO_ROOT/.venv/bin/cml"
CONFIG="${CML_CONFIG:-$REPO_ROOT/configs/cn-market-lake.toml}"
LOG_DIR="${CML_LOG_DIR:-$REPO_ROOT/data/cn-market-lake/logs}"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/health-$(date +%Y%m%d).log"

notify() {
  # $1 = title, $2 = message. macOS only; no-op elsewhere or when suppressed.
  [[ "${CML_NOTIFY:-1}" == "0" ]] && return 0
  command -v osascript >/dev/null 2>&1 || return 0
  local msg="${2//\"/\'}"
  osascript -e "display notification \"${msg}\" with title \"${1}\"" >/dev/null 2>&1 || true
}

{
  echo "==== health check $(date '+%Y-%m-%d %H:%M:%S') ===="
} >>"$LOG"

problems=()

# 1. Whole-lake health snapshot (errors + STALE datasets -> exit 1).
if ! health_out="$("$CML" audit --full --config "$CONFIG" 2>&1)"; then
  problems+=("lake health UNHEALTHY")
fi
echo "$health_out" >>"$LOG"

# 2. Per-dataset freshness gate (any STALE -> exit 1).
if ! status_out="$("$CML" status --datasets --config "$CONFIG" 2>&1)"; then
  problems+=("dataset(s) STALE")
fi
echo "$status_out" >>"$LOG"

if [[ ${#problems[@]} -gt 0 ]]; then
  summary="$(IFS='; '; echo "${problems[*]}")"
  # Pull the concise UNHEALTHY/STALE lines for the notification body.
  detail="$(printf '%s\n%s\n' "$health_out" "$status_out" \
    | grep -iE 'UNHEALTHY|STALE|\[error\]' | head -4 | tr '\n' ' ')"
  echo "RESULT: FAIL — $summary" >>"$LOG"
  notify "cn-market-lake 数据异常" "${summary}. ${detail} 见 $LOG"
  echo "health_notify: FAIL — $summary (log: $LOG)" >&2
  exit 1
fi

echo "RESULT: OK" >>"$LOG"
echo "health_notify: OK (log: $LOG)"
