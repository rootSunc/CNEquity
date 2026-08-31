#!/usr/bin/env bash
# B1 — Daily ingestion pipeline. Runs the schedule groups in dependency order,
# then the health check and metadata backup. Designed to be the single entry
# point a launchd/cron job fires each trading day.
#
# Groups run sequentially on purpose: the engine is pinned to workers=1 because
# mootdx is not fork-safe, and running one source-heavy group at a time avoids
# hammering the same upstream. A non-trading-day run is a cheap no-op (each
# `cne run daily` exits 0 with skipped_non_trading_day).
#
# One group failing does not abort the rest — we want as much of the day's data
# as possible — but any failure makes the pipeline exit non-zero after the
# health check reports it.
#
# A late stale-only pass is installed separately (see stale_pipeline.sh), so a
# source outage cannot hold the six-group pipeline open for half an hour. The
# old in-process delayed retry remains available as an explicit compatibility
# switch (`CNE_STALE_RETRY=1`) for callers that still want that behaviour.
#
# Usage: scripts/daily_pipeline.sh [YYYY-MM-DD]
# Env: CNE_CONFIG, CNE_LOG_DIR, CNE_GROUPS (space-separated override),
#      CNE_GATE_GROUPS (space-separated; default "core" — failure ⇒ hard fail),
#      CNE_SOFT_FAIL_OK=1 (default) — gate OK 时东财/soft 失败只告警、exit 0；
#        设为 0 则 soft 失败仍 exit 1（国内全组日更可用），
#      CNE_STALE_RETRY=0 (default) — 兼容开关；设为 1 才在本进程收尾补抓，
#      CNE_STALE_RETRY_DELAY_SEC=1800 (default) — 兼容补抓前等多久，
#      CNE_SOURCE_HEALTH=1 (default) — 每日串行探测并积累 SLO 样本；0 关闭，
#      CNE_SOURCE_VANTAGE=local — 当前网络出口的稳定标签，
#      CNE_TRADE_DATE (same as optional CLI arg — catch up a prior session).
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Overridable so the control flow can be exercised against a stub instead of a
# real lake and a real network.
CNE="${CNE_BIN:-$REPO_ROOT/.venv/bin/cne}"
CONFIG="${CNE_CONFIG:-$REPO_ROOT/configs/cnequity.toml}"
LOG_DIR="${CNE_LOG_DIR:-$REPO_ROOT/data/cnequity/logs}"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/daily-$(date +%Y%m%d).log"
TRADE_DATE="${1:-${CNE_TRADE_DATE:-}}"
# Expanded below as ${DATE_ARGS[@]+"${DATE_ARGS[@]}"}: macOS ships bash 3.2,
# where "${arr[@]}" on an empty array is an unbound-variable error under `set -u`
# (fixed in bash 4.4). Every scheduled run omits --trade-date, so the array is
# empty and the plain form killed the pipeline at its first group.
DATE_ARGS=()
if [[ -n "$TRADE_DATE" ]]; then
  DATE_ARGS=(--trade-date "$TRADE_DATE")
fi

# Order mirrors configs/cnequity.toml [job.daily.groups] cadence
# (core 16:00 → research 18:30). Sequential, not by wall-clock time.
# NB: not named GROUPS — that is a reserved bash builtin (user group IDs).
GROUP_LIST="${CNE_GROUPS:-core capital signals fundamentals macro_risk research}"
GATE_GROUP_LIST="${CNE_GATE_GROUPS:-core}"
# Overseas Mac: expected EM lag must not paint the whole day red.
SOFT_FAIL_OK="${CNE_SOFT_FAIL_OK:-1}"
STALE_RETRY="${CNE_STALE_RETRY:-0}"
STALE_RETRY_DELAY_SEC="${CNE_STALE_RETRY_DELAY_SEC:-1800}"
SOURCE_HEALTH="${CNE_SOURCE_HEALTH:-1}"
SOURCE_VANTAGE="${CNE_SOURCE_VANTAGE:-local}"

# `mkdir` is the portable atomic primitive available in macOS Bash 3.2. Keep
# one lock around the entire script so the independently scheduled stale pass
# cannot overlap a group, health check, or metadata backup.
. "$REPO_ROOT/scripts/scheduler_lock.sh"

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG"; }

lock_rc=0
scheduler_lock_acquire "$REPO_ROOT" daily || lock_rc=$?
if [[ "$lock_rc" -eq 1 ]]; then
  log "another daily/stale scheduler run is active — skipping"
  exit 0
elif [[ "$lock_rc" -ne 0 ]]; then
  log "unable to acquire scheduler lock (rc=$lock_rc)"
  exit 1
fi
scheduler_lock_install_traps

_is_gate_group() {
  local g="$1" x
  for x in $GATE_GROUP_LIST; do
    [[ "$x" == "$g" ]] && return 0
  done
  return 1
}

log "==== daily pipeline start $(date '+%Y-%m-%d %H:%M:%S') trade_date=${TRADE_DATE:-today} ===="
failed_groups=()
gate_failed=()
soft_failed=()
# parallel arrays: group name → OK|FAILED|…  (bash 3.2 compatible, no assoc arrays)
summary_names=()
summary_status=()

for g in $GROUP_LIST; do
  log "--- group: $g ---"
  if "$CNE" run daily --group "$g" --config "$CONFIG" ${DATE_ARGS[@]+"${DATE_ARGS[@]}"} >>"$LOG" 2>&1; then
    log "group $g OK"
    summary_names+=("$g")
    summary_status+=("OK")
  else
    log "group $g FAILED (see $LOG)"
    failed_groups+=("$g")
    summary_names+=("$g")
    summary_status+=("FAILED")
    if _is_gate_group "$g"; then
      gate_failed+=("$g")
    else
      soft_failed+=("$g")
    fi
  fi
done

# Second attempt at whatever is still behind, before the health check so a
# successful repair does not page anyone. `cne status --datasets` exits 1 when
# something is STALE, which makes it the probe: on a clean day this costs one
# directory walk and skips the sleep entirely.
stale_retry_status="skipped"
if [[ "$STALE_RETRY" == "1" ]]; then
  log "--- stale probe ---"
  if "$CNE" status --datasets --config "$CONFIG" >>"$LOG" 2>&1; then
    log "nothing stale — no retry needed"
    stale_retry_status="not needed"
  else
    log "something is stale; waiting ${STALE_RETRY_DELAY_SEC}s before re-fetching"
    sleep "$STALE_RETRY_DELAY_SEC"
    log "--- stale retry ---"
    if "$CNE" run daily --stale-only --config "$CONFIG" \
      ${DATE_ARGS[@]+"${DATE_ARGS[@]}"} >>"$LOG" 2>&1; then
      log "stale retry OK"
      stale_retry_status="OK"
    else
      log "stale retry FAILED (see $LOG)"
      stale_retry_status="FAILED"
      # Soft by construction: the groups already had their turn, and a source
      # still down after the wait is not something this run can fix.
      soft_failed+=("stale-retry")
    fi
  fi
fi

# Health check (fires desktop notification on problems) and backup run
# regardless of group outcomes so we always get a status signal and a snapshot.
log "--- health check ---"
if ! "$REPO_ROOT/scripts/health_notify.sh" >>"$LOG" 2>&1; then
  log "health check reported problems"
fi

# Availability evidence is non-blocking while it accumulates: a red public
# source is the observation, not a reason to discard an otherwise valid core
# revision.  `cne sources slo` still writes fail-closed incidents/report state,
# and release/acceptance gates invoke it separately with `--enforce`.
if [[ "$SOURCE_HEALTH" == "1" ]]; then
  log "--- source health (vantage=${SOURCE_VANTAGE}) ---"
  if ! "$CNE" sources probe --config "$CONFIG" --vantage "$SOURCE_VANTAGE" >>"$LOG" 2>&1; then
    log "source probe command FAILED (non-fatal)"
  fi
  if ! "$CNE" sources slo --config "$CONFIG" >>"$LOG" 2>&1; then
    log "source SLO reporting FAILED (non-fatal)"
  fi
fi

# Persist the current consecutive-day evidence after every scheduled run.  It
# is intentionally not enforced here: the first 19 clean days must not make a
# healthy ingestion job exit non-zero.  Release governance enforces day 20.
log "--- stability evidence ---"
if ! "$CNE" stability --config "$CONFIG" --days 20 >>"$LOG" 2>&1; then
  log "stability reporting FAILED (non-fatal)"
fi

log "--- backup ---"
if ! "$REPO_ROOT/scripts/backup_meta.sh" >>"$LOG" 2>&1; then
  log "backup FAILED"
fi

# Staging is per-run scratch; once a run succeeded and compact merged it into
# curated it is pure duplication. Nothing ran this automatically before, so it
# grew to ~60% of the curated layer. `cne clean` only drops staging whose run
# succeeded *and* compacted (or is an unknown orphan past retention) — the
# staging of a failed run is resumable state and is always kept.
log "--- clean staging ---"
if ! "$CNE" clean --config "$CONFIG" >>"$LOG" 2>&1; then
  log "staging cleanup FAILED (non-fatal)"
fi

log "---- group summary (gate=${GATE_GROUP_LIST}) ----"
i=0
while [[ $i -lt ${#summary_names[@]} ]]; do
  g="${summary_names[$i]}"
  st="${summary_status[$i]}"
  kind="soft"
  _is_gate_group "$g" && kind="gate"
  log "  ${g}: ${st}  [${kind}]"
  i=$((i + 1))
done
log "  stale-retry: ${stale_retry_status}"

if [[ ${#gate_failed[@]} -gt 0 ]]; then
  log "==== daily pipeline DONE — GATE FAILED: ${gate_failed[*]} (soft also: ${soft_failed[*]:-none}) ===="
  exit 1
fi
if [[ ${#soft_failed[@]} -gt 0 ]]; then
  if [[ "$SOFT_FAIL_OK" == "1" ]]; then
    log "==== daily pipeline DONE — gate OK, EM/soft FAILED (warn-only): ${soft_failed[*]} ===="
    exit 0
  fi
  log "==== daily pipeline DONE — gate OK, EM/soft FAILED: ${soft_failed[*]} ===="
  exit 1
fi
log "==== daily pipeline DONE ok ===="
