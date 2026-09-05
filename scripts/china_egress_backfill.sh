#!/usr/bin/env bash
# One-shot mainland-egress backfills that fail from overseas IPs:
#   1) sector_bars --force  (EastMoney push2his → replace hybrid/TDX OHLC)
#   2) trading_status       (baostock ST history; resumable)
#
# Run ON a China VPS (or behind a working mainland egress). Do not open a
# public HTTP proxy; prefer running this script on the VPS itself.
#
# Usage:
#   scripts/china_egress_backfill.sh
#   scripts/china_egress_backfill.sh --sector-only
#   scripts/china_egress_backfill.sh --st-only
# Env: CNE_CONFIG, CNE_LOG_DIR
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# CNE_BIN is the documented override; the older CNE spelling still works.
CNE="${CNE_BIN:-${CNE:-$REPO_ROOT/.venv/bin/cne}}"
CONFIG="${CNE_CONFIG:-$REPO_ROOT/configs/cnequity.toml}"
LOG_DIR="${CNE_LOG_DIR:-$REPO_ROOT/data/cnequity/logs}"
mkdir -p "$LOG_DIR"
STAMP="$(date +%Y%m%d-%H%M%S)"
LOG="$LOG_DIR/china-egress-backfill-$STAMP.log"

DO_SECTOR=1
DO_ST=1
for arg in "$@"; do
  case "$arg" in
    --sector-only) DO_ST=0 ;;
    --st-only) DO_SECTOR=0 ;;
    -h|--help)
      sed -n '2,16p' "$0"
      exit 0
      ;;
    *)
      echo "unknown arg: $arg" >&2
      exit 2
      ;;
  esac
done

if [[ ! -x "$CNE" ]]; then
  echo "missing cne binary at $CNE — run: cd $REPO_ROOT && uv sync" >&2
  exit 1
fi

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"; }

log "==== china egress backfill start ===="
log "config=$CONFIG log=$LOG"
log "cwd=$REPO_ROOT"

cd "$REPO_ROOT"
export PYTHONUNBUFFERED=1

if [[ "$DO_SECTOR" -eq 1 ]]; then
  log "--- sector_bars --force (pure EM kline) ---"
  if "$CNE" backfill sector_bars --config "$CONFIG" --force 2>&1 | tee -a "$LOG"; then
    log "sector_bars OK"
  else
    log "sector_bars FAILED (exit $?); see $LOG"
    exit 1
  fi
fi

if [[ "$DO_ST" -eq 1 ]]; then
  log "--- trading_status ST backfill (baostock, resumable) ---"
  if "$CNE" backfill trading_status --config "$CONFIG" 2>&1 | tee -a "$LOG"; then
    log "trading_status OK"
  else
    log "trading_status FAILED (exit $?); re-run with --st-only to resume"
    exit 1
  fi
fi

log "==== china egress backfill done ===="
log "checkpoint hints:"
log "  meta/state/sector_bars_backfill.json"
log "  meta/state/trading_status_st_backfill.json (swept symbols)"
