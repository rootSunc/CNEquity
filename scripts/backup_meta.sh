#!/usr/bin/env bash
# B3 — Daily snapshot of the metadata that cannot be rebuilt from the curated
# lake: the manifest DB, incremental state, revision/source receipts, quality
# findings and the operational acceptance evidence. Curated parquet,
# revision generations (`meta/revisions/data`), adj_factors_cache and runtime
# locks are deliberately excluded — they are large or reproducible. Portable
# research snapshots cover curated data.
#
# Usage: scripts/backup_meta.sh [DATA_ROOT] [BACKUP_DIR] [RETENTION_DAYS] [RETENTION_COUNT]
# Defaults resolve to the repo's ./data/cnequity lake.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT_INPUT="${1:-${CNE_DATA_ROOT:-$REPO_ROOT/data/cnequity}}"
RETENTION_DAYS="${3:-${CNE_BACKUP_RETENTION_DAYS:-14}}"
# Age alone is not a bound when the caller runs more than once a day. 30 covers
# two weeks of one-a-day with slack; 0 disables the count cap.
RETENTION_COUNT="${4:-${CNE_BACKUP_RETENTION_COUNT:-30}}"

META_DIR="$DATA_ROOT_INPUT/meta"
if [[ ! -d "$META_DIR" ]]; then
  echo "backup_meta: meta dir not found: $META_DIR" >&2
  exit 1
fi
DATA_ROOT="$(cd "$DATA_ROOT_INPUT" && pwd)"
META_DIR="$DATA_ROOT/meta"
BACKUP_DIR_INPUT="${2:-${CNE_BACKUP_DIR:-$DATA_ROOT/backups}}"

mkdir -p "$BACKUP_DIR_INPUT"
BACKUP_DIR="$(cd "$BACKUP_DIR_INPUT" && pwd)"
STAMP="$(date +%Y%m%d-%H%M%S)"
ARCHIVE="$BACKUP_DIR/meta-$STAMP.tar.gz"

# Snapshot the SQLite manifest via the backup API so an in-flight run's
# writes can't produce a torn copy; fall back to a plain file copy if the
# sqlite3 CLI is unavailable.
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
MANIFEST="$META_DIR/manifest.db"
if [[ -f "$MANIFEST" ]]; then
  if command -v sqlite3 >/dev/null 2>&1; then
    sqlite3 "$MANIFEST" ".backup '$TMP_DIR/manifest.db'"
  else
    cp "$MANIFEST" "$TMP_DIR/manifest.db"
  fi
fi

# Assemble the archive: consistent manifest snapshot plus non-reconstructable
# metadata and accumulated acceptance evidence. Missing optional directories
# are harmless on a newly initialized lake.
#
# `revisions/` is included for its RECEIPTS (`{dataset}/*.json`, a few MB) —
# those name the generation a dataset currently points at and cannot be
# rebuilt. `revisions/data/` is excluded: it holds the copy-on-write
# generations themselves, which are byte-for-byte reconstructable from
# `curated/` and are the overwhelming majority of the tree (16 GB vs ~120 MB
# of receipts on a two-year lake). Archiving them turned a ~1 GB/day
# generation growth into ~14 GB/day of rotating tarballs.
TAR_ARGS=()
[[ -f "$TMP_DIR/manifest.db" ]] && TAR_ARGS+=(-C "$TMP_DIR" manifest.db)
for sub in state quality revisions source_snapshots source_health stability; do
  [[ -e "$META_DIR/$sub" ]] && TAR_ARGS+=(-C "$META_DIR" "$sub")
done
if [[ ${#TAR_ARGS[@]} -eq 0 ]]; then
  echo "backup_meta: nothing to back up under $META_DIR" >&2
  exit 1
fi
tar -czf "$ARCHIVE" --exclude 'revisions/data' "${TAR_ARGS[@]}"

# Rotate by age *and* by count, whichever is stricter.
#
# Age alone assumed one archive a day. Nothing enforced that: any caller that
# runs more often keeps every copy for the whole window, and a 45 MB archive
# taken 262 times in one day is 11.8 GB that age-based rotation will not touch
# for a fortnight. On the reference lake `backups/` had reached 14 GB — 35% of
# the lake, larger than `curated/` itself.
find "$BACKUP_DIR" -name 'meta-*.tar.gz' -type f -mtime "+$RETENTION_DAYS" -delete 2>/dev/null || true
if [[ "$RETENTION_COUNT" -gt 0 ]]; then
  # Newest first, skip the ones we keep, delete the rest. `ls -t` is safe here:
  # the names are our own timestamped pattern, no spaces or newlines.
  # shellcheck disable=SC2012
  ls -t "$BACKUP_DIR"/meta-*.tar.gz 2>/dev/null \
    | tail -n "+$((RETENTION_COUNT + 1))" \
    | while IFS= read -r stale; do rm -f -- "$stale"; done
fi

SIZE="$(du -h "$ARCHIVE" | cut -f1)"
KEPT="$(ls -1 "$BACKUP_DIR"/meta-*.tar.gz 2>/dev/null | wc -l | tr -d ' ')"
echo "backup_meta: wrote $ARCHIVE ($SIZE); retention ${RETENTION_DAYS}d/${RETENTION_COUNT} archives; ${KEPT} kept"
