#!/usr/bin/env bash
# B3 — Daily snapshot of the metadata that cannot be rebuilt from the curated
# lake: the manifest DB (run history) and meta/state (per-source incremental
# watermarks, the PIT-critical bit). Curated parquet, adj_factors_cache, and
# runtime locks are deliberately excluded — they are large and reproducible.
#
# Usage: scripts/backup_meta.sh [DATA_ROOT] [BACKUP_DIR] [RETENTION_DAYS]
# Defaults resolve to the repo's ./data/cn-market-lake lake.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${1:-${CML_DATA_ROOT:-$REPO_ROOT/data/cn-market-lake}}"
BACKUP_DIR="${2:-${CML_BACKUP_DIR:-$DATA_ROOT/backups}}"
RETENTION_DAYS="${3:-${CML_BACKUP_RETENTION_DAYS:-14}}"

META_DIR="$DATA_ROOT/meta"
if [[ ! -d "$META_DIR" ]]; then
  echo "backup_meta: meta dir not found: $META_DIR" >&2
  exit 1
fi

mkdir -p "$BACKUP_DIR"
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

# Assemble the archive: consistent manifest snapshot + state/quality dirs.
TAR_ARGS=()
[[ -f "$TMP_DIR/manifest.db" ]] && TAR_ARGS+=(-C "$TMP_DIR" manifest.db)
for sub in state quality; do
  [[ -e "$META_DIR/$sub" ]] && TAR_ARGS+=(-C "$META_DIR" "$sub")
done
if [[ ${#TAR_ARGS[@]} -eq 0 ]]; then
  echo "backup_meta: nothing to back up under $META_DIR" >&2
  exit 1
fi
tar -czf "$ARCHIVE" "${TAR_ARGS[@]}"

# Rotate: drop archives older than RETENTION_DAYS.
find "$BACKUP_DIR" -name 'meta-*.tar.gz' -type f -mtime "+$RETENTION_DAYS" -delete 2>/dev/null || true

SIZE="$(du -h "$ARCHIVE" | cut -f1)"
echo "backup_meta: wrote $ARCHIVE ($SIZE); retention ${RETENTION_DAYS}d"
