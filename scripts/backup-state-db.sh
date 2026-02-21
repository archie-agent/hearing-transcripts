#!/usr/bin/env bash
# Create a point-in-time SQLite backup before queue migrations/cutovers.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DB_PATH="${1:-$PROJECT_DIR/data/state.db}"
BACKUP_DIR="${2:-$PROJECT_DIR/data/backups}"

if [[ ! -f "$DB_PATH" ]]; then
  echo "state db not found: $DB_PATH" >&2
  exit 1
fi

mkdir -p "$BACKUP_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="$BACKUP_DIR/state.db.$STAMP.bak"

sqlite3 "$DB_PATH" ".backup '$DEST'"
echo "backup created: $DEST"
