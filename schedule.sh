#!/usr/bin/env bash
# schedule.sh -- run the hearing-transcript pipeline once and log output.
#
# Usage (manual):
#   ./schedule.sh
#
# The script activates the project venv, loads .env, runs the pipeline
# looking back 2 days (overlap for safety), and writes a dated log file.

set -euo pipefail

# ── Resolve paths relative to this script, not the caller's cwd ──────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Activate virtual environment ─────────────────────────────────────────
source .venv/bin/activate

# ── Load environment variables (.env) ────────────────────────────────────
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

# ── Ensure log directory exists ──────────────────────────────────────────
LOG_DIR="$SCRIPT_DIR/data/logs"
mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/run-$(date +%Y-%m-%d).log"

# ── Run the pipeline ────────────────────────────────────────────────────
echo "=== hearing-transcript run started at $(date -u '+%Y-%m-%dT%H:%M:%SZ') ===" | tee -a "$LOG_FILE"
python run.py --days 2 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}
echo "=== run finished at $(date -u '+%Y-%m-%dT%H:%M:%SZ') exit=$EXIT_CODE ===" | tee -a "$LOG_FILE"

exit "$EXIT_CODE"

# Deployed launchd plists:
#   ~/Library/LaunchAgents/com.clawdbot.hearing-transcripts.plist  (daily pipeline)
#   ~/Library/LaunchAgents/com.clawdbot.hearing-digest.plist       (twice-weekly digest)
