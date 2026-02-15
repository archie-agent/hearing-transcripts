#!/bin/bash
# Daily hearing transcript pipeline run.
# Invoked by launchd (com.clawdbot.hearing-transcripts).

set -euo pipefail

PROJECT_DIR="/Users/agent/code/hearing-transcripts"
VENV="$PROJECT_DIR/.venv"
LOG_DIR="/Users/agent/data/hearing-transcripts/logs"

mkdir -p "$LOG_DIR"

# Retain logs for 30 days
find "$LOG_DIR" -name "*.log" -mtime +30 -delete 2>/dev/null || true

# Activate venv
if [[ ! -f "$VENV/bin/activate" ]]; then
    echo "ERROR: venv not found at $VENV" >&2
    exit 1
fi
source "$VENV/bin/activate"

# Run the pipeline: 3-day lookback, default tier (<=2).
# --workers 1: serialized to avoid C-SPAN WAF captcha storms during unattended runs.
cd "$PROJECT_DIR"
python3 run.py --days 3 --workers 1 2>&1 | tee "$LOG_DIR/$(date +%Y-%m-%d).log"
exit "${PIPESTATUS[0]}"
