#!/bin/bash
# Daily hearing transcript pipeline run.
# Invoked by launchd (com.clawdbot.hearing-transcripts).

set -uo pipefail

NOTIFY="/Users/agent/bin/notify-cron-failure.sh"
PROJECT_DIR="/Users/agent/code/hearing-transcripts"
VENV="$PROJECT_DIR/.venv"
LOG_DIR="/Users/agent/data/hearing-transcripts/logs"

mkdir -p "$LOG_DIR"

# Retain logs for 30 days
find "$LOG_DIR" -name "*.log" -mtime +30 -delete 2>/dev/null || true

# Activate venv
if [[ ! -f "$VENV/bin/activate" ]]; then
    "$NOTIFY" hearing-transcripts "venv not found at $VENV" 2>/dev/null || true
    exit 1
fi
source "$VENV/bin/activate"

# Run the pipeline: 3-day lookback, default tier (<=2).
# --workers 1: serialized to avoid C-SPAN WAF captcha storms during unattended runs.
cd "$PROJECT_DIR"
LOG_FILE="$LOG_DIR/$(date +%Y-%m-%d).log"

if [[ "${QUEUE_READ_ENABLED:-0}" == "1" ]]; then
    # Producer/worker topology (north-star cutover path)
    python3 run.py --enqueue-only --days 3 --workers 1 2>&1 | tee "$LOG_FILE"
    RC_ENQUEUE="${PIPESTATUS[0]}"
    if [[ "$RC_ENQUEUE" -ne 0 ]]; then
        RC="$RC_ENQUEUE"
    else
        DRAIN_MAX_TASKS="${DRAIN_MAX_TASKS:-60}"
        LEASE_SECONDS="${LEASE_SECONDS:-900}"
        python3 run.py --drain-only --workers 1 --max-tasks "$DRAIN_MAX_TASKS" --lease-seconds "$LEASE_SECONDS" 2>&1 | tee -a "$LOG_FILE"
        RC="${PIPESTATUS[0]}"
    fi
else
    # Monolith fallback (rollback-safe default)
    python3 run.py --days 3 --workers 1 2>&1 | tee "$LOG_FILE"
    RC="${PIPESTATUS[0]}"
fi

if [ "$RC" -ne 0 ]; then
    # Extract last error line from log for the notification
    ERR=$(grep -i -E 'error|exception|traceback' "$LOG_FILE" 2>/dev/null | tail -1 | head -c 120)
    "$NOTIFY" hearing-transcripts "${ERR:-exit code $RC}" 2>/dev/null || true
fi
exit "$RC"
