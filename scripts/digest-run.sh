#!/bin/bash
# Twice-weekly hearing digest pipeline run.
# Invoked by launchd (com.clawdbot.hearing-digest).

set -uo pipefail

NOTIFY="/Users/agent/bin/notify-cron-failure.sh"
PROJECT_DIR="/Users/agent/code/hearing-transcripts"
VENV="$PROJECT_DIR/.venv"
LOG_DIR="/Users/agent/data/hearing-transcripts/logs"

mkdir -p "$LOG_DIR"

# Activate venv
if [[ ! -f "$VENV/bin/activate" ]]; then
    "$NOTIFY" hearing-digest "venv not found at $VENV" 2>/dev/null || true
    exit 1
fi
source "$VENV/bin/activate"

cd "$PROJECT_DIR"
LOG_FILE="$LOG_DIR/digest-$(date +%Y-%m-%d).log"
if [[ "${DIGEST_USE_LEGACY_INDEX:-0}" == "1" ]]; then
    # Rollback path: scan transcripts/index.json directly.
    python3 digest.py 2>&1 | tee "$LOG_FILE"
else
    # Default path: consume transcript_published events from delivery outbox.
    OUTBOX_MAX_EVENTS="${OUTBOX_MAX_EVENTS:-20}"
    OUTBOX_LEASE_SECONDS="${OUTBOX_LEASE_SECONDS:-900}"
    OUTBOX_WORKER_ID="${OUTBOX_WORKER_ID:-digest-cron}"
    OUTBOX_DIGEST_ENABLED=1 python3 digest.py \
        --consume-outbox \
        --max-events "$OUTBOX_MAX_EVENTS" \
        --lease-seconds "$OUTBOX_LEASE_SECONDS" \
        --worker-id "$OUTBOX_WORKER_ID" \
        2>&1 | tee "$LOG_FILE"
fi
RC="${PIPESTATUS[0]}"

if [ "$RC" -ne 0 ]; then
    ERR=$(grep -i -E 'error|exception|traceback' "$LOG_FILE" 2>/dev/null | tail -1 | head -c 120)
    "$NOTIFY" hearing-digest "${ERR:-exit code $RC}" 2>/dev/null || true
fi
exit "$RC"
