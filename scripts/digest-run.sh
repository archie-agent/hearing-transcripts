#!/bin/bash
# Twice-weekly hearing digest pipeline run.
# Invoked by launchd (com.clawdbot.hearing-digest).

set -euo pipefail

PROJECT_DIR="/Users/agent/code/hearing-transcripts"
VENV="$PROJECT_DIR/.venv"
LOG_DIR="/Users/agent/data/hearing-transcripts/logs"

mkdir -p "$LOG_DIR"

# Activate venv
source "$VENV/bin/activate"

cd "$PROJECT_DIR"
python3 digest.py 2>&1 | tee "$LOG_DIR/digest-$(date +%Y-%m-%d).log"
