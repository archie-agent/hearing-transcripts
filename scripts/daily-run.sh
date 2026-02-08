#!/bin/bash
# Daily hearing transcript pipeline run.
# Invoked by launchd (com.clawdbot.hearing-transcripts).

set -euo pipefail

PROJECT_DIR="/Users/agent/code/hearing-transcripts"
VENV="$PROJECT_DIR/.venv"
LOG_DIR="/Users/agent/data/hearing-transcripts/logs"

mkdir -p "$LOG_DIR"

# Activate venv
source "$VENV/bin/activate"

# Run the pipeline: 3-day lookback, default tier (<=2), 3 workers
cd "$PROJECT_DIR"
python3 run.py --days 3 --workers 1 2>&1 | tee "$LOG_DIR/$(date +%Y-%m-%d).log"
