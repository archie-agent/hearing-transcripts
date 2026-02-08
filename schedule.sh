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

# =========================================================================
# macOS launchd plist -- scheduled daily execution
# =========================================================================
#
# Save the XML below to:
#   ~/Library/LaunchAgents/com.hearing-transcripts.daily.plist
#
# Then load it with:
#   launchctl load ~/Library/LaunchAgents/com.hearing-transcripts.daily.plist
#
# To unload:
#   launchctl unload ~/Library/LaunchAgents/com.hearing-transcripts.daily.plist
#
# To verify it is registered:
#   launchctl list | grep hearing-transcripts
#
# NOTE: Replace __FULL_PATH__ with the absolute path to this repository,
# e.g. /Users/yourname/projects/hearing-transcripts
#
# <?xml version="1.0" encoding="UTF-8"?>
# <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
#   "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
# <plist version="1.0">
# <dict>
#     <key>Label</key>
#     <string>com.hearing-transcripts.daily</string>
#
#     <key>ProgramArguments</key>
#     <array>
#         <string>/bin/bash</string>
#         <string>__FULL_PATH__/schedule.sh</string>
#     </array>
#
#     <key>WorkingDirectory</key>
#     <string>__FULL_PATH__</string>
#
#     <!-- 7:00 AM ET (12:00 UTC during EST, 11:00 UTC during EDT) -->
#     <key>StartCalendarInterval</key>
#     <dict>
#         <key>Hour</key>
#         <integer>12</integer>
#         <key>Minute</key>
#         <integer>0</integer>
#     </dict>
#
#     <key>StandardOutPath</key>
#     <string>__FULL_PATH__/data/logs/launchd-stdout.log</string>
#
#     <key>StandardErrorPath</key>
#     <string>__FULL_PATH__/data/logs/launchd-stderr.log</string>
#
#     <!-- Run immediately if the Mac was asleep at the scheduled time -->
#     <key>MissedTaskPolicy</key>
#     <string>Aggressive</string>
# </dict>
# </plist>
