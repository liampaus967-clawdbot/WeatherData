#!/bin/bash
#
# Cron Wrapper Script for Weather Pipeline
#
# This script is called by cron and sets up the proper environment
# before running the main pipeline script.
#
# Part of TICKET-011: Configure Cron Job for Hourly Execution
#

set -euo pipefail

# Script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source environment variables first (may override PROJECT_ROOT)
if [[ -f "$SCRIPT_DIR/weather-pipeline.env" ]]; then
    source "$SCRIPT_DIR/weather-pipeline.env"
fi

# Calculate PROJECT_ROOT if not set in env file
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../.." && pwd)}"

# Ensure log directory exists
mkdir -p "${LOG_DIR:-/var/log/weather-pipeline}"

# Log start time
echo "=========================================="
echo "Cron job started at $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "=========================================="

# Check if another instance is running (prevent overlap)
LOCKFILE="/tmp/weather-pipeline.lock"
if [[ -f "$LOCKFILE" ]]; then
    # Check if the process is still running
    if kill -0 "$(cat "$LOCKFILE")" 2>/dev/null; then
        echo "ERROR: Pipeline already running (PID: $(cat "$LOCKFILE"))"
        exit 1
    else
        echo "WARN: Stale lock file found, removing..."
        rm -f "$LOCKFILE"
    fi
fi

# Create lock file
echo $$ > "$LOCKFILE"
trap "rm -f $LOCKFILE" EXIT

# Run the pipeline
"$PROJECT_ROOT/scripts/pipeline.sh" \
    --enable-s3 \
    --s3-bucket "${S3_BUCKET:-sat-data-container}" \
    --priority "${PRIORITY:-1}" \
    --zoom "${ZOOM_LEVELS:-0-8}" \
    --work-dir "${WORK_DIR:-/tmp/weather-pipeline}" \
    --log-dir "${LOG_DIR:-/var/log/weather-pipeline}"

EXIT_CODE=$?

# Log completion
echo "=========================================="
echo "Cron job finished at $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "Exit code: $EXIT_CODE"
echo "=========================================="

exit $EXIT_CODE
