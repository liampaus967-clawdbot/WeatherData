#!/bin/bash
#
# Wind Tile Pipeline Runner
#
# Generates wind UV tiles for custom WebGL particle rendering.
# Tiles are encoded with R=U, G=V, B=magnitude.
#
# Usage:
#   ./run_wind_pipeline.sh              # Latest data, F00-F12
#   ./run_wind_pipeline.sh --fxx 0-6    # Latest data, F00-F06
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="${LOG_DIR:-/var/log/weather-pipeline}"
VENV_DIR="${PROJECT_ROOT}/venv"

# Load environment
if [[ -f "$PROJECT_ROOT/.env" ]]; then
    source "$PROJECT_ROOT/.env"
fi

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Timestamp for log file
DATE_UTC=$(date -u +%Y%m%d)
HOUR_UTC=$(date -u +%H)
LOG_FILE="${LOG_DIR}/wind_tiles_${DATE_UTC}_${HOUR_UTC}00.log"

log() {
    local timestamp=$(date -u +"%Y-%m-%d %H:%M:%S UTC")
    echo "[${timestamp}] $*" | tee -a "$LOG_FILE"
}

log "=========================================="
log "Wind Tile Pipeline Starting"
log "=========================================="

# Activate virtual environment
if [[ -d "$VENV_DIR" ]]; then
    source "$VENV_DIR/bin/activate"
    log "Activated virtual environment"
else
    log "WARNING: Virtual environment not found at $VENV_DIR"
    log "Using system Python"
fi

# Run the wind tile generator
log "Running wind tile generator..."

python3 "$SCRIPT_DIR/generate_wind_tiles.py" \
    --latest \
    --fxx "${FORECAST_HOURS:-0-12}" \
    --bucket "${S3_BUCKET:-driftwise-weather-data}" \
    "$@" \
    2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

if [[ $EXIT_CODE -eq 0 ]]; then
    log "=========================================="
    log "Wind tile pipeline completed successfully!"
    log "=========================================="
else
    log "=========================================="
    log "Wind tile pipeline FAILED with exit code $EXIT_CODE"
    log "=========================================="
fi

exit $EXIT_CODE
