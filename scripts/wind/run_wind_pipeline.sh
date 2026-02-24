#!/bin/bash
#
# Wind Tile Pipeline Runner (Docker)
#
# Generates wind UV tiles for custom WebGL particle rendering.
# Tiles are encoded with R=U, G=V, B=magnitude.
#
# Usage:
#   ./run_wind_pipeline.sh              # Latest data, F00-F12
#   ./run_wind_pipeline.sh --fxx 0-6    # Latest data, F00-F06
#   ./run_wind_pipeline.sh --dry-run    # Test without uploading
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="${LOG_DIR:-/var/log/weather-pipeline}"
DOCKER_IMAGE="${DOCKER_IMAGE:-weather-pipeline:latest}"

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

# Check if Docker image exists, build if needed
if ! docker image inspect "$DOCKER_IMAGE" &>/dev/null; then
    log "Docker image not found. Building..."
    cd "$PROJECT_ROOT/docker"
    docker build -t "$DOCKER_IMAGE" .
    cd "$PROJECT_ROOT"
    log "Docker image built successfully"
fi

# Prepare Docker run command
DOCKER_CMD="docker run --rm \
    -v $PROJECT_ROOT:/app \
    -v /tmp/weather-pipeline:/tmp/weather-pipeline \
    -e S3_BUCKET=${S3_BUCKET:-driftwise-weather-data} \
    -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-} \
    -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-} \
    -e AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-east-1} \
    $DOCKER_IMAGE"

# If AWS credentials not in env, try to mount .aws directory
if [[ -z "${AWS_ACCESS_KEY_ID:-}" ]] && [[ -d "$HOME/.aws" ]]; then
    DOCKER_CMD="docker run --rm \
        -v $PROJECT_ROOT:/app \
        -v /tmp/weather-pipeline:/tmp/weather-pipeline \
        -v $HOME/.aws:/root/.aws:ro \
        -e S3_BUCKET=${S3_BUCKET:-driftwise-weather-data} \
        -e AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-east-1} \
        -e AWS_PROFILE=${AWS_PROFILE:-default} \
        $DOCKER_IMAGE"
fi

log "Running wind tile generator via Docker..."

# Run the wind tile generator
$DOCKER_CMD python3 /app/scripts/wind/generate_wind_tiles.py \
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
