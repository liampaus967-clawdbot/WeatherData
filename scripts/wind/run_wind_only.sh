#!/bin/bash
#
# Wind-Only Pipeline - Downloads HRRR and extracts wind tiles
#
# Usage:
#   ./run_wind_only.sh                    # Latest cycle, no S3
#   ./run_wind_only.sh --enable-s3        # Latest cycle, upload to S3
#   ./run_wind_only.sh --date 2026-02-24 --cycle 15 --enable-s3
#

set -euo pipefail

# Defaults
WORK_DIR="${WORK_DIR:-/tmp/wind-pipeline}"
S3_BUCKET="${S3_BUCKET:-driftwise-weather-data}"
ENABLE_S3="${ENABLE_S3:-false}"
FORECAST_HOURS="${FORECAST_HOURS:-0-12}"
DATE=""
CYCLE=""
VERBOSE=""

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --date) DATE="$2"; shift 2 ;;
        --cycle) CYCLE="$2"; shift 2 ;;
        --forecast-hours) FORECAST_HOURS="$2"; shift 2 ;;
        --enable-s3) ENABLE_S3=true; shift ;;
        --s3-bucket) S3_BUCKET="$2"; ENABLE_S3=true; shift 2 ;;
        --work-dir) WORK_DIR="$2"; shift 2 ;;
        --verbose|-v) VERBOSE="--verbose"; shift ;;
        --help) echo "Usage: $0 [--date YYYY-MM-DD] [--cycle HH] [--enable-s3] [--s3-bucket NAME]"; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Calculate latest available cycle if not specified
if [[ -z "$DATE" ]] || [[ -z "$CYCLE" ]]; then
    # HRRR available ~2-3 hours after model run
    LATEST=$(date -u -d "3 hours ago" +"%Y-%m-%d %H" 2>/dev/null || date -u -v-3H +"%Y-%m-%d %H")
    DATE="${DATE:-$(echo $LATEST | cut -d' ' -f1)}"
    CYCLE="${CYCLE:-$(echo $LATEST | cut -d' ' -f2)}"
fi

DOWNLOAD_DIR="$WORK_DIR/downloads"
WIND_DIR="$WORK_DIR/wind-tiles"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

mkdir -p "$DOWNLOAD_DIR" "$WIND_DIR"

echo "=============================================="
echo "Wind Tile Pipeline"
echo "=============================================="
echo "Date: $DATE"
echo "Cycle: ${CYCLE}Z"
echo "Forecast Hours: $FORECAST_HOURS"
echo "S3 Upload: $ENABLE_S3"
[[ "$ENABLE_S3" == "true" ]] && echo "S3 Bucket: $S3_BUCKET"
echo "Work Dir: $WORK_DIR"
echo "=============================================="

# Step 1: Download HRRR wind data (full GRIB, same as main pipeline)
echo ""
echo "==> Step 1: Downloading HRRR GRIB2 data..."
docker run --rm \
    --user $(id -u):$(id -g) \
    -e HOME=/tmp \
    -e HERBIE_HOME=/data/output \
    -v "$DOWNLOAD_DIR:/data/output" \
    -v "$PROJECT_ROOT:/app" \
    weather-processor:latest \
    python3 /app/scripts/hrrr/download_hrrr.py \
    --date "$DATE" \
    --cycle="$CYCLE" \
    --fxx "$FORECAST_HOURS" \
    --variables all \
    --output /data/output \
    --keep-local

# Check files downloaded (can be .grib2 or .nc depending on download mode)
GRIB_COUNT=$(find "$DOWNLOAD_DIR" -name "*.grib2" | wc -l)
NC_COUNT=$(find "$DOWNLOAD_DIR" -name "*.nc" | wc -l)
TOTAL_COUNT=$((GRIB_COUNT + NC_COUNT))
echo "Downloaded $GRIB_COUNT GRIB2 files, $NC_COUNT NetCDF files"

if [[ "$TOTAL_COUNT" -eq 0 ]]; then
    echo "ERROR: No data files downloaded!"
    exit 1
fi

# Step 2: Extract wind tiles
echo ""
echo "==> Step 2: Extracting wind tiles..."

S3_ARG=""
if [[ "$ENABLE_S3" == "true" ]]; then
    S3_ARG="--s3-bucket $S3_BUCKET"
fi

# Mount AWS credentials directory if it exists (for S3 uploads)
AWS_MOUNT=""
if [[ -d "$HOME/.aws" ]]; then
    AWS_MOUNT="-v $HOME/.aws:/root/.aws:ro"
fi

docker run --rm \
    --user 0:0 \
    -e HOME=/root \
    -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}" \
    -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}" \
    -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
    -e AWS_PROFILE="${AWS_PROFILE:-default}" \
    $AWS_MOUNT \
    -v "$DOWNLOAD_DIR:/data/input" \
    -v "$WIND_DIR:/data/output" \
    -v "$PROJECT_ROOT:/app" \
    weather-processor:latest \
    python3 /app/scripts/wind/extract_wind_from_grib.py \
    --input /data/input \
    --output /data/output \
    $VERBOSE $S3_ARG

# Results
WIND_COUNT=$(find "$WIND_DIR" -name "*.png" | wc -l)
echo ""
echo "=============================================="
echo "Complete! Generated $WIND_COUNT wind tiles"
echo "Output: $WIND_DIR"
[[ "$ENABLE_S3" == "true" ]] && echo "Uploaded to: s3://$S3_BUCKET/wind-tiles/"
echo "=============================================="
