#!/bin/bash
#
# Wind Tile Pipeline (Python only, no Docker)
#
# Usage:
#   ./run_wind_python.sh                      # Latest cycle, F00-F12, no S3
#   ./run_wind_python.sh --enable-s3          # Latest cycle, upload to S3
#   ./run_wind_python.sh --forecast-hours 0-6 --enable-s3
#

set -euo pipefail

# Defaults
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORK_DIR="${WORK_DIR:-/tmp/wind-pipeline}"
S3_BUCKET="${S3_BUCKET:-driftwise-weather-data}"
ENABLE_S3=false
FORECAST_HOURS="0-12"
VERBOSE=""

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --forecast-hours) FORECAST_HOURS="$2"; shift 2 ;;
        --enable-s3) ENABLE_S3=true; shift ;;
        --s3-bucket) S3_BUCKET="$2"; ENABLE_S3=true; shift 2 ;;
        --work-dir) WORK_DIR="$2"; shift 2 ;;
        --verbose|-v) VERBOSE="-v"; shift ;;
        --help) echo "Usage: $0 [--forecast-hours 0-12] [--enable-s3] [--s3-bucket NAME] [--verbose]"; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

DOWNLOAD_DIR="$WORK_DIR/downloads"
WIND_DIR="$WORK_DIR/wind-tiles"

mkdir -p "$DOWNLOAD_DIR" "$WIND_DIR"

echo "=============================================="
echo "Wind Tile Pipeline (Python)"
echo "=============================================="
echo "Forecast Hours: $FORECAST_HOURS"
echo "S3 Upload: $ENABLE_S3"
[[ "$ENABLE_S3" == "true" ]] && echo "S3 Bucket: $S3_BUCKET"
echo "Work Dir: $WORK_DIR"
echo "=============================================="

# Activate virtual environment
cd "$PROJECT_ROOT"
if [[ -f "venv/bin/activate" ]]; then
    source venv/bin/activate
else
    echo "ERROR: Virtual environment not found at $PROJECT_ROOT/venv"
    echo "Create it with: python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# Step 1: Download HRRR wind data
echo ""
echo "==> Step 1: Downloading HRRR GRIB2 data..."
python scripts/hrrr/download_hrrr.py \
    --latest \
    --fxx "$FORECAST_HOURS" \
    --variables all \
    --output "$DOWNLOAD_DIR" \
    --local-only \
    --keep-local \
    $VERBOSE

# Check files downloaded
GRIB_COUNT=$(find "$DOWNLOAD_DIR" -name "*.grib2" 2>/dev/null | wc -l)
NC_COUNT=$(find "$DOWNLOAD_DIR" -name "*.nc" 2>/dev/null | wc -l)
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

python scripts/wind/extract_wind_from_grib.py \
    --input "$DOWNLOAD_DIR" \
    --output "$WIND_DIR" \
    $VERBOSE $S3_ARG

# Results
WIND_COUNT=$(find "$WIND_DIR" -name "*.png" 2>/dev/null | wc -l)
echo ""
echo "=============================================="
echo "Complete! Generated $WIND_COUNT wind tiles"
echo "Output: $WIND_DIR"
[[ "$ENABLE_S3" == "true" ]] && echo "Uploaded to: s3://$S3_BUCKET/wind-tiles/"
echo "=============================================="
