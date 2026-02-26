#!/bin/bash
#
# Wind Tile Pipeline (Python only, no Docker)
#
# Uses Herbie to download fresh HRRR data with proper valid-time matching
# and reprojects to WGS84 for accurate coordinate mapping.
#
# Usage:
#   ./run_wind_python.sh                      # Current hour, no S3
#   ./run_wind_python.sh --enable-s3          # Current hour, upload to S3
#   ./run_wind_python.sh --forecast-hours 0,1,2,3 --enable-s3
#

set -euo pipefail

# Defaults
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORK_DIR="${WORK_DIR:-/tmp/wind-pipeline}"
S3_BUCKET="${S3_BUCKET:-driftwise-weather-data}"
ENABLE_S3=false
FORECAST_HOURS="0"  # Default: just current hour
VERBOSE=""

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --forecast-hours) FORECAST_HOURS="$2"; shift 2 ;;
        --enable-s3) ENABLE_S3=true; shift ;;
        --s3-bucket) S3_BUCKET="$2"; ENABLE_S3=true; shift 2 ;;
        --work-dir) WORK_DIR="$2"; shift 2 ;;
        --verbose|-v) VERBOSE="-v"; shift ;;
        --help) 
            echo "Usage: $0 [--forecast-hours 0,1,2,3] [--enable-s3] [--s3-bucket NAME] [--verbose]"
            echo ""
            echo "Options:"
            echo "  --forecast-hours  Comma-separated hour offsets from now (default: 0)"
            echo "  --enable-s3       Upload tiles to S3"
            echo "  --s3-bucket       S3 bucket name (default: driftwise-weather-data)"
            echo "  --verbose         Show detailed output"
            exit 0 
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

WIND_DIR="$WORK_DIR/wind-tiles"
mkdir -p "$WIND_DIR"

echo "=============================================="
echo "Wind Tile Pipeline (Herbie + WGS84 Reprojection)"
echo "=============================================="
echo "Forecast Hours: $FORECAST_HOURS"
echo "S3 Upload: $ENABLE_S3"
[[ "$ENABLE_S3" == "true" ]] && echo "S3 Bucket: $S3_BUCKET"
echo "Output Dir: $WIND_DIR"
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

# Build command
CMD="python scripts/wind/extract_wind_from_grib.py --herbie --forecast-hours $FORECAST_HOURS --output $WIND_DIR"

if [[ "$ENABLE_S3" == "true" ]]; then
    CMD="$CMD --s3-bucket $S3_BUCKET"
fi

if [[ -n "$VERBOSE" ]]; then
    CMD="$CMD --verbose"
fi

echo ""
echo "==> Generating wind tiles via Herbie..."
echo "Command: $CMD"
echo ""

$CMD

# Count results
WIND_COUNT=$(find "$WIND_DIR" -name "*.png" 2>/dev/null | wc -l)

# Generate latest_wind.json metadata if S3 enabled
if [[ "$ENABLE_S3" == "true" ]] && [[ "$WIND_COUNT" -gt 0 ]]; then
    echo ""
    echo "==> Uploading latest_wind.json metadata..."
    
    # Get info from latest tile (format: wind_20260226_t17z_f00.png)
    LATEST_PNG=$(ls -t "$WIND_DIR"/*.png 2>/dev/null | head -1)
    if [[ -n "$LATEST_PNG" ]]; then
        LATEST_NAME=$(basename "$LATEST_PNG")
        DATE_STR=$(echo "$LATEST_NAME" | sed -n 's/wind_\([0-9]*\)_t.*/\1/p')
        CYCLE=$(echo "$LATEST_NAME" | sed -n 's/.*_t\([0-9]*\)z_.*/\1/p')
        
        FORMATTED_DATE="${DATE_STR:0:4}-${DATE_STR:4:2}-${DATE_STR:6:2}"
        
        # Build list of forecast hours
        FORECAST_LIST=$(ls "$WIND_DIR"/*.png 2>/dev/null | xargs -I{} basename {} | sed -n 's/.*_f\([0-9]*\)\.png/"\1"/p' | sort -t'"' -k2 -n | tr '\n' ',' | sed 's/,$//')
        
        # Generate metadata in old format that app expects
        METADATA_FILE="$WIND_DIR/latest_wind.json"
        cat > "$METADATA_FILE" << EOF
{
  "model": "HRRR",
  "model_run": {
    "date": "$FORMATTED_DATE",
    "cycle": "${CYCLE}Z",
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  },
  "forecast_hours": [$FORECAST_LIST],
  "tiles": {
    "base_url": "https://${S3_BUCKET}.s3.amazonaws.com/wind-tiles/${FORMATTED_DATE}/${CYCLE}Z",
    "filename_pattern": "wind_${DATE_STR}_t${CYCLE}z_f{forecast}.png",
    "width": 1799,
    "height": 1059
  },
  "encoding": {
    "r_channel": "U component (m/s)",
    "g_channel": "V component (m/s)",
    "b_channel": "magnitude",
    "min_value": -50,
    "max_value": 50,
    "zero_value": 128
  },
  "bounds": {
    "west": -134.1,
    "east": -60.9,
    "north": 52.6,
    "south": 21.1
  },
  "projection": "EPSG:4326 (WGS84)",
  "generated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
        
        aws s3 cp "$METADATA_FILE" "s3://$S3_BUCKET/metadata/latest_wind.json" --content-type "application/json"
        echo "Uploaded: s3://$S3_BUCKET/metadata/latest_wind.json"
    fi
fi

echo ""
echo "=============================================="
echo "Complete! Generated $WIND_COUNT wind tiles"
echo "Output: $WIND_DIR"
[[ "$ENABLE_S3" == "true" ]] && echo "Uploaded to: s3://$S3_BUCKET/wind-tiles/"
echo "=============================================="
