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
SKIP_CLEANUP=false

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --forecast-hours) FORECAST_HOURS="$2"; shift 2 ;;
        --enable-s3) ENABLE_S3=true; shift ;;
        --s3-bucket) S3_BUCKET="$2"; ENABLE_S3=true; shift 2 ;;
        --work-dir) WORK_DIR="$2"; shift 2 ;;
        --verbose|-v) VERBOSE="-v"; shift ;;
        --skip-cleanup) SKIP_CLEANUP=true; shift ;;
        --help) echo "Usage: $0 [--forecast-hours 0-12] [--enable-s3] [--s3-bucket NAME] [--skip-cleanup] [--verbose]"; exit 0 ;;
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

# Step 3: Generate and upload latest_wind.json metadata
if [[ "$ENABLE_S3" == "true" ]] && [[ "$WIND_COUNT" -gt 0 ]]; then
    echo ""
    echo "==> Step 3: Uploading latest_wind.json metadata..."
    
    # Get the most recent wind tile info
    LATEST_PNG=$(ls -t "$WIND_DIR"/*.png 2>/dev/null | head -1)
    if [[ -n "$LATEST_PNG" ]]; then
        LATEST_NAME=$(basename "$LATEST_PNG")
        # Extract date, cycle, forecast from filename: wind_20260224_t17z_f00.png
        DATE_STR=$(echo "$LATEST_NAME" | sed -n 's/wind_\([0-9]*\)_t.*/\1/p')
        CYCLE=$(echo "$LATEST_NAME" | sed -n 's/.*_t\([0-9]*\)z_.*/\1/p')
        
        # Build list of available forecast hours (as JSON strings to handle leading zeros)
        FORECAST_LIST=$(ls "$WIND_DIR"/*.png 2>/dev/null | xargs -I{} basename {} | sed -n 's/.*_f\([0-9]*\)\.png/"\1"/p' | sort -t'"' -k2 -n | tr '\n' ',' | sed 's/,$//')
        
        # Format date for display
        FORMATTED_DATE="${DATE_STR:0:4}-${DATE_STR:4:2}-${DATE_STR:6:2}"
        
        # Generate latest_wind.json
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
    "north": 49.0,
    "south": 21.1
  },
  "generated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
        
        # Upload to S3
        aws s3 cp "$METADATA_FILE" "s3://$S3_BUCKET/metadata/latest_wind.json" --content-type "application/json"
        echo "Uploaded: s3://$S3_BUCKET/metadata/latest_wind.json"
    fi
fi

# Step 4: Clean up old data (unless --skip-cleanup is set)
if [[ "$SKIP_CLEANUP" == "false" ]]; then
    # Clean up old wind tiles from S3 (keep only current cycle)
    if [[ "$ENABLE_S3" == "true" ]] && [[ "$WIND_COUNT" -gt 0 ]] && [[ -n "$DATE_STR" ]] && [[ -n "$CYCLE" ]]; then
        echo ""
        echo "==> Step 4a: Cleaning up old wind tiles from S3..."
        
        CURRENT_PREFIX="wind-tiles/${FORMATTED_DATE}/${CYCLE}Z"
        
        # List all wind-tiles prefixes and delete those that aren't the current one
        OLD_PREFIXES=$(aws s3 ls "s3://$S3_BUCKET/wind-tiles/" | awk '{print $2}' | tr -d '/')
        
        for DATE_PREFIX in $OLD_PREFIXES; do
            # List cycles within each date
            CYCLES=$(aws s3 ls "s3://$S3_BUCKET/wind-tiles/${DATE_PREFIX}/" 2>/dev/null | awk '{print $2}' | tr -d '/')
            for CYCLE_PREFIX in $CYCLES; do
                FULL_PREFIX="wind-tiles/${DATE_PREFIX}/${CYCLE_PREFIX}"
                if [[ "$FULL_PREFIX" != "$CURRENT_PREFIX" ]]; then
                    echo "Deleting old tiles: s3://$S3_BUCKET/$FULL_PREFIX/"
                    aws s3 rm "s3://$S3_BUCKET/$FULL_PREFIX/" --recursive --quiet
                fi
            done
            
            # Clean up empty date folders
            REMAINING=$(aws s3 ls "s3://$S3_BUCKET/wind-tiles/${DATE_PREFIX}/" 2>/dev/null | wc -l)
            if [[ "$REMAINING" -eq 0 ]]; then
                echo "Removing empty date folder: $DATE_PREFIX"
            fi
        done
        
        echo "Wind tiles cleanup complete - kept only: $CURRENT_PREFIX"
        
        # Clean up raw GRIB2 files from S3 (not needed after tile generation)
        echo ""
        echo "==> Step 4b: Cleaning up raw GRIB2 files from S3..."
        GRIB_COUNT_S3=$(aws s3 ls "s3://$S3_BUCKET/raw-grib2/" --recursive 2>/dev/null | wc -l)
        if [[ "$GRIB_COUNT_S3" -gt 0 ]]; then
            echo "Removing $GRIB_COUNT_S3 GRIB2 files from S3..."
            aws s3 rm "s3://$S3_BUCKET/raw-grib2/" --recursive --quiet
            echo "S3 GRIB2 cleanup complete"
        else
            echo "No GRIB2 files to clean up in S3"
        fi
    fi

    # Clean up local GRIB/NetCDF files (they're large and no longer needed)
    echo ""
    echo "==> Step 4c: Cleaning up local data files..."
    GRIB_SIZE=$(du -sh "$DOWNLOAD_DIR" 2>/dev/null | cut -f1 || echo "0")
    echo "Removing downloaded files ($GRIB_SIZE)..."
    rm -rf "$DOWNLOAD_DIR"/*
    echo "Local cleanup complete"
else
    echo ""
    echo "==> Skipping cleanup (--skip-cleanup flag set)"
fi

echo ""
echo "=============================================="
echo "Complete! Generated $WIND_COUNT wind tiles"
echo "Output: $WIND_DIR"
[[ "$ENABLE_S3" == "true" ]] && echo "Uploaded to: s3://$S3_BUCKET/wind-tiles/"
[[ "$ENABLE_S3" == "true" ]] && echo "Metadata: s3://$S3_BUCKET/metadata/latest_wind.json"
[[ "$ENABLE_S3" == "true" ]] && [[ "$SKIP_CLEANUP" == "false" ]] && echo "Old tiles & GRIB files cleaned up"
[[ "$SKIP_CLEANUP" == "true" ]] && echo "Cleanup skipped (--skip-cleanup)"
echo "=============================================="
