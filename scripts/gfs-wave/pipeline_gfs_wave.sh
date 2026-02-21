#!/bin/bash
#
# GFS-Wave Data Pipeline Orchestration Script
#
# Automates the complete GFS-Wave data processing workflow:
# 1. Download GFS-Wave GRIB2 data
# 2. Process GRIB2 to grayscale COGs
# 3. Apply color ramps to COGs
# 4. Generate web map tiles
# 5. Upload to S3
# 6. Generate metadata JSON
# 7. Clean up temporary files
#
# GFS-Wave runs every 6 hours (00, 06, 12, 18 UTC) with ~5 hour data delay
#

set -euo pipefail  # Exit on error, undefined variables, and pipe failures

# ============================================================================
# Configuration
# ============================================================================

# Default configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="${LOG_DIR:-/var/log/gfs-wave-pipeline}"
WORK_DIR="${WORK_DIR:-/tmp/gfs-wave-pipeline}"
S3_BUCKET="${S3_BUCKET:-}"
ENABLE_S3_UPLOAD="${ENABLE_S3_UPLOAD:-false}"
ENABLE_TILES="${ENABLE_TILES:-true}"
DRY_RUN="${DRY_RUN:-false}"
PRIORITY="${PRIORITY:-1}"
ZOOM_LEVELS="${ZOOM_LEVELS:-0-10}"
TILE_PROCESSES="${TILE_PROCESSES:-4}"
FORECAST_HOURS="${FORECAST_HOURS:-0}"  # Forecast hours to download (0 = current/analysis only)

# GFS-Wave specific settings
MODEL_NAME="gfs_wave"
CONFIG_FILE="$PROJECT_ROOT/config/variables_gfs_wave.yaml"
DATA_DELAY_HOURS=5  # GFS-Wave has longer delay than HRRR
VALID_CYCLES=(0 6 12 18)  # GFS-Wave runs every 6 hours

# S3 paths for GFS-Wave (separate from HRRR)
S3_PREFIX_RAW="gfs-wave/raw-grib2"
S3_PREFIX_COLORED="gfs-wave/colored-cogs"
S3_PREFIX_TILES="gfs-wave/tiles"
S3_PREFIX_METADATA="gfs-wave/metadata"

# Timestamps
START_TIME=$(date +%s)
DATE_UTC=$(date -u +%Y%m%d)
HOUR_UTC=$(date -u +%H)

# Logging
LOG_FILE="${LOG_DIR}/pipeline_${DATE_UTC}_${HOUR_UTC}00.log"

# ============================================================================
# Functions
# ============================================================================

log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date -u +"%Y-%m-%d %H:%M:%S UTC")
    echo "[${timestamp}] [${level}] ${message}" | tee -a "$LOG_FILE"
}

log_info() {
    log "INFO" "$@"
}

log_warn() {
    log "WARN" "$@"
}

log_error() {
    log "ERROR" "$@"
}

log_success() {
    log "SUCCESS" "$@"
}

cleanup() {
    local exit_code=$?
    log_info "Cleaning up temporary files..."

    if [[ "$DRY_RUN" == "false" ]]; then
        # Clean up work directory (keep logs)
        # Use sudo if needed because Docker creates files as root
        if [[ -d "$WORK_DIR/downloads" ]]; then
            rm -rf "$WORK_DIR/downloads" 2>/dev/null || sudo rm -rf "$WORK_DIR/downloads"
        fi
        if [[ -d "$WORK_DIR/processed" ]]; then
            rm -rf "$WORK_DIR/processed" 2>/dev/null || sudo rm -rf "$WORK_DIR/processed"
        fi
        if [[ -d "$WORK_DIR/colored" ]]; then
            rm -rf "$WORK_DIR/colored" 2>/dev/null || sudo rm -rf "$WORK_DIR/colored"
        fi
        if [[ -d "$WORK_DIR/tiles" ]]; then
            rm -rf "$WORK_DIR/tiles" 2>/dev/null || sudo rm -rf "$WORK_DIR/tiles"
        fi
    fi

    local end_time=$(date +%s)
    local duration=$((end_time - START_TIME))
    log_info "Pipeline execution time: ${duration}s"

    if [[ $exit_code -eq 0 ]]; then
        log_success "Pipeline completed successfully"
    else
        log_error "Pipeline failed with exit code: $exit_code"
    fi

    exit $exit_code
}

trap cleanup EXIT

check_dependencies() {
    log_info "Checking dependencies..."

    local missing_deps=()

    # Check Docker (required for all processing steps)
    if ! command -v docker &> /dev/null; then
        missing_deps+=("docker")
    fi

    # Check AWS CLI (if S3 upload enabled)
    if [[ "$ENABLE_S3_UPLOAD" == "true" ]] && ! command -v aws &> /dev/null; then
        missing_deps+=("aws-cli")
    fi

    # Check config file exists
    if [[ ! -f "$CONFIG_FILE" ]]; then
        log_error "Config file not found: $CONFIG_FILE"
        return 1
    fi

    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        log_error "Missing dependencies: ${missing_deps[*]}"
        return 1
    fi

    log_success "All dependencies found"
    return 0
}

calculate_model_run_time() {
    log_info "Calculating GFS-Wave model run time..."

    # GFS-Wave data is typically available 4-5 hours after model run
    # Use current time - 5 hours and round down to nearest 6-hour cycle
    local current_epoch=$(date -u +%s)
    local delayed_epoch=$((current_epoch - DATA_DELAY_HOURS * 3600))

    # Use -r for BSD date (macOS) or -d for GNU date (Linux)
    if date -r "$delayed_epoch" +%Y-%m-%d &> /dev/null; then
        # BSD date (macOS)
        MODEL_DATE=$(date -u -r "$delayed_epoch" +%Y-%m-%d)
        local hour=$(date -u -r "$delayed_epoch" +%H)
    else
        # GNU date (Linux)
        MODEL_DATE=$(date -u -d "@$delayed_epoch" +%Y-%m-%d)
        local hour=$(date -u -d "@$delayed_epoch" +%H)
    fi

    # Round down to nearest 6-hour cycle (10# forces base-10 interpretation)
    MODEL_CYCLE=$(( (10#$hour / 6) * 6 ))
    MODEL_CYCLE=$(printf "%02d" $MODEL_CYCLE)

    log_info "Model run: ${MODEL_DATE} cycle ${MODEL_CYCLE}Z"

    export MODEL_DATE
    export MODEL_CYCLE
}

download_data() {
    log_info "==> Step 1: Downloading GFS-Wave data..."
    start_step_timer "Download"

    local download_dir="$WORK_DIR/downloads"
    mkdir -p "$download_dir"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would download GFS-Wave data via Docker"
        # Create dummy files for dry run (current time only by default)
        touch "$download_dir/gfs_wave.${MODEL_DATE//-/}.t${MODEL_CYCLE}z.f000.grib2"
        FILES_DOWNLOADED=1
        end_step_timer "Download"
        return 0
    fi

    local cmd="docker run --rm \
        --user $(id -u):$(id -g) \
        -e HOME=/tmp \
        -e HERBIE_HOME=/data/output \
        -v $download_dir:/data/output \
        -v $PROJECT_ROOT:/app \
        weather-processor:latest \
        python3 /app/scripts/gfs-wave/download_gfs_wave.py \
        --date $MODEL_DATE \
        --cycle=$MODEL_CYCLE \
        --fxx $FORECAST_HOURS \
        --variables all \
        --output-dir /data/output \
        --keep-local"

    log_info "Executing: $cmd"

    if $cmd >> "$LOG_FILE" 2>&1; then
        log_success "Download completed"

        # Find all downloaded GRIB files
        GRIB2_FILES=($(find "$download_dir" -name "*.grib2" | sort))
        if [[ ${#GRIB2_FILES[@]} -eq 0 ]]; then
            record_pipeline_error "Download" "No GRIB2 files found after download"
            end_step_timer "Download"
            return 1
        fi

        FILES_DOWNLOADED=${#GRIB2_FILES[@]}
        log_info "Downloaded ${FILES_DOWNLOADED} GRIB2 files:"
        for f in "${GRIB2_FILES[@]}"; do
            local file_size=$(du -h "$f" | cut -f1)
            log_info "  - $(basename $f) ($file_size)"
        done

        export GRIB2_FILES
        export DOWNLOAD_DIR="$download_dir"
        end_step_timer "Download"
        return 0
    else
        record_pipeline_error "Download" "Download failed"
        end_step_timer "Download"
        return 1
    fi
}

process_grib2() {
    log_info "==> Step 2: Processing GRIB2 to COGs..."
    start_step_timer "Processing"

    local processed_dir="$WORK_DIR/processed"
    mkdir -p "$processed_dir"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would process GRIB2 to COGs"
        # Create dummy files (current time only by default)
        local model_date_compact="${MODEL_DATE//-/}"
        touch "$processed_dir/wave_height_gfs_wave.${model_date_compact}.t${MODEL_CYCLE}z.f000.tif"
        touch "$processed_dir/wave_period_gfs_wave.${model_date_compact}.t${MODEL_CYCLE}z.f000.tif"
        FILES_PROCESSED=2
        end_step_timer "Processing"
        return 0
    fi

    # Process each GRIB file
    local processed_count=0
    local failed_count=0
    local total_files=${#GRIB2_FILES[@]}

    for grib_file in "${GRIB2_FILES[@]}"; do
        local grib_name=$(basename "$grib_file")
        processed_count=$((processed_count + 1))
        log_info "Processing GRIB file $processed_count/$total_files: $grib_name"

        local cmd="docker run --rm \
            --user $(id -u):$(id -g) \
            -e HOME=/tmp \
            -v $DOWNLOAD_DIR:/data/input \
            -v $processed_dir:/data/output \
            -v $PROJECT_ROOT:/app \
            weather-processor:latest \
            python3 /app/scripts/processing/process_weather.py \
            --input /data/input/$grib_name \
            --output /data/output \
            --config /app/config/variables_gfs_wave.yaml \
            --priority $PRIORITY"

        if $cmd >> "$LOG_FILE" 2>&1; then
            log_info "  Processed: $grib_name"
        else
            failed_count=$((failed_count + 1))
            log_warn "  Failed to process: $grib_name (continuing with remaining files)"
        fi
    done

    # Record any processing failures
    if [[ $failed_count -gt 0 ]]; then
        record_pipeline_error "Processing" "Failed to process $failed_count of $total_files GRIB files"
    fi

    log_success "GRIB2 processing completed"

    # Count processed files
    local cog_count=$(find "$processed_dir" -name "*.tif" | wc -l | tr -d ' ')
    FILES_PROCESSED=$cog_count
    log_info "Generated $cog_count COG files from $total_files GRIB files"
    export PROCESSED_DIR="$processed_dir"
    end_step_timer "Processing"
    return 0
}

apply_colormaps() {
    log_info "==> Step 3: Applying color ramps..."
    start_step_timer "Colormap"

    local colored_dir="$WORK_DIR/colored"
    mkdir -p "$colored_dir"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would apply color ramps to COGs"
        # Create dummy files (current time only by default)
        local model_date_compact="${MODEL_DATE//-/}"
        touch "$colored_dir/wave_height_gfs_wave.${model_date_compact}.t${MODEL_CYCLE}z.f000_colored.tif"
        end_step_timer "Colormap"
        return 0
    fi

    local cmd="docker run --rm \
        --user $(id -u):$(id -g) \
        -e HOME=/tmp \
        -v $PROCESSED_DIR:/data/input \
        -v $colored_dir:/data/output \
        -v $PROJECT_ROOT:/app \
        weather-processor:latest \
        python3 /app/scripts/processing/apply_colormap.py \
        --input /data/input \
        --output /data/output \
        --config /app/config/variables_gfs_wave.yaml"

    log_info "Executing: $cmd"

    if $cmd >> "$LOG_FILE" 2>&1; then
        log_success "Color ramp application completed"

        local colored_count=$(find "$colored_dir" -name "*_colored.tif" | wc -l | tr -d ' ')
        log_info "Generated $colored_count colored COG files"
        export COLORED_DIR="$colored_dir"
        end_step_timer "Colormap"
        return 0
    else
        record_pipeline_error "Colormap" "Color ramp application failed"
        end_step_timer "Colormap"
        return 1
    fi
}

generate_tiles() {
    if [[ "$ENABLE_TILES" != "true" ]]; then
        log_info "==> Step 4: Tile generation disabled (skipped)"
        return 0
    fi

    log_info "==> Step 4: Generating web map tiles..."
    start_step_timer "TileGeneration"

    local tiles_dir="$WORK_DIR/tiles"
    mkdir -p "$tiles_dir"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would generate tiles from colored COGs"
        local model_date_compact="${MODEL_DATE//-/}"
        mkdir -p "$tiles_dir/wave_height/${model_date_compact}T${MODEL_CYCLE}z/000/0/0"
        for i in {0..9}; do
            touch "$tiles_dir/wave_height/${model_date_compact}T${MODEL_CYCLE}z/000/0/0/${i}.png"
        done
        TILES_GENERATED=10
        end_step_timer "TileGeneration"
        return 0
    fi

    local cmd="docker run --rm \
        --user $(id -u):$(id -g) \
        -e HOME=/tmp \
        -v $COLORED_DIR:/data/input \
        -v $tiles_dir:/data/output \
        -v $PROJECT_ROOT:/app \
        weather-processor:latest \
        python3 /app/scripts/processing/generate_tiles.py \
        --input /data/input \
        --output /data/output \
        --zoom $ZOOM_LEVELS \
        --processes $TILE_PROCESSES \
        --exclude-transparent \
        --organize"

    log_info "Executing: $cmd"

    if $cmd >> "$LOG_FILE" 2>&1; then
        log_success "Tile generation completed"

        local tile_count=$(find "$tiles_dir" -name "*.png" | wc -l | tr -d ' ')
        TILES_GENERATED=$tile_count
        log_info "Generated $tile_count tiles"
        export TILES_DIR="$tiles_dir"
        end_step_timer "TileGeneration"
        return 0
    else
        record_pipeline_error "TileGeneration" "Tile generation failed"
        end_step_timer "TileGeneration"
        return 1
    fi
}

upload_to_s3() {
    if [[ "$ENABLE_S3_UPLOAD" != "true" ]]; then
        log_info "==> Step 5: S3 upload disabled (skipped)"
        return 0
    fi

    if [[ -z "$S3_BUCKET" ]]; then
        log_warn "S3_BUCKET not set, skipping upload"
        return 0
    fi

    log_info "==> Step 5: Uploading to S3..."
    start_step_timer "S3Upload"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would upload to s3://$S3_BUCKET"
        end_step_timer "S3Upload"
        return 0
    fi

    # Upload colored COGs
    log_info "Uploading colored COGs..."
    if aws s3 sync "$COLORED_DIR" "s3://$S3_BUCKET/$S3_PREFIX_COLORED/${MODEL_DATE}/" \
        --exclude "*.txt" \
        --quiet >> "$LOG_FILE" 2>&1; then
        log_success "Colored COGs uploaded"
    else
        record_pipeline_error "S3Upload" "Failed to upload colored COGs"
        end_step_timer "S3Upload"
        return 1
    fi

    # Upload tiles (if generated)
    if [[ "$ENABLE_TILES" == "true" ]] && [[ -d "$TILES_DIR" ]]; then
        log_info "Uploading tiles..."
        if aws s3 sync "$TILES_DIR" "s3://$S3_BUCKET/$S3_PREFIX_TILES/" \
            --quiet >> "$LOG_FILE" 2>&1; then
            log_success "Tiles uploaded"
        else
            record_pipeline_error "S3Upload" "Failed to upload tiles"
            end_step_timer "S3Upload"
            return 1
        fi
    fi

    end_step_timer "S3Upload"
    return 0
}

generate_metadata() {
    log_info "==> Step 6: Generating metadata..."
    start_step_timer "Metadata"

    local metadata_file="$WORK_DIR/latest.json"
    local metadata_dir="$WORK_DIR/metadata"
    mkdir -p "$metadata_dir"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would generate metadata JSON"
        end_step_timer "Metadata"
        return 0
    fi

    # Use Python script to generate comprehensive metadata
    local cmd="docker run --rm \
        --user $(id -u):$(id -g) \
        -e HOME=/tmp \
        -v $TILES_DIR:/data/tiles \
        -v $metadata_dir:/data/output \
        -v $PROJECT_ROOT:/app \
        weather-processor:latest \
        python3 /app/scripts/generate_metadata.py \
        --date $MODEL_DATE \
        --cycle=$MODEL_CYCLE \
        --s3-bucket $S3_BUCKET \
        --tiles-dir /data/tiles \
        --config /app/config/variables_gfs_wave.yaml \
        --s3-prefix gfs-wave \
        --output /data/output/latest.json"

    log_info "Executing metadata generation..."

    if $cmd >> "$LOG_FILE" 2>&1; then
        log_success "Metadata generated"

        # Copy to work dir for upload
        if [[ -f "$metadata_dir/latest.json" ]]; then
            cp "$metadata_dir/latest.json" "$metadata_file"
        fi
    else
        log_warn "Python metadata generation failed, using fallback"
        # Fallback: Generate simple metadata
        local variable_count=$(find "$COLORED_DIR" -name "*_colored.tif" 2>/dev/null | wc -l)
        cat > "$metadata_file" << EOF
{
  "version": "1.0",
  "model": "gfs_wave",
  "product": "global.0p25",
  "model_run": {
    "date": "${MODEL_DATE}",
    "cycle": "${MODEL_CYCLE}",
    "cycle_formatted": "${MODEL_CYCLE}Z",
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  },
  "forecast_hours": ["000"],
  "variables": [],
  "variable_count": ${variable_count},
  "tiles": {
    "url_template": "https://${S3_BUCKET}.s3.${AWS_REGION:-us-east-1}.amazonaws.com/${S3_PREFIX_TILES}/{variable}/{timestamp}/{forecast}/{z}/{x}/{y}.png",
    "format": "png",
    "tile_size": 256
  },
  "generated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "pipeline_version": "1.0"
}
EOF
        log_info "Fallback metadata generated"
    fi

    # Upload metadata to S3
    if [[ "$ENABLE_S3_UPLOAD" == "true" ]] && [[ -n "$S3_BUCKET" ]]; then
        log_info "Uploading metadata to S3..."
        if aws s3 cp "$metadata_file" "s3://$S3_BUCKET/$S3_PREFIX_METADATA/latest.json" \
            --content-type "application/json" \
            --cache-control "max-age=300" \
            --quiet >> "$LOG_FILE" 2>&1; then
            log_success "Metadata uploaded"
        else
            log_warn "Failed to upload metadata"
        fi
    fi

    end_step_timer "Metadata"
    return 0
}

cleanup_old_grib_files() {
    # Clean up old GRIB files from S3, keeping only the most recent model run

    if [[ "$ENABLE_S3_UPLOAD" != "true" ]] || [[ -z "$S3_BUCKET" ]]; then
        return 0
    fi

    log_info "Cleaning up old GFS-Wave GRIB files from S3..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would clean up old GRIB files"
        return 0
    fi

    # Get timestamp for the current run (keep this one)
    local current_run_date="${MODEL_DATE//-/}"
    local current_run_pattern="gfs_wave.${current_run_date}.t${MODEL_CYCLE}z"

    log_info "Keeping files matching: $current_run_pattern"

    # List all grib2 files in gfs-wave/raw-grib2 prefix
    local grib_files=$(aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX_RAW/" --recursive 2>/dev/null | grep "\.grib2$" || true)

    if [[ -z "$grib_files" ]]; then
        log_info "No GRIB files found to clean up"
        return 0
    fi

    local deleted_count=0
    local kept_count=0

    while IFS= read -r line; do
        local s3_key=$(echo "$line" | awk '{print $4}')

        if [[ -z "$s3_key" ]]; then
            continue
        fi

        if [[ "$s3_key" == *"$current_run_pattern"* ]]; then
            kept_count=$((kept_count + 1))
        else
            if aws s3 rm "s3://$S3_BUCKET/$s3_key" --quiet >> "$LOG_FILE" 2>&1; then
                deleted_count=$((deleted_count + 1))
            fi
        fi
    done <<< "$grib_files"

    log_info "GRIB cleanup complete: kept $kept_count files, deleted $deleted_count old files"
    return 0
}

cleanup_old_cog_files() {
    # Clean up old COG files from S3, keeping only the most recent model run

    if [[ "$ENABLE_S3_UPLOAD" != "true" ]] || [[ -z "$S3_BUCKET" ]]; then
        return 0
    fi

    log_info "Cleaning up old GFS-Wave COG files from S3..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would clean up old COG files"
        return 0
    fi

    local current_date_prefix="$S3_PREFIX_COLORED/${MODEL_DATE}/"

    local cog_prefixes=$(aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX_COLORED/" 2>/dev/null | awk '{print $2}' || true)

    if [[ -z "$cog_prefixes" ]]; then
        log_info "No COG directories found to clean up"
        return 0
    fi

    local deleted_count=0
    local kept_count=0

    while IFS= read -r prefix; do
        if [[ -z "$prefix" ]]; then
            continue
        fi

        local full_prefix="$S3_PREFIX_COLORED/${prefix}"

        if [[ "$full_prefix" == "$current_date_prefix" ]]; then
            kept_count=$((kept_count + 1))
            log_info "Keeping COG directory: $full_prefix"
        else
            log_info "Deleting old COG directory: $full_prefix"
            if aws s3 rm "s3://$S3_BUCKET/$full_prefix" --recursive --quiet >> "$LOG_FILE" 2>&1; then
                deleted_count=$((deleted_count + 1))
            fi
        fi
    done <<< "$cog_prefixes"

    log_info "COG cleanup complete: kept $kept_count directories, deleted $deleted_count old directories"
    return 0
}

cleanup_old_tiles() {
    # Clean up old tiles from S3, keeping only the most recent model run

    if [[ "$ENABLE_S3_UPLOAD" != "true" ]] || [[ -z "$S3_BUCKET" ]]; then
        return 0
    fi

    if [[ "$ENABLE_TILES" != "true" ]]; then
        return 0
    fi

    log_info "Cleaning up old GFS-Wave tile files from S3..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would clean up old tile files"
        return 0
    fi

    local current_date="${MODEL_DATE//-/}"
    local current_timestamp="${current_date}T${MODEL_CYCLE}z"

    local variable_dirs=$(aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX_TILES/" 2>/dev/null | awk '{print $2}' || true)

    if [[ -z "$variable_dirs" ]]; then
        log_info "No tile directories found to clean up"
        return 0
    fi

    local deleted_count=0
    local kept_count=0

    while IFS= read -r var_dir; do
        if [[ -z "$var_dir" ]]; then
            continue
        fi

        local timestamp_dirs=$(aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX_TILES/${var_dir}" 2>/dev/null | awk '{print $2}' || true)

        while IFS= read -r ts_dir; do
            if [[ -z "$ts_dir" ]]; then
                continue
            fi

            local ts_name="${ts_dir%/}"

            if [[ "$ts_name" == "$current_timestamp" ]]; then
                kept_count=$((kept_count + 1))
            else
                local full_path="$S3_PREFIX_TILES/${var_dir}${ts_dir}"
                log_info "Deleting old tiles: $full_path"
                if aws s3 rm "s3://$S3_BUCKET/$full_path" --recursive --quiet >> "$LOG_FILE" 2>&1; then
                    deleted_count=$((deleted_count + 1))
                fi
            fi
        done <<< "$timestamp_dirs"
    done <<< "$variable_dirs"

    log_info "Tiles cleanup complete: kept $kept_count timestamp dirs, deleted $deleted_count old timestamp dirs"
    return 0
}

# ============================================================================
# CloudWatch Metrics Functions
# ============================================================================

declare -A STEP_START_TIMES
declare -A STEP_DURATIONS
METRICS_NAMESPACE="WeatherPipeline"
PIPELINE_ERRORS=0
FILES_DOWNLOADED=0
FILES_PROCESSED=0
TILES_GENERATED=0

start_step_timer() {
    local step_name="$1"
    STEP_START_TIMES[$step_name]=$(date +%s)
}

end_step_timer() {
    local step_name="$1"
    local end_time=$(date +%s)
    local start_time=${STEP_START_TIMES[$step_name]:-$end_time}
    STEP_DURATIONS[$step_name]=$((end_time - start_time))
    log_info "Step '$step_name' completed in ${STEP_DURATIONS[$step_name]}s"
}

send_metric() {
    local metric_name="$1"
    local value="$2"
    local unit="${3:-Count}"
    local dimensions="${4:-Pipeline=GFS-Wave}"

    if ! command -v aws &> /dev/null; then
        return 0
    fi

    aws cloudwatch put-metric-data \
        --namespace "$METRICS_NAMESPACE" \
        --metric-name "$metric_name" \
        --value "$value" \
        --unit "$unit" \
        --dimensions "$dimensions" >> "$LOG_FILE" 2>&1 || {
            log_warn "Failed to send metric: $metric_name"
        }
}

send_cloudwatch_metrics() {
    if ! command -v aws &> /dev/null; then
        log_warn "AWS CLI not found, skipping CloudWatch metrics"
        return 0
    fi

    log_info "Sending CloudWatch metrics..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would send CloudWatch metrics:"
        log_info "  - ProcessingTime: $(($(date +%s) - START_TIME))s"
        log_info "  - FilesDownloaded: ${FILES_DOWNLOADED:-0}"
        log_info "  - FilesProcessed: ${FILES_PROCESSED:-0}"
        log_info "  - TilesGenerated: ${TILES_GENERATED:-0}"
        log_info "  - Errors: $PIPELINE_ERRORS"
        return 0
    fi

    local duration=$(($(date +%s) - START_TIME))

    # Calculate data age (minutes since model run)
    local model_epoch
    if date -j -f "%Y-%m-%d %H" "$MODEL_DATE $MODEL_CYCLE" +%s &> /dev/null; then
        model_epoch=$(date -j -f "%Y-%m-%d %H" "$MODEL_DATE $MODEL_CYCLE" +%s 2>/dev/null || echo "0")
    else
        model_epoch=$(date -d "$MODEL_DATE $MODEL_CYCLE:00:00 UTC" +%s 2>/dev/null || echo "0")
    fi
    local current_epoch=$(date +%s)
    local data_age_minutes=0
    if [[ "$model_epoch" -gt 0 ]]; then
        data_age_minutes=$(( (current_epoch - model_epoch) / 60 ))
    fi

    log_info "Sending metrics to CloudWatch namespace: $METRICS_NAMESPACE"

    send_metric "ProcessingTime" "$duration" "Seconds" "Pipeline=GFS-Wave"
    send_metric "DataAge" "$data_age_minutes" "None" "Pipeline=GFS-Wave"
    send_metric "FilesDownloaded" "${FILES_DOWNLOADED:-0}" "Count" "Pipeline=GFS-Wave,Step=Download"
    send_metric "FilesProcessed" "${FILES_PROCESSED:-0}" "Count" "Pipeline=GFS-Wave,Step=Processing"
    send_metric "TilesGenerated" "${TILES_GENERATED:-0}" "Count" "Pipeline=GFS-Wave,Step=TileGeneration"
    send_metric "Errors" "$PIPELINE_ERRORS" "Count" "Pipeline=GFS-Wave"

    if [[ "$PIPELINE_ERRORS" -eq 0 ]]; then
        send_metric "Success" "1" "Count" "Pipeline=GFS-Wave"
    else
        send_metric "Failure" "1" "Count" "Pipeline=GFS-Wave"
    fi

    for step in "${!STEP_DURATIONS[@]}"; do
        send_metric "StepDuration" "${STEP_DURATIONS[$step]}" "Seconds" "Pipeline=GFS-Wave,Step=$step"
    done

    log_success "CloudWatch metrics sent successfully"
    log_info "  Total Duration: ${duration}s"
    log_info "  Data Age: ${data_age_minutes} minutes"
    log_info "  Files Downloaded: ${FILES_DOWNLOADED:-0}"
    log_info "  Files Processed: ${FILES_PROCESSED:-0}"
    log_info "  Tiles Generated: ${TILES_GENERATED:-0}"
    log_info "  Errors: $PIPELINE_ERRORS"
}

record_pipeline_error() {
    local step="$1"
    local message="${2:-Unknown error}"
    PIPELINE_ERRORS=$((PIPELINE_ERRORS + 1))
    log_error "[$step] $message"

    if [[ "$DRY_RUN" != "true" ]] && command -v aws &> /dev/null; then
        send_metric "Errors" "1" "Count" "Pipeline=GFS-Wave,Step=$step"
    fi
}

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

GFS-Wave Data Pipeline Orchestration Script

OPTIONS:
  --dry-run           Simulate pipeline without executing commands
  --priority N        Processing priority (1-3, default: 1)
  --zoom LEVELS       Zoom levels for tiles (default: 0-6)
  --forecast-hours H  Forecast hours to download (default: 0, e.g., "0", "0-12", "0,3,6")
  --enable-s3         Enable S3 upload
  --s3-bucket NAME    S3 bucket for uploads
  --disable-tiles     Disable tile generation
  --work-dir PATH     Working directory (default: /tmp/gfs-wave-pipeline)
  --log-dir PATH      Log directory (default: /var/log/gfs-wave-pipeline)
  --help              Show this help message

EXAMPLES:
  # Dry run (test without execution)
  $0 --dry-run

  # Run with S3 upload
  $0 --enable-s3 --s3-bucket my-weather-bucket

  # Run with extended forecast hours (F000-F012)
  $0 --forecast-hours 0-12 --enable-s3 --s3-bucket my-bucket

  # Run without tile generation
  $0 --disable-tiles

  # Custom configuration
  $0 --priority 1 --zoom 0-8 --forecast-hours 0-12 --enable-s3 --s3-bucket my-bucket

ENVIRONMENT VARIABLES:
  WORK_DIR            Working directory for temporary files
  LOG_DIR             Directory for log files
  S3_BUCKET           S3 bucket name
  ENABLE_S3_UPLOAD    Enable S3 upload (true/false)
  ENABLE_TILES        Enable tile generation (true/false)
  PRIORITY            Processing priority (1-3)
  ZOOM_LEVELS         Zoom levels for tile generation
  FORECAST_HOURS      Forecast hours to download (e.g., "0", "0-6", "0-12")
  DRY_RUN             Dry run mode (true/false)

NOTES:
  GFS-Wave runs every 6 hours (00, 06, 12, 18 UTC)
  Data is typically available ~5 hours after model run
  Recommended cron schedule: 30 5,11,17,23 * * * (runs 30 min after data availability)

EOF
}

# ============================================================================
# Main Pipeline
# ============================================================================

main() {
    # Parse command-line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --priority)
                PRIORITY="$2"
                shift 2
                ;;
            --zoom)
                ZOOM_LEVELS="$2"
                shift 2
                ;;
            --forecast-hours)
                FORECAST_HOURS="$2"
                shift 2
                ;;
            --enable-s3)
                ENABLE_S3_UPLOAD=true
                shift
                ;;
            --s3-bucket)
                S3_BUCKET="$2"
                ENABLE_S3_UPLOAD=true
                shift 2
                ;;
            --disable-tiles)
                ENABLE_TILES=false
                shift
                ;;
            --work-dir)
                WORK_DIR="$2"
                shift 2
                ;;
            --log-dir)
                LOG_DIR="$2"
                shift 2
                ;;
            --help)
                usage
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done

    # Create directories
    mkdir -p "$LOG_DIR"
    mkdir -p "$WORK_DIR"

    # Start pipeline
    log_info "=========================================="
    log_info "GFS-Wave Data Pipeline Starting"
    log_info "=========================================="
    log_info "Date: $(date -u +%Y-%m-%d)"
    log_info "Time: $(date -u +%H:%M:%S) UTC"
    log_info "Dry Run: $DRY_RUN"
    log_info "Priority: $PRIORITY"
    log_info "Forecast Hours: $FORECAST_HOURS"
    log_info "Tiles Enabled: $ENABLE_TILES"
    log_info "Zoom Levels: $ZOOM_LEVELS"
    log_info "S3 Upload: $ENABLE_S3_UPLOAD"
    if [[ -n "$S3_BUCKET" ]]; then
        log_info "S3 Bucket: $S3_BUCKET"
    fi
    log_info "Work Directory: $WORK_DIR"
    log_info "Log File: $LOG_FILE"
    log_info "Config File: $CONFIG_FILE"
    log_info "=========================================="

    # Check dependencies
    check_dependencies || exit 1

    # Calculate model run time
    calculate_model_run_time || exit 1

    # Execute pipeline steps
    download_data || exit 1
    process_grib2 || exit 1
    apply_colormaps || exit 1
    generate_tiles || exit 1
    upload_to_s3 || exit 1
    generate_metadata || exit 1

    # Clean up old files to reduce S3 storage costs
    cleanup_old_grib_files
    cleanup_old_cog_files
    cleanup_old_tiles

    # Send metrics
    send_cloudwatch_metrics

    log_info "=========================================="
    log_success "Pipeline completed successfully!"
    log_info "=========================================="
}

# Run main function
main "$@"
