#!/bin/bash
#
# Pipeline Health Check Script
#
# Monitors pipeline execution and sends alerts for missed runs or failures.
# Can be run manually or via cron to verify pipeline health.
#
# Part of TICKET-011: Configure Cron Job for Hourly Execution
#

set -euo pipefail

# Configuration
LOG_DIR="${LOG_DIR:-/var/log/weather-pipeline}"
S3_BUCKET="${S3_BUCKET:-sat-data-container}"
MAX_DATA_AGE_HOURS="${MAX_DATA_AGE_HOURS:-4}"
SNS_TOPIC_ARN="${SNS_TOPIC_ARN:-}"
ALERT_EMAIL="${ALERT_EMAIL:-}"

# Colors for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_ok() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

send_alert() {
    local subject="$1"
    local message="$2"

    echo "ALERT: $subject"
    echo "$message"

    # Send SNS notification if configured
    if [[ -n "$SNS_TOPIC_ARN" ]]; then
        aws sns publish \
            --topic-arn "$SNS_TOPIC_ARN" \
            --subject "$subject" \
            --message "$message" 2>/dev/null || true
    fi

    # Send email if configured (requires mailutils)
    if [[ -n "$ALERT_EMAIL" ]] && command -v mail &>/dev/null; then
        echo "$message" | mail -s "$subject" "$ALERT_EMAIL" 2>/dev/null || true
    fi
}

check_last_run() {
    echo "Checking last pipeline run..."

    # Find the most recent log file
    local latest_log=$(find "$LOG_DIR" -name "pipeline_*.log" -type f 2>/dev/null | sort -r | head -1)

    if [[ -z "$latest_log" ]]; then
        log_error "No pipeline logs found in $LOG_DIR"
        return 1
    fi

    local log_name=$(basename "$latest_log")
    local log_age_seconds=$(($(date +%s) - $(stat -f %m "$latest_log" 2>/dev/null || stat -c %Y "$latest_log" 2>/dev/null)))
    local log_age_hours=$((log_age_seconds / 3600))

    echo "  Latest log: $log_name"
    echo "  Log age: ${log_age_hours}h ${log_age_seconds}s"

    # Check if log is too old
    if [[ $log_age_hours -ge $MAX_DATA_AGE_HOURS ]]; then
        log_error "Pipeline hasn't run in ${log_age_hours} hours (max: ${MAX_DATA_AGE_HOURS}h)"
        send_alert "Weather Pipeline - Missed Execution" \
            "The weather pipeline hasn't run in ${log_age_hours} hours. Last log: $log_name"
        return 1
    fi

    # Check if last run was successful
    if grep -q "\[SUCCESS\] Pipeline completed successfully" "$latest_log"; then
        log_ok "Last pipeline run completed successfully"
        return 0
    elif grep -q "\[ERROR\]" "$latest_log"; then
        local error_msg=$(grep "\[ERROR\]" "$latest_log" | tail -5)
        log_error "Last pipeline run had errors"
        echo "  Recent errors:"
        echo "$error_msg" | sed 's/^/    /'
        send_alert "Weather Pipeline - Execution Failed" \
            "The weather pipeline failed. Log: $log_name\n\nErrors:\n$error_msg"
        return 1
    else
        log_warn "Last pipeline run status unclear"
        return 0
    fi
}

check_s3_freshness() {
    echo ""
    echo "Checking S3 data freshness..."

    if ! command -v aws &>/dev/null; then
        log_warn "AWS CLI not available, skipping S3 check"
        return 0
    fi

    # Check latest.json metadata
    local metadata_date=$(aws s3api head-object \
        --bucket "$S3_BUCKET" \
        --key "metadata/latest.json" \
        --query "LastModified" \
        --output text 2>/dev/null || echo "")

    if [[ -z "$metadata_date" ]]; then
        log_warn "Could not retrieve metadata from S3"
        return 0
    fi

    echo "  Metadata last modified: $metadata_date"

    # Parse date and check age
    local metadata_epoch=$(date -j -f "%Y-%m-%dT%H:%M:%S" "${metadata_date%%.*}" +%s 2>/dev/null || \
                          date -d "$metadata_date" +%s 2>/dev/null || echo "0")

    if [[ "$metadata_epoch" != "0" ]]; then
        local age_hours=$(( ($(date +%s) - metadata_epoch) / 3600 ))
        echo "  Data age: ${age_hours} hours"

        if [[ $age_hours -ge $MAX_DATA_AGE_HOURS ]]; then
            log_error "S3 data is ${age_hours} hours old (max: ${MAX_DATA_AGE_HOURS}h)"
            return 1
        fi
    fi

    log_ok "S3 data is fresh"
    return 0
}

check_disk_space() {
    echo ""
    echo "Checking disk space..."

    local usage=$(df /tmp | awk 'NR==2 {print $5}' | tr -d '%')
    echo "  /tmp usage: ${usage}%"

    if [[ $usage -ge 80 ]]; then
        log_warn "/tmp disk usage is high: ${usage}%"
        return 1
    fi

    log_ok "Disk space OK"
    return 0
}

check_docker() {
    echo ""
    echo "Checking Docker..."

    if ! command -v docker &>/dev/null; then
        log_error "Docker not installed"
        return 1
    fi

    if ! docker info &>/dev/null; then
        log_error "Docker daemon not running or not accessible"
        return 1
    fi

    # Check if weather-processor image exists
    if docker images weather-processor:latest --format "{{.Repository}}" | grep -q weather-processor; then
        log_ok "Docker and weather-processor image OK"
        return 0
    else
        log_warn "weather-processor:latest image not found"
        return 1
    fi
}

check_cron() {
    echo ""
    echo "Checking cron job..."

    if crontab -l 2>/dev/null | grep -q "weather-pipeline\|run_pipeline"; then
        log_ok "Cron job is configured"
        crontab -l | grep -E "weather-pipeline|run_pipeline" | sed 's/^/  /'
        return 0
    else
        log_warn "Cron job not found in crontab"
        return 1
    fi
}

# Main
main() {
    echo "=========================================="
    echo "Weather Pipeline Health Check"
    echo "Time: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "=========================================="
    echo ""

    local exit_code=0

    check_last_run || exit_code=1
    check_s3_freshness || exit_code=1
    check_disk_space || exit_code=1
    check_docker || exit_code=1
    check_cron || exit_code=1

    echo ""
    echo "=========================================="
    if [[ $exit_code -eq 0 ]]; then
        echo -e "${GREEN}All checks passed${NC}"
    else
        echo -e "${RED}Some checks failed${NC}"
    fi
    echo "=========================================="

    exit $exit_code
}

main "$@"
