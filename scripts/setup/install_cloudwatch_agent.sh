#!/bin/bash
#
# CloudWatch Agent Installation Script
# Part of TICKET-016: Set Up CloudWatch Monitoring
#
# This script installs and configures the Amazon CloudWatch Agent on EC2 instances
# for the Weather Data Pipeline.
#
# Usage:
#   ./install_cloudwatch_agent.sh [--config-path PATH]
#
# Prerequisites:
#   - Ubuntu 20.04+ or Amazon Linux 2
#   - IAM role with CloudWatch permissions attached to EC2 instance
#   - Internet access for downloading the agent

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEFAULT_CONFIG_PATH="${REPO_ROOT}/config/cloudwatch-agent-config.json"
CONFIG_PATH="${1:-$DEFAULT_CONFIG_PATH}"
LOG_DIR="/var/log/weather-pipeline"

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run as root (use sudo)"
        exit 1
    fi
}

detect_os() {
    if [[ -f /etc/os-release ]]; then
        . /etc/os-release
        OS=$ID
        VERSION=$VERSION_ID
    else
        log_error "Cannot detect OS. /etc/os-release not found."
        exit 1
    fi
    log_info "Detected OS: $OS $VERSION"
}

install_agent_ubuntu() {
    log_info "Installing CloudWatch Agent for Ubuntu..."

    # Download the agent
    local ARCH=$(dpkg --print-architecture)
    local AGENT_URL="https://amazoncloudwatch-agent.s3.amazonaws.com/ubuntu/${ARCH}/latest/amazon-cloudwatch-agent.deb"

    log_info "Downloading CloudWatch Agent from ${AGENT_URL}..."
    wget -q -O /tmp/amazon-cloudwatch-agent.deb "$AGENT_URL"

    # Install the agent
    log_info "Installing package..."
    dpkg -i /tmp/amazon-cloudwatch-agent.deb

    # Clean up
    rm -f /tmp/amazon-cloudwatch-agent.deb
}

install_agent_amazon_linux() {
    log_info "Installing CloudWatch Agent for Amazon Linux..."

    # Install using yum
    yum install -y amazon-cloudwatch-agent
}

install_agent() {
    case $OS in
        ubuntu|debian)
            install_agent_ubuntu
            ;;
        amzn|rhel|centos|fedora)
            install_agent_amazon_linux
            ;;
        *)
            log_error "Unsupported OS: $OS"
            exit 1
            ;;
    esac
}

create_log_directories() {
    log_info "Creating log directories..."

    mkdir -p "$LOG_DIR"
    chmod 755 "$LOG_DIR"

    # Set ownership if running as weather-pipeline user exists
    if id "ubuntu" &>/dev/null; then
        chown ubuntu:ubuntu "$LOG_DIR"
    fi

    log_info "Created $LOG_DIR"
}

configure_agent() {
    log_info "Configuring CloudWatch Agent..."

    if [[ ! -f "$CONFIG_PATH" ]]; then
        log_error "Configuration file not found: $CONFIG_PATH"
        exit 1
    fi

    # Copy config to the agent's config directory
    local AGENT_CONFIG_DIR="/opt/aws/amazon-cloudwatch-agent/etc"
    mkdir -p "$AGENT_CONFIG_DIR"
    cp "$CONFIG_PATH" "$AGENT_CONFIG_DIR/amazon-cloudwatch-agent.json"

    log_info "Configuration copied to $AGENT_CONFIG_DIR/amazon-cloudwatch-agent.json"
}

start_agent() {
    log_info "Starting CloudWatch Agent..."

    # Use the CloudWatch Agent control script
    /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
        -a fetch-config \
        -m ec2 \
        -s \
        -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json

    log_info "CloudWatch Agent started successfully"
}

enable_agent_on_boot() {
    log_info "Enabling CloudWatch Agent to start on boot..."

    systemctl enable amazon-cloudwatch-agent

    log_info "CloudWatch Agent will start automatically on boot"
}

verify_agent() {
    log_info "Verifying CloudWatch Agent status..."

    local STATUS=$(/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a status)
    echo "$STATUS"

    if echo "$STATUS" | grep -q "running"; then
        log_info "CloudWatch Agent is running!"
        return 0
    else
        log_warn "CloudWatch Agent may not be running properly"
        return 1
    fi
}

show_usage() {
    cat <<EOF
CloudWatch Agent Installation Script
=====================================

This script installs and configures the Amazon CloudWatch Agent on EC2 instances.

Usage:
    sudo ./install_cloudwatch_agent.sh [--config-path PATH]

Options:
    --config-path PATH    Path to CloudWatch agent config file
                         Default: ${DEFAULT_CONFIG_PATH}

Prerequisites:
    1. Ubuntu 20.04+ or Amazon Linux 2
    2. IAM role with these permissions attached to EC2 instance:
       - CloudWatchAgentServerPolicy
       - Or custom policy with:
         - logs:CreateLogGroup
         - logs:CreateLogStream
         - logs:PutLogEvents
         - logs:DescribeLogStreams
         - cloudwatch:PutMetricData

Example:
    sudo ./install_cloudwatch_agent.sh

After Installation:
    - Check status: sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a status
    - View logs: sudo tail -f /opt/aws/amazon-cloudwatch-agent/logs/amazon-cloudwatch-agent.log
    - Restart: sudo systemctl restart amazon-cloudwatch-agent

EOF
}

main() {
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --config-path)
                CONFIG_PATH="$2"
                shift 2
                ;;
            --help|-h)
                show_usage
                exit 0
                ;;
            *)
                # Assume it's the config path for backwards compatibility
                CONFIG_PATH="$1"
                shift
                ;;
        esac
    done

    log_info "=========================================="
    log_info "CloudWatch Agent Installation"
    log_info "=========================================="
    log_info "Config path: $CONFIG_PATH"

    check_root
    detect_os

    # Check if agent is already installed
    if command -v /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl &>/dev/null; then
        log_info "CloudWatch Agent is already installed"
    else
        install_agent
    fi

    create_log_directories
    configure_agent
    start_agent
    enable_agent_on_boot
    verify_agent

    log_info "=========================================="
    log_info "Installation Complete!"
    log_info "=========================================="
    log_info ""
    log_info "Next steps:"
    log_info "  1. Verify logs are being collected:"
    log_info "     aws logs describe-log-streams --log-group-name /weather-pipeline/pipeline"
    log_info ""
    log_info "  2. Run a test pipeline to generate logs:"
    log_info "     ./scripts/pipeline.sh --dry-run"
    log_info ""
    log_info "  3. Check CloudWatch Logs in AWS Console"
}

main "$@"
