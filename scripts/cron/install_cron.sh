#!/bin/bash
#
# Install Weather Pipeline Cron Job
#
# This script installs the cron job, logrotate configuration,
# and sets up all necessary directories and permissions.
#
# Usage: sudo ./install_cron.sh
#
# Part of TICKET-011: Configure Cron Job for Hourly Execution
#

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="/var/log/weather-pipeline"
INSTALL_USER="${SUDO_USER:-ubuntu}"

echo "=========================================="
echo "Weather Pipeline Cron Job Installer"
echo "=========================================="
echo ""
echo "Project root: $PROJECT_ROOT"
echo "Install user: $INSTALL_USER"
echo "Log directory: $LOG_DIR"
echo ""

# Check if running as root (needed for logrotate install)
if [[ $EUID -ne 0 ]]; then
    echo "Note: Run with sudo for full installation (logrotate config)"
    echo "      Continuing with user-level installation..."
    INSTALL_LOGROTATE=false
else
    INSTALL_LOGROTATE=true
fi

# Create log directory
echo "Creating log directory..."
if [[ $EUID -eq 0 ]]; then
    mkdir -p "$LOG_DIR"
    chown "$INSTALL_USER:$INSTALL_USER" "$LOG_DIR"
    chmod 755 "$LOG_DIR"
else
    mkdir -p "$LOG_DIR" 2>/dev/null || sudo mkdir -p "$LOG_DIR"
fi
echo "  Created: $LOG_DIR"

# Make scripts executable
echo ""
echo "Setting script permissions..."
chmod +x "$SCRIPT_DIR/run_pipeline.sh"
chmod +x "$SCRIPT_DIR/check_pipeline_status.sh"
chmod +x "$PROJECT_ROOT/scripts/pipeline.sh"
echo "  Made scripts executable"

# Install logrotate configuration
if [[ "$INSTALL_LOGROTATE" == "true" ]]; then
    echo ""
    echo "Installing logrotate configuration..."
    cp "$PROJECT_ROOT/config/logrotate/weather-pipeline" /etc/logrotate.d/weather-pipeline
    chmod 644 /etc/logrotate.d/weather-pipeline
    echo "  Installed: /etc/logrotate.d/weather-pipeline"
fi

# Install cron job for the user
echo ""
echo "Installing cron job for user: $INSTALL_USER"

# Create a temporary file with the new cron entry
TEMP_CRON=$(mktemp)
chmod 644 "$TEMP_CRON"

# Get existing crontab (if any)
crontab -u "$INSTALL_USER" -l 2>/dev/null | grep -v "weather-pipeline\|run_pipeline" > "$TEMP_CRON" || true

# Add our cron entry
cat >> "$TEMP_CRON" << EOF

# Weather Data Pipeline - runs at :15 past every hour (UTC)
# Installed by install_cron.sh on $(date -u +%Y-%m-%d)
15 * * * * $SCRIPT_DIR/run_pipeline.sh >> $LOG_DIR/cron.log 2>&1
EOF

# Install the new crontab
crontab -u "$INSTALL_USER" "$TEMP_CRON"
rm -f "$TEMP_CRON"

echo "  Cron job installed"

# Verify installation
echo ""
echo "=========================================="
echo "Installation Complete!"
echo "=========================================="
echo ""
echo "Cron schedule: Every hour at :15 (UTC)"
echo ""
echo "Current crontab for $INSTALL_USER:"
crontab -u "$INSTALL_USER" -l 2>/dev/null | grep -A1 "Weather Data Pipeline" || echo "  (none found)"
echo ""
echo "Next steps:"
echo "  1. Edit environment variables if needed:"
echo "     $SCRIPT_DIR/weather-pipeline.env"
echo ""
echo "  2. Test the pipeline manually:"
echo "     $PROJECT_ROOT/scripts/pipeline.sh --dry-run"
echo ""
echo "  3. Monitor execution:"
echo "     tail -f $LOG_DIR/cron.log"
echo ""
echo "  4. Check pipeline health:"
echo "     $SCRIPT_DIR/check_pipeline_status.sh"
echo ""
