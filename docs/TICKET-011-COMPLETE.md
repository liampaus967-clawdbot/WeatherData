# TICKET-011: Configure Cron Job for Hourly Execution

**Status**: Complete
**Completion Date**: 2026-01-11
**Effort**: S (<4 hours)

## Summary

Configured automated hourly execution of the weather data pipeline using cron. The pipeline runs at 15 minutes past each hour (UTC), which aligns with HRRR data availability (typically 2-3 hours after model run).

## Files Created

| File | Purpose |
|------|---------|
| `scripts/cron/weather-pipeline.cron` | Cron entry configuration file |
| `scripts/cron/weather-pipeline.env` | Environment variables for pipeline |
| `scripts/cron/run_pipeline.sh` | Cron wrapper script with locking |
| `scripts/cron/install_cron.sh` | Installation script |
| `scripts/cron/check_pipeline_status.sh` | Health check and monitoring script |
| `config/logrotate/weather-pipeline` | Log rotation configuration |

## Cron Schedule

```
# Runs at :15 past every hour (UTC)
15 * * * * /home/ubuntu/weather-pipeline/scripts/cron/run_pipeline.sh
```

### Why :15 Past the Hour?

- HRRR model runs every hour (00Z, 01Z, 02Z, etc.)
- Data is typically available on NOAA S3 ~2-3 hours after model run
- Running at :15 gives a buffer for data availability
- Pipeline calculates model run time as `current_time - 3 hours`

## Installation

### On EC2 Instance

```bash
# 1. Clone/pull the repository
cd /home/ubuntu/weather-pipeline
git pull

# 2. Run the installer
sudo ./scripts/cron/install_cron.sh

# 3. Verify installation
crontab -l
```

### Manual Installation

```bash
# Copy cron entry
crontab -e
# Add: 15 * * * * /home/ubuntu/weather-pipeline/scripts/cron/run_pipeline.sh >> /var/log/weather-pipeline/cron.log 2>&1

# Install logrotate
sudo cp config/logrotate/weather-pipeline /etc/logrotate.d/

# Create log directory
sudo mkdir -p /var/log/weather-pipeline
sudo chown ubuntu:ubuntu /var/log/weather-pipeline
```

## Environment Configuration

Edit `scripts/cron/weather-pipeline.env` to customize:

```bash
# AWS Configuration
export AWS_REGION="us-east-2"
export S3_BUCKET="sat-data-automation-test"
export ENABLE_S3_UPLOAD="true"

# Pipeline Configuration
export ENABLE_TILES="true"
export PRIORITY="1"
export ZOOM_LEVELS="0-8"
export TILE_PROCESSES="4"
```

## Log Rotation

Logs are automatically rotated by logrotate:

| Log Type | Rotation | Retention |
|----------|----------|-----------|
| `pipeline_*.log` | Daily | 14 days |
| `cron.log` | Weekly | 4 weeks |

Configuration: `/etc/logrotate.d/weather-pipeline`

## Monitoring

### Health Check Script

```bash
# Run manually
./scripts/cron/check_pipeline_status.sh

# Or add to cron for periodic monitoring (every 4 hours)
0 */4 * * * /home/ubuntu/weather-pipeline/scripts/cron/check_pipeline_status.sh
```

### What It Checks

1. **Last Run Status**: Verifies pipeline ran successfully
2. **S3 Data Freshness**: Checks metadata age
3. **Disk Space**: Warns if /tmp is >80% full
4. **Docker**: Verifies Docker daemon and image availability
5. **Cron Job**: Confirms cron is configured

### Setting Up Alerts

Configure SNS notifications in `check_pipeline_status.sh`:

```bash
export SNS_TOPIC_ARN="arn:aws:sns:us-east-2:123456789:weather-alerts"
export ALERT_EMAIL="admin@example.com"
```

## Lock File

The wrapper script uses a lock file to prevent overlapping runs:

- Location: `/tmp/weather-pipeline.lock`
- Contains: PID of running process
- Auto-cleaned on exit (even on failure)

## Troubleshooting

### Pipeline Not Running

```bash
# Check cron is running
systemctl status cron

# Check crontab
crontab -l | grep weather

# Check cron logs
grep CRON /var/log/syslog | tail -20

# Check pipeline logs
tail -100 /var/log/weather-pipeline/cron.log
```

### Permission Issues

```bash
# Ensure scripts are executable
chmod +x scripts/cron/*.sh
chmod +x scripts/pipeline.sh

# Ensure log directory is writable
ls -la /var/log/weather-pipeline/
```

### Docker Issues in Cron

```bash
# Ensure user is in docker group
groups ubuntu  # Should include 'docker'

# If not, add user to docker group
sudo usermod -aG docker ubuntu
# Then log out and back in
```

## Acceptance Criteria

- [x] Cron job runs at :15 past each hour
- [x] Pipeline executes successfully via cron
- [x] Logs are rotated (14 days retention)
- [x] Lock file prevents overlapping runs
- [x] Health check script monitors execution
- [x] Environment variables configurable
- [x] Documentation complete

## Timeline (UTC)

```
HH:00  - HRRR model run (NOAA)
HH:15  - Pipeline starts (cron)
HH:20  - Download complete (~5 min)
HH:30  - Processing complete (~10 min)
HH:35  - Upload to S3 complete (~5 min)
HH:40  - Users see new data in web app
```

## Related Tickets

- TICKET-010: Pipeline orchestration script (prerequisite)
- TICKET-012: Metadata generation (enhances pipeline output)
- TICKET-016: CloudWatch monitoring (integrates with health checks)
- TICKET-017: CloudWatch alarms (SNS alert integration)
