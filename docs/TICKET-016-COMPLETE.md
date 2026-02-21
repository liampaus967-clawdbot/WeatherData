# TICKET-016: Set Up CloudWatch Monitoring

**Status**: Complete
**Priority**: P1
**Effort**: M (4-8 hours)
**Completion Date**: 2026-01-12

## Summary

Implemented comprehensive CloudWatch monitoring for the Weather Data Pipeline, including custom metrics, log aggregation, and metric filters for error detection.

## Implementation Details

### 1. CloudWatch Metrics Helper Module (Python)

**File**: `scripts/common/cloudwatch_metrics.py`

A reusable Python module for sending metrics to CloudWatch from any script:

```python
from scripts.common import get_metrics, MetricNames, MetricUnits

# Get metrics instance
metrics = get_metrics()

# Set default dimensions for all metrics
metrics.set_default_dimensions({'Pipeline': 'HRRR'})

# Send simple metric
metrics.put_metric('FilesProcessed', 10, MetricUnits.COUNT)

# Use timer context manager
with metrics.timer('COGConversion'):
    process_files()

# Record specific metrics
metrics.record_files_processed(10, 'COG', 'Processing')
metrics.record_error('Download', 'NetworkTimeout')
metrics.record_success()
```

**Features**:
- Singleton pattern for easy import
- Context manager for automatic timing
- Decorator support for function timing
- Batch metric sending
- Graceful handling of missing credentials
- Dry-run mode for testing

### 2. Enhanced Pipeline Metrics (Bash)

**File**: `scripts/pipeline.sh`

Enhanced CloudWatch metrics in the pipeline script:

**Metrics Sent**:
| Metric | Unit | Description |
|--------|------|-------------|
| `ProcessingTime` | Seconds | Total pipeline duration |
| `DataAge` | None (minutes) | Minutes since model run |
| `FilesDownloaded` | Count | Number of GRIB2 files downloaded |
| `FilesProcessed` | Count | Number of COG files created |
| `TilesGenerated` | Count | Number of PNG tiles generated |
| `Errors` | Count | Total error count |
| `Success` | Count | 1 if pipeline completed successfully |
| `Failure` | Count | 1 if pipeline failed |
| `StepDuration` | Seconds | Duration per step (Download, Processing, Colormap, TileGeneration, S3Upload, Metadata) |

**Step Timing Functions**:
```bash
start_step_timer "Download"
# ... download operations ...
end_step_timer "Download"
```

### 3. CloudWatch Logs Configuration

**File**: `config/cloudwatch-agent-config.json`

CloudWatch Agent configuration for log collection:

**Log Groups Created**:
- `/weather-pipeline/pipeline` - Main orchestration logs
- `/weather-pipeline/download` - HRRR download logs
- `/weather-pipeline/processing` - GRIB2 to COG processing logs
- `/weather-pipeline/tiles` - Tile generation logs
- `/weather-pipeline/s3-upload` - S3 upload logs

**System Metrics Collected**:
- CPU utilization (idle, user, system)
- Memory usage (used_percent, available)
- Disk usage (used_percent)
- Network I/O (bytes sent/received)
- Disk I/O (reads, writes)
- Process counts

### 4. Terraform Resources

**File**: `terraform/cloudwatch.tf`

Infrastructure-as-code for CloudWatch resources:

**Log Groups**:
- 5 log groups with 30-day retention
- Automatic creation via Terraform

**Metric Filters**:
| Filter | Pattern | Metric Created |
|--------|---------|----------------|
| `PipelineErrors` | `[timestamp, level=ERROR, ...]` | `PipelineErrors` |
| `PipelineWarnings` | `[timestamp, level=WARN, ...]` | `PipelineWarnings` |
| `DownloadErrors` | `?ERROR ?FAILED ?Exception` | `DownloadErrors` |
| `ProcessingErrors` | `?ERROR ?FAILED ?Exception` | `ProcessingErrors` |
| `TileGenerationErrors` | `?ERROR ?FAILED ?Exception` | `TileGenerationErrors` |
| `S3UploadErrors` | `?ERROR ?FAILED ?upload failed` | `S3UploadErrors` |
| `PipelineSuccess` | `[timestamp, level=SUCCESS, msg="Pipeline completed*"]` | `PipelineSuccess` |

**New Variable**:
- `log_retention_days` - Default: 30 days

### 5. CloudWatch Agent Setup Script

**File**: `scripts/setup/install_cloudwatch_agent.sh`

Automated installation script for EC2 instances:

```bash
# Install and configure CloudWatch Agent
sudo ./scripts/setup/install_cloudwatch_agent.sh

# Or with custom config path
sudo ./scripts/setup/install_cloudwatch_agent.sh --config-path /path/to/config.json
```

**Features**:
- Supports Ubuntu and Amazon Linux
- Automatic OS detection
- Creates required log directories
- Enables agent on boot
- Verification of agent status

## Deployment Steps

### Step 1: Apply Terraform Changes

```bash
cd terraform
terraform plan
terraform apply
```

This creates:
- 5 CloudWatch Log Groups
- 7 Metric Filters

### Step 2: Install CloudWatch Agent on EC2

```bash
# SSH to EC2 instance
ssh ubuntu@<ec2-instance>

# Clone/pull latest code
cd /home/ubuntu/weather-pipeline
git pull

# Install CloudWatch Agent
sudo ./scripts/setup/install_cloudwatch_agent.sh
```

### Step 3: Verify Agent Status

```bash
# Check agent status
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a status

# View agent logs
sudo tail -f /opt/aws/amazon-cloudwatch-agent/logs/amazon-cloudwatch-agent.log
```

### Step 4: Test Pipeline with Metrics

```bash
# Run pipeline in dry-run mode to test metrics
./scripts/pipeline.sh --dry-run

# Run actual pipeline
./scripts/pipeline.sh --enable-s3 --s3-bucket sat-data-automation-test
```

### Step 5: Verify in AWS Console

1. Go to **CloudWatch > Metrics > WeatherPipeline**
2. Verify metrics are appearing:
   - ProcessingTime
   - DataAge
   - FilesProcessed
   - etc.

3. Go to **CloudWatch > Log groups**
4. Verify log streams are being created

## Required IAM Permissions

The EC2 instance IAM role needs these permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:DescribeLogStreams"
            ],
            "Resource": "arn:aws:logs:*:*:log-group:/weather-pipeline/*"
        }
    ]
}
```

Or attach the managed policy: `CloudWatchAgentServerPolicy`

## CloudWatch Namespace

All custom metrics are in the `WeatherPipeline` namespace.

### Dimensions

| Dimension | Values | Description |
|-----------|--------|-------------|
| `Pipeline` | HRRR | Pipeline/model type |
| `Step` | Download, Processing, Colormap, TileGeneration, S3Upload, Metadata | Processing step |
| `FileType` | GRIB2, COG, PNG | File type for FilesProcessed |
| `ErrorType` | General, NetworkTimeout, etc. | Error categorization |

## Files Created/Modified

### New Files
- `scripts/common/cloudwatch_metrics.py` - Python metrics helper module
- `scripts/common/__init__.py` - Module initialization
- `config/cloudwatch-agent-config.json` - CloudWatch Agent configuration
- `terraform/cloudwatch.tf` - Terraform resources for log groups and metric filters
- `scripts/setup/install_cloudwatch_agent.sh` - Agent installation script
- `docs/TICKET-016-COMPLETE.md` - This documentation

### Modified Files
- `terraform/variables.tf` - Added `log_retention_days` variable
- `scripts/pipeline.sh` - Enhanced CloudWatch metrics with:
  - Step timing functions
  - Detailed metric tracking
  - Error recording
  - Per-step duration metrics

## Acceptance Criteria

- [x] Custom CloudWatch metrics created:
  - [x] `DataAge`: Minutes since model run
  - [x] `ProcessingTime`: Total pipeline duration
  - [x] `FilesProcessed`: Count of successful files
  - [x] `Errors`: Pipeline failure count
  - [x] `StepDuration`: Per-step timing
- [x] CloudWatch Logs agent configuration created
- [x] Log groups created for pipeline components
- [x] Log retention set to 30 days
- [x] Metric filters created for error detection
- [x] Python metrics helper module created with boto3 integration

## Next Steps (TICKET-017)

The CloudWatch Alarms ticket will:
1. Create SNS topic for alerts
2. Create alarms for:
   - Data Stale: DataAge > 120 minutes
   - Pipeline Failed: Errors > 0
   - Processing Slow: ProcessingTime > 35 minutes
   - Disk Full: EC2 disk usage > 80%
3. Configure email notifications

## Testing

### Dry-Run Test
```bash
./scripts/pipeline.sh --dry-run
```

Expected output includes:
```
[INFO] [DRY-RUN] Would send CloudWatch metrics:
[INFO]   - ProcessingTime: Xs
[INFO]   - FilesDownloaded: 7
[INFO]   - FilesProcessed: 14
[INFO]   - TilesGenerated: 10
[INFO]   - Errors: 0
```

### Python Module Test
```bash
cd /path/to/WeatherData
python -c "from scripts.common import get_metrics; m = get_metrics(enabled=False); print('Module loaded successfully')"
```

## Troubleshooting

### Metrics Not Appearing
1. Check IAM permissions on EC2 role
2. Verify AWS CLI is configured: `aws sts get-caller-identity`
3. Check pipeline logs for metric errors

### Logs Not Appearing
1. Check CloudWatch Agent status: `amazon-cloudwatch-agent-ctl -a status`
2. Verify log files exist: `ls -la /var/log/weather-pipeline/`
3. Check agent logs: `tail /opt/aws/amazon-cloudwatch-agent/logs/amazon-cloudwatch-agent.log`

### Agent Won't Start
1. Check IAM role is attached to EC2
2. Verify configuration file is valid JSON
3. Check system logs: `journalctl -u amazon-cloudwatch-agent`
