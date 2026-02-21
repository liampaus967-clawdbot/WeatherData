# ✅ TICKET-002: EC2 Instance Provisioning - COMPLETE

**Status**: Complete
**Date**: 2026-01-10
**Instance Region**: us-east-2 (Ohio)

## What Was Completed

### ✅ EC2 Instance Launched

**Instance Details:**
- **State**: Running and verified
- **Region**: us-east-2 (same as S3 bucket - no data transfer costs)
- **Storage**: 48 GB total, 45 GB available (94% free)
- **Connection**: AWS Systems Manager Session Manager

### ✅ Software Installed and Verified

| Software | Version | Status |
|----------|---------|--------|
| **Docker** | 29.1.4 | ✅ Installed, working |
| **AWS CLI** | 2.32.32 | ✅ Installed, working |
| **Python** | 3.12.3 | ✅ Installed, working |

### ✅ Permissions Configured

- **Docker**: Ubuntu user has docker group permissions (no sudo required)
- **S3 Access**: Verified - can list `s3://sat-data-automation-test/`
- **IAM Role**: Attached with S3 and CloudWatch permissions

### ✅ Directory Structure

```
/home/ubuntu/weather-pipeline/
├── scripts/    # For download and processing scripts
├── config/     # For variables.yaml configuration
└── logs/       # For pipeline logs
```

### ✅ Network Connectivity

- Outbound HTTPS working (Docker pull, AWS API, package downloads)
- Can access NOAA data sources (required for Herbie)
- Session Manager connection working

## Verification Results

```bash
=== Software Versions ===
Docker version 29.1.4, build 0e6fee6
aws-cli/2.32.32 Python/3.13.11 Linux/6.14.0-1015-aws exe/x86_64.ubuntu.24
Python 3.12.3

=== Docker Test ===
CONTAINER ID   IMAGE     COMMAND   CREATED   STATUS    PORTS     NAMES
✅ Docker daemon running, no permission issues

=== S3 Access Test ===
                           PRE tiles/
2026-01-09 20:54:23    1875968 band72_pot_temp.mbtiles
✅ S3 bucket accessible

=== Disk Space ===
Filesystem      Size  Used Avail Use% Mounted on
/dev/root        48G  3.4G   45G   8% /
✅ 45 GB free space available
```

## Next Steps on EC2

### 1. Clone Repository to EC2

**Option A: Clone from GitHub (Recommended)**
```bash
cd /home/ubuntu/weather-pipeline
git clone https://github.com/lpaus967/WeatherData.git .
```

**Option B: Copy from Local Machine**
```bash
# From your local machine (if you have SSH access)
scp -r ~/Documents/GIT/WeatherData/* ubuntu@<PUBLIC-IP>:/home/ubuntu/weather-pipeline/
```

### 2. Build Docker Image on EC2

```bash
cd /home/ubuntu/weather-pipeline/docker
docker build -t weather-processor:latest .

# Test the image
docker run --rm weather-processor:latest python3 -c "from herbie import Herbie; print('Herbie OK')"
```

### 3. Test Herbie Download

```bash
# Quick Herbie test (downloads small metadata)
docker run --rm \
  -v ~/.aws:/root/.aws \
  -v /tmp/weather-data:/tmp/weather-data \
  weather-processor:latest \
  python3 -c "
from herbie import Herbie
H = Herbie('2026-01-10 12:00', model='hrrr', fxx=0)
print(H)
print('✅ Herbie works!')
"
```

## Acceptance Criteria

All criteria from TICKET-002 met:

- [x] EC2 instance launched and running
- [x] Instance in us-east-2 region (same as S3)
- [x] Docker installed and verified (29.1.4)
- [x] AWS CLI v2 installed (2.32.32)
- [x] Python 3.10+ installed (3.12.3)
- [x] IAM role attached with S3 access
- [x] S3 bucket access verified
- [x] Directory structure created
- [x] Ubuntu user has docker permissions
- [x] Sufficient disk space (45 GB free)
- [x] Session Manager connection working
- [x] Outbound network connectivity verified

## Cost Summary

**Current Monthly Estimate:**
- EC2 instance: Variable (depends on usage and instance type)
- EBS Storage (48 GB): ~$3.84/month
- S3 (from TICKET-001): ~$3.40/month
- Data Transfer: Minimal (same region as S3)

**Total Infrastructure Cost**: ~$7-20/month depending on EC2 usage

## Files Created

During TICKET-002, these documentation files were created:

```
terraform/
├── EC2-QUICK-REFERENCE.md           # Quick reference card
├── TICKET-002-EC2-LAUNCH-GUIDE.md   # Comprehensive launch guide
├── TICKET-002-STATUS.md             # Status tracking
├── TICKET-002-COMPLETE.md           # This file
└── EC2-INSTANCE-VERIFICATION.md     # Verification checklist
```

## Security Configuration

### ✅ Implemented

- **No SSH Key**: Using Session Manager instead (more secure)
- **IAM Role**: Instance uses role-based authentication (no hardcoded credentials)
- **Encrypted Storage**: EBS volume encrypted at rest
- **Security Group**: Only required outbound access
- **AWS CLI**: Uses instance IAM role automatically

### ⚠️ Action Required

From SECURITY_NOTICE.md:
- **Rotate AWS credentials** in `terraform_s3/terraform.tfvars` if still active
- Those credentials were detected by GitHub push protection and blocked
- Already using IAM role on EC2, so no impact on pipeline

## Project Status

### ✅ Completed Tickets

1. **TICKET-001**: AWS S3 Infrastructure
   - S3 bucket configured with lifecycle policies
   - Encryption, versioning, CORS enabled
   - Lifecycle rules for automatic data management

2. **TICKET-002**: EC2 Instance Provisioning ← **Just Completed**
   - Instance launched and verified
   - All software installed
   - S3 access confirmed

3. **TICKET-003**: Docker Container
   - Docker image built and tested
   - All dependencies included (Herbie, GDAL, xarray, rioxarray)
   - NumPy compatibility fixed

### 📝 Next Up: TICKET-004

**TICKET-004: Create HRRR Download Script with Herbie**

Now that infrastructure is ready, we can create the download script:

**Location**: `/home/ubuntu/weather-pipeline/scripts/download_hrrr.py`

**Features**:
- Use Herbie for HRRR data downloads
- Download specific variables (temperature, wind, etc.)
- Save to local temporary directory
- Upload to S3 bucket
- Error handling and logging

**Estimated Time**: 1-2 hours
**Priority**: P0 (Next task)

## Troubleshooting Reference

### Common Issues and Solutions

**Issue: Docker permission denied**
```bash
# Add user to docker group and reconnect
sudo usermod -aG docker ubuntu
exit
# Then reconnect
```

**Issue: S3 access denied**
```bash
# Check IAM role is attached
aws sts get-caller-identity
# Should show instance role, not a user
```

**Issue: Out of disk space**
```bash
# Clean up old Docker images
docker system prune -af
# Remove old logs
sudo journalctl --vacuum-time=7d
```

## Documentation Links

- **Launch Guide**: TICKET-002-EC2-LAUNCH-GUIDE.md
- **Quick Reference**: EC2-QUICK-REFERENCE.md
- **S3 Configuration**: TICKET-001-COMPLETE.md
- **Docker Setup**: ../docker/BUILD_SUMMARY.md
- **Security Notice**: ../SECURITY_NOTICE.md

---

**Completed**: 2026-01-10
**Instance Ready**: Yes
**Ready for TICKET-004**: Yes

## Summary

EC2 instance is fully provisioned and verified. All required software is installed and working:
- ✅ Docker 29.1.4
- ✅ AWS CLI 2.32.32
- ✅ Python 3.12.3
- ✅ S3 access working
- ✅ 45 GB free space

The instance is ready for the next phase: building the Docker image and creating the Herbie download script (TICKET-004).
