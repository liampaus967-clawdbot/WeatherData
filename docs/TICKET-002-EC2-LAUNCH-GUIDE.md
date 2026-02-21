# TICKET-002: EC2 Instance Launch Guide

This guide walks you through launching an EC2 instance for the weather data pipeline via AWS Console.

## Why AWS Console Instead of Terraform?

Your AWS user (`laptop-s3-user`) doesn't have IAM permissions to create roles. The EC2 launch wizard in AWS Console can create the necessary IAM role automatically.x

## Prerequisites

- ✅ AWS Console access
- ✅ S3 bucket ready (`sat-data-automation-test`)
- ✅ Docker image built locally (`weather-processor:latest`)

## Instance Specifications

| Setting           | Value             | Reason                                  |
| ----------------- | ----------------- | --------------------------------------- |
| **Instance Type** | `t3.small`        | 2 vCPU, 2GB RAM - enough for processing |
| **OS**            | Ubuntu 22.04 LTS  | Docker support, familiar                |
| **Storage**       | 50GB gp3          | Temporary processing space              |
| **Pricing**       | On-Demand or Spot | Spot saves ~70% ($3/mo vs $10/mo)       |
| **Region**        | us-east-2         | Same as S3 bucket                       |

## Step-by-Step Launch Instructions

### Step 1: Go to EC2 Console

1. Open AWS Console: https://console.aws.amazon.com/
2. Navigate to **EC2** service
3. Click **Launch Instance** button

### Step 2: Name and Tags

```
Name: weather-pipeline-processor
Tags:
  - Project: Weather Data Pipeline
  - Environment: production
  - ManagedBy: Manual (will migrate to Terraform)
```

### Step 3: Application and OS Images (AMI)

1. **Quick Start**: Select **Ubuntu**
2. **AMI**: Ubuntu Server 22.04 LTS (HVM), SSD Volume Type
3. **Architecture**: 64-bit (x86) or 64-bit (Arm) - your choice
   - x86: More compatible
   - Arm: Cheaper (~20% less)

**Recommended**: Ubuntu Server 22.04 LTS (64-bit x86)

### Step 4: Instance Type

1. **Instance Type**: `t3.small`

   - 2 vCPU
   - 2 GB Memory
   - Up to 5 Gbps network

2. **Spot Instance (Optional - Save 70%)**:
   - Click "Request Spot Instances" checkbox
   - Max price: Leave as default (on-demand price)
   - Interruption behavior: Stop (not terminate)

**Recommendation**: Use Spot to save costs

### Step 5: Key Pair (Optional)

**Option A: No Key Pair (Use SSM Session Manager)**

- Select: "Proceed without a key pair"
- ✅ More secure (no SSH keys to manage)
- Access via AWS Systems Manager Session Manager

**Option B: Create/Use SSH Key**

- Click "Create new key pair"
- Name: `weather-pipeline-key`
- Type: RSA
- Format: .pem
- Download and save securely

**Recommendation**: Option A (SSM) - simpler and more secure

### Step 6: Network Settings

**VPC**: Default VPC (or your preferred VPC)

**Subnet**: No preference (or select us-east-2a)

**Auto-assign public IP**: ✅ Enable

**Firewall (Security Group)**: Create new security group

```
Security Group Name: weather-pipeline-sg
Description: Security group for weather data processing instance

Inbound Rules:
- Type: SSH (only if using SSH)
  Port: 22
  Source: My IP (your current IP address)
  Description: SSH access from my IP

Outbound Rules:
- Type: All traffic
  Destination: 0.0.0.0/0
  Description: Allow all outbound (needed for Docker, AWS, NOAA data)
```

**Note**: If using SSM (no key pair), you don't need SSH inbound rule.

### Step 7: Configure Storage

```
Volume 1 (Root):
  - Size: 50 GB
  - Volume Type: gp3
  - IOPS: 3000 (default)
  - Throughput: 125 MB/s (default)
  - Delete on Termination: ✅ Yes
  - Encrypted: ✅ Yes (use default KMS key)
```

### Step 8: Advanced Details

**IAM Instance Profile**:

Click "Create new IAM role" which opens a new tab:

1. **Use Case**: EC2
2. Click **Next**
3. **Attach Policies**: Search and add these AWS managed policies:
   - `AmazonS3FullAccess` (or create custom policy for your bucket only)
   - `CloudWatchAgentServerPolicy`
   - `AmazonSSMManagedInstanceCore`
4. **Role Name**: `EC2-WeatherPipeline-Role`
5. **Description**: IAM role for weather data pipeline EC2 instance
6. Click **Create Role**
7. Return to EC2 launch tab, click refresh icon
8. Select `EC2-WeatherPipeline-Role`

**User Data** (Bootstrap Script):

Paste this script in the User Data field (this automatically installs everything on first boot):

```bash
#!/bin/bash
set -e

# Log everything
exec > >(tee /var/log/user-data.log)
exec 2>&1

echo "========================================="
echo "Weather Pipeline EC2 Setup"
echo "Started: $(date)"
echo "========================================="

# Update system
echo "Updating system packages..."
apt-get update
apt-get upgrade -y

# Install Docker
echo "Installing Docker..."
apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

# Add Docker's official GPG key
mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Set up Docker repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Start and enable Docker
systemctl start docker
systemctl enable docker

# Create ubuntu user docker access
usermod -aG docker ubuntu

# Install AWS CLI v2
echo "Installing AWS CLI..."
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
apt-get install -y unzip
unzip /tmp/awscliv2.zip -d /tmp
/tmp/aws/install
rm -rf /tmp/aws /tmp/awscliv2.zip

# Install Python and pip
echo "Installing Python..."
apt-get install -y python3 python3-pip python3-venv git

# Install CloudWatch Agent
echo "Installing CloudWatch Agent..."
wget https://s3.amazonaws.com/amazoncloudwatch-agent/ubuntu/amd64/latest/amazon-cloudwatch-agent.deb -O /tmp/amazon-cloudwatch-agent.deb
dpkg -i /tmp/amazon-cloudwatch-agent.deb
rm /tmp/amazon-cloudwatch-agent.deb

# Create weather pipeline directory structure
echo "Creating directory structure..."
mkdir -p /home/ubuntu/weather-pipeline/{scripts,config,logs}
mkdir -p /tmp/weather-data

# Set permissions
chown -R ubuntu:ubuntu /home/ubuntu/weather-pipeline
chown -R ubuntu:ubuntu /tmp/weather-data

# Create CloudWatch config
echo "Configuring CloudWatch..."
cat > /opt/aws/amazon-cloudwatch-agent/etc/config.json <<'EOF'
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/weather-pipeline.log",
            "log_group_name": "/aws/weather-pipeline",
            "log_stream_name": "{instance_id}",
            "timezone": "UTC"
          }
        ]
      }
    }
  },
  "metrics": {
    "namespace": "WeatherPipeline",
    "metrics_collected": {
      "disk": {
        "measurement": [
          {
            "name": "used_percent",
            "rename": "DiskUsedPercent",
            "unit": "Percent"
          }
        ],
        "metrics_collection_interval": 300,
        "resources": [
          "*"
        ]
      },
      "mem": {
        "measurement": [
          {
            "name": "mem_used_percent",
            "rename": "MemoryUsedPercent",
            "unit": "Percent"
          }
        ],
        "metrics_collection_interval": 300
      }
    }
  }
}
EOF

# Start CloudWatch Agent
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
    -a fetch-config \
    -m ec2 \
    -s \
    -c file:/opt/aws/amazon-cloudwatch-agent/etc/config.json

# Configure logrotate for pipeline logs
cat > /etc/logrotate.d/weather-pipeline <<'EOF'
/var/log/weather-pipeline.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0644 ubuntu ubuntu
}
EOF

# Create status file
cat > /home/ubuntu/weather-pipeline/setup-status.txt <<EOF
Setup completed: $(date)
Docker version: $(docker --version)
AWS CLI version: $(aws --version)
Python version: $(python3 --version)

Next steps:
1. Clone weather data repository
2. Build Docker image
3. Configure cron job
4. Test pipeline
EOF

chown ubuntu:ubuntu /home/ubuntu/weather-pipeline/setup-status.txt

echo "========================================="
echo "Setup Complete!"
echo "Completed: $(date)"
echo "========================================="

# Reboot to ensure all services start properly
echo "Rebooting in 10 seconds..."
sleep 10
reboot
```

### Step 9: Summary

Review your configuration:

- ✅ Instance type: t3.small
- ✅ AMI: Ubuntu 22.04 LTS
- ✅ Storage: 50 GB gp3
- ✅ Security group: SSH (optional) + outbound all
- ✅ IAM role: EC2-WeatherPipeline-Role
- ✅ User data: Bootstrap script

Click **Launch Instance**

---

## Post-Launch Steps

### Step 1: Wait for Instance to Start

1. Go to EC2 Dashboard → Instances
2. Wait for **Instance State**: Running
3. Wait for **Status Check**: 2/2 checks passed (~5 minutes)
4. The instance will **reboot once** after user data completes

### Step 2: Connect to Instance

**Option A: Systems Manager Session Manager** (No SSH key needed)

1. Select your instance
2. Click **Connect** button
3. Choose **Session Manager** tab
4. Click **Connect**

**Option B: SSH** (If you created a key pair)

```bash
# Get instance public IP from EC2 console
ssh -i ~/path/to/weather-pipeline-key.pem ubuntu@<PUBLIC-IP>
```

### Step 3: Verify Installation

Once connected, run:
sudo su - ubuntu

```bash
# Check setup status
cat /home/ubuntu/weather-pipeline/setup-status.txt

# Verify Docker
docker --version
docker ps

# Verify AWS CLI
aws --version

# Test S3 access
aws s3 ls s3://sat-data-automation-test/

# Verify Python
python3 --version

# Check CloudWatch Agent
sudo systemctl status amazon-cloudwatch-agent

# Check user data log (if issues)
cat /var/log/user-data.log
```

### Step 4: Clone Repository and Build Docker Image

```bash
# Clone your repository
cd /home/ubuntu/weather-pipeline
git clone https://github.com/lpaus967/WeatherData.git .

# OR copy files from local machine
# From your local machine:
# scp -r -i ~/weather-pipeline-key.pem \
#   ~/Documents/GIT/WeatherData/* \
#   ubuntu@<PUBLIC-IP>:/home/ubuntu/weather-pipeline/

# Build Docker image
cd docker
docker build -t weather-processor:latest .

# Test Docker image
docker run --rm weather-processor:latest
```

### Step 5: Test Pipeline Components

```bash
# Test Herbie download (will take a few minutes)
docker run --rm \
  -v ~/.aws:/root/.aws \
  -v /tmp/weather-data:/tmp/weather-data \
  weather-processor:latest \
  python3 -c "
from herbie import Herbie
H = Herbie('2026-01-09 12:00', model='hrrr', fxx=0)
print(H)
print('Herbie works!')
"

# Test S3 upload
echo "test" > /tmp/test.txt
aws s3 cp /tmp/test.txt s3://sat-data-automation-test/test.txt
aws s3 rm s3://sat-data-automation-test/test.txt
rm /tmp/test.txt
echo "S3 access works!"
```

---

## Cost Estimate

### Monthly Costs

| Item                  | Configuration            | Cost           |
| --------------------- | ------------------------ | -------------- |
| **EC2 t3.small**      | On-Demand 24/7           | $15.18/mo      |
| **EC2 t3.small**      | Spot 24/7 (~70% savings) | $4.56/mo       |
| **EBS gp3**           | 50 GB                    | $4.00/mo       |
| **Data Transfer**     | 50 GB/mo to Internet     | $4.50/mo       |
| **CloudWatch Logs**   | 5 GB/mo                  | $2.50/mo       |
| **S3**                | (from TICKET-001)        | $3.40/mo       |
| **Total (On-Demand)** |                          | **~$29.58/mo** |
| **Total (Spot)**      |                          | **~$18.96/mo** |

**Recommendation**: Use Spot instances to save ~$10/month

---

## Troubleshooting

### Issue: Instance won't connect

**Check**:

```bash
# In EC2 console, check:
1. Instance State: Running
2. Status Checks: 2/2 passed
3. Security Group: Allows your IP (if using SSH)
```

### Issue: User data didn't run

**Check log**:

```bash
cat /var/log/user-data.log
# Look for errors
```

### Issue: S3 access denied

**Fix**: Check IAM role attached to instance

```bash
# Should show EC2-WeatherPipeline-Role
aws sts get-caller-identity

# If not, attach role in EC2 console:
# Actions → Security → Modify IAM role
```

### Issue: Docker permission denied

**Fix**:

```bash
# Add ubuntu user to docker group (should be done by user data)
sudo usermod -aG docker ubuntu

# Log out and back in
exit
# Then reconnect
```

---

## Verification Checklist

Before marking TICKET-002 complete, verify:

- [ ] Instance is running
- [ ] Can connect via SSM or SSH
- [ ] Docker installed and working (`docker --version`)
- [ ] AWS CLI installed (`aws --version`)
- [ ] S3 access working (`aws s3 ls s3://sat-data-automation-test/`)
- [ ] Python 3 installed (`python3 --version`)
- [ ] CloudWatch Agent running
- [ ] Docker image built (`docker images | grep weather-processor`)
- [ ] Herbie test successful
- [ ] User data log shows no errors

---

## Next Steps After TICKET-002

Once EC2 is verified:

1. ✅ **TICKET-001**: S3 Infrastructure (Complete)
2. ✅ **TICKET-002**: EC2 Instance (Complete after verification)
3. ✅ **TICKET-003**: Docker Container (Complete)
4. 📝 **TICKET-004**: Create download script (`scripts/download_hrrr.py`)
5. 📝 **TICKET-005**: Create variable config (`config/variables.yaml`)
6. 📝 **TICKET-006**: Create processing script (`scripts/process_weather.py`)

---

**Last Updated**: 2026-01-10
**Estimated Time**: 30-45 minutes (including instance boot and setup)
