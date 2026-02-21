# EC2 Instance Verification Checklist

Please gather this information from your existing EC2 instance via AWS Console:

## 1. Instance Details

In EC2 Console → Instances → Select your instance:

- [ ] **Instance ID**: (e.g., i-0123456789abcdef0)
- [ ] **Instance Type**: (e.g., t3.small, t2.micro, etc.)
- [ ] **Operating System**: (e.g., Ubuntu 22.04, Amazon Linux 2, etc.)
- [ ] **State**: Running / Stopped
- [ ] **Public IPv4 address**: (if assigned)

## 2. Storage

In the same view, check "Storage" tab:

- [ ] **Root volume size**: ___ GB
- [ ] **Volume type**: (gp2, gp3, etc.)
- [ ] **Available space**: Can you check if there's enough space?

**Required**: At least 30 GB free for Docker images and temporary weather data

## 3. IAM Role

In the same view, check "Security" tab:

- [ ] **IAM Role**: (role name, or "None" if no role attached)

**Required**: Role must have these permissions:
- S3 read/write access to `sat-data-automation-test`
- CloudWatch Logs write access
- SSM access (for Session Manager connection)

## 4. Security Group

In "Security" tab:

- [ ] **Security Group Name**: ___
- [ ] **Outbound rules**: Does it allow all outbound traffic to 0.0.0.0/0?

**Required**: Outbound HTTPS (443) and HTTP (80) to download Docker images, weather data, and access AWS services

## 5. Software Installed (Need to Check via Connection)

If you can connect to the instance (SSH or Session Manager), please run these commands and share the output:

```bash
# Check Docker
docker --version

# Check AWS CLI
aws --version

# Check Python
python3 --version

# Check disk space
df -h

# Check if we can access S3
aws s3 ls s3://sat-data-automation-test/
```

**Required Software**:
- Docker (any recent version)
- AWS CLI v2
- Python 3.8+
- At least 30 GB free disk space

---

## Quick Check - Can You Answer These?

1. **Instance Type**: What instance type is it? (t2.micro, t3.small, etc.)
2. **OS**: What operating system? (Ubuntu, Amazon Linux, etc.)
3. **IAM Role**: Is there an IAM role attached? What's it called?
4. **Disk Space**: How big is the root volume?
5. **Software**: Is Docker installed? AWS CLI?

---

## What I'll Do With This Information

Based on your answers, I'll:
1. ✅ Verify the instance meets minimum requirements
2. 🔧 Provide commands to install missing software if needed
3. 🔧 Help attach IAM role if missing
4. 🔧 Help expand storage if needed
5. ✅ Create setup script to configure the instance for weather pipeline
6. ✅ Update TICKET-002 to reflect using existing instance

---

**Minimum Requirements**:
- Instance Type: t3.small or better (2 vCPU, 2 GB RAM)
- Storage: 50 GB minimum (30 GB free for processing)
- IAM Role: S3 and CloudWatch access
- OS: Ubuntu 20.04+ or Amazon Linux 2023
- Region: us-east-2 ✅ (You're good!)
