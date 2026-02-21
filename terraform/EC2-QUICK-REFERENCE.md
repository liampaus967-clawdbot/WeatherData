# EC2 Launch Quick Reference Card

## Launch Settings (Copy-Paste Ready)

### Instance Details
```
Name: weather-pipeline-processor
Instance Type: t3.small
AMI: Ubuntu Server 22.04 LTS (64-bit x86)
```

### Network
```
VPC: Default
Auto-assign Public IP: Enable
Security Group: weather-pipeline-sg
  - Outbound: All traffic to 0.0.0.0/0
  - Inbound: SSH from My IP (optional)
```

### Storage
```
Volume: 50 GB gp3
Encrypted: Yes
Delete on Termination: Yes
```

### IAM Role
```
Role Name: EC2-WeatherPipeline-Role
Policies:
  - AmazonS3FullAccess
  - CloudWatchAgentServerPolicy
  - AmazonSSMManagedInstanceCore
```

### Cost Estimate
```
On-Demand: ~$30/month
Spot: ~$19/month (recommended)
```

## After Launch - Verification Commands

```bash
# 1. Check setup completed
cat /home/ubuntu/weather-pipeline/setup-status.txt

# 2. Verify Docker
docker --version

# 3. Test S3 access
aws s3 ls s3://sat-data-automation-test/

# 4. Check services
sudo systemctl status docker
sudo systemctl status amazon-cloudwatch-agent

# 5. View setup log (if issues)
cat /var/log/user-data.log | tail -50
```

## Quick Docker Test

```bash
# Test Herbie
docker run --rm weather-processor:latest python3 -c "from herbie import Herbie; print('Herbie OK')"
```

---

**Full Guide**: See `TICKET-002-EC2-LAUNCH-GUIDE.md`
