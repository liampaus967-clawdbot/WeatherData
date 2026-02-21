# Weather Data Pipeline - Terraform Infrastructure

This directory contains Terraform configuration for AWS infrastructure supporting the weather data pipeline.

## What Gets Created

### S3 Storage
- **S3 Bucket**: Central storage for all weather data
- **Lifecycle Policies**: Automatic data retention management
  - raw-grib2/: 7-day retention
  - processed-cog/: 30-day retention (IA after 2 days)
  - tiles/: 3-day retention
  - metadata/: No expiration
- **Versioning**: Enabled for processed-cog rollback capability
- **Encryption**: AES256 server-side encryption
- **CORS**: Configured for web access

### IAM Roles & Policies
- **EC2 Role**: `ec2-weather-pipeline-role`
  - S3 read/write access to weather bucket
  - S3 read access to NOAA HRRR public bucket
  - CloudWatch Logs write permissions
  - CloudWatch Metrics write permissions
  - Systems Manager (SSM) access
- **Instance Profile**: `ec2-weather-pipeline-profile`

### CloudWatch
- **Log Group**: `/aws/weather-pipeline`
  - 30-day retention
  - For pipeline execution logs

## Prerequisites

### 1. AWS CLI Configuration

**Option A: AWS CLI Profile (Recommended)**
```bash
# Configure AWS CLI with your credentials
aws configure

# Or create a named profile
aws configure --profile weather-pipeline
```

**Option B: Environment Variables**
```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-2"
```

### 2. Terraform Installation

```bash
# macOS
brew install terraform

# Verify installation
terraform version
```

## Quick Start

### 1. Initialize Terraform

```bash
cd terraform/
terraform init
```

This downloads the AWS provider and sets up the backend.

### 2. Review Configuration

Edit `terraform.tfvars` to customize:
```hcl
bucket_name    = "your-unique-bucket-name"
aws_region     = "us-east-2"
aws_profile    = "default"
```

### 3. Plan Changes

```bash
terraform plan
```

Review the planned changes carefully before applying.

### 4. Apply Configuration

```bash
terraform apply
```

Type `yes` when prompted to create the infrastructure.

### 5. View Outputs

```bash
terraform output
```

## Directory Structure

```
terraform/
├── main.tf                      # Main resources (S3, IAM, CloudWatch)
├── variables.tf                 # Variable definitions
├── lifecycle-policies.tf        # S3 lifecycle rules
├── outputs.tf                   # Output values
├── terraform.tfvars             # Variable values (gitignored)
├── terraform.tfvars.example     # Example configuration
├── .gitignore                   # Protect sensitive files
└── README.md                    # This file
```

## Important Files

### Configuration Files

| File | Purpose | Committed to Git? |
|------|---------|-------------------|
| `terraform.tfvars` | Your actual configuration values | ❌ NO |
| `terraform.tfvars.example` | Example configuration | ✅ YES |
| `*.tf` | Terraform code | ✅ YES |
| `.terraform/` | Provider plugins | ❌ NO |
| `*.tfstate` | Current infrastructure state | ❌ NO |

**NEVER commit sensitive files!** The `.gitignore` is configured to protect them.

## Common Operations

### Check Current State

```bash
# Show current infrastructure
terraform show

# List all resources
terraform state list

# Show specific resource
terraform state show aws_s3_bucket.weather_data
```

### Update Infrastructure

```bash
# See what would change
terraform plan

# Apply changes
terraform apply

# Apply without confirmation prompt
terraform apply -auto-approve
```

### View Outputs

```bash
# All outputs
terraform output

# Specific output
terraform output bucket_name

# JSON format
terraform output -json
```

### Destroy Infrastructure

```bash
# Preview what will be destroyed
terraform plan -destroy

# Destroy all resources
terraform destroy

# WARNING: This will delete your S3 bucket and all data!
```

## S3 Bucket Structure

Once created, organize your data like this:

```
s3://sat-data-automation-test/
├── raw-grib2/                    # Original GRIB2 files (7-day retention)
│   └── 2026/01/09/
│       ├── hrrr.t00z.f00.grib2
│       └── ...
├── processed-cog/                # Cloud Optimized GeoTIFFs (30-day retention)
│   └── temperature/
│       └── 2026-01-09T00Z/
│           ├── f00.tif
│           └── ...
├── tiles/                        # Pre-generated tiles (3-day retention, optional)
│   └── temperature/
│       └── 2026-01-09T00Z/
│           └── f00/
│               └── {z}/{x}/{y}.png
└── metadata/                     # Current forecast metadata (no expiration)
    └── latest.json
```

## IAM Permissions

The EC2 instance profile provides:

### S3 Access
- Read/write to your weather data bucket
- Read access to NOAA HRRR public bucket (`s3://noaa-hrrr-bdp-pds/`)

### CloudWatch Access
- Create log streams and groups
- Put log events
- Put custom metrics to `WeatherPipeline` namespace

### Systems Manager
- EC2 instance management via SSM
- Session Manager access (no SSH required)

## Lifecycle Policy Details

### raw-grib2/
- **Retention**: 7 days
- **Transition**: None
- **Reason**: Temporary staging, can be re-downloaded if needed

### processed-cog/
- **Transition to Standard-IA**: After 2 days
- **Retention**: 30 days
- **Versioning**: Old versions deleted after 7 days
- **Reason**: Recent data accessed frequently, older data infrequently

### tiles/
- **Retention**: 3 days
- **Transition**: None
- **Reason**: Can be regenerated from COG files if needed

### metadata/
- **Retention**: Infinite
- **Versioning**: Old versions deleted after 30 days
- **Reason**: Always need current forecast information

## Cost Estimation

### Monthly Costs (Approximate)

| Resource | Configuration | Estimated Cost |
|----------|---------------|----------------|
| S3 Standard | ~50GB | $1.15 |
| S3 Standard-IA | ~100GB | $1.25 |
| S3 Requests | ~225K PUT, 1M GET | $1.50 |
| CloudWatch Logs | 5GB/month | $2.50 |
| **Total Infrastructure** | | **~$6.40/month** |

*Note: EC2 costs ($5-10/month) not included - will be added in TICKET-002*

## Security Best Practices

### 1. Use AWS Profiles (Not Access Keys)

❌ **BAD** - Hardcoded credentials:
```hcl
provider "aws" {
  access_key = "AKIA..."
  secret_key = "secret..."
}
```

✅ **GOOD** - AWS CLI profile:
```hcl
provider "aws" {
  profile = "default"
}
```

### 2. Never Commit Sensitive Files

The `.gitignore` protects:
- `terraform.tfvars` (your actual values)
- `*.tfstate` (contains resource IDs)
- AWS credentials

### 3. Use Least Privilege

IAM policies grant only necessary permissions for the pipeline to function.

### 4. Enable Encryption

- S3 server-side encryption (AES256)
- Encryption at rest for all data

## Troubleshooting

### Error: "bucket already exists"

**Problem**: The S3 bucket name is taken globally.

**Solution**: Change `bucket_name` in `terraform.tfvars` to something unique:
```hcl
bucket_name = "weather-data-yourname-123"
```

### Error: "Access Denied"

**Problem**: AWS credentials don't have sufficient permissions.

**Solution**: Ensure your AWS user/role has permissions for:
- S3 (create buckets, configure policies)
- IAM (create roles and policies)
- CloudWatch (create log groups)

### Error: "No valid credential sources"

**Problem**: Terraform can't find AWS credentials.

**Solution**:
```bash
# Configure AWS CLI
aws configure

# Or set environment variables
export AWS_PROFILE=default
```

### Lifecycle Policy Not Working

**Problem**: Objects aren't being deleted as expected.

**Solution**:
- Lifecycle policies can take up to 24 hours to take effect
- Check S3 console → Management → Lifecycle rules
- Verify objects have the correct prefix (e.g., `raw-grib2/`)

## Next Steps

After `terraform apply` completes:

### ✅ TICKET-001: Complete
- S3 bucket created with proper structure
- IAM roles and policies configured
- CloudWatch log group ready
- Lifecycle policies active

### 📝 TICKET-002: Provision EC2 Instance
1. Set `enable_ec2 = true` in `terraform.tfvars`
2. Run `terraform apply`
3. SSH into instance and install dependencies

### 📝 TICKET-004: Upload Test Data
```bash
# Get bucket name
BUCKET=$(terraform output -raw bucket_name)

# Upload test file
aws s3 cp test.tif s3://$BUCKET/processed-cog/temperature/test/f00.tif

# Verify upload
aws s3 ls s3://$BUCKET/processed-cog/temperature/test/
```

## Support

### Terraform Documentation
- [AWS Provider Docs](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [Terraform CLI Docs](https://developer.hashicorp.com/terraform/cli)

### Project Documentation
- See main `README.md` for pipeline overview
- See `TICKETS.md` for implementation plan

---

**Last Updated**: 2026-01-10
**Terraform Version**: >= 1.5.0
**AWS Provider Version**: ~> 5.0
