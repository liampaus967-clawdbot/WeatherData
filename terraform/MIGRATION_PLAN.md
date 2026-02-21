# Terraform Migration Plan

## Current Situation

You have an **existing S3 bucket** managed by the old Terraform configuration in `terraform_s3/`:
- **Bucket Name**: `sat-data-automation-test`
- **Region**: `us-east-2`
- **Status**: Active and deployed

The new Terraform configuration in `terraform/` is ready but needs to either:
1. **Import the existing bucket** (recommended), OR
2. **Create a new bucket** with a different name

## Recommended Approach: Import Existing Bucket

### Why Import?

✅ **Pros**:
- Keep existing bucket and any data
- No downtime
- Simpler migration
- Same bucket name

❌ **Cons**:
- Terraform will update bucket settings (lifecycle policies, CORS, etc.)
- Need to run import command

### Import Steps

```bash
cd /Users/liampaus/Documents/GIT/WeatherData/terraform

# Import the existing S3 bucket
terraform import aws_s3_bucket.weather_data sat-data-automation-test

# This tells Terraform: "this bucket already exists, manage it from now on"
```

After import, Terraform will:
1. ✅ Manage the existing bucket
2. ✅ Add lifecycle policies (auto-delete old data)
3. ✅ Enable versioning
4. ✅ Add encryption
5. ✅ Configure CORS
6. ✅ Create IAM roles and CloudWatch log group

## Alternative: New Bucket with Different Name

If you prefer a fresh start:

```bash
# Edit terraform/terraform.tfvars
bucket_name = "weather-data-pipeline-prod"  # New unique name

# Then apply
terraform apply
```

This creates a completely new bucket alongside the old one.

## What Happens When We Apply?

### Resources to be Created (14 total)

1. **S3 Bucket** (or import existing)
   - `aws_s3_bucket.weather_data`

2. **S3 Configurations**
   - Public access block
   - Versioning (enabled)
   - Server-side encryption (AES256)
   - CORS rules
   - Lifecycle policies (4 rules)

3. **IAM Resources**
   - EC2 role: `ec2-weather-pipeline-role`
   - Instance profile: `ec2-weather-pipeline-profile`
   - S3 access policy
   - CloudWatch logs policy
   - SSM managed instance policy

4. **CloudWatch**
   - Log group: `/aws/weather-pipeline`

### No EC2 Instance Yet

Note: `enable_ec2 = false` in `terraform.tfvars`, so **no EC2 instance will be created yet**.
We'll do that in TICKET-002.

## Step-by-Step Migration

### Option 1: Import Existing Bucket (Recommended)

```bash
cd /Users/liampaus/Documents/GIT/WeatherData/terraform

# Step 1: Import existing bucket
terraform import aws_s3_bucket.weather_data sat-data-automation-test

# Step 2: Preview changes
terraform plan

# Step 3: Apply (creates IAM, lifecycle policies, etc.)
terraform apply

# Step 4: Verify
terraform show
aws s3 ls s3://sat-data-automation-test/
```

### Option 2: Fresh Start with New Bucket

```bash
cd /Users/liampaus/Documents/GIT/WeatherData/terraform

# Step 1: Edit terraform.tfvars
# Change: bucket_name = "weather-data-yourname-2026"

# Step 2: Apply
terraform apply

# Step 3: (Optional) Migrate data from old bucket
aws s3 sync s3://sat-data-automation-test/ s3://weather-data-yourname-2026/

# Step 4: (Optional) Delete old bucket
cd ../terraform_s3
terraform destroy
```

## Impact Analysis

### Will Existing Data Be Affected?

#### If Importing Existing Bucket:
- ✅ **No data loss**
- ✅ Existing files remain untouched
- ⚠️  Lifecycle policies will apply going forward (deletes files after retention period)
- ⚠️  Versioning will be enabled (keeps old versions of files)

#### If Creating New Bucket:
- Old bucket data remains in `sat-data-automation-test`
- New bucket starts empty
- You can migrate data manually if needed

### Will This Cost Money?

**New Monthly Costs** (approximate):
- S3 storage: ~$2-5/month (depending on data volume)
- S3 requests: ~$1-2/month
- CloudWatch Logs: ~$2/month
- **Total**: ~$5-10/month

**No EC2 costs yet** (TICKET-002)

## Verification Checklist

After applying Terraform:

```bash
# 1. Check S3 bucket
aws s3 ls s3://sat-data-automation-test/
terraform output bucket_name

# 2. Check IAM role
aws iam get-role --role-name ec2-weather-pipeline-role
terraform output ec2_iam_role_name

# 3. Check lifecycle policies
aws s3api get-bucket-lifecycle-configuration \
  --bucket sat-data-automation-test

# 4. Check CloudWatch log group
aws logs describe-log-groups --log-group-name-prefix /aws/weather-pipeline
```

## What to Do About terraform_s3/?

After successful migration to `terraform/`:

### Option A: Keep as Backup
```bash
# Rename to indicate it's deprecated
mv terraform_s3 terraform_s3_OLD_DO_NOT_USE
```

### Option B: Remove Old State
```bash
# If you imported the bucket to new terraform/
cd terraform_s3
terraform destroy  # This will NOT delete the bucket since it's managed by terraform/ now
cd ..
rm -rf terraform_s3
```

### Option C: Destroy Everything and Start Fresh
```bash
cd terraform_s3
terraform destroy  # This WILL delete the bucket
cd ../terraform
terraform apply    # Creates everything fresh
```

## Security Notes

### Credentials

The new `terraform/` directory uses **AWS CLI profiles** instead of hardcoded credentials:

```hcl
# OLD (terraform_s3/) - INSECURE
provider "aws" {
  access_key = "AKIA..."  # ❌ Hardcoded
  secret_key = "secret"   # ❌ Hardcoded
}

# NEW (terraform/) - SECURE
provider "aws" {
  profile = "default"     # ✅ Uses ~/.aws/credentials
}
```

**Action Required**: See `SECURITY_NOTICE.md` for rotating compromised credentials.

## Rollback Plan

If something goes wrong:

```bash
# View current state
terraform show

# Revert to previous state
terraform state pull > backup.tfstate
terraform state push backup.tfstate

# Or destroy and start over
terraform destroy
```

The old `terraform_s3/` state is backed up in `terraform_s3/terraform.tfstate.backup`.

## Next Steps

1. **Choose migration approach** (import vs new bucket)
2. **Run commands** from chosen option above
3. **Verify infrastructure** using checklist
4. **Mark TICKET-001 as complete**
5. **Proceed to TICKET-002** (EC2 provisioning)

---

**Ready to proceed?** Let me know which option you prefer:
- **Option 1**: Import existing bucket (safer, recommended)
- **Option 2**: Create new bucket (fresh start)

I can help with either approach!
