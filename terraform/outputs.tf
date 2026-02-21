# Weather Data Pipeline - Terraform Outputs

output "bucket_name" {
  description = "The name of the S3 bucket"
  value       = aws_s3_bucket.weather_data.id
}

output "bucket_arn" {
  description = "The ARN of the S3 bucket"
  value       = aws_s3_bucket.weather_data.arn
}

output "bucket_region" {
  description = "The region where the bucket is deployed"
  value       = aws_s3_bucket.weather_data.region
}

output "bucket_domain_name" {
  description = "The bucket domain name"
  value       = aws_s3_bucket.weather_data.bucket_domain_name
}

output "bucket_regional_domain_name" {
  description = "The bucket regional domain name"
  value       = aws_s3_bucket.weather_data.bucket_regional_domain_name
}

output "example_s3_upload_command" {
  description = "Example AWS CLI command to upload a file"
  value       = "aws s3 cp your-file.tif s3://${aws_s3_bucket.weather_data.id}/processed-cog/temperature/2026-01-09T12Z/f00.tif"
}

output "example_s3_list_command" {
  description = "Example AWS CLI command to list bucket contents"
  value       = "aws s3 ls s3://${aws_s3_bucket.weather_data.id}/ --recursive"
}

output "public_url_example" {
  description = "Example public URL format for accessing tiles"
  value       = "https://${aws_s3_bucket.weather_data.bucket_regional_domain_name}/tiles/temperature/2026-01-09T12Z/f00/10/285/391.png"
}

# IAM Outputs (only when EC2 is enabled)

output "ec2_iam_role_name" {
  description = "The name of the IAM role for EC2"
  value       = var.enable_ec2 ? aws_iam_role.ec2_weather_pipeline[0].name : null
}

output "ec2_iam_role_arn" {
  description = "The ARN of the IAM role for EC2"
  value       = var.enable_ec2 ? aws_iam_role.ec2_weather_pipeline[0].arn : null
}

output "ec2_instance_profile_name" {
  description = "The name of the instance profile to attach to EC2"
  value       = var.enable_ec2 ? aws_iam_instance_profile.ec2_weather_pipeline[0].name : null
}

output "ec2_instance_profile_arn" {
  description = "The ARN of the instance profile"
  value       = var.enable_ec2 ? aws_iam_instance_profile.ec2_weather_pipeline[0].arn : null
}

# EC2 Instance Outputs (only when EC2 is enabled)

output "ec2_instance_id" {
  description = "The ID of the EC2 instance"
  value       = var.enable_ec2 ? aws_instance.weather_pipeline[0].id : null
}

output "ec2_private_ip" {
  description = "The private IP address of the EC2 instance"
  value       = var.enable_ec2 ? aws_instance.weather_pipeline[0].private_ip : null
}

output "ec2_security_group_id" {
  description = "The ID of the security group"
  value       = var.enable_ec2 ? aws_security_group.weather_pipeline[0].id : null
}

output "ssm_connect_command" {
  description = "AWS CLI command to connect to the instance via Session Manager"
  value       = var.enable_ec2 ? "aws ssm start-session --target ${aws_instance.weather_pipeline[0].id} --profile fgp" : null
}
