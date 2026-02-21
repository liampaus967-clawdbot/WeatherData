# AWS Configuration
variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-2"
}

variable "aws_profile" {
  description = "AWS CLI profile to use (recommended over access keys)"
  type        = string
  default     = "default"
}

# S3 Configuration
variable "bucket_name" {
  description = "Name of the S3 bucket (must be globally unique)"
  type        = string
  default     = "sat-data-automation-test"
}

variable "environment" {
  description = "Environment name (e.g., production, staging, development)"
  type        = string
  default     = "production"
}

# S3 Lifecycle Policies
variable "grib2_retention_days" {
  description = "Number of days to retain raw GRIB2 files (safety net - pipeline cleans up old files automatically)"
  type        = number
  default     = 1
}

variable "cog_retention_days" {
  description = "Number of days to retain processed COG files"
  type        = number
  default     = 30
}

variable "cog_ia_transition_days" {
  description = "Number of days before transitioning COG to Infrequent Access (minimum 30 for STANDARD_IA)"
  type        = number
  default     = 30 # AWS minimum for STANDARD_IA
}

variable "tiles_retention_days" {
  description = "Number of days to retain pre-generated tiles"
  type        = number
  default     = 3
}

# CloudFront Configuration
variable "enable_cloudfront" {
  description = "Enable CloudFront CDN for tile delivery"
  type        = bool
  default     = false
}

variable "cloudfront_price_class" {
  description = "CloudFront price class (PriceClass_All, PriceClass_200, PriceClass_100)"
  type        = string
  default     = "PriceClass_100" # US, Canada, Europe
}

# EC2 Configuration
variable "ec2_instance_type" {
  description = "EC2 instance type for processing"
  type        = string
  default     = "t3.small"
}

variable "ec2_key_name" {
  description = "SSH key pair name for EC2 instance"
  type        = string
  default     = "" # Leave empty if not using SSH
}

variable "enable_ec2" {
  description = "Enable EC2 instance provisioning"
  type        = bool
  default     = false # Set to true when ready for TICKET-002
}

# CloudWatch Configuration
variable "log_retention_days" {
  description = "Number of days to retain CloudWatch logs"
  type        = number
  default     = 30
}

# Tags
variable "project_tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default = {
    Project     = "Weather Data Pipeline"
    ManagedBy   = "Terraform"
    Repository  = "WeatherData"
  }
}
