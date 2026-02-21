# Weather Data Pipeline - Terraform Configuration
# This file configures AWS infrastructure for the weather data pipeline:
# - S3 bucket with lifecycle policies, versioning, encryption, and CORS
# - IAM role and instance profile for EC2 (when enable_ec2 = true)
# - Security group (no inbound - SSM only)
# - EC2 instance with Docker, AWS CLI, Python pre-installed

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  default_tags {
    tags = var.project_tags
  }
}

###################
# S3 Bucket
###################

resource "aws_s3_bucket" "weather_data" {
  bucket = var.bucket_name

  tags = {
    Name        = "Weather Data Bucket"
    Environment = var.environment
    Purpose     = "Store HRRR weather forecast data and processed tiles"
  }
}

# Allow public access for serving tiles
resource "aws_s3_bucket_public_access_block" "weather_data_pab" {
  bucket = aws_s3_bucket.weather_data.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

# Bucket policy for public read access to tiles
resource "aws_s3_bucket_policy" "weather_data_policy" {
  bucket = aws_s3_bucket.weather_data.id

  depends_on = [aws_s3_bucket_public_access_block.weather_data_pab]

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "PublicReadGetObject"
        Effect    = "Allow"
        Principal = "*"
        Action    = "s3:GetObject"
        Resource  = "${aws_s3_bucket.weather_data.arn}/*"
      }
    ]
  })
}

# Enable versioning
resource "aws_s3_bucket_versioning" "weather_data_versioning" {
  bucket = aws_s3_bucket.weather_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "weather_data_encryption" {
  bucket = aws_s3_bucket.weather_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# CORS configuration
resource "aws_s3_bucket_cors_configuration" "weather_data_cors" {
  bucket = aws_s3_bucket.weather_data.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["*"]
    expose_headers  = ["ETag", "Content-Length"]
    max_age_seconds = 3000
  }
}

###################
# IAM Role for EC2
###################

# IAM role for EC2 weather pipeline
resource "aws_iam_role" "ec2_weather_pipeline" {
  count = var.enable_ec2 ? 1 : 0

  name        = "EC2-WeatherPipeline-Role"
  description = "IAM role for weather data pipeline EC2 instance"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name = "EC2-WeatherPipeline-Role"
  }
}

# Instance profile for EC2
resource "aws_iam_instance_profile" "ec2_weather_pipeline" {
  count = var.enable_ec2 ? 1 : 0

  name = "EC2-WeatherPipeline-Profile"
  role = aws_iam_role.ec2_weather_pipeline[0].name
}

# Custom policy for S3 access to weather bucket
resource "aws_iam_role_policy" "s3_weather_bucket_access" {
  count = var.enable_ec2 ? 1 : 0

  name = "S3-WeatherBucket-Access"
  role = aws_iam_role.ec2_weather_pipeline[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = aws_s3_bucket.weather_data.arn
      },
      {
        Sid    = "ReadWriteObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${aws_s3_bucket.weather_data.arn}/*"
      },
      {
        Sid    = "ReadNOAAPublicBuckets"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetObject"
        ]
        Resource = [
          "arn:aws:s3:::noaa-hrrr-bdp-pds",
          "arn:aws:s3:::noaa-hrrr-bdp-pds/*",
          "arn:aws:s3:::noaa-gfs-bdp-pds",
          "arn:aws:s3:::noaa-gfs-bdp-pds/*",
          "arn:aws:s3:::noaa-rap-pds",
          "arn:aws:s3:::noaa-rap-pds/*"
        ]
      }
    ]
  })
}

# Attach AWS managed policy for CloudWatch Agent
resource "aws_iam_role_policy_attachment" "cloudwatch_agent" {
  count = var.enable_ec2 ? 1 : 0

  role       = aws_iam_role.ec2_weather_pipeline[0].name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

# Attach AWS managed policy for SSM (Session Manager)
resource "aws_iam_role_policy_attachment" "ssm_managed_instance" {
  count = var.enable_ec2 ? 1 : 0

  role       = aws_iam_role.ec2_weather_pipeline[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# CloudWatch Logs policy for custom logging
resource "aws_iam_role_policy" "cloudwatch_logs" {
  count = var.enable_ec2 ? 1 : 0

  name = "CloudWatch-Logs-Access"
  role = aws_iam_role.ec2_weather_pipeline[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:*:log-group:/aws/weather-pipeline:*"
      }
    ]
  })
}

###################
# Security Group
###################

resource "aws_security_group" "weather_pipeline" {
  count = var.enable_ec2 ? 1 : 0

  name        = "weather-pipeline-sg"
  description = "Security group for weather data pipeline EC2 instance"

  # No inbound rules - using SSM Session Manager only (more secure)

  # Outbound: Allow all (needed for AWS APIs, NOAA data downloads, Docker Hub)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound traffic"
  }

  tags = {
    Name = "weather-pipeline-sg"
  }
}

###################
# EC2 Instance
###################

# Get latest Ubuntu 22.04 AMI
data "aws_ami" "ubuntu" {
  count = var.enable_ec2 ? 1 : 0

  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# EC2 Instance
resource "aws_instance" "weather_pipeline" {
  count = var.enable_ec2 ? 1 : 0

  ami                    = data.aws_ami.ubuntu[0].id
  instance_type          = var.ec2_instance_type
  iam_instance_profile   = aws_iam_instance_profile.ec2_weather_pipeline[0].name
  vpc_security_group_ids = [aws_security_group.weather_pipeline[0].id]

  # Storage
  root_block_device {
    volume_size           = 50
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true

    tags = {
      Name = "weather-pipeline-root"
    }
  }

  # User data bootstrap script
  user_data = base64encode(<<-EOF
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
    cat > /opt/aws/amazon-cloudwatch-agent/etc/config.json <<'CWEOF'
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
            "resources": ["*"]
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
    CWEOF

    # Start CloudWatch Agent
    /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
        -a fetch-config \
        -m ec2 \
        -s \
        -c file:/opt/aws/amazon-cloudwatch-agent/etc/config.json

    # Configure logrotate for pipeline logs
    cat > /etc/logrotate.d/weather-pipeline <<'LREOF'
    /var/log/weather-pipeline.log {
        daily
        rotate 7
        compress
        delaycompress
        missingok
        notifempty
        create 0644 ubuntu ubuntu
    }
    LREOF

    # Create status file
    cat > /home/ubuntu/weather-pipeline/setup-status.txt <<STATUSEOF
    Setup completed: $(date)
    Docker version: $(docker --version)
    AWS CLI version: $(aws --version)
    Python version: $(python3 --version)

    Next steps:
    1. Clone weather data repository
    2. Build Docker image
    3. Configure cron job
    4. Test pipeline
    STATUSEOF

    chown ubuntu:ubuntu /home/ubuntu/weather-pipeline/setup-status.txt

    echo "========================================="
    echo "Setup Complete!"
    echo "Completed: $(date)"
    echo "========================================="

    # Reboot to ensure all services start properly
    echo "Rebooting in 10 seconds..."
    sleep 10
    reboot
  EOF
  )

  tags = {
    Name        = "weather-pipeline-processor"
    Environment = var.environment
  }

  # Wait for instance profile to be ready
  depends_on = [
    aws_iam_instance_profile.ec2_weather_pipeline,
    aws_iam_role_policy.s3_weather_bucket_access,
    aws_iam_role_policy_attachment.cloudwatch_agent,
    aws_iam_role_policy_attachment.ssm_managed_instance
  ]
}
