###################
# CloudWatch Log Groups
# Part of TICKET-016: Set Up CloudWatch Monitoring
###################

# Log group for pipeline orchestration
resource "aws_cloudwatch_log_group" "pipeline" {
  name              = "/weather-pipeline/pipeline"
  retention_in_days = var.log_retention_days

  tags = {
    Name    = "Weather Pipeline Logs"
    Purpose = "Pipeline orchestration logs"
  }
}

# Log group for HRRR download operations
resource "aws_cloudwatch_log_group" "download" {
  name              = "/weather-pipeline/download"
  retention_in_days = var.log_retention_days

  tags = {
    Name    = "HRRR Download Logs"
    Purpose = "Data download operation logs"
  }
}

# Log group for GDAL processing operations
resource "aws_cloudwatch_log_group" "processing" {
  name              = "/weather-pipeline/processing"
  retention_in_days = var.log_retention_days

  tags = {
    Name    = "Processing Logs"
    Purpose = "GRIB2 to COG processing logs"
  }
}

# Log group for tile generation
resource "aws_cloudwatch_log_group" "tiles" {
  name              = "/weather-pipeline/tiles"
  retention_in_days = var.log_retention_days

  tags = {
    Name    = "Tile Generation Logs"
    Purpose = "XYZ tile generation logs"
  }
}

# Log group for S3 operations
resource "aws_cloudwatch_log_group" "s3_upload" {
  name              = "/weather-pipeline/s3-upload"
  retention_in_days = var.log_retention_days

  tags = {
    Name    = "S3 Upload Logs"
    Purpose = "S3 sync and upload logs"
  }
}

###################
# Metric Filters for Error Detection
###################

# Filter for ERROR level logs in pipeline
resource "aws_cloudwatch_log_metric_filter" "pipeline_errors" {
  name           = "PipelineErrors"
  pattern        = "[timestamp, level=ERROR, ...]"
  log_group_name = aws_cloudwatch_log_group.pipeline.name

  metric_transformation {
    name      = "PipelineErrors"
    namespace = "WeatherPipeline"
    value     = "1"
  }
}

# Filter for WARN level logs in pipeline
resource "aws_cloudwatch_log_metric_filter" "pipeline_warnings" {
  name           = "PipelineWarnings"
  pattern        = "[timestamp, level=WARN, ...]"
  log_group_name = aws_cloudwatch_log_group.pipeline.name

  metric_transformation {
    name      = "PipelineWarnings"
    namespace = "WeatherPipeline"
    value     = "1"
  }
}

# Filter for download failures
resource "aws_cloudwatch_log_metric_filter" "download_errors" {
  name           = "DownloadErrors"
  pattern        = "?ERROR ?FAILED ?Exception ?Traceback"
  log_group_name = aws_cloudwatch_log_group.download.name

  metric_transformation {
    name      = "DownloadErrors"
    namespace = "WeatherPipeline"
    value     = "1"
  }
}

# Filter for processing failures
resource "aws_cloudwatch_log_metric_filter" "processing_errors" {
  name           = "ProcessingErrors"
  pattern        = "?ERROR ?FAILED ?Exception ?Traceback"
  log_group_name = aws_cloudwatch_log_group.processing.name

  metric_transformation {
    name      = "ProcessingErrors"
    namespace = "WeatherPipeline"
    value     = "1"
  }
}

# Filter for tile generation failures
resource "aws_cloudwatch_log_metric_filter" "tile_errors" {
  name           = "TileGenerationErrors"
  pattern        = "?ERROR ?FAILED ?Exception ?Traceback"
  log_group_name = aws_cloudwatch_log_group.tiles.name

  metric_transformation {
    name      = "TileGenerationErrors"
    namespace = "WeatherPipeline"
    value     = "1"
  }
}

# Filter for S3 upload failures
resource "aws_cloudwatch_log_metric_filter" "s3_upload_errors" {
  name           = "S3UploadErrors"
  pattern        = "?ERROR ?FAILED ?\"upload failed\""
  log_group_name = aws_cloudwatch_log_group.s3_upload.name

  metric_transformation {
    name      = "S3UploadErrors"
    namespace = "WeatherPipeline"
    value     = "1"
  }
}

# Filter for successful pipeline completions
resource "aws_cloudwatch_log_metric_filter" "pipeline_success" {
  name           = "PipelineSuccess"
  pattern        = "[timestamp, level=SUCCESS, msg=\"Pipeline completed*\"]"
  log_group_name = aws_cloudwatch_log_group.pipeline.name

  metric_transformation {
    name      = "PipelineSuccess"
    namespace = "WeatherPipeline"
    value     = "1"
  }
}

###################
# CloudWatch Dashboard
# Part of TICKET-018: Create CloudWatch Dashboard
###################

resource "aws_cloudwatch_dashboard" "weather_pipeline" {
  dashboard_name = "WeatherPipeline"

  dashboard_body = jsonencode({
    widgets = [
      # Row 1: Header and Key Metrics
      {
        type   = "text"
        x      = 0
        y      = 0
        width  = 24
        height = 1
        properties = {
          markdown = "# Weather Data Pipeline Dashboard\nReal-time monitoring of HRRR weather data processing pipeline"
        }
      },
      # Data Age - Single Value
      {
        type   = "metric"
        x      = 0
        y      = 1
        width  = 6
        height = 4
        properties = {
          metrics = [
            ["WeatherPipeline", "DataAge", "Pipeline", "HRRR", { stat = "Average", period = 300 }]
          ]
          title  = "Data Age (minutes)"
          view   = "singleValue"
          region = var.aws_region
          stat   = "Average"
          period = 300
        }
      },
      # Processing Time - Single Value
      {
        type   = "metric"
        x      = 6
        y      = 1
        width  = 6
        height = 4
        properties = {
          metrics = [
            ["WeatherPipeline", "ProcessingTime", "Pipeline", "HRRR", { stat = "Average", period = 300 }]
          ]
          title  = "Last Processing Time (sec)"
          view   = "singleValue"
          region = var.aws_region
          stat   = "Average"
          period = 300
        }
      },
      # Success Count - Single Value
      {
        type   = "metric"
        x      = 12
        y      = 1
        width  = 6
        height = 4
        properties = {
          metrics = [
            ["WeatherPipeline", "Success", "Pipeline", "HRRR", { stat = "Sum", period = 86400 }]
          ]
          title  = "Successful Runs (24h)"
          view   = "singleValue"
          region = var.aws_region
          stat   = "Sum"
          period = 86400
        }
      },
      # Error Count - Single Value
      {
        type   = "metric"
        x      = 18
        y      = 1
        width  = 6
        height = 4
        properties = {
          metrics = [
            ["WeatherPipeline", "Errors", "Pipeline", "HRRR", { stat = "Sum", period = 86400 }]
          ]
          title  = "Errors (24h)"
          view   = "singleValue"
          region = var.aws_region
          stat   = "Sum"
          period = 86400
        }
      },
      # Row 2: Time Series Charts
      # Data Age Over Time
      {
        type   = "metric"
        x      = 0
        y      = 5
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["WeatherPipeline", "DataAge", "Pipeline", "HRRR"]
          ]
          title  = "Data Age Over Time (24h)"
          view   = "timeSeries"
          region = var.aws_region
          stat   = "Average"
          period = 300
          yAxis = {
            left = {
              min   = 0
              label = "Minutes"
            }
          }
        }
      },
      # Processing Time Trend
      {
        type   = "metric"
        x      = 12
        y      = 5
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["WeatherPipeline", "ProcessingTime", "Pipeline", "HRRR"]
          ]
          title  = "Processing Time Trend (24h)"
          view   = "timeSeries"
          region = var.aws_region
          stat   = "Average"
          period = 300
          yAxis = {
            left = {
              min   = 0
              label = "Seconds"
            }
          }
        }
      },
      # Row 3: File Processing Metrics
      # Files Downloaded
      {
        type   = "metric"
        x      = 0
        y      = 11
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["WeatherPipeline", "FilesDownloaded", "Pipeline", "HRRR", "Step", "Download"]
          ]
          title  = "Files Downloaded Per Run"
          view   = "timeSeries"
          region = var.aws_region
          stat   = "Sum"
          period = 3600
          yAxis = {
            left = {
              min = 0
            }
          }
        }
      },
      # Files Processed
      {
        type   = "metric"
        x      = 8
        y      = 11
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["WeatherPipeline", "FilesProcessed", "Pipeline", "HRRR", "Step", "Processing"]
          ]
          title  = "COG Files Processed Per Run"
          view   = "timeSeries"
          region = var.aws_region
          stat   = "Sum"
          period = 3600
          yAxis = {
            left = {
              min = 0
            }
          }
        }
      },
      # Tiles Generated
      {
        type   = "metric"
        x      = 16
        y      = 11
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["WeatherPipeline", "TilesGenerated", "Pipeline", "HRRR", "Step", "TileGeneration"]
          ]
          title  = "Tiles Generated Per Run"
          view   = "timeSeries"
          region = var.aws_region
          stat   = "Sum"
          period = 3600
          yAxis = {
            left = {
              min = 0
            }
          }
        }
      },
      # Row 4: Step Duration and Errors
      # Step Duration Breakdown
      {
        type   = "metric"
        x      = 0
        y      = 17
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["WeatherPipeline", "StepDuration", "Pipeline", "HRRR", "Step", "Download", { label = "Download" }],
            [".", ".", ".", ".", ".", "Processing", { label = "Processing" }],
            [".", ".", ".", ".", ".", "Colormap", { label = "Colormap" }],
            [".", ".", ".", ".", ".", "TileGeneration", { label = "Tile Generation" }],
            [".", ".", ".", ".", ".", "S3Upload", { label = "S3 Upload" }],
            [".", ".", ".", ".", ".", "Metadata", { label = "Metadata" }]
          ]
          title  = "Step Duration Breakdown"
          view   = "timeSeries"
          region = var.aws_region
          stat   = "Average"
          period = 3600
          stacked = true
          yAxis = {
            left = {
              min   = 0
              label = "Seconds"
            }
          }
        }
      },
      # Error Counts by Type
      {
        type   = "metric"
        x      = 12
        y      = 17
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["WeatherPipeline", "PipelineErrors", { label = "Pipeline Errors" }],
            [".", "DownloadErrors", { label = "Download Errors" }],
            [".", "ProcessingErrors", { label = "Processing Errors" }],
            [".", "TileGenerationErrors", { label = "Tile Errors" }],
            [".", "S3UploadErrors", { label = "S3 Upload Errors" }]
          ]
          title  = "Errors by Type (24h)"
          view   = "timeSeries"
          region = var.aws_region
          stat   = "Sum"
          period = 3600
          yAxis = {
            left = {
              min = 0
            }
          }
        }
      },
      # Row 5: EC2 Metrics
      {
        type   = "text"
        x      = 0
        y      = 23
        width  = 24
        height = 1
        properties = {
          markdown = "## EC2 Instance Metrics"
        }
      },
      # CPU Utilization
      {
        type   = "metric"
        x      = 0
        y      = 24
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["WeatherPipeline/EC2", "cpu_usage_user", { label = "User" }],
            [".", "cpu_usage_system", { label = "System" }]
          ]
          title  = "CPU Utilization"
          view   = "timeSeries"
          region = var.aws_region
          stat   = "Average"
          period = 300
          yAxis = {
            left = {
              min   = 0
              max   = 100
              label = "Percent"
            }
          }
        }
      },
      # Memory Utilization
      {
        type   = "metric"
        x      = 8
        y      = 24
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["WeatherPipeline/EC2", "mem_used_percent"]
          ]
          title  = "Memory Utilization"
          view   = "timeSeries"
          region = var.aws_region
          stat   = "Average"
          period = 300
          yAxis = {
            left = {
              min   = 0
              max   = 100
              label = "Percent"
            }
          }
        }
      },
      # Disk Utilization
      {
        type   = "metric"
        x      = 16
        y      = 24
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["WeatherPipeline/EC2", "disk_used_percent", "path", "/", { label = "Root (/)" }],
            [".", ".", ".", "/home", { label = "Home (/home)" }]
          ]
          title  = "Disk Utilization"
          view   = "timeSeries"
          region = var.aws_region
          stat   = "Average"
          period = 300
          yAxis = {
            left = {
              min   = 0
              max   = 100
              label = "Percent"
            }
          }
        }
      },
      # Row 6: Log Insights
      {
        type   = "text"
        x      = 0
        y      = 30
        width  = 24
        height = 1
        properties = {
          markdown = "## Recent Pipeline Activity"
        }
      },
      # Recent Errors Log Query
      {
        type   = "log"
        x      = 0
        y      = 31
        width  = 12
        height = 6
        properties = {
          query  = "SOURCE '/weather-pipeline/pipeline' | fields @timestamp, @message | filter @message like /ERROR/ | sort @timestamp desc | limit 20"
          region = var.aws_region
          title  = "Recent Errors"
          view   = "table"
        }
      },
      # Recent Success Log Query
      {
        type   = "log"
        x      = 12
        y      = 31
        width  = 12
        height = 6
        properties = {
          query  = "SOURCE '/weather-pipeline/pipeline' | fields @timestamp, @message | filter @message like /SUCCESS|completed/ | sort @timestamp desc | limit 20"
          region = var.aws_region
          title  = "Recent Successes"
          view   = "table"
        }
      }
    ]
  })
}

###################
# Outputs
###################

output "dashboard_url" {
  description = "URL to the CloudWatch Dashboard"
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.weather_pipeline.dashboard_name}"
}

output "log_group_arns" {
  description = "ARNs of created log groups"
  value = {
    pipeline   = aws_cloudwatch_log_group.pipeline.arn
    download   = aws_cloudwatch_log_group.download.arn
    processing = aws_cloudwatch_log_group.processing.arn
    tiles      = aws_cloudwatch_log_group.tiles.arn
    s3_upload  = aws_cloudwatch_log_group.s3_upload.arn
  }
}

output "log_group_names" {
  description = "Names of created log groups"
  value = {
    pipeline   = aws_cloudwatch_log_group.pipeline.name
    download   = aws_cloudwatch_log_group.download.name
    processing = aws_cloudwatch_log_group.processing.name
    tiles      = aws_cloudwatch_log_group.tiles.name
    s3_upload  = aws_cloudwatch_log_group.s3_upload.name
  }
}
