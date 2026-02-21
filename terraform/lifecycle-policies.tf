# S3 Lifecycle Configuration
# Automatically manage data retention and storage class transitions

resource "aws_s3_bucket_lifecycle_configuration" "weather_data_lifecycle" {
  bucket = aws_s3_bucket.weather_data.id

  # Rule 1: Raw GRIB2 files - Safety net deletion after 1 day
  # Note: The pipeline script (cleanup_old_grib_files) actively removes old GRIB
  # files after each run, keeping only the most recent model run. This lifecycle
  # policy acts as a fallback in case cleanup fails.
  rule {
    id     = "expire-raw-grib2"
    status = "Enabled"

    filter {
      prefix = "raw-grib2/"
    }

    expiration {
      days = var.grib2_retention_days
    }

    # Clean up incomplete multipart uploads
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Rule 2: Processed COG files - Transition to IA after 2 days, delete after 30 days
  rule {
    id     = "lifecycle-processed-cog"
    status = "Enabled"

    filter {
      prefix = "processed-cog/"
    }

    # Transition to Infrequent Access storage class
    transition {
      days          = var.cog_ia_transition_days
      storage_class = "STANDARD_IA"
    }

    # Delete after retention period
    expiration {
      days = var.cog_retention_days
    }

    # Also apply to versioned objects
    noncurrent_version_expiration {
      noncurrent_days = 7
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Rule 3: Pre-generated tiles - Delete after 3 days (optional, if using pre-generated tiles)
  rule {
    id     = "expire-tiles"
    status = "Enabled"

    filter {
      prefix = "tiles/"
    }

    expiration {
      days = var.tiles_retention_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Rule 4: Colored COGs - Safety net deletion after 3 days
  # Note: The pipeline script (cleanup_old_cog_files) actively removes old colored
  # COGs after each run, keeping only the current date. This lifecycle policy acts
  # as a fallback in case cleanup fails.
  rule {
    id     = "expire-colored-cogs"
    status = "Enabled"

    filter {
      prefix = "colored-cogs/"
    }

    expiration {
      days = var.tiles_retention_days  # Same retention as tiles (3 days)
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Rule 5: Metadata files - Never expire, but clean up old versions
  rule {
    id     = "manage-metadata-versions"
    status = "Enabled"

    filter {
      prefix = "metadata/"
    }

    # Keep current version forever, but delete old versions
    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}
