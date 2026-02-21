# Automated Weather Data Pipeline

Real-time weather data ingestion, processing, and tile generation pipeline for Mapbox web applications.

## Overview

This project automates the process of downloading HRRR (High-Resolution Rapid Refresh) weather forecast data, processing it with GDAL, generating map tiles, and serving them for visualization in a Mapbox web application.

### Key Features

- **Hourly automated updates** of weather forecast data
- **12-hour forecast horizon** (F00-F12 forecast hours)
- **HRRR model data** from NOAA's AWS S3 bucket
- **Cloud Optimized GeoTIFF (COG)** processing for efficient storage
- **Automated tile generation** for web map display
- **S3 storage** with lifecycle management
- **CloudFront CDN** for fast global delivery

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     HRRR Data Source                            │
│              s3://noaa-hrrr-bdp-pds (AWS)                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ Download (hourly, HH:15)
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  EC2 Processing Instance                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  Download    │→ │  GDAL        │→ │  Upload      │         │
│  │  Script      │  │  Processing  │  │  to S3       │         │
│  │  (Python)    │  │  (Docker)    │  │  (AWS CLI)   │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    S3 Storage Buckets                           │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ raw-grib2/        (7-day retention)                     │   │
│  │ processed-cog/    (30-day retention, IA after 2 days)   │   │
│  │ tiles/            (3-day retention, optional)           │   │
│  │ metadata/         (latest.json for web app)             │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                CloudFront CDN                                   │
│          (Cache tiles, serve to web app)                        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Mapbox Web Application                          │
│          (Display weather data on interactive map)              │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow

### Timeline for Each Model Run

```
12:00 UTC → HRRR model run starts at NOAA
15:00 UTC → Data available on S3 (noaa-hrrr-bdp-pds)
15:15 UTC → Pipeline downloads 13 forecast hours (F00-F12)
15:20 UTC → GDAL processing begins (band extraction, reprojection, COG creation)
15:35 UTC → Processing complete, upload to your S3 bucket
15:36 UTC → CloudFront cache updated
15:40 UTC → Users see new forecast in web app
```

### Hourly Schedule

- **00:15 UTC, 01:15 UTC, ..., 23:15 UTC**: Pipeline runs 24 times per day
- Each run processes 13 forecast hours (F00-F12) = 312 files per day
- Total data processed: ~50GB GRIB2 → ~5GB COG per day

## Directory Structure

```
weather-pipeline/
├── README.md                          # This file
├── TICKETS.md                         # Implementation tickets
├── docker/
│   ├── Dockerfile                     # GDAL processing container
│   └── requirements.txt               # Python dependencies
├── scripts/
│   ├── download_hrrr.py               # Download from NOAA S3
│   ├── process_grib.py                # GDAL batch processing
│   ├── upload_to_s3.py                # Upload processed files
│   ├── generate_metadata.py           # Create latest.json
│   └── pipeline.sh                    # Main orchestration script
├── terraform/
│   ├── main.tf                        # S3 buckets, IAM, CloudFront
│   ├── variables.tf                   # Configuration variables
│   ├── outputs.tf                     # Resource outputs
│   └── lifecycle-policies.tf          # S3 lifecycle rules
├── web/
│   ├── index.html                     # Mapbox web app
│   ├── weather-map.js                 # Map initialization and updates
│   └── styles.css                     # UI styling
└── monitoring/
    ├── cloudwatch-alarms.tf           # Monitoring and alerts
    └── dashboard.json                 # CloudWatch dashboard config
```

## Technology Stack

### Data Processing

- **GDAL 3.8+**: Geospatial data processing
- **Python 3.10+**: Automation scripts
- **Docker**: Containerized GDAL environment
- **boto3**: AWS SDK for Python

### Infrastructure

- **AWS EC2**: Processing instance (t3.small spot)
- **AWS S3**: Object storage for GRIB2, COG, and tiles
- **AWS CloudFront**: CDN for tile delivery
- **Terraform**: Infrastructure as Code

### Web Application

- **Mapbox GL JS**: Interactive map rendering
- **JavaScript**: Web app logic
- **CloudFront**: Static website hosting + API

## Storage Organization

### S3 Bucket Structure

```
s3://your-weather-bucket/

raw-grib2/                                    # Original GRIB2 files
└── 2026/
    └── 01/
        └── 09/
            ├── hrrr.t00z.f00.grib2          # 155MB per file
            ├── hrrr.t00z.f01.grib2
            └── ...f12.grib2
            └── hrrr.t01z.f00.grib2
            └── ...

processed-cog/                                # Cloud Optimized GeoTIFFs
└── temperature/
    ├── 2026-01-09T00Z/
    │   ├── f00.tif                          # 2-3MB per file
    │   ├── f01.tif
    │   └── ...f12.tif
    └── 2026-01-09T01Z/
        └── ...

tiles/                                        # Optional: Pre-generated tiles
└── temperature/
    └── 2026-01-09T00Z/
        └── f00/
            ├── 0/0/0.png
            ├── 1/0/0.png
            └── ...

metadata/
└── latest.json                               # Current forecast metadata
```

### S3 Lifecycle Policies

| Prefix           | Transition           | Deletion |
| ---------------- | -------------------- | -------- |
| `raw-grib2/`     | None                 | 7 days   |
| `processed-cog/` | Standard-IA (2 days) | 30 days  |
| `tiles/`         | None                 | 3 days   |
| `metadata/`      | None                 | Never    |

## Cost Estimate

### Monthly Costs

| Service                  | Configuration                      | Cost              |
| ------------------------ | ---------------------------------- | ----------------- |
| EC2 t3.small spot        | 24 hours/day, 730 hours/month      | $5                |
| S3 Standard storage      | ~100GB (7 days GRIB2, current COG) | $2.30             |
| S3 Standard-IA           | ~200GB (older COG files)           | $2.50             |
| S3 PUT requests          | 312 files × 24 runs × 30 days      | $1.12             |
| S3 data transfer         | ~50GB/day egress to CloudFront     | $0 (free)         |
| CloudFront data transfer | ~100GB/month                       | $8.50             |
| CloudFront requests      | ~10M requests/month                | $1.00             |
| **Total**                |                                    | **~$20-25/month** |

_Note: Costs will scale with traffic to your web application_

## Prerequisites

### Local Development

- Python 3.10+
- GDAL 3.6+
- Docker
- AWS CLI
- Terraform 1.5+

### AWS Resources

- AWS account with appropriate IAM permissions
- S3 bucket for storing data
- EC2 instance (t3.small recommended)
- CloudFront distribution (optional, for CDN)

### API Access

- NOAA HRRR data (publicly accessible, no API key needed)

## Quick Start

### 1. Clone and Setup

```bash
# Navigate to project directory
cd /path/to/weather-pipeline

# Install Python dependencies
pip install -r docker/requirements.txt

# Configure AWS credentials
aws configure
```

### 2. Deploy Infrastructure

```bash
cd terraform/
terraform init
terraform plan
terraform apply
```

### 3. Build Docker Image

```bash
cd docker/
docker build -t weather-processor:latest .
```

### 4. Test Pipeline Manually

```bash
# Download HRRR data
python3 scripts/download_hrrr.py --test

# Process with GDAL
docker run --rm \
  -v /tmp:/tmp \
  -v ~/.aws:/root/.aws \
  weather-processor:latest \
  python3 /app/process_grib.py --test

# Upload to S3
python3 scripts/upload_to_s3.py --test
```

### 5. Deploy Automated Pipeline

```bash
# Copy scripts to EC2
scp -r scripts/ ubuntu@your-ec2-ip:/home/ubuntu/weather-pipeline/

# SSH to EC2 and set up cron
ssh ubuntu@your-ec2-ip
crontab -e

# Add this line:
15 * * * * /home/ubuntu/weather-pipeline/scripts/pipeline.sh >> /var/log/weather-pipeline.log 2>&1
```

## Configuration

### Environment Variables

```bash
# AWS Configuration
export AWS_REGION="us-east-1"
export AWS_PROFILE="default"
export S3_BUCKET="your-weather-bucket"

# HRRR Data Configuration
export HRRR_SOURCE_BUCKET="noaa-hrrr-bdp-pds"
export FORECAST_HOURS="0,1,2,3,6,9,12"  # Which forecast hours to process
export TEMPERATURE_BAND="72"             # GRIB2 band number for temperature

# Processing Configuration
export GDAL_CACHEMAX="512"               # GDAL cache size in MB
export NUM_PARALLEL_PROCESSES="4"        # Parallel GDAL processes

# Tile Generation (if using pre-generated tiles)
export GENERATE_TILES="false"            # Set to "true" to enable
export TILE_ZOOM_LEVELS="1-10"           # Zoom levels to generate
```

### Terraform Variables

Create `terraform/terraform.tfvars`:

```hcl
aws_region     = "us-east-1"
bucket_name    = "your-weather-bucket"
environment    = "production"

# S3 lifecycle policies
grib2_retention_days = 7
cog_retention_days   = 30
cog_ia_transition_days = 2

# CloudFront configuration
enable_cloudfront = true
cloudfront_price_class = "PriceClass_100"  # US, Canada, Europe
```

## Monitoring

### CloudWatch Metrics

The pipeline sends custom metrics to CloudWatch:

- `WeatherPipeline/DataAge`: Minutes since model run time
- `WeatherPipeline/ProcessingTime`: Duration of GDAL processing
- `WeatherPipeline/FilesProcessed`: Number of forecast hours processed
- `WeatherPipeline/Errors`: Count of pipeline failures

### CloudWatch Alarms

Recommended alarms:

1. **Data Freshness**: Alert if data is >120 minutes old
2. **Pipeline Failures**: Alert on any processing errors
3. **S3 Upload Failures**: Alert on upload errors
4. **Disk Space**: Alert if EC2 /tmp fills up

### Logs

```bash
# View pipeline logs on EC2
tail -f /var/log/weather-pipeline.log

# View CloudWatch logs
aws logs tail /aws/weather-pipeline --follow
```

## Mapbox Integration

### Loading Tiles in Mapbox GL JS

```javascript
// Initialize map
const map = new mapboxgl.Map({
  container: "map",
  style: "mapbox://styles/mapbox/dark-v10",
  center: [-98, 39],
  zoom: 4,
});

// Load latest forecast metadata
async function loadWeatherData() {
  const response = await fetch(
    "https://your-bucket.s3.amazonaws.com/metadata/latest.json"
  );
  const forecast = await response.json();

  // Add temperature layer
  map.addSource("temperature", {
    type: "raster",
    tiles: [
      `https://d123456789.cloudfront.net/tiles/temperature/${forecast.modelRun}/f00/{z}/{x}/{y}.png`,
    ],
    tileSize: 256,
  });

  map.addLayer({
    id: "temperature-layer",
    type: "raster",
    source: "temperature",
    paint: {
      "raster-opacity": 0.7,
    },
  });
}

map.on("load", loadWeatherData);
```

## Troubleshooting

### Common Issues

**Problem**: GDAL fails with "out of memory" error

```bash
# Solution: Increase GDAL cache and process files sequentially
export GDAL_CACHEMAX="1024"
export NUM_PARALLEL_PROCESSES="1"
```

**Problem**: Pipeline times out downloading GRIB2 files

```bash
# Solution: Use byte-range downloads to get only needed variables
# See scripts/download_hrrr.py for implementation
```

**Problem**: Tiles not updating in web app

```bash
# Solution: Check CloudFront cache invalidation
aws cloudfront create-invalidation \
  --distribution-id E1234567890 \
  --paths "/tiles/temperature/*"
```

**Problem**: S3 storage costs increasing

```bash
# Solution: Verify lifecycle policies are applied
aws s3api get-bucket-lifecycle-configuration \
  --bucket your-weather-bucket
```

## Development

### Running Tests

```bash
# Test HRRR download
python3 -m pytest tests/test_download.py

# Test GDAL processing
python3 -m pytest tests/test_processing.py

# Test S3 upload
python3 -m pytest tests/test_upload.py
```

### Local Development Workflow

```bash
# 1. Download sample GRIB2 file
aws s3 cp s3://noaa-hrrr-bdp-pds/hrrr.20260109/conus/hrrr.t14z.wrfsfcf00.grib2 ./test-data/

# 2. Process locally with GDAL
gdalwarp -t_srs EPSG:3857 \
  -of COG \
  -b 72 \
  test-data/hrrr.t14z.wrfsfcf00.grib2 \
  test-data/temperature_f00.tif

# 3. Generate test tiles
gdal2tiles.py -z 1-6 \
  test-data/temperature_f00.tif \
  test-data/tiles/

# 4. Test Mapbox integration locally
python3 -m http.server 8000
# Open http://localhost:8000/web/
```

## Future Enhancements

- [ ] Add more weather variables (wind speed, precipitation, humidity)
- [ ] Implement TiTiler for dynamic tile generation
- [ ] Add animation support for forecast time series
- [ ] Create mobile app with offline tile caching
- [ ] Add user-selectable forecast models (GFS, NAM, etc.)
- [ ] Implement webhook notifications for pipeline failures
- [ ] Add historical weather data archive

## Resources

### Documentation

- [HRRR Model Documentation](https://rapidrefresh.noaa.gov/hrrr/)
- [NOAA HRRR S3 Bucket](https://registry.opendata.aws/noaa-hrrr-pds/)
- [GDAL Documentation](https://gdal.org/)
- [Mapbox GL JS API](https://docs.mapbox.com/mapbox-gl-js/api/)
- [Cloud Optimized GeoTIFF](https://cogeo.org/)

### Related Projects

- [TiTiler](https://github.com/developmentseed/titiler) - Dynamic tile server
- [GeoLambda](https://github.com/developmentseed/geolambda) - AWS Lambda GDAL layer
- [MBTiles](https://github.com/mapbox/mbtiles-spec) - Tile storage format

## License

MIT License - See LICENSE file for details

## Support

For issues and questions:

- Open an issue on GitHub
- Check existing documentation in `docs/`
- Review CloudWatch logs for pipeline errors

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Submit a pull request with tests

---

**Last Updated**: January 2026
**Version**: 1.0.0
**Maintainer**: [Your Name]
