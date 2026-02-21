# EC2 Wrapper Scripts

Quick reference for wrapper scripts installed on the EC2 instance for manual testing and quick data retrieval.

## get-weather-variable.sh

**Location**: `~/get-weather-variable.sh` (on EC2 instance)

**Purpose**: Quick utility to download, process, and upload a single weather variable to S3.

### What It Does

1. ✅ Downloads latest HRRR GRIB2 file from NOAA
2. ✅ Uploads raw GRIB2 to S3 (`raw-grib2/`)
3. ✅ Extracts specified variable from GRIB2
4. ✅ Converts to Cloud Optimized GeoTIFF (COG)
5. ✅ Uploads processed COG to S3 (`processed-cogs/`)
6. ✅ Shows local and S3 locations

### Usage

```bash
# Basic usage (downloads latest temperature)
~/get-weather-variable.sh

# Specific variable
~/get-weather-variable.sh <variable_name>

# Specific variable and forecast hour
~/get-weather-variable.sh <variable_name> <forecast_hour>
```

### Examples

```bash
# Temperature at 2 meters (default)
~/get-weather-variable.sh temperature_2m

# Composite reflectivity (radar)
~/get-weather-variable.sh reflectivity_composite

# Wind U-component
~/get-weather-variable.sh wind_u_10m

# Temperature for 6-hour forecast
~/get-weather-variable.sh temperature_2m 6

# Precipitation for 12-hour forecast
~/get-weather-variable.sh precipitation_accumulated 12
```

### Available Variables

#### Priority 1 (Critical)
- `temperature_2m` - Air temperature at 2m (~16 MB)
- `wind_u_10m` - U-component of wind at 10m (~17 MB)
- `wind_v_10m` - V-component of wind at 10m (~17 MB)
- `precipitation_accumulated` - Total precipitation (~55 KB)
- `reflectivity_composite` - Composite reflectivity/radar (~3.5 MB)

#### Priority 2 (Important)
- `dewpoint_2m` - Dewpoint temperature at 2m (~16 MB)
- `wind_gust_surface` - Surface wind gusts (~17 MB)
- `relative_humidity_2m` - Relative humidity at 2m (~16 MB)
- `cloud_cover_total` - Total cloud cover (~4 MB)
- `cape` - Convective Available Potential Energy (~4 MB)

### Output Locations

#### Local (Temporary)
```
/tmp/weather-data/          # Downloaded GRIB2 files
/tmp/processed-weather/     # Processed COG files
```

**Note**: Local files are cleaned up on each run.

#### S3 (Permanent)

**Raw GRIB2**:
```
s3://sat-data-automation-test/raw-grib2/YYYY/MM/DD/hrrr.YYYYMMDD.tHHz.fFF.grib2
```

**Processed COGs**:
```
s3://sat-data-automation-test/processed-cogs/YYYY/MM/DD/{variable}_hrrr.YYYYMMDD.tHHz.fFF.tif
```

**Example**:
```
s3://sat-data-automation-test/
├── raw-grib2/
│   └── 2026/01/11/
│       └── hrrr.20260111.t01z.f00.grib2 (67 MB)
│
└── processed-cogs/
    └── 2026/01/11/
        └── temperature_2m_hrrr.20260111.t01z.f00.tif (16 MB)
```

### Expected Output

```
Getting temperature_2m (F0)...
Downloading GRIB2...
✅ Downloaded and uploaded to S3: raw-grib2/2026/01/11/hrrr.20260111.t01z.f00.grib2

Processing /tmp/weather-data/hrrr.20260111.t01z.f00.grib2...
INFO - Processing variable: temperature_2m
INFO - Extracting 'TMP:2 m' from hrrr.20260111.t01z.f00.grib2
INFO - Data already in Celsius, skipping K→C conversion
INFO - Reprojecting to EPSG:3857 (Web Mercator)
INFO - Created COG: temperature_2m_hrrr.20260111.t01z.f00.tif
INFO - Processed 1/1 variables successfully

Uploading COG to S3...
Uploading to: s3://sat-data-automation-test/processed-cogs/2026/01/11/
upload: ./temperature_2m_hrrr.20260111.t01z.f00.tif to s3://...

✅ Complete!

Local output:
-rw-r--r-- 1 ubuntu ubuntu 16M Jan 11 01:30 temperature_2m_hrrr.20260111.t01z.f00.tif

S3 location:
s3://sat-data-automation-test/processed-cogs/2026/01/11/

Verify upload:
2026-01-11 01:30:45   15.5 MiB temperature_2m_hrrr.20260111.t01z.f00.tif
```

### Verify S3 Upload

```bash
# List all processed COGs for today
aws s3 ls s3://sat-data-automation-test/processed-cogs/2026/01/11/ --human-readable

# Download a COG from S3 to check it
aws s3 cp s3://sat-data-automation-test/processed-cogs/2026/01/11/temperature_2m_hrrr.20260111.t01z.f00.tif ./test.tif

# Inspect with GDAL
gdalinfo ./test.tif
```

### Script Source Code

**File**: `~/get-weather-variable.sh` on EC2

```bash
#!/bin/bash
# Quick weather variable download, process, and upload to S3

VARIABLE=${1:-temperature_2m}
FXX=${2:-0}

# 1. Clean up
sudo rm -rf /tmp/weather-data/* /tmp/processed-weather/* /tmp/herbie-cache/*

# 2. Download GRIB2 (keeps local copy with --keep-local)
docker run --rm \
  -v ~/.aws:/root/.aws \
  -v /tmp/weather-data:/tmp/weather-data \
  -v /tmp/herbie-cache:/root/data \
  -v ~/weather-pipeline/WeatherData:/app \
  weather-processor:latest \
  python3 /app/scripts/hrrr/download_hrrr.py \
    --latest --fxx $FXX --variables all \
    --keep-local

# 3. Fix permissions
sudo chown -R ubuntu:ubuntu /tmp/weather-data/

# 4. Process to COG
GRIB_FILE=$(ls /tmp/weather-data/hrrr.*.grib2 2>/dev/null | head -1)
docker run --rm \
  -v /tmp/weather-data:/tmp/weather-data \
  -v /tmp/processed-weather:/tmp/processed-weather \
  -v ~/weather-pipeline/WeatherData:/app \
  weather-processor:latest \
  python3 /app/scripts/processing/process_weather.py \
    --input $GRIB_FILE \
    --output /tmp/processed-weather/ \
    --variables $VARIABLE

# 5. Upload COG to S3
sudo chown -R ubuntu:ubuntu /tmp/processed-weather/
BASENAME=$(basename $GRIB_FILE .grib2)
DATE_STR=$(echo $BASENAME | sed 's/.*\.\([0-9]\{8\}\)\..*/\1/')
YEAR=${DATE_STR:0:4}
MONTH=${DATE_STR:4:2}
DAY=${DATE_STR:6:2}
S3_COG_PATH="s3://sat-data-automation-test/processed-cogs/$YEAR/$MONTH/$DAY/"

aws s3 cp /tmp/processed-weather/ $S3_COG_PATH \
  --recursive --exclude "*" --include "*.tif"

# 6. Show results
ls -lh /tmp/processed-weather/
aws s3 ls $S3_COG_PATH --human-readable
```

### Troubleshooting

**Issue**: "No GRIB2 file found"
```bash
# Check if download succeeded
ls -la /tmp/weather-data/
sudo ls -la /tmp/weather-data/  # Check with sudo in case of permissions
```

**Issue**: Docker not found
```bash
# Verify Docker is running
docker ps

# Check Docker image exists
docker images | grep weather-processor
```

**Issue**: AWS credentials error
```bash
# Verify IAM role is attached
aws sts get-caller-identity

# Test S3 access
aws s3 ls s3://sat-data-automation-test/
```

**Issue**: Permission denied on /tmp/
```bash
# Fix permissions
sudo chown -R ubuntu:ubuntu /tmp/weather-data/
sudo chown -R ubuntu:ubuntu /tmp/processed-weather/
```

## When to Use This Script

### Good Use Cases ✅

- **Manual testing** of new variables
- **Quick data retrieval** for specific forecast
- **Debugging** processing issues
- **One-off requests** for specific data
- **Testing** before deploying automation

### Not Recommended For ❌

- **Production automation** - Use scheduled pipeline (TICKET-008)
- **Bulk downloads** - Use batch processing scripts
- **Multiple variables** - Process all priority 1 at once instead
- **Historical data** - Use date-specific download scripts

## Future Automation

**Note**: This wrapper is for **manual testing only**.

**TICKET-008** will provide full automation:
- Scheduled cron job (runs every hour)
- Downloads F00-F12 (13 forecast hours)
- Processes all priority 1 variables automatically
- No manual intervention needed

After TICKET-008 deployment, this wrapper will still be useful for:
- Testing new variables before adding to automation
- Quick retrieval of specific forecast hours
- Debugging pipeline issues
- Manual data pulls for special requests

---

**Created**: 2026-01-11
**Last Updated**: 2026-01-11
**EC2 Instance**: EC2-WeatherPipeline (us-east-2)
**Script Location**: `~/get-weather-variable.sh`
**Status**: Active, manual testing only
