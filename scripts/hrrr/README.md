# HRRR Download Scripts

Scripts for downloading HRRR (High-Resolution Rapid Refresh) forecast data.

## About HRRR

**HRRR (High-Resolution Rapid Refresh)** is a NOAA real-time 3-km resolution, hourly updated, cloud-resolving, convection-allowing atmospheric model.

- **Resolution**: 3 km
- **Coverage**: Continental United States
- **Update Frequency**: Every hour
- **Forecast Length**: 0-48 hours
- **Best For**: Short-term high-resolution forecasts, severe weather monitoring

## Scripts

### download_hrrr.py

Main script for downloading HRRR forecast data using the Herbie library.

**Features:**
- ✅ Uses Herbie for intelligent data download with automatic fallback across multiple sources
- ✅ Downloads specific variables or full GRIB2 files
- ✅ Automatic S3 upload with organized path structure
- ✅ Comprehensive logging with timestamps
- ✅ Multiple command-line options for flexibility
- ✅ Dry-run mode for testing
- ✅ Handles NOAA server delays gracefully

## Quick Start

```bash
# Download latest forecast (simplest usage)
python scripts/hrrr/download_hrrr.py --latest

# Download with verbose logging
python scripts/hrrr/download_hrrr.py --latest -v

# Test without actually downloading
python scripts/hrrr/download_hrrr.py --latest --dry-run
```

## Usage

### Command-Line Options

```
usage: download_hrrr.py [-h] (--date DATE | --latest) [--cycle HOUR]
                        [--fxx FXX] [--variables VARIABLES]
                        [--s3-bucket S3_BUCKET] [--s3-prefix S3_PREFIX]
                        [--local-only] [--output-dir OUTPUT_DIR]
                        [--keep-local] [--dry-run] [-v]
```

### Required Arguments (choose one)

- `--date YYYY-MM-DD` - Specific forecast date
- `--latest` - Use latest available forecast (recommended)

### Optional Arguments

- `--cycle HOUR` - Model cycle hour 0-23 (required with `--date`)
- `--fxx FXX` - Forecast hours to download (default: `0-12`)
  - Examples: `0-12`, `0,6,12`, `0-6,12`
- `--variables VARS` - Variables to download (default: `default`)
  - `default` - Common surface variables (temperature, wind, etc.)
  - `all` - Full GRIB2 file
  - Custom list: `"TMP:2 m,UGRD:10 m,VGRD:10 m"`
- `--s3-bucket BUCKET` - S3 bucket name (default: `sat-data-automation-test`)
- `--s3-prefix PREFIX` - S3 folder prefix (default: `raw-grib2`)
- `--local-only` - Save locally only, skip S3 upload
- `--output-dir DIR` - Local output directory (default: `/tmp/weather-data`)
- `--keep-local` - Keep local files after S3 upload
- `--dry-run` - Show what would be downloaded without downloading
- `-v, --verbose` - Enable debug logging

## Examples

### Basic Usage

```bash
# Download latest forecast with defaults
python scripts/hrrr/download_hrrr.py --latest

# Download specific date/cycle
python scripts/hrrr/download_hrrr.py --date 2026-01-10 --cycle 12

# Download yesterday's 18Z cycle
python scripts/hrrr/download_hrrr.py --date 2026-01-09 --cycle 18
```

### Custom Forecast Hours

```bash
# First 6 hours only
python scripts/hrrr/download_hrrr.py --latest --fxx 0-5

# Specific hours (0, 6, 12)
python scripts/hrrr/download_hrrr.py --latest --fxx 0,6,12

# Extended forecast (0-18)
python scripts/hrrr/download_hrrr.py --latest --fxx 0-18
```

### Custom Variables

```bash
# Temperature and wind only
python scripts/hrrr/download_hrrr.py --latest \
  --variables "TMP:2 m,UGRD:10 m,VGRD:10 m"

# Add precipitation
python scripts/hrrr/download_hrrr.py --latest \
  --variables "TMP:2 m,APCP:surface,REFC:entire atmosphere"

# Download everything (full GRIB2)
python scripts/hrrr/download_hrrr.py --latest --variables all
```

### Testing and Development

```bash
# Dry run - see what would be downloaded
python scripts/hrrr/download_hrrr.py --latest --dry-run -v

# Download one hour for testing
python scripts/hrrr/download_hrrr.py --latest --fxx 0 -v

# Save locally without S3 upload
python scripts/hrrr/download_hrrr.py --latest --local-only \
  --output-dir ./test-data

# Keep local files after upload
python scripts/hrrr/download_hrrr.py --latest --keep-local
```

## Default Variables

When using `--variables default`, these variables are downloaded:

| Variable | Description | Units |
|----------|-------------|-------|
| `TMP:2 m` | 2-meter temperature | Kelvin |
| `UGRD:10 m` | 10-meter U-wind component | m/s |
| `VGRD:10 m` | 10-meter V-wind component | m/s |
| `DPT:2 m` | 2-meter dewpoint temperature | Kelvin |
| `RH:2 m` | 2-meter relative humidity | % |
| `GUST:surface` | Surface wind gust | m/s |

## Available Variables

Common HRRR variables you can download:

### Temperature
- `TMP:2 m` - 2-meter temperature
- `TMP:surface` - Surface temperature
- `DPT:2 m` - Dewpoint temperature

### Wind
- `UGRD:10 m` - U-component of wind
- `VGRD:10 m` - V-component of wind
- `GUST:surface` - Wind gust
- `WIND:10 m` - Wind speed

### Precipitation
- `APCP:surface` - Accumulated precipitation
- `PRATE:surface` - Precipitation rate

### Clouds
- `TCDC:entire atmosphere` - Total cloud cover
- `LCDC:low cloud layer` - Low cloud cover
- `MCDC:middle cloud layer` - Mid cloud cover
- `HCDC:high cloud layer` - High cloud cover

### Severe Weather
- `REFC:entire atmosphere` - Composite reflectivity
- `CAPE:surface` - Convective available potential energy
- `CIN:surface` - Convective inhibition
- `HLCY:3000-0 m above ground` - Storm relative helicity

### Other
- `VIS:surface` - Visibility
- `SNOWC:surface` - Snow cover
- `SNOD:surface` - Snow depth

**View all available variables:**
```python
from herbie import Herbie
H = Herbie('2026-01-10 12:00', model='hrrr')
H.inventory()
```

## Output

### Local Files

Format: `hrrr.YYYYMMDD.tHHz.fFF.nc`

Example: `hrrr.20260110.t12z.f06.nc`

Location: `/tmp/weather-data/` (configurable with `--output-dir`)

### S3 Structure

```
s3://sat-data-automation-test/raw-grib2/
└── 2026/
    └── 01/
        └── 10/
            ├── hrrr.20260110.t12z.f00.nc
            ├── hrrr.20260110.t12z.f01.nc
            ├── hrrr.20260110.t12z.f02.nc
            └── ...
```

S3 URI format:
```
s3://{bucket}/{prefix}/{YYYY}/{MM}/{DD}/hrrr.{YYYYMMDD}.t{HH}z.f{FF}.nc
```

### Metadata File

A JSON metadata file is generated after downloads:

Location: `/tmp/weather-data/metadata_YYYYMMDD_HHz.json`

Example content:
```json
{
  "model": "hrrr",
  "product": "sfc",
  "initialization_time": "2026-01-10 12:00 UTC",
  "forecast_hours": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
  "variables": ["TMP:2 m", "UGRD:10 m", "VGRD:10 m"],
  "files": [
    {
      "forecast_hour": 0,
      "valid_time": "2026-01-10 12:00 UTC",
      "s3_uri": "s3://sat-data-automation-test/raw-grib2/2026/01/10/hrrr.20260110.t12z.f00.nc"
    }
  ]
}
```

## Running in Docker

**IMPORTANT:** When using `--variables all` (full GRIB2), you must also mount Herbie's cache directory.

```bash
# Download with full GRIB2 (recommended - works with current Docker)
docker run --rm \
  -v ~/.aws:/root/.aws \
  -v /tmp/weather-data:/tmp/weather-data \
  -v /tmp/herbie-cache:/root/data \
  -v /path/to/WeatherData:/app \
  weather-processor:latest \
  python3 /app/scripts/hrrr/download_hrrr.py \
    --latest --variables all

# With custom options
docker run --rm \
  -v ~/.aws:/root/.aws \
  -v /tmp/weather-data:/tmp/weather-data \
  -v /tmp/herbie-cache:/root/data \
  -v /path/to/WeatherData:/app \
  weather-processor:latest \
  python3 /app/scripts/hrrr/download_hrrr.py \
    --latest --fxx 0-6 --variables all -v
```

**Required Docker Mounts:**
- `-v ~/.aws:/root/.aws` - AWS credentials for S3 upload
- `-v /tmp/weather-data:/tmp/weather-data` - Output directory
- `-v /tmp/herbie-cache:/root/data` - Herbie's download cache
- `-v /path/to/WeatherData:/app` - Your project code

**Note:** Variable-specific downloads (`--variables "TMP:2 m"`) currently crash due to cfgrib segfault. Use `--variables all` for now.

## Performance

### Download Times

| Configuration | Time | Notes |
|---------------|------|-------|
| Single hour (6 variables) | 30-60s | Default variables |
| Single hour (full GRIB2) | 2-5min | ~70 MB file |
| F00-F12 (13 hours, variables) | 5-10min | ~65-130 MB total |
| F00-F12 (13 hours, full) | 20-30min | ~780-1040 MB total |

### Storage

| Configuration | Size per Hour | F00-F12 Total |
|---------------|---------------|---------------|
| Default variables (6) | 5-10 MB | 65-130 MB |
| Full GRIB2 | 60-80 MB | 780-1040 MB |

## Troubleshooting

### "No data available"

**Cause:** HRRR data has a ~65 minute processing delay

**Solutions:**
- Use `--latest` (automatically goes back 3 hours)
- Wait for data to become available
- Try a slightly older cycle

### "Variable not found: TMP:2m"

**Cause:** Incorrect variable search string (missing space)

**Solution:** Use `TMP:2 m` (with space before 'm')

### "S3 upload failed: Access Denied"

**Cause:** IAM role missing S3 write permissions

**Solutions:**
- Verify EC2 instance has IAM role attached
- Check IAM role has `AmazonS3FullAccess` or custom policy with `s3:PutObject`
- Test with: `aws s3 ls s3://sat-data-automation-test/`

### "Out of disk space"

**Cause:** Downloaded files filling `/tmp/weather-data`

**Solutions:**
- Don't use `--keep-local` flag (files deleted after S3 upload by default)
- Clean temp directory: `rm -rf /tmp/weather-data/*`
- Download fewer forecast hours

### "Connection timeout"

**Cause:** NOAA servers slow or unavailable

**Solutions:**
- Herbie automatically retries with fallback sources
- Wait and try again later
- Check NOAA status: https://www.weather.gov/

## Exit Codes

- `0` - All downloads successful
- `1` - Some downloads failed
- `2` - All downloads failed

## Integration with Pipeline

The download script is designed to be part of the full weather pipeline:

```bash
# 1. Download data
python scripts/hrrr/download_hrrr.py --latest

# 2. Process to COG (TICKET-006 - coming soon)
python scripts/processing/process_weather.py \
  --input /tmp/weather-data \
  --output /tmp/processed

# 3. Generate tiles (TICKET-008 - coming soon)
python scripts/processing/generate_tiles.py \
  --input /tmp/processed
```

## Documentation

- **Herbie Documentation**: https://herbie.readthedocs.io/
- **HRRR Model Info**: https://rapidrefresh.noaa.gov/hrrr/
- **HRRR on AWS**: https://registry.opendata.aws/noaa-hrrr-pds/
- **Variable Inventory**: https://www.nco.ncep.noaa.gov/pmb/products/hrrr/

---

**Model**: HRRR (High-Resolution Rapid Refresh)
**Created**: 2026-01-10
**Part of**: TICKET-004