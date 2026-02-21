# Weather Pipeline Scripts

Organized collection of scripts for downloading and processing weather forecast data from multiple models.

## Directory Structure

```
scripts/
├── hrrr/              # HRRR (High-Resolution Rapid Refresh) model scripts
│   └── download_hrrr.py
├── gfs/               # GFS (Global Forecast System) model scripts (future)
├── rap/               # RAP (Rapid Refresh) model scripts (future)
├── common/            # Shared utilities and base classes
├── processing/        # Data processing scripts (COG conversion, reprojection)
├── utils/             # Helper utilities (logging, S3, metadata)
└── README.md          # This file
```

## Weather Models Supported

### Currently Implemented

#### HRRR (High-Resolution Rapid Refresh)
- **Directory**: `hrrr/`
- **Resolution**: 3 km
- **Coverage**: Continental US
- **Frequency**: Hourly
- **Forecast Length**: 0-48 hours
- **Best For**: Short-term high-resolution forecasts, severe weather
- **Documentation**: [hrrr/README.md](hrrr/README.md)

### Future Models

#### GFS (Global Forecast System)
- **Directory**: `gfs/`
- **Resolution**: 0.25° (~25 km)
- **Coverage**: Global
- **Frequency**: Every 6 hours
- **Forecast Length**: 0-384 hours (16 days)
- **Best For**: Long-range forecasts, global coverage

#### RAP (Rapid Refresh)
- **Directory**: `rap/`
- **Resolution**: 13 km
- **Coverage**: North America
- **Frequency**: Hourly
- **Forecast Length**: 0-21 hours
- **Best For**: Aviation, medium-resolution forecasts

## Quick Start

### 1. Download Latest HRRR Forecast

```bash
# From project root
python scripts/hrrr/download_hrrr.py --latest

# From scripts directory
cd scripts
python hrrr/download_hrrr.py --latest --verbose
```

### 2. Running in Docker

```bash
# Run HRRR download in Docker
docker run --rm \
  -v ~/.aws:/root/.aws \
  -v /tmp/weather-data:/tmp/weather-data \
  weather-processor:latest \
  python3 /app/scripts/hrrr/download_hrrr.py --latest
```

### 3. Test Before Downloading

```bash
# Dry run - see what would be downloaded
python scripts/hrrr/download_hrrr.py --latest --dry-run -v
```

## Common Workflows

### Daily Forecast Update (HRRR)

```bash
# Download latest HRRR forecast with default variables
python scripts/hrrr/download_hrrr.py \
  --latest \
  --fxx 0-12 \
  --variables default
```

### Custom Variables

```bash
# Download specific variables only
python scripts/hrrr/download_hrrr.py \
  --latest \
  --variables "TMP:2 m,UGRD:10 m,VGRD:10 m,APCP:surface"
```

### Historical Download

```bash
# Download specific date/cycle
python scripts/hrrr/download_hrrr.py \
  --date 2026-01-10 \
  --cycle 12 \
  --fxx 0-18
```

### Local Testing (No S3)

```bash
# Save files locally only
python scripts/hrrr/download_hrrr.py \
  --latest \
  --fxx 0-6 \
  --local-only \
  --output-dir ./test-data
```

## Shared Utilities

### common/
Shared code used across multiple weather models:
- Base downloader classes
- Common configuration
- Shared constants

### utils/
Helper utilities:
- Logging configuration
- S3 upload/download
- Metadata generation
- File management

### processing/
Data processing scripts (TICKET-006):
- NetCDF to Cloud Optimized GeoTIFF (COG)
- Reprojection (EPSG:3857)
- Unit conversions
- Color ramp application

## Adding New Weather Models

To add a new weather model (e.g., NAM, ECMWF):

1. **Create model directory**:
   ```bash
   mkdir scripts/nam
   ```

2. **Create download script**:
   ```bash
   cp scripts/hrrr/download_hrrr.py scripts/nam/download_nam.py
   # Modify for NAM-specific parameters
   ```

3. **Create model README**:
   ```bash
   # Document model-specific details
   touch scripts/nam/README.md
   ```

4. **Update this README**:
   - Add to "Weather Models Supported"
   - Add quick start example

## Environment Variables

Optional configuration via environment variables:

```bash
# S3 bucket for uploads
export WEATHER_S3_BUCKET="sat-data-automation-test"

# Temporary directory for downloads
export WEATHER_TEMP_DIR="/tmp/weather-data"

# AWS profile (if not using EC2 role)
export AWS_PROFILE="default"

# Log level
export WEATHER_LOG_LEVEL="INFO"
```

## Testing

```bash
# Test HRRR download (dry run)
python scripts/hrrr/download_hrrr.py --latest --dry-run

# Test with single forecast hour
python scripts/hrrr/download_hrrr.py --latest --fxx 0 -v

# Test S3 upload (requires AWS credentials)
python scripts/hrrr/download_hrrr.py --latest --fxx 0
```

## Performance Benchmarks

### HRRR Model
- Single forecast hour (6 variables): ~30-60 seconds
- Full forecast F00-F12 (13 hours): ~5-10 minutes
- Storage per forecast hour: ~5-10 MB (variables), ~60-80 MB (full GRIB2)

### Expected for GFS (future)
- Single forecast hour: ~2-5 minutes
- Full forecast: ~30-60 minutes
- Storage per hour: ~50-100 MB

## Troubleshooting

### Common Issues

**"No such file or directory"**
- Check script path from project root
- Use correct relative path: `scripts/hrrr/download_hrrr.py`

**"S3 upload failed"**
- Verify IAM role has S3 write permissions
- Check bucket name is correct
- Ensure EC2 instance has IAM role attached

**"No data available"**
- HRRR has ~65 minute delay, use `--latest` (goes back 3 hours)
- Check NOAA servers are operational
- Try different date/cycle

**"Variable not found"**
- Verify variable search string
- Use `H.inventory()` to see available variables

## Documentation Links

- **Herbie Library**: https://herbie.readthedocs.io/
- **HRRR Model**: https://rapidrefresh.noaa.gov/hrrr/
- **GFS Model**: https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast
- **NOAA Data Access**: https://www.noaa.gov/information-technology/open-data-dissemination

## Model-Specific READMEs

- [hrrr/README.md](hrrr/README.md) - HRRR download script documentation
- `gfs/README.md` - (Coming soon)
- `rap/README.md` - (Coming soon)

---

**Organization**: Modular structure supports multiple weather models
**Created**: 2026-01-10
**Updated**: 2026-01-10
**Current Status**: HRRR implemented, GFS and RAP planned
