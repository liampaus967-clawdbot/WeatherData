# GFS-Wave Pipeline

Ocean wave forecast data processing pipeline using NOAA's GFS-Wave model.

## Overview

GFS-Wave provides global ocean wave forecasts at 0.25 degree (~28 km) resolution. The model runs every 6 hours (00, 06, 12, 18 UTC) with forecasts out to 384 hours (16 days).

## Variables

| Variable | GRIB Search | Description | Units |
|----------|-------------|-------------|-------|
| wave_height | `HTSGW:surface` | Significant Wave Height | m → ft |
| wave_period | `PERPW:surface` | Primary Wave Period | s |
| wave_direction | `DIRPW:surface` | Primary Wave Direction | ° |
| wind_wave_height | `WVHGT:surface` | Wind Wave Height | m → ft |
| swell_height_1 | `SWELL:1 in sequence` | Primary Swell Height | m → ft |
| swell_period_1 | `SWPER:1 in sequence` | Primary Swell Period | s |
| swell_direction_1 | `SWDIR:1 in sequence` | Primary Swell Direction | ° |

## Usage

### Download Only

```bash
# Download latest forecast
python download_gfs_wave.py --latest

# Download specific date and cycle
python download_gfs_wave.py --date 2026-01-10 --cycle 12

# Dry run (see what would be downloaded)
python download_gfs_wave.py --latest --dry-run

# Local only (no S3 upload)
python download_gfs_wave.py --latest --local-only --output-dir /tmp/gfs-wave-test
```

### Full Pipeline

```bash
# Dry run
./pipeline_gfs_wave.sh --dry-run

# Run with S3 upload
./pipeline_gfs_wave.sh --enable-s3 --s3-bucket my-bucket

# Custom forecast hours
./pipeline_gfs_wave.sh --forecast-hours 0-24 --enable-s3 --s3-bucket my-bucket
```

## Configuration

The pipeline uses `config/variables_gfs_wave.yaml` for:
- Variable definitions and GRIB search strings
- Color ramp definitions
- Unit conversions
- Processing settings

## Differences from HRRR Pipeline

| Aspect | HRRR | GFS-Wave |
|--------|------|----------|
| Model | `hrrr` | `gfs_wave` |
| Product | `sfc` | `global.0p25` |
| Update Frequency | Hourly | Every 6 hours |
| Forecast Length | 0-48 hours | 0-384 hours |
| Resolution | 3 km (CONUS) | 0.25° (~28 km global) |
| Data Delay | ~3 hours | ~5 hours |

## S3 Organization

```
s3://bucket/
└── gfs-wave/
    ├── raw-grib2/
    ├── colored-cogs/
    ├── tiles/
    └── metadata/
```

## Cron Schedule

GFS-Wave data becomes available ~5 hours after model run:
- 00Z run available ~05:00 UTC
- 06Z run available ~11:00 UTC
- 12Z run available ~17:00 UTC
- 18Z run available ~23:00 UTC

Recommended cron (30 minutes after availability):
```bash
30 5,11,17,23 * * * /path/to/pipeline_gfs_wave.sh --enable-s3 --s3-bucket my-bucket
```
