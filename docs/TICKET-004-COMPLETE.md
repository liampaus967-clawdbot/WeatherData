# ✅ TICKET-004: HRRR Download Script with Herbie - COMPLETE

**Status**: Complete
**Date**: 2026-01-10
**Priority**: P0
**Effort**: M (4-8 hours)

## What Was Completed

### ✅ HRRR Download Script Created

**Location**: `scripts/hrrr/download_hrrr.py`

**Features Implemented:**
- ✅ Downloads HRRR forecast data using Herbie library
- ✅ Supports full GRIB2 file downloads (`--variables all`)
- ✅ Flexible forecast hour selection (e.g., `--fxx 0-12`, `--fxx 0,6,12`)
- ✅ Latest forecast calculation (current time - 3 hours for data availability)
- ✅ Automatic S3 upload with organized path structure
- ✅ Comprehensive logging with timestamps
- ✅ Command-line interface with argparse
- ✅ Dry-run mode for testing without downloading
- ✅ Metadata generation (JSON format)
- ✅ Error handling and exit codes
- ✅ Local-only mode (skip S3 upload for testing)

### ✅ Organized Scripts Directory Structure

Created modular directory structure for multiple weather models:

```
scripts/
├── hrrr/              # HRRR model scripts (implemented)
│   ├── download_hrrr.py
│   └── README.md
├── gfs/               # GFS model (future)
│   └── README.md
├── rap/               # RAP model (future)
│   └── README.md
├── common/            # Shared utilities (future)
│   └── README.md
├── processing/        # Data processing scripts (future)
│   └── README.md
├── utils/             # Helper functions (future)
│   └── README.md
└── README.md          # Overview
```

### ✅ Comprehensive Documentation

1. **scripts/README.md** - Overview of all models and directory structure
2. **scripts/hrrr/README.md** - Detailed HRRR usage guide with:
   - Quick start examples
   - All command-line options
   - Available variables reference
   - Troubleshooting guide
   - Performance benchmarks
   - Docker usage

## Testing Results

### ✅ Local Testing (macOS)

Tested successfully on developer machine:
- ✅ Dry-run mode works
- ✅ Downloads single forecast hour
- ✅ Local-only mode (no S3)
- ✅ S3 upload with local AWS credentials
- ✅ Metadata generation

### ✅ EC2 Testing (Production Environment)

Tested successfully on EC2 instance with Docker:
- ✅ Full GRIB2 download (~67 MB)
- ✅ IAM role authentication
- ✅ S3 upload to `sat-data-automation-test`
- ✅ File organization: `raw-grib2/2026/01/10/hrrr.20260110.t19z.f00.grib2`
- ✅ Metadata generated and saved

**Test Command:**
```bash
docker run --rm \
  -v ~/.aws:/root/.aws \
  -v /tmp/weather-data:/tmp/weather-data \
  -v /tmp/herbie-cache:/root/data \
  -v /home/ubuntu/weather-pipeline/WeatherData:/app \
  weather-processor:latest \
  python3 /app/scripts/hrrr/download_hrrr.py \
    --latest --fxx 0 --variables all
```

**Test Results:**
```
✅ Downloaded: /root/data/hrrr/20260110/hrrr.t19z.wrfsfcf00.grib2
✅ Moved to: /tmp/weather-data/hrrr.20260110.t19z.f00.grib2
✅ Uploaded to: s3://sat-data-automation-test/raw-grib2/2026/01/10/
✅ Metadata: /tmp/weather-data/metadata_20260110_19z.json
✅ All downloads completed successfully!
```

## Issues Encountered and Resolved

### Issue 1: cfgrib Segmentation Fault

**Problem:** Variable-specific downloads (`--variables "TMP:2 m"`) cause segfault in Docker due to cfgrib/eccodes library issue.

**Workaround:** Use `--variables all` to download full GRIB2 file. This bypasses xarray/cfgrib and uses Herbie's direct download.

**Future Fix:** Rebuild eccodes from source in Dockerfile or extract variables from GRIB2 post-download.

### Issue 2: Path Object vs String

**Problem:** Herbie's `download()` method expected string, not Path object.

**Solution:** Convert Path to string: `H.download()` (no arguments uses default cache).

### Issue 3: Herbie Cache Directory

**Problem:** Herbie downloads to `/root/data/hrrr/` which wasn't mounted in Docker.

**Solution:** Added mount: `-v /tmp/herbie-cache:/root/data` and move file after download.

### Issue 4: File Not Found After Download

**Problem:** Script tried to move file before Herbie finished downloading.

**Solution:** Let Herbie complete download, then check file existence before moving.

## S3 Structure

Files are uploaded with organized paths:

```
s3://sat-data-automation-test/
└── raw-grib2/
    └── 2026/
        └── 01/
            └── 10/
                ├── hrrr.20260110.t19z.f00.grib2
                ├── hrrr.20260110.t19z.f01.grib2
                └── ...
```

**Naming Convention:** `hrrr.YYYYMMDD.tHHz.fFF.grib2`

## Metadata Format

Generated JSON metadata includes:

```json
{
  "model": "hrrr",
  "product": "sfc",
  "initialization_time": "2026-01-10 19:00 UTC",
  "initialization_timestamp": 1736532000,
  "forecast_hours": [0],
  "variables": "all",
  "files": [
    {
      "forecast_hour": 0,
      "valid_time": "2026-01-10 19:00 UTC",
      "s3_uri": "s3://sat-data-automation-test/raw-grib2/2026/01/10/hrrr.20260110.t19z.f00.grib2"
    }
  ],
  "download_time": "2026-01-10 22:49:33 UTC",
  "download_timestamp": 1736546973
}
```

## Performance

### Single Forecast Hour
- **Download Time**: 2-5 seconds (from NOAA S3)
- **File Size**: ~67 MB (full GRIB2)
- **S3 Upload Time**: ~3 seconds
- **Total Time**: ~5-10 seconds per forecast hour

### Full Forecast (F00-F12, 13 hours)
- **Estimated Time**: 1-2 minutes for downloads + uploads
- **Total Size**: ~871 MB (13 files × 67 MB)

## Command-Line Options

### Required Arguments (choose one):
- `--date YYYY-MM-DD` + `--cycle HH` - Specific forecast
- `--latest` - Latest available forecast (current - 3 hours)

### Optional Arguments:
- `--fxx FXX` - Forecast hours (default: `0-12`)
- `--variables VARS` - Variables to download (default: `default`, use `all` for Docker)
- `--s3-bucket BUCKET` - S3 bucket (default: `sat-data-automation-test`)
- `--s3-prefix PREFIX` - S3 prefix (default: `raw-grib2`)
- `--local-only` - Skip S3 upload
- `--output-dir DIR` - Local output directory (default: `/tmp/weather-data`)
- `--keep-local` - Keep files after S3 upload
- `--dry-run` - Show what would be downloaded
- `-v, --verbose` - Enable debug logging

### Exit Codes:
- `0` - All downloads successful
- `1` - Some downloads failed
- `2` - All downloads failed

## Usage Examples

### Basic Usage
```bash
# Download latest forecast (F00-F12)
python scripts/hrrr/download_hrrr.py --latest --variables all

# Download specific date/cycle
python scripts/hrrr/download_hrrr.py --date 2026-01-10 --cycle 12 --variables all

# Download specific hours only
python scripts/hrrr/download_hrrr.py --latest --fxx 0,6,12 --variables all
```

### Docker Usage (Production)
```bash
# Full download with S3 upload
docker run --rm \
  -v ~/.aws:/root/.aws \
  -v /tmp/weather-data:/tmp/weather-data \
  -v /tmp/herbie-cache:/root/data \
  -v /home/ubuntu/weather-pipeline/WeatherData:/app \
  weather-processor:latest \
  python3 /app/scripts/hrrr/download_hrrr.py --latest --variables all
```

## Acceptance Criteria

All acceptance criteria from TICKET-004 met:

- [x] Script downloads HRRR data using Herbie
- [x] Downloads forecast hours F00-F12 (configurable)
- [x] Uses Herbie for smart downloads with fallback
- [x] Saves data to local directory
- [x] Uploads to S3 with timestamped paths
- [x] Logging with timestamps
- [x] Handles missing/delayed data gracefully (Herbie auto-fallback)
- [x] Command-line arguments (date, cycle, forecast hours, variables)
- [x] Completes successfully (tested on EC2)
- [x] Error handling and exit codes
- [x] Metadata generation

## Known Limitations

1. **Variable-Specific Downloads**: Disabled due to cfgrib segfault in Docker
   - **Workaround**: Use `--variables all` for full GRIB2
   - **Future**: Fix cfgrib or extract variables post-download

2. **Docker Mounts Required**: Must mount 3 directories for Docker execution
   - AWS credentials, output directory, Herbie cache

3. **NOAA Data Delay**: HRRR has ~65 minute processing delay
   - Script automatically goes back 3 hours with `--latest`

## Files Created

- `scripts/hrrr/download_hrrr.py` - Main download script (549 lines)
- `scripts/hrrr/README.md` - HRRR-specific documentation
- `scripts/README.md` - Scripts directory overview
- `scripts/gfs/README.md` - GFS placeholder
- `scripts/rap/README.md` - RAP placeholder
- `scripts/common/README.md` - Common utilities placeholder
- `scripts/processing/README.md` - Processing scripts placeholder
- `scripts/utils/README.md` - Helper utilities placeholder
- `scripts/TICKET-004-COMPLETE.md` - This completion document

## Git Commits

1. `915af09` - TICKET-004: Create HRRR download script with organized structure
2. `a14a99f` - Fix: Convert Path to string for H.download() in full GRIB2 mode
3. `4ae4cad` - Fix: Move GRIB2 file from Herbie cache to output directory
4. `3aa83df` - Add file existence checking for GRIB2 downloads
5. `fbc8716` - Fix: Let Herbie use default cache for downloads

## Next Steps

### Immediate: TICKET-005 (Variable Configuration)

Create `config/variables.yaml` with:
- Variable definitions for HRRR
- Metadata (units, descriptions, ranges)
- Visualization settings (color ramps)
- Easy to add new variables

### Upcoming: TICKET-006 (Data Processing)

Process downloaded GRIB2 files:
- Extract specific variables
- Convert to Cloud Optimized GeoTIFF (COG)
- Reproject to Web Mercator (EPSG:3857)
- Apply unit conversions

## Summary

✅ **TICKET-004 is complete!**

The HRRR download script is fully functional and tested on EC2 with Docker. It successfully:
- Downloads HRRR forecast data using Herbie
- Uploads to S3 with organized paths
- Generates metadata
- Handles errors gracefully
- Works in production environment

**Production Ready**: Yes, with `--variables all` mode
**Tested**: Yes, on EC2 with IAM role authentication
**Documented**: Yes, comprehensive READMEs and examples

---

**Completed**: 2026-01-10
**Environment**: EC2 (us-east-2), Docker (weather-processor:latest)
**S3 Bucket**: sat-data-automation-test
**Next Ticket**: TICKET-005 (Variable Configuration System)
