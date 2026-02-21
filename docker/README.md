# Weather Processor Docker Container

This directory contains the Docker configuration for the weather data processing pipeline.

## Contents

- `Dockerfile` - Docker image definition with GDAL, Herbie, and all dependencies
- `requirements.txt` - Python package dependencies
- `.dockerignore` - Files to exclude from Docker build context
- `build.sh` - Script to build the Docker image
- `test.sh` - Script to test the Docker image

## Prerequisites

- Docker Desktop installed and running
- At least 2GB of free disk space
- Internet connection (for downloading base image and dependencies)

## Quick Start

### 1. Start Docker Desktop

Make sure Docker Desktop is running before proceeding.

### 2. Build the Image

```bash
cd docker/
./build.sh
```

This will:
- Download the GDAL base image (~400MB)
- Install system dependencies (eccodes, etc.)
- Install Python packages (Herbie, xarray, rioxarray, etc.)
- Configure the environment

**Build time**: ~5-10 minutes on first build

### 3. Test the Image

```bash
./test.sh
```

This runs 6 comprehensive tests:
1. Package version checks
2. Herbie functionality
3. GDAL drivers and functionality
4. xarray and rioxarray
5. cfgrib (GRIB2 decoder)
6. boto3 (AWS SDK)

All tests should pass before proceeding to the next ticket.

## Manual Build and Test

If you prefer to build manually:

```bash
# Build
docker build -t weather-processor:latest .

# Test - show versions
docker run --rm weather-processor:latest

# Test - Herbie
docker run --rm weather-processor:latest python3 -c "from herbie import Herbie; print('Herbie OK')"

# Test - GDAL
docker run --rm weather-processor:latest python3 -c "from osgeo import gdal; print(f'GDAL {gdal.__version__}')"
```

## Image Details

### Base Image
- `ghcr.io/osgeo/gdal:ubuntu-small-3.8.0`
- Ubuntu-based with GDAL 3.8+
- Optimized for geospatial processing

### Installed Packages

**Python Libraries:**
- `herbie-data` - Weather data access
- `xarray` - N-dimensional labeled arrays
- `rioxarray` - Geospatial xarray extension
- `cfgrib` - GRIB2 file decoder
- `boto3` - AWS SDK
- `pandas`, `numpy` - Data processing
- `netCDF4`, `h5netcdf` - NetCDF support
- `dask` - Parallel computing

**System Packages:**
- `libeccodes-dev` - ECMWF GRIB API
- `python3`, `pip3` - Python runtime
- `curl`, `wget`, `git` - Utilities

### Environment Variables

- `ECCODES_DIR=/usr` - eccodes installation directory
- `ECCODES_DEFINITION_PATH=/usr/share/eccodes/definitions` - eccodes definitions
- `GDAL_CACHEMAX=512` - GDAL cache size (512MB)
- `PYTHONUNBUFFERED=1` - Unbuffered Python output for better logging

### Working Directory
- `/app` - Main working directory
- `/app/scripts` - For processing scripts
- `/app/config` - For configuration files
- `/tmp/weather-data` - For temporary data storage

## Expected Image Size

- Final image: ~800MB - 1GB
- Base GDAL image: ~400MB
- Python packages: ~300-400MB
- System dependencies: ~100MB

## Usage Examples

### Interactive Shell

```bash
# Start bash inside container
docker run --rm -it weather-processor:latest bash

# Inside container:
python3
>>> from herbie import Herbie
>>> H = Herbie('2026-01-09 12:00', model='hrrr', fxx=0)
>>> print(H)
```

### Run a Script

```bash
# Run download script (once created)
docker run --rm \
  -v $(pwd)/scripts:/app/scripts \
  -v /tmp/weather-data:/tmp/weather-data \
  -e AWS_REGION=us-east-1 \
  weather-processor:latest \
  python3 /app/scripts/download_hrrr.py --help
```

### Mount AWS Credentials

```bash
# Mount AWS credentials for S3 access
docker run --rm \
  -v ~/.aws:/root/.aws:ro \
  -v /tmp/weather-data:/tmp/weather-data \
  weather-processor:latest \
  python3 -c "import boto3; s3 = boto3.client('s3'); print('AWS OK')"
```

## Troubleshooting

### Build Fails: "unable to connect to daemon"

**Problem**: Docker Desktop is not running

**Solution**:
```bash
# macOS - start Docker Desktop
open -a Docker

# Wait for Docker to start, then retry build
./build.sh
```

### Build Fails: "failed to fetch eccodes"

**Problem**: Network connectivity or package repository issue

**Solution**:
```bash
# Rebuild with no cache
docker build --no-cache -t weather-processor:latest .
```

### Import Error: "cannot import name 'Herbie'"

**Problem**: Herbie installation incomplete

**Solution**:
```bash
# Rebuild image
docker build --no-cache -t weather-processor:latest .

# Or enter container and install manually
docker run --rm -it weather-processor:latest bash
pip3 install herbie-data --force-reinstall
```

### GRIB2 Error: "eccodes not found"

**Problem**: eccodes not properly installed

**Solution**:
- Check that `libeccodes-dev` is installed
- Verify `ECCODES_DIR` environment variable is set
- Rebuild image if necessary

## Next Steps

After successful build and test:

1. ✅ TICKET-003 Complete
2. 📝 Move to TICKET-004: Create download script (scripts/download_hrrr.py)
3. 📝 Move to TICKET-005: Create variable configuration (config/variables.yaml)
4. 📝 Move to TICKET-006: Create processing script (scripts/process_weather.py)

## Resources

- [GDAL Docker Images](https://github.com/OSGeo/gdal/tree/master/docker)
- [Herbie Documentation](https://herbie.readthedocs.io/)
- [xarray Documentation](https://docs.xarray.dev/)
- [Docker Best Practices](https://docs.docker.com/develop/dev-best-practices/)

---

**Last Updated**: 2026-01-10
