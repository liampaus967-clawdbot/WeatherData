#!/bin/bash
# Test script for weather-processor Docker image

set -e

echo "Testing weather-processor Docker image..."
echo ""

# Test 1: Check versions
echo "Test 1: Checking installed versions..."
docker run --rm weather-processor:latest

echo ""
echo "✅ Test 1 passed: All packages installed"
echo ""

# Test 2: Test Herbie functionality
echo "Test 2: Testing Herbie functionality..."
docker run --rm weather-processor:latest python3 -c "
from herbie import Herbie
import datetime

# Create a Herbie object (won't download, just test initialization)
dt = datetime.datetime(2026, 1, 9, 12, 0)
H = Herbie(dt, model='hrrr', fxx=0)
print(f'✓ Herbie initialized successfully')
print(f'  Model: {H.model}')
print(f'  Date: {H.date}')
print(f'  Forecast hour: {H.fxx}')
"

echo ""
echo "✅ Test 2 passed: Herbie works correctly"
echo ""

# Test 3: Test GDAL
echo "Test 3: Testing GDAL functionality..."
docker run --rm weather-processor:latest python3 -c "
from osgeo import gdal
import sys

# Enable exceptions
gdal.UseExceptions()

# Check GDAL drivers
driver_count = gdal.GetDriverCount()
print(f'✓ GDAL has {driver_count} drivers available')

# Check for important drivers
drivers = ['GTiff', 'COG', 'netCDF', 'GRIB']
for driver_name in drivers:
    driver = gdal.GetDriverByName(driver_name)
    if driver:
        print(f'  ✓ {driver_name} driver available')
    else:
        print(f'  ✗ {driver_name} driver NOT available')
        sys.exit(1)
"

echo ""
echo "✅ Test 3 passed: GDAL works correctly"
echo ""

# Test 4: Test xarray and rioxarray
echo "Test 4: Testing xarray and rioxarray..."
docker run --rm weather-processor:latest python3 -c "
import xarray as xr
import rioxarray
import numpy as np

# Create a sample dataset
data = np.random.rand(10, 10)
lats = np.linspace(25, 50, 10)
lons = np.linspace(-125, -65, 10)

ds = xr.Dataset(
    {'temperature': (['lat', 'lon'], data)},
    coords={'lat': lats, 'lon': lons}
)

# Add CRS
ds['temperature'].rio.write_crs('EPSG:4326', inplace=True)

print('✓ xarray dataset created')
print('✓ rioxarray CRS set')
print(f'  Dataset shape: {ds.temperature.shape}')
print(f'  CRS: {ds.temperature.rio.crs}')
"

echo ""
echo "✅ Test 4 passed: xarray and rioxarray work correctly"
echo ""

# Test 5: Test cfgrib (GRIB2 support)
echo "Test 5: Testing cfgrib (GRIB2 decoder)..."
docker run --rm weather-processor:latest python3 -c "
import cfgrib
print('✓ cfgrib imported successfully')
print(f'  cfgrib version: {cfgrib.__version__}')
"

echo ""
echo "✅ Test 5 passed: cfgrib works correctly"
echo ""

# Test 6: Test AWS SDK
echo "Test 6: Testing boto3 (AWS SDK)..."
docker run --rm weather-processor:latest python3 -c "
import boto3
print('✓ boto3 imported successfully')
print(f'  boto3 version: {boto3.__version__}')
# Note: Won't test actual S3 access without credentials
"

echo ""
echo "✅ Test 6 passed: boto3 works correctly"
echo ""

echo "========================================="
echo "🎉 All tests passed!"
echo "========================================="
echo ""
echo "The weather-processor image is ready to use."
echo ""
echo "Next steps:"
echo "  1. Create scripts/download_hrrr.py"
echo "  2. Test downloading actual HRRR data"
echo "  3. Create scripts/process_weather.py"
echo "  4. Test processing workflow"
