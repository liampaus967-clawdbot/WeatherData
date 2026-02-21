# Herbie Quick Start Guide

This guide provides a quick overview of using Herbie for the weather data pipeline.

## What is Herbie?

Herbie is a Python package that simplifies downloading numerical weather prediction (NWP) model data. It handles the complexity of accessing GRIB2 files from various sources and provides a clean interface for working with forecast data.

**Documentation**: https://herbie.readthedocs.io/en/stable/

## Installation

```bash
# Install via pip
pip install herbie-data

# Or include in requirements.txt
herbie-data>=2024.0.0
cfgrib>=0.9.10
eccodes>=1.6.0
```

## Basic Usage

### Download a Single Forecast Hour

```python
from herbie import Herbie

# Create Herbie object for a specific model run
H = Herbie(
    '2026-01-09 12:00',  # Model run date/time
    model='hrrr',         # Model name
    product='sfc',        # Product type (surface fields)
    fxx=6                 # Forecast hour (F06)
)

# Download and load temperature data into xarray
ds = H.xarray('TMP:2 m')

# Access the data
print(ds)
```

### Download Multiple Variables

```python
# Download temperature, wind components, and precipitation
variables = [
    'TMP:2 m',           # Temperature at 2 meters
    'UGRD:10 m',         # U-component of wind at 10 meters
    'VGRD:10 m',         # V-component of wind at 10 meters
    'APCP'               # Accumulated precipitation
]

for var in variables:
    ds = H.xarray(var)
    print(f"Downloaded {var}")
```

### Download Multiple Forecast Hours

```python
from herbie import FastHerbie

# Download F00-F12 for a single model run
FH = FastHerbie(
    '2026-01-09 12:00',
    model='hrrr',
    fxx=range(0, 13)  # F00 through F12
)

# Download temperature for all forecast hours in parallel
ds = FH.xarray('TMP:2 m', max_threads=4)
```

## Common HRRR Variables

Here are some commonly used GRIB2 search strings for HRRR:

| Variable | Search String | Description |
|----------|---------------|-------------|
| Temperature (2m) | `TMP:2 m` | Temperature at 2 meters above ground |
| Temperature (surface) | `TMP:surface` | Skin temperature |
| Dewpoint (2m) | `DPT:2 m` | Dewpoint temperature at 2 meters |
| U-Wind (10m) | `UGRD:10 m` | U-component of wind at 10 meters |
| V-Wind (10m) | `VGRD:10 m` | V-component of wind at 10 meters |
| Precipitation | `APCP` | Accumulated precipitation |
| Relative Humidity | `RH:2 m` | Relative humidity at 2 meters |
| Cloud Cover | `TCDC` | Total cloud cover |
| Visibility | `VIS` | Visibility |
| Reflectivity | `REFC` | Composite reflectivity (radar) |

## Finding Available Variables

Use the inventory method to see all available variables:

```python
from herbie import Herbie

H = Herbie('2026-01-09 12:00', model='hrrr', fxx=0)

# Get inventory of all variables
inventory = H.inventory()

# Display as DataFrame
print(inventory)

# Search for specific variable
temp_vars = inventory[inventory['search_this'].str.contains('TMP')]
print(temp_vars)
```

## Supported Models

Herbie supports 15+ weather models:

### US Models (NOAA)
- **HRRR** - High-Resolution Rapid Refresh (3km, hourly updates)
- **RAP** - Rapid Refresh (13km)
- **GFS** - Global Forecast System (0.25°, 4x daily)
- **GEFS** - Global Ensemble Forecast System
- **NAM** - North American Mesoscale
- **NBM** - National Blend of Models

### International Models
- **ECMWF** - European Centre (IFS, AIFS models)
- **HRDPS** - Canadian High Resolution Deterministic Prediction System
- **NAVGEM** - US Navy Global Environmental Model

### Example: Switching Models

```python
# HRRR
H = Herbie('2026-01-09 12:00', model='hrrr')

# GFS (global)
H = Herbie('2026-01-09 00:00', model='gfs', fxx=24)

# RAP
H = Herbie('2026-01-09 12:00', model='rap')

# ECMWF
H = Herbie('2026-01-09 00:00', model='ecmwf', product='oper')
```

## Data Sources

Herbie automatically tries multiple sources in order:

1. **AWS** (default, fastest)
2. **Google Cloud Platform**
3. **NOMADS** (NOAA operational servers)
4. **Azure**

If one source is unavailable, Herbie automatically falls back to the next.

```python
# Manually specify a source priority
H = Herbie(
    '2026-01-09 12:00',
    model='hrrr',
    priority=['aws', 'google', 'nomads']
)
```

## Working with xarray Datasets

Herbie loads data directly into xarray, making it easy to work with:

```python
from herbie import Herbie

H = Herbie('2026-01-09 12:00', model='hrrr', fxx=6)
ds = H.xarray('TMP:2 m')

# Convert Kelvin to Celsius
ds['t2m'] = ds['t2m'] - 273.15
ds['t2m'].attrs['units'] = 'Celsius'

# Subset to a region (lat/lon bounds)
ds_subset = ds.sel(
    latitude=slice(50, 25),
    longitude=slice(-125, -65)
)

# Save to NetCDF
ds.to_netcdf('temperature_f06.nc')

# Plot with matplotlib
import matplotlib.pyplot as plt
ds['t2m'].plot()
plt.show()
```

## Integration with Our Pipeline

### 1. Download Script (`scripts/download_hrrr.py`)

```python
from herbie import Herbie

def download_forecast_hour(date, fxx, variables, output_dir):
    """Download a single forecast hour"""
    H = Herbie(date, model='hrrr', product='sfc', fxx=fxx)

    for var in variables:
        ds = H.xarray(var, remove_grib=False)

        # Save to NetCDF
        var_name = var.split(':')[0].lower()
        output_file = f"{output_dir}/hrrr_{H.date:%Y%m%d_%H}z_f{fxx:02d}_{var_name}.nc"
        ds.to_netcdf(output_file)

        print(f"Downloaded {var} to {output_file}")
```

### 2. Processing Script (`scripts/process_weather.py`)

```python
import xarray as xr
import rioxarray

def process_to_cog(input_nc, output_tif):
    """Process NetCDF to Cloud Optimized GeoTIFF"""
    # Load data
    ds = xr.open_dataset(input_nc)
    da = ds[list(ds.data_vars)[0]]

    # Set CRS and reproject
    da = da.rio.write_crs("EPSG:4326")
    da_reproj = da.rio.reproject("EPSG:3857", resampling='bilinear')

    # Save as COG
    da_reproj.rio.to_raster(
        output_tif,
        driver='COG',
        compress='DEFLATE'
    )
```

## Best Practices

### 1. Use FastHerbie for Bulk Downloads

```python
# Better: Parallel downloads
from herbie import FastHerbie

FH = FastHerbie(
    '2026-01-09 12:00',
    model='hrrr',
    fxx=range(0, 13)
)
ds = FH.xarray('TMP:2 m', max_threads=4)
```

Instead of:

```python
# Slower: Sequential downloads
for fxx in range(0, 13):
    H = Herbie('2026-01-09 12:00', model='hrrr', fxx=fxx)
    ds = H.xarray('TMP:2 m')
```

### 2. Use Specific Search Strings

```python
# Better: Specific level
ds = H.xarray('TMP:2 m above ground')

# Avoid: May match multiple variables
ds = H.xarray('TMP')  # Could match TMP:surface, TMP:2 m, etc.
```

### 3. Handle Errors Gracefully

```python
try:
    H = Herbie('2026-01-09 12:00', model='hrrr', fxx=6)
    ds = H.xarray('TMP:2 m')
except Exception as e:
    print(f"Download failed: {e}")
    # Fall back to different source or skip
```

### 4. Cache Downloaded Files

Herbie automatically caches GRIB2 files in `~/.cache/herbie/` to avoid re-downloading.

```python
# Remove cache to force fresh download
H = Herbie('2026-01-09 12:00', model='hrrr', fxx=0)
H.download('TMP:2 m', overwrite=True)
```

## Advanced Features

### 1. Get File Information Without Downloading

```python
H = Herbie('2026-01-09 12:00', model='hrrr', fxx=6)

# Get inventory without downloading
inv = H.inventory()
print(inv)

# Get GRIB2 file URL
print(H.grib)

# Get .idx file URL
print(H.idx)
```

### 2. Download Raw GRIB2 File

```python
# Download full GRIB2 file (not recommended, very large)
H.download()

# Download subset to local file
local_file = H.download('TMP:2 m', save_dir='/tmp/grib/')
```

### 3. Herbie with Dask for Large Datasets

```python
import dask

# Open multiple files with dask
ds = xr.open_mfdataset(
    '/tmp/hrrr_*.nc',
    parallel=True,
    engine='netcdf4'
)

# Lazy evaluation - computations only run when needed
mean_temp = ds['t2m'].mean()
result = mean_temp.compute()  # Actually compute here
```

## Troubleshooting

### Issue: "No GRIB2 file found"

**Cause**: Data not yet available on servers (HRRR has ~2-3 hour delay)

**Solution**: Use model run time 3+ hours in the past

```python
from datetime import datetime, timedelta

# Use data from 3 hours ago
model_time = datetime.utcnow() - timedelta(hours=3)
H = Herbie(model_time, model='hrrr', fxx=0)
```

### Issue: "Variable not found in inventory"

**Cause**: Incorrect search string or variable not in this product

**Solution**: Check inventory first

```python
H = Herbie('2026-01-09 12:00', model='hrrr', product='sfc', fxx=0)
print(H.inventory())

# Verify your search string exists
```

### Issue: "cfgrib.dataset.DatasetBuildError"

**Cause**: Missing eccodes library

**Solution**: Install system dependencies

```bash
# Ubuntu/Debian
sudo apt-get install libeccodes-dev libeccodes-tools

# macOS
brew install eccodes

# Then reinstall cfgrib
pip install cfgrib --force-reinstall
```

## Resources

- **Official Documentation**: https://herbie.readthedocs.io/
- **GitHub Repository**: https://github.com/blaylockbk/Herbie
- **HRRR Model Info**: https://rapidrefresh.noaa.gov/hrrr/
- **GRIB2 Variable Tables**: https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/

## Next Steps

1. Review updated **TICKET-004** in TICKETS.md for download script implementation
2. Review updated **TICKET-006** in TICKETS.md for processing script implementation
3. Check `config/variables.yaml` example in **TICKET-005** for variable configuration
4. Test Herbie locally before integrating into Docker container

---

**Last Updated**: 2026-01-10
