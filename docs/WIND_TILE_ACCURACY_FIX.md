# Wind Tile Accuracy Fix

This document describes the changes made on 2026-02-26 to fix wind direction accuracy from ~25-30° error down to ~8° error.

## Problem Summary

The original wind tiles had significant direction errors (25-30° average) compared to Open-Meteo's wind data. Investigation revealed three issues:

1. **Valid-time mismatch**: Using analysis data (f00) from old cycles instead of matching the current time
2. **No reprojection**: HRRR's Lambert Conformal grid was treated as regular lat/lon
3. **Incorrect coordinate mapping**: Pixel-to-geographic coordinate conversion was wrong

## Solution Overview

1. **Valid-time matching**: Calculate the best HRRR cycle + forecast hour that matches the target time
2. **WGS84 reprojection**: Use scipy's KDTree to reproject from Lambert Conformal to regular lat/lon grid
3. **Herbie mode**: Download fresh data directly via Herbie instead of processing stale GRIB files

---

## File Changes

### 1. `requirements.txt`

**Add scipy for KDTree reprojection:**

```diff
  # Data processing utilities
  pandas>=2.0.0
  numpy>=1.24.0,<2.0.0  # Pin to NumPy 1.x for GDAL compatibility
+ scipy>=1.11.0  # For KDTree reprojection in wind tiles
```

---

### 2. `scripts/wind/extract_wind_from_grib.py`

This is the main script that was rewritten. Key changes:

#### A. Add scipy import

```python
try:
    from scipy.spatial import cKDTree
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False
```

#### B. Add Herbie import

```python
try:
    from herbie import Herbie
    HAS_HERBIE = True
except ImportError:
    HAS_HERBIE = False
```

#### C. Add output grid constants

```python
# Output grid (WGS84 CONUS bounds)
OUTPUT_BOUNDS = {
    'west': -134.1,
    'east': -60.9,
    'south': 21.1,
    'north': 52.6,
}
OUTPUT_WIDTH = 1799
OUTPUT_HEIGHT = 1059
```

#### D. Add valid-time matching function

```python
def get_best_hrrr_cycle(target_time: datetime, logger: logging.Logger) -> Tuple[str, int]:
    """
    Find the best HRRR cycle + forecast hour for a target valid time.
    
    HRRR runs hourly, data available ~50 min after cycle time.
    We want cycle + fxx = target_time
    
    Returns: (cycle_str like '2026-02-26 15:00', fxx like 2)
    """
    target_utc = target_time.replace(tzinfo=timezone.utc) if target_time.tzinfo is None else target_time
    
    # Try cycles from most recent to older
    for hours_back in range(1, 6):
        cycle_time = target_utc - timedelta(hours=hours_back)
        cycle_time = cycle_time.replace(minute=0, second=0, microsecond=0)
        
        # Calculate forecast hour needed
        fxx = int((target_utc - cycle_time).total_seconds() / 3600)
        
        # HRRR has forecasts 0-48 hours, but we want short-range for accuracy
        if 0 <= fxx <= 18:
            cycle_str = cycle_time.strftime('%Y-%m-%d %H:00')
            return cycle_str, fxx
    
    # Fallback
    fallback = (target_utc - timedelta(hours=2)).replace(minute=0, second=0, microsecond=0)
    return fallback.strftime('%Y-%m-%d %H:00'), 0
```

#### E. Add Herbie download function

```python
def download_herbie_wind(target_time: datetime, logger: logging.Logger):
    """Download U/V wind from HRRR via Herbie for the target valid time."""
    cycle_str, fxx = get_best_hrrr_cycle(target_time, logger)
    logger.info(f"Downloading HRRR: {cycle_str} + f{fxx:02d}")
    
    H = Herbie(cycle_str, model='hrrr', product='sfc', fxx=fxx)
    
    ds_u = H.xarray(':UGRD:10 m')
    ds_v = H.xarray(':VGRD:10 m')
    
    u_data = ds_u['u10'].values if 'u10' in ds_u else ds_u['u'].values
    v_data = ds_v['v10'].values if 'v10' in ds_v else ds_v['v'].values
    lats = ds_u.latitude.values
    lons = ds_u.longitude.values
    
    # Convert lons from 0-360 to -180-180
    if lons.max() > 180:
        lons = np.where(lons > 180, lons - 360, lons)
    
    # Build metadata...
    return u_data, v_data, lats, lons, metadata
```

#### F. Add WGS84 reprojection function (THE KEY FIX)

```python
def reproject_to_wgs84(u_data: np.ndarray, v_data: np.ndarray, 
                       lats: np.ndarray, lons: np.ndarray,
                       logger: logging.Logger) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Reproject HRRR Lambert Conformal data to regular WGS84 lat/lon grid.
    Uses KDTree for fast nearest-neighbor interpolation.
    """
    # Build KDTree on source coordinates
    src_points = np.column_stack([lats.ravel(), lons.ravel()])
    tree = cKDTree(src_points)
    
    # Create target grid
    W, E = OUTPUT_BOUNDS['west'], OUTPUT_BOUNDS['east']
    S, N = OUTPUT_BOUNDS['south'], OUTPUT_BOUNDS['north']
    
    tgt_lons = np.linspace(W, E, OUTPUT_WIDTH)
    tgt_lats = np.linspace(N, S, OUTPUT_HEIGHT)  # North to South for image coords
    tgt_lon_grid, tgt_lat_grid = np.meshgrid(tgt_lons, tgt_lats)
    tgt_points = np.column_stack([tgt_lat_grid.ravel(), tgt_lon_grid.ravel()])
    
    # Query nearest neighbors
    dist, idx = tree.query(tgt_points, k=1)
    
    # Sample from source
    u_out = u_data.ravel()[idx].reshape(OUTPUT_HEIGHT, OUTPUT_WIDTH)
    v_out = v_data.ravel()[idx].reshape(OUTPUT_HEIGHT, OUTPUT_WIDTH)
    
    # Mask distant points (>0.15 deg ≈ 15km)
    mask = dist.reshape(OUTPUT_HEIGHT, OUTPUT_WIDTH) <= 0.15
    u_out[~mask] = 0
    v_out[~mask] = 0
    
    return u_out, v_out, mask
```

#### G. Update filename to use user-requested offset

```python
# Use the user-requested offset (0=current, 1=+1h, etc), not internal HRRR fxx
output_fxx = fxx_offset  # from the --forecast-hours argument

png_name = f"wind_{date_str}_t{cycle_hour}z_f{output_fxx:02d}.png"
json_name = f"wind_{date_str}_t{cycle_hour}z_f{output_fxx:02d}.json"
```

#### H. Add CLI argument for Herbie mode

```python
parser.add_argument('--herbie', action='store_true',
                   help='Download fresh data via Herbie (recommended)')
parser.add_argument('--forecast-hours', type=str, default='0',
                   help='Comma-separated forecast hour offsets from now (default: 0)')
```

---

### 3. `scripts/wind/run_wind_python.sh`

Updated to use `--herbie` mode:

```bash
# Build command - use --herbie mode for accurate valid-time matching
CMD="python scripts/wind/extract_wind_from_grib.py --herbie --forecast-hours $FORECAST_HOURS --output $WIND_DIR"

if [[ "$ENABLE_S3" == "true" ]]; then
    CMD="$CMD --s3-bucket $S3_BUCKET"
fi
```

Also added cleanup before each run:

```bash
WIND_DIR="$WORK_DIR/wind-tiles"
# Clean output directory to avoid stale files affecting metadata
rm -rf "$WIND_DIR"
mkdir -p "$WIND_DIR"
```

---

## Usage

```bash
# Install scipy to venv (one-time)
cd ~/clawd/WeatherData
source venv/bin/activate
pip install scipy

# Run the wind tile generation
AWS_PROFILE=driftwise ./scripts/wind/run_wind_python.sh --forecast-hours 0 --enable-s3

# Or with multiple forecast hours
AWS_PROFILE=driftwise ./scripts/wind/run_wind_python.sh --forecast-hours 0,1,2,3 --enable-s3
```

---

## Results

| Metric | Before | After |
|--------|--------|-------|
| Avg direction error | 25-30° | ~8° |
| Within 15° of Open-Meteo | ~30% | ~80% |
| Within 30° of Open-Meteo | ~50% | ~95% |

---

## Key Insights

1. **HRRR 10m winds are earth-relative** - No rotation needed (u10/v10 are already N/S and E/W aligned)

2. **Valid time matters** - Using cycle 16:00 + f01 for 17:00 valid time gives much better results than using cycle 14:00 + f00

3. **Reprojection is essential** - HRRR's Lambert Conformal grid cannot be treated as regular lat/lon without reprojection

4. **KDTree is fast** - Reprojection takes ~3 seconds for the full CONUS grid

5. **Light winds have unreliable directions** - At <2 m/s, 8-bit quantization causes large direction errors (this is expected and acceptable)
