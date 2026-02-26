#!/usr/bin/env python3
"""
Extract wind tiles from HRRR data.

Two modes:
1. --herbie: Download fresh data via Herbie with correct valid-time matching
2. --input: Process existing GRIB2/NetCDF files from a directory

Usage:
    # Download fresh data for current time
    python extract_wind_from_grib.py --herbie --output /data/wind-tiles --s3-bucket driftwise-weather-data
    
    # Process existing files
    python extract_wind_from_grib.py --input /data/downloads --output /data/wind-tiles
"""

import argparse
import logging
import os
import sys
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple, List
import re

import numpy as np
from PIL import Image

try:
    from scipy.spatial import cKDTree
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

try:
    from herbie import Herbie
    HAS_HERBIE = True
except ImportError:
    HAS_HERBIE = False

try:
    import pygrib
except ImportError:
    pygrib = None

try:
    import xarray as xr
    import cfgrib
    HAS_CFGRIB = True
except ImportError:
    HAS_CFGRIB = False

import boto3
from botocore.exceptions import ClientError


# Wind encoding parameters (HRRR wind typically -50 to +50 m/s)
WIND_MIN = -50.0
WIND_MAX = 50.0

# Output grid (WGS84 CONUS bounds)
OUTPUT_BOUNDS = {
    'west': -134.1,
    'east': -60.9,
    'south': 21.1,
    'north': 52.6,
}
OUTPUT_WIDTH = 1799
OUTPUT_HEIGHT = 1059


def setup_logging(verbose: bool = False) -> logging.Logger:
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger('wind_extract')


def get_best_hrrr_cycle(target_time: datetime, logger: logging.Logger) -> Tuple[str, int]:
    """
    Find the best HRRR cycle + forecast hour for a target valid time.
    
    HRRR runs hourly, data available ~50 min after cycle time.
    We want cycle + fxx = target_time
    
    Returns: (cycle_str like '2026-02-26 15:00', fxx like 2)
    """
    target_utc = target_time.replace(tzinfo=timezone.utc) if target_time.tzinfo is None else target_time
    
    # Try cycles from most recent to older, finding one that's available
    # and has a forecast hour that reaches our target time
    for hours_back in range(1, 6):
        cycle_time = target_utc - timedelta(hours=hours_back)
        cycle_time = cycle_time.replace(minute=0, second=0, microsecond=0)
        
        # Calculate forecast hour needed
        fxx = int((target_utc - cycle_time).total_seconds() / 3600)
        
        # HRRR has forecasts 0-48 hours, but we want short-range for accuracy
        if 0 <= fxx <= 18:
            cycle_str = cycle_time.strftime('%Y-%m-%d %H:00')
            logger.debug(f"Trying cycle {cycle_str} + f{fxx:02d} for valid time {target_utc.strftime('%H:%M UTC')}")
            return cycle_str, fxx
    
    # Fallback to 2 hours ago with f00
    fallback = (target_utc - timedelta(hours=2)).replace(minute=0, second=0, microsecond=0)
    return fallback.strftime('%Y-%m-%d %H:00'), 0


def download_herbie_wind(target_time: datetime, logger: logging.Logger) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], Optional[np.ndarray], Optional[np.ndarray], Optional[dict]]:
    """
    Download U/V wind from HRRR via Herbie for the target valid time.
    
    Returns: (u_data, v_data, lats, lons, metadata) or (None, None, None, None, None) on failure
    """
    if not HAS_HERBIE:
        logger.error("Herbie not installed. Run: pip install herbie-data")
        return None, None, None, None, None
    
    cycle_str, fxx = get_best_hrrr_cycle(target_time, logger)
    logger.info(f"Downloading HRRR: {cycle_str} + f{fxx:02d}")
    
    try:
        H = Herbie(cycle_str, model='hrrr', product='sfc', fxx=fxx)
        
        logger.debug("Fetching U wind component...")
        ds_u = H.xarray(':UGRD:10 m')
        
        logger.debug("Fetching V wind component...")
        ds_v = H.xarray(':VGRD:10 m')
        
        # Extract data
        u_data = ds_u['u10'].values if 'u10' in ds_u else ds_u['u'].values
        v_data = ds_v['v10'].values if 'v10' in ds_v else ds_v['v'].values
        lats = ds_u.latitude.values
        lons = ds_u.longitude.values
        
        # Convert lons from 0-360 to -180-180
        if lons.max() > 180:
            lons = np.where(lons > 180, lons - 360, lons)
        
        # Parse cycle info for metadata
        cycle_dt = datetime.strptime(cycle_str, '%Y-%m-%d %H:%M')
        valid_dt = cycle_dt + timedelta(hours=fxx)
        
        metadata = {
            'source': 'HRRR via Herbie',
            'cycle': cycle_str,
            'forecast_hour': fxx,
            'valid_time': valid_dt.strftime('%Y-%m-%d %H:%M UTC'),
            'shape': list(u_data.shape),
            'wind_encoding': {
                'min': WIND_MIN,
                'max': WIND_MAX,
                'unit': 'm/s',
                'r_channel': 'U component (east-west)',
                'g_channel': 'V component (north-south)',
                'b_channel': 'magnitude',
                'encoding': '128 = 0 m/s, 0 = -50 m/s, 255 = +50 m/s'
            },
            'bounds': OUTPUT_BOUNDS,
            'projection': 'EPSG:4326 (WGS84)',
            'reprojected': True,
        }
        
        logger.info(f"Downloaded: shape={u_data.shape}, valid={valid_dt.strftime('%H:%M UTC')}")
        return u_data, v_data, lats, lons, metadata
        
    except Exception as e:
        logger.error(f"Herbie download failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return None, None, None, None, None


def reproject_to_wgs84(u_data: np.ndarray, v_data: np.ndarray, 
                       lats: np.ndarray, lons: np.ndarray,
                       logger: logging.Logger) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Reproject HRRR Lambert Conformal data to regular WGS84 lat/lon grid.
    
    Uses KDTree for fast nearest-neighbor interpolation.
    
    Returns: (u_reprojected, v_reprojected, valid_mask)
    """
    if not HAS_SCIPY:
        logger.error("scipy not installed. Run: pip install scipy")
        raise ImportError("scipy required for reprojection")
    
    logger.info("Reprojecting to WGS84 grid...")
    
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
    logger.debug("Querying KDTree for nearest neighbors...")
    dist, idx = tree.query(tgt_points, k=1)
    
    # Sample from source
    u_out = u_data.ravel()[idx].reshape(OUTPUT_HEIGHT, OUTPUT_WIDTH)
    v_out = v_data.ravel()[idx].reshape(OUTPUT_HEIGHT, OUTPUT_WIDTH)
    
    # Mask distant points (>0.15 deg ≈ 15km)
    mask = dist.reshape(OUTPUT_HEIGHT, OUTPUT_WIDTH) <= 0.15
    u_out[~mask] = 0
    v_out[~mask] = 0
    
    valid_pct = 100 * mask.sum() / mask.size
    logger.info(f"Reprojection complete: {valid_pct:.1f}% valid pixels")
    
    return u_out, v_out, mask


def encode_wind_component(value: np.ndarray) -> np.ndarray:
    """Encode wind component to 0-255. 0 m/s maps to 128."""
    clipped = np.clip(value, WIND_MIN, WIND_MAX)
    normalized = (clipped - WIND_MIN) / (WIND_MAX - WIND_MIN)
    return (normalized * 255).astype(np.uint8)


def create_wind_image(u_data: np.ndarray, v_data: np.ndarray, 
                      valid_mask: Optional[np.ndarray] = None) -> Image.Image:
    """Create RGBA image: R=U, G=V, B=magnitude, A=valid"""
    r_channel = encode_wind_component(u_data)
    g_channel = encode_wind_component(v_data)
    
    # Magnitude for B channel
    magnitude = np.sqrt(u_data**2 + v_data**2)
    max_speed = math.sqrt(WIND_MAX**2 + WIND_MAX**2)
    b_channel = np.clip((magnitude / max_speed) * 255, 0, 255).astype(np.uint8)
    
    # Alpha channel
    if valid_mask is not None:
        a_channel = np.where(valid_mask, 255, 0).astype(np.uint8)
    else:
        a_channel = np.where(np.isnan(u_data) | np.isnan(v_data), 0, 255).astype(np.uint8)
    
    # Handle NaN
    r_channel = np.nan_to_num(r_channel, nan=128).astype(np.uint8)
    g_channel = np.nan_to_num(g_channel, nan=128).astype(np.uint8)
    b_channel = np.nan_to_num(b_channel, nan=0).astype(np.uint8)
    
    rgba = np.stack([r_channel, g_channel, b_channel, a_channel], axis=-1)
    return Image.fromarray(rgba, mode='RGBA')


def create_wind_image_legacy(u_data: np.ndarray, v_data: np.ndarray) -> Image.Image:
    """Legacy: Create wind image without reprojection (for existing GRIB files)."""
    # Flip arrays vertically - GRIB stores row 0 as SOUTH, but PNG row 0 should be NORTH
    u_data = np.flipud(u_data)
    v_data = np.flipud(v_data)
    return create_wind_image(u_data, v_data)


def upload_to_s3(local_path: Path, s3_bucket: str, s3_key: str, logger: logging.Logger) -> bool:
    """Upload file to S3."""
    try:
        s3 = boto3.client('s3')
        content_type = 'image/png' if local_path.suffix == '.png' else 'application/json'
        s3.upload_file(
            str(local_path),
            s3_bucket,
            s3_key,
            ExtraArgs={'ContentType': content_type}
        )
        logger.debug(f"Uploaded: s3://{s3_bucket}/{s3_key}")
        return True
    except ClientError as e:
        logger.error(f"S3 upload failed: {e}")
        return False


def process_herbie(output_dir: Path, s3_bucket: Optional[str], 
                   forecast_hours: List[int], logger: logging.Logger) -> int:
    """Download and process wind data via Herbie."""
    output_dir.mkdir(parents=True, exist_ok=True)
    processed = 0
    
    now = datetime.now(timezone.utc)
    
    for fxx_offset in forecast_hours:
        target_time = now + timedelta(hours=fxx_offset)
        logger.info(f"Processing for valid time: {target_time.strftime('%Y-%m-%d %H:%M UTC')}")
        
        # Download data
        u_data, v_data, lats, lons, metadata = download_herbie_wind(target_time, logger)
        if u_data is None:
            logger.warning(f"Skipping forecast hour {fxx_offset}")
            continue
        
        # Reproject to WGS84
        u_reproj, v_reproj, valid_mask = reproject_to_wgs84(u_data, v_data, lats, lons, logger)
        
        # Create image
        wind_img = create_wind_image(u_reproj, v_reproj, valid_mask)
        
        # Output filenames (using cycle-based format for app compatibility)
        date_str = target_time.strftime('%Y%m%d')
        # Extract cycle hour from metadata
        cycle_str = metadata.get('cycle', '')
        if cycle_str:
            cycle_hour = cycle_str.split()[1].replace(':00', '')
        else:
            cycle_hour = target_time.strftime('%H')
        # Use the user-requested offset (0=current, 1=+1h, etc), not internal HRRR fxx
        output_fxx = fxx_offset
        
        png_name = f"wind_{date_str}_t{cycle_hour}z_f{output_fxx:02d}.png"
        json_name = f"wind_{date_str}_t{cycle_hour}z_f{output_fxx:02d}.json"
        
        png_path = output_dir / png_name
        json_path = output_dir / json_name
        
        # Save locally
        wind_img.save(png_path, 'PNG')
        with open(json_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Created: {png_name} ({wind_img.size[0]}x{wind_img.size[1]})")
        
        # Upload to S3 using cycle-based folder structure
        if s3_bucket:
            # Get the cycle hour from metadata
            cycle_hour = metadata.get('cycle', '').split()[1].replace(':00', '') if metadata.get('cycle') else target_time.strftime('%H')
            s3_prefix = f"wind-tiles/{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}/{cycle_hour}Z"
            upload_to_s3(png_path, s3_bucket, f"{s3_prefix}/{png_name}", logger)
            upload_to_s3(json_path, s3_bucket, f"{s3_prefix}/{json_name}", logger)
        
        processed += 1
    
    return processed


# ============================================================================
# Legacy GRIB file processing (kept for backward compatibility)
# ============================================================================

def extract_wind_from_grib(grib_path: Path, logger: logging.Logger) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], Optional[dict]]:
    """Extract U and V wind components from a GRIB2 file (legacy method)."""
    logger.info(f"Extracting wind from: {grib_path.name}")
    
    u_data = None
    v_data = None
    lats = None
    lons = None
    
    # Try pygrib first (faster)
    if pygrib is not None:
        try:
            grbs = pygrib.open(str(grib_path))
            for grb in grbs:
                if grb.shortName == '10u' or (grb.parameterName == 'U component of wind' and grb.level == 10):
                    u_data = grb.values
                    lats, lons = grb.latlons()
                elif grb.shortName == '10v' or (grb.parameterName == 'V component of wind' and grb.level == 10):
                    v_data = grb.values
            grbs.close()
        except Exception as e:
            logger.warning(f"pygrib failed: {e}, trying cfgrib...")
    
    # Fallback to cfgrib/xarray
    if (u_data is None or v_data is None) and HAS_CFGRIB:
        filter_attempts = [
            {'typeOfLevel': 'heightAboveGround', 'level': 10},
            {'shortName': ['10u', '10v']},
            {},
        ]
        
        for filter_keys in filter_attempts:
            try:
                if filter_keys:
                    ds = xr.open_dataset(grib_path, engine='cfgrib', filter_by_keys=filter_keys)
                else:
                    ds = xr.open_dataset(grib_path, engine='cfgrib', backend_kwargs={'indexpath': ''})
                
                for var_name in ['u10', 'u', '10u', 'UGRD_10maboveground']:
                    if var_name in ds:
                        u_data = ds[var_name].values.squeeze()
                        break
                
                for var_name in ['v10', 'v', '10v', 'VGRD_10maboveground']:
                    if var_name in ds:
                        v_data = ds[var_name].values.squeeze()
                        break
                
                if 'latitude' in ds:
                    lats = ds['latitude'].values
                    lons = ds['longitude'].values
                
                ds.close()
                
                if u_data is not None and v_data is not None:
                    break
            except Exception:
                continue
    
    if u_data is None or v_data is None:
        logger.warning(f"Could not find U/V wind components in {grib_path.name}")
        return None, None, None
    
    if lons is not None and lons.max() > 180:
        lons = np.where(lons > 180, lons - 360, lons)
    
    metadata = {
        'source_file': grib_path.name,
        'shape': list(u_data.shape),
        'wind_encoding': {
            'min': WIND_MIN,
            'max': WIND_MAX,
            'unit': 'm/s',
            'r_channel': 'U component (east-west)',
            'g_channel': 'V component (north-south)',
            'b_channel': 'magnitude',
            'encoding': '128 = 0 m/s, 0 = -50 m/s, 255 = +50 m/s'
        }
    }
    
    if lats is not None:
        metadata['bounds'] = {
            'west': float(lons.min()),
            'east': float(lons.max()),
            'south': float(lats.min()),
            'north': float(lats.max()),
        }
        metadata['reprojected'] = False
        metadata['warning'] = 'Legacy mode: not reprojected to WGS84'
    
    logger.info(f"Extracted wind: shape={u_data.shape}")
    return u_data, v_data, metadata


def parse_grib_filename(filename: str) -> Optional[dict]:
    """Extract date, cycle, fxx from GRIB filename."""
    patterns = [
        r'hrrr\.(\d{8})\.t(\d{2})z\.wrfsfcf(\d{2})\.grib2',
        r'hrrr\.(\d{8})\.t(\d{2})z\.f(\d{2})\.grib2',
        r'hrrr\.(\d{8})\.t(\d{2})z\.f(\d{2})\.nc',
    ]
    for pattern in patterns:
        match = re.match(pattern, filename)
        if match:
            return {
                'date': match.group(1),
                'cycle': match.group(2),
                'fxx': match.group(3)
            }
    return None


def process_grib_files(input_dir: Path, output_dir: Path, 
                       s3_bucket: Optional[str], logger: logging.Logger) -> int:
    """Process existing GRIB2 files (legacy method)."""
    grib_files = sorted(input_dir.glob("*.grib2"))
    nc_files = sorted(input_dir.glob("*.nc"))
    all_files = grib_files + nc_files
    
    if not all_files:
        logger.warning(f"No GRIB2 or NetCDF files found in {input_dir}")
        return 0
    
    logger.info(f"Found {len(all_files)} files to process")
    logger.warning("Using legacy mode without WGS84 reprojection")
    
    processed = 0
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for file_path in all_files:
        file_info = parse_grib_filename(file_path.name)
        if not file_info:
            logger.warning(f"Could not parse filename: {file_path.name}")
            continue
        
        u_data, v_data, metadata = extract_wind_from_grib(file_path, logger)
        if u_data is None:
            continue
        
        wind_img = create_wind_image_legacy(u_data, v_data)
        
        date_str = file_info['date']
        cycle = file_info['cycle']
        fxx = file_info['fxx']
        
        png_name = f"wind_{date_str}_t{cycle}z_f{fxx}.png"
        json_name = f"wind_{date_str}_t{cycle}z_f{fxx}.json"
        
        png_path = output_dir / png_name
        json_path = output_dir / json_name
        
        wind_img.save(png_path, 'PNG')
        with open(json_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Created: {png_name}")
        
        if s3_bucket:
            s3_prefix = f"wind-tiles/{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}/{cycle}Z"
            upload_to_s3(png_path, s3_bucket, f"{s3_prefix}/{png_name}", logger)
            upload_to_s3(json_path, s3_bucket, f"{s3_prefix}/{json_name}", logger)
        
        processed += 1
    
    return processed


def main():
    parser = argparse.ArgumentParser(
        description='Extract wind tiles from HRRR data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download current wind data via Herbie (recommended)
  python extract_wind_from_grib.py --herbie --output ./wind-tiles --s3-bucket my-bucket
  
  # Download with multiple forecast hours (0=now, 1=+1h, 2=+2h, etc.)
  python extract_wind_from_grib.py --herbie --forecast-hours 0,1,2,3 --output ./wind-tiles
  
  # Process existing GRIB files (legacy, no reprojection)
  python extract_wind_from_grib.py --input ./downloads --output ./wind-tiles
        """
    )
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--herbie', action='store_true',
                           help='Download fresh data via Herbie (recommended)')
    mode_group.add_argument('--input', '-i', type=Path,
                           help='Input directory with existing GRIB2 files (legacy)')
    
    # Common options
    parser.add_argument('--output', '-o', type=Path, required=True,
                       help='Output directory for wind tiles')
    parser.add_argument('--s3-bucket', help='S3 bucket for upload (optional)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    # Herbie-specific options
    parser.add_argument('--forecast-hours', type=str, default='0',
                       help='Comma-separated forecast hour offsets from now (default: 0)')
    
    args = parser.parse_args()
    logger = setup_logging(args.verbose)
    
    logger.info("=" * 50)
    logger.info("Wind Tile Extraction")
    logger.info("=" * 50)
    
    if args.herbie:
        if not HAS_HERBIE:
            logger.error("Herbie not installed. Run: pip install herbie-data")
            sys.exit(1)
        if not HAS_SCIPY:
            logger.error("scipy not installed. Run: pip install scipy")
            sys.exit(1)
        
        forecast_hours = [int(h.strip()) for h in args.forecast_hours.split(',')]
        logger.info(f"Mode: Herbie download with WGS84 reprojection")
        logger.info(f"Forecast hours: {forecast_hours}")
        logger.info(f"Output: {args.output}")
        
        processed = process_herbie(args.output, args.s3_bucket, forecast_hours, logger)
    else:
        if not args.input.exists():
            logger.error(f"Input directory does not exist: {args.input}")
            sys.exit(1)
        
        logger.info(f"Mode: Legacy GRIB file processing")
        logger.info(f"Input: {args.input}")
        logger.info(f"Output: {args.output}")
        
        processed = process_grib_files(args.input, args.output, args.s3_bucket, logger)
    
    logger.info("=" * 50)
    logger.info(f"Processed {processed} wind tiles")
    logger.info("=" * 50)
    
    sys.exit(0 if processed > 0 else 1)


if __name__ == '__main__':
    main()
