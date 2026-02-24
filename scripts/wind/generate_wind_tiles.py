#!/usr/bin/env python3
"""
Wind UV Tile Generator for Custom Particle Rendering

Downloads HRRR U/V wind components and generates PNG tiles where:
- R channel = U component (east-west wind, encoded 0-255)
- G channel = V component (north-south wind, encoded 0-255)
- B channel = Wind magnitude (optional)
- A channel = 255 (fully opaque)

These tiles can be used with custom WebGL wind particle renderers
without requiring Mapbox's raster-particle layer.

Usage:
    python generate_wind_tiles.py --latest
    python generate_wind_tiles.py --date 2026-02-24 --cycle 12
"""

import argparse
import logging
import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple
import math

import numpy as np
from PIL import Image
import boto3
from botocore.exceptions import ClientError

try:
    from herbie import Herbie
    import xarray as xr
    import rasterio
    from rasterio.transform import from_bounds
    from rasterio.warp import calculate_default_transform, reproject, Resampling
    from pyproj import CRS
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Install with: pip install herbie-data xarray rasterio pyproj pillow boto3")
    sys.exit(1)


# Configuration
DEFAULT_S3_BUCKET = os.environ.get("S3_BUCKET", "driftwise-weather-data")
WIND_TILES_PREFIX = "wind-particles"
TEMP_DIR = Path("/tmp/wind-tiles")
LOG_DIR = Path(os.environ.get("LOG_DIR", "/var/log/weather-pipeline"))

# Wind encoding parameters
# HRRR wind typically ranges from -50 to +50 m/s
WIND_MIN = -50.0  # m/s
WIND_MAX = 50.0   # m/s

# Tile settings
TILE_SIZE = 256
MIN_ZOOM = 0
MAX_ZOOM = 6


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging."""
    log_level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    return logging.getLogger('wind_tiles')


def encode_wind_component(value: np.ndarray, vmin: float = WIND_MIN, vmax: float = WIND_MAX) -> np.ndarray:
    """
    Encode wind component to 0-255 range.
    
    Maps [vmin, vmax] -> [0, 255]
    Midpoint (0 m/s wind) maps to 128.
    
    Args:
        value: Wind component array in m/s
        vmin: Minimum wind value (default -50 m/s)
        vmax: Maximum wind value (default +50 m/s)
    
    Returns:
        Encoded array as uint8 (0-255)
    """
    # Clip to valid range
    clipped = np.clip(value, vmin, vmax)
    
    # Normalize to 0-1
    normalized = (clipped - vmin) / (vmax - vmin)
    
    # Scale to 0-255
    encoded = (normalized * 255).astype(np.uint8)
    
    return encoded


def decode_wind_component(encoded: np.ndarray, vmin: float = WIND_MIN, vmax: float = WIND_MAX) -> np.ndarray:
    """
    Decode wind component from 0-255 back to m/s.
    
    Args:
        encoded: Encoded array (0-255)
        vmin: Minimum wind value
        vmax: Maximum wind value
    
    Returns:
        Wind component in m/s
    """
    normalized = encoded.astype(np.float32) / 255.0
    return normalized * (vmax - vmin) + vmin


def download_wind_data(
    date: datetime,
    cycle: int,
    fxx: int,
    logger: logging.Logger
) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], Optional[dict]]:
    """
    Download U and V wind components from HRRR using Herbie.
    
    Args:
        date: Date to download
        cycle: Model cycle (0-23)
        fxx: Forecast hour
        logger: Logger instance
    
    Returns:
        Tuple of (u_data, v_data, metadata) or (None, None, None) on failure
    """
    try:
        logger.info(f"Downloading HRRR wind data: {date.strftime('%Y-%m-%d')} {cycle:02d}Z F{fxx:02d}")
        
        # Create Herbie object
        H = Herbie(
            date.strftime('%Y-%m-%d'),
            model='hrrr',
            product='sfc',
            fxx=fxx,
            cycle=cycle
        )
        
        # Download U component (10m)
        logger.debug("Fetching U-wind (UGRD:10 m)...")
        ds_u = H.xarray("UGRD:10 m")
        u_data = ds_u['u10'].values.squeeze()
        
        # Download V component (10m)
        logger.debug("Fetching V-wind (VGRD:10 m)...")
        ds_v = H.xarray("VGRD:10 m")
        v_data = ds_v['v10'].values.squeeze()
        
        # Get coordinate info for metadata
        lats = ds_u['latitude'].values
        lons = ds_u['longitude'].values
        
        # HRRR uses longitude 0-360, convert to -180 to 180
        lons = np.where(lons > 180, lons - 360, lons)
        
        metadata = {
            'date': date.strftime('%Y-%m-%d'),
            'cycle': f"{cycle:02d}Z",
            'forecast_hour': f"F{fxx:02d}",
            'valid_time': (date.replace(hour=cycle) + timedelta(hours=fxx)).isoformat(),
            'bounds': {
                'west': float(lons.min()),
                'east': float(lons.max()),
                'south': float(lats.min()),
                'north': float(lats.max()),
            },
            'shape': list(u_data.shape),
            'wind_encoding': {
                'min': WIND_MIN,
                'max': WIND_MAX,
                'unit': 'm/s',
                'description': 'R=U component, G=V component, B=magnitude, encoded 0-255'
            }
        }
        
        logger.info(f"Downloaded wind data: shape={u_data.shape}, bounds=[{metadata['bounds']['west']:.1f}, {metadata['bounds']['south']:.1f}, {metadata['bounds']['east']:.1f}, {metadata['bounds']['north']:.1f}]")
        
        return u_data, v_data, metadata
        
    except Exception as e:
        logger.error(f"Failed to download wind data: {e}")
        return None, None, None


def create_wind_image(u_data: np.ndarray, v_data: np.ndarray) -> Image.Image:
    """
    Create RGBA image from U/V wind components.
    
    R = U component (encoded)
    G = V component (encoded)
    B = Wind magnitude (encoded, 0-255 where 255 = max_speed)
    A = 255 (fully opaque where we have data)
    
    Args:
        u_data: U wind component array
        v_data: V wind component array
    
    Returns:
        PIL Image in RGBA format
    """
    # Encode U and V
    r_channel = encode_wind_component(u_data)
    g_channel = encode_wind_component(v_data)
    
    # Calculate magnitude for B channel
    magnitude = np.sqrt(u_data**2 + v_data**2)
    max_speed = math.sqrt(WIND_MAX**2 + WIND_MAX**2)  # ~70 m/s
    b_channel = np.clip((magnitude / max_speed) * 255, 0, 255).astype(np.uint8)
    
    # Alpha channel (255 where we have valid data)
    a_channel = np.where(np.isnan(u_data) | np.isnan(v_data), 0, 255).astype(np.uint8)
    
    # Handle NaN values in R, G, B
    r_channel = np.nan_to_num(r_channel, nan=128).astype(np.uint8)
    g_channel = np.nan_to_num(g_channel, nan=128).astype(np.uint8)
    b_channel = np.nan_to_num(b_channel, nan=0).astype(np.uint8)
    
    # Stack into RGBA
    rgba = np.stack([r_channel, g_channel, b_channel, a_channel], axis=-1)
    
    return Image.fromarray(rgba, mode='RGBA')


def latlon_to_tile(lat: float, lon: float, zoom: int) -> Tuple[int, int]:
    """Convert lat/lon to tile coordinates at given zoom level."""
    n = 2 ** zoom
    x = int((lon + 180) / 360 * n)
    lat_rad = math.radians(lat)
    y = int((1 - math.asinh(math.tan(lat_rad)) / math.pi) / 2 * n)
    return x, y


def tile_bounds(x: int, y: int, zoom: int) -> Tuple[float, float, float, float]:
    """Get lat/lon bounds for a tile. Returns (west, south, east, north)."""
    n = 2 ** zoom
    west = x / n * 360 - 180
    east = (x + 1) / n * 360 - 180
    
    north_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    south_rad = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
    
    north = math.degrees(north_rad)
    south = math.degrees(south_rad)
    
    return west, south, east, north


def generate_tiles(
    u_data: np.ndarray,
    v_data: np.ndarray,
    metadata: dict,
    output_dir: Path,
    min_zoom: int = MIN_ZOOM,
    max_zoom: int = MAX_ZOOM,
    logger: logging.Logger = None
) -> int:
    """
    Generate XYZ tiles from wind data.
    
    Args:
        u_data: U wind component
        v_data: V wind component
        metadata: Data metadata with bounds
        output_dir: Directory to save tiles
        min_zoom: Minimum zoom level
        max_zoom: Maximum zoom level
        logger: Logger instance
    
    Returns:
        Number of tiles generated
    """
    bounds = metadata['bounds']
    data_west, data_east = bounds['west'], bounds['east']
    data_south, data_north = bounds['south'], bounds['north']
    
    # Create full wind image
    wind_img = create_wind_image(u_data, v_data)
    img_height, img_width = u_data.shape
    
    tile_count = 0
    
    for zoom in range(min_zoom, max_zoom + 1):
        # Get tile range for this zoom level
        min_tile_x, max_tile_y = latlon_to_tile(data_north, data_west, zoom)
        max_tile_x, min_tile_y = latlon_to_tile(data_south, data_east, zoom)
        
        # Ensure valid ranges
        n = 2 ** zoom
        min_tile_x = max(0, min_tile_x)
        max_tile_x = min(n - 1, max_tile_x)
        min_tile_y = max(0, min_tile_y)
        max_tile_y = min(n - 1, max_tile_y)
        
        if logger:
            logger.debug(f"Zoom {zoom}: tiles x=[{min_tile_x}, {max_tile_x}], y=[{min_tile_y}, {max_tile_y}]")
        
        for tile_x in range(min_tile_x, max_tile_x + 1):
            for tile_y in range(min_tile_y, max_tile_y + 1):
                # Get tile bounds
                t_west, t_south, t_east, t_north = tile_bounds(tile_x, tile_y, zoom)
                
                # Calculate pixel coordinates in source image
                # Map tile bounds to image pixel coordinates
                px_left = int((t_west - data_west) / (data_east - data_west) * img_width)
                px_right = int((t_east - data_west) / (data_east - data_west) * img_width)
                px_top = int((data_north - t_north) / (data_north - data_south) * img_height)
                px_bottom = int((data_north - t_south) / (data_north - data_south) * img_height)
                
                # Clip to image bounds
                px_left = max(0, px_left)
                px_right = min(img_width, px_right)
                px_top = max(0, px_top)
                px_bottom = min(img_height, px_bottom)
                
                # Skip if outside image
                if px_left >= px_right or px_top >= px_bottom:
                    continue
                
                # Extract region from wind image
                region = wind_img.crop((px_left, px_top, px_right, px_bottom))
                
                # Resize to tile size
                tile_img = region.resize((TILE_SIZE, TILE_SIZE), Image.BILINEAR)
                
                # Save tile
                tile_dir = output_dir / str(zoom) / str(tile_x)
                tile_dir.mkdir(parents=True, exist_ok=True)
                tile_path = tile_dir / f"{tile_y}.png"
                tile_img.save(tile_path, 'PNG', optimize=True)
                
                tile_count += 1
        
        if logger:
            logger.info(f"Generated {tile_count} tiles at zoom {zoom}")
    
    return tile_count


def upload_to_s3(
    local_dir: Path,
    bucket: str,
    prefix: str,
    timestamp: str,
    forecast_hour: str,
    logger: logging.Logger,
    dry_run: bool = False
) -> int:
    """
    Upload tiles to S3.
    
    Args:
        local_dir: Local directory containing tiles
        bucket: S3 bucket name
        prefix: S3 prefix (e.g., 'wind-particles')
        timestamp: Timestamp string (e.g., '20260224T12z')
        forecast_hour: Forecast hour (e.g., '00')
        logger: Logger instance
        dry_run: If True, don't actually upload
    
    Returns:
        Number of files uploaded
    """
    if dry_run:
        logger.info("[DRY-RUN] Would upload tiles to S3")
        return 0
    
    s3 = boto3.client('s3')
    upload_count = 0
    
    for tile_path in local_dir.rglob('*.png'):
        # Build S3 key: wind-particles/{timestamp}/{forecast}/{z}/{x}/{y}.png
        rel_path = tile_path.relative_to(local_dir)
        s3_key = f"{prefix}/{timestamp}/{forecast_hour}/{rel_path}"
        
        try:
            s3.upload_file(
                str(tile_path),
                bucket,
                s3_key,
                ExtraArgs={
                    'ContentType': 'image/png',
                    'CacheControl': 'public, max-age=3600'
                }
            )
            upload_count += 1
        except ClientError as e:
            logger.error(f"Failed to upload {s3_key}: {e}")
    
    logger.info(f"Uploaded {upload_count} tiles to s3://{bucket}/{prefix}/{timestamp}/{forecast_hour}/")
    return upload_count


def upload_metadata(
    metadata: dict,
    bucket: str,
    prefix: str,
    logger: logging.Logger,
    dry_run: bool = False
):
    """Upload wind metadata JSON to S3."""
    if dry_run:
        logger.info("[DRY-RUN] Would upload metadata")
        return
    
    s3 = boto3.client('s3')
    
    # Add tile URL template
    metadata['tiles'] = {
        'url_template': f"https://{bucket}.s3.amazonaws.com/{prefix}/{{timestamp}}/{{forecast}}/{{z}}/{{x}}/{{y}}.png",
        'tile_size': TILE_SIZE,
        'min_zoom': MIN_ZOOM,
        'max_zoom': MAX_ZOOM,
    }
    
    try:
        s3.put_object(
            Bucket=bucket,
            Key=f"{prefix}/metadata.json",
            Body=json.dumps(metadata, indent=2),
            ContentType='application/json',
            CacheControl='no-cache'
        )
        logger.info(f"Uploaded metadata to s3://{bucket}/{prefix}/metadata.json")
    except ClientError as e:
        logger.error(f"Failed to upload metadata: {e}")


def main():
    parser = argparse.ArgumentParser(description='Generate wind UV tiles for custom particle rendering')
    
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument('--date', type=str, help='Date (YYYY-MM-DD)')
    date_group.add_argument('--latest', action='store_true', help='Use latest available data')
    
    parser.add_argument('--cycle', type=int, help='Model cycle (0-23)')
    parser.add_argument('--fxx', type=str, default='0', help='Forecast hours (e.g., "0-12" or "0,3,6")')
    parser.add_argument('--bucket', type=str, default=DEFAULT_S3_BUCKET, help='S3 bucket')
    parser.add_argument('--dry-run', action='store_true', help='Skip S3 upload')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--keep-temp', action='store_true', help='Keep temporary files')
    
    args = parser.parse_args()
    logger = setup_logging(args.verbose)
    
    # Determine date and cycle
    if args.latest:
        # Use ~3 hours ago to ensure data is available
        now = datetime.utcnow()
        target_time = now - timedelta(hours=3)
        date = target_time.replace(hour=0, minute=0, second=0, microsecond=0)
        cycle = target_time.hour
    else:
        date = datetime.strptime(args.date, '%Y-%m-%d')
        cycle = args.cycle if args.cycle is not None else 12
    
    # Parse forecast hours
    if '-' in args.fxx:
        start, end = map(int, args.fxx.split('-'))
        forecast_hours = list(range(start, end + 1))
    elif ',' in args.fxx:
        forecast_hours = [int(x.strip()) for x in args.fxx.split(',')]
    else:
        forecast_hours = [int(args.fxx)]
    
    logger.info(f"Wind Tile Generator starting")
    logger.info(f"  Date: {date.strftime('%Y-%m-%d')}")
    logger.info(f"  Cycle: {cycle:02d}Z")
    logger.info(f"  Forecast hours: {forecast_hours}")
    logger.info(f"  S3 Bucket: {args.bucket}")
    
    # Create temp directory
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    
    timestamp = f"{date.strftime('%Y%m%d')}T{cycle:02d}z"
    total_tiles = 0
    latest_metadata = None
    
    for fxx in forecast_hours:
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing forecast hour F{fxx:02d}")
        logger.info(f"{'='*50}")
        
        # Download wind data
        u_data, v_data, metadata = download_wind_data(date, cycle, fxx, logger)
        
        if u_data is None:
            logger.error(f"Skipping F{fxx:02d} due to download failure")
            continue
        
        latest_metadata = metadata
        
        # Generate tiles
        output_dir = TEMP_DIR / timestamp / f"{fxx:02d}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        tile_count = generate_tiles(
            u_data, v_data, metadata,
            output_dir,
            logger=logger
        )
        total_tiles += tile_count
        
        logger.info(f"Generated {tile_count} tiles for F{fxx:02d}")
        
        # Upload to S3
        if not args.dry_run:
            upload_to_s3(
                output_dir,
                args.bucket,
                WIND_TILES_PREFIX,
                timestamp,
                f"{fxx:02d}",
                logger,
                dry_run=args.dry_run
            )
    
    # Upload metadata
    if latest_metadata and not args.dry_run:
        latest_metadata['forecast_hours'] = [f"{fxx:02d}" for fxx in forecast_hours]
        latest_metadata['timestamp'] = timestamp
        upload_metadata(latest_metadata, args.bucket, WIND_TILES_PREFIX, logger, args.dry_run)
    
    # Cleanup
    if not args.keep_temp:
        import shutil
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
        logger.debug("Cleaned up temporary files")
    
    logger.info(f"\n{'='*50}")
    logger.info(f"Wind tile generation complete!")
    logger.info(f"Total tiles generated: {total_tiles}")
    logger.info(f"{'='*50}")


if __name__ == '__main__':
    main()
