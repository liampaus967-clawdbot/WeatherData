#!/usr/bin/env python3
"""
Extract wind tiles from existing GRIB2 files.

Reads U/V wind components from GRIB2 files already downloaded by the main pipeline,
encodes them into PNG tiles (R=U, G=V, B=magnitude), and uploads to S3.

Usage:
    python extract_wind_from_grib.py --input /data/downloads --output /data/wind-tiles --s3-bucket driftwise-weather-data
"""

import argparse
import logging
import os
import sys
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, List
import re

import numpy as np
from PIL import Image

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
TILE_SIZE = 256


def setup_logging(verbose: bool = False) -> logging.Logger:
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger('wind_extract')


def encode_wind_component(value: np.ndarray) -> np.ndarray:
    """Encode wind component to 0-255. 0 m/s maps to 128."""
    clipped = np.clip(value, WIND_MIN, WIND_MAX)
    normalized = (clipped - WIND_MIN) / (WIND_MAX - WIND_MIN)
    return (normalized * 255).astype(np.uint8)


def extract_wind_from_grib(grib_path: Path, logger: logging.Logger) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], Optional[dict]]:
    """
    Extract U and V wind components from a GRIB2 file.
    
    Returns: (u_data, v_data, metadata) or (None, None, None) on failure
    """
    logger.info(f"Extracting wind from: {grib_path.name}")
    
    u_data = None
    v_data = None
    lats = None
    lons = None
    
    # Try pygrib first (faster)
    if pygrib is not None:
        try:
            grbs = pygrib.open(str(grib_path))
            
            # Find U and V wind at 10m
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
        # Try multiple filter strategies
        filter_attempts = [
            {'typeOfLevel': 'heightAboveGround', 'level': 10},
            {'shortName': ['10u', '10v']},
            {'cfVarName': ['u10', 'v10']},
            {},  # No filter - load all and search
        ]
        
        for filter_keys in filter_attempts:
            try:
                logger.debug(f"Trying cfgrib with filter: {filter_keys}")
                if filter_keys:
                    ds = xr.open_dataset(
                        grib_path,
                        engine='cfgrib',
                        filter_by_keys=filter_keys
                    )
                else:
                    # No filter - use backend_kwargs to handle multiple messages
                    ds = xr.open_dataset(
                        grib_path,
                        engine='cfgrib',
                        backend_kwargs={'indexpath': ''}
                    )
                
                # Look for U component
                for var_name in ['u10', 'u', '10u', 'UGRD_10maboveground']:
                    if var_name in ds:
                        u_data = ds[var_name].values.squeeze()
                        logger.debug(f"Found U in variable: {var_name}")
                        break
                
                # Look for V component
                for var_name in ['v10', 'v', '10v', 'VGRD_10maboveground']:
                    if var_name in ds:
                        v_data = ds[var_name].values.squeeze()
                        logger.debug(f"Found V in variable: {var_name}")
                        break
                
                if 'latitude' in ds:
                    lats = ds['latitude'].values
                    lons = ds['longitude'].values
                
                ds.close()
                
                if u_data is not None and v_data is not None:
                    logger.debug(f"Successfully extracted with filter: {filter_keys}")
                    break
                    
            except Exception as e:
                logger.debug(f"Filter {filter_keys} failed: {e}")
                continue
        
        if u_data is None or v_data is None:
            logger.error(f"All cfgrib filter attempts failed for {grib_path.name}")
            return None, None, None
    
    if u_data is None or v_data is None:
        logger.warning(f"Could not find U/V wind components in {grib_path.name}")
        return None, None, None
    
    # Convert longitude from 0-360 to -180-180 if needed
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
    
    logger.info(f"Extracted wind: shape={u_data.shape}, U range=[{u_data.min():.1f}, {u_data.max():.1f}], V range=[{v_data.min():.1f}, {v_data.max():.1f}]")
    
    return u_data, v_data, metadata


def create_wind_image(u_data: np.ndarray, v_data: np.ndarray) -> Image.Image:
    """Create RGBA image: R=U, G=V, B=magnitude, A=255"""
    r_channel = encode_wind_component(u_data)
    g_channel = encode_wind_component(v_data)
    
    # Magnitude for B channel (0-255 scaled to max possible ~70 m/s)
    magnitude = np.sqrt(u_data**2 + v_data**2)
    max_speed = math.sqrt(WIND_MAX**2 + WIND_MAX**2)
    b_channel = np.clip((magnitude / max_speed) * 255, 0, 255).astype(np.uint8)
    
    # Alpha = 255 where valid
    a_channel = np.where(np.isnan(u_data) | np.isnan(v_data), 0, 255).astype(np.uint8)
    
    # Handle NaN
    r_channel = np.nan_to_num(r_channel, nan=128).astype(np.uint8)
    g_channel = np.nan_to_num(g_channel, nan=128).astype(np.uint8)
    b_channel = np.nan_to_num(b_channel, nan=0).astype(np.uint8)
    
    rgba = np.stack([r_channel, g_channel, b_channel, a_channel], axis=-1)
    return Image.fromarray(rgba, mode='RGBA')


def parse_grib_filename(filename: str) -> Optional[dict]:
    """Extract date, cycle, fxx from GRIB/NetCDF filename like hrrr.20260224.t14z.wrfsfcf00.grib2 or hrrr.20260224.t15z.f00.nc"""
    patterns = [
        r'hrrr\.(\d{8})\.t(\d{2})z\.wrfsfcf(\d{2})\.grib2',
        r'hrrr\.(\d{8})\.t(\d{2})z\.f(\d{2})\.grib2',
        r'hrrr\.(\d{8})\.t(\d{2})z\.f(\d{2})\.nc',  # NetCDF format
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


def extract_wind_from_netcdf(nc_path: Path, logger: logging.Logger) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], Optional[dict]]:
    """
    Extract U and V wind components from a NetCDF file.
    
    Returns: (u_data, v_data, metadata) or (None, None, None) on failure
    """
    logger.info(f"Extracting wind from NetCDF: {nc_path.name}")
    
    try:
        ds = xr.open_dataset(nc_path)
        
        u_data = None
        v_data = None
        
        # Try common variable names for U/V wind
        u_names = ['u10', 'u', 'UGRD_10maboveground', 'ugrd']
        v_names = ['v10', 'v', 'VGRD_10maboveground', 'vgrd']
        
        for name in u_names:
            if name in ds:
                u_data = ds[name].values.squeeze()
                break
        
        for name in v_names:
            if name in ds:
                v_data = ds[name].values.squeeze()
                break
        
        if u_data is None or v_data is None:
            logger.warning(f"Could not find U/V wind in {nc_path.name}. Variables: {list(ds.data_vars)}")
            ds.close()
            return None, None, None
        
        metadata = {
            'source_file': nc_path.name,
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
        
        ds.close()
        logger.info(f"Extracted wind: shape={u_data.shape}, U range=[{np.nanmin(u_data):.1f}, {np.nanmax(u_data):.1f}], V range=[{np.nanmin(v_data):.1f}, {np.nanmax(v_data):.1f}]")
        
        return u_data, v_data, metadata
        
    except Exception as e:
        logger.error(f"Failed to read NetCDF: {e}")
        return None, None, None


def process_grib_files(
    input_dir: Path,
    output_dir: Path,
    s3_bucket: Optional[str],
    logger: logging.Logger
) -> int:
    """Process all GRIB2/NetCDF files in input directory."""
    
    grib_files = sorted(input_dir.glob("*.grib2"))
    nc_files = sorted(input_dir.glob("*.nc"))
    all_files = grib_files + nc_files
    
    if not all_files:
        logger.warning(f"No GRIB2 or NetCDF files found in {input_dir}")
        return 0
    
    logger.info(f"Found {len(all_files)} files to process ({len(grib_files)} GRIB2, {len(nc_files)} NetCDF)")
    
    processed = 0
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for file_path in all_files:
        file_info = parse_grib_filename(file_path.name)
        if not file_info:
            logger.warning(f"Could not parse filename: {file_path.name}")
            continue
        
        # Extract wind data based on file type
        try:
            if file_path.suffix == '.nc':
                u_data, v_data, metadata = extract_wind_from_netcdf(file_path, logger)
            else:
                u_data, v_data, metadata = extract_wind_from_grib(file_path, logger)
            
            if u_data is None:
                logger.warning(f"Skipping {file_path.name} - no wind data extracted")
                continue
        except Exception as e:
            logger.error(f"Exception processing {file_path.name}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            continue
        
        # Create wind image
        wind_img = create_wind_image(u_data, v_data)
        
        # Output paths
        date_str = file_info['date']
        cycle = file_info['cycle']
        fxx = file_info['fxx']
        
        png_name = f"wind_{date_str}_t{cycle}z_f{fxx}.png"
        json_name = f"wind_{date_str}_t{cycle}z_f{fxx}.json"
        
        png_path = output_dir / png_name
        json_path = output_dir / json_name
        
        # Save locally
        wind_img.save(png_path, 'PNG')
        with open(json_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Created: {png_name} ({wind_img.size[0]}x{wind_img.size[1]})")
        
        # Upload to S3
        if s3_bucket:
            s3_prefix = f"wind-tiles/{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}/{cycle}Z"
            upload_to_s3(png_path, s3_bucket, f"{s3_prefix}/{png_name}", logger)
            upload_to_s3(json_path, s3_bucket, f"{s3_prefix}/{json_name}", logger)
        
        processed += 1
    
    return processed


def main():
    parser = argparse.ArgumentParser(description='Extract wind tiles from GRIB2 files')
    parser.add_argument('--input', '-i', required=True, help='Input directory with GRIB2 files')
    parser.add_argument('--output', '-o', required=True, help='Output directory for wind tiles')
    parser.add_argument('--s3-bucket', help='S3 bucket for upload (optional)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    logger = setup_logging(args.verbose)
    
    input_dir = Path(args.input)
    output_dir = Path(args.output)
    
    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        sys.exit(1)
    
    logger.info("=" * 50)
    logger.info("Wind Tile Extraction Starting")
    logger.info("=" * 50)
    logger.info(f"Input: {input_dir}")
    logger.info(f"Output: {output_dir}")
    if args.s3_bucket:
        logger.info(f"S3 Bucket: {args.s3_bucket}")
    
    processed = process_grib_files(input_dir, output_dir, args.s3_bucket, logger)
    
    logger.info("=" * 50)
    logger.info(f"Processed {processed} wind tiles")
    logger.info("=" * 50)
    
    sys.exit(0 if processed > 0 else 1)


if __name__ == '__main__':
    main()
