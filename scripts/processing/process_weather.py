#!/usr/bin/env python3
"""
HRRR Weather Data Processing Script

Processes GRIB2 files into Cloud Optimized GeoTIFFs (COGs):
- Extracts variables from GRIB2 using GDAL
- Applies unit conversions
- Reprojects to EPSG:3857 (Web Mercator)
- Creates COGs with compression and overviews

Part of TICKET-006: Data Processing with GDAL/rioxarray
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json
from datetime import datetime

import numpy as np
from osgeo import gdal, osr
import rioxarray as rxr
import xarray as xr
from rasterio.enums import Resampling

# Add config directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.config_manager import VariableConfig


# Configure GDAL
gdal.UseExceptions()
gdal.SetConfigOption('GDAL_NUM_THREADS', 'ALL_CPUS')
gdal.SetConfigOption('GDAL_CACHEMAX', '512')


def setup_logging(verbose: bool = False) -> logging.Logger:
    """
    Configure logging.

    Args:
        verbose: Enable debug logging

    Returns:
        Logger instance
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger('process_weather')


def list_grib_bands(grib_file: Path) -> List[Tuple[int, str, str]]:
    """
    List all bands in a GRIB2 file.

    Args:
        grib_file: Path to GRIB2 file

    Returns:
        List of (band_number, description, metadata) tuples
    """
    ds = gdal.Open(str(grib_file))
    if not ds:
        raise ValueError(f"Cannot open GRIB2 file: {grib_file}")

    bands = []
    for i in range(1, ds.RasterCount + 1):
        band = ds.GetRasterBand(i)
        desc = band.GetDescription()
        metadata = band.GetMetadata()

        # Get GRIB element and level
        grib_element = metadata.get('GRIB_ELEMENT', 'Unknown')
        grib_short_name = metadata.get('GRIB_SHORT_NAME', 'Unknown')
        grib_level = metadata.get('GRIB_SHORT_NAME', '')

        bands.append((i, desc, grib_element, grib_short_name, metadata))

    ds = None
    return bands


def find_band_by_search_string(grib_file: Path, search_string: str, logger: logging.Logger) -> Optional[int]:
    """
    Find band number matching a GRIB search string.

    Args:
        grib_file: Path to GRIB2 file
        search_string: Search pattern (e.g., "TMP:2 m", "UGRD:10 m", "REFC:entire atmosphere")
        logger: Logger instance

    Returns:
        Band number (1-indexed) or None if not found
    """
    # Parse search string (e.g., "TMP:2 m" -> element="TMP", level="2 m")
    if ':' in search_string:
        element, level = search_string.split(':', 1)
        element = element.strip()
        level = level.strip()
    else:
        element = search_string.strip()
        level = None

    logger.debug(f"Searching for element='{element}', level='{level}'")

    bands = list_grib_bands(grib_file)

    for band_num, desc, grib_element, grib_short_name, metadata in bands:
        # Check if element matches
        logger.debug(f"Band {band_num}: element='{grib_element}' vs search='{element}'")
        if element.upper() != grib_element.upper():
            continue
        logger.debug(f"Band {band_num}: Element match! Checking level...")

        # If no level specified, first match is good
        if not level:
            logger.debug(f"Found match: Band {band_num} - {desc}")
            return band_num

        # Level specified - need to match it
        # For HRRR: "2 m" should match "2-HTGL" or "2[m]" in description
        # For HRRR: "10 m" should match "10-HTGL" or "10[m]" in description
        # For HRRR: "surface" should match "0-SFC" or "SFC" in description
        # For HRRR: "entire atmosphere" should match "0-EATM" or "EATM" in description

        # Normalize level string for matching
        level_lower = level.lower()
        desc_lower = desc.lower()
        short_name_lower = grib_short_name.lower()
        logger.debug(f"Level matching: level_lower='{level_lower}', desc='{desc_lower}', short_name='{short_name_lower}'")

        # Handle different level formats
        # Check entire atmosphere first (before checking for 'm' which matches "atmosphere")
        if 'entire atmosphere' in level_lower or 'eatm' in level_lower:
            # Match entire atmosphere
            logger.debug(f"Checking EATM: short_name='{short_name_lower}', desc='{desc_lower}'")
            if 'eatm' in short_name_lower or 'entire atmosphere' in desc_lower:
                logger.debug(f"Found match: Band {band_num} - {desc}")
                return band_num

        elif 'surface' in level_lower or 'sfc' in level_lower:
            # Match surface level
            if 'sfc' in short_name_lower or 'surface' in desc_lower:
                logger.debug(f"Found match: Band {band_num} - {desc}")
                return band_num

        elif ' m' in level_lower or 'meter' in level_lower:
            # Extract number from level (e.g., "2 m" -> "2", "10 m" -> "10")
            # Note: checking for ' m' with space to avoid matching 'atmosphere'
            import re
            level_num = re.search(r'(\d+)\s*m', level_lower)
            if level_num:
                level_num = level_num.group(1)
                # Check if this number appears in GRIB_SHORT_NAME or description
                if f"{level_num}-htgl" in short_name_lower or f"{level_num}[m]" in desc_lower:
                    logger.debug(f"Found match: Band {band_num} - {desc}")
                    return band_num

        # Fallback: check if level string appears in description
        else:
            logger.debug(f"Fallback check: '{level_lower}' in '{desc_lower}'")
            if level_lower in desc_lower:
                logger.debug(f"Found match: Band {band_num} - {desc}")
                return band_num

    logger.warning(f"No band found matching '{search_string}'")
    return None


def extract_variable_from_grib(
    grib_file: Path,
    search_string: str,
    logger: logging.Logger
) -> Optional[xr.DataArray]:
    """
    Extract a single variable from GRIB2 file using GDAL.

    Args:
        grib_file: Path to GRIB2 file
        search_string: GRIB search pattern
        logger: Logger instance

    Returns:
        xarray DataArray with the variable data, or None if not found
    """
    logger.info(f"Extracting '{search_string}' from {grib_file.name}")

    # Find band number
    band_num = find_band_by_search_string(grib_file, search_string, logger)
    if band_num is None:
        logger.error(f"Variable '{search_string}' not found in GRIB2 file")
        return None

    # Open GRIB2 file with GDAL
    ds = gdal.Open(str(grib_file))
    if not ds:
        logger.error(f"Cannot open GRIB2 file: {grib_file}")
        return None

    # Get specific band
    band = ds.GetRasterBand(band_num)

    # Read data as numpy array
    data = band.ReadAsArray()

    # Get geotransform and projection
    geotransform = ds.GetGeoTransform()
    projection = ds.GetProjection()

    # Get metadata
    metadata = band.GetMetadata()

    # Create coordinates
    width = ds.RasterXSize
    height = ds.RasterYSize

    # Calculate coordinates from geotransform
    # geotransform = (x_min, pixel_width, 0, y_max, 0, -pixel_height)
    x_coords = geotransform[0] + np.arange(width) * geotransform[1]
    y_coords = geotransform[3] + np.arange(height) * geotransform[5]

    # Create xarray DataArray
    data_array = xr.DataArray(
        data,
        dims=['y', 'x'],
        coords={
            'x': x_coords,
            'y': y_coords
        },
        attrs=metadata
    )

    # Set CRS using rioxarray
    data_array = data_array.rio.write_crs(projection)

    # Add nodata value if present
    nodata = band.GetNoDataValue()
    if nodata is not None:
        data_array = data_array.rio.write_nodata(nodata)

    # Close dataset
    ds = None

    logger.info(f"Extracted data: shape={data.shape}, dtype={data.dtype}")

    return data_array


def apply_unit_conversion(
    data_array: xr.DataArray,
    conversion_name: Optional[str],
    config: VariableConfig,
    logger: logging.Logger
) -> xr.DataArray:
    """
    Apply unit conversion to data.

    Args:
        data_array: Input data
        conversion_name: Name of conversion formula
        config: Variable configuration
        logger: Logger instance

    Returns:
        Converted data array
    """
    if not conversion_name:
        logger.debug("No unit conversion needed")
        return data_array

    # Check GRIB metadata for actual units
    grib_unit = data_array.attrs.get('GRIB_UNIT', '')
    if grib_unit:
        logger.debug(f"GRIB source units: {grib_unit}")

        # Skip conversion if already in correct units
        # Common cases: GRIB may already have converted to Celsius
        if conversion_name == 'kelvin_to_celsius' and '[C]' in grib_unit:
            logger.info("Data already in Celsius, skipping K→C conversion")
            return data_array
        if conversion_name == 'kelvin_to_fahrenheit' and '[F]' in grib_unit:
            logger.info("Data already in Fahrenheit, skipping K→F conversion")
            return data_array

    logger.info(f"Applying unit conversion: {conversion_name}")

    # Get nodata value before conversion
    nodata = data_array.rio.nodata

    # Create mask for valid data
    if nodata is not None:
        valid_mask = data_array.values != nodata
    else:
        valid_mask = ~np.isnan(data_array.values)

    # Apply conversion only to valid data
    converted_data = data_array.copy()
    if valid_mask.any():
        valid_values = data_array.values[valid_mask]
        converted_values = np.array([
            config.apply_conversion(float(val), conversion_name)
            for val in valid_values
        ])
        converted_data.values[valid_mask] = converted_values

    logger.debug(f"Converted: min={converted_data.values[valid_mask].min():.2f}, "
                f"max={converted_data.values[valid_mask].max():.2f}")

    return converted_data


def reproject_to_web_mercator(
    data_array: xr.DataArray,
    resampling_method: str,
    logger: logging.Logger,
    target_resolution_meters: Optional[float] = None
) -> xr.DataArray:
    """
    Reproject data to EPSG:3857 (Web Mercator).

    Args:
        data_array: Input data
        resampling_method: GDAL resampling method
        logger: Logger instance
        target_resolution_meters: Optional output resolution in meters per pixel
            (EPSG:3857). Use to upsample coarse data for smoother display; omit
            to keep native resolution.

    Returns:
        Reprojected data array
    """
    logger.info("Reprojecting to EPSG:3857 (Web Mercator)")
    if target_resolution_meters is not None:
        logger.info(f"Target resolution: {target_resolution_meters} m/pixel (upsampling)")

    # Get current CRS
    current_crs = data_array.rio.crs
    logger.debug(f"Current CRS: {current_crs}")

    resampling_enum = getattr(Resampling, resampling_method.lower(), Resampling.bilinear)
    try:
        # Try rioxarray reprojection first
        if target_resolution_meters is not None:
            # rioxarray: resolution in destination CRS units (meters for 3857)
            # y resolution negative (pixel height) for north-up
            reprojected = data_array.rio.reproject(
                "EPSG:3857",
                resampling=resampling_enum,
                resolution=(target_resolution_meters, -target_resolution_meters)
            )
        else:
            reprojected = data_array.rio.reproject(
                "EPSG:3857",
                resampling=resampling_enum
            )
    except Exception as e:
        logger.warning(f"rioxarray reprojection failed: {e}")
        logger.info("Falling back to GDAL reprojection via temporary file")

        # Write to temp file and use gdalwarp
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp_src:
            src_path = tmp_src.name
        with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp_dst:
            dst_path = tmp_dst.name

        try:
            # Write source data
            data_array.rio.to_raster(src_path)

            # Reproject with gdalwarp
            warp_options_dict = {
                "format": "GTiff",
                "dstSRS": "EPSG:3857",
                "resampleAlg": resampling_method,
                "multithread": True,
            }
            if target_resolution_meters is not None:
                warp_options_dict["xRes"] = target_resolution_meters
                warp_options_dict["yRes"] = target_resolution_meters
            warp_options = gdal.WarpOptions(**warp_options_dict)

            gdal.Warp(dst_path, src_path, options=warp_options)

            # Read back reprojected data
            reprojected = rxr.open_rasterio(dst_path, masked=True).squeeze()

            # Clean up temp files
            Path(src_path).unlink()
            Path(dst_path).unlink()

        except Exception as e2:
            logger.error(f"GDAL reprojection also failed: {e2}")
            # Clean up
            if Path(src_path).exists():
                Path(src_path).unlink()
            if Path(dst_path).exists():
                Path(dst_path).unlink()
            raise

    logger.info(f"Reprojected: shape={reprojected.shape}")

    return reprojected


def create_cog(
    data_array: xr.DataArray,
    output_path: Path,
    compression: str,
    tile_size: int,
    overview_levels: List[int],
    logger: logging.Logger
) -> bool:
    """
    Create Cloud Optimized GeoTIFF.

    Args:
        data_array: Input data
        output_path: Output file path
        compression: Compression method
        tile_size: Tile size for COG
        overview_levels: Overview pyramid levels
        logger: Logger instance

    Returns:
        True if successful
    """
    logger.info(f"Creating COG: {output_path.name}")

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write to temporary file first
    temp_path = output_path.with_suffix('.tmp.tif')

    try:
        # Write GeoTIFF with rioxarray
        data_array.rio.to_raster(
            str(temp_path),
            driver='GTiff',
            compress=compression,
            tiled=True,
            blockxsize=tile_size,
            blockysize=tile_size,
            BIGTIFF='IF_SAFER'
        )

        # Add overviews and convert to COG using GDAL
        logger.debug("Adding overviews and converting to COG")

        # Open the temp file
        ds = gdal.Open(str(temp_path), gdal.GA_Update)

        # Build overviews
        ds.BuildOverviews(
            'AVERAGE',
            overview_levels
        )

        ds = None  # Close file

        # Convert to COG using gdal_translate
        translate_options = gdal.TranslateOptions(
            format='COG',
            creationOptions=[
                f'COMPRESS={compression}',
                f'BLOCKSIZE={tile_size}',
                'TILED=YES',
                'BIGTIFF=IF_SAFER'
            ]
        )

        gdal.Translate(
            str(output_path),
            str(temp_path),
            options=translate_options
        )

        # Remove temp file
        temp_path.unlink()

        logger.info(f"Created COG: {output_path}")
        logger.debug(f"Size: {output_path.stat().st_size / 1024 / 1024:.2f} MB")

        return True

    except Exception as e:
        logger.error(f"Failed to create COG: {e}")
        if temp_path.exists():
            temp_path.unlink()
        return False


def process_variable(
    grib_file: Path,
    variable_name: str,
    variable_config: Dict,
    config: VariableConfig,
    output_dir: Path,
    logger: logging.Logger
) -> Optional[Path]:
    """
    Process a single variable from GRIB2 to COG.

    Args:
        grib_file: Input GRIB2 file
        variable_name: Variable identifier
        variable_config: Variable configuration dict
        config: Global configuration
        output_dir: Output directory
        logger: Logger instance

    Returns:
        Path to created COG, or None if failed
    """
    logger.info(f"Processing variable: {variable_name}")

    # Extract variable from GRIB2
    data_array = extract_variable_from_grib(
        grib_file,
        variable_config['grib_search'],
        logger
    )

    if data_array is None:
        return None

    # Apply unit conversion
    if variable_config.get('conversion'):
        data_array = apply_unit_conversion(
            data_array,
            variable_config['conversion'],
            config,
            logger
        )

    # Get processing settings
    processing = config.get_processing_config()

    # Reproject to Web Mercator (optional target resolution for upsampling coarse data)
    target_res = processing.get('target_resolution_meters')
    if target_res is not None and target_res <= 0:
        target_res = None
    data_array = reproject_to_web_mercator(
        data_array,
        processing.get('resampling_method', 'bilinear'),
        logger,
        target_resolution_meters=target_res
    )

    # Generate output filename
    # Extract timestamp from GRIB filename (e.g., hrrr.20260110.t19z.f00.grib2)
    grib_name = grib_file.stem
    output_name = f"{variable_name}_{grib_name}.tif"
    output_path = output_dir / output_name

    # Create COG
    success = create_cog(
        data_array,
        output_path,
        compression=processing.get('compression', 'DEFLATE'),
        tile_size=processing.get('tile_size', 512),
        overview_levels=processing.get('overview_levels', [2, 4, 8, 16]),
        logger=logger
    )

    if success:
        return output_path
    else:
        return None


def process_grib_file(
    grib_file: Path,
    config: VariableConfig,
    output_dir: Path,
    priority: Optional[int],
    variables: Optional[List[str]],
    logger: logging.Logger
) -> Dict[str, Path]:
    """
    Process all enabled variables from a GRIB2 file.

    Args:
        grib_file: Input GRIB2 file
        config: Variable configuration
        output_dir: Output directory
        priority: Filter by priority level
        variables: Specific variables to process (None = all enabled)
        logger: Logger instance

    Returns:
        Dict mapping variable names to output file paths
    """
    logger.info(f"Processing GRIB2 file: {grib_file}")

    # Get variables to process
    if variables:
        # Process specific variables
        all_vars = config.config.get('variables', {})
        vars_to_process = {
            name: all_vars[name]
            for name in variables
            if name in all_vars
        }
    elif priority is not None:
        # Filter by priority
        vars_to_process = config.get_variables_by_priority(priority)
    else:
        # All enabled variables
        vars_to_process = config.get_enabled_variables()

    logger.info(f"Processing {len(vars_to_process)} variables")

    # Process each variable
    results = {}
    success_count = 0

    for var_name, var_config in vars_to_process.items():
        try:
            output_path = process_variable(
                grib_file,
                var_name,
                var_config,
                config,
                output_dir,
                logger
            )

            if output_path:
                results[var_name] = output_path
                success_count += 1
            else:
                logger.warning(f"Failed to process variable: {var_name}")

        except Exception as e:
            logger.error(f"Error processing {var_name}: {e}", exc_info=True)

    logger.info(f"Processed {success_count}/{len(vars_to_process)} variables successfully")

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Process HRRR GRIB2 files to Cloud Optimized GeoTIFFs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all enabled variables
  %(prog)s --input /tmp/hrrr.grib2 --output /tmp/processed/

  # Process only priority 1 variables
  %(prog)s --input /tmp/hrrr.grib2 --output /tmp/processed/ --priority 1

  # Process specific variables
  %(prog)s --input /tmp/hrrr.grib2 --output /tmp/processed/ --variables temperature_2m wind_u_10m

  # List available bands in GRIB2 file
  %(prog)s --input /tmp/hrrr.grib2 --list-bands
        """
    )

    parser.add_argument('--input', '-i', type=Path, required=True,
                       help='Input GRIB2 file')
    parser.add_argument('--output', '-o', type=Path,
                       help='Output directory for COG files')
    parser.add_argument('--config', '-c', type=Path,
                       help='Path to variables.yaml (default: config/variables.yaml)')
    parser.add_argument('--priority', type=int, choices=[1, 2, 3],
                       help='Process only variables with this priority')
    parser.add_argument('--variables', nargs='+',
                       help='Specific variables to process')
    parser.add_argument('--list-bands', action='store_true',
                       help='List all bands in GRIB2 file and exit')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable debug logging')

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.verbose)

    # Check input file exists
    if not args.input.exists():
        logger.error(f"Input file not found: {args.input}")
        return 2

    # List bands mode
    if args.list_bands:
        logger.info(f"Listing bands in {args.input}")
        try:
            bands = list_grib_bands(args.input)
            print(f"\n{'Band':<6} {'Element':<15} {'Short Name':<15} {'Description'}")
            print("=" * 80)
            for band_num, desc, element, short_name, metadata in bands:
                print(f"{band_num:<6} {element:<15} {short_name:<15} {desc[:50]}")
            print(f"\nTotal bands: {len(bands)}")
            return 0
        except Exception as e:
            logger.error(f"Failed to list bands: {e}")
            return 2

    # Output directory required for processing
    if not args.output:
        logger.error("--output is required for processing")
        return 2

    # Load configuration
    try:
        config = VariableConfig(args.config)
        logger.info(f"Loaded configuration from {config.config_path}")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return 2

    # Process GRIB2 file
    try:
        results = process_grib_file(
            args.input,
            config,
            args.output,
            args.priority,
            args.variables,
            logger
        )

        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("Processing Complete")
        logger.info("=" * 60)
        logger.info(f"Processed: {len(results)} variables")
        logger.info(f"Output directory: {args.output}")

        for var_name, output_path in results.items():
            size_mb = output_path.stat().st_size / 1024 / 1024
            logger.info(f"  ✓ {var_name}: {output_path.name} ({size_mb:.2f} MB)")

        if len(results) == 0:
            logger.error("No variables processed successfully")
            return 1

        return 0

    except Exception as e:
        logger.error(f"Processing failed: {e}", exc_info=True)
        return 2


if __name__ == '__main__':
    sys.exit(main())
