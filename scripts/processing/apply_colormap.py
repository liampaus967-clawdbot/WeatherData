#!/usr/bin/env python3
"""
Apply Color Ramps to Weather Data COGs

Takes grayscale Cloud Optimized GeoTIFFs and applies color ramps for visualization:
- Reads color ramp configurations from variables.yaml
- Converts YAML color definitions to GDAL color-relief format
- Applies color ramps using gdaldem color-relief
- Outputs RGB GeoTIFFs optimized for web display
- Supports transparency for no-data values

Part of TICKET-007: Add Color Ramp and Visualization Styling
"""

import argparse
import logging
import sys
import tempfile
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import subprocess

from osgeo import gdal

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
    return logging.getLogger('apply_colormap')


def create_color_relief_file(color_ramp: Dict, temp_dir: Path, logger: logging.Logger) -> Path:
    """
    Create a GDAL color-relief text file from YAML color ramp configuration.

    Args:
        color_ramp: Color ramp configuration from variables.yaml
        temp_dir: Temporary directory for color file
        logger: Logger instance

    Returns:
        Path to color-relief text file

    GDAL color-relief format:
        value red green blue [alpha]

    Example:
        -40 26 0 102
        -30 77 0 153
        0 0 255 0
        50 255 0 0
    """
    color_file = temp_dir / "color_ramp.txt"

    with open(color_file, 'w') as f:
        # Write header comment
        f.write("# GDAL color-relief file\n")
        f.write("# Format: value red green blue [alpha]\n\n")

        # Get color stops
        colors = color_ramp.get('colors', [])

        for color_stop in colors:
            value = color_stop['value']
            hex_color = color_stop['color']

            # Convert hex color to RGB
            # Remove '#' if present
            hex_color = hex_color.lstrip('#')

            # Parse hex to RGB
            r = int(hex_color[0:2], 16)
            g = int(hex_color[2:4], 16)
            b = int(hex_color[4:6], 16)

            # Write color entry
            f.write(f"{value} {r} {g} {b}\n")

        # Add nodata value (transparent)
        f.write("nv 0 0 0 0\n")

    logger.debug(f"Created color-relief file with {len(colors)} color stops: {color_file}")
    return color_file


def apply_color_ramp(
    input_cog: Path,
    output_path: Path,
    color_file: Path,
    logger: logging.Logger
) -> bool:
    """
    Apply color ramp to a COG using gdaldem color-relief.

    Args:
        input_cog: Input grayscale COG
        output_path: Output RGB GeoTIFF path
        color_file: Color-relief text file
        logger: Logger instance

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Applying color ramp to {input_cog.name}")

    try:
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Use a temporary file for initial output
        with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp_file:
            temp_output = Path(tmp_file.name)

        # Run gdaldem color-relief
        cmd = [
            'gdaldem',
            'color-relief',
            str(input_cog),
            str(color_file),
            str(temp_output),
            '-alpha',  # Add alpha channel for transparency
            '-co', 'COMPRESS=DEFLATE',
            '-co', 'PREDICTOR=2',
            '-co', 'ZLEVEL=6',
            '-co', 'TILED=YES',
            '-co', 'BLOCKXSIZE=512',
            '-co', 'BLOCKYSIZE=512',
            '-co', 'NUM_THREADS=ALL_CPUS'
        ]

        logger.debug(f"Running: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )

        if result.returncode != 0:
            logger.error(f"gdaldem failed: {result.stderr}")
            return False

        # Add overviews to the output file
        logger.debug("Adding overviews to RGB output")
        ds = gdal.Open(str(temp_output), gdal.GA_Update)
        if ds:
            ds.BuildOverviews('AVERAGE', [2, 4, 8, 16])
            ds = None

        # Move temp file to final location
        shutil.move(str(temp_output), str(output_path))

        # Get file size
        size_mb = output_path.stat().st_size / 1024 / 1024
        logger.info(f"Created colored output: {output_path.name} ({size_mb:.2f} MB)")

        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"gdaldem command failed: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Error applying color ramp: {e}")
        return False


def process_cog_file(
    input_cog: Path,
    variable_name: str,
    config: VariableConfig,
    output_dir: Path,
    logger: logging.Logger
) -> Optional[Path]:
    """
    Process a single COG file with color ramp.

    Args:
        input_cog: Input grayscale COG
        variable_name: Variable name (to lookup color ramp)
        config: Variable configuration
        output_dir: Output directory
        logger: Logger instance

    Returns:
        Path to output file if successful, None otherwise
    """
    # Get variable configuration
    variable_config = config.get_variable_by_name(variable_name)
    if not variable_config:
        logger.error(f"Variable '{variable_name}' not found in configuration")
        return None

    # Get color ramp name
    color_ramp_name = variable_config.get('color_ramp')
    if not color_ramp_name:
        logger.error(f"No color ramp specified for variable '{variable_name}'")
        return None

    # Get color ramp configuration
    color_ramp = config.get_color_ramp(color_ramp_name)
    if not color_ramp:
        logger.error(f"Color ramp '{color_ramp_name}' not found in configuration")
        return None

    # Create temporary directory for color file
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create color-relief file
        color_file = create_color_relief_file(color_ramp, temp_path, logger)

        # Generate output filename
        # Input: temperature_2m_hrrr.20260110.t19z.f00.tif
        # Output: temperature_2m_hrrr.20260110.t19z.f00_colored.tif
        output_name = input_cog.stem + "_colored.tif"
        output_path = output_dir / output_name

        # Apply color ramp
        success = apply_color_ramp(input_cog, output_path, color_file, logger)

        if success:
            return output_path
        else:
            return None


def find_cog_files(input_path: Path, variable_name: Optional[str] = None) -> List[Path]:
    """
    Find COG files to process.

    Args:
        input_path: Input directory or file
        variable_name: Optional variable name filter

    Returns:
        List of COG file paths
    """
    if input_path.is_file():
        return [input_path]

    # Search for .tif files
    pattern = f"{variable_name}_*.tif" if variable_name else "*.tif"

    # Exclude already-colored files
    cog_files = [
        f for f in input_path.glob(pattern)
        if not f.stem.endswith('_colored')
    ]

    return sorted(cog_files)


def infer_variable_name(cog_file: Path) -> Optional[str]:
    """
    Infer variable name from COG filename.

    Args:
        cog_file: COG file path

    Returns:
        Variable name or None

    Example:
        temperature_2m_hrrr.20260110.t19z.f00.tif -> temperature_2m
        wind_u_10m_hrrr.20260110.t19z.f00.tif -> wind_u_10m
    """
    # Filename pattern: {variable_name}_{model}.{date}.{cycle}.{forecast}.tif
    # Split on underscore and look for pattern
    name = cog_file.stem

    # Remove _colored suffix if present
    if name.endswith('_colored'):
        name = name[:-8]

    # Common patterns: variable_name_hrrr.YYYYMMDD... or variable_name_gfs_wave.YYYYMMDD...
    # Split by underscore
    parts = name.split('_')

    # Look for model name in parts (indicates start of timestamp)
    # Support: hrrr, gfs, gfs_wave, etc.
    model_prefixes = ['hrrr', 'gfs']
    try:
        model_index = next(i for i, p in enumerate(parts) if any(p.startswith(m) for m in model_prefixes))
        # Variable name is everything before model name
        variable_name = '_'.join(parts[:model_index])
        return variable_name
    except StopIteration:
        # No model found, might be a different naming pattern
        # Try first two parts joined (e.g., temperature_2m)
        if len(parts) >= 2:
            return '_'.join(parts[:2])

        return None


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Apply color ramps to weather data COG files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process single file (auto-detect variable)
  %(prog)s --input temperature_2m_hrrr.20260110.t19z.f00.tif

  # Process single file with explicit variable
  %(prog)s --input temp.tif --variable temperature_2m

  # Process all COG files in directory
  %(prog)s --input /tmp/processed-weather/ --output /tmp/colored/

  # Process only temperature files
  %(prog)s --input /tmp/processed-weather/ --variable temperature_2m

  # Process with custom config
  %(prog)s --input data/ --config custom_variables.yaml

  # Verbose logging
  %(prog)s --input data/ --verbose
        """
    )

    parser.add_argument(
        '--input', '-i',
        type=Path,
        required=True,
        help='Input COG file or directory'
    )

    parser.add_argument(
        '--output', '-o',
        type=Path,
        help='Output directory (default: same as input)'
    )

    parser.add_argument(
        '--variable', '-v',
        type=str,
        help='Variable name (auto-detected from filename if not specified)'
    )

    parser.add_argument(
        '--config', '-c',
        type=Path,
        help='Path to variables.yaml config file'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.verbose)

    # Validate input
    if not args.input.exists():
        logger.error(f"Input path does not exist: {args.input}")
        return 1

    # Determine output directory
    if args.output:
        output_dir = args.output
    elif args.input.is_file():
        output_dir = args.input.parent
    else:
        output_dir = args.input

    output_dir.mkdir(parents=True, exist_ok=True)

    # Load configuration
    config_path = args.config or Path(__file__).parent.parent.parent / 'config' / 'variables.yaml'

    try:
        config = VariableConfig(config_path)
        logger.info(f"Loaded configuration from {config_path}")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return 1

    # Find COG files to process
    cog_files = find_cog_files(args.input, args.variable)

    if not cog_files:
        logger.error(f"No COG files found in {args.input}")
        return 1

    logger.info(f"Found {len(cog_files)} COG file(s) to process")

    # Process each file
    results = {}
    success_count = 0

    for cog_file in cog_files:
        # Infer variable name if not specified
        if args.variable:
            variable_name = args.variable
        else:
            variable_name = infer_variable_name(cog_file)
            if not variable_name:
                logger.warning(f"Cannot infer variable name from {cog_file.name}, skipping")
                continue
            logger.debug(f"Inferred variable name: {variable_name}")

        # Process the file
        try:
            output_path = process_cog_file(
                cog_file,
                variable_name,
                config,
                output_dir,
                logger
            )

            if output_path:
                results[cog_file.name] = output_path
                success_count += 1
        except Exception as e:
            logger.error(f"Error processing {cog_file.name}: {e}")
            continue

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Processing Summary")
    logger.info("=" * 60)
    logger.info(f"Total files processed: {len(cog_files)}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Failed: {len(cog_files) - success_count}")

    if results:
        logger.info("\nOutput files:")
        for input_name, output_path in results.items():
            size_mb = output_path.stat().st_size / 1024 / 1024
            logger.info(f"  ✓ {input_name} → {output_path.name} ({size_mb:.2f} MB)")

    return 0 if success_count == len(cog_files) else 1


if __name__ == '__main__':
    sys.exit(main())
