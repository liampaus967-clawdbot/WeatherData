#!/usr/bin/env python3
"""
Generate Web Map Tiles from Weather COG Files

Generates XYZ tile pyramid from colored Cloud Optimized GeoTIFFs:
- Wraps gdal2tiles.py for web map tile generation
- Supports XYZ tile naming (OSM/Slippy Map standard)
- Parallel tile generation for performance
- Organized directory structure by variable/timestamp/forecast
- PNG tiles with transparency
- Configurable zoom levels

Part of TICKET-008: Implement Tile Generation Strategy
"""

import argparse
import logging
import sys
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re
import shutil
from osgeo import gdal, osr

# Configure GDAL
gdal.UseExceptions()

# Configure logging
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
    return logging.getLogger('generate_tiles')


def fix_srs_if_needed(input_cog: Path, logger: logging.Logger) -> Path:
    """
    Fix spatial reference system if needed.

    Args:
        input_cog: Input COG file
        logger: Logger instance

    Returns:
        Path to COG (may be temp file if SRS was fixed)

    gdal2tiles requires proper EPSG:3857 definition, but gdaldem color-relief
    sometimes creates files with "Unknown engineering datum" instead.
    This function creates a temp copy with proper SRS if needed.
    """
    # Check current SRS
    ds = gdal.Open(str(input_cog))
    if not ds:
        logger.error(f"Cannot open file: {input_cog}")
        return input_cog

    srs_wkt = ds.GetProjection()
    spatial_ref = ds.GetSpatialRef()
    ds = None

    # Always fix SRS for colored COGs to ensure proper EPSG:3857
    # gdaldem color-relief often creates invalid SRS even though data is in 3857
    needs_fix = False

    if spatial_ref:
        # Try to get authority code
        auth_name = spatial_ref.GetAuthorityName(None)
        auth_code = spatial_ref.GetAuthorityCode(None)

        # Check for problematic SRS
        if "Unknown engineering datum" in srs_wkt or "ENGCRS" in srs_wkt:
            needs_fix = True
            logger.warning(f"File has unknown engineering datum")
        elif not auth_code or auth_code != "3857":
            needs_fix = True
            logger.warning(f"File has non-standard EPSG code: {auth_name}:{auth_code}")
    else:
        needs_fix = True
        logger.warning(f"File has no spatial reference")

    if needs_fix:
        logger.info(f"Fixing SRS to EPSG:3857")

        # Create temp file with proper SRS
        temp_file = Path(tempfile.mktemp(suffix='.tif', prefix='fixed_srs_'))

        # Use gdal.Translate to create copy with proper SRS
        # Don't copy overviews to avoid warnings
        options = gdal.TranslateOptions(
            format='GTiff',
            outputSRS='EPSG:3857',
            creationOptions=['TILED=YES', 'COMPRESS=DEFLATE']
        )

        result = gdal.Translate(str(temp_file), str(input_cog), options=options)
        if result:
            result = None  # Close dataset
            logger.info(f"Created temp file with fixed SRS: {temp_file}")
            return temp_file
        else:
            logger.error(f"Failed to fix SRS")
            return input_cog

    logger.debug(f"SRS is correct, no fix needed")
    return input_cog


def parse_cog_filename(cog_file: Path) -> Optional[Dict[str, str]]:
    """
    Parse metadata from COG filename.

    Args:
        cog_file: Path to COG file

    Returns:
        Dict with variable, model, date, cycle, forecast, or None

    Example:
        temperature_2m_hrrr.20260110.t19z.f00_colored.tif
        Returns: {
            'variable': 'temperature_2m',
            'model': 'hrrr',
            'date': '20260110',
            'cycle': '19z',
            'forecast': 'f00'
        }
    """
    # Remove _colored suffix if present
    name = cog_file.stem
    if name.endswith('_colored'):
        name = name[:-8]

    # Pattern: {variable}_{model}.{date}.{cycle}.{forecast}
    # Example: temperature_2m_hrrr.20260110.t19z.f00
    # Example: wave_height_gfs_wave.20260203.t00z.f000
    pattern = r'^(.+?)_(hrrr|gfs_wave|gfs|nam)\.(\d{8})\.t(\d{2}z)\.f(\d{2,3})$'
    match = re.match(pattern, name)

    if match:
        return {
            'variable': match.group(1),
            'model': match.group(2),
            'date': match.group(3),
            'cycle': match.group(4),
            'forecast': match.group(5)
        }

    # Fallback: try to extract at least variable name
    # Pattern: variable_hrrr... or variable_gfs...
    parts = name.split('_')
    if len(parts) >= 2:
        # Find where model name starts
        model_prefixes = ['hrrr', 'gfs', 'nam']
        try:
            model_idx = next(i for i, p in enumerate(parts) if any(p.startswith(m) for m in model_prefixes))
            variable = '_'.join(parts[:model_idx])
            return {
                'variable': variable,
                'model': 'unknown',
                'date': 'unknown',
                'cycle': 'unknown',
                'forecast': 'unknown'
            }
        except StopIteration:
            pass

    return None


def generate_tiles(
    input_cog: Path,
    output_dir: Path,
    zoom_levels: str,
    processes: int,
    exclude_transparent: bool,
    resume: bool,
    png_level: int,
    use_ramdisk: bool,
    logger: logging.Logger
) -> Dict[str, any]:
    """
    Generate tiles from a COG file using gdal2tiles.py.

    Args:
        input_cog: Input colored COG file
        output_dir: Output directory for tiles
        zoom_levels: Zoom level range (e.g., "0-10", "5-8")
        processes: Number of parallel processes
        exclude_transparent: Exclude fully transparent tiles
        resume: Resume mode (only generate missing tiles)
        png_level: PNG compression level (1-9, default 6)
        use_ramdisk: Use RAM disk for temporary storage
        logger: Logger instance

    Returns:
        Dict with success status and performance metrics
    """
    start_time = time.time()

    logger.info(f"Generating tiles: {input_cog.name}")
    logger.info(f"  Zoom levels: {zoom_levels}")
    logger.info(f"  Output: {output_dir}")
    logger.info(f"  Processes: {processes}")
    logger.info(f"  PNG compression: {png_level}")
    if use_ramdisk:
        logger.info(f"  Using RAM disk for temporary storage")

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Handle RAM disk (if requested and available)
    actual_output = output_dir
    temp_ramdisk = None
    if use_ramdisk:
        # Try to use /dev/shm (Linux) or /tmp (fallback)
        ramdisk_base = Path('/dev/shm') if Path('/dev/shm').exists() else Path('/tmp')
        if ramdisk_base.exists() and ramdisk_base.is_dir():
            temp_ramdisk = ramdisk_base / f"tiles_{time.time()}"
            temp_ramdisk.mkdir(parents=True, exist_ok=True)
            actual_output = temp_ramdisk
            logger.debug(f"Using RAM disk: {actual_output}")
        else:
            logger.warning("RAM disk not available, using regular disk")

    # Build gdal2tiles command
    cmd = [
        'gdal2tiles.py',
        '--profile=mercator',  # Web Mercator (EPSG:3857)
        '--xyz',  # XYZ tile numbering (not TMS)
        f'--zoom={zoom_levels}',
        '--resampling=average',  # Good for downsampling
        f'--processes={processes}',
        '--tilesize=256',  # Standard tile size
        '--tiledriver=PNG',  # PNG format
        '--webviewer=none',  # Don't generate HTML viewer
    ]

    # Add PNG compression level (TICKET-009 optimization)
    if png_level != 6:  # 6 is gdal2tiles default
        cmd.append(f'--tiledriver-options=ZLEVEL={png_level}')

    # Add optional flags
    if exclude_transparent:
        cmd.append('--exclude')  # Exclude fully transparent tiles

    if resume:
        cmd.append('--resume')  # Only generate missing files

    # Add verbose flag if debug logging
    if logger.level == logging.DEBUG:
        cmd.append('--verbose')
    else:
        cmd.append('--quiet')

    # Add input and output
    cmd.extend([
        str(input_cog),
        str(actual_output)
    ])

    logger.debug(f"Running: {' '.join(cmd)}")

    try:
        # Run gdal2tiles
        tile_gen_start = time.time()
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        tile_gen_time = time.time() - tile_gen_start

        if result.returncode != 0:
            logger.error(f"gdal2tiles failed: {result.stderr}")
            return {'success': False, 'error': result.stderr}

        # Copy from RAM disk to final location if needed
        copy_time = 0
        if temp_ramdisk and temp_ramdisk != output_dir:
            copy_start = time.time()
            logger.info(f"Copying tiles from RAM disk to {output_dir}")

            # Copy all zoom directories
            for item in temp_ramdisk.iterdir():
                if item.is_dir():
                    dest = output_dir / item.name
                    if dest.exists():
                        shutil.rmtree(dest)
                    shutil.copytree(item, dest)

            copy_time = time.time() - copy_start

            # Clean up RAM disk
            shutil.rmtree(temp_ramdisk)
            logger.debug(f"Cleaned up RAM disk: {temp_ramdisk}")

        total_time = time.time() - start_time

        logger.info(f"Tiles generated successfully: {output_dir}")
        logger.info(f"  Tile generation: {tile_gen_time:.1f}s")
        if copy_time > 0:
            logger.info(f"  RAM disk copy: {copy_time:.1f}s")
        logger.info(f"  Total time: {total_time:.1f}s")

        return {
            'success': True,
            'tile_gen_time': tile_gen_time,
            'copy_time': copy_time,
            'total_time': total_time,
            'used_ramdisk': temp_ramdisk is not None
        }

    except subprocess.CalledProcessError as e:
        logger.error(f"gdal2tiles command failed: {e.stderr}")
        return {'success': False, 'error': str(e.stderr)}
    except FileNotFoundError:
        logger.error("gdal2tiles.py not found. Ensure GDAL is installed.")
        return {'success': False, 'error': 'gdal2tiles.py not found'}
    except Exception as e:
        logger.error(f"Error generating tiles: {e}")
        return {'success': False, 'error': str(e)}


def get_tile_stats(output_dir: Path) -> Dict[str, int]:
    """
    Get statistics about generated tiles.

    Args:
        output_dir: Tile output directory

    Returns:
        Dict with tile counts per zoom level
    """
    stats = {}

    # Iterate through zoom level directories
    for zoom_dir in sorted(output_dir.iterdir()):
        if zoom_dir.is_dir() and zoom_dir.name.isdigit():
            zoom_level = int(zoom_dir.name)

            # Count PNG files recursively
            tile_count = len(list(zoom_dir.rglob('*.png')))
            stats[zoom_level] = tile_count

    return stats


def find_cog_files(input_path: Path) -> List[Path]:
    """
    Find colored COG files to process.

    Args:
        input_path: Input directory or file

    Returns:
        List of COG file paths
    """
    if input_path.is_file():
        return [input_path]

    # Search for colored COG files
    cog_files = list(input_path.glob('*_colored.tif'))

    return sorted(cog_files)


def organize_tile_structure(
    temp_dir: Path,
    final_dir: Path,
    metadata: Dict[str, str],
    logger: logging.Logger
) -> Path:
    """
    Reorganize tiles into final directory structure.

    Args:
        temp_dir: Temporary gdal2tiles output directory
        final_dir: Final organized directory
        metadata: Parsed filename metadata
        logger: Logger instance

    Returns:
        Path to final tile directory

    Directory structure:
        {variable}/{date}T{cycle}/{forecast}/{z}/{x}/{y}.png

    Example:
        temperature_2m/20260110T19z/f00/5/10/15.png
    """
    # Create organized path
    organized_path = final_dir / metadata['variable'] / f"{metadata['date']}T{metadata['cycle']}" / metadata['forecast']

    # Create directory
    organized_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Organizing tiles: {temp_dir} → {organized_path}")

    # Move zoom level directories
    for zoom_dir in temp_dir.iterdir():
        if zoom_dir.is_dir() and zoom_dir.name.isdigit():
            dest_zoom = organized_path / zoom_dir.name

            # Remove existing if present
            if dest_zoom.exists():
                shutil.rmtree(dest_zoom)

            # Move directory
            shutil.move(str(zoom_dir), str(dest_zoom))
            logger.debug(f"Moved zoom level {zoom_dir.name}")

    # Clean up temp directory
    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    return organized_path


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Generate web map tiles from colored weather COG files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate tiles from single file (zoom 0-10)
  %(prog)s --input temperature_2m_*_colored.tif --output /tmp/tiles

  # Generate tiles for specific zoom levels
  %(prog)s --input data/ --output /tmp/tiles --zoom 5-8

  # Use 8 parallel processes
  %(prog)s --input data/ --output /tmp/tiles --processes 8

  # Exclude transparent tiles to save space
  %(prog)s --input data/ --output /tmp/tiles --exclude-transparent

  # Resume interrupted generation
  %(prog)s --input data/ --output /tmp/tiles --resume

  # Organized directory structure
  %(prog)s --input data/ --output /tmp/tiles --organize

  # Verbose logging
  %(prog)s --input data/ --output /tmp/tiles --verbose
        """
    )

    parser.add_argument(
        '--input', '-i',
        type=Path,
        required=True,
        help='Input colored COG file or directory'
    )

    parser.add_argument(
        '--output', '-o',
        type=Path,
        required=True,
        help='Output directory for tiles'
    )

    parser.add_argument(
        '--zoom', '-z',
        type=str,
        default='0-10',
        help='Zoom level range (e.g., "0-10", "5-8") (default: 0-10)'
    )

    parser.add_argument(
        '--processes', '-p',
        type=int,
        default=4,
        help='Number of parallel processes (default: 4)'
    )

    parser.add_argument(
        '--exclude-transparent', '-x',
        action='store_true',
        help='Exclude fully transparent tiles'
    )

    parser.add_argument(
        '--resume', '-r',
        action='store_true',
        help='Resume mode (only generate missing tiles)'
    )

    parser.add_argument(
        '--png-level',
        type=int,
        default=6,
        choices=range(1, 10),
        metavar='LEVEL',
        help='PNG compression level (1-9, default: 6, higher=smaller/slower)'
    )

    parser.add_argument(
        '--use-ramdisk',
        action='store_true',
        help='Use RAM disk for temporary tile storage (faster, requires /dev/shm)'
    )

    parser.add_argument(
        '--organize',
        action='store_true',
        help='Organize tiles by variable/timestamp/forecast structure'
    )

    parser.add_argument(
        '--verbose', '-v',
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

    # Find COG files to process
    cog_files = find_cog_files(args.input)

    if not cog_files:
        logger.error(f"No colored COG files found in {args.input}")
        return 1

    logger.info(f"Found {len(cog_files)} COG file(s) to process")

    # Process each file
    results = {}
    success_count = 0

    for cog_file in cog_files:
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Processing: {cog_file.name}")
        logger.info(f"{'=' * 60}")

        # Parse filename metadata
        metadata = parse_cog_filename(cog_file)
        if not metadata:
            logger.warning(f"Cannot parse filename: {cog_file.name}, skipping")
            continue

        logger.debug(f"Metadata: {metadata}")

        # Determine output directory
        if args.organize:
            # Use temporary directory first, then reorganize
            import tempfile
            temp_output = Path(tempfile.mkdtemp(prefix='tiles_'))
            final_output = args.output
        else:
            # Direct output
            temp_output = args.output / cog_file.stem
            final_output = None

        try:
            # Fix SRS if needed (gdaldem color-relief sometimes creates invalid SRS)
            fixed_cog = fix_srs_if_needed(cog_file, logger)

            # Generate tiles
            result = generate_tiles(
                fixed_cog,
                temp_output,
                args.zoom,
                args.processes,
                args.exclude_transparent,
                args.resume,
                args.png_level,
                args.use_ramdisk,
                logger
            )

            # Clean up temp file if SRS was fixed
            if fixed_cog != cog_file and fixed_cog.exists():
                fixed_cog.unlink()
                logger.debug(f"Cleaned up temp file: {fixed_cog}")

            if not result.get('success'):
                logger.error(f"Failed to generate tiles for {cog_file.name}")
                continue

            # Organize if requested
            if args.organize and final_output:
                organized_path = organize_tile_structure(
                    temp_output,
                    final_output,
                    metadata,
                    logger
                )
                output_path = organized_path
            else:
                output_path = temp_output

            # Get statistics
            stats = get_tile_stats(output_path)
            total_tiles = sum(stats.values())

            logger.info(f"Generated {total_tiles} tiles across {len(stats)} zoom levels")
            for zoom, count in sorted(stats.items()):
                logger.info(f"  Zoom {zoom}: {count} tiles")

            # Calculate tiles per second
            if result.get('total_time', 0) > 0:
                tiles_per_sec = total_tiles / result['total_time']
                logger.info(f"  Performance: {tiles_per_sec:.1f} tiles/second")

            results[cog_file.name] = {
                'success': True,
                'output': output_path,
                'total_tiles': total_tiles,
                'stats': stats,
                'tile_gen_time': result.get('tile_gen_time', 0),
                'copy_time': result.get('copy_time', 0),
                'total_time': result.get('total_time', 0),
                'used_ramdisk': result.get('used_ramdisk', False)
            }
            success_count += 1

        except Exception as e:
            logger.error(f"Error processing {cog_file.name}: {e}")
            results[cog_file.name] = {'success': False, 'error': str(e)}

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Tile Generation Summary")
    logger.info("=" * 60)
    logger.info(f"Total files processed: {len(cog_files)}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Failed: {len(cog_files) - success_count}")

    if results:
        logger.info("\nResults:")
        total_time = 0
        total_tiles_all = 0
        for filename, result in results.items():
            if result['success']:
                tiles = result['total_tiles']
                time_taken = result.get('total_time', 0)
                total_time += time_taken
                total_tiles_all += tiles
                tps = tiles / time_taken if time_taken > 0 else 0
                logger.info(f"  ✓ {filename}: {tiles} tiles in {time_taken:.1f}s ({tps:.0f} tiles/s)")
            else:
                logger.info(f"  ✗ {filename}: {result.get('error', 'Unknown error')}")

        # Overall performance stats
        if total_time > 0 and total_tiles_all > 0:
            logger.info(f"\nOverall Performance:")
            logger.info(f"  Total tiles: {total_tiles_all}")
            logger.info(f"  Total time: {total_time:.1f}s")
            logger.info(f"  Average: {total_tiles_all / total_time:.0f} tiles/second")

    return 0 if success_count == len(cog_files) else 1


if __name__ == '__main__':
    sys.exit(main())
