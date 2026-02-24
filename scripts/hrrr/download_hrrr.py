#!/usr/bin/env python3
"""
HRRR Weather Data Download Script using Herbie

Downloads HRRR (High-Resolution Rapid Refresh) forecast data using the Herbie library
and uploads processed files to S3.

Usage:
    python download_hrrr.py --date 2026-01-10 --cycle 12 --fxx 0-12 --variables TMP:2m,UGRD:10m,VGRD:10m
    python download_hrrr.py --latest --variables all
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
import json

import boto3
from botocore.exceptions import ClientError
from herbie import Herbie
import xarray as xr


# Configuration
DEFAULT_S3_BUCKET = os.environ.get("S3_BUCKET", "driftwise-weather-data")
DEFAULT_MODEL = "hrrr"
DEFAULT_PRODUCT = "sfc"  # Surface level product
TEMP_DIR = Path("/tmp/weather-data")

# Default variables to download
DEFAULT_VARIABLES = [
    "TMP:2 m",       # 2-meter temperature
    "UGRD:10 m",     # 10-meter U-component of wind
    "VGRD:10 m",     # 10-meter V-component of wind
    "DPT:2 m",       # 2-meter dewpoint temperature
    "RH:2 m",        # 2-meter relative humidity
    "GUST:surface",  # Surface wind gust
]


def setup_logging(verbose: bool = False) -> logging.Logger:
    """
    Configure logging with timestamps and appropriate level.

    Args:
        verbose: If True, set log level to DEBUG

    Returns:
        Configured logger instance
    """
    log_level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger = logging.getLogger('download_hrrr')
    return logger


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description='Download HRRR forecast data using Herbie',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download latest forecast, all default variables, F00-F12
  python download_hrrr.py --latest

  # Download specific date and cycle
  python download_hrrr.py --date 2026-01-10 --cycle 12

  # Download specific forecast hours
  python download_hrrr.py --latest --fxx 0,1,2,3

  # Download specific variables
  python download_hrrr.py --latest --variables "TMP:2 m,UGRD:10 m"

  # Dry run (no S3 upload)
  python download_hrrr.py --latest --dry-run
        """
    )

    # Date/time options
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        '--date',
        type=str,
        help='Forecast date in YYYY-MM-DD format'
    )
    date_group.add_argument(
        '--latest',
        action='store_true',
        help='Use latest available forecast (current time - 3 hours)'
    )

    parser.add_argument(
        '--cycle',
        type=int,
        choices=range(24),
        metavar='HOUR',
        help='Model cycle hour (0-23). Required with --date, ignored with --latest'
    )

    parser.add_argument(
        '--fxx',
        type=str,
        default='0-12',
        help='Forecast hours to download (e.g., "0-12", "0,6,12", "0-6,12"). Default: 0-12'
    )

    parser.add_argument(
        '--variables',
        type=str,
        default='default',
        help=(
            'Variables to download. Options: '
            '"default" (common variables), '
            '"all" (download full GRIB2), '
            'or comma-separated list (e.g., "TMP:2 m,UGRD:10 m"). '
            'Default: default'
        )
    )

    # S3 options
    parser.add_argument(
        '--s3-bucket',
        type=str,
        default=DEFAULT_S3_BUCKET,
        help=f'S3 bucket name. Default: {DEFAULT_S3_BUCKET}'
    )

    parser.add_argument(
        '--s3-prefix',
        type=str,
        default='raw-grib2',
        help='S3 prefix/folder for uploads. Default: raw-grib2'
    )

    parser.add_argument(
        '--local-only',
        action='store_true',
        help='Save files locally only, skip S3 upload'
    )

    # Processing options
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=TEMP_DIR,
        help=f'Local directory for downloaded files. Default: {TEMP_DIR}'
    )

    parser.add_argument(
        '--keep-local',
        action='store_true',
        help='Keep local files after S3 upload. Default: delete after upload'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be downloaded without actually downloading'
    )

    # Logging
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging (DEBUG level)'
    )

    args = parser.parse_args()

    # Validation
    if args.date and args.cycle is None:
        parser.error('--cycle is required when using --date')

    return args


def parse_forecast_hours(fxx_str: str) -> List[int]:
    """
    Parse forecast hour string into list of integers.

    Args:
        fxx_str: Forecast hour string (e.g., "0-12", "0,6,12", "0-6,12")

    Returns:
        List of forecast hour integers

    Examples:
        >>> parse_forecast_hours("0-12")
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        >>> parse_forecast_hours("0,6,12")
        [0, 6, 12]
        >>> parse_forecast_hours("0-3,6,12")
        [0, 1, 2, 3, 6, 12]
    """
    hours = []

    for part in fxx_str.split(','):
        part = part.strip()
        if '-' in part:
            # Range (e.g., "0-12")
            start, end = map(int, part.split('-'))
            hours.extend(range(start, end + 1))
        else:
            # Single value (e.g., "6")
            hours.append(int(part))

    return sorted(list(set(hours)))  # Remove duplicates and sort


def parse_variables(var_str: str) -> Optional[List[str]]:
    """
    Parse variable string into list.

    Args:
        var_str: Variable specification string

    Returns:
        List of variable strings, or None for "all"
    """
    if var_str.lower() == 'all':
        return None  # Download full GRIB2
    elif var_str.lower() == 'default':
        return DEFAULT_VARIABLES
    else:
        return [v.strip() for v in var_str.split(',')]


def calculate_latest_forecast_time() -> datetime:
    """
    Calculate the latest available HRRR forecast time.

    HRRR runs hourly but has a ~65 minute delay, so we go back 3 hours
    to ensure data availability.

    Returns:
        Datetime object for latest forecast initialization
    """
    now = datetime.utcnow()
    # Go back 3 hours to ensure data is available
    latest = now - timedelta(hours=3)
    # Round down to the hour
    latest = latest.replace(minute=0, second=0, microsecond=0)
    return latest


def download_hrrr_data(
    date: datetime,
    fxx: int,
    variables: Optional[List[str]],
    output_dir: Path,
    logger: logging.Logger,
    dry_run: bool = False
) -> Optional[Path]:
    """
    Download HRRR data using Herbie.

    Args:
        date: Forecast initialization datetime
        fxx: Forecast hour (0-48)
        variables: List of variable search strings, or None for full GRIB2
        output_dir: Directory to save downloaded files
        logger: Logger instance
        dry_run: If True, show what would be downloaded without downloading

    Returns:
        Path to downloaded file, or None if download failed
    """
    try:
        # Create Herbie object
        H = Herbie(
            date,
            model=DEFAULT_MODEL,
            product=DEFAULT_PRODUCT,
            fxx=fxx
        )

        logger.info(f"Forecast: {H}")

        if dry_run:
            logger.info(f"[DRY RUN] Would download: {H.grib}")
            if variables:
                logger.info(f"[DRY RUN] Variables: {', '.join(variables)}")
            return None

        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        # Download data
        if variables:
            # Download specific variables
            logger.info(f"Downloading variables: {', '.join(variables)}")

            # Download each variable
            ds_list = []
            
            for var in variables:
                logger.info(f"  Downloading {var}...")
                try:
                    ds = H.xarray(var, remove_grib=False)
                    ds_list.append(ds)
                    logger.info(f"  ✓ Downloaded {var}")
                except Exception as e:
                    logger.warning(f"  ✗ Failed to download {var}: {e}")

            if not ds_list:
                logger.error("No variables downloaded successfully")
                return None

            # Merge all datasets
            if len(ds_list) > 1:
                ds = xr.merge(ds_list)
            else:
                ds = ds_list[0]

            # Save as NetCDF
            filename = f"hrrr.{date.strftime('%Y%m%d')}.t{date.hour:02d}z.f{fxx:02d}.nc"
            output_path = output_dir / filename
            ds.to_netcdf(output_path)
            logger.info(f"Saved to {output_path}")

        else:
            # Download full GRIB2 file
            logger.info("Downloading full GRIB2 file")
            # Herbie downloads to its cache by default
            # Don't pass directory - let it use default cache, then we'll move it
            downloaded_path = H.download()
            logger.info(f"Herbie reported download to: {downloaded_path}")

            # Check if file actually exists
            downloaded_path = Path(downloaded_path)
            if not downloaded_path.exists():
                logger.error(f"File doesn't exist at reported path: {downloaded_path}")
                # Check if it's in output_dir instead
                possible_files = list(output_dir.glob("*.grib2"))
                if possible_files:
                    logger.info(f"Found GRIB2 files in output dir: {possible_files}")
                    output_path = possible_files[0]
                else:
                    logger.error("No GRIB2 files found in output directory")
                    return None
            elif downloaded_path.parent != output_dir:
                # Move to our output directory with standard naming
                filename = f"hrrr.{date.strftime('%Y%m%d')}.t{date.hour:02d}z.f{fxx:02d}.grib2"
                output_path = output_dir / filename
                import shutil
                shutil.move(str(downloaded_path), str(output_path))
                logger.info(f"Moved to {output_path}")
            else:
                output_path = downloaded_path

            logger.info(f"Final file location: {output_path}")

        return output_path

    except Exception as e:
        logger.error(f"Failed to download F{fxx:02d}: {e}")
        return None


def upload_to_s3(
    local_path: Path,
    bucket: str,
    s3_prefix: str,
    date: datetime,
    logger: logging.Logger
) -> bool:
    """
    Upload file to S3 with organized path structure.

    Args:
        local_path: Local file path
        bucket: S3 bucket name
        s3_prefix: S3 prefix/folder
        date: Forecast initialization date
        logger: Logger instance

    Returns:
        True if upload successful, False otherwise
    """
    try:
        s3_client = boto3.client('s3')

        # Organize by date: raw-grib2/2026/01/10/hrrr.t12z.f00.nc
        s3_key = f"{s3_prefix}/{date.year}/{date.month:02d}/{date.day:02d}/{local_path.name}"

        logger.info(f"Uploading to s3://{bucket}/{s3_key}")

        s3_client.upload_file(
            str(local_path),
            bucket,
            s3_key,
            ExtraArgs={
                'ServerSideEncryption': 'AES256',
                'Metadata': {
                    'forecast-date': date.strftime('%Y-%m-%d'),
                    'forecast-cycle': str(date.hour),
                    'model': DEFAULT_MODEL
                }
            }
        )

        logger.info(f"Successfully uploaded to S3")
        return True

    except ClientError as e:
        logger.error(f"Failed to upload to S3: {e}")
        return False


def generate_metadata(
    date: datetime,
    forecast_hours: List[int],
    variables: Optional[List[str]],
    bucket: str,
    s3_prefix: str
) -> dict:
    """
    Generate metadata for the downloaded forecast.

    Args:
        date: Forecast initialization datetime
        forecast_hours: List of forecast hours downloaded
        variables: List of variables downloaded
        bucket: S3 bucket name
        s3_prefix: S3 prefix

    Returns:
        Metadata dictionary
    """
    return {
        'model': DEFAULT_MODEL,
        'product': DEFAULT_PRODUCT,
        'initialization_time': date.strftime('%Y-%m-%d %H:%M UTC'),
        'initialization_timestamp': int(date.timestamp()),
        'forecast_hours': forecast_hours,
        'variables': variables if variables else 'all',
        'files': [
            {
                'forecast_hour': fxx,
                'valid_time': (date + timedelta(hours=fxx)).strftime('%Y-%m-%d %H:%M UTC'),
                's3_uri': f"s3://{bucket}/{s3_prefix}/{date.year}/{date.month:02d}/{date.day:02d}/hrrr.{date.strftime('%Y%m%d')}.t{date.hour:02d}z.f{fxx:02d}.nc"
            }
            for fxx in forecast_hours
        ],
        'download_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'),
        'download_timestamp': int(datetime.utcnow().timestamp())
    }


def main():
    """Main execution function."""
    args = parse_arguments()
    logger = setup_logging(args.verbose)

    logger.info("=" * 60)
    logger.info("HRRR Data Download Script")
    logger.info("=" * 60)

    # Determine forecast date/time
    if args.latest:
        forecast_date = calculate_latest_forecast_time()
        logger.info(f"Using latest forecast: {forecast_date.strftime('%Y-%m-%d %H:00 UTC')}")
    else:
        forecast_date = datetime.strptime(f"{args.date} {args.cycle:02d}", "%Y-%m-%d %H")
        logger.info(f"Using specified forecast: {forecast_date.strftime('%Y-%m-%d %H:00 UTC')}")

    # Parse forecast hours
    forecast_hours = parse_forecast_hours(args.fxx)
    logger.info(f"Forecast hours: {forecast_hours}")

    # Parse variables
    variables = parse_variables(args.variables)
    if variables:
        logger.info(f"Variables: {', '.join(variables)}")
    else:
        logger.info("Variables: all (full GRIB2)")

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {args.output_dir}")

    # Download data
    logger.info("=" * 60)
    logger.info("Starting downloads...")
    logger.info("=" * 60)

    downloaded_files = []
    successful_hours = []

    for fxx in forecast_hours:
        logger.info(f"\nDownloading forecast hour F{fxx:02d}...")

        file_path = download_hrrr_data(
            date=forecast_date,
            fxx=fxx,
            variables=variables,
            output_dir=args.output_dir,
            logger=logger,
            dry_run=args.dry_run
        )

        if file_path:
            downloaded_files.append(file_path)
            successful_hours.append(fxx)

            # Upload to S3 unless local-only or dry-run
            if not args.local_only and not args.dry_run:
                upload_success = upload_to_s3(
                    local_path=file_path,
                    bucket=args.s3_bucket,
                    s3_prefix=args.s3_prefix,
                    date=forecast_date,
                    logger=logger
                )

                # Delete local file unless --keep-local
                if upload_success and not args.keep_local:
                    logger.debug(f"Removing local file: {file_path}")
                    file_path.unlink()

    # Summary
    logger.info("=" * 60)
    logger.info("Download Summary")
    logger.info("=" * 60)
    logger.info(f"Requested: {len(forecast_hours)} forecast hours")
    logger.info(f"Successful: {len(successful_hours)} downloads")
    logger.info(f"Failed: {len(forecast_hours) - len(successful_hours)}")

    if successful_hours:
        logger.info(f"Downloaded hours: {successful_hours}")

    # Generate and save metadata
    if successful_hours and not args.dry_run:
        metadata = generate_metadata(
            date=forecast_date,
            forecast_hours=successful_hours,
            variables=variables,
            bucket=args.s3_bucket,
            s3_prefix=args.s3_prefix
        )

        metadata_file = args.output_dir / f"metadata_{forecast_date.strftime('%Y%m%d_%H')}z.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Metadata saved to: {metadata_file}")

    # Exit code
    if len(successful_hours) == len(forecast_hours):
        logger.info("All downloads completed successfully!")
        sys.exit(0)
    elif successful_hours:
        logger.warning("Some downloads failed")
        sys.exit(1)
    else:
        logger.error("All downloads failed")
        sys.exit(2)


if __name__ == '__main__':
    main()
