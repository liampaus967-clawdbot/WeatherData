#!/usr/bin/env python3
"""
Generate Metadata JSON for Weather Data Pipeline

Creates a latest.json file containing:
- Current model run information
- Available variables with display names and units
- Available forecast hours
- Tile URL templates for web app consumption
- Data freshness indicator

Part of TICKET-012: Create Metadata Generation Script
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('generate_metadata')


def load_variables_config(config_path: str) -> dict:
    """Load variables configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.warning(f"Could not load variables config: {e}")
        return {}


def get_available_variables(tiles_dir: str, config: dict) -> list:
    """
    Get list of available variables from tiles directory.

    Returns list of variable objects with metadata from config.
    """
    variables = []
    tiles_path = Path(tiles_dir)

    if not tiles_path.exists():
        logger.warning(f"Tiles directory does not exist: {tiles_dir}")
        return variables

    # Get variable definitions from config
    var_config = config.get('variables', {})
    color_ramps = config.get('color_ramps', {})

    # Scan tiles directory for variable folders
    for var_dir in sorted(tiles_path.iterdir()):
        if var_dir.is_dir() and not var_dir.name.startswith('.'):
            var_id = var_dir.name

            # Get config for this variable
            var_info = var_config.get(var_id, {})

            variable = {
                'id': var_id,
                'name': var_info.get('display_name', var_id.replace('_', ' ').title()),
                'description': var_info.get('description', ''),
                'units': var_info.get('units_display', ''),
                'color_ramp': var_info.get('color_ramp', 'default'),
            }

            # Add color ramp details if available
            ramp_name = variable['color_ramp']
            if ramp_name in color_ramps:
                ramp = color_ramps[ramp_name]
                variable['color_stops'] = ramp.get('colors', [])

            # Get available timestamps for this variable
            timestamps = []
            for ts_dir in sorted(var_dir.iterdir()):
                if ts_dir.is_dir() and not ts_dir.name.startswith('.'):
                    timestamps.append(ts_dir.name)

            if timestamps:
                variable['latest_timestamp'] = timestamps[-1]
                variable['timestamps'] = timestamps
                variables.append(variable)

    return variables


def get_available_runs(tiles_dir: str) -> list:
    """
    Scan tiles directory for all available model runs (timestamps).

    Returns list of run objects with timestamp and forecast hours.
    Structure: tiles/{variable}/{timestamp}/{forecast}/{z}/{x}/{y}.png
    """
    tiles_path = Path(tiles_dir)
    runs = {}  # {timestamp: set(forecast_hours)}

    if not tiles_path.exists():
        return []

    # Scan all variable directories
    for var_dir in tiles_path.iterdir():
        if not var_dir.is_dir() or var_dir.name.startswith('.'):
            continue

        # Scan all timestamp directories
        for ts_dir in var_dir.iterdir():
            if not ts_dir.is_dir() or ts_dir.name.startswith('.'):
                continue

            timestamp = ts_dir.name
            if timestamp not in runs:
                runs[timestamp] = set()

            # Scan forecast hour directories
            for fxx_dir in ts_dir.iterdir():
                if fxx_dir.is_dir() and fxx_dir.name.isdigit():
                    runs[timestamp].add(fxx_dir.name)

    # Convert to sorted list of run objects
    available_runs = []
    for timestamp in sorted(runs.keys(), reverse=True):  # Newest first
        forecast_hours = sorted(runs[timestamp])
        available_runs.append({
            'timestamp': timestamp,
            'forecast_hours': forecast_hours,
            'forecast_count': len(forecast_hours)
        })

    return available_runs


def get_forecast_hours(tiles_dir: str, variable: str = None) -> list:
    """Get available forecast hours from tiles directory."""
    forecast_hours = set()
    tiles_path = Path(tiles_dir)

    if not tiles_path.exists():
        return []

    # If variable specified, look in that folder
    if variable:
        var_path = tiles_path / variable
        if var_path.exists():
            for ts_dir in var_path.iterdir():
                if ts_dir.is_dir():
                    for fxx_dir in ts_dir.iterdir():
                        if fxx_dir.is_dir() and fxx_dir.name.isdigit():
                            forecast_hours.add(fxx_dir.name)
    else:
        # Look in all variable folders
        for var_dir in tiles_path.iterdir():
            if var_dir.is_dir():
                for ts_dir in var_dir.iterdir():
                    if ts_dir.is_dir():
                        for fxx_dir in ts_dir.iterdir():
                            if fxx_dir.is_dir() and fxx_dir.name.isdigit():
                                forecast_hours.add(fxx_dir.name)

    return sorted(list(forecast_hours))


def parse_model_run(model_date: str, model_cycle: str) -> dict:
    """Parse model run information into structured format."""
    try:
        # Parse date
        if '-' in model_date:
            date_obj = datetime.strptime(model_date, '%Y-%m-%d')
        else:
            date_obj = datetime.strptime(model_date, '%Y%m%d')

        # Clean cycle (remove 'z' or 'Z' if present)
        cycle = model_cycle.lower().replace('z', '').zfill(2)

        # Create timestamp
        timestamp = date_obj.replace(
            hour=int(cycle),
            minute=0,
            second=0,
            tzinfo=timezone.utc
        )

        return {
            'date': date_obj.strftime('%Y-%m-%d'),
            'cycle': cycle,
            'cycle_formatted': f"{cycle}Z",
            'timestamp': timestamp.isoformat(),
            'unix_timestamp': int(timestamp.timestamp()),
            'display': f"{date_obj.strftime('%Y-%m-%d')} {cycle}:00 UTC"
        }
    except Exception as e:
        logger.error(f"Error parsing model run: {e}")
        return {
            'date': model_date,
            'cycle': model_cycle,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }


def calculate_data_age(model_run: dict) -> int:
    """Calculate data age in minutes from model run time."""
    try:
        model_time = datetime.fromisoformat(model_run['timestamp'])
        now = datetime.now(timezone.utc)
        age_seconds = (now - model_time).total_seconds()
        return int(age_seconds / 60)
    except Exception:
        return -1


def generate_metadata(
    model_date: str,
    model_cycle: str,
    s3_bucket: str,
    tiles_dir: str,
    colored_cogs_dir: str = None,
    config_path: str = None,
    base_url: str = None,
    s3_prefix: str = None
) -> dict:
    """Generate complete metadata JSON."""

    # Load variables config
    config = {}
    if config_path and os.path.exists(config_path):
        config = load_variables_config(config_path)

    # Parse model run
    model_run = parse_model_run(model_date, model_cycle)

    # Get available variables
    variables = get_available_variables(tiles_dir, config)

    # Get forecast hours for current run
    forecast_hours = get_forecast_hours(tiles_dir)

    # Get all available model runs (for historical data)
    available_runs = get_available_runs(tiles_dir)

    # Calculate data age
    data_age_minutes = calculate_data_age(model_run)

    # Build base URL
    if not base_url:
        base_url = f"https://{s3_bucket}.s3.{os.environ.get('AWS_REGION', 'us-east-1')}.amazonaws.com"

    # Build tile URL template (use s3_prefix if provided, otherwise default to 'tiles')
    tiles_path = f"{s3_prefix}/tiles" if s3_prefix else "tiles"
    tile_url_template = f"{base_url}/{tiles_path}/{{variable}}/{{timestamp}}/{{forecast}}/{{z}}/{{x}}/{{y}}.png"

    # Generate metadata
    metadata = {
        'version': '1.0',
        'model': config.get('model', 'hrrr'),
        'product': config.get('product', 'sfc'),

        'model_run': model_run,

        'data_freshness': {
            'age_minutes': data_age_minutes,
            'status': 'fresh' if data_age_minutes < 120 else 'stale' if data_age_minutes < 240 else 'old',
            'generated_at': datetime.now(timezone.utc).isoformat(),
        },

        'variables': variables,
        'variable_ids': [v['id'] for v in variables],

        'forecast_hours': forecast_hours,

        # Historical model runs (for animation/time navigation)
        'available_runs': available_runs,
        'available_runs_count': len(available_runs),

        'tiles': {
            'url_template': tile_url_template,
            'format': 'png',
            'tile_size': 256,
            'min_zoom': 0,
            'max_zoom': 8,
            'bounds': [-134.12, 21.14, -60.88, 52.62],  # CONUS bounds
        },

        'endpoints': {
            'metadata': f"{base_url}/{s3_prefix + '/metadata' if s3_prefix else 'metadata'}/latest.json",
            'tiles': f"{base_url}/{tiles_path}/",
            'colored_cogs': f"{base_url}/{s3_prefix + '/colored-cogs' if s3_prefix else 'colored-cogs'}/",
        },

        'generated_at': datetime.now(timezone.utc).isoformat(),
        'pipeline_version': '1.0',
    }

    return metadata


def save_metadata(metadata: dict, output_path: str) -> bool:
    """Save metadata to JSON file."""
    try:
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        with open(output_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"Metadata saved to: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save metadata: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Generate metadata JSON for weather data pipeline'
    )
    parser.add_argument(
        '--date', '-d',
        required=True,
        help='Model run date (YYYY-MM-DD or YYYYMMDD)'
    )
    parser.add_argument(
        '--cycle', '-c',
        required=True,
        help='Model cycle hour (e.g., 18 or 18z)'
    )
    parser.add_argument(
        '--s3-bucket', '-b',
        required=True,
        help='S3 bucket name'
    )
    parser.add_argument(
        '--tiles-dir', '-t',
        required=True,
        help='Local tiles directory to scan'
    )
    parser.add_argument(
        '--config', '-f',
        help='Path to variables.yaml config file'
    )
    parser.add_argument(
        '--output', '-o',
        required=True,
        help='Output path for metadata JSON'
    )
    parser.add_argument(
        '--base-url',
        help='Override base URL for tile endpoints'
    )
    parser.add_argument(
        '--s3-prefix',
        help='S3 prefix for model-specific paths (e.g., "gfs-wave" for gfs-wave/tiles/)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("=" * 60)
    logger.info("Generating Weather Data Metadata")
    logger.info("=" * 60)
    logger.info(f"Model run: {args.date} {args.cycle}Z")
    logger.info(f"S3 bucket: {args.s3_bucket}")
    logger.info(f"Tiles directory: {args.tiles_dir}")

    # Generate metadata
    metadata = generate_metadata(
        model_date=args.date,
        model_cycle=args.cycle,
        s3_bucket=args.s3_bucket,
        tiles_dir=args.tiles_dir,
        config_path=args.config,
        base_url=args.base_url,
        s3_prefix=args.s3_prefix
    )

    # Log summary
    logger.info(f"Variables found: {len(metadata['variables'])}")
    logger.info(f"Forecast hours: {metadata['forecast_hours']}")
    logger.info(f"Available model runs: {metadata['available_runs_count']}")
    logger.info(f"Data age: {metadata['data_freshness']['age_minutes']} minutes")

    # Save metadata
    if save_metadata(metadata, args.output):
        logger.info("=" * 60)
        logger.info("Metadata generation complete!")
        logger.info("=" * 60)

        # Print JSON for debugging
        if args.verbose:
            print(json.dumps(metadata, indent=2))

        return 0
    else:
        return 1


if __name__ == '__main__':
    sys.exit(main())
