# Color Ramp Application for Weather Data

Apply color ramps to grayscale Cloud Optimized GeoTIFFs (COGs) for visualization.

Part of **TICKET-007: Add Color Ramp and Visualization Styling**

## Overview

The `apply_colormap.py` script converts grayscale weather data COGs into RGB images with color ramps applied, making them suitable for web map visualization.

### What It Does

1. **Reads configuration** from `config/variables.yaml`
2. **Extracts color ramp** definitions for each variable
3. **Converts to GDAL format** (color-relief text files)
4. **Applies color ramps** using `gdaldem color-relief`
5. **Creates RGB GeoTIFFs** with compression and overviews
6. **Adds transparency** (alpha channel) for no-data values

### Input/Output

| Input | Output |
|-------|--------|
| Grayscale COG (single-band, Float32) | RGB GeoTIFF (4-band RGBA, Byte) |
| 15-17 MB per file | 2-3 MB per file (dense data) |
| Raw data values | Color-mapped visualization |

## Quick Start

### Process Single File

```bash
python3 scripts/processing/apply_colormap.py \
  --input /tmp/processed-weather/temperature_2m_hrrr.20260110.t21z.f00.tif \
  --output /tmp/colored-weather
```

### Process All Files in Directory

```bash
python3 scripts/processing/apply_colormap.py \
  --input /tmp/processed-weather \
  --output /tmp/colored-weather
```

### Process Specific Variable

```bash
python3 scripts/processing/apply_colormap.py \
  --input /tmp/processed-weather \
  --output /tmp/colored-weather \
  --variable temperature_2m
```

### Using Docker

```bash
docker run --rm \
  -v /tmp/processed-weather:/data/input \
  -v /tmp/colored-weather:/data/output \
  -v $(pwd):/app \
  weather-processor:latest \
  python3 /app/scripts/processing/apply_colormap.py \
  --input /data/input \
  --output /data/output
```

## Command-Line Interface

### Usage

```
apply_colormap.py --input PATH [OPTIONS]
```

### Arguments

| Argument | Short | Required | Description |
|----------|-------|----------|-------------|
| `--input` | `-i` | Yes | Input COG file or directory |
| `--output` | `-o` | No | Output directory (default: same as input) |
| `--variable` | `-v` | No | Variable name (auto-detected if not specified) |
| `--config` | `-c` | No | Path to variables.yaml config file |
| `--verbose` | | No | Enable verbose logging |

### Examples

#### Auto-detect Variable from Filename

```bash
python3 scripts/processing/apply_colormap.py \
  --input temperature_2m_hrrr.20260110.t19z.f00.tif
```

The script infers `temperature_2m` from the filename pattern.

#### Explicit Variable Name

```bash
python3 scripts/processing/apply_colormap.py \
  --input temp.tif \
  --variable temperature_2m
```

#### Process with Custom Config

```bash
python3 scripts/processing/apply_colormap.py \
  --input data/ \
  --config custom_variables.yaml
```

#### Verbose Logging

```bash
python3 scripts/processing/apply_colormap.py \
  --input data/ \
  --verbose
```

## Color Ramp Configuration

Color ramps are defined in `config/variables.yaml` under the `color_ramps` section.

### Format

```yaml
color_ramps:
  temperature:
    type: "gradient"
    colors:
      - value: -40
        color: "#1a0066"  # Deep purple
      - value: 0
        color: "#00ff00"  # Green
      - value: 50
        color: "#ff0000"  # Red
```

### Color Ramp Types

| Type | Description | Use Case |
|------|-------------|----------|
| `gradient` | Sequential color scale | Temperature, precipitation, speed |
| `diverging` | Two-way scale from center | Wind components (U/V) |

### Predefined Color Ramps

| Name | Range | Colors | Use |
|------|-------|--------|-----|
| `temperature` | -40°C to 50°C | Purple → Blue → Green → Yellow → Red | Temperature fields |
| `dewpoint` | -40°C to 40°C | Brown → Gold → Turquoise → Blue | Dewpoint temperature |
| `wind_speed` | 0 to 100 mph | White → Blue → Orange → Red → Purple | Wind speed, gusts |
| `wind_component` | -50 to 50 m/s | Blue ← White → Red | U/V wind components |
| `humidity` | 0% to 100% | Brown → Gold → Green → Blue | Relative humidity |
| `precipitation` | 0 to 10 in | White → Cyan → Blue → Magenta | Accumulated precip |
| `reflectivity` | -20 to 80 dBZ | White → Cyan → Blue → Green → Yellow → Red → White | Radar reflectivity |
| `cloud_cover` | 0% to 100% | Sky Blue → Gray | Cloud coverage |
| `cape` | 0 to 5000 J/kg | White → Yellow → Orange → Red | Convective energy |

### Creating Custom Color Ramps

1. Add new entry to `color_ramps` in `variables.yaml`:

```yaml
my_custom_ramp:
  type: "gradient"
  colors:
    - value: 0
      color: "#FFFFFF"  # White
    - value: 50
      color: "#FF0000"  # Red
    - value: 100
      color: "#0000FF"  # Blue
```

2. Reference it in a variable:

```yaml
variables:
  my_variable:
    color_ramp: "my_custom_ramp"
```

## Output Specifications

### File Format

- **Format**: GeoTIFF
- **Bands**: 4 (Red, Green, Blue, Alpha)
- **Data Type**: Byte (8-bit unsigned)
- **Projection**: EPSG:3857 (Web Mercator)
- **Compression**: DEFLATE with PREDICTOR=2
- **Tiling**: 512×512 blocks
- **Overviews**: 4 levels (2×, 4×, 8×, 16×)

### File Naming

```
{variable}_{model}.{date}.{cycle}.{forecast}_colored.tif
```

**Examples:**
- `temperature_2m_hrrr.20260110.t19z.f00_colored.tif`
- `reflectivity_composite_hrrr.20260110.t19z.f00_colored.tif`

### File Sizes

| Variable | Grayscale | RGB | Reduction |
|----------|-----------|-----|-----------|
| Temperature (2m) | 15.5 MB | 2.1 MB | 86% |
| Wind U/V (10m) | 17.0 MB | 2.3 MB | 86% |
| Reflectivity | 3.5 MB | 1.0 MB | 71% |
| Precipitation | 0.05 MB | 0.05 MB | 0% |

**Why RGB is smaller:**
- Grayscale: Float32 (4 bytes/pixel)
- RGB: Byte (1 byte/pixel × 4 bands = 4 bytes/pixel)
- Better compression with PREDICTOR=2 on byte data

## Processing Pipeline

### Data Flow

```
Input: Grayscale COG
  ↓
Load Variable Config
  ↓
Get Color Ramp Definition
  ↓
Create GDAL Color-Relief File
  ↓
Apply gdaldem color-relief
  ↓
Add Alpha Channel (transparency)
  ↓
Generate Overviews
  ↓
Output: RGB GeoTIFF
```

### GDAL Color-Relief Format

The script converts YAML color ramps to GDAL color-relief text format:

**YAML:**
```yaml
- value: -40
  color: "#1a0066"
- value: 0
  color: "#00ff00"
```

**GDAL Text File:**
```
-40 26 0 102
0 0 255 0
nv 0 0 0 0
```

Format: `value red green blue [alpha]`

### gdaldem Command

```bash
gdaldem color-relief \
  input.tif \
  color_ramp.txt \
  output.tif \
  -alpha \
  -co COMPRESS=DEFLATE \
  -co PREDICTOR=2 \
  -co ZLEVEL=6 \
  -co TILED=YES \
  -co BLOCKXSIZE=512 \
  -co BLOCKYSIZE=512 \
  -co NUM_THREADS=ALL_CPUS
```

## Performance

### Single File

- **Processing Time**: ~1-2 seconds per file
- **CPU Usage**: 1 core (gdaldem is single-threaded for color-relief)
- **Memory**: ~100-200 MB peak

### Batch Processing (5 files)

```
Total files processed: 5
Successful: 5
Failed: 0
Total time: ~5-10 seconds
```

### Bottlenecks

1. **gdaldem is single-threaded** for color-relief
2. **I/O bound** for large files
3. **Overview generation** adds ~20% time

### Optimization Tips

- Process multiple files in parallel using shell scripting
- Use faster storage (SSD) for temp files
- Pre-generate color-relief files to avoid repeated parsing

## Integration with Pipeline

### Current Workflow

```bash
# Step 1: Download GRIB2 data
python3 scripts/hrrr/download_hrrr.py --latest --fxx 0

# Step 2: Process to grayscale COGs (TICKET-006)
python3 scripts/processing/process_weather.py \
  --input /tmp/weather-data/hrrr.*.grib2 \
  --output /tmp/processed-weather \
  --priority 1

# Step 3: Apply color ramps (TICKET-007 - THIS STEP)
python3 scripts/processing/apply_colormap.py \
  --input /tmp/processed-weather \
  --output /tmp/colored-weather

# Step 4: Upload to S3 or generate tiles
# (Future tickets)
```

### Automation Script Example

```bash
#!/bin/bash
# colorize_batch.sh

INPUT_DIR="/tmp/processed-weather"
OUTPUT_DIR="/tmp/colored-weather"

# Process all COG files
python3 scripts/processing/apply_colormap.py \
  --input "$INPUT_DIR" \
  --output "$OUTPUT_DIR"

# Upload to S3 (optional)
aws s3 sync "$OUTPUT_DIR" s3://your-bucket/colored-weather/
```

## Validation

### Verify Output Quality

```bash
# Check file properties
gdalinfo temperature_2m_*_colored.tif

# Expected output:
# - Driver: GTiff/GeoTIFF
# - Size: 2181x1206
# - Bands: 4 (Red, Green, Blue, Alpha)
# - Type: Byte
# - Compression: DEFLATE
# - Overviews: 4 levels
```

### Visual Inspection

```bash
# Open in QGIS
qgis temperature_2m_*_colored.tif

# Or convert to PNG for preview
gdal_translate \
  -of PNG \
  -outsize 800 0 \
  temperature_2m_*_colored.tif \
  preview.png
```

### Check Color Accuracy

```bash
# Get pixel values at specific locations
gdallocationinfo \
  -valonly \
  temperature_2m_*_colored.tif \
  1000 600

# Compare to original grayscale values
gdallocationinfo \
  -valonly \
  temperature_2m_*.tif \
  1000 600
```

## Troubleshooting

### Common Issues

#### "Variable not found in configuration"

**Cause**: Filename doesn't match variable naming pattern, or explicit variable is misspelled.

**Solution**:
```bash
# Use explicit variable name
python3 scripts/processing/apply_colormap.py \
  --input file.tif \
  --variable temperature_2m
```

#### "Color ramp not found"

**Cause**: Variable references undefined color ramp in config.

**Solution**: Check `config/variables.yaml` and ensure color ramp exists:
```bash
# Validate configuration
python3 config/config_manager.py --validate
```

#### "gdaldem command failed"

**Cause**: GDAL not installed or color-relief file syntax error.

**Solution**:
```bash
# Check GDAL installation
gdaldem --version

# Test color-relief manually
gdaldem color-relief input.tif colors.txt output.tif -alpha
```

#### "Invalid cross-device link"

**Cause**: Trying to move files across different filesystems (already fixed in code).

**Solution**: Use `shutil.move()` instead of `Path.rename()`.

#### File Size Larger Than Expected

**Cause**: Overviews not compressed or wrong compression settings.

**Solution**: Verify compression settings in gdaldem command:
```bash
-co COMPRESS=DEFLATE -co PREDICTOR=2 -co ZLEVEL=6
```

## Advanced Usage

### Custom Color Ramp from File

```python
from pathlib import Path
import yaml

# Load custom color ramp
with open('my_colors.yaml', 'r') as f:
    custom_config = yaml.safe_load(f)

# Use with script
python3 scripts/processing/apply_colormap.py \
  --config my_colors.yaml \
  --input data/
```

### Programmatic Usage

```python
from apply_colormap import process_cog_file, VariableConfig
from pathlib import Path
import logging

# Setup
logger = logging.getLogger(__name__)
config = VariableConfig(Path('config/variables.yaml'))

# Process single file
output = process_cog_file(
    input_cog=Path('temperature.tif'),
    variable_name='temperature_2m',
    config=config,
    output_dir=Path('output/'),
    logger=logger
)

print(f"Created: {output}")
```

### Batch Processing with GNU Parallel

```bash
# Process files in parallel
find /tmp/processed-weather -name "*.tif" -type f | \
  parallel -j 4 \
    python3 scripts/processing/apply_colormap.py \
      --input {} \
      --output /tmp/colored-weather
```

## Examples

### Example 1: Temperature

**Input**: Grayscale COG with temperature in Celsius (-40 to 50°C)

**Color Ramp**: Purple (cold) → Green (freezing) → Red (hot)

**Output**: RGB image where:
- -40°C = Deep purple `#1a0066`
- 0°C = Green `#00ff00`
- 20°C = Yellow `#ffff00`
- 50°C = Red `#ff0000`

### Example 2: Radar Reflectivity

**Input**: Grayscale COG with dBZ values (-20 to 80)

**Color Ramp**: Standard radar colors

**Output**: RGB image matching weather radar displays:
- 20 dBZ = Blue (light rain)
- 40 dBZ = Yellow (heavy rain)
- 60 dBZ = Magenta (severe)

### Example 3: Wind Components

**Input**: Grayscale COG with U/V wind in m/s (-50 to 50)

**Color Ramp**: Diverging scale

**Output**: RGB image where:
- -50 m/s = Blue (westward/southward)
- 0 m/s = White (calm)
- +50 m/s = Red (eastward/northward)

## Next Steps

After applying color ramps:

1. **TICKET-008**: Generate web tiles from colored COGs
2. **TICKET-010**: Integrate into automated pipeline
3. **TICKET-013**: Display in Mapbox web application

## References

- **GDAL Color Relief**: https://gdal.org/programs/gdaldem.html#color-relief
- **Color Brewer**: https://colorbrewer2.org/ (color scale inspiration)
- **COG Specification**: https://cogeo.org/

## Support

For issues or questions:
- Check this README
- Validate config: `python3 config/config_manager.py --validate`
- Run with `--verbose` flag for detailed logging
- Review GDAL logs for gdaldem errors

---

**Created**: 2026-01-11
**Part of**: TICKET-007 Color Ramp Application
**Script**: `scripts/processing/apply_colormap.py`
**Dependencies**: GDAL, Python 3.10+, variables.yaml
