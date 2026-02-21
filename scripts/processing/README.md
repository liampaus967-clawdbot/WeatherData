# Data Processing Scripts

Scripts for processing HRRR GRIB2 files into Cloud Optimized GeoTIFFs (COGs).

## Purpose

Process downloaded HRRR GRIB2 files into web-friendly formats:
- Extract variables from GRIB2 using GDAL
- Apply unit conversions (Kelvin to Celsius, etc.)
- Reproject to Web Mercator (EPSG:3857)
- Create Cloud Optimized GeoTIFFs with compression and overviews

## Scripts

### process_weather.py

Main processing script that converts GRIB2 files to COGs.

**Features:**
- Extracts variables from GRIB2 using GDAL (no cfgrib segfault issues)
- Automatic unit conversion detection (skips if already converted)
- Reprojection to EPSG:3857 with fallback to GDAL
- COG creation with DEFLATE compression and overviews
- Priority-based processing
- Configuration-driven (uses `config/variables.yaml`)

**Requirements:**
- GDAL 3.x with GRIB2 support
- rioxarray
- xarray
- numpy

## Quick Start

### List Available Bands in GRIB2

```bash
python scripts/processing/process_weather.py \
  --input /tmp/weather-data/hrrr.20260110.t21z.f00.grib2 \
  --list-bands
```

Output:
```
Band   Element         Short Name      Description
================================================================================
1      REFC            0-EATM          0[-] EATM="Entire Atmosphere"
71     TMP             2-HTGL          2[m] HTGL="Specified height level above ground"
77     UGRD            10-HTGL         10[m] HTGL="Specified height level above ground"
...
```

### Process All Enabled Variables

```bash
python scripts/processing/process_weather.py \
  --input /tmp/weather-data/hrrr.20260110.t21z.f00.grib2 \
  --output /tmp/processed-weather/
```

This processes all variables with `enabled: true` in `config/variables.yaml`.

### Process Only Priority 1 Variables

```bash
python scripts/processing/process_weather.py \
  --input /tmp/weather-data/hrrr.20260110.t21z.f00.grib2 \
  --output /tmp/processed-weather/ \
  --priority 1
```

Priority 1 variables (critical):
- temperature_2m
- wind_u_10m
- wind_v_10m
- precipitation_accumulated
- reflectivity_composite

### Process Specific Variables

```bash
python scripts/processing/process_weather.py \
  --input /tmp/weather-data/hrrr.20260110.t21z.f00.grib2 \
  --output /tmp/processed-weather/ \
  --variables temperature_2m wind_u_10m wind_v_10m
```

## Usage

```
python scripts/processing/process_weather.py [OPTIONS]

Required:
  --input, -i PATH          Input GRIB2 file

Optional:
  --output, -o PATH         Output directory for COG files
  --config, -c PATH         Path to variables.yaml (default: config/variables.yaml)
  --priority {1,2,3}        Process only variables with this priority
  --variables VAR [VAR ...]  Specific variables to process
  --list-bands              List all bands in GRIB2 file and exit
  --verbose, -v             Enable debug logging
```

## Output Format

### File Naming

Output files are named: `{variable_name}_{grib_filename}.tif`

Example: `temperature_2m_hrrr.20260110.t21z.f00.tif`

### COG Specifications

- **Format**: Cloud Optimized GeoTIFF (COG)
- **Projection**: EPSG:3857 (Web Mercator)
- **Compression**: DEFLATE (level 6)
- **Tile Size**: 512x512
- **Overviews**: Yes (2x, 4x, 8x, 16x)
- **Resolution**: ~3km per pixel (reprojected from ~3km native HRRR grid)

### Metadata Preserved

GeoTIFFs retain GRIB metadata:
- GRIB_ELEMENT (e.g., "TMP", "UGRD")
- GRIB_UNIT (e.g., "[C]", "[m/s]")
- GRIB_COMMENT (e.g., "Temperature [C]")
- GRIB_REF_TIME (forecast initialization time)
- GRIB_VALID_TIME (valid time)

## Examples

### Full Workflow: Download → Process

```bash
# 1. Download HRRR GRIB2
python scripts/hrrr/download_hrrr.py --latest --fxx 0 --variables all

# 2. Process priority 1 variables
python scripts/processing/process_weather.py \
  --input /tmp/weather-data/hrrr.20260110.t21z.f00.grib2 \
  --output /tmp/processed-weather/ \
  --priority 1

# 3. Check results
ls -lh /tmp/processed-weather/
```

### Docker Usage (Production)

```bash
docker run --rm \
  -v /tmp/weather-data:/tmp/weather-data \
  -v /tmp/processed-weather:/tmp/processed-weather \
  -v /home/ubuntu/weather-pipeline/WeatherData:/app \
  weather-processor:latest \
  python3 /app/scripts/processing/process_weather.py \
    --input /tmp/weather-data/hrrr.20260110.t21z.f00.grib2 \
    --output /tmp/processed-weather/ \
    --priority 1
```

### Batch Processing Multiple Forecast Hours

```bash
# Process all forecast hours in directory
for grib in /tmp/weather-data/hrrr.*.grib2; do
  basename=$(basename "$grib" .grib2)
  echo "Processing $basename..."
  python scripts/processing/process_weather.py \
    --input "$grib" \
    --output "/tmp/processed-weather/$basename/" \
    --priority 1
done
```

## Performance

### Processing Times (local development)

Single variable (temperature_2m):
- Extract from GRIB2: ~0.5s
- Reproject to EPSG:3857: ~1-2s
- Create COG: ~0.5s
- **Total**: ~2-3 seconds

All priority 1 variables (5 variables):
- **Total**: ~10-15 seconds

### File Sizes

| Variable | Input (GRIB2) | Output (COG) | Compression Ratio |
|----------|---------------|--------------|-------------------|
| temperature_2m | ~67 MB (full file) | ~16 MB | ~4:1 |
| wind_u_10m | ~67 MB (full file) | ~17 MB | ~4:1 |
| wind_v_10m | ~67 MB (full file) | ~17 MB | ~4:1 |
| precipitation | ~67 MB (full file) | ~55 KB | ~1000:1 (sparse) |
| reflectivity | ~67 MB (full file) | ~3.5 MB | ~19:1 |

**Note**: Input sizes show full GRIB2 file size. Each variable occupies only a portion of the file.

## Variable Configuration

Variables are configured in `config/variables.yaml`:

```yaml
temperature_2m:
  grib_search: "TMP:2 m"              # GRIB search pattern
  display_name: "Temperature (2m)"    # Human-readable name
  description: "Air temperature at 2 meters above ground"
  units_source: "K"                   # Units in GRIB2 file
  units_display: "°C"                 # Units to convert to
  conversion: "kelvin_to_celsius"     # Conversion formula name
  typical_range: [-40, 50]            # Expected min/max values
  color_ramp: "temperature"           # Color scheme for visualization
  priority: 1                         # Processing priority (1=highest)
  enabled: true                       # Whether to process
```

## Troubleshooting

### Band Not Found

**Error**: `No band found matching 'TMP:2 m'`

**Solution**: List bands to find correct search pattern:
```bash
python scripts/processing/process_weather.py --input file.grib2 --list-bands | grep TMP
```

### Unit Conversion Issues

The script automatically detects if GRIB data is already in target units:

```
INFO - GRIB source units: [C]
INFO - Data already in Celsius, skipping K→C conversion
```

If units are incorrect, check `config/variables.yaml` and ensure `units_source` matches GRIB metadata.

### PROJ Database Errors

**Error**: `PROJ: internal_proj_create_from_database: ... DATABASE.LAYOUT.VERSION.MINOR = 2`

**Impact**: Warning only, processing continues using GDAL fallback.

**Solution**: This is a local development environment issue. In Docker with consistent PROJ/GDAL versions, this warning doesn't appear.

### Memory Issues

For large GRIB2 files or many variables, processing may require significant RAM.

**Solution**: Process fewer variables at once:
```bash
# Process one priority level at a time
python scripts/processing/process_weather.py --input file.grib2 --output out/ --priority 1
python scripts/processing/process_weather.py --input file.grib2 --output out/ --priority 2
```

## Integration with Pipeline

### TICKET-004: Download

Downloads GRIB2 files using Herbie:
```bash
python scripts/hrrr/download_hrrr.py --latest --variables all
```

### TICKET-006: Processing (This Script)

Processes GRIB2 to COG:
```bash
python scripts/processing/process_weather.py --input grib2 --output cogs/ --priority 1
```

### TICKET-007: Color Ramps (Next)

Applies color ramps to COGs for visualization:
```bash
python scripts/processing/apply_colormap.py --input cogs/ --config config/variables.yaml
```

### TICKET-008: Tile Generation (Future)

Generates web tiles from COGs:
```bash
python scripts/processing/generate_tiles.py --input cogs/ --output tiles/
```

## Known Limitations

1. **Full GRIB2 Required**: Must download entire GRIB2 file (cannot use variable-specific downloads due to cfgrib segfault in Docker). Variables are extracted during processing.

2. **Memory Usage**: Each variable loads full grid into memory during processing (~1059x1799 = 1.9M cells). For very large grids, this may require chunking.

3. **Single Forecast Time**: Processes one forecast hour at a time. For multiple hours, loop over files.

4. **Local PROJ Issues**: Local development may show PROJ database warnings. These are harmless and don't affect output.

## Development

### Adding New Variables

1. Add to `config/variables.yaml`:
   ```yaml
   my_new_variable:
     grib_search: "ELEMENT:level"
     display_name: "My Variable"
     # ... other config ...
     enabled: true
   ```

2. Test processing:
   ```bash
   python scripts/processing/process_weather.py \
     --input test.grib2 \
     --output out/ \
     --variables my_new_variable \
     --verbose
   ```

3. Validate output:
   ```bash
   gdalinfo out/my_new_variable_*.tif
   ```

### Running Tests

```bash
# Test with sample GRIB2 file
python scripts/processing/process_weather.py \
  --input /tmp/weather-data/hrrr.20260110.t21z.f00.grib2 \
  --output /tmp/test-processing/ \
  --priority 1 \
  --verbose
```

---

**Created**: 2026-01-10
**Part of**: TICKET-006 (Data Processing with GDAL/rioxarray)
**Next**: TICKET-007 (Color Ramp Application)
