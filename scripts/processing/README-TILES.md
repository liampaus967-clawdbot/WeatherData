# Web Map Tile Generation for Weather Data

Generate XYZ tile pyramids from colored Cloud Optimized GeoTIFFs for web map display.

Part of **TICKET-008: Implement Tile Generation Strategy**

## Overview

The `generate_tiles.py` script creates web map tiles from colored weather COG files using gdal2tiles.py. Tiles are organized in standard XYZ format compatible with Mapbox, Leaflet, OpenLayers, and other web mapping libraries.

### What It Does

1. **Reads colored COG files** (output from TICKET-007)
2. **Fixes SRS issues** automatically (gdaldem color-relief artifacts)
3. **Generates XYZ tiles** using gdal2tiles.py
4. **Organizes tiles** by variable/timestamp/forecast structure
5. **Creates PNG tiles** with transparency
6. **Parallel processing** for performance
7. **Excludes empty tiles** to save storage

### Input/Output

| Input | Output |
|-------|--------|
| Colored RGB COG (4-band RGBA, 2-3 MB) | PNG tile pyramid (256×256 px) |
| Single GeoTIFF file | 2,203 tiles (zoom 0-8) |
| 5 variables | 11,015 tiles total (~79 MB) |

## Quick Start

### Generate Tiles from Single File

```bash
python3 scripts/processing/generate_tiles.py \
  --input /tmp/colored-weather/temperature_2m_*_colored.tif \
  --output /tmp/tiles \
  --zoom 0-10 \
  --processes 4 \
  --exclude-transparent \
  --organize
```

### Batch Process All Variables

```bash
python3 scripts/processing/generate_tiles.py \
  --input /tmp/colored-weather \
  --output /tmp/tiles \
  --zoom 0-8 \
  --processes 4 \
  --exclude-transparent \
  --organize
```

### Using Docker

```bash
docker run --rm \
  -v /tmp/colored-weather:/data/input \
  -v /tmp/tiles:/data/output \
  -v $(pwd):/app \
  weather-processor:latest \
  python3 /app/scripts/processing/generate_tiles.py \
  --input /data/input \
  --output /data/output \
  --zoom 0-8 \
  --processes 4 \
  --exclude-transparent \
  --organize
```

## Command-Line Interface

### Usage

```
generate_tiles.py --input PATH --output PATH [OPTIONS]
```

### Arguments

| Argument | Short | Required | Description |
|----------|-------|----------|-------------|
| `--input` | `-i` | Yes | Input colored COG file or directory |
| `--output` | `-o` | Yes | Output directory for tiles |
| `--zoom` | `-z` | No | Zoom level range (e.g., "0-10", "5-8") (default: 0-10) |
| `--processes` | `-p` | No | Number of parallel processes (default: 4) |
| `--exclude-transparent` | `-x` | No | Exclude fully transparent tiles |
| `--resume` | `-r` | No | Resume mode (only generate missing tiles) |
| `--organize` | | No | Organize tiles by variable/timestamp/forecast |
| `--verbose` | `-v` | No | Enable verbose logging |

### Examples

#### Limited Zoom Range

```bash
python3 scripts/processing/generate_tiles.py \
  --input data/ \
  --output tiles/ \
  --zoom 0-6
```

Generates fewer tiles (181 tiles for zoom 0-6 vs 2,203 for 0-8).

#### High Performance

```bash
python3 scripts/processing/generate_tiles.py \
  --input data/ \
  --output tiles/ \
  --processes 8
```

Uses 8 CPU cores for faster generation.

#### Resume Interrupted Generation

```bash
python3 scripts/processing/generate_tiles.py \
  --input data/ \
  --output tiles/ \
  --resume
```

Only generates missing tiles if generation was interrupted.

## Directory Structure

### Organized Mode (`--organize`)

```
{output}/
├── temperature_2m/
│   └── 20260110T21z/
│       └── 00/              # Forecast hour
│           ├── 0/           # Zoom level
│           │   └── 0/       # X coordinate
│           │       └── 0.png    # Y coordinate
│           ├── 1/
│           │   └── 0/
│           │       └── 0.png
│           └── 8/
│               ├── 45/
│               │   ├── 88.png
│               │   └── 89.png
│               └── 46/
│                   └── 88.png
├── wind_u_10m/
│   └── 20260110T21z/
│       └── 00/
│           └── ...
└── reflectivity_composite/
    └── 20260110T21z/
        └── 00/
            └── ...
```

### Non-Organized Mode

```
{output}/
├── temperature_2m_hrrr.20260110.t21z.f00_colored/
│   ├── 0/
│   │   └── 0/
│   │       └── 0.png
│   └── 8/
│       └── ...
└── wind_u_10m_hrrr.20260110.t21z.f00_colored/
    └── ...
```

## Tile URL Format

### For Organized Structure

```
https://yourdomain.com/tiles/{variable}/{date}T{cycle}/{forecast}/{z}/{x}/{y}.png
```

**Example:**
```
https://tiles.example.com/temperature_2m/20260110T21z/00/8/45/88.png
```

### Components

| Component | Example | Description |
|-----------|---------|-------------|
| `variable` | `temperature_2m` | Weather variable name |
| `date` | `20260110` | Model run date (YYYYMMDD) |
| `cycle` | `21z` | Model cycle hour |
| `forecast` | `00` | Forecast hour (f00, f01, etc.) |
| `z` | `8` | Zoom level (0-10) |
| `x` | `45` | Tile X coordinate |
| `y` | `88` | Tile Y coordinate |

## Performance

### Single Variable (Zoom 0-8)

- **Tiles Generated**: 2,203
- **Processing Time**: ~6-7 seconds
- **Storage**: 8-19 MB (depending on data density)
- **Processes**: 4 cores

### Batch Processing (5 Variables, Zoom 0-8)

- **Total Tiles**: 11,015
- **Total Time**: ~32 seconds
- **Total Storage**: ~79 MB
- **Throughput**: ~345 tiles/second

### Performance by Zoom Level

| Zoom | Tiles | Coverage | Avg Size |
|------|-------|----------|----------|
| 0 | 1 | Global | 10 KB |
| 1 | 1 | Hemisphere | 15 KB |
| 2 | 2 | Continent | 20 KB |
| 3 | 4 | Region | 25 KB |
| 4 | 12 | State | 30 KB |
| 5 | 35 | County | 35 KB |
| 6 | 126 | City | 40 KB |
| 7 | 432 | Neighborhood | 45 KB |
| 8 | 1,590 | Street | 50 KB |

**Note**: Higher zoom levels generate more tiles with smaller data per tile.

## Technical Details

### SRS Fixing

The script automatically detects and fixes spatial reference system issues:

```python
# gdaldem color-relief sometimes creates invalid SRS
# Script detects "Unknown engineering datum" and fixes to EPSG:3857
if "Unknown engineering datum" in srs_wkt:
    gdal.Translate(temp_file, input_cog, outputSRS='EPSG:3857')
```

**Why needed**: gdal2tiles.py requires proper EPSG:3857 definition.

### gdal2tiles.py Parameters

```bash
gdal2tiles.py \
  --profile=mercator \      # Web Mercator (EPSG:3857)
  --xyz \                   # XYZ tile numbering (not TMS)
  --zoom=0-8 \              # Zoom level range
  --resampling=average \    # Good for downsampling
  --processes=4 \           # Parallel processing
  --tilesize=256 \          # Standard tile size
  --tiledriver=PNG \        # PNG format
  --webviewer=none \        # Don't generate HTML viewer
  --exclude                 # Exclude transparent tiles
```

### Tile Format

- **Size**: 256×256 pixels (standard)
- **Format**: PNG with transparency
- **Color**: RGB + Alpha channel
- **Projection**: Web Mercator (EPSG:3857)
- **Naming**: XYZ (OSM Slippy Map standard)

## Integration with Pipeline

### Complete Processing Workflow

```bash
# 1. Download GRIB2 data (TICKET-004)
python3 scripts/hrrr/download_hrrr.py --latest --fxx 0

# 2. Process to grayscale COGs (TICKET-006)
python3 scripts/processing/process_weather.py \
  --input /tmp/weather-data/*.grib2 \
  --output /tmp/processed-weather \
  --priority 1

# 3. Apply color ramps (TICKET-007)
python3 scripts/processing/apply_colormap.py \
  --input /tmp/processed-weather \
  --output /tmp/colored-weather

# 4. Generate tiles (TICKET-008 - THIS SCRIPT)
python3 scripts/processing/generate_tiles.py \
  --input /tmp/colored-weather \
  --output /tmp/tiles \
  --zoom 0-8 \
  --processes 4 \
  --exclude-transparent \
  --organize

# 5. Upload to S3 (future)
aws s3 sync /tmp/tiles s3://your-bucket/tiles/
```

## Storage Considerations

### Zoom Level vs Storage

| Zoom Range | Tiles | Storage per Variable | 5 Variables |
|------------|-------|---------------------|-------------|
| 0-6 | 181 | ~2 MB | ~10 MB |
| 0-8 | 2,203 | ~15 MB | ~79 MB |
| 0-10 | ~35,000 | ~240 MB | ~1.2 GB |
| 0-12 | ~560,000 | ~3.8 GB | ~19 GB |

**Recommendation**: Start with zoom 0-8 for MVP, add higher zooms as needed.

### Storage Optimization

1. **Use `--exclude-transparent`**: Saves ~20-30% by excluding empty tiles
2. **Limit zoom levels**: Only generate zooms you'll actually use
3. **S3 lifecycle policies**: Move old tiles to Glacier after 3 days
4. **CloudFront caching**: Reduce S3 requests

## Web Map Integration

### Mapbox GL JS

```javascript
map.addSource('temperature', {
  type: 'raster',
  tiles: [
    'https://tiles.example.com/temperature_2m/20260110T21z/00/{z}/{x}/{y}.png'
  ],
  tileSize: 256
});

map.addLayer({
  id: 'temperature-layer',
  type: 'raster',
  source: 'temperature',
  paint: {
    'raster-opacity': 0.7
  }
});
```

### Leaflet

```javascript
L.tileLayer(
  'https://tiles.example.com/temperature_2m/20260110T21z/00/{z}/{x}/{y}.png',
  {
    attribution: 'NOAA HRRR',
    maxZoom: 8,
    tileSize: 256
  }
).addTo(map);
```

### OpenLayers

```javascript
new ol.layer.Tile({
  source: new ol.source.XYZ({
    url: 'https://tiles.example.com/temperature_2m/20260110T21z/00/{z}/{x}/{y}.png',
    maxZoom: 8
  })
});
```

## Troubleshooting

### Issue: "RuntimeError: OGR Error: General Error"

**Cause**: Invalid spatial reference system in COG file.

**Solution**: Script automatically fixes this, but if it persists:

```bash
# Manually fix SRS
gdal_translate \
  -a_srs EPSG:3857 \
  input.tif \
  output.tif
```

### Issue: Tiles Not Generating

**Cause**: gdal2tiles.py not found or GDAL not installed.

**Solution**:
```bash
# Check GDAL installation
gdal2tiles.py --version

# Install GDAL if needed (Ubuntu)
apt-get install gdal-bin python3-gdal
```

### Issue: Very Large Storage Usage

**Cause**: Too many zoom levels or not excluding transparent tiles.

**Solution**:
```bash
# Reduce zoom levels
--zoom 0-6

# Exclude transparent tiles
--exclude-transparent
```

### Issue: Slow Processing

**Cause**: Not using enough parallel processes.

**Solution**:
```bash
# Use more CPU cores
--processes 8
```

## Alternative: Dynamic Tile Generation

### Option B: TiTiler (Future Enhancement - TICKET-025)

Instead of pre-generating tiles, use TiTiler for dynamic tile generation:

**Pros**:
- No storage for tiles (serves directly from COGs)
- Automatic updates when COGs change
- Dynamic styling and filtering
- Lower upfront cost

**Cons**:
- Requires additional infrastructure (ECS, Load Balancer)
- Higher operational cost (compute)
- Slightly slower first-tile load

**Implementation** (future):
```
Deploy TiTiler → Point to S3 COGs → CloudFront caching → Web app
```

## Comparison: Pre-generated vs Dynamic

| Aspect | Pre-generated (Current) | Dynamic (TiTiler) |
|--------|------------------------|-------------------|
| Storage | ~80 MB per forecast | ~8 MB per forecast (COGs only) |
| Processing | Upfront (6s per variable) | On-demand |
| Latency | <50ms (cached) | ~200ms (first request) |
| Infrastructure | Simple (S3 + CloudFront) | Complex (ECS + ALB + S3) |
| Cost (low traffic) | $5/month | $30/month |
| Cost (high traffic) | $5/month | $50+/month |
| Best for | MVP, predictable usage | Production, dynamic styling |

**Current Choice**: Pre-generated tiles (simpler, cheaper for MVP).

## Next Steps

After generating tiles:

1. **Upload to S3** (`aws s3 sync /tmp/tiles s3://bucket/`)
2. **Configure CloudFront** (add origin, set caching)
3. **Create metadata JSON** (tile URL template, available variables)
4. **Implement web app** (TICKET-013)

## References

- **gdal2tiles.py docs**: https://gdal.org/programs/gdal2tiles.html
- **XYZ Tile Standard**: https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
- **TiTiler**: https://developmentseed.org/titiler/
- **Mapbox tile spec**: https://docs.mapbox.com/help/glossary/tileset/

---

**Created**: 2026-01-11
**Part of**: TICKET-008 Tile Generation
**Script**: `scripts/processing/generate_tiles.py`
**Dependencies**: GDAL 3.6+, Python 3.10+, colored COGs from TICKET-007
