# ✅ TICKET-005: Variable Configuration System - COMPLETE

**Status**: Complete
**Date**: 2026-01-10
**Priority**: P1
**Effort**: XS (< 2 hours)

## What Was Completed

### ✅ Variable Configuration File (`variables.yaml`)

Created comprehensive YAML configuration defining:

**Variables Configured**: 23 total (10 enabled by default)
- Temperature variables (2m, surface, dewpoint)
- Wind variables (U/V components, gusts)
- Moisture variables (humidity)
- Precipitation variables (accumulated, rate)
- Cloud variables (total, by layer)
- Severe weather variables (reflectivity, CAPE, CIN, helicity)
- Visibility and snow variables

**Metadata for Each Variable**:
- `grib_search` - Herbie/GRIB2 search pattern
- `display_name` - User-friendly name
- `description` - What the variable measures
- `units_source` - Units in GRIB2 file
- `units_display` - Units to display (after conversion)
- `conversion` - Unit conversion formula name
- `typical_range` - Expected min/max values
- `color_ramp` - Visualization color scheme
- `priority` - Processing priority (1=highest, 3=lowest)
- `enabled` - Whether to process this variable

**Color Ramps**: 15 color schemes defined
- `temperature` - Purple → Green → Yellow → Red
- `dewpoint` - Brown → Green → Blue
- `wind_speed` - White → Blue → Orange → Purple
- `humidity` - Brown → Gold → Blue
- `precipitation` - White → Blue → Magenta
- `cloud_cover` - Sky blue → Dark gray
- `reflectivity` - Cyan → Green → Yellow → Red → White
- `cape` - White → Yellow → Red
- And more...

**Unit Conversions**: 11 conversion formulas
- Kelvin → Celsius/Fahrenheit
- m/s → mph/km/h
- kg/m² → inches/mm (precipitation)
- meters → miles/feet/inches
- Rate conversions for precipitation

**Processing Settings**:
- Output format: Cloud Optimized GeoTIFF (COG)
- Target projection: EPSG:3857 (Web Mercator)
- Resampling: Bilinear
- Compression: DEFLATE (level 6)
- Tile size: 512x512
- Overviews: Yes (2x, 4x, 8x, 16x)

### ✅ Configuration Manager (`config_manager.py`)

Python utility for working with configuration:

**Features**:
- Load and validate YAML configuration
- Get enabled variables
- Filter variables by priority
- Access color ramps and conversions
- Apply unit conversions programmatically
- Generate configuration summaries
- Command-line interface

**Command-Line Interface**:
```bash
# Validate configuration
python config/config_manager.py --validate

# Show summary
python config/config_manager.py --summary

# List enabled variables
python config/config_manager.py --list-enabled

# List all variables (enabled + disabled)
python config/config_manager.py --list-all

# Filter by priority
python config/config_manager.py --priority 1

# Get GRIB search strings
python config/config_manager.py --grib-search
```

**Python API**:
```python
from config.config_manager import VariableConfig

config = VariableConfig()
enabled = config.get_enabled_variables()
searches = config.get_grib_search_strings()
ramp = config.get_color_ramp('temperature')
celsius = config.apply_conversion(293.15, 'kelvin_to_celsius')
```

### ✅ Documentation (`config/README.md`)

Comprehensive documentation including:
- Quick start guide
- Configuration structure reference
- Python usage examples
- How to add new variables
- Color ramp definitions
- Unit conversion reference
- Integration with pipeline
- Validation instructions

## Testing Results

### ✅ Configuration Validation

```bash
$ python config/config_manager.py --validate
✅ Configuration is valid!
```

**Validation Checks**:
- ✅ Required top-level keys present
- ✅ All variables have required fields
- ✅ Color ramp references exist
- ✅ Conversion formula references exist
- ✅ Valid YAML syntax

### ✅ Enabled Variables

```bash
$ python config/config_manager.py --list-enabled
Enabled Variables (10):
  - temperature_2m            | Temperature (2m)
  - dewpoint_2m               | Dewpoint (2m)
  - wind_u_10m                | U-Wind (10m)
  - wind_v_10m                | V-Wind (10m)
  - wind_gust_surface         | Wind Gust
  - relative_humidity_2m      | Relative Humidity (2m)
  - precipitation_accumulated | Accumulated Precipitation
  - cloud_cover_total         | Total Cloud Cover
  - reflectivity_composite    | Composite Reflectivity
  - cape                      | CAPE
```

### ✅ GRIB Search Strings

```bash
$ python config/config_manager.py --grib-search
GRIB Search Strings (10):
  - TMP:2 m
  - DPT:2 m
  - UGRD:10 m
  - VGRD:10 m
  - GUST:surface
  - RH:2 m
  - APCP:surface
  - TCDC:entire atmosphere
  - REFC:entire atmosphere
  - CAPE:surface
```

### ✅ Priority Filtering

**Priority 1 Variables** (Always process):
- temperature_2m
- wind_u_10m
- wind_v_10m
- precipitation_accumulated
- reflectivity_composite

**Priority 2 Variables** (Process if time permits):
- dewpoint_2m
- wind_gust_surface
- relative_humidity_2m
- cloud_cover_total
- cape

**Priority 3 Variables** (Optional):
- Additional cloud layers
- Snow variables
- Visibility
- Advanced parameters
- (Disabled by default)

## Configuration Examples

### Variable Definition

```yaml
temperature_2m:
  grib_search: "TMP:2 m"
  display_name: "Temperature (2m)"
  description: "Air temperature at 2 meters above ground"
  units_source: "K"
  units_display: "°C"
  conversion: "kelvin_to_celsius"
  typical_range: [-40, 50]
  color_ramp: "temperature"
  priority: 1
  enabled: true
```

### Color Ramp Definition

```yaml
temperature:
  type: "gradient"
  colors:
    - value: -40
      color: "#1a0066"  # Deep purple (extreme cold)
    - value: 0
      color: "#00ff00"  # Green (freezing)
    - value: 20
      color: "#ffff00"  # Yellow (room temp)
    - value: 50
      color: "#ff0000"  # Red (extreme heat)
```

### Unit Conversion

```yaml
kelvin_to_celsius:
  formula: "value - 273.15"
  description: "Convert Kelvin to Celsius"
```

## Usage in Pipeline

### Integration with TICKET-006 (Processing)

The processing script will use this configuration to:

1. **Extract Variables**: Read enabled variables from config
2. **Process Each**: Extract from GRIB2 using `grib_search` patterns
3. **Convert Units**: Apply conversion formulas
4. **Apply Colors**: Use color ramps for visualization
5. **Create COGs**: Use processing settings (projection, compression, etc.)

Example usage:
```python
from config.config_manager import VariableConfig

config = VariableConfig()
enabled = config.get_enabled_variables()

for var_name, var_config in enabled.items():
    # Extract from GRIB2
    grib_search = var_config['grib_search']

    # Read data using GDAL
    data = extract_from_grib(grib_file, grib_search)

    # Apply unit conversion
    if var_config['conversion']:
        data = config.apply_conversion(data, var_config['conversion'])

    # Create COG with color ramp
    ramp = config.get_color_ramp(var_config['color_ramp'])
    create_cog(data, ramp, output_file)
```

### Integration with TICKET-007 (Color Ramps)

Color ramp application will use definitions from config:

```python
ramp = config.get_color_ramp('temperature')
# Apply to GeoTIFF using GDAL color table
```

## Acceptance Criteria

All acceptance criteria from TICKET-005 met:

- [x] Created `config/variables.yaml` configuration file
- [x] Defined variable mappings with Herbie search strings
- [x] Included visualization settings (color ramps, units, display names)
- [x] Added metadata for each variable (description, units, typical range)
- [x] Created helper script to list available variables
- [x] Validated configuration on startup
- [x] Configuration is valid YAML
- [x] Helper script can query available variables
- [x] Easy to add new variables

## File Structure

```
config/
├── variables.yaml         # Main configuration (600+ lines)
├── config_manager.py      # Configuration utility (250+ lines)
├── README.md              # Documentation (400+ lines)
└── TICKET-005-COMPLETE.md # This file
```

## Benefits

### For Development

- ✅ **Single Source of Truth**: All variable definitions in one place
- ✅ **Easy to Extend**: Add new variables by editing YAML
- ✅ **Type Safe**: Validation catches configuration errors
- ✅ **Reusable**: Same config for download, processing, and visualization
- ✅ **Documented**: Every variable has description and metadata

### For Operations

- ✅ **Configurable**: Enable/disable variables without code changes
- ✅ **Priority System**: Process critical variables first
- ✅ **Consistent Units**: Automatic unit conversions
- ✅ **Validated**: Configuration checked before processing
- ✅ **Extensible**: Easy to add new models (GFS, RAP) later

## Next Steps

### Immediate: TICKET-006 (Data Processing)

Use this configuration to:
1. Extract enabled variables from GRIB2 files
2. Apply unit conversions
3. Reproject to EPSG:3857
4. Create Cloud Optimized GeoTIFFs
5. Apply color ramps

Example workflow:
```bash
# 1. Download GRIB2 (TICKET-004)
python scripts/hrrr/download_hrrr.py --latest --variables all

# 2. Process variables (TICKET-006 - next!)
python scripts/processing/process_weather.py \
  --input /tmp/weather-data/hrrr.20260110.t19z.f00.grib2 \
  --config config/variables.yaml \
  --output /tmp/processed/
```

### Future Enhancements

- [ ] Add GFS model configuration
- [ ] Add RAP model configuration
- [ ] Interactive variable selector (web UI)
- [ ] Auto-detect available variables from GRIB2
- [ ] Variable aliases for common names

## Summary

✅ **TICKET-005 is complete!**

Created comprehensive variable configuration system with:
- 23 weather variables defined (10 enabled)
- 15 color ramps for visualization
- 11 unit conversion formulas
- Python configuration manager
- Full documentation
- Validated and tested

**Files Created**: 3
**Total Lines**: 1,250+
**Variables Configured**: 23
**Enabled by Default**: 10
**Ready for**: TICKET-006 (Data Processing)

---

**Completed**: 2026-01-10
**Time Spent**: ~1 hour
**Next Ticket**: TICKET-006 (Data Processing with rioxarray)
