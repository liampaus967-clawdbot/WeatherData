# Weather Variable Configuration

Configuration files for weather data processing and visualization.

## Files

### variables.yaml

Master configuration file defining:
- **Weather Variables**: Which variables to extract from GRIB2 files
- **Metadata**: Display names, descriptions, units
- **Unit Conversions**: Formulas for converting units (K→°C, m/s→mph, etc.)
- **Color Ramps**: Visualization color schemes
- **Processing Settings**: COG creation, projection, compression

### config_manager.py

Python utility for working with `variables.yaml`:
- Load and validate configuration
- Get enabled variables
- Filter by priority
- Access color ramps and conversion formulas
- Generate summaries

## Quick Start

### View Configuration Summary

```bash
python config/config_manager.py --summary
```

### Validate Configuration

```bash
python config/config_manager.py --validate
```

### List Enabled Variables

```bash
python config/config_manager.py --list-enabled
```

### List All Variables

```bash
python config/config_manager.py --list-all
```

### Get GRIB Search Strings

```bash
python config/config_manager.py --grib-search
```

### Filter by Priority

```bash
# Get only priority 1 (highest priority) variables
python config/config_manager.py --priority 1
```

## Using in Python

```python
from config.config_manager import VariableConfig

# Load configuration
config = VariableConfig()

# Get enabled variables
enabled = config.get_enabled_variables()
print(f"Processing {len(enabled)} variables")

# Get GRIB search strings for download
search_strings = config.get_grib_search_strings()
# ['TMP:2 m', 'UGRD:10 m', 'VGRD:10 m', ...]

# Get specific variable config
temp_config = config.get_variable_by_name('temperature_2m')
print(temp_config['display_name'])  # "Temperature (2m)"
print(temp_config['units_display'])  # "°C"

# Apply unit conversion
kelvin_value = 293.15
celsius = config.apply_conversion(kelvin_value, 'kelvin_to_celsius')
print(celsius)  # 20.0

# Get color ramp
ramp = config.get_color_ramp('temperature')
print(ramp['colors'])  # List of color stops

# Get processing settings
processing = config.get_processing_config()
print(processing['target_projection'])  # "EPSG:3857"
```

## Variables Configuration

### Structure

Each variable in `variables.yaml` includes:

```yaml
variable_name:
  grib_search: "SEARCH_STRING"      # Herbie/GRIB2 search pattern
  display_name: "Display Name"      # User-friendly name
  description: "What this measures" # Detailed description
  units_source: "K"                 # Units in GRIB2 file
  units_display: "°C"               # Units to convert to
  conversion: "kelvin_to_celsius"   # Conversion formula name
  typical_range: [min, max]         # Expected value range
  color_ramp: "temperature"         # Color scheme name
  priority: 1                       # Processing priority (1=highest)
  enabled: true                     # Whether to process
```

### Current Variables

#### Priority 1 (Always Process)
- `temperature_2m` - 2-meter temperature
- `wind_u_10m` - U-component of wind
- `wind_v_10m` - V-component of wind
- `precipitation_accumulated` - Total accumulated precipitation
- `reflectivity_composite` - Composite reflectivity (radar)

#### Priority 2 (Process if time permits)
- `dewpoint_2m` - Dewpoint temperature
- `wind_gust_surface` - Surface wind gusts
- `relative_humidity_2m` - Relative humidity
- `cloud_cover_total` - Total cloud cover
- `cape` - Convective Available Potential Energy

#### Priority 3 (Optional/Future)
- Additional cloud layers
- Snow variables
- Visibility
- Advanced severe weather parameters

### Enabling/Disabling Variables

Edit `variables.yaml` and change `enabled: true` to `enabled: false`:

```yaml
visibility:
  grib_search: "VIS:surface"
  # ... other settings ...
  enabled: false  # Disabled - won't be processed
```

## Color Ramps

Color ramps define visualization colors for each variable.

### Available Ramps

- `temperature` - Purple (cold) → Green → Yellow → Red (hot)
- `dewpoint` - Brown (dry) → Green → Blue (humid)
- `wind_speed` - White (calm) → Blue → Orange → Purple (extreme)
- `wind_component` - Blue (negative) → White (zero) → Red (positive)
- `humidity` - Brown (dry) → Gold → Blue (saturated)
- `precipitation` - White (none) → Blue → Magenta (heavy)
- `cloud_cover` - Sky blue (clear) → Dark gray (overcast)
- `reflectivity` - Cyan (weak) → Green → Yellow → Red → White (hail)
- `cape` - White (stable) → Yellow → Red (explosive)
- And more...

### Color Ramp Structure

```yaml
color_ramps:
  ramp_name:
    type: "gradient"  # or "diverging"
    colors:
      - value: 0
        color: "#FFFFFF"
      - value: 50
        color: "#FF0000"
```

## Unit Conversions

Formulas for converting between units:

- `kelvin_to_celsius` - K → °C
- `kelvin_to_fahrenheit` - K → °F
- `ms_to_mph` - m/s → mph
- `ms_to_kmh` - m/s → km/h
- `kgm2_to_inches` - kg/m² → inches (precipitation)
- `meters_to_miles` - m → mi
- `meters_to_feet` - m → ft
- And more...

### Adding New Conversions

```yaml
conversions:
  my_conversion:
    formula: "value * 2.5 + 10"
    description: "My custom conversion"
```

Then use in variables:

```yaml
my_variable:
  conversion: "my_conversion"
```

## Processing Configuration

Settings for COG creation and data processing:

```yaml
processing:
  extract_enabled_only: true
  process_by_priority: true
  output_format: "COG"              # Cloud Optimized GeoTIFF
  target_projection: "EPSG:3857"    # Web Mercator
  resampling_method: "bilinear"
  compression: "DEFLATE"
  tile_size: 512
  create_overviews: true
  overview_levels: [2, 4, 8, 16]
```

## Adding New Variables

1. Find the GRIB search string:
   ```python
   from herbie import Herbie
   H = Herbie('2026-01-10 12:00', model='hrrr')
   H.inventory()  # Shows all available variables
   ```

2. Add to `variables.yaml`:
   ```yaml
   my_new_variable:
     grib_search: "NEW:search string"
     display_name: "My Variable"
     description: "What it measures"
     units_source: "original_unit"
     units_display: "display_unit"
     conversion: "conversion_name"  # or null
     typical_range: [0, 100]
     color_ramp: "existing_ramp"
     priority: 2
     enabled: true
   ```

3. Optionally add color ramp if needed

4. Validate:
   ```bash
   python config/config_manager.py --validate
   ```

## Validation

The configuration manager performs these validations:

- ✅ Required keys present (model, product, variables)
- ✅ All variables have required fields
- ✅ Color ramp references exist
- ✅ Conversion references exist
- ✅ Configuration is valid YAML

Run validation:
```bash
python config/config_manager.py --validate
```

## Integration with Pipeline

The variable configuration integrates with:

- **TICKET-006 (Processing)**: Uses this config to extract and process variables
- **TICKET-007 (Color Ramps)**: Uses color definitions for visualization
- **TICKET-008 (Tile Generation)**: Uses processed COGs for tile creation

## Examples

### Get Variables for Download

```python
config = VariableConfig()
searches = config.get_grib_search_strings()

# Use with download script
python scripts/hrrr/download_hrrr.py --latest --variables all
```

### Process Only Priority 1 Variables

```python
config = VariableConfig()
priority1 = config.get_variables_by_priority(1)

for name, var_config in priority1.items():
    print(f"Processing {var_config['display_name']}...")
    # Extract from GRIB2 and process
```

### Apply Unit Conversion

```python
config = VariableConfig()

# Temperature
temp_k = 293.15
temp_c = config.apply_conversion(temp_k, 'kelvin_to_celsius')
# 20.0

# Wind speed
wind_ms = 10.0
wind_mph = config.apply_conversion(wind_ms, 'ms_to_mph')
# 22.369
```

## Future Enhancements

- [ ] Add GFS model variables
- [ ] Add RAP model variables
- [ ] Interactive variable selector
- [ ] Validation against actual GRIB2 files
- [ ] Auto-detection of available variables

---

**Created**: 2026-01-10
**Part of**: TICKET-005 (Variable Configuration System)
**Next**: TICKET-006 (Data Processing with rioxarray)
