#!/usr/bin/env python3
"""Debug script to diagnose GRIB2 processing issues."""

import sys
from pathlib import Path
from osgeo import gdal

# Enable GDAL exceptions
gdal.UseExceptions()

def list_bands(grib_file):
    """List all bands and their metadata."""
    print(f"\n{'='*60}")
    print(f"Analyzing: {grib_file}")
    print(f"{'='*60}\n")
    
    ds = gdal.Open(str(grib_file))
    if not ds:
        print(f"ERROR: Cannot open file")
        return
    
    print(f"Raster size: {ds.RasterXSize} x {ds.RasterYSize}")
    print(f"Band count: {ds.RasterCount}")
    print(f"\n{'Band':<6} {'Element':<12} {'Short Name':<20} {'Description'}")
    print("-" * 80)
    
    # Find TMP bands specifically
    tmp_bands = []
    
    for i in range(1, min(ds.RasterCount + 1, 500)):  # Limit to first 500 bands
        band = ds.GetRasterBand(i)
        desc = band.GetDescription()
        metadata = band.GetMetadata()
        
        element = metadata.get('GRIB_ELEMENT', 'Unknown')
        short_name = metadata.get('GRIB_SHORT_NAME', 'Unknown')
        
        # Print all TMP bands
        if element == 'TMP':
            print(f"{i:<6} {element:<12} {short_name:<20} {desc[:40]}")
            tmp_bands.append((i, element, short_name, desc, metadata))
    
    print(f"\n{'='*60}")
    print(f"Found {len(tmp_bands)} TMP (temperature) bands")
    print(f"{'='*60}\n")
    
    # Show detailed info for first few TMP bands
    for i, (band_num, element, short_name, desc, metadata) in enumerate(tmp_bands[:5]):
        print(f"\nTMP Band {band_num} details:")
        print(f"  Description: {desc}")
        print(f"  Short name: {short_name}")
        print(f"  Relevant metadata:")
        for key in ['GRIB_ELEMENT', 'GRIB_SHORT_NAME', 'GRIB_COMMENT', 'GRIB_UNIT']:
            if key in metadata:
                print(f"    {key}: {metadata[key]}")
    
    ds = None
    
    # Try to extract 2m temperature
    print(f"\n{'='*60}")
    print("Testing extraction of '2 m above ground' temperature...")
    print(f"{'='*60}\n")
    
    # Find band with 2m level
    for band_num, element, short_name, desc, metadata in tmp_bands:
        desc_lower = desc.lower()
        short_lower = short_name.lower()
        
        # Check for 2m indicators
        if '2-htgl' in short_lower or '2[m]' in desc_lower or '2 m' in desc_lower:
            print(f"✓ Found 2m temperature at band {band_num}")
            print(f"  Short name: {short_name}")
            print(f"  Description: {desc}")
            
            # Try to read the data
            ds = gdal.Open(str(grib_file))
            band = ds.GetRasterBand(band_num)
            data = band.ReadAsArray()
            print(f"  Data shape: {data.shape}")
            print(f"  Data range: {data.min():.2f} to {data.max():.2f}")
            print(f"  (If in Kelvin, 273.15 K = 0°C)")
            ds = None
            return band_num
    
    print("✗ Could not find 2m temperature band")
    print("\nAll TMP band short names:")
    for band_num, element, short_name, desc, metadata in tmp_bands:
        print(f"  Band {band_num}: {short_name}")
    
    return None

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python debug_grib.py <path_to_grib2_file>")
        print("\nTo download a test file first:")
        print("  python scripts/hrrr/download_hrrr.py --date $(date -u +%Y-%m-%d) --cycle 0 --fxx 0 --variables all --output /tmp/test --keep-local")
        sys.exit(1)
    
    grib_path = Path(sys.argv[1])
    if not grib_path.exists():
        print(f"File not found: {grib_path}")
        sys.exit(1)
    
    list_bands(grib_path)
