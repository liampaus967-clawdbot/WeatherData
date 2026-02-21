#!/bin/bash
#
# Setup Python Virtual Environment for Weather Data Pipeline
#
# This script:
# 1. Installs system dependencies (GDAL, eccodes)
# 2. Creates a Python virtual environment
# 3. Installs Python dependencies
#
# Usage: ./scripts/setup/setup_venv.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VENV_DIR="$PROJECT_ROOT/venv"

echo "=============================================="
echo "Weather Data Pipeline - Environment Setup"
echo "=============================================="
echo "Project root: $PROJECT_ROOT"
echo "Venv location: $VENV_DIR"
echo ""

# Check if running as root for apt installs
if [[ $EUID -ne 0 ]]; then
    SUDO="sudo"
else
    SUDO=""
fi

# ============================================================================
# Step 1: Install System Dependencies
# ============================================================================
echo "==> Step 1: Installing system dependencies..."

# Detect OS
if [[ -f /etc/os-release ]]; then
    source /etc/os-release
    OS_ID="${ID:-unknown}"
else
    OS_ID="unknown"
fi

case "$OS_ID" in
    ubuntu|debian)
        echo "Detected Ubuntu/Debian"
        
        # Add UbuntuGIS PPA for latest GDAL (Ubuntu only)
        if [[ "$OS_ID" == "ubuntu" ]]; then
            $SUDO add-apt-repository -y ppa:ubuntugis/ppa 2>/dev/null || true
        fi
        
        $SUDO apt-get update
        $SUDO apt-get install -y \
            python3 \
            python3-pip \
            python3-venv \
            python3-dev \
            gdal-bin \
            libgdal-dev \
            libeccodes-dev \
            libeccodes-tools \
            libnetcdf-dev \
            libhdf5-dev \
            curl \
            wget
        
        # Get GDAL version for pip install
        GDAL_VERSION=$(gdal-config --version)
        echo "System GDAL version: $GDAL_VERSION"
        ;;
    
    amzn|rhel|centos|fedora)
        echo "Detected RHEL/Amazon Linux/Fedora"
        
        $SUDO yum install -y \
            python3 \
            python3-pip \
            python3-devel \
            gdal \
            gdal-devel \
            eccodes \
            eccodes-devel \
            netcdf-devel \
            hdf5-devel \
            curl \
            wget
        
        GDAL_VERSION=$(gdal-config --version)
        echo "System GDAL version: $GDAL_VERSION"
        ;;
    
    *)
        echo "Warning: Unknown OS ($OS_ID). Please install manually:"
        echo "  - Python 3.10+"
        echo "  - GDAL (gdal-bin, libgdal-dev)"
        echo "  - eccodes (libeccodes-dev, libeccodes-tools)"
        echo "  - netCDF (libnetcdf-dev)"
        echo "  - HDF5 (libhdf5-dev)"
        
        if command -v gdal-config &> /dev/null; then
            GDAL_VERSION=$(gdal-config --version)
            echo "Found system GDAL: $GDAL_VERSION"
        else
            echo "Error: GDAL not found. Please install GDAL first."
            exit 1
        fi
        ;;
esac

# Verify GDAL installation
echo ""
echo "Verifying GDAL installation..."
gdalinfo --version
echo ""

# ============================================================================
# Step 2: Create Virtual Environment
# ============================================================================
echo "==> Step 2: Creating Python virtual environment..."

if [[ -d "$VENV_DIR" ]]; then
    echo "Removing existing venv..."
    rm -rf "$VENV_DIR"
fi

python3 -m venv "$VENV_DIR"

# Activate venv
source "$VENV_DIR/bin/activate"

echo "Python: $(which python)"
echo "Version: $(python --version)"
echo ""

# ============================================================================
# Step 3: Install Python Dependencies
# ============================================================================
echo "==> Step 3: Installing Python dependencies..."

# Upgrade pip
pip install --upgrade pip setuptools wheel

# Install GDAL Python bindings matching system version
echo "Installing GDAL Python bindings (version $GDAL_VERSION)..."
pip install "GDAL==$GDAL_VERSION"

# Install remaining dependencies
pip install -r "$PROJECT_ROOT/requirements.txt"

# ============================================================================
# Step 4: Verify Installation
# ============================================================================
echo ""
echo "==> Step 4: Verifying installation..."

python -c "
import sys
print(f'Python {sys.version}')

from osgeo import gdal
print(f'GDAL {gdal.__version__}')

import xarray as xr
print(f'xarray {xr.__version__}')

import rioxarray
print(f'rioxarray installed')

import cfgrib
print(f'cfgrib {cfgrib.__version__}')

from herbie import Herbie
print(f'Herbie installed')

import boto3
print(f'boto3 {boto3.__version__}')

print()
print('✅ All dependencies installed successfully!')
"

# ============================================================================
# Done
# ============================================================================
echo ""
echo "=============================================="
echo "✅ Setup Complete!"
echo "=============================================="
echo ""
echo "To activate the environment:"
echo "  source $VENV_DIR/bin/activate"
echo ""
echo "To run the pipeline:"
echo "  ./scripts/pipeline.sh --dry-run"
echo ""
