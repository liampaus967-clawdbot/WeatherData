FROM ghcr.io/osgeo/gdal:ubuntu-small-3.8.0

# Install system dependencies for eccodes (required by cfgrib/Herbie)
# and other utilities
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    libeccodes-dev \
    libeccodes-tools \
    curl \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install build tools
RUN pip3 install --upgrade pip setuptools wheel

# Copy and install Python requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Set environment variables for eccodes
ENV ECCODES_DIR=/usr
ENV ECCODES_DEFINITION_PATH=/usr/share/eccodes/definitions

# Set GDAL environment variables
ENV GDAL_CACHEMAX=512
ENV GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR
ENV CPL_VSIL_CURL_ALLOWED_EXTENSIONS=.tif,.tiff,.nc,.grib2,.idx

# Create working directory
WORKDIR /app

# Set Python to unbuffered mode for better logging in containers
ENV PYTHONUNBUFFERED=1

# Create directories for data processing
RUN mkdir -p /app/scripts /app/config /tmp/weather-data

# Default command - show versions
CMD python3 -c "import sys; print(f'Python {sys.version}'); \
    from osgeo import gdal; print(f'GDAL {gdal.__version__}'); \
    import xarray as xr; print(f'xarray {xr.__version__}'); \
    import rioxarray; print(f'rioxarray {rioxarray.__version__}'); \
    from herbie import Herbie; print(f'Herbie installed successfully')"
