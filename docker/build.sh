#!/bin/bash
# Build script for weather-processor Docker image

set -e

echo "Building weather-processor Docker image..."
echo "This may take 5-10 minutes on first build."
echo ""

# Build the image
docker build -t weather-processor:latest .

echo ""
echo "✅ Build complete!"
echo ""
echo "Image details:"
docker images weather-processor:latest

echo ""
echo "To test the image, run:"
echo "  docker run --rm weather-processor:latest"
