# Installing Docker Desktop on macOS

Docker Desktop is required to build and run the weather-processor container.

## Installation Options

### Option 1: Install via Homebrew (Recommended)

```bash
# Install Docker Desktop
brew install --cask docker

# Start Docker Desktop
open -a Docker

# Wait for Docker to start (look for whale icon in menu bar)
# Then verify installation
docker info
```

### Option 2: Download from Docker Website

1. Visit: https://www.docker.com/products/docker-desktop
2. Download Docker Desktop for Mac (Apple Silicon or Intel)
3. Open the downloaded `.dmg` file
4. Drag Docker to Applications folder
5. Open Docker from Applications
6. Wait for Docker to start (whale icon in menu bar)

## Verify Installation

Once Docker Desktop is running:

```bash
# Check Docker is running
docker info

# You should see output like:
# Client: Docker Engine - Community
# Server:
#  Containers: 0
#  Images: 0
#  ...
```

## After Installation

Once Docker is running, build the weather-processor image:

```bash
cd /Users/liampaus/Documents/GIT/WeatherData/docker
./build.sh
```

Then run the tests:

```bash
./test.sh
```

## Troubleshooting

### "Docker is not running"

**Solution**: Look for the whale icon in your menu bar (top right). If it's not there:
- Open Docker from Applications
- Wait 30-60 seconds for it to start
- The whale icon should appear when ready

### "Cannot connect to Docker daemon"

**Solution**:
```bash
# Restart Docker Desktop
pkill Docker
open -a Docker

# Wait 60 seconds, then try again
docker info
```

### Installation is slow

Docker Desktop download is ~500MB. First-time startup can take 1-2 minutes.

---

## System Requirements

- macOS 11 or newer
- At least 4GB RAM (8GB recommended)
- At least 10GB free disk space

---

**Ready to proceed?** Once Docker Desktop is installed and running, return to this terminal and we'll build the image!
