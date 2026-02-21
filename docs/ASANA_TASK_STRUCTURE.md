# Asana Task Structure for Weather Data Pipeline

## Parent Task

**Weather Data Pipeline - Automated Infrastructure**
_Status: In Progress | Priority: P0 | Timeline: ~2.5-3 weeks | **18/25 tickets complete (72%)**_

---

## Phase 1: Infrastructure Setup

**Section** (or sub-project in Asana)

### TICKET-001: Set Up AWS Infrastructure with Terraform

- **Priority**: P0 | **Effort**: M (4-8 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Update `main.tf` to create organized S3 bucket structure (raw-grib2/, processed-cog/, tiles/, metadata/)
  - [x] Create S3 lifecycle policies for automated data retention
    - raw-grib2: Delete after 7 days
    - processed-cog: Transition to Standard-IA after 30 days, delete after 60 days
    - tiles: Delete after 3 days
    - metadata: Never expire (versions after 30 days)
  - [x] Create IAM role for EC2 instance with S3 read/write permissions (manual via Console)
  - [x] Create IAM policy for CloudWatch metrics and logs (manual via Console)
  - [ ] Add CloudFront distribution (optional for CDN) - Skipped for now
  - [x] Configure CORS for S3 bucket to allow web access
  - [x] Add versioning for processed-cog bucket (for rollback capability)
- **Acceptance**: ✅ `terraform apply` succeeded, S3 bucket `sat-data-automation-test` configured, lifecycle policies active
- **Completion Date**: 2026-01-10
- **Documentation**: `terraform/TICKET-001-COMPLETE.md`

### TICKET-002: Provision EC2 Instance for Data Processing

- **Priority**: P0 | **Effort**: S (<4 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Launch EC2 instance in us-east-2 (same region as S3)
  - [x] Configure security group (Session Manager connection)
  - [x] Attach IAM role with S3 and CloudWatch permissions
  - [x] Install system dependencies:
    - [x] Docker 29.1.4 ✅
    - [x] AWS CLI v2.32.32 ✅
    - [x] Python 3.12.3 ✅
    - [x] pip, git ✅
  - [x] Configure AWS CLI with IAM role (automatic)
  - [x] Set up CloudWatch Logs agent
  - [x] Create working directory: `/home/ubuntu/weather-pipeline/`
  - [x] Verify S3 access to `sat-data-automation-test`
  - [x] Verify Docker permissions (no sudo required)
  - [x] Clone repository to EC2
  - [x] Build Docker image on EC2: `weather-processor:latest`
  - [x] Test Herbie functionality
- **Acceptance**: ✅ Can connect via Session Manager, Docker 29.1.4 running, AWS CLI configured, S3 access verified, 45 GB free disk space
- **Completion Date**: 2026-01-10
- **Documentation**: `terraform/TICKET-002-COMPLETE.md`

### TICKET-003: Create Docker Container for Weather Data Processing

- **Priority**: P0 | **Effort**: M (4-8 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Create `docker/Dockerfile` based on `osgeo/gdal:ubuntu-small-3.8.0`
  - [x] Install Python dependencies (herbie-data, rioxarray, xarray, boto3, pyyaml)
  - [x] Create `docker/requirements.txt`
  - [x] Install cfgrib and eccodes for GRIB2 support (required by Herbie)
  - [x] Add processing scripts to container (ready for TICKET-004)
  - [x] Test download and processing workflow locally
  - [x] Build and tag image: `weather-processor:latest`
  - [x] Test container with sample HRRR download on EC2 ✅
  - [x] Document resource requirements (memory, CPU)
  - [x] Fixed NumPy compatibility issue (pinned to 1.x for GDAL)
- **Acceptance**: ✅ Docker image built (2.35GB with all dependencies), Herbie working, S3 access verified, all 6 tests passing
- **Completion Date**: 2026-01-10
- **Documentation**: `docker/BUILD_SUMMARY.md`

---

## Phase 2: Data Ingestion Scripts

**Section** (or sub-project in Asana)

### TICKET-004: Create HRRR Download Script with Herbie

- **Priority**: P0 | **Effort**: M (4-8 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Create `scripts/hrrr/download_hrrr.py` using Herbie
  - [x] Install herbie-data package in Docker container
  - [x] Download forecast hours F00-F12 for specified variables
  - [x] Use Herbie's smart download for specific variables (TMP:2 m, UGRD:10 m, VGRD:10 m)
  - [x] Save downloaded data as GRIB2 format
  - [x] Add logging with timestamps
  - [x] Handle missing or delayed NOAA data gracefully (Herbie auto-fallback)
  - [x] Add command-line arguments (--date, --cycle, --fxx, --variables)
  - [x] Fixed cfgrib segmentation fault issue (switched to GDAL)
- **Acceptance**: ✅ Can download all 13 forecast hours, handles network errors, Herbie working on EC2
- **Completion Date**: 2026-01-10
- **Documentation**: `docs/TICKET-004-COMPLETE.md` (if exists) or covered in TICKET-006
- **Note**: cfgrib issues resolved by using GDAL directly in TICKET-006

### TICKET-005: Create Variable Configuration System

- **Priority**: P1 | **Effort**: XS (<2 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Create `config/variables.yaml` configuration file
  - [x] Define variable mappings with GRIB search strings (20+ variables)
  - [x] Include visualization settings (15 color ramps, units, display names)
  - [x] Add metadata for each variable (description, units, typical range)
  - [x] Create `config/config_manager.py` helper class
  - [x] Validate configuration on startup
- **Acceptance**: ✅ Configuration file is valid YAML, helper script can query available variables, 15 color ramps defined
- **Completion Date**: 2026-01-10
- **Documentation**: `docs/TICKET-005-COMPLETE.md`

---

## Phase 3: GDAL Processing Pipeline

**Section** (or sub-project in Asana)

### TICKET-006: Create Data Processing Script with GDAL/rioxarray

- **Priority**: P0 | **Effort**: M (4-8 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Create `scripts/processing/process_weather.py`
  - [x] Load GRIB2 files using GDAL (not xarray to avoid cfgrib issues)
  - [x] Implement data processing function:
    - [x] Extract specific variables using GRIB band matching
    - [x] Reproject to EPSG:3857 (Web Mercator) using rioxarray
    - [x] Apply unit conversions (Kelvin to Celsius, etc.) with auto-detection
    - [x] Apply bilinear resampling
    - [x] Export to Cloud Optimized GeoTIFF (COG)
    - [x] Apply DEFLATE compression (level 6)
    - [x] Add overviews for multi-scale viewing (2x, 4x, 8x, 16x)
  - [x] Priority-based processing (process P1 variables first)
  - [x] Add progress tracking and logging
  - [x] Handle processing errors gracefully (fallback to GDAL warp)
  - [x] Validate output COG files
  - [x] Add command-line arguments (--input, --output, --priority, --variables)
  - [x] Optimize for web serving (512x512 tile blocks)
  - [x] Create comprehensive documentation (344 lines)
- **Acceptance**: ✅ Processes GRIB2 to COG successfully, outputs ~2-17MB per variable, processes 5 variables in ~10-15 seconds
- **Completion Date**: 2026-01-10
- **Documentation**: `docs/TICKET-006-COMPLETE.md`

### TICKET-007: Add Color Ramp and Visualization Styling

- **Priority**: P1 | **Effort**: M (4-8 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Use color ramp definitions from `config/variables.yaml` (15 predefined ramps)
  - [x] Create `scripts/processing/apply_colormap.py`
  - [x] Define temperature ranges and colors (-40°C to 50°C with 10 color stops)
  - [x] Implement `gdaldem color-relief` wrapper in processing pipeline
  - [x] Convert YAML color definitions to GDAL color-relief format
  - [x] Convert to 8-bit RGBA with transparency (alpha channel)
  - [x] Generate RGB GeoTIFFs for web serving
  - [x] Create 15 color schemes (temperature, precipitation, wind, reflectivity, etc.)
  - [x] Add command-line option to select variable (auto-detect from filename)
  - [x] Apply DEFLATE compression with predictor
  - [x] Generate overview pyramids (2x, 4x, 8x, 16x)
  - [x] Document color ramp customization (600+ line README)
- **Acceptance**: ✅ Output files have color applied, temperature ranges map to intuitive colors, file sizes reduced by 86%
- **Completion Date**: 2026-01-11
- **Documentation**: `docs/TICKET-007-COMPLETE.md`, `scripts/processing/README-COLORMAP.md`

---

## Phase 4: Tile Generation

**Section** (or sub-project in Asana)

### TICKET-008: Implement Tile Generation Strategy

- **Priority**: P1 | **Effort**: M (4-8 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] **Option A: Pre-generated Tiles** (Selected and implemented)
    - [x] Create `scripts/processing/generate_tiles.py` wrapper for gdal2tiles
    - [x] Generate zoom levels 0-10 (configurable, tested with 0-8)
    - [x] Use XYZ tile naming convention
    - [x] Output PNG tiles with transparency (RGBA format)
    - [x] Implement parallel tile generation (4+ processes)
    - [x] Create directory structure: `{variable}/{timestamp}/{forecast}/{z}/{x}/{y}.png`
    - [x] Automatic SRS fixing for gdaldem color-relief artifacts
    - [x] Batch processing support
    - [x] Resume mode for interrupted generation
  - [ ] **Option B: Dynamic TiTiler** (Deferred to TICKET-025)
    - [ ] Deploy TiTiler on ECS Fargate
    - [ ] Configure TiTiler to read COGs from S3
    - [ ] Set up Application Load Balancer
    - [ ] Configure caching headers
    - [ ] Document tile URL format
  - [x] Benchmark approaches (speed, storage, cost)
  - [x] Document pros/cons of each approach
- **Acceptance**: ✅ Tiles generated successfully (11,015 tiles in 32s), XYZ format, 100% success rate, organized structure
- **Completion Date**: 2026-01-11
- **Documentation**: `docs/TICKET-008-COMPLETE.md`, `scripts/processing/README-TILES.md`

### TICKET-009: Optimize Tile Generation Performance

- **Priority**: P2 | **Effort**: M (4-8 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Profile tile generation performance (added timing and metrics)
  - [x] Implement parallel tile generation (use all CPU cores) (already in TICKET-008, `--processes`)
  - [x] Skip generating tiles for zoom levels with no data (already in TICKET-008, `--exclude-transparent`)
  - [x] Use RAM disk for temporary tile storage during generation (new `--use-ramdisk`)
  - [x] Optimize PNG compression settings (new `--png-level 1-9`)
  - [x] Implement incremental tile updates (already in TICKET-008, `--resume`)
  - [ ] Batch upload tiles to S3 (not one-by-one) (deferred to TICKET-010)
- **Acceptance**: ✅ Tile generation for 13 forecast hours completes in <10 minutes (target: <20), utilizes all CPU cores, performance metrics tracked
- **Completion Date**: 2026-01-11
- **Documentation**: `docs/TICKET-009-COMPLETE.md`

---

## Phase 5: Automation and Orchestration

**Section** (or sub-project in Asana)

### TICKET-010: Create Master Pipeline Orchestration Script

- **Priority**: P0 | **Effort**: M (4-8 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Create `scripts/pipeline.sh` (590 lines)
  - [x] Calculate model run time (current UTC - 3 hours, cross-platform BSD/GNU date)
  - [x] Call download script (download_hrrr.py)
  - [x] Call GDAL processing (via Docker: process_weather.py, apply_colormap.py, generate_tiles.py)
  - [x] Call tile generation (if enabled, configurable via --disable-tiles)
  - [x] Upload processed files to S3 (batch upload with aws s3 sync)
  - [x] Generate and upload metadata (latest.json with model run info)
  - [x] Add error handling and rollback logic (trap EXIT, set -euo pipefail)
  - [x] Log all steps with timestamps (structured logging: INFO/WARN/ERROR/SUCCESS)
  - [x] Send CloudWatch metrics (ProcessingTime, Success)
  - [x] Clean up temporary files (automatic cleanup on exit)
  - [x] Add dry-run mode for testing (--dry-run flag)
- **Acceptance**: ✅ Pipeline runs end-to-end successfully in dry-run mode, estimated 3-5 minutes per forecast (target: <30), handles errors gracefully
- **Completion Date**: 2026-01-11
- **Documentation**: `docs/TICKET-010-COMPLETE.md`

### TICKET-011: Configure Cron Job for Hourly Execution

- **Priority**: P0 | **Effort**: S (<4 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Create cron entry for hourly execution (runs at :15 past each hour UTC)
  - [x] Configure proper environment variables in cron (`weather-pipeline.env`)
  - [x] Set up log rotation for pipeline logs (14 days retention)
  - [x] Test cron job execution (via `run_pipeline.sh` wrapper)
  - [x] Add monitoring for missed cron executions (`check_pipeline_status.sh`)
  - [x] Configure email/SNS alerts for failures (SNS integration in health check)
  - [x] Document cron schedule and timezone (UTC)
- **Acceptance**: ✅ Cron job runs at :15 past each hour, pipeline executes successfully, logs rotated
- **Completion Date**: 2026-01-11
- **Documentation**: `docs/TICKET-011-COMPLETE.md`

### TICKET-012: Create Metadata Generation Script

- **Priority**: P1 | **Effort**: S (<4 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Create `scripts/generate_metadata.py` (354 lines)
  - [x] Calculate model run timestamp (parse YYYYMMDD or YYYY-MM-DD formats)
  - [x] List available forecast hours (scan tiles directory)
  - [x] Generate tile URL template with placeholders ({variable}, {timestamp}, {forecast}, {z}/{x}/{y})
  - [x] Include forecast validity times (ISO 8601 timestamps)
  - [x] Add data age/freshness indicator (fresh/stale/old status)
  - [x] Upload to S3 with cache-control headers (via pipeline.sh)
  - [x] Validate JSON schema (TypeScript types created)
  - [x] Created TypeScript hook `useWeatherMetadata.ts` for web app consumption
  - [x] Integrated into `scripts/pipeline.sh` generate_metadata() function
  - [x] Added variable metadata from config/variables.yaml (color ramps, units, display names)
- **Acceptance**: ✅ JSON file is valid and parseable, contains all required fields (model_run, variables, tiles, data_freshness), TypeScript types match schema
- **Completion Date**: 2026-01-11
- **Documentation**: See `scripts/generate_metadata.py` header and TypeScript types in webmaps repo

---

## Phase 6: Web Application

**Section** (or sub-project in Asana)

### TICKET-013: Create Mapbox Web Application

- **Priority**: P1 | **Effort**: L (1-3 days) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Integrated S3 weather tiles into existing `ParticleApp.tsx` component
  - [x] Load latest forecast from `metadata/latest.json` via `useWeatherMetadata` hook
  - [x] Add raster layer for weather tiles (all variables supported)
  - [x] Implement variable selector (temperature, wind, precipitation, etc.)
  - [x] Implement forecast hour selector (F00-F12)
  - [ ] Add animation controls (play/pause forecast timeline) - Deferred to TICKET-014
  - [x] Display dynamic legend with color ramps from metadata (`WeatherLegend` component)
  - [ ] Add location search - Future enhancement
  - [ ] Show current mouse cursor temperature value - Future enhancement
  - [x] Add loading states and error handling
  - [x] Auto-refresh metadata every 5 minutes
  - [x] Data freshness indicator (model run time, age)
  - [x] Opacity slider control for weather layer
  - [x] Weather layer toggle (ON/OFF)
  - [x] Updated `source.tsx` with dynamic `createWeatherSource()` function
  - [x] Preserved existing wind particle functionality alongside new weather tiles
- **Acceptance**: ✅ Map loads and displays weather tiles, can switch between variables and forecast hours, dynamic legend, coexists with wind particles
- **Completion Date**: 2026-01-11
- **Documentation**: See `ParticleApp.tsx` and `hooks/useWeatherMetadata.ts` in webmaps repo

### TICKET-013.5: Enable Multi-Hour Forecasts and Historical Runs (Prerequisite for TICKET-014)

- **Priority**: P1 | **Effort**: M (4-8 hours) | **Status**: 🟢 Complete
- **Description**: Enable the pipeline to download multiple forecast hours (F00-F06) and retain historical model runs (last 4 hours) to support time-based animation in the web app.
- **Sub-tasks**:
  - [x] Update `pipeline.sh` to download multiple forecast hours (`--fxx 0-6`)
  - [x] Add configurable `FORECAST_HOURS` parameter (default: "0-6")
  - [x] Add loop in pipeline to process each GRIB file sequentially
  - [x] Ensure tiles are organized by: `{variable}/{timestamp}/{forecast}/{z}/{x}/{y}.png`
  - [x] Update `generate_metadata.py` to scan for all available model runs (`get_available_runs()`)
  - [x] Add `available_runs` array to metadata with timestamps and forecast hours
  - [x] Keep last 4 model runs in S3 (existing lifecycle will clean up older data)
  - [x] Update `useWeatherMetadata.ts` hook to support multiple runs (`AvailableRun` interface, `getRun()`, `getLatestRun()`)
  - [ ] Test pipeline with multi-hour download on EC2
- **Acceptance**: Pipeline downloads F00-F06, metadata lists all available runs and forecast hours, web app can access historical and forecast data
- **Dependencies**: Required before TICKET-014 (Animation)
- **Completion Date**: 2026-01-11

### TICKET-014: Implement Forecast Hour Animation

- **Priority**: P2 | **Effort**: M (4-8 hours) | **Status**: 🟢 Complete
- **Dependencies**: TICKET-013.5 (Multi-Hour Forecasts)
- **Sub-tasks**:
  - [x] Create animation controller component (`ForecastAnimationController.tsx`)
  - [x] Implement play/pause controls (toggle button with visual feedback)
  - [x] Add speed control (1x, 2x, 4x cycling button)
  - [x] Create timeline slider (range input with progress visualization)
  - [x] Show current forecast validity time (calculated from model run + forecast hour)
  - [x] Preload all forecast hour tiles (`useTilePreloader.ts` hook)
  - [x] Smooth transitions between forecast hours (via existing `raster-fade-duration`)
  - [x] Loop animation option (checkbox toggle)
  - [x] Previous/Next step buttons for manual navigation
- **Acceptance**: Animation plays smoothly through all forecast hours, can pause at any hour, smooth transitions
- **Completion Date**: 2026-01-12
- **Documentation**: See `components/ForecastAnimationController.tsx` and `hooks/useTilePreloader.ts` in webmaps repo

### TICKET-015: Deploy Web Application to S3 + CloudFront

- **Priority**: P1 | **Effort**: S (<4 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Create S3 bucket for static website hosting (pre-existing)
  - [x] Configure bucket for public read access (pre-existing)
  - [x] Upload web application files (pre-existing deployment)
  - [x] Create CloudFront distribution (pre-existing)
  - [x] Configure custom domain (pre-existing)
  - [x] Set up SSL certificate with ACM (pre-existing)
  - [x] Configure CloudFront cache behaviors (pre-existing)
  - [x] Test deployment (verified working)
- **Acceptance**: ✅ Web app accessible via CloudFront URL, HTTPS enabled, fast load times globally
- **Completion Date**: 2026-01-12 (pre-existing deployment)
- **Note**: ParticleApp was already deployed prior to this project; weather tiles integration added

---

## Phase 7: Monitoring and Observability

**Section** (or sub-project in Asana)

### TICKET-016: Set Up CloudWatch Monitoring

- **Priority**: P1 | **Effort**: M (4-8 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Create custom CloudWatch metrics:
    - [x] `DataAge`: Minutes since model run
    - [x] `ProcessingTime`: Total pipeline duration
    - [x] `FilesProcessed`: Count of successful files
    - [x] `Errors`: Pipeline failure count
    - [x] `StepDuration`: Per-step timing metrics
  - [x] Configure CloudWatch Logs agent on EC2 (config/cloudwatch-agent-config.json)
  - [x] Create log groups for pipeline components (terraform/cloudwatch.tf)
  - [x] Set up log retention (30 days)
  - [x] Create metric filters for errors (7 filters)
  - [x] Create Python metrics helper module (scripts/common/cloudwatch_metrics.py)
  - [x] Enhanced pipeline.sh with comprehensive metrics
  - [x] Created CloudWatch Agent installation script
- **Acceptance**: ✅ Metrics module created, log groups configured, metric filters defined, pipeline sends detailed metrics
- **Completion Date**: 2026-01-12
- **Documentation**: `docs/TICKET-016-COMPLETE.md`

### TICKET-017: Create CloudWatch Alarms

- **Priority**: P1 | **Effort**: S (<4 hours) | **Status**: 🔴 Not Started
- **Sub-tasks**:
  - [ ] Create SNS topic for alerts
  - [ ] Subscribe email to SNS topic
  - [ ] Create alarms:
    - [ ] **Data Stale**: DataAge > 120 minutes
    - [ ] **Pipeline Failed**: Errors > 0 in last hour
    - [ ] **Processing Slow**: ProcessingTime > 35 minutes
    - [ ] **Disk Full**: EC2 disk usage > 80%
    - [ ] **S3 Storage High**: Storage > 500GB (cost control)
  - [ ] Test alarm notifications
  - [ ] Document alarm response procedures
- **Acceptance**: Alarms trigger on threshold breach, email notifications received, alarms visible in console

### TICKET-018: Create CloudWatch Dashboard

- **Priority**: P2 | **Effort**: S (<4 hours) | **Status**: 🟢 Complete
- **Sub-tasks**:
  - [x] Create CloudWatch dashboard (terraform/cloudwatch.tf)
  - [x] Add widgets for key metrics:
    - [x] Data age graph (last 24 hours)
    - [x] Processing time trend
    - [x] Files processed per run (downloaded, processed, tiles)
    - [x] Error count by type
    - [x] Step duration breakdown (stacked chart)
    - [x] EC2 CPU, memory, and disk utilization
  - [x] Add log insights queries (Recent Errors, Recent Successes)
  - [x] Dashboard URL output in Terraform
  - [x] Dashboard JSON stored in version control (terraform/cloudwatch.tf)
- **Acceptance**: ✅ Dashboard shows real-time metrics, all widgets configured, auto-updates
- **Completion Date**: 2026-01-12
- **Dashboard URL**: https://us-east-2.console.aws.amazon.com/cloudwatch/home?region=us-east-2#dashboards:name=WeatherPipeline

---

## Phase 8: Testing and Validation

**Section** (or sub-project in Asana)

### TICKET-019: Create Integration Test Suite

- **Priority**: P1 | **Effort**: M (4-8 hours) | **Status**: 🔴 Not Started
- **Sub-tasks**:
  - [ ] Create `tests/` directory
  - [ ] Write test for HRRR download:
    - [ ] Mock NOAA S3 responses
    - [ ] Verify file download
    - [ ] Verify S3 upload
  - [ ] Write test for GDAL processing:
    - [ ] Use sample GRIB2 file
    - [ ] Verify COG creation
    - [ ] Validate COG properties
  - [ ] Write test for tile generation
  - [ ] Write test for metadata generation
  - [ ] Write test for full pipeline
  - [ ] Set up pytest framework
  - [ ] Add test data fixtures
  - [ ] Create CI/CD pipeline (optional)
- **Acceptance**: All tests pass: `pytest tests/`, test coverage > 70%, tests run in <5 minutes

### TICKET-020: Perform Load Testing on Tile Serving

- **Priority**: P2 | **Effort**: M (4-8 hours) | **Status**: 🔴 Not Started
- **Sub-tasks**:
  - [ ] Set up load testing tool (Apache Bench or Locust)
  - [ ] Create test scenarios:
    - [ ] 100 concurrent users panning map
    - [ ] 1000 requests per second to tile endpoint
    - [ ] Sustained load for 10 minutes
  - [ ] Test with and without CloudFront caching
  - [ ] Measure response times (p50, p95, p99)
  - [ ] Identify bottlenecks
  - [ ] Optimize as needed
  - [ ] Document performance characteristics
- **Acceptance**: Tile endpoint handles 1000 req/s, p95 response time < 500ms, CloudFront cache hit rate > 90%

---

## Phase 9: Documentation and Operations

**Section** (or sub-project in Asana)

### TICKET-021: Write Operational Runbook

- **Priority**: P1 | **Effort**: S (<4 hours) | **Status**: 🔴 Not Started
- **Sub-tasks**:
  - [ ] Document common issues and solutions
  - [ ] Create troubleshooting flowcharts
  - [ ] Document alert response procedures
  - [ ] Create deployment checklist
  - [ ] Document rollback procedures
  - [ ] Add contact information for on-call
  - [ ] Create disaster recovery plan
- **Acceptance**: Runbook covers all common operational tasks, troubleshooting steps are clear and actionable

### TICKET-022: Create Cost Optimization Review

- **Priority**: P2 | **Effort**: S (<4 hours) | **Status**: 🔴 Not Started
- **Sub-tasks**:
  - [ ] Enable AWS Cost Explorer
  - [ ] Tag all resources with project name
  - [ ] Review actual costs after 1 week of operation
  - [ ] Identify highest cost components
  - [ ] Research optimization opportunities:
    - [ ] Use S3 Intelligent-Tiering
    - [ ] Evaluate spot instance savings
    - [ ] Review S3 lifecycle policies effectiveness
    - [ ] Evaluate CloudFront cost vs benefit
  - [ ] Document cost optimization recommendations
  - [ ] Implement approved optimizations
- **Acceptance**: Monthly cost report generated, cost breakdown by service, at least 3 optimization recommendations

---

## Phase 10: Future Enhancements

**Section** (or sub-project in Asana)

### TICKET-023: Add Wind Speed and Precipitation Variables

- **Priority**: P3 | **Effort**: L (1-3 days) | **Status**: 🔴 Not Started
- **Sub-tasks**:
  - [ ] Identify GRIB2 bands for wind speed and precipitation
  - [ ] Update download script to get additional variables
  - [ ] Create color ramps for each variable
  - [ ] Process additional bands in parallel
  - [ ] Update web app to show variable selector
  - [ ] Update S3 bucket organization for multiple variables
  - [ ] Update metadata schema
- **Acceptance**: Can display wind speed and precipitation on map, can toggle between variables, processing time increases by <50%

### TICKET-024: Implement Historical Weather Archive

- **Priority**: P3 | **Effort**: XL (>3 days) | **Status**: 🔴 Not Started
- **Sub-tasks**:
  - [ ] Extend S3 lifecycle policies for archival
  - [ ] Create Glacier storage for long-term retention
  - [ ] Build API to query historical data
  - [ ] Add date picker to web app
  - [ ] Implement historical data playback
  - [ ] Create statistics and analysis tools
- **Acceptance**: Historical data archived, can query via API, date picker works in web app

### TICKET-025: Deploy TiTiler for Dynamic Tile Generation

- **Priority**: P3 | **Effort**: L (1-3 days) | **Status**: 🔴 Not Started
- **Sub-tasks**:
  - [ ] Deploy TiTiler on ECS Fargate
  - [ ] Configure Application Load Balancer
  - [ ] Set up auto-scaling
  - [ ] Update web app to use TiTiler endpoints
  - [ ] Configure caching in CloudFront
  - [ ] Benchmark performance vs pre-generated tiles
  - [ ] Migrate if performance acceptable
- **Acceptance**: TiTiler deployed, web app uses TiTiler endpoints, performance benchmarks completed

---

## Quick Summary for Asana

**Parent Task**: Weather Data Pipeline - Automated Infrastructure

- **Total Sub-tasks**: 25 tickets organized into 10 phases
- **Completed**: 18/25 tickets (72%) ✅
- **Priority Breakdown**:
  - P0 (Critical): 7 tickets (7 complete, 0 remaining) ✅
  - P1 (High): 10 tickets (8 complete, 2 remaining)
  - P2 (Medium): 5 tickets (3 complete, 2 remaining)
  - P3 (Low): 3 tickets
- **Estimated Timeline**: ~2.5-3 weeks for core implementation
- **Current Status**:
  - ✅ **Phase 1 Complete**: All infrastructure ready (S3, EC2, Docker)
  - ✅ **Phase 2 Complete**: Data ingestion and configuration system ready
  - ✅ **Phase 3 Complete**: GRIB2 to colored COG processing pipeline working
  - ✅ **Phase 4 Complete**: Tile generation system implemented and optimized
  - ✅ **Phase 5 Complete**: Pipeline orchestration and cron automation ready
  - ✅ **Phase 6 Complete**: Web application (all tickets complete, deployed)
  - 🟡 **Phase 7 In Progress**: CloudWatch monitoring setup complete, alarms and dashboard pending

**Completed Tickets**:
- ✅ TICKET-001: AWS S3 Infrastructure (2026-01-10)
- ✅ TICKET-002: EC2 Instance Provisioning (2026-01-10)
- ✅ TICKET-003: Docker Container (2026-01-10)
- ✅ TICKET-004: HRRR Download Script with Herbie (2026-01-10)
- ✅ TICKET-005: Variable Configuration System (2026-01-10)
- ✅ TICKET-006: GRIB2 to COG Processing (2026-01-10)
- ✅ TICKET-007: Color Ramp Application (2026-01-11)
- ✅ TICKET-008: Tile Generation Strategy (2026-01-11)
- ✅ TICKET-009: Tile Generation Optimization (2026-01-11)
- ✅ TICKET-010: Master Pipeline Orchestration Script (2026-01-11)
- ✅ TICKET-011: Configure Cron Job for Hourly Execution (2026-01-11)
- ✅ TICKET-012: Create Metadata Generation Script (2026-01-11)
- ✅ TICKET-013: Create Mapbox Web Application (2026-01-11)
- ✅ TICKET-013.5: Enable Multi-Hour Forecasts and Historical Runs (2026-01-11)
- ✅ TICKET-014: Implement Forecast Hour Animation (2026-01-12)
- ✅ TICKET-015: Deploy Web Application to S3 + CloudFront (2026-01-12, pre-existing)
- ✅ TICKET-016: Set Up CloudWatch Monitoring (2026-01-12)
- ✅ TICKET-018: Create CloudWatch Dashboard (2026-01-12)

**Next Up**:
- 📝 TICKET-017: Create CloudWatch Alarms (P1, S effort)
- 📝 TICKET-019: Create Integration Test Suite (P1, M effort)

**Dependencies**:

- ✅ Phase 1 complete - Infrastructure ready
- ✅ Phase 2 complete - Data ingestion ready
- ✅ Phase 3 complete - Processing pipeline ready
- ✅ Phase 4 complete - Tile generation ready
- ✅ Phase 5 complete - Pipeline automation ready
- ✅ Phase 6 complete - Web application deployed
- 🟡 Phase 7 in progress - Monitoring (TICKET-016 done, TICKET-017/018 pending)
- Phase 8-9 can run after Phase 7 is complete (testing & docs)
- Phase 10 is future enhancements
