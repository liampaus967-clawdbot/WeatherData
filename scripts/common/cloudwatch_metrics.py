#!/usr/bin/env python3
"""
CloudWatch Metrics Helper Module

Provides utilities for sending custom metrics to AWS CloudWatch for the
Weather Data Pipeline. Supports timing measurements, file counts, error tracking,
and custom metric dimensions.

Part of TICKET-016: Set Up CloudWatch Monitoring
"""

import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union
from functools import wraps

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# CloudWatch configuration
NAMESPACE = 'WeatherPipeline'
DEFAULT_REGION = 'us-east-1'

# Metric names
class MetricNames:
    """Standard metric names for the weather pipeline."""
    DATA_AGE = 'DataAge'
    PROCESSING_TIME = 'ProcessingTime'
    FILES_PROCESSED = 'FilesProcessed'
    FILES_DOWNLOADED = 'FilesDownloaded'
    TILES_GENERATED = 'TilesGenerated'
    ERRORS = 'Errors'
    S3_UPLOAD_SIZE = 'S3UploadSize'
    STEP_DURATION = 'StepDuration'
    SUCCESS = 'Success'
    FAILURE = 'Failure'


class MetricUnits:
    """CloudWatch metric units."""
    SECONDS = 'Seconds'
    MILLISECONDS = 'Milliseconds'
    MINUTES = 'Minutes'
    COUNT = 'Count'
    BYTES = 'Bytes'
    KILOBYTES = 'Kilobytes'
    MEGABYTES = 'Megabytes'
    NONE = 'None'


class CloudWatchMetrics:
    """
    CloudWatch metrics client for the Weather Data Pipeline.

    Usage:
        metrics = CloudWatchMetrics()

        # Send a simple metric
        metrics.put_metric('FilesProcessed', 10, unit='Count')

        # Send metric with dimensions
        metrics.put_metric('ProcessingTime', 45.5, unit='Seconds',
                          dimensions={'Step': 'COGConversion'})

        # Use timing context manager
        with metrics.timer('COGConversion'):
            process_files()

        # Use as decorator
        @metrics.timed('TileGeneration')
        def generate_tiles():
            pass
    """

    def __init__(
        self,
        namespace: str = NAMESPACE,
        region: str = DEFAULT_REGION,
        enabled: bool = True,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize CloudWatch metrics client.

        Args:
            namespace: CloudWatch namespace for metrics
            region: AWS region
            enabled: Whether to actually send metrics (useful for testing)
            logger: Logger instance for debug output
        """
        self.namespace = namespace
        self.region = region
        self.enabled = enabled
        self.logger = logger or logging.getLogger(__name__)
        self._client = None
        self._default_dimensions = {}

    @property
    def client(self):
        """Lazy initialization of CloudWatch client."""
        if self._client is None:
            try:
                self._client = boto3.client('cloudwatch', region_name=self.region)
            except NoCredentialsError:
                self.logger.warning("AWS credentials not available. Metrics disabled.")
                self.enabled = False
                self._client = None
        return self._client

    def set_default_dimensions(self, dimensions: Dict[str, str]) -> None:
        """
        Set default dimensions to include with all metrics.

        Args:
            dimensions: Dictionary of dimension name/value pairs
        """
        self._default_dimensions = dimensions

    def put_metric(
        self,
        name: str,
        value: Union[int, float],
        unit: str = MetricUnits.NONE,
        dimensions: Optional[Dict[str, str]] = None,
        timestamp: Optional[datetime] = None
    ) -> bool:
        """
        Send a metric to CloudWatch.

        Args:
            name: Metric name
            value: Metric value
            unit: CloudWatch unit (see MetricUnits class)
            dimensions: Optional dimension name/value pairs
            timestamp: Optional timestamp (defaults to now)

        Returns:
            True if metric was sent successfully, False otherwise
        """
        if not self.enabled or self.client is None:
            self.logger.debug(f"Metrics disabled. Would send: {name}={value}")
            return False

        # Merge default dimensions with provided dimensions
        all_dimensions = {**self._default_dimensions}
        if dimensions:
            all_dimensions.update(dimensions)

        # Convert dimensions to CloudWatch format
        dimension_list = [
            {'Name': k, 'Value': str(v)}
            for k, v in all_dimensions.items()
        ]

        metric_data = {
            'MetricName': name,
            'Value': value,
            'Unit': unit,
        }

        if dimension_list:
            metric_data['Dimensions'] = dimension_list

        if timestamp:
            metric_data['Timestamp'] = timestamp

        try:
            self.client.put_metric_data(
                Namespace=self.namespace,
                MetricData=[metric_data]
            )
            self.logger.debug(
                f"Sent metric: {name}={value} {unit} "
                f"(dimensions: {all_dimensions})"
            )
            return True
        except ClientError as e:
            self.logger.error(f"Failed to send metric {name}: {e}")
            return False

    def put_metrics_batch(
        self,
        metrics: List[Dict],
        dimensions: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Send multiple metrics in a single API call.

        Args:
            metrics: List of metric dicts with keys: name, value, unit (optional)
            dimensions: Optional dimensions to apply to all metrics

        Returns:
            True if metrics were sent successfully
        """
        if not self.enabled or self.client is None:
            return False

        # Merge default dimensions
        all_dimensions = {**self._default_dimensions}
        if dimensions:
            all_dimensions.update(dimensions)

        dimension_list = [
            {'Name': k, 'Value': str(v)}
            for k, v in all_dimensions.items()
        ]

        metric_data = []
        for m in metrics:
            data = {
                'MetricName': m['name'],
                'Value': m['value'],
                'Unit': m.get('unit', MetricUnits.NONE),
            }
            if dimension_list:
                data['Dimensions'] = dimension_list
            metric_data.append(data)

        # CloudWatch allows max 1000 metrics per call, split if needed
        try:
            for i in range(0, len(metric_data), 1000):
                batch = metric_data[i:i+1000]
                self.client.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=batch
                )
            self.logger.debug(f"Sent {len(metric_data)} metrics in batch")
            return True
        except ClientError as e:
            self.logger.error(f"Failed to send metrics batch: {e}")
            return False

    @contextmanager
    def timer(
        self,
        step_name: str,
        metric_name: str = MetricNames.STEP_DURATION,
        dimensions: Optional[Dict[str, str]] = None
    ):
        """
        Context manager for timing a code block and sending duration metric.

        Args:
            step_name: Name of the step being timed
            metric_name: Metric name to use
            dimensions: Additional dimensions

        Usage:
            with metrics.timer('COGConversion'):
                convert_files()
        """
        start_time = time.time()
        error_occurred = False
        try:
            yield
        except Exception as e:
            error_occurred = True
            raise
        finally:
            duration = time.time() - start_time
            dims = {'Step': step_name}
            if dimensions:
                dims.update(dimensions)
            self.put_metric(metric_name, duration, MetricUnits.SECONDS, dims)

            if error_occurred:
                self.put_metric(MetricNames.ERRORS, 1, MetricUnits.COUNT, dims)

    def timed(
        self,
        step_name: str,
        metric_name: str = MetricNames.STEP_DURATION,
        dimensions: Optional[Dict[str, str]] = None
    ):
        """
        Decorator for timing a function and sending duration metric.

        Args:
            step_name: Name of the step being timed
            metric_name: Metric name to use
            dimensions: Additional dimensions

        Usage:
            @metrics.timed('TileGeneration')
            def generate_tiles():
                pass
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                with self.timer(step_name, metric_name, dimensions):
                    return func(*args, **kwargs)
            return wrapper
        return decorator

    # Convenience methods for common metrics

    def record_data_age(self, minutes: float, model_run: Optional[str] = None) -> bool:
        """
        Record the age of the data (minutes since model run).

        Args:
            minutes: Minutes since the model run
            model_run: Optional model run identifier (e.g., '2026011212')
        """
        dims = {}
        if model_run:
            dims['ModelRun'] = model_run
        return self.put_metric(
            MetricNames.DATA_AGE,
            minutes,
            MetricUnits.MINUTES,
            dims
        )

    def record_files_processed(
        self,
        count: int,
        file_type: str = 'COG',
        step: Optional[str] = None
    ) -> bool:
        """
        Record number of files processed.

        Args:
            count: Number of files
            file_type: Type of file (COG, GRIB2, PNG, etc.)
            step: Processing step name
        """
        dims = {'FileType': file_type}
        if step:
            dims['Step'] = step
        return self.put_metric(
            MetricNames.FILES_PROCESSED,
            count,
            MetricUnits.COUNT,
            dims
        )

    def record_files_downloaded(self, count: int, source: str = 'NOAA') -> bool:
        """Record number of files downloaded."""
        return self.put_metric(
            MetricNames.FILES_DOWNLOADED,
            count,
            MetricUnits.COUNT,
            {'Source': source}
        )

    def record_tiles_generated(self, count: int, variable: Optional[str] = None) -> bool:
        """Record number of tiles generated."""
        dims = {}
        if variable:
            dims['Variable'] = variable
        return self.put_metric(
            MetricNames.TILES_GENERATED,
            count,
            MetricUnits.COUNT,
            dims
        )

    def record_error(
        self,
        step: str,
        error_type: str = 'General',
        count: int = 1
    ) -> bool:
        """
        Record an error occurrence.

        Args:
            step: Processing step where error occurred
            error_type: Type of error
            count: Number of errors (default 1)
        """
        return self.put_metric(
            MetricNames.ERRORS,
            count,
            MetricUnits.COUNT,
            {'Step': step, 'ErrorType': error_type}
        )

    def record_success(self, pipeline: str = 'HRRR') -> bool:
        """Record a successful pipeline run."""
        return self.put_metric(
            MetricNames.SUCCESS,
            1,
            MetricUnits.COUNT,
            {'Pipeline': pipeline}
        )

    def record_failure(self, pipeline: str = 'HRRR', step: Optional[str] = None) -> bool:
        """Record a pipeline failure."""
        dims = {'Pipeline': pipeline}
        if step:
            dims['FailedStep'] = step
        return self.put_metric(
            MetricNames.FAILURE,
            1,
            MetricUnits.COUNT,
            dims
        )

    def record_processing_time(
        self,
        seconds: float,
        step: Optional[str] = None,
        pipeline: str = 'HRRR'
    ) -> bool:
        """
        Record processing time.

        Args:
            seconds: Processing duration in seconds
            step: Optional step name (for step-level timing)
            pipeline: Pipeline name
        """
        dims = {'Pipeline': pipeline}
        if step:
            dims['Step'] = step
        return self.put_metric(
            MetricNames.PROCESSING_TIME,
            seconds,
            MetricUnits.SECONDS,
            dims
        )

    def record_s3_upload_size(self, bytes_uploaded: int, data_type: str = 'tiles') -> bool:
        """Record S3 upload size in bytes."""
        return self.put_metric(
            MetricNames.S3_UPLOAD_SIZE,
            bytes_uploaded,
            MetricUnits.BYTES,
            {'DataType': data_type}
        )


# Singleton instance for easy import
_default_metrics: Optional[CloudWatchMetrics] = None


def get_metrics(
    namespace: str = NAMESPACE,
    region: str = DEFAULT_REGION,
    enabled: bool = True,
    logger: Optional[logging.Logger] = None
) -> CloudWatchMetrics:
    """
    Get or create the default CloudWatch metrics instance.

    Args:
        namespace: CloudWatch namespace
        region: AWS region
        enabled: Whether metrics are enabled
        logger: Logger instance

    Returns:
        CloudWatchMetrics instance
    """
    global _default_metrics
    if _default_metrics is None:
        _default_metrics = CloudWatchMetrics(
            namespace=namespace,
            region=region,
            enabled=enabled,
            logger=logger
        )
    return _default_metrics


def reset_metrics() -> None:
    """Reset the default metrics instance (useful for testing)."""
    global _default_metrics
    _default_metrics = None


# Calculate data age utility
def calculate_data_age_minutes(model_run_time: datetime) -> float:
    """
    Calculate the age of data in minutes since model run.

    Args:
        model_run_time: The model run datetime (should be UTC)

    Returns:
        Age in minutes
    """
    now = datetime.now(timezone.utc)
    if model_run_time.tzinfo is None:
        model_run_time = model_run_time.replace(tzinfo=timezone.utc)
    delta = now - model_run_time
    return delta.total_seconds() / 60


if __name__ == '__main__':
    # Test/demo code
    import argparse

    parser = argparse.ArgumentParser(description='Test CloudWatch metrics')
    parser.add_argument('--dry-run', action='store_true', help='Disable actual metric sending')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('test')

    metrics = CloudWatchMetrics(enabled=not args.dry_run, logger=logger)
    metrics.set_default_dimensions({'Pipeline': 'HRRR', 'Environment': 'test'})

    # Test various metric types
    print("Testing CloudWatch metrics module...")

    # Simple metric
    metrics.put_metric('TestMetric', 42, MetricUnits.COUNT)

    # With dimensions
    metrics.record_files_processed(10, 'COG', 'Processing')

    # Timer context manager
    with metrics.timer('TestStep'):
        time.sleep(0.1)

    # Batch metrics
    metrics.put_metrics_batch([
        {'name': 'Metric1', 'value': 1, 'unit': MetricUnits.COUNT},
        {'name': 'Metric2', 'value': 2, 'unit': MetricUnits.COUNT},
    ])

    print("Test complete!")
