"""
Metrics collection and monitoring for DataFlow Pro.

Provides Prometheus-compatible metrics for pipeline monitoring,
including counters, gauges, histograms, and summaries.
"""

import time
from typing import Optional, Dict, Any, Callable
from functools import wraps
from contextlib import contextmanager
import threading
from collections import defaultdict

try:
    from prometheus_client import Counter, Gauge, Histogram, Summary, generate_locs, CONTENT_TYPE_LATEST
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    Counter = Gauge = Histogram = Summary = None


class MetricsCollector:
    """
    Metrics collector for pipeline monitoring.
    
    Collects and tracks various metrics including:
    - Record counts (processed, failed, skipped)
    - Processing times
    - Data quality metrics
    - Pipeline health metrics
    """
    
    def __init__(self, service_name: str = "dataflow_pro"):
        self.service_name = service_name
        self.metrics: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = {}
        self._timings: Dict[str, list] = defaultdict(list)
    
    def increment_counter(self, name: str, value: int = 1, labels: Optional[Dict[str, str]] = None) -> None:
        """
        Increment a counter metric.
        
        Args:
            name: Counter name
            value: Value to increment by
            labels: Optional labels
        """
        with self._lock:
            key = f"{name}:{labels}" if labels else name
            self._counters[key] += value
    
    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """
        Set a gauge metric.
        
        Args:
            name: Gauge name
            value: Value to set
            labels: Optional labels
        """
        with self._lock:
            key = f"{name}:{labels}" if labels else name
            self._gauges[key] = value
    
    def record_timing(self, name: str, duration: float) -> None:
        """
        Record a timing metric.
        
        Args:
            name: Timing name
            duration: Duration in seconds
        """
        with self._lock:
            self._timings[name].append(duration)
    
    def get_counter(self, name: str, labels: Optional[Dict[str, str]] = None) -> int:
        """Get counter value."""
        key = f"{name}:{labels}" if labels else name
        return self._counters.get(key, 0)
    
    def get_gauge(self, name: str, labels: Optional[Dict[str, str]] = None) -> Optional[float]:
        """Get gauge value."""
        key = f"{name}:{labels}" if labels else name
        return self._gauges.get(key)
    
    def get_timing_stats(self, name: str) -> Dict[str, float]:
        """Get timing statistics."""
        timings = self._timings.get(name, [])
        if not timings:
            return {}
        
        sorted_timings = sorted(timings)
        return {
            "count": len(timings),
            "min": min(timings),
            "max": max(timings),
            "avg": sum(timings) / len(timings),
            "p50": sorted_timings[len(sorted_timings) // 2],
            "p95": sorted_timings[int(len(sorted_timings) * 0.95)],
            "p99": sorted_timings[int(len(sorted_timings) * 0.99)],
        }
    
    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._timings.clear()
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics as dictionary."""
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "timings": {name: self.get_timing_stats(name) for name in self._timings},
            }


class PrometheusMetrics:
    """
    Prometheus metrics wrapper.
    
    Provides Prometheus-compatible metrics if available,
    otherwise uses in-memory fallback.
    """
    
    def __init__(self, service_name: str = "dataflow_pro"):
        self.service_name = service_name
        self.prometheus_available = PROMETHEUS_AVAILABLE
        
        if PROMETHEUS_AVAILABLE:
            self._setup_prometheus_metrics()
        else:
            self.collector = MetricsCollector(service_name)
    
    def _setup_prometheus_metrics(self) -> None:
        """Setup Prometheus metrics."""
        # Pipeline metrics
        self.pipeline_start = Counter(
            f"{self.service_name}_pipeline_start_total",
            "Total number of pipeline starts",
            ["pipeline_name"]
        )
        self.pipeline_success = Counter(
            f"{self.service_name}_pipeline_success_total",
            "Total number of successful pipelines",
            ["pipeline_name"]
        )
        self.pipeline_failure = Counter(
            f"{self.service_name}_pipeline_failure_total",
            "Total number of failed pipelines",
            ["pipeline_name"]
        )
        self.pipeline_duration = Histogram(
            f"{self.service_name}_pipeline_duration_seconds",
            "Pipeline duration in seconds",
            ["pipeline_name"]
        )
        
        # Task metrics
        self.task_start = Counter(
            f"{self.service_name}_task_start_total",
            "Total number of task starts",
            ["task_name", "pipeline_name"]
        )
        self.task_success = Counter(
            f"{self.service_name}_task_success_total",
            "Total number of successful tasks",
            ["task_name", "pipeline_name"]
        )
        self.task_failure = Counter(
            f"{self.service_name}_task_failure_total",
            "Total number of failed tasks",
            ["task_name", "pipeline_name"]
        )
        self.task_duration = Histogram(
            f"{self.service_name}_task_duration_seconds",
            "Task duration in seconds",
            ["task_name", "pipeline_name"]
        )
        
        # Data metrics
        self.records_processed = Counter(
            f"{self.service_name}_records_processed_total",
            "Total number of records processed",
            ["pipeline_name", "stage"]
        )
        self.records_failed = Counter(
            f"{self.service_name}_records_failed_total",
            "Total number of failed records",
            ["pipeline_name", "stage"]
        )
        self.records_quality_failed = Counter(
            f"{self.service_name}_records_quality_failed_total",
            "Total number of records failed quality checks",
            ["pipeline_name", "expectation"]
        )
        
        # Processing metrics
        self.batch_size = Gauge(
            f"{self.service_name}_batch_size",
            "Current batch size",
            ["pipeline_name"]
        )
        self.processing_rate = Gauge(
            f"{self.service_name}_processing_rate",
            "Records per second processing rate",
            ["pipeline_name"]
        )
    
    def record_pipeline_start(self, pipeline_name: str) -> None:
        """Record pipeline start."""
        if self.prometheus_available:
            self.pipeline_start.labels(pipeline_name=pipeline_name).inc()
        else:
            self.collector.increment_counter(f"pipeline_start_{pipeline_name}")
    
    def record_pipeline_success(self, pipeline_name: str) -> None:
        """Record pipeline success."""
        if self.prometheus_available:
            self.pipeline_success.labels(pipeline_name=pipeline_name).inc()
        else:
            self.collector.increment_counter(f"pipeline_success_{pipeline_name}")
    
    def record_pipeline_failure(self, pipeline_name: str) -> None:
        """Record pipeline failure."""
        if self.prometheus_available:
            self.pipeline_failure.labels(pipeline_name=pipeline_name).inc()
        else:
            self.collector.increment_counter(f"pipeline_failure_{pipeline_name}")
    
    def record_pipeline_duration(self, pipeline_name: str, duration: float) -> None:
        """Record pipeline duration."""
        if self.prometheus_available:
            self.pipeline_duration.labels(pipeline_name=pipeline_name).observe(duration)
        else:
            self.collector.record_timing(f"pipeline_duration_{pipeline_name}", duration)
    
    def record_task_start(self, task_name: str, pipeline_name: str) -> None:
        """Record task start."""
        if self.prometheus_available:
            self.task_start.labels(task_name=task_name, pipeline_name=pipeline_name).inc()
        else:
            self.collector.increment_counter(f"task_start_{pipeline_name}_{task_name}")
    
    def record_task_success(self, task_name: str, pipeline_name: str) -> None:
        """Record task success."""
        if self.prometheus_available:
            self.task_success.labels(task_name=task_name, pipeline_name=pipeline_name).inc()
        else:
            self.collector.increment_counter(f"task_success_{pipeline_name}_{task_name}")
    
    def record_task_failure(self, task_name: str, pipeline_name: str) -> None:
        """Record task failure."""
        if self.prometheus_available:
            self.task_failure.labels(task_name=task_name, pipeline_name=pipeline_name).inc()
        else:
            self.collector.increment_counter(f"task_failure_{pipeline_name}_{task_name}")
    
    def record_task_duration(self, task_name: str, pipeline_name: str, duration: float) -> None:
        """Record task duration."""
        if self.prometheus_available:
            self.task_duration.labels(
                task_name=task_name,
                pipeline_name=pipeline_name
            ).observe(duration)
        else:
            self.collector.record_timing(f"task_duration_{pipeline_name}_{task_name}", duration)
    
    def record_processed(self, pipeline_name: str, stage: str, count: int) -> None:
        """Record processed records."""
        if self.prometheus_available:
            self.records_processed.labels(
                pipeline_name=pipeline_name,
                stage=stage
            ).inc(count)
        else:
            self.collector.increment_counter(f"records_processed_{pipeline_name}_{stage}", count)
    
    def record_failed(self, pipeline_name: str, stage: str, count: int) -> None:
        """Record failed records."""
        if self.prometheus_available:
            self.records_failed.labels(
                pipeline_name=pipeline_name,
                stage=stage
            ).inc(count)
        else:
            self.collector.increment_counter(f"records_failed_{pipeline_name}_{stage}", count)
    
    def record_quality_failure(
        self,
        pipeline_name: str,
        expectation: str,
        count: int = 1
    ) -> None:
        """Record quality check failure."""
        if self.prometheus_available:
            self.records_quality_failed.labels(
                pipeline_name=pipeline_name,
                expectation=expectation
            ).inc(count)
        else:
            self.collector.increment_counter(
                f"records_quality_failed_{pipeline_name}_{expectation}",
                count
            )
    
    def set_batch_size(self, pipeline_name: str, size: int) -> None:
        """Set batch size gauge."""
        if self.prometheus_available:
            self.batch_size.labels(pipeline_name=pipeline_name).set(size)
        else:
            self.collector.set_gauge(f"batch_size_{pipeline_name}", size)
    
    def set_processing_rate(self, pipeline_name: str, rate: float) -> None:
        """Set processing rate gauge."""
        if self.prometheus_available:
            self.processing_rate.labels(pipeline_name=pipeline_name).set(rate)
        else:
            self.collector.set_gauge(f"processing_rate_{pipeline_name}", rate)


class Timer:
    """
    Context manager for timing code execution.
    
    Example:
        with Timer("pipeline"):
            # code to time
            pass
    """
    
    def __init__(self, name: str, metrics: Optional[PrometheusMetrics] = None):
        self.name = name
        self.metrics = metrics
        self.start_time: Optional[float] = None
        self.duration: Optional[float] = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.duration = time.time() - self.start_time
        if self.metrics:
            self.metrics.record_task_duration(
                self.name,
                "default",
                self.duration
            )
        return False


def timed(
    func: Optional[Callable] = None,
    *,
    metric_name: Optional[str] = None,
    pipeline_name: str = "default"
) -> Callable:
    """
    Decorator for timing function execution.
    
    Example:
        @timed(metric_name="process_data", pipeline_name="etl")
        def process_data(data):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                duration = time.time() - start_time
                name = metric_name or func.__name__
                # Could integrate with global metrics here
        return wrapper
    
    if func is None:
        return decorator
    return decorator(func)


# Global metrics instance
_global_metrics: Optional[PrometheusMetrics] = None


def get_metrics(service_name: str = "dataflow_pro") -> PrometheusMetrics:
    """
    Get global metrics instance.
    
    Args:
        service_name: Service name
    
    Returns:
        PrometheusMetrics: Metrics instance
    """
    global _global_metrics
    if _global_metrics is None:
        _global_metrics = PrometheusMetrics(service_name)
    return _global_metrics


def metrics_context(
    name: str,
    pipeline_name: Optional[str] = None
) -> MetricsCollector:
    """
    Get metrics context for a pipeline.
    
    Args:
        name: Pipeline name
        pipeline_name: Optional pipeline name
    
    Returns:
        MetricsCollector: Metrics collector instance
    """
    return MetricsCollector(name)