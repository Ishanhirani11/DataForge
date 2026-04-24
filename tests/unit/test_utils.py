"""
Unit tests for utilities.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import logging
import time
import json


class TestLogger:
    """Tests for logger utilities."""
    
    def test_default_logger(self):
        """Test getting default logger."""
        from dataflow_pro.utils import get_logger
        
        logger = get_logger()
        
        assert logger is not None
        assert isinstance(logger, logging.Logger)
    
    def test_get_named_logger(self):
        """Test getting named logger."""
        from dataflow_pro.utils import get_logger
        
        logger = get_logger("test_logger")
        
        assert logger.name == "test_logger"
    
    def test_context_filter(self):
        """Test context filter."""
        from dataflow_pro.utils import ContextFilter
        
        filter = ContextFilter()
        
        # Set context
        ContextFilter.set_context(pipeline_name="test_pipeline", task_id="task_1")
        
        # Get context
        context = ContextFilter.get_context()
        
        assert context.get("pipeline_name") == "test_pipeline"
        
        # Clear context
        ContextFilter.clear_context()
        
        context = ContextFilter.get_context()
        assert len(context) == 0
    
    def test_json_formatter(self):
        """Test JSON formatter."""
        from dataflow_pro.utils import JSONFormatter
        
        formatter = JSONFormatter()
        
        # Create log record
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        
        result = formatter.format(record)
        
        # Should be valid JSON
        parsed = json.loads(result)
        assert parsed["message"] == "Test message"
    
    def test_colored_formatter(self):
        """Test colored formatter."""
        from dataflow_pro.utils import ColoredFormatter
        
        formatter = ColoredFormatter()
        
        # Create log record
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        
        result = formatter.format(record)
        
        # Should contain ANSI codes for colored output
        assert "Test message" in result


class TestMetrics:
    """Tests for metrics utilities."""
    
    def test_metrics_collector(self):
        """Test metrics collector."""
        from dataflow_pro.utils import MetricsCollector
        
        collector = MetricsCollector("test_service")
        
        # Increment counter
        collector.increment_counter("test_counter", 5)
        
        # Get counter
        value = collector.get_counter("test_counter")
        
        assert value == 5
    
    def test_gauge(self):
        """Test gauge metric."""
        from dataflow_pro.utils import MetricsCollector
        
        collector = MetricsCollector("test_service")
        
        # Set gauge
        collector.set_gauge("test_gauge", 100.0)
        
        # Get gauge
        value = collector.get_gauge("test_gauge")
        
        assert value == 100.0
    
    def test_timing_stats(self):
        """Test timing statistics."""
        from dataflow_pro.utils import MetricsCollector
        
        collector = MetricsCollector("test_service")
        
        # Record timings
        collector.record_timing("test_timing", 1.0)
        collector.record_timing("test_timing", 2.0)
        collector.record_timing("test_timing", 3.0)
        
        # Get stats
        stats = collector.get_timing_stats("test_timing")
        
        assert stats["count"] == 3
        assert stats["min"] == 1.0
        assert stats["max"] == 3.0
        assert stats["avg"] == 2.0
    
    def test_reset_metrics(self):
        """Test resetting metrics."""
        from dataflow_pro.utils import MetricsCollector
        
        collector = MetricsCollector("test_service")
        
        collector.increment_counter("test", 10)
        collector.set_gauge("test_gauge", 50.0)
        
        # Reset
        collector.reset()
        
        # Should be empty
        assert collector.get_counter("test") == 0
    
    def test_get_all_metrics(self):
        """Test getting all metrics."""
        from dataflow_pro.utils import MetricsCollector
        
        collector = MetricsCollector("test_service")
        
        collector.increment_counter("counter1", 5)
        collector.set_gauge("gauge1", 10.0)
        
        all_metrics = collector.get_all_metrics()
        
        assert "counters" in all_metrics
        assert "gauges" in all_metrics
    
    def test_prometheus_metrics(self):
        """Test Prometheus metrics."""
        from dataflow_pro.utils import PrometheusMetrics
        
        metrics = PrometheusMetrics("test_service")
        
        # Should initialize
        assert metrics.service_name == "test_service"


class TestRetryHandler:
    """Tests for retry handler."""
    
    def test_retry_config(self):
        """Test retry configuration."""
        from dataflow_pro.utils import RetryConfig, BackoffStrategy
        
        config = RetryConfig(
            max_attempts=3,
            min_wait=1.0,
            max_wait=30.0,
        )
        
        # Test exponential backoff
        wait_time = config.calculate_wait(0)
        assert wait_time >= 1.0
        
        wait_time = config.calculate_wait(1)
        assert wait_time >= config.calculate_wait(0)
    
    def test_linear_backoff(self):
        """Test linear backoff strategy."""
        from dataflow_pro.utils import RetryConfig, BackoffStrategy
        
        config = RetryConfig(
            strategy=BackoffStrategy.LINEAR,
            min_wait=1.0,
        )
        
        # Should increase linearly
        assert config.calculate_wait(1) > config.calculate_wait(0)
    
    def test_fibonacci_backoff(self):
        """Test Fibonacci backoff strategy."""
        from dataflow_pro.utils import RetryConfig, BackoffStrategy
        
        config = RetryConfig(
            strategy=BackoffStrategy.FIBONACCI,
            min_wait=1.0,
        )
        
        # Should use Fibonacci sequence
        wait1 = config.calculate_wait(1)
        wait2 = config.calculate_wait(2)
        
        assert wait2 > wait1
    
    def test_retry_handler_execute(self):
        """Test retry handler execution."""
        from dataflow_pro.utils import RetryHandler
        
        handler = RetryHandler()
        
        call_count = 0
        
        def failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary error")
            return "success"
        
        # Should succeed after retries
        result = handler.execute(failing_func)
        
        assert result == "success"
    
    def test_retry_handler_failure(self):
        """Test retry handler with persistent failure."""
        from dataflow_pro.utils import RetryHandler, RetryError, RetryConfig
        
        config = RetryConfig(max_attempts=3)
        handler = RetryHandler(config)
        
        def always_fails():
            raise ValueError("Always fails")
        
        # Should raise RetryError
        with pytest.raises(RetryError):
            handler.execute(always_fails)
    
    def test_retry_decorator(self):
        """Test retry decorator."""
        from dataflow_pro.utils import retry_on_exception
        
        @retry_on_exception(max_attempts=3)
        def flaky_func():
            return "success"
        
        result = flaky_func()
        
        assert result == "success"


__all__ = ["TestLogger", "TestMetrics", "TestRetryHandler"]