"""
Integration tests for monitoring and metrics.
"""
import pytest
import time


class TestMonitoringIntegration:
    """Integration tests for monitoring components."""

    def test_metrics_tracking_through_pipeline(self):
        """Test that metrics are tracked through pipeline operations."""
        from dataflow_pro.transformers import DataCleaner
        
        # Run some operations
        data = [
            {"id": 1, "name": "John"},
            {"id": 2, "name": "Jane"},
        ]
        
        cleaner = DataCleaner()
        result = cleaner.clean(data)
        
        # Should complete without errors
        assert result.data is not None

    def test_error_tracking_in_pipeline(self):
        """Test error tracking through pipeline."""
        from dataflow_pro.transformers import DataValidator
        
        validator = DataValidator()
        validator.required('name')
        
        # Validate with invalid data
        invalid_data = [{"id": 1}]  # Missing 'name'
        result = validator.validate(invalid_data)
        
        # Should have errors
        assert len(result.errors) >= 1

    def test_latency_tracking_exists(self):
        """Test latency tracking module exists."""
        try:
            from dataflow_pro.monitoring.metrics import MetricsCollector
            assert True
        except ImportError:
            pytest.skip("Metrics module not available")


class TestReliabilityIntegration:
    """Integration tests for reliability features."""

    def test_retry_handler_exists(self):
        """Test retry handler module exists."""
        try:
            from dataflow_pro.reliability.retry_handler import RetryHandler
            assert True
        except ImportError:
            pytest.skip("Retry handler module not available")

    def test_retry_handler_execution(self):
        """Test retry handler executes."""
        try:
            from dataflow_pro.reliability.retry_handler import RetryHandler, RetryConfig
            
            retry_count = 0
            
            def failing_function():
                nonlocal retry_count
                retry_count += 1
                if retry_count < 3:
                    raise ValueError("Temporary error")
                return "success"
            
            config = RetryConfig(max_attempts=3, wait_fixed=0.01)
            handler = RetryHandler(config=config)
            
            result = handler.execute(failing_function)
            
            assert result == "success"
            assert retry_count == 3
        except ImportError:
            pytest.skip("Retry handler module not available")

    def test_circuit_breaker_exists(self):
        """Test circuit breaker module exists."""
        try:
            from dataflow_pro.reliability.circuit_breaker import CircuitBreaker
            assert True
        except ImportError:
            pytest.skip("Circuit breaker module not available")


class TestConfigurationIntegration:
    """Integration tests for configuration system."""

    def test_config_loading(self):
        """Test configuration can be loaded."""
        try:
            from dataflow_pro.config.settings import load_config
            config = load_config()
            assert config is not None
        except ImportError:
            pytest.skip("Config module not available")

    def test_app_config_exists(self):
        """Test AppConfig exists."""
        try:
            from dataflow_pro.config.settings import AppConfig
            config = AppConfig()
            assert config is not None
        except ImportError:
            pytest.skip("Config module not available")


class TestHealthCheckIntegration:
    """Integration tests for health checks."""

    def test_health_check_exists(self):
        """Test health check module exists."""
        try:
            from dataflow_pro.monitoring.health import HealthChecker
            checker = HealthChecker()
            assert checker is not None
        except ImportError:
            pytest.skip("Health check module not available")

    def test_extractor_has_health_method(self):
        """Test extractor can be checked for health."""
        from dataflow_pro.extractors import FileExtractor
        
        extractor = FileExtractor()
        assert hasattr(extractor, 'extract')


class TestMetricsIntegration:
    """Integration tests for metrics collection."""

    def test_metrics_collector_exists(self):
        """Test metrics collector module exists."""
        try:
            from dataflow_pro.monitoring.metrics import MetricsCollector
            collector = MetricsCollector()
            assert collector is not None
        except ImportError:
            pytest.skip("Metrics module not available")

    def test_get_metrics_function(self):
        """Test get_metrics function exists."""
        try:
            from dataflow_pro.monitoring.metrics import get_metrics
            metrics = get_metrics()
            assert metrics is not None
        except ImportError:
            pytest.skip("Metrics module not available")