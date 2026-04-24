"""
Pytest configuration and fixtures.
"""

import pytest
import os
import sys
from pathlib import Path
import tempfile
import json


# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))


@pytest.fixture
def temp_dir():
    """Create temporary directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def sample_data():
    """Sample data for testing."""
    return [
        {"id": 1, "name": "John", "email": "john@example.com", "age": 30},
        {"id": 2, "name": "Jane", "email": "jane@example.com", "age": 25},
        {"id": 3, "name": "Bob", "email": "bob@example.com", "age": 35},
    ]


@pytest.fixture
def sample_csv(temp_dir):
    """Create sample CSV file."""
    csv_path = os.path.join(temp_dir, "test.csv")
    
    with open(csv_path, "w") as f:
        f.write("id,name,email,age\n")
        f.write("1,john,john@example.com,30\n")
        f.write("2,jane,jane@example.com,25\n")
        f.write("3,bob,bob@example.com,35\n")
    
    return csv_path


@pytest.fixture
def sample_json(temp_dir):
    """Create sample JSON file."""
    json_path = os.path.join(temp_dir, "test.json")
    
    data = [
        {"id": 1, "name": "John", "email": "john@example.com"},
        {"id": 2, "name": "Jane", "email": "jane@example.com"},
    ]
    
    with open(json_path, "w") as f:
        json.dump(data, f)
    
    return json_path


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set mock environment variables."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/testdb")
    monkeypatch.setenv("REDIS_HOST", "localhost")
    monkeypatch.setenv("REDIS_PORT", "6379")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test_key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test_secret")
    monkeypatch.setenv("S3_BUCKET", "test-bucket")


@pytest.fixture
def app_config():
    """Get app configuration."""
    from dataflow_pro.config import get_app_config
    
    return get_app_config()


@pytest.fixture
def test_db_url():
    """Get test database URL."""
    return "sqlite:///:memory:"


@pytest.fixture
def sample_records():
    """Get sample records for testing."""
    return [
        {
            "id": 1,
            "name": "John Doe",
            "email": "john@example.com",
            "created_at": "2024-01-01T00:00:00",
            "status": "active",
        },
        {
            "id": 2,
            "name": "Jane Doe",
            "email": "jane@example.com",
            "created_at": "2024-01-02T00:00:00",
            "status": "active",
        },
    ]


@pytest.fixture
def dirty_data():
    """Get dirty data for cleaning tests."""
    return [
        {"id": 1, "name": "  John  ", "email": "john@example.com", "age": 30},
        {"id": 2, "name": "Jane", "email": None, "age": "25"},
        {"id": 3, "name": None, "email": "bob@example.com", "age": None},
    ]


@pytest.fixture
def invalid_data():
    """Get invalid data for validation tests."""
    return [
        {"id": 1, "name": "John", "email": "valid@example.com", "age": 30},
        {"id": 2, "name": "Jane", "email": "not-an-email", "age": -5},
        {"id": 3, "name": "Bob", "email": "another@example.com", "age": 150},
    ]


@pytest.fixture(autouse=True)
def reset_metrics():
    """Reset metrics before each test."""
    from dataflow_pro.utils.metrics import _global_metrics
    
    # Reset global metrics
    yield
    
    # Cleanup after test
    if _global_metrics:
        _global_metrics.reset()


# Configuration for pytest
def pytest_configure(config):
    """Configure pytest."""
    config.addinivalue_line(
        "markers", "unit: Unit tests"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests"
    )
    config.addinivalue_line(
        "markers", "slow: Slow running tests"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection."""
    for item in items:
        # Add markers based on test location
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        elif "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)