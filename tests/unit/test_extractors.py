"""
Unit tests for data extractors.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime


class TestAPIExtractor:
    """Tests for API extractor."""
    
    @pytest.fixture
    def sample_response(self):
        """Create sample API response."""
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "data": [
                {"id": 1, "name": "John", "email": "john@example.com"},
                {"id": 2, "name": "Jane", "email": "jane@example.com"},
            ],
            "total": 2,
        }
        response.headers = {}
        return response
    
    @patch("requests.Session")
    def test_make_request(self, mock_session, sample_response):
        """Test making HTTP request."""
        from dataflow_pro.extractors import APIExtractor
        
        mock_session.return_value.request.return_value = sample_response
        
        extractor = APIExtractor(base_url="https://api.example.com")
        response = extractor.make_request(endpoint="users")
        
        assert response.status_code == 200
        assert len(response.json()["data"]) == 2
    
    @patch("requests.Session")
    def test_extract_data(self, mock_session):
        """Test extracting data."""
        # Create mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {"id": 1, "name": "John"},
                {"id": 2, "name": "Jane"},
            ]
        }
        mock_response.headers = {}
        mock_session.return_value.request.return_value = mock_response
        
        from dataflow_pro.extractors import APIExtractor, APIExtractionResult
        
        extractor = APIExtractor(base_url="https://api.example.com")
        result = extractor.extract(endpoint="users")
        
        assert isinstance(result, APIExtractionResult)
        assert len(result.data) == 2
    
    @patch("requests.Session")
    def test_rate_limiting(self, mock_session):
        """Test rate limiting."""
        from dataflow_pro.extractors import APIExtractor
        
        extractor = APIExtractor(
            base_url="https://api.example.com",
            rate_limit_calls=10,
            rate_limit_period=60,
        )
        
        # Should not raise
        extractor._apply_rate_limit()


class TestDatabaseExtractor:
    """Tests for database extractor."""
    
    @patch("sqlalchemy.create_engine")
    def test_create_engine(self, mock_engine):
        """Test creating database engine."""
        from dataflow_pro.extractors import DatabaseExtractor
        
        extractor = DatabaseExtractor(database_url="sqlite://")
        
        assert extractor.engine is not None
    
    @patch("sqlalchemy.create_engine")
    @patch("sqlalchemy.engine.Engine.connect")
    def test_execute_query(self, mock_connect, mock_engine):
        """Test executing query."""
        from dataflow_pro.extractors import DatabaseExtractor
        
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = (1,)
        mock_result._mapping = {"count": 1}
        mock_conn.execute.return_value = mock_result
        mock_connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_connect.return_value.__exit__ = Mock(return_value=False)
        
        mock_engine.return_value.connect.return_value = mock_conn
        
        extractor = DatabaseExtractor(database_url="sqlite://")
        result = extractor.execute_query("SELECT COUNT(*) FROM users")
        
        assert result is not None
    
    def test_get_table_columns(self):
        """Test getting table columns method exists."""
        from dataflow_pro.extractors import DatabaseExtractor
        
        # Verify method exists
        extractor = DatabaseExtractor.__new__(DatabaseExtractor)
        assert hasattr(extractor, 'get_table_columns')
        assert callable(extractor.get_table_columns)


class TestFileExtractor:
    """Tests for file extractor."""
    
    def test_detect_format_csv(self):
        """Test detecting CSV format."""
        from dataflow_pro.extractors import FileExtractor, FileFormat
        
        extractor = FileExtractor()
        format = extractor._detect_format("data.csv")
        
        assert format == FileFormat.CSV
    
    def test_detect_format_json(self):
        """Test detecting JSON format."""
        from dataflow_pro.extractors import FileExtractor, FileFormat
        
        extractor = FileExtractor()
        format = extractor._detect_format("data.json")
        
        assert format == FileFormat.JSON
    
    def test_detect_format_parquet(self):
        """Test detecting Parquet format."""
        from dataflow_pro.extractors import FileExtractor, FileFormat
        
        extractor = FileExtractor()
        format = extractor._detect_format("data.parquet")
        
        assert format == FileFormat.PARQUET
    
    @patch("builtins.open", create=True)
    def test_read_csv(self, mock_open):
        """Test reading CSV file."""
        from io import StringIO
        from dataflow_pro.extractors import FileExtractor
        
        csv_content = "id,name,email\n1,john,john@example.com\n2,jane,jane@example.com"
        mock_open.return_value.__enter__.return_value = StringIO(csv_content)
        
        extractor = FileExtractor()
        data = extractor._read_csv("test.csv")
        
        assert len(data) == 2
        assert data[0]["id"] == "1"
    
    @patch("builtins.open", create=True)
    def test_read_json(self, mock_open):
        """Test reading JSON file."""
        from dataflow_pro.extractors import FileExtractor
        
        json_data = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
        mock_open.return_value.__enter__.return_value.__iter__ = Mock(
            side_factory=lambda: iter(json_data)
        )
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(json_data)
        
        with patch("json.load", return_value=json_data):
            extractor = FileExtractor()
            data = extractor._read_json("test.json")
            
            assert len(data) == 2


__all__ = ["TestAPIExtractor", "TestDatabaseExtractor", "TestFileExtractor"]