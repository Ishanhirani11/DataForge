"""
Integration tests for the complete data pipeline workflow.
"""
import json
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import pytest


class TestPipelineIntegration:
    """End-to-end pipeline integration tests."""

    @pytest.fixture
    def sample_data(self) -> List[Dict[str, Any]]:
        """Create sample data for pipeline testing."""
        return [
            {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30, "country": "US"},
            {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 25, "country": "UK"},
            {"id": 3, "name": "Bob Wilson", "email": "bob@example.com", "age": 35, "country": "US"},
            {"id": 4, "name": "Alice Brown", "email": "alice@example.com", "age": 28, "country": "CA"},
        ]

    @pytest.fixture
    def temp_csv_file(self, sample_data) -> str:
        """Create a temporary CSV file."""
        import csv
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=['id', 'name', 'email', 'age', 'country'])
            writer.writeheader()
            writer.writerows(sample_data)
            return f.name

    def test_extract_from_csv_file(self, temp_csv_file):
        """Test extracting data from a CSV file."""
        from dataflow_pro.extractors import FileExtractor, FileConfig, FileFormat
        
        config = FileConfig(format=FileFormat.CSV)
        extractor = FileExtractor(config=config)
        result = extractor.extract(file_path=temp_csv_file)
        
        assert result is not None
        assert len(result.data) == 4
        assert result.data[0]['name'] == 'John Doe'

    def test_transform_with_cleaner_and_validator(self, sample_data):
        """Test cleaning and validating data in sequence."""
        from dataflow_pro.transformers import DataCleaner, DataValidator
        
        # Clean data
        cleaner = DataCleaner()
        cleaned_result = cleaner.clean(sample_data)
        
        assert len(cleaned_result.data) == 4
        
        # Validate cleaned data
        validator = DataValidator()
        validator.required('name')
        validator.required('email')
        
        validation_result = validator.validate(cleaned_result.data)
        
        assert validation_result.is_valid == True
        assert validation_result.rows_valid == 4

    def test_enrich_data_with_derived_columns(self, sample_data):
        """Test enriching data with derived columns."""
        from dataflow_pro.transformers import DataEnricher
        
        enricher = DataEnricher()
        enricher.derived_column(
            name='full_name_upper',
            func=lambda row: row.get('name', '').upper()
        )
        enricher.derived_column(
            name='age_group',
            func=lambda row: 'adult' if row.get('age', 0) >= 18 else 'minor'
        )
        
        result = enricher.enrich(sample_data)
        
        assert len(result.data) == 4
        assert result.data[0]['full_name_upper'] == 'JOHN DOE'
        assert result.data[0]['age_group'] == 'adult'

    def test_full_pipeline_extract_transform_load(self, sample_data, temp_csv_file):
        """Test complete ETL pipeline."""
        from dataflow_pro.extractors import FileExtractor, FileConfig, FileFormat
        from dataflow_pro.transformers import DataCleaner, DataEnricher, DataValidator
        
        # Extract
        extract_config = FileConfig(format=FileFormat.CSV)
        extractor = FileExtractor(config=extract_config)
        extract_result = extractor.extract(file_path=temp_csv_file)
        
        # Transform - Clean
        cleaner = DataCleaner()
        clean_result = cleaner.clean(extract_result.data)
        
        # Transform - Enrich
        enricher = DataEnricher()
        enricher.add_label_encoding('country')
        enrich_result = enricher.enrich(clean_result.data)
        
        # Validate
        validator = DataValidator()
        validator.required('name')
        validation_result = validator.validate(enrich_result.data)
        
        # Pipeline completes without errors
        assert len(enrich_result.data) == 4
        assert 'country_encoded' in enrich_result.data[0]

    def test_pipeline_with_duplicate_handling(self):
        """Test pipeline handles duplicates correctly."""
        from dataflow_pro.transformers import DataCleaner
        
        data_with_duplicates = [
            {"id": 1, "name": "John"},
            {"id": 2, "name": "Jane"},
            {"id": 1, "name": "John"},  # Duplicate
        ]
        
        cleaner = DataCleaner()
        result = cleaner.clean(data_with_duplicates)
        
        # With deduplication, should have 2 rows
        assert len(result.data) == 2

    def test_pipeline_with_missing_values(self):
        """Test pipeline handles missing values."""
        from dataflow_pro.transformers import DataCleaner
        
        data_with_missing = [
            {"id": 1, "name": "John", "email": None},
            {"id": 2, "name": None, "email": "jane@example.com"},
        ]
        
        cleaner = DataCleaner()
        result = cleaner.clean(data_with_missing)
        
        # Should handle gracefully
        assert result.data is not None

    def test_pipeline_with_lookup_enrichment(self):
        """Test lookup-based enrichment."""
        from dataflow_pro.transformers import DataEnricher
        
        data = [
            {"country_code": "US"},
            {"country_code": "UK"},
        ]
        
        country_lookup = {
            "US": "United States",
            "UK": "United Kingdom",
        }
        
        enricher = DataEnricher()
        enricher.add_lookup(
            name='country_name',
            lookup_table=country_lookup,
            lookup_key='country_code',
            lookup_value='country_name'
        )
        
        result = enricher.enrich(data)
        
        assert result.data[0]['country_name'] == 'United States'
        assert result.data[1]['country_name'] == 'United Kingdom'


class TestValidatorIntegration:
    """Integration tests for validation pipeline."""

    def test_validation_with_multiple_rules(self):
        """Test validation with multiple validation rules."""
        from dataflow_pro.transformers import DataValidator
        
        validator = DataValidator()
        validator.required('email')
        validator.required('name')
        validator.check_type('age', int)
        validator.check_range('age', min_value=0, max_value=120)
        
        valid_data = [
            {"name": "John", "email": "john@example.com", "age": 30},
            {"name": "Jane", "email": "jane@example.com", "age": 25},
        ]
        
        invalid_data = [
            {"name": "John", "email": "john@example.com", "age": 30},
            {"name": None, "email": None, "age": -5},  # Multiple errors
        ]
        
        valid_result = validator.validate(valid_data)
        invalid_result = validator.validate(invalid_data)
        
        assert valid_result.is_valid == True
        assert invalid_result.is_valid == False
        assert len(invalid_result.errors) >= 3  # Missing name, missing email, invalid age

    def test_validation_with_pattern_check(self):
        """Test validation with pattern check."""
        from dataflow_pro.transformers import DataValidator
        
        validator = DataValidator()
        validator.required('email')
        validator.check_pattern('email', r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')
        
        valid_data = [{"name": "John", "email": "john@example.com"}]
        invalid_data = [{"name": "John", "email": "invalid-email"}]
        
        valid_result = validator.validate(valid_data)
        invalid_result = validator.validate(invalid_data)
        
        assert valid_result.is_valid == True
        assert invalid_result.is_valid == False


class TestErrorHandlingIntegration:
    """Integration tests for error handling."""

    def test_extractor_handles_invalid_format(self):
        """Test extractor handles invalid format gracefully."""
        from dataflow_pro.extractors import FileExtractor, FileConfig
        
        config = FileConfig()
        extractor = FileExtractor(config=config)
        
        # Should handle gracefully or raise appropriate error
        try:
            result = extractor.extract(file_path='/nonexistent/file.csv')
            # If it returns, data should be empty
            assert result.data == []
        except Exception as e:
            # Or it may raise an exception - both are acceptable
            assert True

    def test_transformer_handles_empty_data(self):
        """Test transformers handle empty data."""
        from dataflow_pro.transformers import DataCleaner, DataEnricher
        
        empty_data = []
        
        cleaner = DataCleaner()
        clean_result = cleaner.clean(empty_data)
        
        assert clean_result.data == []
        assert clean_result.rows_processed == 0
        
        enricher = DataEnricher()
        enrich_result = enricher.enrich(empty_data)
        
        assert enrich_result.data == []

    def test_validation_handles_empty_data(self):
        """Test validator handles empty data."""
        from dataflow_pro.transformers import DataValidator
        
        validator = DataValidator()
        validator.required('name')
        
        result = validator.validate([])
        
        assert result.is_valid == True
        assert result.rows_valid == 0
        assert result.rows_invalid == 0

    def test_enricher_handles_invalid_lookup(self):
        """Test enricher handles missing lookup values."""
        from dataflow_pro.transformers import DataEnricher
        
        data = [{"country_code": "XX"}]  # Unknown code
        
        country_lookup = {
            "US": "United States",
        }
        
        enricher = DataEnricher()
        enricher.add_lookup(
            name='country_name',
            lookup_table=country_lookup,
            lookup_key='country_code',
            lookup_value='country_name'
        )
        
        result = enricher.enrich(data)
        
        # Should handle gracefully - unknown key maps to None
        assert result.data[0]['country_name'] is None