"""
Unit tests for data transformers.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json


class TestDataCleaner:
    """Tests for data cleaner."""
    
    @pytest.fixture
    def sample_data(self):
        """Create sample data."""
        return [
            {"id": 1, "name": "John", "email": "john@example.com", "age": 30},
            {"id": 2, "name": "Jane", "email": "jane@example.com", "age": None},
            {"id": 3, "name": None, "email": "bob@example.com", "age": 25},
        ]
    
    def test_remove_duplicates(self, sample_data):
        """Test removing duplicate rows."""
        from dataflow_pro.transformers import DataCleaner, MissingValueStrategy
        
        data = sample_data + [sample_data[0]]  # Add duplicate
        
        cleaner = DataCleaner()
        cleaned_data, dropped = cleaner._remove_duplicates(data)
        
        # Should have duplicates removed
        assert dropped > 0
    
    def test_handle_missing_values_drop(self, sample_data):
        """Test handling missing values (drop)."""
        from dataflow_pro.transformers import DataCleaner, MissingValueStrategy
        
        cleaner = DataCleaner()
        cleaner.config.missing_value_strategy = MissingValueStrategy.DROP
        
        cleaned_data, modified = cleaner._handle_missing_values(sample_data)
        
        # Should drop rows with missing values
        assert len(cleaned_data) < len(sample_data)
    
    def test_handle_missing_values_fill(self, sample_data):
        """Test handling missing values (fill)."""
        from dataflow_pro.transformers import DataCleaner, MissingValueStrategy
        
        cleaner = DataCleaner()
        cleaner.config.missing_value_strategy = MissingValueStrategy.FILL_DEFAULT
        cleaner.config.missing_value_default = "N/A"
        
        cleaned_data, modified = cleaner._handle_missing_values(sample_data)
        
        # Should have filled values
        assert all(row.get("name") is not None for row in cleaned_data)
    
    def test_clean_strings(self, sample_data):
        """Test cleaning string values."""
        from dataflow_pro.transformers import DataCleaner
        
        data_with_whitespace = [
            {"name": "  John  "},
            {"name": "Jane"},
        ]
        
        cleaner = DataCleaner()
        cleaner.config.strip_whitespace = True
        cleaner.config.lowercase = False
        
        cleaned = cleaner._clean_strings(data_with_whitespace)
        
        # Should have stripped whitespace
        assert cleaned[0]["name"] == "John"
    
    @pytest.mark.parametrize("value,expected", [
        (5, True),
        (5.5, True),
        ("5", True),  # String numbers are also numeric
        ("abc", False),
    ])
    def test_is_numeric(self, value, expected):
        """Test numeric detection."""
        from dataflow_pro.transformers import DataCleaner
        
        cleaner = DataCleaner()
        
        assert cleaner._is_numeric(value) == expected
    
    def test_clean_returns_result(self, sample_data):
        """Test clean method returns CleaningResult."""
        from dataflow_pro.transformers import DataCleaner
        
        cleaner = DataCleaner()
        result = cleaner.clean(sample_data)
        
        assert result.rows_processed == len(sample_data)
        assert result.metadata is not None


class TestDataValidator:
    """Tests for data validator."""
    
    def test_required_validation(self):
        """Test required field validation."""
        from dataflow_pro.transformers import DataValidator, ValidationType, ValidationSeverity
        
        validator = DataValidator()
        validator.required("name")
        
        data = [
            {"name": "John", "email": "john@example.com"},
            {"email": "jane@example.com"},  # Missing name
        ]
        
        result = validator.validate(data)
        
        # Should have error on second row
        errors = [e for e in result.errors if e.column == "name"]
        assert len(errors) > 0
    
    def test_type_validation(self):
        """Test type validation."""
        from dataflow_pro.transformers import DataValidator
        
        validator = DataValidator()
        validator.check_type("age", int)
        
        data = [{"age": 30}, {"age": "30"}]
        
        result = validator.validate(data)
        
        # Should have error on second row
        errors = [e for e in result.errors if e.column == "age"]
        assert len(errors) == 1
    
    def test_range_validation(self):
        """Test range validation."""
        from dataflow_pro.transformers import DataValidator
        
        validator = DataValidator()
        validator.check_range("age", min_value=0, max_value=120)
        
        data = [
            {"age": 30},
            {"age": 150},
        ]
        
        result = validator.validate(data)
        
        # Should have error on second row
        errors = [e for e in result.errors if e.column == "age"]
        assert len(errors) == 1
    
    def test_pattern_validation(self):
        """Test pattern validation."""
        from dataflow_pro.transformers import DataValidator
        
        validator = DataValidator()
        validator.check_pattern("email", r"^[\w.-]+@[\w.-]+\.\w+$")
        
        data = [
            {"email": "john@example.com"},
            {"email": "invalid"},
        ]
        
        result = validator.validate(data)
        
        # Should have error on second row
        errors = [e for e in result.errors if e.column == "email"]
        assert len(errors) == 1
    
    def test_enum_validation(self):
        """Test enum validation."""
        from dataflow_pro.transformers import DataValidator
        
        validator = DataValidator()
        validator.check_enum("status", ["active", "inactive"])
        
        data = [
            {"status": "active"},
            {"status": "pending"},
        ]
        
        result = validator.validate(data)
        
        # Should have error on second row
        errors = [e for e in result.errors if e.column == "status"]
        assert len(errors) == 1
    
    def test_custom_validation(self):
        """Test custom validation."""
        from dataflow_pro.transformers import DataValidator
        
        validator = DataValidator()
        validator.add_custom("age", lambda x: x > 0, "Age must be positive")
        
        data = [
            {"age": 30},
            {"age": -1},
        ]
        
        result = validator.validate(data)
        
        # Should have error on second row
        errors = [e for e in result.errors if e.column == "age"]
        assert len(errors) == 1
    
    def test_valid_data_no_errors(self):
        """Test valid data produces no errors."""
        from dataflow_pro.transformers import DataValidator
        
        validator = DataValidator()
        validator.required("name")
        validator.check_type("age", int)
        
        data = [
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 25},
        ]
        
        result = validator.validate(data)
        
        assert result.is_valid
    
    def test_get_valid_rows(self):
        """Test separating valid and invalid rows."""
        from dataflow_pro.transformers import DataValidator
        
        validator = DataValidator()
        validator.required("name")
        
        data = [
            {"name": "John"},
            {"name": "Jane"},
            {},  # Invalid
        ]
        
        valid_rows, invalid_rows = validator.get_valid_rows(data)
        
        assert len(valid_rows) == 2
        assert len(invalid_rows) == 1


class TestDataEnricher:
    """Tests for data enricher."""
    
    def test_derived_column(self):
        """Test adding derived column."""
        from dataflow_pro.transformers import DataEnricher, EnrichmentType
        
        enricher = DataEnricher()
        enricher.derived_column(
            name="full_name",
            func=lambda row: f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
        )
        
        data = [
            {"first_name": "John", "last_name": "Doe"},
        ]
        
        result = enricher.enrich(data)
        
        assert "full_name" in result.data[0]
        assert result.data[0]["full_name"] == "John Doe"
    
    def test_lookup_enrichment(self):
        """Test lookup enrichment."""
        from dataflow_pro.transformers import DataEnricher
        
        lookup = {
            "US": "United States",
            "UK": "United Kingdom",
        }
        
        enricher = DataEnricher()
        enricher.add_lookup(
            name="country_name",
            lookup_table=lookup,
            lookup_key="country_code",
            lookup_value="country_name",
        )
        
        data = [
            {"country_code": "US"},
            {"country_code": "UK"},
        ]
        
        result = enricher.enrich(data)
        
        assert result.data[0]["country_name"] == "United States"
        assert result.data[1]["country_name"] == "United Kingdom"
    
    def test_label_encoding(self):
        """Test label encoding."""
        from dataflow_pro.transformers import DataEnricher
        
        enricher = DataEnricher()
        enricher.add_label_encoding("status")
        
        data = [
            {"status": "active"},
            {"status": "inactive"},
            {"status": "active"},
        ]
        
        result = enricher.enrich(data)
        
        # Should have encoded column
        assert "status_encoded" in result.data[0]
    
    def test_binning(self):
        """Test binning enrichment."""
        from dataflow_pro.transformers import DataEnricher
        
        enricher = DataEnricher()
        enricher.add_binning(
            source_column="age",
            bin_ranges=[18, 30, 60],
            bin_labels=["minor", "adult", "senior"],
        )
        
        data = [
            {"age": 15},
            {"age": 25},
            {"age": 45},
            {"age": 70},
        ]
        
        result = enricher.enrich(data)
        
        # Should have binned column
        assert "bin_age" in result.data[0]
    
    def test_normalization(self):
        """Test normalization enrichment."""
        from dataflow_pro.transformers import DataEnricher
        
        enricher = DataEnricher()
        enricher.add_normalization("score", "minmax")
        
        data = [
            {"score": 10},
            {"score": 20},
            {"score": 30},
        ]
        
        result = enricher.enrich(data)
        
        # Should have normalized column
        assert "norm_score" in result.data[0]
        # Values should be between 0 and 1
        assert 0 <= result.data[0]["norm_score"] <= 1
    
    def test_add_custom_enrichment(self):
        """Test custom enrichment."""
        from dataflow_pro.transformers import DataEnricher
        
        def uppercase_name(row):
            row["name_upper"] = row.get("name", "").upper()
            return row
        
        enricher = DataEnricher()
        enricher.add_custom("uppercase", uppercase_name)
        
        data = [{"name": "john"}]
        
        result = enricher.enrich(data)
        
        assert result.data[0].get("name_upper") == "JOHN"


__all__ = ["TestDataCleaner", "TestDataValidator", "TestDataEnricher"]