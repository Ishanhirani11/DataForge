"""
Great Expectations Suite Configuration for DataFlow Pro.

This module provides data quality expectations suite
for validating data at various pipeline stages.
"""

import logging
from typing import Dict, List, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

# Try to import Great Expectations
try:
    import great_expectations as gx
    from great_expectations.core.expectation_configuration import ExpectationConfiguration
    from great_expectations.dataset import PandasDataset
    from great_expectations.data_context import DataContext
    from great_expectations.core import ExpectationSuite
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.expectations import (
        expect_column_values_to_be_between,
        expect_column_values_to_not_be_null,
        expect_column_values_to_be_unique,
        expect_column_values_to_match_regex,
        expect_table_row_count_to_be_between,
        expect_table_columns_to_match_set,
    )
    GX_AVAILABLE = True
except ImportError:
    GX_AVAILABLE = False


# Default Expectations Configuration
DEFAULT_SUITE_NAME = "dataflow_pro_suite"

# Column Expectations
COLUMN_EXPECTATIONS = {
    "id": {
        "not_null": True,
        "unique": True,
        "type": "integer",
    },
    "email": {
        "not_null": True,
        "regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    },
    "name": {
        "not_null": True,
        "min_length": 1,
        "max_length": 255,
    },
    "created_at": {
        "not_null": True,
        "datetime": True,
    },
    "updated_at": {
        "datetime": True,
    },
    "status": {
        "not_null": True,
        "values_in": ["active", "inactive", "pending"],
    },
}

# Table Expectations
TABLE_EXPECTATIONS = {
    "min_rows": 0,
    "max_rows": 10000000,
    "required_columns": ["id", "created_at"],
}


class DataQualitySuite:
    """
    Data Quality Suite using Great Expectations.
    
    Provides configurable validation with multiple
    expectations for data quality assurance.
    """
    
    def __init__(
        self,
        suite_name: str = DEFAULT_SUITE_NAME,
        data_context_dir: Optional[str] = None,
    ):
        self.suite_name = suite_name
        self.data_context_dir = data_context_dir
        self.gx_available = GX_AVAILABLE
        
        if not GX_AVAILABLE:
            logger.warning("Great Expectations not available, using fallback validation")
            return
        
        # Initialize data context
        if data_context_dir:
            self.data_context = DataContext.create(data_context_dir)
        else:
            # Create ephemeral context
            self.data_context = None
        
        # Create expectation suite
        self.suite = ExpectationSuite(suite_name)
    
    def add_column_expectations(
        self,
        column: str,
        expectations: Dict[str, Any],
    ) -> None:
        """Add expectations for a column."""
        if not self.gx_available:
            return
        
        # Not null expectation
        if expectations.get("not_null"):
            self.suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={"column": column},
                )
            )
        
        # Unique expectation
        if expectations.get("unique"):
            self.suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_unique",
                    kwargs={"column": column},
                )
            )
        
        # Range expectation (for numeric)
        if "min" in expectations or "max" in expectations:
            self.suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": column,
                        "min_value": expectations.get("min"),
                        "max_value": expectations.get("max"),
                    },
                )
            )
        
        # Regex expectation
        if expectations.get("regex"):
            self.suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_match_regex",
                    kwargs={
                        "column": column,
                        "regex": expectations["regex"],
                    },
                )
            )
        
        # Length expectations
        if "min_length" in expectations or "max_length" in expectations:
            self.suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_value_lengths_to_be_between",
                    kwargs={
                        "column": column,
                        "min_value": expectations.get("min_length"),
                        "max_value": expectations.get("max_length"),
                    },
                )
            )
        
        # Values in expectation
        if expectations.get("values_in"):
            self.suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_in_set",
                    kwargs={
                        "column": column,
                        "value_set": expectations["values_in"],
                    },
                )
            )
    
    def add_table_expectations(
        self,
        expectations: Dict[str, Any],
    ) -> None:
        """Add table-level expectations."""
        if not self.gx_available:
            return
        
        # Row count expectations
        if "min_rows" in expectations or "max_rows" in expectations:
            self.suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_table_row_count_to_be_between",
                    kwargs={
                        "min_value": expectations.get("min_rows"),
                        "max_value": expectations.get("max_rows"),
                    },
                )
            )
        
        # Required columns
        if "required_columns" in expectations:
            self.suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_table_columns_to_match_set",
                    kwargs={
                        "column_set": expectations["required_columns"],
                    },
                )
            )
    
    def validate_data(
        self,
        data: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Validate data against expectations.
        
        Args:
            data: Data to validate
        
        Returns:
            Dict with validation results
        """
        if not data:
            return {
                "success": True,
                "results": [],
                "statistics": {"evaluated_count": 0},
            }
        
        if not self.gx_available:
            # Fallback validation
            return self._fallback_validation(data)
        
        # Create dataset
        dataset = PandasDataset(data)
        
        # Run validation
        results = dataset.validate(
            expectation_suite=self.suite,
            only_return\Exception": False,
        )
        
        return self._parse_results(results)
    
    def _fallback_validation(
        self,
        data: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Fallback validation without Great Expectations."""
        import json
        
        results = []
        errors = []
        
        for expectation_name, config in COLUMN_EXPECTATIONS.items():
            column_expects = config
            
            # Check if column exists in data
            if data and expectation_name not in data[0]:
                continue
            
            # Not null check
            if column_expects.get("not_null", False):
                null_count = sum(
                    1 for row in data
                    if row.get(expectation_name) is None
                )
                
                if null_count > 0:
                    errors.append({
                        "expectation": "expect_column_values_to_not_be_null",
                        "column": expectation_name,
                        "success": False,
                        "unexpected_count": null_count,
                    })
                else:
                    results.append({
                        "expectation": "expect_column_values_to_not_be_null",
                        "column": expectation_name,
                        "success": True,
                    })
            
            # Unique check
            if column_expects.get("unique", False):
                values = [row.get(expectation_name) for row in data]
                unique_values = set(values)
                
                if len(values) != len(unique_values):
                    errors.append({
                        "expectation": "expect_column_values_to_be_unique",
                        "column": expectation_name,
                        "success": False,
                        "unexpected_count": len(values) - len(unique_values),
                    })
        
        return {
            "success": len(errors) == 0,
            "results": results + errors,
            "statistics": {
                "evaluated_count": len(data),
                "expectation_count": len(results) + len(errors),
            },
        }
    
    def _parse_results(
        self,
        results: Any,
    ) -> Dict[str, Any]:
        """Parse Great Expectations results."""
        return {
            "success": results.success,
            "results": results.results,
            "statistics": results.statistics,
        }


def create_quality_suite(
    suite_name: str = "dataflow_pro_suite",
    data_context_dir: Optional[str] = None,
) -> DataQualitySuite:
    """
    Create data quality suite with default expectations.
    
    Args:
        suite_name: Name of the suite
        data_context_dir: Great Expectations data context directory
    
    Returns:
        DataQualitySuite: Configured suite
    """
    suite = DataQualitySuite(suite_name, data_context_dir)
    
    # Add column expectations
    for column, expectations in COLUMN_EXPECTATIONS.items():
        suite.add_column_expectations(column, expectations)
    
    # Add table expectations
    suite.add_table_expectations(TABLE_EXPECTATIONS)
    
    return suite


# CLI for running validation
def validate_from_cli(
    data_file: str,
    suite_name: str = DEFAULT_SUITE_NAME,
) -> Dict[str, Any]:
    """
    Validate data from CLI.
    
    Args:
        data_file: Path to data file (CSV, JSON)
        suite_name: Name of expectation suite
    
    Returns:
        Dict: Validation results
    """
    import json
    
    # Load data
    if data_file.endswith(".json"):
        with open(data_file) as f:
            data = json.load(f)
    else:
        import pandas as pd
        data = pd.read_csv(data_file).to_dict(orient="records")
    
    # Create suite
    suite = create_quality_suite(suite_name)
    
    # Validate
    return suite.validate_data(data)


# Example usage
if __name__ == "__main__":
    import tempfile
    
    # Sample data
    sample_data = [
        {"id": 1, "name": "John", "email": "john@example.com", "status": "active"},
        {"id": 2, "name": "Jane", "email": "jane@example.com", "status": "active"},
    ]
    
    # Create suite
    suite = create_quality_suite()
    
    # Validate
    results = suite.validate_data(sample_data)
    
    print(f"Validation Success: {results['success']}")
    print(f"Results: {results['statistics']}")