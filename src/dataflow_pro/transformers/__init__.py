"""
Transformers package for DataFlow Pro.

Provides data transformation methods including:
- Data cleaning (missing values, outliers, duplicates)
- Data validation (schema, types, patterns)
- Data enrichment (derived columns, lookups, encoding)
"""

from dataflow_pro.transformers.data_cleaner import (
    MissingValueStrategy,
    OutlierStrategy,
    CleaningConfig,
    CleaningResult,
    DataCleaner,
    create_data_cleaner,
)

from dataflow_pro.transformers.data_validator import (
    ValidationType,
    ValidationSeverity,
    ValidationRule,
    ValidationError,
    ValidationResult,
    DataValidator,
    create_data_validator,
    email_validator,
    phone_validator,
)

from dataflow_pro.transformers.data_enricher import (
    EnrichmentType,
    EnrichmentConfig,
    EnrichmentResult,
    DataEnricher,
    create_data_enricher,
    calculate_age,
    extract_year,
    extract_month,
    concatenate_fields,
)

__all__ = [
    # Data Cleaner
    "MissingValueStrategy",
    "OutlierStrategy",
    "CleaningConfig",
    "CleaningResult",
    "DataCleaner",
    "create_data_cleaner",
    # Data Validator
    "ValidationType",
    "ValidationSeverity",
    "ValidationRule",
    "ValidationError",
    "ValidationResult",
    "DataValidator",
    "create_data_validator",
    "email_validator",
    "phone_validator",
    # Data Enricher
    "EnrichmentType",
    "EnrichmentConfig",
    "EnrichmentResult",
    "DataEnricher",
    "create_data_enricher",
    "calculate_age",
    "extract_year",
    "extract_month",
    "concatenate_fields",
]