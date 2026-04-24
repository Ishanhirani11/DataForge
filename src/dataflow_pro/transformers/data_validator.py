"""
Data validator transformer for DataFlow Pro.

Provides comprehensive data validation functions including:
- Schema validation
- Type checking
- Range validation
- Pattern matching
- Custom validation rules
- Business rule validation
"""

from typing import Any, Dict, Iterator, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
import re
from datetime import datetime
import json

from dataflow_pro.utils.logger import get_logger
from dataflow_pro.utils.metrics import get_metrics


logger = get_logger(__name__)


class ValidationType(str, Enum):
    """Validation types."""
    REQUIRED = "required"
    TYPE = "type"
    RANGE = "range"
    PATTERN = "pattern"
    ENUM = "enum"
    CUSTOM = "custom"
    LENGTH = "length"
    SCHEMA = "schema"


class ValidationSeverity(str, Enum):
    """Validation severity levels."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationRule:
    """Single validation rule."""
    column: str
    validation_type: ValidationType
    severity: ValidationSeverity = ValidationSeverity.ERROR
    message: Optional[str] = None
    
    # For type validation
    expected_type: Optional[type] = None
    
    # For range validation
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    
    # For pattern validation
    pattern: Optional[str] = None
    
    # For enum validation
    allowed_values: Optional[List[Any]] = None
    
    # For custom validation
    custom_func: Optional[Callable] = None
    
    # For length validation
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    
    # For schema validation
    schema: Optional[Dict[str, Any]] = None


@dataclass
class ValidationError:
    """Validation error."""
    column: str
    row: int
    value: Any
    validation_type: ValidationType
    severity: ValidationSeverity
    message: str


@dataclass
class ValidationResult:
    """Result of data validation."""
    is_valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    rows_valid: int = 0
    rows_invalid: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataValidator:
    """
    Data validator with comprehensive validation functions.
    
    Supports:
    - Required field validation
    - Type checking
    - Range validation
    - Pattern matching (regex)
    - Enumeration validation
    - Length validation
    - Custom validation functions
    - Schema validation
    """
    
    def __init__(self):
        self.rules: List[ValidationRule] = []
        self.metrics = get_metrics()
        self._schema_validators: Dict[str, Callable] = {}
    
    def add_rule(self, rule: ValidationRule) -> "DataValidator":
        """
        Add validation rule.
        
        Args:
            rule: Validation rule
        
        Returns:
            Self for chaining
        """
        self.rules.append(rule)
        return self
    
    def add_rules(self, rules: List[ValidationRule]) -> "DataValidator":
        """Add multiple validation rules."""
        self.rules.extend(rules)
        return self
    
    def required(
        self,
        column: str,
        message: Optional[str] = None,
    ) -> "DataValidator":
        """Add required field validation."""
        rule = ValidationRule(
            column=column,
            validation_type=ValidationType.REQUIRED,
            message=message or f"{column} is required",
        )
        return self.add_rule(rule)
    
    def check_type(
        self,
        column: str,
        expected_type: type,
        message: Optional[str] = None,
    ) -> "DataValidator":
        """Add type validation."""
        rule = ValidationRule(
            column=column,
            validation_type=ValidationType.TYPE,
            expected_type=expected_type,
            message=message or f"{column} must be of type {expected_type.__name__}",
        )
        return self.add_rule(rule)
    
    def check_range(
        self,
        column: str,
        min_value: Optional[Union[int, float]] = None,
        max_value: Optional[Union[int, float]] = None,
        message: Optional[str] = None,
    ) -> "DataValidator":
        """Add range validation."""
        rule = ValidationRule(
            column=column,
            validation_type=ValidationType.RANGE,
            min_value=min_value,
            max_value=max_value,
            message=message or f"{column} must be between {min_value} and {max_value}",
        )
        return self.add_rule(rule)
    
    def check_pattern(
        self,
        column: str,
        pattern: str,
        message: Optional[str] = None,
    ) -> "DataValidator":
        """Add pattern validation."""
        rule = ValidationRule(
            column=column,
            validation_type=ValidationType.PATTERN,
            pattern=pattern,
            message=message or f"{column} does not match pattern {pattern}",
        )
        return self.add_rule(rule)
    
    def check_enum(
        self,
        column: str,
        allowed_values: List[Any],
        message: Optional[str] = None,
    ) -> "DataValidator":
        """Add enum validation."""
        rule = ValidationRule(
            column=column,
            validation_type=ValidationType.ENUM,
            allowed_values=allowed_values,
            message=message or f"{column} must be one of {allowed_values}",
        )
        return self.add_rule(rule)
    
    def check_length(
        self,
        column: str,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        message: Optional[str] = None,
    ) -> "DataValidator":
        """Add length validation."""
        rule = ValidationRule(
            column=column,
            validation_type=ValidationType.LENGTH,
            min_length=min_length,
            max_length=max_length,
            message=message or f"{column} length must be between {min_length} and {max_length}",
        )
        return self.add_rule(rule)
    
    def add_custom(
        self,
        column: str,
        custom_func: Callable[[Any], bool],
        message: str = "Custom validation failed",
    ) -> "DataValidator":
        """Add custom validation."""
        rule = ValidationRule(
            column=column,
            validation_type=ValidationType.CUSTOM,
            custom_func=custom_func,
            message=message,
        )
        return self.add_rule(rule)
    
    def validate(
        self,
        data: List[Dict[str, Any]],
    ) -> ValidationResult:
        """
        Validate data.
        
        Args:
            data: Data to validate
        
        Returns:
            ValidationResult: Validation result
        """
        if not data:
            return ValidationResult(is_valid=True, rows_valid=0)
        
        logger.info(f"Validating {len(data)} rows")
        
        errors: List[ValidationError] = []
        warnings: List[ValidationError] = []
        
        for i, row in enumerate(data):
            row_errors, row_warnings = self._validate_row(row, i)
            errors.extend(row_errors)
            warnings.extend(row_warnings)
        
        # Determine validity
        has_errors = any(e.severity == ValidationSeverity.ERROR for e in errors)
        is_valid = not has_errors
        
        rows_valid = len(data) - len([e for e in errors if e.severity == ValidationSeverity.ERROR])
        rows_invalid = len([e for e in errors if e.severity == ValidationSeverity.ERROR])
        
        logger.info(
            f"Validation complete: {rows_valid} valid, {rows_invalid} invalid, "
            f"{len(warnings)} warnings"
        )
        
        result = ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            rows_valid=rows_valid,
            rows_invalid=rows_invalid,
            metadata={
                "total_rules": len(self.rules),
                "total_rows": len(data),
            },
        )
        
        # Record metrics
        if not is_valid:
            self.metrics.record_quality_failure(
                "default",
                "validation",
                rows_invalid
            )
        
        return result
    
    def _validate_row(
        self,
        row: Dict[str, Any],
        row_index: int,
    ) -> tuple[List[ValidationError], List[ValidationError]]:
        """Validate single row."""
        errors = []
        warnings = []
        
        for rule in self.rules:
            if rule.column not in row and rule.validation_type != ValidationType.REQUIRED:
                continue
            
            error = self._validate_value(
                rule,
                row.get(rule.column),
                row_index,
            )
            
            if error:
                if error.severity == ValidationSeverity.ERROR:
                    errors.append(error)
                else:
                    warnings.append(error)
        
        return errors, warnings
    
    def _validate_value(
        self,
        rule: ValidationRule,
        value: Any,
        row_index: int,
    ) -> Optional[ValidationError]:
        """Validate single value against rule."""
        try:
            if rule.validation_type == ValidationType.REQUIRED:
                if value is None or value == "":
                    return ValidationError(
                        column=rule.column,
                        row=row_index,
                        value=value,
                        validation_type=rule.validation_type,
                        severity=rule.severity,
                        message=rule.message or f"{rule.column} is required",
                    )
            
            elif rule.validation_type == ValidationType.TYPE:
                if value is not None and value != "":
                    if rule.expected_type and not isinstance(value, rule.expected_type):
                        return ValidationError(
                            column=rule.column,
                            row=row_index,
                            value=value,
                            validation_type=rule.validation_type,
                            severity=rule.severity,
                            message=rule.message or f"{rule.column} must be {rule.expected_type.__name__}",
                        )
            
            elif rule.validation_type == ValidationType.RANGE:
                if value is not None and value != "":
                    try:
                        num_value = float(value)
                        
                        if rule.min_value is not None and num_value < rule.min_value:
                            return ValidationError(
                                column=rule.column,
                                row=row_index,
                                value=value,
                                validation_type=rule.validation_type,
                                severity=rule.severity,
                                message=rule.message or f"{rule.column} must be >= {rule.min_value}",
                            )
                        
                        if rule.max_value is not None and num_value > rule.max_value:
                            return ValidationError(
                                column=rule.column,
                                row=row_index,
                                value=value,
                                validation_type=rule.validation_type,
                                severity=rule.severity,
                                message=rule.message or f"{rule.column} must be <= {rule.max_value}",
                            )
                    except (ValueError, TypeError):
                        return ValidationError(
                            column=rule.column,
                            row=row_index,
                            value=value,
                            validation_type=rule.validation_type,
                            severity=rule.severity,
                            message=rule.message or f"{rule.column} must be numeric",
                        )
            
            elif rule.validation_type == ValidationType.PATTERN:
                if value is not None and value != "":
                    if rule.pattern:
                        if not re.match(rule.pattern, str(value)):
                            return ValidationError(
                                column=rule.column,
                                row=row_index,
                                value=value,
                                validation_type=rule.validation_type,
                                severity=rule.severity,
                                message=rule.message or f"{rule.column} does not match pattern",
                            )
            
            elif rule.validation_type == ValidationType.ENUM:
                if value is not None and value != "":
                    if rule.allowed_values and value not in rule.allowed_values:
                        return ValidationError(
                            column=rule.column,
                            row=row_index,
                            value=value,
                            validation_type=rule.validation_type,
                            severity=rule.severity,
                            message=rule.message or f"{rule.column} must be one of {rule.allowed_values}",
                        )
            
            elif rule.validation_type == ValidationType.LENGTH:
                if value is not None and value != "":
                    length = len(str(value))
                    
                    if rule.min_length is not None and length < rule.min_length:
                        return ValidationError(
                            column=rule.column,
                            row=row_index,
                            value=value,
                            validation_type=rule.validation_type,
                            severity=rule.severity,
                            message=rule.message or f"{rule.column} too short",
                        )
                    
                    if rule.max_length is not None and length > rule.max_length:
                        return ValidationError(
                            column=rule.column,
                            row=row_index,
                            value=value,
                            validation_type=rule.validation_type,
                            severity=rule.severity,
                            message=rule.message or f"{rule.column} too long",
                        )
            
            elif rule.validation_type == ValidationType.CUSTOM:
                if value is not None and rule.custom_func:
                    try:
                        if not rule.custom_func(value):
                            return ValidationError(
                                column=rule.column,
                                row=row_index,
                                value=value,
                                validation_type=rule.validation_type,
                                severity=rule.severity,
                                message=rule.message or "Custom validation failed",
                            )
                    except Exception as e:
                        return ValidationError(
                            column=rule.column,
                            row=row_index,
                            value=value,
                            validation_type=rule.validation_type,
                            severity=rule.severity,
                            message=f"Custom validation error: {str(e)}",
                        )
        
        except Exception as e:
            return ValidationError(
                column=rule.column,
                row=row_index,
                value=value,
                validation_type=rule.validation_type,
                severity=ValidationSeverity.ERROR,
                message=f"Validation error: {str(e)}",
            )
        
        return None
    
    def get_valid_rows(
        self,
        data: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Separate valid and invalid rows.
        
        Args:
            data: Data to validate
        
        Returns:
            tuple: (valid_rows, invalid_rows)
        """
        result = self.validate(data)
        
        valid_indices = set()
        invalid_indices = set()
        
        for error in result.errors:
            if error.severity == ValidationSeverity.ERROR:
                invalid_indices.add(error.row)
            else:
                valid_indices.add(error.row)
        
        valid_rows = [row for i, row in enumerate(data) if i in valid_indices]
        invalid_rows = [row for i, row in enumerate(data) if i in invalid_indices]
        
        return valid_rows, invalid_rows


def create_data_validator() -> DataValidator:
    """
    Create data validator.
    
    Returns:
        DataValidator: Ready to use validator
    """
    return DataValidator()


# Pre-built validators
def email_validator() -> DataValidator:
    """Create email validator."""
    return (
        create_data_validator()
        .required("email")
        .check_type("email", str)
        .check_pattern(
            "email",
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "Invalid email format",
        )
    )


def phone_validator(column: str = "phone") -> DataValidator:
    """Create phone validator."""
    return (
        create_data_validator()
        .required(column)
        .check_pattern(
            column,
            r"^\+?1?\d{9,15}$",
            "Invalid phone format",
        )
    )


__all__ = [
    "ValidationType",
    "ValidationSeverity",
    "ValidationRule",
    "ValidationError",
    "ValidationResult",
    "DataValidator",
    "create_data_validator",
    "email_validator",
    "phone_validator",
]