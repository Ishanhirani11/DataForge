"""
Data cleaner transformer for DataFlow Pro.

Provides comprehensive data cleaning functions including:
- Missing value handling
- Outlier detection and handling
- Duplicate removal
- Data type conversions
- String cleaning and normalization
- Date parsing
"""

from typing import Any, Dict, Iterator, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
import re
from datetime import datetime
import math
from functools import partial
import warnings

try:
    import pandas as pd
    from pandas import DataFrame, Series
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from dataflow_pro.utils.logger import get_logger
from dataflow_pro.utils.metrics import get_metrics


logger = get_logger(__name__)


class MissingValueStrategy(str, Enum):
    """Missing value handling strategies."""
    DROP = "drop"
    FILL_FORWARD = "ffill"
    FILL_BACKWARD = "bfill"
    FILL_DEFAULT = "fill_default"
    FILL_MEAN = "fill_mean"
    FILL_MEDIAN = "fill_median"
    FILL_MODE = "fill_mode"
    FILL_INTERPOLATE = "interpolate"


class OutlierStrategy(str, Enum):
    """Outlier handling strategies."""
    DROP = "drop"
    CLIP = "clip"
    IQR = "iqr"
    ZSCORE = "zscore"
    NONE = "none"


@dataclass
class CleaningConfig:
    """Data cleaning configuration."""
    # Missing values
    missing_value_strategy: MissingValueStrategy = MissingValueStrategy.FILL_DEFAULT
    missing_value_default: Any = None
    missing_value_columns: Optional[List[str]] = None
    
    # Duplicates
    remove_duplicates: bool = True
    duplicate_columns: Optional[List[str]] = None
    
    # Outliers
    outlier_strategy: OutlierStrategy = OutlierStrategy.NONE
    outlier_threshold: float = 3.0
    outlier_columns: Optional[List[str]] = None
    
    # Strings
    strip_whitespace: bool = True
    lowercase: bool = False
    remove_special_chars: bool = False
    
    # Dates
    parse_dates: bool = True
    date_columns: Optional[List[str]] = None
    date_format: Optional[str] = None
    
    # Numeric
    coerce_numeric: bool = True
    numeric_columns: Optional[List[str]] = None


@dataclass
class CleaningResult:
    """Result of data cleaning."""
    data: List[Dict[str, Any]]
    rows_processed: int
    rows_dropped: int = 0
    rows_modified: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataCleaner:
    """
    Data cleaner with comprehensive cleaning functions.
    
    Supports:
    - Missing value handling (multiple strategies)
    - Outlier detection and handling
    - Duplicate removal
    - String cleaning
    - Date parsing
    - Type coercion
    - Custom transformations
    """
    
    def __init__(self, config: Optional[CleaningConfig] = None):
        self.config = config or CleaningConfig()
        self.metrics = get_metrics()
        self._transformations: List[tuple] = []
    
    def add_transformation(
        self,
        func: Callable,
        columns: Optional[List[str]] = None,
    ) -> "DataCleaner":
        """
        Add custom transformation.
        
        Args:
            func: Transformation function
            columns: Columns to apply to
        
        Returns:
            Self for chaining
        """
        self._transformations.append((func, columns))
        return self
    
    def clean(
        self,
        data: List[Dict[str, Any]],
    ) -> CleaningResult:
        """
        Clean data.
        
        Args:
            data: Data to clean
        
        Returns:
            CleaningResult: Cleaning result
        """
        if not data:
            return CleaningResult(data=[], rows_processed=0)
        
        logger.info(f"Cleaning {len(data)} rows")
        
        rows_processed = len(data)
        initial_count = len(data)
        
        # Start with list of dicts
        cleaned_data = list(data)
        
        # Apply transformations
        cleaned_data = self._apply_transformations(cleaned_data)
        
        # Remove duplicates
        if self.config.remove_duplicates:
            cleaned_data, dropped = self._remove_duplicates(cleaned_data)
            logger.info(f"Removed {dropped} duplicate rows")
        
        # Clean missing values
        cleaned_data, modified = self._handle_missing_values(cleaned_data)
        rows_modified += modified
        
        # Handle outliers
        if self.config.outlier_strategy != OutlierStrategy.NONE:
            cleaned_data, dropped = self._handle_outliers(cleaned_data)
            logger.info(f"Removed {dropped} outlier rows")
        
        # Clean strings
        if self.config.strip_whitespace or self.config.lowercase:
            cleaned_data = self._clean_strings(cleaned_data)
        
        # Parse dates
        if self.config.parse_dates and self.config.date_columns:
            cleaned_data = self._parse_dates(cleaned_data)
        
        # Coerce numeric
        if self.config.coerce_numeric and self.config.numeric_columns:
            cleaned_data = self._coerce_numeric(cleaned_data)
        
        rows_dropped = initial_count - len(cleaned_data)
        
        logger.info(
            f"Cleaning complete: {initial_count} -> {len(cleaned_data)} rows "
            f"({rows_dropped} dropped)"
        )
        
        return CleaningResult(
            data=cleaned_data,
            rows_processed=rows_processed,
            rows_dropped=rows_dropped,
            rows_modified=rows_modified,
            metadata={
                "missing_values_handled": modified,
                "duplicates_removed": rows_dropped,
            },
        )
    
    def _apply_transformations(
        self,
        data: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Apply custom transformations."""
        for func, columns in self._transformations:
            if columns:
                # Apply to specific columns
                data = [
                    {
                        **row,
                        **{col: func(row.get(col)) for col in columns if col in row}
                    }
                    for row in data
                ]
            else:
                # Apply to all
                data = [func(row) for row in data]
        
        return data
    
    def _remove_duplicates(
        self,
        data: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], int]:
        """Remove duplicate rows."""
        if self.config.duplicate_columns:
            # Based on specific columns
            seen = set()
            unique = []
            dropped = 0
            
            for row in data:
                key = tuple(row.get(col) for col in self.config.duplicate_columns)
                if key not in seen:
                    seen.add(key)
                    unique.append(row)
                else:
                    dropped += 1
            
            return unique, dropped
        else:
            # Full row deduplication
            seen = set()
            unique = []
            dropped = 0
            
            for row in data:
                row_key = tuple(sorted(row.items()))
                if row_key not in seen:
                    seen.add(row_key)
                    unique.append(row)
                else:
                    dropped += 1
            
            return unique, dropped
    
    def _handle_missing_values(
        self,
        data: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], int]:
        """Handle missing values."""
        strategy = self.config.missing_value_strategy
        modified = 0
        
        if strategy == MissingValueStrategy.DROP:
            return [row for row in data if self._has_all_values(row)], modified
        
        # Determine columns to process
        columns = self.config.missing_value_columns
        if not columns:
            columns = list(set().union(*[row.keys() for row in data[:100]]))
        
        # Calculate fill values
        fill_value = self.config.missing_value_default
        
        if strategy in [MissingValueStrategy.FILL_MEAN, MissingValueStrategy.FILL_MEDIAN]:
            fill_values = self._calculate_fill_values(data, columns, strategy)
        else:
            fill_values = {col: fill_value for col in columns}
        
        # Apply fill
        for i, row in enumerate(data):
            for col in columns:
                if col in row and (row[col] is None or row[col] == ""):
                    if col in fill_values:
                        row[col] = fill_values[col]
                        modified += 1
                elif col not in row:
                    row[col] = fill_values.get(col)
                    modified += 1
        
        # Forward/backward fill
        if strategy in [
            MissingValueStrategy.FILL_FORWARD,
            MissingValueStrategy.FILL_BACKWARD,
        ]:
            data = self._propagate_fill(data, strategy)
        
        return data, modified
    
    def _has_all_values(self, row: Dict[str, Any]) -> bool:
        """Check if row has no missing values."""
        return all(v is not None and v != "" for v in row.values())
    
    def _calculate_fill_values(
        self,
        data: List[Dict[str, Any]],
        columns: List[str],
        strategy: MissingValueStrategy,
    ) -> Dict[str, Any]:
        """Calculate fill values for columns."""
        fill_values = {}
        
        for col in columns:
            values = [
                row.get(col)
                for row in data
                if row.get(col) is not None and row.get(col) != ""
            ]
            
            if not values:
                continue
            
            try:
                numeric_values = [float(v) for v in values if self._is_numeric(v)]
                
                if strategy == MissingValueStrategy.FILL_MEAN:
                    fill_values[col] = sum(numeric_values) / len(numeric_values)
                elif strategy == MissingValueStrategy.FILL_MEDIAN:
                    sorted_values = sorted(numeric_values)
                    mid = len(sorted_values) // 2
                    if len(sorted_values) % 2 == 0:
                        fill_values[col] = (sorted_values[mid - 1] + sorted_values[mid]) / 2
                    else:
                        fill_values[col] = sorted_values[mid]
            except (ValueError, TypeError):
                # Use mode for non-numeric
                from collections import Counter
                counter = Counter(values)
                fill_values[col] = counter.most_common(1)[0][0]
        
        return fill_values
    
    def _propagate_fill(
        self,
        data: List[Dict[str, Any]],
        strategy: MissingValueStrategy,
    ) -> List[Dict[str, Any]]:
        """Apply forward or backward fill."""
        if not data:
            return data
        
        columns = data[0].keys()
        
        if strategy == MissingValueStrategy.FILL_FORWARD:
            for col in columns:
                last_value = None
                for row in data:
                    if row.get(col) is not None and row.get(col) != "":
                        last_value = row[col]
                    elif last_value is not None:
                        row[col] = last_value
        
        elif strategy == MissingValueStrategy.FILL_BACKWARD:
            for col in reversed(columns):
                last_value = None
                for row in reversed(data):
                    if row.get(col) is not None and row.get(col) != "":
                        last_value = row[col]
                    elif last_value is not None:
                        row[col] = last_value
        
        return data
    
    def _handle_outliers(
        self,
        data: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], int]:
        """Handle outliers."""
        strategy = self.config.outlier_strategy
        threshold = self.config.outlier_threshold
        columns = self.config.outlier_columns
        
        if not columns:
            return data, 0
        
        if strategy == OutlierStrategy.DROP:
            return self._remove_outliers_zscore(data, columns, threshold)
        
        elif strategy == OutlierStrategy.CLIP:
            return self._clip_outliers(data, columns, threshold), 0
        
        return data, 0
    
    def _remove_outliers_zscore(
        self,
        data: List[Dict[str, Any]],
        columns: List[str],
        threshold: float,
    ) -> tuple[List[Dict[str, Any]], int]:
        """Remove outliers using z-score."""
        import math
        
        for col in columns:
            values = [
                row.get(col)
                for row in data
                if row.get(col) is not None and self._is_numeric(row.get(col))
            ]
            
            if not values:
                continue
            
            mean = sum(values) / len(values)
            std = math.sqrt(
                sum((x - mean) ** 2 for x in values) / len(values)
            )
            
            if std == 0:
                continue
            
            new_data = []
            dropped = 0
            
            for row in data:
                if row.get(col) is not None:
                    z_score = abs((row[col] - mean) / std)
                    if z_score <= threshold:
                        new_data.append(row)
                    else:
                        dropped += 1
                else:
                    new_data.append(row)
            
            data = new_data
        
        return data, dropped
    
    def _clip_outliers(
        self,
        data: List[Dict[str, Any]],
        columns: List[str],
        threshold: float,
    ) -> List[Dict[str, Any]]:
        """Clip outliers to threshold."""
        for col in columns:
            values = [
                row.get(col)
                for row in data
                if row.get(col) is not None and self._is_numeric(row.get(col))
            ]
            
            if not values:
                continue
            
            mean = sum(values) / len(values)
            for row in data:
                if row.get(col) is not None:
                    if abs(row[col]) > threshold * mean:
                        row[col] = threshold * mean if row[col] > 0 else -threshold * mean
        
        return data
    
    def _clean_strings(
        self,
        data: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Clean string values."""
        for row in data:
            for key, value in row.items():
                if isinstance(value, str):
                    if self.config.strip_whitespace:
                        value = value.strip()
                    
                    if self.config.lowercase:
                        value = value.lower()
                    
                    if self.config.remove_special_chars:
                        value = re.sub(r'[^\w\s]', '', value)
                    
                    row[key] = value
        
        return data
    
    def _parse_dates(
        self,
        data: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Parse date columns."""
        for col in self.config.date_columns:
            for row in data:
                if col in row and row[col]:
                    try:
                        if self.config.date_format:
                            row[col] = datetime.strptime(
                                str(row[col]),
                                self.config.date_format
                            )
                        else:
                            # Try common formats
                            row[col] = pd.to_datetime(row[col])
                    except (ValueError, TypeError):
                        pass
        
        return data
    
    def _coerce_numeric(
        self,
        data: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Coerce numeric columns."""
        for col in self.config.numeric_columns:
            for row in data:
                if col in row and row[col]:
                    try:
                        row[col] = float(row[col])
                    except (ValueError, TypeError):
                        row[col] = None
        
        return data
    
    def _is_numeric(self, value: Any) -> bool:
        """Check if value is numeric."""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def clean_dataframe(
        self,
        df: "DataFrame",
    ) -> "DataFrame":
        """
        Clean pandas DataFrame.
        
        Args:
            df: DataFrame to clean
        
        Returns:
            DataFrame: Cleaned DataFrame
        """
        result = self.clean(df.to_dict(orient="records"))
        return DataFrame(result.data)


def create_data_cleaner(
    missing_strategy: MissingValueStrategy = MissingValueStrategy.FILL_DEFAULT,
    remove_duplicates: bool = True,
    outlier_strategy: OutlierStrategy = OutlierStrategy.NONE,
    strip_whitespace: bool = True,
    **kwargs,
) -> DataCleaner:
    """
    Factory function to create data cleaner.
    
    Args:
        missing_strategy: Missing value strategy
        remove_duplicates: Remove duplicates
        outlier_strategy: Outlier strategy
        strip_whitespace: Strip whitespace from strings
    
    Returns:
        DataCleaner: Configured cleaner
    """
    config = CleaningConfig(
        missing_value_strategy=missing_strategy,
        remove_duplicates=remove_duplicates,
        outlier_strategy=outlier_strategy,
        strip_whitespace=strip_whitespace,
        **kwargs,
    )
    
    return DataCleaner(config)


__all__ = [
    "MissingValueStrategy",
    "OutlierStrategy",
    "CleaningConfig",
    "CleaningResult",
    "DataCleaner",
    "create_data_cleaner",
]