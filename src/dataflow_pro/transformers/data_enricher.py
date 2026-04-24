"""
Data enricher transformer for DataFlow Pro.

Provides data enrichment functions including:
- Data type conversions
- Feature engineering
- Derived columns
- Lookups and joins
- Data aggregation
- String manipulations
"""

from typing import Any, Dict, Iterator, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
from datetime import datetime
import re
import json
from functools import wraps

from dataflow_pro.utils.logger import get_logger
from dataflow_pro.utils.metrics import get_metrics


logger = get_logger(__name__)


class EnrichmentType(str, Enum):
    """Enrichment operation types."""
    DERIVED = "derived"
    LOOKUP = "lookup"
    AGGREGATE = "aggregate"
    ENCODE = "encode"
    BIN = "bin"
    NORMALIZE = "normalize"
    CUSTOM = "custom"


@dataclass
class EnrichmentConfig:
    """Enrichment configuration."""
    name: str
    enrichment_type: EnrichmentType
    source_columns: List[str] = field(default_factory=list)
    target_column: Optional[str] = None
    
    # For derived columns
    expression: Optional[str] = None
    func: Optional[Callable] = None
    
    # For lookup
    lookup_table: Optional[Dict[str, Any]] = None
    lookup_key: Optional[str] = None
    lookup_value: Optional[str] = None
    
    # For encoding
    encoding_type: Optional[str] = None  # onehot, label, ordinal
    
    # For binning
    bin_labels: Optional[List[str]] = None
    bin_ranges: Optional[List[float]] = None
    
    # For normalization
    normalization_type: Optional[str] = None  # minmax, zscore


@dataclass
class EnrichmentResult:
    """Result of data enrichment."""
    data: List[Dict[str, Any]]
    rows_enriched: int
    columns_added: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataEnricher:
    """
    Data enricher with comprehensive enrichment functions.
    
    Supports:
    - Derived columns (calculations, concatenations)
    - Lookup/join enrichment
    - Encoding (one-hot, label, ordinal)
    - Binning/discretization
    - Normalization
    - Feature engineering
    - Custom transformations
    """
    
    def __init__(self):
        self.enrichments: List[EnrichmentConfig] = []
        self.metrics = get_metrics()
    
    def add_enrichment(
        self,
        config: EnrichmentConfig,
    ) -> "DataEnricher":
        """Add enrichment config."""
        self.enrichments.append(config)
        return self
    
    def derived_column(
        self,
        name: str,
        expression: Optional[str] = None,
        func: Optional[Callable] = None,
    ) -> "DataEnricher":
        """Add derived column."""
        config = EnrichmentConfig(
            name=name,
            enrichment_type=EnrichmentType.DERIVED,
            expression=expression,
            func=func,
        )
        return self.add_enrichment(config)
    
    def add_lookup(
        self,
        name: str,
        lookup_table: Dict[str, Any],
        lookup_key: str,
        lookup_value: str,
    ) -> "DataEnricher":
        """Add lookup enrichment."""
        config = EnrichmentConfig(
            name=name,
            enrichment_type=EnrichmentType.LOOKUP,
            lookup_table=lookup_table,
            lookup_key=lookup_key,
            lookup_value=lookup_value,
        )
        return self.add_enrichment(config)
    
    def add_one_hot_encoding(
        self,
        source_column: str,
    ) -> "DataEnricher":
        """Add one-hot encoding."""
        config = EnrichmentConfig(
            name=f"onehot_{source_column}",
            enrichment_type=EnrichmentType.ENCODE,
            source_columns=[source_column],
            encoding_type="onehot",
        )
        return self.add_enrichment(config)
    
    def add_label_encoding(
        self,
        source_column: str,
        target_column: Optional[str] = None,
    ) -> "DataEnricher":
        """Add label encoding."""
        config = EnrichmentConfig(
            name=f"label_{source_column}",
            enrichment_type=EnrichmentType.ENCODE,
            source_columns=[source_column],
            target_column=target_column or f"{source_column}_encoded",
            encoding_type="label",
        )
        return self.add_enrichment(config)
    
    def add_binning(
        self,
        source_column: str,
        bin_ranges: List[float],
        bin_labels: Optional[List[str]] = None,
    ) -> "DataEnricher":
        """Add binning enrichment."""
        config = EnrichmentConfig(
            name=f"bin_{source_column}",
            enrichment_type=EnrichmentType.BIN,
            source_columns=[source_column],
            bin_ranges=bin_ranges,
            bin_labels=bin_labels,
        )
        return self.add_enrichment(config)
    
    def add_normalization(
        self,
        source_column: str,
        normalization_type: str = "minmax",
    ) -> "DataEnricher":
        """Add normalization."""
        config = EnrichmentConfig(
            name=f"norm_{source_column}",
            enrichment_type=EnrichmentType.NORMALIZE,
            source_columns=[source_column],
            normalization_type=normalization_type,
        )
        return self.add_enrichment(config)
    
    def add_custom(
        self,
        name: str,
        func: Callable,
    ) -> "DataEnricher":
        """Add custom enrichment."""
        config = EnrichmentConfig(
            name=name,
            enrichment_type=EnrichmentType.CUSTOM,
            func=func,
        )
        return self.add_enrichment(config)
    
    def enrich(
        self,
        data: List[Dict[str, Any]],
    ) -> EnrichmentResult:
        """
        Enrich data.
        
        Args:
            data: Data to enrich
        
        Returns:
            EnrichmentResult: Enrichment result
        """
        if not data:
            return EnrichmentResult(data=[], rows_enriched=0)
        
        logger.info(f"Enriching {len(data)} rows with {len(self.enrichments)} enrichments")
        
        enriched_data = list(data)
        columns_added = 0
        
        for config in self.enrichments:
            if config.enrichment_type == EnrichmentType.DERIVED:
                enriched_data = self._add_derived_columns(enriched_data, config)
                columns_added += 1
            
            elif config.enrichment_type == EnrichmentType.LOOKUP:
                enriched_data = self._add_lookup(enriched_data, config)
                columns_added += 1
            
            elif config.enrichment_type == EnrichmentType.ENCODE:
                enriched_data = self._add_encoding(enriched_data, config)
                columns_added += 1 if config.encoding_type == "onehot" else 1
            
            elif config.enrichment_type == EnrichmentType.BIN:
                enriched_data = self._add_binning(enriched_data, config)
                columns_added += 1
            
            elif config.enrichment_type == EnrichmentType.NORMALIZE:
                enriched_data = self._add_normalization(enriched_data, config)
                columns_added += 1
            
            elif config.enrichment_type == EnrichmentType.CUSTOM:
                enriched_data = self._add_custom(enriched_data, config)
                columns_added += 1
        
        logger.info(f"Enrichment complete: {columns_added} columns added")
        
        return EnrichmentResult(
            data=enriched_data,
            rows_enriched=len(enriched_data),
            columns_added=columns_added,
            metadata={
                "enrichments_applied": len(self.enrichments),
            },
        )
    
    def _add_derived_columns(
        self,
        data: List[Dict[str, Any]],
        config: EnrichmentConfig,
    ) -> List[Dict[str, Any]]:
        """Add derived columns."""
        for row in data:
            if config.func:
                # Use custom function
                try:
                    row[config.name] = config.func(row)
                except Exception as e:
                    logger.warning(f"Custom function error: {str(e)}")
                    row[config.name] = None
            
            elif config.expression:
                # Simple expression evaluation
                try:
                    row[config.name] = self._evaluate_expression(
                        row,
                        config.expression
                    )
                except Exception as e:
                    logger.warning(f"Expression evaluation error: {str(e)}")
                    row[config.name] = None
        
        return data
    
    def _evaluate_expression(
        self,
        row: Dict[str, Any],
        expression: str,
    ) -> Any:
        """Evaluate simple expression."""
        # Simple expressions support basic operations
        # Replace column names with values
        expr = expression
        
        for col, value in row.items():
            # Replace column references
            placeholder = f"{{{col}}}"
            if placeholder in expr:
                if isinstance(value, str):
                    expr = expr.replace(placeholder, f"'{value}'")
                else:
                    expr = expr.replace(placeholder, str(value))
        
        # Evaluate using eval (careful with security)
        # In production, use a safe expression evaluator
        try:
            result = eval(expr, {"__builtins__": {}}, {})
            return result
        except Exception:
            return None
    
    def _add_lookup(
        self,
        data: List[Dict[str, Any]],
        config: EnrichmentConfig,
    ) -> List[Dict[str, Any]]:
        """Add lookup enrichment."""
        lookup = config.lookup_table or {}
        key_col = config.lookup_key
        value_col = config.lookup_value
        result_col = config.name
        
        for row in data:
            key = row.get(key_col)
            row[result_col] = lookup.get(key)
        
        return data
    
    def _add_encoding(
        self,
        data: List[Dict[str, Any]],
        config: EnrichmentConfig,
    ) -> List[Dict[str, Any]]:
        """Add encoding."""
        source_column = config.source_columns[0]
        
        if config.encoding_type == "onehot":
            # Get all unique values
            unique_values = set(row.get(source_column) for row in data)
            unique_values.discard(None)
            unique_values = sorted(unique_values)
            
            # Add columns for each value
            for value in unique_values:
                for row in data:
                    row[f"{source_column}_{value}"] = 1 if row.get(source_column) == value else 0
        
        elif config.encoding_type == "label":
            # Get all unique values
            unique_values = set(row.get(source_column) for row in data)
            unique_values.discard(None)
            unique_values = sorted(unique_values)
            
            # Map to integers
            value_map = {v: i for i, v in enumerate(unique_values)}
            
            target_column = config.target_column or f"{source_column}_encoded"
            
            for row in data:
                row[target_column] = value_map.get(row.get(source_column))
        
        return data
    
    def _add_binning(
        self,
        data: List[Dict[str, Any]],
        config: EnrichmentConfig,
    ) -> List[Dict[str, Any]]:
        """Add binning."""
        source_column = config.source_columns[0]
        bin_ranges = config.bin_ranges or []
        bin_labels = config.bin_labels
        
        target_column = config.name
        
        for row in data:
            value = row.get(source_column)
            
            if value is None:
                row[target_column] = None
                continue
            
            # Find appropriate bin
            assigned = False
            for i, threshold in enumerate(bin_ranges):
                if value <= threshold:
                    label = bin_labels[i] if bin_labels else f"bin_{i}"
                    row[target_column] = label
                    assigned = True
                    break
            
            if not assigned:
                label = bin_labels[-1] if bin_labels else f"bin_{len(bin_ranges)}"
                row[target_column] = label
        
        return data
    
    def _add_normalization(
        self,
        data: List[Dict[str, Any]],
        config: EnrichmentConfig,
    ) -> List[Dict[str, Any]]:
        """Add normalization."""
        source_column = config.source_columns[0]
        norm_type = config.normalization_type or "minmax"
        
        target_column = config.name
        
        if norm_type == "minmax":
            # Min-max normalization
            values = [row.get(source_column) for row in data if row.get(source_column) is not None]
            
            if values:
                min_val = min(values)
                max_val = max(values)
                range_val = max_val - min_val
                
                for row in data:
                    value = row.get(source_column)
                    if value is not None:
                        if range_val > 0:
                            row[target_column] = (value - min_val) / range_val
                        else:
                            row[target_column] = 0
        
        elif norm_type == "zscore":
            # Z-score normalization
            values = [row.get(source_column) for row in data if row.get(source_column) is not None]
            
            if values:
                mean = sum(values) / len(values)
                variance = sum((x - mean) ** 2 for x in values) / len(values)
                std = variance ** 0.5
                
                for row in data:
                    value = row.get(source_column)
                    if value is not None:
                        if std > 0:
                            row[target_column] = (value - mean) / std
                        else:
                            row[target_column] = 0
        
        return data
    
    def _add_custom(
        self,
        data: List[Dict[str, Any]],
        config: EnrichmentConfig,
    ) -> List[Dict[str, Any]]:
        """Add custom enrichment."""
        if config.func:
            try:
                # Apply function to each row
                return [config.func(row) for row in data]
            except Exception as e:
                logger.warning(f"Custom enrichment error: {str(e)}")
        
        return data


# Helper functions for common enrichments
def calculate_age(birth_date: Union[str, datetime]) -> Optional[int]:
    """Calculate age from birth date."""
    try:
        if isinstance(birth_date, str):
            birth_date = datetime.fromisoformat(birth_date)
        
        today = datetime.now()
        age = today.year - birth_date.year
        
        if today.month < birth_date.month or (
            today.month == birth_date.month and today.day < birth_date.day
        ):
            age -= 1
        
        return age
    except Exception:
        return None


def extract_year(date: Union[str, datetime]) -> Optional[int]:
    """Extract year from date."""
    try:
        if isinstance(date, str):
            date = datetime.fromisoformat(date)
        return date.year
    except Exception:
        return None


def extract_month(date: Union[str, datetime]) -> Optional[int]:
    """Extract month from date."""
    try:
        if isinstance(date, str):
            date = datetime.fromisoformat(date)
        return date.month
    except Exception:
        return None


def concatenate_fields(*fields) -> str:
    """Concatenate multiple fields."""
    return " ".join(str(f) for f in fields if f)


def create_data_enricher() -> DataEnricher:
    """Create data enricher."""
    return DataEnricher()


__all__ = [
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