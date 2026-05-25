"""
Simple file-based ETL pipeline runner.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List

import yaml

from dataforge.data_quality import DataQualityRunner
from dataforge.extractors import FileExtractor, FileFormat
from dataforge.transformers import (
    CleaningConfig,
    DataCleaner,
    DataEnricher,
    DataValidator,
    MissingValueStrategy,
)


def load_pipeline_config(config_path: str) -> Dict[str, Any]:
    with Path(config_path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def run_pipeline(config_path: str) -> Dict[str, Any]:
    config = load_pipeline_config(config_path)

    source = config.get("source", {})
    if source.get("type") != "file":
        raise ValueError("Only file sources are currently supported by the sample runner")

    extractor = FileExtractor()
    if source.get("sheet_name") is not None:
        extractor.config.sheet_name = source["sheet_name"]
    extracted = extractor.extract(
        source["path"],
        _parse_file_format(source.get("format")),
    )

    working_rows = extracted.data

    clean_config = _build_cleaning_config(config.get("transform", {}).get("clean", {}))
    cleaned = DataCleaner(clean_config).clean(working_rows)
    working_rows = cleaned.data

    filter_config = config.get("transform", {}).get("filter", {})
    if filter_config:
        working_rows = _apply_filter(working_rows, filter_config)

    enrich_config = config.get("transform", {}).get("enrich", {})
    if enrich_config:
        enricher = DataEnricher()
        for column in enrich_config.get("label_encode", []):
            enricher.add_label_encoding(column)
        working_rows = enricher.enrich(working_rows).data

    validator = _build_validator(config.get("transform", {}).get("validate", {}))
    validation = validator.validate(working_rows)
    valid_rows, invalid_rows = validator.get_valid_rows(working_rows)

    quality_report = None
    quality_config = config.get("quality")
    if quality_config and quality_config.get("suite"):
        quality_report = DataQualityRunner(quality_config["suite"]).validate_records(working_rows)

    target = config.get("target", {})
    output_path = None
    if target:
        output_path = _write_output(valid_rows, target)

    return {
        "source_rows": extracted.rows_extracted,
        "clean_rows": len(working_rows),
        "valid_rows": validation.rows_valid,
        "invalid_rows": validation.rows_invalid,
        "output_path": output_path,
        "quality_success": quality_report["success"] if quality_report else None,
        "quality_report": quality_report,
        "invalid_records": invalid_rows,
    }


def _parse_file_format(raw_format: str | None) -> FileFormat | None:
    if not raw_format:
        return None
    normalized = raw_format.strip().lower()
    if normalized == "csv":
        return FileFormat.CSV
    if normalized == "json":
        return FileFormat.JSON
    if normalized == "jsonl":
        return FileFormat.JSONL
    if normalized == "parquet":
        return FileFormat.PARQUET
    if normalized in ("excel", "xlsx", "xls"):
        return FileFormat.EXCEL
    return None


def _apply_filter(rows: List[Dict[str, Any]], filter_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Apply OR-based row filtering from YAML filter config.

    Each condition is: {column, operator, value}
    Supported operators: contains, equals, regex
    Rows matching ANY condition are kept (OR logic).
    """
    import re as _re

    conditions = filter_config.get("or", [])
    if not conditions:
        return rows

    def matches(row: Dict[str, Any]) -> bool:
        for cond in conditions:
            col = cond.get("column", "")
            op = cond.get("operator", "contains").lower()
            val = str(cond.get("value", ""))
            cell = str(row.get(col, ""))
            if op == "contains" and val.lower() in cell.lower():
                return True
            if op == "equals" and cell.lower() == val.lower():
                return True
            if op == "regex" and _re.search(val, cell, _re.IGNORECASE):
                return True
        return False

    return [row for row in rows if matches(row)]


def _build_cleaning_config(config: Dict[str, Any]) -> CleaningConfig:
    strategy_name = config.get("missing_value_strategy", "fill_default")
    try:
        strategy = MissingValueStrategy(strategy_name)
    except ValueError:
        strategy = MissingValueStrategy.FILL_DEFAULT

    return CleaningConfig(
        missing_value_strategy=strategy,
        missing_value_default=config.get("missing_value_default"),
        remove_duplicates=config.get("remove_duplicates", True),
        duplicate_columns=config.get("duplicate_columns"),
        parse_dates=config.get("parse_dates", True),
        date_columns=config.get("date_columns"),
        coerce_numeric=config.get("coerce_numeric", True),
        numeric_columns=config.get("numeric_columns"),
    )


def _build_validator(config: Dict[str, Any]) -> DataValidator:
    validator = DataValidator()

    for column in config.get("required", []):
        validator.required(column)

    for column, type_name in config.get("types", {}).items():
        expected_type = {"int": int, "integer": int, "float": float, "str": str, "string": str}.get(
            str(type_name).lower(),
            str,
        )
        validator.check_type(column, expected_type)

    for column, pattern in config.get("patterns", {}).items():
        validator.check_pattern(column, pattern)

    for column, bounds in config.get("ranges", {}).items():
        validator.check_range(
            column,
            min_value=bounds.get("min"),
            max_value=bounds.get("max"),
        )

    return validator


def _write_output(rows: List[Dict[str, Any]], target: Dict[str, Any]) -> str:
    if target.get("type") != "file":
        raise ValueError("Only file targets are currently supported by the sample runner")

    destination = Path(target["path"])
    destination.parent.mkdir(parents=True, exist_ok=True)
    output_format = str(target.get("format", "json")).lower()

    if output_format == "csv":
        fieldnames = list(rows[0].keys()) if rows else []
        with destination.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return str(destination)

    with destination.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, indent=2, default=str)
    return str(destination)


__all__ = ["load_pipeline_config", "run_pipeline"]
