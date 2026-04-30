"""
Data quality helpers for DataForge.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from dataforge.extractors import FileExtractor


@dataclass
class QualityCheckResult:
    expectation_type: str
    success: bool
    kwargs: Dict[str, Any]
    unexpected_count: int = 0
    message: str = ""


class DataQualityRunner:
    """Evaluate a JSON expectation suite against list-of-dict records."""

    def __init__(self, suite_path: str):
        self.suite_path = Path(suite_path)
        self.suite = self._load_suite()

    def _load_suite(self) -> Dict[str, Any]:
        if not self.suite_path.exists():
            raise FileNotFoundError(f"Expectation suite not found: {self.suite_path}")

        import json

        with self.suite_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def validate_records(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        results: List[QualityCheckResult] = []
        expectations = self.suite.get("expectations", [])

        for expectation in expectations:
            results.append(self._evaluate_expectation(records, expectation))

        success_count = sum(1 for result in results if result.success)
        failure_count = len(results) - success_count

        return {
            "suite_name": self.suite.get("expectation_suite_name", self.suite_path.stem),
            "success": failure_count == 0,
            "statistics": {
                "evaluated_expectations": len(results),
                "successful_expectations": success_count,
                "failed_expectations": failure_count,
                "evaluated_rows": len(records),
            },
            "results": [
                {
                    "expectation_type": result.expectation_type,
                    "success": result.success,
                    "kwargs": result.kwargs,
                    "unexpected_count": result.unexpected_count,
                    "message": result.message,
                }
                for result in results
            ],
        }

    def validate_file(self, input_path: str) -> Dict[str, Any]:
        extractor = FileExtractor()
        extracted = extractor.extract(input_path)
        report = self.validate_records(extracted.data)
        report["input_path"] = str(input_path)
        return report

    def _evaluate_expectation(
        self,
        records: List[Dict[str, Any]],
        expectation: Dict[str, Any],
    ) -> QualityCheckResult:
        expectation_type = expectation.get("expectation_type", "")
        kwargs = expectation.get("kwargs", {})

        if expectation_type == "expect_table_row_count_to_be_greater_than":
            threshold = int(kwargs.get("value", 0))
            success = len(records) > threshold
            return QualityCheckResult(
                expectation_type=expectation_type,
                success=success,
                kwargs=kwargs,
                unexpected_count=0 if success else 1,
                message=f"Expected row count > {threshold}, got {len(records)}",
            )

        if expectation_type == "expect_table_row_count_to_be_between":
            min_value = kwargs.get("min_value")
            max_value = kwargs.get("max_value")
            success = True
            if min_value is not None and len(records) < int(min_value):
                success = False
            if max_value is not None and len(records) > int(max_value):
                success = False
            return QualityCheckResult(
                expectation_type=expectation_type,
                success=success,
                kwargs=kwargs,
                unexpected_count=0 if success else 1,
                message=f"Expected row count between {min_value} and {max_value}, got {len(records)}",
            )

        if expectation_type == "expect_column_values_to_not_be_null":
            column = kwargs["column"]
            unexpected_count = sum(
                1 for record in records if record.get(column) in (None, "")
            )
            return QualityCheckResult(
                expectation_type=expectation_type,
                success=unexpected_count == 0,
                kwargs=kwargs,
                unexpected_count=unexpected_count,
                message=f"Column {column} contains {unexpected_count} null/blank values",
            )

        if expectation_type == "expect_column_values_to_be_unique":
            column = kwargs["column"]
            values = [record.get(column) for record in records]
            unique_values = {value for value in values}
            unexpected_count = len(values) - len(unique_values)
            return QualityCheckResult(
                expectation_type=expectation_type,
                success=unexpected_count == 0,
                kwargs=kwargs,
                unexpected_count=unexpected_count,
                message=f"Column {column} contains {unexpected_count} duplicate values",
            )

        if expectation_type == "expect_column_values_to_match_regex":
            column = kwargs["column"]
            regex = re.compile(kwargs["regex"])
            unexpected_count = sum(
                1
                for record in records
                if record.get(column) not in (None, "") and not regex.match(str(record.get(column)))
            )
            return QualityCheckResult(
                expectation_type=expectation_type,
                success=unexpected_count == 0,
                kwargs=kwargs,
                unexpected_count=unexpected_count,
                message=f"Column {column} contains {unexpected_count} values that do not match the regex",
            )

        if expectation_type == "expect_column_values_to_be_in_set":
            column = kwargs["column"]
            allowed = set(kwargs.get("value_set", []))
            unexpected_count = sum(
                1
                for record in records
                if record.get(column) not in (None, "") and record.get(column) not in allowed
            )
            return QualityCheckResult(
                expectation_type=expectation_type,
                success=unexpected_count == 0,
                kwargs=kwargs,
                unexpected_count=unexpected_count,
                message=f"Column {column} contains {unexpected_count} values outside the allowed set",
            )

        return QualityCheckResult(
            expectation_type=expectation_type,
            success=False,
            kwargs=kwargs,
            unexpected_count=1,
            message=f"Unsupported expectation type: {expectation_type}",
        )


def run_quality_check(suite_path: str, input_path: str) -> Dict[str, Any]:
    """Run a suite against a local file."""
    return DataQualityRunner(suite_path).validate_file(input_path)


__all__ = ["DataQualityRunner", "QualityCheckResult", "run_quality_check"]
