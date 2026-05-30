"""
Minimal CLI entrypoints for DataForge.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from dataforge.config import get_app_config, validate_config
from dataforge.data_quality import run_quality_check
from dataforge.extractors import FileExtractor
from dataforge.pipeline import run_pipeline
from dataforge.transformers import DataValidator


def _cmd_run(args: argparse.Namespace) -> int:
    config = get_app_config()
    result = run_pipeline(args.config)
    payload: dict[str, Any] = {
        "command": "run",
        "config_file": args.config,
        "environment": config.environment.value,
        "batch_size": config.pipeline.batch_size,
        **result,
    }
    print(json.dumps(payload, indent=2))
    return 0 if result["invalid_rows"] == 0 else 1


def _cmd_validate(args: argparse.Namespace) -> int:
    extractor = FileExtractor()
    result = extractor.extract(args.input)

    validator = DataValidator()
    for column in args.required:
        validator.required(column)

    validation = validator.validate(result.data)
    payload = {
        "rows_read": result.rows_extracted,
        "rows_valid": validation.rows_valid,
        "rows_invalid": validation.rows_invalid,
        "is_valid": validation.is_valid,
    }
    print(json.dumps(payload, indent=2))
    return 0 if validation.is_valid else 1


def _cmd_quality(args: argparse.Namespace) -> int:
    suite = Path(args.expectations)
    payload: dict[str, Any] = {
        "command": "quality",
        "expectations": str(suite),
        "exists": suite.exists(),
    }

    if not suite.exists():
        print(json.dumps(payload, indent=2))
        return 1

    if not args.input:
        payload["input_required"] = True
        payload["message"] = "Pass --input to evaluate the suite against a data file."
        print(json.dumps(payload, indent=2))
        return 0

    report = run_quality_check(str(suite), args.input)
    payload.update(report)
    print(json.dumps(payload, indent=2))
    return 0 if report["success"] else 1


def _cmd_worker(args: argparse.Namespace) -> int:
    is_valid, errors = validate_config()
    payload = {
        "command": "worker",
        "config_valid": is_valid,
        "errors": errors,
    }
    print(json.dumps(payload, indent=2))
    return 0 if is_valid else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dataforge")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a pipeline")
    run_parser.add_argument("--config", default="pipeline.yaml")
    run_parser.set_defaults(func=_cmd_run)

    validate_parser = subparsers.add_parser("validate", help="Validate data from a local file")
    validate_parser.add_argument("--input", required=True)
    validate_parser.add_argument("--required", nargs="*", default=[])
    validate_parser.set_defaults(func=_cmd_validate)

    quality_parser = subparsers.add_parser("quality", help="Check an expectations file exists")
    quality_parser.add_argument("--expectations", required=True)
    quality_parser.add_argument("--input")
    quality_parser.set_defaults(func=_cmd_quality)

    worker_parser = subparsers.add_parser("worker", help="Start the worker bootstrap check")
    worker_parser.set_defaults(func=_cmd_worker)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
