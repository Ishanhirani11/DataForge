"""
DataForge - Enterprise Data Pipeline Platform

A production-ready ETL/ELT pipeline framework with comprehensive
data quality management, orchestration, and monitoring.
"""

__version__ = "1.0.0"
__author__ = "DataForge"

from dataforge import config
from dataforge import extractors
from dataforge import loaders
from dataforge import transformers
from dataforge import utils
from dataforge import pipeline

__all__ = [
    "__version__",
    "config",
    "extractors",
    "loaders",
    "transformers",
    "utils",
    "pipeline",
]
