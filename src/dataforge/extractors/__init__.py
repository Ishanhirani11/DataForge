"""
Extractors package for DataForge.

Provides various data extraction methods including:
- API extraction with pagination and rate limiting
- Database extraction with incremental loading
- File extraction for multiple formats
"""

from dataforge.extractors.api_extractor import (
    HTTPMethod,
    PaginationType,
    RequestConfig,
    PaginationConfig,
    ExtractionResult as APIExtractionResult,
    APIExtractor,
    CustomAPIExtractor,
    create_api_extractor,
)

from dataforge.extractors.database_extractor import (
    DatabaseType,
    QueryConfig,
    ExtractionResult as DatabaseExtractionResult,
    DatabaseExtractor,
    create_database_extractor,
)

from dataforge.extractors.file_extractor import (
    FileFormat,
    CompressionType,
    FileConfig,
    ExtractionResult as FileExtractionResult,
    FileExtractor,
    create_file_extractor,
)

__all__ = [
    # API Extractor
    "HTTPMethod",
    "PaginationType",
    "RequestConfig",
    "PaginationConfig",
    "APIExtractionResult",
    "APIExtractor",
    "CustomAPIExtractor",
    "create_api_extractor",
    # Database Extractor
    "DatabaseType",
    "QueryConfig",
    "DatabaseExtractionResult",
    "DatabaseExtractor",
    "create_database_extractor",
    # File Extractor
    "FileFormat",
    "CompressionType",
    "FileConfig",
    "FileExtractionResult",
    "FileExtractor",
    "create_file_extractor",
]