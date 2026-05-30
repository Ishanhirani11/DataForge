"""
Loaders package for DataForge.

Provides data loading methods including:
- Database loading with batch inserts and upserts
- S3 loading with multiple formats
- Cache loading with Redis
"""

from dataforge.loaders.database_loader import (
    LoadStrategy,
    LoadResult as DatabaseLoadResult,
    DatabaseLoader,
    create_database_loader,
)

from dataforge.loaders.s3_loader import (
    FileFormat,
    CompressionType,
    LoadResult as S3LoadResult,
    S3Loader,
    create_s3_loader,
)

from dataforge.loaders.cache_loader import (
    CacheDataType,
    SerializationType,
    CacheResult,
    CacheLoader,
    create_cache_loader,
)

__all__ = [
    # Database Loader
    "LoadStrategy",
    "DatabaseLoadResult",
    "DatabaseLoader",
    "create_database_loader",
    # S3 Loader
    "FileFormat",
    "CompressionType",
    "S3LoadResult",
    "S3Loader",
    "create_s3_loader",
    # Cache Loader
    "CacheDataType",
    "SerializationType",
    "CacheResult",
    "CacheLoader",
    "create_cache_loader",
]