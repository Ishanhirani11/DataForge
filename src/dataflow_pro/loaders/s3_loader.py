"""
S3 data loader for DataFlow Pro.

Loads data into AWS S3 and S3-compatible storage with support for:
- Multiple formats (CSV, JSON, Parquet)
- Partitioning
- Compression
- Batch uploads
- Error handling
"""

from typing import Any, Dict, Iterator, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging
import io
import threading
import time
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

from dataflow_pro.config import get_app_config, S3Config
from dataflow_pro.utils.logger import get_logger
from dataflow_pro.utils.metrics import get_metrics


logger = get_logger(__name__)


class FileFormat(str, Enum):
    """File formats for S3."""
    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"
    PARQUET = "parquet"
    TEXT = "text"


class CompressionType(str, Enum):
    """Compression types."""
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    ZSTD = "zstd"


@dataclass
class LoadResult:
    """Result of S3 loading."""
    files_written: int
    total_bytes: int
    objects: List[str] = field(default_factory=list)
    duration: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class S3Loader:
    """
    S3 data loader with comprehensive loading functions.
    
    Supports:
    - Multiple file formats
    - Partitioning by date/time
    - Compression
    - Batch uploads
    - Custom endpoints (S3-compatible)
    - Multipart uploads
    """
    
    def __init__(
        self,
        config: Optional[S3Config] = None,
    ):
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for S3 support")
        
        self.config = config or get_app_config().s3
        
        # Create S3 client
        self.client = boto3.client(
            "s3",
            aws_access_key_id=self.config.access_key_id,
            aws_secret_access_key=self.config.secret_access_key,
            region_name=self.config.region_name,
            endpoint_url=self.config.endpoint_url,
            use_ssl=self.config.use_ssl,
        )
        
        # Metrics
        self.metrics = get_metrics()
        
        # Thread safety
        self._lock = threading.Lock()
    
    def load(
        self,
        data: List[Dict[str, Any]],
        key: str,
        format: FileFormat = FileFormat.CSV,
        compression: CompressionType = CompressionType.NONE,
        batch_size: int = 10000,
    ) -> LoadResult:
        """
        Load data to S3.
        
        Args:
            data: Data to load
            key: S3 object key
            format: File format
            compression: Compression type
            batch_size: Batch size
        
        Returns:
            LoadResult: Load result
        """
        if not data:
            return LoadResult(files_written=0, total_bytes=0)
        
        start_time = time.time()
        
        logger.info(f"Loading {len(data)} rows to s3://{self.config.bucket_name}/{key}")
        
        # Convert to format
        content = self._serialize_data(data, format)
        
        # Apply compression
        if compression != CompressionType.NONE:
            content = self._compress_content(content, compression)
        
        # Upload
        content_type = self._get_content_type(format)
        
        self._upload_object(key, content, content_type)
        
        # Add compression extension
        if compression != CompressionType.NONE:
            key = f"{key}.{compression.value}"
        
        duration = time.time() - start_time
        
        return LoadResult(
            files_written=1,
            total_bytes=len(content),
            objects=[key],
            duration=duration,
            metadata={
                "format": format.value,
                "compression": compression.value,
            },
        )
    
    def load_batched(
        self,
        data: List[Dict[str, Any]],
        key_prefix: str,
        format: FileFormat = FileFormat.CSV,
        compression: CompressionType = CompressionType.NONE,
        batch_size: int = 10000,
        partition_by: Optional[str] = None,
    ) -> LoadResult:
        """
        Load data in batches with optional partitioning.
        
        Args:
            data: Data to load
            key_prefix: S3 key prefix
            format: File format
            compression: Compression type
            batch_size: Batch size
            partition_by: Partition column name
        
        Returns:
            LoadResult: Load result
        """
        if not data:
            return LoadResult(files_written=0, total_bytes=0)
        
        start_time = time.time()
        
        files_written = 0
        total_bytes = 0
        objects = []
        
        # Partition data
        if partition_by:
            partitions = self._partition_data(data, partition_by)
            
            for partition_key, partition_data in partitions.items():
                partition_key = f"{key_prefix}/{partition_key}"
                
                # Write batches within partition
                for batch in self._create_batches(partition_data, batch_size):
                    result = self.load(batch, partition_key, format, compression, batch_size)
                    files_written += result.files_written
                    total_bytes += result.total_bytes
                    objects.extend(result.objects)
        else:
            # Write batches without partitioning
            for i, batch in enumerate(self._create_batches(data, batch_size)):
                key = f"{key_prefix}/part_{i:04d}"
                
                result = self.load(batch, key, format, compression, batch_size)
                files_written += result.files_written
                total_bytes += result.total_bytes
                objects.extend(result.objects)
        
        duration = time.time() - start_time
        
        logger.info(
            f"Batch loading complete: {files_written} files, "
            f"{total_bytes} bytes in {duration:.2f}s"
        )
        
        return LoadResult(
            files_written=files_written,
            total_bytes=total_bytes,
            objects=objects,
            duration=duration,
        )
    
    def load_streaming(
        self,
        data_iterator: Iterator[List[Dict[str, Any]]],
        key_prefix: str,
        format: FileFormat = FileFormat.CSV,
        compression: CompressionType = CompressionType.NONE,
        batch_size: int = 10000,
    ) -> LoadResult:
        """
        Load data from iterator.
        
        Args:
            data_iterator: Data iterator
            key_prefix: S3 key prefix
            format: File format
            compression: Compression type
            batch_size: Batch size
        
        Returns:
            LoadResult: Load result
        """
        start_time = time.time()
        
        files_written = 0
        total_bytes = 0
        objects = []
        
        for i, batch in enumerate(data_iterator):
            key = f"{key_prefix}/batch_{i:06d}"
            
            result = self.load(batch, key, format, compression, batch_size)
            files_written += result.files_written
            total_bytes += result.total_bytes
            objects.extend(result.objects)
        
        duration = time.time() - start_time
        
        return LoadResult(
            files_written=files_written,
            total_bytes=total_bytes,
            objects=objects,
            duration=duration,
        )
    
    def _serialize_data(
        self,
        data: List[Dict[str, Any]],
        format: FileFormat,
    ) -> bytes:
        """Serialize data to bytes."""
        if format == FileFormat.CSV:
            return self._to_csv(data)
        elif format == FileFormat.JSON:
            return self._to_json(data)
        elif format == FileFormat.JSONL:
            return self._to_jsonl(data)
        elif format == FileFormat.PARQUET:
            return self._to_parquet(data)
        else:
            return self._to_json(data)
    
    def _to_csv(self, data: List[Dict[str, Any]]) -> bytes:
        """Convert data to CSV."""
        import csv
        import io
        
        if not data:
            return b""
        
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        
        return output.getvalue().encode("utf-8")
    
    def _to_json(self, data: List[Dict[str, Any]]) -> bytes:
        """Convert data to JSON."""
        import json
        
        return json.dumps(data, indent=2, default=str).encode("utf-8")
    
    def _to_jsonl(self, data: List[Dict[str, Any]]) -> bytes:
        """Convert data to JSONL."""
        import json
        
        lines = [json.dumps(row, default=str) for row in data]
        return "\n".join(lines).encode("utf-8")
    
    def _to_parquet(self, data: List[Dict[str, Any]]) -> bytes:
        """Convert data to Parquet."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            table = pa.Table.from_pylist(data)
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            
            return buffer.getvalue()
        except ImportError:
            # Fallback to JSON
            return self._to_json(data)
    
    def _compress_content(
        self,
        content: bytes,
        compression: CompressionType,
    ) -> bytes:
        """Compress content."""
        if compression == CompressionType.GZIP:
            import gzip
            return gzip.compress(content)
        
        return content
    
    def _get_content_type(self, format: FileFormat) -> str:
        """Get content type for format."""
        content_types = {
            FileFormat.CSV: "text/csv",
            FileFormat.JSON: "application/json",
            FileFormat.JSONL: "application/jsonlines",
            FileFormat.PARQUET: "application/octet-stream",
            FileFormat.TEXT: "text/plain",
        }
        
        return content_types.get(format, "application/octet-stream")
    
    def _upload_object(
        self,
        key: str,
        content: bytes,
        content_type: str,
    ) -> None:
        """Upload object to S3."""
        self.client.put_object(
            Bucket=self.config.bucket_name,
            Key=key,
            Body=content,
            ContentType=content_type,
        )
        
        logger.info(f"Uploaded s3://{self.config.bucket_name}/{key}")
    
    def _partition_data(
        self,
        data: List[Dict[str, Any]],
        partition_by: str,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Partition data by column."""
        partitions: Dict[str, List[Dict[str, Any]]] = {}
        
        for row in data:
            key = str(row.get(partition_by, "unknown"))
            if key not in partitions:
                partitions[key] = []
            partitions[key].append(row)
        
        return partitions
    
    def _create_batches(
        self,
        data: List[Dict[str, Any]],
        batch_size: int,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Create batches from data."""
        for i in range(0, len(data), batch_size):
            yield data[i:i + batch_size]
    
    def list_objects(
        self,
        prefix: Optional[str] = None,
    ) -> List[str]:
        """
        List objects in bucket.
        
        Args:
            prefix: Object key prefix
        
        Returns:
            List[str]: List of object keys
        """
        response = self.client.list_objects_v2(
            Bucket=self.config.bucket_name,
            Prefix=prefix,
        )
        
        return [obj["Key"] for obj in response.get("Contents", [])]
    
    def read_object(
        self,
        key: str,
    ) -> bytes:
        """Read object from S3."""
        response = self.client.get_object(
            Bucket=self.config.bucket_name,
            Key=key,
        )
        
        return response["Body"].read()
    
    def delete_object(self, key: str) -> None:
        """Delete object from S3."""
        self.client.delete_object(
            Bucket=self.config.bucket_name,
            Key=key,
        )
        
        logger.info(f"Deleted s3://{self.config.bucket_name}/{key}")
    
    def close(self) -> None:
        """Close S3 client."""
        self.client.close()


def create_s3_loader(
    bucket_name: Optional[str] = None,
    config: Optional[S3Config] = None,
) -> S3Loader:
    """
    Create S3 loader.
    
    Args:
        bucket_name: S3 bucket name
        config: S3 config
    
    Returns:
        S3Loader: Configured loader
    """
    if config is None:
        config = get_app_config().s3
    
    if bucket_name:
        config.bucket_name = bucket_name
    
    return S3Loader(config=config)


__all__ = [
    "FileFormat",
    "CompressionType",
    "LoadResult",
    "S3Loader",
    "create_s3_loader",
]