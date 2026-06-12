"""
File data extractor for DataForge.

Extracts data from various file formats including CSV, JSON, Parquet,
Excel, XML, and more. Supports compression, encoding, and streaming.
"""

from typing import Any, Dict, Iterator, Optional, List, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
from pathlib import Path
import json
import csv
import gzip
import zipfile
import tarfile
import io
import re

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

try:
    import openpyxl
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

from dataforge.utils.logger import get_logger
from dataforge.utils.metrics import get_metrics


logger = get_logger(__name__)


class FileFormat(str, Enum):
    """Supported file formats."""
    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"
    PARQUET = "parquet"
    EXCEL = "excel"
    XML = "xml"
    TXT = "txt"
    FIXED_WIDTH = "fixed_width"


class CompressionType(str, Enum):
    """Compression types."""
    NONE = "none"
    GZIP = "gzip"
    ZIP = "zip"
    TAR = "tar"
    TAR_GZ = "tar.gz"


@dataclass
class FileConfig:
    """File extraction configuration."""
    format: FileFormat = FileFormat.CSV
    encoding: str = "utf-8"
    delimiter: str = ","
    quotechar: str = '"'
    escapechar: Optional[str] = None
    skip_rows: int = 0
    max_rows: Optional[int] = None
    usecols: Optional[List[str]] = None
    dtype: Optional[Dict[str, str]] = None
    
    # For JSON
    json_lines: bool = False
    json_object: bool = False
    
    # For Parquet
    columns: Optional[List[str]] = None
    filters: Optional[List[tuple]] = None

    # For Excel
    sheet_name: Optional[Union[str, int]] = 0

    # For fixed width
    colspecs: Optional[List[tuple]] = None


@dataclass
class ExtractionResult:
    """Result of file extraction."""
    data: List[Dict[str, Any]]
    rows_extracted: int
    file_path: str
    metadata: Dict[str, Any] = field(default_factory=dict)


class FileExtractor:
    """
    File data extractor with support for multiple formats.
    
    Supports:
    - CSV, JSON, JSONL, Parquet, Excel, XML
    - Streaming extraction
    - Compression (gzip, zip, tar)
    - Various encodings
    - Column selection
    - Type inference and casting
    """
    
    def __init__(
        self,
        config: Optional[FileConfig] = None,
    ):
        self.config = config or FileConfig()
        self.metrics = get_metrics()
    
    def _detect_format(self, file_path: str) -> FileFormat:
        """Detect file format from extension."""
        path = Path(file_path)
        extension = path.suffix.lower()
        
        format_map = {
            ".csv": FileFormat.CSV,
            ".tsv": FileFormat.CSV,
            ".json": FileFormat.JSON,
            ".jsonl": FileFormat.JSONL,
            ".ndjson": FileFormat.JSONL,
            ".parquet": FileFormat.PARQUET,
            ".pq": FileFormat.PARQUET,
            ".xlsx": FileFormat.EXCEL,
            ".xls": FileFormat.EXCEL,
            ".xml": FileFormat.XML,
            ".txt": FileFormat.TXT,
        }
        
        return format_map.get(extension, FileFormat.CSV)
    
    def _detect_compression(self, file_path: str) -> CompressionType:
        """Detect compression type."""
        path = Path(file_path)
        suffix = path.suffix.lower()
        
        if suffix == ".gz":
            if path.stem.endswith(".tar"):
                return CompressionType.TAR_GZ
            return CompressionType.GZIP
        elif suffix == ".zip":
            return CompressionType.ZIP
        elif suffix in [".tar", ".tgz"]:
            return CompressionType.TAR
        
        return CompressionType.NONE
    
    def _open_file(self, file_path: str, mode: str = "r"):
        """Open file with appropriate handler."""
        path = Path(file_path)
        compression = self._detect_compression(file_path)
        
        if compression == CompressionType.GZIP:
            return gzip.open(path, mode)
        elif compression in [CompressionType.TAR, CompressionType.TAR_GZ]:
            return tarfile.open(path, mode)
        
        # For text files
        if "b" not in mode:
            return open(
                path,
                mode,
                encoding=self.config.encoding,
            )
        
        return open(path, mode)
    
    def _read_csv(self, file_path: str) -> List[Dict[str, Any]]:
        """Read CSV file."""
        rows = []
        
        with self._open_file(file_path, "r") as f:
            reader = csv.DictReader(
                f,
                delimiter=self.config.delimiter,
                quotechar=self.config.quotechar,
                escapechar=self.config.escapechar,
            )
            
            for i, row in enumerate(reader):
                # Skip rows if configured
                if self.config.skip_rows and i < self.config.skip_rows:
                    continue
                if self.config.max_rows and (i - (self.config.skip_rows or 0)) >= self.config.max_rows:
                    break
                
                # Try to cast types
                row = self._cast_row_types(row)
                rows.append(row)
        
        return rows
    
    def _read_json(self, file_path: str) -> List[Dict[str, Any]]:
        """Read JSON file."""
        rows = []
        
        with self._open_file(file_path, "r") as f:
            if self.config.json_lines:
                # JSON Lines format
                for i, line in enumerate(f):
                    if self.config.max_rows and i >= self.config.max_rows:
                        break
                    
                    line = line.strip()
                    if line:
                        rows.append(json.loads(line))
            else:
                # Single JSON array or object
                data = json.load(f)
                
                if isinstance(data, list):
                    rows = data
                elif isinstance(data, dict):
                    # Check for data wrapper
                    if "data" in data:
                        rows = data["data"]
                    else:
                        rows = [data]
                
                # Apply limit
                if self.config.max_rows:
                    rows = rows[:self.config.max_rows]
        
        return rows
    
    def _read_jsonl(self, file_path: str) -> List[Dict[str, Any]]:
        """Read JSONL file."""
        self.config.json_lines = True
        return self._read_json(file_path)
    
    def _read_parquet(self, file_path: str) -> List[Dict[str, Any]]:
        """Read Parquet file."""
        if not PYARROW_AVAILABLE:
            raise ImportError("pyarrow is required for Parquet support")
        
        table = pq.read_table(
            file_path,
            columns=self.config.columns,
            filters=self.config.filters,
        )
        
        df = table.to_pandas()
        
        # Apply limit
        if self.config.max_rows:
            df = df.head(self.config.max_rows)
        
        return df.to_dict(orient="records")
    
    def _read_excel(self, file_path: str) -> List[Dict[str, Any]]:
    """Read Excel file."""
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for Excel support")
    
        df = pd.read_excel(
            file_path,
            sheet_name=self.config.sheet_name,
            usecols=self.config.usecols,
            nrows=self.config.max_rows,
        )
    
        # If multiple sheets are returned, use the first one
        if isinstance(df, dict):
            first_sheet = next(iter(df))
            df = df[first_sheet]
    
        return df.to_dict(orient="records")
    
    def _read_xml(self, file_path: str) -> List[Dict[str, Any]]:
        """Read XML file."""
        import xml.etree.ElementTree as ET
        
        rows = []
        
        with self._open_file(file_path, "r") as f:
            tree = ET.parse(f)
            root = tree.getroot()
            
            # Get all child elements
            for i, elem in enumerate(root):
                if self.config.max_rows and i >= self.config.max_rows:
                    break
                
                row = {}
                for child in elem:
                    row[child.tag] = child.text
                
                rows.append(row)
        
        return rows
    
    def _read_fixed_width(self, file_path: str) -> List[Dict[str, Any]]:
        """Read fixed-width text file."""
        rows = []
        
        with self._open_file(file_path, "r") as f:
            for i, line in enumerate(f):
                if self.config.skip_rows and i < self.config.skip_rows:
                    continue
                
                if self.config.max_rows and i >= self.config.max_rows + self.config.skip_rows:
                    break
                
                row = {}
                pos = 0
                
                if self.config.colspecs:
                    for col_idx, (start, end) in enumerate(self.config.colspecs):
                        row[f"col_{col_idx}"] = line[start:end].strip()
                
                rows.append(row)
        
        return rows
    
    def _cast_row_types(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Cast row values to appropriate types."""
        if not self.config.dtype:
            return row
        
        casted_row = {}
        
        for key, value in row.items():
            target_type = self.config.dtype.get(key)
            
            if target_type and value:
                try:
                    if target_type == "int":
                        casted_row[key] = int(value)
                    elif target_type == "float":
                        casted_row[key] = float(value)
                    elif target_type == "bool":
                        casted_row[key] = value.lower() in ("true", "1", "yes")
                    else:
                        casted_row[key] = value
                except (ValueError, TypeError):
                    casted_row[key] = value
            else:
                casted_row[key] = value
        
        return casted_row
    
    def extract(
        self,
        file_path: str,
        format: Optional[FileFormat] = None,
    ) -> ExtractionResult:
        """
        Extract data from file.
        
        Args:
            file_path: Path to file
            format: File format (auto-detected if not provided)
        
        Returns:
            ExtractionResult: Extraction result
        """
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Detect format
        if format is None:
            format = self._detect_format(file_path)
        
        self.config.format = format
        
        logger.info(f"Reading {format.value} file: {file_path}")
        
        # Read based on format
        if format == FileFormat.CSV:
            data = self._read_csv(file_path)
        elif format == FileFormat.JSON:
            data = self._read_json(file_path)
        elif format == FileFormat.JSONL:
            data = self._read_jsonl(file_path)
        elif format == FileFormat.PARQUET:
            data = self._read_parquet(file_path)
        elif format == FileFormat.EXCEL:
            data = self._read_excel(file_path)
        elif format == FileFormat.XML:
            data = self._read_xml(file_path)
        elif format == FileFormat.FIXED_WIDTH:
            data = self._read_fixed_width(file_path)
        else:
            # Default to CSV
            data = self._read_csv(file_path)
        
        # Get metadata
        metadata = {
            "file_name": path.name,
            "file_size": path.stat().st_size,
            "format": format.value,
        }
        
        logger.info(f"Extracted {len(data)} rows from {file_path}")
        
        return ExtractionResult(
            data=data,
            rows_extracted=len(data),
            file_path=str(file_path),
            metadata=metadata,
        )
    
    def extract_streaming(
        self,
        file_path: str,
        format: Optional[FileFormat] = None,
        chunk_size: int = 1000,
    ) -> Iterator[list]:
        """
        Extract data in chunks for large files.
        
        Args:
            file_path: Path to file
            format: File format
            chunk_size: Number of rows per chunk
        
        Yields:
            list: Chunks of data
        """
        # For now, read all and yield in chunks
        # Could be optimized for specific formats
        result = self.extract(file_path, format)
        
        for i in range(0, len(result.data), chunk_size):
            yield result.data[i:i + chunk_size]
    
    def extract_to_dataframe(
        self,
        file_path: str,
        format: Optional[FileFormat] = None,
    ) -> "pd.DataFrame":
        """
        Extract data to Pandas DataFrame.
        
        Args:
            file_path: Path to file
            format: File format
        
        Returns:
            pd.DataFrame: DataFrame
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for DataFrame output")
        
        result = self.extract(file_path, format)
        
        return pd.DataFrame(result.data)
    
    def list_files(
        self,
        directory: str,
        pattern: str = "*",
        recursive: bool = False,
    ) -> List[str]:
        """
        List files in directory matching pattern.
        
        Args:
            directory: Directory path
            pattern: File pattern (glob)
            recursive: Recursive search
        
        Returns:
            List[str]: List of file paths
        """
        path = Path(directory)
        
        if recursive:
            files = list(path.rglob(pattern))
        else:
            files = list(path.glob(pattern))
        
        return [str(f) for f in files if f.is_file()]
    
    def extract_directory(
        self,
        directory: str,
        pattern: str = "*",
        recursive: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Extract all files in directory.
        
        Args:
            directory: Directory path
            pattern: File pattern
            recursive: Recursive search
        
        Returns:
            List[Dict[str, Any]]: Combined data
        """
        files = self.list_files(directory, pattern, recursive)
        
        all_data = []
        
        for file_path in files:
            try:
                result = self.extract(file_path)
                all_data.extend(result.data)
            except Exception as e:
                logger.warning(f"Failed to extract {file_path}: {str(e)}")
        
        return all_data


def create_file_extractor(
    format: FileFormat = FileFormat.CSV,
    encoding: str = "utf-8",
    delimiter: str = ",",
    **kwargs,
) -> FileExtractor:
    """
    Factory function to create file extractor.
    
    Args:
        format: File format
        encoding: File encoding
        delimiter: CSV delimiter
    
    Returns:
        FileExtractor: Configured extractor
    """
    config = FileConfig(
        format=format,
        encoding=encoding,
        delimiter=delimiter,
        **kwargs,
    )
    
    return FileExtractor(config)


__all__ = [
    "FileFormat",
    "CompressionType",
    "FileConfig",
    "ExtractionResult",
    "FileExtractor",
    "create_file_extractor",
]
