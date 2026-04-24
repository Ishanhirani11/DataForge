"""
Database data extractor for DataFlow Pro.

Extracts data from PostgreSQL, MySQL, and other SQL databases
with support for batching, incremental loading, and change data capture.
"""

from typing import Any, Dict, Iterator, Optional, List, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging
from datetime import datetime
import threading

from sqlalchemy import (
    create_engine,
    Engine,
    text,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DateTime,
    inspect,
)
from sqlalchemy.engine import Result
from sqlalchemy.dialects import postgresql, mysql
from sqlalchemy.pool import QueuePool, NullPool

from dataflow_pro.config import get_app_config
from dataflow_pro.utils.logger import get_logger
from dataflow_pro.utils.metrics import get_metrics


logger = get_logger(__name__)


class DatabaseType(str, Enum):
    """Supported database types."""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"


@dataclass
class QueryConfig:
    """Query configuration."""
    query: str
    params: Optional[Dict[str, Any]] = None
    fetch_size: int = 10000
    timeout: int = 300
    
    # For incremental loading
    incremental_column: Optional[str] = None
    last_value: Optional[Any] = None


@dataclass
class ExtractionResult:
    """Result of database extraction."""
    data: List[Dict[str, Any]]
    rows_extracted: int
    last_value: Optional[Any] = None
    has_more: bool = False
    execution_time: float = 0.0


class DatabaseExtractor:
    """
    Database data extractor with advanced features.
    
    Supports:
    - Multiple database backends
    - Batched extraction
    - Incremental loading
    - Change Data Capture (CDC) patterns
    - Query optimization
    - Connection pooling
    """
    
    def __init__(
        self,
        database_url: Optional[str] = None,
        database_type: DatabaseType = DatabaseType.POSTGRESQL,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 3600,
        echo: bool = False,
    ):
        # Get config from app config if not provided
        config = get_app_config()
        
        if database_url is None:
            database_url = config.database.database_url
        
        self.database_url = database_url
        self.database_type = database_type
        
        # Create engine
        self.engine = self._create_engine(
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            echo=echo,
        )
        
        # Setup metadata
        self.metadata = MetaData()
        
        # Metrics
        self.metrics = get_metrics()
        
        # Thread safety
        self._lock = threading.Lock()
    
    def _create_engine(
        self,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 3600,
        echo: bool = False,
    ) -> Engine:
        """Create database engine with connection pooling."""
        engine = create_engine(
            self.database_url,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            pool_pre_ping=True,
            echo=echo,
        )
        
        logger.info(f"Created {self.database_type} engine: {engine.url.host}")
        
        return engine
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Get column names for a table.
        
        Args:
            table_name: Table name
        
        Returns:
            List[str]: Column names
        """
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name)
        return [col["name"] for col in columns]
    
    def get_table_primary_key(self, table_name: str) -> Optional[List[str]]:
        """Get primary key columns."""
        inspector = inspect(self.engine)
        pks = inspector.get_pk_constraint(table_name)
        return pks.get("constrained_columns")
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()
    
    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        fetch_size: int = 10000,
        timeout: int = 300,
    ) -> Result:
        """
        Execute SQL query.
        
        Args:
            query: SQL query
            params: Query parameters
            fetch_size: Fetch size
            timeout: Query timeout in seconds
        
        Returns:
            Result: SQLAlchemy result
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text(query),
                params or {},
            )
            return result
    
    def extract(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        fetch_size: int = 10000,
        incremental_column: Optional[str] = None,
        last_value: Optional[Any] = None,
    ) -> ExtractionResult:
        """
        Extract data from database.
        
        Args:
            query: SQL query
            params: Query parameters
            fetch_size: Fetch size
            incremental_column: Column for incremental extraction
            last_value: Last extracted value
        
        Returns:
            ExtractionResult: Extraction result
        """
        import time
        start_time = time.time()
        
        # Build query
        full_query = query
        
        # Add incremental filter
        if incremental_column and last_value is not None:
            if "WHERE" in query.upper():
                full_query = f"{query} AND {incremental_column} > :last_value"
            else:
                full_query = f"{query} WHERE {incremental_column} > :last_value"
            
            params = params or {}
            params["last_value"] = last_value
        
        # Add limit
        if "LIMIT" not in full_query.upper():
            full_query = f"{full_query} LIMIT :fetch_size"
            params = params or {}
            params["fetch_size"] = fetch_size
        
        logger.debug(f"Executing query: {full_query}")
        logger.debug(f"Query params: {params}")
        
        # Execute
        try:
            result = self.execute_query(
                full_query,
                params,
                fetch_size,
            )
            
            # Convert to list of dicts
            rows = [dict(row._mapping) for row in result]
            
            execution_time = time.time() - start_time
            
            # Get last value
            last_record_value = None
            if incremental_column and rows:
                last_record_value = rows[-1].get(incremental_column)
            
            extraction_result = ExtractionResult(
                data=rows,
                rows_extracted=len(rows),
                last_value=last_record_value,
                has_more=len(rows) == fetch_size,
                execution_time=execution_time,
            )
            
            logger.info(
                f"Extracted {len(rows)} rows in {execution_time:.2f}s"
            )
            
            return extraction_result
        
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def extract_batched(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
    ) -> Iterator[list]:
        """
        Extract data in batches.
        
        Args:
            query: SQL query
            params: Query parameters
            batch_size: Batch size
        
        Yields:
            list: Batches of data
        """
        offset = 0
        
        while True:
            # Build batched query
            batched_query = f"{query} LIMIT :limit OFFSET :offset"
            
            batch_params = params or {}
            batch_params["limit"] = batch_size
            batch_params["offset"] = offset
            
            result = self.execute_query(batched_query, batch_params)
            rows = [dict(row._mapping) for row in result]
            
            if not rows:
                break
            
            yield rows
            
            offset += batch_size
            
            self.metrics.record_processed(
                query[:50],
                "extracted",
                len(rows)
            )
            
            # Check if we got less than batch_size
            if len(rows) < batch_size:
                break
    
    def extract_incremental(
        self,
        table_name: str,
        columns: List[str],
        incremental_column: str,
        last_value: Optional[Any] = None,
        batch_size: int = 10000,
    ) -> Iterator[list]:
        """
        Extract data incrementally.
        
        This method extracts data where incremental_column > last_value,
        useful for CDC patterns.
        
        Args:
            table_name: Table name
            columns: Columns to extract
            incremental_column: Column for incremental filtering
            last_value: Last extracted value
            batch_size: Batch size
        
        Yields:
            list: Batches of data
        """
        columns_str = ", ".join(columns)
        
        query = f"SELECT {columns_str} FROM {table_name}"
        
        params = {}
        
        if last_value is not None:
            query = f"{query} WHERE {incremental_column} > :last_value"
            params["last_value"] = last_value
        
        query = f"{query} ORDER BY {incremental_column}"
        
        for batch in self.extract_batched(query, params, batch_size):
            yield batch
    
    def extract_to_file(
        self,
        query: str,
        output_path: str = "output.csv",
        params: Optional[Dict[str, Any]] = None,
        format: str = "csv",
    ) -> int:
        """
        Extract data to file.
        
        Args:
            query: SQL query
            output_path: Output file path
            params: Query parameters
            format: Output format (csv, json)
        
        Returns:
            int: Total rows extracted
        """
        import csv
        import json
        
        all_data = []
        
        for batch in self.extract_batched(query, params):
            all_data.extend(batch)
        
        if format == "csv" and all_data:
            with open(output_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=all_data[0].keys())
                writer.writeheader()
                writer.writerows(all_data)
        
        elif format == "json":
            with open(output_path, "w") as f:
                json.dump(all_data, f, indent=2, default=str)
        
        logger.info(f"Extracted {len(all_data)} rows to {output_path}")
        
        return len(all_data)
    
    def get_row_count(
        self,
        table_name: str,
        where_clause: Optional[str] = None,
    ) -> int:
        """
        Get row count for a table.
        
        Args:
            table_name: Table name
            where_clause: Optional WHERE clause
        
        Returns:
            int: Row count
        """
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        
        if where_clause:
            query = f"{query} WHERE {where_clause}"
        
        result = self.execute_query(query)
        row = result.fetchone()
        
        return row[0] if row else 0
    
    def get_max_value(self, table_name: str, column: str) -> Optional[Any]:
        """Get maximum value of a column."""
        query = f"SELECT MAX({column}) as max_val FROM {table_name}"
        
        result = self.execute_query(query)
        row = result.fetchone()
        
        return row[0] if row else None
    
    def get_min_value(self, table_name: str, column: str) -> Optional[Any]:
        """Get minimum value of a column."""
        query = f"SELECT MIN({column}) as min_val FROM {table_name}"
        
        result = self.execute_query(query)
        row = result.fetchone()
        
        return row[0] if row else None
    
    def begin(self):
        """Begin transaction."""
        return self.engine.begin()
    
    def close(self) -> None:
        """Close database connection."""
        self.engine.dispose()
        logger.info("Database connection closed")


# Factory function
def create_database_extractor(
    database_type: DatabaseType = DatabaseType.POSTGRESQL,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    **kwargs,
) -> DatabaseExtractor:
    """
    Create database extractor.
    
    Args:
        database_type: Database type
        host: Database host
        port: Database port
        database: Database name
        username: Username
        password: Password
    
    Returns:
        DatabaseExtractor: Configured extractor
    """
    config = get_app_config()
    
    # Get values from config if not provided
    host = host or config.database.host
    port = port or config.database.port
    database = database or config.database.database
    username = username or config.database.username
    password = password or config.database.password
    
    # Build database URL
    database_url = f"{database_type.value}://{username}:{password}@{host}:{port}/{database}"
    
    return DatabaseExtractor(
        database_url=database_url,
        database_type=database_type,
        **kwargs,
    )


__all__ = [
    "DatabaseType",
    "QueryConfig",
    "ExtractionResult",
    "DatabaseExtractor",
    "create_database_extractor",
]