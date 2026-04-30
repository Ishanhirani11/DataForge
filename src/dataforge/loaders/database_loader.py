"""
Database data loader for DataForge.

Loads data into databases with support for:
- Batch inserts
- Upserts (insert or update)
- Bulk operations
- Transaction management
- Error handling and recovery
"""

from typing import Any, Dict, Iterator, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging
import threading
from contextlib import contextmanager
import time

from sqlalchemy import (
    create_engine,
    Engine,
    text,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Boolean,
    JSON,
    insert,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

from dataforge.config import get_app_config, DatabaseConfig
from dataforge.utils.logger import get_logger
from dataforge.utils.metrics import get_metrics


logger = get_logger(__name__)


class LoadStrategy(str, Enum):
    """Load strategies."""
    INSERT = "insert"
    UPSERT = "upsert"
    UPDATE = "update"
    REPLACE = "replace"


@dataclass
class LoadResult:
    """Result of data loading."""
    rows_loaded: int
    rows_updated: int = 0
    rows_failed: int = 0
    duration: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class DatabaseLoader:
    """
    Database loader with comprehensive loading functions.
    
    Supports:
    - Batch inserts
    - Upserts (PostgreSQL ON CONFLICT)
    - Bulk loading
    - Transaction management
    - Error handling and recovery
    - Connection pooling
    """
    
    def __init__(
        self,
        config: Optional[DatabaseConfig] = None,
        table_name: Optional[str] = None,
    ):
        self.config = config if config is not None else get_app_config().database
        self.table_name = table_name
        
        # Create engine
        self.engine = self._create_engine()
        
        # Metadata
        self.metadata = MetaData()
        
        # Metrics
        self.metrics = get_metrics()
        
        # Thread safety
        self._lock = threading.Lock()
    
    def _create_engine(self) -> Engine:
        """Create database engine."""
        engine = create_engine(
            self.config.database_url,
            poolclass=QueuePool,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_timeout=self.config.pool_timeout,
            pool_recycle=self.config.pool_recycle,
            pool_pre_ping=True,
            echo=self.config.echo,
        )
        
        logger.info(f"Created database engine: {self.config.host}")
        
        return engine

    def _get_table(self, table_name: Optional[str] = None) -> Table:
        """Reflect a table from the configured engine."""
        resolved_name = table_name or self.table_name
        if not resolved_name:
            raise ValueError("table_name is required for database load operations")

        return Table(
            resolved_name,
            self.metadata,
            autoload_with=self.engine,
            extend_existing=True,
        )
    
    @contextmanager
    def get_connection(self):
        """Get database connection with context manager."""
        conn = self.engine.connect()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {str(e)}")
            raise
        finally:
            conn.close()
    
    def load(
        self,
        data: List[Dict[str, Any]],
        columns: Optional[List[str]] = None,
        strategy: LoadStrategy = LoadStrategy.INSERT,
        batch_size: int = 1000,
    ) -> LoadResult:
        """
        Load data into database.
        
        Args:
            data: Data to load
            columns: Column names (auto-detected if not provided)
            strategy: Load strategy
            batch_size: Batch size
        
        Returns:
            LoadResult: Load result
        """
        if not data:
            return LoadResult(rows_loaded=0)
        
        start_time = time.time()
        
        logger.info(f"Loading {len(data)} rows with strategy {strategy.value}")
        
        # Detect columns
        if not columns:
            columns = list(data[0].keys())
        
        rows_loaded = 0
        rows_updated = 0
        rows_failed = 0
        
        # Load in batches
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            
            try:
                if strategy == LoadStrategy.INSERT:
                    loaded, updated = self._batch_insert(batch, columns)
                elif strategy == LoadStrategy.UPSERT:
                    loaded, updated = self._batch_upsert(batch, columns)
                elif strategy == LoadStrategy.REPLACE:
                    loaded, updated = self._batch_replace(batch, columns)
                else:
                    loaded, updated = self._batch_insert(batch, columns)
                
                rows_loaded += loaded
                rows_updated += updated
            
            except Exception as e:
                logger.error(f"Batch {i // batch_size} failed: {str(e)}")
                rows_failed += len(batch)
        
        duration = time.time() - start_time
        
        logger.info(
            f"Loading complete: {rows_loaded} loaded, {rows_updated} updated, "
            f"{rows_failed} failed in {duration:.2f}s"
        )
        
        return LoadResult(
            rows_loaded=rows_loaded,
            rows_updated=rows_updated,
            rows_failed=rows_failed,
            duration=duration,
            metadata={
                "strategy": strategy.value,
                "batch_size": batch_size,
            },
        )
    
    def _batch_insert(
        self,
        data: List[Dict[str, Any]],
        columns: List[str],
    ) -> tuple[int, int]:
        """Batch insert."""
        table = self._get_table()
        with self.get_connection() as conn:
            stmt = insert(table).values(data)
            result = conn.execute(stmt)
            
            return result.rowcount, 0
    
    def _batch_upsert(
        self,
        data: List[Dict[str, Any]],
        columns: List[str],
    ) -> tuple[int, int]:
        """Batch upsert (PostgreSQL ON CONFLICT)."""
        if not self.table_name:
            return self._batch_insert(data, columns)

        table = self._get_table()
        with self.get_connection() as conn:
            if conn.dialect.name != "postgresql":
                return self._batch_insert(data, columns)

            stmt = postgresql.insert(table).values(data)
            primary_columns = [column.name for column in table.primary_key.columns] or ["id"]
            update_columns = [c for c in columns if c not in primary_columns]

            if update_columns:
                on_conflict = stmt.on_conflict_do_update(
                    index_elements=primary_columns,
                    set_={
                        col: getattr(stmt.excluded, col)
                        for col in update_columns
                    }
                )
            else:
                on_conflict = stmt.on_conflict_do_nothing(index_elements=primary_columns)
            
            result = conn.execute(on_conflict)
            
            return result.rowcount, 0
    
    def _batch_replace(
        self,
        data: List[Dict[str, Any]],
        columns: List[str],
    ) -> tuple[int, int]:
        """Batch replace (delete and insert)."""
        if not self.table_name:
            return self._batch_insert(data, columns)

        table = self._get_table()
        with self.get_connection() as conn:
            with conn.begin():
                conn.execute(table.delete())
                stmt = insert(table).values(data)
                result = conn.execute(stmt)
            
            return result.rowcount, 0
    
    def load_streaming(
        self,
        data_iterator: Iterator[List[Dict[str, Any]]],
        columns: Optional[List[str]] = None,
        strategy: LoadStrategy = LoadStrategy.INSERT,
        batch_size: int = 1000,
    ) -> LoadResult:
        """
        Load data from iterator.
        
        Args:
            data_iterator: Data iterator
            columns: Column names
            strategy: Load strategy
            batch_size: Batch size
        
        Returns:
            LoadResult: Load result
        """
        total_loaded = 0
        total_updated = 0
        total_failed = 0
        start_time = time.time()
        
        for batch in data_iterator:
            result = self.load(batch, columns, strategy, batch_size)
            total_loaded += result.rows_loaded
            total_updated += result.rows_updated
            total_failed += result.rows_failed
        
        duration = time.time() - start_time
        
        return LoadResult(
            rows_loaded=total_loaded,
            rows_updated=total_updated,
            rows_failed=total_failed,
            duration=duration,
        )
    
    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Execute raw SQL query.
        
        Args:
            query: SQL query
            params: Query parameters
        
        Returns:
            Any: Query result
        """
        with self.get_connection() as conn:
            result = conn.execute(text(query), params or {})
            
            if result.returns_rows:
                return [dict(row._mapping) for row in result]
            
            return result.rowcount
    
    def create_table(
        self,
        table_name: str,
        columns: Dict[str, str],
        primary_key: Optional[List[str]] = None,
    ) -> None:
        """
        Create table.
        
        Args:
            table_name: Table name
            columns: Column definitions {name: type}
            primary_key: Primary key columns
        """
        from sqlalchemy import Table, Column, Integer, String, Float, DateTime, Boolean, JSON
        
        column_map = {
            "integer": Integer,
            "string": String,
            "float": Float,
            "datetime": DateTime,
            "boolean": Boolean,
            "json": JSON,
        }
        
        column_objects = []
        for name, col_type in columns.items():
            sql_type = column_map.get(col_type, String)
            column_objects.append(Column(name, sql_type))
        
        if primary_key:
            for col in column_objects:
                if col.name in primary_key:
                    col.primary_key = True
        
        table = Table(table_name, self.metadata, *column_objects)
        
        with self.engine.begin() as conn:
            table.create(conn, checkfirst=True)
        
        logger.info(f"Created table: {table_name}")
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        from sqlalchemy import inspect
        
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()
    
    def get_row_count(self, table_name: str) -> int:
        """Get row count."""
        result = self.execute_query(f"SELECT COUNT(*) AS count FROM {table_name}")
        return result[0].get("count", 0) if result else 0
    
    def truncate(self, table_name: str) -> int:
        """Truncate table."""
        return self.execute_query(f"TRUNCATE TABLE {table_name}")
    
    def close(self) -> None:
        """Close database connection."""
        self.engine.dispose()
        logger.info("Database connection closed")


def create_database_loader(
    table_name: str,
    config: Optional[DatabaseConfig] = None,
) -> DatabaseLoader:
    """
    Create database loader.
    
    Args:
        table_name: Table name
        config: Database config
    
    Returns:
        DatabaseLoader: Configured loader
    """
    return DatabaseLoader(config=config, table_name=table_name)


__all__ = [
    "LoadStrategy",
    "LoadResult",
    "DatabaseLoader",
    "create_database_loader",
]
