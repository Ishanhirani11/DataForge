"""
Cache data loader for DataFlow Pro.

Loads data into Redis and other cache backends with support for:
- Multiple data structures (strings, hashes, lists, sets)
- TTL/expiration
- Serialization (JSON, Pickle)
- Batch operations
- Error handling
"""

from typing import Any, Dict, Iterator, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
import threading
import time
import json
import pickle
from functools import wraps

try:
    import redis
    from redis.exceptions import RedisError
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from dataflow_pro.config import get_app_config, RedisConfig
from dataflow_pro.utils.logger import get_logger
from dataflow_pro.utils.metrics import get_metrics


logger = get_logger(__name__)


class CacheDataType(str, Enum):
    """Cache data types."""
    STRING = "string"
    HASH = "hash"
    LIST = "list"
    SET = "set"
    SORTED_SET = "sorted_set"


class SerializationType(str, Enum):
    """Serialization types."""
    JSON = "json"
    PICKLE = "pickle"
    STRING = "string"


@dataclass
class CacheResult:
    """Result of cache operation."""
    keys_set: int
    keys_deleted: int = 0
    bytes_set: int = 0
    duration: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class CacheLoader:
    """
    Cache data loader with comprehensive caching functions.
    
    Supports:
    - Multiple data types
    - TTL/expiration
    - Multiple serialization formats
    - Batch operations
    - Pipeline operations
    """
    
    def __init__(
        self,
        config: Optional[RedisConfig] = None,
    ):
        if not REDIS_AVAILABLE:
            raise ImportError("redis is required for cache support")
        
        self.config = config or get_app_config().redis
        
        # Create Redis client
        self.client = redis.Redis(
            host=self.config.host,
            port=self.config.port,
            password=self.config.password,
            db=self.config.db,
            decode_responses=self.config.decode_responses,
            socket_timeout=self.config.socket_timeout,
            socket_connect_timeout=self.config.socket_connect_timeout,
            max_connections=self.config.max_connections,
        )
        
        # Metrics
        self.metrics = get_metrics()
        
        # Pipeline for batch operations
        self._pipeline: Optional[redis.Pipeline] = None
        
        # Thread safety
        self._lock = threading.Lock()
    
    def set_string(
        self,
        key: str,
        value: Union[str, int, float],
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Set string value.
        
        Args:
            key: Cache key
            value: Value to set
            ttl: Time to live in seconds
        
        Returns:
            bool: Success
        """
        try:
            if ttl:
                return self.client.setex(key, ttl, str(value))
            else:
                return self.client.set(key, str(value))
        except RedisError as e:
            logger.error(f"Cache error: {str(e)}")
            return False
    
    def get_string(self, key: str) -> Optional[str]:
        """Get string value."""
        try:
            return self.client.get(key)
        except RedisError as e:
            logger.error(f"Cache error: {str(e)}")
            return None
    
    def set_hash(
        self,
        key: str,
        mapping: Dict[str, Any],
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Set hash value.
        
        Args:
            key: Cache key
            mapping: Hash mapping
            ttl: Time to live in seconds
        
        Returns:
            bool: Success
        """
        try:
            # Convert values to strings
            str_mapping = {k: json.dumps(v) if not isinstance(v, str) else v
                          for k, v in mapping.items()}
            
            if ttl:
                pipe = self.client.pipeline()
                pipe.hset(key, mapping=str_mapping)
                pipe.expire(key, ttl)
                pipe.execute()
            else:
                self.client.hset(key, mapping=str_mapping)
            
            return True
        except RedisError as e:
            logger.error(f"Cache error: {str(e)}")
            return False
    
    def get_hash(self, key: str) -> Optional[Dict[str, Any]]:
        """Get hash value."""
        try:
            mapping = self.client.hgetall(key)
            
            # Try to parse JSON values
            result = {}
            for k, v in mapping.items():
                try:
                    result[k] = json.loads(v)
                except (json.JSONDecodeError, TypeError):
                    result[k] = v
            
            return result
        except RedisError as e:
            logger.error(f"Cache error: {str(e)}")
            return None
    
    def set_list(
        self,
        key: str,
        values: List[Any],
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Set list value.
        
        Args:
            key: Cache key
            values: List values
            ttl: Time to live in seconds
        
        Returns:
            bool: Success
        """
        try:
            # Convert values to strings
            str_values = [json.dumps(v) if not isinstance(v, str) else v
                          for v in values]
            
            if ttl:
                pipe = self.client.pipeline()
                pipe.rpush(key, *str_values)
                pipe.expire(key, ttl)
                pipe.execute()
            else:
                self.client.rpush(key, *str_values)
            
            return True
        except RedisError as e:
            logger.error(f"Cache error: {str(e)}")
            return False
    
    def get_list(self, key: str) -> Optional[List[Any]]:
        """Get list value."""
        try:
            values = self.client.lrange(key, 0, -1)
            
            # Try to parse JSON values
            result = []
            for v in values:
                try:
                    result.append(json.loads(v))
                except (json.JSONDecodeError, TypeError):
                    result.append(v)
            
            return result
        except RedisError as e:
            logger.error(f"Cache error: {str(e)}")
            return None
    
    def set_sorted_set(
        self,
        key: str,
        mapping: Dict[str, float],
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Set sorted set value.
        
        Args:
            key: Cache key
            mapping: Member to score mapping
            ttl: Time to live in seconds
        
        Returns:
            bool: Success
        """
        try:
            if mapping:
                if ttl:
                    pipe = self.client.pipeline()
                    pipe.zadd(key, mapping)
                    pipe.expire(key, ttl)
                    pipe.execute()
                else:
                    self.client.zadd(key, mapping)
            
            return True
        except RedisError as e:
            logger.error(f"Cache error: {str(e)}")
            return False
    
    def get_sorted_set(
        self,
        key: str,
        start: int = 0,
        end: int = -1,
        desc: bool = True,
    ) -> Optional[List[tuple]]:
        """Get sorted set value."""
        try:
            if desc:
                return self.client.zrevrange(key, start, end, withscores=True)
            else:
                return self.client.zrange(key, start, end, withscores=True)
        except RedisError as e:
            logger.error(f"Cache error: {str(e)}")
            return None
    
    def load(
        self,
        data: List[Dict[str, Any]],
        key_prefix: str = "data",
        data_type: CacheDataType = CacheDataType.HASH,
        ttl: Optional[int] = None,
    ) -> CacheResult:
        """
        Load data into cache.
        
        Args:
            data: Data to load
            key_prefix: Key prefix
            data_type: Cache data type
            ttl: Time to live in seconds
        
        Returns:
            CacheResult: Load result
        """
        start_time = time.time()
        
        keys_set = 0
        bytes_set = 0
        
        for i, row in enumerate(data):
            key = f"{key_prefix}:{i}"
            
            if data_type == CacheDataType.STRING:
                value = json.dumps(row)
                self.client.set(key, value)
                bytes_set += len(value)
                keys_set += 1
            
            elif data_type == CacheDataType.HASH:
                self.set_hash(key, row, ttl)
                keys_set += 1
            
            elif data_type == CacheDataType.LIST:
                self.set_list(key, [row], ttl)
                keys_set += 1
        
        duration = time.time() - start_time
        
        logger.info(f"Cache loading complete: {keys_set} keys in {duration:.2f}s")
        
        return CacheResult(
            keys_set=keys_set,
            bytes_set=bytes_set,
            duration=duration,
        )
    
    def load_batched(
        self,
        data: List[Dict[str, Any]],
        key_prefix: str = "data",
        ttl: Optional[int] = None,
    ) -> CacheResult:
        """
        Load data in batch using pipeline.
        
        Args:
            data: Data to load
            key_prefix: Key prefix
            ttl: Time to live in seconds
        
        Returns:
            CacheResult: Load result
        """
        start_time = time.time()
        
        pipe = self.client.pipeline()
        keys_set = 0
        bytes_set = 0
        
        for i, row in enumerate(data):
            key = f"{key_prefix}:{i}"
            value = json.dumps(row)
            
            pipe.set(key, value)
            
            if ttl:
                pipe.expire(key, ttl)
            
            bytes_set += len(value)
            keys_set += 1
        
        # Execute pipeline
        pipe.execute()
        
        duration = time.time() - start_time
        
        logger.info(f"Batch cache loading complete: {keys_set} keys in {duration:.2f}s")
        
        return CacheResult(
            keys_set=keys_set,
            bytes_set=bytes_set,
            duration=duration,
        )
    
    def delete(self, key: str) -> bool:
        """Delete key."""
        try:
            return bool(self.client.delete(key))
        except RedisError as e:
            logger.error(f"Cache error: {str(e)}")
            return False
    
    def delete_pattern(self, pattern: str) -> int:
        """Delete keys matching pattern."""
        try:
            keys = self.client.keys(pattern)
            if keys:
                return self.client.delete(*keys)
            return 0
        except RedisError as e:
            logger.error(f"Cache error: {str(e)}")
            return 0
    
    def exists(self, key: str) -> bool:
        """Check if key exists."""
        try:
            return bool(self.client.exists(key))
        except RedisError as e:
            logger.error(f"Cache error: {str(e)}")
            return False
    
    def get_ttl(self, key: str) -> int:
        """Get TTL for key."""
        try:
            return self.client.ttl(key)
        except RedisError as e:
            logger.error(f"Cache error: {str(e)}")
            return -2
    
    def expire(self, key: str, ttl: int) -> bool:
        """Set TTL for key."""
        try:
            return self.client.expire(key, ttl)
        except RedisError as e:
            logger.error(f"Cache error: {str(e)}")
            return False
    
    def flush_db(self) -> bool:
        """Flush current database."""
        try:
            return self.client.flushdb()
        except RedisError as e:
            logger.error(f"Cache error: {str(e)}")
            return False
    
    def close(self) -> None:
        """Close cache connection."""
        self.client.close()


def create_cache_loader(
    config: Optional[RedisConfig] = None,
) -> CacheLoader:
    """
    Create cache loader.
    
    Args:
        config: Redis config
    
    Returns:
        CacheLoader: Configured loader
    """
    return CacheLoader(config=config)


__all__ = [
    "CacheDataType",
    "SerializationType",
    "CacheResult",
    "CacheLoader",
    "create_cache_loader",
]