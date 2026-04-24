"""
Configuration management for DataFlow Pro.

This module handles all configuration settings using Pydantic Settings,
supporting environment variables and .env file loading.
"""

from typing import Any, Dict, Optional
from functools import lru_cache
from pathlib import Path
from enum import Enum

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(str, Enum):
    """Environment types."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class LogLevel(str, Enum):
    """Logging levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class DatabaseConfig(BaseSettings):
    """Database configuration settings."""
    
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, description="Database port")
    database: str = Field(default="dataflow", description="Database name")
    username: str = Field(default="dataflow_user", description="Database username")
    password: str = Field(default="", description="Database password", exclude_from_schema=True)
    pool_size: int = Field(default=5, description="Connection pool size")
    max_overflow: int = Field(default=10, description="Max connection overflow")
    pool_timeout: int = Field(default=30, description="Pool timeout in seconds")
    pool_recycle: int = Field(default=3600, description="Pool recycle time in seconds")
    echo: bool = Field(default=False, description="SQL echo mode")
    
    @property
    def database_url(self) -> str:
        """Generate database URL."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    @property
    def async_database_url(self) -> str:
        """Generate async database URL."""
        return f"postgresql+asyncpg://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class RedisConfig(BaseSettings):
    """Redis configuration settings."""
    
    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, description="Redis port")
    password: Optional[str] = Field(default=None, description="Redis password", exclude_from_schema=True)
    db: int = Field(default=0, description="Redis database number")
    decode_responses: bool = Field(default=True, description="Decode responses")
    socket_timeout: int = Field(default=5, description="Socket timeout")
    socket_connect_timeout: int = Field(default=5, description="Socket connect timeout")
    max_connections: int = Field(default=50, description="Max connections")
    
    @property
    def redis_url(self) -> str:
        """Generate Redis URL."""
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


class S3Config(BaseSettings):
    """AWS S3 configuration settings."""
    
    bucket_name: str = Field(default="dataflow-pro", description="S3 bucket name")
    region_name: str = Field(default="us-east-1", description="AWS region")
    access_key_id: Optional[str] = Field(default=None, description="AWS access key ID")
    secret_access_key: Optional[str] = Field(default=None, description="AWS secret access key", exclude_from_schema=True)
    endpoint_url: Optional[str] = Field(default=None, description="Custom endpoint URL (for S3-compatible services)")
    use_ssl: bool = Field(default=True, description="Use SSL")
    verify: bool = Field(default=True, description="Verify SSL certificates")
    
    @field_validator("access_key_id", "secret_access_key", mode="before")
    @classmethod
    def get_from_env(cls, v: Optional[str]) -> Optional[str]:
        """Get credentials from environment if not provided."""
        import os
        return v or os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("AWS_SECRET_ACCESS_KEY")


class APIConfig(BaseSettings):
    """External API configuration settings."""
    
    base_url: str = Field(default="", description="API base URL")
    timeout: int = Field(default=30, description="Request timeout in seconds")
    max_retries: int = Field(default=3, description="Max retry attempts")
    retry_delay: float = Field(default=1.0, description="Retry delay in seconds")
    rate_limit_calls: int = Field(default=100, description="Rate limit calls per window")
    rate_limit_period: int = Field(default=60, description="Rate limit period in seconds")


class DataQualityConfig(BaseSettings):
    """Data quality configuration settings."""
    
    enable_checks: bool = Field(default=True, description="Enable data quality checks")
    expectation_suite_name: str = Field(default="dataflow_pro_suite", description="Expectation suite name")
    data_context_dir: Optional[str] = Field(default=None, description="Great Expectations data context directory")
    fail_on_warning: bool = Field(default=False, description="Fail pipeline on warning")
    critical_expectations: list[str] = Field(default_factory=list, description="Critical expectations that cannot fail")


class PipelineConfig(BaseSettings):
    """Pipeline configuration settings."""
    
    batch_size: int = Field(default=10000, description="Batch size for processing")
    parallelism: int = Field(default=4, description="Parallel workers")
    error_threshold: float = Field(default=0.05, description="Error threshold percentage")
    checkpoint_frequency: int = Field(default=1000, description="Checkpoint frequency")
    enable_checkpointing: bool = Field(default=True, description="Enable checkpointing")
    temp_dir: str = Field(default="/tmp/dataflow", description="Temporary directory")


class MonitoringConfig(BaseSettings):
    """Monitoring and metrics configuration."""
    
    enable_metrics: bool = Field(default=True, description="Enable metrics collection")
    metrics_port: int = Field(default=9090, description="Metrics port")
    metrics_host: str = Field(default="0.0.0.0", description="Metrics host")
    health_check_port: int = Field(default=8080, description="Health check port")
    enable_tracing: bool = Field(default=False, description="Enable tracing")
    tracing_endpoint: Optional[str] = Field(default=None, description="Tracing endpoint")


class AirflowConfig(BaseSettings):
    """Airflow configuration settings."""
    
    base_url: str = Field(default="http://localhost:8080", description="Airflow base URL")
    username: str = Field(default="admin", description="Airflow username")
    password: str = Field(default="admin", description="Airflow password", exclude_from_schema=True)
    dag_run_timeout: int = Field(default=3600, description="DAG run timeout in seconds")
    max_active_runs: int = Field(default=1, description="Max active runs per DAG")
    catchup: bool = Field(default=False, description="Catchup past runs")


class AppConfig(BaseSettings):
    """Main application configuration."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Application
    app_name: str = Field(default="DataFlow Pro", description="Application name")
    environment: Environment = Field(default=Environment.DEVELOPMENT, description="Environment")
    debug: bool = Field(default=False, description="Debug mode")
    log_level: LogLevel = Field(default=LogLevel.INFO, description="Log level")
    
    # Feature Flags
    enable_data_quality: bool = Field(default=True, description="Enable data quality")
    enable_caching: bool = Field(default=True, description="Enable caching")
    enable_incremental: bool = Field(default=True, description="Enable incremental loading")
    enable_retry: bool = Field(default=True, description="Enable retry logic")
    
    # Sub-configurations
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    s3: S3Config = Field(default_factory=S3Config)
    api: APIConfig = Field(default_factory=APIConfig)
    data_quality: DataQualityConfig = Field(default_factory=DataQualityConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    airflow: AirflowConfig = Field(default_factory=AirflowConfig)
    
    def get_config_dict(self) -> Dict[str, Any]:
        """Get configuration as dictionary."""
        config_dict: Dict[str, Any] = {}
        for field_name, field_info in self.model_fields.items():
            value = getattr(self, field_name)
            if hasattr(value, 'get_config_dict'):
                config_dict[field_name] = value.get_config_dict()
            else:
                config_dict[field_name] = value
        return config_dict


@lru_cache()
def get_app_config() -> AppConfig:
    """
    Get application configuration (cached).
    
    This function returns a cached instance of AppConfig,
    ensuring consistent configuration across the application.
    
    Returns:
        AppConfig: Application configuration instance
    """
    return AppConfig()


def get_config_value(key: str, default: Any = None) -> Any:
    """
    Get a specific configuration value.
    
    Args:
        key: Configuration key (supports dot notation for nested values)
        default: Default value if key not found
    
    Returns:
        Any: Configuration value
    """
    config = get_app_config()
    keys = key.split(".")
    value = config
    for k in keys:
        if hasattr(value, k):
            value = getattr(value, k)
        else:
            return default
    return value


def reload_config() -> AppConfig:
    """
    Reload application configuration.
    
    This clears the cached configuration and returns a new instance.
    
    Returns:
        AppConfig: New application configuration instance
    """
    get_app_config.cache_clear()
    return get_app_config()


def validate_config() -> tuple[bool, list[str]]:
    """
    Validate application configuration.
    
    Returns:
        tuple: (is_valid, list of error messages)
    """
    errors: list[str] = []
    
    try:
        config = get_app_config()
        
        # Validate database config
        if not config.database.host:
            errors.append("Database host is required")
        
        # Validate pipeline config
        if config.pipeline.batch_size <= 0:
            errors.append("Batch size must be positive")
        
        if config.pipeline.parallelism <= 0:
            errors.append("Parallelism must be positive")
        
        # Validate environment
        if config.environment == Environment.PRODUCTION:
            if not config.database.password:
                errors.append("Database password is required in production")
            
            if not config.redis.password:
                errors.append("Redis password is required in production")
    
    except Exception as e:
        errors.append(f"Configuration validation error: {str(e)}")
    
    return len(errors) == 0, errors