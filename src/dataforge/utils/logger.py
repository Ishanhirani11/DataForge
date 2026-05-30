"""
Logging utilities for DataForge.

Provides structured logging with different output formats,
log levels, and handlers. Supports both file and console logging.
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime
from functools import lru_cache
import threading
import json


class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    
    Outputs logs in JSON format for easy parsing
    by log aggregation systems.
    """
    
    def __init__(self, include_extra: bool = True):
        super().__init__()
        self.include_extra = include_extra
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields
        if self.include_extra:
            extra_fields = {
                key: value for key, value in record.__dict__.items()
                if key not in logging.LogRecord(
                    "name", logging.DEBUG, "", 0, "", (), None
                ).__dict__ and key not in [
                    "args", "exc_info", "exc_text", "levelname",
                    "levelno", "lineno", "msg", "module", "msecs",
                    "name", "pathname", "process", "processName",
                    "relativeCreated", "stack_info", "thread", "threadName",
                    "created", "filename", "funcName"
                ]
            }
            if extra_fields:
                log_data["extra"] = extra_fields
        
        # Add custom fields
        if hasattr(record, "pipeline_name"):
            log_data["pipeline_name"] = record.pipeline_name
        if hasattr(record, "task_id"):
            log_data["task_id"] = record.task_id
        if hasattr(record, "batch_id"):
            log_data["batch_id"] = record.batch_id
        
        return json.dumps(log_data)


class ColoredFormatter(logging.Formatter):
    """
    Colored formatter for console output.
    
    Uses ANSI color codes for different log levels.
    """
    
    COLORS = {
        "DEBUG": "\033[36m",      # Cyan
        "INFO": "\033[32m",       # Green
        "WARNING": "\033[33m",   # Yellow
        "ERROR": "\033[31m",      # Red
        "CRITICAL": "\033[35m",   # Magenta
    }
    RESET = "\033[0m"
    
    def __init__(self, fmt: Optional[str] = None, datefmt: Optional[str] = None):
        super().__init__(fmt, datefmt)
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors."""
        color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        return super().format(record)


class ContextFilter(logging.Filter):
    """
    Context filter for adding contextual information to logs.
    
    Adds pipeline name, task ID, and other context
    to log records automatically.
    """
    
    _context: dict = {}
    _lock = threading.Lock()
    
    @classmethod
    def set_context(cls, **kwargs) -> None:
        """Set context values."""
        with cls._lock:
            cls._context.update(kwargs)
    
    @classmethod
    def clear_context(cls) -> None:
        """Clear context values."""
        with cls._lock:
            cls._context.clear()
    
    @classmethod
    def get_context(cls) -> dict:
        """Get current context."""
        with cls._lock:
            return cls._context.copy()
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add context to log record."""
        for key, value in self._context.items():
            setattr(record, key, value)
        return True


class LoggerConfig:
    """Logger configuration."""
    
    def __init__(
        self,
        name: str = "dataforge",
        level: str = "INFO",
        log_file: Optional[str] = None,
        json_format: bool = False,
        colored: bool = True,
    ):
        self.name = name
        self.level = getattr(logging, level.upper())
        self.log_file = log_file
        self.json_format = json_format
        self.colored = colored
    
    def create_logger(self) -> logging.Logger:
        """Create and configure logger."""
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)
        logger.propagate = False
        
        # Remove existing handlers
        logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.level)
        
        if self.json_format:
            console_handler.setFormatter(JSONFormatter())
        elif self.colored and sys.stdout.isatty():
            console_handler.setFormatter(
                ColoredFormatter(
                    "%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d - %(message)s",
                    "%Y-%m-%d %H:%M:%S"
                )
            )
        else:
            console_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d - %(message)s",
                    "%Y-%m-%d %H:%M:%S"
                )
            )
        
        logger.addHandler(console_handler)
        
        # File handler
        if self.log_file:
            log_path = Path(self.log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.FileHandler(log_path)
            file_handler.setLevel(self.level)
            file_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d - %(message)s",
                    "%Y-%m-%d %H:%M:%S"
                )
            )
            logger.addHandler(file_handler)
        
        # Add context filter
        logger.addFilter(ContextFilter())
        
        return logger


@lru_cache()
def get_logger(
    name: str = "dataforge",
    level: str = "INFO",
    log_file: Optional[str] = None,
    json_format: bool = False,
    colored: bool = True,
) -> logging.Logger:
    """
    Get configured logger (cached).
    
    Args:
        name: Logger name
        level: Log level
        log_file: Optional log file path
        json_format: Use JSON format
        colored: Use colored output
    
    Returns:
        logging.Logger: Configured logger
    """
    config = LoggerConfig(
        name=name,
        level=level,
        log_file=log_file,
        json_format=json_format,
        colored=colored,
    )
    return config.create_logger()


def get_pipeline_logger(
    pipeline_name: str,
    task_id: Optional[str] = None,
    batch_id: Optional[str] = None,
) -> logging.Logger:
    """
    Get logger with pipeline context.
    
    Args:
        pipeline_name: Name of the pipeline
        task_id: Optional task ID
        batch_id: Optional batch ID
    
    Returns:
        logging.Logger: Logger with context
    """
    logger = get_logger(f"dataforge.{pipeline_name}")
    
    ContextFilter.set_context(
        pipeline_name=pipeline_name,
        task_id=task_id,
        batch_id=batch_id,
    )
    
    return logger


def log_execution(
    logger: logging.Logger,
    func_name: str,
    *args,
    **kwargs
) -> logging.Logger:
    """
    Decorator for logging function execution.
    
    Args:
        logger: Logger instance
        func_name: Function name
    
    Returns:
        Callable: Decorated function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger.debug(f"Executing {func_name} with args={args}, kwargs={kwargs}")
            try:
                result = func(*args, **kwargs)
                logger.debug(f"Completed {func_name}")
                return result
            except Exception as e:
                logger.error(f"Error in {func_name}: {str(e)}")
                raise
        return wrapper
    return decorator


# Default logger instance
default_logger = get_logger()


__all__ = [
    "JSONFormatter",
    "ColoredFormatter",
    "ContextFilter",
    "LoggerConfig",
    "get_logger",
    "get_pipeline_logger",
    "log_execution",
    "default_logger",
]
