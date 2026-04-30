"""
Retry handler with exponential backoff for DataForge.

Provides robust retry logic with configurable backoff strategies,
jitter, and error handling for transient failures.
"""

import time
import random
from typing import Callable, Optional, TypeVar, Any, Type, Tuple
from functools import wraps
from enum import Enum
import threading
import logging

try:
    from tenacity import (
        retry,
        stop_after_attempt,
        wait_exponential,
        retry_if_exception_type,
        before_sleep_log,
        after_log,
    )
    TENANCY_AVAILABLE = True
except ImportError:
    TENANCY_AVAILABLE = False


logger = logging.getLogger(__name__)

T = TypeVar("T")


class BackoffStrategy(str, Enum):
    """Backoff strategy types."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"
    FIBONACCI = "fibonacci"


class RetryError(Exception):
    """Retry error with last exception information."""
    
    def __init__(
        self,
        message: str,
        last_exception: Optional[Exception] = None,
        attempts: int = 0,
    ):
        super().__init__(message)
        self.last_exception = last_exception
        self.attempts = attempts


class RetryConfig:
    """Retry configuration."""
    
    def __init__(
        self,
        max_attempts: int = 3,
        min_wait: float = 1.0,
        max_wait: float = 60.0,
        multiplier: float = 2.0,
        strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
        jitter: bool = True,
        jitter_factor: float = 0.1,
        exponential_base: float = 2.0,
    ):
        self.max_attempts = max_attempts
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.multiplier = multiplier
        self.strategy = strategy
        self.jitter = jitter
        self.jitter_factor = jitter_factor
        self.exponential_base = exponential_base
    
    def calculate_wait(self, attempt: int) -> float:
        """
        Calculate wait time for given attempt.
        
        Args:
            attempt: Attempt number (0-indexed)
        
        Returns:
            float: Wait time in seconds
        """
        if self.strategy == BackoffStrategy.EXPONENTIAL:
            wait = self.min_wait * (self.exponential_base ** attempt)
        elif self.strategy == BackoffStrategy.LINEAR:
            wait = self.min_wait * (attempt + 1)
        elif self.strategy == BackoffStrategy.FIBONACCI:
            wait = self._fibonacci(attempt + 1) * self.min_wait
        else:  # FIXED
            wait = self.min_wait
        
        # Apply jitter
        if self.jitter:
            jitter_range = wait * self.jitter_factor
            wait += random.uniform(-jitter_range, jitter_range)
        
        # Clamp to min/max
        return max(self.min_wait, min(self.max_wait, wait))
    
    def _fibonacci(self, n: int) -> int:
        """Calculate nth Fibonacci number."""
        if n <= 1:
            return 1
        a, b = 1, 1
        for _ in range(n - 1):
            a, b = b, a + b
        return b


class RetryHandler:
    """
    Retry handler with configurable backoff.
    
    Provides flexible retry logic for handling transient failures
    in network calls, database connections, and external API calls.
    """
    
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
        self._lock = threading.Lock()
        self._attempt_counts: dict = {}
    
    def calculate_wait(self, attempt: int) -> float:
        """Calculate wait time for attempt."""
        return self.config.calculate_wait(attempt)
    
    def execute(
        self,
        func: Callable[[], T],
        *args,
        **kwargs
    ) -> T:
        """
        Execute function with retry logic.
        
        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
        
        Returns:
            T: Function result
        
        Raises:
            RetryError: If all retry attempts fail
        """
        last_exception: Optional[Exception] = None
        
        for attempt in range(self.config.max_attempts):
            with self._lock:
                self._attempt_counts[func.__name__] = attempt + 1
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                logger.warning(
                    f"Attempt {attempt + 1}/{self.config.max_attempts} failed "
                    f"for {func.__name__}: {str(e)}"
                )
                
                if attempt < self.config.max_attempts - 1:
                    wait_time = self.calculate_wait(attempt)
                    logger.debug(f"Waiting {wait_time:.2f}s before retry")
                    time.sleep(wait_time)
                else:
                    raise RetryError(
                        f"Failed after {self.config.max_attempts} attempts",
                        last_exception=last_exception,
                        attempts=self.config.max_attempts,
                    ) from last_exception
    
    def wrap(self, func: Callable[[], T]) -> Callable[[], T]:
        """
        Wrap function with retry logic.
        
        Args:
            func: Function to wrap
        
        Returns:
            Callable: Wrapped function
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.execute(func, *args, **kwargs)
        return wrapper
    
    def get_attempt_count(self, func_name: str) -> int:
        """Get attempt count for function."""
        with self._lock:
            return self._attempt_counts.get(func_name, 0)
    
    def reset_attempts(self, func_name: Optional[str] = None) -> None:
        """Reset attempt counts."""
        with self._lock:
            if func_name:
                self._attempt_counts.pop(func_name, None)
            else:
                self._attempt_counts.clear()


def retry_on_exception(
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    max_attempts: int = 3,
    min_wait: float = 1.0,
    max_wait: float = 60.0,
    multiplier: float = 2.0,
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
    jitter: bool = True,
) -> Callable:
    """
    Decorator for retrying on exception.
    
    Example:
        @retry_on_exception(
            exceptions=(ConnectionError, TimeoutError),
            max_attempts=3,
            strategy=BackoffStrategy.EXPONENTIAL
        )
        def fetch_data(url):
            ...
    """
    def decorator(func: Callable) -> Callable:
        config = RetryConfig(
            max_attempts=max_attempts,
            min_wait=min_wait,
            max_wait=max_wait,
            multiplier=multiplier,
            strategy=strategy,
            jitter=jitter,
        )
        handler = RetryHandler(config)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            def call_func():
                return func(*args, **kwargs)
            
            last_error = None
            for attempt in range(config.max_attempts):
                try:
                    return call_func()
                except exceptions as e:
                    last_error = e
                    if attempt < config.max_attempts - 1:
                        wait_time = config.calculate_wait(attempt)
                        logger.warning(
                            f"Attempt {attempt + 1}/{config.max_attempts} failed: {e}. "
                            f"Retrying in {wait_time:.2f}s"
                        )
                        time.sleep(wait_time)
            
            raise RetryError(
                f"Failed after {config.max_attempts} attempts",
                last_exception=last_error,
                attempts=config.max_attempts,
            ) from last_error
        
        return wrapper
    return decorator


if TENANCY_AVAILABLE:
    def tenacity_retry(
        exceptions: Tuple[Type[Exception], ...] = (Exception,),
        max_attempts: int = 3,
        min_wait: int = 1,
        max_wait: int = 60,
    ) -> Callable:
        """
        Tenacity-based retry decorator.
        
        Example:
            @tenacity_retry(
                exceptions=(ConnectionError, TimeoutError),
                max_attempts=3
            )
            def fetch_data(url):
                ...
        """
        return retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(min=min_wait, max=max_wait),
            retry=retry_if_exception_type(exceptions),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            after=after_log(logger, logging.DEBUG),
        )


# Default retry handler
default_retry_handler = RetryHandler()


__all__ = [
    "BackoffStrategy",
    "RetryError",
    "RetryConfig",
    "RetryHandler",
    "retry_on_exception",
    "default_retry_handler",
]