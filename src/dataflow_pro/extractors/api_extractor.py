"""
API data extractor for DataFlow Pro.

Extracts data from external REST APIs with pagination support,
rate limiting, authentication, and error handling.
"""

from typing import Any, Dict, Iterator, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging
import time
import json
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from dataflow_pro.utils.retry_handler import RetryConfig, BackoffStrategy, RetryHandler
from dataflow_pro.utils.metrics import get_metrics


logger = logging.getLogger(__name__)


class HTTPMethod(str, Enum):
    """HTTP methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


class PaginationType(str, Enum):
    """Pagination types."""
    OFFSET = "offset"
    PAGE = "page"
    CURSOR = "cursor"
    LINK = "link"


@dataclass
class RequestConfig:
    """Request configuration."""
    timeout: int = 30
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    auth: Optional[Callable[[], Dict[str, str]]] = None


@dataclass
class PaginationConfig:
    """Pagination configuration."""
    type: PaginationType = PaginationType.OFFSET
    page_param: str = "page"
    offset_param: str = "offset"
    limit_param: str = "limit"
    limit: int = 100
    max_pages: Optional[int] = None
    cursor_param: str = "cursor"
    link_param: str = "next"


@dataclass
class ExtractionResult:
    """Result of data extraction."""
    data: list
    total_records: Optional[int] = None
    page: Optional[int] = None
    next_page: Optional[int] = None
    next_cursor: Optional[str] = None
    has_more: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


class APIExtractor:
    """
    API data extractor with advanced features.
    
    Supports:
    - Various pagination strategies
    - Rate limiting
    - Authentication (Bearer, API Key, Basic)
    - Retry logic with backoff
    - Response parsing
    - Error handling
    """
    
    def __init__(
        self,
        base_url: str,
        request_config: Optional[RequestConfig] = None,
        pagination_config: Optional[PaginationConfig] = None,
        retry_config: Optional[RetryConfig] = None,
        rate_limit_calls: int = 100,
        rate_limit_period: int = 60,
    ):
        self.base_url = base_url
        self.request_config = request_config or RequestConfig()
        self.pagination_config = pagination_config or PaginationConfig()
        self.retry_config = retry_config or RetryConfig()
        
        # Rate limiting
        self.rate_limit_calls = rate_limit_calls
        self.rate_limit_period = rate_limit_period
        self._call_times: list = []
        
        # HTTP session
        self.session = requests.Session()
        self._setup_session()
        
        # Metrics
        self.metrics = get_metrics()
    
    def _setup_session(self) -> None:
        """Setup HTTP session with retry adapter."""
        retry_strategy = Retry(
            total=self.retry_config.max_attempts,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def _apply_rate_limit(self) -> None:
        """Apply rate limiting."""
        now = time.time()
        cutoff = now - self.rate_limit_period
        
        # Remove old calls
        self._call_times = [t for t in self._call_times if t > cutoff]
        
        # Check limit
        if len(self._call_times) >= self.rate_limit_calls:
            wait_time = self._call_times[0] + self.rate_limit_period - now
            if wait_time > 0:
                logger.debug(f"Rate limit reached, waiting {wait_time:.2f}s")
                time.sleep(wait_time)
                self._apply_rate_limit()
        
        self._call_times.append(time.time())
    
    def _build_url(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> str:
        """Build URL with parameters."""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        if params:
            query_params = {**self.request_config.params, **params}
            if query_params:
                parsed = urlparse(url)
                existing_params = parse_qs(parsed.query)
                existing_params.update({k: [str(v)] for k, v in query_params.items()})
                new_query = urlencode(existing_params, doseq=True)
                url = urlunparse((
                    parsed.scheme,
                    parsed.netloc,
                    parsed.path,
                    parsed.params,
                    new_query,
                    parsed.fragment
                ))
        
        return url
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            **self.request_config.headers,
        }
        
        # Apply auth if configured
        if self.request_config.auth:
            headers.update(self.request_config.auth())
        
        return headers
    
    def _parse_pagination_response(
        self,
        response: requests.Response,
        current_page: int = 1
    ) -> ExtractionResult:
        """Parse pagination from response."""
        data = response.json()
        
        # Extract data and pagination info
        data_list = data.get("data", data.get("results", [data]))
        
        total = data.get("total", data.get("total_count"))
        next_offset = data.get("next_offset")
        next_page = data.get("next_page")
        next_cursor = data.get("next_cursor")
        
        # Check link header for cursor-based pagination
        link_header = response.headers.get("Link") or response.headers.get("link")
        has_more = False
        if link_header:
            has_more = "rel=\"next\"" in link_header or "rel='next'" in link_header
        
        return ExtractionResult(
            data=data_list,
            total_records=total,
            page=current_page,
            next_page=next_page,
            next_cursor=next_cursor,
            has_more=has_more or (next_offset is not None) or (next_page is not None),
        )
    
    def make_request(
        self,
        method: HTTPMethod = HTTPMethod.GET,
        endpoint: str = "",
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """
        Make HTTP request with retry and rate limiting.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters
            data: Request body data
        
        Returns:
            requests.Response: Response object
        """
        self._apply_rate_limit()
        
        url = self._build_url(endpoint, params)
        headers = self._get_headers()
        
        logger.debug(f"Making {method.value} request to {url}")
        
        retry_handler = RetryHandler(self.retry_config)
        
        def make_request_func():
            response = self.session.request(
                method=method.value,
                url=url,
                headers=headers,
                params=params,
                json=data if method != HTTPMethod.GET else None,
                timeout=self.request_config.timeout,
            )
            response.raise_for_status()
            return response
        
        response = retry_handler.execute(make_request_func)
        
        logger.debug(f"Response status: {response.status_code}")
        
        return response
    
    def extract(
        self,
        endpoint: str = "",
        params: Optional[Dict[str, Any]] = None,
    ) -> ExtractionResult:
        """
        Extract data from single API call.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
        
        Returns:
            ExtractionResult: Extraction result
        """
        response = self.make_request(
            method=HTTPMethod.GET,
            endpoint=endpoint,
            params=params,
        )
        
        return self._parse_pagination_response(response)
    
    def extract_all(
        self,
        endpoint: str = "",
        params: Optional[Dict[str, Any]] = None,
        transform: Optional[Callable[[list], list]] = None,
    ) -> Iterator[list]:
        """
        Extract all data with pagination.
        
        Args:
            endpoint: API endpoint
            params: Base query parameters
            transform: Optional transform function
        
        Yields:
            list: Batches of extracted data
        """
        current_params = params or {}
        page = 1
        all_data = []
        
        while True:
            # Apply pagination params
            request_params = {**current_params}
            
            if self.pagination_config.type == PaginationType.OFFSET:
                offset = (page - 1) * self.pagination_config.limit
                request_params[self.pagination_config.offset_param] = offset
                request_params[self.pagination_config.limit_param] = self.pagination_config.limit
            
            elif self.pagination_config.type == PaginationType.PAGE:
                request_params[self.pagination_config.page_param] = page
                request_params[self.pagination_config.limit_param] = self.pagination_config.limit
            
            elif self.pagination_config.type == PaginationType.CURSOR:
                if page > 1 and result.next_cursor:
                    request_params[self.pagination_config.cursor_param] = result.next_cursor
            
            # Make request
            result = self.extract(endpoint, request_params)
            
            # Apply transform
            data = result.data
            if transform:
                data = transform(data)
            
            all_data.extend(data)
            
            # Check if more pages
            if not result.has_more:
                break
            
            # Check max pages
            if self.pagination_config.max_pages and page >= self.pagination_config.max_pages:
                break
            
            page += 1
            
            self.metrics.record_processed(
                endpoint or "default",
                "extracted",
                len(result.data)
            )
        
        # Yield in batches
        limit = self.pagination_config.limit
        for i in range(0, len(all_data), limit):
            yield all_data[i:i + limit]
    
    def extract_to_file(
        self,
        endpoint: str = "",
        params: Optional[Dict[str, Any]] = None,
        output_path: str = "output.json",
        transform: Optional[Callable[[list], list]] = None,
    ) -> int:
        """
        Extract all data to file.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            output_path: Output file path
            transform: Optional transform function
        
        Returns:
            int: Total records extracted
        """
        all_records = []
        
        for batch in self.extract_all(endpoint, params, transform):
            all_records.extend(batch)
        
        with open(output_path, "w") as f:
            json.dump(all_records, f, indent=2, default=str)
        
        logger.info(f"Extracted {len(all_records)} records to {output_path}")
        
        return len(all_records)
    
    def close(self) -> None:
        """Close the HTTP session."""
        self.session.close()


class CustomAPIExtractor(APIExtractor):
    """
    Custom API extractor for specific API patterns.
    
    Override methods for custom API implementations.
    """
    
    def _parse_pagination_response(
        self,
        response: requests.Response,
        current_page: int = 1
    ) -> ExtractionResult:
        """Override for custom pagination parsing."""
        return super()._parse_pagination_response(response, current_page)
    
    def _get_headers(self) -> Dict[str, str]:
        """Override for custom headers."""
        return super()._get_headers()


def create_api_extractor(
    base_url: str,
    api_key: Optional[str] = None,
    bearer_token: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    pagination_type: PaginationType = PaginationType.OFFSET,
    page_size: int = 100,
) -> APIExtractor:
    """
    Factory function to create API extractor.
    
    Args:
        base_url: API base URL
        api_key: API key for authentication
        bearer_token: Bearer token for authentication
        username: Username for basic auth
        password: Password for basic auth
        pagination_type: Type of pagination
        page_size: Page size for pagination
    
    Returns:
        APIExtractor: Configured API extractor
    """
    # Setup authentication
    auth = None
    headers = {}
    
    if api_key:
        headers["X-API-Key"] = api_key
    
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    
    if username and password:
        import base64
        credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {credentials}"
    
    request_config = RequestConfig(
        headers=headers,
    )
    
    pagination_config = PaginationConfig(
        type=pagination_type,
        limit=page_size,
    )
    
    return APIExtractor(
        base_url=base_url,
        request_config=request_config,
        pagination_config=pagination_config,
    )


__all__ = [
    "HTTPMethod",
    "PaginationType",
    "RequestConfig",
    "PaginationConfig",
    "ExtractionResult",
    "APIExtractor",
    "CustomAPIExtractor",
    "create_api_extractor",
]