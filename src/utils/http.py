from typing import Optional, Any, Dict
import httpx
from ..utils.decorators import cache
from .format import log_error, log_warn


# Default HTTP client configuration
DEFAULT_TIMEOUT = 5.0
DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_MAX_CONNECTIONS = 128
DEFAULT_MAX_KEEPALIVE = 128


@cache(ttl=3600, maxsize=32)
def get_timeout_config(timeout: Optional[float] = None, connect_timeout: Optional[float] = None) -> httpx.Timeout:
  """Get standardized timeout configuration."""
  from .. import state
  return httpx.Timeout(
    timeout=timeout or state.args.ingestion_timeout or DEFAULT_TIMEOUT,
    connect=connect_timeout or state.args.ingestion_timeout or DEFAULT_CONNECT_TIMEOUT
  )


@cache(ttl=3600, maxsize=32)
def get_limits_config(max_connections: Optional[int] = None, max_keepalive: Optional[int] = None) -> httpx.Limits:
  """Get standardized connection limits configuration."""
  return httpx.Limits(
    max_connections=max_connections or DEFAULT_MAX_CONNECTIONS,
    max_keepalive_connections=max_keepalive or DEFAULT_MAX_KEEPALIVE
  )


def get_auth(user: Optional[str] = None, password: Optional[str] = None) -> Optional[httpx.BasicAuth]:
  """Get standardized authentication configuration."""
  return httpx.BasicAuth(user, password) if user and password else None


class HttpClientSingleton:
  """Singleton HTTP client with consistent configuration and session management."""

  _instance: Optional['HttpClientSingleton'] = None
  _client: Optional[httpx.AsyncClient] = None

  def __new__(cls) -> 'HttpClientSingleton':
    if cls._instance is None:
      cls._instance = super().__new__(cls)
    return cls._instance

  async def get_client(self, **config) -> httpx.AsyncClient:
    """Get or create the singleton HTTP client with consistent configuration."""
    if self._client is None or self._client.is_closed:
      self._client = httpx.AsyncClient(
        timeout=get_timeout_config(config.get('timeout'), config.get('connect_timeout')),
        limits=get_limits_config(config.get('max_connections'), config.get('max_keepalive')),
        verify=config.get('verify', True),
        follow_redirects=config.get('follow_redirects', True)
      )
    return self._client

  async def close(self) -> None:
    """Close the HTTP client and clean up resources."""
    if self._client and not self._client.is_closed:
      await self._client.aclose()
      self._client = None

  async def request(self, method: str, url: str, **kwargs) -> httpx.Response:
    """Perform generic HTTP request with consistent error handling."""
    client = await self.get_client()
    try:
      return await client.request(method, url, **kwargs)
    except httpx.RequestError as e:
      log_error(f"HTTP {method.upper()} request failed for {url}: {e}")
      raise
    except Exception as e:
      log_error(f"Unexpected error during {method.upper()} request to {url}: {e}")
      raise


# Global singleton instance
_http_client = HttpClientSingleton()


# Convenience function that returns the singleton client for context manager usage
def http_client():
  """Get the singleton HTTP client for use as an async context manager."""
  class ClientContextManager:
    async def __aenter__(self):
      return await _http_client.get_client()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
      # Don't close the singleton client here since it's reused
      pass

  return ClientContextManager()


# Convenience functions that use the singleton
async def request(method: str, url: str, user: Optional[str] = None, password: Optional[str] = None, **kwargs) -> httpx.Response:
  """Perform generic HTTP request using the singleton HTTP client."""
  return await _http_client.request(method, url, auth=get_auth(user, password), **kwargs)


async def get(url: str, user: Optional[str] = None, password: Optional[str] = None, **kwargs) -> httpx.Response:
  """Perform GET request using the singleton HTTP client."""
  return await request("GET", url, user, password, **kwargs)


async def post(url: str, user: Optional[str] = None, password: Optional[str] = None, **kwargs) -> httpx.Response:
  """Perform POST request using the singleton HTTP client."""
  return await request("POST", url, user, password, **kwargs)


async def put(url: str, user: Optional[str] = None, password: Optional[str] = None, **kwargs) -> httpx.Response:
  """Perform PUT request using the singleton HTTP client."""
  return await request("PUT", url, user, password, **kwargs)


async def close_http_client() -> None:
  """Close the singleton HTTP client."""
  await _http_client.close()


async def cached_http_get(url: str, cache_key: str, ttl: int = 604800, **kwargs) -> Optional[Dict[str, Any]]:
  """HTTP GET request with Redis caching support using the singleton client."""
  from ..cache import get_cache, cache

  # Try to get from cache first
  cached_response = await get_cache(cache_key, pickled=True)
  if cached_response is not None:
    return cached_response

  # Cache miss - make HTTP request
  try:
    response = await get(url, **kwargs)
    if response.status_code == 200:
      data = response.json()
      await cache(cache_key, data, ttl, pickled=True)
      return data
    else:
      log_warn(f"HTTP request to {url} returned {response.status_code}")
      return None
  except Exception as e:
    log_warn(f"HTTP request error for {url}: {e}")
    return None
