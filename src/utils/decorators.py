"""
Utility decorators for caching and service logging.
Leverages FastAPI's native exception handling instead of custom HTTP wrappers.
"""

from functools import wraps
from typing import Any, Callable, Optional, OrderedDict as TOrderedDict, Union
import asyncio
from collections import OrderedDict
from .date import now


# === CACHING DECORATORS ===

def _make_cache_key(args: tuple, kwargs: dict) -> Any:
  """Create a cache key from function arguments, handling both hashable and non-hashable objects."""
  try:
    # Attempt to create a key from hashable arguments directly.
    return tuple(args), tuple(sorted(kwargs.items()))
  except TypeError:
    # Fallback to a string-based key for non-hashable arguments.
    return str(args) + str(sorted(kwargs.items()))


def cache(ttl: int = 300, maxsize: Optional[int] = 512, key_func: Optional[Callable] = None):
  """
  Universal cache decorator for both sync and async functions.

  Features:
  - Time-to-live (TTL) expiration for cached items.
  - Maximum cache size (maxsize) with LRU (Least Recently Used) eviction.
  - Custom key generation function (key_func).
  - Handles object-based keys (e.g., Web3 instances).
  - Automatically detects and handles both synchronous and asynchronous functions.

  Args:
    ttl: Cache lifetime in seconds. -1 for infinite TTL.
    maxsize: Max number of items in cache. None for unlimited size. LRU eviction is used.
    key_func: Optional function to generate a custom cache key from function arguments.
  """
  def decorator(func: Callable) -> Callable:
    cache_storage: Union[dict, TOrderedDict[Any, tuple[Any, float]]] = OrderedDict() if maxsize is not None else {}
    is_async = asyncio.iscoroutinefunction(func)

    def _get_key(*args, **kwargs):
      if key_func:
        return key_func(*args, **kwargs)
      return (func.__name__, _make_cache_key(args, kwargs))

    def _evict():
      if maxsize is not None and len(cache_storage) > maxsize:
        # First, remove any expired items to make space.
        if ttl != -1:
          current_time = now().timestamp()
          expired_keys = [k for k, (_, ts) in cache_storage.items() if current_time - ts > ttl]
          for k in expired_keys:
            if k in cache_storage: # Check if not already removed/updated
              del cache_storage[k]
        # If still over maxsize, evict LRU items.
        while len(cache_storage) > maxsize:
          cache_storage.popitem(last=False)

    @wraps(func)
    def sync_wrapper(*args, **kwargs):
      key = _get_key(*args, **kwargs)
      if key in cache_storage:
        result, timestamp = cache_storage[key]
        if ttl == -1 or now().timestamp() - timestamp < ttl:
          if maxsize is not None:
            cache_storage.move_to_end(key)
          return result
        del cache_storage[key] # Expired

      result = func(*args, **kwargs)
      cache_storage[key] = (result, now().timestamp())
      _evict()
      return result

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
      key = _get_key(*args, **kwargs)
      if key in cache_storage:
        result, timestamp = cache_storage[key]
        if ttl == -1 or now().timestamp() - timestamp < ttl:
          if maxsize is not None:
            cache_storage.move_to_end(key)
          return result
        del cache_storage[key] # Expired

      result = await func(*args, **kwargs)
      cache_storage[key] = (result, now().timestamp())
      _evict()
      return result

    return async_wrapper if is_async else sync_wrapper
  return decorator


# === SERVICE LOGGING DECORATORS ===

def service_method(operation_name: Optional[str] = None):
  """
  Decorator for service methods that ensures proper logging before raising exceptions.

  Args:
    operation_name: Descriptive name for the operation (defaults to function name)
    log_level: Level to log at - "error", "warn", or "info"
  """
  def decorator(func: Callable) -> Callable:
    @wraps(func)
    async def wrapper(*args, **kwargs):
      op_name = operation_name or func.__name__

      try:
        result = await func(*args, **kwargs)
        return result

      except Exception as e:
        from .format import log_warn
        log_warn(f"Service operation '{op_name}' failed: {e}")
        # Re-raise the exception (FastAPI converts RuntimeError/ValueError/PermissionError to HTTP)
        raise

    return wrapper
  return decorator


def logged_operation(operation_name: str, log_success: bool = False):
  """
  Decorator that logs operation start, success, and failures.

  Args:
    operation_name: Descriptive name for the operation
    log_success: Whether to log successful operations
  """
  def decorator(func: Callable) -> Callable:
    @wraps(func)
    async def wrapper(*args, **kwargs):
      from .format import log_info
      log_info(f"Starting operation: {operation_name}")

      try:
        result = await func(*args, **kwargs)

        if log_success:
          log_info(f"Operation completed successfully: {operation_name}")

        return result

      except Exception as e:
        from .format import log_error
        log_error(f"Operation failed: {operation_name} - {e}")
        raise

    return wrapper
  return decorator
