from pathlib import Path
import re
import time
from asyncio import iscoroutinefunction, new_event_loop
from importlib import metadata, resources
import tomli
from typing import Coroutine, Optional, Any
from functools import wraps

from .format import log_error, log_warn

class PackageMeta:
  def __init__(self, package="chomp"):
    try:
      dist = metadata.distribution(package)
      self._set_metadata(dist.metadata)
      self.root = resources.files(package)
    except Exception:
      print(f"Package {package} not resolved, searching for pyproject.toml...")
      self._parse_pyproject_toml()

  def _set_metadata(self, meta):
    self.name = meta.get("Name", "Unknown")
    self.version = meta.get("Version", "0.0.0")
    self.major_version, self.minor_version, self.patch_version = self.version.split(".")
    self.description = meta.get("Summary", "No description available")
    self.authors = meta.get_all("Author", ["Unknown"])

  def _parse_pyproject_toml(self):
    pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"
    # Set root to the project directory (parent of pyproject.toml)
    self.root = pyproject_path.parent

    if pyproject_path.exists():
      with pyproject_path.open("rb") as f:
        project_data = tomli.load(f).get("project", {})
        self.name = project_data.get("name", "Unknown")
        self.version = project_data.get("version", "0.0.0")
        self.major_version, self.minor_version, self.patch_version = self.version.split(".")
        self.description = project_data.get("description", "No description available")
        self.authors = [author.get("name", "Unknown") for author in project_data.get("authors", [])]
    else:
      print("pyproject.toml not found, using fallback values...")
      self._set_metadata({})
      # Still set the root to current directory as fallback
      self.root = Path(__file__).parent.parent.parent

def run_async_in_thread(fn: Coroutine):
  loop = new_event_loop()
  try:
    return loop.run_until_complete(fn)
  except Exception as e:
    log_error(f"Failed to run async function in thread: {e}")
    loop.close()

def submit_to_threadpool(executor, fn, *args, **kwargs):
  if iscoroutinefunction(fn):
      return executor.submit(run_async_in_thread, fn(*args, **kwargs))
  return executor.submit(fn, *args, **kwargs)

def select_nested(selector: Optional[str], data: dict) -> Any:

  # invalid selectors
  if selector and not isinstance(selector, str):
    log_error("Invalid selector. Please use a valid path string")
    return None

  if not selector or [".", "root"].count(selector.lower()) > 0:
    return data

  # optional starting dot and "root" keyword (case-insensitive)
  if selector.startswith("."):
    selector = selector[1:]

  current: Any = data # base access
  segment_pattern = re.compile(r'([^.\[\]]+)(?:\[(\d+)\])?') # match selector segments eg. ".key" or ".key[index]"

  # loop through segments
  for match in segment_pattern.finditer(selector):
    key, index = match.groups()
    if key and key.isnumeric() and not index:
      key, index = None, key  # Keep as string for now
    # dict access
    if key and isinstance(current, dict):
      current = current.get(key)
    if not current:
      return log_warn(f"Key not found in dict: {key}")
    # list access
    if index is not None:
      if current is None:
        return log_error("Cannot access index on None value")
      index_int = int(index)
      if not isinstance(current, list) or index_int >= len(current):
        return log_error(f"Index out of range in dict.{key}: {index}")
      current = current[index_int]
  return current

def merge_replace_empty(dest: dict, src: dict) -> dict:
  """Merge src into dest, replacing empty arrays/objects in dest with values from src."""
  for key, value in src.items():
    if isinstance(value, dict) and isinstance(dest.get(key), dict):
      # Recursively merge dictionaries
      dest[key] = merge_replace_empty(dest.get(key, {}), value)
    elif isinstance(value, list) and isinstance(dest.get(key), list):
      # Replace empty lists in dest with src's value
      dest[key] = value if not dest[key] else dest[key]
    else:
      # Replace empty values in dest with src's value
      if dest.get(key) in [None, [], {}, ""]:
        dest[key] = value
  return dest

def _cache_decorator(ttl=None, maxsize=128, is_async=False):
  def decorator(func):
    cache_data: dict[Any, tuple[Any, float]] = {}
    access_order: list[Any] = []

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
      key = (args, frozenset(kwargs.items()))
      now = time.time()

      if key in cache_data:
        value, timestamp = cache_data[key]
        if ttl is None or now - timestamp < ttl:
          # LRU update
          access_order.remove(key)
          access_order.append(key)
          return value

      # Cache miss
      result = await func(*args, **kwargs)
      cache_data[key] = (result, now)

      # Update LRU
      if key in access_order:
        access_order.remove(key)
      access_order.append(key)

      # Evict oldest if needed
      while len(cache_data) > maxsize:
        oldest_key = access_order.pop(0)
        cache_data.pop(oldest_key, None)

      return result

    @wraps(func)
    def sync_wrapper(*args, **kwargs):
      key = (args, frozenset(kwargs.items()))
      now = time.time()

      if key in cache_data:
        value, timestamp = cache_data[key]
        if ttl is None or now - timestamp < ttl:
          # LRU update
          access_order.remove(key)
          access_order.append(key)
          return value

      # Cache miss
      result = func(*args, **kwargs)
      cache_data[key] = (result, now)

      # Update LRU
      if key in access_order:
        access_order.remove(key)
      access_order.append(key)

      # Evict oldest if needed
      while len(cache_data) > maxsize:
        oldest_key = access_order.pop(0)
        cache_data.pop(oldest_key, None)

      return result

    return async_wrapper if is_async else sync_wrapper

  return decorator

def cache(ttl=None, maxsize=128):
  """Sync cache decorator with TTL and LRU support"""
  return _cache_decorator(ttl=ttl, maxsize=maxsize, is_async=False)

def async_cache(ttl=None, maxsize=128):
  """Async cache decorator with TTL and LRU support"""
  return _cache_decorator(ttl=ttl, maxsize=maxsize, is_async=True)
