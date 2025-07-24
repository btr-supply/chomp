from pathlib import Path
import re
from asyncio import iscoroutinefunction, new_event_loop
from importlib import metadata, resources
import tomli
from typing import Coroutine, Any, Optional, Union
from datetime import datetime

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
    self.major_version, self.minor_version, self.patch_version = self.version.split(
        ".")
    self.description = meta.get("Summary", "")
    self.authors = meta.get_all("Author", [])

  def _parse_pyproject_toml(self):
    pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"
    # Set root to the project directory (parent of pyproject.toml)
    self.root = pyproject_path.parent

    if pyproject_path.exists():
      with pyproject_path.open("rb") as f:
        project_data = tomli.load(f).get("project", {})
        self.name = project_data.get("name", "chomp")
        self.version = project_data.get("version", "0.0.0")
        self.major_version, self.minor_version, self.patch_version = self.version.split(
            ".")
        self.description = project_data.get("description",
                                            "")
        self.authors = [
            author.get("name", "")
            for author in project_data.get("authors", [])
        ]
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


def select_nested(selector: Optional[str],
                  data: dict,
                  name: Optional[str] = None) -> Any:

  # invalid selectors
  if selector and not isinstance(selector, str):
    log_error("Invalid selector. Please use a valid path string")
    return None

  if not selector or [".", "root"].count(selector.lower()) > 0:
    return data

  # optional starting dot and "root" keyword (case-insensitive)
  if selector.startswith("."):
    selector = selector[1:]

  current: Any = data  # base access
  segment_pattern = re.compile(
      r'([^.\[\]]+)(?:\[(\d+)\])?'
  )  # match selector segments eg. ".key" or ".key[index]"

  # loop through segments
  for match in segment_pattern.finditer(selector):
    key, index = match.groups()
    if key and key.isnumeric() and not index:
      key, index = None, key  # Keep as string for now
    # dict access
    if key and isinstance(current, dict):
      current = current.get(key)
    if current is None:
      log_warn(f"Key not found in {name} dict: {key}")
      return None
    # list access
    if index is not None:
      if current is None:
        return log_error("Cannot access index on None value")
      index_int = int(index)
      if not isinstance(current, list) or index_int >= len(current):
        return log_error(f"Index out of range in {name} dict.{key}: {index}")
      current = current[index_int]
  return current


def merge_replace_empty(dest: dict, src: dict) -> dict:
  """Merge src into dest, replacing empty arrays/objects in dest with values from src."""
  for key, value in src.items():
    dest_value = dest.get(key)

    # Check type compatibility and merge accordingly
    if isinstance(value, dict) and isinstance(dest_value, dict):
      # Recursively merge dictionaries
      dest[key] = merge_replace_empty(dest_value, value)
    elif isinstance(value, list) and isinstance(dest_value, list):
      # Replace empty lists in dest with src's value
      dest[key] = value if not dest_value else dest_value
    else:
      # Replace empty values in dest with src's value
      if dest_value in [None, [], {}, ""]:
        dest[key] = value
  return dest


class DictMixin:
  """Lightweight mixin for dict conversion using __iter__"""

  def __iter__(self):
    """Iterate over serializable attributes (instance only), excluding private ones and properties"""
    # Only include instance attributes, excluding private ones
    # This avoids properties, methods, and other class descriptors that can't be pickled
    for key, value in self.__dict__.items():
      if not key.startswith('_'):
        if isinstance(value, datetime):
          yield key, value.isoformat()
        else:
          yield key, value

  def to_dict(self) -> dict:
    """Convert to dictionary using __iter__"""
    return dict(self)

  @classmethod
  def from_dict(cls, data: dict):
    """Create instance from dictionary, handling datetime strings"""
    from datetime import datetime
    from ..utils import parse_date, now

    kwargs = {}
    for key, value in data.items():
      # Handle datetime fields for dataclasses
      if hasattr(cls, '__dataclass_fields__') and key in cls.__dataclass_fields__:
        field_type = cls.__dataclass_fields__[key].type
        if field_type == datetime or (getattr(field_type, '__origin__', None) is Union and
                                    datetime in getattr(field_type, '___args_', ())):
          if isinstance(value, str):
            value = parse_date(value) or now()
      kwargs[key] = value
    return cls(**kwargs)
