"""
Unified runtime state management for Chomp.
Handles PIDs, UID, configuration, instance names, and other runtime variables in a single, elegant interface.
"""

import json
import secrets
from pathlib import Path
from typing import Any, Awaitable, Dict, Iterable, List, Optional, Union
from .types import T

# Runtime file path relative to project root
RUNTIME_FILE = Path(__file__).parent.parent.parent / ".runtime"


class RuntimeState:
  """Singleton runtime state manager."""

  _instance = None
  _data = None

  def __new__(cls):
    if cls._instance is None:
      cls._instance = super().__new__(cls)
    return cls._instance

  def __init__(self):
    if self._data is None:
      self._load()

  def _load(self) -> None:
    """Load runtime data from file."""
    if RUNTIME_FILE.exists():
      try:
        with open(RUNTIME_FILE, 'r') as f:
          self._data = json.load(f)
      except (json.JSONDecodeError, IOError):
        self._data = {}
    else:
      self._data = {}

  def _save(self) -> None:
    """Save runtime data to file."""
    try:
      RUNTIME_FILE.parent.mkdir(parents=True, exist_ok=True)
      with open(RUNTIME_FILE, 'w') as f:
        json.dump(self._data, f, indent=2)
    except IOError:
      pass  # Fail silently for non-critical operations

  # Configuration management
  def set_config(self,
                 mode: str = "dev",
                 deployment: str = "local",
                 api: str = "api") -> None:
    """Set runtime configuration."""
    self._data.update({"MODE": mode, "DEPLOYMENT": deployment, "API": api})
    self._save()

  def get_config(self) -> Dict[str, str]:
    """Get runtime configuration with defaults."""
    return {
        "MODE": self._data.get("MODE", "dev"),
        "DEPLOYMENT": self._data.get("DEPLOYMENT", "local"),
        "API": self._data.get("API", "api")
    }

  # UID management
  def get_uid(self) -> str:
    """Get instance UID, generating if needed."""
    if "uid" not in self._data:
      self._data["uid"] = secrets.token_hex(16)
      self._save()
    return self._data["uid"]

  def set_uid(self, uid: str) -> None:
    """Set instance UID."""
    self._data["uid"] = uid
    self._save()

  # Instance name management
  def get_instance_name(self) -> Optional[str]:
    """Get stored instance name."""
    return self._data.get("instance_name")

  def set_instance_name(self, name: str) -> None:
    """Set instance name."""
    self._data["instance_name"] = name
    self._save()

  def get_or_generate_instance_name_sync(self) -> str:
    """Synchronous version that falls back to simple generation if needed."""
    name = self.get_instance_name()
    if not name:
      # Simple fallback for synchronous contexts
      uid = self.get_uid()
      name = f"instance-{uid[:8]}"
      self.set_instance_name(name)
    return name

  # PID management
  def add_pid(self, pid: int, is_api: bool = False) -> None:
    """Add a process ID."""
    if is_api:
      self._data["api_pid"] = pid
    else:
      pids = self._data.get("pids", [])
      if pid not in pids:
        pids.append(pid)
        self._data["pids"] = pids
    self._save()

  def get_pids(self) -> Dict[str, Union[int, List[int]]]:
    """Get all PIDs."""
    return {
        "api_pid": self._data.get("api_pid"),
        "pids": self._data.get("pids", [])
    }

  def clear_pids(self) -> None:
    """Clear all PIDs."""
    self._data.pop("api_pid", None)
    self._data.pop("pids", None)
    self._save()

  # Generic key-value operations
  def get(self, key: str, default: Any = None) -> Any:
    """Get any value by key."""
    return self._data.get(key, default)

  def set(self, key: str, value: Any) -> None:
    """Set any key-value pair."""
    self._data[key] = value
    self._save()

  def clear(self) -> None:
    """Clear all runtime data."""
    self._data = {}
    if RUNTIME_FILE.exists():
      RUNTIME_FILE.unlink()

  def get_instance_info(self) -> Dict[str, str]:
    """Get complete instance information."""
    return {
        "uid": self.get_uid(),
        "name": self.get_or_generate_instance_name_sync(),
        "mode": self.get_config()["MODE"],
        "deployment": self.get_config()["DEPLOYMENT"]
    }


# Global instance
runtime = RuntimeState()


# Convenience functions for backward compatibility
def get_instance_uid() -> str:
  """Get instance UID."""
  return runtime.get_uid()


def generate_instance_uid() -> str:
  """Generate new instance UID."""
  uid = secrets.token_hex(16)
  runtime.set_uid(uid)
  return uid


def raise_if_exception(obj: Any) -> None:
  if isinstance(obj, Exception):
    raise obj


def raise_if_exception_in(iterable: Iterable[Any]) -> None:
  for obj in iterable:
    raise_if_exception(obj)


async def gather(coroutines: Iterable[Awaitable[T]]) -> List[T]:
  results = await gather(*coroutines, return_exceptions=True)
  raise_if_exception_in(results)
  return results  # type: ignore [return-value]
