"""
Import utilities for safe dynamic importing.
"""

from typing import Optional, Any
import importlib


def safe_import(module_path: str,
                class_name: Optional[str] = None) -> Optional[Any]:
  """
    Safely import a module or class, returning None if import fails.

    Args:
      module_path: The module path to import (e.g., 'src.adapters.sqlite')
      class_name: Optional class name to get from the module

    Returns:
      The imported module/class or None if import failed
    """
  try:
    module = importlib.import_module(module_path)
    if class_name:
      return getattr(module, class_name, None)
    return module
  except (ImportError, ModuleNotFoundError, AttributeError):
    return None
