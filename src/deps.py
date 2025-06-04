# Simple dependency management for Chomp
# Handles lazy loading of optional dependencies

import importlib
from typing import Optional, Any

def lazy_import(module_name: str, package: Optional[str] = None, alias: Optional[str] = None) -> Optional[Any]:
  """
  Lazily import a module with helpful error message if missing.

  Args:
    module_name: The module to import (e.g., 'taos', 'web3')
    package: Package name for pip install (e.g., 'taospy', 'web3')
    alias: Optional extra name for chomp[extra] installation

  Returns:
    The imported module or None if import fails
  """
  try:
    return importlib.import_module(module_name)
  except ImportError:
    install_name = package or module_name
    extra_msg = f" or 'pip install chomp[{alias}]'" if alias else ""
    raise ImportError(
      f"Missing optional dependency '{module_name}'. "
      f"Install with 'pip install {install_name}'{extra_msg}"
    )

def safe_import(module_name: str) -> Optional[Any]:
  """
  Safely import a module, returning None if not available.
  Use this for optional features that should degrade gracefully.
  """
  try:
    return importlib.import_module(module_name)
  except ImportError:
    return None
