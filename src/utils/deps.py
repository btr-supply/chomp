# Simple dependency management for Chomp
# Handles lazy loading of optional dependencies

import importlib
from typing import Any, Optional


class MockModule:
  """A mock module that satisfies mypy type checking by providing Any for all attributes."""

  def __bool__(self) -> bool:
    """Return False when used in boolean context to maintain compatibility."""
    return False

  def __getattr__(self, name: str) -> Any:
    # Return a callable that can be used as both a type and a runtime value
    class MockType:

      def __call__(self, *args, **kwargs):
        return None

      def __getattr__(self, attr):
        return MockType()

      def __getitem__(self, item):
        return MockType()

    return MockType()


class MissingDependencyError(ImportError):
  """Custom ImportError for missing optional dependencies with uv-specific installation instructions."""

  def __init__(self, package_name: str, extra_name: Optional[str] = None):
    self.package_name = package_name
    self.extra_name = extra_name

    # Create uv-specific installation message
    uv_install_msg = f"uv add {package_name}"
    if extra_name:
      chomp_extra_msg = f"uv add chomp[{extra_name}]"
      message = f"Missing optional dependency '{package_name}'. Install with: {uv_install_msg} or {chomp_extra_msg}"
    else:
      message = f"Missing optional dependency '{package_name}'. Install with: {uv_install_msg}"

    super().__init__(message)


def require_dependency(package_name: str,
                       extra_name: Optional[str] = None,
                       module=None) -> Any:
  """
  Check if a required dependency is available and raise MissingDependencyError if not.

  Args:
    package_name: The name of the package to install (e.g. 'asyncpg', 'playwright')
    extra_name: Optional extra name for chomp[extra] installation (e.g. 'timescale', 'web2')
    module: The imported module to check (if None, uses safe_import)

  Raises:
    MissingDependencyError: If the dependency is not available

  Returns:
    The module if available (guaranteed to be not None)
  """
  if module is None:
    module = safe_import(package_name)

  # Check if module is actually a MockModule (meaning import failed)
  if isinstance(module, MockModule):
    raise MissingDependencyError(package_name, extra_name)

  return module


def lazy_import(module_name: str,
                package: Optional[str] = None,
                alias: Optional[str] = None) -> Optional[Any]:
  """
  Lazily import a module with helpful error message if missing.

  Args:
    module_name: The module to import (e.g., 'taos', 'web3')
    package: Package name for uv add install (e.g., 'taospy', 'web3')
    alias: Optional extra name for chomp[extra] installation

  Returns:
    The imported module or None if import fails
  """
  # Handle edge cases for invalid inputs
  if module_name is None:
    raise TypeError("module_name cannot be None")
  if module_name == '':
    raise ValueError("module_name cannot be empty")

  try:
    return importlib.import_module(module_name)
  except ImportError:
    install_name = package or module_name
    extra_msg = f" or 'uv add chomp[{alias}]'" if alias else ""
    raise MissingDependencyError(
        f"Missing optional dependency '{module_name}'. "
        f"Install with 'uv add {install_name}'{extra_msg}")


def safe_import(module_name: str,
                package: Optional[str] = None,
                alias: Optional[str] = None,
                fallback: Any = None) -> Any:
  """
  Safely import a module, returning empty module cast as Any if missing instead of raising.

  Args:
    module_name: The module to import (e.g., 'taos', 'web3')
    package: Package name for uv add install (e.g., 'taospy', 'web3')
    alias: Optional extra name for chomp[extra] installation
    fallback: Value to return if import fails (default: empty module)

  Returns:
    The imported module or empty module cast as Any if import fails
  """
  # Handle edge cases for invalid inputs
  if module_name is None:
    raise TypeError("module_name cannot be None")
  if module_name == '':
    raise ValueError("module_name cannot be empty")

  try:
    return importlib.import_module(module_name)
  except ImportError:
    if fallback is not None:
      return fallback
    # Return mock module that satisfies mypy for type annotations
    return MockModule()


__all__ = [
    'lazy_import', 'safe_import', 'MissingDependencyError',
    'require_dependency'
]
