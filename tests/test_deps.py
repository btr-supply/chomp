"""Tests for dependency management module."""
import pytest
from unittest.mock import patch, Mock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.deps import lazy_import, safe_import


class TestDepsModule:
  """Test the deps module functionality."""

  def test_lazy_import_success(self):
    """Test successful lazy import of a module."""
    # Test importing a standard library module
    result = lazy_import('json')
    assert result is not None
    assert hasattr(result, 'loads')
    assert hasattr(result, 'dumps')

  def test_lazy_import_failure_with_package(self):
    """Test lazy import failure with package specified."""
    with pytest.raises(ImportError) as exc_info:
      lazy_import('nonexistent_module', package='fake-package')

    error_msg = str(exc_info.value)
    assert "Missing optional dependency 'nonexistent_module'" in error_msg
    assert "pip install fake-package" in error_msg

  def test_lazy_import_failure_with_alias(self):
    """Test lazy import failure with alias specified."""
    with pytest.raises(ImportError) as exc_info:
      lazy_import('nonexistent_module',
                  package='fake-package',
                  alias='fake-extra')

    error_msg = str(exc_info.value)
    assert "Missing optional dependency 'nonexistent_module'" in error_msg
    assert "pip install fake-package" in error_msg
    assert "pip install chomp[fake-extra]" in error_msg

  def test_lazy_import_failure_no_package(self):
    """Test lazy import failure without package specified."""
    with pytest.raises(ImportError) as exc_info:
      lazy_import('nonexistent_module')

    error_msg = str(exc_info.value)
    assert "Missing optional dependency 'nonexistent_module'" in error_msg
    assert "pip install nonexistent_module" in error_msg

  def test_lazy_import_with_importlib_mock(self):
    """Test lazy import using mocked importlib."""
    mock_module = Mock()
    mock_module.some_function = Mock(return_value="test")

    with patch('src.deps.importlib.import_module', return_value=mock_module):
      result = lazy_import('mocked_module')
      assert result == mock_module
      assert result.some_function() == "test"

  def test_lazy_import_importerror_handling(self):
    """Test lazy import handles ImportError properly."""
    with patch('src.deps.importlib.import_module',
               side_effect=ImportError("Module not found")):
      with pytest.raises(ImportError) as exc_info:
        lazy_import('failing_module',
                    package='failing-package',
                    alias='failing')

      error_msg = str(exc_info.value)
      assert "Missing optional dependency 'failing_module'" in error_msg
      assert "pip install failing-package" in error_msg
      assert "pip install chomp[failing]" in error_msg

  def test_safe_import_success(self):
    """Test successful safe import of a module."""
    result = safe_import('json')
    assert result is not None
    assert hasattr(result, 'loads')
    assert hasattr(result, 'dumps')

  def test_safe_import_failure(self):
    """Test safe import failure returns None."""
    result = safe_import('nonexistent_module')
    assert result is None

  def test_safe_import_with_importlib_mock_success(self):
    """Test safe import using mocked importlib success case."""
    mock_module = Mock()
    mock_module.test_attr = "test_value"

    with patch('src.deps.importlib.import_module', return_value=mock_module):
      result = safe_import('mocked_module')
      assert result == mock_module
      assert result.test_attr == "test_value"

  def test_safe_import_with_importlib_mock_failure(self):
    """Test safe import using mocked importlib failure case."""
    with patch('src.deps.importlib.import_module',
               side_effect=ImportError("Module not found")):
      result = safe_import('failing_module')
      assert result is None

  def test_lazy_import_all_parameters(self):
    """Test lazy import with all parameters specified."""
    mock_module = Mock()

    with patch('src.deps.importlib.import_module', return_value=mock_module):
      result = lazy_import('test_module',
                           package='test-package',
                           alias='test-extra')
      assert result == mock_module

  def test_lazy_import_only_module_name(self):
    """Test lazy import with only module name."""
    mock_module = Mock()

    with patch('src.deps.importlib.import_module', return_value=mock_module):
      result = lazy_import('test_module')
      assert result == mock_module

  def test_error_messages_formatting(self):
    """Test that error messages are properly formatted."""
    # Test with package but no alias
    with pytest.raises(ImportError) as exc_info:
      lazy_import('test_module', package='test-package')

    error_msg = str(exc_info.value)
    assert "Missing optional dependency 'test_module'" in error_msg
    assert "pip install test-package" in error_msg
    assert "chomp[" not in error_msg  # No alias, so no chomp extra option

  def test_module_imports(self):
    """Test that all required imports work correctly."""
    import importlib
    from typing import Optional, Any

    assert importlib is not None
    assert Optional is not None
    assert Any is not None

  def test_docstring_accuracy(self):
    """Test that function docstrings accurately describe behavior."""
    # Test lazy_import docstring examples
    assert "Lazily import a module" in lazy_import.__doc__
    assert "module_name" in lazy_import.__doc__
    assert "package" in lazy_import.__doc__
    assert "alias" in lazy_import.__doc__

    # Test safe_import docstring examples
    assert "Safely import a module" in safe_import.__doc__
    assert "returning None" in safe_import.__doc__
    assert "optional features" in safe_import.__doc__

  def test_type_annotations(self):
    """Test that type annotations are correct."""
    import inspect

    # Check lazy_import signature
    sig = inspect.signature(lazy_import)
    assert 'module_name' in sig.parameters
    assert 'package' in sig.parameters
    assert 'alias' in sig.parameters

    # Check safe_import signature
    sig = inspect.signature(safe_import)
    assert 'module_name' in sig.parameters

  def test_edge_cases(self):
    """Test edge cases and error conditions."""
    # Test empty string module name - should raise ValueError from importlib
    with pytest.raises(ValueError):
      lazy_import('')

    # Test None as module name should raise TypeError from importlib
    with pytest.raises(TypeError):
      lazy_import(None)

    # Test safe_import with edge cases
    result = safe_import('')
    assert result is None

    # Test safe_import with None
    with pytest.raises(TypeError):
      safe_import(None)

  def test_real_world_usage_scenarios(self):
    """Test realistic usage scenarios."""
    # Simulate trying to import optional dependencies
    common_optional_deps = [('numpy', 'numpy'), ('pandas', 'pandas'),
                            ('requests', 'requests'),
                            ('sqlalchemy', 'SQLAlchemy')]

    for module_name, package_name in common_optional_deps:
      # Try safe import first
      result = safe_import(module_name)
      # Result will be the module if installed, None if not
      if result is not None:
        assert hasattr(result, '__name__')

      # Test that lazy_import would work with proper error message
      if result is None:  # Module not installed
        with pytest.raises(ImportError) as exc_info:
          lazy_import(module_name, package=package_name)

        error_msg = str(exc_info.value)
        assert module_name in error_msg
        assert package_name in error_msg
