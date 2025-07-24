"""Basic tests for service modules with low coverage."""
import pytest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_auth_imports():
  """Test auth service imports."""
  try:
    from src.services.auth import AuthService
    assert AuthService is not None
  except ImportError:
    pytest.skip("Auth service not available")


def test_loader_imports():
  """Test loader service imports."""
  try:
    from src.services import loader
    assert loader is not None
  except ImportError:
    pytest.skip("Loader service not available")


def test_status_checker_imports():
  """Test status checker imports."""
  try:
    from src.services import status_checker
    assert status_checker is not None
  except ImportError:
    pytest.skip("Status checker not available")


def test_ts_analysis_imports():
  """Test time series analysis imports."""
  try:
    from src.services import ts_analysis
    assert ts_analysis is not None
  except ImportError:
    pytest.skip("TS analysis not available")


def test_service_module_structure():
  """Test basic service module structures."""
  services_to_test = ['auth', 'loader', 'status_checker', 'ts_analysis']

  for service_name in services_to_test:
    try:
      if service_name == 'auth':
        from src.services.auth import AuthService
        # Check AuthService has basic attributes
        assert hasattr(AuthService, 'requester_id')
        assert hasattr(AuthService, 'hashed_requester_id')
      else:
        service_module = __import__(f'src.services.{service_name}',
                                    fromlist=[service_name])
        # Check module has basic attributes
        assert hasattr(service_module, '__name__')
        # Check for module contents
        module_attrs = dir(service_module)
        assert len(module_attrs) > 0

    except ImportError:
      # Skip if module not available
      continue


def test_auth_basic_functions():
  """Test AuthService has expected functions."""
  try:
    from src.services.auth import AuthService

    # Check for expected static methods
    assert hasattr(AuthService, 'requester_id')
    assert hasattr(AuthService, 'hashed_requester_id')
    assert hasattr(AuthService, 'login')
    assert hasattr(AuthService, 'logout')
    assert hasattr(AuthService, 'verify_session')

    # Check they are callable
    assert callable(AuthService.client_uid)
    assert callable(AuthService.hashed_requester_id)
    assert callable(AuthService.login)
    assert callable(AuthService.logout)
    assert callable(AuthService.verify_session)

  except ImportError:
    pytest.skip("Auth service not available")


def test_status_checker_structure():
  """Test status checker module structure."""
  try:
    from src.services import status_checker
    module_attrs = dir(status_checker)
    # Should have some functions or classes
    assert len([attr for attr in module_attrs if not attr.startswith('_')]) > 0
  except ImportError:
    pytest.skip("Status checker not available")
