"""Basic tests for Chomp functionality."""
import pytest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_import_main():
  """Test that main module can be imported."""
  try:
    import main
    assert hasattr(main, 'main')
  except ImportError:
    pytest.skip("main.py not found")


def test_import_src():
  """Test that src module can be imported."""
  import src
  assert src is not None


def test_import_core_modules():
  """Test that core modules can be imported."""
  # Test core modules
  import src.model
  assert src.models is not None

  import src.cache
  assert src.cache is not None

  import src.state
  assert src.state is not None

  import chomp.src.utils.deps
  assert chomp.src.utils.deps is not None

  import src.proxies
  assert src.proxies is not None


def test_import_utils():
  """Test that utils modules can be imported."""
  from src.utils import types, date, format, maths, safe_eval, runtime, argparser

  assert types is not None
  assert date is not None
  assert format is not None
  assert maths is not None
  assert safe_eval is not None
  assert runtime is not None
  assert argparser is not None


def test_import_services():
  """Test that services can be imported."""
  from src.services import loader, converter, limiter, status_checker, ts_analysis

  assert loader is not None
  assert converter is not None
  assert limiter is not None
  assert status_checker is not None
  assert ts_analysis is not None


def test_import_adapters():
  """Test that common adapters can be imported."""
  # Test adapters that don't require external dependencies
  from src.adapters.jsonrpc import JsonRpcClient
  from src.adapters.sql import SqlAdapter

  assert JsonRpcClient is not None
  assert SqlAdapter is not None


def test_import_server():
  """Test that server modules can be imported."""
  from src.server import responses
  from src.server.routers import forwarder, retriever
  from src.server.middlewares import version_resolver

  assert responses is not None
  assert forwarder is not None
  assert retriever is not None
  assert version_resolver is not None
