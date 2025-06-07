"""Tests for server.middlewares.__init__ module."""
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_middlewares_imports():
  """Test that middlewares module imports work correctly."""
  import src.server.middlewares

  # Test that __all__ is defined
  assert hasattr(src.server.middlewares, '__all__')
  assert isinstance(src.server.middlewares.__all__, list)
  assert len(src.server.middlewares.__all__) > 0


def test_middlewares_all_exports():
  """Test that all items in __all__ are accessible."""
  import src.server.middlewares

  for item in src.server.middlewares.__all__:
    assert hasattr(src.server.middlewares, item), f"Module should export '{item}'"


def test_middlewares_specific_exports():
  """Test specific middleware exports."""
  import src.server.middlewares

  # Test expected middleware components
  expected_items = ["Limiter", "limit", "VersionResolver"]

  for item in expected_items:
    if item in src.server.middlewares.__all__:
      assert hasattr(src.server.middlewares, item), f"Should have '{item}' middleware"
      middleware = getattr(src.server.middlewares, item)
      assert middleware is not None, f"Middleware '{item}' should not be None"
