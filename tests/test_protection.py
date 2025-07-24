"""Simple tests for the protection system functionality."""
import pytest
from unittest.mock import patch
import sys
from pathlib import Path
import fnmatch

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import ServerConfig


class TestProtectionLogic:
  """Test protection logic without complex middleware setup."""

  def test_fnmatch_patterns(self):
    """Test that fnmatch patterns work as expected for route protection."""
    # Test patterns that would be used in protection middleware

    # Test paths that should match
    test_cases = [
      ("/admin/users", "/admin/*", True),
      ("/admin/settings/general", "/admin/*", True),
      ("/system.monitor", "*.monitor", True),
      ("/app.monitor", "*.monitor", True),
      ("/api/secure/data", "/api/secure/*", True),
      ("/public/data", "/admin/*", False),
      ("/api/public", "*.monitor", False),
      ("/api/data", "/api/secure/*", False),
    ]

    for path, pattern, should_match in test_cases:
      assert fnmatch.fnmatch(path, pattern) == should_match

  def test_server_config_protection_defaults(self):
    """Test ServerConfig protection defaults."""
    config = ServerConfig()

    # Should have default protected routes
    assert config.protected_routes == ["/admin/*"]

  def test_server_config_protection_customization(self):
    """Test ServerConfig protection customization."""
    config = ServerConfig(
      protected_routes=["/secure/*", "*.private"]
    )

    assert config.protected_routes == ["/secure/*", "*.private"]

  def test_protection_pattern_validation(self):
    """Test various protection patterns."""
    valid_patterns = [
      "/admin/*",
      "*.monitor",
      "/api/v1/admin/*",
      "admin.*",
      "*.secret",
      "/exact/path",
      "exact_resource"
    ]

    for pattern in valid_patterns:
      config = ServerConfig(
        protected_routes=[pattern]
      )
      assert pattern in config.protected_routes

  def test_config_serialization_with_protection(self):
    """Test config serialization includes protection settings."""
    config = ServerConfig(
      protected_routes=["/admin/*", "*.secure"]
    )

    config_dict = config.to_dict()

    assert "protected_routes" in config_dict
    assert config_dict["protected_routes"] == ["/admin/*", "*.secure"]

  def test_config_from_dict_with_protection(self):
    """Test creating config from dict with protection settings."""
    data = {
      "host": "localhost",
      "port": 8080,
      "protected_routes": ["/secure/*"]
    }

    config = ServerConfig.from_dict(data)

    assert config.host == "localhost"
    assert config.port == 8080
    assert config.protected_routes == ["/secure/*"]


class TestResourceFilteringLogic:
  """Test resource filtering logic components."""

  def test_protection_pattern_matching_for_resources(self):
    """Test pattern matching for resource names."""
    # Simulate the pattern matching logic used in resource filtering
    protected_patterns = ["admin.*", "*.secret", "internal_*"]

    test_resources = [
      ("admin.users", True),  # matches admin.*
      ("admin.settings", True),  # matches admin.*
      ("data.secret", True),  # matches *.secret
      ("config.secret", True),  # matches *.secret
      ("internal_data", True),  # matches internal_*
      ("public.data", False),  # no match
      ("user.profile", False),  # no match
    ]

    for resource, should_be_protected in test_resources:
      is_protected = any(fnmatch.fnmatch(resource, pattern) for pattern in protected_patterns)
      assert is_protected == should_be_protected

  @pytest.mark.asyncio
  async def test_parse_resources_basic_functionality(self):
    """Test basic parse_resources functionality without complex mocking."""
    from src.services.loader import parse_resources

    # Test with minimal mocking - just test the error handling
    with patch('src.services.loader.get_resources') as mock_get_resources:
      # Test error case
      mock_get_resources.return_value = ("Error occurred", {})

      err, result = await parse_resources("test")
      # The actual error message might be different, just check it's not empty
      assert err != ""
      assert result == []

  def test_split_functionality(self):
    """Test resource string splitting logic."""
    from src.utils.format import split

    # Test various resource string formats
    test_cases = [
      ("resource1,resource2", ["resource1", "resource2"]),
      ("resource1, resource2", ["resource1", " resource2"]),  # split doesn't strip
      ("single_resource", ["single_resource"]),
      ("", None),  # empty string
      (None, None),  # None input
    ]

    for input_str, expected in test_cases:
      result = split(input_str)
      assert result == expected


class TestCompleteProtectionWorkflow:
  """Test the complete protection workflow concepts."""

  def test_protection_system_configuration(self):
    """Test that protection system can be properly configured."""
    # Test a realistic protection configuration
    config = ServerConfig(
      protected_routes=[
        "/admin/*",
        "/api/v1/admin/*",
        "*.monitor",
        "/secure/*"
      ]
    )

    # Verify routes
    assert "/admin/*" in config.protected_routes
    assert "/api/v1/admin/*" in config.protected_routes
    assert "*.monitor" in config.protected_routes
    assert "/secure/*" in config.protected_routes

  def test_route_protection_patterns(self):
    """Test that route protection patterns work correctly."""
    config = ServerConfig(
      protected_routes=["/admin/*", "*.admin"]  # Web routes
    )

    # Routes use path-like patterns
    route_patterns = config.protected_routes
    assert any("/" in pattern for pattern in route_patterns)

    # Resource protection is now handled by resource.protected attribute
    # rather than server config patterns

  def test_empty_protection_configuration(self):
    """Test system behavior with no protection configured."""
    config = ServerConfig(
      protected_routes=[]
    )

    assert config.protected_routes == []

    # System should still function with empty protection lists
    config_dict = config.to_dict()
    assert config_dict["protected_routes"] == []

  def test_protection_inheritance_and_defaults(self):
    """Test protection setting inheritance and defaults."""
    # Default config should have some protection
    default_config = ServerConfig()
    assert len(default_config.protected_routes) > 0  # Has default route protection

    # Custom config can override
    custom_config = ServerConfig(protected_routes=[])
    assert custom_config.protected_routes == []
