"""Tests for ServerConfig class."""
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import ServerConfig


class TestServerConfig:
  """Test ServerConfig functionality."""

  def test_server_config_default(self):
    """Test ServerConfig with default values."""
    config = ServerConfig()

    assert config.host == "127.0.0.1"
    assert config.port == 40004
    assert config.ws_ping_interval == 30
    assert config.ws_ping_timeout == 20
    assert config.auth_methods == ["static"]
    assert config.static_auth_token is None
    assert config.jwt_secret_key is None
    assert config.jwt_expires_hours == 24
    assert config.protected_routes == ["/admin/*"]

  def test_server_config_custom_values(self):
    """Test ServerConfig with custom values."""
    config = ServerConfig(
        host="0.0.0.0",
        port=8080,
        auth_methods=["email", "oauth2"],
        jwt_secret_key="test-key",
        jwt_expires_hours=48,
        protected_routes=["/secure/*", "*.admin"]
    )

    assert config.host == "0.0.0.0"
    assert config.port == 8080
    assert config.auth_methods == ["email", "oauth2"]
    assert config.jwt_secret_key == "test-key"
    assert config.jwt_expires_hours == 48
    assert config.protected_routes == ["/secure/*", "*.admin"]

  def test_server_config_to_dict(self):
    """Test ServerConfig to_dict conversion."""
    config = ServerConfig()
    config_dict = config.to_dict()

    assert isinstance(config_dict, dict)
    assert "host" in config_dict
    assert "port" in config_dict
    assert "protected_routes" in config_dict

  def test_server_config_from_dict(self):
    """Test ServerConfig from_dict creation."""
    config_data = {
        "host": "0.0.0.0",
        "port": 8080,
        "protected_routes": ["/api/admin/*"]
    }

    config = ServerConfig.from_dict(config_data)

    assert config.host == "0.0.0.0"
    assert config.port == 8080
    assert config.protected_routes == ["/api/admin/*"]

  def test_server_config_partial_dict(self):
    """Test ServerConfig from_dict with partial data."""
    config_data = {"host": "example.com", "port": 5000}
    config = ServerConfig.from_dict(config_data)

    assert config.host == "example.com"
    assert config.port == 5000
    assert config.protected_routes == ["/admin/*"]  # Has default value

  def test_server_config_empty_protected_resources_by_default(self):
    """Test that protected routes have defaults but resources don't."""
    config = ServerConfig()

    assert config.protected_routes == ["/admin/*"]  # Has default value

  def test_auth_methods_defaults(self):
    """Test auth methods default configuration."""
    config = ServerConfig()

    assert config.auth_methods == ["static"]
    assert config.static_auth_token is None
    assert config.jwt_secret_key is None
    assert config.jwt_expires_hours == 24

  def test_rate_limit_defaults(self):
    """Test that rate limit configs have proper defaults."""
    config = ServerConfig()

    # Should have default rate limit configs
    assert config.default_rate_limits is not None
    assert config.input_rate_limits is not None

    # Check a few key rate limit values
    assert config.default_rate_limits.rpm == 60
    assert config.default_rate_limits.rph == 1200
    assert config.input_rate_limits.start == 50
    assert config.input_rate_limits.factor == 6.0

  def test_server_config_serialization_roundtrip(self):
    """Test that ServerConfig can be serialized and deserialized."""
    original = ServerConfig(
        host="test.example.com",
        port=9999,
        protected_routes=["/secret/*", "*.private"]
    )

    # Serialize to dict
    config_dict = original.to_dict()

    # Deserialize back
    restored = ServerConfig.from_dict(config_dict)

    # Verify round-trip integrity
    assert restored.host == original.host
    assert restored.port == original.port
    assert restored.protected_routes == original.protected_routes
