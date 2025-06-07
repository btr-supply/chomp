"""Tests for adapters.victoriametrics module."""
import pytest
import sys
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch
from os import environ as env

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.adapters.victoriametrics import VictoriaMetrics


class TestVictoriaMetrics:
  """Test VictoriaMetrics adapter."""

  @pytest.mark.asyncio
  async def test_inherit_from_prometheus(self):
    """Test that VictoriaMetrics inherits from PrometheusAdapter."""
    from src.adapters.prometheus import PrometheusAdapter
    assert issubclass(VictoriaMetrics, PrometheusAdapter)

  @pytest.mark.asyncio
  async def test_connect_with_defaults(self):
    """Test connect with default parameters."""
    with patch.dict(env, {}, clear=True), \
         patch.object(VictoriaMetrics, 'ensure_connected', new_callable=AsyncMock) as mock_ensure:

      adapter = await VictoriaMetrics.connect()

      assert adapter.host == "localhost"
      assert adapter.port == 8428
      assert adapter.db == "default"
      assert adapter.user == ""
      assert adapter.password == ""
      mock_ensure.assert_called_once()

  @pytest.mark.asyncio
  async def test_connect_with_env_vars(self):
    """Test connect with environment variables."""
    env_vars = {
        'VICTORIAMETRICS_HOST': 'vm-host',
        'VICTORIAMETRICS_PORT': '9090',
        'VICTORIAMETRICS_DB': 'test-db',
        'DB_RW_USER': 'test-user',
        'DB_RW_PASS': 'test-pass'
    }

    with patch.dict(env, env_vars), \
         patch.object(VictoriaMetrics, 'ensure_connected', new_callable=AsyncMock) as mock_ensure:

      adapter = await VictoriaMetrics.connect()

      assert adapter.host == "vm-host"
      assert adapter.port == 9090
      assert adapter.db == "test-db"
      assert adapter.user == "test-user"
      assert adapter.password == "test-pass"
      mock_ensure.assert_called_once()

  @pytest.mark.asyncio
  async def test_connect_with_parameters(self):
    """Test connect with explicit parameters."""
    with patch.object(VictoriaMetrics,
                      'ensure_connected',
                      new_callable=AsyncMock) as mock_ensure:

      adapter = await VictoriaMetrics.connect(host="custom-host",
                                              port=8080,
                                              db="custom-db",
                                              user="custom-user",
                                              password="custom-pass")

      assert adapter.host == "custom-host"
      assert adapter.port == 8080
      assert adapter.db == "custom-db"
      assert adapter.user == "custom-user"
      assert adapter.password == "custom-pass"
      mock_ensure.assert_called_once()

  @pytest.mark.asyncio
  async def test_ensure_connected_without_auth(self):
    """Test ensure_connected without authentication."""
    adapter = VictoriaMetrics(host="localhost",
                              port=8428,
                              db="test",
                              user="",
                              password="")
    adapter.session = None

    with patch('httpx.AsyncClient') as mock_session, \
         patch('src.adapters.victoriametrics.log_info') as mock_log:

      await adapter.ensure_connected()

      mock_session.assert_called_once_with(auth=None)
      mock_log.assert_called_once_with(
          "Connected to VictoriaMetrics on localhost:8428")
      assert adapter.session is not None

  @pytest.mark.asyncio
  async def test_ensure_connected_with_auth(self):
    """Test ensure_connected with authentication."""
    adapter = VictoriaMetrics(host="localhost",
                              port=8428,
                              db="test",
                              user="user",
                              password="pass")
    adapter.session = None

    with patch('httpx.AsyncClient') as mock_session, \
         patch('httpx.BasicAuth') as mock_auth, \
         patch('src.adapters.victoriametrics.log_info') as mock_log:

      mock_auth.return_value = "mock_auth"

      await adapter.ensure_connected()

      mock_auth.assert_called_once_with("user", "pass")
      mock_session.assert_called_once_with(auth="mock_auth")
      mock_log.assert_called_once_with(
          "Connected to VictoriaMetrics on localhost:8428")

  @pytest.mark.asyncio
  async def test_ensure_connected_already_connected(self):
    """Test ensure_connected when already connected."""
    adapter = VictoriaMetrics(host="localhost",
                              port=8428,
                              db="test",
                              user="",
                              password="")
    adapter.session = Mock()  # Already has a session

    with patch('httpx.AsyncClient') as mock_session, \
         patch('src.adapters.victoriametrics.log_info') as mock_log:

      await adapter.ensure_connected()

      # Should not create a new session or log
      mock_session.assert_not_called()
      mock_log.assert_not_called()
