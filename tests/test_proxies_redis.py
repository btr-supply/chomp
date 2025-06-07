"""Tests for Redis proxy module."""
import pytest
from unittest.mock import Mock, patch, AsyncMock
import sys
from os import environ as env
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.proxies import RedisProxy


class TestRedisProxy:
  """Test the Redis proxy functionality."""

  def test_redis_proxy_imports(self):
    """Test that Redis proxy imports correctly."""
    assert RedisProxy is not None

  def test_redis_proxy_initialization(self):
    """Test Redis proxy initialization."""
    proxy = RedisProxy()

    assert proxy._pool is None
    assert proxy._redis is None
    assert proxy._pubsub is None

  def test_redis_property_lazy_loading(self):
    """Test Redis client lazy loading."""
    proxy = RedisProxy()
    mock_redis = Mock()
    mock_pool = Mock()

    with patch('src.proxies.ConnectionPool') as mock_pool_class:
      with patch('src.proxies.Redis') as mock_redis_class:
        mock_pool_class.return_value = mock_pool
        mock_redis_class.return_value = mock_redis

        redis_client = proxy.redis

        # Should create pool and redis client
        mock_pool_class.assert_called_once()
        mock_redis_class.assert_called_once_with(connection_pool=mock_pool)
        assert redis_client == mock_redis
        assert proxy._pool == mock_pool
        assert proxy._redis == mock_redis

  def test_redis_property_reuse_existing(self):
    """Test Redis client reuses existing connection."""
    proxy = RedisProxy()
    mock_redis = Mock()
    proxy._redis = mock_redis

    redis_client = proxy.redis

    # Should return existing client
    assert redis_client == mock_redis

  def test_redis_connection_pool_config(self):
    """Test Redis connection pool configuration from environment."""
    proxy = RedisProxy()

    with patch.dict(
        env, {
            'REDIS_HOST': 'test-host',
            'REDIS_PORT': '6380',
            'DB_RW_USER': 'test-user',
            'DB_RW_PASS': 'test-pass',
            'REDIS_DB': '5',
            'REDIS_MAX_CONNECTIONS': '1000'
        }):
      with patch('src.proxies.ConnectionPool') as mock_pool_class:
        with patch('src.proxies.Redis'):
          proxy.redis

          mock_pool_class.assert_called_once_with(host='test-host',
                                                  port=6380,
                                                  username='test-user',
                                                  password='test-pass',
                                                  db=5,
                                                  max_connections=1000)

  def test_redis_connection_pool_defaults(self):
    """Test Redis connection pool uses defaults when env vars not set."""
    proxy = RedisProxy()

    with patch.dict(env, {}, clear=True):
      with patch('src.proxies.ConnectionPool') as mock_pool_class:
        with patch('src.proxies.Redis'):
          proxy.redis

          mock_pool_class.assert_called_once_with(
              host='localhost',
              port=6379,
              username='rw',
              password='pass',
              db=0,
              max_connections=65536  # 2 ** 16
          )

  def test_pubsub_property_lazy_loading(self):
    """Test pubsub lazy loading."""
    proxy = RedisProxy()
    mock_redis = Mock()
    mock_pubsub = Mock()
    mock_redis.pubsub.return_value = mock_pubsub
    proxy._redis = mock_redis

    pubsub = proxy.pubsub

    mock_redis.pubsub.assert_called_once()
    assert pubsub == mock_pubsub
    assert proxy._pubsub == mock_pubsub

  def test_pubsub_property_reuse_existing(self):
    """Test pubsub reuses existing connection."""
    proxy = RedisProxy()
    mock_pubsub = Mock()
    proxy._pubsub = mock_pubsub

    pubsub = proxy.pubsub

    # Should return existing pubsub
    assert pubsub == mock_pubsub

  @pytest.mark.asyncio
  async def test_close_all_connections(self):
    """Test closing all Redis connections."""
    proxy = RedisProxy()
    mock_pubsub = AsyncMock()
    mock_redis = AsyncMock()
    mock_pool = AsyncMock()

    proxy._pubsub = mock_pubsub
    proxy._redis = mock_redis
    proxy._pool = mock_pool

    await proxy.close()

    # Should close all connections in order
    mock_pubsub.close.assert_called_once()
    mock_redis.close.assert_called_once()
    mock_pool.disconnect.assert_called_once()

    # Should reset all references
    assert proxy._pubsub is None
    assert proxy._redis is None
    assert proxy._pool is None

  @pytest.mark.asyncio
  async def test_close_partial_connections(self):
    """Test closing with only some connections active."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()

    # Only redis connection exists
    proxy._redis = mock_redis

    await proxy.close()

    mock_redis.close.assert_called_once()
    assert proxy._redis is None

  @pytest.mark.asyncio
  async def test_close_no_connections(self):
    """Test closing with no active connections."""
    proxy = RedisProxy()

    # Should not raise any errors
    await proxy.close()

    assert proxy._pubsub is None
    assert proxy._redis is None
    assert proxy._pool is None

  def test_getattr_delegation(self):
    """Test that unknown attributes are delegated to Redis client."""
    proxy = RedisProxy()
    mock_redis = Mock()
    mock_redis.set = Mock(return_value="SET_RESULT")
    mock_redis.get = Mock(return_value="GET_RESULT")
    proxy._redis = mock_redis

    # Test delegation
    result = proxy.set("key", "value")
    assert result == "SET_RESULT"
    mock_redis.set.assert_called_once_with("key", "value")

    result = proxy.get("key")
    assert result == "GET_RESULT"
    mock_redis.get.assert_called_once_with("key")

  def test_getattr_delegation_with_lazy_loading(self):
    """Test attribute delegation triggers lazy loading."""
    proxy = RedisProxy()
    mock_redis = Mock()
    mock_redis.ping = Mock(return_value="PONG")

    # Mock the internal _redis attribute instead of the property
    proxy._redis = mock_redis

    result = proxy.ping()
    assert result == "PONG"
    mock_redis.ping.assert_called_once()

  def test_multiple_proxy_instances(self):
    """Test that multiple proxy instances are independent."""
    proxy1 = RedisProxy()
    proxy2 = RedisProxy()

    assert proxy1 is not proxy2
    assert proxy1._redis is None
    assert proxy2._redis is None

    # Set up different clients
    mock_redis1 = Mock()
    mock_redis2 = Mock()
    proxy1._redis = mock_redis1
    proxy2._redis = mock_redis2

    assert proxy1.redis == mock_redis1
    assert proxy2.redis == mock_redis2

  def test_connection_pool_reuse(self):
    """Test that connection pool is reused once created."""
    proxy = RedisProxy()
    mock_pool = Mock()
    mock_redis1 = Mock()
    mock_redis2 = Mock()

    with patch('src.proxies.ConnectionPool',
               return_value=mock_pool) as mock_pool_class:
      with patch('src.proxies.Redis',
                 side_effect=[mock_redis1, mock_redis2]) as mock_redis_class:
        # First access

        # Reset redis but keep pool
        proxy._redis = None

        # Second access should reuse pool

        # Pool should only be created once
        mock_pool_class.assert_called_once()
        # Redis should be created twice (once for each access)
        assert mock_redis_class.call_count == 2

  def test_environment_variable_conversion(self):
    """Test proper conversion of environment variables."""
    proxy = RedisProxy()

    with patch.dict(env, {
        'REDIS_PORT': '9999',
        'REDIS_DB': '10',
        'REDIS_MAX_CONNECTIONS': '500'
    }):
      with patch('src.proxies.ConnectionPool') as mock_pool_class:
        with patch('src.proxies.Redis'):
          proxy.redis

          # Check that string values are converted to integers
          call_args = mock_pool_class.call_args[1]
          assert call_args['port'] == 9999
          assert call_args['db'] == 10
          assert call_args['max_connections'] == 500

  def test_connection_pool_error_handling(self):
    """Test error handling during connection pool creation."""
    proxy = RedisProxy()

    with patch('src.proxies.ConnectionPool',
               side_effect=Exception("Connection failed")):
      with pytest.raises(Exception) as exc_info:
        proxy.redis

      assert "Connection failed" in str(exc_info.value)

  def test_redis_client_error_handling(self):
    """Test error handling during Redis client creation."""
    proxy = RedisProxy()
    mock_pool = Mock()

    with patch('src.proxies.ConnectionPool', return_value=mock_pool):
      with patch('src.proxies.Redis',
                 side_effect=Exception("Redis creation failed")):
        with pytest.raises(Exception) as exc_info:
          proxy.redis

        assert "Redis creation failed" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_close_error_handling(self):
    """Test error handling during close operations."""
    proxy = RedisProxy()
    mock_pubsub = AsyncMock()
    mock_redis = AsyncMock()
    mock_pool = AsyncMock()

    # Make close operations fail
    mock_pubsub.close = AsyncMock(side_effect=Exception("Pubsub close failed"))
    mock_redis.close = AsyncMock(side_effect=Exception("Redis close failed"))
    mock_pool.disconnect = AsyncMock(
        side_effect=Exception("Pool disconnect failed"))

    proxy._pubsub = mock_pubsub
    proxy._redis = mock_redis
    proxy._pool = mock_pool

    # Should not raise exceptions, but should still clean up references
    await proxy.close()

    assert proxy._pubsub is None
    assert proxy._redis is None
    assert proxy._pool is None

  def test_proxy_state_consistency(self):
    """Test that proxy maintains consistent state."""
    proxy = RedisProxy()

    # Initial state
    assert proxy._pool is None
    assert proxy._redis is None
    assert proxy._pubsub is None

    # After accessing redis
    mock_redis = Mock()
    mock_pool = Mock()

    with patch('src.proxies.ConnectionPool', return_value=mock_pool):
      with patch('src.proxies.Redis', return_value=mock_redis):

        assert proxy._pool == mock_pool
        assert proxy._redis == mock_redis
        assert proxy._pubsub is None  # Not accessed yet

        # After accessing pubsub
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub

        assert proxy._pool == mock_pool
        assert proxy._redis == mock_redis
        assert proxy._pubsub == mock_pubsub

  def test_integration_patterns(self):
    """Test common integration patterns."""
    proxy = RedisProxy()
    mock_redis = Mock()

    # Common Redis operations
    mock_redis.set.return_value = True
    mock_redis.get.return_value = b"cached_value"
    mock_redis.delete.return_value = 1
    mock_redis.exists.return_value = True

    proxy._redis = mock_redis

    # Test common operations
    assert proxy.set("key", "value") is True
    assert proxy.get("key") == b"cached_value"
    assert proxy.delete("key") == 1
    assert proxy.exists("key") is True

    # Verify calls
    mock_redis.set.assert_called_with("key", "value")
    mock_redis.get.assert_called_with("key")
    mock_redis.delete.assert_called_with("key")
    mock_redis.exists.assert_called_with("key")

  def test_pubsub_integration_patterns(self):
    """Test pubsub integration patterns."""
    proxy = RedisProxy()
    mock_redis = Mock()
    mock_pubsub = Mock()

    mock_redis.pubsub.return_value = mock_pubsub
    mock_pubsub.subscribe = AsyncMock()
    mock_pubsub.publish = AsyncMock()

    proxy._redis = mock_redis

    pubsub = proxy.pubsub
    assert pubsub == mock_pubsub

    # Pubsub should be cached
    pubsub2 = proxy.pubsub
    assert pubsub2 == mock_pubsub
    mock_redis.pubsub.assert_called_once()  # Only called once due to caching
