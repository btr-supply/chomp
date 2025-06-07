"""Tests for src.proxies module."""
import pytest
import sys
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch
from os import environ as env

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.proxies import RedisProxy


class TestRedisProxy:
  """Test RedisProxy class functionality."""

  def test_redis_proxy_init(self):
    """Test RedisProxy initialization."""
    proxy = RedisProxy()
    assert proxy._pool is None
    assert proxy._redis is None
    assert proxy._pubsub is None

  def test_redis_property_creates_connection(self):
    """Test that accessing redis property creates connection."""
    proxy = RedisProxy()
    mock_redis = Mock()
    mock_pool = Mock()

    with patch('src.proxies.ConnectionPool', return_value=mock_pool) as mock_pool_class:
      with patch('src.proxies.Redis', return_value=mock_redis) as mock_redis_class:
        result = proxy.redis

        # Should create pool and redis connection
        mock_pool_class.assert_called_once()
        mock_redis_class.assert_called_once_with(connection_pool=mock_pool)
        assert result == mock_redis
        assert proxy._pool == mock_pool
        assert proxy._redis == mock_redis

  def test_redis_property_reuses_existing(self):
    """Test that redis property reuses existing connection."""
    proxy = RedisProxy()
    mock_redis = Mock()
    proxy._redis = mock_redis

    result = proxy.redis
    assert result == mock_redis

  def test_pubsub_property_creates_pubsub(self):
    """Test that accessing pubsub property creates pubsub."""
    proxy = RedisProxy()
    mock_redis = Mock()
    mock_pubsub = Mock()
    mock_redis.pubsub.return_value = mock_pubsub
    proxy._redis = mock_redis

    result = proxy.pubsub

    mock_redis.pubsub.assert_called_once()
    assert result == mock_pubsub
    assert proxy._pubsub == mock_pubsub

  def test_pubsub_property_reuses_existing(self):
    """Test that pubsub property reuses existing pubsub."""
    proxy = RedisProxy()
    mock_pubsub = Mock()
    proxy._pubsub = mock_pubsub

    result = proxy.pubsub
    assert result == mock_pubsub

  @pytest.mark.asyncio
  async def test_ping_success(self):
    """Test successful ping."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    mock_redis.ping.return_value = True
    proxy._redis = mock_redis

    result = await proxy.ping()

    mock_redis.ping.assert_called_once()
    assert result is True

  @pytest.mark.asyncio
  async def test_ping_failure(self):
    """Test RedisProxy ping when connection fails."""
    proxy = RedisProxy()

    # Create an async mock that raises an exception
    mock_redis = AsyncMock()
    mock_redis.ping.side_effect = Exception("Connection failed")
    proxy._redis = mock_redis

    result = await proxy.ping()

    mock_redis.ping.assert_called_once()
    assert result is False

  @pytest.mark.asyncio
  async def test_set_operation(self):
    """Test Redis SET operation."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    proxy._redis = mock_redis

    await proxy.set("test_key", "test_value")

    mock_redis.set.assert_called_once_with("test_key", "test_value")

  @pytest.mark.asyncio
  async def test_get_operation(self):
    """Test Redis GET operation."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    mock_redis.get.return_value = b"test_value"
    proxy._redis = mock_redis

    result = await proxy.get("test_key")

    mock_redis.get.assert_called_once_with("test_key")
    assert result == b"test_value"

  @pytest.mark.asyncio
  async def test_setex_operation(self):
    """Test Redis SETEX operation."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    proxy._redis = mock_redis

    await proxy.setex("test_key", 3600, "test_value")

    mock_redis.setex.assert_called_once_with("test_key", 3600, "test_value")

  @pytest.mark.asyncio
  async def test_delete_operation(self):
    """Test Redis DELETE operation."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    mock_redis.delete.return_value = 1
    proxy._redis = mock_redis

    result = await proxy.delete("test_key")

    mock_redis.delete.assert_called_once_with("test_key")
    assert result == 1

  @pytest.mark.asyncio
  async def test_exists_operation(self):
    """Test Redis EXISTS operation."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    mock_redis.exists.return_value = 1
    proxy._redis = mock_redis

    result = await proxy.exists("test_key")

    mock_redis.exists.assert_called_once_with("test_key")
    assert result == 1

  @pytest.mark.asyncio
  async def test_publish_operation(self):
    """Test Redis PUBLISH operation."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    mock_redis.publish.return_value = 1
    proxy._redis = mock_redis

    result = await proxy.publish("test_channel", "test_message")

    mock_redis.publish.assert_called_once_with("test_channel", "test_message")
    assert result == 1

  @pytest.mark.asyncio
  async def test_pipeline_operation(self):
    """Test Redis pipeline operation."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    mock_pipeline = AsyncMock()
    mock_redis.pipeline.return_value = mock_pipeline
    proxy._redis = mock_redis

    # Make the pipeline call async for consistency with redis async API
    result = await proxy.redis.pipeline()

    mock_redis.pipeline.assert_called_once()
    assert result == mock_pipeline

  @pytest.mark.asyncio
  async def test_keys_operation(self):
    """Test Redis KEYS operation."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    mock_redis.keys.return_value = [b"key1", b"key2"]
    proxy._redis = mock_redis

    result = await proxy.keys("test_pattern")

    mock_redis.keys.assert_called_once_with("test_pattern")
    assert result == [b"key1", b"key2"]

  @pytest.mark.asyncio
  async def test_scan_operation(self):
    """Test Redis SCAN operation."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    mock_redis.scan.return_value = (0, [b"key1", b"key2"])
    proxy._redis = mock_redis

    result = await proxy.scan(cursor=0, match="test_*", count=10)

    mock_redis.scan.assert_called_once_with(cursor=0, match="test_*", count=10)
    assert result == (0, [b"key1", b"key2"])

  @pytest.mark.asyncio
  async def test_flushdb_operation(self):
    """Test Redis FLUSHDB operation."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    mock_redis.flushdb.return_value = True
    proxy._redis = mock_redis

    result = await proxy.flushdb()

    mock_redis.flushdb.assert_called_once()
    assert result is True

  @pytest.mark.asyncio
  async def test_info_operation(self):
    """Test Redis INFO operation."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    mock_redis.info.return_value = {"redis_version": "6.0.0"}
    proxy._redis = mock_redis

    result = await proxy.info()

    mock_redis.info.assert_called_once()
    assert result == {"redis_version": "6.0.0"}

  @pytest.mark.asyncio
  async def test_error_handling(self):
    """Test error handling in Redis operations."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    mock_redis.get.side_effect = Exception("Redis error")
    proxy._redis = mock_redis

    with pytest.raises(Exception, match="Redis error"):
      await proxy.get("test_key")

  @pytest.mark.asyncio
  async def test_transaction_operations(self):
    """Test Redis transaction operations."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    mock_pipeline = AsyncMock()
    mock_redis.pipeline.return_value = mock_pipeline
    proxy._redis = mock_redis

    # Test pipeline with transaction
    proxy.pipeline(transaction=True)
    mock_redis.pipeline.assert_called_once_with(transaction=True)

  @pytest.mark.asyncio
  async def test_hash_operations(self):
    """Test Redis hash operations."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    proxy._redis = mock_redis

    # Test HSET
    await proxy.hset("test_hash", "field1", "value1")
    mock_redis.hset.assert_called_once_with("test_hash", "field1", "value1")

    # Test HGET
    mock_redis.hget.return_value = b"value1"
    result = await proxy.hget("test_hash", "field1")
    mock_redis.hget.assert_called_once_with("test_hash", "field1")
    assert result == b"value1"

    # Test HGETALL
    mock_redis.hgetall.return_value = {b"field1": b"value1", b"field2": b"value2"}
    result = await proxy.hgetall("test_hash")
    mock_redis.hgetall.assert_called_once_with("test_hash")
    assert result == {b"field1": b"value1", b"field2": b"value2"}

  @pytest.mark.asyncio
  async def test_list_operations(self):
    """Test Redis list operations."""
    proxy = RedisProxy()
    mock_redis = AsyncMock()
    proxy._redis = mock_redis

    # Test LPUSH
    await proxy.lpush("test_list", "item1")
    mock_redis.lpush.assert_called_once_with("test_list", "item1")

    # Test RPOP
    mock_redis.rpop.return_value = b"item1"
    result = await proxy.rpop("test_list")
    mock_redis.rpop.assert_called_once_with("test_list")
    assert result == b"item1"

  def test_connection_pool_configuration(self):
    """Test connection pool configuration."""
    proxy = RedisProxy()

    with patch('src.proxies.ConnectionPool') as mock_pool_class:
      with patch('src.proxies.Redis'):
        with patch.dict(env, {
          'REDIS_HOST': 'test_host',
          'REDIS_PORT': '6380',
          'REDIS_DB': '2'
        }):
          _ = proxy.redis

          # Verify connection pool was created with environment variables
          mock_pool_class.assert_called_once()

  @pytest.mark.asyncio
  async def test_pubsub_subscribe(self):
    """Test pubsub subscribe operation."""
    proxy = RedisProxy()
    mock_pubsub = AsyncMock()
    proxy._pubsub = mock_pubsub

    await proxy.pubsub.subscribe("test_channel")
    mock_pubsub.subscribe.assert_called_once_with("test_channel")

  @pytest.mark.asyncio
  async def test_pubsub_unsubscribe(self):
    """Test pubsub unsubscribe operation."""
    proxy = RedisProxy()
    mock_pubsub = AsyncMock()
    proxy._pubsub = mock_pubsub

    await proxy.pubsub.unsubscribe("test_channel")
    mock_pubsub.unsubscribe.assert_called_once_with("test_channel")

  @pytest.mark.asyncio
  async def test_pubsub_get_message(self):
    """Test pubsub get_message operation."""
    proxy = RedisProxy()
    mock_pubsub = AsyncMock()
    mock_pubsub.get_message.return_value = {"type": "message", "data": b"test"}
    proxy._pubsub = mock_pubsub

    result = await proxy.pubsub.get_message()
    mock_pubsub.get_message.assert_called_once()
    assert result == {"type": "message", "data": b"test"}
