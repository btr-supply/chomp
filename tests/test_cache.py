"""Tests for src.cache module."""
import pytest
import sys
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch
import pickle

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src import cache


class TestCacheModule:
  """Test cache module functionality."""

  @pytest.mark.asyncio
  async def test_ping_success(self):
    """Test successful Redis ping."""
    with patch('src.cache.state') as mock_state:
      mock_state.redis.ping = AsyncMock(return_value=True)
      result = await cache.ping()
      assert result is True
      mock_state.redis.ping.assert_called_once()

  @pytest.mark.asyncio
  async def test_ping_failure(self):
    """Test Redis ping failure."""
    with patch('src.cache.state') as mock_state, \
         patch('src.cache.log_error') as mock_log_error:
      mock_state.redis.ping = AsyncMock(
          side_effect=Exception("Connection failed"))
      result = await cache.ping()
      assert result is False
      mock_log_error.assert_called_once()

  def test_claim_key(self):
    """Test claim key generation."""
    mock_ingester = Mock()
    mock_ingester.id = "test_ingester_123"

    key = cache.claim_key(mock_ingester)
    assert key == f"{cache.NS}:claims:test_ingester_123"

  @pytest.mark.asyncio
  async def test_claim_task_success(self):
    """Test successful task claim."""
    mock_ingester = Mock()
    mock_ingester.name = "test_ingester"
    mock_ingester.interval = "m1"
    mock_ingester.interval_sec = 60

    with patch('src.cache.state') as mock_state, \
         patch('src.cache.is_task_claimed', return_value=False), \
         patch('src.cache.log_debug') as mock_log_debug:
      mock_state.args.verbose = True
      mock_state.args.proc_id = "worker_1"
      mock_state.redis.setex = AsyncMock(return_value=True)

      result = await cache.claim_task(mock_ingester, until=120)

      assert result is True
      mock_state.redis.setex.assert_called_once()
      mock_log_debug.assert_called_once()

  @pytest.mark.asyncio
  async def test_claim_task_already_claimed(self):
    """Test claim task when already claimed."""
    mock_ingester = Mock()
    mock_ingester.name = "test_ingester"
    mock_ingester.interval = "m1"

    with patch('src.cache.state') as mock_state, \
         patch('src.cache.is_task_claimed', return_value=True):
      mock_state.args.verbose = False
      result = await cache.claim_task(mock_ingester)
      assert result is False

  @pytest.mark.asyncio
  async def test_ensure_claim_task_success(self):
    """Test ensure claim task success."""
    mock_ingester = Mock()
    mock_ingester.probablity = 1.0

    with patch('src.cache.claim_task', return_value=True):
      result = await cache.ensure_claim_task(mock_ingester)
      assert result is True

  @pytest.mark.asyncio
  async def test_ensure_claim_task_failed_claim(self):
    """Test ensure claim task when claim fails."""
    mock_ingester = Mock()
    mock_ingester.name = "test_ingester"
    mock_ingester.interval = "m1"

    with patch('src.cache.claim_task', return_value=False):
      with pytest.raises(ValueError, match="Failed to claim task"):
        await cache.ensure_claim_task(mock_ingester)

  @pytest.mark.asyncio
  async def test_ensure_claim_task_probabilistic_skip(self):
    """Test ensure claim task with probabilistic skip."""
    mock_ingester = Mock()
    mock_ingester.name = "test_ingester"
    mock_ingester.interval = "m1"
    mock_ingester.probablity = 0.1

    with patch('src.cache.claim_task', return_value=True), \
         patch('src.cache.random', return_value=0.9):  # > 0.1, so should skip
      with pytest.raises(ValueError, match="probabilistically skipped"):
        await cache.ensure_claim_task(mock_ingester)

  @pytest.mark.asyncio
  async def test_is_task_claimed_true(self):
    """Test is_task_claimed when task is claimed."""
    mock_ingester = Mock()

    with patch('src.cache.state') as mock_state:
      mock_state.redis.get = AsyncMock(return_value=b"worker_2")
      mock_state.args.proc_id = "worker_1"

      result = await cache.is_task_claimed(mock_ingester, exclude_self=True)
      assert result is True

  @pytest.mark.asyncio
  async def test_is_task_claimed_false(self):
    """Test is_task_claimed when task is not claimed."""
    mock_ingester = Mock()

    with patch('src.cache.state') as mock_state:
      mock_state.redis.get = AsyncMock(return_value=None)

      result = await cache.is_task_claimed(mock_ingester)
      assert result is False

  @pytest.mark.asyncio
  async def test_free_task_success(self):
    """Test successful task release."""
    mock_ingester = Mock()

    with patch('src.cache.state') as mock_state, \
         patch('src.cache.is_task_claimed', return_value=True):
      mock_state.args.proc_id = "worker_1"
      mock_state.redis.get = AsyncMock(return_value=b"worker_1")
      mock_state.redis.delete = AsyncMock(return_value=True)

      result = await cache.free_task(mock_ingester)
      assert result is True

  @pytest.mark.asyncio
  async def test_free_task_not_owned(self):
    """Test free task when not owned by current process."""
    mock_ingester = Mock()

    with patch('src.cache.state') as mock_state, \
         patch('src.cache.is_task_claimed', return_value=True):
      mock_state.args.proc_id = "worker_1"
      mock_state.redis.get = AsyncMock(
          return_value=b"worker_2")  # Different owner

      result = await cache.free_task(mock_ingester)
      assert result is False

  def test_ingester_registry_lock_key(self):
    """Test ingester registry lock key generation."""
    key = cache.ingester_registry_lock_key()
    assert key == f"{cache.NS}:locks:ingesters"

  @pytest.mark.asyncio
  async def test_acquire_registry_lock_success(self):
    """Test successful registry lock acquisition."""
    with patch('src.cache.state') as mock_state:
      mock_state.redis.set = AsyncMock(return_value=True)
      mock_state.args.proc_id = "worker_1"

      result = await cache.acquire_registry_lock()
      assert result is True

  @pytest.mark.asyncio
  async def test_wait_acquire_registry_lock_success(self):
    """Test successful wait for registry lock."""
    with patch('src.cache.acquire_registry_lock') as mock_acquire, \
         patch('src.cache.sleep') as _:
      mock_acquire.side_effect = [False, False, True]  # Succeed on third try

      result = await cache.wait_acquire_registry_lock(timeout_ms=1000)
      assert result is True
      assert mock_acquire.call_count == 3

  @pytest.mark.asyncio
  async def test_wait_acquire_registry_lock_timeout(self):
    """Test registry lock wait timeout."""
    with patch('src.cache.acquire_registry_lock', return_value=False), \
         patch('src.cache.sleep'), \
         patch('src.cache.monotonic', side_effect=[0, 0.1, 0.2, 15]):  # Timeout after 10s

      result = await cache.wait_acquire_registry_lock(timeout_ms=10000)
      assert result is False

  @pytest.mark.asyncio
  async def test_release_registry_lock(self):
    """Test registry lock release."""
    with patch('src.cache.state') as mock_state:
      mock_state.redis.delete = AsyncMock(return_value=True)

      result = await cache.release_registry_lock()
      assert result is True

  def test_cache_key(self):
    """Test cache key generation."""
    key = cache.cache_key("test_name")
    assert key == f"{cache.NS}:cache:test_name"

  @pytest.mark.asyncio
  async def test_cache_string_value(self):
    """Test caching string value."""
    with patch('src.cache.state') as mock_state:
      mock_state.redis.setex = AsyncMock(return_value=True)

      result = await cache.cache("test_key", "test_value", expiry=3600)
      assert result is True
      mock_state.redis.setex.assert_called_once()

  @pytest.mark.asyncio
  async def test_cache_pickled_value(self):
    """Test caching pickled value."""
    test_data = {"key": "value", "number": 42}

    with patch('src.cache.state') as mock_state:
      mock_state.redis.setex = AsyncMock(return_value=True)

      result = await cache.cache("test_key", test_data, pickled=True)
      assert result is True

      # Verify pickled data was passed
      call_args = mock_state.redis.setex.call_args[0]
      assert call_args[0] == f"{cache.NS}:cache:test_key"
      assert isinstance(call_args[2], bytes)  # Should be pickled

  @pytest.mark.asyncio
  async def test_cache_batch(self):
    """Test batch caching."""
    test_data = {"key1": "value1", "key2": "value2"}

    with patch('src.cache.state') as mock_state:
      mock_pipeline = AsyncMock()
      mock_state.redis.pipeline.return_value.__aenter__.return_value = mock_pipeline
      mock_pipeline.execute = AsyncMock()

      await cache.cache_batch(test_data, expiry=3600)

      # Verify pipeline was used
      mock_state.redis.pipeline.assert_called_once()
      mock_pipeline.execute.assert_called_once()

  @pytest.mark.asyncio
  async def test_get_cache_string(self):
    """Test getting cached string value."""
    with patch('src.cache.state') as mock_state:
      mock_state.redis.get = AsyncMock(return_value=b"test_value")

      result = await cache.get_cache("test_key")
      assert result == b"test_value"

  @pytest.mark.asyncio
  async def test_get_cache_none(self):
    """Test getting non-existent cache value."""
    with patch('src.cache.state') as mock_state:
      mock_state.redis.get = AsyncMock(return_value=None)

      result = await cache.get_cache("test_key")
      assert result is None

  @pytest.mark.asyncio
  async def test_get_cache_pickled(self):
    """Test getting pickled cache value."""
    test_data = {"key": "value"}
    pickled_data = pickle.dumps(test_data)

    with patch('src.cache.state') as mock_state:
      mock_state.redis.get = AsyncMock(return_value=pickled_data)

      result = await cache.get_cache("test_key", pickled=True)
      assert result == test_data

  @pytest.mark.asyncio
  async def test_get_cache_batch(self):
    """Test batch cache retrieval."""
    with patch('src.cache.state') as mock_state:
      mock_state.redis.mget = AsyncMock(
          return_value=[b"value1", b"value2", None])

      result = await cache.get_cache_batch(["key1", "key2", "key3"])

      expected = {"key1": b"value1", "key2": b"value2", "key3": None}
      assert result == expected

  @pytest.mark.asyncio
  async def test_get_or_set_cache_exists(self):
    """Test get_or_set when cache exists."""
    with patch('src.cache.get_cache', return_value="cached_value"):

      def callback():
        return "new_value"

      result = await cache.get_or_set_cache("test_key", callback)
      assert result == "cached_value"

  @pytest.mark.asyncio
  async def test_get_or_set_cache_missing(self):
    """Test get_or_set when cache is missing."""
    with patch('src.cache.get_cache', return_value=None), \
         patch('src.cache.cache') as mock_cache:

      def callback():
        return "new_value"

      result = await cache.get_or_set_cache("test_key", callback)
      assert result == "new_value"
      mock_cache.assert_called_once()

  @pytest.mark.asyncio
  async def test_get_or_set_cache_async_callback(self):
    """Test get_or_set with async callback."""
    with patch('src.cache.get_cache', return_value=None), \
         patch('src.cache.cache') as mock_cache:

      async def async_callback():
        return "async_value"

      result = await cache.get_or_set_cache("test_key", async_callback)
      assert result == "async_value"
      mock_cache.assert_called_once()

  @pytest.mark.asyncio
  async def test_get_or_set_cache_empty_value(self):
    """Test get_or_set when callback returns empty value."""
    with patch('src.cache.get_cache', return_value=None), \
         patch('src.cache.log_warn') as mock_log_warn:

      def callback():
        return None

      result = await cache.get_or_set_cache("test_key", callback)
      assert result is None
      mock_log_warn.assert_called_once()

  @pytest.mark.asyncio
  async def test_pub_single_topic(self):
    """Test publishing to single topic."""
    with patch('src.cache.state') as mock_state, \
         patch('asyncio.gather', new_callable=AsyncMock, return_value=[1]):
      mock_state.redis.publish = AsyncMock(return_value=1)

      result = await cache.pub("test_topic", "test_message")
      assert result == [1]

  @pytest.mark.asyncio
  async def test_pub_multiple_topics(self):
    """Test publishing to multiple topics."""
    with patch('src.cache.state') as mock_state, \
         patch('asyncio.gather', new_callable=AsyncMock, return_value=[1, 1]):
      mock_state.redis.publish = AsyncMock(return_value=1)

      result = await cache.pub(["topic1", "topic2"], "test_message")
      assert result == [1, 1]

  @pytest.mark.asyncio
  async def test_get_active_ingesters(self):
    """Test getting active ingesters from monitoring data."""
    mock_resources = ["test_ingester1", "test_ingester2", "test_monitor"]
    mock_data = {
        "ts": "2024-01-01T00:00:00Z",
        "field1": "value1",
        "field2": "value2"
    }

    with patch('src.cache.get_cached_resources', return_value=mock_resources), \
         patch('src.cache.get_cache', return_value=mock_data):
      result = await cache.get_active_ingesters()

      # Should exclude _monitor resources
      assert "test_monitor" not in result
      assert "test_ingester1" in result
      assert "test_ingester2" in result
      assert result["test_ingester1"]["status"] == "active"

  @pytest.mark.asyncio
  async def test_get_active_instances(self):
    """Test getting active instances from monitoring data."""
    mock_keys = [
        b"chomp:cache:instance1.monitor", b"chomp:cache:resource1.monitor"
    ]
    mock_instance_data = {
        "instance_name": "instance1",
        "ts": "2024-01-01T00:00:00Z",
        "cpu_usage": 25.5,
        "memory_usage": 1024,
        "coordinates": "40.7128,-74.0060",
        "location": "New York, NY, USA"
    }

    with patch('src.cache.state') as mock_state, \
         patch('src.cache.get_cache') as mock_get_cache:
      mock_state.redis.keys.return_value = mock_keys
      mock_get_cache.return_value = mock_instance_data

      result = await cache.get_active_instances()

      assert "instance1" in result
      assert result["instance1"]["status"] == "active"
      assert result["instance1"]["cpu_usage"] == 25.5

  @pytest.mark.asyncio
  async def test_get_ingester_status(self):
    """Test getting status of a specific ingester."""
    mock_cached_data = {"ts": "2024-01-01T00:00:00Z", "field1": "value1"}
    mock_monitor_data = {
        "latency_ms": 150.5,
        "response_bytes": 2048,
        "status_code": 200,
        "instance_name": "test_instance"
    }

    with patch('src.cache.get_cache') as mock_get_cache:
      mock_get_cache.side_effect = lambda name, **kwargs: {
          "test_ingester": mock_cached_data,
          "test_ingester.monitor": mock_monitor_data
      }.get(name)

      result = await cache.get_ingester_status("test_ingester")

      assert result is not None
      assert result["name"] == "test_ingester"
      assert result["status"] == "active"
      assert result["latency_ms"] == 150.5

  @pytest.mark.asyncio
  async def test_discover_cluster_state(self):
    """Test discovering complete cluster state."""
    mock_instances = {"instance1": {"status": "active"}}
    mock_ingesters = {"ingester1": {"status": "active"}}
    mock_topics = ["topic1", "topic2"]

    with patch('src.cache.get_active_instances', return_value=mock_instances), \
         patch('src.cache.get_active_ingesters', return_value=mock_ingesters), \
         patch('src.cache.get_topics', return_value=mock_topics):

      result = await cache.discover_cluster_state()

      assert result["total_instances"] == 1
      assert result["total_ingesters"] == 1
      assert result["total_topics"] == 2
      assert "timestamp" in result

  def test_get_status_key(self):
    """Test status key generation."""
    key = cache.get_status_key("test_resource")
    assert key == f"{cache.NS}:status:test_resource"

  @pytest.mark.asyncio
  async def test_get_cached_resources(self):
    """Test getting cached resources."""
    with patch('src.cache.state') as mock_state:
      mock_state.redis.keys = AsyncMock(
          return_value=[b"chomp:status:resource1", b"chomp:status:resource2"])

      result = await cache.get_cached_resources()
      assert result == ["resource1", "resource2"]

  @pytest.mark.asyncio
  async def test_topic_exist_true(self):
    """Test topic existence check when topic exists."""
    with patch('src.cache.get_topics', return_value=["topic1", "topic2"]):
      result = await cache.topic_exist("topic1")
      assert result is True

  @pytest.mark.asyncio
  async def test_topic_exist_false(self):
    """Test topic existence check when topic doesn't exist."""
    with patch('src.cache.get_topics', return_value=["topic1", "topic2"]):
      result = await cache.topic_exist("topic3")
      assert result is False

  @pytest.mark.asyncio
  async def test_topics_exist(self):
    """Test multiple topics existence check."""
    with patch('src.cache.get_topics', return_value=["topic1", "topic2"]):
      result = await cache.topics_exist(["topic1", "topic3"])
      expected = {"topic1": True, "topic3": False}
      assert result == expected
