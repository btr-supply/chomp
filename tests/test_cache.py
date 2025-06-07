"""Tests for src.cache module."""
import pytest
import sys
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch
import pickle

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src import cache
from src.model import Ingester


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
      mock_state.redis.ping = AsyncMock(side_effect=Exception("Connection failed"))
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
      mock_state.redis.get = AsyncMock(return_value=b"worker_2")  # Different owner

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
      mock_state.redis.mget = AsyncMock(return_value=[b"value1", b"value2", None])

      result = await cache.get_cache_batch(["key1", "key2", "key3"])

      expected = {
        "key1": b"value1",
        "key2": b"value2",
        "key3": None
      }
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
  async def test_register_ingester(self):
    """Test ingester registration."""
    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_ingester"
    mock_ingester.to_dict.return_value = {"name": "test_ingester"}

    with patch('src.cache.wait_acquire_registry_lock', return_value=True), \
         patch('src.cache.get_cache', return_value={}), \
         patch('src.cache.cache'), \
         patch('asyncio.gather', new_callable=AsyncMock, return_value=[True, True]), \
         patch('src.cache.release_registry_lock') as mock_release, \
         patch('src.cache.log_info') as mock_log_info:

      result = await cache.register_ingester(mock_ingester)
      assert result is True
      mock_log_info.assert_called_once()
      mock_release.assert_called_once()

  @pytest.mark.asyncio
  async def test_register_ingester_lock_fail(self):
    """Test ingester registration when lock acquisition fails."""
    mock_ingester = Mock(spec=Ingester)
    mock_ingester.name = "test_ingester"

    with patch('src.cache.wait_acquire_registry_lock', return_value=False):
      with pytest.raises(ValueError, match="Failed to acquire registry lock"):
        await cache.register_ingester(mock_ingester)

  @pytest.mark.asyncio
  async def test_get_registered_ingester(self):
    """Test getting registered ingester."""
    test_data = {"name": "test_ingester"}

    with patch('src.cache.get_cache', return_value=test_data):
      result = await cache.get_registered_ingester("test_ingester")
      assert result == test_data

  @pytest.mark.asyncio
  async def test_get_registered_ingesters(self):
    """Test getting all registered ingesters."""
    test_data = {"ingester1": {}, "ingester2": {}}

    with patch('src.cache.get_cache', return_value=test_data):
      result = await cache.get_registered_ingesters()
      assert result == test_data

  def test_get_status_key(self):
    """Test status key generation."""
    key = cache.get_status_key("test_resource")
    assert key == f"{cache.NS}:status:test_resource"

  @pytest.mark.asyncio
  async def test_get_cached_resources(self):
    """Test getting cached resources."""
    with patch('src.cache.state') as mock_state:
      mock_state.redis.keys = AsyncMock(return_value=[b"chomp:status:resource1", b"chomp:status:resource2"])

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
