"""
Purpose: Test suite for services/limiter module
Tests the rate limiting functionality using Redis for storage
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import patch, Mock, AsyncMock

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services import limiter


class TestLimiterService:
  """Test rate limiting service functionality."""

  @pytest.mark.asyncio
  async def test_check_limits_blacklisted_user(self):
    """Test that blacklisted users are rejected."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.blacklist = ["baduser"]
      mock_limiter.whitelist = []
      mock_state.server.limiter = mock_limiter

      error, data = await limiter.check_limits("baduser")

      assert error == "User is blacklisted"
      assert data == {}

  @pytest.mark.asyncio
  async def test_check_limits_whitelisted_user(self):
    """Test that whitelisted users are allowed."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.blacklist = []
      mock_limiter.whitelist = ["gooduser"]
      mock_state.server.limiter = mock_limiter

      error, data = await limiter.check_limits("gooduser")

      assert error == ""
      assert data == {"whitelisted": True}

  @pytest.mark.asyncio
  async def test_check_limits_under_limit(self):
    """Test user under rate limits."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.blacklist = []
      mock_limiter.whitelist = []
      mock_limiter.limits = {"requests": (100, 60)}
      mock_limiter.ppr = {}

      mock_state.server.limiter = mock_limiter
      mock_state.redis.mget = AsyncMock(return_value=["50"])

      error, data = await limiter.check_limits("testuser")

      assert error == ""
      assert "current_counts" in data
      assert data["current_counts"] == [50]

  @pytest.mark.asyncio
  async def test_check_limits_over_limit(self):
    """Test user over rate limits."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.blacklist = []
      mock_limiter.whitelist = []
      mock_limiter.limits = {"requests": (100, 60)}
      mock_limiter.ppr = {}

      mock_state.server.limiter = mock_limiter
      mock_state.redis.mget = AsyncMock(return_value=["150"])

      error, data = await limiter.check_limits("testuser")

      assert "Rate limit exceeded" in error
      assert data == {}

  @pytest.mark.asyncio
  async def test_get_user_limits_success(self):
    """Test getting user limits successfully."""
    with patch('src.services.limiter.state') as mock_state, \
         patch('src.services.limiter.secs_to_ceil_date') as mock_secs, \
         patch('src.services.limiter.fmt_date') as mock_fmt:

      mock_limiter = Mock()
      mock_limiter.limits = {"requests": (100, 60)}
      mock_state.server.limiter = mock_limiter

      # Mock pipeline
      mock_pipe = AsyncMock()
      mock_pipe.get = Mock()
      mock_pipe.ttl = Mock()
      mock_pipe.execute = AsyncMock(return_value=["50", "3600"])
      mock_state.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=mock_pipe)
      mock_state.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)

      mock_secs.return_value = 3600
      mock_fmt.return_value = "2023-12-01T12:00:00Z"

      error, data = await limiter.get_user_limits("testuser")

      assert error == ""
      assert "requests" in data
      assert data["requests"]["cap"] == 100
      assert data["requests"]["remaining"] == 50

  @pytest.mark.asyncio
  async def test_get_user_limits_error(self):
    """Test get_user_limits error handling."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.limits = {"requests": (100, 60)}
      mock_state.server.limiter = mock_limiter

      mock_state.redis.pipeline.side_effect = Exception("Redis error")

      error, data = await limiter.get_user_limits("testuser")

      assert "Error fetching limits" in error
      assert data == {}

  @pytest.mark.asyncio
  async def test_increment_counters_success(self):
    """Test incrementing counters successfully."""
    with patch('src.services.limiter.state') as mock_state, \
         patch('src.services.limiter.secs_to_ceil_date') as mock_secs:

      mock_limiter = Mock()
      mock_limiter.limits = {"requests": (100, 60)}
      mock_state.server.limiter = mock_limiter

      # Mock pipeline
      mock_pipe = AsyncMock()
      mock_pipe.incrby = Mock()
      mock_pipe.expire = Mock()
      mock_pipe.execute = AsyncMock(return_value=[])
      mock_state.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=mock_pipe)
      mock_state.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
      mock_state.redis.get = AsyncMock(return_value="50")

      mock_secs.return_value = 3600

      error, data = await limiter.increment_counters("testuser", 1024, 1)

      assert error == ""
      assert "limits" in data
      assert "remaining" in data

  @pytest.mark.asyncio
  async def test_increment_counters_error(self):
    """Test increment_counters error handling."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.limits = {"requests": (100, 60)}
      mock_state.server.limiter = mock_limiter

      mock_state.redis.pipeline.side_effect = Exception("Redis error")

      error, data = await limiter.increment_counters("testuser", 1024, 1)

      assert "Error incrementing counters" in error
      assert data == {}

  @pytest.mark.asyncio
  async def test_check_limits_with_path_ppr(self):
    """Test check_limits with path-specific points per request."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.blacklist = []
      mock_limiter.whitelist = []
      mock_limiter.limits = {"requests": (100, 60)}
      mock_limiter.ppr = {"/api/heavy": 5}

      mock_state.server.limiter = mock_limiter
      mock_state.redis.mget = AsyncMock(return_value=["50", "10"])

      error, data = await limiter.check_limits("testuser", "/api/heavy")

      assert error == ""
      assert data["ppr"] == 5

  @pytest.mark.asyncio
  async def test_check_limits_no_redis_data(self):
    """Test check_limits when Redis returns None values."""
    with patch('src.services.limiter.state') as mock_state:
      mock_limiter = Mock()
      mock_limiter.blacklist = []
      mock_limiter.whitelist = []
      mock_limiter.limits = {"requests": (100, 60)}
      mock_limiter.ppr = {}

      mock_state.server.limiter = mock_limiter
      mock_state.redis.mget = AsyncMock(return_value=[None])

      error, data = await limiter.check_limits("testuser")

      assert error == ""
      assert data["current_counts"] == [0]

# Legacy test functions for basic imports and structure
def test_limiter_imports():
  """Test that limiter module imports correctly."""
  assert hasattr(limiter, 'check_limits')
  assert hasattr(limiter, 'get_user_limits')
  assert hasattr(limiter, 'increment_counters')

def test_limiter_basic_functionality():
  """Test basic limiter functionality."""
  assert callable(limiter.check_limits)
